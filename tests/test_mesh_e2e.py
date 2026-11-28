import asyncio
import contextlib
import json
import os
import shlex
import shutil
import signal
import socket
import sqlite3
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import aiomqtt
import httpx
import pytest
import yaml
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from paho.mqtt import client as mqtt

SETUP = Path(__file__).resolve().parents[1] / "examples" / "local" / "setup.py"
MOSQUITTO = shutil.which("mosquitto") or (
    "/opt/homebrew/sbin/mosquitto"
    if os.access("/opt/homebrew/sbin/mosquitto", os.X_OK)
    else None
)
pytestmark = pytest.mark.skipif(
    not MOSQUITTO or not shutil.which("mosquitto_passwd"),
    reason="Mosquitto and mosquitto_passwd are required for the local integration test",
)
CLI = ("-m", "agent_mesh.cli")
METHOD = "demo.analyze"
CALL_TOPIC = "agents/worker/invoke/caller"
REPLY_TOPIC = "agents/caller/replies/worker"


async def stop(process):
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 5)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            await process.wait()


async def eventually(check, description, timeout=12):
    try:
        async with asyncio.timeout(timeout):
            while not (value := await check()):
                await asyncio.sleep(0.05)
            return value
    except TimeoutError as exc:
        raise AssertionError(f"Timed out waiting for {description}") from exc


class Demo:
    def __init__(self, directory, port, env):
        self.directory = directory
        self.port = port
        self.env = env
        self.broker = None
        self.nodes = {}

    async def process(self, name, *command):
        with (self.directory / f"{name}.log").open("ab") as log:
            return await asyncio.create_subprocess_exec(
                *command, stdout=log, stderr=log, env=self.env
            )

    async def start_broker(self):
        self.broker = await self.process(
            "broker", MOSQUITTO, "-c", str(self.directory / "mosquitto.conf"), "-v"
        )

        async def listening():
            if self.broker.returncode is not None:
                pytest.fail((self.directory / "broker.log").read_text())
            try:
                _, writer = await asyncio.open_connection("127.0.0.1", self.port)
            except OSError:
                return False
            writer.close()
            await writer.wait_closed()
            return True

        await eventually(listening, "the loopback broker to listen")

    async def start_node(self, node):
        process = self.nodes[node] = await self.process(
            node, sys.executable, *CLI, "serve", "--config", str(self.config(node))
        )

        async def bound():
            if process.returncode is not None:
                pytest.fail((self.directory / f"{node}.log").read_text())
            return self.socket(node).is_socket()

        await eventually(bound, f"{node}'s Unix socket")

    def config(self, node):
        return self.directory / f"{node}.yaml"

    def socket(self, node):
        return self.directory / f"{node}.sock"

    def password(self, node):
        return self.env[f"MESH_{node.upper()}_PASSWORD"]

    async def cli(self, node, *args, expected=0):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            *CLI,
            "--config",
            str(self.config(node)),
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 35)
        finally:
            await stop(process)
        assert process.returncode == expected, (args, stdout.decode(), stderr.decode())
        return json.loads(stdout)

    async def expect_node(self, reporting_node, agent, status):
        async def discovery():
            agents = (await self.cli(reporting_node, "list"))["agents"]
            return next(
                (
                    row
                    for row in agents
                    if row["id"] == agent and row["status"] == status
                ),
                None,
            )

        return await eventually(
            discovery, f"{reporting_node} to report {agent} {status}"
        )

    async def wait_inbound(self, job_id, status):
        async def persisted():
            with contextlib.closing(
                sqlite3.connect(self.directory / "worker.db")
            ) as db:
                row = db.execute(
                    "SELECT snapshot FROM inbound WHERE caller_id=? AND job_id=?",
                    ("caller", job_id),
                ).fetchone()
            snapshot = json.loads(row[0]) if row else None
            return snapshot if snapshot and snapshot["status"] == status else None

        return await eventually(persisted, f"worker to persist {job_id} as {status}")


@pytest.fixture
async def demo():
    with tempfile.TemporaryDirectory(prefix="am-e2e-", dir="/tmp") as root:
        directory = Path(root) / "demo"
        with socket.socket() as port_probe:
            port_probe.bind(("127.0.0.1", 0))
            port = port_probe.getsockname()[1]
        setup = await asyncio.create_subprocess_exec(
            sys.executable,
            str(SETUP),
            str(directory),
            "--port",
            str(port),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(setup.communicate(), 12)
        finally:
            await stop(setup)
        assert setup.returncode == 0, (stdout.decode(), stderr.decode())

        env = os.environ.copy()
        env.pop("AGENT_MESH_SOCKET", None)
        for line in (directory / "env").read_text().splitlines():
            _, assignment = shlex.split(line)
            key, value = assignment.split("=", 1)
            env[key] = value
        if os.geteuid() == 0:
            # Mosquitto otherwise drops to an account that cannot read this private fixture.
            with (directory / "mosquitto.conf").open("a") as conf:
                conf.write("user root\n")

        mesh = Demo(directory, port, env)
        try:
            await mesh.start_broker()
            yield mesh
        finally:
            await asyncio.gather(*(stop(process) for process in mesh.nodes.values()))
            if mesh.broker is not None:
                await stop(mesh.broker)


async def tool(session, name, **arguments):
    result = await session.call_tool(f"mesh_{name}", arguments)
    assert not result.isError, result.content
    return result.structuredContent


async def test_cli_and_mcp_find_lost_question_after_broker_reconnect(demo):
    await demo.start_node("worker")
    await demo.start_node("caller")
    worker = await demo.expect_node("caller", "worker", "online")
    card = await demo.cli("caller", "inspect", "worker")
    assert worker["capabilities"] == card["capabilities"]
    assert card["capabilities"][0]["interaction"] == "clarify"

    # Leave the worker connected and able to execute, but discard delivery to the caller.
    acl = demo.directory / "acl"
    original_acl = acl.read_text()
    await stop(demo.broker)
    await demo.expect_node("caller", "worker", "unknown")
    acl.write_text(original_acl.replace("topic read agents/caller/replies/+\n", ""))
    await demo.start_broker()
    await demo.expect_node("worker", "worker", "online")
    await demo.expect_node("caller", "worker", "online")

    prompt = "Find module boundaries for the release fixture"
    submitted = await demo.cli("caller", "call", "worker", METHOD, "--prompt", prompt)
    job_id = submitted["job_id"]
    assert submitted["status"] == "unconfirmed"
    expected = await demo.wait_inbound(job_id, "needs_input")
    missed = await demo.cli("caller", "job", job_id, "--wait-timeout-s", "0.2")
    assert missed["status"] == "unconfirmed" and missed["snapshot"] is None

    await stop(demo.broker)
    await demo.expect_node("caller", "worker", "unknown")
    offline_jobs = await demo.cli("caller", "jobs")
    assert [row["job_id"] for row in offline_jobs["jobs"]] == [job_id]
    refused = await demo.cli(
        "caller", "call", "worker", METHOD, "--prompt", "offline", expected=1
    )
    assert refused["error"]["code"] == "NOT_READY"
    acl.write_text(original_acl)
    await demo.start_broker()
    await demo.expect_node("worker", "worker", "online")
    await demo.expect_node("caller", "worker", "online")

    server = StdioServerParameters(
        command=sys.executable,
        args=[*CLI, "mcp", "--socket", str(demo.socket("caller"))],
        env=demo.env,
    )
    async with (
        asyncio.timeout(25),
        stdio_client(server) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        assert (await tool(session, "list", method=METHOD))["agents"] == [worker]
        assert await tool(session, "inspect", agent_id="worker") == card
        recovered = await tool(session, "job_status", job_id=job_id, wait_timeout_s=5)
        assert recovered["snapshot"] == expected
        assert recovered["answer"]["status"] == "waiting"

        answer = await demo.cli("caller", "answer", job_id, "--text", "API", "--wait")
        assert answer["status"] == "succeeded"
        assert answer["answer_to"] == job_id and answer["job_id"] != job_id
        assert answer["session_id"] == recovered["session_id"]
        assert prompt in answer["result"]["text"] and "API" in answer["result"]["text"]

        followup = await tool(
            session,
            "invoke",
            target="worker",
            method=METHOD,
            params={"prompt": "Check contracts"},
            session_id=answer["session_id"],
        )
        final = await tool(
            session, "job_status", job_id=followup["job_id"], wait_timeout_s=5
        )
        assert (
            final["status"] == "succeeded"
            and final["session_id"] == answer["session_id"]
        )
        assert all(
            text in final["result"]["text"]
            for text in (prompt, "API", "Check contracts")
        )
        original = await demo.cli("caller", "job", job_id)
        assert (
            original["status"] == "needs_input"
            and original["answer"]["status"] == "answered"
        )
        assert original["latest_answer"]["job_id"] == answer["job_id"]
        assert original["latest_answer"]["snapshot"]["result"] == answer["result"]
        assert {row["job_id"] for row in (await tool(session, "jobs"))["jobs"]} == {
            job_id,
            answer["job_id"],
            followup["job_id"],
        }


async def test_browser_workbench_answers_a_cli_task(demo):
    await demo.start_node("worker")
    await demo.start_node("caller")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        *CLI,
        "ui",
        "--config",
        str(demo.config("caller")),
        "--no-open",
        stdout=asyncio.subprocess.PIPE,
        env=demo.env,
    )
    try:
        link = urlsplit(
            (await asyncio.wait_for(process.stdout.readline(), 5)).decode().strip()
        )
        assert link.hostname == "127.0.0.1"
        token = parse_qs(link.fragment)["token"][0]
        async with httpx.AsyncClient(
            base_url=link._replace(fragment="").geturl(),
            headers={"Authorization": f"Bearer {token}"},
            trust_env=False,
        ) as browser:

            async def list_nodes():
                response = await browser.post("/api/list", json={})
                response.raise_for_status()
                discovery = response.json()
                return discovery, {node["id"]: node for node in discovery["agents"]}

            async def online_nodes():
                discovery, nodes = await list_nodes()
                if set(nodes) == {"caller", "worker"} and all(
                    node["status"] == "online" for node in nodes.values()
                ):
                    return discovery, nodes

            discovery, online = await eventually(
                online_nodes, "UI to list both nodes as online"
            )
            assert discovery["local_id"] == "caller"
            assert discovery["broker_connected"] is True
            assert online["caller"]["capabilities"] == []
            (capability,) = online["worker"]["capabilities"]
            assert capability["method"] == METHOD
            assert (
                capability["description"]
                == "Analyze the demo project with an explicit API or Web scope"
            )

            question = await demo.cli(
                "caller",
                "call",
                "worker",
                METHOD,
                "--prompt",
                "Review orders",
                "--wait",
            )
            jobs = (await browser.post("/api/jobs", json={})).json()["jobs"]
            assert jobs[0]["job_id"] == question["job_id"]
            assert jobs[0]["answer"]["status"] == "waiting"
            answer = await browser.post(
                "/api/answer", json={"job_id": question["job_id"], "text": "API"}
            )
            answer.raise_for_status()
            result = await demo.cli(
                "caller", "job", answer.json()["job_id"], "--wait-timeout-s", "5"
            )
            assert result["session_id"] == question["session_id"]
            assert result["result"]["text"] == "Review orders | API"

            await stop(demo.nodes["worker"])

            async def offline_worker():
                _, nodes = await list_nodes()
                if nodes.get("worker", {}).get("status") == "offline":
                    return nodes

            offline = await eventually(
                offline_worker, "UI to retain the offline worker"
            )
            assert offline["caller"] == online["caller"]
            assert offline["worker"] == {**online["worker"], "status": "offline"}
    finally:
        await stop(process)


async def test_target_sigkill_recovers_history_and_execution_slot(demo):
    workdir = demo.directory / "project"
    handler = workdir / "handler.py"
    handler.write_text(
        textwrap.dedent("""\
        import json
        import os
        import signal
        import subprocess
        import sys
        import time
        from pathlib import Path

        prompt = json.load(sys.stdin)["params"]["prompt"]
        with Path("effects.log").open("a") as effects:
            effects.write(prompt + "\\n")
            effects.flush()
            os.fsync(effects.fileno())
        if prompt == "history":
            while not Path("release-history").exists():
                time.sleep(0.02)
        elif prompt == "hold":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(45)"])
            marker = Path("held-pids.tmp")
            marker.write_text(json.dumps([os.getpid(), child.pid]))
            marker.replace("held-pids.json")
            time.sleep(45)
            child.wait()
        print(f"completed {prompt}")
        """)
    )
    config_file = demo.config("worker")
    config = yaml.safe_load(config_file.read_text())
    config["runtime"].update(
        type="command",
        command=[sys.executable, str(handler)],
        max_concurrent_jobs=1,
        execution_timeout_s=60,
        methods=[{"name": METHOD}],
    )
    config_file.write_text(yaml.safe_dump(config))
    await demo.start_node("worker")
    await demo.start_node("caller")
    await demo.expect_node("caller", "worker", "online")

    async def accept(prompt):
        submitted = await demo.cli(
            "caller", "call", "worker", METHOD, "--prompt", prompt, "--async"
        )
        job_id = submitted["job_id"]

        async def ready():
            state = await demo.cli("caller", "job", job_id)
            return state["status"] == "accepted"

        await eventually(ready, f"confirmed acceptance of {prompt}")
        return job_id

    history_id = await accept("history")
    await stop(demo.nodes["caller"])
    (workdir / "release-history").touch()
    historical = await demo.wait_inbound(history_id, "succeeded")
    await demo.start_node("caller")
    await demo.expect_node("caller", "worker", "online")
    cached = next(
        row
        for row in (await demo.cli("caller", "jobs"))["jobs"]
        if row["job_id"] == history_id
    )
    assert cached["snapshot"]["status"] == "accepted"

    held_id = await accept("hold")
    marker = workdir / "held-pids.json"

    async def started():
        return json.loads(marker.read_text()) if marker.exists() else None

    pids = await eventually(started, "the held Runtime and its child to start")
    for pid in pids:
        os.kill(pid, 0)
    target = demo.nodes["worker"]
    target.kill()
    assert await target.wait() == -signal.SIGKILL
    await demo.expect_node("caller", "worker", "offline")
    await demo.start_node("worker")
    await demo.expect_node("caller", "worker", "online")

    history = await demo.cli("caller", "job", history_id, "--wait-timeout-s", "5")
    assert history["snapshot"] == historical and history["result"] == {
        "text": "completed history"
    }
    old = await demo.cli("caller", "job", held_id, "--wait-timeout-s", "5", expected=1)
    assert old["job_id"] == held_id and old["status"] == "unknown"
    assert old["error"]["code"] == "OUTCOME_UNKNOWN"

    async def recovered():
        for pid in pids:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, 0)
                return False
        with contextlib.closing(sqlite3.connect(demo.directory / "worker.db")) as db:
            return (
                db.execute(
                    "SELECT 1 FROM execution_slots WHERE job_id=?", (held_id,)
                ).fetchone()
                is None
            )

    await eventually(recovered, "the held group to exit and its slot to be released")
    fresh = await demo.cli(
        "caller", "call", "worker", METHOD, "--prompt", "fresh", "--wait"
    )
    assert fresh["job_id"] not in {history_id, held_id}
    assert fresh["status"] == "succeeded" and fresh["result"] == {
        "text": "completed fresh"
    }
    assert (workdir / "effects.log").read_text().splitlines() == [
        "history",
        "hold",
        "fresh",
    ]


def publish_ack(demo, topic):
    # aiomqtt exposes a QoS confirmation without the broker's reason code.
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    client.username_pw_set("caller", demo.password("caller"))
    results = []

    def publish(connection, userdata, flags, reason, properties):
        if reason.is_failure:
            results.append(reason.value)
        else:
            connection.publish(topic, "{}", qos=1)

    def confirmed(connection, userdata, message_id, reason, properties):
        results.append(reason.value)

    client.on_connect = publish
    client.on_publish = confirmed
    client.connect("127.0.0.1", demo.port)
    try:
        deadline = time.monotonic() + 5
        while not results and time.monotonic() < deadline:
            client.loop(timeout=0.1)
        assert results, "Broker did not return a QoS 1 acknowledgement"
        return results[0]
    finally:
        client.disconnect()


def invocation():
    return {
        "spec_version": "1.3",
        "type": "invoke",
        "job_id": str(uuid4()),
        "expires_at": int(time.time()) + 30,
        "method": METHOD,
        "params": {"prompt": "Retained call fixture", "scope": "API"},
    }


async def test_broker_acl_and_worker_discard_retained_calls(demo):
    # The caller may use its own sender Topic. The worker identity in this Topic is unauthorized.
    code = await asyncio.to_thread(publish_ack, demo, "agents/worker/invoke/worker")
    assert code == 0x87  # MQTT 5 Not authorized.

    async with aiomqtt.Client(
        "127.0.0.1",
        demo.port,
        username="caller",
        password=demo.password("caller"),
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await client.subscribe(REPLY_TOPIC, qos=1)

        async def state(job_id, *, terminal=False):
            async with asyncio.timeout(5):
                async for message in client.messages:
                    reply = json.loads(message.payload)
                    if reply["job_id"] == job_id and (
                        not terminal or reply.get("status") == "succeeded"
                    ):
                        return reply
            raise AssertionError(f"Missing reply for {job_id}")

        async def missing(retained):
            query = {
                "spec_version": "1.3",
                "type": "query",
                "job_id": retained["job_id"],
            }
            await client.publish(CALL_TOPIC, json.dumps(query), qos=1)
            reply = await state(retained["job_id"])
            assert (reply["type"], reply["error"]["code"]) == (
                "query_error",
                "JOB_NOT_FOUND",
            )

        before_start = invocation()
        await client.publish(CALL_TOPIC, json.dumps(before_start), qos=1, retain=True)
        await demo.start_node("worker")
        await demo.expect_node("worker", "worker", "online")
        await missing(before_start)

        live = invocation()
        await client.publish(CALL_TOPIC, json.dumps(live), qos=1, retain=True)
        await missing(live)

        normal = invocation()
        await client.publish(CALL_TOPIC, json.dumps(normal), qos=1)
        assert (await state(normal["job_id"], terminal=True))["status"] == "succeeded"
