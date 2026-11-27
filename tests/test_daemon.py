import asyncio
import json
import socket
import sqlite3
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import pytest

from agent_mesh import daemon
from agent_mesh.store import Store
from agent_mesh.transport import encode

METHOD = "echo"


def config(directory, slots=1):
    return {
        "agent_id": "local",
        "job_store": str(directory / "jobs.db"),
        "mqtt": {"password_env": "MESH_PASSWORD"},
        "runtime": {
            "type": "echo",
            "methods": [{"name": METHOD, "interaction": "clarify"}],
            "execution_timeout_s": 30,
            "max_concurrent_jobs": slots,
        },
    }


def invocation(**fields):
    request = {
        "spec_version": "1.3",
        "type": "invoke",
        "job_id": str(uuid4()),
        "method": METHOD,
        "expires_at": int(time.time()) + 30,
    }
    if "answer_to" not in fields:
        request["params"] = {"prompt": "analyze"}
    return {**request, **fields}


def response(job_id, kind, **fields):
    return {"spec_version": "1.3", "type": kind, "job_id": job_id, **fields}


def save(mesh, **kwargs):
    job = invocation(**kwargs)
    mesh.store.save_outbound("peer", encode(job))
    return job["job_id"]


async def dispatch(mesh, action, data):
    await mesh.packet(action, "peer", encode(data), data)


@pytest.fixture
async def mesh(tmp_path):
    instance = daemon.Mesh(config(tmp_path, slots=2))
    instance.link.client = object()
    yield instance
    tasks = tuple(instance.work)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    instance.store.close()


async def test_zero_wait_gets_the_query_reply_without_waiting_for_execution(
    mesh, monkeypatch
):
    jid = save(mesh)
    unrelated = save(mesh)
    published = []

    async def publish(topic, payload, *, expiry):
        published.append((topic, json.loads(payload)))

        async def deliver():
            await dispatch(
                mesh,
                "replies",
                response(unrelated, "job", status="succeeded", result="unrelated"),
            )
            await asyncio.sleep(0.01)
            await dispatch(
                mesh,
                "replies",
                response(jid, "job", status="accepted", execution_timeout_s=30),
            )

        mesh.start(deliver())

    monkeypatch.setattr(mesh.link, "publish", publish)
    result = await mesh.status({"job_id": jid, "wait_timeout_s": 0})
    assert result["status"] == "accepted"
    assert published == [("agents/peer/invoke/local", response(jid, "query"))]


async def test_wait_timeout_covers_blocked_query_publish(mesh, monkeypatch):
    jid = save(mesh)

    async def stalled_publish(*args, **kwargs):
        await asyncio.Future()

    monkeypatch.setattr(mesh.link, "publish", stalled_publish)
    result = await asyncio.wait_for(
        mesh.status({"job_id": jid, "wait_timeout_s": 0.025}), timeout=0.4
    )
    assert result["status"] == "unconfirmed"
    assert result["job_id"] == jid


async def test_wait_returns_terminal_even_while_query_publish_is_blocked(
    mesh, monkeypatch
):
    jid = save(mesh)

    async def stalled_publish(*args, **kwargs):
        await dispatch(
            mesh, "replies", response(jid, "job", status="succeeded", result="done")
        )
        await asyncio.Future()

    monkeypatch.setattr(mesh.link, "publish", stalled_publish)
    result = await asyncio.wait_for(
        mesh.status({"job_id": jid, "wait_timeout_s": 20}), timeout=0.4
    )
    assert result["status"] == "succeeded"
    assert result["result"] == "done"


async def test_detail_probes_answer_tail_but_bounded_wait_stays_on_explicit_round(
    mesh, monkeypatch
):
    root = save(mesh)
    session_id = str(uuid4())
    await dispatch(
        mesh,
        "replies",
        response(
            root,
            "job",
            status="needs_input",
            session_id=session_id,
            question={"text": "Which scope?", "expires_at": int(time.time()) + 30},
        ),
    )
    answer = save(mesh, answer_to=root, answer_text="API", session_id=session_id)
    await dispatch(
        mesh,
        "replies",
        response(
            answer,
            "job",
            status="accepted",
            session_id=session_id,
            execution_timeout_s=30,
        ),
    )
    queries = []

    async def publish(topic, payload, *, expiry):
        queries.append(json.loads(payload))
        await asyncio.sleep(0.01)
        await dispatch(
            mesh,
            "replies",
            response(
                answer, "job", status="succeeded", session_id=session_id, result="done"
            ),
        )

    monkeypatch.setattr(mesh.link, "publish", publish)
    explicit = await mesh.status({"job_id": root, "wait_timeout_s": 0.1})
    assert queries == []
    assert explicit["status"] == "needs_input"
    assert explicit["latest_answer"]["snapshot"]["status"] == "accepted"

    detail = await mesh.status({"job_id": root})
    assert queries == [response(answer, "query")]
    assert detail["status"] == "needs_input"
    assert detail["latest_answer"]["snapshot"]["result"] == "done"


async def test_retry_skips_explicit_busy_but_keeps_exact_unconfirmed_request(
    mesh, monkeypatch
):
    monkeypatch.setattr(daemon, "RECEIPT_TIMEOUT_S", 0.01, raising=False)
    rejected = save(mesh)
    await dispatch(
        mesh,
        "replies",
        response(rejected, "invoke_error", error={"code": "BUSY", "message": "In use"}),
    )
    request = invocation()
    original = json.dumps(request, indent=2).encode()
    mesh.store.save_outbound("peer", original)
    outgoing = []

    async def publish(topic, payload, *, expiry):
        outgoing.append(payload)

    monkeypatch.setattr(mesh.link, "publish", publish)
    await asyncio.wait_for(mesh.retry_once(rejected, mesh.link.client), timeout=0.3)
    await asyncio.wait_for(
        mesh.retry_once(request["job_id"], mesh.link.client), timeout=0.3
    )
    assert outgoing == [original]
    assert mesh.view(rejected)["invoke_error"]["code"] == "BUSY"


async def test_submit_rejects_another_answer_while_confirmation_is_pending(
    mesh, monkeypatch
):
    root = save(mesh)
    session_id = str(uuid4())
    await dispatch(
        mesh,
        "replies",
        response(
            root,
            "job",
            status="needs_input",
            session_id=session_id,
            question={"text": "Which scope?", "expires_at": int(time.time()) + 30},
        ),
    )
    outgoing = []

    async def publish(topic, payload, *, expiry):
        outgoing.append(json.loads(payload))

    monkeypatch.setattr(mesh.link, "publish", publish)
    args = {"job_id": root, "text": "API"}
    first = await mesh.submit(args, answer=True)
    with pytest.raises(daemon.LocalError, match=first["job_id"]):
        await mesh.submit(args, answer=True)
    assert len(outgoing) == 1
    await dispatch(
        mesh,
        "replies",
        response(
            first["job_id"], "invoke_error", error={"code": "BUSY", "message": "In use"}
        ),
    )
    second = await mesh.submit(args, answer=True)
    assert (second["answer_to"], second["session_id"]) == (root, session_id)
    assert second["job_id"] != first["job_id"]


async def test_terminal_write_failure_rejects_new_work_and_preserves_durable_reply(
    mesh, monkeypatch
):
    monkeypatch.setattr(daemon, "STORAGE_RETRY_S", 0.01, raising=False)
    write_failed = asyncio.Event()
    available = asyncio.Event()
    first = invocation()
    another = invocation()
    outgoing = []
    finish = mesh.store.finish_inbound

    def finish_inbound(*args, **kwargs):
        if not available.is_set():
            write_failed.set()
            raise sqlite3.OperationalError("database unavailable")
        return finish(*args, **kwargs)

    async def publish(topic, payload, *, expiry):
        outgoing.append(json.loads(payload))

    async def runtime(*args, **kwargs):
        return {"status": "succeeded", "result": "done"}

    monkeypatch.setattr(mesh.store, "finish_inbound", finish_inbound)
    monkeypatch.setattr(mesh.link, "publish", publish)
    monkeypatch.setattr(daemon.runtime, "run", runtime)
    await dispatch(mesh, "invoke", first)
    await asyncio.wait_for(write_failed.wait(), timeout=0.3)
    await dispatch(mesh, "invoke", another)
    await dispatch(mesh, "invoke", response(first["job_id"], "query"))
    assert outgoing[1]["error"]["code"] == "NOT_READY"
    assert outgoing[2]["status"] == "accepted"
    assert not any(message.get("status") == "succeeded" for message in outgoing)

    available.set()
    await asyncio.wait_for(asyncio.gather(*mesh.work), timeout=0.3)
    assert outgoing[-1]["result"] == "done"
    assert mesh.store.query_inbound("peer", first["job_id"]) == outgoing[-1]


async def test_bad_runtime_output_releases_slot_after_persisting_protocol_failure(
    mesh, monkeypatch
):
    first = invocation()
    outgoing = []

    async def publish(topic, payload, *, expiry):
        outgoing.append(json.loads(payload))

    async def runtime(*args, **kwargs):
        return {"status": "succeeded", "result": float("nan")}

    monkeypatch.setattr(mesh.link, "publish", publish)
    monkeypatch.setattr(daemon.runtime, "run", runtime)
    await dispatch(mesh, "invoke", first)
    await asyncio.wait_for(asyncio.gather(*mesh.work), timeout=0.3)
    assert outgoing[-1]["status"] == "failed"
    assert outgoing[-1]["error"]["code"] == "EXECUTION_ERROR"
    assert mesh.store.recover_inbound() == []


async def test_outbound_reply_write_error_keeps_transport_available(mesh, monkeypatch):
    jid = save(mesh)
    good = mesh.store.record_outbound_reply

    def failing(*args):
        raise sqlite3.OperationalError("database unavailable")

    reply = response(jid, "job", status="succeeded", result="done")
    monkeypatch.setattr(mesh.store, "record_outbound_reply", failing)
    await dispatch(mesh, "replies", reply)
    assert mesh.view(jid)["status"] == "unconfirmed"
    monkeypatch.setattr(mesh.store, "record_outbound_reply", good)
    await dispatch(mesh, "replies", reply)
    assert mesh.view(jid)["status"] == "succeeded"


async def test_restart_keeps_read_access_until_storage_and_old_slot_are_reconciled(
    tmp_path, monkeypatch
):
    settings = config(tmp_path)
    first = invocation()
    seed = Store(settings["job_store"])
    try:
        seed.accept_inbound("peer", encode(first), methods={METHOD: {}}, max_slots=1)
        seed.record_execution(
            "peer",
            first["job_id"],
            execution_id={"kind": "process", "token": str(uuid4())},
        )
    finally:
        seed.close()
    recover = Store.recover_inbound
    storage_ok = False
    ended = False

    def recover_inbound(store):
        if not storage_ok:
            raise sqlite3.OperationalError("database unavailable")
        return recover(store)

    monkeypatch.setattr(Store, "recover_inbound", recover_inbound)
    monkeypatch.setattr(daemon.runtime, "execution_ended", lambda *args: ended)
    monkeypatch.setattr(daemon.runtime, "forget_execution", lambda *args: None)
    mesh = daemon.Mesh(settings)
    outgoing = []

    async def publish(topic, payload, *, expiry):
        outgoing.append(json.loads(payload))

    monkeypatch.setattr(mesh.link, "publish", publish)
    try:
        await dispatch(mesh, "invoke", response(first["job_id"], "query"))
        assert outgoing[-1]["status"] == "accepted"
        await dispatch(mesh, "invoke", invocation())
        assert outgoing[-1]["error"]["code"] == "NOT_READY"

        storage_ok = True
        mesh.reconcile()
        await dispatch(mesh, "invoke", response(first["job_id"], "query"))
        assert outgoing[-1]["status"] == "unknown"
        await dispatch(mesh, "invoke", invocation())
        assert outgoing[-1]["error"]["code"] == "NOT_READY"

        ended = True
        mesh.reconcile()
        await dispatch(mesh, "invoke", invocation())
        assert outgoing[-1]["status"] == "accepted"
        await asyncio.wait_for(asyncio.gather(*mesh.work), timeout=0.3)
    finally:
        mesh.store.close()


def test_instance_lock_conflicts_across_database_symlinks(tmp_path):
    database = tmp_path / "actual.db"
    database.touch()
    alias = tmp_path / "alias.db"
    alias.symlink_to(database)
    with daemon.instance_lock(database):
        with (
            pytest.raises(ValueError, match="already in use"),
            daemon.instance_lock(alias),
        ):
            pytest.fail("a second daemon could recover the same database")


async def test_stale_socket_cleanup_preserves_a_running_listener():
    with tempfile.TemporaryDirectory(prefix="mesh-", dir="/tmp") as directory:
        path = Path(directory) / "live.sock"
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(path))
            listener.listen()
            with pytest.raises(ValueError, match="socket already in use"):
                await daemon.remove_stale_socket(path)
            assert path.is_socket()
        await daemon.remove_stale_socket(path)
        assert not path.exists()


async def test_stale_socket_cleanup_preserves_dangling_symlinks(tmp_path):
    path = tmp_path / "mesh.sock"
    path.symlink_to(tmp_path / "missing.sock")
    with pytest.raises(ValueError, match="occupied by a non-socket"):
        await daemon.remove_stale_socket(path)
    assert path.is_symlink()
