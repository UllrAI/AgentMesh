import asyncio
import json
import os
import signal
import sys
import textwrap
from uuid import uuid4

import pytest

from agent_mesh import runtime


def script(path, content):
    path.write_text(textwrap.dedent(content))
    return str(path)


def command(tmp_path, body):
    return {
        "type": "command",
        "workdir": str(tmp_path),
        "command": [sys.executable, script(tmp_path / "handler.py", body)],
    }


async def test_echo_keeps_context_and_distinguishes_questions(tmp_path):
    settings = {"type": "echo"}
    method = {"interaction": "clarify"}
    sessions = []
    first = await runtime.run(
        settings,
        method,
        {"params": {"prompt": "分析退款"}},
        on_session_id=sessions.append,
        state_dir=tmp_path,
    )
    assert first == {
        "status": "needs_input",
        "question": {"text": "请选择范围：API 或 Web？"},
    }
    assert len(sessions) == 1
    second = await runtime.run(
        settings,
        method,
        {"answer_text": "API"},
        sessions[0],
        state_dir=tmp_path,
    )
    assert second == {"status": "succeeded", "result": {"text": "分析退款 | API"}}
    followup = await runtime.run(
        settings,
        method,
        {"params": {"prompt": "还涉及哪些模块？"}},
        sessions[0],
        state_dir=tmp_path,
    )
    assert followup["result"]["text"] == "分析退款 | API | 还涉及哪些模块？"
    direct = await runtime.run(
        settings,
        method,
        {"params": {"prompt": "分析退款", "scope": "web"}},
        state_dir=tmp_path,
    )
    assert direct["status"] == "succeeded"


async def test_command_receives_arguments_and_only_allowed_environment(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("MESH_BROKER_PASSWORD", "broker-secret")
    monkeypatch.setenv("EXAMPLE_TOKEN", "runtime-token")
    monkeypatch.setenv("UNDECLARED_TOKEN", "other-secret")
    settings = command(
        tmp_path,
        """
        import json, os, sys
        request = json.load(sys.stdin)
        values = {name: os.getenv(name) for name in (
            'MESH_BROKER_PASSWORD', 'EXAMPLE_TOKEN', 'UNDECLARED_TOKEN',
        )}
        print(json.dumps({"type":"result", "text":json.dumps([request, values])}))
    """,
    )
    settings["env_allowlist"] = ["MESH_BROKER_PASSWORD", "EXAMPLE_TOKEN"]
    states = []
    request = {
        "method": "run",
        "params": {"prompt": "--help; touch unintended"},
        "workdir": "/bogus",
        "env": {"EXAMPLE_TOKEN": "injected"},
    }
    result = await runtime.run(
        settings,
        {},
        request,
        state_dir=tmp_path / "states",
        on_execution_id=states.append,
        blocked_env=["MESH_BROKER_PASSWORD"],
    )
    actual_request, env = json.loads(result["result"]["text"])
    assert actual_request == {
        "method": "run",
        "session_id": None,
        "params": request["params"],
    }
    assert env == {
        "MESH_BROKER_PASSWORD": None,
        "EXAMPLE_TOKEN": "runtime-token",
        "UNDECLARED_TOKEN": None,
    }
    assert not (tmp_path / "unintended").exists()
    assert len(states) == 1
    assert runtime.execution_ended(tmp_path / "states", states[0])
    runtime.forget_execution(tmp_path / "states", states[0])
    assert not list((tmp_path / "states").iterdir())


async def test_command_session_is_recorded_before_final_event(tmp_path):
    settings = command(
        tmp_path,
        """
        import json, time
        from pathlib import Path
        print(json.dumps({"type":"session.started", "session_id":"opaque-session"}), flush=True)
        while not Path("callback-complete").exists():
            time.sleep(.01)
        print(json.dumps({"type":"question", "text":"API or web?"}))
    """,
    )
    sessions = []

    async def record_session(session):
        sessions.append(session)
        (tmp_path / "callback-complete").touch()

    result = await runtime.run(
        settings,
        {"interaction": "clarify"},
        {"params": {}},
        on_session_id=record_session,
        state_dir=tmp_path / "states",
        execution_timeout_s=3,
    )
    assert sessions == ["opaque-session"]
    assert result == {"status": "needs_input", "question": {"text": "API or web?"}}


async def test_codex_uses_final_message_and_resumes_via_stdin(tmp_path):
    session_id = str(uuid4())
    fake = script(
        tmp_path / "codex.py",
        f"""
        import json, sys
        from pathlib import Path
        args = sys.argv[1:]
        assert args[0] == "exec" and args[-1] == "-"
        assert "approval_policy=\\"never\\"" in args
        schema = json.loads(Path(args[args.index("--output-schema") + 1]).read_bytes())
        assert schema["properties"]["type"]["enum"] == ["question", "result"]
        input_text = sys.stdin.read()
        if "resume" in args:
            assert args.index("--cd") < args.index("resume")
            resume = args[args.index("resume") + 1]
            if resume != {session_id!r}:
                sys.stderr.write("Error: No rollout found for the given ID")
                sys.exit(1)
            assert input_text == "--literal answer"
        def event(kind, **values):
            print(json.dumps({{"type":kind, **values}}), flush=True)
        event("thread.started", thread_id={session_id!r})
        event("turn.started")
        preview = json.dumps({{"type":"question", "text":"progress only?"}})
        event("item.completed", item={{"type":"agent_message", "text":preview}})
        event("item.completed", item={{"type":"agent_message", "text":"Who owns this?"}})
        event("turn.completed")
    """,
    )
    settings = {
        "type": "codex",
        "workdir": str(tmp_path),
        "command": [sys.executable, fake],
        "codex_home": str(tmp_path),
    }
    method = {"interaction": "clarify", "permission_profile": ":read-only"}
    states = []
    first = await runtime.run(
        settings,
        method,
        {"params": {"prompt": "literal prompt"}},
        on_session_id=states.append,
        state_dir=tmp_path / "states",
    )
    assert states == [session_id]
    assert first == {"status": "succeeded", "result": {"text": "Who owns this?"}}
    resumed = await runtime.run(
        settings,
        method,
        {"answer_text": "--literal answer"},
        session_id,
        state_dir=tmp_path / "states",
    )
    assert resumed == first
    missing = await runtime.run(
        settings,
        method,
        {"answer_text": "x"},
        str(uuid4()),
        state_dir=tmp_path / "states",
    )
    assert missing["error"]["code"] == "SESSION_NOT_FOUND"


def test_codex_legacy_sandbox_cannot_override_configured_profile(tmp_path):
    settings = {
        "type": "codex",
        "workdir": str(tmp_path),
        "command": [sys.executable],
        "codex_home": str(tmp_path),
    }
    method = {"permission_profile": "project"}
    config = tmp_path / "config.toml"
    profile_text = '[permissions.project.filesystem]\n":minimal" = "read"\n'
    config.write_text(profile_text)
    assert runtime.ready(settings, method)
    config.write_text('sandbox_mode = "danger-full-access"\n' + profile_text)
    assert not runtime.ready(settings, method)


def test_codex_uses_only_the_explicit_home_and_accepts_other_mcp(tmp_path, monkeypatch):
    caller_home = tmp_path / "caller"
    caller_home.mkdir()
    (caller_home / "config.toml").write_text(
        '[mcp_servers.mesh]\ncommand = "agent-mesh"\nargs = ["mcp"]\n'
    )
    monkeypatch.setenv("CODEX_HOME", str(caller_home))
    settings = {
        "type": "codex",
        "workdir": str(tmp_path),
        "command": [sys.executable],
        "env_allowlist": ["CODEX_HOME"],
    }
    method = {"permission_profile": ":read-only"}
    assert not runtime.ready(settings, method)
    settings["codex_home"] = str(tmp_path)
    config = tmp_path / "config.toml"
    config.write_text(
        '[mcp_servers.mesh]\ncommand = "/opt/docs/bin/read-docs"\nargs = ["mcp"]\n'
        '[mcp_servers.other]\ncommand = "agent-mesh"\nargs = ["--socket", "mcp", "jobs"]\n'
    )
    assert runtime.ready(settings, method)


@pytest.mark.parametrize(
    ("command", "args"),
    [
        ("agent-mesh", ["mcp"]),
        ("/opt/mesh/bin/agent-mesh", ["mcp", "--socket", "/tmp/current.sock"]),
        (
            "./.venv/bin/agent-mesh",
            ["--config", "/tmp/config.yaml", "--socket=/tmp/current.sock", "mcp"],
        ),
    ],
)
def test_codex_rejects_an_enabled_direct_mesh_bridge(tmp_path, command, args):
    settings = {
        "type": "codex",
        "workdir": str(tmp_path),
        "command": [sys.executable],
        "codex_home": str(tmp_path),
    }
    method = {"permission_profile": ":read-only"}
    config = tmp_path / "config.toml"
    entry = f"[mcp_servers.arbitrary_label]\ncommand = {json.dumps(command)}\nargs = {json.dumps(args)}\n"
    config.write_text(entry)
    assert not runtime.ready(settings, method)
    config.write_text(entry + "enabled = false\n")
    assert runtime.ready(settings, method)


async def test_claude_stream_uses_structured_result_and_current_settings(tmp_path):
    session_id = str(uuid4())
    profile = tmp_path / "settings.json"
    profile.write_text("{}")
    fake = script(
        tmp_path / "claude.py",
        f"""
        import json, sys
        from pathlib import Path
        args = sys.argv[1:]
        assert args[args.index("--output-format")+1] == "stream-json"
        assert args[args.index("--settings")+1] == {str(profile)!r}
        assert args[args.index("--resume")+1] == {session_id!r}
        assert json.loads(Path({str(profile)!r}).read_bytes()) == {{"sandbox": {{"enabled": True}}}}
        assert sys.stdin.read() == "API"
        print(json.dumps({{"type":"system","subtype":"init","session_id":{session_id!r}}}))
        print(json.dumps({{
            "type":"result","session_id":{session_id!r},"is_error":False,
            "structured_output":{{"type":"result","text":"API only"}},
        }}))
        print(json.dumps({{"type":"system","subtype":"task_notification"}}))
    """,
    )
    settings = {
        "type": "claude",
        "workdir": str(tmp_path),
        "command": [sys.executable, fake],
    }
    method = {"interaction": "clarify", "permission_profile": str(profile)}
    assert runtime.ready(settings, method)
    profile.write_text('{"sandbox":{"enabled":true}}')
    result = await runtime.run(
        settings,
        method,
        {"answer_text": "API"},
        session_id,
        state_dir=tmp_path / "states",
    )
    assert result == {"status": "succeeded", "result": {"text": "API only"}}


async def test_output_limit_applies_after_json_serialization(tmp_path):
    settings = command(
        tmp_path,
        """
        import json
        print(json.dumps({"type":"result", "text":"你" * 25000}))
    """,
    )
    result = await runtime.run(
        settings, {}, {"params": {}}, state_dir=tmp_path / "states"
    )
    assert result["error"]["code"] == "RESULT_TOO_LARGE"


async def until(predicate, limit=5):
    async with asyncio.timeout(limit):
        while not predicate():
            await asyncio.sleep(0.02)


async def test_timeout_reaps_group_that_ignores_sigterm(tmp_path):
    settings = command(
        tmp_path,
        """
        import signal, subprocess, sys
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        child = subprocess.Popen([sys.executable, "-c",
            "import os,signal,time; from pathlib import Path;"
            "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
            "Path('child-pid').write_text(str(os.getpid()));"
            "time.sleep(30)"
        ])
        child.wait()
    """,
    )
    state_dir = tmp_path / "states"
    ids = []
    result = await runtime.run(
        settings,
        {},
        {"params": {}},
        state_dir=state_dir,
        on_execution_id=ids.append,
        execution_timeout_s=1,
    )
    assert result["status"] == "failed"
    assert result["error"]["code"] == "EXECUTION_TIMEOUT"
    assert runtime.execution_ended(state_dir, ids[0])
    pid = int((tmp_path / "child-pid").read_text())
    assert not process_alive(pid)


def process_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def test_normal_daemon_cancellation_also_reaps_the_group(tmp_path):
    settings = command(
        tmp_path,
        """
        import os, time
        from pathlib import Path
        Path("handler-pid").write_text(str(os.getpid()))
        time.sleep(30)
    """,
    )
    state_dir = tmp_path / "states"
    ids = []
    task = asyncio.create_task(
        runtime.run(
            settings,
            {},
            {"params": {}},
            state_dir=state_dir,
            on_execution_id=ids.append,
        )
    )
    try:
        pid_path = tmp_path / "handler-pid"
        await until(
            lambda: pid_path.exists() and pid_path.read_text().strip().isdigit()
        )
        pid = int(pid_path.read_text())
        assert process_alive(pid)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert runtime.execution_ended(state_dir, ids[0])
    assert not process_alive(pid)


async def test_recovery_can_confirm_group_ended_after_supervisor_loss(tmp_path):
    execution_id = {"kind": "process", "token": str(uuid4())}
    guard = tmp_path / f"exec-{execution_id['token']}.lock"
    group = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(30)",
        start_new_session=True,
    )
    try:
        guard.write_text(json.dumps({"phase": "active", "group": group.pid}))
        assert not runtime.execution_ended(tmp_path, execution_id)
    finally:
        group.kill()
        await group.wait()
    assert runtime.execution_ended(tmp_path, execution_id)


async def test_supervisor_reaps_after_owner_is_killed(tmp_path):
    handler = script(
        tmp_path / "handler.py",
        """
        import os, signal, time
        from pathlib import Path
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        Path("handler-pid").write_text(str(os.getpid()))
        time.sleep(30)
    """,
    )
    config = {
        "type": "command",
        "workdir": str(tmp_path),
        "command": [sys.executable, handler],
    }
    state_dir = tmp_path / "states"
    identifier = tmp_path / "execution-id"
    owner_script = script(
        tmp_path / "owner.py",
        f"""
        import asyncio, json
        from pathlib import Path
        from agent_mesh import runtime
        def save(execution_id):
            Path({str(identifier)!r}).write_text(json.dumps(execution_id))
        asyncio.run(runtime.run({config!r}, {{}}, {{"params": {{}}}},
            state_dir={str(state_dir)!r}, on_execution_id=save))
    """,
    )
    owner = await asyncio.create_subprocess_exec(sys.executable, owner_script)
    try:
        pid_path = tmp_path / "handler-pid"
        await until(
            lambda: pid_path.exists() and pid_path.read_text().strip().isdigit()
        )
        pid = int(pid_path.read_text())
        execution_id = json.loads(identifier.read_text())
        assert not runtime.execution_ended(state_dir, execution_id)
        assert process_alive(pid)
        owner.kill()
        await owner.wait()
        assert owner.returncode == -signal.SIGKILL
        await until(lambda: runtime.execution_ended(state_dir, execution_id))
        assert not process_alive(pid)
        runtime.forget_execution(state_dir, execution_id)
        assert not list(state_dir.iterdir())
    finally:
        if owner.returncode is None:
            owner.kill()
            await owner.wait()
