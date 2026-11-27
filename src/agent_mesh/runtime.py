"""Run a locally configured agent method and return its Mesh terminal fields."""

import asyncio
import fcntl
import inspect
import json
import math
import os
import re
import shutil
import sys
import tempfile
import tomllib
from pathlib import Path
from uuid import UUID, uuid4

from ._runtime_worker import TERMINATION_GRACE, group_exists

MAX_RESULT_BYTES = (
    64 * 1024 - 512
)  # Leave room for job identifiers and question expiry.
MAX_EVENT_BYTES = 1024 * 1024
SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "enum": ["question", "result"]},
        "text": {"type": "string"},
    },
    "required": ["type", "text"],
    "additionalProperties": False,
}
CLARIFY_INSTRUCTIONS = (
    "Return a result when you can complete the request. If a choice or missing fact "
    "would materially change the result, return a concise question and wait for a "
    "reply in a new turn. Use the structured response type 'question' only for a "
    "question the caller needs to answer. Apply this instruction in follow-up turns.\n\n"
    "Request:\n"
)
MISSING_SESSION = re.compile(
    r"no (?:rollout|conversation|session) found|"
    r"(?:could not|failed to) find (?:the )?(?:conversation|session)|"
    r"(?:conversation|session)[^\r\n]{0,100}not found",
    re.IGNORECASE,
)
INHERITED_ENV = {
    "HOME",
    "LANG",
    "LC_ALL",
    "LOGNAME",
    "PATH",
    "SHELL",
    "TERM",
    "TMP",
    "TMPDIR",
    "TEMP",
    "TZ",
    "USER",
}
_default_state = None


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _object(text):
    try:
        value = json.loads(text)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _uuid(value):
    try:
        return str(UUID(value)) if isinstance(value, str) else None
    except ValueError:
        return None


def _adapter(runtime):
    kind = runtime.get("type", "echo")
    if kind == "claude_code":
        return "claude"
    return "command" if kind == "api" else kind


def _environment(runtime, blocked_env):
    blocked = set(blocked_env)
    allowed = (INHERITED_ENV | set(runtime.get("env_allowlist", ()))) - blocked
    env = {name: value for name, value in os.environ.items() if name in allowed}
    for setting, name in (
        ("codex_home", "CODEX_HOME"),
        ("claude_config_dir", "CLAUDE_CONFIG_DIR"),
    ):
        if runtime.get(setting) and name not in blocked:
            env[name] = str(Path(runtime[setting]).expanduser().resolve())
    return env


def _command(runtime, kind, env):
    default = [kind] if kind in {"codex", "claude"} else None
    argv = runtime.get("command", default)
    if (
        not isinstance(argv, list)
        or not argv
        or not all(
            isinstance(part, str) and part and "\x00" not in part for part in argv
        )
    ):
        raise ValueError("runtime.command must be a nonempty argv array")
    prog = Path(argv[0]).expanduser()
    if "/" in argv[0] and not prog.is_absolute():
        prog = Path(runtime.get("workdir", ".")).expanduser() / prog
    executable = shutil.which(str(prog), path=env.get("PATH", os.defpath))
    if not executable:
        raise ValueError("Runtime executable is unavailable")
    executable = Path(executable).absolute()
    if kind == "codex" and "command" not in runtime:
        # The sandbox helper reexecutes the launcher, which must be readable.
        executable = executable.resolve()
    return [str(executable), *argv[1:]]


def _is_mesh_mcp(entry):
    command = entry.get("command")
    if not isinstance(command, str) or Path(command).name != "agent-mesh":
        return False
    # The bridge allows --config and --socket before or after the subcommand.
    args = iter(entry.get("args", ()))
    for arg in args:
        if arg in ("--config", "--socket"):
            next(args, None)
        elif not isinstance(arg, str) or not arg.startswith(("--config=", "--socket=")):
            return arg == "mcp"
    return False


def _codex_settings(runtime, env):
    home = env.get("CODEX_HOME")
    if not runtime.get("codex_home") or not home or not Path(home).is_dir():
        raise ValueError("Codex requires a dedicated runtime.codex_home")
    path = Path(home) / "config.toml"
    try:
        with path.open("rb") as config:
            settings = tomllib.load(config)
    except FileNotFoundError:
        return {}
    servers = settings.get("mcp_servers", {})
    if not isinstance(servers, dict) or not all(
        isinstance(entry, dict) for entry in servers.values()
    ):
        raise ValueError("Invalid Codex MCP server configuration")
    if any(
        entry.get("enabled", True) and _is_mesh_mcp(entry) for entry in servers.values()
    ):
        raise ValueError("Inbound Codex cannot load the Agent Mesh MCP bridge")
    return settings


def _profile(runtime, method, kind, env):
    profile = method.get("permission_profile")
    if kind not in {"codex", "claude"}:
        return None
    settings = _codex_settings(runtime, env) if kind == "codex" else {}
    if not profile:
        if runtime.get("permissions_via_host") is True:
            return None
        raise ValueError("Runtime permission_profile is required")
    if kind == "claude":
        path = Path(profile).expanduser().resolve()
        if (
            not path.is_file()
            or path.stat().st_size > 2 * 1024 * 1024
            or _object(path.read_bytes()) is None
        ):
            raise ValueError(
                "Claude permission_profile must be a local settings JSON file"
            )
        return str(path)
    if not isinstance(profile, str) or not re.fullmatch(r"[:A-Za-z0-9_-]+", profile):
        raise ValueError("Invalid Codex permission profile")
    built_in = profile in {":read-only", ":workspace", ":danger-full-access"}
    if "sandbox_mode" in settings or "sandbox_workspace_write" in settings:
        raise ValueError("Legacy sandbox settings override Codex permission profiles")
    if not built_in and profile not in settings.get("permissions", {}):
        raise ValueError("Unknown Codex permission profile")
    return profile


def ready(runtime, method=None, *, blocked_env=()):
    """Check that the locally configured executable, directory and profile exist."""
    kind = _adapter(runtime)
    if kind == "echo":
        return True
    if kind not in ("codex", "claude", "command"):
        return False
    try:
        env = _environment(runtime, blocked_env)
        if not Path(runtime.get("workdir", ".")).expanduser().is_dir():
            return False
        _command(runtime, kind, env)
        for item in [method] if method is not None else runtime.get("methods", []):
            _profile(runtime, item, kind, env)
    except (OSError, TypeError, ValueError):
        return False
    return True


def _state_directory(state_dir):
    global _default_state
    if state_dir is None:
        if _default_state is None:
            _default_state = tempfile.TemporaryDirectory(prefix="agent-mesh-runtime-")
        state_dir = _default_state.name
    path = Path(state_dir).expanduser().absolute()
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def _guard_path(state_dir, execution_id):
    if not isinstance(execution_id, dict) or execution_id.get("kind") != "process":
        return None
    token = _uuid(execution_id.get("token"))
    return Path(state_dir) / f"exec-{token}.lock" if token else None


def _guard_state(path):
    try:
        with path.open("rb") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            data = stream.read(4096)
    except FileNotFoundError:
        # The guard is created after the DB identifier is durable and before launching.
        return {"phase": "pending"}
    except OSError:
        return {}
    return (_object(data) or {}) if data else {"phase": "pending"}


def execution_ended(state_dir, execution_id):
    """Return true only when the execution never started or its group was reaped."""
    if execution_id is None:
        return True
    path = _guard_path(state_dir, execution_id)
    if path is None:
        return False
    state = _guard_state(path)
    if state.get("phase") in {"pending", "finished"}:
        return True
    # A service manager may have stopped both the supervisor and the Runtime.
    # A PID alone is insufficient to identify a process to signal. Probe only.
    group = state.get("group")
    return (
        state.get("phase") == "active"
        and type(group) is int
        and group > 0
        and not group_exists(group)
    )


def forget_execution(state_dir, execution_id):
    """Remove completed recovery metadata after the daemon has released its slot."""
    if execution_id is not None and execution_ended(state_dir, execution_id):
        _guard_path(state_dir, execution_id).unlink(missing_ok=True)
        (Path(state_dir) / f"schema-{_uuid(execution_id['token'])}.json").unlink(
            missing_ok=True
        )


def _error(code, message, *, unknown=False):
    return {
        "status": "unknown" if unknown else "failed",
        "error": {"code": code, "message": message},
    }


def _reply(text, question=False):
    answer = (
        {"status": "needs_input", "question": {"text": text}}
        if question
        else {"status": "succeeded", "result": {"text": text}}
    )
    if len(_json(answer).encode()) > MAX_RESULT_BYTES:
        return _error(
            "RESULT_TOO_LARGE", "Runtime output exceeds the Mesh payload limit"
        )
    return answer


async def _notify(callback, value):
    if callback is not None:
        result = callback(value)
        if inspect.isawaitable(result):
            await result


def _input(request):
    if "answer_text" in request:
        return request["answer_text"]
    params = request["params"]
    if set(params) == {"prompt"} and isinstance(params["prompt"], str):
        return params["prompt"]
    return _json(params)


async def _echo(method, request, session_id, callback, state_dir):
    text = _input(request)
    interaction = method.get("interaction", "single_turn")
    if interaction == "single_turn":
        return _reply(text)
    scope = request.get("params", {}).get("scope")
    if not isinstance(scope, str) or not scope.strip():
        scope = None
    if session_id is not None:
        token = _uuid(session_id)
        path = state_dir / f"echo-{token}.json"
        if not token or not path.is_file():
            return _error("SESSION_NOT_FOUND", "Runtime session does not exist")
        context = json.loads(path.read_bytes())
        scope = request.get("answer_text", scope) or context["scope"]
    else:
        token = str(uuid4())
        path = state_dir / f"echo-{token}.json"
        context = {"prompt": text}
    context["scope"] = scope
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=state_dir, delete=False
    ) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(_json(context))
            stream.flush()
            os.fsync(stream.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    if session_id is None:
        await _notify(callback, token)
    if interaction == "clarify" and not scope:
        return _reply("请选择范围：API 或 Web？", question=True)
    return _reply(
        " | ".join(
            dict.fromkeys(part for part in (context["prompt"], scope, text) if part)
        )
    )


def _invocation(kind, runtime, method, request, session_id, env, schema_path):
    argv = _command(runtime, kind, env)
    profile = _profile(runtime, method, kind, env)
    interaction = method.get("interaction", "single_turn")
    text = _input(request)
    if kind == "command":
        job = {
            "method": request.get("method", method.get("name")),
            "session_id": session_id,
        }
        if "answer_text" in request:
            job["answer_text"] = text
        else:
            job["params"] = request["params"]
        return argv, (_json(job) + "\n").encode()
    if interaction == "clarify" and session_id is None:
        text = CLARIFY_INSTRUCTIONS + text
    if kind == "codex":
        cwd = Path(runtime.get("workdir", ".")).expanduser().resolve()
        # Trusted project configuration can register its own MCP tools.
        untrusted = ",".join(
            f'{_json(str(folder))}={{trust_level="untrusted"}}'
            for folder in (cwd, *cwd.parents)
        )
        argv += [
            "exec",
            "--cd",
            str(cwd),
            "--json",
            "--color",
            "never",
            "-c",
            'approval_policy="never"',
            "-c",
            f"projects={{{untrusted}}}",
            "--disable",
            "apps",
            "--disable",
            "plugins",
            "--disable",
            "skill_mcp_dependency_install",
        ]
        if runtime.get("skip_git_repo_check", False):
            argv.append("--skip-git-repo-check")
        if profile:
            argv += ["-c", "default_permissions=" + _json(profile)]
        if interaction == "single_turn":
            argv.append("--ephemeral")
        elif interaction == "clarify":
            argv += ["--output-schema", str(schema_path)]
        if session_id:
            argv += ["resume", session_id]
        argv.append("-")
    else:
        argv += [
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--setting-sources",
            "",
            "--strict-mcp-config",
            "--mcp-config",
            '{"mcpServers":{}}',
            "--permission-mode",
            "dontAsk",
            "--permission-prompts",
            "none",
        ]
        if profile:
            argv += ["--settings", profile]
        if interaction == "single_turn":
            argv.append("--no-session-persistence")
        elif interaction == "clarify":
            argv += ["--json-schema", _json(SCHEMA)]
        if session_id:
            argv += ["--resume", session_id]
    return argv, text.encode()


async def _lines(stream):
    """Yield bounded lines, discarding the remainder of any oversized event."""
    line = bytearray()
    oversized = False
    while chunk := await stream.read(65536):
        parts = chunk.split(b"\n")
        for index, part in enumerate(parts):
            if len(line) + len(part) > MAX_EVENT_BYTES:
                oversized = True
                line.clear()
            if not oversized:
                line.extend(part)
            if index < len(parts) - 1:
                yield None if oversized else bytes(line)
                line.clear()
                oversized = False
    if line or oversized:
        yield None if oversized else bytes(line)


async def _receive(kind, stdout, session_id, callback):
    current_id = session_id
    reply = None
    condition = {}
    plain_text = []
    plain_size = 0
    async for line in _lines(stdout):
        if line is None:
            condition["oversized"] = True
            reply = None
            continue
        event = _object(line) or {}
        event_type = event.get("type")
        if not isinstance(event_type, str):
            event_type = None
        new_id = None
        if kind == "codex":
            if event_type == "thread.started":
                new_id = event.get("thread_id")
            if event_type == "turn.completed":
                condition["complete"] = True
            if event_type == "turn.failed":
                condition["failed"] = True
            if event_type in {"turn.failed", "error"}:
                error = event.get("error")
                diagnostic = (
                    error.get("message")
                    if isinstance(error, dict)
                    else event.get("message")
                )
                condition["missing"] = condition.get("missing") or (
                    isinstance(diagnostic, str)
                    and bool(MISSING_SESSION.search(diagnostic))
                )
            item = event.get("item")
            if (
                event_type == "item.completed"
                and isinstance(item, dict)
                and item.get("type") == "agent_message"
            ):
                reply = item.get("text")
        elif kind == "claude":
            if event_type == "system" and event.get("subtype") == "init":
                new_id = event.get("session_id")
            if event_type == "result":
                new_id = event.get("session_id")
                condition["complete"] = not event.get("is_error", False)
                reply = event.get("structured_output") or event.get("result")
                if isinstance(reply, str):
                    condition["missing"] = (
                        bool(MISSING_SESSION.search(reply))
                        and not condition["complete"]
                    )
                if not condition["complete"] and isinstance(event.get("errors"), list):
                    condition["missing"] = condition.get("missing") or any(
                        isinstance(error, str) and MISSING_SESSION.search(error)
                        for error in event["errors"]
                    )
        elif event_type in {"session.started", "result", "question", "error"}:
            new_id = event.get("session_id")
            if event_type == "error":
                condition["missing"] = event.get("code") == "SESSION_NOT_FOUND"
                condition["handler_error"] = True
            elif event_type != "session.started":
                reply = event
        else:
            plain_size += len(line) + 1
            if plain_size <= MAX_EVENT_BYTES:
                plain_text.append(line)
            else:
                condition["oversized"] = True
        if new_id is not None:
            valid_id = (
                _uuid(new_id)
                if kind in {"codex", "claude"}
                else new_id
                if isinstance(new_id, str) and 0 < len(new_id) <= 256
                else None
            )
            if not valid_id or (current_id is not None and current_id != valid_id):
                condition["invalid_session"] = True
            elif current_id is None:
                current_id = valid_id
                await _notify(callback, valid_id)
    if (
        kind == "command"
        and reply is None
        and plain_text
        and not condition.get("oversized")
    ):
        reply = b"\n".join(plain_text).decode("utf-8", "replace")
    return reply, current_id, condition


async def _tail(stream):
    last = b""
    missing = False
    while chunk := await stream.read(8192):
        text = (last + chunk).decode("utf-8", "replace")
        missing = missing or bool(MISSING_SESSION.search(text))
        last = (last + chunk)[-512:]
    return missing


async def _send(stream, data):
    try:
        stream.write(data)
        await stream.drain()
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        stream.close()


async def _reader(pipe):
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await asyncio.get_running_loop().connect_read_pipe(
        lambda: protocol, pipe
    )
    return reader, transport


async def _execute(guard_path, command, cwd, env, timeout, stdin, receive):
    guard = os.open(guard_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    owner_read, owner_write = os.pipe()
    stdout_read, stdout_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    process = None
    tasks = []
    transports = []
    try:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX)
            worker = Path(__file__).with_name("_runtime_worker.py")
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(worker),
                str(guard),
                str(owner_read),
                str(timeout),
                *command,
                cwd=cwd,
                env=env,
                pass_fds=(guard, owner_read),
                start_new_session=True,
                stdin=asyncio.subprocess.PIPE,
                stdout=stdout_write,
                stderr=stderr_write,
            )
        finally:
            for descriptor in (guard, owner_read, stdout_write, stderr_write):
                os.close(descriptor)
        with (
            os.fdopen(stdout_read, "rb", buffering=0) as stdout_pipe,
            os.fdopen(stderr_read, "rb", buffering=0) as stderr_pipe,
        ):
            stdout_read = stderr_read = None
            stdout, transport = await _reader(stdout_pipe)
            transports.append(transport)
            stderr, transport = await _reader(stderr_pipe)
            transports.append(transport)
            coroutines = [
                _send(process.stdin, stdin),
                receive(stdout),
                _tail(stderr),
                process.wait(),
            ]
            tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
            async with asyncio.timeout(timeout + TERMINATION_GRACE + 2):
                _, output, missing_session, _ = await asyncio.gather(*tasks)
            return output, missing_session
    finally:
        os.close(owner_write)
        for descriptor in (stdout_read, stderr_read):
            if descriptor is not None:
                os.close(descriptor)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for transport in transports:
            transport.close()
        if process is not None and process.returncode is None:
            process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), TERMINATION_GRACE + 1)
            except TimeoutError:
                pass  # The supervisor continues to hold the guard until the group is reaped.


def _terminal(kind, interaction, session_id, output, missing_stderr, guard):
    phase = guard.get("phase")
    reason = guard.get("reason")
    if phase != "finished":
        return _error(
            "OUTCOME_UNKNOWN",
            "Runtime process group could not be confirmed stopped",
            unknown=True,
        )
    if reason == "timeout":
        return _error("EXECUTION_TIMEOUT", "Runtime exceeded its execution limit")
    text, active_session, condition = output
    if session_id and (
        condition.get("missing") or (guard.get("returncode") and missing_stderr)
    ):
        return _error("SESSION_NOT_FOUND", "Runtime session does not exist")
    if (
        reason != "exit"
        or guard.get("returncode") != 0
        or condition.get("failed")
        or condition.get("handler_error")
    ):
        return _error("EXECUTION_ERROR", "Runtime did not finish successfully")
    if condition.get("invalid_session") or (
        interaction != "single_turn" and not active_session
    ):
        return _error(
            "EXECUTION_ERROR", "Runtime did not report the expected session ID"
        )
    if text is None and condition.get("oversized"):
        return _error(
            "RESULT_TOO_LARGE", "Runtime output exceeds the Mesh payload limit"
        )
    if kind != "command" and not condition.get("complete"):
        return _error("EXECUTION_ERROR", "Runtime emitted no successful terminal event")
    structured = (
        text
        if isinstance(text, dict)
        else (
            _object(text)
            if isinstance(text, str) and interaction == "clarify"
            else None
        )
    )
    if (
        structured
        and structured.get("type") in ("question", "result")
        and (kind == "command" or structured.keys() == SCHEMA["properties"].keys())
    ):
        message = structured.get("text")
        if isinstance(message, str) and message.strip():
            return _reply(
                message,
                question=structured["type"] == "question" and interaction == "clarify",
            )
    if isinstance(text, str):
        return _reply(text)
    return _error("EXECUTION_ERROR", "Runtime did not return a result")


async def run(
    runtime,
    method,
    request,
    runtime_session_id=None,
    on_session_id=None,
    *,
    execution_timeout_s=None,
    state_dir=None,
    on_execution_id=None,
    blocked_env=(),
):
    """Execute a validated request. The execution callback must persist before returning."""
    kind = _adapter(runtime)
    interaction = method.get("interaction", "single_turn")
    if request.get("session_id") and not runtime_session_id:
        return _error("SESSION_NOT_FOUND", "Runtime session does not exist")
    if (
        runtime_session_id
        and kind in {"codex", "claude"}
        and not _uuid(runtime_session_id)
    ):
        return _error("SESSION_NOT_FOUND", "Runtime session does not exist")
    timeout = (
        execution_timeout_s
        if execution_timeout_s is not None
        else runtime.get("execution_timeout_s", 300)
    )
    if (
        not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        return _error("EXECUTION_ERROR", "Invalid Runtime execution limit")
    if not ready(runtime, method, blocked_env=blocked_env):
        return _error(
            "EXECUTION_ERROR", "Runtime executable or local settings are unavailable"
        )
    directory = _state_directory(state_dir)
    if kind == "echo":
        return await _echo(
            method, request, runtime_session_id, on_session_id, directory
        )

    execution_id = {"kind": "process", "token": str(uuid4())}
    schema_path = directory / f"schema-{execution_id['token']}.json"
    guard_path = _guard_path(directory, execution_id)
    try:
        env = _environment(runtime, blocked_env)
        command, stdin = _invocation(
            kind, runtime, method, request, runtime_session_id, env, schema_path
        )
        cwd = Path(runtime.get("workdir", ".")).expanduser().absolute()
        await _notify(on_execution_id, execution_id)
        if kind == "codex" and interaction == "clarify":
            with os.fdopen(
                os.open(schema_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
            ) as stream:
                stream.write(_json(SCHEMA))
        callback = on_session_id if interaction != "single_turn" else None
        output, missing = await _execute(
            guard_path,
            command,
            cwd,
            env,
            timeout,
            stdin,
            lambda stdout: _receive(kind, stdout, runtime_session_id, callback),
        )
        return _terminal(
            kind,
            interaction,
            runtime_session_id,
            output,
            missing,
            _guard_state(guard_path),
        )
    except TimeoutError:
        return _error(
            "EXECUTION_TIMEOUT",
            "Runtime exceeded its execution limit",
            unknown=not execution_ended(directory, execution_id),
        )
    except (OSError, ValueError):
        return _error(
            "EXECUTION_ERROR",
            "Runtime could not be started or its output was unreadable",
            unknown=not execution_ended(directory, execution_id),
        )
    finally:
        schema_path.unlink(missing_ok=True)
        if on_execution_id is None:
            forget_execution(directory, execution_id)
