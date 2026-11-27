import asyncio
import io
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from aiohttp import web
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from agent_mesh import cli


@pytest.fixture
async def daemon():
    calls = []
    responses = {}

    async def handle(request):
        operation = request.match_info["operation"]
        payload = await request.json()
        calls.append((operation, payload))
        status, result = responses[operation]
        return web.json_response(result, status=status)

    with tempfile.TemporaryDirectory(
        prefix="agent-mesh-test-", dir="/tmp"
    ) as directory:
        socket = str(Path(directory) / "mesh.sock")
        app = web.Application()
        app.router.add_post("/v1/{operation}", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            await web.UnixSite(runner, socket).start()
            yield socket, calls, responses
        finally:
            await runner.cleanup()


async def run_cli(*arguments):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "agent_mesh.cli",
        *arguments,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


async def test_cli_async_by_default_and_waits_on_explicit_request(daemon, monkeypatch):
    socket, calls, responses = daemon
    monkeypatch.setenv("AGENT_MESH_SOCKET", "/tmp/unused-agent-mesh.sock")
    accepted = {"job_id": "new-job", "status": "accepted"}
    responses["invoke"] = (200, accepted)
    responses["job_status"] = (
        200,
        {**accepted, "status": "succeeded", "result": {"text": "分析完成"}},
    )

    code, out, err = await run_cli(
        "--socket", socket, "call", "remote", "inspect", "--prompt", "分析项目"
    )
    assert (code, json.loads(out), err) == (0, accepted, "")
    assert calls == [
        (
            "invoke",
            {"target": "remote", "method": "inspect", "params": {"prompt": "分析项目"}},
        )
    ]

    code, out, err = await run_cli(
        "call",
        "remote",
        "inspect",
        "--params",
        '{"scope":"api"}',
        "--session",
        "old-session",
        "--execution-timeout-s",
        "60",
        "--wait",
        "--socket",
        socket,
    )
    assert (code, json.loads(out)["result"], err) == (0, {"text": "分析完成"}, "")
    assert calls[1:] == [
        (
            "invoke",
            {
                "target": "remote",
                "method": "inspect",
                "params": {"scope": "api"},
                "session_id": "old-session",
                "execution_timeout_s": 60,
            },
        ),
        ("job_status", {"job_id": "new-job", "wait_timeout_s": 30}),
    ]


async def test_answer_wait_uses_new_job_and_preserves_id_on_query_error(daemon):
    socket, calls, responses = daemon
    accepted = {
        "job_id": "answer-job",
        "status": "accepted",
        "answer_to": "question-job",
    }
    error = {"code": "JOB_NOT_FOUND", "message": "未找到任务"}
    responses["answer"] = (200, accepted)
    responses["job_status"] = (404, {"error": error})

    code, out, err = await run_cli(
        "answer", "question-job", "--text", "仅分析 API", "--wait", "--socket", socket
    )
    assert (code, json.loads(out), err) == (1, {**accepted, "query_error": error}, "")
    assert calls == [
        ("answer", {"job_id": "question-job", "text": "仅分析 API"}),
        ("job_status", {"job_id": "answer-job", "wait_timeout_s": 30}),
    ]


def test_interactive_wait_and_question_display(monkeypatch):
    calls = []

    class Terminal(io.StringIO):
        def isatty(self):
            return True

    async def request(socket, operation, **payload):
        calls.append((operation, payload))
        result = {"job_id": "job-1", "status": "accepted"}
        if operation == "invoke":
            return result
        return {
            **result,
            "status": "needs_input",
            "question": {"text": "要分析 API 还是 Web？", "expires_at": 4_102_444_800},
        }

    terminal = Terminal()
    monkeypatch.setattr(cli.sys, "stdin", Terminal())
    monkeypatch.setattr(cli.sys, "stdout", terminal)
    monkeypatch.setattr(cli, "request", request)
    args = [
        "call",
        "remote",
        "inspect",
        "--prompt",
        "分析项目",
        "--socket",
        "/tmp/socket with spaces",
    ]

    assert cli.main(args) == 0
    assert calls[-1] == ("job_status", {"job_id": "job-1", "wait_timeout_s": 30})
    assert "问题：要分析 API 还是 Web？" in terminal.getvalue()
    assert "答复截止：2100-01-01 00:00:00 UTC" in terminal.getvalue()
    assert (
        "agent-mesh answer job-1 --text '填写答复' --socket '/tmp/socket with spaces'"
        in terminal.getvalue()
    )

    calls.clear()
    assert cli.main([*args, "--async"]) == 0
    assert [operation for operation, _ in calls] == ["invoke"]


def test_cli_rejects_non_object_params():
    with pytest.raises(SystemExit, match="2"):
        cli.main(["call", "remote", "inspect", "--params", "[]"])


def test_cli_socket_precedence(monkeypatch, tmp_path):
    sockets = []

    async def request(socket, operation, **payload):
        sockets.append(socket)
        return {"agents": []}

    monkeypatch.setattr(cli, "request", request)
    config = tmp_path / "mesh.yaml"
    config.write_text("socket: /tmp/config.sock\n")
    monkeypatch.setenv("AGENT_MESH_SOCKET", "/tmp/env.sock")

    assert (
        cli.main(["list", "--config", str(config), "--socket", "/tmp/flag.sock"]) == 0
    )
    assert cli.main(["--config", str(config), "list"]) == 0
    monkeypatch.delenv("AGENT_MESH_SOCKET")
    assert cli.main(["list", "--config", str(config)]) == 0
    assert cli.main(["list"]) == 0
    assert sockets == [
        "/tmp/flag.sock",
        "/tmp/env.sock",
        "/tmp/config.sock",
        str(Path.home() / ".agent-mesh" / "agent.sock"),
    ]


async def test_stdio_mcp_forwards_structured_tools_over_socket(daemon):
    socket, calls, responses = daemon
    accepted = {"job_id": "question-job", "status": "accepted"}
    question = {
        **accepted,
        "status": "needs_input",
        "question": {"text": "选择范围？", "expires_at": 4_102_444_800},
    }
    responses.update(
        {
            "list": (200, {"agents": []}),
            "inspect": (200, {"id": "remote", "capabilities": []}),
            "jobs": (200, {"jobs": [accepted]}),
            "invoke": (200, accepted),
            "job_status": (200, question),
            "answer": (
                200,
                {
                    "job_id": "answer-job",
                    "answer_to": "question-job",
                    "status": "accepted",
                },
            ),
        }
    )
    server = StdioServerParameters(
        command=sys.executable,
        args=["-m", "agent_mesh.cli", "mcp", "--socket", socket],
        env=os.environ.copy(),
    )

    async with (
        stdio_client(server) as (read, write),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        tools = {tool.name: tool for tool in (await session.list_tools()).tools}
        assert set(tools) == {
            "mesh_list",
            "mesh_inspect",
            "mesh_jobs",
            "mesh_invoke",
            "mesh_job_status",
            "mesh_answer",
        }
        assert "wait_timeout_s" in tools["mesh_job_status"].description
        assert tools["mesh_job_status"].annotations.readOnlyHint is True

        cases = [
            ("list", {"method": "inspect"}, {"method": "inspect"}),
            ("inspect", {"agent_id": "remote"}, {"agent_id": "remote"}),
            ("jobs", {}, {"limit": 20}),
            (
                "invoke",
                {"target": "remote", "method": "inspect", "params": {"prompt": "分析"}},
                None,
            ),
            (
                "job_status",
                {"job_id": "question-job"},
                {"job_id": "question-job", "wait_timeout_s": 0},
            ),
            ("job_status", {"job_id": "question-job", "wait_timeout_s": 45}, None),
            ("answer", {"job_id": "question-job", "text": "仅 API"}, None),
        ]
        for operation, arguments, expected_payload in cases:
            result = await session.call_tool(f"mesh_{operation}", arguments)
            assert not result.isError
            assert result.structuredContent == responses[operation][1]
            assert calls[-1] == (
                operation,
                arguments if expected_payload is None else expected_payload,
            )
