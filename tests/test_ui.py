import tempfile
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from agent_mesh.ui import create_app

TOKEN = "local-ui-test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
QUESTION_JOB = "550e8400-e29b-41d4-a716-446655440000"
ANSWER_JOB = "99d317c5-4917-46f9-9e9e-1eeb7905c3db"
SESSION = "91c9d994-9492-4896-8ede-a741ca24bc6a"


@pytest.fixture
async def daemon():
    calls = []
    responses = {}

    async def handle(request):
        operation = request.match_info["operation"]
        payload = await request.json()
        calls.append((operation, payload, request.headers.get("Authorization")))
        status, data = responses[operation]
        return web.json_response(data, status=status)

    with tempfile.TemporaryDirectory(prefix="mesh-ui-test-", dir="/tmp") as directory:
        socket = str(Path(directory) / "daemon.sock")
        app = web.Application()
        app.router.add_post("/v1/{operation}", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            await web.UnixSite(runner, socket).start()
            yield socket, calls, responses
        finally:
            await runner.cleanup()


@pytest.fixture
async def ui(daemon):
    socket, _, _ = daemon
    async with TestClient(TestServer(create_app(socket, TOKEN))) as client:
        yield client


@pytest.mark.parametrize(
    ("path", "content_types"),
    [
        ("/", {"text/html"}),
        ("/app.js", {"text/javascript", "application/javascript"}),
        ("/style.css", {"text/css"}),
    ],
)
async def test_ui_serves_assets_without_contacting_daemon(
    ui, daemon, path, content_types
):
    _, calls, _ = daemon
    async with ui.get(path) as response:
        assert response.status == 200
        assert response.content_type in content_types
        assert (await response.text()).strip()
    assert calls == []


@pytest.mark.parametrize(
    ("operation", "payload", "result"),
    [
        ("list", {"method": "codex.analyze"}, {"agents": [{"id": "peer"}]}),
        (
            "jobs",
            {"limit": 3},
            {"jobs": [{"job_id": QUESTION_JOB, "status": "accepted"}]},
        ),
        (
            "job_status",
            {"job_id": QUESTION_JOB, "wait_timeout_s": 0},
            {
                "job_id": QUESTION_JOB,
                "status": "needs_input",
                "session_id": SESSION,
                "question": {"text": "分析 API 还是 Web？", "expires_at": 4102444800},
            },
        ),
        (
            "answer",
            {"job_id": QUESTION_JOB, "text": "仅分析 API"},
            {"job_id": ANSWER_JOB, "status": "accepted", "answer_to": QUESTION_JOB},
        ),
        (
            "invoke",
            {
                "target": "peer",
                "method": "codex.analyze",
                "params": {"prompt": "分析边界"},
            },
            {"job_id": QUESTION_JOB, "status": "accepted"},
        ),
    ],
)
async def test_ui_proxies_operations_over_unix_socket(
    ui, daemon, operation, payload, result
):
    _, calls, responses = daemon
    responses[operation] = (200, result)
    async with ui.post(f"/api/{operation}", json=payload, headers=AUTH) as response:
        assert response.status == 200
        assert await response.json() == result
    assert calls == [(operation, payload, None)]


@pytest.mark.parametrize(
    ("upstream_status", "payload"),
    [
        (
            200,
            {
                "job_id": QUESTION_JOB,
                "status": "failed",
                "error": {
                    "code": "EXECUTION_ERROR",
                    "message": "Remote execution failed",
                },
            },
        ),
        (404, {"error": {"code": "JOB_NOT_FOUND", "message": "No local record"}}),
    ],
)
async def test_ui_distinguishes_failed_job_from_daemon_api_error(
    ui, daemon, upstream_status, payload
):
    _, calls, responses = daemon
    responses["job_status"] = (upstream_status, payload)
    query = {"job_id": QUESTION_JOB}
    async with ui.post("/api/job_status", json=query, headers=AUTH) as response:
        assert response.status == upstream_status
        assert await response.json() == payload
    assert calls == [("job_status", query, None)]


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong-token"}])
async def test_ui_requires_token_before_proxying(ui, daemon, headers):
    _, calls, _ = daemon
    async with ui.post("/api/jobs", json={}, headers=headers) as response:
        assert response.status == 401
        assert (await response.json())["error"]["code"] == "UNAUTHORIZED"
    assert calls == []


@pytest.mark.parametrize("body", ["{broken", "[]", "null"])
async def test_ui_rejects_broken_json_and_non_objects(ui, daemon, body):
    _, calls, _ = daemon
    headers = {**AUTH, "Content-Type": "application/json"}
    async with ui.post("/api/invoke", data=body, headers=headers) as response:
        assert response.status == 400
        assert (await response.json())["error"]["code"] == "INVALID_REQUEST"
    assert calls == []


async def test_ui_rejects_json_sent_with_the_wrong_content_type(ui, daemon):
    _, calls, _ = daemon
    async with ui.post("/api/jobs", data="{}", headers=AUTH) as response:
        assert response.status == 415
        assert (await response.json())["error"]["code"] == "INVALID_REQUEST"
    assert calls == []


async def test_ui_does_not_proxy_non_allowlisted_operations(ui, daemon):
    _, calls, _ = daemon
    async with ui.post(
        "/api/inspect", json={"agent_id": "peer"}, headers=AUTH
    ) as response:
        assert response.status == 404
    assert calls == []


async def test_ui_reports_daemon_connection_failure():
    with tempfile.TemporaryDirectory(prefix="mesh-ui-test-", dir="/tmp") as directory:
        socket = str(Path(directory) / "missing.sock")
        async with TestClient(TestServer(create_app(socket, TOKEN))) as client:
            async with client.post("/api/jobs", json={}, headers=AUTH) as response:
                assert response.status == 503
                assert (await response.json())["error"]["code"] == "DAEMON_UNAVAILABLE"
