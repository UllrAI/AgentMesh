"""On-demand browser client for the local daemon."""

import asyncio
import secrets
import webbrowser
from pathlib import Path

from aiohttp import ClientError, ClientSession, ClientTimeout, UnixConnector, web

ASSETS = Path(__file__).with_name("web")
CLIENT = web.AppKey("daemon_client", ClientSession)


def create_app(socket: str, token: str) -> web.Application:
    app = web.Application()

    async def connect(app):
        async with ClientSession(
            connector=UnixConnector(path=socket), timeout=ClientTimeout(total=15)
        ) as client:
            app[CLIENT] = client
            yield

    app.cleanup_ctx.append(connect)

    async def asset(req):
        name = req.path.lstrip("/") or "index.html"
        return web.FileResponse(
            ASSETS / name,
            headers={
                "Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'none'",
                "Referrer-Policy": "no-referrer",
            },
        )

    for path in ("/", "/app.js", "/style.css"):
        app.router.add_get(path, asset)

    def error(status, code, message):
        return web.json_response(
            {"error": {"code": code, "message": message}}, status=status
        )

    async def api(req):
        bearer = req.headers.get("Authorization", "").encode()
        if not secrets.compare_digest(bearer, f"Bearer {token}".encode()):
            return error(401, "UNAUTHORIZED", "Open the link printed by agent-mesh ui.")
        if req.content_type != "application/json":
            return error(415, "INVALID_REQUEST", "Expected application/json.")
        try:
            payload = await req.json()
            if not isinstance(payload, dict):
                raise ValueError
        except (ValueError, LookupError):
            return error(400, "INVALID_REQUEST", "Expected a JSON object.")

        action = req.match_info["action"]
        try:
            async with req.app[CLIENT].post(
                f"http://agent-mesh/v1/{action}", json=payload
            ) as response:
                return web.Response(
                    status=response.status,
                    body=await response.read(),
                    content_type="application/json",
                    headers={"Cache-Control": "no-store"},
                )
        except (ClientError, TimeoutError):
            return error(
                503, "DAEMON_UNAVAILABLE", f"Cannot reach the local daemon at {socket}."
            )

    app.router.add_post("/api/{action:list|jobs|job_status|invoke|answer}", api)
    return app


async def serve_ui(socket: str, *, open_browser: bool) -> None:
    token = secrets.token_urlsafe(32)
    runner = web.AppRunner(create_app(socket, token), access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        port = runner.addresses[0][1]
        url = f"http://127.0.0.1:{port}/#token={token}"
        print(url, flush=True)
        if open_browser:
            await asyncio.to_thread(webbrowser.open, url)
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
