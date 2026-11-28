from __future__ import annotations

import argparse
import asyncio
import json
import math
import shlex
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import httpx

from .config import load_config, socket_path

WAIT_SECONDS = 30


class DaemonError(Exception):
    pass


async def request(socket: str, operation: str, **payload: Any) -> dict[str, Any]:
    wait = payload.get("wait_timeout_s") or 0
    read_timeout = max(15, wait + 5) if math.isfinite(wait) else 15
    try:
        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=socket),
            base_url="http://agent-mesh",
            timeout=httpx.Timeout(5, read=read_timeout),
        ) as client:
            response = await client.post(
                f"/v1/{operation}",
                json={
                    key: value for key, value in payload.items() if value is not None
                },
            )
    except httpx.TimeoutException as exc:
        raise DaemonError(f"本地 Daemon 请求超时（{socket}）") from exc
    except httpx.RequestError as exc:
        raise DaemonError(f"无法访问本地 Daemon（{socket}）") from exc

    try:
        result = response.json()
    except ValueError as exc:
        raise DaemonError(
            f"本地 Daemon 返回了无效响应（HTTP {response.status_code}）"
        ) from exc
    if not isinstance(result, dict) or (
        not response.is_success and not result.get("error")
    ):
        raise DaemonError(f"本地 Daemon 返回了无效响应（HTTP {response.status_code}）")
    return result


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("请输入正整数")
    return number


def _wait_timeout(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("请输入有限的非负秒数")
    return number


def _json_object(value: str) -> dict[str, Any]:
    try:
        result = json.loads(value)
        json.dumps(result, allow_nan=False)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"无效的 JSON 对象：{exc}") from exc
    if not isinstance(result, dict):
        raise argparse.ArgumentTypeError("--params 必须是 JSON 对象")
    return result


def _socket_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--socket", default=argparse.SUPPRESS, help="本地 Daemon 的 Unix Socket"
    )
    parser.add_argument("--config", default=argparse.SUPPRESS, help="配置文件路径")


def _wait_options(parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--wait", action="store_true", dest="wait", help="等待本轮结果或问题"
    )
    group.add_argument(
        "--async", action="store_false", dest="wait", help="提交后返回当前状态"
    )
    parser.set_defaults(wait=None)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent-mesh", description="调用私有 Agent Mesh 中的 Agent"
    )
    _socket_options(parser)
    commands = parser.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="启动本地 Daemon")
    ui = commands.add_parser("ui", help="查看节点、公开能力和任务")
    ui.add_argument("--no-open", action="store_true", help="仅打印访问链接")
    mcp = commands.add_parser("mcp", help="通过标准输入输出启动 MCP 桥")
    discover = commands.add_parser("list", help="列出 Agent、在线状态和能力")
    discover.add_argument("--method", help="仅列出公开此方法的 Agent")
    inspect = commands.add_parser("inspect", help="查看 Agent Card")
    inspect.add_argument("agent_id")

    call = commands.add_parser("call", help="提交任务或继续会话")
    call.add_argument("target")
    call.add_argument("method")
    inputs = call.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--prompt", help="作为 prompt 参数传给目标")
    inputs.add_argument("--params", type=_json_object, help="传给目标的 JSON 参数对象")
    call.add_argument("--session", dest="session_id", help="继续指定会话")
    call.add_argument(
        "--execution-timeout-s", type=_positive_int, help="申请的目标执行时限（秒）"
    )
    _wait_options(call)

    answer = commands.add_parser("answer", help="答复指定轮次的问题")
    answer.add_argument("job_id")
    answer.add_argument("--text", required=True, help="答复文本")
    _wait_options(answer)

    jobs = commands.add_parser("jobs", help="查看最近提交的任务")
    jobs.add_argument(
        "--limit", type=_positive_int, default=20, help="最多显示的任务数（默认 20）"
    )
    job = commands.add_parser("job", help="查看指定轮次的状态")
    job.add_argument("job_id")
    job.add_argument("--wait-timeout-s", type=_wait_timeout, help="本地等待时限（秒）")
    for command in (serve, ui, mcp, discover, inspect, call, answer, jobs, job):
        _socket_options(command)
    return parser


def _failed(result: dict[str, Any]) -> bool:
    if result.get("status") in {"needs_input", "succeeded"}:
        return False
    return bool(
        result.get("error")
        or result.get("invoke_error")
        or result.get("query_error")
        or result.get("status") in {"failed", "unknown"}
        or result.get("type") in {"invoke_error", "query_error"}
    )


async def _run_command(
    args: argparse.Namespace, socket: str, interactive: bool
) -> dict[str, Any]:
    if args.command == "list":
        return await request(socket, "list", method=args.method)
    if args.command == "inspect":
        return await request(socket, "inspect", agent_id=args.agent_id)
    if args.command == "jobs":
        return await request(socket, "jobs", limit=args.limit)
    if args.command == "job":
        return await request(
            socket, "job_status", job_id=args.job_id, wait_timeout_s=args.wait_timeout_s
        )

    if args.command == "call":
        params = {"prompt": args.prompt} if args.prompt is not None else args.params
        result = await request(
            socket,
            "invoke",
            target=args.target,
            method=args.method,
            params=params,
            session_id=args.session_id,
            execution_timeout_s=args.execution_timeout_s,
        )
    else:
        result = await request(socket, "answer", job_id=args.job_id, text=args.text)

    wait = interactive if args.wait is None else args.wait
    if not (
        wait
        and result.get("job_id")
        and result.get("status") in {"accepted", "unconfirmed"}
        and not _failed(result)
    ):
        return result
    try:
        status = await request(
            socket, "job_status", job_id=result["job_id"], wait_timeout_s=WAIT_SECONDS
        )
    except DaemonError as exc:
        return {
            **result,
            "query_error": {"code": "DAEMON_QUERY_FAILED", "message": str(exc)},
        }
    if not status.get("job_id") and status.get("error"):
        return {**result, "query_error": status["error"]}
    return status


def _print_result(result: dict[str, Any], socket: str, interactive: bool) -> None:
    print(json.dumps(result, ensure_ascii=False, indent=2 if interactive else None))
    if not interactive or result.get("status") != "needs_input":
        return
    question = result.get("question")
    if not question:
        return
    print(f"问题：{question['text']}")
    expires_at = question["expires_at"]
    answer = result.get("answer") or {}
    answer_status = answer.get("status", "waiting")
    if answer_status in {"answered", "pending"}:
        label = "已答复" if answer_status == "answered" else "答复待确认"
        ids = answer.get("pending_job_ids") or [answer["job_id"]]
        print(f"{label}：{', '.join(ids)}")
        return
    try:
        deadline = datetime.fromtimestamp(expires_at, UTC).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except (OverflowError, OSError, ValueError):
        deadline = f"Unix 时间 {expires_at} 秒"
    expired = expires_at <= time.time() or answer_status == "expired"
    print(f"答复截止：{deadline}" + ("（已过期）" if expired else ""))
    if not expired:
        command = shlex.join(
            [
                "agent-mesh",
                "answer",
                result["job_id"],
                "--text",
                "填写答复",
                "--socket",
                socket,
            ]
        )
        print(command)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    config_file = getattr(args, "config", None)
    socket_arg = getattr(args, "socket", None)
    if args.command == "serve" and not config_file:
        parser.error("serve 需要 --config")

    try:
        socket = socket_path(socket_arg, config_file)
        if args.command == "ui":
            from .ui import serve_ui

            asyncio.run(serve_ui(socket, open_browser=not args.no_open))
            return 0
        if args.command == "serve":
            from .daemon import serve

            config = load_config(config_file)
            asyncio.run(serve({**config, "socket": socket}))
            return 0
        if args.command == "mcp":
            from .mcp import create_server

            create_server(socket).run(transport="stdio")
            return 0

        terminal_output = sys.stdout.isatty()
        interactive = terminal_output and sys.stdin.isatty()
        result = asyncio.run(_run_command(args, socket, interactive))
        _print_result(result, socket, terminal_output)
        return 1 if _failed(result) else 0
    except KeyboardInterrupt:
        return 130
    except (DaemonError, OSError, ValueError) as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
