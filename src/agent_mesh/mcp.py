from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from .cli import request


def create_server(socket: str) -> FastMCP:
    server = FastMCP("agent-mesh")
    read_only = ToolAnnotations(readOnlyHint=True)

    @server.tool(annotations=read_only)
    async def mesh_list(method: str | None = None) -> dict[str, Any]:
        """列出 Agent、在线状态和能力。可按方法筛选。"""
        return await request(socket, "list", method=method)

    @server.tool(annotations=read_only)
    async def mesh_inspect(agent_id: str) -> dict[str, Any]:
        """查看指定 Agent 的 Card 和方法输入约定。"""
        return await request(socket, "inspect", agent_id=agent_id)

    @server.tool()
    async def mesh_invoke(
        target: str,
        method: str,
        params: dict[str, Any],
        session_id: str | None = None,
        execution_timeout_s: int | None = None,
    ) -> dict[str, Any]:
        """异步提交新任务或继续指定会话。按目标方法的输入约定填写 params。

        用户需要结果或澄清时，用本次返回的 job_id 调用 mesh_job_status。
        """
        return await request(
            socket,
            "invoke",
            target=target,
            method=method,
            params=params,
            session_id=session_id,
            execution_timeout_s=execution_timeout_s,
        )

    @server.tool()
    async def mesh_answer(job_id: str, text: str) -> dict[str, Any]:
        """用提问轮的 job_id 和答复文本异步答复。用返回的新 job_id 查询答复轮进展。"""
        return await request(socket, "answer", job_id=job_id, text=text)

    @server.tool(annotations=read_only)
    async def mesh_jobs(limit: int = 20) -> dict[str, Any]:
        """按提交时间倒序列出本地出站任务。适用于找回先前提交的任务。"""
        return await request(socket, "jobs", limit=limit)

    @server.tool(annotations=read_only)
    async def mesh_job_status(job_id: str, wait_timeout_s: float = 0) -> dict[str, Any]:
        """查询指定轮及其澄清答复进展。

        用户在等结果或问题时，用刚提交返回的 job_id，并设置短于宿主工具时限的
        wait_timeout_s（例如宿主允许 60 秒时等 50 秒）。省略或传 0 则不等待执行结束。
        若返回 needs_input，先依据已有上下文作答，仍需用户决定时把该任务的问题转交给用户。
        """
        return await request(
            socket, "job_status", job_id=job_id, wait_timeout_s=wait_timeout_s
        )

    return server
