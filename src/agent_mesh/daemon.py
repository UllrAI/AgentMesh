"""Local HTTP API, MQTT request dispatch and execution lifecycle."""

import asyncio
import contextlib
import fcntl
import logging
import math
import os
import signal
import sqlite3
import stat
import time
import uuid
from pathlib import Path

import aiomqtt
from aiohttp import web

from . import runtime
from .config import AGENT_ID
from .store import Store
from .transport import MAX_PAYLOAD, Transport, encode

LOG = logging.getLogger(__name__)
LINK_ERRORS = (aiomqtt.MqttError, OSError)
TERMINAL = {"failed", "unknown", "succeeded", "needs_input"}
RECEIPT_TIMEOUT_S = 10
QUERY_REPLY_TIMEOUT_S = 1
STORAGE_RETRY_S = 1


class LocalError(Exception):
    def __init__(self, code, message, http=400):
        super().__init__(message)
        self.code = code
        self.http = http


class Mesh:
    def __init__(self, config):
        self.config = config
        self.settings = config["runtime"]
        self.methods = {
            m["name"]: {
                **m,
                "execution_timeout_s": self.settings["execution_timeout_s"],
            }
            for m in self.settings["methods"]
        }
        store_path = Path(config["job_store"]).resolve()
        self.state_dir = Path(str(store_path) + ".runs")
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.event = asyncio.Event()
        self.link = Transport(config, self.packet, self.changed)
        self.work = set()
        self.query_watchers = {}
        self.store = Store(store_path)
        self.recovery = None
        self.pending_writes = 0
        try:
            self.reconcile()
        except (OSError, sqlite3.Error) as exc:
            LOG.warning("storage recovery pending (%s)", type(exc).__name__)

    def changed(self):
        self.event.set()
        self.event = asyncio.Event()

    def start(self, coro):
        task = asyncio.create_task(coro)
        self.work.add(task)
        task.add_done_callback(self.work.discard)
        task.add_done_callback(self.report_exception)
        return task

    @staticmethod
    def report_exception(task):
        if not task.cancelled() and task.exception():
            LOG.error(
                "background operation failed (%s)", type(task.exception()).__name__
            )

    def reconcile(self):
        if self.recovery is None:
            self.recovery = self.store.recover_inbound()
        for job in self.recovery[:]:
            execution = job.get("execution_id")
            if not execution or runtime.execution_ended(self.state_dir, execution):
                self.store.release_execution(job["caller_id"], job["job_id"])
                self.recovery.remove(job)
                if execution:
                    self.forget_execution(execution)

    def forget_execution(self, execution):
        try:
            runtime.forget_execution(self.state_dir, execution)
        except OSError as exc:
            LOG.warning("execution metadata cleanup failed (%s)", type(exc).__name__)

    async def maintenance(self):
        while True:
            try:
                self.reconcile()
                self.store.prune()
            except (OSError, sqlite3.Error) as exc:
                LOG.warning("storage maintenance unavailable (%s)", type(exc).__name__)
            await asyncio.sleep(STORAGE_RETRY_S if self.recovery != [] else 30)

    async def reply(self, peer, data):
        try:
            await self.link.publish(
                f"agents/{peer}/replies/{self.config['agent_id']}",
                encode(data),
                expiry=60,
            )
        except LINK_ERRORS:
            pass  # The committed snapshot remains queryable.

    async def packet(self, action, peer, payload, data):
        if data.get("spec_version") != "1.3":
            return
        if action == "replies":
            try:
                recorded = self.store.record_outbound_reply(peer, data)
            except sqlite3.Error as exc:
                LOG.warning("could not persist job reply (%s)", type(exc).__name__)
                return
            if recorded:
                jid = str(uuid.UUID(data["job_id"]))
                for event in self.query_watchers.get(jid, ()):
                    event.set()
                self.changed()
            return
        if action != "invoke":
            return
        method = (
            self.methods.get(data.get("method"))
            if isinstance(data.get("method"), str)
            else None
        )
        ready = (
            self.recovery is not None
            and not self.pending_writes
            and (
                method is None
                or runtime.ready(
                    self.settings,
                    method,
                    blocked_env=(self.config["mqtt"]["password_env"],),
                )
            )
        )
        try:
            entry = self.store.accept_inbound(
                peer,
                payload,
                methods=self.methods,
                max_slots=self.settings["max_concurrent_jobs"],
                ready=ready,
            )
        except sqlite3.Error:
            jid = data.get("job_id")
            try:
                valid_id = isinstance(jid, str) and uuid.UUID(jid).version == 4
            except ValueError:
                valid_id = False
            if data.get("type") == "invoke" and valid_id:
                await self.reply(
                    peer,
                    {
                        "spec_version": "1.3",
                        "type": "invoke_error",
                        "job_id": jid,
                        "error": {
                            "code": "NOT_READY",
                            "message": "task store unavailable",
                        },
                    },
                )
            return
        if entry:
            if entry["execute"]:
                self.start(self.execute(peer, entry))
            await self.reply(peer, entry["reply"])

    async def execute(self, peer, entry):
        request = entry["request"]
        jid = request["job_id"]
        execution = None

        def record_execution(value):
            nonlocal execution
            self.store.record_execution(peer, jid, execution_id=value)
            execution = value

        def record_session(value):
            self.store.record_execution(peer, jid, runtime_session_id=value)

        try:
            if request.get("session_id") and not entry["runtime_session_id"]:
                result = {
                    "status": "failed",
                    "error": {
                        "code": "SESSION_NOT_FOUND",
                        "message": "runtime session unavailable",
                    },
                }
            else:
                result = await runtime.run(
                    self.settings,
                    self.methods[request["method"]],
                    request,
                    entry["runtime_session_id"],
                    record_session,
                    execution_timeout_s=entry["reply"]["execution_timeout_s"],
                    state_dir=self.state_dir,
                    on_execution_id=record_execution,
                    blocked_env=(self.config["mqtt"]["password_env"],),
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.error(
                "runtime outcome unavailable for %s (%s)", jid, type(exc).__name__
            )
            result = {
                "status": "unknown",
                "error": {
                    "code": "OUTCOME_UNKNOWN",
                    "message": "runtime outcome unavailable",
                },
            }
        ended = not execution or runtime.execution_ended(self.state_dir, execution)
        self.pending_writes += 1
        try:
            while True:
                try:
                    try:
                        fields = {
                            k: result[k]
                            for k in ("result", "question", "error")
                            if k in result
                        }
                        reply = self.store.finish_inbound(
                            peer, jid, result["status"], **fields, release_slot=ended
                        )
                    except (KeyError, TypeError, ValueError, RecursionError):
                        reply = self.store.finish_inbound(
                            peer,
                            jid,
                            "failed" if ended else "unknown",
                            error={
                                "code": "EXECUTION_ERROR"
                                if ended
                                else "OUTCOME_UNKNOWN",
                                "message": "runtime produced an invalid outcome",
                            },
                            release_slot=ended,
                        )
                    break
                except sqlite3.Error:
                    await asyncio.sleep(STORAGE_RETRY_S)
        finally:
            self.pending_writes -= 1
        if execution and ended:
            self.forget_execution(execution)
        elif not ended:
            self.recovery.append(
                {"caller_id": peer, "job_id": jid, "execution_id": execution}
            )
        await self.reply(peer, reply)

    def view(self, jid):
        row = self.store.get_outbound(jid)
        if row is None:
            raise LocalError("JOB_NOT_FOUND", "no local record of this job", 404)
        return {**row, "status": "unconfirmed", **(row.get("snapshot") or {})}

    async def send_request(self, target, payload, ttl):
        await self.link.publish(
            f"agents/{target}/invoke/{self.config['agent_id']}", payload, expiry=ttl
        )

    async def retry_once(self, jid, connection):
        await asyncio.sleep(RECEIPT_TIMEOUT_S)
        if (
            self.link.client is not connection
            or self.link.client is None
            or self.view(jid)["invoke_error"]
        ):
            return
        item = self.store.retry_outbound(jid)
        if item:
            try:
                await self.send_request(*item)
            except LINK_ERRORS:
                pass

    async def submit(self, args, *, answer=False):
        if self.link.client is None:
            raise LocalError("NOT_READY", "broker disconnected", 503)
        request = {
            "spec_version": "1.3",
            "type": "invoke",
            "job_id": str(uuid.uuid4()),
            "expires_at": int(time.time()) + 30,
        }
        if answer:
            prior = self.view(args.get("job_id"))
            if prior["status"] != "needs_input":
                raise LocalError("INPUT_NOT_PENDING", "expected a question job")
            answer_state = prior["answer"]
            if answer_state["status"] == "pending":
                raise LocalError(
                    "INPUT_NOT_PENDING",
                    f"query pending answer {answer_state['job_id']} before submitting another",
                )
            if answer_state["status"] != "waiting":
                raise LocalError(
                    "INPUT_NOT_PENDING", "this question is no longer pending"
                )
            target = prior["target_id"]
            request.update(
                method=prior["method"],
                session_id=prior["session_id"],
                answer_to=prior["job_id"],
                answer_text=args.get("text"),
            )
            if (
                not isinstance(request["answer_text"], str)
                or not request["answer_text"].strip()
            ):
                raise LocalError("INVALID_REQUEST", "answer text is required")
        else:
            target = args.get("target")
            if not isinstance(target, str) or not AGENT_ID.fullmatch(target):
                raise LocalError("INVALID_REQUEST", "invalid target agent ID")
            if (
                not isinstance(args.get("method"), str)
                or not args["method"]
                or not isinstance(args.get("params"), dict)
            ):
                raise LocalError(
                    "INVALID_REQUEST", "method and params object are required"
                )
            request.update(method=args["method"], params=args["params"])
            for field in ("session_id", "execution_timeout_s"):
                if args.get(field) is not None:
                    request[field] = args[field]
        payload = encode(request)
        if len(payload) > MAX_PAYLOAD:
            raise LocalError("INVALID_REQUEST", "request exceeds 64 KiB")
        self.store.save_outbound(target, payload)
        jid = request["job_id"]
        connection = self.link.client
        try:
            await self.send_request(target, payload, 30)
        except LINK_ERRORS:
            self.store.record_outbound_error(
                jid,
                "invoke",
                {"code": "OUTCOME_UNKNOWN", "message": "publish unconfirmed"},
            )
        else:
            self.start(self.retry_once(jid, connection))
        return self.view(jid)

    @staticmethod
    def settled(row):
        error = row.get("invoke_error")
        return row["status"] in TERMINAL or (
            row["status"] == "unconfirmed"
            and error
            and error["code"] != "OUTCOME_UNKNOWN"
        )

    async def query(self, target, jid):
        try:
            await self.send_request(
                target,
                encode({"spec_version": "1.3", "type": "query", "job_id": jid}),
                30,
            )
        except LINK_ERRORS:
            pass

    async def status(self, args):
        wait = args.get("wait_timeout_s", 0)
        if type(wait) not in (int, float) or not math.isfinite(wait) or wait < 0:
            raise LocalError(
                "INVALID_REQUEST", "wait_timeout_s must be finite and nonnegative"
            )
        row = self.view(args.get("job_id"))
        jid = row["job_id"]
        query_id = jid if wait else row.get("query_job_id")
        if query_id is None or self.link.client is None:
            return row
        if self.settled(row if query_id == jid else self.view(query_id)):
            return row

        received = asyncio.Event()
        watchers = self.query_watchers.setdefault(query_id, set())
        watchers.add(received)
        probe = asyncio.create_task(self.query(row["target_id"], query_id))
        try:
            async with asyncio.timeout(wait or QUERY_REPLY_TIMEOUT_S):
                while True:
                    changed = self.event
                    row = self.view(jid)
                    if self.link.client is None or (
                        self.settled(row) if wait else received.is_set()
                    ):
                        return row
                    await changed.wait()
        except TimeoutError:
            return self.view(jid)
        finally:
            watchers.remove(received)
            if not watchers:
                del self.query_watchers[query_id]
            probe.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await probe

    async def api(self, req):
        try:
            args = await req.json()
            if not isinstance(args, dict):
                raise ValueError("expected a JSON object")
            action = req.match_info["action"]
            if action == "list":
                result = {
                    "local_id": self.config["agent_id"],
                    "broker_connected": self.link.client is not None,
                    "agents": self.link.list_agents(args.get("method")),
                }
            elif action == "inspect":
                result = self.link.cards.get(args.get("agent_id"))
                if result is None:
                    raise LocalError("NOT_FOUND", "agent card unavailable", 404)
            elif action in ("invoke", "answer"):
                result = await self.submit(args, answer=action == "answer")
            elif action == "job_status":
                result = await self.status(args)
            elif action == "jobs":
                result = {"jobs": self.store.list_outbound(args.get("limit", 20))}
            else:
                raise LocalError("NOT_FOUND", "unknown local command", 404)
            return web.json_response(result)
        except (TypeError, ValueError, OverflowError) as exc:
            return web.json_response(
                {"error": {"code": "INVALID_REQUEST", "message": str(exc)}}, status=400
            )
        except LocalError as exc:
            return web.json_response(
                {"error": {"code": exc.code, "message": str(exc)}}, status=exc.http
            )
        except sqlite3.Error:
            return web.json_response(
                {"error": {"code": "NOT_READY", "message": "task store unavailable"}},
                status=503,
            )


@contextlib.contextmanager
def instance_lock(path):
    path = path.resolve()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with open(str(path) + ".lock", "a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(f"already in use: {path}") from exc
        yield


async def remove_stale_socket(path):
    try:
        existing = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(existing.st_mode):
        raise ValueError(f"socket path occupied by a non-socket: {path}")
    try:
        async with asyncio.timeout(QUERY_REPLY_TIMEOUT_S):
            _, writer = await asyncio.open_unix_connection(path)
    except ConnectionRefusedError:
        current = path.lstat()
        if (current.st_dev, current.st_ino) == (existing.st_dev, existing.st_ino):
            path.unlink()
            return
    except FileNotFoundError:
        return
    except TimeoutError:
        pass
    else:
        writer.close()
    raise ValueError(f"socket already in use: {path}")


async def serve(config):
    os.umask(0o077)
    path = Path(config["socket"])
    with instance_lock(path), instance_lock(Path(config["job_store"])):
        await remove_stale_socket(path)
        mesh = Mesh(config)
        app = web.Application(client_max_size=2 * MAX_PAYLOAD)
        app.router.add_post("/v1/{action}", mesh.api)
        runner = web.AppRunner(app, access_log=None, handler_cancellation=True)
        tasks = []
        bound_socket = None
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        signals = []
        try:
            await runner.setup()
            await web.UnixSite(runner, str(path)).start()
            bound_socket = path.lstat()
            path.chmod(0o600)
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
                signals.append(sig)
            tasks = [
                asyncio.create_task(mesh.link.run()),
                asyncio.create_task(mesh.maintenance()),
            ]
            await stop.wait()
        finally:
            for sig in signals:
                loop.remove_signal_handler(sig)
            for site in tuple(runner.sites):
                await site.stop()
            tasks.extend(mesh.work)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await runner.cleanup()
            mesh.store.close()
            if bound_socket is not None:
                with contextlib.suppress(FileNotFoundError):
                    current = path.lstat()
                    if (current.st_dev, current.st_ino) == (
                        bound_socket.st_dev,
                        bound_socket.st_ino,
                    ):
                        path.unlink()
