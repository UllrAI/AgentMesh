"""Durable local state for Agent Mesh requests and execution slots."""

import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from uuid import UUID, uuid4

from jsonschema.validators import validator_for

SPEC_VERSION = "1.3"
MAX_PAYLOAD_BYTES = 64 * 1024
TERMINAL = frozenset({"succeeded", "failed", "needs_input", "unknown"})

SCHEMA = """
CREATE TABLE IF NOT EXISTS inbound (
    caller_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    digest BLOB NOT NULL,
    session_id TEXT,
    interaction TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    retain_until INTEGER,
    snapshot TEXT NOT NULL,
    PRIMARY KEY (caller_id, job_id)
);
CREATE TABLE IF NOT EXISTS execution_slots (
    caller_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    session_id TEXT UNIQUE,
    execution_id TEXT,
    recovering INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (caller_id, job_id)
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    caller_id TEXT NOT NULL,
    method TEXT NOT NULL,
    runtime_session_id TEXT,
    expires_at INTEGER NOT NULL,
    valid INTEGER NOT NULL DEFAULT 1,
    pending_job_id TEXT,
    pending_expires_at INTEGER
);
CREATE TABLE IF NOT EXISTS outbound (
    job_id TEXT PRIMARY KEY,
    target_id TEXT NOT NULL,
    method TEXT NOT NULL,
    submitted_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    session_id TEXT,
    answer_to TEXT,
    snapshot TEXT,
    invoke_error TEXT,
    query_error TEXT,
    invoke_rejected INTEGER NOT NULL DEFAULT 0,
    payload BLOB
);
CREATE INDEX IF NOT EXISTS outbound_answers ON outbound(answer_to);
"""


def _now(value):
    return int(time.time()) if value is None else int(value)


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _decode(value):
    return json.loads(value) if value is not None else None


def _invalid_constant(value):
    raise ValueError(f"Invalid JSON constant: {value}")


def _load_payload(payload):
    try:
        request = json.loads(payload.decode("utf-8"), parse_constant=_invalid_constant)
    except (ValueError, RecursionError):
        return None
    return request if isinstance(request, dict) else None


def _uuid(value):
    if not isinstance(value, str) or len(value) != 36:
        return None
    try:
        identifier = UUID(value)
    except ValueError:
        return None
    if identifier.version == 4 and str(identifier) == value.lower():
        return str(identifier)
    return None


def _positive_int(value):
    return type(value) is int and value > 0


def _wire(kind, job_id, **fields):
    return {"spec_version": SPEC_VERSION, "type": kind, "job_id": job_id, **fields}


def _error(kind, job_id, code, message):
    return _wire(f"{kind}_error", job_id, error={"code": code, "message": message})


def _valid_error(error):
    return isinstance(error, dict) and all(
        isinstance(error.get(key), str) and error[key] for key in ("code", "message")
    )


def _delivery(reply, *, request=None, runtime_session_id=None):
    return {
        "reply": reply,
        "execute": request is not None,
        "request": request,
        "runtime_session_id": runtime_session_id,
    }


def _valid_invoke(request):
    expires_at = request.get("expires_at")
    if type(expires_at) is not int or not -(2**63) <= expires_at < 2**63:
        return False
    if not isinstance(request.get("method"), str) or not request["method"]:
        return False
    if "session_id" in request and not _uuid(request["session_id"]):
        return False
    if "execution_timeout_s" in request and not _positive_int(
        request["execution_timeout_s"]
    ):
        return False
    if "answer_to" in request or "answer_text" in request:
        answer_text = request.get("answer_text")
        return (
            "params" not in request
            and "session_id" in request
            and _uuid(request.get("answer_to")) is not None
            and isinstance(answer_text, str)
            and bool(answer_text)
        )
    return isinstance(request.get("params"), dict)


class Store:
    def __init__(
        self, path, *, retention_s=86400, session_ttl_s=86400, question_ttl_s=86400
    ):
        self.retention_s = retention_s
        self.session_ttl_s = session_ttl_s
        self.question_ttl_s = question_ttl_s
        self._db = sqlite3.connect(path, isolation_level=None)
        try:
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.executescript(SCHEMA)
        except BaseException:
            self._db.close()
            raise

    def close(self):
        self._db.close()

    @contextmanager
    def _transaction(self):
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def _inbound(self, caller_id, job_id):
        return self._db.execute(
            "SELECT * FROM inbound WHERE caller_id=? AND job_id=?", (caller_id, job_id)
        ).fetchone()

    def accept_inbound(
        self, caller_id, payload, *, methods, max_slots, ready=True, now=None
    ):
        """Accept an invoke once, or return a snapshot or protocol error for delivery."""
        request = _load_payload(payload)
        job_id = _uuid(request.get("job_id")) if request is not None else None
        if job_id is None:
            return None
        kind = request.get("type")
        if (
            len(payload) > MAX_PAYLOAD_BYTES
            or request.get("spec_version") != SPEC_VERSION
        ):
            error_kind = "query" if kind == "query" else "invoke"
            return _delivery(
                _error(
                    error_kind, job_id, "INVALID_REQUEST", "Invalid request envelope"
                )
            )
        if kind == "query":
            return _delivery(self.query_inbound(caller_id, job_id))
        if kind != "invoke":
            return _delivery(
                _error("invoke", job_id, "INVALID_REQUEST", "Unknown request type")
            )

        digest = hashlib.sha256(payload).digest()
        try:
            existing = self._inbound(caller_id, job_id)
            if existing is not None:
                return self._duplicate(existing, digest)
            with self._transaction():
                existing = self._inbound(caller_id, job_id)
                if existing is not None:
                    return self._duplicate(existing, digest)
                return self._accept_new(
                    caller_id,
                    job_id,
                    request,
                    digest,
                    methods,
                    max_slots,
                    ready,
                    _now(now),
                )
        except sqlite3.OperationalError:
            return _delivery(
                _error("invoke", job_id, "NOT_READY", "Task storage is not writable")
            )

    @staticmethod
    def _duplicate(existing, digest):
        if existing["digest"] == digest:
            return _delivery(_decode(existing["snapshot"]))
        return _delivery(
            _error(
                "invoke", existing["job_id"], "JOB_ID_CONFLICT", "Job ID already used"
            )
        )

    def _accept_new(
        self, caller_id, job_id, request, digest, methods, max_slots, ready, now
    ):
        def reject(code, message):
            return _delivery(_error("invoke", job_id, code, message))

        if not _valid_invoke(request):
            return reject("INVALID_REQUEST", "Invalid invocation fields")
        if request["expires_at"] <= now:
            return reject("EXPIRED", "The first acceptance deadline has passed")
        method = request["method"]
        config = methods.get(method)
        if config is None:
            return reject("METHOD_NOT_FOUND", "Method is unavailable")

        interaction = config.get("interaction", "single_turn")
        session_id = _uuid(request.get("session_id"))
        answer_to = _uuid(request.get("answer_to"))
        if session_id is not None and interaction == "single_turn":
            return reject(
                "INVALID_REQUEST", "Single-turn methods do not accept a session"
            )
        if answer_to is not None and interaction != "clarify":
            return reject(
                "INVALID_REQUEST", "The method does not accept clarification answers"
            )
        if (
            answer_to is None
            and "input_schema" in config
            and config["input_schema"] is not None
        ):
            schema = config["input_schema"]
            if not validator_for(schema)(schema).is_valid(request["params"]):
                return reject(
                    "INVALID_REQUEST", "Parameters do not conform to the method schema"
                )

        session = None
        if session_id is not None:
            session = self._db.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
            if (
                session is None
                or session["caller_id"] != caller_id
                or session["method"] != method
                or not session["valid"]
            ):
                return reject("SESSION_NOT_FOUND", "Session is unavailable")
            occupied = (
                self._db.execute(
                    "SELECT 1 FROM execution_slots WHERE session_id=?", (session_id,)
                ).fetchone()
                is not None
            )
            if session["expires_at"] <= now and not occupied:
                return reject("SESSION_NOT_FOUND", "Session has expired")
            pending = (
                session["pending_job_id"]
                if (session["pending_expires_at"] or 0) > now
                else None
            )
            if answer_to is not None and answer_to != pending:
                return reject(
                    "INPUT_NOT_PENDING", "The specified question is no longer pending"
                )
            if answer_to is None and pending is not None:
                return reject(
                    "INPUT_REQUIRED", "The session is awaiting a clarification answer"
                )
            if occupied:
                return reject("BUSY", "The session is executing another round")

        if not ready:
            return reject("NOT_READY", "The execution adapter is not ready")
        slots = self._db.execute(
            "SELECT COUNT(*) AS total, COUNT(CASE WHEN recovering=0 THEN 1 END) AS running FROM execution_slots"
        ).fetchone()
        if slots["running"] >= max_slots:
            return reject("BUSY", "All execution slots are in use")
        if slots["total"] >= max_slots:
            return reject("NOT_READY", "An old execution still holds a slot")

        if session_id is None and interaction != "single_turn":
            session_id = str(uuid4())
            self._db.execute(
                "INSERT INTO sessions (session_id, caller_id, method, expires_at) VALUES (?,?,?,?)",
                (session_id, caller_id, method, now + self.session_ttl_s),
            )
        elif session_id is not None:
            self._db.execute(
                "UPDATE sessions SET pending_job_id=NULL, pending_expires_at=NULL WHERE session_id=?",
                (session_id,),
            )

        configured_timeout = config.get("execution_timeout_s", 300)
        timeout = min(
            request.get("execution_timeout_s", configured_timeout), configured_timeout
        )
        reply = _wire("job", job_id, status="accepted", execution_timeout_s=timeout)
        if session_id is not None:
            reply["session_id"] = session_id
        self._db.execute(
            """INSERT INTO inbound (caller_id, job_id, digest, session_id, interaction, expires_at, updated_at, snapshot)
            VALUES (?,?,?,?,?,?,?,?)""",
            (
                caller_id,
                job_id,
                digest,
                session_id,
                interaction,
                request["expires_at"],
                now,
                _json(reply),
            ),
        )
        self._db.execute(
            "INSERT INTO execution_slots (caller_id, job_id, session_id) VALUES (?,?,?)",
            (caller_id, job_id, session_id),
        )
        runtime_session_id = (
            session["runtime_session_id"] if session is not None else None
        )
        return _delivery(reply, request=request, runtime_session_id=runtime_session_id)

    def query_inbound(self, caller_id, job_id):
        normalized = _uuid(job_id)
        row = self._inbound(caller_id, normalized)
        if row is None:
            return _error(
                "query", normalized or job_id, "JOB_NOT_FOUND", "No record of this job"
            )
        return _decode(row["snapshot"])

    def record_execution(
        self, caller_id, job_id, *, execution_id=None, runtime_session_id=None
    ):
        job_id = _uuid(job_id)
        with self._transaction():
            slot = self._db.execute(
                "SELECT * FROM execution_slots WHERE caller_id=? AND job_id=? AND recovering=0",
                (caller_id, job_id),
            ).fetchone()
            if slot is None:
                raise LookupError("No active execution for this job")
            if execution_id is not None:
                self._db.execute(
                    "UPDATE execution_slots SET execution_id=? WHERE caller_id=? AND job_id=?",
                    (_json(execution_id), caller_id, job_id),
                )
            if runtime_session_id is not None and slot["session_id"] is not None:
                self._db.execute(
                    "UPDATE sessions SET runtime_session_id=? WHERE session_id=?",
                    (runtime_session_id, slot["session_id"]),
                )

    def finish_inbound(
        self,
        caller_id,
        job_id,
        status,
        *,
        result=None,
        question=None,
        error=None,
        release_slot=True,
        now=None,
    ):
        with self._transaction():
            row = self._inbound(caller_id, _uuid(job_id))
            if row is None:
                raise LookupError("No accepted job to finish")
            return self._finish(
                row, status, result, question, error, release_slot, _now(now)
            )

    def _finish(self, row, status, result, question, error, release_slot, now):
        snapshot = _decode(row["snapshot"])
        if snapshot["status"] in TERMINAL:
            return snapshot
        if status not in TERMINAL:
            raise ValueError("A job can only finish with a terminal status")

        caller_id, job_id, session_id = (
            row["caller_id"],
            row["job_id"],
            row["session_id"],
        )
        snapshot = _wire("job", job_id, status=status)
        if session_id is not None:
            snapshot["session_id"] = session_id
        pending_job_id = None
        pending_expires_at = None
        session_expires_at = now + self.session_ttl_s
        if status == "needs_input":
            question_text = (
                question.get("text") if isinstance(question, dict) else question
            )
            if (
                row["interaction"] != "clarify"
                or not isinstance(question_text, str)
                or not question_text
            ):
                raise ValueError(
                    "Clarification requires a clarify method and nonempty question"
                )
            pending_job_id = job_id
            pending_expires_at = min(now + self.question_ttl_s, session_expires_at)
            snapshot["question"] = {
                "text": question_text,
                "expires_at": pending_expires_at,
            }
        elif status == "succeeded":
            snapshot["result"] = result
        else:
            if not _valid_error(error):
                raise ValueError(
                    "A failed or unknown outcome requires an error code and message"
                )
            snapshot["error"] = error

        encoded = _json(snapshot)
        if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES:
            oversized_error = {
                "code": "RESULT_TOO_LARGE",
                "message": "Result exceeds the payload limit",
            }
            if status == "unknown":
                oversized_error["code"] = error["code"]
                snapshot["error"] = oversized_error
                encoded = _json(snapshot)
            else:
                return self._finish(
                    row, "failed", None, None, oversized_error, release_slot, now
                )
        retain_until = max(
            now + self.retention_s, row["expires_at"], pending_expires_at or 0
        )
        self._db.execute(
            "UPDATE inbound SET snapshot=?, updated_at=?, retain_until=? WHERE caller_id=? AND job_id=?",
            (encoded, now, retain_until, caller_id, job_id),
        )
        if session_id is not None:
            session_valid = status != "unknown" and (
                error is None or error["code"] != "SESSION_NOT_FOUND"
            )
            self._db.execute(
                "UPDATE sessions SET expires_at=?, valid=?, pending_job_id=?, pending_expires_at=? WHERE session_id=?",
                (
                    session_expires_at,
                    session_valid,
                    pending_job_id,
                    pending_expires_at,
                    session_id,
                ),
            )
        if release_slot:
            self._db.execute(
                "DELETE FROM execution_slots WHERE caller_id=? AND job_id=?",
                (caller_id, job_id),
            )
        else:
            self._db.execute(
                "UPDATE execution_slots SET recovering=1 WHERE caller_id=? AND job_id=?",
                (caller_id, job_id),
            )
        return snapshot

    def recover_inbound(self, now=None):
        """Publish no work on restart, and retain old slots until execution has ended."""
        now = _now(now)
        error = {
            "code": "OUTCOME_UNKNOWN",
            "message": "Daemon restarted before the outcome was persisted",
        }
        with self._transaction():
            rows = self._db.execute(
                """SELECT inbound.* FROM inbound JOIN execution_slots USING (caller_id, job_id)
                WHERE execution_slots.recovering=0"""
            ).fetchall()
            for row in rows:
                self._finish(row, "unknown", None, None, error, False, now)
            slots = self._db.execute(
                "SELECT caller_id, job_id, execution_id FROM execution_slots"
            ).fetchall()
            return [
                {**dict(slot), "execution_id": _decode(slot["execution_id"])}
                for slot in slots
            ]

    def release_execution(self, caller_id, job_id):
        """Release a held slot after the runtime or supervisor confirms termination."""
        cursor = self._db.execute(
            "DELETE FROM execution_slots WHERE caller_id=? AND job_id=? AND recovering=1",
            (caller_id, _uuid(job_id)),
        )
        return cursor.rowcount == 1

    def prune(self, now=None):
        now = _now(now)
        with self._transaction():
            self._db.execute("DELETE FROM inbound WHERE retain_until<=?", (now,))
            self._db.execute(
                """DELETE FROM sessions WHERE expires_at<=? AND NOT EXISTS
                (SELECT 1 FROM execution_slots WHERE execution_slots.session_id=sessions.session_id)""",
                (now,),
            )
            self._db.execute(
                "UPDATE outbound SET payload=NULL WHERE expires_at<=? AND payload IS NOT NULL",
                (now,),
            )

    def save_outbound(self, target_id, payload, *, now=None):
        """Index an invocation and persist its exact bytes before the first send."""
        request = _load_payload(payload)
        job_id = _uuid(request.get("job_id")) if request is not None else None
        if (
            not job_id
            or len(payload) > MAX_PAYLOAD_BYTES
            or request.get("type") != "invoke"
            or request.get("spec_version") != SPEC_VERSION
            or not _valid_invoke(request)
        ):
            raise ValueError("Invalid outbound invocation")
        self._db.execute(
            """INSERT INTO outbound (job_id, target_id, method, submitted_at, expires_at, session_id, answer_to, payload)
            VALUES (?,?,?,?,?,?,?,?)""",
            (
                job_id,
                target_id,
                request["method"],
                _now(now),
                request["expires_at"],
                _uuid(request.get("session_id")),
                _uuid(request.get("answer_to")),
                payload,
            ),
        )
        return self.get_outbound(job_id, now=now)

    def _outbound(self, job_id):
        return self._db.execute(
            "SELECT * FROM outbound WHERE job_id=?", (_uuid(job_id),)
        ).fetchone()

    def record_outbound_reply(self, target_id, reply):
        """Apply a validated business snapshot or error from the registered target."""
        if not isinstance(reply, dict) or reply.get("spec_version") != SPEC_VERSION:
            return False
        kind = reply.get("type")
        with self._transaction():
            row = self._outbound(reply.get("job_id"))
            if row is None or row["target_id"] != target_id:
                return False
            if kind in ("invoke_error", "query_error") and _valid_error(
                reply.get("error")
            ):
                self.record_outbound_error(
                    row["job_id"], kind, reply["error"], explicit=True
                )
                return True
            if kind != "job" or not self._valid_snapshot(reply):
                return False
            previous = _decode(row["snapshot"])
            if previous is not None and previous["status"] in TERMINAL:
                return False
            session_id = _uuid(reply.get("session_id"))
            if session_id and row["session_id"] and session_id != row["session_id"]:
                return False
            self._db.execute(
                "UPDATE outbound SET snapshot=?, session_id=COALESCE(?, session_id), payload=NULL WHERE job_id=?",
                (_json(reply), session_id, row["job_id"]),
            )
            return True

    @staticmethod
    def _valid_snapshot(reply):
        if "session_id" in reply and not _uuid(reply["session_id"]):
            return False
        status = reply.get("status")
        if status == "accepted":
            return _positive_int(reply.get("execution_timeout_s"))
        if status == "succeeded":
            return "result" in reply
        if status in ("failed", "unknown"):
            return _valid_error(reply.get("error"))
        if status == "needs_input":
            question = reply.get("question")
            return (
                "session_id" in reply
                and isinstance(question, dict)
                and isinstance(question.get("text"), str)
                and bool(question["text"])
                and _positive_int(question.get("expires_at"))
            )
        return False

    def record_outbound_error(self, job_id, kind, error, *, explicit=False):
        if not _valid_error(error):
            raise ValueError("An error requires a code and message")
        if kind in ("invoke", "invoke_error"):
            cursor = self._db.execute(
                """UPDATE outbound SET invoke_error=?, invoke_rejected=?
                WHERE job_id=? AND (invoke_rejected=0 OR ?)""",
                (_json(error), explicit, _uuid(job_id), explicit),
            )
        elif kind in ("query", "query_error"):
            cursor = self._db.execute(
                "UPDATE outbound SET query_error=? WHERE job_id=?",
                (_json(error), _uuid(job_id)),
            )
        else:
            raise ValueError("Expected an invoke or query error")
        return cursor.rowcount == 1

    def retry_outbound(self, job_id, now=None):
        now = _now(now)
        with self._transaction():
            row = self._outbound(job_id)
            if row is None or row["payload"] is None:
                return None
            if row["expires_at"] <= now:
                self._db.execute(
                    "UPDATE outbound SET payload=NULL WHERE job_id=?", (row["job_id"],)
                )
                return None
            if row["invoke_rejected"]:
                if _decode(row["invoke_error"])["code"] != "BUSY":
                    return None
                self._db.execute(
                    "UPDATE outbound SET invoke_error=NULL, invoke_rejected=0 WHERE job_id=?",
                    (row["job_id"],),
                )
            return row["target_id"], row["payload"], min(30, row["expires_at"] - now)

    def _answer(self, row, snapshot, now):
        if snapshot is None or snapshot["status"] != "needs_input":
            return None, None
        expires_at = snapshot["question"]["expires_at"]
        candidates = self._db.execute(
            """SELECT * FROM outbound WHERE answer_to=? AND target_id=? AND session_id=? AND method=?
            AND (snapshot IS NOT NULL OR invoke_rejected=0) ORDER BY snapshot IS NULL, rowid DESC""",
            (row["job_id"], row["target_id"], row["session_id"], row["method"]),
        )
        followup = candidates.fetchone()
        if followup is not None:
            accepted = followup["snapshot"] is not None
            answer = {
                "status": "answered" if accepted else "pending",
                "expires_at": expires_at,
            }
            if accepted:
                answer["job_id"] = followup["job_id"]
            else:
                pending = [followup["job_id"], *(item["job_id"] for item in candidates)]
                if len(pending) == 1:
                    answer["job_id"] = pending[0]
                else:
                    answer["pending_job_ids"] = pending
            return answer, followup
        return {
            "status": "expired" if expires_at <= now else "waiting",
            "expires_at": expires_at,
        }, None

    def _outbound_summary(self, row, now):
        snapshot = _decode(row["snapshot"])
        answer, followup = self._answer(row, snapshot, now)
        summary = {
            key: row[key]
            for key in (
                "job_id",
                "target_id",
                "method",
                "submitted_at",
                "expires_at",
                "session_id",
                "answer_to",
            )
        }
        summary.update(
            snapshot=snapshot,
            invoke_error=_decode(row["invoke_error"]),
            query_error=_decode(row["query_error"]),
            answer=answer,
            latest_answer=None,
            query_job_id=row["job_id"]
            if snapshot is None or snapshot["status"] == "accepted"
            else None,
        )
        return summary, followup

    def get_outbound(self, job_id, now=None):
        row = self._outbound(job_id)
        if row is None:
            return None
        now = _now(now)
        summary, followup = self._outbound_summary(row, now)
        cursor = summary
        seen = {row["job_id"]}
        while (
            followup is not None
            and cursor["answer"]["status"] == "answered"
            and followup["job_id"] not in seen
        ):
            seen.add(followup["job_id"])
            cursor, followup = self._outbound_summary(followup, now)
        if cursor is not summary:
            summary["latest_answer"] = cursor
            summary["query_job_id"] = cursor["query_job_id"]
        return summary

    def list_outbound(self, limit=20, now=None):
        if type(limit) is not int or limit < 1:
            raise ValueError("List limit must be a positive integer")
        now = _now(now)
        rows = self._db.execute(
            "SELECT * FROM outbound ORDER BY submitted_at DESC, rowid DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._outbound_summary(row, now)[0] for row in rows]
