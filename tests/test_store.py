import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from agent_mesh.store import MAX_PAYLOAD_BYTES, Store

METHOD = "codex.analyze"
METHODS = {
    METHOD: {
        "interaction": "clarify",
        "execution_timeout_s": 40,
        "input_schema": {
            "type": "object",
            "required": ["prompt", "project"],
            "properties": {"prompt": {"type": "string"}, "project": {"type": "string"}},
            "additionalProperties": False,
        },
    },
    "other": {"interaction": "session"},
    "once": {},
}


@pytest.fixture
def store(tmp_path):
    instance = Store(
        tmp_path / "jobs.db", retention_s=10, session_ttl_s=50, question_ttl_s=20
    )
    yield instance
    instance.close()


def request(**fields):
    base = {
        "spec_version": "1.3",
        "type": "invoke",
        "job_id": str(uuid4()),
        "method": METHOD,
        "params": {"prompt": "Analyze the modules", "project": "Tide"},
        "expires_at": 130,
    }
    if "answer_to" in fields:
        del base["params"]
    return {**base, **fields}


def wire(data):
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()


def reply(job, status, **fields):
    return {
        "spec_version": "1.3",
        "type": "job",
        "job_id": job["job_id"],
        "status": status,
        **fields,
    }


def error(job, kind, code):
    return {
        "spec_version": "1.3",
        "type": f"{kind}_error",
        "job_id": job["job_id"],
        "error": {"code": code, "message": "test explanation"},
    }


def receive(store, job, *, caller="caller", now=100, max_slots=1, ready=True):
    payload = job if isinstance(job, bytes) else wire(job)
    return store.accept_inbound(
        caller, payload, methods=METHODS, max_slots=max_slots, ready=ready, now=now
    )


def code(entry):
    return entry["reply"]["error"]["code"]


def ask(store, job, *, now=100):
    first = receive(store, job, now=now)
    assert first["execute"]
    store.record_execution("caller", job["job_id"], runtime_session_id="runtime-101")
    return store.finish_inbound(
        "caller", job["job_id"], "needs_input", question="Which service?", now=now + 1
    )


def test_inbound_deduplicates_original_bytes_and_keeps_terminal(store):
    job = request(execution_timeout_s=100)
    first = receive(store, job)
    assert first["execute"]
    assert first["reply"]["execution_timeout_s"] == 40
    assert first["runtime_session_id"] is None
    assert first["request"] == job

    duplicate = receive(store, job, ready=False, now=150)
    assert not duplicate["execute"]
    assert duplicate["reply"] == first["reply"]
    assert code(receive(store, json.dumps(job).encode(), now=150)) == "JOB_ID_CONFLICT"
    assert code(receive(store, {**job, "params": []})) == "JOB_ID_CONFLICT"
    completed = store.finish_inbound(
        "caller", job["job_id"], "succeeded", result="Done", now=102
    )
    assert (
        store.finish_inbound(
            "caller", job["job_id"], "succeeded", result="Other", now=103
        )
        == completed
    )
    assert receive(store, job, now=112)["reply"] == completed

    query = {"spec_version": "1.3", "type": "query", "job_id": job["job_id"]}
    assert receive(store, query, ready=False)["reply"] == completed
    assert code(receive(store, query, caller="another")) == "JOB_NOT_FOUND"
    assert receive(store, job, caller="another")["execute"]

    store.prune(now=129)
    assert store.query_inbound("caller", job["job_id"]) == completed
    store.prune(now=130)
    assert (
        store.query_inbound("caller", job["job_id"])["error"]["code"] == "JOB_NOT_FOUND"
    )
    assert code(receive(store, job, now=130)) == "EXPIRED"


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({"method": "missing"}, "METHOD_NOT_FOUND"),
        ({"expires_at": 0}, "EXPIRED"),
        ({"params": {"prompt": "missing project"}}, "INVALID_REQUEST"),
        ({"params": {"prompt": 3, "project": "Tide"}}, "INVALID_REQUEST"),
        ({"expires_at": True}, "INVALID_REQUEST"),
        ({"execution_timeout_s": 0}, "INVALID_REQUEST"),
        ({"session_id": None}, "INVALID_REQUEST"),
        ({"answer_text": "API"}, "INVALID_REQUEST"),
        (
            {"answer_to": str(uuid4()), "session_id": str(uuid4()), "answer_text": ""},
            "INVALID_REQUEST",
        ),
        (
            {
                "answer_to": str(uuid4()),
                "session_id": str(uuid4()),
                "answer_text": "API",
                "params": {},
            },
            "INVALID_REQUEST",
        ),
        ({"method": "once", "session_id": str(uuid4())}, "INVALID_REQUEST"),
    ],
)
def test_inbound_rejects_invalid_calls_without_creating_job(store, fields, expected):
    job = request(**fields)
    assert code(receive(store, job)) == expected
    assert (
        store.query_inbound("caller", job["job_id"])["error"]["code"] == "JOB_NOT_FOUND"
    )


def test_invalid_or_unidentified_wire_input(store):
    assert receive(store, b"{invalid-json}") is None
    assert receive(store, b'{"job_id":"unknown"}') is None
    assert code(receive(store, request(spec_version="1.4"))) == "INVALID_REQUEST"
    assert code(receive(store, request(type="invalid"))) == "INVALID_REQUEST"
    assert (
        code(
            receive(
                store,
                request(params={"prompt": "é" * MAX_PAYLOAD_BYTES, "project": "Tide"}),
            )
        )
        == "INVALID_REQUEST"
    )
    job = request(method="once")
    assert "session_id" not in receive(store, job)["reply"]
    oversized = store.finish_inbound(
        "caller", job["job_id"], "succeeded", result="界" * MAX_PAYLOAD_BYTES, now=100
    )
    assert oversized["status"] == "failed"
    assert oversized["error"]["code"] == "RESULT_TOO_LARGE"
    assert receive(store, request(method="once"))["execute"]


def test_read_only_storage_rejects_new_work_but_can_return_existing_snapshots(store):
    job = request()
    accepted = receive(store, job)["reply"]
    store._db.execute("PRAGMA query_only=ON")
    assert receive(store, job)["reply"] == accepted
    assert store.query_inbound("caller", job["job_id"]) == accepted
    assert code(receive(store, request())) == "NOT_READY"


def test_answer_preserved_when_invalid_or_busy_and_uses_existing_runtime_session(store):
    question_job = request()
    question = ask(store, question_job)
    session = question["session_id"]
    assert question["question"] == {"text": "Which service?", "expires_at": 121}
    assert (
        code(receive(store, request(session_id=session), caller="alien"))
        == "SESSION_NOT_FOUND"
    )
    assert (
        code(receive(store, request(session_id=session, method="other")))
        == "SESSION_NOT_FOUND"
    )
    assert code(receive(store, request(session_id=session))) == "INPUT_REQUIRED"
    answer = request(
        session_id=session, answer_to=question_job["job_id"], answer_text="API"
    )
    assert (
        code(receive(store, {**answer, "answer_to": str(uuid4())}))
        == "INPUT_NOT_PENDING"
    )
    assert code(receive(store, answer, ready=False)) == "NOT_READY"

    blocker = request(method="once")
    assert receive(store, blocker)["execute"]
    assert code(receive(store, answer)) == "BUSY"
    store.finish_inbound(
        "caller", blocker["job_id"], "succeeded", result="released", now=104
    )

    accepted = receive(store, answer, now=104)
    assert accepted["execute"] and accepted["runtime_session_id"] == "runtime-101"
    assert not receive(store, answer, now=105)["execute"]
    assert (
        code(receive(store, {**answer, "job_id": str(uuid4())})) == "INPUT_NOT_PENDING"
    )
    assert code(receive(store, request(session_id=session))) == "BUSY"
    assert store.query_inbound("caller", question_job["job_id"]) == question
    store.finish_inbound(
        "caller", answer["job_id"], "succeeded", result="analyzed", now=107
    )
    assert (
        receive(store, request(session_id=session), now=108)["runtime_session_id"]
        == "runtime-101"
    )


def test_concurrent_answers_consume_the_question_once(tmp_path):
    path = tmp_path / "jobs.db"
    seed = Store(path)
    initial = request()
    question = ask(seed, initial)
    seed.close()
    barrier = Barrier(2, timeout=5)

    def answer(text):
        contender = Store(path)
        try:
            barrier.wait()
            return receive(
                contender,
                request(
                    session_id=question["session_id"],
                    answer_to=initial["job_id"],
                    answer_text=text,
                ),
            )
        finally:
            contender.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        replies = list(pool.map(answer, ["API", "Web"]))
    assert sorted(entry["reply"].get("status") or code(entry) for entry in replies) == [
        "INPUT_NOT_PENDING",
        "accepted",
    ]


def test_question_expiry_and_active_session_ttl(store):
    job = request()
    question = ask(store, job)
    session = question["session_id"]
    answer = request(session_id=session, answer_to=job["job_id"], answer_text="API")
    assert code(receive(store, answer, now=121)) == "INPUT_NOT_PENDING"
    followup = request(session_id=session, expires_at=220)
    assert receive(store, followup, now=149)["execute"]
    store.prune(now=180)
    assert (
        code(receive(store, request(session_id=session, expires_at=220), now=180))
        == "BUSY"
    )
    store.finish_inbound(
        "caller", followup["job_id"], "succeeded", result="done", now=185
    )
    assert (
        code(receive(store, request(session_id=session, expires_at=260), now=235))
        == "SESSION_NOT_FOUND"
    )


def test_question_snapshot_lives_until_deadline(tmp_path):
    path = tmp_path / "jobs.db"
    store = Store(path, retention_s=2, session_ttl_s=60, question_ttl_s=120)
    try:
        job = request(expires_at=103)
        question = ask(store, job)
        assert question["question"]["expires_at"] == 161
        store.prune(now=160)
        assert store.query_inbound("caller", job["job_id"]) == question
        store.prune(now=161)
        assert (
            store.query_inbound("caller", job["job_id"])["error"]["code"]
            == "JOB_NOT_FOUND"
        )
    finally:
        store.close()


def test_restart_retains_old_slots_across_history_prune(tmp_path):
    path = tmp_path / "jobs.db"
    initial = Store(path, retention_s=5)
    question_job = request()
    question = ask(initial, question_job)
    active = request()
    active_session = receive(initial, active)["reply"]["session_id"]
    token = {"kind": "process", "token": "old-execution"}
    initial.record_execution("caller", active["job_id"], execution_id=token)
    initial.close()

    reopened = Store(path, retention_s=5)
    held = [{"caller_id": "caller", "job_id": active["job_id"], "execution_id": token}]
    try:
        assert reopened.recover_inbound(now=105) == held
        assert reopened.query_inbound("caller", question_job["job_id"]) == question
        assert reopened.query_inbound("caller", active["job_id"])["status"] == "unknown"
        assert (
            code(receive(reopened, request(session_id=active_session), now=106))
            == "SESSION_NOT_FOUND"
        )
        assert code(receive(reopened, request(method="once"), now=106)) == "NOT_READY"
        parallel = request(method="once")
        assert receive(reopened, parallel, now=106, max_slots=2)["execute"]
        reopened.finish_inbound(
            "caller", parallel["job_id"], "succeeded", result="done", now=107
        )
        reopened.prune(now=131)
        assert (
            reopened.query_inbound("caller", active["job_id"])["error"]["code"]
            == "JOB_NOT_FOUND"
        )
    finally:
        reopened.close()

    latest = Store(path)
    try:
        assert latest.recover_inbound(now=132) == held
        assert (
            code(receive(latest, request(method="once", expires_at=170), now=132))
            == "NOT_READY"
        )
        assert latest.release_execution("caller", active["job_id"])
        assert receive(latest, request(method="once", expires_at=170), now=133)[
            "execute"
        ]
    finally:
        latest.close()


def test_unknown_runtime_outcome_keeps_slot_while_normal_error_releases_it(store):
    job = request()
    session = receive(store, job)["reply"]["session_id"]
    timeout = {"code": "EXECUTION_TIMEOUT", "message": "Could not confirm termination"}
    unknown = store.finish_inbound(
        "caller", job["job_id"], "unknown", error=timeout, release_slot=False, now=104
    )
    assert unknown["error"] == timeout
    assert code(receive(store, request(session_id=session))) == "SESSION_NOT_FOUND"
    assert code(receive(store, request())) == "NOT_READY"
    assert store.release_execution("caller", job["job_id"])
    next_job = request()
    assert receive(store, next_job)["execute"]
    store.finish_inbound("caller", next_job["job_id"], "failed", error=timeout, now=108)
    assert receive(store, request())["execute"]


def test_outbound_retry_uses_persisted_payload_and_clears_raw_after_snapshot(tmp_path):
    path = tmp_path / "jobs.db"
    job = request()
    payload = json.dumps(job, indent=2).encode()
    first = Store(path)
    first.save_outbound("target", payload, now=100)
    first.close()
    store = Store(path)
    try:
        assert store.retry_outbound(job["job_id"], now=106) == ("target", payload, 24)
        assert store.record_outbound_reply("target", error(job, "invoke", "BUSY"))
        store.record_outbound_error(
            job["job_id"],
            "invoke",
            {"code": "OUTCOME_UNKNOWN", "message": "publish unconfirmed"},
        )
        assert store.retry_outbound(job["job_id"], now=110) == ("target", payload, 20)
        assert not store.record_outbound_reply(
            "impostor", reply(job, "succeeded", result="wrong")
        )
        assert store.record_outbound_reply(
            "target", reply(job, "accepted", execution_timeout_s=40)
        )
        assert store.retry_outbound(job["job_id"], now=111) is None

        expired = request()
        store.save_outbound("target", wire(expired), now=100)
        store.prune(now=130)
        assert store.get_outbound(expired["job_id"])["snapshot"] is None
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT payload FROM outbound").fetchall() == [
                (None,),
                (None,),
            ]
    finally:
        store.close()


def test_outbound_answer_chain_and_query_errors_preserve_terminal(store):
    session = str(uuid4())
    first = request()
    second = request(session_id=session, answer_to=first["job_id"], answer_text="API")
    last = request(session_id=session, answer_to=second["job_id"], answer_text="today")
    refused = request(session_id=session, answer_to=first["job_id"], answer_text="Web")
    ordinary = request(session_id=session)
    questions = {"text": "What should I analyze?", "expires_at": 123}
    first_snapshot = reply(first, "needs_input", session_id=session, question=questions)

    store.save_outbound("target", wire(first), now=100)
    assert store.record_outbound_reply("target", first_snapshot)
    assert store.get_outbound(first["job_id"], now=110)["answer"]["status"] == "waiting"

    store.save_outbound("target", wire(refused), now=100)
    assert store.get_outbound(first["job_id"], now=110)["answer"]["status"] == "pending"
    assert store.record_outbound_reply(
        "target", error(refused, "invoke", "INVALID_REQUEST")
    )
    assert store.retry_outbound(refused["job_id"], now=112) is None
    assert store.get_outbound(first["job_id"], now=123)["answer"]["status"] == "expired"

    store.save_outbound("target", wire(second), now=110)
    assert store.record_outbound_reply(
        "target", reply(second, "needs_input", session_id=session, question=questions)
    )
    store.save_outbound("target", wire(last), now=111)
    detail = store.get_outbound(first["job_id"], now=120)
    assert detail["answer"]["status"] == "answered"
    assert detail["latest_answer"]["answer"]["job_id"] == last["job_id"]
    assert detail["latest_answer"]["answer"]["status"] == "pending"
    assert detail["query_job_id"] is None
    assert store.record_outbound_reply(
        "target", reply(last, "accepted", session_id=session, execution_timeout_s=40)
    )
    assert store.get_outbound(first["job_id"])["query_job_id"] == last["job_id"]

    store.save_outbound("target", wire(ordinary), now=120)
    assert store.record_outbound_reply(
        "target", reply(ordinary, "succeeded", session_id=session, result="new work")
    )
    result = reply(last, "succeeded", session_id=session, result="original result")
    assert store.record_outbound_reply("target", result)
    assert not store.record_outbound_reply(
        "target", reply(last, "accepted", execution_timeout_s=40)
    )
    assert store.record_outbound_reply("target", error(last, "query", "JOB_NOT_FOUND"))
    detail = store.get_outbound(first["job_id"])
    assert detail["snapshot"] == first_snapshot
    assert detail["latest_answer"]["snapshot"] == result
    assert detail["latest_answer"]["query_error"]["code"] == "JOB_NOT_FOUND"
    assert detail["query_job_id"] is None
    assert store.list_outbound(1)[0]["job_id"] == ordinary["job_id"]


def test_pending_sibling_answers_have_no_assumed_winner(store):
    session = str(uuid4())
    parent = request()
    store.save_outbound("target", wire(parent), now=100)
    store.record_outbound_reply(
        "target",
        reply(
            parent,
            "needs_input",
            session_id=session,
            question={"text": "Scope?", "expires_at": 200},
        ),
    )
    attempts = [
        request(session_id=session, answer_to=parent["job_id"], answer_text=text)
        for text in ("API", "Web")
    ]
    for attempt in attempts:
        store.save_outbound("target", wire(attempt), now=110)
    detail = store.get_outbound(parent["job_id"], now=111)
    assert set(detail["answer"]["pending_job_ids"]) == {r["job_id"] for r in attempts}
    assert "job_id" not in detail["answer"] and detail["latest_answer"] is None
    winner = attempts[0]
    store.record_outbound_reply(
        "target", reply(winner, "succeeded", session_id=session, result="done")
    )
    assert (
        store.get_outbound(parent["job_id"], now=112)["latest_answer"]["job_id"]
        == winner["job_id"]
    )
