"""
Fetch/analyze consumer-split tests (two topics, two consumers, two routes).

Mirrors test_end_to_end.py's approach: only the LLM is stubbed. The real
graph, the real routes, and the real storage layer all run, so these are a
genuine regression net for the split -- particularly for the checkpoint/state
guarantees it was required to preserve exactly (see ARCHITECTURE.md 3.11).
"""
import asyncio
import json
import time
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
import src.tools.tool_registry as tool_registry
from src.api.routes import analyze_rejection, fetch_logs, process_rejection
from src.models.schemas import MessagePayload
from src.storage.base import LOGS_FETCHED_STATUS
from src.storage.local import LocalFilesystemCasebookStorage


def _payload(event_id, reason_code="RESIDENT_MAN_DEDUP_REJECT_TD"):
    return {
        "eventId": event_id,
        "sourceTopic": "ENU.BIO.PROCESS.COMPLETION.V2",
        "flowMetaData": {"stage": "Biometric", "subStage": "MDD_POLICY_BATCH_1"},
        "packetMetaData": {"refId": "REF-1", "srn": "SRN-1", "enrolmentType": "U",
                           "pktSource": "REGISTRATION_CLIENT"},
        "packetExecutionSummary": {
            "packetStatus": "REJECTED",
            "errorData": [{"type": None, "errorReasonCode": reason_code}],
        },
    }


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Real local storage and a private checkpoint DB, isolated to a tmp root.

    Same isolation as test_end_to_end.py's fixture, extended to the two new
    get_casebook_storage() call sites the split introduced: fetch_logs_node's
    cache check (orch) and fetch_and_persist_logs' artifact write
    (tool_registry). Each holds its own module-level binding of the name, so
    each must be patched independently or it silently falls through to the
    real default storage location instead of tmp_path.
    """
    import src.api.routes as routes
    import src.core.checkpointer as checkpointer
    import src.log_pipeline.pipeline as pipeline

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(pipeline, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(tool_registry, "get_casebook_storage", lambda: store)
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "false")
    monkeypatch.setenv("RUNBOOK_MODE", "off")

    # A private checkpoint DB per test: these drive a real SqliteSaver-backed
    # graph to completion, and a shared file would let one test resume
    # another's checkpoint for the same thread_id.
    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "checkpoints.db")
    checkpointer.reset_checkpointer()

    monkeypatch.setattr(orch, "_agent", None)
    yield store
    checkpointer.reset_checkpointer()


def _stub_llm(response_text):
    """A graph whose agents all return `response_text`. See test_end_to_end.py
    for why this is a real AIMessage rather than a MagicMock."""
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content=response_text)]}
    return agent


def _run_analyze(event_id, payload=None):
    return asyncio.run(analyze_rejection(
        MessagePayload(**(payload or _payload(event_id)))
    ))


def _run_process(event_id, payload=None):
    return asyncio.run(process_rejection(
        MessagePayload(**(payload or _payload(event_id)))
    ))


# ======================================================================
# POST /fetch-logs
# ======================================================================

def test_fetch_logs_persists_artifact_and_status_and_publishes(storage, monkeypatch):
    published = []
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", published.append)

    response = fetch_logs(MessagePayload(**_payload("fl-happy")))

    assert response["status"] == "queued_for_analysis"
    assert storage.artifact_exists("fl-happy", "fetched_logs.txt")
    assert storage.load_artifact("fl-happy", "fetched_logs.txt") == "Log fetching disabled."
    status = storage.load("fl-happy", filename="status.json")
    assert status["packet_status"]["status"] == LOGS_FETCHED_STATUS
    assert len(published) == 1
    assert published[0]["eventId"] == "fl-happy"


def test_fetch_logs_is_idempotent_on_redelivery(storage, monkeypatch):
    """A redelivered original-topic message must not re-fetch, but should
    still republish -- the slow side's own dedupe is what actually prevents
    a duplicate LLM run, not this endpoint."""
    published = []
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", published.append)
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "true")
    mock_fetch = MagicMock(return_value="some logs")
    monkeypatch.setattr(tool_registry, "fetch_logs_for", mock_fetch)

    payload = MessagePayload(**_payload("fl-dupe"))
    fetch_logs(payload)
    fetch_logs(payload)

    assert mock_fetch.call_count == 1, "the second call must reuse the persisted artifact"
    assert len(published) == 2


def test_fetch_logs_skips_an_already_terminal_event(storage, monkeypatch):
    published = []
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", published.append)
    storage.save_terminal("fl-done", {
        "packet_metadata": {"eid": "fl-done"},
        "packet_status": {"status": "COMPLETED"},
        "resolution": {"synthesis": "already resolved"},
    })

    response = fetch_logs(MessagePayload(**_payload("fl-done")))

    assert response["status"] == "already_processed"
    assert not storage.artifact_exists("fl-done", "fetched_logs.txt")
    assert not published


def test_fetch_logs_never_downgrades_an_in_progress_status(storage, monkeypatch):
    """The race this guards against: a redelivered /fetch-logs call landing
    after /analyze-rejection has already started must not hide the
    IN_PROGRESS marker _investigate_packet's own dedupe guard depends on --
    or a second concurrent invocation of the same thread_id could slip
    through (see ARCHITECTURE.md 3.4's Idempotency bullet)."""
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", lambda payload: None)
    storage.save("fl-race", {
        "packet_metadata": {"eid": "fl-race", "started_at": 12345.0},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    fetch_logs(MessagePayload(**_payload("fl-race")))

    status = storage.load("fl-race", filename="status.json")
    assert status["packet_status"]["status"] == "IN_PROGRESS"


# ======================================================================
# fetch_logs_node cache behaviour
# ======================================================================

def test_fetch_logs_node_uses_the_cached_artifact_end_to_end(storage, monkeypatch):
    """The normal path once /fetch-logs has run first: no live fetch at all."""
    storage.save_artifact("cache-hit", "fetched_logs.txt", "--- cached trace ---")
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "true")  # would fetch live if the cache check failed
    mock_fetch = MagicMock(side_effect=AssertionError(
        "fetch_logs_for must not run -- fetched_logs.txt was already cached"
    ))
    monkeypatch.setattr(tool_registry, "fetch_logs_for", mock_fetch)

    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })
    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        response = _run_analyze("cache-hit")

    assert response["status"] == "processed"
    assert mock_fetch.call_count == 0


def test_fetch_logs_node_falls_back_to_a_live_fetch_when_uncached(storage, monkeypatch):
    """No /fetch-logs run happened first (a direct /process-rejection call,
    local_run.py, or any other caller that invokes the graph without going
    through the fast consumer). The graph must still complete via a live
    fetch, exactly as before the split -- and cache the result for next time."""
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "true")
    mock_fetch = MagicMock(return_value="--- live trace ---")
    monkeypatch.setattr(tool_registry, "fetch_logs_for", mock_fetch)

    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })
    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        response = _run_process("cache-miss")

    assert response["status"] == "processed"
    assert mock_fetch.call_count == 1
    assert storage.load_artifact("cache-miss", "fetched_logs.txt") == "--- live trace ---"


# ======================================================================
# POST /analyze-rejection
# ======================================================================

def test_analyze_rejection_produces_the_same_casebook_shape(storage, monkeypatch):
    """/analyze-rejection, reading logs pre-fetched by /fetch-logs, must
    produce the exact same casebook shape /process-rejection does."""
    storage.save_artifact("an-happy", "fetched_logs.txt", "--- pre-fetched trace ---")

    synthesis = json.dumps({
        "rejection_description": "manual dedup rejected the packet",
        "synthesis": "ask the resident to resubmit",
        "action": "RESIDENT_PACKET_RESUBMIT",
        "resident_action": "NEW_PACKET",
        "confidence": 0.9,
    })
    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        response = _run_analyze("an-happy")

    assert response["status"] == "processed"
    casebook = storage.load("an-happy")
    assert casebook["resolution"]["action"] == "RESIDENT_PACKET_RESUBMIT"
    assert casebook["resolution"]["resident_action"] == "NEW_PACKET"
    assert casebook["schema_version"]
    assert storage.terminal_status("an-happy") == casebook["packet_status"]["status"]


def test_analyze_rejection_runbook_hit_still_costs_zero_llm_calls(storage, monkeypatch):
    """The runbook short-circuit (after fetch_logs_node, per ARCHITECTURE.md
    3.11) is untouched by the split."""
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    storage.save_artifact("an-runbook", "fetched_logs.txt", "irrelevant -- never read on a runbook hit")

    runbook = {
        "runbook_id": "RC__U",
        "version": 3,
        "rule_fingerprint": "sha256:abc",
        "resolution": {
            "rejection_description": "known cause",
            "synthesis": "known fix",
            "action": "REPLAY",
            "resident_action": "NEW_PACKET",
        },
    }
    stub = _stub_llm("should never be called")
    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: stub), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", return_value=runbook), \
         patch.object(orch, "lookup_rule_for", return_value=None):
        response = _run_analyze("an-runbook")

    assert response["status"] == "processed"
    casebook = storage.load("an-runbook")
    assert casebook["resolution"]["source"] == "runbook:RC__U@v3"
    assert stub.invoke.call_count == 0, "a runbook hit must cost zero LLM calls"


def test_a_terminal_packet_reached_via_analyze_rejection_is_not_reprocessed(storage, monkeypatch):
    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })
    stub = _stub_llm(synthesis)

    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: stub), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        _run_analyze("ar-idempotent")
        calls_after_first = stub.invoke.call_count
        second = _run_analyze("ar-idempotent")

    assert second["status"] == "already_processed"
    assert stub.invoke.call_count == calls_after_first, "no LLM calls on replay"


# ======================================================================
# Checkpoint/state parity between /process-rejection and /analyze-rejection
# ======================================================================

def test_process_rejection_and_analyze_rejection_share_investigate_packet(storage):
    """Both routes must delegate to the identical investigation logic --
    including its checkpoint/thread_id handling -- rather than two copies
    that could drift apart from each other."""
    import src.api.routes as routes

    calls = []

    async def fake_investigate(signal, outcome):
        calls.append(signal.eventId)
        outcome["status"] = "processed"
        return {"status": "processed", "event_id": signal.eventId}

    with patch.object(routes, "_investigate_packet", fake_investigate):
        asyncio.run(routes.process_rejection(MessagePayload(**_payload("shared-a"))))
        asyncio.run(routes.analyze_rejection(MessagePayload(**_payload("shared-b"))))

    assert calls == ["shared-a", "shared-b"]


def test_analyze_rejection_short_circuits_when_in_progress_and_not_stale(storage):
    """Mirrors test_phase0_fixes.py's has_active_checkpoint=False case (0.6),
    run through /analyze-rejection instead of /process-rejection -- the split
    must not disturb this guard."""
    import src.api.routes as routes

    storage.save("ar-inprogress", {
        "packet_metadata": {"eid": "ar-inprogress", "started_at": time.time()},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    mock_agent = MagicMock()
    mock_state = MagicMock()
    mock_state.next = None  # no active checkpoint
    mock_agent.get_state.return_value = mock_state

    with patch.object(routes, "get_agent", return_value=mock_agent):
        response = _run_analyze("ar-inprogress")

    assert response["status"] == "already_processing"
    mock_agent.invoke.assert_not_called()


def test_analyze_rejection_declines_to_double_invoke_with_an_active_checkpoint(storage):
    """An active checkpoint means another invocation is already resuming this
    thread_id -- /analyze-rejection must bail out rather than invoke the
    agent concurrently against the same checkpoint."""
    import src.api.routes as routes

    storage.save("ar-resume", {
        "packet_metadata": {"eid": "ar-resume", "started_at": time.time()},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    mock_agent = MagicMock()
    mock_state = MagicMock()
    mock_state.next = ("investigate",)  # active checkpoint
    mock_agent.get_state.return_value = mock_state

    with patch.object(routes, "get_agent", return_value=mock_agent):
        response = _run_analyze("ar-resume")

    assert response["status"] == "already_processing_resumed"
    mock_agent.invoke.assert_not_called()
