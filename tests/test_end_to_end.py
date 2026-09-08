"""
End-to-end contract test (AUDIT_2026_08.md G22a, N4).

Every one of the other 480-odd tests is a unit test over mocks. None of them
drove a packet through POST /process-rejection and asserted the resulting
casebook -- which is exactly why F2 survived: `runbook_lookup_node` raised
TypeError and sent every runbook-matching packet to the DLQ while the whole
unit suite stayed green.

So these tests stub only the LLM. The real graph, the real routing, the real
contract validation, and the real storage layer all run.

`test_a_raising_runbook_node_is_caught` is the specific regression net for F2.
"""
import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
from src.api.routes import process_rejection
from src.models.schemas import MessagePayload
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
    """Real local storage and a private checkpoint DB, isolated to a tmp root."""
    import src.api.routes as routes
    import src.core.checkpointer as checkpointer
    import src.log_pipeline.pipeline as pipeline
    import src.tools.tool_registry as tool_registry

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(pipeline, "get_casebook_storage", lambda: store)
    # fetch_logs_node's cache check (orch) and fetch_and_persist_logs'
    # artifact write (tool_registry) each hold their own module-level
    # binding of get_casebook_storage -- both must be patched too, or the
    # fetch/analyze split's cache check and artifact persistence silently
    # fall through to the real default storage location instead of tmp_path.
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
    """A graph whose agents all return `response_text`.

    Patched at create_react_agent so the graph, its edges, the retry loop and
    the contract validation are all genuinely exercised.

    Returns a real AIMessage rather than a MagicMock: synthesis_node puts the
    message list into graph state, and the checkpointer serialises state --
    a MagicMock there fails msgpack encoding and DLQs the packet, which would
    make these tests pass or fail for reasons unrelated to what they assert.
    """
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content=response_text)]}
    return agent


def _run(event_id, payload=None):
    return asyncio.run(process_rejection(
        MessagePayload(**(payload or _payload(event_id)))
    ))


# ======================================================================
# The happy path
# ======================================================================

def test_a_packet_produces_a_valid_casebook(storage, monkeypatch):
    """The shape every downstream consumer depends on."""
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
        # The Reviewer must approve, so every agent returning the synthesis
        # JSON would loop. Approve explicitly by short-circuiting the check.
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        response = _run("e2e-happy")

    assert response["status"] == "processed"

    casebook = storage.load("e2e-happy")
    assert casebook["resolution"]["action"] == "RESIDENT_PACKET_RESUBMIT"
    assert casebook["resolution"]["resident_action"] == "NEW_PACKET"
    assert casebook["resolution"]["confidence"] == 0.9
    assert casebook["packet_status"]["rejection_data"]["rejection_code"] == \
        "RESIDENT_MAN_DEDUP_REJECT_TD"
    assert casebook["packet_metadata"]["ref_id"] == "REF-1"
    assert casebook["schema_version"]

    # status.json moves with it, so a redelivery sees the same verdict (F4).
    assert storage.terminal_status("e2e-happy") == casebook["packet_status"]["status"]


def test_prompt_fingerprint_is_recorded(storage, monkeypatch):
    """Without it, an accuracy movement cannot be attributed to a prompt
    change rather than merely correlated with one (G23)."""
    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })

    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        _run("e2e-fingerprint")

    fingerprint = storage.load("e2e-fingerprint")["resolution"]["provenance"]["prompt_fingerprint"]
    assert fingerprint.startswith("sha256:")
    assert fingerprint == orch.prompt_fingerprint()


# ======================================================================
# The contract
# ======================================================================

def test_unrepairable_synthesis_escalates_rather_than_writing_null(storage, monkeypatch):
    """Two invalid responses in a row must not yield action: null."""
    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm("not json at all")), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        _run("e2e-garbage")

    casebook = storage.load("e2e-garbage")
    assert casebook["resolution"]["action"] == "MANUAL_REVIEW"
    assert casebook["packet_status"]["status"] == "NEEDS_MANUAL_REVIEW"


def test_reviewer_rejection_escalates_after_max_retries(storage, monkeypatch):
    """The QC loop must terminate, and terminate explicitly."""
    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })
    monkeypatch.setenv("MAX_INVESTIGATION_RETRIES", "2")

    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        # Never approve.
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: False)
        _run("e2e-escalate")

    casebook = storage.load("e2e-escalate")
    assert casebook["resolution"]["action"] == "MANUAL_REVIEW"
    assert casebook["packet_status"]["status"] == "NEEDS_MANUAL_REVIEW"
    assert "ESCALATED" in casebook["packet_status"]["rejection_data"]["rejection_description"]


# ======================================================================
# The F2 regression net
# ======================================================================

def test_a_raising_runbook_node_is_caught(storage, monkeypatch):
    """This is the test F2 needed and did not have.

    F2 was a TypeError inside runbook_lookup_node that propagated out of
    agent.invoke() and sent EVERY runbook-matching packet to the DLQ. Every
    unit test passed throughout. Here the node is forced to raise and the
    packet must still complete through the agents.
    """
    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })
    monkeypatch.setenv("RUNBOOK_MODE", "serve")

    def exploding_get_runbook(*_args, **_kwargs):
        raise TypeError("'StructuredTool' object is not callable")

    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", side_effect=exploding_get_runbook):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        response = _run("e2e-runbook-raises")

    assert response["status"] == "processed", "a runbook fault must not DLQ the packet"
    casebook = storage.load("e2e-runbook-raises")
    assert casebook["resolution"]["action"] == "REPLAY"
    assert casebook["resolution"]["source"] == "agent", "must fall through to the agents"


def test_runbook_hit_short_circuits_the_agents(storage, monkeypatch):
    """The whole point of the runbook path: no LLM call at all."""
    monkeypatch.setenv("RUNBOOK_MODE", "serve")

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
        response = _run("e2e-runbook-hit")

    assert response["status"] == "processed"
    casebook = storage.load("e2e-runbook-hit")
    assert casebook["resolution"]["source"] == "runbook:RC__U@v3"
    assert casebook["resolution"]["action"] == "REPLAY"
    assert stub.invoke.call_count == 0, "a runbook hit must cost zero LLM calls"


def test_shadow_divergence_is_persisted(storage, monkeypatch):
    """Shadow results reached only the log stream, so the promotion gate they
    exist to feed had no data (G18)."""
    monkeypatch.setenv("RUNBOOK_MODE", "shadow")

    runbook = {
        "runbook_id": "RC__U",
        "version": 1,
        "rule_fingerprint": "sha256:abc",
        "resolution": {
            "rejection_description": "runbook says whitelist",
            "synthesis": "whitelist it",
            "action": "WHITELISTING",
            "resident_action": "PENDING",
        },
    }
    synthesis = json.dumps({
        "synthesis": "agent says replay", "action": "REPLAY",
        "resident_action": "NEW_PACKET",
    })

    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", return_value=runbook), \
         patch.object(orch, "lookup_rule_for", return_value=None):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        _run("e2e-shadow")

    resolution = storage.load("e2e-shadow")["resolution"]
    assert resolution["source"] == "agent", "shadow mode must still run the agents"
    assert resolution["action"] == "REPLAY"

    shadow = resolution["shadow"]
    assert shadow["runbook_id"] == "RC__U"
    assert shadow["action"] == "WHITELISTING"
    assert shadow["agreed"] is False


def test_shadow_agreement_is_persisted(storage, monkeypatch):
    """Agreement is the signal that promotes a runbook, so it must be
    recorded just as explicitly as divergence."""
    monkeypatch.setenv("RUNBOOK_MODE", "shadow")

    runbook = {
        "runbook_id": "RC__U",
        "version": 1,
        "rule_fingerprint": "sha256:abc",
        "resolution": {
            "rejection_description": "d", "synthesis": "s",
            "action": "REPLAY", "resident_action": "NEW_PACKET",
        },
    }
    synthesis = json.dumps({
        "synthesis": "agent agrees", "action": "REPLAY",
        "resident_action": "NEW_PACKET",
    })

    with patch.object(orch, "create_react_agent",
                      side_effect=lambda *a, **k: _stub_llm(synthesis)), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()), \
         patch.object(orch, "get_runbook", return_value=runbook), \
         patch.object(orch, "lookup_rule_for", return_value=None):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        _run("e2e-shadow-agree")

    assert storage.load("e2e-shadow-agree")["resolution"]["shadow"]["agreed"] is True


# ======================================================================
# Idempotency
# ======================================================================

def test_a_terminal_packet_is_not_reprocessed(storage, monkeypatch):
    synthesis = json.dumps({
        "synthesis": "ok", "action": "REPLAY", "resident_action": "NEW_PACKET",
    })
    stub = _stub_llm(synthesis)

    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: stub), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        _run("e2e-idempotent")
        calls_after_first = stub.invoke.call_count
        second = _run("e2e-idempotent")

    assert second["status"] == "already_processed"
    assert stub.invoke.call_count == calls_after_first, "no LLM calls on replay"
