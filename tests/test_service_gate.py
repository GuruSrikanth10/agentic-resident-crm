"""
The service gate on the rejection routes (MULTI_SERVICE_PLAN.md Phase 1).

Same approach as test_fetch_analyze_split.py: the real routes, graph and
storage run, and only the LLM is stubbed. The question each test asks is what
a packet leaves behind -- a skipped packet must leave nothing, so that it can
still be analysed in full once its service is switched on.
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
from src.storage.local import LocalFilesystemCasebookStorage
from src.utils import metrics, paths
from src.utils import service_registry as sr

SYNTHESIS = json.dumps({
    "rejection_description": "rejected by the rule",
    "synthesis": "ask the resident to resubmit",
    "action": "RESIDENT_PACKET_RESUBMIT",
    "resident_action": "NEW_PACKET",
    "confidence": 0.9,
})


def _payload(event_id, stage="Biometric", code="RESIDENT_MAN_DEDUP_REJECT_TD"):
    return {
        "eventId": event_id,
        "flowMetaData": {"stage": stage, "subStage": "MDD_POLICY_BATCH_1"},
        "packetMetaData": {"refId": "REF-1", "srn": "SRN-1", "enrolmentType": "U"},
        "packetExecutionSummary": {
            "packetStatus": "REJECTED",
            "errorData": [{"type": None, "errorReasonCode": code}],
        },
    }


#: A payload no rule places: an unknown stage and an undocumented code.
def _unresolved(event_id):
    return _payload(event_id, stage="Nowhere", code="NOT_A_DOCUMENTED_CODE")


def _count(counter, **labels):
    if not metrics.METRICS_AVAILABLE:
        return 0
    return counter.labels(**labels)._value.get()


@pytest.fixture(autouse=True)
def no_reason_code_service_map(tmp_path, monkeypatch):
    """These tests place packets by their stage. The shipped reason-code
    service map would place them by their code first; it has its own tests
    (test_reason_code_service_map.py)."""
    monkeypatch.setattr(paths, "REASON_CODE_SERVICE_MAP_FILE", tmp_path / "no-map.json")


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Real local storage and a private checkpoint DB, isolated to tmp_path.
    The same patching as test_fetch_analyze_split.py's fixture, for the same
    reason: each module holds its own binding of get_casebook_storage."""
    import src.api.routes as routes
    import src.core.checkpointer as checkpointer
    import src.log_pipeline.pipeline as pipeline

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "store"))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(pipeline, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(tool_registry, "get_casebook_storage", lambda: store)
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "false")
    monkeypatch.setenv("RUNBOOK_MODE", "off")

    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "checkpoints.db")
    checkpointer.reset_checkpointer()

    monkeypatch.setattr(orch, "_agent", None)
    yield store
    checkpointer.reset_checkpointer()


@pytest.fixture
def published(monkeypatch):
    sent = []
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", sent.append)
    return sent


@pytest.fixture
def enforce(monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")


@pytest.fixture
def stub_agents(monkeypatch):
    """Every agent answers SYNTHESIS, and the Reviewer approves."""
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content=SYNTHESIS)]}
    monkeypatch.setattr(orch, "is_reviewer_approved", lambda _feedback: True)
    with patch.object(orch, "build_agent", side_effect=lambda *a, **k: agent), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        yield agent


def _left_behind(store, event_id) -> dict:
    return {
        "fetched_logs": store.artifact_exists(event_id, "fetched_logs.txt"),
        "resolution": store.artifact_exists(event_id, sr.RESOLUTION_ARTIFACT),
        "status": store.load(event_id, filename="status.json") is not None,
        "casebook": store.load(event_id, filename="casebook.json") is not None,
    }


NOTHING = {"fetched_logs": False, "resolution": False, "status": False,
           "casebook": False}


# ======================================================================
# POST /fetch-logs
# ======================================================================

def test_record_mode_skips_nothing_and_records_the_resolution(storage, published):
    before = _count(metrics.REJECTIONS_SKIPPED, service=sr.UNRESOLVED,
                    reason=sr.SKIP_UNRESOLVED)

    response = fetch_logs(MessagePayload(**_unresolved("gate-record")))

    assert response["status"] == "queued_for_analysis"
    assert len(published) == 1
    stored = json.loads(storage.load_artifact("gate-record", sr.RESOLUTION_ARTIFACT))
    assert stored["service"] == sr.UNRESOLVED
    assert _count(metrics.REJECTIONS_SKIPPED, service=sr.UNRESOLVED,
                  reason=sr.SKIP_UNRESOLVED) == before


def test_enforce_mode_skips_an_unresolved_packet_and_leaves_nothing(
        storage, published, enforce, monkeypatch):
    monkeypatch.setattr("src.api.routes.fetch_and_persist_logs",
                        MagicMock(side_effect=AssertionError("must not fetch")))
    before = _count(metrics.REJECTIONS_SKIPPED, service=sr.UNRESOLVED,
                    reason=sr.SKIP_UNRESOLVED)

    response = fetch_logs(MessagePayload(**_unresolved("gate-skip")))

    assert response == {"status": "skipped", "reason": sr.SKIP_UNRESOLVED,
                        "service": sr.UNRESOLVED, "event_id": "gate-skip"}
    assert _left_behind(storage, "gate-skip") == NOTHING
    assert published == []
    assert _count(metrics.REJECTIONS_SKIPPED, service=sr.UNRESOLVED,
                  reason=sr.SKIP_UNRESOLVED) == before + (1 if metrics.METRICS_AVAILABLE else 0)


def test_enforce_mode_lets_an_enabled_service_through(storage, published, enforce):
    response = fetch_logs(MessagePayload(**_payload("gate-bio")))

    assert response["status"] == "queued_for_analysis"
    assert len(published) == 1
    stored = json.loads(storage.load_artifact("gate-bio", sr.RESOLUTION_ARTIFACT))
    assert (stored["service"], stored["source"]) == ("enu-biometric", sr.SOURCE_FLOW_STAGE)


def test_enforce_mode_skips_a_registered_service_that_is_not_enabled(
        storage, published, enforce, tmp_path, monkeypatch):
    packs = tmp_path / "packs"
    for name, document in (
            ("_default", {"schema_version": 1, "service": "_default",
                          "display_name": "Default", "match": {},
                          "rule_source": {"type": "none"}}),
            ("enu-biometric", {"schema_version": 1, "service": "enu-biometric",
                               "display_name": "Biometric", "tool_prefix": "bio",
                               "match": {"stages": ["Biometric"]},
                               "rule_source": {"type": "rules_db"}}),
            ("svc-demo", {"schema_version": 1, "service": "svc-demo",
                          "display_name": "Demographic", "tool_prefix": "demo",
                          "match": {"stages": ["Demographic"]},
                          "rule_source": {"type": "none"}})):
        (packs / name).mkdir(parents=True)
        (packs / name / "service.json").write_text(json.dumps(document), encoding="utf-8")
        (packs / name / "policy.md").write_text("A policy.", encoding="utf-8")
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", packs)

    response = fetch_logs(MessagePayload(**_payload("gate-demo", stage="Demographic")))

    assert response["status"] == "skipped"
    assert (response["reason"], response["service"]) == (sr.SKIP_NOT_ENABLED, "svc-demo")
    assert published == []

    monkeypatch.setenv(sr.ENV_ENABLED, "enu-biometric,svc-demo")
    assert fetch_logs(MessagePayload(**_payload("gate-demo", stage="Demographic"))
                      )["status"] == "queued_for_analysis"


def test_the_default_pack_policy_lets_an_unresolved_packet_through(
        storage, published, enforce, monkeypatch):
    monkeypatch.setenv(sr.ENV_UNRESOLVED, "default_pack")

    assert fetch_logs(MessagePayload(**_unresolved("gate-default")))["status"] \
        == "queued_for_analysis"


def test_a_packet_already_finished_is_not_resolved_again(storage, published, enforce):
    storage.save_terminal("gate-done", {
        "packet_metadata": {"eid": "gate-done"},
        "packet_status": {"status": "COMPLETED"},
        "resolution": {"synthesis": "done"},
    })

    assert fetch_logs(MessagePayload(**_unresolved("gate-done")))["status"] \
        == "already_processed"
    assert not storage.artifact_exists("gate-done", sr.RESOLUTION_ARTIFACT)


def test_a_resolution_is_counted_once_across_both_stages(storage, published, stub_agents):
    labels = dict(service="enu-biometric", source=sr.SOURCE_FLOW_STAGE, conflict="false")
    before = _count(metrics.SERVICE_RESOLUTIONS, **labels)

    fetch_logs(MessagePayload(**_payload("gate-once")))
    asyncio.run(analyze_rejection(MessagePayload(**_payload("gate-once"))))

    expected = before + (1 if metrics.METRICS_AVAILABLE else 0)
    assert _count(metrics.SERVICE_RESOLUTIONS, **labels) == expected


# ======================================================================
# POST /analyze-rejection and /process-rejection
# ======================================================================

def test_analysis_skips_before_the_agent_the_claim_or_the_stub(
        storage, enforce, monkeypatch):
    monkeypatch.setattr("src.api.routes.get_agent",
                        MagicMock(side_effect=AssertionError("must not build the graph")))
    before = _count(metrics.PACKETS_TOTAL, status="skipped",
                    resolution_source="agent", service=sr.UNRESOLVED)

    response = asyncio.run(analyze_rejection(MessagePayload(**_unresolved("gate-an-skip"))))

    assert response["status"] == "skipped"
    assert response["reason"] == sr.SKIP_UNRESOLVED
    assert _left_behind(storage, "gate-an-skip") == NOTHING
    assert _count(metrics.PACKETS_TOTAL, status="skipped", resolution_source="agent",
                  service=sr.UNRESOLVED) == before + (1 if metrics.METRICS_AVAILABLE else 0)


def test_the_casebook_records_the_service_and_how_it_was_resolved(storage, stub_agents):
    response = asyncio.run(process_rejection(MessagePayload(**_payload("gate-cb"))))

    assert response["status"] == "processed"
    metadata = storage.load("gate-cb")["packet_metadata"]
    assert metadata["service"] == "enu-biometric"
    assert metadata["service_resolution"]["source"] == sr.SOURCE_FLOW_STAGE
    assert metadata["service_resolution"]["matched"] == "Biometric"
    assert metadata["service_resolution"] == json.loads(
        storage.load_artifact("gate-cb", sr.RESOLUTION_ARTIFACT)), \
        "the direct path has no fetch stage, so it stores its own resolution"


def test_analysis_acts_on_the_resolution_the_fetch_stage_stored(
        storage, stub_agents, enforce):
    """A stored resolution wins, even over evidence that would now resolve
    differently -- the two stages must act on one answer."""
    stored = sr.resolve(_payload("gate-stored")).as_dict()
    storage.save_artifact("gate-stored", sr.RESOLUTION_ARTIFACT, json.dumps(stored))

    response = asyncio.run(analyze_rejection(MessagePayload(**_unresolved("gate-stored"))))

    assert response["status"] == "processed"
    assert storage.load("gate-stored")["packet_metadata"]["service_resolution"] == stored


def test_the_graph_receives_the_resolution_with_the_payload(storage, stub_agents):
    captured = {}
    original = orch._service_update

    def spy(state, event_id):
        captured.update(state)
        return original(state, event_id)

    with patch.object(orch, "_service_update", side_effect=spy):
        asyncio.run(process_rejection(MessagePayload(**_payload("gate-state"))))

    assert captured["service"] == "enu-biometric"
    assert captured["service_resolution"]["service"] == "enu-biometric"


def test_a_graph_invoked_without_a_resolution_resolves_one(storage, stub_agents):
    """Direct invocations -- tests, tools -- still end with the service in
    state, which is what the later phases read."""
    graph = orch.get_agent()
    result = graph.invoke({"payload": _payload("gate-direct"), "retry_count": 0},
                          config={"configurable": {"thread_id": "gate-direct"}})

    assert result["service"] == "enu-biometric"
    assert result["service_resolution"]["source"] == sr.SOURCE_FLOW_STAGE


def test_a_checkpoint_written_without_the_service_resumes(storage, stub_agents,
                                                           monkeypatch):
    """A packet in flight across the deploy: its checkpoint predates the
    service keys. It resumes mid-graph, and the casebook still records the
    service the gate resolved."""
    event_id = "gate-resume"
    config = {"configurable": {"thread_id": event_id}}
    graph = orch.get_agent()
    graph.update_state(config, {"payload": _payload(event_id), "logs": "trace",
                                "logs_artifact": "fetched_logs.txt",
                                "resolution_source": "agent", "retry_count": 0},
                       as_node="runbook_lookup")
    checkpoint = graph.get_state(config)
    assert checkpoint.next == ("investigate",)
    assert "service" not in checkpoint.values

    monkeypatch.setenv("MAX_IN_PROGRESS_AGE_SECONDS", "1800")
    storage.save(event_id, {
        "packet_metadata": {"eid": event_id, "started_at": time.time() - 7200},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    response = asyncio.run(analyze_rejection(MessagePayload(**_payload(event_id))))

    assert response["status"] == "processed"
    assert storage.load(event_id)["packet_metadata"]["service"] == "enu-biometric"


def test_a_resolution_passed_in_is_not_replaced(storage, stub_agents):
    given = {"service": "enu-biometric", "source": sr.SOURCE_FLOW_STAGE,
             "matched": "Biometric", "conflict": None, "detail": {},
             "registry_sha256": "sha256:given"}
    graph = orch.get_agent()
    result = graph.invoke({"payload": _unresolved("gate-given"), "retry_count": 0,
                           "service": "enu-biometric", "service_resolution": given},
                          config={"configurable": {"thread_id": "gate-given"}})

    assert result["service_resolution"] == given
