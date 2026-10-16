"""The DLT lane fixes of 2026-10-06, from an end-to-end review of the lane.

Each block pins one defect the review found:

  cache          a finding reasoned from a CONTRADICTED or PARTIAL verdict
                 cached as the group's answer, and served -- discrepancy,
                 action and all -- to occurrences whose logs agreed
  corroboration  Spring's own wrapper exceptions read as unexplained: PARTIAL,
                 and an LLM call, on every occurrence
  cancellation   a timed-out DLT run keeping its worker to the end of the graph
  budget         the server-side budget counted from the LLM call only
  review         the Reviewer's verdict unrecorded, an unapproved finding
                 uncapped, and a Reviewer prompt with no output contract
  redaction      raw exception messages in what the LLM reads
  code check     a backoff timestamp still in the future used as "failed at"
  replay         a failed replay recorded as queued; the "no refId" guard
                 unreachable; parking replaying a storage key
  consumer       one dead-letter topic only; a rejection-shaped timeout record
  resolution     the payload's generic code shadowing the trace's
  dlq            publish_to_dlq never raising, so a lost DLQ write lost the message
"""
import asyncio
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

from src.api import dlt_routes
from src.api.dlt_routes import FETCHED_LOGS_ARTIFACT, analyze_dlt
from src.core import run_control
from src.core.run_control import RunCancelled
from src.dlt import (auto_replay, case_storage, code_check, groups, parked,
                     registry, reuse)
from src.dlt.corroborate import Corroboration, Verdict, corroborate
from src.dlt.headers import decode_epoch_ms
from src.dlt.identity import storage_key
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy
from src.storage import factory

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]

CORROBORATING = ("[ERROR] in.gov.uidai.common.exception.BusinessException: "
                 "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")
CONTRADICTING = "[ERROR] java.net.SocketTimeoutException: Read timed out"

GOOD = DltFinding(
    narrative="The origin-tracker lookup treats an absent row as fatal.",
    recommendation="Query the origin-tracker table for each matched candidate.",
    action="DATA_FIX_REQUIRED", confidence=0.85)

MISCAST = DltFinding(
    narrative="The logs show a datasource timeout, not a missing row.",
    discrepancy="The declared BusinessException is not supported by the logs.",
    recommendation="Redrive once the datasource recovers.",
    action="REDRIVE_AFTER_RECOVERY", confidence=0.9)

ENV = ("CASEBOOK_STORAGE_BACKEND", "DLT_GROUP_STORE", "DLT_CLAIM_ENABLED",
       "DLT_ANALYSIS_TIMEOUT_SECONDS", "DLT_ANALYZE_TIMEOUT_SECONDS",
       "DLT_SINGLEFLIGHT_ENABLED", "DLT_SINGLEFLIGHT_WAIT_SECONDS",
       "DLT_REUSE_ENABLED", "DLT_PER_CODE_WITHHOLD", "DLT_AUTO_REPLAY_ENABLED",
       "DLT_REPLAY_CONFIDENCE_THRESHOLD", "DLT_REPLAY_ALLOW_UNVERIFIED",
       "DLT_CODE_CHECK_ENABLED", "DLT_CODE_CHECK_GATES_REPLAY",
       "DLT_CODE_CHECK_PARK_ENABLED", "DLT_PARKED_REPLAY_TTL_SECONDS",
       "DLT_UNREVIEWED_CEILING", "DLT_MAX_INVESTIGATION_RETRIES",
       "ENABLE_AUTO_REPLAY")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ENV:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    monkeypatch.setattr(dlt_routes, "publish_to_dlt_analysis_queue", lambda m: True)
    # Offline and instant: the real call probes for a Kubernetes config.
    snapshot = SimpleNamespace(ok=False, mixed=False, versions=(),
                               as_dict=lambda: {"ok": False, "versions": []})
    monkeypatch.setattr(dlt_routes.deployed, "running_version", lambda: snapshot)
    factory.reset_storage_cache()
    case_storage.reset_cache()
    registry.clear_cache()
    yield
    factory.reset_storage_cache()
    case_storage.reset_cache()


def message(case_id="dlt-T-63-1", ref_id="REF-1", headers=None):
    return DltMessage(case_id=case_id,
                      headers=headers if headers is not None else dict(REFERENCE),
                      payload={"packetMetaData": {"refId": ref_id}}, ref_id=ref_id)


def seed_logs(msg: DltMessage, text: str) -> None:
    case_storage.get_dlt_storage().save_artifact(
        storage_key(msg.ref_id, msg.case_id), FETCHED_LOGS_ARTIFACT, text)


def stored(msg: DltMessage) -> dict:
    return case_storage.get_dlt_storage().load(storage_key(msg.ref_id, msg.case_id))


def analyse(msg: DltMessage) -> dict:
    return asyncio.run(analyze_dlt(msg))


def stub_llm(monkeypatch, finding: DltFinding, captured: list = None) -> list:
    calls = []

    def fake(key, failure, corroboration, logs, payload_summary=None, **_service):
        calls.append(key)
        if captured is not None:
            captured.append({"failure": failure, "payload_summary": payload_summary})
        return finding, None

    monkeypatch.setattr(dlt_routes.orchestrator, "investigate", fake)
    return calls


def fingerprint() -> str:
    headers = dlt_routes.parse_headers(REFERENCE)
    return dlt_routes.build_failure(headers, headers.exception_message)["fingerprint"]


# ======================================================================
# Cache -- a discrepancy explains one occurrence, not the failure mode
# ======================================================================

def test_a_discrepancy_finding_is_not_cached_over_the_failure_modes_answer(monkeypatch):
    first, second, third = (message(case_id=f"dlt-T-63-{n}", ref_id=f"REF-C{n}")
                            for n in range(3))

    stub_llm(monkeypatch, GOOD)
    seed_logs(first, CORROBORATING)
    assert analyse(first)["decision"] == "LLM_REQUIRED"

    stub_llm(monkeypatch, MISCAST)
    seed_logs(second, CONTRADICTING)
    result = analyse(second)
    assert (result["corroboration"], result["decision"]) == ("CONTRADICTED", "LLM_REQUIRED")
    casebook = stored(second)
    assert casebook["finding"]["discrepancy"], "it still leads its own casebook"
    assert casebook["provenance"]["recommendation_state"] == groups.STATE_PER_OCCURRENCE

    group = groups.load_group(fingerprint())
    assert group["recommendation"]["narrative"] == GOOD.narrative
    assert group["recommendation_corroboration"] == "CORROBORATED"

    calls = stub_llm(monkeypatch, MISCAST)
    seed_logs(third, CORROBORATING)
    result = analyse(third)
    assert result["decision"] == "REUSE_GROUP" and calls == []
    assert result["action"] == "DATA_FIX_REQUIRED"
    assert stored(third)["finding"]["discrepancy"] is None


def test_a_corroborated_occurrence_is_never_auto_replayed_on_a_mis_cast(monkeypatch):
    """The replay consequence: the mis-cast's REDRIVE_AFTER_RECOVERY, reused
    at 0.6 x 0.95, cleared the 0.55 threshold for a packet whose logs agreed
    with its trace."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    attempts = []
    # The request `attempt` really sends: the casebook contract checks it.
    monkeypatch.setattr(dlt_routes.auto_replay, "attempt",
                        lambda case_key, ref_id: attempts.append(ref_id) or
                        {"queued": True, "result": "ok", "args": {
                            "id": ref_id, "idType": "REFID", "priority": 5,
                            "operatorName": "dlt-analysis-auto",
                            "category": "DLT_REDRIVE", "fromSedaStart": False}})
    first, second, third = (message(case_id=f"dlt-T-64-{n}", ref_id=f"REF-R{n}")
                            for n in range(3))

    stub_llm(monkeypatch, GOOD)
    seed_logs(first, CORROBORATING)
    analyse(first)
    stub_llm(monkeypatch, MISCAST)
    seed_logs(second, CONTRADICTING)
    analyse(second)
    seed_logs(third, CORROBORATING)
    result = analyse(third)

    assert result["replay_attempted"] is False
    assert "REF-R2" not in attempts


def _poisoned_group(recommendation: dict, corroboration=None) -> dict:
    fp = "e" * 64
    groups.record_occurrence(fp, "dlt-T-0-1", failure_class="A",
                             corroboration="CORROBORATED")
    groups.attach_recommendation(fp, recommendation, state=groups.STATE_DRAFT,
                                 corroboration=corroboration)
    return groups.load_group(fp)


@pytest.mark.parametrize("recommendation,corroboration", [
    # Cached before the verdict was stored: known by its discrepancy...
    (MISCAST.model_dump(), None),
    # ...or by the ceiling a CONTRADICTED verdict leaves on it.
    ({**GOOD.model_dump(), "ceilings_applied": ["contradicted"]}, None),
    # Cached with its verdict: known by that alone.
    (GOOD.model_dump(), "PARTIAL"),
])
def test_a_recommendation_cached_from_a_discrepancy_is_not_served(recommendation,
                                                                  corroboration):
    """Such a recommendation, already cached before the fix, is
    re-investigated rather than served, and the next clean finding replaces it."""
    group = _poisoned_group(recommendation, corroboration)

    assert groups.has_usable_recommendation(group) is False
    decision = reuse.decide("A", "CORROBORATED", group)
    assert decision.decision is reuse.Decision.LLM_REQUIRED
    assert decision.awaits_cache is True


def test_a_clean_recommendation_is_still_served():
    group = _poisoned_group(GOOD.model_dump(), "CORROBORATED")
    assert groups.has_usable_recommendation(group) is True
    assert reuse.decide("A", "UNVERIFIABLE", group).decision is reuse.Decision.REUSE_GROUP


@pytest.mark.parametrize("verdict,kept", [
    ("CORROBORATED", False), ("UNVERIFIABLE", False),
    ("CONTRADICTED", True), ("PARTIAL", True),
])
def test_a_discrepancy_survives_only_a_verdict_that_disagreed(verdict, kept):
    finding = apply_dlt_confidence_policy(
        MISCAST, failure_class="A", corroboration=verdict, registry_hit=True,
        reused=True)
    assert bool(finding.discrepancy) is kept


# ======================================================================
# Corroboration -- the trace explains its own wrappers
# ======================================================================

WRAPPED = ("[ERROR] org.springframework.kafka.listener.ListenerExecutionFailedException: "
           "Listener failed; in.gov.uidai.common.exception.BusinessException: "
           "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")
BUSINESS = "in.gov.uidai.common.exception.BusinessException"
WRAPPER = "org.springframework.kafka.listener.ListenerExecutionFailedException"


def test_a_wrapper_in_the_declared_chain_does_not_make_a_corroboration_partial():
    result = corroborate(WRAPPED, BUSINESS, "UID_ORIGIN_TRACKER_DATA_NOT_FOUND",
                         chain=[WRAPPER, BUSINESS])

    assert result.verdict is Verdict.CORROBORATED
    assert result.unexplained == ()
    assert result.details["exceptions_in_declared_chain"] == [WRAPPER]


def test_the_analysis_passes_the_chain(monkeypatch):
    stub_llm(monkeypatch, GOOD)
    msg = message()
    seed_logs(msg, WRAPPED)

    assert analyse(msg)["corroboration"] == "CORROBORATED"


def test_a_same_named_exception_from_another_package_is_still_unexplained():
    """Chain members are matched by full name: a Kafka client TimeoutException
    is not the JDK one the trace declares."""
    logs = ("[ERROR] org.apache.kafka.common.errors.TimeoutException: expired; "
            f"{BUSINESS}: [UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")
    result = corroborate(logs, BUSINESS, "UID_ORIGIN_TRACKER_DATA_NOT_FOUND",
                         chain=["java.util.concurrent.TimeoutException", BUSINESS])

    assert result.verdict is Verdict.PARTIAL
    assert result.unexplained == ("org.apache.kafka.common.errors.TimeoutException",)


# ======================================================================
# Cancellation and the budget
# ======================================================================

def test_a_timed_out_run_is_cancelled_and_its_end_is_logged(monkeypatch):
    monkeypatch.setenv("DLT_ANALYZE_TIMEOUT_SECONDS", "0.5")
    seen, endings, ended = {}, [], threading.Event()

    def slow(*args, **kwargs):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if run_control.cancelled():
                seen["cancelled"] = True
                break
            time.sleep(0.01)
        run_control.check()
        return GOOD, None

    def recording(key, run):
        endings.append(run.exception())
        ended.set()

    monkeypatch.setattr(dlt_routes.orchestrator, "investigate", slow)
    monkeypatch.setattr(dlt_routes, "_log_abandoned_run", recording)
    msg = message(case_id="dlt-T-65-1", ref_id="REF-T1")

    result = analyse(msg)

    assert result["status"] == "failed_timeout"
    assert ended.wait(5)
    assert seen.get("cancelled") is True
    assert isinstance(endings[0], RunCancelled)
    casebook = stored(msg)
    assert casebook["packet_status"]["status"] == "FAILED_TIMEOUT"
    assert (casebook["case_id"], casebook["packet"]["ref_id"]) == (msg.case_id, "REF-T1")
    assert casebook["schema_version"] == dlt_routes.DLT_CASEBOOK_SCHEMA_VERSION


def test_the_graph_starts_no_node_once_its_run_is_abandoned(monkeypatch):
    graph = _stub_graph(monkeypatch, reviewer="APPROVED")
    cancel = threading.Event()
    cancel.set()

    for node in ("investigate", "review", "synthesise"):
        with pytest.raises(RunCancelled):
            run_control.run_in_scope(
                cancel, lambda: graph.builder.nodes[node].runnable.func({}))


def test_time_before_the_llm_counts_against_the_budget(monkeypatch):
    """The consumer's clock starts when it sends the request, not when the
    LLM is called; the server's budget now does too."""
    monkeypatch.setenv("DLT_ANALYZE_TIMEOUT_SECONDS", "0.3")
    calls = stub_llm(monkeypatch, GOOD)
    snapshot = SimpleNamespace(ok=False, mixed=False, versions=())

    def slow_cluster(service):
        time.sleep(0.5)
        return snapshot

    monkeypatch.setattr(dlt_routes.deployed, "for_service", slow_cluster)

    assert analyse(message())["status"] == "failed_timeout"
    assert calls == [], "an investigation with no budget left is not started"


# ======================================================================
# Review -- recorded, capped, and asked for in a parseable form
# ======================================================================

class _Stub:
    def __init__(self, reply):
        self.reply = reply
        self.prompts = []

    def invoke(self, request):
        self.prompts.append(request["messages"][-1].content)
        return {"messages": [AIMessage(content=self.reply)]}


def _stub_graph(monkeypatch, reviewer, synthesis=None):
    from src.dlt import orchestrator

    stubs = {
        "dlt_investigator": _Stub("The chain declares a missing row."),
        "dlt_reviewer": _Stub(reviewer),
        # The model's own claim to have been reviewed is overwritten.
        "dlt_synthesis": _Stub(synthesis or json.dumps({
            "narrative": "n", "recommendation": "r", "action": "DATA_FIX_REQUIRED",
            "confidence": 0.8, "reviewer_approved": True})),
    }
    monkeypatch.setattr(orchestrator, "_agent", None)
    monkeypatch.setattr(orchestrator, "_agent_catalog", None)
    monkeypatch.setattr(orchestrator, "get_llm", lambda tier: object())
    monkeypatch.setattr(orchestrator, "build_agent",
                        lambda role, llm, prompt, **kwargs: stubs[role])
    graph = orchestrator.get_dlt_agent()
    graph.stubs = stubs
    return graph


def _investigate(monkeypatch, reviewer):
    from src.dlt import orchestrator

    graph = _stub_graph(monkeypatch, reviewer)
    finding, error = orchestrator.investigate(
        "REF-1__abc", {"chain": [], "frames": [], "failure_class": "A"},
        Corroboration(Verdict.CORROBORATED, "the declared root appears"), "")
    return finding, error, graph.stubs


def test_a_finding_the_reviewer_never_approved_says_so(monkeypatch):
    monkeypatch.setenv("DLT_MAX_INVESTIGATION_RETRIES", "1")
    finding, error, _ = _investigate(monkeypatch, "REJECTED\nThe second claim is uncited.")

    assert error is None
    assert finding.reviewer_approved is False


def test_an_approved_finding_says_so(monkeypatch):
    finding, _, stubs = _investigate(monkeypatch, "APPROVED")

    assert finding.reviewer_approved is True
    # Synthesis is told the verdict the discrepancy and confidence turn on.
    assert "### Corroboration verdict\nCORROBORATED" in stubs["dlt_synthesis"].prompts[0]


def test_an_unreviewed_finding_is_capped_labelled_and_never_replayed(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_REPLAY_CONFIDENCE_THRESHOLD", "0.4")
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="REDRIVE_AFTER_RECOVERY",
                   confidence=0.9, reviewer_approved=False),
        failure_class="A", corroboration="CORROBORATED", registry_hit=True)

    assert finding.confidence == 0.5
    assert "unreviewed" in finding.ceilings_applied
    decision = auto_replay.decide(finding, "REF-1")
    assert decision.should_replay is False
    assert "Reviewer never approved" in decision.reason


def test_the_reviewer_prompt_leads_with_its_output_contract():
    text = Path("src/prompts/DltReviewerAgent.md").read_text(encoding="utf-8")
    opening = text[:text.index("### REJECT ANY OF THE FOLLOWING")]

    assert "THIS IS CHECKED MECHANICALLY" in opening
    assert "### WHEN TO APPROVE" in text
    assert "Do NOT reject for any of these" in text
    assert text.rstrip().endswith(
        "Remember: your first line is `APPROVED` or `REJECTED`, and nothing else.")


# ======================================================================
# Redaction -- what the LLM reads
# ======================================================================

def test_the_llm_reads_redacted_exception_messages(monkeypatch):
    captured = []
    stub_llm(monkeypatch, GOOD, captured)
    headers = dict(REFERENCE)
    headers["kafka_exception-stacktrace"] = (
        f"{BUSINESS}: [UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent for uid 123456789012\n"
        "\tat com.uidai.enu.biometric.Svc.go(Svc.java:1)\n")

    analyse(message(headers=headers))

    seen = json.dumps(captured[0]["failure"])
    assert "123456789012" not in seen
    assert "[REDACTED:AADHAAR]" in captured[0]["failure"]["root_message"]
    assert "UID_ORIGIN_TRACKER_DATA_NOT_FOUND" in captured[0]["failure"]["root_message"]


# ======================================================================
# Code check -- never a failure time in the future
# ======================================================================

def test_the_code_check_never_gets_a_future_failure_time(monkeypatch):
    seen = {}

    def capture(failure, failed_at_ms, baseline_versions, running_versions):
        seen["failed_at"] = failed_at_ms
        return code_check.CodeCheck()

    monkeypatch.setattr(dlt_routes.code_check, "evaluate", capture)
    stub_llm(monkeypatch, GOOD)
    now_ms = int(time.time() * 1000)
    headers = dict(REFERENCE)
    # Dead-lettered before the retry Spring had scheduled could fire.
    headers["retry_topic-backoff-timestamp"] = format(now_ms + 3_600_000, "012X")

    analyse(message(headers=headers))

    assert seen["failed_at"] <= now_ms
    assert seen["failed_at"] == decode_epoch_ms(REFERENCE["retry_topic-original-timestamp"])


# ======================================================================
# Replay and parking
# ======================================================================

def _fake_tool(reply):
    tool = MagicMock()
    tool.invoke.return_value = reply
    return patch("src.tools.tool_registry.get_tool_by_name", return_value=tool), tool


@pytest.mark.parametrize("reply,queued", [
    ("Successfully queued packet REF-1 for human review before replay.", True),
    ("Successfully auto-replayed packet REF-1: {}", True),
    ("Failed to replay packet REF-1 directly: 503 Service Unavailable", False),
    ("Failed to queue packet REF-1: the id is not a valid storage key.", False),
    ("Replay of REF-1 NOT queued: this investigation was abandoned.", False),
])
def test_a_replay_is_recorded_as_queued_only_when_it_was(reply, queued):
    patcher, _ = _fake_tool(reply)
    with patcher:
        assert auto_replay.attempt("dlt-T-1-1", "REF-1")["queued"] is queued


def test_the_tool_reports_success_in_the_form_replay_succeeded_reads(monkeypatch):
    from src.tools import tool_registry

    monkeypatch.setenv("ENABLE_AUTO_REPLAY", "false")
    args = {"idType": "REFID", "priority": 5, "operatorName": "op",
            "category": "DLT_REDRIVE", "fromSedaStart": False}

    assert tool_registry.replay_succeeded(
        tool_registry.queue_for_replay.invoke({"id": "REF-OK", **args}))
    assert not tool_registry.replay_succeeded(
        tool_registry.queue_for_replay.invoke({"id": "../escape", **args}))
    cancel = threading.Event()
    cancel.set()
    assert not tool_registry.replay_succeeded(run_control.run_in_scope(
        cancel, lambda: tool_registry.queue_for_replay.invoke({"id": "REF-C", **args})))


def test_a_case_with_no_ref_id_is_never_replayed_under_its_case_key(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    patcher, tool = _fake_tool("Successfully queued packet x.")
    redrive = DltFinding(narrative="x", recommendation="y",
                         action="REDRIVE_AFTER_RECOVERY", confidence=0.58)

    with patcher:
        result = auto_replay.maybe_replay("dlt-T-1-1-gabc", None, redrive)
        attempted = auto_replay.attempt("dlt-T-1-1-gabc", None)

    assert result["attempted"] is False and "refId" in result["reason"]
    assert attempted["queued"] is False and attempted["args"] is None
    tool.invoke.assert_not_called()


def _parking_on(monkeypatch):
    for name in ("DLT_CODE_CHECK_PARK_ENABLED", "DLT_AUTO_REPLAY_ENABLED",
                 "DLT_CODE_CHECK_GATES_REPLAY"):
        monkeypatch.setenv(name, "true")


VERDICT = code_check.CodeCheck(verdict=code_check.NOT_DEPLOYED, reason="r",
                               required_version="1.0.1")
REDRIVE = DltFinding(narrative="x", recommendation="y",
                     action="REDRIVE_AFTER_RECOVERY", confidence=0.58)


def test_a_release_sends_the_ref_id_and_a_failed_one_stays_parked(monkeypatch):
    _parking_on(monkeypatch)
    parked.maybe_park("dlt-T-1-1", "59a1a4be-4c25-47ea-87a2-85d70615c678", VERDICT, REDRIVE)

    patcher, tool = _fake_tool("Failed to replay packet x directly: 503")
    with patcher:
        summary = parked.release_ready(["1.0.1"])
    assert (summary["released"], summary["failed"]) == (0, 1)
    [entry] = parked.list_parked()
    assert entry["last_release_result"].startswith("Failed to replay")

    patcher, tool = _fake_tool("Successfully queued packet x for human review.")
    with patcher:
        summary = parked.release_ready(["1.0.1"])
    assert summary["released"] == 1
    assert tool.invoke.call_args[0][0]["id"] == "59a1a4be-4c25-47ea-87a2-85d70615c678"
    assert parked.list_parked() == []


def test_release_marks_an_entry_without_a_conditional_overwrite(monkeypatch):
    """On a store that refuses If-Match, `update_json` fails every time: the
    entry was replayed, never marked, and replayed again on every run."""
    from src.storage.local import LocalFilesystemCasebookStorage

    _parking_on(monkeypatch)
    parked.park("dlt-T-1-2", "REF-2", VERDICT, REDRIVE)
    monkeypatch.setattr(LocalFilesystemCasebookStorage, "update_json",
                        lambda *a, **k: (_ for _ in ()).throw(
                            RuntimeError("If-Match refused")))

    patcher, tool = _fake_tool("Successfully queued packet REF-2 for human review.")
    with patcher:
        parked.release_ready(["1.0.1"])
        parked.release_ready(["1.0.1"])

    assert tool.invoke.call_count == 1
    [entry] = parked.list_parked(include_finished=True)
    assert entry["status"] == parked.STATUS_RELEASED


def test_parking_a_released_case_again_does_not_queue_it_again(monkeypatch):
    _parking_on(monkeypatch)
    parked.park("dlt-T-1-3", "REF-3", VERDICT, REDRIVE)
    patcher, _ = _fake_tool("Successfully queued packet REF-3 for human review.")
    with patcher:
        parked.release_ready(["1.0.1"])

    again = parked.park("dlt-T-1-3", "REF-3", VERDICT, REDRIVE)

    assert again["parked"] is False and "already released" in again["reason"]
    assert parked.list_parked() == []


# ======================================================================
# The consumer
# ======================================================================

def test_the_dlt_consumer_subscribes_to_every_listed_topic(monkeypatch):
    import importlib

    monkeypatch.setenv("CONSUMER_ROLE", "dlt")
    monkeypatch.setenv("DLT_CONSUMER_TOPIC_NAME",
                       "ENU.A.V1-dlt, ENU.B.V1-dlt,,ENU.A.V1-dlt")
    import src.utils.kafkaConsumer as kc

    reloaded = importlib.reload(kc)
    try:
        assert reloaded.kafkaConsumerTopics == ("ENU.A.V1-dlt", "ENU.B.V1-dlt")
    finally:
        monkeypatch.delenv("CONSUMER_ROLE", raising=False)
        monkeypatch.delenv("DLT_CONSUMER_TOPIC_NAME", raising=False)
        importlib.reload(kc)


@pytest.mark.parametrize("topics,problem", [
    ("ENU.A.V1-dlt,ENU.B.V1-dlt", None),
    ("ENU.A.V1-dlt,dlt-analysis-queue", "DLT_ANALYSIS_TOPIC_NAME"),
    ("rejected-packets-dlq", "KAFKA_DLQ_TOPIC"),
    (" , ", "at least one"),
])
def test_the_dlt_consumer_reads_only_dead_letter_topics(monkeypatch, topics, problem):
    from src.utils.config_validator import _dlt_topic_errors

    for name in ("DLT_ANALYSIS_TOPIC_NAME", "PACKET_ANALYSIS_TOPIC_NAME", "KAFKA_DLQ_TOPIC"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("DLT_CONSUMER_TOPIC_NAME", topics)

    errors = _dlt_topic_errors()
    assert (errors == []) if problem is None else (problem in " ".join(errors))


# ======================================================================
# Service resolution -- the trace's code first
# ======================================================================

def test_the_traces_code_is_weighed_before_the_payloads(monkeypatch):
    from src.utils import service_registry as sr

    documented = {"TRACE_CODE": ("enu-biometric",), "UNHANDLED_EXCEPTION": ("svc-other",)}
    monkeypatch.setattr(sr, "_documented_by",
                        lambda code, registry, docs_root=None: documented.get(code, ()))
    payload = {"eventId": "E", "packetExecutionSummary": {
        "errorData": [{"errorReasonCode": "UNHANDLED_EXCEPTION"}]}}

    placed = sr.resolve_dlt(payload, business_code="TRACE_CODE")
    assert (placed.service, placed.source, placed.matched) == \
        ("enu-biometric", sr.SOURCE_REASON_CODE_DOCS, "TRACE_CODE")

    # An undocumented trace code falls back to the payload's, as before.
    fallback = sr.resolve_dlt(payload, business_code="UNDOCUMENTED")
    assert (fallback.service, fallback.matched) == ("svc-other", "UNHANDLED_EXCEPTION")


# ======================================================================
# The DLQ -- a write that did not land says so
# ======================================================================

def test_publish_to_dlq_raises_when_the_message_did_not_get_there(monkeypatch):
    from src.utils import dlq_publisher

    monkeypatch.setattr(dlq_publisher, "get_producer", lambda: None)
    with pytest.raises(RuntimeError):
        dlq_publisher.publish_to_dlq({"case_id": "dlt-T-1-1"}, "boom")

    producer = MagicMock()
    producer.send.return_value.get.side_effect = RuntimeError("NOT_ENOUGH_REPLICAS")
    monkeypatch.setattr(dlq_publisher, "get_producer", lambda: producer)
    with pytest.raises(RuntimeError):
        dlq_publisher.publish_to_dlq({"case_id": "dlt-T-1-1"}, "boom")

    producer.send.return_value.get.side_effect = None
    dlq_publisher.publish_to_dlq({"case_id": "dlt-T-1-1"}, "boom")


def test_a_dlq_write_that_fails_now_holds_the_offset(monkeypatch):
    """The consumer was written for a publisher that raises; the real one
    now does, so the message is redelivered on restart rather than lost."""
    from kafka.structs import OffsetAndMetadata

    import src.utils.kafkaConsumer as kc

    tp = "partition-0"
    monkeypatch.setattr(kc, "_offset_tracker", kc.OffsetTracker())
    kc._offset_tracker.dispatched(tp, 3)
    monkeypatch.setattr("src.utils.dlq_publisher.get_producer", lambda: None)
    monkeypatch.setattr(kc, "forward_signal_to_internal_endpoint",
                        lambda payload: (_ for _ in ()).throw(RuntimeError("API down")))
    monkeypatch.setattr(kc._queue_semaphore, "release", lambda: None)

    kc._process_and_commit({"case_id": "dlt-T-1-1"}, {tp: OffsetAndMetadata(4, None, -1)})

    assert kc._offset_tracker.in_flight() == 1


def test_the_api_still_answers_when_the_dlq_is_down(monkeypatch):
    from src.api import routes

    monkeypatch.setattr(routes, "publish_to_dlq",
                        lambda *a: (_ for _ in ()).throw(RuntimeError("broker down")))
    routes._publish_to_dlq_best_effort({"eventId": "E-1"}, "timed out")
