"""
Phase 8 of DLT_PLAN.md -- the analysis lane.

The LLM is mocked throughout; what is under test is the routing around it.

Three properties carry the phase:

* Class B never invokes the LLM and never claims to have diagnosed anything.
  With no source access, a narrative about *why* the code failed is invention.
* A cached recommendation is never served blind -- a CONTRADICTED verdict
  overrides it and escalates.
* Ceilings compose by minimum and every one that binds is named, so a capped
  score is auditable rather than mysteriously low.
"""
import asyncio
import json
from pathlib import Path

import pytest

from src.api import dlt_routes
from src.api.dlt_routes import FETCHED_LOGS_ARTIFACT
from src.api.dlt_routes import analyze_dlt as _analyze_dlt_async
from src.dlt import canned, case_storage, groups
from src.dlt.corroborate import Corroboration, Verdict
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding, apply_dlt_confidence_policy


# `/analyze-dlt` is a coroutine now: the LLM lane runs on a bounded executor
# under a server-side budget, mirroring /process-rejection. These tests drive
# the endpoint directly and synchronously, so they go through this shim rather
# than sprouting an asyncio.run() at every call site.
def analyze_dlt(message):
    return asyncio.run(_analyze_dlt_async(message))


FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]

NPE_TRACE = (
    "org.springframework.X: outer\n\tat org.springframework.A.b(A.java:1)"
    "\nCaused by: java.lang.NullPointerException: Cannot invoke getId()"
    "\n\tat com.uidai.enu.biometric.Svc.doWork(Svc.java:88)\n\t... 3 more\n"
)
TIMEOUT_TRACE = (
    "org.springframework.X: outer\n\tat org.springframework.A.b(A.java:1)"
    "\nCaused by: java.net.SocketTimeoutException: Read timed out"
    "\n\tat com.uidai.enu.biometric.Svc.call(Svc.java:12)\n\t... 3 more\n"
)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_REUSE_ENABLED", "DLT_GROUP_MEMBER_CAP", "CASEBOOK_STORAGE_BACKEND",
                "DLT_CLASS_B_CEILING", "DLT_UNVERIFIED_CONFIDENCE_CEILING",
                "DLT_CONTRADICTED_CEILING", "DLT_REGISTRY_MISS_CEILING",
                "DLT_REUSE_DECAY", "SYNTHESIS_GAP_CONFIDENCE_CEILING"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    # No group-lock directory to isolate any more: group updates go through
    # CasebookStorage.update_json, whose atomicity lives in the backend.
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    case_storage.reset_cache()
    from src.dlt import registry
    registry.clear_cache()
    yield
    case_storage.reset_cache()


def message(case_id="dlt-T-63-3352", trace=None, ref_id="REF-1"):
    headers = dict(REFERENCE)
    if trace is not None:
        headers["kafka_exception-stacktrace"] = trace
        headers.pop("kafka_exception-message", None)
    return DltMessage(case_id=case_id, headers=headers,
                      payload={"packetMetaData": {"refId": ref_id}}, ref_id=ref_id)


def seed_logs(case_id, text):
    case_storage.get_dlt_storage().save_artifact(case_id, FETCHED_LOGS_ARTIFACT, text)


def stub_llm(monkeypatch, finding: DltFinding, calls: list,
             seen_summaries: list = None):
    def fake(case_id, failure, corroboration, logs, payload_summary=None):
        calls.append(case_id)
        if seen_summaries is not None:
            seen_summaries.append(payload_summary)
        return finding, None

    monkeypatch.setattr(dlt_routes.orchestrator, "investigate", fake)


AGENT_FINDING = DltFinding(
    narrative="The code reported the UidOriginTracker row absent.",
    recommendation="Query the table for the refIds in this group.",
    action="DATA_FIX_REQUIRED",
    confidence=0.85,
)


# ======================================================================
# Class B -- no LLM, no diagnosis
# ======================================================================

def test_class_b_never_invokes_the_llm(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-B-0-1", "[ERROR] java.lang.NullPointerException: boom")

    result = analyze_dlt(message(case_id="dlt-B-0-1", trace=NPE_TRACE))

    assert calls == [], "no source access, so there is nothing to pay for"
    assert result["action"] == "ROUTE_TO_DEV"
    assert result["decision"] == "CANNED"


def test_class_b_confidence_is_capped():
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="ROUTE_TO_DEV",
                   confidence=0.95),
        failure_class="B", corroboration="CORROBORATED", registry_hit=False)
    assert finding.confidence <= 0.3
    assert "class_b" in finding.ceilings_applied


def test_class_b_narrative_refuses_to_diagnose(monkeypatch):
    seed_logs("dlt-B-0-1", "[ERROR] java.lang.NullPointerException: boom")
    analyze_dlt(message(case_id="dlt-B-0-1", trace=NPE_TRACE))

    casebook = case_storage.get_dlt_storage().load("dlt-B-0-1")
    narrative = casebook["finding"]["narrative"]
    assert "No diagnosis is offered" in narrative
    assert "no access to the source" in narrative


def test_class_b_reports_aggregation_which_is_the_actual_value(monkeypatch):
    """What this adds over Kafka UI is the occurrence count, not a diagnosis."""
    for i in range(4):
        seed_logs(f"dlt-B-0-{i}", "[ERROR] java.lang.NullPointerException: boom")
        analyze_dlt(message(case_id=f"dlt-B-0-{i}", trace=NPE_TRACE))

    casebook = case_storage.get_dlt_storage().load("dlt-B-0-3")
    assert "4 occurrences" in casebook["finding"]["narrative"]


def test_class_c_recommends_a_redrive(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-C-0-1", "[ERROR] java.net.SocketTimeoutException: Read timed out")

    result = analyze_dlt(message(case_id="dlt-C-0-1", trace=TIMEOUT_TRACE))
    assert result["action"] == "REDRIVE_AFTER_RECOVERY"
    assert calls == []


# ======================================================================
# Class A -- the analysis lane
# ======================================================================

def test_novel_class_a_fingerprint_invokes_the_llm(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    result = analyze_dlt(message())
    assert calls == ["dlt-T-63-3352"]
    assert result["decision"] == "LLM_REQUIRED"
    assert result["action"] == "DATA_FIX_REQUIRED"


def test_second_occurrence_reuses_without_the_llm(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    corroborating = ("[ERROR] in.gov.uidai.common.exception.BusinessException: "
                     "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    seed_logs("dlt-T-63-3352", corroborating)
    analyze_dlt(message(case_id="dlt-T-63-3352"))

    seed_logs("dlt-T-63-3353", corroborating)
    result = analyze_dlt(message(case_id="dlt-T-63-3353"))

    assert len(calls) == 1, "one investigation per bug, not per message"
    assert result["decision"] == "REUSE_GROUP"


def test_reuse_applies_the_confidence_decay(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    corroborating = ("[ERROR] in.gov.uidai.common.exception.BusinessException: "
                     "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    seed_logs("dlt-T-63-3352", corroborating)
    first = analyze_dlt(message(case_id="dlt-T-63-3352"))
    seed_logs("dlt-T-63-3353", corroborating)
    second = analyze_dlt(message(case_id="dlt-T-63-3353"))

    assert second["confidence"] < first["confidence"]


def test_contradiction_overrides_the_cache_and_leads_the_casebook(monkeypatch):
    """The mis-cast case. Blind reuse would bury exactly this."""
    calls = []
    discrepancy_finding = DltFinding(
        narrative="The logs show a timeout, not a missing row.",
        discrepancy="The declared BusinessException is not supported by the logs.",
        recommendation="Investigate the datasource timeout.",
        action="NEEDS_MANUAL_REVIEW",
        confidence=0.8,
    )

    # First occurrence corroborates and caches a recommendation.
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")
    analyze_dlt(message(case_id="dlt-T-63-3352"))
    assert len(calls) == 1

    # Second occurrence, same fingerprint, but the logs tell a different story.
    stub_llm(monkeypatch, discrepancy_finding, calls)
    seed_logs("dlt-T-63-3353",
              "[ERROR] java.net.SocketTimeoutException: Read timed out")
    result = analyze_dlt(message(case_id="dlt-T-63-3353"))

    assert result["corroboration"] == "CONTRADICTED"
    assert result["decision"] == "LLM_REQUIRED", "the cache must not win here"
    assert len(calls) == 2

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3353")
    assert casebook["finding"]["discrepancy"]
    assert casebook["evidence"]["unexplained_exceptions"]


def test_synthesis_failure_yields_a_manual_review_case(monkeypatch):
    monkeypatch.setattr(dlt_routes.orchestrator, "investigate",
                        lambda *a, **kw: (None, "Response contained no JSON object."))
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    result = analyze_dlt(message())
    assert result["action"] == "NEEDS_MANUAL_REVIEW"

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["provenance"]["source"] == "failed_synthesis"
    assert casebook["finding"]["parse_error"]


# ======================================================================
# Confidence policy
# ======================================================================

def test_ceilings_compose_by_minimum():
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="NEEDS_MANUAL_REVIEW",
                   confidence=0.99),
        failure_class="B", corroboration="UNVERIFIABLE", registry_hit=False)

    assert finding.confidence <= 0.3, "the lowest binding ceiling wins"
    assert "class_b" in finding.ceilings_applied
    assert "unverifiable" in finding.ceilings_applied


def test_every_applied_ceiling_is_named():
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="DATA_FIX_REQUIRED",
                   confidence=0.9),
        failure_class="A", corroboration="CONTRADICTED", registry_hit=False)

    assert set(finding.ceilings_applied) >= {"contradicted", "registry_miss"}
    assert finding.confidence <= 0.5


def test_registry_hit_avoids_the_miss_ceiling():
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="DATA_FIX_REQUIRED",
                   confidence=0.9),
        failure_class="A", corroboration="CORROBORATED", registry_hit=True)
    assert finding.confidence == 0.9
    assert finding.ceilings_applied == []


def test_evidence_gap_banner_reuses_the_rejection_ceiling():
    from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER

    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="DATA_FIX_REQUIRED",
                   confidence=0.95),
        failure_class="A", corroboration="CORROBORATED", registry_hit=True,
        logs=f"{BANNER_HEADER}\nLOG_ROTATION: ...\n")
    assert finding.confidence <= 0.6
    assert "evidence_gap" in finding.ceilings_applied


def test_absent_confidence_stays_absent():
    """A model that ignored the instruction is honest; defaulting to 1.0 would
    manufacture certainty."""
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="NO_ACTION",
                   confidence=None),
        failure_class="A", corroboration="CORROBORATED", registry_hit=True)
    assert finding.confidence is None


def test_class_b_action_is_forced_even_if_the_model_disagrees():
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="NO_ACTION",
                   confidence=0.9),
        failure_class="B", corroboration="CORROBORATED", registry_hit=True)
    assert finding.action == "NEEDS_MANUAL_REVIEW"


def test_malformed_ceiling_env_falls_back(monkeypatch):
    monkeypatch.setenv("DLT_CLASS_B_CEILING", "not-a-number")
    finding = apply_dlt_confidence_policy(
        DltFinding(narrative="x", recommendation="y", action="ROUTE_TO_DEV",
                   confidence=0.9),
        failure_class="B", corroboration="CORROBORATED", registry_hit=True)
    assert finding.confidence <= 0.3


# ======================================================================
# Casebook and lifecycle
# ======================================================================

def test_casebook_carries_provenance_and_evidence(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    analyze_dlt(message())
    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")

    assert casebook["schema_version"] == dlt_routes.DLT_CASEBOOK_SCHEMA_VERSION
    assert casebook["source"]["consumer_group"] == "enu-biodedup-cg"
    assert casebook["source"]["attempts"] == 5
    assert casebook["failure"]["business_code"] == "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"
    assert casebook["failure"]["registry_description"]
    assert casebook["evidence"]["corroboration"] == "CORROBORATED"
    assert casebook["evidence"]["citations"]
    assert casebook["provenance"]["source"] == "agent"
    assert casebook["provenance"]["group_occurrences"] == 1


def test_no_path_writes_a_final_recommendation(monkeypatch):
    """There is no review mechanism in v1, so every recommendation stays
    explicitly marked unreviewed."""
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")
    analyze_dlt(message())

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["provenance"]["recommendation_state"] == groups.STATE_DRAFT

    fingerprint = casebook["failure"]["fingerprint"]
    assert groups.load_group(fingerprint)["recommendation_state"] == groups.STATE_DRAFT


def test_terminal_case_is_not_reanalysed(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    analyze_dlt(message())
    result = analyze_dlt(message())

    assert result["status"] == "already_processed"
    assert len(calls) == 1


def test_missing_logs_yield_unverifiable_not_a_crash(monkeypatch):
    calls = []
    stub_llm(monkeypatch, AGENT_FINDING, calls)
    result = analyze_dlt(message())
    assert result["corroboration"] == "UNVERIFIABLE"


# ======================================================================
# Canned treatments
# ======================================================================

def test_canned_first_occurrence_wording():
    finding = canned.build("B", {"signature": "NPE @ Svc.doWork"}, None, None)
    assert "first recorded occurrence" in finding.narrative


def test_canned_surfaces_a_discrepancy():
    corroboration = Corroboration(Verdict.CONTRADICTED, "logs show a timeout",
                                  unexplained=("java.net.SocketTimeoutException",))
    finding = canned.build("C", {"signature": "Timeout @ Svc.call"}, corroboration, None)
    assert finding.discrepancy
    assert "suspicion" in finding.discrepancy


def test_canned_class_u_asks_for_the_class_map():
    finding = canned.build("U", {"signature": "WeirdFault @ Other.go",
                                 "class_reason": "unrecognised"}, None, None)
    assert finding.action == "NEEDS_MANUAL_REVIEW"
    assert "DLT_CLASS_MAP" in finding.recommendation
