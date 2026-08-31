"""
Auto-replay wired into `/analyze-dlt` end-to-end.

`tests/test_dlt_auto_replay.py` proves the decision gate in isolation; this
file proves it is actually reached at the right point in `analyze_dlt` --
after ceilings and reuse decay have already been applied to the finding, so
the gate never sees a model's raw, uncapped confidence number -- and that the
casebook and HTTP response both say plainly whether replay was attempted.
"""
import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.api import dlt_routes
from src.api.dlt_routes import FETCHED_LOGS_ARTIFACT
from src.api.dlt_routes import analyze_dlt as _analyze_dlt_async
from src.dlt import case_storage
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding


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


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    for var in ("DLT_REUSE_ENABLED", "DLT_CLASS_B_CEILING",
                "DLT_UNVERIFIED_CONFIDENCE_CEILING", "DLT_CONTRADICTED_CEILING",
                "DLT_REGISTRY_MISS_CEILING", "DLT_REUSE_DECAY",
                "SYNTHESIS_GAP_CONFIDENCE_CEILING", "DLT_AUTO_REPLAY_ENABLED",
                "DLT_REPLAY_CONFIDENCE_THRESHOLD", "CASEBOOK_STORAGE_BACKEND"):
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


def message(case_id="dlt-T-63-3352", ref_id="REF-1", trace=None):
    headers = dict(REFERENCE)
    if trace is not None:
        headers["kafka_exception-stacktrace"] = trace
        headers.pop("kafka_exception-message", None)
    return DltMessage(case_id=case_id, headers=headers,
                      payload={"packetMetaData": {"refId": ref_id}}, ref_id=ref_id)


def seed_logs(case_id, text):
    case_storage.get_dlt_storage().save_artifact(case_id, FETCHED_LOGS_ARTIFACT, text)


def stub_llm(monkeypatch, finding: DltFinding):
    monkeypatch.setattr(dlt_routes.orchestrator, "investigate",
                        lambda *a, **k: (finding, None))


def mock_replay_tool():
    fake_tool = MagicMock()
    fake_tool.invoke.return_value = "Successfully queued for replay"
    return patch("src.tools.tool_registry.get_tool_by_name", return_value=fake_tool), fake_tool


# ======================================================================
# The mis-cast case: what this feature exists for
# ======================================================================

def test_a_high_confidence_mis_cast_finding_triggers_replay(monkeypatch):
    """CONTRADICTED corroboration, the LLM concludes it was actually a
    transient fault and says so at 0.6 (the max the ceiling allows) -- the
    exact shape of finding this feature is built for."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    stub_llm(monkeypatch, DltFinding(
        narrative="x", discrepancy="the logs show a timeout, not the "
                  "declared business exception",
        recommendation="redrive", action="REDRIVE_AFTER_RECOVERY",
        confidence=0.9))  # raw model number, ABOVE the 0.6 ceiling
    seed_logs("dlt-T-63-3352",
              "[ERROR] java.net.SocketTimeoutException: Read timed out")

    patcher, fake_tool = mock_replay_tool()
    with patcher:
        result = analyze_dlt(message())

    assert result["decision"] == "LLM_REQUIRED"
    assert result["replay_attempted"] is True
    assert result["replay_queued"] is True
    fake_tool.invoke.assert_called_once()
    called_with = fake_tool.invoke.call_args[0][0]
    assert called_with["id"] == "REF-1"

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["replay"]["attempted"] is True
    assert casebook["replay"]["queued"] is True
    assert "REDRIVE_AFTER_RECOVERY" in casebook["replay"]["reason"]


def test_the_ceiling_applies_before_the_replay_gate_not_after(monkeypatch):
    """The core safety property: the gate must see the CAPPED confidence
    (<=0.6 under CONTRADICTED), never the model's raw, uncapped number. A
    threshold sitting strictly between the two would prove this either way."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    monkeypatch.setenv("DLT_REPLAY_CONFIDENCE_THRESHOLD", "0.7")
    stub_llm(monkeypatch, DltFinding(
        narrative="x", discrepancy="mismatch", recommendation="redrive",
        action="REDRIVE_AFTER_RECOVERY", confidence=0.95))
    seed_logs("dlt-T-63-3352",
              "[ERROR] java.net.SocketTimeoutException: Read timed out")

    patcher, fake_tool = mock_replay_tool()
    with patcher:
        result = analyze_dlt(message())

    # 0.95 raw would pass a 0.7 threshold; capped to <=0.6 by the
    # CONTRADICTED ceiling, it must not.
    assert result["replay_attempted"] is False
    fake_tool.invoke.assert_not_called()


# ======================================================================
# Everything that must NOT trigger replay
# ======================================================================

def test_class_b_never_replays_even_with_the_feature_on(monkeypatch):
    """Canned Class C's REDRIVE_AFTER_RECOVERY has no confidence at all, but
    Class B (ROUTE_TO_DEV) is the cleaner end-to-end check: wrong action AND
    no LLM confidence, both independently disqualifying."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    seed_logs("dlt-B-0-1", "[ERROR] java.lang.NullPointerException: boom")

    patcher, fake_tool = mock_replay_tool()
    with patcher:
        result = analyze_dlt(message(case_id="dlt-B-0-1", ref_id="REF-2",
                                     trace=NPE_TRACE))

    assert result["decision"] == "CANNED"
    assert result["action"] == "ROUTE_TO_DEV"
    assert result["replay_attempted"] is False
    fake_tool.invoke.assert_not_called()

    casebook = case_storage.get_dlt_storage().load("dlt-B-0-1")
    assert casebook["replay"]["attempted"] is False


def test_disabled_by_default_never_calls_the_tool(monkeypatch):
    """No DLT_AUTO_REPLAY_ENABLED set at all -- the default-off posture."""
    stub_llm(monkeypatch, DltFinding(
        narrative="x", discrepancy="mismatch", recommendation="redrive",
        action="REDRIVE_AFTER_RECOVERY", confidence=0.9))
    seed_logs("dlt-T-63-3352",
              "[ERROR] java.net.SocketTimeoutException: Read timed out")

    patcher, fake_tool = mock_replay_tool()
    with patcher:
        result = analyze_dlt(message())

    assert result["replay_attempted"] is False
    fake_tool.invoke.assert_not_called()


def test_a_data_fix_required_finding_never_replays(monkeypatch):
    """The ordinary Class A case: corroborated, no discrepancy, the code
    genuinely means what it says. Replaying reproduces the same missing row."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    stub_llm(monkeypatch, DltFinding(
        narrative="x", recommendation="check the table",
        action="DATA_FIX_REQUIRED", confidence=0.9))
    seed_logs("dlt-T-63-3352",
              "[ERROR] in.gov.uidai.common.exception.BusinessException: "
              "[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] absent")

    patcher, fake_tool = mock_replay_tool()
    with patcher:
        result = analyze_dlt(message())

    assert result["action"] == "DATA_FIX_REQUIRED"
    assert result["replay_attempted"] is False
    fake_tool.invoke.assert_not_called()


def test_a_replay_tool_failure_does_not_break_casebook_persistence(monkeypatch):
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    stub_llm(monkeypatch, DltFinding(
        narrative="x", discrepancy="mismatch", recommendation="redrive",
        action="REDRIVE_AFTER_RECOVERY", confidence=0.9))
    seed_logs("dlt-T-63-3352",
              "[ERROR] java.net.SocketTimeoutException: Read timed out")

    broken_tool = MagicMock()
    broken_tool.invoke.side_effect = RuntimeError("OIS unreachable")

    with patch("src.tools.tool_registry.get_tool_by_name", return_value=broken_tool):
        result = analyze_dlt(message())

    assert result["status"] == "processed"
    assert result["replay_attempted"] is True
    assert result["replay_queued"] is False

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["packet_status"]["status"]  # casebook still persisted
    assert "OIS unreachable" in casebook["replay"]["result"]


# ======================================================================
# Phase C5 -- the replay precheck, wired in and observing only
# ======================================================================

REPO_MAP = json.dumps({
    "com.uidai.enu.biometric": {
        "project": "ENU", "repo": "enu-biometric", "branch": "release",
        "version_file": "pom.xml", "source_roots": ["src/main/java"],
    },
})


def enable_code_check(monkeypatch, *, commits, changes=None, versions=None,
                      path="src/main/java/com/uidai/enu/biometric/Svc.java"):
    from src.dlt import bitbucket, deployed
    monkeypatch.setenv("DLT_CODE_CHECK_ENABLED", "true")
    monkeypatch.setenv("BITBUCKET_BASE_URL", "https://bitbucket.example")
    monkeypatch.setenv("BITBUCKET_TOKEN", "read-only")
    monkeypatch.setenv("DLT_REPO_MAP", REPO_MAP)
    monkeypatch.setattr(bitbucket, "resolve_path", lambda repo, suffix: path)
    monkeypatch.setattr(bitbucket, "commits_touching",
                        lambda repo, p, since_ms=None, limit=None:
                        (commits if p == path else []))
    monkeypatch.setattr(bitbucket, "changed_paths",
                        lambda repo, cid, limit=500: (changes or {}).get(cid))
    monkeypatch.setattr(bitbucket, "version_at",
                        lambda repo, ref=None, version_file=None:
                        (versions or {}).get(ref))
    bitbucket.reset_cache()
    deployed.reset_cache()


def stub_running(monkeypatch, running_versions):
    from src.dlt import deployed
    monkeypatch.setattr(
        deployed, "running_version",
        lambda app=None, namespace=None: deployed.DeployedVersions(
            versions=tuple(running_versions), ok=bool(running_versions),
            reason="" if running_versions else "stubbed as unavailable"))


def seed_baseline(case_id, versions):
    case_storage.get_dlt_storage().save_artifact(
        case_id, dlt_routes.DEPLOYED_ARTIFACT,
        json.dumps({"versions": list(versions), "ok": True}))


def test_the_casebook_always_carries_a_code_check_block(monkeypatch):
    """Even with the feature off. "We did not look" must be legible."""
    result = analyze_dlt(message(trace=NPE_TRACE))

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["code_check"]["verdict"] == "UNKNOWN"
    assert "DLT_CODE_CHECK_ENABLED" in casebook["code_check"]["reason"]
    assert result["code_check"] == "UNKNOWN"


def test_a_no_change_verdict_reaches_the_casebook(monkeypatch):
    enable_code_check(monkeypatch, commits=[])
    stub_running(monkeypatch, ["1.0.1"])
    seed_baseline("dlt-T-63-3352", ["1.0.0"])

    result = analyze_dlt(message(trace=NPE_TRACE))

    assert result["code_check"] == "NO_CHANGE"
    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["code_check"]["repo"] == "ENU/enu-biometric"
    assert casebook["code_check"]["branch"] == "release"


def test_the_baseline_is_read_from_the_artifact_not_from_today(monkeypatch):
    """The version that was running when the packet failed, not the one
    running now. Substituting today's would silently defeat the T9 guard."""
    from src.dlt import bitbucket
    path = "src/main/java/com/uidai/enu/biometric/Svc.java"
    enable_code_check(
        monkeypatch,
        commits=[bitbucket.Commit(id="aaa", subject="Fix NPE",
                                  timestamp_ms=1787019700000)],
        changes={"aaa": ["pom.xml"]}, versions={"aaa": "1.0.1"}, path=path)
    stub_running(monkeypatch, ["1.0.2"])
    seed_baseline("dlt-T-63-3352", ["1.0.0"])

    result = analyze_dlt(message(trace=NPE_TRACE))

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    assert casebook["code_check"]["baseline_version"] == "1.0.0"
    assert casebook["code_check"]["running_version"] == "1.0.2"
    assert result["code_check"] == "FIX_DEPLOYED"


def test_a_missing_baseline_artifact_degrades_to_unknown(monkeypatch):
    from src.dlt import bitbucket
    path = "src/main/java/com/uidai/enu/biometric/Svc.java"
    enable_code_check(
        monkeypatch,
        commits=[bitbucket.Commit(id="aaa", subject="Fix", timestamp_ms=1787019700000)],
        changes={"aaa": ["pom.xml"]}, versions={"aaa": "1.0.1"}, path=path)
    stub_running(monkeypatch, ["1.0.2"])

    result = analyze_dlt(message(trace=NPE_TRACE))

    assert result["code_check"] == "UNKNOWN"


def test_the_verdict_is_recorded_on_the_group(monkeypatch):
    """What `dlt_report --code-check` reads and the accuracy loop joins on."""
    from src.dlt import groups
    enable_code_check(monkeypatch, commits=[])
    stub_running(monkeypatch, ["1.0.1"])
    seed_baseline("dlt-T-63-3352", ["1.0.0"])

    analyze_dlt(message(trace=NPE_TRACE))

    casebook = case_storage.get_dlt_storage().load("dlt-T-63-3352")
    group = groups.load_group(casebook["failure"]["fingerprint"])
    assert group["code_check"]["verdict"] == "NO_CHANGE"
    assert group["code_check_history"] == {"NO_CHANGE": 1}


@pytest.mark.parametrize("commits,changes,versions,expected", [
    ([], None, None, "NO_CHANGE"),
    (None, None, None, "UNKNOWN"),
])
def test_c5_never_changes_the_replay_decision(monkeypatch, commits, changes,
                                              versions, expected):
    """C5 observes. Only C6 acts, and only behind its own second flag."""
    monkeypatch.setenv("DLT_AUTO_REPLAY_ENABLED", "true")
    enable_code_check(monkeypatch, commits=commits, changes=changes,
                      versions=versions)
    stub_running(monkeypatch, ["1.0.1"])
    seed_baseline("dlt-T-63-3352", ["1.0.0"])
    stub_llm(monkeypatch, DltFinding(
        narrative="x", discrepancy="the logs show a timeout",
        recommendation="redrive", action="REDRIVE_AFTER_RECOVERY",
        confidence=0.9))
    seed_logs("dlt-T-63-3352",
              "[ERROR] java.net.SocketTimeoutException: Read timed out")

    patcher, fake_tool = mock_replay_tool()
    with patcher:
        result = analyze_dlt(message())

    assert result["code_check"] == expected
    # The replay fired regardless of the verdict -- C5 is observe-only.
    assert result["replay_attempted"] is True
    assert result["replay_queued"] is True
    fake_tool.invoke.assert_called_once()


def test_a_bitbucket_outage_does_not_cost_the_case(monkeypatch):
    from src.dlt import bitbucket
    enable_code_check(monkeypatch, commits=[])
    monkeypatch.setattr(bitbucket, "resolve_path",
                        lambda repo, suffix: (_ for _ in ()).throw(
                            ConnectionError("bitbucket unreachable")))
    stub_running(monkeypatch, ["1.0.1"])
    seed_baseline("dlt-T-63-3352", ["1.0.0"])

    result = analyze_dlt(message(trace=NPE_TRACE))

    assert result["status"] == "processed"
    assert result["code_check"] == "UNKNOWN"
