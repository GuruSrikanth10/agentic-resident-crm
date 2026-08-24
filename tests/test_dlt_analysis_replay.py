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
