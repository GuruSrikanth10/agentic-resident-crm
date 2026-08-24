"""Phase 3 of REMEDIATION_PLAN_2026_08_21.md -- DLT lane parity.

The DLT lane reused the rejection lane's shape but not three of its
protections. Each is a bug the rejection path already fixed and documented,
inherited unfixed by the newer lane:

3.1  a server-side invocation budget (0.8)
3.2  a late-result guard against a terminal status another actor recorded (F4)
3.3  LLM work on a dedicated bounded pool, not Starlette's shared one (2.6)
"""
import asyncio
import concurrent.futures
import inspect
import json
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from src.api import dlt_routes
from src.api.dlt_routes import analyze_dlt as _analyze_dlt_async
from src.dlt import case_storage
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]


def analyze_dlt(message):
    return asyncio.run(_analyze_dlt_async(message))


@pytest.fixture(autouse=True)
def _isolated_store(tmp_path, monkeypatch):
    # `storage.factory._build_scoped` imports LOCAL_CASESHEETS_DIR at call
    # time, so patching the canonical definition reaches every scoped root.
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    case_storage.reset_cache()
    yield
    case_storage.reset_cache()


def _message(case_id="dlt-T-63-9001", ref_id="REF-9001"):
    return DltMessage(case_id=case_id, headers=dict(REFERENCE), ref_id=ref_id,
                      ref_id_source="record_key")


def _finding(action="DATA_FIX_REQUIRED", confidence=0.4):
    return DltFinding(narrative="n", recommendation="r",
                      action=action, confidence=confidence)


# ======================================================================
# 3.1 -- the server-side budget
# ======================================================================

def test_the_budget_tracks_the_consumers_client_side_timeout(monkeypatch):
    """Read at call time and derived from the consumer's own budget, so the
    server always gives up slightly BEFORE the client does."""
    monkeypatch.delenv("DLT_ANALYZE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("DLT_ANALYSIS_TIMEOUT_SECONDS", "300")
    assert dlt_routes._dlt_analyze_timeout_seconds() == 270

    monkeypatch.setenv("DLT_ANALYSIS_TIMEOUT_SECONDS", "600")
    assert dlt_routes._dlt_analyze_timeout_seconds() == 570


def test_the_budget_never_goes_below_a_floor(monkeypatch):
    """A consumer timeout under 30s must not yield a zero or negative budget."""
    monkeypatch.delenv("DLT_ANALYZE_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("DLT_ANALYSIS_TIMEOUT_SECONDS", "10")
    assert dlt_routes._dlt_analyze_timeout_seconds() == 30


def test_the_budget_can_be_set_explicitly(monkeypatch):
    monkeypatch.setenv("DLT_ANALYZE_TIMEOUT_SECONDS", "42")
    assert dlt_routes._dlt_analyze_timeout_seconds() == 42


def test_an_overrunning_analysis_is_recorded_as_failed_timeout(monkeypatch):
    """Without a budget the API thread kept running an investigation the
    consumer had already abandoned, and could later overwrite its verdict."""
    monkeypatch.setenv("DLT_ANALYZE_TIMEOUT_SECONDS", "0.2")

    def never_returns(*args, **kwargs):
        time.sleep(5)
        return _finding(), None

    with patch.object(dlt_routes.orchestrator, "investigate", never_returns), \
         patch.object(dlt_routes.reuse, "decide",
                      return_value=dlt_routes.reuse.ReuseDecision(
                          dlt_routes.reuse.Decision.LLM_REQUIRED, "test")):
        result = analyze_dlt(_message())

    assert result["status"] == "failed_timeout"

    storage = case_storage.get_dlt_storage()
    assert storage.terminal_status("dlt-T-63-9001") == "FAILED_TIMEOUT"
    casebook = storage.load("dlt-T-63-9001")
    assert casebook["finding"]["action"] == "NEEDS_MANUAL_REVIEW"
    assert "0.2" in casebook["finding"]["narrative"]


def test_an_analysis_inside_its_budget_completes_normally(monkeypatch):
    """The budget must not fire on a normal run."""
    monkeypatch.setenv("DLT_ANALYZE_TIMEOUT_SECONDS", "30")

    with patch.object(dlt_routes.orchestrator, "investigate",
                      return_value=(_finding(), None)), \
         patch.object(dlt_routes.reuse, "decide",
                      return_value=dlt_routes.reuse.ReuseDecision(
                          dlt_routes.reuse.Decision.LLM_REQUIRED, "test")):
        result = analyze_dlt(_message())

    assert result["status"] == "processed"


# ======================================================================
# 3.2 -- the late-result guard
# ======================================================================

@pytest.mark.parametrize("recorded", ["FAILED_TIMEOUT", "DLQ"])
def test_a_late_result_never_overwrites_another_actors_verdict(monkeypatch, recorded):
    """The consumer's client-side timeout writes FAILED_TIMEOUT and DLQs the
    message while this analysis is still running. A "successful" casebook
    landing afterwards leaves the verdict and the queued DLQ record
    disagreeing about what happened (0.8 / F4)."""
    storage = case_storage.get_dlt_storage()
    case_id = "dlt-T-63-9002"

    def slow_investigate(*args, **kwargs):
        # Another actor records a terminal verdict mid-investigation.
        storage.save_terminal(case_id, {
            "case_id": case_id,
            "packet_status": {"status": recorded},
            "resolution": {"synthesis": "consumer timed out"},
        })
        return _finding(), None

    with patch.object(dlt_routes.orchestrator, "investigate", slow_investigate), \
         patch.object(dlt_routes.reuse, "decide",
                      return_value=dlt_routes.reuse.ReuseDecision(
                          dlt_routes.reuse.Decision.LLM_REQUIRED, "test")):
        result = analyze_dlt(_message(case_id=case_id))

    assert result["status"] == "already_processed"
    # The other actor's verdict stands.
    assert storage.terminal_status(case_id) == recorded


def test_a_normal_result_is_still_written(monkeypatch):
    """The guard must not swallow the ordinary path."""
    case_id = "dlt-T-63-9003"

    with patch.object(dlt_routes.orchestrator, "investigate",
                      return_value=(_finding(), None)), \
         patch.object(dlt_routes.reuse, "decide",
                      return_value=dlt_routes.reuse.ReuseDecision(
                          dlt_routes.reuse.Decision.LLM_REQUIRED, "test")):
        result = analyze_dlt(_message(case_id=case_id))

    assert result["status"] == "processed"
    assert case_storage.get_dlt_storage().terminal_status(case_id) == "NEEDS_MANUAL_REVIEW"


# ======================================================================
# 3.3 -- the LLM lane has its own bounded pool
# ======================================================================

def test_the_analysis_runs_on_the_dedicated_dlt_pool():
    """Not Starlette's shared sync-dispatch pool, and not the rejection lane's
    executor -- a DLT backlog must not starve either."""
    seen = {}

    def record_thread(*args, **kwargs):
        seen["thread"] = threading.current_thread().name
        return _finding(), None

    with patch.object(dlt_routes.orchestrator, "investigate", record_thread), \
         patch.object(dlt_routes.reuse, "decide",
                      return_value=dlt_routes.reuse.ReuseDecision(
                          dlt_routes.reuse.Decision.LLM_REQUIRED, "test")):
        analyze_dlt(_message(case_id="dlt-T-63-9004"))

    assert seen["thread"].startswith("dlt-analyze")


def test_the_dlt_pool_is_a_sibling_not_the_rejection_pool():
    from src.api import routes

    assert dlt_routes._dlt_invoke_executor is not routes._agent_invoke_executor


def test_the_endpoint_is_a_coroutine():
    """A sync def would hold one of anyio's 40 shared slots for the whole
    multi-minute investigation -- the thing routes.py documents at length."""
    assert inspect.iscoroutinefunction(_analyze_dlt_async)


def test_the_drain_tears_down_the_dlt_pool(monkeypatch, tmp_path):
    """Its threads used to outlive the drain and be left for SIGKILL."""
    from src.api import routes
    from src.storage.local import LocalFilesystemCasebookStorage

    throwaway_rejection = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    throwaway_dlt = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    monkeypatch.setattr(routes, "_agent_invoke_executor", throwaway_rejection)
    monkeypatch.setattr(dlt_routes, "_dlt_invoke_executor", throwaway_dlt)
    monkeypatch.setattr(routes, "API_SHUTDOWN_DRAIN_SECONDS", 0.01)
    monkeypatch.setattr(routes, "get_casebook_storage",
                        lambda: LocalFilesystemCasebookStorage(base_dir=str(tmp_path)))

    try:
        routes.drain_and_shutdown()
    finally:
        routes._draining.clear()

    # A shut-down pool refuses new work.
    for pool in (throwaway_rejection, throwaway_dlt):
        with pytest.raises(RuntimeError):
            pool.submit(lambda: None)


def test_registering_a_bare_executor_is_refused():
    """The registry stores getters so a substituted module attribute is
    honoured at drain time. A reference captured at import would silently
    shut down the real pool instead of a test's throwaway."""
    from src.api import routes

    with pytest.raises(TypeError, match="zero-argument callable"):
        routes.register_executor(concurrent.futures.ThreadPoolExecutor(max_workers=1))


def test_the_guard_sees_a_verdict_left_only_in_the_casebook():
    """save_terminal writes casebook.json FIRST, so an actor that died between
    its two writes leaves a terminal casebook and a stale status.json. A guard
    reading only status.json sails past it -- which is F4 by a narrower route.
    """
    storage = case_storage.get_dlt_storage()
    case_id = "dlt-T-63-9005"

    def crash_midway_then_finish(*args, **kwargs):
        # Only casebook.json, exactly as a crash between the two writes leaves it.
        storage.save(case_id, {
            "case_id": case_id,
            "packet_status": {"status": "FAILED_TIMEOUT"},
        }, filename="casebook.json")
        return _finding(), None

    with patch.object(dlt_routes.orchestrator, "investigate", crash_midway_then_finish), \
         patch.object(dlt_routes.reuse, "decide",
                      return_value=dlt_routes.reuse.ReuseDecision(
                          dlt_routes.reuse.Decision.LLM_REQUIRED, "test")):
        result = analyze_dlt(_message(case_id=case_id))

    assert result["status"] == "already_processed"
    assert storage.terminal_status(case_id) == "FAILED_TIMEOUT"
