"""
Phase 3 regression tests (AUDIT_2026_08.md section 6).

G7  -- revoked partitions are committed and dropped, not left in the tracker.
G8  -- the consumer answers its own liveness.
G9  -- the API drains and marks what it could not finish.
G10 -- a Kubernetes failure does not disable the Elasticsearch fallback.
G11 -- a cached rule survives an open DB breaker.
G12 -- the retry prompt keeps the log evidence.
G13 -- one definition per path.
G14 -- LOG_MAX_DOCUMENTS is read at call time.
N1  -- the shadow report is the promotion gate.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import src.utils.kafkaConsumer as kc


# ======================================================================
# G7 -- rebalance handling
# ======================================================================

def test_revoked_partitions_are_dropped_from_the_tracker():
    """Stale entries let the consumer commit offsets for a partition another
    member now owns, moving that member past messages it is still working."""
    tracker = kc.OffsetTracker()
    keep, revoke = "p-0", "p-1"

    tracker.dispatched(keep, 1)
    tracker.completed(keep, 1)
    tracker.dispatched(revoke, 5)
    tracker.completed(revoke, 5)

    commits = tracker.committable_for([revoke])
    assert set(commits) == {revoke}
    assert commits[revoke].offset == 6

    tracker.forget([revoke])
    assert revoke not in tracker.tracked_partitions()
    assert keep in tracker.tracked_partitions()

    # The retained partition is untouched by the revocation.
    assert tracker.take_committable()[keep].offset == 2


def test_committable_for_respects_the_in_flight_floor():
    """Revocation must not commit past work that is still running."""
    tracker = kc.OffsetTracker()
    tp = "p-0"
    for offset in (10, 11):
        tracker.dispatched(tp, offset)
    tracker.completed(tp, 10)

    commits = tracker.committable_for([tp])
    assert commits[tp].offset == 11, "11 is still in flight, so stop below it"


def test_rebalance_listener_commits_then_forgets(monkeypatch):
    tracker = kc.OffsetTracker()
    monkeypatch.setattr(kc, "_offset_tracker", tracker)

    committed = []
    fake_consumer = SimpleNamespace(commit=lambda c: committed.append(c))
    monkeypatch.setattr(kc, "consumer", fake_consumer)

    tp = "p-3"
    tracker.dispatched(tp, 9)
    tracker.completed(tp, 9)

    kc._RebalanceListener().on_partitions_revoked([tp])

    assert committed and committed[0][tp].offset == 10
    assert tp not in tracker.tracked_partitions()


def test_reassignment_clears_stale_state(monkeypatch):
    """A partition coming back must not inherit offsets from a previous
    ownership period another member has since advanced past."""
    tracker = kc.OffsetTracker()
    monkeypatch.setattr(kc, "_offset_tracker", tracker)

    tp = "p-4"
    tracker.dispatched(tp, 100)
    kc._RebalanceListener().on_partitions_assigned([tp])

    assert tp not in tracker.tracked_partitions()


def test_a_failed_revoke_commit_is_not_fatal(monkeypatch):
    """Mid-rebalance the commit can legitimately fail; the new owner
    redelivers from the last committed offset, which is correct."""
    tracker = kc.OffsetTracker()
    monkeypatch.setattr(kc, "_offset_tracker", tracker)

    def failing_commit(_c):
        raise RuntimeError("rebalance in progress")

    monkeypatch.setattr(kc, "consumer", SimpleNamespace(commit=failing_commit))

    tp = "p-5"
    tracker.dispatched(tp, 1)
    tracker.completed(tp, 1)

    kc._RebalanceListener().on_partitions_revoked([tp])  # must not raise
    assert tp not in tracker.tracked_partitions()


# ======================================================================
# G8 -- the consumer answers for itself
# ======================================================================

def test_consumer_health_reads_its_own_heartbeat(tmp_path, monkeypatch):
    import time

    heartbeat = tmp_path / "hb.txt"
    monkeypatch.setattr(kc, "_heartbeat_path", lambda: heartbeat)
    kc._shutdown.clear()

    heartbeat.write_text(str(time.time()), encoding="utf-8")
    assert kc.is_healthy() is True

    heartbeat.write_text(str(time.time() - 3600), encoding="utf-8")
    assert kc.is_healthy() is False, "a stale heartbeat means dead"


def test_consumer_health_is_false_while_draining(tmp_path, monkeypatch):
    import time

    heartbeat = tmp_path / "hb.txt"
    heartbeat.write_text(str(time.time()), encoding="utf-8")
    monkeypatch.setattr(kc, "_heartbeat_path", lambda: heartbeat)

    kc._shutdown.set()
    try:
        assert kc.is_healthy() is False
    finally:
        kc._shutdown.clear()


def test_consumer_health_is_false_without_a_heartbeat(tmp_path, monkeypatch):
    monkeypatch.setattr(kc, "_heartbeat_path", lambda: tmp_path / "missing.txt")
    kc._shutdown.clear()
    assert kc.is_healthy() is False


# ======================================================================
# G9 -- API graceful shutdown
# ======================================================================

def test_draining_fails_readiness(monkeypatch):
    """A draining pod must stop receiving new packets."""
    from fastapi import HTTPException

    import src.api.routes as routes

    routes._draining.set()
    try:
        with pytest.raises(HTTPException) as exc:
            routes.readiness_check()
        assert exc.value.status_code == 503
    finally:
        routes._draining.clear()


def test_shutdown_marks_abandoned_investigations(tmp_path, monkeypatch):
    """An interrupted packet must not be left at IN_PROGRESS, which blocks
    reprocessing for MAX_IN_PROGRESS_AGE_SECONDS (30 minutes by default)."""
    import concurrent.futures

    import src.api.routes as routes
    from src.storage.local import LocalFilesystemCasebookStorage

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(routes, "API_SHUTDOWN_DRAIN_SECONDS", 0.1)

    # Throwaway executors: drain_and_shutdown() shuts down whatever it is
    # given, and the real ones are module-level and shared by every other test
    # in the suite -- shutting them down here would break all of them with
    # "cannot schedule new futures after shutdown".
    #
    # BOTH pools, because the drain now also tears down the DLT analysis lane's
    # sibling executor. Both are resolved by attribute at drain time precisely
    # so this substitution works; see routes._extra_executor_getters.
    from src.api import dlt_routes

    monkeypatch.setattr(routes, "_agent_invoke_executor",
                        concurrent.futures.ThreadPoolExecutor(max_workers=1))
    monkeypatch.setattr(dlt_routes, "_dlt_invoke_executor",
                        concurrent.futures.ThreadPoolExecutor(max_workers=1))

    store.save("stuck", {
        "packet_metadata": {"eid": "stuck"},
        "packet_status": {"status": "IN_PROGRESS"},
    }, filename="status.json")

    with routes._in_flight_lock:
        # A Counter now, so a duplicate in-flight id stays visible to the
        # drain until BOTH invocations have finished.
        routes._in_flight_events["stuck"] += 1

    try:
        routes.drain_and_shutdown()
    finally:
        routes._draining.clear()
        with routes._in_flight_lock:
            routes._in_flight_events.pop("stuck", None)

    assert store.terminal_status("stuck") == "FAILED_SHUTDOWN"


def test_in_flight_tracking_deregisters_on_every_path():
    import src.api.routes as routes

    before = routes._in_flight_investigations()

    with routes._tracked_in_flight("evt-a"):
        assert routes._in_flight_investigations() == before + 1
    assert routes._in_flight_investigations() == before

    with pytest.raises(RuntimeError):
        with routes._tracked_in_flight("evt-b"):
            raise RuntimeError("boom")
    assert routes._in_flight_investigations() == before, "must deregister on error"


# ======================================================================
# G10 -- breakers are per-source
# ======================================================================

def test_fetch_logs_for_carries_no_circuit_breaker():
    """An es_breaker around the whole chain meant a Kubernetes outage tripped
    it and then blocked the healthy Elasticsearch fallback."""
    from src.tools import tool_registry

    source = tool_registry.fetch_logs_for
    wrapped = getattr(source, "__wrapped__", source)
    # tenacity's retry survives; pybreaker's decorator must not be present.
    assert "circuit" not in repr(source).lower()
    assert wrapped is not None


def test_elastic_source_owns_its_breaker(monkeypatch):
    """The breaker belongs where the failure is attributable."""
    import pybreaker

    from src.log_pipeline.sources import elastic
    from src.log_pipeline.types import FetchContext, TimeWindow

    breaker = pybreaker.CircuitBreaker(fail_max=2, reset_timeout=60)
    monkeypatch.setattr(elastic, "es_breaker", breaker)

    calls = []

    def failing_fetch(*_a, **_k):
        calls.append(1)
        raise RuntimeError("ES down")

    monkeypatch.setattr(elastic, "fetch_logs", failing_fetch)

    ctx = FetchContext(event_id="e1")
    source = elastic.ElasticLogSource()

    # Under the threshold the original exception propagates, which is what
    # keeps @retry_transient's type-based dispatch working.
    with pytest.raises(RuntimeError):
        source.fetch("e1", TimeWindow(hours=1), ctx)

    # At the threshold pybreaker opens and converts the error.
    with pytest.raises(pybreaker.CircuitBreakerError):
        source.fetch("e1", TimeWindow(hours=1), ctx)

    # Open: fails fast without touching Elasticsearch again. The chain reads
    # that as "this source produced nothing" and falls through to the next one
    # -- rather than the whole fetch being refused, which is what a breaker
    # wrapped around the entire chain did (G10).
    with pytest.raises(pybreaker.CircuitBreakerError):
        source.fetch("e1", TimeWindow(hours=1), ctx)
    assert len(calls) == 2, "an open breaker must not call the backend"


# ======================================================================
# G11 -- the rule cache outlives an open breaker
# ======================================================================

def test_a_cached_rule_is_served_while_the_db_breaker_is_open(monkeypatch):
    """The cache existed so a transient blip would not poison a reason code.
    Reading it inside the breaker undid exactly that."""
    from src.tools import tool_registry

    tool_registry._rule_cache.clear()
    tool_registry._rule_cache["RC"] = '[{"rule_data": "{}"}]'

    def must_not_be_called(_reason_code):
        raise AssertionError("a cache hit must not reach the protected path")

    monkeypatch.setattr(tool_registry, "_lookup_rule_uncached", must_not_be_called)

    assert tool_registry._lookup_rule_json("RC") == '[{"rule_data": "{}"}]'
    tool_registry._rule_cache.clear()


def test_a_cache_miss_still_goes_through_the_breaker(monkeypatch):
    from src.tools import tool_registry

    tool_registry._rule_cache.clear()
    calls = []
    monkeypatch.setattr(tool_registry, "_lookup_rule_uncached",
                        lambda rc: calls.append(rc) or '[{"rule_data": "{}"}]')

    tool_registry._lookup_rule_json("MISS")
    assert calls == ["MISS"]
    tool_registry._rule_cache.clear()


# ======================================================================
# G12 -- the retry prompt keeps the evidence
# ======================================================================

def test_retry_prompt_includes_the_logs():
    """The Reviewer's usual rejection is "you did not cite the evidence"; the
    retry then removed the evidence from the Investigator's context."""
    import src.core.agent_orchestrator as orch

    captured = {}

    def capture(messages_dict):
        captured["prompt"] = messages_dict["messages"][-1].content
        return {"messages": [SimpleNamespace(content="analysis",
                                             usage_metadata=None)]}

    agent = MagicMock()
    agent.invoke.side_effect = capture

    with patch.object(orch, "create_react_agent", side_effect=lambda *a, **k: agent), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        orch._agent = None
        graph = orch.get_agent()

    node = graph.nodes["investigate"].bound.func
    node({
        "payload": {"eventId": "e1"},
        "logs": "ERROR dedup rejected at 10:02",
        "db_rule": "rule text",
        "investigation": "my earlier analysis",
        "reviewer_feedback": "you did not cite the log evidence",
    })

    assert "you did not cite the log evidence" in captured["prompt"]
    assert "ERROR dedup rejected at 10:02" in captured["prompt"], \
        "the retry must carry the evidence it is being asked to cite"


# ======================================================================
# G13 / G14 -- one path definition; a live-read document cap
# ======================================================================

def test_paths_have_a_single_definition():
    from src.log_pipeline import config
    from src.utils import paths

    assert config.DRAIN3_STATE_DIR == paths.DRAIN3_STATE_DIR
    assert config.CATALOG_PATH == paths.CATALOG_PATH
    assert paths.CONSUMER_HEARTBEAT_PATH.parent == paths.LOCAL_CHECKPOINTS_DIR


def test_build_runbooks_derives_no_casesheets_path_at_all():
    """This used to assert `build_runbooks.CASESHEETS_DIR ==
    paths.LOCAL_CASESHEETS_DIR` -- one path, not two derivations (G13).

    The tool now reads casebooks through CasebookStorage and holds no path
    constant, which satisfies G13 more strongly than agreeing with it did: on
    the S3 backend the local path was correct AND useless, since walking it
    found nothing and the runbook learning loop silently produced no drafts.
    """
    import inspect

    from src.tools import build_runbooks

    assert not hasattr(build_runbooks, "CASESHEETS_DIR")
    source = inspect.getsource(build_runbooks)
    assert "LOCAL_CASESHEETS_DIR" not in source
    assert "list_events()" in source


def test_heartbeat_path_is_shared_between_api_and_consumer():
    """Two independent derivations agreed only by coincidence."""
    from src.utils import paths

    assert kc._heartbeat_path() == paths.CONSUMER_HEARTBEAT_PATH


def test_max_documents_is_read_at_call_time(monkeypatch):
    from src.log_pipeline import fetcher

    monkeypatch.setenv("LOG_MAX_DOCUMENTS", "123")
    assert fetcher._max_documents() == 123

    monkeypatch.setenv("LOG_MAX_DOCUMENTS", "not-a-number")
    assert fetcher._max_documents() == fetcher.LOG_MAX_DOCUMENTS


# ======================================================================
# N1 -- the promotion gate
# ======================================================================

def _shadow_outcome(verdict, agreed, reason_code="RC", runbook_id="RC__U"):
    return {
        "reason_code": reason_code,
        "enrolment_type": "U",
        "verdict": verdict,
        "resolution_source": "agent",
        "shadow_runbook_id": runbook_id,
        "shadow_agreed": agreed,
    }


def test_shadow_summary_scores_the_counterfactual():
    from src.utils.outcomes import summarise_shadow

    rows = summarise_shadow([
        _shadow_outcome("CORRECT", True),      # runbook would have been right
        _shadow_outcome("CORRECT", True),
        _shadow_outcome("CORRECT", False),     # agents right, runbook wrong
        _shadow_outcome("INCORRECT", True),    # both wrong
    ])["rows"]

    assert len(rows) == 1
    row = rows[0]
    assert row["shadowed"] == 4
    assert row["agreed"] == 3
    assert row["disagreed"] == 1
    assert row["verdicts"] == 4
    assert row["would_be_correct"] == 2
    assert row["agent_correct"] == 3
    assert row["would_be_accuracy"] == 0.5
    assert row["agent_accuracy"] == 0.75


def test_shadow_summary_ignores_unshadowed_outcomes():
    from src.utils.outcomes import summarise_shadow

    summary = summarise_shadow([
        {"reason_code": "RC", "verdict": "CORRECT", "resolution_source": "agent"},
    ])
    assert summary["rows"] == []
    assert summary["total_shadowed"] == 0


def test_shadow_summary_separates_runbooks():
    from src.utils.outcomes import summarise_shadow

    rows = summarise_shadow([
        _shadow_outcome("CORRECT", True, "RC1", "RC1__U"),
        _shadow_outcome("CORRECT", False, "RC2", "RC2__U"),
    ])["rows"]

    assert {r["runbook_id"] for r in rows} == {"RC1__U", "RC2__U"}


def test_shadow_report_names_the_promotion_verdict(capsys, monkeypatch):
    """The report must state the decision, not leave it to be inferred."""
    from src.tools import accuracy_report

    monkeypatch.setattr(accuracy_report, "iter_outcomes",
                        lambda: [_shadow_outcome("CORRECT", True) for _ in range(5)])
    monkeypatch.setattr("sys.argv", [
        "accuracy_report", "--shadow", "--min-verdicts", "3",
    ])
    accuracy_report.main()

    out = capsys.readouterr().out
    assert "READY" in out
    assert "RUNBOOK_SERVE_ALLOWLIST" in out


def test_shadow_report_withholds_promotion_on_a_regression(capsys, monkeypatch):
    from src.tools import accuracy_report

    outcomes = [_shadow_outcome("CORRECT", False) for _ in range(5)]
    monkeypatch.setattr(accuracy_report, "iter_outcomes", lambda: outcomes)
    monkeypatch.setattr("sys.argv", [
        "accuracy_report", "--shadow", "--min-verdicts", "3",
    ])
    accuracy_report.main()

    assert "DO NOT PROMOTE" in capsys.readouterr().out


def test_shadow_report_withholds_promotion_on_thin_evidence(capsys, monkeypatch):
    from src.tools import accuracy_report

    monkeypatch.setattr(accuracy_report, "iter_outcomes",
                        lambda: [_shadow_outcome("CORRECT", True)])
    monkeypatch.setattr("sys.argv", [
        "accuracy_report", "--shadow", "--min-verdicts", "20",
    ])
    accuracy_report.main()

    assert "NOT READY" in capsys.readouterr().out
