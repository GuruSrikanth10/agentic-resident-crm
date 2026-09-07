"""
Phase C regression tests (ENHANCEMENT_PLAN.md section 5).

F7  -- the wall-clock fetch deadline actually bounds the fan-out.
F8  -- Kubernetes API calls retry 429/5xx and are guarded by a breaker.
F11 -- refId/srn reach the identifier filter and the redaction allowlist.
F14 -- the live-DB query uses SQLAlchemy named binds.
F15 -- both TTL caches survive concurrent access.
"""
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.log_pipeline.sources.k8s import discovery, filtering, retrieval
from src.log_pipeline.types import TimeWindow


# ======================================================================
# F7 -- the deadline bounds the fan-out
# ======================================================================

def _slow_target(name):
    return discovery.PodTarget(
        namespace="ns", pod_name=name, container="app", restart_count=0
    )


def test_fetch_deadline_actually_bounds_the_fanout(monkeypatch):
    """The core F7 bug.

    `with ThreadPoolExecutor(...)` calls shutdown(wait=True) on exit, so the
    deadline used to record a TRUNCATED gap and then block for every slow pod
    anyway -- consuming the packet's whole AGENT_INVOKE_TIMEOUT_SECONDS.
    """
    budget = 0.4
    pod_read_seconds = 3.0

    monkeypatch.setattr(retrieval, "_total_fetch_timeout", lambda: budget)
    monkeypatch.setattr(retrieval, "_concurrency", lambda: 4)

    def slow_read(target, window, selector, allowlist):
        time.sleep(pod_read_seconds)
        return retrieval.PodFetchOutcome(target=target)

    monkeypatch.setattr(retrieval, "read_pod_logs", slow_read)

    targets = [_slow_target(f"pod-{i}") for i in range(4)]

    started = time.monotonic()
    outcome = retrieval.read_all(targets, TimeWindow(hours=1))
    elapsed = time.monotonic() - started

    # Must return near the budget, NOT after the slowest pod.
    assert elapsed < pod_read_seconds, (
        f"read_all took {elapsed:.2f}s; the {budget}s deadline did not bound it"
    )
    # And it must say so rather than silently returning a partial trace.
    assert any(gap.gap_type.value == "TRUNCATED" for gap in outcome.gaps)


def test_deadline_fires_even_when_no_future_completes(monkeypatch):
    """Checking the clock only inside the as_completed loop means a fan-out
    where nothing finishes never evaluates the deadline at all."""
    monkeypatch.setattr(retrieval, "_total_fetch_timeout", lambda: 0.3)
    monkeypatch.setattr(retrieval, "_concurrency", lambda: 2)
    monkeypatch.setattr(
        retrieval, "read_pod_logs",
        lambda *_a, **_kw: (time.sleep(5), retrieval.PodFetchOutcome(target=None))[1],
    )

    started = time.monotonic()
    outcome = retrieval.read_all([_slow_target("a"), _slow_target("b")],
                                 TimeWindow(hours=1))
    assert time.monotonic() - started < 3
    assert any(gap.gap_type.value == "TRUNCATED" for gap in outcome.gaps)


def test_fast_fetch_still_returns_everything(monkeypatch):
    """The deadline must not truncate a healthy fetch."""
    monkeypatch.setattr(retrieval, "_total_fetch_timeout", lambda: 5.0)
    monkeypatch.setattr(retrieval, "_concurrency", lambda: 4)
    monkeypatch.setattr(
        retrieval, "read_pod_logs",
        lambda target, *_a, **_kw: retrieval.PodFetchOutcome(target=target),
    )

    targets = [_slow_target(f"pod-{i}") for i in range(4)]
    outcome = retrieval.read_all(targets, TimeWindow(hours=1))

    assert outcome.pods_queried == 4
    assert not any(gap.gap_type.value == "TRUNCATED" for gap in outcome.gaps)


# ======================================================================
# F8 -- retries and breaker are actually wired in
# ======================================================================

class _ApiError(Exception):
    def __init__(self, status):
        super().__init__(f"status={status}")
        self.status = status


def test_pod_log_read_retries_a_429(monkeypatch):
    """K8S_FETCH_CONCURRENCY workers against one API server make 429 the
    expected failure, and the retry policy was never wired in (F8)."""
    calls = []

    def flaky(**kwargs):
        calls.append(kwargs)
        if len(calls) < 3:
            raise _ApiError(429)
        return MagicMock(stream=lambda *_a, **_kw: iter([b"ok\n"]))

    api = MagicMock()
    api.read_namespaced_pod_log.side_effect = flaky

    monkeypatch.setattr(retrieval.k8s_client_module, "get_client", lambda: api)
    monkeypatch.setattr(retrieval.fixtures, "is_active", lambda: False)
    monkeypatch.setattr(retrieval.retry, "backoff_delay", lambda *_a, **_kw: 0)

    list(retrieval._open_stream(_slow_target("p"), TimeWindow(hours=1), previous=False))
    assert len(calls) == 3


def test_pod_log_read_does_not_retry_a_403(monkeypatch):
    """Retrying an RBAC denial burns the packet's whole budget on a call that
    can never succeed."""
    calls = []

    def denied(**kwargs):
        calls.append(kwargs)
        raise _ApiError(403)

    api = MagicMock()
    api.read_namespaced_pod_log.side_effect = denied

    monkeypatch.setattr(retrieval.k8s_client_module, "get_client", lambda: api)
    monkeypatch.setattr(retrieval.fixtures, "is_active", lambda: False)

    with pytest.raises(_ApiError):
        retrieval._open_stream(_slow_target("p"), TimeWindow(hours=1), previous=False)
    assert len(calls) == 1


def test_namespace_verify_retries_a_503(monkeypatch):
    calls = []

    def flaky(**kwargs):
        calls.append(kwargs)
        if len(calls) < 2:
            raise _ApiError(503)
        return MagicMock()

    api = MagicMock()
    api.read_namespace.side_effect = flaky

    monkeypatch.setattr(discovery.k8s_client_module, "get_client", lambda: api)
    monkeypatch.setattr(discovery.fixtures, "is_active", lambda: False)
    monkeypatch.setattr(discovery.retry, "backoff_delay", lambda *_a, **_kw: 0)

    ok, reason = discovery._verify_namespace("ns", 5.0)
    assert ok is True
    assert len(calls) == 2


def test_open_breaker_fails_fast_without_touching_the_cluster(monkeypatch):
    """A cluster that is down entirely must not cost every packet a full
    discovery + fan-out timeout."""
    import pybreaker
    from src.log_pipeline.sources.k8s.source import KubernetesLogSource
    from src.log_pipeline.types import FetchContext

    source = KubernetesLogSource()
    called = []

    def should_not_run(*_a, **_kw):
        called.append(1)

    monkeypatch.setattr(source, "_fetch", should_not_run)

    with patch("src.log_pipeline.sources.k8s.source.k8s_breaker") as breaker:
        breaker.call.side_effect = pybreaker.CircuitBreakerError("open")
        result = source.fetch("evt", TimeWindow(hours=1), FetchContext(event_id="evt"))

    assert result.ok is False
    assert not called


# ======================================================================
# F11 -- identifiers reach the filter
# ======================================================================

def test_identifiers_are_pulled_from_the_payload():
    payload = {
        "eventId": "evt-1",
        "packetMetaData": {"refId": "ref-9", "srn": "srn-3"},
    }
    assert filtering.identifiers_from_payload(payload) == ("evt-1", "ref-9")


def test_search_fields_are_configurable(monkeypatch):
    """K8S_SEARCH_FIELDS was documented in .env.example and read nowhere."""
    monkeypatch.setenv("K8S_SEARCH_FIELDS", "eventId,refId,srn")
    payload = {
        "eventId": "evt-1",
        "packetMetaData": {"refId": "ref-9", "srn": "srn-3"},
    }
    assert filtering.identifiers_from_payload(payload) == ("evt-1", "ref-9", "srn-3")


def test_missing_fields_are_skipped_not_emitted_as_none():
    payload = {"eventId": "evt-1", "packetMetaData": {}}
    assert filtering.identifiers_from_payload(payload) == ("evt-1",)


def test_empty_payload_yields_no_identifiers():
    assert filtering.identifiers_from_payload({}) == ()
    assert filtering.identifiers_from_payload(None) == ()


def test_a_line_mentioning_only_refid_is_matched():
    """The failure F11 describes: if services log refId rather than eventId,
    the Kubernetes source silently returned nothing."""
    selector = filtering.build_selector("evt-1", ["ref-9"])
    emitted = selector.feed("processing packet refId=ref-9 stage=BIO")
    assert emitted == [("processing packet refId=ref-9 stage=BIO", True)]


def test_fetch_logs_node_passes_identifiers_through(monkeypatch):
    """End-to-end plumbing: payload -> fetch_logs_for -> reduce_logs."""
    from src.tools import tool_registry

    seen = {}

    def fake_reduce(event_id, extra_identifiers=()):
        seen["event_id"] = event_id
        seen["extra"] = tuple(extra_identifiers)
        return "logs"

    monkeypatch.setattr("src.log_pipeline.pipeline.reduce_logs", fake_reduce)
    tool_registry.fetch_logs_for("evt-1", extra_identifiers=("ref-9",))

    assert seen == {"event_id": "evt-1", "extra": ("ref-9",)}


# ======================================================================
# F14 -- live DB query round-trips against a real SQLAlchemy engine
# ======================================================================

def test_live_db_query_uses_named_binds(monkeypatch):
    """The old "%s" + tuple form is DBAPI paramstyle and fails against a
    SQLAlchemy 2.x Engine. Exercised here with SQLite in memory."""
    from sqlalchemy import create_engine

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        from sqlalchemy import text as sa_text
        conn.execute(sa_text(
            "CREATE TABLE rules (rule_id TEXT, reject_reason_code TEXT, rule_data TEXT)"
        ))
        conn.execute(sa_text(
            "INSERT INTO rules VALUES ('R1', 'DEDUP_REJECT', '{}')"
        ))

    from src.tools import tool_registry
    monkeypatch.setattr(tool_registry, "get_live_db_engine", lambda: engine)
    monkeypatch.setenv("USE_MOCK_DB", "false")

    result = tool_registry._lookup_rule_by_reason_code_impl("DEDUP_REJECT")
    assert "R1" in result
    assert "Failed to query live DB" not in result


def test_live_db_engine_is_configured_with_pre_ping(monkeypatch):
    """Without pre-ping, the first query after MySQL's wait_timeout fails."""
    from src.tools import tool_registry

    captured = {}

    def fake_create_engine(url, **kwargs):
        captured.update(kwargs)
        return MagicMock()

    monkeypatch.setattr(tool_registry, "_LIVE_DB_ENGINE", None)
    monkeypatch.setattr(tool_registry, "create_engine", fake_create_engine)
    tool_registry.get_live_db_engine()
    monkeypatch.setattr(tool_registry, "_LIVE_DB_ENGINE", None)

    assert captured.get("pool_pre_ping") is True
    assert captured.get("pool_recycle")


# ======================================================================
# F15 -- TTL caches under concurrent access
# ======================================================================

def test_rule_cache_survives_concurrent_access(monkeypatch):
    """TTLCache is not thread-safe; expiry mutates internal state on read."""
    from src.tools import tool_registry

    monkeypatch.setattr(
        tool_registry, "_lookup_rule_by_reason_code_impl",
        lambda reason_code: f'[{{"rule_id": "{reason_code}"}}]',
    )

    errors = []

    def hammer(worker):
        try:
            for i in range(200):
                tool_registry._lookup_rule_json(f"code-{i % 20}")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=hammer, args=(w,)) for w in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"concurrent cache access raised: {errors[:3]}"


def test_runbook_cache_survives_concurrent_access():
    from src.utils import runbook_store

    errors = []

    def hammer():
        try:
            for i in range(200):
                key = runbook_store.runbook_cache_key(f"C{i % 20}", "E")
                with runbook_store._runbook_cache_lock:
                    runbook_store._runbook_cache[key] = {"v": i}
                    runbook_store._runbook_cache.get(key)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=hammer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
