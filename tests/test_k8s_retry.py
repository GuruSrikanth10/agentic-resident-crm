"""
Phase 7 of KUBERNETES_LOGS_PLAN.md -- resilience and bounds.

The retry predicate carries real cost: retrying a 403 burns the packet's
entire time budget on a call that can never succeed, while failing to retry a
429 throws away a fetch that would have worked a second later. One test per
row of the plan's 6.1 table.
"""
import time
from unittest.mock import patch

import pytest
from kubernetes.client.exceptions import ApiException

from src.log_pipeline.sources.k8s import retrieval
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.sources.k8s.retry import (
    backoff_delay,
    call_with_retry,
    should_retry,
    status_of,
)
from src.log_pipeline.types import GapType, TimeWindow


# ======================================================================
# The 6.1 status-code table
# ======================================================================

@pytest.mark.parametrize("status,expected,why", [
    (400, False, "expected on previous=True when no prior container exists"),
    (401, False, "credentials invalid; will not self-heal"),
    (403, False, "RBAC misconfiguration"),
    (404, False, "pod vanished"),
    (409, False, "conflict; will not self-heal"),
    (410, False, "log expired server-side"),
    (422, False, "unprocessable"),
    (429, True, "rate limited"),
    (500, True, "transient server error"),
    (502, True, "transient server error"),
    (503, True, "transient server error"),
    (504, True, "transient server error"),
])
def test_retry_decision_per_status(status, expected, why):
    assert should_retry(ApiException(status=status)) is expected, why


def test_unlisted_4xx_is_not_retried():
    """A client error we have not enumerated is still a client error."""
    assert should_retry(ApiException(status=418)) is False


def test_unlisted_5xx_is_retried():
    assert should_retry(ApiException(status=599)) is True


def test_transport_errors_are_retried():
    import urllib3
    assert should_retry(urllib3.exceptions.HTTPError("reset")) is True
    assert should_retry(ConnectionError("refused")) is True
    assert should_retry(TimeoutError("slow")) is True


def test_unrelated_exception_is_not_retried():
    assert should_retry(ValueError("programming error")) is False


def test_status_of_handles_missing_and_malformed():
    assert status_of(ValueError("x")) is None
    bad = ApiException()
    bad.status = "not-a-number"
    assert status_of(bad) is None


# ======================================================================
# Backoff
# ======================================================================

def test_backoff_grows_and_is_capped():
    ceilings = [max(backoff_delay(a, base=1.0, maximum=8.0) for _ in range(50))
                for a in range(1, 6)]
    assert ceilings[0] <= 1.0 + 1e-9
    assert all(c <= 8.0 + 1e-9 for c in ceilings)
    assert ceilings[-1] > ceilings[0]


def test_backoff_is_jittered():
    """Five workers retrying a 429 in lockstep is a self-inflicted
    thundering herd against a server already asking us to slow down."""
    samples = {backoff_delay(3, base=1.0, maximum=8.0) for _ in range(50)}
    assert len(samples) > 1


# ======================================================================
# call_with_retry
# ======================================================================

def test_retries_then_succeeds():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ApiException(status=503)
        return "ok"

    assert call_with_retry(flaky, _sleep=lambda _s: None) == "ok"
    assert calls["n"] == 3


def test_non_retryable_fails_on_the_first_attempt():
    calls = {"n": 0}

    def denied():
        calls["n"] += 1
        raise ApiException(status=403)

    with pytest.raises(ApiException):
        call_with_retry(denied, _sleep=lambda _s: None)

    assert calls["n"] == 1, "a 403 must not be retried"


def test_gives_up_after_max_attempts():
    calls = {"n": 0}

    def always_down():
        calls["n"] += 1
        raise ApiException(status=503)

    with pytest.raises(ApiException):
        call_with_retry(always_down, _max_attempts=4, _sleep=lambda _s: None)

    assert calls["n"] == 4


def test_no_sleep_on_success():
    slept = []
    call_with_retry(lambda: "fine", _sleep=slept.append)
    assert slept == []


# ======================================================================
# Circuit breaker
# ======================================================================

def test_k8s_breaker_exists_and_matches_the_others():
    from src.utils.resilience import es_breaker, k8s_breaker
    assert k8s_breaker is not es_breaker
    assert k8s_breaker.fail_max == es_breaker.fail_max


# ======================================================================
# Wall-clock deadline
# ======================================================================

def _target(name):
    return PodTarget(namespace="enu", pod_name=name, container="app")


def test_deadline_produces_partial_results_and_a_gap(monkeypatch):
    """A slow cluster must degrade to partial results, not consume the
    packet's whole AGENT_INVOKE_TIMEOUT_SECONDS budget."""
    monkeypatch.setenv("K8S_TOTAL_FETCH_TIMEOUT_SECONDS", "0.15")
    monkeypatch.setenv("K8S_FETCH_CONCURRENCY", "2")

    def slow_read(target, window, selector, allowlist=None):
        if target.pod_name == "fast":
            return retrieval.PodFetchOutcome(target=target, records=[])
        time.sleep(1.0)
        return retrieval.PodFetchOutcome(target=target, records=[])

    with patch.object(retrieval, "read_pod_logs", side_effect=slow_read):
        outcome = retrieval.read_all(
            [_target("fast"), _target("slow-1"), _target("slow-2")],
            TimeWindow.default(),
        )

    assert any(g.gap_type == GapType.TRUNCATED for g in outcome.gaps)
    assert any("budget expired" in g.detail for g in outcome.gaps)


def test_no_deadline_gap_when_everything_finishes(monkeypatch):
    monkeypatch.setenv("K8S_TOTAL_FETCH_TIMEOUT_SECONDS", "30")

    def quick_read(target, window, selector, allowlist=None):
        return retrieval.PodFetchOutcome(target=target, records=[])

    with patch.object(retrieval, "read_pod_logs", side_effect=quick_read):
        outcome = retrieval.read_all([_target("a"), _target("b")], TimeWindow.default())

    assert outcome.gaps == []
    assert outcome.pods_queried == 2
