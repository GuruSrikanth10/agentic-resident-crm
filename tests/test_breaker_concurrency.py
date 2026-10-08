"""
utils/resilience.py -- circuit breakers that do not serialise their calls, and
the deliberate limit on concurrent model calls that replaced the accidental one.

pybreaker 1.4.1 runs the guarded call while holding the breaker's lock, so
every agent call in the process -- `llm_breaker` wraps all of them -- ran one
at a time, and a traced packet spent 9m23s of 12m45s waiting before its agents
could start (2026-10-07). These pin both halves of the fix: calls overlap, and
the breaker still trips, refuses, recovers and ignores excluded exceptions
exactly as pybreaker does.
"""
import threading
import time

import pybreaker
import pytest

from src.core import run_control
from src.core.run_control import RunCancelled
from src.utils import resilience
from src.utils.resilience import CircuitBreaker


def _overlap(fn, workers: int = 3, seconds: float = 0.3) -> float:
    """Run `fn(seconds)` on `workers` threads at once; the wall time."""
    threads = [threading.Thread(target=fn, args=(seconds,)) for _ in range(workers)]
    started = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return time.monotonic() - started


# ======================================================================
# The breaker
# ======================================================================

def test_every_shared_breaker_is_the_non_serialising_one():
    for name in ("db_breaker", "process_db_breaker", "es_breaker", "llm_breaker",
                 "k8s_breaker", "bitbucket_breaker"):
        assert isinstance(getattr(resilience, name), CircuitBreaker), name


def test_calls_through_one_breaker_run_at_the_same_time():
    breaker = CircuitBreaker(fail_max=3, reset_timeout=60)

    @breaker
    def slow_call(seconds):
        time.sleep(seconds)

    # Three 0.3s calls: 0.9s one after another, about 0.3s side by side.
    assert _overlap(slow_call) < 0.6


def test_pybreakers_own_breaker_did_serialise_them():
    """The defect, pinned against the library, so an upgrade that fixes it
    upstream is noticed and this subclass can be retired."""
    breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)

    @breaker
    def slow_call(seconds):
        time.sleep(seconds)

    assert _overlap(slow_call) >= 0.85


def test_it_trips_at_fail_max_and_then_refuses_without_calling():
    breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
    calls = []

    def failing():
        calls.append(1)
        raise ValueError("down")

    for _ in range(2):
        with pytest.raises(ValueError):
            breaker.call(failing)
    # The failure that trips it raises CircuitBreakerError, as in pybreaker.
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(failing)
    assert breaker.current_state == pybreaker.STATE_OPEN

    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(failing)
    assert len(calls) == 3


def test_a_success_resets_the_failure_count():
    breaker = CircuitBreaker(fail_max=2, reset_timeout=60)
    with pytest.raises(ValueError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    assert breaker.fail_counter == 1
    assert breaker.call(lambda: "ok") == "ok"
    assert breaker.fail_counter == 0


def test_after_the_timeout_a_trial_that_succeeds_closes_it():
    breaker = CircuitBreaker(fail_max=1, reset_timeout=0.2)
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(lambda: "refused")
    time.sleep(0.25)
    assert breaker.call(lambda: "trial") == "trial"
    assert breaker.current_state == pybreaker.STATE_CLOSED


def test_a_trial_that_fails_opens_it_again():
    breaker = CircuitBreaker(fail_max=1, reset_timeout=0.2)
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    time.sleep(0.25)
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("still down")))
    assert breaker.current_state == pybreaker.STATE_OPEN


def test_calls_arriving_during_the_trial_go_ahead_instead_of_queueing():
    breaker = CircuitBreaker(fail_max=1, reset_timeout=0.1)
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    time.sleep(0.15)

    @breaker
    def slow_call(seconds):
        time.sleep(seconds)

    # The first is the trial; the others arrive while it runs and are let
    # through beside it rather than waiting for it.
    assert _overlap(slow_call) < 0.6
    assert breaker.current_state == pybreaker.STATE_CLOSED


def test_a_call_already_running_when_it_opens_finishes():
    breaker = CircuitBreaker(fail_max=1, reset_timeout=60)
    started, result = threading.Event(), {}

    def slow_success():
        started.set()
        time.sleep(0.3)
        return "done"

    worker = threading.Thread(target=lambda: result.update(value=breaker.call(slow_success)))
    worker.start()
    started.wait(1)
    with pytest.raises(pybreaker.CircuitBreakerError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    worker.join()
    assert result["value"] == "done"


def test_an_excluded_exception_never_counts():
    breaker = CircuitBreaker(fail_max=2, reset_timeout=60)
    breaker.add_excluded_exception(RunCancelled)
    for _ in range(5):
        with pytest.raises(RunCancelled):
            breaker.call(lambda: (_ for _ in ()).throw(RunCancelled("abandoned")))
    assert breaker.current_state == pybreaker.STATE_CLOSED


@pytest.mark.parametrize("breaker_class", [CircuitBreaker, pybreaker.CircuitBreaker])
def test_a_generator_is_counted_as_it_is_consumed(breaker_class):
    """Exactly as pybreaker does: the failure mid-stream is counted when it
    happens, and the one that trips the breaker surfaces as
    CircuitBreakerError."""
    breaker = breaker_class(fail_max=1, reset_timeout=60)

    def generator():
        yield 1
        raise ValueError("mid-stream")

    stream = breaker.call(generator)
    assert next(stream) == 1
    assert breaker.current_state == pybreaker.STATE_CLOSED
    with pytest.raises(pybreaker.CircuitBreakerError):
        next(stream)
    assert breaker.current_state == pybreaker.STATE_OPEN


# ======================================================================
# The model-call slots
# ======================================================================

@pytest.fixture
def slots(monkeypatch):
    """Model-call slots sized afresh for the test, and forgotten after."""
    def size(limit):
        monkeypatch.setenv("LLM_MAX_CONCURRENT_CALLS", str(limit))
        resilience.reset_model_call_slots()
    yield size
    resilience.reset_model_call_slots()


@pytest.mark.parametrize("raw, expected", [
    ("", 2), ("4", 4), ("0", 2), ("-1", 2), ("lots", 2), (" 3 ", 3),
])
def test_the_limit_is_read_forgivingly(monkeypatch, raw, expected):
    monkeypatch.setenv("LLM_MAX_CONCURRENT_CALLS", raw)
    assert resilience.max_concurrent_model_calls() == expected


def test_no_more_model_calls_than_the_limit_run_at_once(slots):
    slots(2)
    running, peak, lock = [0], [0], threading.Lock()

    def model_call(seconds):
        with resilience.model_call_slot():
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            time.sleep(seconds)
            with lock:
                running[0] -= 1

    elapsed = _overlap(model_call, workers=4, seconds=0.2)
    assert peak[0] == 2
    assert elapsed >= 0.35   # two rounds of two


def test_a_run_abandoned_while_waiting_stops_waiting(slots):
    slots(1)
    resilience.acquire_model_call_slot()
    try:
        cancel = threading.Event()
        cancel.set()
        started = time.monotonic()
        with run_control.scope(cancel), pytest.raises(RunCancelled):
            resilience.acquire_model_call_slot()
        assert time.monotonic() - started < 2.5
    finally:
        resilience.release_model_call_slot()


def test_every_agent_takes_a_slot_for_each_model_call():
    from src.core import agent_factory

    limits = agent_factory._limits()
    kinds = [type(middleware).__name__ for middleware in limits]
    # After cancellation, so an abandoned run never waits for a slot.
    assert kinds[:2] == ["CancellationMiddleware", "ModelCallSlotMiddleware"]


def test_the_middleware_holds_a_slot_only_for_the_call(slots):
    from src.core.agent_factory import ModelCallSlotMiddleware

    slots(1)
    middleware = ModelCallSlotMiddleware()
    assert middleware.wrap_model_call("request", lambda request: f"answer to {request}") \
        == "answer to request"
    # Released: the one slot can be taken again at once.
    assert resilience._slots().acquire(blocking=False)
    resilience.release_model_call_slot()


def test_the_async_middleware_releases_its_slot(slots):
    import asyncio

    from src.core.agent_factory import ModelCallSlotMiddleware

    slots(1)

    async def handler(request):
        return "async answer"

    assert asyncio.run(ModelCallSlotMiddleware().awrap_model_call("r", handler)) \
        == "async answer"
    assert resilience._slots().acquire(blocking=False)
    resilience.release_model_call_slot()
