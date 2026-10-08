import asyncio
import contextlib
import os
import threading
import time
import types
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, TypeVar, cast

import pybreaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
import urllib3.exceptions
from elasticsearch import ConnectionError as ESConnectionError
from sqlalchemy.exc import OperationalError

from src.core import run_control
from src.core.run_control import RunCancelled

T = TypeVar("T")


class CircuitBreaker(pybreaker.CircuitBreaker):
    """pybreaker's circuit breaker, without its lock around the guarded call.

    pybreaker (1.4.1, its latest release) runs the guarded function while
    holding the breaker's lock, so every call through a breaker waited for
    the call before it to finish. Each breaker below is one module-level
    object shared by every thread: the API made one LLM call at a time however
    many investigations it ran -- `llm_breaker` wraps every agent call in both
    lanes -- and log fetches, rule lookups and Bitbucket reads queued the same
    way. One traced packet spent 9m23s of its 12m45s waiting for the lock
    before its agents could start (2026-10-07), time that counted against the
    server-side budget like work.

    Here the lock covers only the breaker's own bookkeeping: whether a call
    may go ahead, and how it ended. That bookkeeping is pybreaker's own -- its
    state objects count a failure, trip the breaker at `fail_max` (raising
    CircuitBreakerError on the call that trips it), treat an excluded
    exception as a success and notify listeners -- so everything but the
    waiting is as it was. What not waiting changes:

    - Calls already running when the breaker opens finish as they would have;
      only new calls are refused.
    - Once `reset_timeout` has passed, calls that arrive while the trial call
      is still running go ahead too, instead of queueing behind it. The first
      of them to succeed closes the breaker; the first to fail opens it again.
    """

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        with self._lock:
            self._admit(func, *args, **kwargs)
        try:
            result = func(*args, **kwargs)
        except BaseException as error:
            with self._lock:
                # Raises `error` -- or CircuitBreakerError, when this failure
                # is the one that trips the breaker.
                self.state._handle_error(error)
            raise
        if isinstance(result, types.GeneratorType):
            # pybreaker records a generator's outcome as it is consumed.
            return cast(T, self.state.generator_call(result))
        with self._lock:
            self.state._handle_success()
        return result

    def _admit(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        """Refuse a call while the breaker is open, and half-open it once
        `reset_timeout` has passed -- pybreaker's CircuitOpenState.before_call,
        without making the call inside the lock. Caller holds `_lock`."""
        if self.current_state == pybreaker.STATE_OPEN:
            opened_at = self._state_storage.opened_at
            if opened_at and datetime.now(timezone.utc) < opened_at + timedelta(
                    seconds=self.reset_timeout):
                raise pybreaker.CircuitBreakerError(
                    "Timeout not elapsed yet, circuit breaker still open")
            self.half_open()
        self.state.before_call(func, *args, **kwargs)
        for listener in self.listeners:
            listener.before_call(self, func, *args, **kwargs)


# Circuit breaker trips after 3 consecutive failures, resets after 60 seconds
db_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
# The enu-biometric process DB (src/tools/agent_tools) is a different MySQL
# from the rules DB db_breaker guards; sharing one breaker would let an outage
# of either refuse lookups against the other. It is the `process` database's
# breaker in the tools' database layer (agent_tools/_database.py), which
# gives every other database key a breaker of its own.
process_db_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
es_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
llm_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
# An abandoned run stopping itself says nothing about the LLM's health. Counted
# as a failure, three timeouts in a row would open the circuit for every other
# packet (src/core/run_control.py).
llm_breaker.add_excluded_exception(RunCancelled)
# The Kubernetes source retries per-status rather than per-exception-type
# (see log_pipeline/sources/k8s/retry.py); the breaker still guards against a
# cluster that is down entirely.
k8s_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)
# Bitbucket reads for the DLT replay precheck (DLT_PLAN.md 14, phase C4). A
# repository server that is down must degrade a verdict to UNKNOWN, not stall
# the analysis lane behind repeated timeouts.
bitbucket_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)


#: Model calls one process makes at once when LLM_MAX_CONCURRENT_CALLS is
#: unset or unusable.
DEFAULT_MAX_CONCURRENT_MODEL_CALLS = 2

_model_call_slots = None
_model_call_slots_lock = threading.Lock()


def max_concurrent_model_calls() -> int:
    """LLM_MAX_CONCURRENT_CALLS: how many model calls this process makes at
    once, across every agent of both lanes. A typo must not stop every call,
    so an unusable value falls back to the default."""
    raw = os.environ.get("LLM_MAX_CONCURRENT_CALLS", "")
    try:
        value = int(raw.strip())
    except (TypeError, ValueError):
        return DEFAULT_MAX_CONCURRENT_MODEL_CALLS
    return value if value > 0 else DEFAULT_MAX_CONCURRENT_MODEL_CALLS


def _slots() -> threading.BoundedSemaphore:
    """The process's model-call slots, sized on first use."""
    global _model_call_slots
    if _model_call_slots is None:
        with _model_call_slots_lock:
            if _model_call_slots is None:
                _model_call_slots = threading.BoundedSemaphore(max_concurrent_model_calls())
    return _model_call_slots


def acquire_model_call_slot() -> None:
    """Take one of the process's model-call slots, waiting while all are taken.

    The limit is what `llm_breaker` used to impose by accident at one, made
    deliberate and sized by LLM_MAX_CONCURRENT_CALLS, so the model server is
    never sent more than that many calls at once by one pod. A wait is
    recorded in the packet's trace as `wait-for-model-slot`, never left as an
    unexplained gap, and an abandoned run stops waiting within a second
    (run_control).
    """
    slots = _slots()
    if slots.acquire(blocking=False):
        return
    from src.utils import tracing

    with tracing.step("wait-for-model-slot",
                      metadata={"limit": max_concurrent_model_calls()}) as step:
        started = time.monotonic()
        while not slots.acquire(timeout=1.0):
            run_control.check()
        step.update(output={"waited_seconds": round(time.monotonic() - started, 3)})


async def acquire_model_call_slot_async() -> None:
    """`acquire_model_call_slot` for an agent run on an event loop. It polls
    rather than blocking a thread on the semaphore: a cancelled task must not
    leave a thread that takes a slot nobody will release."""
    slots = _slots()
    if slots.acquire(blocking=False):
        return
    from src.utils import tracing

    with tracing.step("wait-for-model-slot",
                      metadata={"limit": max_concurrent_model_calls()}) as step:
        started = time.monotonic()
        while not slots.acquire(blocking=False):
            run_control.check()
            await asyncio.sleep(0.05)
        step.update(output={"waited_seconds": round(time.monotonic() - started, 3)})


def release_model_call_slot() -> None:
    _slots().release()


@contextlib.contextmanager
def model_call_slot():
    """Hold one model-call slot for the block (`acquire_model_call_slot`)."""
    acquire_model_call_slot()
    try:
        yield
    finally:
        release_model_call_slot()


def reset_model_call_slots() -> None:
    """Forget the slots, so the next call sizes them afresh. For tests."""
    global _model_call_slots
    with _model_call_slots_lock:
        _model_call_slots = None

# langchain-openai raises openai/httpx exceptions on transient failures, none
# of which are requests/urllib3/ES/SQLAlchemy errors. Without these, LLM
# calls were never actually retried: the first blip propagated straight to
# the circuit breaker (0.7). Imported lazily so this module still loads
# without the openai/httpx extras installed.
_LLM_TRANSIENT_EXCEPTIONS = ()
try:
    import openai
    _LLM_TRANSIENT_EXCEPTIONS += (
        openai.APIConnectionError,
        openai.APITimeoutError,
        openai.RateLimitError,
        openai.InternalServerError,
    )
except ImportError:
    pass

try:
    import httpx
    _LLM_TRANSIENT_EXCEPTIONS += (httpx.TransportError,)
except ImportError:
    pass

# Transient exceptions to retry
TRANSIENT_EXCEPTIONS = (
    requests.exceptions.RequestException,
    urllib3.exceptions.HTTPError,
    ESConnectionError,
    OperationalError,
    TimeoutError,
) + _LLM_TRANSIENT_EXCEPTIONS

def get_retry_decorator():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(TRANSIENT_EXCEPTIONS)
    )

retry_transient = get_retry_decorator()
