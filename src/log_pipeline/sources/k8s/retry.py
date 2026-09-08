"""
Retry policy for Kubernetes API calls (KUBERNETES_LOGS_PLAN.md 6.1).

`ApiException` must NOT be blanket-retried. Retrying a 403 burns the packet's
entire time budget on a call that can never succeed, and the packet is already
bounded by AGENT_INVOKE_TIMEOUT_SECONDS. The decision has to be made per
status code, which needs a predicate rather than the exception-type tuple
`retry_transient` uses.

    401  no   credentials invalid; will not self-heal
    403  no   RBAC misconfiguration; surfaced distinctly
    404  no   pod vanished; skip it and continue the loop
    400  no   expected on previous=True when no prior container exists
    410  no   log expired server-side
    429  yes  rate limited -- with jitter
    5xx  yes  transient server error
    conn yes  transient network
"""
import random
import time
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Status codes worth another attempt.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

#: Status codes that will never succeed on retry.
NON_RETRYABLE_STATUSES = frozenset({400, 401, 403, 404, 409, 410, 422})

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BASE_DELAY = 0.5
DEFAULT_MAX_DELAY = 8.0


def status_of(exception) -> Optional[int]:
    """HTTP status carried by a Kubernetes ApiException, if any."""
    status = getattr(exception, "status", None)
    if status is None:
        return None
    try:
        return int(status)
    except (TypeError, ValueError):
        return None


def should_retry(exception) -> bool:
    """True when retrying `exception` could plausibly succeed."""
    status = status_of(exception)

    if status is not None:
        if status in NON_RETRYABLE_STATUSES:
            return False
        if status in RETRYABLE_STATUSES:
            return True
        # Any other 4xx is a client error; only 5xx is worth another attempt.
        return status >= 500

    # No status: a transport-level failure (DNS, connection reset, timeout).
    import urllib3

    return isinstance(exception, (
        urllib3.exceptions.HTTPError,
        ConnectionError,
        TimeoutError,
        OSError,
    ))


def backoff_delay(attempt: int, base: float = DEFAULT_BASE_DELAY,
                  maximum: float = DEFAULT_MAX_DELAY) -> float:
    """Exponential backoff with full jitter.

    Jitter is not optional here: five worker threads retrying a 429 in
    lockstep is a self-inflicted thundering herd against an API server that
    has already told us to slow down.
    """
    ceiling = min(maximum, base * (2 ** max(0, attempt - 1)))
    return random.uniform(0, ceiling)


def call_with_retry(func, *args, _max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                    _sleep=time.sleep, **kwargs):
    """Invoke `func`, retrying only what is worth retrying.

    The two policy parameters are underscore-prefixed because every call site
    splats a caller-built dict of Kubernetes API keyword arguments in here. A
    plain `max_attempts` or `sleep` among them would silently rebind the retry
    policy instead of reaching the API. The underscore convention is the
    kubernetes client's own, for exactly this reason -- see `_request_timeout`
    and `_preload_content`.
    """
    max_attempts = _max_attempts
    sleep = _sleep
    last_exception = None

    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 -- predicate decides
            last_exception = e
            if not should_retry(e) or attempt == max_attempts:
                raise
            delay = backoff_delay(attempt)
            logger.warning(
                "Retrying Kubernetes API call",
                attempt=attempt,
                max_attempts=max_attempts,
                status=status_of(e),
                delay_seconds=round(delay, 3),
                error=f"{type(e).__name__}: {e}",
            )
            sleep(delay)

    raise last_exception
