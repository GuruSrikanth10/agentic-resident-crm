import pybreaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
import urllib3.exceptions
from elasticsearch import ConnectionError as ESConnectionError
from sqlalchemy.exc import OperationalError

# Circuit breaker trips after 3 consecutive failures, resets after 60 seconds
db_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
es_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
llm_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
# The Kubernetes source retries per-status rather than per-exception-type
# (see log_pipeline/sources/k8s/retry.py); the breaker still guards against a
# cluster that is down entirely.
k8s_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)
# Bitbucket reads for the DLT replay precheck (DLT_PLAN.md 14, phase C4). A
# repository server that is down must degrade a verdict to UNKNOWN, not stall
# the analysis lane behind repeated timeouts.
bitbucket_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)

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
