"""
Metrics (ENHANCEMENT_PLAN.md section 4.5).

`types.py` stated the problem plainly: "there is no metrics backend in this
project." FetchDiagnostics was constructed on every fetch and discarded, so
none of p95 packet latency, token spend, retry-rate, runbook hit rate,
log-source win rate, breaker trips, or evidence-gap rate was knowable.

prometheus_client is an OPTIONAL dependency. If it isn't installed every
helper here degrades to a no-op and /metrics returns 501 -- an observability
gap must never take the pipeline down with it.
"""
from typing import Optional

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
    METRICS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by deployments without it
    CONTENT_TYPE_LATEST = "text/plain"
    METRICS_AVAILABLE = False


class _NoOpMetric:
    """Stand-in so call sites never need to check METRICS_AVAILABLE."""

    def labels(self, *_args, **_kwargs):
        return self

    def inc(self, *_args, **_kwargs):
        pass

    def observe(self, *_args, **_kwargs):
        pass

    def set(self, *_args, **_kwargs):
        pass


def _counter(name, documentation, labelnames=()):
    if not METRICS_AVAILABLE:
        return _NoOpMetric()
    return Counter(name, documentation, labelnames)


def _histogram(name, documentation, labelnames=(), buckets=None):
    if not METRICS_AVAILABLE:
        return _NoOpMetric()
    kwargs = {"buckets": buckets} if buckets else {}
    return Histogram(name, documentation, labelnames, **kwargs)


def _gauge(name, documentation, labelnames=()):
    if not METRICS_AVAILABLE:
        return _NoOpMetric()
    return Gauge(name, documentation, labelnames)


# Packet lifecycle -----------------------------------------------------
PACKETS_TOTAL = _counter(
    "agentic_resident_crm_packets_total",
    "Packets processed, by terminal status and resolution source.",
    ("status", "resolution_source"),
)

# Buckets run to 600s because an investigation is minutes, not milliseconds --
# the prometheus defaults top out at 10s and would put every packet in +Inf.
PACKET_DURATION = _histogram(
    "agentic_resident_crm_packet_duration_seconds",
    "Wall-clock duration of a full investigation.",
    ("resolution_source",),
    buckets=(1, 5, 15, 30, 60, 120, 180, 300, 450, 600, float("inf")),
)

INVESTIGATOR_RETRIES = _histogram(
    "agentic_resident_crm_investigator_retries",
    "Reviewer rejections before a packet was approved or escalated.",
    buckets=(0, 1, 2, 3, 4, 5, float("inf")),
)

RUNBOOK_LOOKUPS = _counter(
    "agentic_resident_crm_runbook_lookups_total",
    "Runbook lookups by outcome (hit, shadow, miss, no_reason_code, "
    "fingerprint_mismatch, error). Every outcome is recorded, so the hit rate "
    "has a denominator.",
    ("outcome",),
)

SHADOW_DIVERGENCE = _counter(
    "agentic_resident_crm_shadow_divergence_total",
    "Shadowed runbooks whose action disagreed with the agents' verdict.",
    ("runbook_id",),
)

# Log pipeline ---------------------------------------------------------
LOG_FETCHES = _counter(
    "agentic_resident_crm_log_fetches_total",
    "Log fetches by source and outcome -- this is the source win rate.",
    ("source", "outcome"),
)

LOG_FETCH_DURATION = _histogram(
    "agentic_resident_crm_log_fetch_duration_seconds",
    "Log fetch latency by source.",
    ("source",),
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, float("inf")),
)

LOG_RECORDS_FETCHED = _histogram(
    "agentic_resident_crm_log_records_fetched",
    "Records returned per fetch, by source.",
    ("source",),
    buckets=(0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, float("inf")),
)

EVIDENCE_GAPS = _counter(
    "agentic_resident_crm_evidence_gaps_total",
    "Evidence gaps detected, by type.",
    ("gap_type",),
)

REDACTIONS = _counter(
    "agentic_resident_crm_redactions_total",
    "PII values redacted, by pattern label.",
    ("pattern",),
)

# LLM ------------------------------------------------------------------
LLM_TOKENS = _counter(
    "agentic_resident_crm_llm_tokens_total",
    "LLM tokens consumed, by agent node and direction.",
    ("node", "direction"),
)

LLM_CALLS = _counter(
    "agentic_resident_crm_llm_calls_total",
    "LLM invocations by agent node and outcome (ok, error, invalid, ...).",
    ("node", "outcome"),
)

# Circuit breakers ------------------------------------------------------
#: 0 closed, 1 half-open, 2 open. Breaker trip frequency is named in
#: ENHANCEMENT_PLAN section 4.5 as an unknowable, and it stayed one: a call that
#: raised propagated through the breaker and was counted nowhere (G17).
# ---------------------------------------------------------------------------
# DLT analysis (DLT_PLAN.md)
# ---------------------------------------------------------------------------
DLT_CASES = _counter(
    "agentic_resident_crm_dlt_cases_total",
    "Dead-lettered records processed, by failure class (A/B/C/U).",
    ("failure_class",),
)

DLT_CORROBORATION = _counter(
    "agentic_resident_crm_dlt_corroboration_total",
    "Trace-vs-log corroboration verdicts.",
    ("verdict",),
)

DLT_REUSE = _counter(
    "agentic_resident_crm_dlt_reuse_total",
    "Reuse decisions: LLM_REQUIRED, REUSE_GROUP or CANNED.",
    ("decision",),
)

DLT_REGISTRY_MISSES = _counter(
    "agentic_resident_crm_dlt_registry_misses_total",
    "BusinessException codes with no registry entry.",
)

DLT_WINDOW_AGE_HOURS = _histogram(
    "agentic_resident_crm_dlt_window_age_hours",
    "Age of the log window at fetch time. The reference sample sits at 43h.",
    buckets=(0.5, 1, 2, 4, 8, 12, 24, 48, 96),
)

DLT_AUTO_REPLAY = _counter(
    "agentic_resident_crm_dlt_auto_replay_total",
    "Auto-replay outcomes: not_attempted (gate declined), queued, or failed.",
    ("outcome",),
)

DLT_DEPLOYED_VERSION_READS = _counter(
    "agentic_resident_crm_dlt_deployed_version_reads_total",
    "Readings of the running image version: ok, mixed (rolling deploy), or "
    "failed. A sustained `failed` rate means every code-check verdict is "
    "UNKNOWN (DLT_PLAN.md 14, phase C1).",
    ("outcome",),
)


def record_dlt_case(failure_class: str) -> None:
    if DLT_CASES is not None:
        DLT_CASES.labels(failure_class=failure_class).inc()


def record_dlt_corroboration(verdict: str) -> None:
    if DLT_CORROBORATION is not None:
        DLT_CORROBORATION.labels(verdict=verdict).inc()


def record_dlt_reuse(decision: str) -> None:
    if DLT_REUSE is not None:
        DLT_REUSE.labels(decision=decision).inc()


def record_dlt_registry_miss() -> None:
    if DLT_REGISTRY_MISSES is not None:
        DLT_REGISTRY_MISSES.inc()


def record_dlt_window_age(age_seconds: float) -> None:
    if DLT_WINDOW_AGE_HOURS is not None:
        DLT_WINDOW_AGE_HOURS.observe(max(0.0, age_seconds) / 3600.0)


def record_dlt_auto_replay(outcome: str) -> None:
    if DLT_AUTO_REPLAY is not None:
        DLT_AUTO_REPLAY.labels(outcome=outcome).inc()


def record_dlt_deployed_version_read(ok: bool, mixed: bool = False) -> None:
    """`mixed` is reported separately from `ok`: pods disagreeing about what
    is running is a real state an operator wants to see, not a failure."""
    if DLT_DEPLOYED_VERSION_READS is not None:
        outcome = "failed" if not ok else ("mixed" if mixed else "ok")
        DLT_DEPLOYED_VERSION_READS.labels(outcome=outcome).inc()


BREAKER_STATE = _gauge(
    "agentic_resident_crm_breaker_state",
    "Circuit breaker state: 0 closed, 1 half-open, 2 open.",
    ("breaker",),
)

_BREAKER_STATE_VALUES = {"closed": 0, "half-open": 1, "open": 2}


def sample_breaker_states() -> None:
    """Publish the current state of every circuit breaker.

    Sampled on scrape rather than pushed on transition: pybreaker has no
    transition hook we control from here, and a breaker that resets on a
    timeout would otherwise leave a stale "open" reading behind.
    """
    try:
        from src.utils import resilience

        for name in ("db_breaker", "es_breaker", "llm_breaker", "k8s_breaker",
                     "bitbucket_breaker"):
            breaker = getattr(resilience, name, None)
            if breaker is None:
                continue
            state = str(getattr(breaker, "current_state", "closed"))
            BREAKER_STATE.labels(breaker=name).set(
                _BREAKER_STATE_VALUES.get(state, 0)
            )
    except Exception as e:
        logger.debug("Could not sample breaker states", error=str(e))


def record_fetch_diagnostics(diagnostics, ok: bool, gaps=None):
    """Emit a FetchResult's diagnostics as metrics AND structured logs.

    Logging is the half that works with no metrics backend at all, and it is
    what makes a single packet's fetch explainable after the fact.
    """
    if diagnostics is None:
        return

    source = getattr(diagnostics, "source", "unknown")
    outcome = "ok" if ok else "failed"

    LOG_FETCHES.labels(source=source, outcome=outcome).inc()
    LOG_FETCH_DURATION.labels(source=source).observe(
        getattr(diagnostics, "latency_ms", 0.0) / 1000.0
    )
    LOG_RECORDS_FETCHED.labels(source=source).observe(
        getattr(diagnostics, "records_returned", 0)
    )

    for label, count in (getattr(diagnostics, "redaction_counts", None) or {}).items():
        REDACTIONS.labels(pattern=label).inc(count)

    for gap in (gaps or []):
        gap_type = getattr(getattr(gap, "gap_type", None), "value", "unknown")
        EVIDENCE_GAPS.labels(gap_type=gap_type).inc()

    logger.info(
        "Log fetch diagnostics",
        source=source,
        ok=ok,
        records_returned=getattr(diagnostics, "records_returned", 0),
        bytes_read=getattr(diagnostics, "bytes_read", 0),
        latency_ms=round(getattr(diagnostics, "latency_ms", 0.0), 1),
        pods_queried=getattr(diagnostics, "pods_queried", 0),
        pods_failed=getattr(diagnostics, "pods_failed", 0),
        gap_count=len(gaps or []),
    )


def record_llm_usage(node: str, response) -> None:
    """Pull token counts off a LangChain response, when the provider reports them.

    Local OpenAI-compatible endpoints don't always populate usage_metadata, so
    this is best-effort by design: absent usage must not raise, and must not
    be recorded as zero (which would understate real spend).
    """
    try:
        messages = response.get("messages") if isinstance(response, dict) else None
        if not messages:
            return
        for message in messages:
            usage = getattr(message, "usage_metadata", None)
            if not usage:
                continue
            input_tokens = usage.get("input_tokens") or 0
            output_tokens = usage.get("output_tokens") or 0
            if input_tokens:
                LLM_TOKENS.labels(node=node, direction="input").inc(input_tokens)
            if output_tokens:
                LLM_TOKENS.labels(node=node, direction="output").inc(output_tokens)
    except Exception as e:
        logger.debug("Could not record LLM usage", node=node, error=str(e))


def render_latest() -> Optional[bytes]:
    """Prometheus exposition text, or None when the library isn't installed."""
    if not METRICS_AVAILABLE:
        return None
    return generate_latest()
