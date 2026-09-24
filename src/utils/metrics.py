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

REASON_CODE_DOC_LOOKUPS = _counter(
    "agentic_resident_crm_reason_code_doc_lookups_total",
    "Reason-code document lookups by outcome (hit, miss, error, "
    "no_reason_code, disabled) and by which enrolment type matched (exact, "
    "any, none). The miss rate per reason code is what says which document "
    "to write next, so every outcome is counted and the rate has a "
    "denominator.",
    ("outcome", "match"),
)

REJECTION_PROMPT_TRIMS = _counter(
    "agentic_resident_crm_rejection_prompt_trims_total",
    "Direct rejection prompts whose logs were trimmed to fit "
    "REJECTION_PROMPT_MAX_CHARS, by graph node. A rising count means the cap "
    "is binding and the model is reasoning from a partial trace.",
    ("node",),
)

PACKET_CLAIMS = _counter(
    "agentic_resident_crm_packet_claims_total",
    "Analysis claims at /analyze-rejection: won, duplicate (another delivery "
    "of the same packet is already being investigated), finished, reclaimed "
    "(the holder died or released it), disabled, or error (the claim store "
    "was unreachable and the packet proceeded). A rising `duplicate` count is "
    "duplicate LLM work that is now being avoided.",
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

DLT_CODE_CHECK = _counter(
    "agentic_resident_crm_dlt_code_check_total",
    "Replay-precheck verdicts: NO_CHANGE, NOT_DEPLOYED, FIX_DEPLOYED or "
    "UNKNOWN. The UNKNOWN share is the feature's real coverage number "
    "(DLT_PLAN.md 14, phase C5).",
    ("verdict",),
)

DLT_DEPLOYED_VERSION_READS = _counter(
    "agentic_resident_crm_dlt_deployed_version_reads_total",
    "Readings of the running image version: ok, mixed (rolling deploy), or "
    "failed. A sustained `failed` rate means every code-check verdict is "
    "UNKNOWN (DLT_PLAN.md 14, phase C1).",
    ("outcome",),
)


DLT_GROUP_WRITES = _counter(
    "agentic_resident_crm_dlt_group_writes_total",
    "Writes to a DLT group record, by operation (occurrence, code_check, "
    "recommendation) and outcome (ok, failed). A sustained `failed` rate "
    "means group state is not accumulating -- occurrence counts freeze and "
    "reuse never fires, so every packet pays for the LLM.",
    ("operation", "outcome"),
)

DLT_CLAIMS = _counter(
    "agentic_resident_crm_dlt_claims_total",
    "Case claims at fast-lane entry: won, duplicate (another delivery of the "
    "same DLT record already holds it), reclaimed (the holder died), or "
    "error (the claim store was unreachable and the case proceeded).",
    ("outcome",),
)

DLT_SINGLEFLIGHT = _counter(
    "agentic_resident_crm_dlt_singleflight_total",
    "Single-flight outcomes per fingerprint: leader (ran the analysis), "
    "reused (found a finding another request cached, so no LLM call), "
    "waited_then_ran (waited but none appeared), timeout (gave up waiting).",
    ("outcome",),
)

DLT_PER_CODE_VIOLATIONS = _counter(
    "agentic_resident_crm_dlt_per_code_violations_total",
    "Agent findings containing packet-specific text that would be cached "
    "against the fingerprint and re-served to every later packet.",
    ("pattern",),
)


def record_dlt_group_write(operation: str, ok: bool) -> None:
    if DLT_GROUP_WRITES is not None:
        DLT_GROUP_WRITES.labels(operation=operation,
                                outcome="ok" if ok else "failed").inc()


def record_packet_claim(outcome: str) -> None:
    if PACKET_CLAIMS is not None:
        PACKET_CLAIMS.labels(outcome=outcome).inc()


def record_dlt_claim(outcome: str) -> None:
    if DLT_CLAIMS is not None:
        DLT_CLAIMS.labels(outcome=outcome).inc()


def record_dlt_singleflight(outcome: str) -> None:
    if DLT_SINGLEFLIGHT is not None:
        DLT_SINGLEFLIGHT.labels(outcome=outcome).inc()


def record_dlt_per_code_violation(pattern: str) -> None:
    if DLT_PER_CODE_VIOLATIONS is not None:
        DLT_PER_CODE_VIOLATIONS.labels(pattern=pattern).inc()


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


def record_dlt_code_check(verdict: str) -> None:
    if DLT_CODE_CHECK is not None:
        DLT_CODE_CHECK.labels(verdict=verdict).inc()


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


def record_harness_usage(node: str, trace: dict) -> None:
    """Meter one opencode harness task from its `--format json` event trace.

    The harness path recorded nothing at all. `record_llm_usage` reads
    `usage_metadata` off a LangChain response, and a harness task has no
    response object -- it returns a file on disk. So with
    USE_OPENCODE_HARNESS=true the Investigator and Reviewer nodes reported
    zero calls and zero tokens while doing all of the work, and the spend
    graph read as though those nodes had stopped running.

    One task is an agentic loop, so `llm_calls` counts the round-trips inside
    it, not the task -- the same unit the direct path counts. Only input and
    output are metered, the two directions the direct path records, so a
    dashboard can compare them without knowing which path served a packet;
    the reasoning and cache breakdown rides along in the structured log.
    """
    try:
        calls = int((trace or {}).get("llm_calls") or 0)
        if calls:
            LLM_CALLS.labels(node=node, outcome="ok").inc(calls)
        tokens = (trace or {}).get("tokens") or {}
        for direction in ("input", "output"):
            count = int(tokens.get(direction) or 0)
            if count:
                LLM_TOKENS.labels(node=node, direction=direction).inc(count)
    except Exception as e:
        logger.debug("Could not record harness usage", node=node, error=str(e))


def render_latest() -> Optional[bytes]:
    """Prometheus exposition text, or None when the library isn't installed."""
    if not METRICS_AVAILABLE:
        return None
    return generate_latest()
