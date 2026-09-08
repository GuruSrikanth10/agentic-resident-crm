"""
Log Reduction Pipeline -- Top-level Orchestrator.

Wires Stages 1-4 together and returns a compact, evidence-preserving
JSON string ready for LLM injection.

Flow:
  Stage 1 (fetch) -> Stage 2 (branch on ERROR)
    -> ERROR path:   trim to error + context, format, return
    -> Normal path:  Stage 3 (cluster) -> Stage 4 (guardrails) -> format, return
"""
import os
import threading
from typing import Optional

from src.log_pipeline import redaction
from src.log_pipeline.catalog import TemplateCatalog
from src.log_pipeline.config import MAX_REDUCED_CHARS
from src.log_pipeline.reducer import (
    apply_evidence_guardrails,
    apply_noise_floor,
    branch_on_error,
    cluster_logs,
    collapse_error_window,
)
from src.log_pipeline.sources import chain as source_chain
from src.log_pipeline.sources.k8s import gaps as k8s_gaps
from src.log_pipeline.types import CONTEXT_LINE_MARKER, FetchContext, TimeWindow
from src.storage.factory import get_casebook_storage
from src.utils import metrics
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


_cached_catalog = None
# TemplateCatalog() reads and parses the catalog JSON, so racing callers each
# paid for a full parse and all but one result was discarded.
_catalog_lock = threading.Lock()


def _get_catalog() -> TemplateCatalog:
    """Return a module-level cached catalog instance."""
    global _cached_catalog
    if _cached_catalog is not None:
        return _cached_catalog

    with _catalog_lock:
        if _cached_catalog is None:
            _cached_catalog = TemplateCatalog()
        return _cached_catalog


def _default_window() -> TimeWindow:
    """Look-back window for sources that honour one.

    Elasticsearch ignores it (its query has no time bound); the Kubernetes
    source uses it for `since_seconds`.
    """
    try:
        return TimeWindow(hours=float(os.environ.get("K8S_DEFAULT_SINCE_HOURS", "2")))
    except ValueError:
        return TimeWindow.default()


def reduce_logs(event_id: str, extra_identifiers: tuple = (),
                storage_key: Optional[str] = None,
                window: Optional[TimeWindow] = None,
                storage=None) -> str:
    """Run the full log reduction pipeline for an event_id.

    `extra_identifiers` are additional correlation ids (refId, srn) that the
    Kubernetes source matches lines against alongside `event_id`, and that
    redaction allowlists so they survive scrubbing. Wired to the payload in
    Phase C (F11); defaults to empty so callers that don't have them work
    unchanged.

    `storage_key` separates *what we search for* from *where we persist*. The
    rejection path uses one value for both, and passes neither. The DLT path
    searches on `refId` -- the only identifier the service actually logs --
    but persists under its own `case_id`, since one packet can be dead-lettered
    at several stages and each occurrence is a distinct case (DLT_PLAN.md 5.5).

    `window` overrides the `K8S_DEFAULT_SINCE_HOURS` look-back. The DLT path
    derives its window from `retry_topic-backoff-timestamp` instead: in the
    reference sample the last attempt is 43 hours after the original produce
    time, so the default two-hour look-back would search the wrong window
    entirely and find nothing (DLT_PLAN.md 3.2, Trap 2).

    Returns a formatted string suitable for LLM context injection.
    Raw logs are also persisted to disk for audit.
    """
    extra_identifiers = tuple(v for v in (extra_identifiers or ()) if v)
    artifact_key = storage_key or event_id
    # Load catalog (cached -- only reads disk once)
    catalog = _get_catalog()

    # ------------------------------------------------------------------
    # Stage 1: Fetch (through the LogSource seam -- see sources/base.py)
    # ------------------------------------------------------------------
    fetch_result = source_chain.fetch_with_fallback(
        event_id,
        window or _default_window(),
        FetchContext(event_id=event_id, catalog=catalog,
                     extra_identifiers=extra_identifiers),
    )
    raw_logs = fetch_result.records

    # FetchDiagnostics was built on every fetch and then discarded (4.5).
    metrics.record_fetch_diagnostics(
        fetch_result.diagnostics, ok=fetch_result.ok, gaps=fetch_result.gaps
    )

    gap_banner = k8s_gaps.render_banner(fetch_result.gaps)

    if not raw_logs:
        empty = f"No logs found for ID: {event_id}"
        return f"{gap_banner}\n{empty}" if gap_banner else empty

    total_fetched = len(raw_logs)
    log = logger.bind(event_id=event_id)
    log.info("Log pipeline fetch completed", record_count=total_fetched)

    # ------------------------------------------------------------------
    # Redaction -- after fetch, before ANY persistence.
    # ------------------------------------------------------------------
    # This is the one seam every source passes through, so redacting here
    # covers Elasticsearch too. It previously ran only inside the Kubernetes
    # retrieval path, and since LOG_SOURCE's Kubernetes leg fails fast when no
    # cluster is configured, in practice most deployments wrote entirely
    # unredacted resident data to raw_logs.txt, into the casebook's
    # rejection_logs field, and on to S3 (F10).
    #
    # The Kubernetes source still redacts internally before writing its own
    # snapshot -- that is a separate persistence point, reached before this
    # one. Redaction is idempotent (redacted text has no PII left to match),
    # so the second pass over those records is a no-op that simply counts 0.
    #
    # The identifiers we searched on are the allowlist: they are operational
    # correlation ids, not resident PII, and scrubbing them would destroy the
    # investigation.
    redaction_counts = redaction.redact_records(
        raw_logs, allowlist=[event_id, *(extra_identifiers or ())]
    )
    if redaction_counts:
        log.info("Redacted PII before persistence", **{
            f"redacted_{label.lower()}": count
            for label, count in redaction_counts.items()
        })
        for label, count in redaction_counts.items():
            metrics.REDACTIONS.labels(pattern=label).inc(count)

    # Persist raw logs to disk for audit
    log_file_path = _save_raw_logs(artifact_key, raw_logs, storage=storage)

    # ------------------------------------------------------------------
    # Stage 2.5: Noise floor -- AFTER the audit copy is on disk.
    # ------------------------------------------------------------------
    # Framework DEBUG chatter and echoed SQL column lists are together ~66%
    # and ~39% of the bytes in a real trace and carry no packet-specific
    # signal. Thinning them here rather than at fetch keeps raw_logs.txt the
    # complete record while shrinking only the copy the model reads.
    #
    # If the floor would empty the trace, keep the original: handing the
    # agent nothing reads as "no logs existed", which is the one conclusion
    # this pipeline must never invite (design principle 3).
    floored, noise_report = apply_noise_floor(raw_logs)
    if floored:
        raw_logs = floored
    else:
        noise_report = {"dropped_below_level": 0, "min_level": noise_report["min_level"],
                        "sql_collapsed": 0, "sql_chars_saved": 0}
    # Folded into the gap banner so every output path -- small, ERROR and
    # clustered -- announces it, and so it lands ahead of the trace rather
    # than after it.
    noise_banner = _noise_banner(noise_report, total_fetched)
    gap_banner = "\n".join(part for part in (gap_banner, noise_banner) if part)

    # If logs are small enough, return directly
    if len(raw_logs) < 50:
        lines = [f"--- Log Trace for ID: {event_id} ---",
                 *_context_legend(raw_logs)]
        # `record`, not `log`: this loop used to rebind `log`, the bound
        # structlog logger created above, to a log-record dict. It survived
        # only because this branch returns immediately -- any line added
        # between here and the return would have raised AttributeError.
        for record in raw_logs:
            lines.append(f"[{record['timestamp']}] [{_origin(record)}] "
                         f"[{record['level']}]{_context_tag(record)} {record['message']}")
        lines.append(f"--- End of Trace ({total_fetched} logs total) ---")
        formatted = _with_banner(gap_banner, "\n".join(lines))
        _save_reduced_logs(artifact_key, formatted, storage=storage)
        return formatted

    # ------------------------------------------------------------------
    # Stage 2: Branch on ERROR
    # ------------------------------------------------------------------
    branch_result = branch_on_error(raw_logs)

    if branch_result["has_error"]:
        # Stuck path: chronological, but with repeated boilerplate folded to
        # its first occurrence. The sequence is what this branch is for, so it
        # is never replaced by a cluster summary the way the normal path is.
        window, _suppressed = collapse_error_window(branch_result["payload"])
        formatted = _with_banner(
            gap_banner,
            _format_error_path(event_id, window, total_fetched, log_file_path),
        )
        _save_reduced_logs(artifact_key, formatted, storage=storage)
        return formatted

    # ------------------------------------------------------------------
    # Stage 3: Clustering (approve/reject path)
    # ------------------------------------------------------------------
    clusters = cluster_logs(raw_logs, catalog=catalog)

    # ------------------------------------------------------------------
    # Stage 4: Evidence guardrails
    # ------------------------------------------------------------------
    assembled = apply_evidence_guardrails(clusters, raw_logs, catalog=catalog)

    # ------------------------------------------------------------------
    # Format for LLM injection
    # ------------------------------------------------------------------
    formatted = _with_banner(
        gap_banner,
        _format_normal_path(event_id, assembled, total_fetched, log_file_path),
    )
    _save_reduced_logs(artifact_key, formatted, storage=storage)
    return formatted


# ======================================================================
# Formatting helpers
# ======================================================================

def _with_banner(banner: str, body: str) -> str:
    """Put the evidence-gap banner ahead of the trace, then bound the whole.

    The LLM must see that the trace is incomplete before it reads the
    trace, not after -- so the banner is prepended, and the size ceiling below
    trims the BODY rather than the banner.
    """
    body = _bound_total_size(body)
    return f"{banner}\n\n{body}" if banner else body


def _bound_total_size(body: str) -> str:
    """Last-resort ceiling on the text handed to the LLM.

    The per-section bounds cap the parts; this caps the whole. It exists for
    the ERROR branch in particular, which is bounded in *lines*
    (LOG_ERROR_CONTEXT_LINES + LOG_ERROR_TRAILING_LINES plus every ERROR in
    between) and not in characters -- a stack-trace-heavy trace can blow the
    context window on a few hundred lines.

    Trims the middle and says so, for the same reason the decision-vocabulary
    bound does: the head establishes what the flow attempted and the tail
    holds how it ended.
    """
    if len(body) <= MAX_REDUCED_CHARS:
        return body

    marker = (f"\n\n... {len(body) - MAX_REDUCED_CHARS} characters omitted from "
              f"the middle of this trace (LOG_MAX_REDUCED_CHARS) ...\n\n")
    keep = max(0, (MAX_REDUCED_CHARS - len(marker)) // 2)
    logger.warning("Reduced log output exceeded LOG_MAX_REDUCED_CHARS; trimming",
                   original_chars=len(body), limit=MAX_REDUCED_CHARS)
    return body[:keep] + marker + body[-keep:]


def _noise_banner(report: dict, total_fetched: int) -> str:
    """Announce what the noise floor removed, in the model's own view.

    An omission the model cannot see is one it reasons as though it never
    happened -- the same rule the decision-vocabulary and truncation markers
    follow. It matters most for the dropped DEBUG lines: without this line the
    model has no way to tell a service that logged nothing at DEBUG from one
    whose DEBUG was filtered out on the way here.
    """
    parts = []
    dropped = report.get("dropped_below_level", 0)
    if dropped:
        parts.append(
            f"{dropped} of {total_fetched} lines below {report.get('min_level', 'INFO')} "
            f"were omitted (LOG_MIN_LEVEL); raw_logs.txt holds the complete trace."
        )
    collapsed = report.get("sql_collapsed", 0)
    if collapsed:
        parts.append(
            f"{collapsed} SQL statements had their column lists replaced with a "
            f"count (LOG_COLLAPSE_SQL); tables and predicates are unchanged."
        )
    return "NOTE: " + " ".join(parts) if parts else ""


def _origin(log: dict) -> str:
    """Render pod attribution when present.

    Without it a merged multi-replica trace gives the LLM no way to tell
    which replica a line came from -- which matters when replicas disagree.
    """
    pod = log.get('pod_name')
    return f"{log.get('app_name','')}@{pod}" if pod else str(log.get('app_name',''))


def _context_tag(log: dict) -> str:
    """Mark a line the identifier filter kept only as surrounding context.

    Such a line was emitted because it sits within K8S_CONTEXT_LINES_BEFORE /
    _AFTER of a match, not because it carries the id -- so on a pod serving
    packets concurrently it may belong to a different refId entirely. Both
    readers of this text need to know: the Investigator, so it does not build
    a narrative on another packet's failure, and `dlt/corroborate.py`, so it
    does not report a neighbour's exception as one this packet's stack trace
    failed to explain.

    A missing key renders nothing, so Elasticsearch records -- filtered
    server-side, hence always matches -- and snapshots written before the flag
    existed produce byte-for-byte the previous output.
    """
    return "" if log.get("identifier_match", True) else f" {CONTEXT_LINE_MARKER}"


def _context_legend(logs: list) -> list:
    """One line explaining the marker, emitted only when it actually appears.

    The marker is meaningless to a reader who has not been told what it means,
    and a legend on a trace with no context lines is noise.
    """
    if not any(log.get("identifier_match", True) is False for log in logs):
        return []
    return [f"Lines tagged {CONTEXT_LINE_MARKER} did not carry the searched "
            f"identifier; they were kept as surrounding context and may "
            f"belong to another transaction on the same pod."]


def _format_error_path(event_id: str, trimmed_logs: list[dict],
                       total_fetched: int, log_file_path: str) -> str:
    """Format the ERROR/stuck branch output for the LLM."""
    lines = [
        f"--- ERROR DETECTED in log trace for ID: {event_id} ---",
        f"Total raw logs: {total_fetched} (saved at {log_file_path})",
        f"Showing ERROR(s) + {len(trimmed_logs)} surrounding context lines:",
        *_context_legend(trimmed_logs),
        "",
    ]
    for log in trimmed_logs:
        # Synthetic marker standing in for folded repeats of the line above.
        if "_note" in log:
            lines.append(f"          {log['_note']}")
            continue
        marker = " *** " if log.get("level", "").upper() == "ERROR" else "     "
        lines.append(f"{marker}[{log['timestamp']}] [{_origin(log)}] "
                     f"[{log['level']}]{_context_tag(log)} {log['message']}")
    lines.append("")
    lines.append("--- End of ERROR context ---")
    return "\n".join(lines)


def _format_normal_path(event_id: str, assembled: dict,
                        total_fetched: int, log_file_path: str) -> str:
    """Format the clustered + guardrail-processed output for the LLM."""
    clusters = assembled.get("clusters", [])
    decision_lines = assembled.get("decision_vocabulary_lines", [])
    boundary_lines = assembled.get("boundary_lines", [])

    lines = [
        f"--- Reduced Log Analysis for ID: {event_id} ---",
        f"Total raw logs: {total_fetched} (saved at {log_file_path})",
        f"Compressed into {len(clusters)} structural templates.",
        f"Rare templates (count < 5) and decision-vocabulary matches are kept in full.",
        "",
    ]

    # Boundary context
    if boundary_lines:
        lines.append("== Flow Boundaries ==")
        for bl in boundary_lines:
            lines.append(f"  [{bl['timestamp']}] [{bl['level']}] {bl['message']}")
        lines.append("")

    # Decision vocabulary matches
    if decision_lines:
        omitted = assembled.get("decision_vocabulary_omitted", 0)
        header = f"== Decision-Vocabulary Matches ({len(decision_lines)} lines) =="
        if omitted:
            header = (f"== Decision-Vocabulary Matches "
                      f"({len(decision_lines)} of {len(decision_lines) + omitted} "
                      f"lines; the middle was omitted) ==")
        lines.append(header)

        # The omission marker sits at the seam, not in the header alone: the
        # model reads the lines in order, and two adjacent decision lines with
        # a gap of thousands between them would otherwise read as consecutive.
        half = len(decision_lines) // 2 if omitted else len(decision_lines)
        for index, dl in enumerate(decision_lines):
            if omitted and index == half:
                lines.append(f"  ... {omitted} further decision-vocabulary lines "
                             f"omitted (LOG_MAX_DECISION_LINES) ...")
            lines.append(f"  [{dl['timestamp']}] [{dl['level']}] {dl['message']}")
        lines.append("")

    # Template clusters (ordered by first_seen)
    #
    # Rendered compactly, because the scaffolding used to cost more than the
    # evidence: a standalone `first_seen=/last_seen=` line per cluster was
    # 26.6% of the whole output, and a cluster of count 1 printed the same
    # content three times over -- once as the Drain3 template, once as the
    # timestamp pair, and once as its single "example".
    lines.append("== Template Clusters (ordered by first appearance) ==")
    for cluster in clusters:
        classification = cluster.get("classification", "unknown")
        tid = cluster.get("template_id", "?")
        count = cluster.get("count", 0)
        template = cluster.get("template", "")
        first = cluster.get("first_seen", "")
        last = cluster.get("last_seen", "")
        examples = cluster.get("examples", [])

        span = first if (not last or last == first) else f"{first}..{_time_only(last)}"

        # A cluster seen once IS its example; the template adds nothing but a
        # placeholder-substituted copy of the same text.
        if count == 1 and examples:
            lines.append(f"  [{tid}] x1 [{span}] {examples[0]}")
            continue

        # Collapsed (boilerplate/repetitive): the template and the count are
        # the whole point -- examples were stripped in Stage 4.
        if not examples:
            lines.append(f"  [{tid}] x{count} [{classification}] [{span}] {template}")
            continue

        # Rare but repeated: the examples carry the varying parameters, which
        # is what the template's <*> placeholders throw away.
        lines.append(f"  [{tid}] x{count} [{classification}] [{span}]")
        for ex in examples[:3]:
            lines.append(f"      {ex}")

    lines.append("--- End of Reduced Log Analysis ---")
    return "\n".join(lines)


def _time_only(timestamp: str) -> str:
    """Render the time half of an RFC3339 stamp, for the end of a same-day span.

    A cluster's first and last line are almost always seconds apart, so
    repeating the full date in the range doubles the cost of the span for no
    information. Falls back to the whole string when the shape is unfamiliar.
    """
    if "T" in timestamp:
        return timestamp.split("T", 1)[1]
    return timestamp


# ======================================================================
# Disk persistence
# ======================================================================

def _write_artifact(event_id: str, filename: str, content: str, storage=None) -> str:
    """Persist one log artifact beside the casebook and return its locator.

    `storage` lets a caller target a different root -- the DLT path writes into
    `dlt_cases/` rather than beside the rejection casebooks (DLT_PLAN.md 7).
    Defaults to the rejection store, so existing callers are unaffected.

    Goes through CasebookStorage rather than writing to the local filesystem.
    F22 consolidated the *path* derivation here but left the storage bypass in
    place, so under CASEBOOK_STORAGE_BACKEND=s3 these artifacts still landed on
    whichever pod ran the packet and died with it (G3). The casebook itself was
    in S3, its evidence was not.
    """
    try:
        target = storage or get_casebook_storage()
        locator = target.save_artifact(event_id, filename, content)
        logger.bind(event_id=event_id).info("Log artifact saved", filename=filename, path=locator)
        return locator
    except Exception as e:
        logger.bind(event_id=event_id).error("Failed to save log artifact", filename=filename, error=f"{type(e).__name__}: {e}")
        return "Failed to save"


def _save_raw_logs(event_id: str, logs: list[dict], storage=None) -> str:
    """Persist raw logs and return the artifact locator."""
    body = "".join(
        f"[{log['timestamp']}] [{_origin(log)}] "
        f"[{log['level']}]{_context_tag(log)} {log['message']}\n"
        for log in logs
    )
    return _write_artifact(event_id, "raw_logs.txt", body, storage=storage)


def _save_reduced_logs(event_id: str, reduced_text: str, storage=None) -> str:
    """Persist reduced logs and return the artifact locator."""
    return _write_artifact(event_id, "reduced_logs.txt", reduced_text, storage=storage)
