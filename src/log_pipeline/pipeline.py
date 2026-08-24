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
from src.log_pipeline.reducer import branch_on_error, cluster_logs, apply_evidence_guardrails
from src.log_pipeline.sources import chain as source_chain
from src.log_pipeline.sources.k8s import gaps as k8s_gaps
from src.log_pipeline.types import FetchContext, TimeWindow
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

    # If logs are small enough, return directly
    if total_fetched < 50:
        lines = [f"--- Log Trace for ID: {event_id} ---"]
        # `record`, not `log`: this loop used to rebind `log`, the bound
        # structlog logger created above, to a log-record dict. It survived
        # only because this branch returns immediately -- any line added
        # between here and the return would have raised AttributeError.
        for record in raw_logs:
            lines.append(f"[{record['timestamp']}] [{_origin(record)}] [{record['level']}] {record['message']}")
        lines.append(f"--- End of Trace ({total_fetched} logs total) ---")
        formatted = _with_banner(gap_banner, "\n".join(lines))
        _save_reduced_logs(artifact_key, formatted, storage=storage)
        return formatted

    # ------------------------------------------------------------------
    # Stage 2: Branch on ERROR
    # ------------------------------------------------------------------
    branch_result = branch_on_error(raw_logs)

    if branch_result["has_error"]:
        # Stuck path: format the trimmed context directly
        formatted = _with_banner(
            gap_banner,
            _format_error_path(event_id, branch_result["payload"], total_fetched, log_file_path),
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


def _origin(log: dict) -> str:
    """Render pod attribution when present.

    Without it a merged multi-replica trace gives the LLM no way to tell
    which replica a line came from -- which matters when replicas disagree.
    """
    pod = log.get('pod_name')
    return f"{log.get('app_name','')}@{pod}" if pod else str(log.get('app_name',''))


def _format_error_path(event_id: str, trimmed_logs: list[dict],
                       total_fetched: int, log_file_path: str) -> str:
    """Format the ERROR/stuck branch output for the LLM."""
    lines = [
        f"--- ERROR DETECTED in log trace for ID: {event_id} ---",
        f"Total raw logs: {total_fetched} (saved at {log_file_path})",
        f"Showing ERROR(s) + {len(trimmed_logs)} surrounding context lines:",
        "",
    ]
    for log in trimmed_logs:
        marker = " *** " if log.get("level", "").upper() == "ERROR" else "     "
        lines.append(f"{marker}[{log['timestamp']}] [{_origin(log)}] [{log['level']}] {log['message']}")
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
    lines.append("== Template Clusters (ordered by first appearance) ==")
    for cluster in clusters:
        classification = cluster.get("classification", "unknown")
        tid = cluster.get("template_id", "?")
        count = cluster.get("count", 0)
        template = cluster.get("template", "")
        first = cluster.get("first_seen", "")
        last = cluster.get("last_seen", "")
        examples = cluster.get("examples", [])

        lines.append(f"  [{tid}] [Count:{count:4d}] [{classification}] {template}")
        lines.append(f"         first_seen={first}  last_seen={last}")
        if examples:
            for ex in examples[:3]:
                lines.append(f"         example: {ex}")
        lines.append("")

    lines.append("--- End of Reduced Log Analysis ---")
    return "\n".join(lines)


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
        f"[{log['timestamp']}] [{_origin(log)}] [{log['level']}] {log['message']}\n"
        for log in logs
    )
    return _write_artifact(event_id, "raw_logs.txt", body, storage=storage)


def _save_reduced_logs(event_id: str, reduced_text: str, storage=None) -> str:
    """Persist reduced logs and return the artifact locator."""
    return _write_artifact(event_id, "reduced_logs.txt", reduced_text, storage=storage)
