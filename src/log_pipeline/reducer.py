"""
Stages 2, 3, 4 -- Log Reduction Engine.

Stage 2: Branch on ERROR presence.
Stage 3: Drain3 clustering with persisted state.
Stage 4: Evidence assembly guardrails.
"""
import os
import threading
from typing import Optional

from filelock import FileLock

from src.log_pipeline.config import (
    DECISION_VOCABULARY_REGEX,
    DRAIN3_STATE_DIR,
    ERROR_CONTEXT_LINES,
    ERROR_TRAILING_LINES,
    MAX_DECISION_VOCABULARY_LINES,
    RARE_TEMPLATE_THRESHOLD,
)
from src.log_pipeline.catalog import TemplateCatalog
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# Serializes access to the shared, file-persisted Drain3 parse tree so two
# packets processed concurrently in this process never read-modify-write the
# state file at the same time (0.2). The FileLock below extends the same
# protection across process boundaries (e.g. API + consumer).
_drain3_intraprocess_lock = threading.Lock()

# Held for the process lifetime and reused across packets -- constructing a
# fresh TemplateMiner per call re-reads and re-deserializes the whole state
# file every time, which only grows as more templates accumulate (2.5).
# Access is still only ever under _drain3_intraprocess_lock / FileLock above.
_template_miner_instance = None


# ======================================================================
# Stage 2 -- Branch on ERROR
# ======================================================================

def branch_on_error(logs: list[dict]) -> dict:
    """Check if any log has level=ERROR.

    Returns:
        {
          "has_error": bool,
          "payload": list[dict]   -- if has_error, the trimmed context window
        }
    """
    error_indices = [i for i, log in enumerate(logs) if log.get("level", "").upper() == "ERROR"]

    if not error_indices:
        return {"has_error": False, "payload": []}

    # Take ERROR lines + preceding N context lines, and a bounded trailing
    # window after the last error. Previously this kept everything from the
    # first error to the end of the trace -- on a cascading failure that is
    # effectively the whole log (1.11).
    first_error_idx = error_indices[0]
    last_error_idx = error_indices[-1]
    context_start = max(0, first_error_idx - ERROR_CONTEXT_LINES)
    context_end = min(len(logs), last_error_idx + 1 + ERROR_TRAILING_LINES)

    trimmed = logs[context_start:context_end]

    logger.info(
        "Reducer took the ERROR branch",
        error_count=len(error_indices),
        trimmed_lines=len(trimmed),
        context_start=context_start,
        context_end=context_end,
    )

    return {"has_error": True, "payload": trimmed}


# ======================================================================
# Stage 3 -- Drain3 Clustering
# ======================================================================

def _get_template_miner():
    """Return the process-wide TemplateMiner singleton, building it (and
    reading the persisted state file) at most once per process (2.5).

    Caller must already hold _drain3_intraprocess_lock / the FileLock.
    """
    global _template_miner_instance
    if _template_miner_instance is not None:
        return _template_miner_instance

    from drain3 import TemplateMiner
    from drain3.template_miner_config import TemplateMinerConfig
    from drain3.file_persistence import FilePersistence

    os.makedirs(DRAIN3_STATE_DIR, exist_ok=True)
    state_file = os.path.join(DRAIN3_STATE_DIR, "drain3_state.bin")
    persistence = FilePersistence(state_file)
    config = TemplateMinerConfig()
    _template_miner_instance = TemplateMiner(persistence, config)
    return _template_miner_instance


def cluster_logs(logs: list[dict], catalog: Optional[TemplateCatalog] = None) -> list[dict]:
    """Cluster log messages using Drain3 with persisted state.

    Returns a list of cluster dicts ordered by first_seen timestamp:
    {
        "template_id": str,
        "template": str,
        "count": int,
        "first_seen": str,
        "last_seen": str,
        "classification": str,
        "examples": list[str]
    }
    """
    os.makedirs(DRAIN3_STATE_DIR, exist_ok=True)
    state_file = os.path.join(DRAIN3_STATE_DIR, "drain3_state.bin")
    lock_file = state_file + ".lock"

    # Hold both locks for the entire construct-feed-persist cycle so no other
    # thread/process can interleave a read-modify-write on the shared parse
    # tree (0.2), and so the cluster set below reflects only this call's logs.
    with _drain3_intraprocess_lock, FileLock(lock_file, timeout=30):
        template_miner = _get_template_miner()

        # Track per-cluster metadata as we feed logs
        # cluster_id -> {first_seen, last_seen, examples, count}
        cluster_meta: dict[int, dict] = {}
        # Only clusters actually touched by *this* call's logs may be emitted --
        # template_miner.drain.clusters holds every cluster ever seen across
        # every packet, since the parse tree is shared and file-persisted.
        seen_cluster_ids: set[int] = set()

        for log_entry in logs:
            msg = log_entry.get("message", "")
            ts = log_entry.get("timestamp", "")

            result = template_miner.add_log_message(msg)
            cluster_id = result["cluster_id"]
            seen_cluster_ids.add(cluster_id)

            if cluster_id not in cluster_meta:
                cluster_meta[cluster_id] = {
                    "first_seen": ts,
                    "last_seen": ts,
                    "examples": [],
                    "count": 0,
                }

            meta = cluster_meta[cluster_id]
            meta["last_seen"] = ts
            meta["count"] += 1
            # Keep up to 3 example lines for non-boilerplate clusters
            if len(meta["examples"]) < 3:
                meta["examples"].append(msg)

        # Force the updated parse tree to disk before releasing the lock, so
        # the next caller always starts from a fully-written, consistent state.
        template_miner.save_state("cluster_logs: end of batch")

        # Build output ordered by first_seen timestamp, restricted to clusters
        # actually matched by this call's logs.
        clusters_output = []
        for cluster in template_miner.drain.clusters:
            cid = cluster.cluster_id
            if cid not in seen_cluster_ids:
                continue

            meta = cluster_meta.get(cid, {})
            tid = f"t_{cid:04d}"

            classification = "unknown"
            if catalog:
                classification = catalog.get_classification(tid)

            clusters_output.append({
                "template_id": tid,
                "template": cluster.get_template(),
                "count": meta.get("count", 0),
                "first_seen": meta.get("first_seen", ""),
                "last_seen": meta.get("last_seen", ""),
                "classification": classification,
                "examples": meta.get("examples", []),
            })

    # Sort by first_seen timestamp (not frequency -- the LLM needs the sequence)
    clusters_output.sort(key=lambda c: c["first_seen"])

    logger.info("Reducer clustered log lines", input_lines=len(logs), template_count=len(clusters_output))
    return clusters_output


# ======================================================================
# Stage 4 -- Evidence Assembly Guardrails
# ======================================================================

def apply_evidence_guardrails(
    clusters: list[dict],
    raw_logs: list[dict],
    catalog: Optional[TemplateCatalog] = None,
) -> dict:
    """Post-process clusters to enforce evidence retention rules.

    Regardless of catalog classification, always force full-text retention for:
    1. Lines matching the decision-vocabulary regex.
    2. Templates with count < RARE_TEMPLATE_THRESHOLD.
    3. First and last log line of the flow.

    For boilerplate clusters, strip examples (count-only).
    For everything else, keep examples.
    """
    # -------------------------------------------------------------------
    # 1. Collect decision-vocabulary lines from raw logs
    # -------------------------------------------------------------------
    decision_lines = []
    for log_entry in raw_logs:
        msg = log_entry.get("message", "")
        if DECISION_VOCABULARY_REGEX.search(msg):
            decision_lines.append({
                "timestamp": log_entry.get("timestamp", ""),
                "level": log_entry.get("level", ""),
                "message": msg,
                "source": "decision_vocabulary_match",
            })

    # Bound it. These are kept in full text, and the default vocabulary matches
    # strings that appear on most lines in this domain -- so an unbounded list
    # made the reduced output larger than the input it reduces.
    decision_lines, decision_lines_omitted = _bound_head_and_tail(
        decision_lines, MAX_DECISION_VOCABULARY_LINES)

    # -------------------------------------------------------------------
    # 2. Process each cluster
    # -------------------------------------------------------------------
    processed = []
    for cluster in clusters:
        classification = cluster.get("classification", "unknown")
        count = cluster.get("count", 0)

        # Rule 2: rare templates always keep examples
        if count < RARE_TEMPLATE_THRESHOLD:
            cluster["classification"] = "rare"
            # examples are already populated from Stage 3
            processed.append(cluster)
            continue

        # Boilerplate: collapse to count-only (no examples)
        if classification == "boilerplate":
            cluster["examples"] = []
            processed.append(cluster)
            continue

        # Everything else (informative, decision-marker, unknown): keep examples
        processed.append(cluster)

    # -------------------------------------------------------------------
    # 3. Force first and last log line as boundary context
    # -------------------------------------------------------------------
    boundary_lines = []
    if raw_logs:
        first = raw_logs[0]
        last = raw_logs[-1]
        boundary_lines.append({
            "timestamp": first.get("timestamp", ""),
            "level": first.get("level", ""),
            "message": first.get("message", ""),
            "source": "flow_boundary_first",
        })
        if len(raw_logs) > 1:
            boundary_lines.append({
                "timestamp": last.get("timestamp", ""),
                "level": last.get("level", ""),
                "message": last.get("message", ""),
                "source": "flow_boundary_last",
            })

    logger.info(
        "Reducer applied evidence guardrails",
        decision_vocabulary_lines=len(decision_lines),
        decision_vocabulary_omitted=decision_lines_omitted,
        boundary_lines=len(boundary_lines),
        rare_templates=sum(1 for c in processed if c.get("classification") == "rare"),
    )

    return {
        "clusters": processed,
        "decision_vocabulary_lines": decision_lines,
        # How many matches the bound dropped. Rendered explicitly by the
        # formatter: an omission the model cannot see is an omission it will
        # reason as though it never existed.
        "decision_vocabulary_omitted": decision_lines_omitted,
        "boundary_lines": boundary_lines,
    }


def _bound_head_and_tail(items: list, limit: int) -> tuple:
    """Keep the first and last `limit // 2` items. Returns (kept, omitted).

    Head-and-tail rather than a plain head slice because both ends of a
    decision sequence carry information -- what the flow set out to do, and
    what it concluded -- while the middle is where the repetition lives.
    """
    if limit <= 0 or len(items) <= limit:
        return items, 0

    head = limit // 2
    tail = limit - head
    return items[:head] + items[-tail:], len(items) - limit
