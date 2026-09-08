"""
Stages 2, 3, 4 -- Log Reduction Engine.

Stage 2: Branch on ERROR presence.
Stage 3: Drain3 clustering with persisted state.
Stage 4: Evidence assembly guardrails.
"""
import os
import re
import threading
from typing import Optional

from filelock import FileLock

from src.log_pipeline.config import (
    BOILERPLATE_COUNT_THRESHOLD,
    COLLAPSE_SQL,
    DECISION_VOCABULARY_REGEX,
    DRAIN3_STATE_DIR,
    ERROR_CONTEXT_LINES,
    ERROR_REPEAT_THRESHOLD,
    ERROR_TRAILING_LINES,
    LEVEL_ORDER,
    MAX_DECISION_VOCABULARY_LINES,
    MIN_LEVEL,
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
# Stage 2.5 -- Noise floor
# ======================================================================

#: `select <cols> from ...`, `insert into t (<cols>) values (<vals>)` and
#: `update t set <assignments> where ...`. Only the list between the anchors is
#: replaced -- the statement kind, the table and the predicate all survive,
#: because those are the parts that say what the flow was doing.
_SQL_SELECT = re.compile(r"^(\s*select\s+)(.+?)(\s+from\s+.*)$", re.I | re.S)
_SQL_INSERT = re.compile(r"^(\s*insert\s+into\s+\S+\s*\()([^)]*)(\).*)$", re.I | re.S)
_SQL_UPDATE = re.compile(r"^(\s*update\s+\S+\s+set\s+)(.+?)(\s+where\s+.*)$", re.I | re.S)


def _count_items(fragment: str) -> int:
    """Count comma-separated items, ignoring commas inside parentheses.

    `count(*), coalesce(a, b)` is two items, not three -- a naive split would
    over-report and make the collapsed marker wrong.
    """
    depth = items = 0
    seen = False
    for char in fragment:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items += 1
        if not char.isspace():
            seen = True
    return items + 1 if seen else 0


def collapse_sql(message: str) -> str:
    """Replace a SQL column/assignment list with a count of its entries.

    Returns the message unchanged when it is not one of the three recognised
    shapes, so an unfamiliar statement is never mangled into something the
    Investigator would read as truncated evidence.
    """
    if not COLLAPSE_SQL or not message:
        return message
    for pattern, noun in ((_SQL_SELECT, "columns"),
                          (_SQL_INSERT, "columns"),
                          (_SQL_UPDATE, "assignments")):
        match = pattern.match(message)
        if not match:
            continue
        head, body, tail = match.groups()
        count = _count_items(body)
        # One or two items is already shorter than the marker replacing it.
        if count < 3:
            return message
        return f"{head}<{count} {noun} elided>{tail}"
    return message


def apply_noise_floor(logs: list[dict]) -> tuple:
    """Drop sub-threshold lines and collapse SQL. Returns (kept, report).

    Runs AFTER raw_logs.txt is persisted, so this thins only the copy the LLM
    reads; the audit record keeps every line at full length.

    An unrecognised level is kept rather than dropped. `parse_line` defaults to
    INFO when it cannot find a level, but a source that emits something outside
    LEVEL_ORDER would otherwise have its whole trace silently deleted by a
    floor it was never measured against.
    """
    # Clamped at WARN: the floor exists to strip framework DEBUG chatter, and
    # no configuration of it may discard a warning or an error. `branch_on_error`
    # keys off ERROR, and a WARN like "Integrity verification failed." is
    # exactly the evidence this pipeline exists to surface -- a misconfigured
    # LOG_MIN_LEVEL=ERROR must not be able to delete it.
    floor = min(LEVEL_ORDER.get(MIN_LEVEL, LEVEL_ORDER["INFO"]), LEVEL_ORDER["WARN"])
    kept, dropped, collapsed, saved = [], 0, 0, 0

    for record in logs:
        rank = LEVEL_ORDER.get((record.get("level") or "").upper())
        if rank is not None and rank < floor:
            dropped += 1
            continue
        message = record.get("message", "")
        shortened = collapse_sql(message)
        if shortened != message:
            collapsed += 1
            saved += len(message) - len(shortened)
            record = {**record, "message": shortened}
        kept.append(record)

    report = {
        "dropped_below_level": dropped,
        "min_level": MIN_LEVEL,
        "sql_collapsed": collapsed,
        "sql_chars_saved": saved,
    }
    if dropped or collapsed:
        logger.info("Reducer applied the noise floor", **report)
    return kept, report


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


def local_template_ids(logs: list[dict]) -> list[str]:
    """Group these lines by template, using a miner local to this call.

    Deliberately NOT the shared, file-persisted miner that `cluster_logs`
    uses. Two reasons, and the first is the important one:

    * Determinism. Feeding the ERROR window into the shared tree trains it,
      so a packet that took the ERROR branch would silently change how the
      NEXT packet's clustered path groups its lines -- the reduced evidence
      for packet B would depend on whether packet A happened to be analysed
      first. Verified: routing this through the shared miner made one
      packet's output differ by 111 lines between a cold and a warm run.
      A per-packet investigation must not depend on processing history.

    * Scope. Folding repeats inside one window only needs template identity
      *within that window*. Globally stable ids buy nothing here, and the
      shared tree costs a file lock and a state write to obtain them.

    Returns one id per input line, in order.
    """
    if not logs:
        return []

    from drain3 import TemplateMiner
    from drain3.template_miner_config import TemplateMinerConfig

    # persistence_handler=None -> in-memory only; nothing is written and
    # nothing from previous packets is read.
    miner = TemplateMiner(None, TemplateMinerConfig())
    return [f"t_{miner.add_log_message(entry.get('message', ''))['cluster_id']:04d}"
            for entry in logs]


def collapse_error_window(logs: list[dict]) -> tuple:
    """Suppress repeated boilerplate inside the ERROR branch's window.

    The ERROR branch never clustered: it emitted every line of its
    (ERROR_CONTEXT_LINES + ERROR_TRAILING_LINES) window verbatim, which is why
    it stayed ~37k characters while the clustered path fell to ~18k.

    Clustering it outright is wrong -- this branch exists to show the *sequence*
    leading into a failure, and a cluster summary destroys the ordering the
    Investigator reads. So the trace stays chronological and line-by-line, and
    only the repeats are folded: the first occurrence of a repeated template is
    kept in place, later ones are replaced by a single marker at that first
    occurrence.

    Never folds a WARN or an ERROR, however often it repeats -- in this branch
    those lines are the evidence.

    Returns (records, suppressed_count) where records may contain synthetic
    `_note` entries the formatter renders as an inline marker.
    """
    if len(logs) < ERROR_REPEAT_THRESHOLD:
        return logs, 0

    ids = local_template_ids(logs)
    counts: dict[str, int] = {}
    last_ts: dict[str, str] = {}
    for record, tid in zip(logs, ids):
        counts[tid] = counts.get(tid, 0) + 1
        last_ts[tid] = record.get("timestamp", "")

    out, seen, suppressed = [], set(), 0
    for record, tid in zip(logs, ids):
        level = (record.get("level") or "").upper()
        if level in ("ERROR", "WARN") or counts[tid] < ERROR_REPEAT_THRESHOLD:
            out.append(record)
            continue
        if tid not in seen:
            seen.add(tid)
            out.append(record)
            out.append({"_note": (f"(+{counts[tid] - 1} further lines matching this "
                                  f"template, last at {last_ts[tid]})")})
            continue
        suppressed += 1

    if suppressed:
        logger.info("Reducer folded repeats in the ERROR window",
                    input_lines=len(logs), suppressed=suppressed,
                    kept=len(logs) - suppressed)
    return out, suppressed


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
                    "has_error": False,
                }

            meta = cluster_meta[cluster_id]
            meta["last_seen"] = ts
            meta["count"] += 1
            # Carried so Stage 4 can refuse to collapse a template that ever
            # carried a WARN or ERROR, however often it repeats. Losing an
            # error to a frequency heuristic is the expensive direction.
            if (log_entry.get("level", "") or "").upper() in ("ERROR", "WARN"):
                meta["has_error"] = True
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
                "has_error": meta.get("has_error", False),
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
    collapsed_repetitive = 0
    for cluster in clusters:
        classification = cluster.get("classification", "unknown")
        count = cluster.get("count", 0)

        # Order matters, and it used to be wrong. The rare check ran first and
        # short-circuited, so a template the catalog had classified as
        # boilerplate -- on the cross-flow evidence that it appears in ~every
        # flow regardless of outcome -- was still kept in full whenever it
        # happened to appear fewer than RARE_TEMPLATE_THRESHOLD times in THIS
        # flow. On the reference trace that is 89 of ~123 templates per flow,
        # which is why building a catalog appeared to change nothing at all.
        #
        # The precedence below says what we actually mean:
        #   1. anything that ever errored is evidence           -> keep
        #   2. known noise in every flow beats local rarity     -> collapse
        #   3. an unknown template we saw once or twice         -> keep
        #   4. an unknown template that repeats within the flow -> collapse

        # 1. Never collapse a template that ever carried a WARN or ERROR, no
        # matter how often it repeated.
        if cluster.get("has_error"):
            processed.append(cluster)
            continue

        # 2. Boilerplate: collapse to count-only (no examples). Cross-flow
        # evidence, so it outranks this flow's count either way.
        if classification == "boilerplate":
            cluster["examples"] = []
            processed.append(cluster)
            continue

        # 3. Rare AND unclassified: keep examples. The guard protects
        # templates we know nothing about -- not ones the catalog has already
        # told us are noise.
        if count < RARE_TEMPLATE_THRESHOLD:
            cluster["classification"] = "rare"
            # examples are already populated from Stage 3
            processed.append(cluster)
            continue

        # 4. Repetitive-within-this-flow: collapse too. Without this the
        # branch above was the ONLY route to a collapse, so a deployment with
        # no template catalog (every classification "unknown") collapsed
        # nothing and the "reduced" output came out larger than its input.
        if count >= BOILERPLATE_COUNT_THRESHOLD:
            cluster["classification"] = "repetitive"
            cluster["examples"] = []
            collapsed_repetitive += 1
            processed.append(cluster)
            continue

        # Everything else (informative, decision-marker): keep examples
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
        collapsed_repetitive=collapsed_repetitive,
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
