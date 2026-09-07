"""
Log Reduction Pipeline - Configuration and Constants.

All tunables for the 6-stage pipeline live here so they can be adjusted
without touching pipeline logic.
"""
import os
import re

from src.utils.paths import CATALOG_PATH, DRAIN3_STATE_DIR  # noqa: F401

# ---------------------------------------------------------------------------
# Stage 2 -- ERROR branch
# ---------------------------------------------------------------------------
# How many INFO lines preceding the first ERROR to keep as context.
ERROR_CONTEXT_LINES = int(os.environ.get("LOG_ERROR_CONTEXT_LINES", "200"))

# How many lines after the last ERROR to keep as trailing context. Without a
# cap, a cascading failure keeps everything from the first error to the end
# of the trace, which is effectively the whole log (1.11).
ERROR_TRAILING_LINES = int(os.environ.get("LOG_ERROR_TRAILING_LINES", "200"))

# Within the ERROR window, a template repeating at least this often is folded
# to its first occurrence plus a count. Lower than the clustered path's
# threshold because the window is a few hundred lines, not a whole flow -- a
# line appearing three times in 233 is already boilerplate here. WARN and ERROR
# lines are never folded regardless.
ERROR_REPEAT_THRESHOLD = int(os.environ.get("LOG_ERROR_REPEAT_THRESHOLD", "3"))

# ---------------------------------------------------------------------------
# Stage 2.5 -- Noise floor (applied AFTER raw_logs.txt is written)
# ---------------------------------------------------------------------------
# Severity floor for the text handed to the reducer. Framework DEBUG chatter
# -- shard hints, JPA transaction bookkeeping, integrity interceptors, SQL
# echo -- is ~50% of the lines and ~66% of the BYTES in a real trace, and
# none of it distinguishes one packet's outcome from another's.
#
# Applied after `_save_raw_logs`, never before: raw_logs.txt stays the
# complete, unfiltered record for audit. Only the LLM's copy is thinned, and
# the count of what was dropped is announced in the header rather than
# removed silently.
MIN_LEVEL = os.environ.get("LOG_MIN_LEVEL", "INFO").strip().upper()

#: Severity ordering used by the floor above. WARN and ERROR must always sit
#: above the default so no floor can ever discard them by accident.
LEVEL_ORDER = {"TRACE": 0, "DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}

# Collapse the column list of an echoed SQL statement to a count. These are
# 10.6% of the lines but 38.6% of the bytes (avg 465 chars) in the reference
# trace, and the diagnostic content of `select a,b,c,...,z from t where x=?`
# is entirely in the table and the predicate -- never in the 43 column names.
COLLAPSE_SQL = os.environ.get("LOG_COLLAPSE_SQL", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Stage 3 -- Drain3 clustering
# ---------------------------------------------------------------------------
# Path where the Drain3 TemplateMiner persists its parse tree between runs.
# Keeping it stable across invocations is what gives us stable template IDs.
# Re-exported from utils.paths rather than re-derived with a third
# dirname(dirname(dirname(...))) walk (G13).

# ---------------------------------------------------------------------------
# Stage 4 -- Evidence assembly guardrails
# ---------------------------------------------------------------------------
# Templates whose per-flow count is below this threshold are always kept in
# full (with example lines), never collapsed to count-only.
RARE_TEMPLATE_THRESHOLD = int(os.environ.get("LOG_RARE_TEMPLATE_THRESHOLD", "5"))

# Collapse a template to count-only once it repeats this often within a single
# flow, EVEN IF the catalog does not classify it as boilerplate.
#
# Without this the collapse branch in `apply_evidence_guardrails` was reachable
# only via a catalog classification, so a deployment that never ran
# `build_catalog` (every template classifies as "unknown") collapsed nothing at
# all: 107 of 139 templates were kept in full with up to 3 example lines each,
# and the "reduced" output came out ~1.8x LARGER than the trace it reduced.
# Frequency within the flow is evidence of boilerplate on its own; the catalog
# now improves this judgement instead of being the only thing that makes it.
BOILERPLATE_COUNT_THRESHOLD = int(
    os.environ.get("LOG_BOILERPLATE_COUNT", "5"))

# Decision-vocabulary matches are kept in FULL TEXT, so an unbounded list can
# make the "reduced" output larger than the raw trace it reduces. The default
# regex below matches `packet.*status`, `rejected` and `approved` -- among the
# most common strings in this domain's logs -- so 20,000 raw lines produced
# 20,000 full lines, ~1.1MB, ~285k tokens, in a prompt with a 60s timeout.
#
# The first and last half are kept rather than the first N: the decision
# sequence's beginning and end both carry information, the middle repeats.
MAX_DECISION_VOCABULARY_LINES = int(os.environ.get("LOG_MAX_DECISION_LINES", "300"))

# Final ceiling on the formatted string handed to the LLM. The per-section
# bounds above cap the parts; this caps the whole, including the ERROR branch,
# which can still emit LOG_ERROR_CONTEXT_LINES + LOG_ERROR_TRAILING_LINES plus
# every ERROR in between.
MAX_REDUCED_CHARS = int(os.environ.get("LOG_MAX_REDUCED_CHARS", "120000"))

# Decision-vocabulary regex -- any raw log line matching this is *always*
# forwarded to the LLM in full text, regardless of its Drain3 cluster
# classification.  Build this from your domain; err on the side of inclusion.
DECISION_VOCABULARY_REGEX = re.compile(
    os.environ.get(
        "LOG_DECISION_VOCAB_REGEX",
        r"(?i)"
        r"(?:approved|rejected|denied|final.?decision|rule\s.*triggered"
        r"|score.?threshold|validation.?failed|dedup.*reject"
        r"|packet.*status|enrolment.*result|biometric.*match"
        r"|MAN_DEDUP|operator.*reject|quality.*check.*fail)",
    )
)

# ---------------------------------------------------------------------------
# Stage 0 -- Offline template catalog
# ---------------------------------------------------------------------------
# Path to the persisted catalog JSON built by `build_catalog.py`.
# Also re-exported from utils.paths (G13).
