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
