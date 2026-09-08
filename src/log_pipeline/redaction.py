"""
PII redaction (KUBERNETES_LOGS_PLAN.md 5.9).

Raw pod logs are unfiltered, where the Elasticsearch path source-filtered to
four fields. In a biometric enrolment context they may carry resident
identifiers or entire request payloads -- and that text is persisted to
`raw_logs.txt`, to the log snapshot, and potentially to S3 via the
>5000-character offload.

ORDERING IS LOAD-BEARING:

    fetch -> filter by identifier -> extract context -> REDACT -> persist

Redaction runs *after* identifier filtering, because the raw identifier must
still be matchable, and *before* any persistence, so unredacted text never
reaches disk.

Placeholders are retained rather than deleting the matched text, so the LLM
can see that a value existed and reason about its presence.
"""
import os
import re
import threading
from dataclasses import dataclass, field
from typing import Optional

from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Ordered longest-first. A 16-digit VID must be matched before the 12-digit
#: Aadhaar pattern gets a chance at its leading digits. Word boundaries make
#: this robust, but ordering keeps it obvious.
DEFAULT_PATTERNS = (
    ("VID", re.compile(r"\b\d{16}\b")),
    ("AADHAAR", re.compile(r"\b\d{4}[ -]\d{4}[ -]\d{4}\b")),
    ("AADHAAR", re.compile(r"\b\d{12}\b")),
    ("MOBILE", re.compile(r"\b[6-9]\d{9}\b")),
    ("EMAIL", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
)

_ALLOWLIST_TOKEN = "\x00ALLOW{}\x00"


@dataclass
class RedactionResult:
    text: str
    counts: dict = field(default_factory=dict)


def is_enabled() -> bool:
    return get_bool_env("K8S_REDACT_ENABLED", True)


#: Cache of (env value -> compiled pattern tuple). Keyed on the raw env string
#: so a config change is picked up without a restart, while the steady state
#: costs one dict lookup.
#:
#: This used to re-read os.environ, split, and re.compile on *every record*.
#: That was tolerable when only the Kubernetes path redacted; now that every
#: source does (F10), an Elasticsearch fetch at LOG_MAX_DOCUMENTS=50000 would
#: pay it 50k times per packet (F18).
_extra_patterns_cache: dict = {}
_extra_patterns_lock = threading.Lock()


def _extra_patterns():
    raw = os.environ.get("K8S_REDACT_EXTRA_PATTERNS", "").strip()
    if not raw:
        return ()

    cached = _extra_patterns_cache.get(raw)
    if cached is not None:
        return cached

    compiled = []
    for pattern in raw.split("|||"):
        pattern = pattern.strip()
        if not pattern:
            continue
        try:
            compiled.append(("CUSTOM", re.compile(pattern)))
        except re.error as e:
            logger.warning("Ignoring invalid K8S_REDACT_EXTRA_PATTERNS entry",
                           pattern=pattern, error=str(e))

    compiled = tuple(compiled)
    with _extra_patterns_lock:
        # Bound the cache: the key is an env value, so it changes rarely, but
        # an unbounded dict keyed on external input is still a leak.
        if len(_extra_patterns_cache) > 16:
            _extra_patterns_cache.clear()
        _extra_patterns_cache[raw] = compiled
    return compiled


def _active_patterns():
    """All patterns to apply, defaults first. Allocation-free in the common
    case where no extra patterns are configured."""
    extra = _extra_patterns()
    return DEFAULT_PATTERNS if not extra else DEFAULT_PATTERNS + extra


def redact_text(text: str, allowlist: Optional[list] = None) -> RedactionResult:
    """Redact PII from one string.

    `allowlist` holds internal correlation ids (eventId, refId, srn) that must
    survive: they are operational identifiers, not resident PII, and scrubbing
    them would destroy the investigation. They are stashed behind sentinels
    before redaction and restored afterwards, so a 12-digit refId is not
    mistaken for an Aadhaar number.
    """
    if not text:
        return RedactionResult(text=text, counts={})

    counts = {}
    stashed = {}

    for index, value in enumerate(v for v in (allowlist or []) if v):
        token = _ALLOWLIST_TOKEN.format(index)
        if value in text:
            stashed[token] = value
            text = text.replace(value, token)

    for label, pattern in _active_patterns():
        text, hits = pattern.subn(f"[REDACTED:{label}]", text)
        if hits:
            counts[label] = counts.get(label, 0) + hits

    for token, value in stashed.items():
        text = text.replace(token, value)

    return RedactionResult(text=text, counts=counts)


def redact_records(records: list, allowlist: Optional[list] = None) -> dict:
    """Redact every record's message in place. Returns per-pattern counts.

    Counts are surfaced in `FetchDiagnostics` and logged, because
    over-redaction is a real risk -- a 12-digit correlation id outside the
    allowlist would be scrubbed -- and it should be visible rather than
    mysterious.
    """
    if not is_enabled():
        return {}

    totals = {}
    for record in records:
        result = redact_text(record.get("message", ""), allowlist=allowlist)
        record["message"] = result.text
        for label, count in result.counts.items():
            totals[label] = totals.get(label, 0) + count

    if totals:
        logger.info("Redaction applied", **{f"redacted_{k.lower()}": v
                                            for k, v in totals.items()})
    return totals
