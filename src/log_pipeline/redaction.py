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


def active_patterns():
    """All patterns to apply, defaults first. Allocation-free in the common
    case where no extra patterns are configured."""
    extra = _extra_patterns()
    return DEFAULT_PATTERNS if not extra else DEFAULT_PATTERNS + extra


#: JSON keys whose values are personal data (MULTI_SERVICE_PLAN.md Phase 6).
#:
#: The patterns above recognise a value by its shape, and names, dates of
#: birth, genders and addresses have none -- so these are recognised by the key
#: they are logged under instead: the value of `"<key>": ...` is replaced,
#: whatever it is (a string, a number, an object or an array), in plain or
#: escaped JSON, at any depth. Matched case-insensitively. Redaction is global
#: (D12): the list is the union of every service's sensitive keys, applied to
#: every packet's logs whatever its service, because it must not depend on the
#: service having been resolved correctly. `REDACT_JSON_KEYS` replaces it; the
#: keys actually logged come from sampling each service (Phase 0).
DEFAULT_JSON_KEYS = (
    # The resident's name, in any script.
    "name", "fullName", "firstName", "middleName", "lastName", "residentName",
    "localName", "nameLocal",
    # Relatives' names.
    "fatherName", "motherName", "spouseName", "husbandName", "guardianName",
    "parentName", "relativeName", "careOf",
    # Birth and gender.
    "dob", "dateOfBirth", "yearOfBirth", "gender",
    # The address.
    "address", "addressLine1", "addressLine2", "addressLine3", "house",
    "houseNumber", "street", "landmark", "locality", "vtc", "village",
    "subDistrict", "district", "city", "postOffice", "pincode", "postalCode",
)

#: The label, and the placeholder `[REDACTED:JSON_FIELD]`, of a value
#: redacted by its key.
JSON_FIELD_LABEL = "JSON_FIELD"

#: The backslashes before a key's quotes at each level of JSON-in-a-string
#: escaping: `"k"`, `\"k\"`, `\\\"k\\\"`, and one more level.
_ESCAPE_LEVELS = frozenset({0, 1, 3, 7})

#: (env value -> (keys, compiled key pattern)), cached like the extra
#: patterns above.
_json_keys_cache: dict = {}


def _json_key_config() -> tuple:
    raw = os.environ.get("REDACT_JSON_KEYS", "").strip()
    cached = _json_keys_cache.get(raw)
    if cached is not None:
        return cached

    source = raw.split(",") if raw else DEFAULT_JSON_KEYS
    keys = tuple(dict.fromkeys(key.strip().lower() for key in source if key.strip()))
    alternatives = "|".join(re.escape(key) for key in sorted(keys, key=len, reverse=True))
    # The same run of backslashes before both quotes of the key: the key and
    # its value are then at one level of escaping, which says how the value's
    # own quotes are escaped.
    pattern = re.compile(rf'(?P<bs>\\*)"(?P<key>{alternatives})(?P=bs)"\s*:\s*',
                         re.IGNORECASE) if keys else None
    with _extra_patterns_lock:
        if len(_json_keys_cache) > 16:
            _json_keys_cache.clear()
        _json_keys_cache[raw] = (keys, pattern)
    return keys, pattern


def json_keys() -> tuple:
    """The keys whose values `redact_text` replaces, lower-cased and
    de-duplicated. `REDACT_JSON_KEYS` (comma-separated) replaces the default
    list; blank means the default, never none."""
    return _json_key_config()[0]


def _backslashes_before(text: str, index: int, floor: int) -> int:
    count = index - 1
    while count >= floor and text[count] == "\\":
        count -= 1
    return index - 1 - count


def _string_end(text: str, opening: int, level: int) -> int:
    """Just past the quote closing the string opened by the quote at
    `opening`, or the end of the text for an unterminated one.

    At escaping level `level` (the backslashes in the delimiter), a quote
    closes the string when the backslashes before it number `level` modulo
    2 * (level + 1): at level 0 an even run, at level 1 one, five, nine... A
    run of any other length escapes the quote into the value.
    """
    period = 2 * (level + 1)
    index = opening + 1
    while True:
        index = text.find('"', index)
        if index < 0:
            return len(text)
        if _backslashes_before(text, index, opening + 1) % period == level:
            return index + 1
        index += 1


def _container_end(text: str, start: int, level: int) -> int:
    """Just past the bracket closing the object or array opening at `start`,
    skipping strings, or the end of the text for an unterminated one."""
    period = 2 * (level + 1)
    depth, index = 0, start
    while index < len(text):
        char = text[index]
        if char == '"' and _backslashes_before(text, index, start) % period == level:
            index = _string_end(text, index, level)
            continue
        if char in "{[":
            depth += 1
        elif char in "}]":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return len(text)


_SCALAR = re.compile(r'[^,}\]\s\\"]+')


def _value_end(text: str, start: int, level: int) -> Optional[int]:
    """Where the value starting at `start` ends, or None when there is
    nothing to redact: an empty string, null, a boolean, a value already
    redacted, or an escaping level this does not recognise.

    A value cut off by the end of the line is redacted to the end: a
    truncated log line must not be where personal data gets through.
    """
    if level not in _ESCAPE_LEVELS or start >= len(text):
        return None
    quote = "\\" * level + '"'
    if text.startswith(quote, start):
        opening = start + level
        if text.startswith("[REDACTED:", opening + 1):
            return None
        end = _string_end(text, opening, level)
        return None if end == opening + 2 + level else end
    if text[start] in "{[":
        return _container_end(text, start, level)
    match = _SCALAR.match(text, start)
    if not match or match.group().lower() in ("null", "true", "false"):
        return None
    return match.end()


def redact_json_fields(text: str) -> tuple:
    """(text, count): `text` with the value of every `json_keys()` key
    replaced by a placeholder quoted as the key is."""
    if '"' not in text:
        return text, 0
    pattern = _json_key_config()[1]
    if pattern is None:
        return text, 0

    pieces, position, hits = [], 0, 0
    while True:
        match = pattern.search(text, position)
        if not match:
            break
        level = len(match.group("bs"))
        start = match.end()
        end = _value_end(text, start, level)
        pieces.append(text[position:start])
        position = start
        if end is None:
            continue
        quote = "\\" * level + '"'
        pieces.append(f"{quote}[REDACTED:{JSON_FIELD_LABEL}]{quote}")
        position = end
        hits += 1
    pieces.append(text[position:])
    return "".join(pieces), hits


def redact_text(text: str, allowlist: Optional[list] = None) -> RedactionResult:
    """Redact PII from one string.

    `allowlist` holds internal correlation ids (eventId, refId, srn) that must
    survive: they are operational identifiers, not resident PII, and scrubbing
    them would destroy the investigation. They are stashed behind sentinels
    before redaction and restored afterwards, so a 12-digit refId is not
    mistaken for an Aadhaar number.

    The values of personal-data JSON keys are replaced first
    (`redact_json_fields`), then every pattern is applied to what is left.
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

    text, hits = redact_json_fields(text)
    if hits:
        counts[JSON_FIELD_LABEL] = hits

    for label, pattern in active_patterns():
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
