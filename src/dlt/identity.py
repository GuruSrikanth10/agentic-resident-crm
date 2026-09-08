"""Phase 3 of DLT_PLAN.md -- case identity.

    case_id = dlt-{original_topic}-{partition}-{offset}

`(topic, partition, offset)` is the only naturally unique, naturally idempotent
key available on a DLT message. It survives redrive: if a developer replays
from the DLT after shipping a fix, the same record yields the same case id and
the existing terminal-status check skips it.

`refId` is deliberately **not** the case id. One packet can fail at several
stages and produce several distinct DLT messages, and keying on refId would
collapse them into one case and lose all but the first.

The generated id is interpolated into filesystem paths and S3 keys by the
storage layer, so it must satisfy `EVENT_ID_PATTERN` from
`src.models.schemas` -- the same guard that stops a `../../` eventId escaping
the storage root.
"""
import hashlib
import re
from typing import Optional

from src.models.schemas import EVENT_ID_PATTERN

CASE_ID_PREFIX = "dlt"

#: Matches the `{1,128}` bound in EVENT_ID_PATTERN.
MAX_CASE_ID_LENGTH = 128

#: Characters the pattern permits. Anything else becomes "-".
_DISALLOWED = re.compile(r"[^A-Za-z0-9_.:-]")

_VALID = re.compile(EVENT_ID_PATTERN)

#: Hash suffix length used when a long topic name has to be truncated.
_HASH_LENGTH = 10


def sanitise(text: str) -> str:
    return _DISALLOWED.sub("-", str(text))


def is_valid_case_id(case_id: Optional[str]) -> bool:
    return bool(case_id) and bool(_VALID.match(case_id))


def derive_case_id(topic: Optional[str],
                   partition: Optional[int],
                   offset: Optional[int]) -> Optional[str]:
    """Build a case id from record coordinates.

    Returns None when any coordinate is missing: the caller must then fall
    back to the DLT record's own coordinates, which the consumer always has.
    Inventing a placeholder here would let two different messages collide on
    one case id, and the second would be silently skipped as a duplicate.
    """
    if topic is None or partition is None or offset is None:
        return None

    suffix = f"-{partition}-{offset}"
    head = f"{CASE_ID_PREFIX}-{sanitise(topic)}"
    case_id = head + suffix

    if len(case_id) > MAX_CASE_ID_LENGTH:
        # Truncate the topic, not the coordinates: the coordinates are what
        # make the id unique. A hash of the full topic keeps two long topics
        # sharing a prefix from colliding.
        digest = hashlib.sha256(str(topic).encode("utf-8")).hexdigest()[:_HASH_LENGTH]
        room = MAX_CASE_ID_LENGTH - len(suffix) - len(digest) - len(CASE_ID_PREFIX) - 2
        head = f"{CASE_ID_PREFIX}-{sanitise(topic)[:max(0, room)]}-{digest}"
        case_id = head + suffix

    return case_id if is_valid_case_id(case_id) else None
