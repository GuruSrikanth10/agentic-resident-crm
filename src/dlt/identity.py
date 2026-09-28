"""Phase 3 of DLT_PLAN.md -- case identity.

    case_id = dlt-{original_topic}-{partition}-{offset}[-g{consumer group digest}]

`(topic, partition, offset)` is the only naturally unique, naturally idempotent
key available on a DLT message -- with the consumer group, since two groups
can each dead-letter one original record (MULTI_SERVICE_PLAN.md Phase 8). It survives redrive: if a developer replays
from the DLT after shipping a fix, the same record yields the same case id and
the existing terminal-status check skips it.

`refId` is deliberately **not** the case id. One packet can fail at several
stages and produce several distinct DLT messages, and keying on refId would
collapse them into one case and lose all but the first. Storage was keyed on
it until Phase 8, with exactly that effect; `storage_key` now keeps the refId
as a prefix and adds the record.

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


def _digest(text: str) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()[:_HASH_LENGTH]


def derive_case_id(topic: Optional[str],
                   partition: Optional[int],
                   offset: Optional[int],
                   consumer_group: Optional[str] = None) -> Optional[str]:
    """Build a case id from record coordinates.

    Returns None when any coordinate is missing: the caller must then fall
    back to the DLT record's own coordinates, which the consumer always has.
    Inventing a placeholder here would let two different messages collide on
    one case id, and the second would be silently skipped as a duplicate.

    `consumer_group` is the group that gave up on the original record
    (MULTI_SERVICE_PLAN.md Phase 8). Two services consuming one topic under
    their own groups can each dead-letter the same original record; without
    the group the two would share a case id, and the second would be skipped
    as a duplicate of the first. It is appended as a digest, `-g<hash>`, so
    its length and characters never matter; the casebook names it in full.
    """
    if topic is None or partition is None or offset is None:
        return None

    suffix = f"-{partition}-{offset}"
    group = (consumer_group or "").strip()
    if group:
        suffix += f"-g{_digest(group)}"
    head = f"{CASE_ID_PREFIX}-{sanitise(topic)}"
    case_id = head + suffix

    if len(case_id) > MAX_CASE_ID_LENGTH:
        # Truncate the topic, not the coordinates: the coordinates are what
        # make the id unique. A hash of the full topic keeps two long topics
        # sharing a prefix from colliding.
        digest = _digest(topic)
        room = MAX_CASE_ID_LENGTH - len(suffix) - len(digest) - len(CASE_ID_PREFIX) - 2
        head = f"{CASE_ID_PREFIX}-{sanitise(topic)[:max(0, room)]}-{digest}"
        case_id = head + suffix

    return case_id if is_valid_case_id(case_id) else None


#: Joins a refId and its record's digest in a storage key.
STORAGE_KEY_SEPARATOR = "__"


def storage_key(ref_id: Optional[str], case_id: str) -> str:
    """Where one DLT record's case is stored: `<refId>__<digest of case_id>`.

    One key per record (MULTI_SERVICE_PLAN.md Phase 8). Keyed on the refId
    alone, a second dead-lettered record of the same packet -- from another
    service, or another stage -- found the first one's terminal casebook and
    was acknowledged without ever being analysed. The refId still leads the
    key, so an operator who knows only the refId finds every case of that
    packet under one prefix.

    The case id alone when there is no refId, or when the refId would not
    make a safe key.
    """
    if not ref_id:
        return case_id
    key = f"{ref_id}{STORAGE_KEY_SEPARATOR}{_digest(case_id)}"
    return key if len(key) <= MAX_CASE_ID_LENGTH and is_valid_case_id(key) else case_id
