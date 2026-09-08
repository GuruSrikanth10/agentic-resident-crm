"""Phase 3 of DLT_PLAN.md -- extracting the log-correlation identifier.

`refId` is the only thing that ties a DLT message to lines in the service's pod
logs, and the DLT *headers* do not carry it (DLT_PLAN.md 3.1).

**The Kafka record key does.** The 2026-08-20 sample settles Open Question 1:
the record is keyed on the refId (`c5d21184-...`), and the same value appears
in the payload at `abisMWResponseNewSeda.refId`. That makes the key the primary
source and the payload the corroborating one, not the other way round --

* the key survives a payload we cannot deserialise, which is precisely the case
  `DltAdapter` was built to keep alive. Before this, an undecodable payload
  meant no refId, no logs, and a guaranteed `UNVERIFIABLE` verdict on a case
  whose stacktrace was perfectly intact;
* the key is one scalar with no path to misconfigure, where the payload path
  differs per `__TypeId__` and is the thing most likely to go stale.

Four layers, in order: the record key, a configured path, the path registered
for the payload's `__TypeId__`, then a bounded search. Every result carries the
layer it came from, so a case that fell through to the search is visible as
such instead of looking identical to one read straight off the key.

**`event_id` is not `refId`.** The `EnrolmentEventResponse` payload carries a
top-level `event_id` that is a different UUID from the refId. This project
calls refId "the event id" throughout (DLT_PLAN.md 3), so it is the natural
field to reach for, and reaching for it fails silently -- a correlation id
matching no log line anywhere. `_DENY_KEYS` makes that unconfigurable.

A payload with no recoverable identifier is **not** an error. The case is still
recorded from its headers alone; the log lane is skipped and corroboration
comes out `UNVERIFIABLE`. Losing the message would be worse than losing its
logs.

Pure functions, no I/O.
"""
import json
import os
import re
from collections import deque
from dataclasses import dataclass
from typing import Any, Optional

from src.models.dlt_payload_schemas import REFID_PATHS_BY_TYPE
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Keys searched when no configured path matches. Ordered by likelihood.
DEFAULT_REFID_KEYS = ("refId", "ref_id", "referenceId")

#: Keys that look like an identifier and are not this one. Never searched, and
#: stripped out of `DLT_REFID_KEYS` if configured. `event_id`/`eventId` is the
#: payload's own id -- a different UUID from the refId in the 2026-08-20
#: sample. `candidateRefId` belongs to a *different* enrolment: the failing
#: dedupe lookup iterates other people's refIds, and matching logs on one would
#: pull in an unrelated packet's lines while missing this one entirely.
_DENY_KEYS = frozenset({
    "event_id", "eventId",
    "candidateRefId", "candidate_ref_id",
    "requestId", "request_id",
})

#: Depth cap on the fallback search. A malformed or hostile payload must not
#: turn identifier extraction into an unbounded walk.
MAX_SEARCH_DEPTH = 8

#: Node cap, for the same reason: a wide payload is as expensive as a deep one.
MAX_SEARCH_NODES = 2000


def refid_path() -> Optional[str]:
    return os.environ.get("DLT_REFID_PATH", "").strip() or None


def refid_keys() -> tuple:
    """Keys the fallback search will accept, with the denylist enforced.

    Filtering here rather than trusting config is deliberate: `event_id` is
    both the obvious thing for an operator to add and a guaranteed silent
    failure (see the module docstring). A denied key is dropped with a warning
    rather than rejected outright, so one bad entry does not disable the rest.
    """
    raw = os.environ.get("DLT_REFID_KEYS", "").strip()
    if not raw:
        return DEFAULT_REFID_KEYS

    parsed, denied = [], []
    for key in (k.strip() for k in raw.split(",")):
        if not key:
            continue
        (denied if key in _DENY_KEYS else parsed).append(key)

    if denied:
        logger.warning(
            "Ignoring denied keys in DLT_REFID_KEYS; these are not the log "
            "correlation id and would silently match no log line",
            denied=denied)

    return tuple(parsed) or DEFAULT_REFID_KEYS


def _scalar(value: Any) -> Optional[str]:
    """Accept only scalars an identifier can plausibly be.

    A dict or list at the target path means the path is wrong, not that the
    identifier is a dict -- returning `str(value)` there would silently poison
    every log query with `{'refId': ...}`.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (str, int)):
        text = str(value).strip()
        return text or None
    return None


def extract_by_path(payload: Any, path: Optional[str]) -> Optional[str]:
    """Follow a dotted path. Numeric segments index into lists.

    `packetMetaData.refId`, `events.0.refId`. Any missing segment yields None
    rather than raising -- a stale configured path degrades to the fallback
    search instead of dropping the message.
    """
    if not path or payload is None:
        return None

    node = payload
    for segment in path.split("."):
        if isinstance(node, dict):
            if segment not in node:
                return None
            node = node[segment]
        elif isinstance(node, list):
            if not segment.lstrip("-").isdigit():
                return None
            try:
                node = node[int(segment)]
            except IndexError:
                return None
        else:
            return None
    return _scalar(node)


def extract_by_search(payload: Any,
                      keys: Optional[tuple] = None,
                      max_depth: int = MAX_SEARCH_DEPTH) -> Optional[str]:
    """Breadth-first search for a refId-shaped key.

    Breadth-first rather than depth-first so a top-level identifier wins over
    one buried inside a nested candidate list -- in the reference trace the
    failing lookup is for a dedup *candidate*, so a nested refId may well
    belong to a different packet than the one that failed.
    """
    if payload is None:
        return None

    wanted = keys or refid_keys()
    queue = deque([(payload, 0)])
    visited = 0

    while queue:
        node, depth = queue.popleft()
        visited += 1
        if depth > max_depth or visited > MAX_SEARCH_NODES:
            break

        if isinstance(node, dict):
            for key in wanted:
                if key in node:
                    found = _scalar(node[key])
                    if found:
                        return found
            for value in node.values():
                if isinstance(value, (dict, list)):
                    queue.append((value, depth + 1))
        elif isinstance(node, list):
            for value in node:
                if isinstance(value, (dict, list)):
                    queue.append((value, depth + 1))

    return None


def extract_ref_id(payload: Any, type_id: Optional[str] = None) -> Optional[str]:
    """The payload's refId: configured path, registered path, then search.

    Payload-only. `resolve_ref_id` is the entry point that also considers the
    record key; this one stays payload-scoped so the two sources can be
    compared against each other.
    """
    return (extract_by_path(payload, refid_path())
            or _extract_by_type(payload, type_id)
            or extract_by_search(payload))


def _extract_by_type(payload: Any, type_id: Optional[str]) -> Optional[str]:
    for path in paths_for_type(type_id):
        found = extract_by_path(payload, path)
        if found:
            return found
    return None


# ---------------------------------------------------------------------------
# Per-__TypeId__ paths
# ---------------------------------------------------------------------------

def paths_for_type(type_id: Optional[str]) -> tuple:
    """Ordered refId paths registered for a payload type.

    The env override wins over the built-in registry so a path that moves in
    production is a restart, not a release -- the property DLT_PLAN.md 5.3 asks
    for now that a second payload schema is confirmed on the DLT.
    """
    if not type_id:
        return ()

    override = _paths_override().get(type_id)
    if override:
        return override
    return REFID_PATHS_BY_TYPE.get(type_id, ())


def _paths_override() -> dict:
    """Parse `DLT_REFID_PATHS_BY_TYPE`: {"<typeId>": "path" | ["p1", "p2"]}.

    Malformed JSON degrades to the built-in registry rather than raising: a
    typo in config must not take the consumer down.
    """
    raw = os.environ.get("DLT_REFID_PATHS_BY_TYPE", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        logger.warning("DLT_REFID_PATHS_BY_TYPE is not valid JSON; ignoring it")
        return {}
    if not isinstance(parsed, dict):
        logger.warning("DLT_REFID_PATHS_BY_TYPE is not a JSON object; ignoring it")
        return {}

    out = {}
    for type_id, paths in parsed.items():
        if isinstance(paths, str):
            out[str(type_id)] = (paths,)
        elif isinstance(paths, (list, tuple)):
            out[str(type_id)] = tuple(str(x) for x in paths if x)
    return out


# ---------------------------------------------------------------------------
# The record key
# ---------------------------------------------------------------------------

#: A key we will accept as a refId. Bounded and character-restricted because a
#: Kafka key is whatever the producer chose -- a routing token like "ABIS1", a
#: partition number, a compound string. Feeding one of those to the log query
#: would return either nothing or, worse, another packet's lines. The sample's
#: 36-char UUID passes; a short token or a bare integer does not.
_KEY_SHAPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}$")


def decode_key(raw: Any) -> Optional[str]:
    """kafka-python hands back `bytes | None`; make it a stripped str or None."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="replace").strip()
    else:
        text = str(raw).strip()
    return text or None


def key_as_ref_id(key: Optional[str]) -> Optional[str]:
    """The record key, if it is shaped like a correlation id."""
    if not key:
        return None
    return key if _KEY_SHAPE.match(key) else None


@dataclass(frozen=True)
class RefIdExtraction:
    """Where the refId came from, and whether the sources agreed.

    Provenance is carried rather than discarded because the four layers are not
    equally trustworthy. A `search` result on an unregistered payload type is a
    guess that happened to land; a `record_key` result is the producer's own
    partitioning key. An operator triaging "why did this case have no logs"
    needs to see which one they got.
    """

    ref_id: Optional[str] = None

    #: "record_key" | "configured_path" | "type_path" | "search" | "none"
    source: str = "none"

    #: What the payload said, independently of the key. Kept even when the key
    #: won, so a disagreement is inspectable rather than merely flagged.
    payload_ref_id: Optional[str] = None

    #: True when key and payload both yielded a value and they differ.
    mismatch: bool = False


def resolve_ref_id(payload: Any,
                   key: Optional[str] = None,
                   type_id: Optional[str] = None) -> RefIdExtraction:
    """The log-correlation id for one DLT record, with its provenance.

    The key wins a disagreement. It is set by the producer and is what the
    original record was partitioned on, whereas the payload path is per-type
    config and the likeliest thing to be stale. The disagreement is still
    recorded and surfaces as an evidence gap -- silently picking one of two
    identifiers that should have been equal is how a misconfigured path stays
    invisible for months.
    """
    from_key = key_as_ref_id(key)

    from_payload = extract_by_path(payload, refid_path())
    source = "configured_path" if from_payload else None
    if not from_payload:
        from_payload = _extract_by_type(payload, type_id)
        source = "type_path" if from_payload else None
    if not from_payload:
        from_payload = extract_by_search(payload)
        source = "search" if from_payload else None

    if from_key:
        return RefIdExtraction(
            ref_id=from_key,
            source="record_key",
            payload_ref_id=from_payload,
            mismatch=bool(from_payload) and from_payload != from_key,
        )

    return RefIdExtraction(
        ref_id=from_payload,
        source=source or "none",
        payload_ref_id=from_payload,
        mismatch=False,
    )
