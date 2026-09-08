"""Phase 3 of DLT_PLAN.md -- the DLT wire model.

What the DLT consumer POSTs to the API, and what it republishes onto the
analysis queue. Deliberately *not* `MessagePayload`: that model requires
`eventId` and `packetExecutionSummary`, neither of which exists on a DLT
message, so validating one against the other would reject every message.

The model carries raw headers and the raw payload, not a parsed failure.
Parsing is pure and deterministic (`src/dlt/stacktrace.py`), so the API
re-derives it rather than trusting a client-supplied summary -- one parser,
one result, no chance of the two drifting apart.
"""
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from src.models.schemas import EVENT_ID_PATTERN


class DltMessage(BaseModel):
    """One dead-lettered record, on its way to analysis."""

    # `extra="allow"` for the same reason `PacketMetaData` allows it: upstream
    # adds fields, and rejecting a message over an unknown key would turn a
    # schema addition into an outage.
    model_config = ConfigDict(extra="allow")

    #: `dlt-{topic}-{partition}-{offset}`. Interpolated into storage paths, so
    #: it carries the same pattern guard as `eventId`.
    case_id: str = Field(pattern=EVENT_ID_PATTERN)

    #: Spring DLT headers, decoded to str. Values may be None; a header whose
    #: value did not decode is kept rather than dropped.
    headers: Dict[str, Optional[str]] = Field(default_factory=dict)

    #: The deserialised payload. Still `Any`, even though
    #: `src/models/dlt_payload_schemas.py` now models the known shapes: those
    #: models describe and locate, they do not gate. Validating here would turn
    #: an upstream field addition into a rejected message, and the payload is
    #: the one part of a DLT record whose evidence we can most afford to lose.
    payload: Optional[Any] = None

    #: Set only when the payload could not be parsed as JSON, so the raw text
    #: is still available for a human.
    payload_raw: Optional[str] = None

    #: The Kafka record key, verbatim. The 2026-08-20 sample is keyed on the
    #: refId, which makes this the primary correlation source -- and the only
    #: one that survives a payload we cannot deserialise. Carried raw as well
    #: as resolved so a key we declined to use is still visible.
    record_key: Optional[str] = None

    #: Log-correlation identifier. None is a valid state: the case proceeds
    #: header-only and skips the log lane.
    ref_id: Optional[str] = None

    #: Which layer produced `ref_id`: "record_key", "configured_path",
    #: "type_path", "search", or "none". The layers are not equally
    #: trustworthy, and a case that fell through to the search should not look
    #: identical to one read off the record key.
    ref_id_source: str = "none"

    #: What the payload said, independently of the key. Equal to `ref_id` in
    #: the normal case; kept separately so a disagreement can be inspected
    #: rather than merely flagged.
    payload_ref_id: Optional[str] = None

    #: True when the record key and the payload both carried an identifier and
    #: they disagreed. The key wins; this is what makes the disagreement
    #: visible instead of silently resolved.
    ref_id_mismatch: bool = False
