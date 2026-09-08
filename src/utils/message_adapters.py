"""Phase 4 of DLT_PLAN.md -- per-role message handling for the Kafka consumer.

`kafkaConsumer.py` is role-agnostic in everything that matters -- offset
tracking, the rebalance listener, the worker semaphore, heartbeats, the
shutdown drain. Only four things differ between a rejection message and a
dead-lettered one:

    what makes it valid   MessagePayload vs a DLT case
    what makes it skippable   packetStatus, or a terminal case already on disk
    what identifies it        eventId vs case_id
    what gets POSTed          the payload, or headers + payload + case_id

Those four move here. `RejectionAdapter` is today's logic *moved, not rewritten*
-- same validation, same REJECTED filter, same terminal-casebook dedupe, same
raw-string DLQ payload on a poison pill -- so the live rejection path is
provably unchanged.

The split into `parse` (never raises) and `should_skip` (may raise) is not
cosmetic. It preserves the existing error semantics exactly: a poison pill
DLQs the *raw undecoded string*, while a storage fault during the dedupe check
DLQs the *parsed payload* and abandons the offset. Collapsing them into one
call would send the wrong thing to the DLQ in one of the two cases.
"""
import json
from dataclasses import dataclass
from typing import Optional, Protocol

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class ParseResult:
    """Outcome of turning a Kafka record into a request body.

    `error` set means poison: publish `raw_text` to the DLQ and commit.
    """

    body: Optional[dict] = None
    raw_text: Optional[str] = None
    error: Optional[str] = None

    @property
    def is_poison(self) -> bool:
        return self.error is not None


class MessageAdapter(Protocol):
    """How one consumer role turns records into work."""

    role: str

    def parse(self, msg) -> ParseResult:
        """Validate and build the request body. Must never raise."""
        ...

    def should_skip(self, body: dict) -> Optional[str]:
        """Reason to skip this message, or None to dispatch.

        May raise -- a storage fault here is routed to the DLQ by the caller,
        which is the existing behaviour.
        """
        ...

    def identity_of(self, body: dict) -> Optional[str]:
        """Storage key and log identifier for this message."""
        ...

    def timeout_casebook(self, body: dict) -> dict:
        """Terminal casebook written when the internal call times out."""
        ...

    def save_terminal(self, identity: str, casebook: dict) -> None:
        """Persist a terminal casebook to this role's own store."""
        ...


# ---------------------------------------------------------------------------
# Rejections -- today's behaviour, moved verbatim
# ---------------------------------------------------------------------------

class RejectionAdapter:
    """The fast and slow consumers. Behaviourally identical to pre-Phase-4.

    Both roles share this: the analysis queue carries the same payload shape as
    the rejections topic, so the poison-pill check and the terminal dedupe are
    the right guards for both, unmodified.
    """

    role = "rejection"

    def parse(self, msg) -> ParseResult:
        payload = msg.value.decode("utf-8", errors="replace")
        try:
            signal_payload = json.loads(payload)
            from src.models.schemas import MessagePayload

            MessagePayload(**signal_payload)
        except Exception as validation_err:
            logger.error("Poison-pill payload detected", error=str(validation_err))
            # The raw *string*, not the parsed dict -- publish_to_dlq handles
            # both, and the string is what survives a JSON decode failure.
            return ParseResult(raw_text=payload,
                               error=f"Structural validation failed: {validation_err}")
        return ParseResult(body=signal_payload, raw_text=payload)

    def should_skip(self, body: dict) -> Optional[str]:
        summary = body.get("packetExecutionSummary", {})
        if summary.get("packetStatus") != "REJECTED":
            return "non-rejected packet"

        from src.storage.factory import get_casebook_storage

        if get_casebook_storage().exists(self.identity_of(body), terminal_only=True):
            return "terminal casebook already exists"
        return None

    def identity_of(self, body: dict) -> Optional[str]:
        return body.get("eventId")

    def timeout_casebook(self, body: dict) -> dict:
        return {
            "packet_metadata": {"eid": self.identity_of(body)},
            "packet_status": {"status": "FAILED_TIMEOUT"},
            "resolution": {"synthesis": "Investigation exceeded maximum allowed time."},
        }

    def save_terminal(self, identity: str, casebook: dict) -> None:
        from src.storage.factory import get_casebook_storage

        get_casebook_storage().save_terminal(identity, casebook)


# ---------------------------------------------------------------------------
# Dead-letter topic
# ---------------------------------------------------------------------------

class DltAdapter:
    """Dead-lettered records. See DLT_PLAN.md 6.1.

    The important departure from `RejectionAdapter`: **an unparseable payload
    is not poison here.** A DLT message's evidence lives in its headers -- the
    stacktrace, the coordinates, the timestamps -- so a payload we cannot
    decode costs us at most the `refId`. The case still proceeds header-only
    and simply skips the log lane. Discarding it would throw away a complete
    stacktrace because a field we only read one identifier out of failed to
    parse.

    "At most", now, because the record key is read too. The 2026-08-20 sample
    is keyed on the refId, so an undecodable payload no longer costs the log
    lane at all -- the case keeps its correlation id and its logs, and only
    loses the payload summary.
    """

    role = "dlt"

    def parse(self, msg) -> ParseResult:
        from src.dlt.headers import decode_kafka_headers, parse_headers
        from src.dlt.identity import derive_case_id
        from src.dlt.payload import decode_key, resolve_ref_id
        from src.models.dlt_schemas import DltMessage

        raw_text = None
        payload = None
        if msg.value is not None:
            raw_text = msg.value.decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw_text)
            except (ValueError, TypeError):
                payload = None  # headers and the key still carry the evidence

        headers = decode_kafka_headers(msg.headers)
        parsed_headers = parse_headers(headers)
        record_key = decode_key(getattr(msg, "key", None))

        # Prefer the original record's coordinates: they are what make the id
        # idempotent across a redrive. Fall back to the DLT record's own, which
        # the consumer always has, so a message missing its DLT headers still
        # gets a stable, unique id instead of being dropped.
        case_id = derive_case_id(parsed_headers.original_topic,
                                 parsed_headers.original_partition,
                                 parsed_headers.original_offset)
        if case_id is None:
            case_id = derive_case_id(msg.topic, msg.partition, msg.offset)
        if case_id is None:
            return ParseResult(raw_text=raw_text or "",
                               error="Could not derive a case id from the record")

        # `__TypeId__` selects the payload path to try; the key is tried first
        # regardless, so an unregistered type still resolves (DLT_PLAN.md 5.3).
        extraction = resolve_ref_id(payload, key=record_key,
                                    type_id=parsed_headers.type_id)
        if extraction.mismatch:
            logger.warning(
                "Record key and payload disagree on the refId; using the key",
                case_id=case_id, record_key=record_key,
                payload_ref_id=extraction.payload_ref_id,
                type_id=parsed_headers.type_id)

        try:
            message = DltMessage(
                case_id=case_id,
                headers=headers,
                payload=payload,
                payload_raw=None if payload is not None else raw_text,
                record_key=record_key,
                ref_id=extraction.ref_id,
                ref_id_source=extraction.source,
                payload_ref_id=extraction.payload_ref_id,
                ref_id_mismatch=extraction.mismatch,
            )
        except Exception as validation_err:
            logger.error("DLT message failed validation", case_id=case_id,
                         error=str(validation_err))
            return ParseResult(raw_text=raw_text or "",
                               error=f"DLT validation failed: {validation_err}")

        return ParseResult(body=message.model_dump(), raw_text=raw_text)

    def should_skip(self, body: dict) -> Optional[str]:
        from src.dlt.case_storage import get_dlt_storage

        if get_dlt_storage().exists(self.identity_of(body), terminal_only=True):
            return "terminal DLT case already exists"
        return None

    def identity_of(self, body: dict) -> Optional[str]:
        return body.get("case_id")

    def timeout_casebook(self, body: dict) -> dict:
        return {
            "packet_metadata": {"eid": self.identity_of(body),
                                "ref_id": body.get("ref_id")},
            "packet_status": {"status": "FAILED_TIMEOUT"},
            "resolution": {"synthesis": "DLT analysis exceeded maximum allowed time."},
        }

    def save_terminal(self, identity: str, casebook: dict) -> None:
        from src.dlt.case_storage import get_dlt_storage

        get_dlt_storage().save_terminal(identity, casebook)


def for_role(role: str) -> MessageAdapter:
    """Adapter for a `CONSUMER_ROLE`. Unknown roles get the rejection adapter,
    matching the module's existing default-to-fast behaviour."""
    if role in ("dlt", "dlt_analysis"):
        return DltAdapter()
    return RejectionAdapter()
