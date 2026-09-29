"""The AUDIT envelope, translated into the packet event (`MessagePayload`).

From 2026-09-29 the producers publish an AUDIT event instead of the packet
event this system was built on: the rejections topic carries it, and so does
the payload of every dead-lettered record. The DLT record's headers and key
are unchanged. Everything downstream -- the routes, the graph, the service
registry, the log pipeline, the DLT lane -- reads the packet event, so the
envelope is translated once, where a record enters, instead of being taught
to every reader.

The packet's facts live under `edata`. What the translation decides:

* **Dead-lettered or rejected.** `executionStatus` ON_HOLD is a record the
  service dead-lettered: its packetStatus is ON_HOLD, which the rejection
  lane skips. Otherwise `validationStatus` "false" is a business rejection:
  packetStatus REJECTED. Anything else keeps its `executionStatus`
  (COMPLETED for a stage that passed), which the rejection lane skips as it
  always skipped a status other than REJECTED.
* **The eventId is the refId.** The envelope has no eventId, and `mid` is
  one per message. The packet event's eventId equalled its refId, so
  casebooks stay keyed, and findable, by the refId.
* **`eventTimestamp` comes from `ets`,** epoch milliseconds, written as
  ISO-8601 UTC. The packet event's was local time with no offset.
* **`sidDate` is read off the sid,** whose last 14 digits are
  yyyyMMddHHmmss in every sample; None when they are not a date.

Any other payload -- the packet event itself, a DLT payload of another type,
something that is not a dict -- is returned unchanged, so a record in the
older contract is handled exactly as before.
"""
from datetime import datetime, timezone
from typing import Any, Optional

#: `executionStatus` of a record the service dead-lettered.
EXECUTION_ON_HOLD = "ON_HOLD"

#: packetStatus given to a dead-lettered record.
STATUS_ON_HOLD = "ON_HOLD"

#: packetStatus given to a business rejection, the one the rejection lane
#: investigates.
STATUS_REJECTED = "REJECTED"


def is_audit_envelope(payload: Any) -> bool:
    """True for a payload in the AUDIT contract: an `edata` object, and no
    packet event alongside it."""
    return (isinstance(payload, dict)
            and isinstance(payload.get("edata"), dict)
            and "packetExecutionSummary" not in payload)


def to_message_payload(payload: Any) -> Any:
    """The packet event for an AUDIT envelope; any other payload unchanged.

    Never raises. A field that is missing or of the wrong type comes out as
    None, and `MessagePayload` decides whether the result is usable.
    """
    if not is_audit_envelope(payload):
        return payload

    edata = payload["edata"]
    execution = _text(edata.get("executionStatus"))
    validation = _flag(edata.get("validationStatus"))
    error_data = edata.get("errorData")
    version = _text(payload.get("ver"))

    return {
        "eventId": _text(edata.get("refId")),
        "category": _text(payload.get("messageType")),
        "eventType": _text(edata.get("stageOutcome")),
        "eventTimestamp": _iso_utc(payload.get("ets")),
        "eventVersion": version,
        "sid": _text(edata.get("sid")),
        "sidDate": _sid_date(edata.get("sid")),
        "version": version,
        "sourceTopic": _text(edata.get("publishedTopic")),
        "flowMetaData": {
            "stage": _text(edata.get("stage")),
            "subStage": _text(edata.get("subStage")),
        },
        "packetMetaData": {
            "refId": _text(edata.get("refId")),
            "enrolmentType": _text(edata.get("enrolmentType")),
            "pktSource": _text(edata.get("pktSource")),
            "isMBU": _flag(edata.get("isMBU")),
            "isNRI": _flag(edata.get("isNRI")),
            "isForeignResident": _flag(edata.get("isForeignResident")),
        },
        "packetExecutionSummary": {
            "packetStatus": _packet_status(execution, validation),
            "errorData": list(error_data) if isinstance(error_data, list) else None,
            "hasExecutionErrors": None if execution is None
            else execution.upper() == EXECUTION_ON_HOLD,
            "isExecutionSuccess": None if execution is None
            else execution.upper() == "COMPLETED",
            "hasValidationErrors": None if validation is None else not validation,
            "isValidationSuccess": validation,
        },
        "resubmissionSummary": {
            "resubmissionCount": edata.get("resubmissionCount"),
            "resubmissionReason": _text(edata.get("resubmissionReason")),
        },
    }


def _packet_status(execution: Optional[str], validation: Optional[bool]) -> Optional[str]:
    if execution and execution.upper() == EXECUTION_ON_HOLD:
        return STATUS_ON_HOLD
    if validation is False:
        return STATUS_REJECTED
    return execution


def _text(value) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _flag(value) -> Optional[bool]:
    """A boolean the producer may write as a bool or as "true"/"false"."""
    if isinstance(value, bool):
        return value
    text = _text(value)
    if text is None:
        return None
    return {"true": True, "false": False}.get(text.lower())


def _iso_utc(value) -> Optional[str]:
    """Epoch milliseconds, as a number or a string, in ISO-8601 UTC."""
    if isinstance(value, bool):
        return None
    try:
        millis = int(value) if isinstance(value, (int, float)) else int(str(value).strip())
        moment = datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _sid_date(value) -> Optional[str]:
    """The sid's trailing yyyyMMddHHmmss, as the packet event wrote sidDate."""
    text = _text(value)
    if text is None or len(text) < 14:
        return None
    try:
        moment = datetime.strptime(text[-14:], "%Y%m%d%H%M%S")
    except ValueError:
        return None
    return moment.strftime("%Y-%m-%d %H:%M:%S")
