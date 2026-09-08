"""Phase 1 of DLT_PLAN.md -- DLT header extraction.

Spring Kafka's `DeadLetterPublishingRecoverer` and `@RetryableTopic` write a
fixed set of headers onto every dead-lettered record. This module turns that
raw string map into a typed object, and nothing more: no classification, no
judgement, no I/O.

Two things here are load-bearing and easy to get wrong (DLT_PLAN.md 3.2):

* `kafka_exception-fqcn` and `kafka_exception-cause-fqcn` are Spring/JDK
  *wrappers*. In the reference sample they are `ListenerExecutionFailedException`
  and `java.lang.RuntimeException` -- values that would be identical for every
  failure in every Spring Kafka consumer in the organisation. They are carried
  here for audit only. The real root comes from parsing the stacktrace text.

* `retry_topic-original-timestamp` and `retry_topic-backoff-timestamp` are
  hex-encoded epoch milliseconds, not decimal. `last_attempt_ms` is the one to
  anchor a log window on -- in the reference sample it is 43 hours later than
  `kafka_original-timestamp`.
"""
from dataclasses import dataclass, field
from typing import Optional

#: Plausibility window for an epoch-millisecond value: 2000-01-01 to 2100-01-01.
#: Used to disambiguate a decimal encoding from a hex one, since a 13-digit
#: decimal string is also syntactically valid hex.
_EPOCH_MS_MIN = 946684800000
_EPOCH_MS_MAX = 4102444800000

H_ORIGINAL_TOPIC = "kafka_original-topic"
H_ORIGINAL_PARTITION = "kafka_original-partition"
H_ORIGINAL_OFFSET = "kafka_original-offset"
H_ORIGINAL_TIMESTAMP = "kafka_original-timestamp"
H_CONSUMER_GROUP = "kafka_dlt-original-consumer-group"
H_EXCEPTION_FQCN = "kafka_exception-fqcn"
H_EXCEPTION_CAUSE_FQCN = "kafka_exception-cause-fqcn"
H_EXCEPTION_MESSAGE = "kafka_exception-message"
H_STACKTRACE = "kafka_exception-stacktrace"
H_ATTEMPTS = "retry_topic-attempts"
H_RETRY_ORIGINAL_TIMESTAMP = "retry_topic-original-timestamp"
H_BACKOFF_TIMESTAMP = "retry_topic-backoff-timestamp"
H_TYPE_ID = "__TypeId__"


def decode_kafka_headers(raw_headers) -> dict:
    """kafka-python hands back `[(str, bytes)]`; make it a plain str->str dict.

    A duplicate key keeps the last value, matching Spring's own consumer-side
    accessors. A non-UTF-8 value is replaced rather than raising: losing one
    header must never cost us the whole message.
    """
    out = {}
    for key, value in (raw_headers or []):
        name = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
        if value is None:
            out[name] = None
        elif isinstance(value, bytes):
            out[name] = value.decode("utf-8", errors="replace")
        else:
            out[name] = str(value)
    return out


def decode_epoch_ms(value) -> Optional[int]:
    """Decode an epoch-millisecond header written in decimal *or* hex.

    Spring's `RetryTopicHeaders` values arrive hex-encoded
    (`01A009712548`), while `kafka_original-timestamp` is plain decimal. A
    13-digit decimal string parses as hex too, so both readings are tried and
    the one landing inside a plausible calendar range wins, decimal first.

    Returns None for anything that cannot be read as a plausible timestamp,
    rather than raising -- a malformed timestamp must not cost us the message.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    candidates = []
    if text.isdigit():
        try:
            candidates.append(int(text, 10))
        except ValueError:
            pass
    try:
        candidates.append(int(text, 16))
    except ValueError:
        pass

    for candidate in candidates:
        if _EPOCH_MS_MIN <= candidate <= _EPOCH_MS_MAX:
            return candidate
    return None


def _as_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None


def _as_str(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True)
class DltHeaders:
    """The Spring DLT header contract, typed. See DLT_PLAN.md 3.1."""

    original_topic: Optional[str] = None
    original_partition: Optional[int] = None
    original_offset: Optional[int] = None
    original_timestamp_ms: Optional[int] = None
    consumer_group: Optional[str] = None

    #: Wrapper FQCNs. Audit only -- never fingerprint on these (3.2, Trap 1).
    exception_fqcn: Optional[str] = None
    exception_cause_fqcn: Optional[str] = None

    exception_message: Optional[str] = None
    stacktrace: Optional[str] = None

    attempts: Optional[int] = None
    retry_original_timestamp_ms: Optional[int] = None
    backoff_timestamp_ms: Optional[int] = None

    type_id: Optional[str] = None

    #: Everything as received, so an unrecognised header is never lost.
    raw: dict = field(default_factory=dict)

    @property
    def last_attempt_ms(self) -> Optional[int]:
        """When the final attempt actually failed -- the log-window anchor.

        Falls back to the original produce time when the backoff header is
        absent or unparseable. That fallback is a degradation, not an
        equivalence: in the reference sample the two are 43 hours apart
        (DLT_PLAN.md 3.2, Trap 2).
        """
        return self.backoff_timestamp_ms or self.original_timestamp_ms

    @property
    def anchor_is_fallback(self) -> bool:
        """True when `last_attempt_ms` had to fall back to the produce time."""
        return self.backoff_timestamp_ms is None


def parse_headers(raw: Optional[dict]) -> DltHeaders:
    """Build a `DltHeaders` from a decoded str->str header map.

    Every field is optional. A DLT message missing half its headers still
    yields a usable object -- the downstream phases decide what they can do
    with what survived, rather than this raising and losing the message.
    """
    raw = dict(raw or {})
    return DltHeaders(
        original_topic=_as_str(raw.get(H_ORIGINAL_TOPIC)),
        original_partition=_as_int(raw.get(H_ORIGINAL_PARTITION)),
        original_offset=_as_int(raw.get(H_ORIGINAL_OFFSET)),
        original_timestamp_ms=decode_epoch_ms(raw.get(H_ORIGINAL_TIMESTAMP)),
        consumer_group=_as_str(raw.get(H_CONSUMER_GROUP)),
        exception_fqcn=_as_str(raw.get(H_EXCEPTION_FQCN)),
        exception_cause_fqcn=_as_str(raw.get(H_EXCEPTION_CAUSE_FQCN)),
        exception_message=_as_str(raw.get(H_EXCEPTION_MESSAGE)),
        stacktrace=_as_str(raw.get(H_STACKTRACE)),
        attempts=_as_int(raw.get(H_ATTEMPTS)),
        retry_original_timestamp_ms=decode_epoch_ms(raw.get(H_RETRY_ORIGINAL_TIMESTAMP)),
        backoff_timestamp_ms=decode_epoch_ms(raw.get(H_BACKOFF_TIMESTAMP)),
        type_id=_as_str(raw.get(H_TYPE_ID)),
        raw=raw,
    )
