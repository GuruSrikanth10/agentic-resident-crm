"""Phase 5 of DLT_PLAN.md -- deriving the log window from DLT headers.

Three timestamps are available on a Spring DLT message, and the choice of
anchor matters:

* `kafka_original-timestamp` -- when the original message was produced. The
  furthest in the past; the failure may have happened hours or days later
  after multiple retries.

* `retry_topic-original-timestamp` -- when Spring's retry mechanism first
  received the message. Always in the past, and close to when the failures
  started occurring.

* `retry_topic-backoff-timestamp` -- Spring's scheduled next retry time. In
  the reference sample this was in the past (the retry had already fired and
  failed), making it the most accurate "when the last attempt ran". But when
  a message is dead-lettered before the scheduled retry fires, this
  timestamp is in the future -- and anchoring a log window on a future time
  searches the wrong window entirely.

The anchor is therefore selected at derivation time: the backoff timestamp
when it is in the past (the normal, most-accurate case), falling back to the
retry-original timestamp when the backoff is in the future or absent.

Pure functions, no I/O (other than reading the clock for `now`).
"""
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.dlt.headers import DltHeaders
from src.log_pipeline.types import TimeWindow

DEFAULT_LEAD_SECONDS = 300
DEFAULT_TRAIL_SECONDS = 120

#: Beyond this age the fetch is skipped outright rather than issued. Pod logs
#: have rotated, and a fetch certain to return nothing still costs a full
#: Kubernetes fan-out across every pod in the namespace.
DEFAULT_MAX_AGE_SECONDS = 86400


def lead_seconds() -> int:
    return _int_env("DLT_LOG_LEAD_SECONDS", DEFAULT_LEAD_SECONDS)


def trail_seconds() -> int:
    return _int_env("DLT_LOG_TRAIL_SECONDS", DEFAULT_TRAIL_SECONDS)


def max_age_seconds() -> int:
    return _int_env("DLT_MAX_LOG_AGE_SECONDS", DEFAULT_MAX_AGE_SECONDS)


def _int_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default))))
    except (ValueError, TypeError):
        return default


@dataclass(frozen=True)
class LogWindow:
    """The span of pod logs worth reading for one dead-lettered record."""

    anchor_ms: int
    start_ms: int
    end_ms: int
    #: True when the window is older than `DLT_MAX_LOG_AGE_SECONDS`; the
    #: caller skips the fetch and records a LOGS_TOO_OLD gap.
    too_old: bool
    #: True when the anchor fell back from the backoff timestamp to the
    #: retry-original or original-produce timestamp. A degradation, not an
    #: equivalence -- the fallback is further from the actual failure time.
    anchor_is_fallback: bool
    age_seconds: float

    @property
    def anchor_iso(self) -> str:
        return datetime.fromtimestamp(self.anchor_ms / 1000, tz=timezone.utc).isoformat()

    @property
    def start_iso(self) -> str:
        return datetime.fromtimestamp(self.start_ms / 1000, tz=timezone.utc).isoformat()

    @property
    def end_iso(self) -> str:
        return datetime.fromtimestamp(self.end_ms / 1000, tz=timezone.utc).isoformat()

    def to_time_window(self, now_ms: Optional[int] = None) -> TimeWindow:
        """As a look-back for the Kubernetes source, plus the trailing bound.

        `TimeWindow.hours` is relative to *now* (it becomes `since_seconds`,
        the only thing the kubelet API accepts), so the absolute start is
        converted here. The trailing bound is not expressible in that shape,
        so it travels as `until` and is applied client-side.

        It used to travel nowhere at all: this returned only the look-back and
        a comment said the trailing bound was "applied during filtering
        instead", which nothing did. A case anchored 20 hours ago therefore
        kept every line from those 20 hours rather than the few minutes around
        the failure.
        """
        now = now_ms if now_ms is not None else _now_ms()
        lookback_ms = max(now - self.start_ms, (lead_seconds() + trail_seconds()) * 1000)
        return TimeWindow(
            hours=lookback_ms / 3_600_000,
            until=datetime.fromtimestamp(self.end_ms / 1000, tz=timezone.utc),
        )

    def describe(self) -> str:
        return (f"{self.start_iso} .. {self.end_iso} "
                f"(anchored on {self.anchor_iso}, age {self.age_seconds / 3600:.1f}h)")


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _select_anchor(headers: DltHeaders, now_ms: int) -> tuple:
    """Choose the best past timestamp to anchor the log window on.

    Returns (anchor_ms, is_fallback). Preference order:
      1. backoff_timestamp_ms, when it is in the past -- the most recent
         attempt, and the most accurate "when the failure happened".
      2. retry_original_timestamp_ms -- when the retry flow started. Always
         in the past, but may be hours before the final attempt.
      3. original_timestamp_ms -- when the original message was produced.
         The furthest from the failure; a last resort.

    A future backoff timestamp means the message was dead-lettered before the
    scheduled retry fired, so the backoff time does not correspond to any
    actual processing. The retry-original timestamp is the next best thing.
    """
    if headers.backoff_timestamp_ms is not None and headers.backoff_timestamp_ms <= now_ms:
        return headers.backoff_timestamp_ms, False

    if headers.retry_original_timestamp_ms is not None:
        return headers.retry_original_timestamp_ms, True

    if headers.original_timestamp_ms is not None:
        return headers.original_timestamp_ms, True

    return None, True


def derive_window(headers: DltHeaders,
                  now_ms: Optional[int] = None) -> Optional[LogWindow]:
    """Build the log window for a dead-lettered record.

    Returns None when the headers carry no usable timestamp at all -- the
    caller then skips the log lane and records the case header-only.
    """
    now = now_ms if now_ms is not None else _now_ms()
    anchor, is_fallback = _select_anchor(headers, now)
    if anchor is None:
        return None

    start = anchor - lead_seconds() * 1000
    end = anchor + trail_seconds() * 1000
    age = max(0.0, (now - start) / 1000.0)

    return LogWindow(
        anchor_ms=anchor,
        start_ms=start,
        end_ms=end,
        too_old=age > max_age_seconds(),
        anchor_is_fallback=is_fallback,
        age_seconds=age,
    )
