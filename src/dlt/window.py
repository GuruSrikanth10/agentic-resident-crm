"""Phase 5 of DLT_PLAN.md -- deriving the log window from DLT headers.

Trap 2 (DLT_PLAN.md 3.2) lives here. In the reference sample the original
message was produced at `2026-08-16T07:20:05Z` and the final attempt failed at
`2026-08-18T02:20:08Z` -- **43 hours apart**. Anchoring on
`kafka_original-timestamp` searches a window that is 43 hours stale, and with
the `K8S_DEFAULT_SINCE_HOURS=2` default it finds nothing at all, with no error
anywhere to say why.

The anchor is therefore `retry_topic-backoff-timestamp`: when the last attempt
actually ran. That attempt is recent, and because Spring already retried the
message several times, the last attempt reproduces the same failure -- which is
why this system never needs to replay a packet to generate evidence.

Pure functions, no I/O.
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
    #: True when `retry_topic-backoff-timestamp` was missing and the anchor
    #: fell back to the original produce time. A degradation, not an
    #: equivalence -- see the module docstring.
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
        return TimeWindow(
            hours=max(0.0, (now - self.start_ms) / 3_600_000),
            until=datetime.fromtimestamp(self.end_ms / 1000, tz=timezone.utc),
        )

    def describe(self) -> str:
        return (f"{self.start_iso} .. {self.end_iso} "
                f"(anchored on {self.anchor_iso}, age {self.age_seconds / 3600:.1f}h)")


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def derive_window(headers: DltHeaders,
                  now_ms: Optional[int] = None) -> Optional[LogWindow]:
    """Build the log window for a dead-lettered record.

    Returns None when the headers carry no usable timestamp at all -- the
    caller then skips the log lane and records the case header-only.
    """
    anchor = headers.last_attempt_ms
    if anchor is None:
        return None

    now = now_ms if now_ms is not None else _now_ms()
    start = anchor - lead_seconds() * 1000
    end = anchor + trail_seconds() * 1000
    age = max(0.0, (now - start) / 1000.0)

    return LogWindow(
        anchor_ms=anchor,
        start_ms=start,
        end_ms=end,
        too_old=age > max_age_seconds(),
        anchor_is_fallback=headers.anchor_is_fallback,
        age_seconds=age,
    )
