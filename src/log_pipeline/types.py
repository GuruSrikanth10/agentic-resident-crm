"""
Canonical contracts shared by every log source (KUBERNETES_LOGS_PLAN.md 4.1).

Stages 2-4 of the reduction pipeline are source-agnostic: `branch_on_error`
reads `level`, `cluster_logs` reads `message` and `timestamp`, the guardrails
read all four required keys, and `pipeline._format_*` / `_save_raw_logs`
render `timestamp` / `app_name` / `level` / `message`.

Any source emitting `LogRecord` therefore works with Drain3 clustering, the
evidence guardrails, the S3 offload, and the casebook wiring unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, NotRequired, Optional, TypedDict

if TYPE_CHECKING:  # avoids an import cycle with catalog -> config -> logging
    from src.log_pipeline.catalog import TemplateCatalog


class LogRecord(TypedDict):
    """One log line, normalised across sources."""

    # Required -- Stages 2-4 depend on all four.
    timestamp: str      # RFC3339 where available; the ordering key
    level: str          # ERROR | WARN | INFO | DEBUG | TRACE
    message: str
    app_name: str

    # Optional provenance, populated by the Kubernetes source from Phase 3.
    pod_name: NotRequired[str]
    container: NotRequired[str]
    source: NotRequired[str]              # "elastic" | "kubernetes"
    container_instance: NotRequired[str]  # "current" | "previous"


#: Keys every source must populate. Used by tests to enforce the contract.
REQUIRED_RECORD_KEYS: tuple[str, ...] = ("timestamp", "level", "message", "app_name")


class GapType(str, Enum):
    """Ways a fetch can return less than was asked for (plan 5.8).

    Announcing these is what separates "we looked and found nothing" from
    "we could not look properly" -- design principle 2.
    """

    LOG_ROTATION = "LOG_ROTATION"
    POD_REPLACED = "POD_REPLACED"
    TRUNCATED = "TRUNCATED"
    POD_VANISHED = "POD_VANISHED"
    LEVEL_PARSE_DEGRADED = "LEVEL_PARSE_DEGRADED"
    SOURCE_FALLBACK = "SOURCE_FALLBACK"

    #: One of several configured services could not be searched at all -- its
    #: namespace was unreadable, or its pod list was denied. The other
    #: services still returned logs, so the fetch is degraded rather than
    #: failed, and this names exactly which part of the packet's journey is
    #: missing. Without it a multi-service fetch would look complete while
    #: silently omitting a whole hop.
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"


@dataclass(frozen=True)
class EvidenceGap:
    """A specific, named incompleteness in a fetch result."""

    gap_type: GapType
    detail: str
    context: dict = field(default_factory=dict)

    def describe(self) -> str:
        """One banner line. Full banner rendering arrives in Phase 4."""
        return f"{self.gap_type.value}: {self.detail}"


@dataclass
class FetchDiagnostics:
    """Per-fetch counters. These are the metrics -- there is no metrics
    backend in this project, so they must be machine-aggregatable (plan 6.4).

    Pod and redaction fields stay zero for sources to which they do not apply.
    """

    source: str
    records_returned: int = 0
    bytes_read: int = 0
    latency_ms: float = 0.0
    pods_queried: int = 0
    pods_failed: int = 0
    redaction_counts: dict = field(default_factory=dict)


@dataclass
class FetchResult:
    """What a source returns.

    `ok` distinguishes could-not-look (False) from looked-and-found-nothing
    (True with empty `records`). Collapsing the two would let the agent
    conclude "no errors occurred" when the truth is "we could not read the
    logs" -- design principle 3.
    """

    records: list[LogRecord]
    diagnostics: FetchDiagnostics
    gaps: list[EvidenceGap] = field(default_factory=list)
    ok: bool = True

    @property
    def is_empty(self) -> bool:
        return not self.records

    @classmethod
    def failure(cls, source: str, detail: str,
                gaps: Optional[list[EvidenceGap]] = None) -> "FetchResult":
        """A could-not-look result. Used by sources that handle their own
        failures; the Elasticsearch adapter deliberately does not (see
        `sources/elastic.py`)."""
        return cls(
            records=[],
            diagnostics=FetchDiagnostics(source=source),
            gaps=list(gaps or []),
            ok=False,
        )


@dataclass(frozen=True)
class TimeWindow:
    """How far back to look, and optionally how recent to stop.

    `hours` is the look-back, which is what the kubelet API accepts
    (`since_seconds`). `until` is an upper bound the API cannot express, so it
    is applied client-side after the read.

    The DLT lane is the caller that needs one: its window is anchored on when
    the last attempt actually failed, and lines from hours of unrelated later
    activity are noise in the evidence handed to the model. `LogWindow`
    described this bound and computed `end_ms` for it, but nothing ever
    applied it -- the trailing half of the contract was documentation only.
    """

    hours: float
    #: Upper bound on record timestamps. None means "up to now", which is
    #: every existing caller and exactly today's behaviour.
    until: Optional[datetime] = None

    @property
    def seconds(self) -> int:
        return int(self.hours * 3600)

    def start_time(self, now: Optional[datetime] = None) -> datetime:
        return (now or datetime.now(timezone.utc)) - timedelta(hours=self.hours)

    def excludes(self, timestamp: Optional[str]) -> bool:
        """Is this record newer than the window's upper bound?

        Deliberately conservative: an absent or unparseable timestamp is NEVER
        excluded. Dropping a line because we could not read its clock would be
        discarding evidence on the strength of a parse failure, which is the
        opposite of what this pipeline is for.
        """
        if self.until is None or not timestamp:
            return False
        try:
            parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        except ValueError:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed > self.until

    @classmethod
    def default(cls) -> "TimeWindow":
        """Two hours, matching `K8S_DEFAULT_SINCE_HOURS` from Phase 3."""
        return cls(hours=2.0)


@dataclass(frozen=True)
class FetchContext:
    """Everything a source needs beyond the identifier and the window."""

    event_id: str
    namespace: Optional[str] = None
    app: Optional[str] = None
    catalog: Optional["TemplateCatalog"] = None
    #: Additional identifiers to match on (e.g. refId alongside eventId).
    #: We do not yet know which one the services actually log, so a caller
    #: that has both should supply both.
    extra_identifiers: tuple = ()
