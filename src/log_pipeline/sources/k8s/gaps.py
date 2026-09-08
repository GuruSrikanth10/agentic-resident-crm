"""
Evidence gap detection and banner rendering (KUBERNETES_LOGS_PLAN.md 5.8).

This is the mechanism that makes the Kubernetes source trustworthy. Kubelet
retention is short and pods are replaced, so a fetch routinely returns less
than was asked for. Announcing that explicitly is the difference between a
system that degrades safely and one that degrades silently:

    "we looked and found no errors"      -- a conclusion the agent may draw
    "we could not see most of the window" -- a conclusion it must not

The banner is prepended to the text handed to the LLM, and the Investigator
and Reviewer prompts (Phase 10) are told to qualify findings when it appears.
"""
import os
from datetime import datetime, timezone
from typing import Optional

from src.log_pipeline.sources.k8s.parser import ParseStats
from src.log_pipeline.types import EvidenceGap, GapType, TimeWindow
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BANNER_HEADER = "--- EVIDENCE GAPS (the trace below is INCOMPLETE) ---"
BANNER_FOOTER = "--- END EVIDENCE GAPS ---"


def _level_parse_threshold() -> float:
    try:
        return float(os.environ.get("K8S_LEVEL_PARSE_WARN_THRESHOLD", "0.9"))
    except ValueError:
        return 0.9


def _as_aware(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _parse_record_timestamp(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return _as_aware(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    except ValueError:
        return None


def detect_rotation_gap(oldest_observed: Optional[str], window: TimeWindow,
                        earliest_pod_start: Optional[datetime] = None,
                        now: Optional[datetime] = None) -> Optional[EvidenceGap]:
    """Detect logs rotated off the node before we could read them.

    `oldest_observed` MUST be the oldest line seen in the raw stream, before
    identifier filtering. Using the oldest *matching* line instead would fire
    this gap on nearly every fetch -- a matching line is naturally recent --
    and a banner that cries wolf every time trains the agents to ignore it,
    destroying the entire mechanism.

    Two conditions must both hold:
      * the stream starts later than the window we asked for, and
      * the pod was already running before that point.

    The second matters: if the pod itself only started inside the window, its
    logs were not rotated -- there was simply nothing earlier to read, and
    POD_REPLACED already describes that situation correctly.
    """
    if not oldest_observed:
        return None

    now = _as_aware(now) or datetime.now(timezone.utc)
    oldest = _parse_record_timestamp(oldest_observed)
    if oldest is None:
        return None

    requested_start = window.start_time(now)
    slack = _rotation_slack()

    # A minute of slack absorbs clock skew and the delay between the request
    # being issued and the first line being written.
    if oldest <= requested_start + slack:
        return None

    pod_start = _as_aware(earliest_pod_start)
    if pod_start is not None and pod_start >= oldest - slack:
        # The pod is younger than the missing span, so nothing was rotated.
        return None

    return EvidenceGap(
        GapType.LOG_ROTATION,
        f"requested from {requested_start.isoformat()}, oldest available line is "
        f"{oldest.isoformat()}. Logs before {oldest.isoformat()} were rotated off "
        f"the node and are unrecoverable.",
        {"requested_start": requested_start.isoformat(), "oldest_line": oldest.isoformat()},
    )


def _rotation_slack():
    from datetime import timedelta
    try:
        seconds = int(os.environ.get("K8S_ROTATION_SLACK_SECONDS", "60"))
    except ValueError:
        seconds = 60
    return timedelta(seconds=seconds)


def detect_pod_replaced_gaps(targets: list, window: TimeWindow,
                             now: Optional[datetime] = None) -> list:
    """Detect windows partly served by a pod that no longer exists.

    If a matching pod started *after* the window began, some earlier pod
    served the first part of that window. Its logs are gone from the API
    permanently -- nothing can recover them, so the only correct response is
    to say so.
    """
    now = _as_aware(now) or datetime.now(timezone.utc)
    requested_start = window.start_time(now)

    gaps, seen = [], set()
    for target in targets:
        started = _as_aware(getattr(target, "start_time", None))
        if started is None or started <= requested_start:
            continue
        if target.pod_name in seen:
            continue
        seen.add(target.pod_name)
        gaps.append(EvidenceGap(
            GapType.POD_REPLACED,
            f"pod {target.pod_name} started {started.isoformat()}, after the "
            f"requested window began at {requested_start.isoformat()}. An earlier "
            f"pod served part of this window; its logs are gone.",
            {"pod_name": target.pod_name, "started": started.isoformat()},
        ))
    return gaps


def detect_parse_degradation_gap(stats: ParseStats) -> Optional[EvidenceGap]:
    """Detect a log format the parser does not understand.

    Matters because `branch_on_error` keys off `level == "ERROR"`. A stream we
    cannot extract levels from means ERROR branching is unreliable for this
    fetch, and the agent should know that rather than trusting an all-INFO
    trace.
    """
    if stats.total == 0:
        return None

    threshold = _level_parse_threshold()
    rate = stats.level_failure_rate
    if rate < threshold:
        return None

    return EvidenceGap(
        GapType.LEVEL_PARSE_DEGRADED,
        f"{rate:.0%} of {stats.total} lines had no recognisable log level. "
        f"ERROR detection is unreliable for this fetch; the absence of ERROR "
        f"lines below must not be read as an absence of errors.",
        {"failure_rate": round(rate, 4), "total_lines": stats.total},
    )


def render_banner(gaps: list) -> str:
    """Render gaps as the banner prepended to the LLM-facing trace."""
    if not gaps:
        return ""

    lines = [BANNER_HEADER]
    for gap in gaps:
        lines.append(gap.describe())
    lines.append(BANNER_FOOTER)
    return "\n".join(lines)


def log_gaps(gaps: list, event_id: Optional[str] = None):
    """Emit each gap as a structured warning so gap rates are aggregatable."""
    log = logger.bind(event_id=event_id) if event_id else logger
    for gap in gaps:
        log.warning(
            "Kubernetes evidence gap",
            gap_type=gap.gap_type.value,
            detail=gap.detail,
            **gap.context,
        )


def dedupe_gaps(gaps: list) -> list:
    """Collapse duplicates while preserving order.

    Twenty pods hitting the same byte cap should produce one TRUNCATED line in
    the banner, not twenty.
    """
    seen, unique = set(), []
    for gap in gaps:
        key = (gap.gap_type, gap.detail)
        if key in seen:
            continue
        seen.add(key)
        unique.append(gap)
    return unique
