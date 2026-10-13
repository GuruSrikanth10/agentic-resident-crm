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

A rejected packet's window is anchored on its rejection (`TimeWindow.anchor`)
and judged differently: see `detect_anchored_gaps` and `coverage_note`.
"""
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from src.log_pipeline.sources.k8s.parser import ParseStats
from src.log_pipeline.types import EvidenceGap, GapType, TimeWindow
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

BANNER_HEADER = "--- EVIDENCE GAPS (the trace below is INCOMPLETE) ---"
BANNER_FOOTER = "--- END EVIDENCE GAPS ---"

#: Opens the coverage line (`coverage_note`). The Investigator and Reviewer
#: prompts name it, so the two must not drift -- tests/test_prompt_gap_guidance.py
#: holds them together, as it does for the banner.
COVERAGE_PREFIX = "LOG COVERAGE:"

#: Gaps that leave the span a trace must cover itself incomplete. Any one of
#: them makes the coverage line's claim false, so the line is not said.
_SPAN_GAPS = frozenset({
    GapType.LOG_ROTATION, GapType.POD_REPLACED, GapType.TRUNCATED,
    GapType.POD_VANISHED, GapType.SERVICE_UNAVAILABLE,
})

#: Pods named in one anchored LOG_ROTATION gap before the rest are counted.
_MAX_NAMED_PODS = 5


@dataclass(frozen=True)
class PodSpan:
    """How far back one container's retained logs reach.

    `oldest_line` is the first line the kubelet still held, observed before
    identifier filtering, for the reason `detect_rotation_gap` gives; None
    when the container wrote nothing in the window or could not be read.
    `start_time` is named as `PodTarget`'s is, so `detect_pod_replaced_gaps`
    reads either.
    """

    pod_name: str
    container: str = ""
    oldest_line: Optional[str] = None
    start_time: Optional[datetime] = None

    @property
    def label(self) -> str:
        return f"{self.pod_name}/{self.container}" if self.container else self.pod_name

    def to_dict(self) -> dict:
        return {
            "pod_name": self.pod_name,
            "container": self.container,
            "oldest_line": self.oldest_line,
            "start_time": self.start_time.isoformat() if self.start_time else None,
        }

    @classmethod
    def from_dict(cls, data) -> Optional["PodSpan"]:
        """None for an entry that is not one, rather than an exception: a
        snapshot is an optimisation, and a damaged one must not fail a packet."""
        if not isinstance(data, dict) or not data.get("pod_name"):
            return None
        return cls(
            pod_name=str(data["pod_name"]),
            container=str(data.get("container") or ""),
            oldest_line=data.get("oldest_line") or None,
            start_time=_parse_record_timestamp(data.get("start_time") or ""),
        )


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

    On a window anchored on a rejection the span that counts begins at
    `cover_from`: a pod that started before then replaced nothing the
    substage that rejected the packet could have run on. `targets` are
    `PodTarget`s, or `PodSpan`s when a snapshot is re-judged.
    """
    now = _as_aware(now) or datetime.now(timezone.utc)
    requested_start = window.coverage_start(now)

    gaps, seen = [], set()
    for target in targets:
        started = _as_aware(getattr(target, "start_time", None))
        if started is None or started <= requested_start:
            continue
        if target.pod_name in seen:
            continue
        seen.add(target.pod_name)
        if window.anchor is not None:
            detail = (
                f"pod {target.pod_name} started {_seconds(started)}, after "
                f"{_seconds(requested_start, started)}, "
                f"{_before_rejection(window, started)}. A pod it replaced may "
                f"have run the substage that rejected this packet; its logs "
                f"are gone."
            )
        else:
            detail = (
                f"pod {target.pod_name} started {started.isoformat()}, after the "
                f"requested window began at {requested_start.isoformat()}. An earlier "
                f"pod served part of this window; its logs are gone."
            )
        gaps.append(EvidenceGap(
            GapType.POD_REPLACED,
            detail,
            {"pod_name": target.pod_name, "started": started.isoformat()},
        ))
    return gaps


def detect_anchored_gaps(spans: list, window: TimeWindow,
                         now: Optional[datetime] = None) -> list:
    """LOG_ROTATION and POD_REPLACED for a window anchored on a rejection:
    judged against `window.cover_from`, pod by pod.

    `detect_rotation_gap` compares the oldest line of ALL pods with the
    look-back, which is wrong twice over for a rejection. The look-back is
    how far back to read -- K8S_DEFAULT_SINCE_HOURS -- while the kubelet API
    serves only each container's current log file: 15-32 minutes on the
    enu-biometric pods. So every rejection reported rotation, every trace was
    called INCOMPLETE and every casebook was capped at 0.6, although the
    packet was fetched a minute after it was rejected with its rejection
    fully in view. And the oldest line of all pods is the quietest pod's,
    which says nothing about a busy pod that may have handled the packet.

    Here every pod must reach `cover_from`, the rejection less
    REJECTION_LOG_LEAD_SECONDS. A pod that does not has lost lines of the
    substage that rejected the packet, if it ran it: a real gap, announced
    as one. Lines from before `cover_from` belong to earlier substages, and
    `coverage_note` reports their absence without calling it a gap.
    """
    gaps = []
    rotation = _anchored_rotation_gap(spans, window, now)
    if rotation:
        gaps.append(rotation)
    gaps.extend(detect_pod_replaced_gaps(spans, window, now=now))
    return gaps


def _anchored_rotation_gap(spans: list, window: TimeWindow,
                           now: Optional[datetime] = None) -> Optional[EvidenceGap]:
    cover_from = window.coverage_start(now)
    slack = _rotation_slack()

    late = []
    for span in spans:
        oldest = _parse_record_timestamp(span.oldest_line or "")
        if oldest is None or oldest <= cover_from + slack:
            continue
        started = _as_aware(span.start_time)
        if started is not None and started >= oldest - slack:
            # Younger than the missing span: nothing was rotated, and
            # POD_REPLACED reports the pod it replaced.
            continue
        late.append((oldest, span))
    if not late:
        return None

    late.sort(key=lambda item: item[0], reverse=True)
    like = late[0][0]
    named = ", ".join(f"{span.label} (from {_seconds(oldest)})"
                      for oldest, span in late[:_MAX_NAMED_PODS])
    if len(late) > _MAX_NAMED_PODS:
        named += f" and {len(late) - _MAX_NAMED_PODS} more"
    return EvidenceGap(
        GapType.LOG_ROTATION,
        f"the logs of {named} begin after {_seconds(cover_from, like)}, "
        f"{_before_rejection(window, like)}. Lines from before then were "
        f"rotated off the node and are unrecoverable, so the substage that "
        f"rejected this packet may be missing from those pods.",
        {
            "cover_from": cover_from.isoformat(),
            "rejected_at": window.anchor.isoformat() if window.anchor else None,
            "pods": [{"pod_name": span.pod_name, "container": span.container,
                      "oldest_line": oldest.isoformat()} for oldest, span in late],
        },
    )


def coverage_note(spans: list, window: TimeWindow,
                  gaps: Iterable = ()) -> Optional[str]:
    """The LOG COVERAGE line: from when a rejected packet's trace is complete.

    Said only on a window anchored on a rejection, and only when no gap says
    the span from `cover_from` to the rejection is itself incomplete -- the
    banner tells that story then, and this line's claim would be false.

    Complete from the LATEST of the pods' oldest lines: before it, at least
    one pod no longer holds its lines; from it on, every pod does. That the
    trace starts there is expected -- a packet's earlier substages can run
    minutes or hours before the one that rejects it, and the kubelet keeps
    minutes -- so it is said as coverage rather than as a gap. Saying it at
    all is what stops an earlier line the pods no longer hold from being read
    as a step that never ran.
    """
    if window.anchor is None:
        return None
    if any(gap.gap_type in _SPAN_GAPS for gap in gaps):
        return None
    reached = [moment for moment in
               (_parse_record_timestamp(span.oldest_line or "") for span in spans)
               if moment is not None]
    if not reached:
        return None
    complete_from = max(reached)
    return (
        f"{COVERAGE_PREFIX} this packet's logs are complete from "
        f"{_seconds(complete_from)} through its rejection at "
        f"{_seconds(window.anchor, complete_from)}. Lines it logged before "
        f"{_seconds(complete_from)} may be missing: the pods no longer hold "
        f"them all."
    )


def rejudge_for_anchor(cached: list, spans: Optional[list], window: TimeWindow,
                       now: Optional[datetime] = None) -> tuple:
    """A reused snapshot's gaps and notes, judged against `window`'s anchor.
    Returns (gaps, notes).

    A snapshot is reused when a fetch re-enters later -- a retry, a replay, a
    checkpoint resume -- and its gaps were judged when it was captured. Given
    the pods' spans, which every snapshot has kept since, rotation and pod
    replacement are judged afresh, exactly as a live fetch judges them. An
    older snapshot kept only what its gaps recorded: the oldest line of all
    pods on its LOG_ROTATION, and each replaced pod's start. Those are
    re-judged from what they recorded, the aggregate's blind spot -- the
    quietest pod answering for every pod -- included, as nothing else was kept.
    """
    if spans is None:
        spans = _spans_recorded_by(cached)
    kept = [gap for gap in cached
            if gap.gap_type not in (GapType.LOG_ROTATION, GapType.POD_REPLACED)]
    gaps = kept + detect_anchored_gaps(spans, window, now=now)
    note = coverage_note(spans, window, gaps)
    return gaps, [note] if note else []


def _spans_recorded_by(cached: list) -> list:
    """What a pre-span snapshot's gaps recorded, as spans."""
    spans = []
    for gap in cached:
        context = gap.context or {}
        if gap.gap_type == GapType.LOG_ROTATION and context.get("oldest_line"):
            spans.append(PodSpan(pod_name="all pods", oldest_line=context["oldest_line"]))
        elif gap.gap_type == GapType.POD_REPLACED and context.get("pod_name"):
            spans.append(PodSpan(
                pod_name=str(context["pod_name"]),
                start_time=_parse_record_timestamp(context.get("started") or ""),
            ))
    return spans


def _seconds(moment: datetime, like: Optional[datetime] = None) -> str:
    """`moment` to the second, in `like`'s zone when given. Kubelet stamps
    carry the node's offset, so every time in one message reads in it."""
    if like is not None and like.tzinfo is not None:
        moment = moment.astimezone(like.tzinfo)
    return moment.isoformat(timespec="seconds")


def _before_rejection(window: TimeWindow, like: Optional[datetime] = None) -> str:
    """'5 minutes before this packet was rejected at <time>'."""
    if window.anchor is None or window.cover_from is None:
        return "the start of the span this trace must cover"
    return (f"{_duration(window.anchor - window.cover_from)} before this packet "
            f"was rejected at {_seconds(window.anchor, like)}")


def _duration(span: timedelta) -> str:
    seconds = max(0, int(span.total_seconds()))
    if seconds >= 60 and seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes} minute{'' if minutes == 1 else 's'}"
    return f"{seconds} second{'' if seconds == 1 else 's'}"


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
