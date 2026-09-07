"""
Phase 4 of KUBERNETES_LOGS_PLAN.md -- filtering, context windows, and
evidence gaps.

The gap machinery is what makes the Kubernetes source trustworthy: kubelet
retention is short and pods get replaced, so a fetch routinely returns less
than was asked for. Announcing that is the difference between a system that
degrades safely and one that degrades silently.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from src.log_pipeline.sources.k8s import gaps as gaps_module
from src.log_pipeline.sources.k8s import retrieval
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.sources.k8s.filtering import (
    ContextWindowSelector,
    KeepAllSelector,
    build_matcher,
    build_selector,
    resolve_search_values,
)
from src.log_pipeline.sources.k8s.parser import ParseStats
from src.log_pipeline.types import EvidenceGap, GapType, TimeWindow

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("K8S_FIXTURE_DIR", "K8S_CONTEXT_LINES_BEFORE",
                "K8S_CONTEXT_LINES_AFTER", "K8S_LEVEL_PARSE_WARN_THRESHOLD",
                "K8S_ROTATION_SLACK_SECONDS", "K8S_MAX_BYTES_PER_POD"):
        monkeypatch.delenv(var, raising=False)


# ======================================================================
# Identifier matching
# ======================================================================

def test_matcher_finds_the_identifier():
    matcher = build_matcher(["evt-123"])
    assert matcher("some line with evt-123 inside") is True
    assert matcher("unrelated line") is False


def test_matcher_is_case_sensitive_by_default():
    """Identifiers are UUIDs and reference numbers; a case-insensitive match
    buys nothing and risks false positives."""
    matcher = build_matcher(["EVT-abc"])
    assert matcher("EVT-abc") is True
    assert matcher("evt-ABC") is False


def test_matcher_supports_multiple_identifiers():
    """We do not yet know whether services log eventId or refId (Open
    Question 1), so both are searched."""
    matcher = build_matcher(["evt-1", "ref-9"])
    assert matcher("carrying ref-9 only") is True
    assert matcher("carrying evt-1 only") is True
    assert matcher("neither") is False


def test_empty_identifier_list_matches_nothing():
    assert build_matcher([])("anything") is False


def test_resolve_search_values_dedupes():
    assert resolve_search_values("evt-1", ["ref-2", "evt-1"]) == ["evt-1", "ref-2"]
    assert resolve_search_values("evt-1") == ["evt-1"]
    assert resolve_search_values("") == []


def test_build_selector_without_identifier_keeps_everything():
    """Filtering on nothing would silently discard the whole trace."""
    assert isinstance(build_selector(""), KeepAllSelector)


# ======================================================================
# Context windows
# ======================================================================

def _run(selector, lines):
    """Emitted lines only. `_run_marked` keeps the match flags."""
    return [line for line, _ in _run_marked(selector, lines)]


def _run_marked(selector, lines):
    emitted = []
    for line in lines:
        emitted.extend(selector.feed(line))
    return emitted


def test_context_lines_before_a_match_are_kept():
    """A match is often one line of a stack trace whose useful part -- the
    exception -- appears just above it."""
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=2, after=0)
    out = _run(selector, ["a", "b", "c", "hit evt-1", "d"])
    assert out == ["b", "c", "hit evt-1"]


def test_context_lines_after_a_match_are_kept():
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=0, after=2)
    out = _run(selector, ["a", "hit evt-1", "b", "c", "d"])
    assert out == ["hit evt-1", "b", "c"]


def test_overlapping_windows_merge_without_duplicates():
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=2, after=2)
    out = _run(selector, ["a", "hit1 evt-1", "b", "hit2 evt-1", "c", "d", "e"])
    assert out == ["a", "hit1 evt-1", "b", "hit2 evt-1", "c", "d"]
    assert len(out) == len(set(out))


def test_context_lines_are_emitted_but_flagged():
    """The whole point of the flag: a context line is kept as evidence and
    labelled as not this packet's, so corroboration can tell them apart."""
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=1, after=1)
    out = _run_marked(selector, ["before", "hit evt-1", "after", "far"])
    assert out == [("before", False), ("hit evt-1", True), ("after", False)]


def test_keep_all_selector_reports_every_line_as_a_match():
    """With no identifier to filter on there are no context lines -- calling
    them context would tell corroboration to discard the whole trace."""
    assert _run_marked(KeepAllSelector(), ["a", "b"]) == [("a", True), ("b", True)]


def test_zero_context_emits_only_matches():
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=0, after=0)
    out = _run(selector, ["a", "hit evt-1", "b"])
    assert out == ["hit evt-1"]


def test_before_buffer_does_not_leak_already_emitted_lines():
    """A trailing-context line must not be re-emitted as leading context for
    a later match."""
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=3, after=1)
    out = _run(selector, ["hit evt-1", "trail", "x", "hit2 evt-1"])
    assert out.count("trail") == 1


def test_no_match_emits_nothing():
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=2, after=2)
    assert _run(selector, ["a", "b", "c"]) == []


def test_reset_clears_state():
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=2, after=2)
    _run(selector, ["a", "b", "hit evt-1"])
    selector.reset()
    assert _run(selector, ["c"]) == []


def test_context_window_sizes_come_from_config(monkeypatch):
    monkeypatch.setenv("K8S_CONTEXT_LINES_BEFORE", "1")
    monkeypatch.setenv("K8S_CONTEXT_LINES_AFTER", "1")
    out = _run(build_selector("evt-1"), ["a", "b", "hit evt-1", "c", "d"])
    assert out == ["b", "hit evt-1", "c"]


def test_selector_state_does_not_leak_between_container_instances(monkeypatch, tmp_path):
    """previous and current are separate streams; trailing context from one
    must not spill into the other."""
    pod_dir = tmp_path / "enu" / "pod-a"
    pod_dir.mkdir(parents=True)
    (pod_dir / "previous.log").write_text("2026-01-01T10:00:00Z ERROR hit evt-1\n")
    (pod_dir / "current.log").write_text("2026-01-01T10:00:05Z INFO unrelated\n")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running", "labels": {"app": "x"},
        "containers": ["app"], "restart_counts": {"app": 1},
    }))
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    target = PodTarget(namespace="enu", pod_name="pod-a", container="app",
                       restart_count=1, phase="Running")
    selector = ContextWindowSelector(build_matcher(["evt-1"]), before=0, after=5)
    outcome = retrieval.read_pod_logs(target, TimeWindow.default(), selector=selector)

    # Only the previous instance matched; the after-window must not carry over.
    assert [r["container_instance"] for r in outcome.records] == ["previous"]


# ======================================================================
# Rotation gap
# ======================================================================

OLD_POD_START = NOW - timedelta(days=1)


def test_rotation_gap_detected_when_stream_starts_too_late():
    """Asked for 2h, the pod's stream starts 45m ago, and the pod is a day
    old: the kubelet discarded everything before that."""
    oldest = (NOW - timedelta(minutes=45)).isoformat()
    gap = gaps_module.detect_rotation_gap(
        oldest, TimeWindow(hours=2), earliest_pod_start=OLD_POD_START, now=NOW
    )

    assert gap is not None
    assert gap.gap_type == GapType.LOG_ROTATION
    assert "unrecoverable" in gap.detail


def test_no_rotation_gap_when_window_is_covered():
    oldest = (NOW - timedelta(hours=2)).isoformat()
    assert gaps_module.detect_rotation_gap(
        oldest, TimeWindow(hours=2), earliest_pod_start=OLD_POD_START, now=NOW
    ) is None


def test_no_rotation_gap_without_any_observed_lines():
    assert gaps_module.detect_rotation_gap(
        None, TimeWindow(hours=2), earliest_pod_start=OLD_POD_START, now=NOW
    ) is None


def test_rotation_ignores_unparseable_timestamps():
    assert gaps_module.detect_rotation_gap(
        "not-a-timestamp", TimeWindow(hours=2),
        earliest_pod_start=OLD_POD_START, now=NOW,
    ) is None


def test_rotation_slack_absorbs_clock_skew():
    """A few seconds of drift must not raise a spurious rotation gap."""
    oldest = (NOW - timedelta(hours=2) + timedelta(seconds=30)).isoformat()
    assert gaps_module.detect_rotation_gap(
        oldest, TimeWindow(hours=2), earliest_pod_start=OLD_POD_START, now=NOW
    ) is None


def test_no_rotation_gap_when_the_pod_is_younger_than_the_missing_span():
    """A pod that only started 45m ago did not have its logs rotated -- there
    was simply nothing earlier to read. POD_REPLACED describes that instead;
    reporting rotation too would be actively misleading."""
    started = NOW - timedelta(minutes=45)
    oldest = (NOW - timedelta(minutes=44)).isoformat()

    assert gaps_module.detect_rotation_gap(
        oldest, TimeWindow(hours=2), earliest_pod_start=started, now=NOW
    ) is None


def test_rotation_still_reported_when_pod_start_is_unknown():
    """Without a start time we cannot rule rotation out, so we report it --
    an unreported gap is worse than an over-reported one."""
    oldest = (NOW - timedelta(minutes=45)).isoformat()
    assert gaps_module.detect_rotation_gap(
        oldest, TimeWindow(hours=2), earliest_pod_start=None, now=NOW
    ) is not None


# ======================================================================
# Pod replaced gap
# ======================================================================

def _target_started(name, started):
    return PodTarget(namespace="enu", pod_name=name, container="app",
                     phase="Running", start_time=started)


def test_pod_replaced_gap_when_pod_started_inside_the_window():
    targets = [_target_started("new-pod", NOW - timedelta(minutes=30))]
    found = gaps_module.detect_pod_replaced_gaps(targets, TimeWindow(hours=2), now=NOW)

    assert len(found) == 1
    assert found[0].gap_type == GapType.POD_REPLACED
    assert "logs are gone" in found[0].detail


def test_no_pod_replaced_gap_for_long_lived_pod():
    targets = [_target_started("old-pod", NOW - timedelta(hours=5))]
    assert gaps_module.detect_pod_replaced_gaps(targets, TimeWindow(hours=2), now=NOW) == []


def test_pod_replaced_gap_is_reported_once_per_pod():
    """A multi-container pod must not produce one gap per container."""
    started = NOW - timedelta(minutes=10)
    targets = [
        PodTarget(namespace="enu", pod_name="p", container="app",
                  phase="Running", start_time=started),
        PodTarget(namespace="enu", pod_name="p", container="worker",
                  phase="Running", start_time=started),
    ]
    assert len(gaps_module.detect_pod_replaced_gaps(targets, TimeWindow(hours=2), now=NOW)) == 1


def test_pod_without_start_time_is_skipped():
    targets = [_target_started("unknown", None)]
    assert gaps_module.detect_pod_replaced_gaps(targets, TimeWindow(hours=2), now=NOW) == []


# ======================================================================
# Parse degradation gap
# ======================================================================

def test_parse_degradation_gap_when_format_is_not_understood():
    """Matters because branch_on_error keys off level == ERROR: a stream we
    cannot read levels from makes ERROR detection unreliable."""
    gap = gaps_module.detect_parse_degradation_gap(
        ParseStats(total=100, level_parsed=2)
    )
    assert gap is not None
    assert gap.gap_type == GapType.LEVEL_PARSE_DEGRADED
    assert "must not be read as an absence of errors" in gap.detail


def test_no_degradation_gap_for_a_healthy_stream():
    assert gaps_module.detect_parse_degradation_gap(
        ParseStats(total=100, level_parsed=95)
    ) is None


def test_no_degradation_gap_for_an_empty_stream():
    assert gaps_module.detect_parse_degradation_gap(ParseStats()) is None


def test_degradation_threshold_is_configurable(monkeypatch):
    monkeypatch.setenv("K8S_LEVEL_PARSE_WARN_THRESHOLD", "0.2")
    gap = gaps_module.detect_parse_degradation_gap(
        ParseStats(total=100, level_parsed=70)  # 30% failure
    )
    assert gap is not None


# ======================================================================
# Banner rendering
# ======================================================================

def test_banner_is_empty_without_gaps():
    assert gaps_module.render_banner([]) == ""


def test_banner_lists_every_gap_between_markers():
    banner = gaps_module.render_banner([
        EvidenceGap(GapType.LOG_ROTATION, "rotated at 09:14"),
        EvidenceGap(GapType.POD_REPLACED, "pod started late"),
    ])
    lines = banner.splitlines()

    assert lines[0] == gaps_module.BANNER_HEADER
    assert lines[-1] == gaps_module.BANNER_FOOTER
    assert "LOG_ROTATION: rotated at 09:14" in lines
    assert "POD_REPLACED: pod started late" in lines


def test_banner_warns_the_trace_is_incomplete():
    banner = gaps_module.render_banner([EvidenceGap(GapType.TRUNCATED, "capped")])
    assert "INCOMPLETE" in banner


def test_duplicate_gaps_are_collapsed():
    """Twenty pods hitting the same cap should be one banner line, not twenty."""
    duplicates = [EvidenceGap(GapType.TRUNCATED, "same detail") for _ in range(20)]
    assert len(gaps_module.dedupe_gaps(duplicates)) == 1


def test_dedupe_preserves_order_and_distinct_gaps():
    unique = gaps_module.dedupe_gaps([
        EvidenceGap(GapType.LOG_ROTATION, "a"),
        EvidenceGap(GapType.TRUNCATED, "b"),
        EvidenceGap(GapType.LOG_ROTATION, "a"),
    ])
    assert [g.detail for g in unique] == ["a", "b"]
