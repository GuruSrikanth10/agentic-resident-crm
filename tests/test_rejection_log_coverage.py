"""
A rejected packet's logs are judged against its rejection, not the look-back.

The office casebook that prompted this: a packet rejected at 15:13:45 IST
was fetched 76 seconds later, and its trace held the whole substage that
rejected it -- yet it opened with

    LOG_ROTATION: requested from 2026-10-10T09:45:01Z, oldest available line
    is 2026-10-12T14:52:33+05:30 ...

because the look-back was K8S_DEFAULT_SINCE_HOURS=48 and the kubelet API
serves only each container's current log file, about 22 minutes there. All
ten office samples carried the same banner: every trace was called
INCOMPLETE, every agent qualified its findings, and every casebook was capped
at confidence 0.6.

These tests pin what replaced it. Rotation and pod replacement are judged pod
by pod against the rejection less REJECTION_LOG_LEAD_SECONDS; a real
shortfall is still a gap; and a covered trace says from when it is complete,
in a LOG COVERAGE line that is not a gap.
"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from src.log_pipeline import pipeline, snapshot
from src.log_pipeline.sources.k8s import gaps
from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER, COVERAGE_PREFIX, PodSpan
from src.log_pipeline.types import (
    EvidenceGap,
    FetchDiagnostics,
    FetchResult,
    GapType,
    TimeWindow,
)
from src.models.synthesis import SynthesisResult, apply_confidence_policy
from src.utils.timestamps import parse_producer_time, utc_now, zoned

#: The office casebook's numbers.
REJECTED_AT = datetime(2026, 10, 12, 9, 43, 45, 16000, tzinfo=timezone.utc)
FETCHED_AT = datetime(2026, 10, 12, 9, 45, 1, tzinfo=timezone.utc)
QUIET_POD_OLDEST = "2026-10-12T14:52:33.989034123+05:30"
BUSY_POD_OLDEST = "2026-10-12T15:01:10.000000001+05:30"
POD_STARTED = datetime(2026, 10, 1, tzinfo=timezone.utc)
REF_ID = "2f41db94-3a08-4344-a6eb-000000000001"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("REJECTION_LOG_LEAD_SECONDS", "K8S_ROTATION_SLACK_SECONDS",
                "K8S_DEFAULT_SINCE_HOURS", "PACKET_LOCAL_UTC_OFFSET",
                "K8S_CONTEXT_LINES_BEFORE", "K8S_CONTEXT_LINES_AFTER"):
        monkeypatch.delenv(var, raising=False)


def _window(lead_minutes=5, hours=48.0):
    return TimeWindow(hours=hours, anchor=REJECTED_AT,
                      cover_from=REJECTED_AT - timedelta(minutes=lead_minutes))


def _span(name, oldest, started=POD_STARTED):
    return PodSpan(pod_name=name, container="app", oldest_line=oldest, start_time=started)


#: Two pods that both reach back past the rejection, as in the office.
COVERED = [_span("enu-biometric-postenrol", QUIET_POD_OLDEST),
           _span("enu-biometric-dcdr", BUSY_POD_OLDEST)]


# ======================================================================
# The window
# ======================================================================

def test_the_window_is_anchored_on_the_rejection(monkeypatch):
    monkeypatch.setenv("K8S_DEFAULT_SINCE_HOURS", "48")
    window = pipeline.rejection_window(REJECTED_AT, now=FETCHED_AT)

    assert window.anchor == REJECTED_AT
    assert window.cover_from == REJECTED_AT - timedelta(seconds=300)
    assert window.coverage_start(FETCHED_AT) == window.cover_from
    # It reads as far back as before: the look-back is how far to READ.
    assert window.hours == 48.0


def test_the_lead_is_configurable(monkeypatch):
    monkeypatch.setenv("REJECTION_LOG_LEAD_SECONDS", "120")
    window = pipeline.rejection_window(REJECTED_AT, now=FETCHED_AT)
    assert window.cover_from == REJECTED_AT - timedelta(seconds=120)


def test_an_unreadable_lead_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("REJECTION_LOG_LEAD_SECONDS", "five minutes")
    window = pipeline.rejection_window(REJECTED_AT, now=FETCHED_AT)
    assert window.cover_from == REJECTED_AT - timedelta(seconds=300)


def test_without_a_rejection_time_the_window_is_the_default_one(monkeypatch):
    monkeypatch.setenv("K8S_DEFAULT_SINCE_HOURS", "48")
    window = pipeline.rejection_window(None, now=FETCHED_AT)

    assert window.anchor is None and window.cover_from is None
    assert window.hours == 48.0


def test_a_rejection_older_than_the_look_back_widens_the_read(monkeypatch):
    """A replay days later still asks for the span the rejection ran in."""
    monkeypatch.setenv("K8S_DEFAULT_SINCE_HOURS", "2")
    later = REJECTED_AT + timedelta(days=3)
    window = pipeline.rejection_window(REJECTED_AT, now=later)
    assert window.start_time(later) <= window.cover_from


def test_a_window_without_an_anchor_is_judged_from_its_start():
    window = TimeWindow(hours=2)
    assert window.coverage_start(FETCHED_AT) == FETCHED_AT - timedelta(hours=2)


# ======================================================================
# The office casebook, judged both ways
# ======================================================================

def test_the_old_check_called_a_covered_rejection_rotated():
    """What every office casebook carried: the oldest line of all pods judged
    against a 48-hour look-back. Kept as the reference for what changed."""
    gap = gaps.detect_rotation_gap(QUIET_POD_OLDEST, TimeWindow(hours=48),
                                   earliest_pod_start=POD_STARTED, now=FETCHED_AT)
    assert gap is not None and gap.gap_type == GapType.LOG_ROTATION


def test_a_covered_rejection_has_no_gap():
    assert gaps.detect_anchored_gaps(COVERED, _window()) == []


def test_a_covered_rejection_says_from_when_it_is_complete():
    note = gaps.coverage_note(COVERED, _window())

    assert note is not None and note.startswith(COVERAGE_PREFIX)
    # The LATEST pod's oldest line: before it, some pod no longer holds its lines.
    assert "complete from 2026-10-12T15:01:10+05:30" in note
    # The rejection, written in the pods' own zone so the two read alike.
    assert "through its rejection at 2026-10-12T15:13:45+05:30" in note
    assert BANNER_HEADER not in note


def test_the_coverage_line_caps_no_confidence():
    """The 0.6 ceiling keys off the banner, and this line is not one."""
    trace = f"{gaps.coverage_note(COVERED, _window())}\n\n--- Log Trace ---\n[t] [app] [INFO] line"
    result = SynthesisResult(rejection_description="d", synthesis="s",
                             action="RESIDENT_PACKET_RESUBMIT",
                             resident_action="NEW_PACKET", confidence=0.9)

    updated, abstained, reason = apply_confidence_policy(result, logs=trace)

    assert updated.confidence == 0.9
    assert reason is None and abstained is False


# ======================================================================
# A real shortfall is still a gap
# ======================================================================

def test_a_pod_that_does_not_reach_the_rejection_is_a_gap():
    """The consumer lagged: the busiest pod's logs now begin inside the span
    the rejecting substage ran in."""
    late = COVERED + [_span("enu-biometric-dcdr-busy", "2026-10-12T15:12:00+05:30")]

    found = gaps.detect_anchored_gaps(late, _window())

    assert [gap.gap_type for gap in found] == [GapType.LOG_ROTATION]
    assert "enu-biometric-dcdr-busy/app (from 2026-10-12T15:12:00+05:30)" in found[0].detail
    assert ("5 minutes before this packet was rejected at "
            "2026-10-12T15:13:45+05:30") in found[0].detail
    assert gaps.coverage_note(late, _window(), found) is None


def test_a_quiet_pod_does_not_answer_for_a_busy_one():
    """The old check took the oldest line of ALL pods -- the quietest pod's --
    which said nothing about the busy pod that may have run the substage."""
    spans = [_span("quiet", "2026-10-12T12:00:00+05:30"),
             _span("busy", "2026-10-12T15:12:00+05:30")]

    found = gaps.detect_anchored_gaps(spans, _window())

    assert len(found) == 1
    assert "busy/app" in found[0].detail and "quiet" not in found[0].detail


def test_a_pod_younger_than_its_missing_span_was_replaced_not_rotated():
    """It started inside the span: nothing was rotated, but the pod it
    replaced may have run the rejecting substage."""
    started = REJECTED_AT - timedelta(minutes=2)
    young = COVERED + [_span("enu-biometric-new", "2026-10-12T15:11:46+05:30",
                             started=started)]

    found = gaps.detect_anchored_gaps(young, _window())

    assert [gap.gap_type for gap in found] == [GapType.POD_REPLACED]
    assert "before this packet was rejected" in found[0].detail
    assert gaps.coverage_note(young, _window(), found) is None


def test_a_pod_restarted_before_the_span_replaced_nothing_that_matters():
    """Under the look-back every pod started in the last 48 hours was
    POD_REPLACED -- thirty of them on two of the office samples."""
    restarted = REJECTED_AT - timedelta(hours=20)
    spans = COVERED + [_span("enu-biometric-restarted", "2026-10-12T14:59:00+05:30",
                             started=restarted)]

    assert gaps.detect_anchored_gaps(spans, _window()) == []
    assert gaps.detect_pod_replaced_gaps(spans, TimeWindow(hours=48), now=FETCHED_AT)


def test_many_late_pods_are_counted_rather_than_listed():
    late = [_span(f"pod-{i}", "2026-10-12T15:12:00+05:30") for i in range(8)]
    detail = gaps.detect_anchored_gaps(late, _window())[0].detail

    assert detail.count("(from ") == 5
    assert "and 3 more" in detail


def test_a_pod_with_no_lines_is_not_judged():
    spans = COVERED + [_span("idle", None)]

    assert gaps.detect_anchored_gaps(spans, _window()) == []
    assert "complete from 2026-10-12T15:01:10+05:30" in gaps.coverage_note(spans, _window())


def test_no_lines_at_all_means_no_coverage_line():
    assert gaps.coverage_note([_span("idle", None)], _window()) is None


@pytest.mark.parametrize("gap_type", [GapType.TRUNCATED, GapType.POD_VANISHED,
                                      GapType.SERVICE_UNAVAILABLE])
def test_a_gap_in_the_span_suppresses_the_coverage_line(gap_type):
    """Its claim -- complete through the rejection -- would be false."""
    assert gaps.coverage_note(COVERED, _window(), [EvidenceGap(gap_type, "x")]) is None


def test_a_parse_gap_does_not_suppress_it():
    """Unreadable levels say nothing about which span the logs cover."""
    degraded = [EvidenceGap(GapType.LEVEL_PARSE_DEGRADED, "x")]
    assert gaps.coverage_note(COVERED, _window(), degraded) is not None


def test_an_unanchored_window_is_judged_as_before():
    """The DLT lane and the CLIs: no coverage line, and the old wording."""
    window = TimeWindow(hours=2)
    young = [_span("p", None, started=FETCHED_AT - timedelta(minutes=30))]

    assert gaps.coverage_note(COVERED, window) is None
    detail = gaps.detect_pod_replaced_gaps(young, window, now=FETCHED_AT)[0].detail
    assert "after the requested window began" in detail


# ======================================================================
# Snapshots
# ======================================================================

@pytest.fixture
def snapshot_store(monkeypatch, tmp_path):
    from src.storage.local import LocalFilesystemCasebookStorage

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "casesheets"))
    monkeypatch.delenv("LOG_SNAPSHOT_REUSE", raising=False)
    monkeypatch.setattr(snapshot, "_casesheets_root", lambda: tmp_path / "casesheets")
    monkeypatch.setattr(snapshot, "get_casebook_storage", lambda: store)
    return store


def _record():
    return {"timestamp": "2026-10-12T15:13:40+05:30", "level": "INFO",
            "message": "m", "app_name": "enu-biometric"}


def test_spans_round_trip_through_a_snapshot(snapshot_store):
    snapshot.save("evt-1", [_record()], spans=COVERED)
    assert snapshot.load_spans("evt-1") == COVERED


def test_a_snapshot_without_spans_says_so(snapshot_store):
    snapshot.save("evt-1", [_record()])
    assert snapshot.load_spans("evt-1") is None


#: What an office snapshot captured before spans were kept recorded:
#: casebook_0922e39e's log_snapshot_meta.json, under a 48-hour look-back.
LEGACY_ROTATION = EvidenceGap(
    GapType.LOG_ROTATION,
    "requested from 2026-10-05T13:11:59.862008+00:00, oldest available line is "
    "2026-10-07T18:09:49.144105+05:30. Logs before 2026-10-07T18:09:49.144105+05:30 "
    "were rotated off the node and are unrecoverable.",
    {"requested_start": "2026-10-05T13:11:59.862008+00:00",
     "oldest_line": "2026-10-07T18:09:49.144105+05:30"},
)
LEGACY_REJECTED_AT = datetime(2026, 10, 7, 13, 8, 39, 935000, tzinfo=timezone.utc)


def _anchored_on(rejected_at):
    return TimeWindow(hours=48, anchor=rejected_at,
                      cover_from=rejected_at - timedelta(minutes=5))


def test_an_old_snapshot_is_rejudged_against_the_rejection():
    degraded = EvidenceGap(GapType.LEVEL_PARSE_DEGRADED, "x")

    found, notes = gaps.rejudge_for_anchor([LEGACY_ROTATION, degraded], None,
                                           _anchored_on(LEGACY_REJECTED_AT))

    assert found == [degraded]
    assert len(notes) == 1
    assert "complete from 2026-10-07T18:09:49+05:30" in notes[0]
    assert "through its rejection at 2026-10-07T18:38:39+05:30" in notes[0]


def test_an_old_snapshot_that_misses_the_rejection_keeps_a_gap():
    rejected_before_the_oldest_line = datetime(2026, 10, 7, 12, 30, tzinfo=timezone.utc)

    found, notes = gaps.rejudge_for_anchor(
        [LEGACY_ROTATION], None, _anchored_on(rejected_before_the_oldest_line))

    assert [gap.gap_type for gap in found] == [GapType.LOG_ROTATION]
    assert "before this packet was rejected" in found[0].detail
    assert notes == []


def test_an_old_snapshots_replaced_pods_are_rejudged_too():
    replaced = EvidenceGap(GapType.POD_REPLACED, "old wording",
                           {"pod_name": "enu-biometric-x",
                            "started": "2026-10-06T10:00:00+00:00"})

    found, _ = gaps.rejudge_for_anchor([LEGACY_ROTATION, replaced], None,
                                       _anchored_on(LEGACY_REJECTED_AT))

    assert found == []


def test_a_snapshot_with_spans_is_judged_afresh():
    stale = EvidenceGap(GapType.LOG_ROTATION, "judged against the look-back", {})

    found, notes = gaps.rejudge_for_anchor([stale], COVERED, _window())

    assert found == []
    assert notes and notes[0].startswith(COVERAGE_PREFIX)


# ======================================================================
# KubernetesLogSource, end to end on fixtures
# ======================================================================

def _pod(root, name, lines, started="2026-10-01T00:00:00+00:00"):
    pod_dir = root / "enu" / name
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running", "labels": {}, "containers": ["app"],
        "restart_counts": {}, "start_time": started,
    }), encoding="utf-8")


def test_the_source_reports_coverage_rather_than_rotation(monkeypatch, tmp_path,
                                                         snapshot_store):
    from src.log_pipeline.sources.k8s import client as k8s_client_module
    from src.log_pipeline.sources.k8s.source import KubernetesLogSource
    from src.log_pipeline.types import FetchContext

    fixtures = tmp_path / "fixtures"
    _pod(fixtures, "enu-biometric-postenrol-a", [
        "2026-10-12T14:52:33.989034123+05:30 INFO startup",
        f"2026-10-12T15:13:40.100000000+05:30 INFO Received CRE Policy Batch "
        f"response for Applicant RefID:{REF_ID}",
    ])
    _pod(fixtures, "enu-biometric-dcdr-b", [
        "2026-10-12T15:01:10.000000001+05:30 INFO another packet",
        f"2026-10-12T15:13:38.500000000+05:30 INFO Received cross match check "
        f"response for Applicant RefID:{REF_ID}",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(fixtures))
    monkeypatch.setenv("K8S_DEFAULT_NAMESPACE", "enu")
    k8s_client_module.reset_client()
    ctx = FetchContext(event_id=REF_ID, apps=("enu-biometric",))

    result = KubernetesLogSource().fetch(REF_ID, _window(), ctx)

    assert result.ok and result.records
    assert result.gaps == []
    assert len(result.notes) == 1
    assert "complete from 2026-10-12T15:01:10+05:30" in result.notes[0]
    assert len(snapshot.load_spans(REF_ID)) == 2

    # A later fetch reuses the snapshot and is judged the same way.
    again = KubernetesLogSource().fetch(REF_ID, _window(), ctx)
    assert again.gaps == [] and again.notes == result.notes


# ======================================================================
# The trace the agents read
# ======================================================================

@pytest.fixture
def fake_fetch(monkeypatch):
    def install(result):
        monkeypatch.setattr(pipeline.source_chain, "fetch_with_fallback",
                            lambda *args, **kwargs: result)
    return install


def test_the_trace_opens_with_the_coverage_line(fake_fetch):
    note = gaps.coverage_note(COVERED, _window())
    fake_fetch(FetchResult(
        records=[{"timestamp": "2026-10-12T15:13:38.5+05:30", "level": "INFO",
                  "message": f"Received cross match check response for {REF_ID}",
                  "app_name": "enu-biometric"}],
        diagnostics=FetchDiagnostics(source="kubernetes"),
        notes=[note],
    ))

    out = pipeline.reduce_logs(REF_ID, window=_window())

    assert out.splitlines()[0] == note
    assert BANNER_HEADER not in out


def test_the_coverage_line_follows_a_banner(fake_fetch):
    note = gaps.coverage_note(COVERED, _window())
    degraded = EvidenceGap(GapType.LEVEL_PARSE_DEGRADED, "levels unreadable")
    fake_fetch(FetchResult(records=[], diagnostics=FetchDiagnostics(source="kubernetes"),
                           gaps=[degraded], notes=[note]))

    out = pipeline.reduce_logs(REF_ID, window=_window())

    lines = out.splitlines()
    assert lines[0] == BANNER_HEADER
    assert lines.index(note) > lines.index(gaps.BANNER_FOOTER)
    assert lines[-1] == f"No logs found for ID: {REF_ID}"


# ======================================================================
# The rejection lane anchors on the payload
# ======================================================================

@pytest.mark.parametrize("event_timestamp, expected", [
    pytest.param("2026-10-12T09:43:45.016Z", REJECTED_AT, id="audit-contract-utc"),
    pytest.param("2026-10-12 15:13:45.016", REJECTED_AT, id="packet-event-ist"),
    pytest.param(None, None, id="absent"),
])
def test_the_fetch_is_anchored_on_the_payloads_event_timestamp(
        monkeypatch, tmp_path, event_timestamp, expected):
    import src.tools.tool_registry as tool_registry
    from src.storage.local import LocalFilesystemCasebookStorage

    seen = {}

    def fake_fetch_logs_for(event_id, extra_identifiers=(), service=None, window=None):
        seen["window"] = window
        return "trace"

    monkeypatch.setenv("ENABLE_LOG_FETCHING", "true")
    monkeypatch.setattr(tool_registry, "fetch_logs_for", fake_fetch_logs_for)
    monkeypatch.setattr(tool_registry, "get_casebook_storage",
                        lambda: LocalFilesystemCasebookStorage(base_dir=str(tmp_path)))

    tool_registry.fetch_and_persist_logs(
        "evt-1", {"eventId": "evt-1", "eventTimestamp": event_timestamp})

    assert seen["window"].anchor == expected


# ======================================================================
# Timestamps
# ======================================================================

def test_a_zoneless_producer_time_is_written_with_ist():
    assert zoned("2026-10-12 12:14:29") == "2026-10-12T12:14:29+05:30"


def test_a_time_that_names_its_zone_is_kept_as_written():
    assert zoned("2026-10-12T09:43:45.016Z") == "2026-10-12T09:43:45.016Z"


def test_an_unreadable_time_is_kept_and_a_missing_one_is_none():
    assert zoned("not a time") == "not a time"
    assert zoned(None) is None and zoned("") is None


def test_the_producer_offset_is_configurable(monkeypatch):
    monkeypatch.setenv("PACKET_LOCAL_UTC_OFFSET", "+04:00")
    assert zoned("2026-10-12 12:14:29") == "2026-10-12T12:14:29+04:00"

    monkeypatch.setenv("PACKET_LOCAL_UTC_OFFSET", "IST")
    assert zoned("2026-10-12 12:14:29") == "2026-10-12T12:14:29+05:30"


def test_a_producer_time_parses_to_an_instant():
    assert parse_producer_time("2026-10-12 15:13:45.016") == REJECTED_AT
    assert parse_producer_time("2026-10-12T09:43:45.016Z") == REJECTED_AT
    assert parse_producer_time("garbage") is None
    assert parse_producer_time(None) is None


def test_this_systems_own_stamps_are_utc_and_say_so():
    stamp = utc_now()
    assert stamp.endswith("Z")
    assert datetime.fromisoformat(stamp).utcoffset() == timedelta(0)


# ======================================================================
# The oldest line is the earliest instant
# ======================================================================

def test_the_oldest_line_is_the_earliest_instant_not_the_smallest_string():
    from src.log_pipeline.sources.k8s.retrieval import _older

    # 09:50Z is 15:20 IST: the later of the two, though it sorts first.
    assert _older("2026-10-12T09:50:00Z", "2026-10-12T14:52:33+05:30") == \
        "2026-10-12T14:52:33+05:30"
    assert _older("garbage", "also garbage") == "also garbage"
