"""
Phase 3 of KUBERNETES_LOGS_PLAN.md -- log line parsing.

Parsing is the highest-risk component in the design: `branch_on_error` decides
between the "stuck packet" and "clean rejection" paths purely on
`level == "ERROR"`. A parser that silently defaults everything to INFO would
misclassify every stuck packet, and no test of the Kubernetes client itself
would catch it.
"""
import pytest

from src.log_pipeline.reducer import branch_on_error
from src.log_pipeline.sources.k8s.parser import (
    ParseStats,
    normalise_level,
    parse_line,
    parse_stream,
    split_kubelet_timestamp,
)
from src.log_pipeline.types import REQUIRED_RECORD_KEYS

KUBELET_TS = "2026-01-01T10:15:30.123456789Z"


# ======================================================================
# THE REGRESSION GUARD
# ======================================================================

def test_branch_on_error_fires_on_a_kubernetes_sourced_error_record():
    """The single most important test in this phase: end-to-end proof that
    adding a Kubernetes source preserves Stage 2 semantics. If this fails,
    stuck packets are being silently misclassified as clean rejections."""
    text = "\n".join([
        f"{KUBELET_TS} INFO  starting biometric stage",
        f"{KUBELET_TS} ERROR dedup service connection refused",
        f"{KUBELET_TS} INFO  retrying",
    ])
    records = parse_stream(text).records

    assert [r["level"] for r in records] == ["INFO", "ERROR", "INFO"]

    result = branch_on_error(records)
    assert result["has_error"] is True, (
        "branch_on_error did not fire on a K8s-sourced ERROR record -- "
        "stuck packets would be misclassified as clean rejections"
    )
    assert any("connection refused" in r["message"] for r in result["payload"])


def test_branch_on_error_does_not_fire_without_errors():
    text = "\n".join([
        f"{KUBELET_TS} INFO  starting",
        f"{KUBELET_TS} INFO  finished",
    ])
    assert branch_on_error(parse_stream(text).records)["has_error"] is False


# ======================================================================
# Kubelet timestamp handling
# ======================================================================

def test_kubelet_timestamp_is_split_off():
    ts, remainder = split_kubelet_timestamp(f"{KUBELET_TS} ERROR boom")
    assert ts == KUBELET_TS
    assert remainder == "ERROR boom"


@pytest.mark.parametrize("ts", [
    "2026-01-01T10:15:30Z",
    "2026-01-01T10:15:30.123Z",
    "2026-01-01T10:15:30.123456789Z",
    "2026-01-01T10:15:30+05:30",
])
def test_timestamp_formats_are_recognised(ts):
    parsed, remainder = split_kubelet_timestamp(f"{ts} INFO hello")
    assert parsed == ts
    assert remainder == "INFO hello"


def test_line_without_kubelet_timestamp_is_still_parsed():
    ts, remainder = split_kubelet_timestamp("ERROR no timestamp here")
    assert ts is None
    assert remainder == "ERROR no timestamp here"


def test_kubelet_timestamp_wins_over_application_timestamp():
    """The kubelet clock is one clock per node; application timestamps vary in
    format and timezone handling, so the kubelet value is authoritative."""
    line = f'{KUBELET_TS} {{"@timestamp":"1999-01-01T00:00:00Z","level":"WARN","message":"x"}}'
    record, _, _ = parse_line(line)
    assert record["timestamp"] == KUBELET_TS


def test_application_timestamp_used_when_kubelet_prefix_absent():
    line = '{"@timestamp":"2026-02-02T02:02:02Z","level":"INFO","message":"x"}'
    record, _, _ = parse_line(line)
    assert record["timestamp"] == "2026-02-02T02:02:02Z"


# ======================================================================
# JSON logs
# ======================================================================

def test_json_line_is_parsed():
    line = (
        f'{KUBELET_TS} {{"level":"ERROR","message":"dedup failed",'
        f'"application_name":"enu-biometric"}}'
    )
    record, level_ok, was_json = parse_line(line)

    assert was_json is True
    assert level_ok is True
    assert record["level"] == "ERROR"
    assert record["message"] == "dedup failed"
    assert record["app_name"] == "enu-biometric"
    assert record["source"] == "kubernetes"


@pytest.mark.parametrize("key", ["level", "severity", "log_level", "lvl"])
def test_alternate_json_level_keys(key):
    line = f'{KUBELET_TS} {{"{key}":"ERROR","message":"x"}}'
    record, level_ok, _ = parse_line(line)
    assert level_ok is True
    assert record["level"] == "ERROR"


@pytest.mark.parametrize("key", ["message", "msg", "log", "text"])
def test_alternate_json_message_keys(key):
    line = f'{KUBELET_TS} {{"level":"INFO","{key}":"the payload"}}'
    record, _, _ = parse_line(line)
    assert record["message"] == "the payload"


def test_malformed_json_falls_back_to_regex():
    line = f'{KUBELET_TS} {{"level":"ERROR", broken json'
    record, level_ok, was_json = parse_line(line)
    assert was_json is False
    # The regex fallback still recovers the level from the raw text.
    assert level_ok is True
    assert record["level"] == "ERROR"


def test_json_array_is_not_treated_as_a_log_object():
    line = f'{KUBELET_TS} [1, 2, 3]'
    record, _, was_json = parse_line(line)
    assert was_json is False
    assert record["message"] == "[1, 2, 3]"


# ======================================================================
# Plain text logs
# ======================================================================

@pytest.mark.parametrize("raw,expected", [
    ("ERROR something broke", "ERROR"),
    ("WARN  degraded", "WARN"),
    ("WARNING degraded", "WARN"),
    ("INFO  all good", "INFO"),
    ("DEBUG verbose", "DEBUG"),
    ("TRACE noisy", "TRACE"),
    ("FATAL dead", "ERROR"),
    ("SEVERE dead", "ERROR"),
])
def test_plain_text_levels(raw, expected):
    record, level_ok, _ = parse_line(f"{KUBELET_TS} {raw}")
    assert level_ok is True
    assert record["level"] == expected


def test_spring_boot_style_line():
    raw = "2026-01-01 10:15:30.123  ERROR 1 --- [main] c.u.BioService : dedup rejected"
    record, level_ok, _ = parse_line(f"{KUBELET_TS} {raw}")
    assert level_ok is True
    assert record["level"] == "ERROR"


def test_unparseable_line_defaults_to_info_but_reports_failure():
    """The distinction matters: a defaulted INFO must be distinguishable from
    a genuine one, or a totally unparseable stream looks healthy."""
    record, level_ok, _ = parse_line(f"{KUBELET_TS} just some prose with no level")
    assert record["level"] == "INFO"
    assert level_ok is False


def test_level_token_requires_a_word_boundary():
    """`ERRORS_TOTAL=0` must not be read as an ERROR line."""
    record, level_ok, _ = parse_line(f"{KUBELET_TS} metric ERRORS_TOTAL=0 reported")
    assert level_ok is False
    assert record["level"] == "INFO"


def test_level_is_only_scanned_near_the_start_of_the_line():
    """A message merely mentioning 'error' far into the text is not a level."""
    tail = "x" * 120 + " ERROR"
    record, level_ok, _ = parse_line(f"{KUBELET_TS} {tail}")
    assert level_ok is False


# ======================================================================
# Canonical shape and stats
# ======================================================================

def test_every_record_has_the_required_keys():
    text = "\n".join([
        f"{KUBELET_TS} INFO plain",
        f'{KUBELET_TS} {{"level":"ERROR","message":"json"}}',
        f"{KUBELET_TS} unparseable",
    ])
    for record in parse_stream(text).records:
        for key in REQUIRED_RECORD_KEYS:
            assert key in record and record[key] is not None


def test_blank_lines_are_skipped():
    text = f"{KUBELET_TS} INFO one\n\n   \n{KUBELET_TS} INFO two\n"
    assert len(parse_stream(text).records) == 2


def test_parse_stats_track_degradation():
    text = "\n".join([
        f"{KUBELET_TS} ERROR recognised",
        f"{KUBELET_TS} prose with no level",
        f"{KUBELET_TS} more prose",
        f"{KUBELET_TS} still prose",
    ])
    stats = parse_stream(text).stats
    assert stats.total == 4
    assert stats.level_parsed == 1
    assert stats.level_failure_rate == 0.75


def test_json_ratio_supports_format_detection():
    text = "\n".join([
        f'{KUBELET_TS} {{"level":"INFO","message":"a"}}',
        f'{KUBELET_TS} {{"level":"INFO","message":"b"}}',
        f"{KUBELET_TS} INFO plain",
    ])
    stats = parse_stream(text).stats
    assert stats.json_lines == 2
    assert stats.json_ratio == pytest.approx(2 / 3)


def test_empty_stream_reports_no_failure():
    stats = parse_stream("").stats
    assert stats.total == 0
    assert stats.level_failure_rate == 0.0


def test_normalise_level_aliases():
    assert normalise_level("warning") == "WARN"
    assert normalise_level("Fatal") == "ERROR"
    assert normalise_level("critical") == "ERROR"
    assert normalise_level("bogus") is None
    assert normalise_level(None) is None


def test_default_app_name_is_used_when_absent():
    record, _, _ = parse_line(f"{KUBELET_TS} INFO x", default_app="my-container")
    assert record["app_name"] == "my-container"


def test_parse_stats_merge_semantics():
    stats = ParseStats(total=2, level_parsed=1, json_lines=1)
    assert stats.level_failure_rate == 0.5
    assert stats.json_ratio == 0.5
