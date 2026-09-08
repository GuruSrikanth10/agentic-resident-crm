"""
Phase 6 of KUBERNETES_LOGS_PLAN.md -- PII redaction.

Raw pod logs are unfiltered where the Elasticsearch path projected to four
fields, and this text lands on disk and possibly in S3. In a biometric
enrolment context that is the difference between a log file and a data breach.
"""
import json

import pytest

from src.log_pipeline import redaction
from src.log_pipeline.sources.k8s import retrieval
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.types import TimeWindow

KUBELET_TS = "2026-01-01T10:15:30.000000000Z"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("K8S_REDACT_ENABLED", "K8S_REDACT_EXTRA_PATTERNS",
                "K8S_FIXTURE_DIR"):
        monkeypatch.delenv(var, raising=False)
    yield


# ======================================================================
# Patterns
# ======================================================================

def test_aadhaar_is_redacted():
    result = redaction.redact_text("resident 123456789012 rejected")
    assert "123456789012" not in result.text
    assert "[REDACTED:AADHAAR]" in result.text
    assert result.counts["AADHAAR"] == 1


def test_spaced_aadhaar_is_redacted():
    for spaced in ("1234 5678 9012", "1234-5678-9012"):
        result = redaction.redact_text(f"uid {spaced} failed")
        assert spaced not in result.text
        assert "[REDACTED:AADHAAR]" in result.text


def test_vid_is_redacted_and_not_split_into_an_aadhaar():
    """A 16-digit VID must not be partially matched by the 12-digit pattern."""
    result = redaction.redact_text("vid 1234567890123456 used")
    assert "[REDACTED:VID]" in result.text
    assert "AADHAAR" not in result.text
    assert result.counts == {"VID": 1}


def test_mobile_is_redacted():
    result = redaction.redact_text("notify 9876543210 now")
    assert "[REDACTED:MOBILE]" in result.text


def test_non_indian_mobile_prefix_is_not_matched():
    """Only 6-9 prefixes are Indian mobile numbers; a 10-digit id starting
    with 1 is something else and must survive."""
    result = redaction.redact_text("counter 1234567890 incremented")
    assert "1234567890" in result.text


def test_email_is_redacted():
    result = redaction.redact_text("contact resident.name@example.co.in today")
    assert "resident.name@example.co.in" not in result.text
    assert "[REDACTED:EMAIL]" in result.text


def test_multiple_pii_types_are_counted_separately():
    result = redaction.redact_text(
        "uid 123456789012 mobile 9876543210 mail a@b.com"
    )
    assert result.counts["AADHAAR"] == 1
    assert result.counts["MOBILE"] == 1
    assert result.counts["EMAIL"] == 1


def test_clean_text_is_untouched():
    text = "dedup rejected for reason RESIDENT_MAN_DEDUP_REJECT_TD"
    result = redaction.redact_text(text)
    assert result.text == text
    assert result.counts == {}


def test_empty_text_is_safe():
    assert redaction.redact_text("").text == ""


# ======================================================================
# Allowlist -- the over-redaction guard
# ======================================================================

def test_allowlisted_identifier_survives_redaction():
    """A 12-digit refId would otherwise be scrubbed as an Aadhaar number,
    destroying the very identifier the investigation is about."""
    result = redaction.redact_text(
        "processing refId 987654321098 for resident 123456789012",
        allowlist=["987654321098"],
    )
    assert "987654321098" in result.text
    assert "123456789012" not in result.text
    assert result.counts["AADHAAR"] == 1


def test_multiple_allowlisted_values_survive():
    result = redaction.redact_text(
        "evt 111111111111 ref 222222222222 uid 333333333333",
        allowlist=["111111111111", "222222222222"],
    )
    assert "111111111111" in result.text
    assert "222222222222" in result.text
    assert "333333333333" not in result.text


def test_allowlist_ignores_values_not_present():
    result = redaction.redact_text("nothing sensitive", allowlist=["absent-id"])
    assert result.text == "nothing sensitive"


def test_allowlisted_email_survives():
    result = redaction.redact_text(
        "operator ops@internal.example sent it",
        allowlist=["ops@internal.example"],
    )
    assert "ops@internal.example" in result.text


# ======================================================================
# Configuration
# ======================================================================

def test_redaction_can_be_disabled(monkeypatch):
    monkeypatch.setenv("K8S_REDACT_ENABLED", "false")
    records = [{"message": "uid 123456789012", "level": "INFO",
                "timestamp": "", "app_name": "a"}]
    counts = redaction.redact_records(records)
    assert counts == {}
    assert records[0]["message"] == "uid 123456789012"


def test_extra_patterns_are_applied(monkeypatch):
    monkeypatch.setenv("K8S_REDACT_EXTRA_PATTERNS", r"SECRET-\w+")
    result = redaction.redact_text("token SECRET-abc123 leaked")
    assert "SECRET-abc123" not in result.text
    assert "[REDACTED:CUSTOM]" in result.text


def test_invalid_extra_pattern_is_ignored_not_fatal(monkeypatch):
    monkeypatch.setenv("K8S_REDACT_EXTRA_PATTERNS", "([unclosed")
    result = redaction.redact_text("uid 123456789012")
    assert "[REDACTED:AADHAAR]" in result.text


# ======================================================================
# Record-level redaction
# ======================================================================

def test_redact_records_mutates_messages_and_totals_counts():
    records = [
        {"message": "uid 123456789012", "level": "INFO", "timestamp": "", "app_name": "a"},
        {"message": "uid 210987654321", "level": "INFO", "timestamp": "", "app_name": "a"},
    ]
    counts = redaction.redact_records(records)

    assert counts["AADHAAR"] == 2
    assert all("REDACTED" in r["message"] for r in records)


# ======================================================================
# Integration: nothing unredacted escapes retrieval
# ======================================================================

def _fixture_pod(root, namespace, pod, lines):
    pod_dir = root / namespace / pod
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running", "labels": {"app": "enu-biometric"},
        "containers": ["app"], "restart_counts": {},
    }), encoding="utf-8")


def test_retrieval_redacts_before_returning(monkeypatch, tmp_path):
    """The end-to-end guarantee: unredacted PII never leaves retrieval, so it
    never reaches raw_logs.txt, the snapshot, or S3."""
    _fixture_pod(tmp_path, "enu", "pod-a", [
        f"{KUBELET_TS} INFO evt-1 resident 123456789012 mobile 9876543210",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    target = PodTarget(namespace="enu", pod_name="pod-a", container="app")
    outcome = retrieval.read_pod_logs(target, TimeWindow.default())

    message = outcome.records[0]["message"]
    assert "123456789012" not in message
    assert "9876543210" not in message
    assert outcome.redaction_counts["AADHAAR"] == 1
    assert outcome.redaction_counts["MOBILE"] == 1


def test_retrieval_preserves_the_searched_identifier(monkeypatch, tmp_path):
    """The identifier is passed as an allowlist so filtering on it stays
    meaningful even when it looks like PII."""
    _fixture_pod(tmp_path, "enu", "pod-a", [
        f"{KUBELET_TS} INFO refId 987654321098 rejected",
    ])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    target = PodTarget(namespace="enu", pod_name="pod-a", container="app")
    outcome = retrieval.read_pod_logs(
        target, TimeWindow.default(), allowlist=["987654321098"]
    )

    assert "987654321098" in outcome.records[0]["message"]


def test_read_all_aggregates_redaction_counts(monkeypatch, tmp_path):
    _fixture_pod(tmp_path, "enu", "pod-a", [f"{KUBELET_TS} INFO uid 123456789012"])
    _fixture_pod(tmp_path, "enu", "pod-b", [f"{KUBELET_TS} INFO uid 210987654321"])
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))

    targets = [
        PodTarget(namespace="enu", pod_name="pod-a", container="app"),
        PodTarget(namespace="enu", pod_name="pod-b", container="app"),
    ]
    outcome = retrieval.read_all(targets, TimeWindow.default())

    assert outcome.redaction_counts["AADHAAR"] == 2
