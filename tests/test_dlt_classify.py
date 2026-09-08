"""
Phase 2 of DLT_PLAN.md -- failure classification and the BusinessException registry.

The load-bearing assertion is `test_unrecognised_exception_is_unknown_never_b`.
Defaulting an unrecognised exception to B would route genuinely novel failures
into the cheapest lane -- enriched, grouped, and never looked at again -- which
is exactly how a new class of failure goes unnoticed for months.

Every registry failure mode must be a *miss*, never a raise: a registry problem
should lower the confidence of a finding, not cost us the message.
"""
import json
from pathlib import Path

import pytest

from src.dlt import registry
from src.dlt.classify import (
    DEFAULT_CLASS_MAP,
    Classification,
    FailureClass,
    class_map,
    classify,
    extract_business_code,
    is_business_exception,
)
from src.dlt.headers import parse_headers
from src.dlt.stacktrace import parse_stacktrace

FIXTURES = Path(__file__).parent / "fixtures" / "dlt"
REFERENCE = FIXTURES / "reference_business_exception.json"
REGISTRY_CSV = FIXTURES / "business_errors.csv"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("DLT_CLASS_MAP", "DLT_BUSINESS_EXCEPTIONS", "DLT_REGISTRY_PATH",
                "DLT_APP_PACKAGES", "DLT_BOILERPLATE_FRAMES"):
        monkeypatch.delenv(var, raising=False)
    registry.clear_cache()
    yield
    registry.clear_cache()


@pytest.fixture
def reference_headers():
    return parse_headers(json.loads(REFERENCE.read_text(encoding="utf-8"))["headers"])


def trace_of(root_fqcn, message="something failed"):
    return parse_stacktrace(
        "org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed"
        "\n\tat org.springframework.kafka.listener.KafkaMessageListenerContainer.run(K.java:1)"
        f"\nCaused by: {root_fqcn}: {message}"
        "\n\tat com.uidai.enu.biometric.service.impl.Svc.method(Svc.java:10)"
        "\n\t... 14 more\n"
    )


# ======================================================================
# The reference sample
# ======================================================================

def test_reference_sample_is_class_a_with_its_code(reference_headers):
    result = classify(parse_stacktrace(reference_headers.stacktrace),
                      reference_headers.exception_message)
    assert result.failure_class is FailureClass.BUSINESS
    assert result.business_code == "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"
    assert result.root_fqcn == "in.gov.uidai.common.exception.BusinessException"
    assert result.needs_llm is True


# ======================================================================
# Class assignment
# ======================================================================

@pytest.mark.parametrize("fqcn", [
    "java.lang.NullPointerException",
    "java.lang.ClassCastException",
    "java.lang.NumberFormatException",
    "java.util.NoSuchElementException",
])
def test_code_defects_are_class_b(fqcn):
    result = classify(trace_of(fqcn))
    assert result.failure_class is FailureClass.CODE_DEFECT
    assert result.needs_llm is False, "no source access, so no diagnosis to pay for"


@pytest.mark.parametrize("fqcn", [
    "java.net.SocketTimeoutException",
    "java.net.ConnectException",
    "java.util.concurrent.TimeoutException",
    "org.springframework.jdbc.CannotGetJdbcConnectionException",
    "feign.RetryableException",
])
def test_technical_faults_are_class_c(fqcn):
    result = classify(trace_of(fqcn))
    assert result.failure_class is FailureClass.TECHNICAL
    assert result.needs_llm is False


def test_unrecognised_exception_is_unknown_never_b():
    """Silently defaulting to B stops us looking at genuinely novel failures."""
    result = classify(trace_of("com.example.SomethingNobodyHasSeen"))
    assert result.failure_class is FailureClass.UNKNOWN
    assert result.failure_class is not FailureClass.CODE_DEFECT
    assert "DLT_CLASS_MAP" in result.reason


def test_absent_stacktrace_is_unknown():
    result = classify(parse_stacktrace(None))
    assert result.failure_class is FailureClass.UNKNOWN
    assert result.business_code is None


def test_prefix_match_falls_back_to_the_package():
    """javax.net.ssl.* is mapped by prefix, not by exact FQCN."""
    result = classify(trace_of("javax.net.ssl.SSLHandshakeException"))
    assert result.failure_class is FailureClass.TECHNICAL


def test_longest_prefix_wins(monkeypatch):
    monkeypatch.setenv("DLT_CLASS_MAP", json.dumps({
        "com.acme": "C",
        "com.acme.specific.Boom": "B",
    }))
    assert classify(trace_of("com.acme.other.Thing")).failure_class is FailureClass.TECHNICAL
    assert classify(trace_of("com.acme.specific.Boom")).failure_class is FailureClass.CODE_DEFECT


# ======================================================================
# Business exception detection and code extraction
# ======================================================================

def test_business_exception_recognised_by_suffix_in_any_package():
    assert is_business_exception("com.other.team.BusinessException")
    assert is_business_exception("in.gov.uidai.common.exception.BusinessException")
    assert not is_business_exception("java.lang.NullPointerException")
    assert not is_business_exception(None)


def test_business_exception_without_a_code_is_still_class_a():
    """It cannot be looked up, but it is still a business failure. Phase 8
    caps its confidence via DLT_REGISTRY_MISS_CEILING."""
    result = classify(trace_of("in.gov.uidai.common.exception.BusinessException",
                               "no enumerated code here"))
    assert result.failure_class is FailureClass.BUSINESS
    assert result.business_code is None
    assert "no enumerated code" in result.reason


def test_code_extraction_variants():
    assert extract_business_code("[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] detail") == \
        "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"
    assert extract_business_code("prefix text then [LATE_CODE_HERE] after") == "LATE_CODE_HERE"
    assert extract_business_code("no brackets at all") is None
    assert extract_business_code("") is None
    assert extract_business_code(None) is None


def test_code_extraction_ignores_incidental_brackets():
    """`[0]` or `[ok]` in free text must not be mistaken for an error code."""
    assert extract_business_code("index [0] failed") is None
    assert extract_business_code("status [ok]") is None
    assert extract_business_code("retry [ab]") is None


def test_code_extraction_prefers_the_first_source():
    assert extract_business_code("[FROM_TRACE] x", "[FROM_HEADER] y") == "FROM_TRACE"
    assert extract_business_code(None, "[FROM_HEADER] y") == "FROM_HEADER"


def test_code_is_recovered_from_the_header_when_the_trace_is_unusable():
    """Spring concatenates the root business error into kafka_exception-message,
    so a truncated stacktrace need not lose the code."""
    result = classify(parse_stacktrace(None),
                      "Listener failed; BusinessException: [RECOVERED_CODE] detail")
    assert result.failure_class is FailureClass.BUSINESS
    assert result.business_code == "RECOVERED_CODE"
    assert "recovered from" in result.reason


def test_business_exceptions_are_configurable(monkeypatch):
    monkeypatch.setenv("DLT_BUSINESS_EXCEPTIONS", "com.acme.DomainFault")
    result = classify(trace_of("com.acme.DomainFault", "[ACME_CODE] boom"))
    assert result.failure_class is FailureClass.BUSINESS
    assert result.business_code == "ACME_CODE"


# ======================================================================
# Class map configuration
# ======================================================================

def test_class_map_extends_rather_than_replaces(monkeypatch):
    monkeypatch.setenv("DLT_CLASS_MAP", json.dumps({"com.acme.Boom": "B"}))
    merged = class_map()
    assert merged["com.acme.Boom"] == "B"
    assert merged["java.lang.NullPointerException"] == "B", \
        "adding one entry must not drop the built-ins"
    assert len(merged) == len(DEFAULT_CLASS_MAP) + 1


def test_malformed_class_map_is_ignored(monkeypatch):
    monkeypatch.setenv("DLT_CLASS_MAP", "{not json")
    assert class_map() == DEFAULT_CLASS_MAP


def test_class_map_with_unknown_class_letter_is_ignored(monkeypatch):
    monkeypatch.setenv("DLT_CLASS_MAP", json.dumps({"com.acme.Boom": "Z"}))
    assert "com.acme.Boom" not in class_map()


def test_class_map_that_is_not_an_object_is_ignored(monkeypatch):
    monkeypatch.setenv("DLT_CLASS_MAP", json.dumps(["a", "b"]))
    assert class_map() == DEFAULT_CLASS_MAP


# ======================================================================
# Registry
# ======================================================================

def test_lookup_resolves_a_known_code(monkeypatch):
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(REGISTRY_CSV))
    assert registry.lookup("UID_ORIGIN_TRACKER_DATA_NOT_FOUND") == \
        "UidOriginTracker record absent for the requested update"


def test_unknown_code_misses(monkeypatch):
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(REGISTRY_CSV))
    assert registry.lookup("NO_SUCH_CODE") is None


def test_lookup_trims_but_stays_case_exact(monkeypatch):
    """These are enumerated constants. Case-insensitive matching would hide a
    real mismatch between the registry and the deployed code."""
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(REGISTRY_CSV))
    assert registry.lookup("  UID_ORIGIN_TRACKER_DATA_NOT_FOUND  ") is not None
    assert registry.lookup("uid_origin_tracker_data_not_found") is None


def test_missing_registry_file_misses_without_raising(monkeypatch, tmp_path):
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(tmp_path / "absent.csv"))
    assert registry.load_registry() == {}
    assert registry.lookup("ANYTHING") is None


def test_empty_registry_file_misses(monkeypatch, tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    assert registry.load_registry() == {}


def test_registry_without_a_header_is_read_positionally(monkeypatch, tmp_path):
    path = tmp_path / "headerless.csv"
    path.write_text("SOME_CODE,its description\nOTHER_CODE,another\n", encoding="utf-8")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    assert registry.lookup("SOME_CODE") == "its description"
    assert registry.lookup("OTHER_CODE") == "another"


def test_registry_accepts_alternative_column_names(monkeypatch, tmp_path):
    path = tmp_path / "alt.csv"
    path.write_text("reason_code,message\nALT_CODE,alt description\n", encoding="utf-8")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    assert registry.lookup("ALT_CODE") == "alt description"


def test_registry_with_only_codes_yields_empty_descriptions(monkeypatch, tmp_path):
    path = tmp_path / "codes_only.csv"
    path.write_text("code\nLONELY_CODE\n", encoding="utf-8")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    assert registry.lookup("LONELY_CODE") == ""


def test_registry_reloads_when_the_file_changes(monkeypatch, tmp_path):
    """An operator drops in an updated registry without restarting consumers."""
    path = tmp_path / "reg.csv"
    path.write_text("code,description\nX_CODE,first\n", encoding="utf-8")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    assert registry.lookup("X_CODE") == "first"

    path.write_text("code,description\nX_CODE,second\nY_CODE,new\n", encoding="utf-8")
    import os
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    assert registry.lookup("X_CODE") == "second"
    assert registry.lookup("Y_CODE") == "new"


def test_relative_registry_path_resolves_against_the_repo_root(monkeypatch):
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    assert registry.lookup("DEMO_DATA_MISMATCH") is not None


# ======================================================================
# Contract
# ======================================================================

def test_classify_is_total_and_never_raises():
    for text in (None, "", "garbage", "a: b", "\n\n", "Caused by: nothing"):
        assert isinstance(classify(parse_stacktrace(text)), Classification)
