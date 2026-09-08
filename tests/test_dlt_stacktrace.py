"""
Phase 1 of DLT_PLAN.md -- header extraction, stacktrace parsing, fingerprinting.

The two tests that matter most are the trap regressions from DLT_PLAN.md 3.2,
because both failure modes are silent:

* `test_root_is_business_exception_not_the_advertised_wrapper` -- the header
  `kafka_exception-cause-fqcn` says `java.lang.RuntimeException`. Fingerprinting
  on that value collapses every failure in every Spring Kafka consumer in the
  organisation into a single group, and one wrong recommendation is then served
  to all of them.

* `test_backoff_timestamp_is_43_hours_after_the_original` -- anchoring a log
  window on the wrong timestamp means every fetch searches a window 43 hours
  stale and returns nothing, with no error anywhere.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.dlt import stacktrace as S
from src.dlt.headers import DltHeaders, decode_epoch_ms, parse_headers

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    for var in ("DLT_APP_PACKAGES", "DLT_FINGERPRINT_FRAMES", "DLT_BOILERPLATE_FRAMES"):
        monkeypatch.delenv(var, raising=False)
    yield


@pytest.fixture
def raw_headers():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]


@pytest.fixture
def headers(raw_headers):
    return parse_headers(raw_headers)


@pytest.fixture
def trace(headers):
    return S.parse_stacktrace(headers.stacktrace)


def make_trace(root_fqcn="java.lang.NullPointerException", root_message="boom",
               frames=("com.uidai.svc.Foo.bar(Foo.java:10)",)):
    frame_text = "".join(f"\n\tat {f}" for f in frames)
    return (
        "org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed"
        "\n\tat org.springframework.kafka.listener.KafkaMessageListenerContainer.run(K.java:1)"
        f"\nCaused by: {root_fqcn}: {root_message}{frame_text}"
        "\n\t... 14 more\n"
    )


# ======================================================================
# Trap 1 -- the advertised cause is a wrapper (DLT_PLAN.md 3.2)
# ======================================================================

def test_root_is_business_exception_not_the_advertised_wrapper(headers, trace):
    """The single most important assertion in this phase."""
    assert headers.exception_cause_fqcn == "java.lang.RuntimeException"
    assert trace.root.fqcn == "in.gov.uidai.common.exception.BusinessException"
    assert trace.root.fqcn != headers.exception_cause_fqcn
    assert trace.root.fqcn != headers.exception_fqcn


def test_reference_chain_has_five_links(trace):
    assert trace.depth == 5
    assert trace.chain[0].fqcn.endswith("ListenerExecutionFailedException")
    assert trace.chain[-1].fqcn.endswith("BusinessException")


def test_reference_trace_is_not_truncated(trace):
    assert trace.truncated is False
    assert trace.root.elided == 43


def test_root_message_carries_the_business_code(trace):
    assert trace.root.message.startswith("[UID_ORIGIN_TRACKER_DATA_NOT_FOUND]")


def test_simple_name(trace):
    assert trace.root.simple_name == "BusinessException"


# ======================================================================
# Trap 2 -- hex epoch timestamps and the log-window anchor
# ======================================================================

def test_retry_original_timestamp_decodes_to_the_kafka_original(raw_headers, headers):
    """The hex decoding is proven by these two agreeing exactly."""
    assert int("01A009712548", 16) == 1786864805192
    assert headers.retry_original_timestamp_ms == headers.original_timestamp_ms == 1786864805192
    assert raw_headers["kafka_original-timestamp"] == "1786864805192"


def test_backoff_timestamp_decodes_to_the_exception_instant(headers):
    """Matches the TimestampedException embedded in the trace, to the ms."""
    assert headers.backoff_timestamp_ms == 1787019608511
    moment = datetime.fromtimestamp(headers.backoff_timestamp_ms / 1000, tz=timezone.utc)
    assert moment.isoformat() == "2026-08-18T02:20:08.511000+00:00"


def test_backoff_timestamp_is_43_hours_after_the_original(headers):
    """Anchoring on the wrong one searches a window 43 hours stale."""
    gap_hours = (headers.backoff_timestamp_ms - headers.original_timestamp_ms) / 3_600_000
    assert round(gap_hours, 1) == 43.0
    assert headers.last_attempt_ms == headers.backoff_timestamp_ms
    assert headers.anchor_is_fallback is False


def test_last_attempt_falls_back_to_original_when_backoff_absent():
    headers = parse_headers({"kafka_original-timestamp": "1786864805192"})
    assert headers.last_attempt_ms == 1786864805192
    assert headers.anchor_is_fallback is True


def test_decimal_epoch_is_preferred_over_its_hex_reading():
    """'1786864805192' is valid hex too, but decodes to a year-58000 date."""
    assert decode_epoch_ms("1786864805192") == 1786864805192


def test_implausible_and_malformed_timestamps_return_none():
    assert decode_epoch_ms("0") is None
    assert decode_epoch_ms("zzz") is None
    assert decode_epoch_ms("") is None
    assert decode_epoch_ms(None) is None


# ======================================================================
# Header extraction
# ======================================================================

def test_all_structural_headers_extract(headers):
    assert headers.original_topic == "ENU.UPDATE.CHECKER.COMPLETION.V1"
    assert headers.original_partition == 63
    assert headers.original_offset == 3352
    assert headers.consumer_group == "enu-biodedup-cg"
    assert headers.attempts == 5
    assert headers.type_id == "com.uidai.enu.common.model.EventMessage"


def test_raw_headers_are_retained(headers):
    assert headers.raw["event-source"] == "scanner"


def test_empty_headers_do_not_raise():
    headers = parse_headers({})
    assert headers == DltHeaders(raw={})
    assert headers.last_attempt_ms is None
    assert parse_headers(None).original_topic is None


def test_non_numeric_partition_degrades_to_none():
    assert parse_headers({"kafka_original-partition": "not-a-number"}).original_partition is None


# ======================================================================
# Frame normalisation
# ======================================================================

def test_framework_and_jdk_frames_are_dropped(trace):
    normalised = S.normalise_frames(trace.root_frames)
    assert normalised
    for frame in normalised:
        assert not frame.startswith("org.springframework.")
        assert not frame.startswith("java.")
        assert not frame.startswith("jdk.")


def test_every_synthetic_form_in_the_real_sample_is_dropped(headers):
    """GeneratedMethodAccessor781, $$SpringCGLIB$$0 and <generated> all appear
    in the reference trace; each carries a counter that varies between runs."""
    assert "GeneratedMethodAccessor781" in headers.stacktrace
    assert "$$SpringCGLIB$$0" in headers.stacktrace
    assert "<generated>" in headers.stacktrace

    all_frames = [f for link in S.parse_stacktrace(headers.stacktrace).chain
                  for f in link.frames]
    normalised = S.normalise_frames(all_frames)
    joined = "\n".join(normalised)
    assert "GeneratedMethodAccessor" not in joined
    assert "SpringCGLIB" not in joined
    assert "<generated>" not in joined


def test_module_prefix_is_stripped():
    link = S._parse_link("x\n\tat java.base/java.lang.Thread.run(Thread.java:840)")
    assert link.frames == ("java.lang.Thread.run",)


def test_line_numbers_never_survive_parsing(trace):
    for frame in trace.root_frames:
        assert ".java:" not in frame
        assert not frame.endswith(")")


def test_application_lambda_methods_survive(trace):
    """`lambda$executeConsumption$0` is real application code with a
    compile-stable index -- only the JVM's `$$Lambda$1234/0x...` runtime class
    is unstable."""
    normalised = S.normalise_frames(trace.root_frames)
    assert any("lambda$executeConsumption$0" in f for f in normalised)


def test_runtime_generated_lambda_classes_are_dropped():
    assert S.is_synthetic("com.uidai.Foo$$Lambda$14/0x00007f.invoke")
    assert not S.is_synthetic("com.uidai.Foo.lambda$doWork$0")


def test_app_packages_are_configurable(monkeypatch, trace):
    monkeypatch.setenv("DLT_APP_PACKAGES", "in.gov.uidai.")
    monkeypatch.setenv("DLT_BOILERPLATE_FRAMES", "")
    normalised = S.normalise_frames(trace.root_frames)
    assert all(f.startswith("in.gov.uidai.") for f in normalised)


# ======================================================================
# Boilerplate frames -- found by running Phase 1 on the real sample
# ======================================================================

def test_exception_factory_frame_is_dropped_by_default(trace):
    """Every BusinessException is built by CommonErrorFactory, so the frame is
    constant across all Class A failures: no discriminating power, and it
    displaces the frame that actually names the failure site."""
    raw = trace.root_frames
    assert any("CommonErrorFactory.instantiateException" in f for f in raw)
    normalised = S.normalise_frames(raw)
    assert not any("CommonErrorFactory" in f for f in normalised)
    assert normalised[0].endswith("BioDataBaseHelperServiceImpl.getUidOriginTrackerData")


def test_boilerplate_filter_can_be_disabled(monkeypatch, trace):
    monkeypatch.setenv("DLT_BOILERPLATE_FRAMES", "")
    normalised = S.normalise_frames(trace.root_frames)
    assert any("CommonErrorFactory" in f for f in normalised)


# ======================================================================
# Fingerprinting
# ======================================================================

def _fingerprint_of(text, code=""):
    parsed = S.parse_stacktrace(text)
    return S.compute_fingerprint(parsed.root.fqcn,
                                 S.normalise_frames(parsed.root_frames), code)


def test_line_number_changes_do_not_change_the_fingerprint():
    """The whole caching model rests on this. BioDeDuplicationServiceImpl is a
    4,000+ line class whose numbers shift on every release."""
    a = make_trace(frames=("com.uidai.svc.Foo.bar(Foo.java:10)",))
    b = make_trace(frames=("com.uidai.svc.Foo.bar(Foo.java:4067)",))
    assert _fingerprint_of(a) == _fingerprint_of(b)


def test_different_root_exceptions_fingerprint_differently():
    a = make_trace(root_fqcn="java.lang.NullPointerException")
    b = make_trace(root_fqcn="java.net.SocketTimeoutException")
    assert _fingerprint_of(a) != _fingerprint_of(b)


def test_different_business_codes_fingerprint_differently():
    text = make_trace(root_fqcn="in.gov.uidai.common.exception.BusinessException")
    assert _fingerprint_of(text, "CODE_A") != _fingerprint_of(text, "CODE_B")


def test_different_frames_fingerprint_differently():
    a = make_trace(frames=("com.uidai.svc.Foo.bar(Foo.java:10)",))
    b = make_trace(frames=("com.uidai.svc.Other.baz(Other.java:10)",))
    assert _fingerprint_of(a) != _fingerprint_of(b)


def test_fingerprint_is_stable_across_processes(headers):
    """sha256 over UTF-8, never Python's randomised hash()."""
    import subprocess
    import sys

    script = (
        "import json,sys;"
        "sys.path.insert(0,'.');"
        "from src.dlt import headers as H, stacktrace as S;"
        "d=json.load(open('tests/fixtures/dlt/reference_business_exception.json'));"
        "h=H.parse_headers(d['headers']);t=S.parse_stacktrace(h.stacktrace);"
        "print(S.compute_fingerprint(t.root.fqcn,S.normalise_frames(t.root_frames),'C'))"
    )
    runs = {
        subprocess.run([sys.executable, "-c", script], capture_output=True,
                       text=True, check=True).stdout.strip()
        for _ in range(2)
    }
    assert len(runs) == 1
    parsed = S.parse_stacktrace(headers.stacktrace)
    assert runs.pop() == S.compute_fingerprint(
        parsed.root.fqcn, S.normalise_frames(parsed.root_frames), "C")


def test_fingerprint_honours_the_frame_limit(monkeypatch):
    """Two traces sharing their top frames but diverging deeper collapse
    together once the limit cuts below the divergence."""
    shared = ("com.uidai.svc.A.one(A.java:1)", "com.uidai.svc.B.two(B.java:2)")
    a = make_trace(frames=shared + ("com.uidai.svc.C.three(C.java:3)",))
    b = make_trace(frames=shared + ("com.uidai.svc.D.four(D.java:4)",))

    monkeypatch.setenv("DLT_FINGERPRINT_FRAMES", "5")
    assert _fingerprint_of(a) != _fingerprint_of(b)

    monkeypatch.setenv("DLT_FINGERPRINT_FRAMES", "2")
    assert _fingerprint_of(a) == _fingerprint_of(b)


# ======================================================================
# Signature
# ======================================================================

def test_signature_names_the_failure_site_not_the_factory(trace):
    normalised = S.normalise_frames(trace.root_frames)
    signature = S.build_signature(trace.root.fqcn, normalised,
                                  "UID_ORIGIN_TRACKER_DATA_NOT_FOUND")
    assert signature == (
        "BusinessException[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] @ "
        "BioDataBaseHelperServiceImpl.getUidOriginTrackerData"
    )


def test_signature_without_code_or_frames():
    assert S.build_signature("java.lang.NullPointerException", ()) == "NullPointerException"
    assert S.build_signature(None, ()) == "UnknownException"


# ======================================================================
# Degradation -- nothing here may raise
# ======================================================================

def test_absent_stacktrace_yields_a_truncated_empty_trace():
    for value in (None, "", "   "):
        parsed = S.parse_stacktrace(value)
        assert parsed.chain == ()
        assert parsed.truncated is True
        assert parsed.root is None
        assert parsed.root_frames == ()


def test_trace_cut_before_any_frame_is_flagged_truncated():
    parsed = S.parse_stacktrace(
        "org.springframework.SomeException: outer\n\tat com.uidai.A.b(A.java:1)"
        "\nCaused by: in.gov.uidai.common.exception.BusinessException: [CODE] cut here"
    )
    assert parsed.truncated is True
    assert parsed.root.fqcn == "in.gov.uidai.common.exception.BusinessException"


def test_header_only_trace_with_no_frames_does_not_raise():
    parsed = S.parse_stacktrace("java.lang.NullPointerException")
    assert parsed.root.fqcn == "java.lang.NullPointerException"
    assert parsed.root.message == ""
    assert parsed.truncated is True


def test_multiline_exception_message_is_captured():
    parsed = S.parse_stacktrace(
        "java.lang.IllegalStateException: line one\nline two\n\tat com.uidai.A.b(A.java:1)"
    )
    assert parsed.root.fqcn == "java.lang.IllegalStateException"
    assert "line two" in parsed.root.message


def test_message_containing_a_colon_splits_on_the_first_one():
    parsed = S.parse_stacktrace(
        "java.lang.RuntimeException: in.gov.uidai.common.exception.BusinessException: [C] x"
        "\n\tat com.uidai.A.b(A.java:1)"
    )
    assert parsed.root.fqcn == "java.lang.RuntimeException"
    assert parsed.root.message.startswith("in.gov.uidai.common.exception.BusinessException:")


def test_garbage_input_does_not_raise():
    parsed = S.parse_stacktrace("!!! not a stacktrace at all !!!")
    assert parsed.truncated is True
    assert parsed.root.fqcn == ""


def test_fingerprint_of_an_empty_trace_does_not_raise():
    assert len(S.compute_fingerprint(None, (), "")) == 64


# ======================================================================
# Phase C2 -- frame locations, kept beside the fingerprint and out of it
# ======================================================================

#: The reference sample's fingerprint, computed before C2 existed. C2 adds a
#: line number to the failure record for the first time; if one ever reaches
#: `compute_fingerprint`, every group in the store fragments and every cached
#: recommendation is orphaned. That is Risk R3, realised in one line of code,
#: and this literal is the guard against it.
REFERENCE_FINGERPRINT = "2bc62d82761a7f19593aea92c2eef08e51bf481bef517aa8883eef61b51ee57b"


def test_the_reference_fingerprint_is_unchanged_by_c2(headers):
    """Byte-identical to its pre-C2 value. Do not update this literal to make
    a failing test pass -- a change here means live groups just fragmented."""
    parsed = S.parse_stacktrace(headers.stacktrace)
    frames = S.normalise_frames(parsed.root_frames)

    assert S.compute_fingerprint(parsed.root.fqcn, frames,
                                 "UID_ORIGIN_TRACKER_DATA_NOT_FOUND") == REFERENCE_FINGERPRINT


def test_locations_are_parsed_from_the_reference_trace(headers):
    parsed = S.parse_stacktrace(headers.stacktrace)
    locations = S.normalise_frame_locations(parsed.root_frames, parsed.root_locations)

    top = locations[0]
    assert top.target.endswith("BioDataBaseHelperServiceImpl.getUidOriginTrackerData")
    assert top.file == "BioDataBaseHelperServiceImpl.java"
    assert top.line == 257

    assert locations[1].file == "BioDeDuplicationServiceImpl.java"
    assert locations[1].line == 4067


def test_locations_are_index_parallel_with_the_normalised_frames(headers):
    """`locations[0]` must describe the frame `build_signature` names."""
    parsed = S.parse_stacktrace(headers.stacktrace)
    frames = S.normalise_frames(parsed.root_frames)
    locations = S.normalise_frame_locations(parsed.root_frames, parsed.root_locations)

    assert len(frames) == len(locations)
    assert [location.target for location in locations] == list(frames)


def test_the_boilerplate_filter_applies_to_locations_too(headers):
    """CommonErrorFactory is dropped from frames; it must not survive here."""
    parsed = S.parse_stacktrace(headers.stacktrace)
    locations = S.normalise_frame_locations(parsed.root_frames, parsed.root_locations)

    assert not any("CommonErrorFactory" in location.target for location in locations)


# -- alignment ---------------------------------------------------------------

def test_a_frame_without_a_location_keeps_its_place(headers):
    """A bare frame must hold a None slot, not be skipped.

    Skipping it would shift every location after it onto the wrong frame --
    silently, and only for traces that mix the two forms.
    """
    trace = (
        "java.lang.IllegalStateException: boom\n"
        "\tat com.uidai.enu.biometric.A.first\n"
        "\tat com.uidai.enu.biometric.B.second(B.java:42)\n"
    )
    locations = S.normalise_frame_locations(*_root_of(trace))

    assert [location.target.rsplit(".", 1)[-1] for location in locations] == ["first", "second"]
    assert locations[0].file is None and locations[0].line is None
    assert locations[1].file == "B.java" and locations[1].line == 42


def _root_of(text):
    parsed = S.parse_stacktrace(text)
    return parsed.root_frames, parsed.root_locations


@pytest.mark.parametrize("location,expected", [
    ("Foo.java:42", ("Foo.java", 42)),
    ("Foo.java", ("Foo.java", None)),
    ("Native Method", ("Native Method", None)),
    ("Unknown Source", ("Unknown Source", None)),
    ("", (None, None)),
    (None, (None, None)),
])
def test_a_location_string_splits_into_file_and_line(location, expected):
    assert S.split_location(location) == expected


def test_a_non_numeric_suffix_is_not_read_as_a_line_number():
    """Inventing a line number is worse than reporting none."""
    assert S.split_location("Foo.java:abc") == ("Foo.java:abc", None)


# -- source paths ------------------------------------------------------------

def test_a_frame_maps_to_its_repository_path_suffix():
    location = S.FrameLocation(
        target="com.uidai.enu.biometric.service.impl.BioDeDuplicationServiceImpl.prepare",
        file="BioDeDuplicationServiceImpl.java", line=4016)

    assert location.package == "com.uidai.enu.biometric.service.impl"
    assert location.source_path_suffix == (
        "com/uidai/enu/biometric/service/impl/BioDeDuplicationServiceImpl.java")


def test_an_inner_class_maps_to_the_outer_file_the_jvm_reported():
    """`com.foo.Outer$Inner.run` lives in Outer.java -- the file that exists.

    Deriving the name from the class instead would ask the repository for
    `Outer$Inner.java`, which never exists.
    """
    location = S.FrameLocation(target="com.uidai.enu.biometric.Outer$Inner.run",
                               file="Outer.java", line=12)

    assert location.class_fqcn == "com.uidai.enu.biometric.Outer$Inner"
    assert location.source_path_suffix == "com/uidai/enu/biometric/Outer.java"


def test_a_frame_with_no_file_falls_back_to_the_outer_class_name():
    location = S.FrameLocation(target="com.uidai.enu.biometric.Outer$Inner.run")

    assert location.source_path_suffix == "com/uidai/enu/biometric/Outer.java"


def test_a_lambda_frame_resolves_to_its_declaring_class():
    location = S.FrameLocation(
        target="com.uidai.enu.biometric.messagingQueue.consumer."
               "AbstractBaseConsumer.lambda$executeConsumption$0",
        file="AbstractBaseConsumer.java", line=29)

    assert location.class_fqcn.endswith("AbstractBaseConsumer")
    assert location.source_path_suffix.endswith(
        "com/uidai/enu/biometric/messagingQueue/consumer/AbstractBaseConsumer.java")


def test_a_location_serialises_to_json_safe_primitives():
    location = S.FrameLocation(target="com.uidai.A.b", file="A.java", line=7)

    assert location.as_dict() == {"target": "com.uidai.A.b", "file": "A.java", "line": 7}
