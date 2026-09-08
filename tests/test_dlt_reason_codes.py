"""
The reason-code catalog: parsing `BusinessReasonCode`, storing it, and using
it to classify.

The property that justifies all of this: **`BusinessReasonCode implements
IRejectCode`, so any of the 760 published codes can arrive inside a
`BusinessException` -- and 198 of them are declared `TECHNICAL_EXCEPTION`.**
On the exception type alone, `BusinessException: [KAFKA_PRODUCER_EXCEPTION]`
and `BusinessException: [INDEX_MASTER_DATA_NOT_FOUND]` are indistinguishable.
Both classify as A, both go to the expensive lane, and the first comes back
with a business narrative for what is a Kafka publish error whose whole
treatment is "redrive once the broker recovers".

Three things are load-bearing and tested here:

* **A declared category and an inferred one are never conflated.** The enum
  states its category; the id-keyed map does not, so anything assigned there
  is inference from a numeric id range. `category_source` records which, and
  a finding built on an inference says so.

* **Section context does not cross the block boundary.** `// Bio Fraud Stage`
  sits 340 lines above the first `tempMap.put`, in a different structure.
  Letting it leak forward files ten bio-fraud codes under a stage they have
  nothing to do with.

* **An absent or partial catalog is a no-op**, never a failure. A catalog
  problem must degrade a finding's confidence, not cost us the message.
"""
from pathlib import Path

import pytest

from src.dlt import registry
from src.dlt.canned import build as build_canned
from src.dlt.classify import FailureClass, classify
from src.dlt.corroborate import Corroboration, Verdict
from src.dlt.stacktrace import parse_stacktrace
from src.tools.parse_reason_codes import (
    BUSINESS_EXCEPTION,
    BUSINESS_VALIDATION_ERROR,
    DECLARED,
    INFERRED,
    TECHNICAL_EXCEPTION,
    parse,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

#: The Java source is optional: it is the *input* to the catalog, not a runtime
#: dependency, and a deployment may reasonably keep only the generated CSV. The
#: parser tests skip without it; the catalog tests do not, because the catalog
#: is what the running system actually reads.
SOURCE = REPO_ROOT / "reason_codes.txt"
CATALOG = REPO_ROOT / "reason_codes.csv"

needs_source = pytest.mark.skipif(not SOURCE.exists(),
                                  reason="reason_codes.txt not present")
needs_catalog = pytest.mark.skipif(not CATALOG.exists(),
                                   reason="reason_codes.csv not generated")


def trace_with(code, fqcn="in.gov.uidai.common.exception.BusinessException"):
    return parse_stacktrace(
        "org.springframework.X: outer\n\tat org.springframework.A.b(A.java:1)"
        f"\nCaused by: {fqcn}: [{code}] some detail"
        "\n\tat com.uidai.enu.biometric.Svc.go(Svc.java:1)\n\t... 3 more\n")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.delenv("DLT_REASON_CODE_CLASSIFY", raising=False)
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(CATALOG))
    registry.clear_cache()
    yield
    registry.clear_cache()


# ======================================================================
# The parser
# ======================================================================

@needs_source
def test_both_structures_are_parsed():
    entries, report = parse(SOURCE)

    assert report.enum_entries == 694
    assert report.map_entries == 69
    # 763 rows, 3 codes present in both structures.
    assert len(entries) == 760


@needs_source
def test_a_commented_out_entry_is_not_resurrected():
    """`// tempMap.put(17041, ...NON_TD)` was retired deliberately."""
    entries, report = parse(SOURCE)

    assert report.skipped_commented == 1
    assert not any(e.code == "RESIDENT_MAN_DEDUP_REJECT_NON_TD" for e in entries)


@needs_source
def test_section_context_does_not_cross_the_block_boundary():
    """The 23xx codes sit under no section of their own. Inheriting one from
    340 lines earlier, in the enum, would be a fabricated attribution."""
    entries, _ = parse(SOURCE)
    by_code = {e.code: e for e in entries}

    for code in ("PACKET_VALIDATION_FAIL", "MDD_TIME_LIMIT_EXCEEDED_NO_RESPONSE"):
        assert by_code[code].stage == ""
        assert by_code[code].section == ""
        assert by_code[code].numeric_id.startswith("23")


@needs_source
def test_a_declared_category_beats_an_inferred_one():
    """The 3 codes in both structures keep the enum's declared category."""
    entries, report = parse(SOURCE)
    by_code = {e.code: e for e in entries}

    assert len(report.corroborated) == 3
    for numeric_id, code, category in report.corroborated:
        assert by_code[code].category_source == DECLARED
        assert by_code[code].category == TECHNICAL_EXCEPTION
        # The map's id is still kept -- it is how the id-range rule is checked.
        assert by_code[code].numeric_id == numeric_id


@needs_source
def test_the_ungrouped_block_is_left_uncategorised():
    """Ten codes mix business rejects with technical failures under no header.
    A guess would be worse than nothing, because classify() acts on this."""
    entries, report = parse(SOURCE)
    by_code = {e.code: e for e in entries}

    assert len(report.uncategorised) == 10
    for code in report.uncategorised:
        assert by_code[code].category == ""
        assert by_code[code].failure_class == ""


@needs_source
def test_categories_map_onto_failure_classes():
    entries, _ = parse(SOURCE)
    by_code = {e.code: e for e in entries}

    assert by_code["INDEX_MASTER_DATA_NOT_FOUND"].category == BUSINESS_EXCEPTION
    assert by_code["INDEX_MASTER_DATA_NOT_FOUND"].failure_class == "A"

    assert by_code["AUTO_TIME_BOUND_REJECTION"].category == BUSINESS_VALIDATION_ERROR
    assert by_code["AUTO_TIME_BOUND_REJECTION"].failure_class == "A"

    assert by_code["KAFKA_PRODUCER_EXCEPTION"].category == TECHNICAL_EXCEPTION
    assert by_code["KAFKA_PRODUCER_EXCEPTION"].failure_class == "C"

    assert by_code["REDIS_GET_EXCEPTION"].category_source == INFERRED
    assert by_code["REDIS_GET_EXCEPTION"].failure_class == "C"


# ======================================================================
# The stored catalog
# ======================================================================

@needs_catalog
def test_the_catalog_loads():
    catalog = registry.load_catalog()

    assert len(catalog) == 760
    assert sum(1 for e in catalog.values() if e.failure_class) == 750


@needs_source
@needs_catalog
def test_the_catalog_is_in_sync_with_the_java_source():
    """Regenerating must be a no-op. If this fails, run
    `python -m src.tools.parse_reason_codes`."""
    entries, _ = parse(SOURCE)
    catalog = registry.load_catalog()

    assert len(entries) == len(catalog)
    for entry in entries:
        stored = catalog.get(entry.code)
        assert stored is not None, f"{entry.code} missing from the catalog"
        assert stored.description == entry.description
        assert stored.category == entry.category
        assert stored.failure_class == entry.failure_class


@needs_catalog
def test_lookup_and_class_for():
    assert registry.class_for("KAFKA_PRODUCER_EXCEPTION") == "C"
    assert registry.class_for("INDEX_MASTER_DATA_NOT_FOUND") == "A"
    # Uncategorised: no opinion, so the stacktrace decides.
    assert registry.class_for("PACKET_VALIDATION_FAIL") is None
    assert registry.class_for("NO_SUCH_CODE_ANYWHERE") is None
    assert registry.class_for(None) is None

    entry = registry.lookup_entry("REDIS_GET_EXCEPTION")
    assert entry.category_source == "inferred"
    assert entry.is_declared is False
    assert registry.lookup_entry("KAFKA_PRODUCER_EXCEPTION").is_declared is True


def test_a_two_column_catalog_still_loads(tmp_path, monkeypatch):
    """The pre-catalog format must keep working, carrying no category."""
    path = tmp_path / "old.csv"
    path.write_text("code,description\nSOME_CODE,its description\n")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    registry.clear_cache()

    assert registry.lookup("SOME_CODE") == "its description"
    assert registry.class_for("SOME_CODE") is None
    assert registry.load_registry() == {"SOME_CODE": "its description"}


def test_a_category_without_a_class_column_still_classifies(tmp_path, monkeypatch):
    path = tmp_path / "cat.csv"
    path.write_text("code,description,category\n"
                    "T_CODE,a technical thing,TECHNICAL_EXCEPTION\n"
                    "B_CODE,a business thing,BUSINESS_EXCEPTION\n")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    registry.clear_cache()

    assert registry.class_for("T_CODE") == "C"
    assert registry.class_for("B_CODE") == "A"


def test_a_catalog_cannot_assert_class_b(tmp_path, monkeypatch):
    """A code defect is identified by its exception type, never by a reject
    code. A data file must not be able to route cases into the B lane."""
    path = tmp_path / "b.csv"
    path.write_text("code,description,category,failure_class\n"
                    "WEIRD,x,TECHNICAL_EXCEPTION,B\n")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    registry.clear_cache()

    assert registry.class_for("WEIRD") == "C"   # falls back to the category


def test_a_nonsense_class_column_is_dropped(tmp_path, monkeypatch):
    path = tmp_path / "junk.csv"
    path.write_text("code,description,category,failure_class\n"
                    "WEIRD,x,,banana\n")
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(path))
    registry.clear_cache()

    assert registry.class_for("WEIRD") is None


def test_a_missing_catalog_is_a_miss_not_a_crash(tmp_path, monkeypatch):
    monkeypatch.setenv("DLT_REGISTRY_PATH", str(tmp_path / "nope.csv"))
    registry.clear_cache()

    assert registry.load_catalog() == {}
    assert registry.class_for("ANYTHING") is None
    assert registry.lookup("ANYTHING") is None


# ======================================================================
# Classification -- the point of all of it
# ======================================================================

def test_a_technical_code_in_a_business_exception_is_class_c():
    """The finding this whole catalog exists for."""
    result = classify(trace_with("KAFKA_PRODUCER_EXCEPTION"),
                      code_class=registry.class_for)

    assert result.failure_class is FailureClass.TECHNICAL
    assert result.business_code == "KAFKA_PRODUCER_EXCEPTION"
    assert "technical fault" in result.reason
    assert result.needs_llm is False


def test_a_business_code_in_a_business_exception_stays_class_a():
    result = classify(trace_with("INDEX_MASTER_DATA_NOT_FOUND"),
                      code_class=registry.class_for)

    assert result.failure_class is FailureClass.BUSINESS
    assert result.needs_llm is True


def test_an_uncategorised_code_leaves_the_trace_to_decide():
    result = classify(trace_with("PACKET_VALIDATION_FAIL"),
                      code_class=registry.class_for)
    assert result.failure_class is FailureClass.BUSINESS


def test_without_the_hook_behaviour_is_unchanged():
    """Every pre-catalog caller must classify exactly as it did before."""
    assert classify(trace_with("KAFKA_PRODUCER_EXCEPTION")).failure_class \
        is FailureClass.BUSINESS


def test_the_override_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("DLT_REASON_CODE_CLASSIFY", "false")
    result = classify(trace_with("KAFKA_PRODUCER_EXCEPTION"),
                      code_class=registry.class_for)
    assert result.failure_class is FailureClass.BUSINESS


def test_a_broken_lookup_degrades_rather_than_raising():
    def boom(code):
        raise RuntimeError("catalog on fire")

    result = classify(trace_with("KAFKA_PRODUCER_EXCEPTION"), code_class=boom)
    assert result.failure_class is FailureClass.BUSINESS


def test_the_header_fallback_path_also_consults_the_catalog():
    """Trace unusable, code recovered from kafka_exception-message."""
    empty = parse_stacktrace("")
    message = ("Listener failed; in.gov.uidai.common.exception.BusinessException: "
               "[KAFKA_PRODUCER_EXCEPTION] Exception in publishing Message")

    result = classify(empty, message, code_class=registry.class_for)
    assert result.failure_class is FailureClass.TECHNICAL
    assert result.business_code == "KAFKA_PRODUCER_EXCEPTION"


# ======================================================================
# What the operator reads
# ======================================================================

def _failure(code, category, source="declared"):
    return {"signature": f"BusinessException[{code}]", "business_code": code,
            "registry_category": category, "registry_category_source": source,
            "registry_description": "Exception in publishing Message"}


def test_a_reclassified_case_explains_itself():
    """A reader seeing BusinessException filed as technical will ask why."""
    finding = build_canned(
        "C", _failure("KAFKA_PRODUCER_EXCEPTION", "TECHNICAL_EXCEPTION"),
        Corroboration(verdict=Verdict.UNVERIFIABLE, reason="no logs"))

    assert "raised as a business exception" in finding.narrative
    assert "KAFKA_PRODUCER_EXCEPTION" in finding.narrative
    assert "declared as such in the BusinessReasonCode source" in finding.narrative
    assert finding.action == "REDRIVE_AFTER_RECOVERY"


def test_an_inferred_reclassification_is_flagged_as_provisional():
    finding = build_canned(
        "C", _failure("REDIS_GET_EXCEPTION", "TECHNICAL_EXCEPTION", "inferred"),
        Corroboration(verdict=Verdict.UNVERIFIABLE, reason="no logs"))

    assert "inferred from its numeric id range" in finding.narrative
    assert "provisional" in finding.narrative


def test_an_ordinary_technical_fault_gets_no_catalog_note():
    """A SocketTimeoutException was never a business exception; there is
    nothing to explain."""
    finding = build_canned(
        "C", {"signature": "java.net.SocketTimeoutException"},
        Corroboration(verdict=Verdict.UNVERIFIABLE, reason="no logs"))

    assert "raised as a business exception" not in finding.narrative
