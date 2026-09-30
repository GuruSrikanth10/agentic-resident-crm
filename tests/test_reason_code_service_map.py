"""
The reason-code -> service map: its format, how it places a rejection, and
how it is fetched from S3.

Every test builds its own registry, documentation store and map under
tmp_path, as test_service_registry.py does.
"""
import json
from pathlib import Path

import pytest

from src.models.audit_contract import to_message_payload
from src.utils import paths
from src.utils import reason_code_docs as rcd
from src.utils import reason_code_service_map as rcsm
from src.utils import service_registry as sr
from tests.test_service_registry import _pack, _payload, _write_docs, _write_packs

AUDIT_FIXTURES = Path(__file__).parent / "fixtures" / "audit"


def _map(**services):
    return {"schema_version": 1, "services": services}


def _write_map(path, document):
    text = document if isinstance(document, str) else json.dumps(document)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _fresh_caches():
    rcsm.reset()
    sr.reset()
    yield
    rcsm.reset()
    sr.reset()


@pytest.fixture
def estate(tmp_path, monkeypatch):
    """The registry and documentation store of test_service_registry, plus a
    QC pack placed by its DLT stage, and the map beside the store."""
    packs = _write_packs(
        tmp_path / "packs",
        _pack("enu-biometric", "bio", stages=["Biometric"],
              rule_source={"type": "rules_db"}),
        _pack("svc-demo", "demo", stages=["Demographic"]),
        _pack("enu-qc", "qc", stages=["QC"], sub_stages=["SMART_QC"]),
    )
    docs = _write_docs(tmp_path / "docs",
                       **{"enu-biometric": ["BIO_CODE"], "svc-demo": ["DEMO_CODE"]})
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", packs)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", docs)
    monkeypatch.setattr(paths, "REASON_CODE_SERVICE_MAP_FILE", None)
    return docs / rcsm.FILENAME


# ======================================================================
# The format
# ======================================================================

def test_a_valid_map_indexes_each_code_by_its_services():
    service_map, errors, warnings = rcsm.parse(json.dumps(_map(
        **{"enu-biometric": ["BIO_CODE", "SHARED"], "enu-qc": ["QC_CODE", "SHARED"],
           "enu-empty": []})))

    assert errors == []
    assert service_map.services_for("BIO_CODE") == ("enu-biometric",)
    assert service_map.services_for("SHARED") == ("enu-biometric", "enu-qc")
    assert service_map.services_for("UNLISTED") == ()
    assert service_map.services == ("enu-biometric", "enu-empty", "enu-qc")
    assert service_map.sha256.startswith("sha256:")
    assert any("SHARED is listed under" in warning for warning in warnings)


@pytest.mark.parametrize("text, expected", [
    ("{not json", "is not valid JSON"),
    ("[]", "holds a list, not an object"),
    (json.dumps({**_map(), "extra": 1}), "unknown key(s) ['extra']"),
    (json.dumps({"schema_version": 2, "services": {}}), "schema_version is 2"),
    (json.dumps({"schema_version": 1}), "'services' must be an object"),
    (json.dumps(_map(Enu_Bio=["X"])), "service name 'Enu_Bio' must match"),
    (json.dumps(_map(**{"enu-bio": "X"})), "must be a list of reason codes"),
    (json.dumps(_map(**{"enu-bio": [" X"]})), "' X', which is not a reason code"),
    (json.dumps(_map(**{"enu-bio": [7]})), "7, which is not a reason code"),
    ('{"schema_version": 1, "services": {"enu-bio": ["A"], "enu-bio": ["B"]}}',
     "the key 'enu-bio' appears twice"),
])
def test_a_malformed_map_is_rejected_whole(text, expected):
    service_map, errors, _warnings = rcsm.parse(text)

    assert service_map is None
    assert any(expected in error for error in errors), errors


def test_a_code_listed_twice_by_one_service_is_a_warning():
    service_map, errors, warnings = rcsm.parse(json.dumps(_map(**{"enu-bio": ["A", "A"]})))

    assert errors == []
    assert service_map.services_for("A") == ("enu-bio",)
    assert warnings == [f"{rcsm.FILENAME}: services.enu-bio lists A twice."]


# ======================================================================
# Placing a rejection
# ======================================================================

def test_the_map_places_a_rejection_the_interceptor_published(estate):
    _write_map(estate, _map(**{"enu-qc": ["RESIDENT_QC_POA_DOCUMENT_NOT_APPROVED"]}))
    audit = json.loads((AUDIT_FIXTURES / "rejection_sample.json").read_text())

    resolution = sr.resolve(to_message_payload(audit))

    assert resolution.detail["stage"] == "REJECTINTERCEPTOR"
    assert (resolution.service, resolution.source, resolution.matched) == \
        ("enu-qc", sr.SOURCE_REASON_CODE_MAP, "RESIDENT_QC_POA_DOCUMENT_NOT_APPROVED")
    assert resolution.conflict is None
    assert resolution.reason_code_map_sha256 == rcsm.current().sha256


def test_the_map_wins_over_the_stage_and_the_disagreement_is_kept(estate):
    _write_map(estate, _map(**{"svc-demo": ["DEMO_CODE"]}))

    resolution = sr.resolve(_payload(stage="Biometric", code="DEMO_CODE"))

    assert (resolution.service, resolution.source) == ("svc-demo", sr.SOURCE_REASON_CODE_MAP)
    assert resolution.conflict == {sr.SOURCE_FLOW_STAGE: "enu-biometric"}


def test_documentation_that_disagrees_with_the_map_is_kept(estate):
    _write_map(estate, _map(**{"svc-demo": ["BIO_CODE"]}))

    resolution = sr.resolve(_payload(code="BIO_CODE"))

    assert resolution.service == "svc-demo"
    assert resolution.conflict == {sr.SOURCE_REASON_CODE_DOCS: "enu-biometric"}


@pytest.mark.parametrize("services", [
    {"svc-demo": ["BIO_CODE"], "enu-qc": ["BIO_CODE"]},   # listed twice: decides nothing
    {"svc-demo": ["OTHER_CODE"]},                           # not listed
])
def test_a_code_the_map_does_not_decide_falls_through(estate, services):
    _write_map(estate, _map(**services))

    resolution = sr.resolve(_payload(stage="Biometric", code="BIO_CODE"))

    assert (resolution.service, resolution.source) == ("enu-biometric", sr.SOURCE_FLOW_STAGE)
    assert resolution.conflict is None


def test_without_a_map_packets_are_placed_as_before(estate):
    resolution = sr.resolve(_payload(stage="Unknown", code="DEMO_CODE"))

    assert (resolution.service, resolution.source) == ("svc-demo", sr.SOURCE_REASON_CODE_DOCS)
    assert resolution.reason_code_map_sha256 is None


def test_a_service_with_no_pack_is_placed_and_the_gate_reports_it(estate, monkeypatch):
    _write_map(estate, _map(**{"enu-packet-validator": ["PKT_CODE"]}))
    monkeypatch.setenv(sr.ENV_GATE, sr.GATE_ENFORCE)

    resolution = sr.resolve(_payload(code="PKT_CODE")).as_dict()
    decision = sr.gate(resolution)

    assert resolution["service"] == "enu-packet-validator"
    assert (decision.skip, decision.reason) == (True, sr.SKIP_NOT_REGISTERED)


def test_only_the_enabled_services_packets_get_through_an_enforcing_gate(estate, monkeypatch):
    _write_map(estate, _map(**{"enu-biometric": ["BIO_CODE"], "enu-qc": ["QC_CODE"]}))
    monkeypatch.setenv(sr.ENV_GATE, sr.GATE_ENFORCE)
    monkeypatch.setenv(sr.ENV_ENABLED, "enu-biometric")

    assert sr.gate(sr.resolve(_payload(code="BIO_CODE")).as_dict()).skip is False
    skipped = sr.gate(sr.resolve(_payload(code="QC_CODE")).as_dict())
    assert (skipped.skip, skipped.reason) == (True, sr.SKIP_NOT_ENABLED)


def test_the_dlt_lane_is_placed_by_its_stage_not_the_map(estate):
    """A dead-lettered record's edata.stage is the failing service's own."""
    _write_map(estate, _map(**{"svc-demo": ["UNHANDLED_EXCEPTION"]}))
    audit = json.loads((AUDIT_FIXTURES / "dlt_sample.json").read_text())

    resolution = sr.resolve_dlt(to_message_payload(audit))

    assert (resolution.service, resolution.source, resolution.matched) == \
        ("enu-qc", sr.SOURCE_FLOW_STAGE, "QC")
    assert resolution.reason_code_map_sha256 is None


# ======================================================================
# The file on disk
# ======================================================================

def test_an_edited_map_is_read_again(estate):
    _write_map(estate, _map(**{"svc-demo": ["A"]}))
    assert rcsm.current().services_for("A") == ("svc-demo",)

    _write_map(estate, _map(**{"enu-biometric": ["A", "B"]}))
    assert rcsm.current().services_for("A") == ("enu-biometric",)


def test_a_broken_edit_keeps_the_last_good_map(estate):
    _write_map(estate, _map(**{"svc-demo": ["A"]}))
    good = rcsm.current()

    _write_map(estate, "{broken")

    assert rcsm.current() is good


def test_no_file_is_an_empty_map(estate):
    assert rcsm.current() is rcsm.EMPTY


def test_the_file_can_be_put_anywhere(estate, tmp_path, monkeypatch):
    elsewhere = _write_map(tmp_path / "map.json", _map(**{"svc-demo": ["A"]}))
    monkeypatch.setattr(paths, "REASON_CODE_SERVICE_MAP_FILE", elsewhere)

    assert rcsm.map_path() == elsewhere
    assert rcsm.current().services_for("A") == ("svc-demo",)


# ======================================================================
# The download from S3
# ======================================================================

KEY = "nalanda/reason-codes/reason_code_services.json"


@pytest.fixture
def s3(estate, monkeypatch):
    from tests import s3_fakes

    fake = s3_fakes.FakeS3()
    s3_fakes.install(monkeypatch, fake)
    monkeypatch.setenv(rcsm.ENV_S3_KEY, KEY)
    return fake


def test_nothing_is_fetched_without_a_key(estate):
    assert rcsm.download() is False
    assert rcsm.start_background_download() is None
    assert rcsm.available() is True


def test_a_downloaded_map_is_written_and_used(s3, estate):
    s3._store(KEY, json.dumps(_map(**{"svc-demo": ["A"]})))
    assert rcsm.available() is False

    assert rcsm.download() is True

    assert rcsm.available() is True
    assert json.loads(estate.read_text())["services"] == {"svc-demo": ["A"]}
    assert sr.resolve(_payload(code="A")).service == "svc-demo"


@pytest.mark.parametrize("body", ["{broken", json.dumps({"schema_version": 9, "services": {}})])
def test_a_download_that_does_not_validate_changes_nothing(s3, estate, body):
    _write_map(estate, _map(**{"svc-demo": ["A"]}))
    before = estate.read_bytes()
    s3._store(KEY, body)

    assert rcsm.download() is False
    assert estate.read_bytes() == before


def test_a_download_that_fails_changes_nothing(s3, estate):
    _write_map(estate, _map(**{"svc-demo": ["A"]}))
    s3.fail_reads = True

    assert rcsm.download() is False
    assert rcsm.current().services_for("A") == ("svc-demo",)


def test_the_bucket_can_be_the_maps_own(s3, estate, monkeypatch):
    s3._store(KEY, json.dumps(_map(**{"svc-demo": ["A"]})))
    monkeypatch.delenv("CASEBOOK_S3_BUCKET")

    assert rcsm.s3_bucket() is None
    assert rcsm.download() is False

    monkeypatch.setenv(rcsm.ENV_S3_BUCKET, "maps")
    assert rcsm.s3_bucket() == "maps"
    assert rcsm.download() is True


def test_the_map_beside_the_documentation_does_not_break_its_download(s3, monkeypatch):
    """Kept under the documentation's prefix, the map is not taken for a
    service file -- which would fail validation and reject every refresh."""
    monkeypatch.setenv(rcd.ENV_S3_DOWNLOAD, "true")
    monkeypatch.setenv("REASON_CODE_DOCS_S3_PREFIX", "nalanda/reason-codes")
    s3._store("nalanda/reason-codes/svc.json", json.dumps({
        "schema_version": 1, "service": "svc",
        "codes": [{"numeric_code": 1, "reason_code": "SOME_CODE",
                   "description": "Raised when the packet cannot be reconciled.",
                   "category": "Technical", "is_retryable": True}],
        "rules": {"total_rules": 0, "rules": []}}))
    s3._store(KEY, json.dumps(_map(**{"svc": ["SOME_CODE"]})))

    assert rcd.download_service_docs() is True
    assert rcd.documented_services() == ("svc",)


@pytest.mark.parametrize("raw, expected", [
    (None, 0.0), ("", 0.0), ("soon", 0.0), ("-1", 0.0), ("300", 300.0),
])
def test_the_refresh_interval(monkeypatch, raw, expected):
    if raw is not None:
        monkeypatch.setenv(rcsm.ENV_REFRESH, raw)
    assert rcsm.refresh_seconds() == expected


# ======================================================================
# Validation at boot
# ======================================================================

def test_a_malformed_map_stops_the_boot(estate):
    _write_map(estate, "{broken")

    errors, _warnings = sr.validate()

    assert any("is not valid JSON" in error for error in errors)


def test_a_key_with_no_bucket_stops_the_boot(estate, monkeypatch):
    monkeypatch.setenv(rcsm.ENV_S3_KEY, KEY)

    errors, _warnings = sr.validate()

    assert any(error.startswith(rcsm.ENV_S3_KEY) for error in errors)


def test_what_the_map_leaves_out_or_misnames_is_a_warning(estate):
    _write_map(estate, _map(**{"enu-biometirc": ["BIO_CODE"], "svc-demo": ["DEMO_CODE"]}))

    errors, warnings = sr.validate()

    assert errors == []
    joined = "\n".join(warnings)
    assert "names services with no pack: ['enu-biometirc']" in joined
    assert "enu-biometric is analysed, but the reason-code service map" in joined
