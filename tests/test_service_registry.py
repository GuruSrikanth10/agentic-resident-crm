"""
The service registry, resolution and gate (MULTI_SERVICE_PLAN.md Phase 1).

Every test builds its own registry and documentation store under tmp_path and
points `paths` at them, so nothing here depends on what ships in
`src/service_packs/` -- except the tests that check exactly that, which are
named for it.
"""
import json

import pytest

from src.storage.local import LocalFilesystemCasebookStorage
from src.utils import paths, reason_code_docs
from src.utils import service_registry as sr

DEFAULT_PACK = {"schema_version": 1, "service": "_default",
                "display_name": "Default", "match": {},
                "rule_source": {"type": "none"}}


def _pack(name, prefix, stages=(), sub_stages=(), topics=(), **extra):
    document = {
        "schema_version": 1,
        "service": name,
        "display_name": f"{name} service",
        "tool_prefix": prefix,
        "match": {"stages": list(stages), "sub_stages": list(sub_stages),
                  "source_topics": list(topics)},
        "rule_source": {"type": "none"},
    }
    document.update(extra)
    return document


def _write_pack(root, document, dir_name=None, policy="The policy of this service."):
    directory = root / (dir_name or document["service"])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "service.json").write_text(json.dumps(document), encoding="utf-8")
    if policy is not None:
        (directory / "policy.md").write_text(policy, encoding="utf-8")


def _write_packs(root, *documents, default=True):
    root.mkdir(parents=True, exist_ok=True)
    if default:
        _write_pack(root, DEFAULT_PACK)
    for document in documents:
        _write_pack(root, document)
    return root


def _write_docs(root, **files):
    """One documentation file per stem, publishing the given codes. A code
    prefixed `rule:` is published as a rule entry instead of a code entry."""
    services = root / "services"
    services.mkdir(parents=True, exist_ok=True)
    for stem, codes in files.items():
        document = {"schema_version": 1, "service": stem, "codes": [],
                    "rules": {"rules": []}}
        for code in codes:
            if code.startswith("rule:"):
                document["rules"]["rules"].append({"reject_reason_code": code[5:]})
            else:
                document["codes"].append({"reason_code": code, "description": "d"})
        (services / f"{stem}.json").write_text(json.dumps(document), encoding="utf-8")
    return root


def _payload(stage=None, sub_stage=None, topic=None, code=None):
    payload = {"eventId": "evt-1"}
    if stage is not None or sub_stage is not None:
        payload["flowMetaData"] = {"stage": stage, "subStage": sub_stage}
    if topic is not None:
        payload["sourceTopic"] = topic
    if code is not None:
        payload["packetExecutionSummary"] = {
            "packetStatus": "REJECTED", "errorData": [{"errorReasonCode": code}]}
    return payload


@pytest.fixture
def estate(tmp_path, monkeypatch):
    """A registry of five services and a documentation store beside it."""
    packs = _write_packs(
        tmp_path / "packs",
        # The one service that has a rules table, as the shipped pack does:
        # every enabled service's rule source has to be a place that exists,
        # and `enu-biometric` is enabled by default.
        _pack("enu-biometric", "bio", stages=["Biometric"],
              rule_source={"type": "rules_db"}),
        _pack("svc-demo", "demo", stages=["Demographic"],
              topics=[r"ENU\.DEMO\..+"]),
        _pack("svc-alpha", "alpha", stages=["Shared"], sub_stages=["ALPHA"]),
        _pack("svc-beta", "beta", stages=["Shared"], sub_stages=["BETA"]),
        _pack("svc-docs", "docs", reason_code_docs_file="docs-file"),
    )
    docs = _write_docs(
        tmp_path / "docs",
        **{"enu-biometric": ["BIO_CODE", "SHARED_CODE"],
           "svc-demo": ["DEMO_CODE", "SHARED_CODE", "rule:DEMO_RULE_CODE"],
           "docs-file": ["DOCS_ONLY_CODE"],
           "unregistered-svc": ["ORPHAN_CODE"]})
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", packs)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", docs)
    return tmp_path


# ======================================================================
# Resolution
# ======================================================================

@pytest.mark.parametrize("payload, service, source, matched", [
    (_payload(stage="Biometric"), "enu-biometric", sr.SOURCE_FLOW_STAGE, "Biometric"),
    (_payload(stage="  biometric "), "enu-biometric", sr.SOURCE_FLOW_STAGE, "biometric"),
    (_payload(stage="Shared", sub_stage="ALPHA"), "svc-alpha", sr.SOURCE_FLOW_STAGE, "Shared"),
    (_payload(stage="shared", sub_stage="beta"), "svc-beta", sr.SOURCE_FLOW_STAGE, "shared"),
    (_payload(stage="Unknown", topic="ENU.DEMO.COMPLETION"), "svc-demo",
     sr.SOURCE_SOURCE_TOPIC, "ENU.DEMO.COMPLETION"),
    (_payload(stage="Unknown", code="DEMO_CODE"), "svc-demo",
     sr.SOURCE_REASON_CODE_DOCS, "DEMO_CODE"),
    (_payload(code="DEMO_RULE_CODE"), "svc-demo", sr.SOURCE_REASON_CODE_DOCS,
     "DEMO_RULE_CODE"),
    (_payload(code="DOCS_ONLY_CODE"), "svc-docs", sr.SOURCE_REASON_CODE_DOCS,
     "DOCS_ONLY_CODE"),
    (_payload(code="ORPHAN_CODE"), "unregistered-svc", sr.SOURCE_REASON_CODE_DOCS,
     "ORPHAN_CODE"),
])
def test_resolution_takes_the_first_step_that_decides(estate, payload, service,
                                                      source, matched):
    resolution = sr.resolve(payload)

    assert (resolution.service, resolution.source, resolution.matched) == \
        (service, source, matched)
    assert resolution.conflict is None
    assert resolution.registry_sha256 == sr.load().sha256


@pytest.mark.parametrize("payload", [
    _payload(stage="Shared"),                        # needs a sub-stage
    _payload(stage="Shared", sub_stage="GAMMA"),     # an unlisted sub-stage
    _payload(topic="XENU.DEMO.COMPLETION"),          # the whole topic must match
    _payload(code="SHARED_CODE"),                    # documented by two services
    _payload(code="NOT_DOCUMENTED"),
    _payload(),
    {"flowMetaData": None, "packetExecutionSummary": None},
    {"flowMetaData": {"stage": 7}, "sourceTopic": ["x"],
     "packetExecutionSummary": {"errorData": [None, {"errorReasonCode": "  "}]}},
    {},
    "not a dict",
    None,
])
def test_evidence_that_decides_nothing_leaves_the_packet_unresolved(estate, payload):
    resolution = sr.resolve(payload)

    assert resolution.service == sr.UNRESOLVED
    assert resolution.source == sr.SOURCE_NONE
    assert resolution.matched is None


def test_the_stage_wins_over_the_documentation_and_the_disagreement_is_kept(estate):
    resolution = sr.resolve(_payload(stage="Biometric", code="DEMO_CODE"))

    assert resolution.service == "enu-biometric"
    assert resolution.source == sr.SOURCE_FLOW_STAGE
    assert resolution.conflict == {"reason_code_docs": "svc-demo"}


@pytest.mark.parametrize("code", ["BIO_CODE", "SHARED_CODE", "NOT_DOCUMENTED"])
def test_documentation_that_agrees_or_is_ambiguous_is_not_a_conflict(estate, code):
    assert sr.resolve(_payload(stage="Biometric", code=code)).conflict is None


def test_the_resolution_records_what_it_looked_at(estate):
    resolution = sr.resolve(_payload(stage="Shared", sub_stage="ALPHA",
                                     topic="T", code="DEMO_CODE")).as_dict()

    assert resolution["detail"] == {"stage": "Shared", "sub_stage": "ALPHA",
                                    "source_topic": "T", "reason_code": "DEMO_CODE"}
    assert json.loads(json.dumps(resolution)) == resolution


def test_the_first_non_empty_reason_code_is_the_one_used(estate):
    payload = {"packetExecutionSummary": {"errorData": [
        None, {"errorReasonCode": ""}, {"errorReasonCode": "DEMO_CODE"},
        {"errorReasonCode": "BIO_CODE"}]}}

    assert sr.resolve(payload).service == "svc-demo"


def test_two_services_matching_one_topic_decide_nothing(tmp_path, monkeypatch):
    packs = _write_packs(tmp_path / "packs",
                         _pack("svc-one", "one", topics=[r"ENU\..+"]),
                         _pack("svc-two", "two", topics=[r"ENU\.X\..+"]))
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", packs)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", tmp_path / "no-docs")

    assert sr.resolve(_payload(topic="ENU.X.Y")).service == sr.UNRESOLVED
    assert sr.resolve(_payload(topic="ENU.Z")).service == "svc-one"


# ======================================================================
# Loading and validation of the packs
# ======================================================================

def _errors(root):
    return "\n".join(sr.load(root).errors)


@pytest.mark.parametrize("first_sub, second_sub, overlaps", [
    ([], [], True),
    (["A"], [], True),
    (["A", "B"], ["B"], True),
    (["A"], ["B"], False),
])
def test_two_services_may_share_a_stage_only_on_disjoint_sub_stages(
        tmp_path, first_sub, second_sub, overlaps):
    root = _write_packs(tmp_path,
                        _pack("svc-one", "one", stages=["Shared"], sub_stages=first_sub),
                        _pack("svc-two", "two", stages=["shared"], sub_stages=second_sub))

    assert ("both match stage 'shared'" in _errors(root)) is overlaps


@pytest.mark.parametrize("change, expected", [
    ({"schema_version": 2}, "schema_version is 2"),
    ({"surprise": True}, "unknown key(s) ['surprise']"),
    ({"tool_prefix": "Bad-Prefix"}, "'tool_prefix' is required"),
    ({"display_name": ""}, "'display_name' must be a non-empty string"),
    ({"match": {"stages": ["S"], "source_topics": ["("]}}, "not a valid regular expression"),
    ({"match": {"sub_stages": ["A"]}}, "needs at least one stage"),
    ({"match": {"stages": "S"}}, "must be a list of non-empty strings"),
    ({"rule_source": {"type": "spreadsheet"}}, "rule_source.type must be one of"),
    ({"rule_source": {"type": "none", "enrolment_type_filter": {"U": "UPDATE"}}},
     "applies only to type 'rules_db'"),
    ({"logs": {"k8s_match": {"name_contains": "a", "label_selector": "b"}}},
     "not both"),
    ({"logs": {"decision_vocabulary": "("}}, "decision_vocabulary is not a valid"),
    ({"enrolment_types": {"payload": {"U": {"family": "U"}}}},
     "payload['U'].label must be a non-empty string"),
    ({"tools": {"include": "x"}}, "tools.include must be a list"),
    ({"reason_code_docs_file": "../elsewhere"}, "'reason_code_docs_file' must match"),
])
def test_a_malformed_pack_is_reported_and_left_out(tmp_path, change, expected):
    document = _pack("svc-one", "one", stages=["S"])
    document.update(change)
    root = _write_packs(tmp_path, document)

    registry = sr.load(root)
    assert expected in "\n".join(registry.errors)
    assert "svc-one" not in registry.packs, "a pack with errors must not place packets"


def test_a_missing_required_key_is_reported(tmp_path):
    document = _pack("svc-one", "one", stages=["S"])
    del document["rule_source"]

    assert "missing required key(s) ['rule_source']" in _errors(_write_packs(tmp_path, document))


def test_the_service_must_equal_its_directory(tmp_path):
    root = _write_packs(tmp_path)
    _write_pack(root, _pack("svc-one", "one", stages=["S"]), dir_name="svc-other")

    assert "must equal its directory name, 'svc-other'" in _errors(root)
    assert not sr.load(root).services()


def test_a_directory_without_a_service_file_is_an_error(tmp_path):
    root = _write_packs(tmp_path)
    (root / "svc-empty").mkdir()

    assert "svc-empty/: has no service.json" in _errors(root)


def test_a_service_file_that_is_not_json_is_an_error(tmp_path):
    root = _write_packs(tmp_path)
    (root / "svc-bad").mkdir()
    (root / "svc-bad" / "service.json").write_text("{not json", encoding="utf-8")

    assert "svc-bad/service.json is not valid JSON" in _errors(root)


def test_a_service_name_must_be_lower_case(tmp_path):
    root = _write_packs(tmp_path, _pack("Svc", "svc", stages=["S"]))

    assert "service name 'Svc' must match" in _errors(root)


@pytest.mark.parametrize("field, expected", [
    ("tool_prefix", "share the tool prefix 'same'"),
    ("reason_code_docs_file", "share the reason-code documentation file 'same'"),
])
def test_prefixes_and_documentation_files_are_not_shared(tmp_path, field, expected):
    first = _pack("svc-one", "one", stages=["A"])
    second = _pack("svc-two", "two", stages=["B"])
    first[field] = second[field] = "same"

    assert expected in _errors(_write_packs(tmp_path, first, second))


def test_also_search_must_name_another_registered_service(tmp_path):
    root = _write_packs(tmp_path,
                        _pack("svc-one", "one", stages=["A"],
                              logs={"also_search": ["svc-one", "svc-ghost"]}))

    errors = _errors(root)
    assert "names 'svc-one', which is not another registered service" in errors
    assert "names 'svc-ghost', which is not another registered service" in errors


def test_the_default_pack_is_required(tmp_path):
    root = _write_packs(tmp_path, _pack("svc-one", "one", stages=["A"]), default=False)

    assert "There is no valid _default/ pack" in _errors(root)


@pytest.mark.parametrize("change, expected", [
    ({"match": {"stages": ["A"]}}, "the default pack must match nothing"),
    ({"tool_prefix": "dflt"}, "takes no 'tool_prefix'"),
])
def test_the_default_pack_matches_nothing_and_serves_no_tools(tmp_path, change, expected):
    root = _write_packs(tmp_path, default=False)
    document = dict(DEFAULT_PACK)
    document.update(change)
    _write_pack(root, document)

    assert expected in _errors(root)


def test_a_service_nothing_can_match_is_a_warning(tmp_path):
    root = _write_packs(tmp_path, _pack("svc-one", "one"))

    registry = sr.load(root)
    assert not registry.errors
    assert any("can be resolved only from a reason code" in w for w in registry.warnings)


def test_a_missing_pack_directory_is_an_error(tmp_path):
    assert "does not exist" in _errors(tmp_path / "nowhere")


def test_the_registry_is_loaded_once_and_kept(tmp_path):
    root = _write_packs(tmp_path, _pack("svc-one", "one", stages=["A"]))
    first = sr.load(root)

    _write_pack(root, _pack("svc-one", "one", stages=["B"]))
    assert sr.load(root) is first, "a pack changes with a deploy, not mid-stream"

    sr.reset()
    assert sr.load(root).packs["svc-one"].stages == ("b",)


# ======================================================================
# validate(): the settings that read the registry
# ======================================================================

def test_the_shipped_registry_is_valid():
    errors, _warnings = sr.validate()

    assert errors == []
    assert "enu-biometric" in sr.load().services()


def test_the_shipped_registry_places_a_biometric_packet(tmp_path, monkeypatch):
    # The registry alone: the shipped reason-code service map, which would
    # place it first, is tested in test_reason_code_service_map.py.
    monkeypatch.setattr(paths, "REASON_CODE_SERVICE_MAP_FILE", tmp_path / "no-map.json")
    resolution = sr.resolve({
        "flowMetaData": {"stage": "Biometric", "subStage": "MDD_POLICY_BATCH_1"},
        "packetExecutionSummary": {"errorData": [
            {"errorReasonCode": "RESIDENT_MAN_DEDUP_DUPLICATE"}]}})

    assert (resolution.service, resolution.source) == ("enu-biometric", sr.SOURCE_FLOW_STAGE)
    assert resolution.conflict is None


@pytest.mark.parametrize("enabled, expected", [
    ("enu-biometric,svc-ghost", "names 'svc-ghost', which has no valid pack"),
    ("_default", "names '_default', which is not a service"),
    ("_unresolved", "names '_unresolved', which is not a service"),
])
def test_every_enabled_service_must_be_registered(estate, monkeypatch, enabled, expected):
    monkeypatch.setenv(sr.ENV_ENABLED, enabled)

    errors, _warnings = sr.validate()
    assert any(expected in error for error in errors)


@pytest.mark.parametrize("variable", [sr.ENV_GATE, sr.ENV_UNRESOLVED])
def test_an_unknown_mode_is_an_error(estate, monkeypatch, variable):
    monkeypatch.setenv(variable, "sometimes")

    errors, _warnings = sr.validate()
    assert any(error.startswith(variable) for error in errors)


def test_documentation_without_a_pack_and_packs_without_documentation_are_warnings(estate):
    errors, warnings = sr.validate()

    assert errors == []
    joined = "\n".join(warnings)
    assert "unregistered-svc.json has no service pack" in joined
    for service in ("svc-alpha", "svc-beta"):
        assert f"{service} has no reason-code documentation file" in joined
    assert "svc-docs has no reason-code documentation" not in joined


# ======================================================================
# The gate
# ======================================================================

@pytest.mark.parametrize("service, reason", [
    (sr.UNRESOLVED, sr.SKIP_UNRESOLVED),
    ("unregistered-svc", sr.SKIP_NOT_REGISTERED),
    (sr.DEFAULT_PACK, sr.SKIP_NOT_REGISTERED),
    ("svc-demo", sr.SKIP_NOT_ENABLED),
    ("enu-biometric", None),
])
def test_record_mode_reports_the_reason_and_skips_nothing(estate, service, reason):
    decision = sr.gate({"service": service})

    assert decision == sr.GateDecision(skip=False, reason=reason, mode=sr.GATE_RECORD)


@pytest.mark.parametrize("service, reason", [
    (sr.UNRESOLVED, sr.SKIP_UNRESOLVED),
    ("unregistered-svc", sr.SKIP_NOT_REGISTERED),
    ("svc-demo", sr.SKIP_NOT_ENABLED),
])
def test_enforce_mode_skips(estate, monkeypatch, service, reason):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")

    assert sr.gate({"service": service}) == sr.GateDecision(
        skip=True, reason=reason, mode=sr.GATE_ENFORCE)


def test_enforce_mode_lets_an_enabled_service_through(estate, monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "ENFORCE")
    monkeypatch.setenv(sr.ENV_ENABLED, " svc-demo , enu-biometric ")

    for service in ("svc-demo", "enu-biometric"):
        assert not sr.gate({"service": service}).skip


def test_the_default_pack_policy_lets_unresolved_packets_through(estate, monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")
    monkeypatch.setenv(sr.ENV_UNRESOLVED, "default_pack")

    assert sr.gate({"service": sr.UNRESOLVED}).skip is False
    assert sr.gate({}).skip is False


@pytest.mark.parametrize("raw", [None, "", " , "])
def test_a_blank_enabled_list_means_the_default_not_nothing(monkeypatch, raw):
    if raw is None:
        monkeypatch.delenv(sr.ENV_ENABLED, raising=False)
    else:
        monkeypatch.setenv(sr.ENV_ENABLED, raw)

    assert sr.enabled_services() == frozenset(sr.DEFAULT_ENABLED)


def test_an_unknown_gate_mode_records_rather_than_skips(estate, monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "sometimes")

    assert sr.gate({"service": sr.UNRESOLVED}).skip is False


# ======================================================================
# The stored resolution
# ======================================================================

def test_a_stored_resolution_wins_over_a_fresh_one(estate, tmp_path):
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "store"))

    first, fresh = sr.load_or_resolve(storage, "evt-1", _payload(stage="Biometric"))
    assert fresh and first["service"] == "enu-biometric"
    assert sr.persist(storage, "evt-1", first)

    # The same packet now reads back as stored, even with evidence that would
    # place it elsewhere: the analysis stage acts on what the fetch stage did.
    again, fresh = sr.load_or_resolve(storage, "evt-1", _payload(stage="Demographic"))
    assert not fresh
    assert again == first


@pytest.mark.parametrize("stored", ["{not json", json.dumps({"service": ""}),
                                    json.dumps(["enu-biometric"])])
def test_an_unreadable_stored_resolution_is_resolved_again(estate, tmp_path, stored):
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "store"))
    storage.save_artifact("evt-1", sr.RESOLUTION_ARTIFACT, stored)

    resolution, fresh = sr.load_or_resolve(storage, "evt-1", _payload(stage="Biometric"))
    assert fresh and resolution["service"] == "enu-biometric"


def test_a_storage_failure_costs_a_resolution_never_the_packet(estate):
    class Broken:
        def load_artifact(self, *_args):
            raise OSError("disk gone")

        def save_artifact(self, *_args):
            raise OSError("disk gone")

    resolution, fresh = sr.load_or_resolve(Broken(), "evt-1", _payload(stage="Biometric"))
    assert fresh and resolution["service"] == "enu-biometric"
    assert sr.persist(Broken(), "evt-1", resolution) is False


# ======================================================================
# reason_code_docs.services_for_code
# ======================================================================

def test_codes_are_indexed_from_code_and_rule_entries(tmp_path):
    root = _write_docs(tmp_path, **{"svc-a": ["ONE", "rule:TWO"], "svc-b": ["ONE"]})

    assert reason_code_docs.services_for_code("ONE", root=root) == ("svc-a", "svc-b")
    assert reason_code_docs.services_for_code("TWO", root=root) == ("svc-a",)
    assert reason_code_docs.services_for_code("THREE", root=root) == ()
    assert reason_code_docs.services_for_code(None, root=root) == ()
    assert reason_code_docs.documented_services(root=root) == ("svc-a", "svc-b")


def test_an_edited_file_is_indexed_again(tmp_path):
    root = _write_docs(tmp_path, **{"svc-a": ["ONE"]})
    assert reason_code_docs.services_for_code("NEW_CODE", root=root) == ()

    _write_docs(tmp_path, **{"svc-a": ["ONE", "NEW_CODE"]})
    assert reason_code_docs.services_for_code("NEW_CODE", root=root) == ("svc-a",)


def test_an_unreadable_store_reads_as_documented_by_no_one(tmp_path):
    root = _write_docs(tmp_path, **{"svc-a": ["ONE"]})
    (root / "services" / "broken.json").write_text("{not json", encoding="utf-8")

    assert reason_code_docs.services_for_code("ONE", root=root) == ()
    assert reason_code_docs.services_for_code("ONE", root=tmp_path / "nowhere") == ()


# ======================================================================
# Every enabled service's rule source must exist (Phase 3, 5.7)
# ======================================================================

def test_a_service_with_no_rules_table_and_no_documents_is_an_error(estate,
                                                                    monkeypatch):
    """Its documentation is its only rule source, so with the documents off it
    has none at all -- the packets would be analysed with no rule."""
    monkeypatch.setenv(sr.ENV_ENABLED, "svc-demo")
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)

    errors, _warnings = sr.validate()

    assert any("svc-demo has rule_source.type 'none'" in error for error in errors)


def test_the_same_service_is_fine_with_the_documents_on(estate, monkeypatch):
    monkeypatch.setenv(sr.ENV_ENABLED, "svc-demo")
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")

    errors, _warnings = sr.validate()

    assert errors == []


def test_a_rules_table_service_with_no_database_settings_is_an_error(estate,
                                                                    monkeypatch):
    monkeypatch.setenv("USE_MOCK_DB", "false")
    monkeypatch.delenv("DB_HOST", raising=False)

    errors, _warnings = sr.validate()

    assert any("enu-biometric has rule_source.type 'rules_db'" in error
               for error in errors)


def test_the_mock_database_is_settings_enough(estate, monkeypatch):
    """The mock is what the tests and a laptop run against, and it needs no
    host at all."""
    monkeypatch.setenv("USE_MOCK_DB", "true")
    monkeypatch.delenv("DB_HOST", raising=False)

    errors, _warnings = sr.validate()

    assert errors == []


def test_a_service_not_enabled_is_not_checked(estate, monkeypatch):
    """The check is about what this deployment will analyse. A pack for a
    service nobody has switched on is allowed to be waiting for its
    documentation."""
    monkeypatch.setenv(sr.ENV_ENABLED, "enu-biometric")
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)

    errors, _warnings = sr.validate()

    assert errors == []


def test_admitting_unresolved_packets_with_no_documents_warns(estate, monkeypatch):
    """The `_default` pack has no service policy and no rules table, so with
    the documents off an unresolved packet is analysed from the payload alone.
    A warning, not an error: it is a deliberate choice, and the packet still
    gets a capped-confidence finding."""
    monkeypatch.setenv(sr.ENV_UNRESOLVED, sr.UNRESOLVED_DEFAULT_PACK)
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)

    errors, warnings = sr.validate()

    assert errors == []
    assert any(sr.UNRESOLVED_DEFAULT_PACK in warning for warning in warnings)
