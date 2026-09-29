"""
The DLT lane per service (MULTI_SERVICE_PLAN.md Phase 8).

Every service dead-letters its records with the rejection lane's payload and
key; only the headers differ, carrying the stack trace. So a record's refId
is a typed field, its service is resolved like a rejection's plus three DLT
signals, and each record is analysed -- and stored, and fingerprinted -- as
its own service's.

The route tests run the real routes and storage with the LLM and the
Kubernetes read stubbed, as test_dlt_flow_fixes.py does.
"""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.api import dlt_routes
from src.api.dlt_routes import analyze_dlt, fetch_dlt_logs
from src.core import prompt_composer
from src.dlt import case_storage, claims, deployed, orchestrator, registry
from src.dlt.identity import derive_case_id, is_valid_case_id, storage_key
from src.dlt.payload import rejection_contract, resolve_ref_id
from src.dlt.stacktrace import compute_fingerprint
from src.models.dlt_payload_schemas import summarise_payload
from src.models.dlt_schemas import DltMessage
from src.models.dlt_synthesis import DltFinding
from src.storage import factory
from src.tools import mcp_client
from src.utils import paths
from src.utils import service_registry as sr
from src.utils.message_adapters import DltAdapter

FIXTURE = Path(__file__).parent / "fixtures" / "dlt" / "reference_business_exception.json"
REFERENCE = json.loads(FIXTURE.read_text(encoding="utf-8"))["headers"]

BIO, DEMO = "enu-biometric", "svc-demo"
DEMO_GROUP = "demo-cg"

FINDING = DltFinding(
    narrative="The origin-tracker lookup treats an absent row as fatal.",
    recommendation="Query the origin-tracker table for each matched candidate.",
    action="DATA_FIX_REQUIRED", confidence=0.9)


def contract(event_id="EV-1", ref_id="REF-1", stage="Biometric", code="SOME_CODE"):
    """A payload in the rejection lane's contract."""
    return {
        "eventId": event_id,
        "category": "ENROLMENT",
        "sourceTopic": "ENU.SOURCE.V1",
        "flowMetaData": {"stage": stage, "subStage": "SUB"},
        "packetMetaData": {"refId": ref_id, "srn": "SRN-1", "enrolmentType": "U",
                           "name": "A Resident", "uid": "999988887777"},
        "packetExecutionSummary": {"packetStatus": "FAILED",
                                   "errorData": [{"errorReasonCode": code}]},
    }


def _write(root: Path, name: str, document: dict, dlt_text: str = None) -> None:
    (root / name).mkdir(parents=True)
    (root / name / "service.json").write_text(json.dumps(document), encoding="utf-8")
    (root / name / "policy.md").write_text("A policy.", encoding="utf-8")
    if dlt_text:
        (root / name / "dlt.md").write_text(dlt_text, encoding="utf-8")


def _pack(name, prefix, stages, dlt=None, rule="none"):
    document = {"schema_version": 1, "service": name, "display_name": name.title(),
                "tool_prefix": prefix, "match": {"stages": stages},
                "rule_source": {"type": rule}}
    if dlt is not None:
        document["dlt"] = dlt
    return document


@pytest.fixture
def packs(tmp_path, monkeypatch):
    """enu-biometric, owning the reference trace's package; svc-demo, owning
    a consumer group, a topic pattern and a package, and a `dlt.md`."""
    root = tmp_path / "packs"
    _write(root, "_default", {"schema_version": 1, "service": "_default",
                              "display_name": "Default", "match": {},
                              "rule_source": {"type": "none"}})
    _write(root, BIO, _pack(BIO, "bio", ["Biometric"],
                            {"java_packages": ["com.uidai.enu.biometric"]},
                            rule="rules_db"))
    _write(root, DEMO, _pack(DEMO, "demo", ["Demographic"],
                             {"consumer_groups": [DEMO_GROUP],
                              "original_topics": [r"DEMO\..*"],
                              "java_packages": ["com.uidai.enu.demographic",
                                                "com.uidai.enu.biometric.service.impl.demo"]}),
           dlt_text="Demographic crashes are almost always a missing address row.")
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", root)
    return root


# ======================================================================
# The contract
# ======================================================================

@pytest.mark.parametrize("key, mismatch", [
    (None, False), ("EV-1", False), ("REF-1", False), ("SRN-1", False),
    ("SOMETHING-ELSE-ENTIRELY", True),
])
def test_a_contract_payload_gives_its_ref_id_and_checks_the_key(key, mismatch):
    extraction = resolve_ref_id(contract(), key=key)

    assert (extraction.ref_id, extraction.source) == ("REF-1", "contract")
    assert extraction.mismatch is mismatch


def test_a_contract_payload_without_a_ref_id_falls_back_to_the_key():
    payload = contract()
    payload["packetMetaData"].pop("refId")

    extraction = resolve_ref_id(payload, key="c5d21184-08f4-4c32-9e5e-5c108c33eb14")

    assert extraction.source == "record_key"


def test_a_payload_outside_the_contract_keeps_the_old_layers():
    assert rejection_contract({"abisMWResponseNewSeda": {"refId": "X"}}) is None
    extraction = resolve_ref_id({"abisMWResponseNewSeda": {"refId": "REF-OLD-1"}})
    assert extraction.source == "search"


def test_the_contract_summary_labels_the_identifiers_and_leaves_out_resident_data():
    summary = summarise_payload(contract(), "com.uidai.enu.common.model.EventMessage")

    assert "MessagePayload" in summary
    assert "Stage / sub-stage: Biometric / SUB" in summary
    assert "SOME_CODE" in summary
    assert "refId = REF-1" in summary and "eventId = EV-1" in summary
    assert "A Resident" not in summary and "999988887777" not in summary


def test_the_adapter_reads_a_contract_record(packs):
    record = SimpleNamespace(
        value=json.dumps(contract()).encode("utf-8"), key=b"EV-1",
        headers=[(name, value.encode("utf-8")) for name, value in REFERENCE.items()],
        topic="DLT", partition=0, offset=7)

    body = DltAdapter().parse(record).body

    assert (body["ref_id"], body["ref_id_source"], body["event_id"]) == \
        ("REF-1", "contract", "EV-1")
    assert body["ref_id_mismatch"] is False
    assert body["case_id"].startswith("dlt-ENU.UPDATE.CHECKER.COMPLETION.V1-63-3352-g")
    assert DltAdapter().identity_of(body) == storage_key("REF-1", body["case_id"])


# ======================================================================
# Identity
# ======================================================================

def test_the_consumer_group_is_part_of_the_case_id():
    plain = derive_case_id("T", 1, 2)
    first = derive_case_id("T", 1, 2, "group-a")
    second = derive_case_id("T", 1, 2, "group-b")

    assert plain == "dlt-T-1-2"
    assert first != second and first.startswith("dlt-T-1-2-g")
    long = derive_case_id("T" * 300, 1, 2, "group-a")
    assert is_valid_case_id(long) and len(long) <= 128 and long.endswith(first[-11:])


def test_each_record_of_one_packet_has_its_own_storage_key():
    first, second = storage_key("REF-1", "dlt-T-1-1"), storage_key("REF-1", "dlt-T-1-2")

    assert first != second
    assert first.startswith("REF-1__") and second.startswith("REF-1__")
    assert storage_key(None, "dlt-T-1-1") == "dlt-T-1-1"
    assert storage_key("../escape", "dlt-T-1-1") == "dlt-T-1-1"
    assert storage_key("R" * 200, "dlt-T-1-1") == "dlt-T-1-1"


# ======================================================================
# The registry's dlt fields
# ======================================================================

@pytest.mark.parametrize("dlt, message", [
    ({"java_packages": ["not a package"]}, "is not a Java package name"),
    ({"original_topics": ["("]}, "is not a valid regular expression"),
    ({"unknown": []}, "unknown key"),
    ({"consumer_groups": "demo-cg"}, "must be a list"),
])
def test_a_malformed_dlt_section_is_an_error(tmp_path, dlt, message):
    root = tmp_path / "packs"
    _write(root, "_default", {"schema_version": 1, "service": "_default",
                              "display_name": "Default", "match": {},
                              "rule_source": {"type": "none"}})
    _write(root, DEMO, _pack(DEMO, "demo", ["Demographic"], dlt))

    assert any(message in error for error in sr.load(root).errors)


@pytest.mark.parametrize("field, value", [
    ("consumer_groups", "shared-cg"),
    ("java_packages", "com.uidai.shared"),
])
def test_two_services_cannot_share_a_dlt_signal(tmp_path, field, value):
    root = tmp_path / "packs"
    _write(root, "_default", {"schema_version": 1, "service": "_default",
                              "display_name": "Default", "match": {},
                              "rule_source": {"type": "none"}})
    _write(root, BIO, _pack(BIO, "bio", ["Biometric"], {field: [value]}))
    _write(root, DEMO, _pack(DEMO, "demo", ["Demographic"], {field: [value]}))

    assert any("share the" in error and value in error for error in sr.load(root).errors)


def test_the_default_pack_takes_no_dlt_signals(tmp_path):
    root = tmp_path / "packs"
    _write(root, "_default", {"schema_version": 1, "service": "_default",
                              "display_name": "Default", "match": {},
                              "rule_source": {"type": "none"},
                              "dlt": {"consumer_groups": ["any-cg"]}})

    assert any("dlt section must be empty" in error for error in sr.load(root).errors)


def test_the_shipped_registry_places_the_reference_record():
    headers = dlt_routes.parse_headers(REFERENCE)
    failure = dlt_routes.build_failure(headers, headers.exception_message)

    resolution = dlt_routes.resolve_service(MagicMock(load_artifact=lambda *a: None),
                                            "k", DltMessage(case_id="dlt-T-1-1",
                                                            headers=REFERENCE),
                                            headers, failure)[0]

    assert (resolution["service"], resolution["source"]) == (BIO, sr.SOURCE_JAVA_PACKAGE)


# ======================================================================
# Resolution
# ======================================================================

BIO_FRAMES = ("com.uidai.enu.biometric.service.impl.Helper.lookup",)


def _resolve(payload=None, **evidence):
    return sr.resolve_dlt(payload or {}, **evidence)


def test_the_consumer_group_decides_first_and_other_evidence_is_a_conflict(packs):
    resolution = _resolve(contract(stage="Biometric"), consumer_group=DEMO_GROUP,
                          frames=BIO_FRAMES)

    assert (resolution.service, resolution.source, resolution.matched) == \
        (DEMO, sr.SOURCE_CONSUMER_GROUP, DEMO_GROUP)
    assert resolution.conflict == {sr.SOURCE_FLOW_STAGE: BIO,
                                   sr.SOURCE_JAVA_PACKAGE: BIO}


@pytest.mark.parametrize("evidence, service, source", [
    ({"payload": contract(stage="Demographic")}, DEMO, sr.SOURCE_FLOW_STAGE),
    ({"original_topic": "DEMO.CHECKER.V1"}, DEMO, sr.SOURCE_ORIGINAL_TOPIC),
    ({"frames": BIO_FRAMES}, BIO, sr.SOURCE_JAVA_PACKAGE),
    # The longest package wins: the demo subpackage inside biometric's.
    ({"frames": ("com.uidai.enu.biometric.service.impl.demo.Address.find",)},
     DEMO, sr.SOURCE_JAVA_PACKAGE),
    # Framework frames first: the first frame a pack claims decides.
    ({"frames": ("org.springframework.X.y", "com.uidai.enu.demographic.A.b")},
     DEMO, sr.SOURCE_JAVA_PACKAGE),
    ({}, sr.UNRESOLVED, sr.SOURCE_NONE),
])
def test_each_piece_of_evidence_places_a_record_in_order(packs, evidence, service, source):
    resolution = _resolve(**evidence)

    assert (resolution.service, resolution.source) == (service, source)


def test_unknown_consumer_groups_and_topics_decide_nothing(packs):
    resolution = _resolve(consumer_group="other-cg", original_topic="OTHER.T",
                          frames=("org.other.A.b",))

    assert resolution.service == sr.UNRESOLVED
    assert resolution.detail["consumer_group"] == "other-cg"


# ======================================================================
# The gate, the pack and the fingerprint
# ======================================================================

@pytest.mark.parametrize("mode, service, skip, reason", [
    ("record", DEMO, False, sr.SKIP_NOT_ENABLED),
    ("enforce", DEMO, True, sr.SKIP_NOT_ENABLED),
    ("enforce", sr.UNRESOLVED, True, sr.SKIP_UNRESOLVED),
    ("enforce", "svc-unknown", True, sr.SKIP_NOT_REGISTERED),
    ("enforce", BIO, False, None),
])
def test_the_dlt_gate(packs, monkeypatch, mode, service, skip, reason):
    monkeypatch.setenv(sr.ENV_DLT_GATE, mode)

    decision = sr.dlt_gate({"service": service})

    assert (decision.skip, decision.reason) == (skip, reason)


def test_the_dlt_pack_is_the_records_own_only_when_it_is_let_through(packs, monkeypatch):
    assert sr.dlt_pack_for({"service": BIO}) == BIO
    assert sr.dlt_pack_for({"service": DEMO}) is None, "record mode: as before"
    assert sr.dlt_pack_for({"service": sr.UNRESOLVED}) is None

    monkeypatch.setenv(sr.ENV_DLT_ENABLED, f"{BIO},{DEMO}")
    assert sr.dlt_pack_for({"service": DEMO}) == DEMO


def test_only_another_services_fingerprint_is_namespaced(packs):
    assert sr.fingerprint_service({"service": BIO}) is None
    assert sr.fingerprint_service({"service": sr.UNRESOLVED}) is None
    assert sr.fingerprint_service({"service": "svc-unknown"}) is None
    assert sr.fingerprint_service({"service": DEMO}) == DEMO

    plain = compute_fingerprint("E", ["a.B.c"], "CODE")
    assert compute_fingerprint("E", ["a.B.c"], "CODE", service=None) == plain
    assert compute_fingerprint("E", ["a.B.c"], "CODE", service=DEMO) != plain


def _errors():
    return sr.validate()[0]


@pytest.mark.parametrize("variable, value, fragment", [
    (sr.ENV_DLT_GATE, "sometimes", sr.ENV_DLT_GATE),
    (sr.ENV_DLT_ENABLED, "svc-nowhere", "svc-nowhere"),
    (sr.ENV_DLT_ENABLED, sr.DEFAULT_PACK, "is not a service"),
])
def test_the_dlt_settings_are_validated(packs, monkeypatch, variable, value, fragment):
    monkeypatch.setenv(variable, value)

    assert any(fragment in error for error in _errors())


# ======================================================================
# The prompts
# ======================================================================

def test_a_pack_without_dlt_md_leaves_the_dlt_prompts_as_they_were(packs):
    assert prompt_composer.compose_dlt_system_prompt("GENERIC", BIO) == "GENERIC"
    assert prompt_composer.compose_dlt_system_prompt("GENERIC", None) == "GENERIC"


def test_a_packs_dlt_md_is_its_service_context(packs):
    prompt = prompt_composer.compose_dlt_system_prompt("GENERIC", DEMO)

    assert prompt.startswith("GENERIC\n\n### SERVICE CONTEXT -- Svc-Demo [svc-demo]")
    assert "missing address row" in prompt
    assert "A policy." not in prompt, "policy.md is for rejections"


def test_the_service_note_and_the_harness_context(packs):
    resolution = _resolve(consumer_group=DEMO_GROUP, frames=BIO_FRAMES).as_dict()

    note = prompt_composer.dlt_service_note(resolution, DEMO)
    assert note.startswith("This record belongs to svc-demo (Svc-Demo), placed "
                           "there by the consumer group that dead-lettered it")
    assert "Other evidence on this record names enu-biometric" in note
    assert prompt_composer.dlt_service_note(resolution, None) is None

    context = prompt_composer.dlt_harness_service_context(resolution, DEMO)
    assert "docs_cache/svc-demo/" in context and "missing address row" in context
    assert prompt_composer.dlt_harness_service_context(resolution, None) == ""


# ======================================================================
# Tools
# ======================================================================

def _remote(name, services, agents=("dlt_investigator",)):
    return mcp_client.RemoteTool(
        server=mcp_client.mcp_config.ServerConfig(name="agent_tools", url="http://x/mcp"),
        name=name, description=name, input_schema={}, agents=tuple(agents),
        services=tuple(services), toolset=None, guidance="", read_only=True)


def test_a_dlt_role_is_scoped_by_service_only_when_given_one(packs):
    catalog = mcp_client.Catalog(
        servers=(), tools=(_remote("dlt_common", ["*"]), _remote("demo_dlt", [DEMO]),
                           _remote("dlt_undeclared", [])),
        failures={})

    def names(service=None):
        return {tool.name for tool in mcp_client.selection("dlt_investigator", service,
                                                           catalog=catalog)}

    assert names() == {"dlt_common", "demo_dlt", "dlt_undeclared"}
    assert names(DEMO) == {"dlt_common", "demo_dlt"}
    assert names(BIO) == {"dlt_common"}
    with pytest.raises(ValueError):
        names(sr.DEFAULT_PACK)
    assert mcp_client.opencode_agent("dlt_investigator", DEMO) == "crm_dlt_investigator__svc_demo"


def test_the_dlt_agents_are_built_per_pack(packs, monkeypatch):
    orchestrator.reset_agent_cache()
    built = []
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [MagicMock(content="APPROVED")]}

    def build(role, _llm, prompt, tools=(), **kwargs):
        built.append((role, kwargs.get("pack"), prompt))
        return agent

    monkeypatch.setattr(orchestrator, "build_agent", build)
    monkeypatch.setattr(orchestrator, "get_llm", lambda tier: MagicMock())
    monkeypatch.setattr(orchestrator, "parse_finding", lambda text: (FINDING, None))
    try:
        orchestrator.get_dlt_agent()
        assert [(role, pack) for role, pack, _ in built] == [
            ("dlt_investigator", None), ("dlt_reviewer", None), ("dlt_synthesis", None)]

        resolution = _resolve(consumer_group=DEMO_GROUP).as_dict()
        orchestrator.investigate("KEY", {"frames": []}, MagicMock(
            verdict=MagicMock(value="UNVERIFIABLE"), reason="", unexplained=()),
            "", service_resolution=resolution, service_pack=DEMO)

        demo = [(role, prompt) for role, pack, prompt in built if pack == DEMO]
        assert {role for role, _ in demo} == {"dlt_investigator", "dlt_reviewer",
                                              "dlt_synthesis"}
        assert all("missing address row" in prompt for _, prompt in demo)
        sent = agent.invoke.call_args_list[0].args[0]["messages"][0].content
        assert sent.startswith("### Service\nThis record belongs to svc-demo")
    finally:
        orchestrator.reset_agent_cache()


# ======================================================================
# The routes
# ======================================================================

@pytest.fixture
def lane(packs, monkeypatch, tmp_path):
    """Real routes and storage; the LLM, the queue and Kubernetes stubbed.
    Returns what the stubs saw."""
    seen = {"investigate": [], "running": [], "reduce": []}
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path / "cases")
    monkeypatch.setenv("CASEBOOK_STORAGE_BACKEND", "local")
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    monkeypatch.setenv("DLT_MAX_LOG_AGE_SECONDS", "999999999")
    monkeypatch.setattr(dlt_routes, "publish_to_dlt_analysis_queue", lambda m: True)

    def running(*args, **kwargs):
        seen["running"].append((args, kwargs))
        return SimpleNamespace(ok=False, mixed=False, versions=(),
                               as_dict=lambda: {"ok": False, "versions": []})

    def reduce(event_id, **kwargs):
        seen["reduce"].append(kwargs.get("service"))
        return "--- no lines ---"

    def investigate(key, failure, corroboration, logs, payload_summary=None, **kwargs):
        seen["investigate"].append((key, kwargs.get("service_pack"), failure))
        return FINDING, None

    monkeypatch.setattr(deployed, "running_version", running)
    monkeypatch.setattr(dlt_routes, "reduce_logs", reduce)
    monkeypatch.setattr(dlt_routes.orchestrator, "investigate", investigate)
    factory.reset_storage_cache()
    case_storage.reset_cache()
    registry.clear_cache()
    yield seen
    factory.reset_storage_cache()
    case_storage.reset_cache()


def record(case_id="dlt-T-63-1", ref_id="REF-1", group=None, stage="Biometric",
           event_id="EV-1"):
    headers = dict(REFERENCE)
    if group:
        headers["kafka_dlt-original-consumer-group"] = group
    return DltMessage(case_id=case_id, headers=headers, ref_id=ref_id,
                      payload=contract(event_id=event_id, ref_id=ref_id, stage=stage),
                      ref_id_source="contract", event_id=event_id)


def run(message):
    fetched = fetch_dlt_logs(message)
    if fetched["status"] != "queued_for_analysis":
        return fetched, None
    analysed = asyncio.run(analyze_dlt(message))
    key = storage_key(message.ref_id, message.case_id)
    return analysed, case_storage.get_dlt_storage().load(key)


def test_a_skipped_record_leaves_nothing_behind(lane, monkeypatch):
    monkeypatch.setenv(sr.ENV_DLT_GATE, "enforce")
    message = record(group=DEMO_GROUP)

    result, _ = run(message)

    assert (result["status"], result["reason"], result["service"]) == \
        ("skipped", sr.SKIP_NOT_ENABLED, DEMO)
    assert case_storage.keys_for_ref_id("REF-1") == []
    assert claims.holder_of(message.case_id) is None
    assert lane["investigate"] == []


def test_record_mode_analyses_another_service_as_before_but_records_it(lane):
    result, casebook = run(record(group=DEMO_GROUP))

    assert result["status"] == "processed"
    (key, pack, failure), = lane["investigate"]
    assert pack is None, "no pack: exactly how every record was analysed before"
    assert lane["reduce"] == [None] and lane["running"] == [((), {}), ((), {})]
    assert casebook["schema_version"] == "1.3"
    assert casebook["packet"]["service"] == DEMO
    assert casebook["packet"]["event_id"] == "EV-1"
    assert casebook["packet"]["service_resolution"]["source"] == sr.SOURCE_CONSUMER_GROUP
    assert casebook["provenance"]["service_pack"] == {"service": None, "sha256": None}
    # Its fingerprint is its own service's even so: a group describes a
    # service's failure whether or not that service is analysed yet.
    assert casebook["failure"]["fingerprint_service"] == DEMO
    assert casebook["failure"]["fingerprint"] == failure["fingerprint"]


def test_an_enabled_service_is_analysed_with_its_own_pack(lane, monkeypatch):
    monkeypatch.setenv(sr.ENV_DLT_ENABLED, f"{BIO},{DEMO}")

    result, casebook = run(record(group=DEMO_GROUP))

    assert result["status"] == "processed"
    (_, pack, _), = lane["investigate"]
    assert pack == DEMO
    assert lane["reduce"] == [DEMO]
    assert lane["running"][0] == ((DEMO,), {}), "its own service's pods"
    assert casebook["provenance"]["service_pack"]["service"] == DEMO
    assert casebook["provenance"]["service_pack"]["sha256"] == sr.pack(DEMO).sha256


def test_an_enu_biometric_record_keeps_its_fingerprint_and_its_version_read(lane):
    headers = dlt_routes.parse_headers(REFERENCE)
    legacy = dlt_routes.build_failure(headers, headers.exception_message)["fingerprint"]

    _, casebook = run(record())

    assert casebook["packet"]["service"] == BIO
    assert casebook["failure"]["fingerprint"] == legacy
    assert casebook["failure"]["fingerprint_service"] is None
    assert lane["running"] == [((), {}), ((), {})]
    (_, pack, _), = lane["investigate"]
    assert pack == BIO


def test_a_second_record_of_the_same_packet_is_analysed_too(lane):
    """MULTI_SERVICE_PLAN.md section 10: keyed on the refId, the second
    record found the first one's terminal casebook and was never analysed."""
    first, first_case = run(record(case_id="dlt-T-63-1"))
    second, second_case = run(record(case_id="dlt-T-63-2"))

    assert (first["status"], second["status"]) == ("processed", "processed")
    assert len(case_storage.keys_for_ref_id("REF-1")) == 2
    assert (first_case["case_id"], second_case["case_id"]) == ("dlt-T-63-1", "dlt-T-63-2")
    # One failure mode: the second is answered from the first's group, as
    # any recurrence is -- but it is a case of its own, not a skipped one.
    assert second_case["provenance"]["source"] == "group_reuse"


def test_the_analysis_stage_acts_on_the_stored_resolution(lane, monkeypatch):
    message = record(group=DEMO_GROUP)
    fetch_dlt_logs(message)
    key = storage_key(message.ref_id, message.case_id)
    stored = json.loads(case_storage.get_dlt_storage().load_artifact(
        key, sr.RESOLUTION_ARTIFACT))
    assert stored["service"] == DEMO

    # A service switched off -- and on in enforce -- while the record waited.
    monkeypatch.setenv(sr.ENV_DLT_GATE, "enforce")
    assert asyncio.run(analyze_dlt(message))["status"] == "skipped"
    assert case_storage.get_dlt_storage().load(key) is None
