"""
The casebook contract: src/models/casebook.py and src/models/dlt_casebook.py,
and the JSON Schema published from them in casebook_structures/.

Other services build their own objects from the published schema, so the
models and the schema must never drift apart, and no casebook may reach
storage without passing the models. These tests hold the two together --
field by field, type by type, value by value -- and hold every writer to the
models: one that breaks the contract is not saved at all.
"""
import ast
import asyncio
import copy
import json
import types
from inspect import isclass
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, Literal, Union, get_args, get_origin
from unittest.mock import MagicMock, patch

import pytest
from jsonschema import Draft7Validator
from langchain_core.messages import AIMessage
from pydantic import BaseModel

from src.models import casebook, dlt_casebook
from src.models.casebook import (
    CasebookContractError,
    check_rejection_casebook,
    save_rejection_casebook,
)
from src.models.dlt_casebook import check_dlt_casebook, save_dlt_casebook
from src.storage.local import LocalFilesystemCasebookStorage
from src.utils import metrics

REPO = Path(__file__).resolve().parents[1]
STRUCTURES = REPO / "casebook_structures"

CHECKS = {"rejection": check_rejection_casebook, "dlt": check_dlt_casebook}
SAVES = {"rejection": save_rejection_casebook, "dlt": save_dlt_casebook}
MODELS = {
    "rejection": (casebook.RejectionCasebook, casebook.RejectionFailureRecord),
    "dlt": (dlt_casebook.DltCasebook, dlt_casebook.DltTimeoutRecord),
}


@pytest.fixture(autouse=True)
def _default_mode(monkeypatch):
    monkeypatch.delenv(casebook.ENV_MODE, raising=False)


def example(lane: str) -> dict:
    return json.loads((STRUCTURES / f"{lane}_casebook.json").read_text(encoding="utf-8"))


def published(lane: str) -> dict:
    return json.loads((STRUCTURES / f"{lane}_casebook.schema.json").read_text(encoding="utf-8"))


def schema_errors(lane: str, document: dict) -> list:
    return [f"{'/'.join(str(part) for part in error.absolute_path)}: {error.message}"
            for error in Draft7Validator(published(lane)).iter_errors(document)]


def as_saved(lane: str, document: dict) -> dict:
    """`document` with the two fields the storage layer stamps on every save
    (`LocalFilesystemCasebookStorage.save`)."""
    saved = copy.deepcopy(document)
    saved.setdefault("schema_version",
                     casebook.CASEBOOK_SCHEMA_VERSION if lane == "rejection"
                     else dlt_casebook.DLT_CASEBOOK_SCHEMA_VERSION)
    saved.setdefault("casebook_metadata", {})["last_updated"] = "2026-10-15T09:30:00Z"
    return saved


# ======================================================================
# Every shape the writers produce
# ======================================================================

def rejection_shapes() -> dict:
    rejected = example("rejection")

    escalated = copy.deepcopy(rejected)
    escalated["pilot"] = True
    escalated["packet_metadata"]["service_resolution"]["conflict"] = {
        "reason_code_docs": "enu-demographic"}
    escalated["packet_status"]["status"] = "NEEDS_MANUAL_REVIEW"
    escalated["packet_status"]["rejection_data"]["rejection_logs"] = {
        "path": "No logs found", "gaps": None, "coverage": None}
    escalated["resolution"].update({
        "source": "runbook:RESIDENT_MAN_DEDUP_REJECT_TD__E@v3",
        "action": "MANUAL_REVIEW", "resident_action": "PENDING",
        "confidence": 0.6, "abstained": True,
        "confidence_policy": {"model_confidence": 0.9,
                              "detail": "confidence 0.6 is below the 0.7 threshold"},
        "shadow": {"runbook_id": "RESIDENT_MAN_DEDUP_REJECT_TD__E", "action": "RO_APPROVAL",
                   "resident_action": "RO_APPLICATION", "synthesis": None, "agreed": False},
    })
    escalated["resolution"]["provenance"].update({
        "prompt_fingerprint": "sha256:" + "0" * 64, "reason_code_doc": None,
        "investigator_path": None, "reviewer_path": None, "tool_calls": []})

    unparseable = copy.deepcopy(rejected)
    unparseable["packet_status"]["status"] = "FAILED_SYNTHESIS_PARSE"
    unparseable["resolution"].update({
        "action": "MANUAL_REVIEW", "resident_action": "PENDING", "confidence": None,
        "parse_error": "Response contained no JSON object.", "raw_output": "not json"})

    def short(status, **packet_metadata):
        return {"packet_metadata": {"eid": "evt-1", **packet_metadata},
                "packet_status": {"status": status},
                "resolution": {"synthesis": f"The run ended {status}."}}

    return {
        "rejected": rejected,
        "escalated": escalated,
        "unparseable": unparseable,
        "timeout": short("FAILED_TIMEOUT"),
        "dlq": short("DLQ"),
        "shutdown": short("FAILED_SHUTDOWN", abandoned_at=1791961961.873),
        "shutdown_unstamped": short("FAILED_SHUTDOWN"),
    }


def dlt_shapes() -> dict:
    from src.dlt.case_storage import timeout_casebook

    agent = example("dlt")

    completed = copy.deepcopy(agent)
    completed["finding"]["action"] = "NO_ACTION"
    completed["packet_status"]["status"] = "COMPLETED"

    canned = copy.deepcopy(agent)
    canned["packet"].update({"ref_id_source": "record_key", "event_id": None,
                             "service": "_unresolved", "service_resolution": None})
    canned["failure"].update({"class": "C", "business_code": None,
                              "registry_description": None, "registry_category": None,
                              "registry_category_source": None, "registry_stage": None})
    canned["evidence"].update({"corroboration": "CONTRADICTED",
                               "unexplained_exceptions": ["java.net.SocketTimeoutException"],
                               "gaps": ["LOGS_TOO_OLD"], "log_window": None})
    canned["finding"] = {"narrative": "A technical failure.", "discrepancy": "The logs disagree.",
                         "recommendation": "Redrive once the dependency recovers.",
                         "action": "REDRIVE_AFTER_RECOVERY", "reviewer_approved": None}
    canned["confidence"] = {"score": None, "ceilings_applied": ["contradicted"],
                            "abstained": False}
    canned["provenance"].update({"source": "canned", "reuse_decision": "CANNED",
                                 "group_occurrences": None, "group_state": "unavailable",
                                 "recommendation_state": "none", "single_flight": None,
                                 "service_pack": {"service": None, "sha256": None}})
    canned["code_check"].update({
        "verdict": "NOT_DEPLOYED", "required_version": "2.14.4", "details": {"note": "x"},
        "candidates": [{"commit": "4f1c2a9b7e", "subject": "Retry a missing row",
                        "path": "src/main/java/A.java", "timestamp_ms": 1791958000000,
                        "first_containing_version": "2.14.4",
                        "resolved_by": "read at the fix commit, which bumped it"}]})
    canned["replay"] = {"attempted": True, "reason": "clears the threshold", "queued": True,
                        "result": "Successfully auto-replayed packet REF",
                        "args": {"id": "REF", "idType": "REFID", "priority": 5,
                                 "operatorName": "dlt-analysis-auto",
                                 "category": "DLT_REDRIVE", "fromSedaStart": False}}
    canned["parked"] = {"parked": True, "reason": "waiting for the pods to reach 2.14.4",
                        "required_version": "2.14.4"}

    unparseable = copy.deepcopy(agent)
    unparseable["provenance"]["source"] = "failed_synthesis"
    unparseable["finding"] = {"narrative": "The analysis output failed its contract.",
                              "discrepancy": None,
                              "recommendation": "A human should read the attached evidence.",
                              "action": "NEEDS_MANUAL_REVIEW", "reviewer_approved": None,
                              "parse_error": "field 'action': not a known action"}
    unparseable["confidence"]["score"] = 0.0

    return {
        "agent": agent,
        "completed": completed,
        "canned": canned,
        "unparseable": unparseable,
        "timeout": timeout_casebook("dlt-T-63-1", "REF-1", "Ran out of time."),
        "timeout_without_ids": timeout_casebook(None, None, "Ran out of time."),
    }


SHAPES = {"rejection": rejection_shapes, "dlt": dlt_shapes}


@pytest.mark.parametrize("lane", ["rejection", "dlt"])
def test_the_published_examples_pass_the_contract_unchanged(lane):
    document = example(lane)
    # Compared as text, so the key order the readers see is held too.
    assert json.dumps(CHECKS[lane](copy.deepcopy(document))) == json.dumps(document)


@pytest.mark.parametrize("lane,shape", [(lane, shape) for lane in SHAPES
                                        for shape in SHAPES[lane]()])
def test_every_shape_saved_through_the_contract_matches_the_published_schema(
        lane, shape, tmp_path):
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    SAVES[lane](storage, "case-1", SHAPES[lane]()[shape])

    saved = storage.load("case-1")
    assert saved is not None
    assert schema_errors(lane, saved) == []
    assert storage.terminal_status("case-1") == saved["packet_status"]["status"]


def _rename(block: dict, old: str, new: str):
    block[new] = block.pop(old)


REFUSED = {
    "rejection": {
        "a key renamed back to schema 1.2's":
            lambda d: _rename(d["packet_metadata"], "rejected_at", "uploaded_at"),
        "a value of the wrong type": lambda d: d["resolution"].update(confidence="high"),
        "an action the contract does not list":
            lambda d: d["resolution"].update(action="REPLAY_PACKET"),
        "a field left out": lambda d: d["packet_status"]["rejection_data"][
            "rejection_logs"].pop("coverage"),
        "null where the contract has no null":
            lambda d: d["packet_metadata"].update(service=None),
        "a block the contract does not have":
            lambda d: d.update(resolution_outcome={"verdict": "CORRECT"}),
        "a parse failure without its evidence":
            lambda d: d["packet_status"].update(status="FAILED_SYNTHESIS_PARSE"),
        "a status the contract does not list":
            lambda d: d["packet_status"].update(status="COMPLETED"),
    },
    "dlt": {
        "a fingerprint that is not a digest":
            lambda d: d["failure"].update(fingerprint="not-a-digest"),
        "an attempted replay without its request": lambda d: d.update(replay={
            "attempted": True, "reason": "x", "queued": False, "result": None}),
        "a parked replay without its version":
            lambda d: d.update(parked={"parked": True, "reason": "x"}),
        "a cap the contract does not list":
            lambda d: d["confidence"].update(ceilings_applied=["class_c"]),
        "an agent finding without its per-code check":
            lambda d: d["finding"].pop("per_code_violations"),
        "more citations than the contract allows":
            lambda d: d["evidence"].update(citations=["a line"] * 21),
        "a number written as text": lambda d: d["source"].update(partition="63"),
    },
}


@pytest.mark.parametrize("lane,case", [(lane, case) for lane in REFUSED
                                       for case in REFUSED[lane]])
def test_what_the_contract_refuses_the_published_schema_refuses_too(lane, case):
    document = example(lane)
    REFUSED[lane][case](document)

    with pytest.raises(CasebookContractError):
        CHECKS[lane](copy.deepcopy(document))
    assert schema_errors(lane, as_saved(lane, document)), \
        "the published schema accepts what the models refuse"


# ======================================================================
# The models and the published schema describe the same document
# ======================================================================

JSON_TYPES = {str: "string", bool: "boolean", int: "integer", float: "number"}


def _options(annotation) -> list:
    """The alternatives of an annotation: Union members, Annotated unwrapped."""
    origin = get_origin(annotation)
    if origin is Annotated:
        return _options(get_args(annotation)[0])
    if origin in (Union, types.UnionType):
        return [option for arg in get_args(annotation) for option in _options(arg)]
    return [annotation]


def _entry(shapes: dict, path: tuple) -> dict:
    return shapes.setdefault(path, {"types": set(), "nullable": False, "enum": set()})


def model_shapes(models) -> dict:
    """{path: types, nullable, values} of every field the models write. A
    field the model leaves out rather than writing as null is not nullable in
    the document, whatever its annotation says."""
    shapes: dict = {}

    def visit_model(model, path):
        omitted = getattr(model, "_OMIT_WHEN_NONE", frozenset())
        for name, field in model.model_fields.items():
            visit(field.annotation, path + (field.alias or name,), name in omitted)

    def visit(annotation, path, omitted=False):
        entry = _entry(shapes, path)
        for option in _options(annotation):
            if option is type(None):
                entry["nullable"] = entry["nullable"] or not omitted
            elif get_origin(option) is Literal:
                for value in get_args(option):
                    entry["enum"].add(value)
                    entry["types"].add(JSON_TYPES[type(value)])
            elif isclass(option) and issubclass(option, BaseModel):
                entry["types"].add("object")
                visit_model(option, path)
            elif get_origin(option) is list:
                entry["types"].add("array")
                visit(get_args(option)[0], path + ("[]",))
            elif get_origin(option) is dict or option is dict:
                entry["types"].add("object")
            elif option is not Any:
                entry["types"].add(JSON_TYPES[option])

    for model in models:
        visit_model(model, ())
    return shapes


def schema_shapes(schema: dict) -> dict:
    """{path: types, nullable, values} of every property the published schema
    declares."""
    shapes: dict = {}
    definitions = schema.get("definitions", {})

    def resolve(node):
        while "$ref" in node:
            node = definitions[node["$ref"].rsplit("/", 1)[-1]]
        return node

    def visit_object(node, path):
        for name, child in resolve(node).get("properties", {}).items():
            visit(resolve(child), path + (name,))

    def visit(node, path):
        entry = _entry(shapes, path)
        declared = node.get("type", [])
        declared = [declared] if isinstance(declared, str) else list(declared)
        values = list(node.get("enum", [])) + ([node["const"]] if "const" in node else [])
        entry["types"].update(name for name in declared if name != "null")
        entry["nullable"] = entry["nullable"] or "null" in declared or None in values
        entry["enum"].update(value for value in values if value is not None)
        if "properties" in node:
            visit_object(node, path)
        if "items" in node:
            visit(resolve(node["items"]), path + ("[]",))

    visit_object(schema, ())
    return shapes


@pytest.mark.parametrize("lane", ["rejection", "dlt"])
def test_the_models_and_the_published_schema_describe_the_same_document(lane):
    ours, theirs = model_shapes(MODELS[lane]), schema_shapes(published(lane))

    assert sorted(set(ours) - set(theirs)) == [], "fields the published schema lacks"
    assert sorted(set(theirs) - set(ours)) == [], "published fields the models lack"
    assert {path: (ours[path], theirs[path]) for path in ours
            if ours[path] != theirs[path]} == {}


def test_the_contract_vocabularies_match_the_code_that_produces_them():
    from src.dlt import code_check, groups, per_code
    from src.dlt.classify import FailureClass
    from src.dlt.corroborate import Verdict
    from src.dlt.reuse import Decision
    from src.models.dlt_synthesis import UNREVIEWED_LABEL, UNVERIFIABLE_LABEL
    from src.storage.base import TERMINAL_STATUSES
    from src.utils import reason_code_docs, service_registry as registry

    assert set(dlt_casebook.FAILURE_CLASSES) == {member.value for member in FailureClass}
    assert set(dlt_casebook.CORROBORATION_VERDICTS) == {member.value for member in Verdict}
    assert set(dlt_casebook.REUSE_DECISIONS) == {member.value for member in Decision}
    assert set(dlt_casebook.RECOMMENDATION_STATES) == {
        groups.STATE_NONE, groups.STATE_DRAFT, groups.STATE_FINAL,
        groups.STATE_UNPERSISTED, groups.STATE_WITHHELD, groups.STATE_PER_OCCURRENCE}
    assert set(dlt_casebook.CODE_CHECK_VERDICTS) == {
        code_check.NO_CHANGE, code_check.NOT_DEPLOYED, code_check.FIX_DEPLOYED,
        code_check.UNKNOWN}
    assert set(dlt_casebook.PER_CODE_PATTERNS) == {name for name, _, _ in per_code._PATTERNS}
    assert set(dlt_casebook.PER_CODE_SEVERITIES) == {per_code.IDENTIFIER, per_code.PHRASING}
    assert {UNVERIFIABLE_LABEL, UNREVIEWED_LABEL} <= set(dlt_casebook.CEILINGS)
    assert set(dlt_casebook.SERVICE_SOURCES) == {
        registry.SOURCE_CONSUMER_GROUP, registry.SOURCE_FLOW_STAGE,
        registry.SOURCE_ORIGINAL_TOPIC, registry.SOURCE_JAVA_PACKAGE,
        registry.SOURCE_SOURCE_TOPIC, registry.SOURCE_REASON_CODE_DOCS, registry.SOURCE_NONE}
    assert set(casebook.SERVICE_SOURCES) == {
        registry.SOURCE_REASON_CODE_MAP, registry.SOURCE_FLOW_STAGE,
        registry.SOURCE_SOURCE_TOPIC, registry.SOURCE_REASON_CODE_DOCS, registry.SOURCE_NONE}
    assert set(casebook.DOC_SCOPES) == {
        reason_code_docs.SCOPE_OWN, reason_code_docs.SCOPE_OTHER_SERVICE,
        reason_code_docs.SCOPE_ALL}
    # Every status a casebook can carry ends the packet's lifecycle.
    assert set(casebook.INVESTIGATED_STATUSES + casebook.FAILURE_RECORD_STATUSES
               + dlt_casebook.ANALYSED_STATUSES) <= set(TERMINAL_STATUSES)


# ======================================================================
# What a violation does
# ======================================================================

@pytest.fixture
def violations(monkeypatch):
    seen = []
    monkeypatch.setattr(metrics, "record_casebook_contract_violation",
                        lambda lane, outcome: seen.append((lane, outcome)))
    return seen


def _drifted(lane: str) -> dict:
    document = example(lane)
    if lane == "rejection":
        document["packet_status"]["statuss"] = document["packet_status"].pop("status")
    else:
        document["failure"]["class"] = "Z"
    return document


@pytest.mark.parametrize("lane", ["rejection", "dlt"])
def test_a_casebook_that_breaks_the_contract_is_not_saved(lane, tmp_path, violations):
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))

    with pytest.raises(CasebookContractError, match="does not match its contract"):
        SAVES[lane](storage, "case-1", _drifted(lane))

    assert storage.load("case-1") is None
    assert storage.terminal_status("case-1") is None
    assert violations == [(lane, "refused")]


@pytest.mark.parametrize("lane", ["rejection", "dlt"])
def test_record_mode_saves_it_as_it_is_and_counts_it(lane, tmp_path, monkeypatch,
                                                     violations):
    monkeypatch.setenv(casebook.ENV_MODE, "record")
    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    document = _drifted(lane)

    SAVES[lane](storage, "case-1", copy.deepcopy(document))

    saved = storage.load("case-1")
    assert saved is not None
    expected = as_saved(lane, document)
    expected["casebook_metadata"]["last_updated"] = saved["casebook_metadata"]["last_updated"]
    assert saved == expected
    assert violations == [(lane, "saved")]


def test_an_unknown_mode_is_enforced_and_reported_at_boot(tmp_path, monkeypatch, violations):
    from src.utils.config_validator import _casebook_contract_errors

    monkeypatch.setenv(casebook.ENV_MODE, "strict")
    with pytest.raises(CasebookContractError):
        save_rejection_casebook(LocalFilesystemCasebookStorage(base_dir=str(tmp_path)),
                                "case-1", _drifted("rejection"))
    assert _casebook_contract_errors() == [
        "Unknown CASEBOOK_CONTRACT_MODE 'strict'; expected 'enforce' or 'record'."]

    monkeypatch.setenv(casebook.ENV_MODE, "record")
    assert _casebook_contract_errors() == []


# ======================================================================
# Every writer saves through the contract
# ======================================================================

#: The modules allowed to call `save_terminal` themselves: the storage layer
#: and the two contract modules. `_adapter` is the consumer's MessageAdapter,
#: whose `save_terminal` saves through the contract.
OWN_SAVE_TERMINAL = ("src/models/casebook.py", "src/models/dlt_casebook.py")


def test_nothing_saves_a_casebook_around_the_contract():
    bypasses = []
    for path in sorted((REPO / "src").rglob("*.py")):
        relative = path.relative_to(REPO).as_posix()
        if relative.startswith("src/storage/") or relative in OWN_SAVE_TERMINAL:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Attribute) and node.attr == "save_terminal":
                receiver = ast.unparse(node.value)
                if receiver != "_adapter":
                    bypasses.append(f"{relative}:{node.lineno} {receiver}.save_terminal")
    assert bypasses == [], ("save casebooks with save_rejection_casebook or "
                            "save_dlt_casebook, which check them first")


@pytest.fixture
def rejection_lane(tmp_path, monkeypatch):
    """The real graph, routes and storage with only the LLM stubbed, as in
    test_fetch_analyze_split.py."""
    import src.api.routes as routes
    import src.core.agent_orchestrator as orch
    import src.core.checkpointer as checkpointer
    import src.tools.tool_registry as tool_registry

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    for module in (routes, orch, tool_registry):
        monkeypatch.setattr(module, "get_casebook_storage", lambda: store)
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "false")
    monkeypatch.setenv("RUNBOOK_MODE", "off")
    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "checkpoints.db")
    checkpointer.reset_checkpointer()
    monkeypatch.setattr(orch, "_agent", None)
    yield SimpleNamespace(store=store, routes=routes, orch=orch)
    checkpointer.reset_checkpointer()


def test_the_rejection_lane_refuses_to_save_a_casebook_that_drifted(rejection_lane,
                                                                    monkeypatch):
    from src.models.schemas import MessagePayload

    lane = rejection_lane
    lane.store.save_artifact("cc-drift", "fetched_logs.txt", "--- pre-fetched trace ---")
    synthesis = json.dumps({"rejection_description": "manual dedup rejected the packet",
                            "synthesis": "ask the resident to resubmit",
                            "action": "RESIDENT_PACKET_RESUBMIT",
                            "resident_action": "NEW_PACKET", "confidence": 0.9})
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content=synthesis)]}
    # A writer that drifted: the prompt fingerprint written as a bare number.
    monkeypatch.setattr(lane.routes, "prompt_fingerprint", lambda *a, **k: 42)
    monkeypatch.setattr(lane.orch, "is_reviewer_approved", lambda _feedback: True)
    payload = MessagePayload(**{
        "eventId": "cc-drift",
        "sourceTopic": "ENU.BIO.PROCESS.COMPLETION.V2",
        "flowMetaData": {"stage": "Biometric", "subStage": "MDD_POLICY_BATCH_1"},
        "packetMetaData": {"refId": "REF-1", "enrolmentType": "U"},
        "packetExecutionSummary": {"packetStatus": "REJECTED", "errorData": [
            {"errorReasonCode": "RESIDENT_MAN_DEDUP_REJECT_TD"}]},
    })

    with patch.object(lane.orch, "build_agent", side_effect=lambda *a, **k: agent), \
         patch.object(lane.orch, "get_llm", side_effect=lambda tier: MagicMock()):
        with pytest.raises(CasebookContractError, match="prompt_fingerprint"):
            asyncio.run(lane.routes.analyze_rejection(payload))

    assert lane.store.load("cc-drift") is None
    assert lane.store.terminal_status("cc-drift") is None


def test_the_dlt_lane_refuses_to_save_a_casebook_that_drifted(tmp_path, monkeypatch):
    from src.api import dlt_routes
    from src.dlt import case_storage, registry
    from src.dlt.identity import storage_key
    from src.models.dlt_schemas import DltMessage
    from src.models.dlt_synthesis import DltFinding
    from src.storage import factory

    reference = json.loads((REPO / "tests" / "fixtures" / "dlt" /
                            "reference_business_exception.json").read_text())["headers"]
    for variable in ("DLT_GROUP_STORE", "DLT_CLAIM_ENABLED", "DLT_CLAIM_TTL_SECONDS",
                     "DLT_ANALYSIS_TIMEOUT_SECONDS", "DLT_ANALYZE_TIMEOUT_SECONDS",
                     "DLT_SINGLEFLIGHT_ENABLED", "DLT_REUSE_ENABLED",
                     "DLT_CODE_CHECK_ENABLED", "DLT_MAX_LOG_AGE_SECONDS"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setenv("CASEBOOK_STORAGE_BACKEND", "local")
    monkeypatch.setenv("DLT_REGISTRY_PATH", "tests/fixtures/dlt/business_errors.csv")
    snapshot = SimpleNamespace(ok=False, mixed=False, versions=(),
                               as_dict=lambda: {"ok": False, "versions": []})
    monkeypatch.setattr(dlt_routes.deployed, "running_version", lambda: snapshot)
    factory.reset_storage_cache()
    case_storage.reset_cache()
    registry.clear_cache()
    finding = DltFinding(narrative="The lookup treats an absent row as fatal.",
                         recommendation="Backfill the row.", action="DATA_FIX_REQUIRED",
                         confidence=0.9)
    monkeypatch.setattr(dlt_routes.orchestrator, "investigate",
                        lambda *a, **k: (finding, None))
    # A writer that drifted: an attempted replay recorded without its request.
    monkeypatch.setattr(dlt_routes.auto_replay, "maybe_replay", lambda *a, **k: {
        "attempted": True, "reason": "x", "queued": False, "result": None})
    message = DltMessage(case_id="dlt-T-63-9", headers=dict(reference),
                         payload={"packetMetaData": {"refId": "REF-9"}}, ref_id="REF-9")

    try:
        with pytest.raises(CasebookContractError, match="replay"):
            asyncio.run(dlt_routes.analyze_dlt(message))
        key = storage_key("REF-9", "dlt-T-63-9")
        assert case_storage.get_dlt_storage().load(key) is None
        assert case_storage.get_dlt_storage().terminal_status(key) is None
    finally:
        factory.reset_storage_cache()
        case_storage.reset_cache()
