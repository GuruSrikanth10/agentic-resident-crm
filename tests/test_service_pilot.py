"""
Pilot mode, and accuracy per service (MULTI_SERVICE_PLAN.md Phase 7).

A pilot service is analysed like an enabled one, with two differences: its
casebooks carry `pilot: true`, and its Synthesis agent is built without
`queue_for_replay`. Outcomes record the packet's service, so
`accuracy_report --service` can say when a pilot is ready to be enabled.

The graph tests run the real routes, graph and storage, with only the agents
stubbed, as test_service_gate.py does.
"""
import asyncio
import json
import sys
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
import src.tools.tool_registry as tool_registry
from src.api.routes import fetch_logs, process_rejection
from src.core import prompt_composer
from src.models.schemas import MessagePayload
from src.storage.local import LocalFilesystemCasebookStorage
from src.utils import outcomes, paths
from src.utils import service_registry as sr

SYNTHESIS = json.dumps({
    "rejection_description": "rejected by the rule",
    "synthesis": "ask the resident to resubmit",
    "action": "RESIDENT_PACKET_RESUBMIT",
    "resident_action": "NEW_PACKET",
    "confidence": 0.9,
})

PILOT = "svc-demo"


def _payload(event_id, stage="Biometric", code="RESIDENT_MAN_DEDUP_REJECT_TD"):
    return {
        "eventId": event_id,
        "flowMetaData": {"stage": stage, "subStage": "MDD_POLICY_BATCH_1"},
        "packetMetaData": {"refId": "REF-1", "srn": "SRN-1", "enrolmentType": "U"},
        "packetExecutionSummary": {
            "packetStatus": "REJECTED",
            "errorData": [{"type": None, "errorReasonCode": code}],
        },
    }


def _demo(event_id):
    return _payload(event_id, stage="Demographic", code="NOT_A_DOCUMENTED_CODE")


@pytest.fixture
def packs(tmp_path, monkeypatch):
    """The shipped enu-biometric and `_default` packs, and a second service,
    svc-demo, that is registered but neither enabled nor piloted."""
    root = tmp_path / "packs"
    for name, document in (
            ("_default", {"schema_version": 1, "service": "_default",
                          "display_name": "Default", "match": {},
                          "rule_source": {"type": "none"}}),
            ("enu-biometric", {"schema_version": 1, "service": "enu-biometric",
                               "display_name": "Biometric", "tool_prefix": "bio",
                               "match": {"stages": ["Biometric"]},
                               "rule_source": {"type": "rules_db"}}),
            (PILOT, {"schema_version": 1, "service": PILOT,
                     "display_name": "Demographic", "tool_prefix": "demo",
                     "match": {"stages": ["Demographic"]},
                     "rule_source": {"type": "none"}})):
        (root / name).mkdir(parents=True)
        (root / name / "service.json").write_text(json.dumps(document), encoding="utf-8")
        (root / name / "policy.md").write_text("A policy.", encoding="utf-8")
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", root)
    return root


@pytest.fixture
def pilot(monkeypatch):
    monkeypatch.setenv(sr.ENV_PILOT, PILOT)


# ======================================================================
# The registry
# ======================================================================

@pytest.mark.parametrize("raw, expected", [
    (None, frozenset()),
    ("", frozenset()),
    ("  ,  ", frozenset()),
    ("svc-demo", frozenset({"svc-demo"})),
    (" svc-demo , svc-other ,", frozenset({"svc-demo", "svc-other"})),
])
def test_the_pilot_list_is_read_and_blank_means_none(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv(sr.ENV_PILOT, raising=False)
    else:
        monkeypatch.setenv(sr.ENV_PILOT, raw)

    assert sr.pilot_services() == expected


def test_a_pilot_service_passes_the_gate(packs, monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")
    resolution = sr.resolve(_demo("p-gate")).as_dict()
    assert resolution["service"] == PILOT
    assert sr.gate(resolution).skip, "not piloted yet"

    monkeypatch.setenv(sr.ENV_PILOT, PILOT)

    decision = sr.gate(resolution)
    assert (decision.skip, decision.reason) == (False, None)
    assert sr.pack_for(resolution) == PILOT
    assert sr.is_pilot(PILOT) and not sr.is_pilot("enu-biometric")


def test_a_pilot_service_is_prebuilt(packs, pilot):
    assert PILOT in sr.packs_to_prebuild()


def test_a_pilot_service_is_checked_for_its_corpus(packs, pilot, tmp_path):
    (tmp_path / "docs" / "enu-biometric").mkdir(parents=True)

    assert sr.missing_corpus_dirs(tmp_path / "docs") == [PILOT]


def _validation_errors():
    errors, _warnings = sr.validate()
    return errors


def test_the_shipped_setting_validates_clean(packs, pilot, monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")

    assert not [e for e in _validation_errors() if sr.ENV_PILOT in e or PILOT in e]


@pytest.mark.parametrize("name", ["svc-nowhere", sr.DEFAULT_PACK, sr.UNRESOLVED])
def test_a_pilot_name_that_is_not_a_registered_service_fails_validation(
        packs, monkeypatch, name):
    monkeypatch.setenv(sr.ENV_PILOT, name)

    assert any(sr.ENV_PILOT in e and repr(name) in e for e in _validation_errors())


def test_a_service_both_piloted_and_enabled_fails_validation(packs, monkeypatch):
    monkeypatch.setenv(sr.ENV_PILOT, PILOT)
    monkeypatch.setenv(sr.ENV_ENABLED, f"enu-biometric,{PILOT}")

    assert any("both" in e and PILOT in e for e in _validation_errors())
    assert sr.is_pilot(PILOT), "where validation has not run, pilot wins"


def test_a_pilot_service_needs_a_rule_source(packs, pilot, monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "false")

    assert any(e.startswith(f"{PILOT} has rule_source.type 'none'")
               for e in _validation_errors())


# ======================================================================
# The prompts and the fingerprint
# ======================================================================

def test_only_a_pilot_synthesis_prompt_gets_the_pilot_section():
    pack = sr.PRE_REGISTRY_PACK
    for role in prompt_composer.ROLE_PROMPTS:
        plain = prompt_composer.compose_system_prompt(role, pack)
        piloted = prompt_composer.compose_system_prompt(role, pack, pilot=True)
        assert prompt_composer.PILOT_SYNTHESIS_SECTION not in plain
        if role == "synthesis":
            assert piloted == f"{plain}\n\n{prompt_composer.PILOT_SYNTHESIS_SECTION}"
        else:
            assert piloted == plain


def test_the_pilot_moves_the_fingerprint_and_nothing_else_does(monkeypatch):
    base = orch.os.path.dirname(orch.os.path.dirname(orch.__file__))
    pack = sr.PRE_REGISTRY_PACK
    plain = orch.compute_prompt_fingerprint(base, pack)

    assert orch.compute_prompt_fingerprint(base, pack, pilot=True) != plain
    assert orch.compute_prompt_fingerprint(base, pack, pilot=False) == plain

    monkeypatch.setattr(orch, "_prompt_fingerprints", {})
    assert orch.prompt_fingerprint(pack) == plain
    monkeypatch.setenv(sr.ENV_PILOT, pack)
    assert orch.prompt_fingerprint(pack) != plain
    assert orch.prompt_fingerprint(pack, pilot=False) == plain


# ======================================================================
# The graph and the casebook
# ======================================================================

@pytest.fixture
def storage(tmp_path, monkeypatch):
    import src.api.routes as routes
    import src.core.checkpointer as checkpointer
    import src.log_pipeline.pipeline as pipeline

    store = LocalFilesystemCasebookStorage(base_dir=str(tmp_path / "store"))
    monkeypatch.setattr(routes, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(pipeline, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(tool_registry, "get_casebook_storage", lambda: store)
    monkeypatch.setattr(outcomes, "get_casebook_storage", lambda: store)
    monkeypatch.setenv("ENABLE_LOG_FETCHING", "false")
    monkeypatch.setenv("RUNBOOK_MODE", "off")

    monkeypatch.setenv("CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setattr(checkpointer, "CHECKPOINT_DB_PATH", tmp_path / "checkpoints.db")
    checkpointer.reset_checkpointer()

    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "_prompt_fingerprints", {})
    yield store
    checkpointer.reset_checkpointer()


@pytest.fixture
def built(monkeypatch):
    """Every agent answers SYNTHESIS and the Reviewer approves. Yields the
    (role, pack, tool names, system prompt) of every agent built."""
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content=SYNTHESIS)]}
    monkeypatch.setattr(orch, "is_reviewer_approved", lambda _feedback: True)
    agents = []

    def build(role, _model, system_prompt, tools=(), *, pack=None):
        agents.append((role, pack, [getattr(t, "name", str(t)) for t in tools],
                       system_prompt))
        return agent

    with patch.object(orch, "build_agent", side_effect=build), \
         patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        yield agents


def _synthesis_agents(agents, pack):
    return [(tools, prompt) for role, built_pack, tools, prompt in agents
            if role == "synthesis" and built_pack == pack]


def test_a_pilot_synthesis_is_built_without_the_replay_tool(packs, pilot, storage, built):
    orch.get_agent()

    [(pilot_tools, pilot_prompt)] = _synthesis_agents(built, PILOT)
    [(bio_tools, bio_prompt)] = _synthesis_agents(built, "enu-biometric")
    assert "queue_for_replay" not in pilot_tools
    assert prompt_composer.PILOT_SYNTHESIS_SECTION in pilot_prompt
    assert "queue_for_replay" in bio_tools
    assert prompt_composer.PILOT_SYNTHESIS_SECTION not in bio_prompt


def test_a_pilot_casebook_says_so(packs, pilot, storage, built, monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")

    response = asyncio.run(process_rejection(MessagePayload(**_demo("p-cb"))))

    assert response["status"] == "processed"
    casebook = storage.load("p-cb")
    assert casebook["pilot"] is True
    assert casebook["packet_metadata"]["service"] == PILOT
    provenance = casebook["resolution"]["provenance"]
    assert provenance["service_pack"]["service"] == PILOT
    assert provenance["prompt_fingerprint"] == orch.prompt_fingerprint(PILOT, pilot=True)


def test_an_enabled_service_casebook_has_no_pilot_key(packs, pilot, storage, built):
    asyncio.run(process_rejection(MessagePayload(**_payload("p-bio"))))

    assert "pilot" not in storage.load("p-bio")


def test_a_pilot_packet_keeps_its_pilot_decision(packs, pilot, storage, built, monkeypatch):
    """Decided with the pack, once per packet: a graph invoked directly gets
    it from its pack, and one passed a decision keeps it."""
    graph = orch.get_agent()
    result = graph.invoke({"payload": _demo("p-direct"), "retry_count": 0},
                          config={"configurable": {"thread_id": "p-direct"}})
    assert result["pilot"] is True

    monkeypatch.delenv(sr.ENV_PILOT)
    result = graph.invoke({"payload": _demo("p-given"), "retry_count": 0,
                           "service_pack": PILOT, "pilot": True},
                          config={"configurable": {"thread_id": "p-given"}})
    assert result["pilot"] is True


def test_the_fetch_stage_lets_a_pilot_service_through(packs, pilot, storage, monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")
    monkeypatch.setattr("src.api.routes.publish_to_analysis_queue", lambda _payload: None)

    assert fetch_logs(MessagePayload(**_demo("p-fetch")))["status"] == "queued_for_analysis"


# ======================================================================
# Outcomes and the accuracy report
# ======================================================================

def test_an_outcome_records_the_service_and_the_pilot(packs, pilot, storage, built,
                                                       monkeypatch):
    monkeypatch.setenv(sr.ENV_GATE, "enforce")
    asyncio.run(process_rejection(MessagePayload(**_demo("p-out"))))
    asyncio.run(process_rejection(MessagePayload(**_payload("p-out-bio"))))

    piloted = outcomes.record_outcome("p-out", "CORRECT", "expert")
    enabled = outcomes.record_outcome("p-out-bio", "INCORRECT", "expert")

    assert (piloted["service"], piloted["pilot"]) == (PILOT, True)
    assert (enabled["service"], enabled["pilot"]) == ("enu-biometric", False)


def test_an_outcome_without_a_service_is_enu_biometrics():
    assert outcomes.outcome_service({"verdict": "CORRECT"}) == "enu-biometric"
    assert outcomes.outcome_service({"service": None}) == "enu-biometric"
    assert outcomes.outcome_service({"service": PILOT}) == PILOT


RECORDS = [
    {"verdict": "CORRECT", "reason_code": "CODE_A", "enrolment_type": "U",
     "resolution_source": "agent"},
    {"verdict": "INCORRECT", "reason_code": "CODE_A", "enrolment_type": "U",
     "resolution_source": "agent", "service": "enu-biometric"},
    {"verdict": "CORRECT", "reason_code": "CODE_A", "enrolment_type": "U",
     "resolution_source": "agent", "service": PILOT, "pilot": True},
]


def test_accuracy_is_grouped_by_service():
    rows = {row["service"]: row for row in outcomes.summarise(RECORDS)["rows"]}

    assert set(rows) == {"enu-biometric", PILOT}
    assert (rows["enu-biometric"]["total"], rows["enu-biometric"]["accuracy"]) == (2, 0.5)
    assert (rows[PILOT]["total"], rows[PILOT]["accuracy"]) == (1, 1.0)


def test_outcomes_are_filtered_by_service():
    assert list(outcomes.for_service(RECORDS, PILOT)) == [RECORDS[2]]
    assert list(outcomes.for_service(RECORDS, "enu-biometric")) == RECORDS[:2]
    assert list(outcomes.for_service(RECORDS, None)) == RECORDS


def test_shadow_rows_name_their_service():
    records = [dict(record, shadow_runbook_id="rb-1", shadow_agreed=True)
               for record in RECORDS]

    rows = outcomes.summarise_shadow(records)["rows"]

    assert sorted(row["service"] for row in rows) == ["enu-biometric", PILOT]


def _report(monkeypatch, capsys, *args):
    from src.tools import accuracy_report

    monkeypatch.setattr(accuracy_report, "iter_outcomes", lambda: iter(RECORDS))
    monkeypatch.setattr(sys, "argv", ["accuracy_report", *args])
    assert accuracy_report.main() == 0
    return capsys.readouterr().out


def test_the_report_filters_by_service(monkeypatch, capsys):
    report = json.loads(_report(monkeypatch, capsys, "--json", "--service", PILOT))

    assert report["total_outcomes"] == 1
    assert [row["service"] for row in report["rows"]] == [PILOT]


def test_the_report_counts_an_outcome_without_a_service_as_enu_biometrics(
        monkeypatch, capsys):
    report = json.loads(_report(monkeypatch, capsys, "--json", "--service",
                                "enu-biometric"))

    assert report["total_outcomes"] == 2


def test_the_shadow_report_names_the_allowlist_entry(monkeypatch, capsys):
    from src.tools import accuracy_report

    records = [dict(RECORDS[2], shadow_runbook_id="rb-1", shadow_agreed=True)]
    monkeypatch.setattr(accuracy_report, "iter_outcomes", lambda: iter(records))
    monkeypatch.setattr(sys, "argv", ["accuracy_report", "--shadow",
                                      "--min-verdicts", "1"])

    assert accuracy_report.main() == 0
    assert f"READY: add {PILOT}:CODE_A to RUNBOOK_SERVE_ALLOWLIST" \
        in capsys.readouterr().out
