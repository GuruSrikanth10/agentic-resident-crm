"""
Where a service's rules come from (MULTI_SERVICE_PLAN.md Phase 3, D6).

enu-biometric's rules are rows in the rules table. Every other service's are
in its reason-code documentation, and its packets must never query that table:
the rows there are one service's, and another service's reason code would
either miss or -- worse -- match a row that means something else.

The nodes are driven directly, as `test_rejection_docs_pipeline.py` does it,
so what the model is sent is assertable without an LLM or a checkpoint.
"""
import json

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
import src.tools.tool_registry as tool_registry
from src.core import prompt_composer, rejection_context as ctx
from src.utils import paths
from src.utils import service_registry as sr

#: A documented code and an undocumented one, in the service's own file.
DOCUMENTED = "NONE_SVC_CODE"
UNDOCUMENTED = "NOT_DOCUMENTED_ANYWHERE"


def _pack_document(name, rule_source, stage, prefix):
    return {
        "schema_version": 1,
        "service": name,
        "display_name": f"{name} service",
        "tool_prefix": prefix,
        "match": {"stages": [stage], "sub_stages": [], "source_topics": []},
        "rule_source": rule_source,
    }


@pytest.fixture
def estate(tmp_path, monkeypatch):
    """Two services: one with a rules table, one without, each documented."""
    packs = tmp_path / "packs"
    for document, policy in (
            ({"schema_version": 1, "service": "_default",
              "display_name": "Default", "match": {},
              "rule_source": {"type": "none"}}, "No service policy applies."),
            (_pack_document("enu-biometric", {"type": "rules_db"}, "Biometric", "bio"),
             "The biometric policy."),
            (_pack_document("svc-none", {"type": "none"}, "Nowhere", "none"),
             "The policy of the service with no rules table.")):
        directory = packs / document["service"]
        directory.mkdir(parents=True)
        (directory / "service.json").write_text(json.dumps(document), encoding="utf-8")
        (directory / "policy.md").write_text(policy, encoding="utf-8")

    docs = tmp_path / "docs" / "services"
    docs.mkdir(parents=True)
    for stem in ("enu-biometric", "svc-none"):
        (docs / f"{stem}.json").write_text(json.dumps({
            "schema_version": 1, "service": stem,
            "codes": [{"numeric_code": 7,
                       "reason_code": DOCUMENTED if stem == "svc-none" else "BIO_CODE",
                       "description": f"Raised by {stem} when the packet is "
                                      f"unreadable.",
                       "category": "Technical", "is_retryable": True}],
            "rules": {"total_rules": 0, "rules": []},
        }), encoding="utf-8")

    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", packs)
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", tmp_path / "docs")
    monkeypatch.setenv("REJECTION_SERVICES_ENABLED", "enu-biometric,svc-none")
    return tmp_path


class _Recorder:
    def __init__(self, reply="the findings"):
        self.reply = reply
        self.prompts = []

    def invoke(self, request):
        self.prompts.append(request["messages"][-1].content)
        return {"messages": [AIMessage(content=self.reply)]}

    @property
    def last(self):
        return self.prompts[-1]


def _node(monkeypatch, name, agent):
    from unittest.mock import MagicMock

    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    return orch._build_agent().builder.nodes[name].runnable.func


def _no_db(monkeypatch):
    """The rules table, wired to fail the test if it is queried at all."""
    def _refuse(*args, **kwargs):
        raise AssertionError("a service with no rules table queried the rules table")

    monkeypatch.setattr(orch, "lookup_rule_text", _refuse)
    monkeypatch.setattr(orch, "lookup_rule_for", _refuse)


def _payload(stage="Nowhere", code=DOCUMENTED, enrolment_type="U"):
    return {
        "eventId": "evt-1",
        "flowMetaData": {"stage": stage},
        "packetMetaData": {"enrolmentType": enrolment_type},
        "packetExecutionSummary": {
            "packetStatus": "REJECTED",
            "errorData": [{"errorReasonCode": code}]},
    }


def _state(payload=None, **overrides):
    state = {"payload": payload or _payload(), "logs": "Log fetching disabled.",
             "db_rule": "", "investigation": "", "retry_count": 0}
    state.update(overrides)
    return state


# ---------------------------------------------------------------------------
# The rules table is never queried for a service that has none.
# ---------------------------------------------------------------------------

def test_a_service_with_no_rules_table_never_queries_it(estate, monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    _no_db(monkeypatch)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    payload = _payload()
    assert sr.pack_for(sr.resolve(payload).as_dict()) == "svc-none"

    result = investigate(_state(payload))

    assert result["db_rule"] == "", "no rule was looked up, and none was invented"


def test_the_biometric_service_still_queries_it(estate, monkeypatch):
    """The parity check: nothing about this path changed."""
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    asked = []

    def _lookup(reason_code, packet_type, type_filter=None):
        asked.append((reason_code, packet_type, type_filter))
        return "THE RULE"

    monkeypatch.setattr(orch, "lookup_rule_text", _lookup)
    investigate = _node(monkeypatch, "investigate", _Recorder())

    payload = _payload(stage="Biometric", code="BIO_CODE")
    assert sr.pack_for(sr.resolve(payload).as_dict()) == "enu-biometric"

    result = investigate(_state(payload))

    assert result["db_rule"] == "THE RULE"
    assert asked == [("BIO_CODE", "U", sr.rule_type_filter("enu-biometric"))]


def test_the_runbook_lookup_of_a_service_with_no_rules_table_never_queries_it(
        estate, monkeypatch):
    """Its runbooks are its own, bound to its documentation (Phase 5), so the
    lookup reads that service's directory and never the rules table."""
    monkeypatch.setenv("RUNBOOK_MODE", "serve")
    _no_db(monkeypatch)
    outcomes, asked = [], []
    monkeypatch.setattr(orch.metrics, "RUNBOOK_LOOKUPS",
                        type("C", (), {"labels": staticmethod(
                            lambda outcome, service: type("L", (), {
                                "inc": staticmethod(
                                    lambda: outcomes.append((outcome, service)))
                            })())})())
    monkeypatch.setattr(orch, "get_runbook",
                        lambda *args: asked.append(args) or None)
    lookup = _node(monkeypatch, "runbook_lookup", _Recorder())

    result = lookup(_state())

    assert result["resolution_source"] == "agent"
    assert asked == [("svc-none", DOCUMENTED, "U")]
    assert outcomes == [("miss", "svc-none")]


# ---------------------------------------------------------------------------
# What the model is told instead.
# ---------------------------------------------------------------------------

def _investigation_prompt(monkeypatch, state):
    agent = _Recorder()
    _node(monkeypatch, "investigate", agent)(state)
    return agent.last


def test_the_prompt_carries_the_rule_source_note_and_no_rule_section(
        estate, monkeypatch):
    """A Database Rule Configuration section with nothing in it would read as
    a lookup that failed, so the section is replaced rather than emptied."""
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    _no_db(monkeypatch)

    prompt = _investigation_prompt(monkeypatch, _state())

    assert f"### {ctx.RULE_SOURCE}\n{ctx.RULE_NOTE_NO_DB_DOC}" in prompt
    assert ctx.DATABASE_RULE not in prompt
    assert "unreadable" in prompt, "its own documentation is the rule"
    assert "the Reason Code Documentation" in prompt


def test_an_undocumented_code_says_there_is_no_rule_at_all(estate, monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    _no_db(monkeypatch)

    prompt = _investigation_prompt(monkeypatch,
                                   _state(_payload(code=UNDOCUMENTED)))

    assert ctx.RULE_NOTE_NO_DB_NEITHER in prompt
    assert ctx.NO_DOCUMENTATION_NO_RULE in prompt
    assert ctx.NO_DOCUMENTATION not in prompt


def test_with_the_documents_off_the_note_says_so(estate, monkeypatch):
    """Both sources unavailable is a configuration no service should run in --
    boot validation refuses it -- but the prompt still has to be honest."""
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    _no_db(monkeypatch)

    prompt = _investigation_prompt(monkeypatch, _state())

    assert f"{ctx.RULE_SOURCE}: {ctx.RULE_NOTE_NO_DB_DOCS_OFF}" in prompt
    assert ctx.DATABASE_RULE not in prompt


def test_the_retry_and_review_prompts_carry_it_too(estate, monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    _no_db(monkeypatch)
    investigator = _Recorder()
    first = _node(monkeypatch, "investigate", investigator)(_state())

    retry = _investigation_prompt(monkeypatch, _state(
        investigation="the previous analysis",
        reviewer_feedback="not grounded in the documentation",
        reason_code_doc=first["reason_code_doc"]))
    assert f"### {ctx.RULE_SOURCE}\n{ctx.RULE_NOTE_NO_DB_DOC}" in retry
    assert ctx.DATABASE_RULE not in retry

    reviewer = _Recorder("APPROVED")
    _node(monkeypatch, "review", reviewer)(
        _state(investigation="the findings",
               reason_code_doc=first["reason_code_doc"]))
    assert f"### {ctx.RULE_SOURCE}\n{ctx.RULE_NOTE_NO_DB_DOC}" in reviewer.last
    assert ctx.DATABASE_RULE not in reviewer.last


def test_the_biometric_prompt_still_carries_the_database_rule(estate, monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    monkeypatch.setattr(orch, "lookup_rule_text", lambda *a, **k: "THE RULE")

    prompt = _investigation_prompt(
        monkeypatch, _state(_payload(stage="Biometric", code="BIO_CODE")))

    assert f"### {ctx.DATABASE_RULE}\nTHE RULE" in prompt
    assert f"### {ctx.RULE_SOURCE}" not in prompt


# ---------------------------------------------------------------------------
# The harness reads the same evidence off disk.
# ---------------------------------------------------------------------------

def _case_files(tmp_path, monkeypatch, pack, doc_state):
    case_dir = tmp_path / "case"
    orch._write_harness_case_files(case_dir, _payload(), "", "THE RULE",
                                   pack=pack, doc_state=doc_state)
    return case_dir


def test_the_harness_case_files_hold_the_documentation_not_a_rule(estate, tmp_path,
                                                                 monkeypatch):
    """The harness is not given the documents for a service whose rules are in
    the table (D13). Without the table they are the only rule there is, so the
    file is written and the task's RULE SOURCE section names it."""
    case_dir = _case_files(tmp_path, monkeypatch, "svc-none",
                           {"outcome": "hit", "text": "the documented rule"})

    context = json.loads((case_dir / "context.json").read_text(encoding="utf-8"))
    assert "db_rule" not in context
    assert (case_dir / prompt_composer.HARNESS_RULE_DOC_FILE).read_text(
        encoding="utf-8") == "the documented rule"


def test_an_undocumented_code_writes_the_note_rather_than_an_empty_file(
        estate, tmp_path, monkeypatch):
    case_dir = _case_files(tmp_path, monkeypatch, "svc-none", {"outcome": "miss"})

    assert (case_dir / prompt_composer.HARNESS_RULE_DOC_FILE).read_text(
        encoding="utf-8") == ctx.RULE_NOTE_NO_DB_NEITHER


def test_the_biometric_case_files_are_unchanged(estate, tmp_path, monkeypatch):
    case_dir = _case_files(tmp_path, monkeypatch, "enu-biometric",
                           {"outcome": "hit", "text": "the documented rule"})

    context = json.loads((case_dir / "context.json").read_text(encoding="utf-8"))
    assert context["db_rule"] == "THE RULE"
    assert not (case_dir / prompt_composer.HARNESS_RULE_DOC_FILE).exists()


def test_a_stale_rule_document_does_not_outlive_its_pack(estate, tmp_path,
                                                         monkeypatch):
    """A retry of the same packet under a pack that has a rules table must not
    leave the previous pass's document behind as evidence."""
    case_dir = _case_files(tmp_path, monkeypatch, "svc-none",
                           {"outcome": "hit", "text": "the documented rule"})
    assert (case_dir / prompt_composer.HARNESS_RULE_DOC_FILE).exists()

    orch._write_harness_case_files(case_dir, _payload(), "", "THE RULE",
                                   pack="enu-biometric",
                                   doc_state={"outcome": "hit", "text": "x"})

    assert not (case_dir / prompt_composer.HARNESS_RULE_DOC_FILE).exists()


def test_the_harness_task_says_where_the_rule_is(estate, monkeypatch):
    block = prompt_composer.harness_service_context(
        "investigator", {"service": "svc-none", "source": sr.SOURCE_FLOW_STAGE,
                         "matched": "Nowhere"}, "svc-none", event_id="evt-1")

    assert "### RULE SOURCE" in block
    assert f"casebook_evt-1/{prompt_composer.HARNESS_RULE_DOC_FILE}" in block

    unchanged = prompt_composer.harness_service_context(
        "investigator", {"service": "enu-biometric",
                         "source": sr.SOURCE_FLOW_STAGE, "matched": "Biometric"},
        "enu-biometric", event_id="evt-1")
    assert "RULE SOURCE" not in unchanged


# ---------------------------------------------------------------------------
# The filter the rules table is queried with.
# ---------------------------------------------------------------------------

def test_the_biometric_pack_filter_is_the_built_in_one():
    """The pack now supplies what `tool_registry` used to hard-code. They have
    to agree exactly, or the rows a biometric packet matches would change."""
    assert sr.rule_type_filter("enu-biometric") == tool_registry._ENROLMENT_TYPE_ALIASES


def test_a_pack_declaring_no_filter_applies_the_built_in_normalisation(estate):
    assert sr.rule_type_filter("svc-none") is None
    assert sr.rule_source_of("svc-none") == sr.NO_RULES_DB
    assert sr.rule_source_of("enu-biometric") == sr.RULES_DB
    assert sr.rule_source_of("_default") == sr.NO_RULES_DB
    assert sr.rule_source_of("not-a-pack") == sr.NO_RULES_DB
