"""
The rejection graph with reason-code documentation
(REASON_CODE_DOCS_PLAN.md Phases 4 to 7).

The nodes are driven directly, the way `tests/test_opencode_harness.py` does
it, so a prompt's exact contents are assertable without an LLM or a
checkpoint store.

The first test is the one that makes the rest safe to have: with the switch
off, the Investigator's prompts are character-for-character what they were
before any of this existed. Every other behaviour here sits behind a switch,
so a rollback is a configuration change rather than a revert.
"""
import json

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
from src.core import rejection_context as ctx
from src.utils import metrics, paths, reason_code_docs as rcd

FIXTURE_ROOT = paths.REPO_ROOT / "tests" / "fixtures" / "reason_code_docs"

PAYLOAD = {
    "eventId": "evt-1",
    "packetMetaData": {"enrolmentType": "E"},
    "packetExecutionSummary": {
        "errorData": [{"errorReasonCode": "FIXTURE_TYPED_CODE"}]},
    "flowMetaData": {"stage": "BIO"},
    "sourceTopic": "ENU.BIO.REJECT",
}

BASE_STATE = {
    "payload": PAYLOAD,
    "logs": "a log line for <refId>",
    "db_rule": "THE RULE",
    "investigation": "",
    "retry_count": 0,
}


class _Recorder:
    """Stands in for every react agent, and keeps the prompts it was sent."""

    def __init__(self, reply="the findings"):
        self.reply = reply
        self.prompts = []

    def invoke(self, request):
        messages = request["messages"]
        self.prompts.append(messages[-1].content)
        return {"messages": [AIMessage(content=self.reply)]}

    @property
    def last(self):
        return self.prompts[-1]


def _node(monkeypatch, name, agent):
    from unittest.mock import MagicMock

    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "create_react_agent", lambda *a, **k: agent)
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    return orch._build_agent().builder.nodes[name].runnable.func


def _docs_on(monkeypatch, root=FIXTURE_ROOT):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    monkeypatch.setattr(paths, "REASON_CODE_DOCS_DIR", root)


def _state(**overrides):
    state = dict(BASE_STATE)
    state.update(overrides)
    return state


# ---------------------------------------------------------------------------
# Switch off: the prompts are exactly what they were (D7).
# ---------------------------------------------------------------------------

def test_the_first_pass_prompt_is_unchanged_with_the_switch_off(monkeypatch):
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    result = investigate(_state())

    expected = (
        f"Kafka Payload: {json.dumps(orch._project_payload(PAYLOAD))}\n\n"
        f"Enrolment Type: {orch.enrolment_type_display(PAYLOAD)}\n\n"
        f"Elasticsearch Logs: {BASE_STATE['logs']}\n\n"
        f"Database Rule Configuration:\n{BASE_STATE['db_rule']}\n\n"
    )
    assert agent.last == expected
    assert result["reason_code_doc"] == {"outcome": "disabled"}
    assert result["investigator_path"] == "direct"


def test_the_retry_prompt_is_unchanged_with_the_switch_off(monkeypatch):
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    investigate(_state(investigation="the previous analysis",
                       reviewer_feedback="not grounded in the logs"))

    assert agent.last == (
        "Your previous analysis:\nthe previous analysis\n\n"
        "Reviewer Feedback (You MUST fix your previous analysis): "
        "not grounded in the logs\n\n"
        f"Elasticsearch Logs (cite these):\n{BASE_STATE['logs']}\n\n"
    )


def test_the_reviewer_prompt_is_unchanged_when_evidence_is_off(monkeypatch):
    monkeypatch.setenv("REJECTION_REVIEWER_EVIDENCE", "false")
    agent = _Recorder("APPROVED")
    review = _node(monkeypatch, "review", agent)

    result = review(_state(investigation="the findings"))

    assert agent.last == (
        "Validate this investigation:\nthe findings\n\n"
        "If it's perfect, reply with exactly 'APPROVED'. If not, explain "
        "what is wrong.")
    assert result["reviewer_path"] == "direct"


# ---------------------------------------------------------------------------
# Switch on: the Investigator is given the document.
# ---------------------------------------------------------------------------

def test_a_hit_puts_the_document_ahead_of_the_rule_and_the_logs_last(monkeypatch):
    _docs_on(monkeypatch)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    result = investigate(_state())
    prompt = agent.last

    assert result["reason_code_doc"]["outcome"] == "hit"
    assert result["reason_code_doc"]["matched_type"] == "E"
    assert result["investigator_path"] == "direct"
    assert "fx-enrolment" in prompt
    assert prompt.index(ctx.DOCUMENTATION) < prompt.index(ctx.DATABASE_RULE)
    assert prompt.index(BASE_STATE["logs"]) < prompt.index(f"### {ctx.TASK}")


def test_a_miss_tells_the_investigator_so(monkeypatch):
    _docs_on(monkeypatch)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    payload = json.loads(json.dumps(PAYLOAD))
    payload["packetExecutionSummary"]["errorData"] = [
        {"errorReasonCode": "NOT_DOCUMENTED"}]
    result = investigate(_state(payload=payload))

    assert result["reason_code_doc"]["outcome"] == "miss"
    assert ctx.NO_DOCUMENTATION in agent.last


def test_the_retry_carries_the_document_but_not_the_payload(monkeypatch):
    _docs_on(monkeypatch)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    investigate(_state(investigation="the previous analysis",
                       reviewer_feedback="not grounded",
                       reason_code_doc=rcd.lookup("FIXTURE_TYPED_CODE", "E",
                                                  root=FIXTURE_ROOT)))
    prompt = agent.last

    for fragment in ("the previous analysis", "not grounded", "fx-enrolment",
                     "THE RULE", BASE_STATE["logs"]):
        assert fragment in prompt
    assert f"### {ctx.KAFKA_PAYLOAD}" not in prompt


# ---------------------------------------------------------------------------
# The document is chosen once per packet (D9).
# ---------------------------------------------------------------------------

def test_a_retry_reuses_the_document_the_first_pass_chose(monkeypatch):
    """One packet must never be reasoned about with two versions of a
    document, so the lookup runs once and every later node reads the state."""
    _docs_on(monkeypatch)
    calls = []
    real = rcd.lookup
    monkeypatch.setattr(rcd, "lookup",
                        lambda *a, **k: calls.append(a) or real(*a, **k))

    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    first = investigate(_state())
    investigate(_state(investigation="previous", reviewer_feedback="fix it",
                       reason_code_doc=first["reason_code_doc"]))

    assert len(calls) == 1


def test_a_retry_resuming_an_older_checkpoint_does_the_lookup(monkeypatch):
    """A checkpoint written before this feature existed has no entry, and a
    retry that skipped the lookup would send no documentation at all."""
    _docs_on(monkeypatch)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    result = investigate(_state(investigation="previous",
                                reviewer_feedback="fix it"))

    assert result["reason_code_doc"]["outcome"] == "hit"
    assert "fx-enrolment" in agent.last


# ---------------------------------------------------------------------------
# A documentation problem never fails a packet (D10).
# ---------------------------------------------------------------------------

def test_a_lookup_that_raises_is_an_error_and_the_packet_carries_on(monkeypatch):
    _docs_on(monkeypatch)

    def explode(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(rcd, "lookup", explode)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    result = investigate(_state())

    assert result["reason_code_doc"]["outcome"] == "error"
    assert result["reason_code_doc"]["detail"] == "RuntimeError: boom"
    assert result["investigation"] == "the findings"
    assert ctx.NO_DOCUMENTATION in agent.last


def test_an_error_state_from_the_node_has_the_full_shape(monkeypatch):
    """A half-filled state makes a provenance record that differs from every
    other packet's."""
    _docs_on(monkeypatch)
    # Captured before the patch below replaces the function it comes from.
    hit_keys = set(rcd.lookup("FIXTURE_TYPED_CODE", "E", root=FIXTURE_ROOT))

    def explode(*_args, **_kwargs):
        raise ValueError("x")

    monkeypatch.setattr(rcd, "lookup", explode)
    investigate = _node(monkeypatch, "investigate", _Recorder())

    assert set(investigate(_state())["reason_code_doc"]) == hit_keys


# ---------------------------------------------------------------------------
# The lookup is counted, by outcome and by what matched.
# ---------------------------------------------------------------------------

def _counted(monkeypatch):
    seen = []

    class _Labels:
        def __init__(self, kwargs):
            self.kwargs = kwargs

        def inc(self, *_a, **_k):
            seen.append(self.kwargs)

    class _Counter:
        def labels(self, **kwargs):
            return _Labels(kwargs)

    monkeypatch.setattr(metrics, "REASON_CODE_DOC_LOOKUPS", _Counter())
    return seen


@pytest.mark.parametrize("reason_code,raw_type,outcome,match", [
    ("FIXTURE_TYPED_CODE", "E", "hit", "exact"),
    ("FIXTURE_ANY_CODE", "E", "hit", "any"),
    ("NOT_DOCUMENTED", "E", "miss", "none"),
    (None, "E", "no_reason_code", "none"),
])
def test_every_lookup_outcome_is_counted(monkeypatch, reason_code, raw_type,
                                         outcome, match):
    """`exact` and `any` are counted apart: `any` means the code is documented
    but not for this enrolment type, which is a different piece of work from
    a miss."""
    _docs_on(monkeypatch)
    seen = _counted(monkeypatch)
    investigate = _node(monkeypatch, "investigate", _Recorder())

    payload = json.loads(json.dumps(PAYLOAD))
    payload["packetMetaData"]["enrolmentType"] = raw_type
    payload["packetExecutionSummary"]["errorData"] = (
        [{"errorReasonCode": reason_code}] if reason_code else [])
    investigate(_state(payload=payload))

    assert seen == [{"outcome": outcome, "match": match}]


def test_the_switch_being_off_is_counted_as_nothing(monkeypatch):
    """`disabled` is not an outcome of a lookup, because no lookup ran; the
    miss rate that drives authoring must not be diluted by packets nobody
    asked about."""
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    seen = _counted(monkeypatch)
    investigate = _node(monkeypatch, "investigate", _Recorder())

    investigate(_state())

    assert seen == []


# ---------------------------------------------------------------------------
# The harness path (D13).
# ---------------------------------------------------------------------------

def _harness_on(monkeypatch, tmp_path, result, fail=False):
    from src.utils import opencode_runner

    monkeypatch.setenv("USE_OPENCODE_HARNESS_REJECTION", "true")
    monkeypatch.setattr(paths, "LOCAL_CASESHEETS_DIR", tmp_path)

    def run(prompt, output_path, node=None):
        if fail:
            raise opencode_runner.OpencodeUnavailable("no binary")
        return {"result": result, "seconds": 0,
                "trace": {"llm_calls": 1, "tools": {}, "tokens": {},
                          "cost": 0.0, "session_id": "ses_test"}}

    monkeypatch.setattr(opencode_runner, "run_task_json", run)


def test_a_harness_investigation_records_its_path(monkeypatch, tmp_path):
    _docs_on(monkeypatch)
    _harness_on(monkeypatch, tmp_path, {"investigation": "from the harness"})
    investigate = _node(monkeypatch, "investigate", _Recorder())

    result = investigate(_state())

    assert result["investigator_path"] == "harness"
    assert result["investigation"] == "from the harness"
    # Carried, so the Reviewer and the casebook see one shape on both paths,
    # even though the harness prompt itself is untouched (D13).
    assert result["reason_code_doc"]["outcome"] == "hit"


def test_a_harness_failure_falls_back_and_says_it_went_direct(monkeypatch,
                                                              tmp_path):
    """The two paths are compared on their casebooks, so a silent fallback
    recorded as `harness` would score the direct LLM as the harness."""
    _docs_on(monkeypatch)
    _harness_on(monkeypatch, tmp_path, None, fail=True)
    agent = _Recorder()
    investigate = _node(monkeypatch, "investigate", agent)

    result = investigate(_state())

    assert result["investigator_path"] == "direct"
    assert "fx-enrolment" in agent.last


def test_a_harness_review_records_its_path(monkeypatch, tmp_path):
    _harness_on(monkeypatch, tmp_path,
                {"verdict": "APPROVED", "feedback": ""})
    review = _node(monkeypatch, "review", _Recorder("APPROVED"))

    assert review(_state(investigation="x"))["reviewer_path"] == "harness"


# ---------------------------------------------------------------------------
# The Reviewer with the evidence (Phase 5).
# ---------------------------------------------------------------------------

def test_the_reviewer_gets_the_evidence_by_default(monkeypatch):
    monkeypatch.delenv("REJECTION_REVIEWER_EVIDENCE", raising=False)
    agent = _Recorder("APPROVED")
    review = _node(monkeypatch, "review", agent)

    review(_state(investigation="the findings"))
    prompt = agent.last

    for fragment in ("THE RULE", json.dumps(orch._project_payload(PAYLOAD)),
                     BASE_STATE["logs"], "the findings"):
        assert fragment in prompt
    assert "'APPROVED' or 'REJECTED' on the first line" in prompt


def test_the_reviewer_gets_the_document_the_investigator_had(monkeypatch):
    monkeypatch.delenv("REJECTION_REVIEWER_EVIDENCE", raising=False)
    _docs_on(monkeypatch)
    agent = _Recorder("APPROVED")
    review = _node(monkeypatch, "review", agent)

    review(_state(investigation="the findings",
                  reason_code_doc=rcd.lookup("FIXTURE_TYPED_CODE", "E",
                                             root=FIXTURE_ROOT)))

    assert "fx-enrolment" in agent.last


def test_the_reviewer_sees_no_documentation_section_when_it_is_off(monkeypatch):
    monkeypatch.delenv("REJECTION_REVIEWER_EVIDENCE", raising=False)
    monkeypatch.delenv("REJECTION_REASON_CODE_DOCS_ENABLED", raising=False)
    agent = _Recorder("APPROVED")
    review = _node(monkeypatch, "review", agent)

    review(_state(investigation="x", reason_code_doc={"outcome": "disabled"}))

    assert ctx.DOCUMENTATION not in agent.last


def test_an_approved_verdict_still_routes_to_synthesis(monkeypatch):
    monkeypatch.delenv("REJECTION_REVIEWER_EVIDENCE", raising=False)
    review = _node(monkeypatch, "review", _Recorder("APPROVED"))

    result = review(_state(investigation="x"))
    assert orch.is_reviewer_approved(result["reviewer_feedback"])


# ---------------------------------------------------------------------------
# Oversized logs are trimmed, and the trim is counted, on both nodes.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("node,expected", [("investigate", "investigator"),
                                           ("review", "reviewer")])
def test_an_oversized_trace_is_trimmed_and_counted(monkeypatch, node, expected):
    _docs_on(monkeypatch)
    monkeypatch.delenv("REJECTION_REVIEWER_EVIDENCE", raising=False)
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "6000")

    seen = []

    class _Counter:
        def labels(self, **kwargs):
            seen.append(kwargs)
            return self

        def inc(self, *_a, **_k):
            pass

    monkeypatch.setattr(metrics, "REJECTION_PROMPT_TRIMS", _Counter())
    agent = _Recorder("APPROVED")
    run = _node(monkeypatch, node, agent)

    run(_state(logs="x" * 60000, investigation="the findings"))

    assert seen == [{"node": expected}]
    assert "characters omitted from the middle of this trace" in agent.last


# ---------------------------------------------------------------------------
# Synthesis guidance (Phase 7).
# ---------------------------------------------------------------------------

def _guided_state(**overrides):
    return _state(investigation="the approved findings",
                  reason_code_doc=rcd.lookup("FIXTURE_ANY_CODE", "E",
                                             root=FIXTURE_ROOT),
                  **overrides)


def test_synthesis_gets_no_guidance_with_the_switch_off(monkeypatch):
    monkeypatch.delenv("REJECTION_SYNTHESIS_DOC_GUIDANCE", raising=False)
    agent = _Recorder('{"rejection_description": "d", "synthesis": "s", '
                      '"action": "REPLAY", "resident_action": "PENDING"}')
    synthesize = _node(monkeypatch, "synthesize", agent)

    synthesize(_guided_state())

    assert agent.last == ("Create the final JSON casebook based strictly on "
                          "this approved investigation:\nthe approved findings")


def test_synthesis_gets_the_guidance_when_asked(monkeypatch):
    monkeypatch.setenv("REJECTION_SYNTHESIS_DOC_GUIDANCE", "true")
    agent = _Recorder('{"rejection_description": "d", "synthesis": "s", '
                      '"action": "REPLAY", "resident_action": "PENDING"}')
    synthesize = _node(monkeypatch, "synthesize", agent)

    synthesize(_guided_state())
    prompt = agent.last

    assert "### Resolution guidance from the reason code documentation" in prompt
    assert ("- action: REPLAY | resident_action: PENDING | when: the "
            "downstream service has recovered") in prompt
    assert "unless the approved investigation shows this packet does not fit" \
        in prompt


@pytest.mark.parametrize("doc_state", [
    None,
    {"outcome": "disabled"},
    {"outcome": "miss", "resolution_guidance": []},
    {"outcome": "hit", "resolution_guidance": []},
])
def test_nothing_is_added_when_there_is_no_guidance(monkeypatch, doc_state):
    """A header with nothing under it reads as an empty recommendation
    rather than as an absent one."""
    monkeypatch.setenv("REJECTION_SYNTHESIS_DOC_GUIDANCE", "true")
    assert orch._synthesis_doc_guidance(doc_state) == ""


# ---------------------------------------------------------------------------
# State, provenance and outcomes (Phases 4 and 6).
# ---------------------------------------------------------------------------

def test_the_new_state_keys_are_declared_on_the_graph_state():
    """LangGraph carries only what GraphState declares, so an undeclared key
    is dropped between nodes without an error."""
    declared = set(orch.GraphState.__annotations__)
    assert {"reason_code_doc", "investigator_path",
            "reviewer_path"} <= declared


def test_the_casebook_provenance_carries_the_document_but_never_its_text():
    from src.utils import reason_code_docs

    state = rcd.lookup("FIXTURE_TYPED_CODE", "E", root=FIXTURE_ROOT)
    provenance = {
        "prompt_fingerprint": "sha256:x",
        "reason_code_doc": reason_code_docs.provenance(state),
        "investigator_path": "direct",
        "reviewer_path": "direct",
    }

    # The refs stay: they are identifiers, and a claim in an investigation has
    # to be traceable to the entry it came from. What must not be here is the
    # prose -- it is large, identical for every packet with this reason code,
    # and the sha256 already says which version the model was shown.
    assert provenance["reason_code_doc"]["sha256"] == state["sha256"]
    assert {ref["ref"] for ref in provenance["reason_code_doc"]["refs"]} == \
        {"9002", "fx-enrolment"}

    written = json.dumps(provenance)
    for prose in ("Fires when:", "the enrolment type is 'ENROLMENT'",
                  "A code entry that sits alongside typed rules"):
        assert prose not in written, \
            "the document text must never be written into a casebook"


def test_a_runbook_answered_packet_records_no_path():
    """It never reaches the Investigator, so `None` is the honest record."""
    from src.utils import reason_code_docs

    result = {}
    assert reason_code_docs.provenance(result.get("reason_code_doc")) is None
    assert result.get("investigator_path") is None


def test_the_outcome_record_denormalises_the_document_fields(tmp_path,
                                                             monkeypatch):
    from src.storage import factory
    from src.storage.local import LocalFilesystemCasebookStorage
    from src.utils import outcomes

    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(factory, "get_casebook_storage", lambda: storage)
    monkeypatch.setattr(outcomes, "get_casebook_storage", lambda: storage)

    storage.save("evt-1", {
        "packet_metadata": {"update_type": "E"},
        "packet_status": {"rejection_data": {"rejection_code": "FIXTURE_TYPED_CODE"}},
        "resolution": {
            "source": "agent", "action": "REPLAY",
            "provenance": {"prompt_fingerprint": "sha256:p",
                           "investigator_path": "direct",
                           "reason_code_doc": {"outcome": "hit",
                                               "sha256": "sha256:d"}},
        },
    })

    record = outcomes.record_outcome("evt-1", "CORRECT", "tester")

    assert record["reason_code_doc_outcome"] == "hit"
    assert record["reason_code_doc_sha256"] == "sha256:d"
    assert record["investigator_path"] == "direct"


def test_an_older_casebook_denormalises_to_none(tmp_path, monkeypatch):
    """Three levels can be absent at once: no provenance, no document, or a
    packet a runbook answered."""
    from src.storage import factory
    from src.storage.local import LocalFilesystemCasebookStorage
    from src.utils import outcomes

    storage = LocalFilesystemCasebookStorage(base_dir=str(tmp_path))
    monkeypatch.setattr(factory, "get_casebook_storage", lambda: storage)
    monkeypatch.setattr(outcomes, "get_casebook_storage", lambda: storage)
    storage.save("evt-2", {"resolution": {"source": "runbook:rb-1@v1"}})

    record = outcomes.record_outcome("evt-2", "CORRECT", "tester")

    assert record["reason_code_doc_outcome"] is None
    assert record["reason_code_doc_sha256"] is None
    assert record["investigator_path"] is None


# ---------------------------------------------------------------------------
# _reason_code_of: the extraction the lookup and the rule share.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("payload,expected", [
    ({}, None),
    ({"packetExecutionSummary": None}, None),
    ({"packetExecutionSummary": {"errorData": None}}, None),
    ({"packetExecutionSummary": {"errorData": []}}, None),
    ({"packetExecutionSummary": {"errorData": [None]}}, None),
    ({"packetExecutionSummary": {"errorData": [{"errorReasonCode": ""}]}}, None),
    ({"packetExecutionSummary": {"errorData": [{"errorReasonCode": "A"}]}}, "A"),
    ({"packetExecutionSummary": {"errorData": [
        {"errorReasonCode": None}, {"errorReasonCode": "B"}]}}, "B"),
])
def test_reason_code_extraction_tolerates_none_at_every_level(payload, expected):
    assert orch._reason_code_of(payload) == expected


# ---------------------------------------------------------------------------
# The Reviewer's verdict: format, and being able to see it.
# ---------------------------------------------------------------------------
#
# On 2026-09-24 the Reviewer rejected three times out of three on every run,
# escalating every packet. Nothing recorded what it objected to, so the loop
# was undiagnosable from the logs alone.

@pytest.mark.parametrize("reply,approved", [
    ("APPROVED", True),
    ("APPROVED\n\nThe investigation cites the decision line correctly.", True),
    ("  APPROVED  \nreasoning follows", True),
    ("\n\nAPPROVED\nreasoning", True),          # a leading blank line
    ("**APPROVED**\nreasoning", True),
    ("REJECTED\nThe candidate count is unsupported.", False),
    ("The investigation is sound. APPROVED", False),   # verdict not first
    ("NOT APPROVED", False),
    ("", False),
    ("\n \n", False),
])
def test_only_a_verdict_on_the_first_line_approves(reply, approved):
    """A react agent's final message can open with a blank line or a fence, so
    the first NON-EMPTY line is the verdict. Reading further would let a
    discussion of what approval requires count as an approval."""
    assert orch.is_reviewer_approved(reply) is approved


def test_the_reviewer_logs_what_it_decided(monkeypatch, caplog):
    """Without the verdict text a rejection loop cannot be diagnosed: the only
    other copy is in an escalation casebook, which exists only after the loop
    has already burned every retry."""
    import logging

    monkeypatch.delenv("REJECTION_REVIEWER_EVIDENCE", raising=False)
    review = _node(monkeypatch, "review",
                   _Recorder("REJECTED\nThe score is not in the logs."))

    with caplog.at_level(logging.INFO):
        review(_state(investigation="the findings"))

    assert "The score is not in the logs." in caplog.text


def test_an_escalation_still_carries_the_verdict_to_the_casebook(monkeypatch):
    escalate = _node(monkeypatch, "escalate", _Recorder())
    result = escalate(_state(investigation="the findings",
                             reviewer_feedback="REJECTED: unsupported claim",
                             retry_count=3))

    assert "unsupported claim" in json.loads(result["synthesis"])["rejection_description"]


def test_the_reviewer_prompt_states_the_output_contract_before_the_policy():
    """`load_prompt` appends ~5KB of business policy AFTER this file, so an
    output contract written at the bottom is the one thing the model reads
    least recently."""
    text = (paths.REPO_ROOT / "src" / "prompts" / "ReviewerAgent.md").read_text(
        encoding="utf-8")
    contract = text.index("FIRST line of your reply")
    assert contract < len(text) / 2, "the output contract has drifted down the file"
    assert "APPROVED" in text and "REJECTED" in text


def test_the_reviewer_prompt_says_when_to_approve():
    """Seven REJECT criteria and no stated grounds for approval is a prompt
    that only knows how to say no."""
    text = (paths.REPO_ROOT / "src" / "prompts" / "ReviewerAgent.md").read_text(
        encoding="utf-8")
    assert "WHEN TO APPROVE" in text
    assert "Do NOT reject for any of these" in text


def test_the_investigator_prompt_defers_to_the_provenance_note():
    """It used to say the database rule always wins. That is wrong when the
    rule comes from a staging copy, and wrong again for a code the rule engine
    never raises."""
    text = (paths.REPO_ROOT / "src" / "prompts" / "InvestigatorAgent.md").read_text(
        encoding="utf-8")
    # Matched on unwrapped text: the file is hard-wrapped, so a phrase can
    # carry a newline and an indent in the middle of it.
    unwrapped = " ".join(text.split())
    assert "Provenance:" in unwrapped
    assert "rule is what actually fired" not in unwrapped
    assert "will never have a database rule at all" in unwrapped
    assert "a missing rule is the expected result" in unwrapped
