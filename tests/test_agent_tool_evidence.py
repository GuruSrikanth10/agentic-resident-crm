"""
Tool evidence through both lanes: what the Investigator's tools return reaches
the Reviewer (direct and harness), a retry, the casebook's provenance and the
prompt fingerprint -- and a packet investigated without tools gets exactly
the prompts it got before tools existed.

The agents are stubs whose `invoke` calls a tool over MCP -- served by the
real tool server (the `tool_server` fixture) -- the way a deep agent's tool
node would, inside the node's recording.
"""
import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
import src.dlt.orchestrator as dlt
from src.core import rejection_context
from src.tools import agent_tools, mcp_client
from src.tools.agent_tools import Toolset, agent_tool

REFID = "3f2b9c1e-0d4a-4a8e-9b1f-6c7d8e9f0a1b"


@pytest.fixture
def probe(tool_server):
    """A tool served over MCP to the investigator roles, removed afterwards.
    Returns its name."""
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    toolset = Toolset(name="evidence_probe", agents=("investigator", "dlt_investigator"),
                      services=("*",))

    @agent_tool(toolset)
    def evidence_probe(refid: str) -> str:
        """Probe."""
        return json.dumps({"refid": refid, "parked_now": False})

    tool_server()
    yield "evidence_probe"
    agent_tools._registry.clear()
    agent_tools._registry.update(saved)


class ToolUsingAgent:
    """Calls `tool_name` over MCP once per invoke, then answers; keeps its
    prompts."""

    def __init__(self, tool_name=None, reply="the findings", role="investigator",
                 service="enu-biometric"):
        self.tool_name = tool_name
        self.reply = reply
        self.role = role
        self.service = service
        self.prompts = []

    def invoke(self, request):
        self.prompts.append(request["messages"][-1].content)
        if self.tool_name:
            tool = next(tool for tool in mcp_client.tools_for(self.role, self.service)
                        if tool.name == self.tool_name)
            tool.invoke({"refid": REFID})
        return {"messages": [AIMessage(content=self.reply)]}


def rejection_node(monkeypatch, name, agent):
    monkeypatch.setattr(orch, "_agent", None)
    # Restored after the test too: a catalog left behind from a server that
    # is gone would be stale for every later test's graph.
    monkeypatch.setattr(orch, "_agent_catalog", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    return orch._build_agent().builder.nodes[name].runnable.func


def dlt_node(monkeypatch, name, agent):
    monkeypatch.setattr(dlt, "_agent", None)
    # Restored after the test too: a catalog left behind from a server that
    # is gone would be stale for every later test's graph.
    monkeypatch.setattr(dlt, "_agent_catalog", None)
    monkeypatch.setattr(dlt, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(dlt, "build_agent", lambda *a, **k: agent)
    return dlt._build_dlt_agent().builder.nodes[name].runnable.func


REJECTION_STATE = {
    "payload": {"eventId": "evt-tools",
                "packetMetaData": {"enrolmentType": "U", "refId": REFID},
                "packetExecutionSummary": {"errorReasonCode": "BIO_PARKING_QUEUE_STORE_DATA_NOT_FOUND"},
                "flowMetaData": {"stage": "BIO"}},
    "logs": "Log fetching disabled.",
    "db_rule": "Rule text.",
}

RECORD = {"tool": "evidence_probe", "args": {"refid": REFID},
          "result": json.dumps({"refid": REFID, "parked_now": False})}


# ----------------------------------------------------------------------
# Rejection lane
# ----------------------------------------------------------------------

def test_investigator_records_its_tool_calls_and_keeps_them(monkeypatch, probe):
    agent = ToolUsingAgent(probe)
    storage = MagicMock()
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: storage)
    node = rejection_node(monkeypatch, "investigate", agent)

    out = node(dict(REJECTION_STATE))

    assert out["tool_evidence"] == [RECORD]
    event_id, filename, content = storage.save_artifact.call_args.args
    assert (event_id, filename) == ("evt-tools", orch.TOOL_EVIDENCE_ARTIFACT)
    assert json.loads(content) == [RECORD]


def test_investigator_without_tool_calls_saves_nothing(monkeypatch):
    storage = MagicMock()
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: storage)
    node = rejection_node(monkeypatch, "investigate", ToolUsingAgent())

    out = node(dict(REJECTION_STATE))

    assert out["tool_evidence"] == []
    storage.save_artifact.assert_not_called()


def test_a_retry_is_shown_the_earlier_tool_results_and_keeps_them(monkeypatch, probe):
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: MagicMock())
    agent = ToolUsingAgent()  # the retry itself calls no tool
    node = rejection_node(monkeypatch, "investigate", agent)
    state = dict(REJECTION_STATE, investigation="previous analysis",
                 reviewer_feedback="REJECTED\ncite the parking row",
                 tool_evidence=[RECORD])

    out = node(state)

    assert rejection_context.TOOL_EVIDENCE in agent.prompts[-1]
    assert '"parked_now": false' in agent.prompts[-1]
    assert out["tool_evidence"] == [RECORD]


def test_the_retry_prompt_with_docs_on_carries_the_evidence(monkeypatch):
    monkeypatch.setenv("REJECTION_REASON_CODE_DOCS_ENABLED", "true")
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: MagicMock())
    agent = ToolUsingAgent()
    node = rejection_node(monkeypatch, "investigate", agent)
    node(dict(REJECTION_STATE, investigation="prev", reviewer_feedback="REJECTED\nfix",
              tool_evidence=[RECORD]))
    assert f"### {rejection_context.TOOL_EVIDENCE}" in agent.prompts[-1]


def test_the_reviewer_is_given_the_tool_results(monkeypatch):
    agent = ToolUsingAgent(reply="APPROVED")
    node = rejection_node(monkeypatch, "review", agent)

    node(dict(REJECTION_STATE, investigation="parked_now is false", tool_evidence=[RECORD]))

    prompt = agent.prompts[-1]
    assert f"### {rejection_context.TOOL_EVIDENCE}" in prompt
    assert prompt.index(rejection_context.TOOL_EVIDENCE) < prompt.index(rejection_context.INVESTIGATION)


def test_the_reviewer_prompt_is_unchanged_without_tool_results(monkeypatch):
    agent = ToolUsingAgent(reply="APPROVED")
    node = rejection_node(monkeypatch, "review", agent)
    node(dict(REJECTION_STATE, investigation="findings"))
    assert rejection_context.TOOL_EVIDENCE not in agent.prompts[-1]


def test_prompts_are_byte_identical_without_tool_evidence():
    common = dict(doc_state=None, db_rule="rule", enrolment_display="U",
                  logs="a log line")
    review = dict(common, investigation="x", payload_projection={"eventId": "e"})
    retry = dict(common, previous_investigation="p", feedback="f")
    for build, kwargs in ((rejection_context.build_review_prompt, review),
                          (rejection_context.build_retry_prompt, retry)):
        assert build(**kwargs) == build(**kwargs, tool_evidence=None) \
            == build(**kwargs, tool_evidence="")


def test_harness_case_files_carry_the_tool_evidence_and_drop_a_stale_file(tmp_path):
    orch._write_harness_case_files(tmp_path, REJECTION_STATE["payload"], "", "rule",
                                   tool_evidence="[1] evidence_probe {}")
    evidence = tmp_path / orch.TOOL_EVIDENCE_FILE
    assert evidence.read_text(encoding="utf-8") == "[1] evidence_probe {}"

    orch._write_harness_case_files(tmp_path, REJECTION_STATE["payload"], "", "rule")
    assert not evidence.exists()


def test_the_harness_reviewer_template_names_the_evidence_file():
    from src.utils.prompt_loader import render

    text = render("RejectionReviewer", event_id="e1", output_path="/tmp/out.json")
    assert f"casebook_e1/{orch.TOOL_EVIDENCE_FILE}" in text


def test_casebook_provenance_lists_the_tool_calls(monkeypatch):
    from src.api.routes import process_rejection
    from src.models.schemas import MessagePayload
    from src.storage.factory import get_casebook_storage
    from test_phase1_fixes import _cleanup_casebook, _payload_with_event_id

    event_id = "tool-provenance"
    agent = MagicMock()
    agent.get_state.return_value = None
    agent.invoke.return_value = {
        "synthesis": json.dumps({"rejection_description": "d", "synthesis": "s",
                                 "action": "MANUAL_REVIEW", "resident_action": "PENDING"}),
        "tool_evidence": [RECORD],
    }
    # cleanup_casebook_dir would delete the local backend's own copy before
    # it could be read back; this test is about what was written.
    monkeypatch.setattr("src.utils.case_cleanup.cleanup_casebook_dir", lambda _e: None)
    try:
        with patch("src.api.routes.get_agent", return_value=agent):
            asyncio.run(process_rejection(MessagePayload(**_payload_with_event_id(event_id))))
        casebook = get_casebook_storage().load(event_id)
        assert casebook["resolution"]["provenance"]["tool_calls"] == [
            {"tool": "evidence_probe", "args": {"refid": REFID}}]
    finally:
        _cleanup_casebook(event_id)


def test_prompt_fingerprint_moves_with_the_tools_served(monkeypatch, probe):
    import os

    base_dir = os.path.dirname(os.path.dirname(orch.__file__))
    served = orch.compute_prompt_fingerprint(base_dir, "enu-biometric")
    assert served == orch.compute_prompt_fingerprint(base_dir, "enu-biometric")
    monkeypatch.delenv("AGENT_MCP_SERVERS")
    assert orch.compute_prompt_fingerprint(base_dir, "enu-biometric") != served


def test_the_agents_are_built_with_their_roles_and_explicit_tools(monkeypatch):
    built = []

    def fake_build_agent(role, model, system_prompt, tools=(), pack=None):
        built.append((role, [tool.name for tool in tools], system_prompt, pack))
        return MagicMock()

    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", fake_build_agent)
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    orch._build_agent()

    assert [(role, tools) for role, tools, _, _ in built] == [
        ("investigator", []), ("log_filter", []),
        ("synthesis", ["queue_for_replay"]), ("reviewer", ["add_learning_rule"])]
    # Each pooled agent is scoped to its pack; the one LogFilter, which serves
    # every service, to the tools for every service.
    assert [(role, pack) for role, _, _, pack in built] == [
        ("investigator", "enu-biometric"), ("log_filter", "_default"),
        ("synthesis", "enu-biometric"), ("reviewer", "enu-biometric")]
    prompts = {role: prompt for role, _, prompt, _ in built}
    assert prompts["investigator"].startswith("You are the Rejection Investigator Agent.")
    assert prompts["reviewer"].startswith("You are the Reviewer Agent.")


# ----------------------------------------------------------------------
# DLT lane
# ----------------------------------------------------------------------

DLT_STATE = {"case_id": "dlt-case-1",
             "failure": {"failure_class": "A", "root_fqcn": "x.Y", "chain": [], "frames": []},
             "corroboration": {"verdict": "UNVERIFIABLE", "reason": "none"},
             "logs": ""}


def test_dlt_evidence_block_is_unchanged_without_tool_evidence():
    assert dlt._evidence_block(dict(DLT_STATE)) == \
        dlt._evidence_block(dict(DLT_STATE, tool_evidence=[]))
    assert "Evidence retrieved with tools" not in dlt._evidence_block(dict(DLT_STATE))


def test_dlt_evidence_block_carries_tool_results_to_the_reviewer(monkeypatch):
    agent = ToolUsingAgent(reply="APPROVED")
    node = dlt_node(monkeypatch, "review", agent)
    node(dict(DLT_STATE, investigation="findings", tool_evidence=[RECORD]))
    assert "### Evidence retrieved with tools" in agent.prompts[-1]


def test_dlt_investigator_records_tool_calls(monkeypatch, probe):
    node = dlt_node(monkeypatch, "investigate",
                    ToolUsingAgent(probe, role="dlt_investigator", service=None))
    out = node(dict(DLT_STATE))
    assert out["tool_evidence"] == [RECORD]


def test_dlt_agents_are_built_with_dlt_roles(monkeypatch):
    roles = []
    monkeypatch.setattr(dlt, "_agent", None)
    monkeypatch.setattr(dlt, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(dlt, "build_agent",
                        lambda role, *a, **k: roles.append(role) or MagicMock())
    dlt._build_dlt_agent()
    assert roles == ["dlt_investigator", "dlt_reviewer", "dlt_synthesis"]


# ----------------------------------------------------------------------
# Every agent runs inside a call context, for the tool call log
# ----------------------------------------------------------------------

class ContextAgent:
    """Answers with `reply`, and keeps the call context of every invoke."""

    def __init__(self, reply="not json"):
        self.reply = reply
        self.contexts = []

    def invoke(self, request):
        self.contexts.append(dict(mcp_client._call_context.get() or {}))
        return {"messages": [AIMessage(content=self.reply)]}


@pytest.mark.parametrize("node_name, role, invokes, state", [
    ("filter_logs", "log_filter", 1, {"logs": "a log line"}),
    ("investigate", "investigator", 1, {}),
    ("review", "reviewer", 1, {"investigation": "findings"}),
    # An invalid answer and its repair: two invokes, both in context.
    ("synthesize", "synthesis", 2, {"investigation": "findings"}),
])
def test_every_rejection_agent_runs_in_its_call_context(monkeypatch, node_name, role,
                                                        invokes, state):
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: MagicMock())
    agent = ContextAgent()
    node = rejection_node(monkeypatch, node_name, agent)
    node(dict(REJECTION_STATE, **state))
    assert agent.contexts == [{"role": role, "event_id": "evt-tools"}] * invokes
    assert mcp_client._call_context.get() is None


@pytest.mark.parametrize("node_name, role, invokes, state", [
    ("investigate", "dlt_investigator", 1, {}),
    ("review", "dlt_reviewer", 1, {"investigation": "findings"}),
    ("synthesise", "dlt_synthesis", 2, {"investigation": "findings"}),
])
def test_every_dlt_agent_runs_in_its_call_context(monkeypatch, node_name, role, invokes,
                                                  state):
    agent = ContextAgent()
    node = dlt_node(monkeypatch, node_name, agent)
    node(dict(DLT_STATE, **state))
    assert agent.contexts == [{"role": role, "case_id": "dlt-case-1"}] * invokes


def test_the_investigators_tool_calls_are_logged_with_its_packet(monkeypatch, probe):
    lines = []
    monkeypatch.setattr(mcp_client, "logger", MagicMock(
        info=lambda event, **fields: lines.append((event, fields))))
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: MagicMock())
    rejection_node(monkeypatch, "investigate", ToolUsingAgent(probe))(dict(REJECTION_STATE))
    [(event, fields)] = [line for line in lines if line[0] == "Agent tool call"]
    assert (fields["tool"], fields["role"], fields["event_id"], fields["outcome"]) == \
        (probe, "investigator", "evt-tools", "ok")
