"""
utils/tracing.py -- Langfuse tracing of the LLM lanes.

Off by default, and then invisible: an agent is invoked exactly as before,
without even a config argument. On, these tests hand the real SDK an
in-memory exporter (its own `span_exporter` hook), behind the same export
filter and masking hook a real export goes through -- so what they assert
on is exactly what would leave the process.
"""
import json
import uuid
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from src.core.run_control import RunCancelled
from src.utils import tracing
# The rejection lane's real storage and graph, with only the LLM stubbed.
from test_end_to_end import _run, _stub_llm, storage  # noqa: F401  (storage: a fixture)

#: A 12-digit eventId: the shape of an Aadhaar number, so it survives only
#: because the trace allowlists its own session.
EVENT_ID = "202610070001"
AADHAAR = "2345 6789 0123"
MOBILE = "9876543210"
RESIDENT = "Ramesh Kumar"


@pytest.fixture
def exported(monkeypatch):
    """Tracing on, into memory. Yields a function that flushes and returns
    the spans exported so far."""
    from langfuse._client.resource_manager import LangfuseResourceManager

    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", f"pk-lf-test-{uuid.uuid4().hex[:8]}")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-lf-test")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://127.0.0.1:9")
    exporter = InMemorySpanExporter()
    build = tracing._build_client
    monkeypatch.setattr(tracing, "_build_client", lambda: build(span_exporter=exporter))
    monkeypatch.setattr(tracing, "_client", None)
    monkeypatch.setattr(tracing, "_client_failed", False)

    def spans():
        tracing.flush()
        return list(exporter.get_finished_spans())

    yield spans
    tracing.shutdown()
    LangfuseResourceManager.reset()


def _type(span):
    return span.attributes.get("langfuse.observation.type")


def _children(spans, parent):
    return sorted((s for s in spans if s.parent and s.parent.span_id == parent.context.span_id),
                  key=lambda s: s.start_time)


def _named(spans, name):
    found = [s for s in spans if s.name == name]
    assert found, f"no span named {name!r} in {[s.name for s in spans]}"
    return found


def _everything_exported(spans) -> str:
    return json.dumps([{key: str(value) for key, value in s.attributes.items()} for s in spans])


class _Request:
    """An agent whose invoke takes the request alone, as the fakes elsewhere
    in the suite do."""

    def __init__(self):
        self.calls = []

    def invoke(self, request):
        self.calls.append(request)
        return {"messages": [AIMessage(content="done")]}


class ScriptedModel(BaseChatModel):
    """Replies from a list, reporting a model name and token usage."""

    replies: list

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        message = self.replies.pop(0)
        message.usage_metadata = {"input_tokens": 120, "output_tokens": 30, "total_tokens": 150}
        return ChatResult(generations=[ChatGeneration(message=message)],
                          llm_output={"model_name": "glm-5.2-fp8"})

    def bind_tools(self, tools, **kwargs):
        return self

    @property
    def _llm_type(self):
        return "scripted"


@tool
def lookup_resident(event_id: str) -> str:
    """Look a packet's resident up."""
    return json.dumps({"eventId": event_id, "name": RESIDENT, "dob": "1990-01-01",
                       "note": f"aadhaar {AADHAAR}, mobile {MOBILE}"})


# ======================================================================
# Off
# ======================================================================

def test_off_without_keys_and_then_invisible():
    assert not tracing.enabled()
    agent = _Request()
    with tracing.run("analyze-rejection", input={"event_id": EVENT_ID}) as trace:
        assert not trace.active
        tracing.invoke(agent, {"messages": []})
    # The call is the one the agent always got: no config argument.
    assert agent.calls == [{"messages": []}]
    assert tracing.node("investigate", lambda state: {"x": 1})({}) == {"x": 1}
    with tracing.harness_task("investigator", prompt="p", model="m") as recorder:
        recorder.add({"type": "step_start"})
        recorder.finish({"result": {}})


def test_keys_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-x")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-lf-x")
    assert tracing.enabled()
    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "false")
    assert not tracing.enabled()


def test_a_client_that_cannot_start_runs_untraced(monkeypatch):
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-x")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-lf-x")
    monkeypatch.setattr(tracing, "_client", None)
    monkeypatch.setattr(tracing, "_client_failed", False)

    def broken():
        raise RuntimeError("no exporter")

    monkeypatch.setattr(tracing, "_build_client", broken)
    agent = _Request()
    with tracing.run("analyze-rejection") as trace:
        assert not trace.active
        tracing.invoke(agent, {"messages": []})
    assert agent.calls == [{"messages": []}]


@pytest.mark.parametrize("raw, expected", [
    ("prod", "prod"), ("Dev", "dev"), ("staging env", "staging-env"),
    ("langfuse", "env-langfuse"),
])
def test_the_environment_is_one_langfuse_accepts(monkeypatch, raw, expected):
    monkeypatch.delenv("LANGFUSE_TRACING_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ENV", raw)
    assert tracing._environment() == expected


def test_the_release_defaults_to_the_version_file(monkeypatch):
    monkeypatch.delenv("LANGFUSE_RELEASE", raising=False)
    with open(tracing.VERSION_FILE, encoding="utf-8") as handle:
        assert tracing._release() == json.load(handle)["version"]
    monkeypatch.setenv("LANGFUSE_RELEASE", "abc123")
    assert tracing._release() == "abc123"


@pytest.mark.parametrize("name, dropped", [
    ("TodoListMiddleware.after_model", True),
    ("PatchToolCallsMiddleware.before_agent", True),
    ("ModelCallLimitMiddleware.before_model", True),
    ("SummarizationMiddleware.before_model", False),
    ("investigator-agent", False),
    ("model", False),
])
def test_middleware_hooks_are_noise(name, dropped):
    assert tracing._is_middleware_hook(name) is dropped


# ======================================================================
# Masking
# ======================================================================

def test_plain_text_is_redacted_but_the_traces_own_id_survives():
    text = (f"event {EVENT_ID}: aadhaar {AADHAAR}, vid 1234567890123456, "
            f"mobile {MOBILE}, mail someone@example.com")
    masked = tracing.mask_text(text, allowlist=[EVENT_ID])
    assert EVENT_ID in masked
    for value in (AADHAAR, "1234567890123456", MOBILE, "someone@example.com"):
        assert value not in masked
    assert "[REDACTED:AADHAAR]" in masked and "[REDACTED:MOBILE]" in masked


def test_json_keeps_tool_and_agent_names_and_redacts_residents():
    document = [
        {"role": "user", "name": "investigator",
         "content": f'Kafka Payload: {{"name": "{RESIDENT}", "dob": "1990-01-01"}}'},
        {"role": "assistant", "content": "",
         "tool_calls": [{"name": "lookup_resident", "args": {"event_id": EVENT_ID},
                         "id": "c1", "type": "tool_call"},
                        {"id": "c2", "type": "function",
                         "function": {"name": "grep", "arguments": "{}"}}]},
        {"role": "tool", "name": "lookup_resident", "content": "ok"},
        {"resident": {"name": RESIDENT, "gender": "M", "address": {"city": "Pune"}}},
    ]
    masked = json.loads(tracing.mask_text(json.dumps(document), allowlist=[EVENT_ID]))
    assert masked[0]["name"] == "investigator"
    assert RESIDENT not in masked[0]["content"] and "1990-01-01" not in masked[0]["content"]
    assert masked[1]["tool_calls"][0]["name"] == "lookup_resident"
    assert masked[1]["tool_calls"][0]["args"]["event_id"] == EVENT_ID
    assert masked[1]["tool_calls"][1]["function"]["name"] == "grep"
    assert masked[2]["name"] == "lookup_resident"
    assert masked[3]["resident"] == {"name": "[REDACTED:JSON_FIELD]",
                                     "gender": "[REDACTED:JSON_FIELD]",
                                     "address": "[REDACTED:JSON_FIELD]"}


def test_an_attribute_that_cannot_be_masked_is_not_sent(monkeypatch):
    from langfuse.types import MaskOtelSpansParams, OtelSpanData, OtelSpanIdentifier

    def broken(text, allowlist=None):
        raise ValueError("boom")

    monkeypatch.setattr(tracing, "mask_text", broken)
    identifier = OtelSpanIdentifier(trace_id="0" * 32, span_id="0" * 16)
    span = MagicMock(spec=OtelSpanData)
    span.attributes = {"langfuse.observation.input": f"aadhaar {AADHAAR}",
                       "langfuse.observation.type": "span"}
    result = tracing.mask_spans(params=MaskOtelSpansParams(spans={identifier: span}))
    patch_ = result.span_patches[identifier]
    assert patch_ is not None
    assert patch_.set_attributes == {"langfuse.observation.input": "[REDACTED:MASKING_FAILED]"}


# ======================================================================
# On: what a run exports
# ======================================================================

def test_an_agent_run_is_one_trace_nested_typed_and_masked(exported):
    from src.core.agent_factory import build_agent

    model = ScriptedModel(replies=[
        AIMessage(content="", tool_calls=[{"name": "lookup_resident",
                                           "args": {"event_id": EVENT_ID}, "id": "c1"}]),
        AIMessage(content="The packet was rejected by the dedup rule."),
    ])
    agent = build_agent("investigator", model, "Investigate.", tools=[lookup_resident],
                        pack="enu-biometric")

    def investigate(state):
        result = tracing.invoke(agent, {"messages": [HumanMessage(
            content=f"Packet {EVENT_ID}, resident aadhaar {AADHAAR}")]})
        return {"investigation": result["messages"][-1].content}

    with tracing.run("analyze-rejection", input={"event_id": EVENT_ID}, session_id=EVENT_ID,
                     tags=["lane:rejection"], metadata={"event_id": EVENT_ID},
                     version="sha256:abc") as trace:
        update = tracing.node("investigate", investigate)({})
        trace.update(output={"resolution": "ok"})
    assert update == {"investigation": "The packet was rejected by the dedup rule."}

    spans = exported()
    (root,) = _named(spans, "analyze-rejection")
    assert _type(root) == "chain" and root.parent is None
    (node,) = _children(spans, root)
    assert (node.name, _type(node)) == ("investigate", "span")
    (agent_span,) = _children(spans, node)
    assert (agent_span.name, _type(agent_span)) == ("investigator-agent", "agent")

    generations = [s for s in spans if _type(s) == "generation"]
    assert len(generations) == 2
    assert all(s.attributes["langfuse.observation.model.name"] == "glm-5.2-fp8"
               for s in generations)
    assert json.loads(generations[0].attributes["langfuse.observation.usage_details"]) \
        == {"input": 120, "output": 30, "total": 150}
    requested = json.loads(generations[0].attributes["langfuse.observation.output"])
    assert requested["tool_calls"][0]["name"] == "lookup_resident"
    assert [s.name for s in spans if _type(s) == "tool"] == ["lookup_resident"]

    # Every observation carries the session, and no middleware hook is sent.
    assert {s.attributes.get("session.id") for s in spans} == {EVENT_ID}
    assert not [s.name for s in spans if tracing._is_middleware_hook(s.name)]

    # Nothing personal leaves the process; the eventId does.
    sent = _everything_exported(spans)
    for value in (AADHAAR, MOBILE, RESIDENT, "1990-01-01"):
        assert value not in sent
    assert EVENT_ID in sent


def test_a_failed_run_is_an_error_and_an_abandoned_one_a_warning(exported):
    with pytest.raises(ValueError):
        with tracing.run("analyze-rejection", session_id=EVENT_ID):
            raise ValueError("graph blew up")
    with pytest.raises(RunCancelled):
        with tracing.run("analyze-dlt", session_id="case-1"):
            raise RunCancelled("budget spent")

    spans = exported()
    (failed,) = _named(spans, "analyze-rejection")
    assert failed.attributes["langfuse.observation.level"] == "ERROR"
    assert "graph blew up" in failed.attributes["langfuse.observation.status_message"]
    (abandoned,) = _named(spans, "analyze-dlt")
    assert abandoned.attributes["langfuse.observation.level"] == "WARNING"


#: An opencode task's stream: two model calls around two tool calls, one of
#: which arrives twice -- as the real stream can repeat a completed part.
_STREAM = [
    {"type": "step_start", "part": {"type": "step-start"}},
    {"type": "text", "part": {"type": "text", "text": "Reading the case files."}},
    {"type": "tool_use", "part": {"tool": "read", "callID": "c1",
                                  "state": {"status": "running", "input": {}}}},
    {"type": "tool_use", "part": {"tool": "read", "callID": "c1",
                                  "state": {"status": "completed",
                                            "input": {"filePath": "context.json"},
                                            "output": f"resident mobile {MOBILE}"}}},
    {"type": "tool_use", "part": {"tool": "read", "callID": "c1",
                                  "state": {"status": "completed",
                                            "input": {"filePath": "context.json"},
                                            "output": f"resident mobile {MOBILE}"}}},
    {"type": "tool_use", "part": {"tool": "grep", "callID": "c2",
                                  "state": {"status": "error", "input": {"pattern": "X"},
                                            "error": "no such file"}}},
    {"type": "step_finish", "part": {"reason": "tool-calls", "cost": 0.5,
                                     "tokens": {"input": 4211, "output": 37, "reasoning": 5,
                                                "cache": {"read": 64, "write": 8}}}},
    {"type": "step_start", "part": {"type": "step-start"}},
    {"type": "text", "part": {"type": "text", "text": "Done."}},
    {"type": "step_finish", "part": {"reason": "stop", "cost": 0.25,
                                     "tokens": {"input": 4390, "output": 9, "reasoning": 0,
                                                "cache": {"read": 0, "write": 0}}}},
]


def test_a_harness_task_is_rebuilt_as_its_agent(exported):
    with tracing.run("analyze-rejection", session_id=EVENT_ID):
        with tracing.harness_task("investigator", prompt="Investigate the case.",
                                  model="uidai/glm-5.2-fp8") as recorder:
            for event in _STREAM:
                recorder.add(event)
            recorder.finish({"result": {"investigation": "found it"}, "seconds": 3.2,
                             "output_path": "/tmp/investigation.json",
                             "trace": {"session_id": "ses_1", "llm_calls": 2}})
            recorder.add({"type": "step_start"})  # after the task closed: ignored

    spans = exported()
    (agent,) = _named(spans, "investigator-agent")
    assert _type(agent) == "agent"
    assert json.loads(agent.attributes["langfuse.observation.output"]) == \
        {"investigation": "found it"}
    assert agent.attributes["langfuse.observation.metadata.path"] == "harness"

    children = _children(spans, agent)
    assert [(s.name, _type(s)) for s in children] == [
        ("harness-model-call", "generation"), ("read", "tool"), ("grep", "tool"),
        ("harness-model-call", "generation")]
    first, read, grep, second = children
    assert first.attributes["langfuse.observation.model.name"] == "glm-5.2-fp8"
    assert json.loads(first.attributes["langfuse.observation.usage_details"]) == {
        "input": 4211, "output": 37, "input_cache_read": 64, "input_cache_write": 8}
    assert json.loads(first.attributes["langfuse.observation.cost_details"]) == {"total": 0.5}
    assert json.loads(first.attributes["langfuse.observation.input"]) == [
        {"role": "user", "content": "Investigate the case."}]
    output = json.loads(first.attributes["langfuse.observation.output"])
    assert output["content"] == "Reading the case files."
    assert [call["function"]["name"] for call in output["tool_calls"]] == ["read", "grep"]
    assert grep.attributes["langfuse.observation.level"] == "ERROR"
    # The second call was given the first one's tool results.
    given = json.loads(second.attributes["langfuse.observation.input"])
    assert [message["name"] for message in given] == ["read", "grep"]
    assert MOBILE not in _everything_exported(spans)


def test_a_failed_harness_task_marks_its_agent(exported):
    with tracing.run("analyze-dlt", session_id="case-1"):
        with pytest.raises(RuntimeError):
            with tracing.harness_task("dlt_investigator", prompt="p", model="m") as recorder:
                recorder.add(_STREAM[0])
                raise RuntimeError("task wrote no output")

    spans = exported()
    (agent,) = _named(spans, "dlt-investigator-agent")
    assert agent.attributes["langfuse.observation.level"] == "ERROR"
    assert "task wrote no output" in agent.attributes["langfuse.observation.status_message"]


# ======================================================================
# On: the lanes
# ======================================================================

@pytest.mark.usefixtures("storage")
def test_a_rejection_packet_is_one_trace_of_its_nodes(exported, monkeypatch):
    """Through POST /process-rejection: the trace opens on the worker the
    graph runs on, and every node lands under it."""
    import src.core.agent_orchestrator as orch

    synthesis = json.dumps({"rejection_description": "dedup rejected the packet",
                            "synthesis": "resubmit", "action": "RESIDENT_PACKET_RESUBMIT",
                            "resident_action": "NEW_PACKET", "confidence": 0.9})
    with patch.object(orch, "build_agent", side_effect=lambda *a, **k: _stub_llm(synthesis)), \
            patch.object(orch, "get_llm", side_effect=lambda tier: MagicMock()):
        monkeypatch.setattr(orch, "is_reviewer_approved", lambda _f: True)
        assert _run(EVENT_ID)["status"] == "processed"

    spans = exported()
    (root,) = _named(spans, "analyze-rejection")
    assert root.attributes["session.id"] == EVENT_ID
    assert "lane:rejection" in root.attributes["langfuse.trace.tags"]
    assert json.loads(root.attributes["langfuse.observation.input"])["reason_code"] == \
        "RESIDENT_MAN_DEDUP_REJECT_TD"
    output = json.loads(root.attributes["langfuse.observation.output"])
    assert output["resolution"]["action"] == "RESIDENT_PACKET_RESUBMIT"
    assert root.attributes["langfuse.version"].startswith("sha256:")
    assert [(s.name, _type(s)) for s in _children(spans, root)] == [
        ("fetch-logs", "retriever"), ("lookup-runbook", "retriever"),
        ("investigate", "span"), ("review", "evaluator"), ("synthesize", "span")]


def test_a_dlt_case_is_one_trace_of_its_nodes(exported, monkeypatch):
    from src.dlt import orchestrator
    from src.models.dlt_synthesis import DltFinding

    finding = DltFinding(narrative="An absent row is fatal.", recommendation="Fix the row.",
                         action="DATA_FIX_REQUIRED", confidence=0.9)
    agent = MagicMock()
    agent.invoke.return_value = {"messages": [AIMessage(content="APPROVED")]}
    monkeypatch.setattr(orchestrator, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(orchestrator, "get_llm", lambda tier: MagicMock())
    monkeypatch.setattr(orchestrator, "parse_finding", lambda text: (finding, None))
    orchestrator.reset_agent_cache()
    try:
        result, error = orchestrator.investigate(
            "case-1", {"failure_class": "A", "root_fqcn": "java.lang.IllegalStateException",
                       "frames": []},
            MagicMock(verdict=MagicMock(value="CORROBORATED"), reason="", unexplained=()), "")
    finally:
        orchestrator.reset_agent_cache()
    # The lane stamps the Reviewer's verdict on the finding (reviewer_approved).
    assert error is None
    assert result == finding.model_copy(update={"reviewer_approved": True})

    spans = exported()
    (root,) = _named(spans, "analyze-dlt")
    assert root.attributes["session.id"] == "case-1"
    assert json.loads(root.attributes["langfuse.observation.output"])["finding"]["action"] \
        == "DATA_FIX_REQUIRED"
    assert [(s.name, _type(s)) for s in _children(spans, root)] == [
        ("investigate", "span"), ("review", "evaluator"), ("synthesize", "span")]
