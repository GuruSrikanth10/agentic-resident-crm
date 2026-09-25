"""
core/agent_factory.py -- every agent is a deep agent, with its tools over MCP.

These build real deep agents (deepagents.create_deep_agent) around a scripted
chat model, with their tools served by the real tool server over HTTP (the
`tool_server` fixture), so the middleware stack, the tool node, the MCP round
trip, the `task` subagent and the limits all genuinely run. Only the model's
replies are canned.
"""
import pytest
from langchain.agents.middleware.model_call_limit import ModelCallLimitExceededError
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool

from src.core import agent_factory
from src.core.agent_factory import OPERATING_MODE, build_agent
from src.tools import agent_tools, mcp_client
from src.tools.agent_tools import Toolset, agent_tool


class ScriptedModel(BaseChatModel):
    """Replies from a list, and remembers every request it was sent."""

    replies: list
    requests: list = []
    bound: list = []

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.requests.append(messages)
        return ChatResult(generations=[ChatGeneration(message=self.replies.pop(0))])

    def bind_tools(self, tools, **kwargs):
        self.bound.append(sorted(getattr(t, "name", None) or t["name"]
                                 if not isinstance(t, dict) else
                                 t.get("name") or t["function"]["name"]
                                 for t in tools))
        return self

    @property
    def _llm_type(self):
        return "scripted"


def calls_tool(name, arguments, call_id):
    return AIMessage(content="", tool_calls=[{"name": name, "args": arguments, "id": call_id}])


@pytest.fixture
def probe_tool(tool_server):
    """A throwaway tool for the reviewer role, served over MCP; removed after."""
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    toolset = Toolset(name="factory_probe", agents=("reviewer",),
                      guidance="Probe guidance for the reviewer.")

    @agent_tool(toolset)
    def probe_lookup(refid: str) -> str:
        """Look a refId up."""
        return f"probe:{refid}"

    tool_server()
    yield "probe_lookup"
    agent_tools._registry.clear()
    agent_tools._registry.update(saved)


@tool
def side_effect_tool(id: str) -> str:
    """Stands in for queue_for_replay."""
    return "done"


def system_text(request) -> str:
    assert isinstance(request[0], SystemMessage)
    return request[0].text


def test_system_prompt_is_the_role_prompt_then_tools_then_operating_mode(probe_tool):
    model = ScriptedModel(replies=[AIMessage(content="ok")])
    agent = build_agent("reviewer", model, "ROLE PROMPT")
    result = agent.invoke({"messages": [HumanMessage(content="go")]})

    assert result["messages"][-1].content == "ok"
    text = system_text(model.requests[0])
    assert text.startswith("ROLE PROMPT")
    assert text.index("ROLE PROMPT") < text.index(mcp_client.TOOLS_HEADING) \
        < text.index("### OPERATING MODE")
    assert "Probe guidance for the reviewer." in text
    assert mcp_client.TOOLS_HEADING in text


def test_a_role_without_tools_gets_no_tools_section():
    model = ScriptedModel(replies=[AIMessage(content="ok")])
    build_agent("synthesis", model, "ROLE").invoke({"messages": [HumanMessage(content="x")]})
    text = system_text(model.requests[0])
    assert mcp_client.TOOLS_HEADING not in text
    assert OPERATING_MODE in text


def test_registered_tool_calls_run_and_are_recorded(probe_tool):
    model = ScriptedModel(replies=[calls_tool(probe_tool, {"refid": "R1"}, "c1"),
                                   AIMessage(content="FINAL")])
    agent = build_agent("reviewer", model, "ROLE", tools=[side_effect_tool])

    with mcp_client.recording() as calls:
        result = agent.invoke({"messages": [HumanMessage(content="go")]})

    assert result["messages"][-1].content == "FINAL"
    assert calls == [{"tool": probe_tool, "args": {"refid": "R1"}, "result": "probe:R1"}]
    assert {probe_tool, "side_effect_tool", "task", "write_todos"} <= set(model.bound[0])


def test_the_subagent_gets_registered_tools_only_and_its_calls_are_recorded(probe_tool):
    model = ScriptedModel(replies=[
        calls_tool("task", {"description": "look up R2", "subagent_type": "general-purpose"}, "t1"),
        calls_tool(probe_tool, {"refid": "R2"}, "s1"),
        AIMessage(content="subagent report"),
        AIMessage(content="MAIN"),
    ])
    agent = build_agent("reviewer", model, "ROLE", tools=[side_effect_tool])

    with mcp_client.recording() as calls:
        result = agent.invoke({"messages": [HumanMessage(content="go")]})

    assert result["messages"][-1].content == "MAIN"
    assert [call["args"] for call in calls] == [{"refid": "R2"}]
    main_tools, sub_tools = model.bound[0], model.bound[1]
    assert "side_effect_tool" in main_tools and "task" in main_tools
    assert probe_tool in sub_tools
    assert "side_effect_tool" not in sub_tools and "task" not in sub_tools


def test_past_the_tool_limit_calls_are_refused_and_the_model_answers(probe_tool, monkeypatch):
    monkeypatch.setenv("AGENT_MAX_TOOL_CALLS", "1")
    model = ScriptedModel(replies=[calls_tool(probe_tool, {"refid": "A"}, "1"),
                                   calls_tool(probe_tool, {"refid": "B"}, "2"),
                                   AIMessage(content="ANSWER")])
    agent = build_agent("reviewer", model, "ROLE")

    with mcp_client.recording() as calls:
        result = agent.invoke({"messages": [HumanMessage(content="go")]})

    assert result["messages"][-1].content == "ANSWER"
    assert [call["args"]["refid"] for call in calls] == ["A"]


def test_past_the_model_call_limit_the_run_fails(probe_tool, monkeypatch):
    monkeypatch.setenv("AGENT_MAX_MODEL_CALLS", "2")
    model = ScriptedModel(replies=[calls_tool(probe_tool, {"refid": str(i)}, str(i))
                                   for i in range(5)])
    agent = build_agent("reviewer", model, "ROLE")
    with pytest.raises(ModelCallLimitExceededError):
        agent.invoke({"messages": [HumanMessage(content="go")]})


def test_limits_fall_back_on_unusable_settings(monkeypatch):
    monkeypatch.setenv("AGENT_MAX_TOOL_CALLS", "many")
    monkeypatch.setenv("AGENT_MAX_MODEL_CALLS", "-3")
    assert agent_factory.max_tool_calls() == agent_factory.DEFAULT_MAX_TOOL_CALLS
    assert agent_factory.max_model_calls() == agent_factory.DEFAULT_MAX_MODEL_CALLS


def test_an_explicit_tool_may_not_share_a_registered_name(probe_tool):
    @tool
    def probe_lookup(refid: str) -> str:
        """Clashes with the registered probe."""
        return refid

    with pytest.raises(ValueError, match="both passed explicitly and served over MCP"):
        build_agent("reviewer", ScriptedModel(replies=[]), "ROLE", tools=[probe_lookup])
