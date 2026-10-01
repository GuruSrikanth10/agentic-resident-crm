"""
The opencode harness reaches the same MCP tool servers as the deep agents.

opencode_runner hands `opencode serve` the servers and one agent per harness
role and service pack (each allowed exactly the tools that role gets for that
pack), runs every harness task as its role's agent for the packet's pack, and
reads the MCP calls a task made off its event stream so they become evidence
like the direct path's. The orchestrators append the role's tools section for
the same pack to the harness prompt.
"""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage

import src.core.agent_orchestrator as orch
import src.dlt.orchestrator as dlt
from src.tools import agent_tools, mcp_client
from src.tools.agent_tools import Toolset, agent_tool
from src.utils import opencode_runner

REFID = "3f2b9c1e-0d4a-4a8e-9b1f-6c7d8e9f0a1b"
BIO = "enu-biometric"


@pytest.fixture
def served(tool_server):
    """A tool for the investigator roles, served over MCP."""
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    toolset = Toolset(name="harness_probe", agents=("investigator", "dlt_investigator"),
                      guidance="Probe guidance for the harness.", read_only=True,
                      services=("*",))

    @agent_tool(toolset)
    def harness_probe(refid: str) -> str:
        """Probe."""
        return f"probe:{refid}"

    url = tool_server()
    yield url
    agent_tools._registry.clear()
    agent_tools._registry.update(saved)


# ----------------------------------------------------------------------
# The config opencode is started with
# ----------------------------------------------------------------------

def test_no_servers_means_no_config_and_no_agent(monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    assert opencode_runner._harness_config("m") == {}
    assert opencode_runner._task_agent("investigator", BIO, {}) is None


def test_servers_give_opencode_an_mcp_block_and_role_agents(served):
    config = opencode_runner._harness_config("m")
    assert config["mcp"]["agent_tools"]["url"] == served
    assert "provider" not in config
    tools = config["agent"]["crm_investigator__enu_biometric"]["tools"]
    assert tools == {"agent_tools_*": False, "agent_tools_harness_probe": True}
    assert opencode_runner._task_agent("investigator", BIO, config) \
        == "crm_investigator__enu_biometric"
    assert opencode_runner._task_agent("dlt_investigator", None, config) \
        == "crm_dlt_investigator"
    assert opencode_runner._task_agent(None, None, config) is None


def test_a_pack_the_server_was_started_without_falls_back_to_the_default_agent(served):
    """Never to a wider set: `__default` has only the tools for every service."""
    config = opencode_runner._harness_config("m")
    assert opencode_runner._task_agent("reviewer", "svc-added-later", config) \
        == "crm_reviewer__default"
    assert opencode_runner._task_agent("reviewer", None, config) == "crm_reviewer__default"


def test_a_task_with_no_agent_of_its_own_is_refused_while_servers_are_configured():
    """opencode's default agent would have every server's tools."""
    config = {"mcp": {"agent_tools": {}}, "agent": {"crm_dlt_investigator": {}}}
    with pytest.raises(opencode_runner.OpencodeUnavailable, match="no agent for reviewer"):
        opencode_runner._task_agent("reviewer", BIO, config)


def test_a_config_that_cannot_be_worked_out_leaves_the_harness_without_tools(monkeypatch):
    def broken():
        raise ValueError("AGENT_TOOLS_INVESTIGATOR names tool(s) ['typo']")

    monkeypatch.setattr(mcp_client, "opencode_config", broken)
    assert opencode_runner._harness_config("m") == {}


# ----------------------------------------------------------------------
# A task runs as its role's agent, and its MCP calls come back
# ----------------------------------------------------------------------

_EVENTS = [
    {"type": "step_start", "sessionID": "ses_1", "part": {"type": "step-start"}},
    {"type": "tool_use", "sessionID": "ses_1",
     "part": {"type": "tool", "tool": "agent_tools_harness_probe", "callID": "c1",
              "state": {"status": "completed", "input": {"refid": REFID},
                        "output": f"probe:{REFID}"}}},
    {"type": "tool_use", "sessionID": "ses_1",
     "part": {"type": "tool", "tool": "agent_tools_harness_probe", "callID": "c2",
              "state": {"status": "error", "input": {"refid": "bad"},
                        "error": "boom"}}},
    {"type": "tool_use", "sessionID": "ses_1",
     "part": {"type": "tool", "tool": "grep", "callID": "c3",
              "state": {"status": "completed", "input": {"pattern": "x"}, "output": "hit"}}},
    {"type": "step_finish", "sessionID": "ses_1",
     "part": {"type": "step-finish", "reason": "stop", "cost": 0,
              "tokens": {"input": 1, "output": 1, "reasoning": 0,
                         "cache": {"read": 0, "write": 0}}}},
]

_FAKE = """#!{python}
import json, re, sys
task = sys.argv[-1]
output_path = task.rsplit(": ", 1)[1]
with open(output_path + ".argv.json", "w", encoding="utf-8") as handle:
    json.dump(sys.argv, handle)
for event in {events}:
    print(json.dumps(event), flush=True)
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump({{"investigation": "done"}}, handle)
"""


def _fake_binary(tmp_path, monkeypatch):
    fake = tmp_path / "opencode"
    fake.write_text(_FAKE.format(python=sys.executable, events=repr(_EVENTS)), encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv(opencode_runner.ENV_DISABLE, "true")
    monkeypatch.setenv(opencode_runner.ENV_BINARY, str(fake))
    monkeypatch.setattr(opencode_runner, "BINARY_PATHS", (str(fake),))


def _run(tmp_path, node=None, session=None, service=None):
    output = tmp_path / "casebook_x" / "investigation.json"
    output.parent.mkdir(exist_ok=True)
    result = opencode_runner.run_task_json("instructions", str(output), node=node,
                                           session=session, service=service)
    argv = json.loads(Path(str(output) + ".argv.json").read_text(encoding="utf-8"))
    return result, argv


def test_a_task_runs_as_its_roles_agent(served, tmp_path, monkeypatch):
    _fake_binary(tmp_path, monkeypatch)
    _, argv = _run(tmp_path, node="investigator", service=BIO)
    assert argv[argv.index("--agent") + 1] == "crm_investigator__enu_biometric"
    assert argv.index("--agent") < argv.index("--dir")


def test_without_servers_a_task_is_unchanged(tmp_path, monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    _fake_binary(tmp_path, monkeypatch)
    _, argv = _run(tmp_path, node="investigator", service=BIO)
    assert "--agent" not in argv


def test_an_attached_task_uses_its_servers_config(tmp_path, monkeypatch):
    """The agent must be one the server was started with -- not one this
    process would configure now."""
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    _fake_binary(tmp_path, monkeypatch)
    session = MagicMock(url="http://127.0.0.1:4096", password="pw",
                        config={"agent": {"crm_reviewer__enu_biometric": {}}})
    _, argv = _run(tmp_path, node="reviewer", session=session, service=BIO)
    assert argv[argv.index("--attach") + 1] == "http://127.0.0.1:4096"
    assert argv[argv.index("--agent") + 1] == "crm_reviewer__enu_biometric"

    session.config = {}
    _, argv = _run(tmp_path, node="reviewer", session=session, service=BIO)
    assert "--agent" not in argv


def test_a_task_returns_its_finished_tool_calls(tmp_path, monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    _fake_binary(tmp_path, monkeypatch)
    result, _ = _run(tmp_path)
    assert result["tool_calls"] == [
        {"tool": "agent_tools_harness_probe", "input": {"refid": REFID},
         "output": f"probe:{REFID}", "status": "completed"},
        {"tool": "agent_tools_harness_probe", "input": {"refid": "bad"},
         "output": "The tool failed, so nothing was read: boom", "status": "error"},
        {"tool": "grep", "input": {"pattern": "x"}, "output": "hit", "status": "completed"},
    ]


def test_the_trace_keeps_one_record_per_call():
    trace = opencode_runner._Trace()
    running = {"type": "tool_use", "part": {"tool": "t", "callID": "c9",
                                            "state": {"status": "running", "input": {}}}}
    done = {"type": "tool_use", "part": {"tool": "t", "callID": "c9",
                                         "state": {"status": "completed", "input": {"a": 1},
                                                   "output": "ok"}}}
    for event in (running, done, done):
        trace.add(event)
    assert trace.tool_calls == [{"tool": "t", "input": {"a": 1}, "output": "ok",
                                 "status": "completed"}]


# ----------------------------------------------------------------------
# The harness Investigators keep those calls as evidence
# ----------------------------------------------------------------------

_HARNESS_CALLS = [
    {"tool": "agent_tools_harness_probe", "input": {"refid": REFID},
     "output": f"probe:{REFID}", "status": "completed"},
    {"tool": "read", "input": {"filePath": "x"}, "output": "file", "status": "completed"},
]


def _capture_harness(monkeypatch, tmp_path, captured):
    import src.utils.paths as paths
    from src.utils import docs_loader

    monkeypatch.setattr(paths, "LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr(docs_loader, "corpus_available", lambda: True)

    def fake_run_task_json(prompt, output_path, node=None, service=None):
        captured["prompt"] = prompt
        captured["node"] = node
        captured["service"] = service
        return {"result": {"investigation": "harness findings"}, "seconds": 0,
                "trace": {}, "tool_calls": list(_HARNESS_CALLS)}

    monkeypatch.setattr(opencode_runner, "run_task_json", fake_run_task_json)


class _Stub:
    def invoke(self, _request):
        return {"messages": [AIMessage(content="direct")]}


def test_the_rejection_harness_investigator_gets_the_tools_and_keeps_its_calls(
        served, monkeypatch, tmp_path):
    monkeypatch.setenv("USE_OPENCODE_HARNESS_REJECTION", "true")
    storage = MagicMock()
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: storage)
    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", lambda *a, **k: _Stub())
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    node = orch._build_agent().builder.nodes["investigate"].runnable.func
    captured = {}
    _capture_harness(monkeypatch, tmp_path, captured)

    out = node({"payload": {"eventId": "evt-h", "packetMetaData": {"enrolmentType": "U",
                                                                   "refId": REFID}},
                "logs": "Log fetching disabled.", "db_rule": "rule"})

    assert out["investigator_path"] == "harness"
    assert out["tool_evidence"] == [{"tool": "harness_probe", "args": {"refid": REFID},
                                     "result": f"probe:{REFID}"}]
    assert storage.save_artifact.call_args.args[1] == orch.TOOL_EVIDENCE_ARTIFACT
    assert captured["node"] == "investigator"
    assert captured["service"] == BIO
    assert mcp_client.TOOLS_HEADING in captured["prompt"]
    assert "agent_tools_harness_probe" in captured["prompt"]


def test_the_rejection_harness_reviewer_prompt_has_no_tools_section_by_default(
        served, monkeypatch, tmp_path):
    monkeypatch.setenv("USE_OPENCODE_HARNESS_REJECTION", "true")
    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", lambda *a, **k: _Stub())
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    node = orch._build_agent().builder.nodes["review"].runnable.func
    captured = {}
    _capture_harness(monkeypatch, tmp_path, captured)
    monkeypatch.setattr(opencode_runner, "run_task_json",
                        lambda prompt, output_path, node=None, service=None:
                        captured.update(prompt=prompt)
                        or {"result": {"verdict": "APPROVED", "feedback": ""}, "seconds": 0,
                            "trace": {}})

    node({"payload": {"eventId": "evt-r", "packetMetaData": {"enrolmentType": "U"}},
          "logs": "", "db_rule": "rule", "investigation": "x", "retry_count": 0})

    assert mcp_client.TOOLS_HEADING not in captured["prompt"]


def test_the_dlt_harness_investigator_keeps_its_calls(served, monkeypatch, tmp_path):
    monkeypatch.setenv("USE_OPENCODE_HARNESS_DLT", "true")
    monkeypatch.setattr(dlt, "_agent", None)
    monkeypatch.setattr(dlt, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(dlt, "build_agent", lambda *a, **k: _Stub())
    node = dlt._build_dlt_agent().builder.nodes["investigate"].runnable.func
    captured = {}
    _capture_harness(monkeypatch, tmp_path, captured)

    out = node({"case_id": "dlt-1", "failure": {"chain": [], "frames": []},
                "corroboration": {}, "logs": ""})

    assert out["tool_evidence"] == [{"tool": "harness_probe", "args": {"refid": REFID},
                                     "result": f"probe:{REFID}"}]
    assert "agent_tools_harness_probe" in captured["prompt"]


# ----------------------------------------------------------------------
# Readiness waits for the API's own tool server
# ----------------------------------------------------------------------

def test_ready_waits_for_the_local_tool_server(monkeypatch):
    from fastapi import HTTPException

    from src.api import routes
    from src.tools import mcp_server

    unhealthy = MagicMock()
    unhealthy.healthy.return_value = False
    monkeypatch.setattr(mcp_server, "_LOCAL", unhealthy)
    with pytest.raises(HTTPException) as refused:
        routes.readiness_check()
    assert refused.value.status_code == 503
    assert refused.value.detail == "Starting agent tool server"
