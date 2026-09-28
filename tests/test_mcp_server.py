"""
The agent tool server (src/tools/mcp_server.py): what it lists, how it
answers, and the API's supervised local instance.
"""
import asyncio
import os
import signal
import socket

import pytest

from src.tools import agent_tools, mcp_config, mcp_server
from src.tools.agent_tools import Toolset, agent_tool


@pytest.fixture
def probe_tools():
    """Two throwaway tools, removed afterwards."""
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    toolset = Toolset(name="server_probe", agents=("investigator", "reviewer"),
                      guidance="Probe guidance.", read_only=True,
                      services=("enu-biometric",))
    off = Toolset(name="server_probe_off", agents=("reviewer",), enabled=lambda: False)

    @agent_tool(toolset)
    def probe_echo(refid: str, times: int = 1) -> str:
        """Echo a refId."""
        return ",".join([refid] * times)

    @agent_tool(off)
    def probe_hidden(refid: str) -> str:
        """Never served."""
        return refid

    yield
    agent_tools._registry.clear()
    agent_tools._registry.update(saved)


def _run(coroutine):
    return asyncio.run(coroutine)


async def _listing():
    from mcp import Client

    async with Client(mcp_server.build_server()) as client:
        return (await client.list_tools()).tools


async def _call(name, arguments):
    from mcp import Client

    async with Client(mcp_server.build_server()) as client:
        return await client.call_tool(name, arguments)


def test_the_server_lists_only_enabled_tools(probe_tools, monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    names = {tool.name for tool in _run(_listing())}
    assert "probe_echo" in names
    assert "probe_hidden" not in names
    assert "bio_get_parking_status" not in names


def test_a_listing_carries_roles_guidance_and_the_read_only_hint(probe_tools):
    tool = next(tool for tool in _run(_listing()) if tool.name == "probe_echo")
    assert tool.description == "Echo a refId."
    assert tool.meta == {mcp_config.META_TOOLSET: "server_probe",
                         mcp_config.META_AGENTS: ["investigator", "reviewer"],
                         mcp_config.META_SERVICES: ["enu-biometric"],
                         mcp_config.META_GUIDANCE: "Probe guidance."}
    assert tool.annotations.read_only_hint is True
    assert tool.input_schema["required"] == ["refid"]
    assert tool.input_schema["properties"]["times"]["default"] == 1


def test_a_call_returns_the_tools_text(probe_tools):
    result = _run(_call("probe_echo", {"refid": "R1", "times": 2}))
    assert result.is_error is False
    assert [item.text for item in result.content] == ["R1,R1"]
    assert result.structured_content is None


def test_bad_arguments_are_an_error_result_not_a_protocol_failure(probe_tools):
    result = _run(_call("probe_echo", {"times": 2}))
    assert result.is_error is True
    assert "refid" in result.content[0].text


def test_the_process_db_tools_are_served_when_switched_on(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    listed = {tool.name: tool for tool in _run(_listing())}
    assert {"bio_get_packet_stage_summary", "bio_get_parking_status",
            "bio_get_helper_record_fields"} <= set(listed)
    assert listed["bio_get_parking_status"].meta[mcp_config.META_SERVICES] == \
        ["enu-biometric"]


def test_health_answers_over_http(tool_server):
    url = tool_server()
    port = int(url.split(":")[2].split("/")[0])
    assert mcp_server.probe_health("127.0.0.1", port) is True


def test_health_is_false_when_nothing_listens():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    assert mcp_server.probe_health("127.0.0.1", port) is False


# ----------------------------------------------------------------------
# The API's local server: a supervised child process
# ----------------------------------------------------------------------

def _free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_local_server_starts_is_restarted_when_it_dies_and_stops():
    server = mcp_server.LocalToolServer(host="127.0.0.1", port=_free_port())
    server.FIRST_BACKOFF_SECONDS = 0.2
    try:
        server.start()
        assert server.wait_healthy(60), "the tool server did not come up"

        first_pid = server._process.pid
        os.kill(first_pid, signal.SIGKILL)
        server._process.wait()
        assert not server.healthy()

        assert server.wait_healthy(60), "the tool server was not restarted"
        assert server._process.pid != first_pid
    finally:
        server.stop()
    assert server._process is None
    assert not mcp_server.probe_health("127.0.0.1", server.port)


def test_start_local_server_follows_the_switch(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_SERVE", "false")
    assert mcp_server.start_local_server() is None
    assert mcp_server.local_server() is None
