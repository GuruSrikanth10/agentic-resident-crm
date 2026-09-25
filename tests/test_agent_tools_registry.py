"""
The tool registry (src/tools/agent_tools/__init__.py) -- the server side.

What a developer relies on when adding a tool: a module dropped into the
package is discovered and served with no other change, a toolset switch
removes its tools from the server, mistakes fail loudly, and a tool's own
failure becomes a result instead of an exception. Which agent gets which tool
is the MCP client's business (tests/test_mcp_client.py).
"""
import json
import sys
import textwrap

import pytest

from src.tools import agent_tools
from src.tools.agent_tools import Toolset, agent_tool

PROCESS_DB_TOOLS = {
    "get_packet_stage_summary", "get_packet_stage_timeline", "get_parking_status",
    "list_helper_records", "get_abis_candidates", "get_candidate_facts",
    "get_parking_match_verdicts", "get_update_checker_result",
    "get_helper_record_fields",
}


@pytest.fixture
def registry_snapshot():
    """Restore the registry after a test registers throwaway tools."""
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    yield
    agent_tools._registry.clear()
    agent_tools._registry.update(saved)


def _enabled_names():
    return {entry.tool.name for entry in agent_tools.enabled_entries()}


# ----------------------------------------------------------------------
# What is served
# ----------------------------------------------------------------------

def test_process_db_tools_are_registered_for_the_investigator():
    entries = {entry.tool.name: entry for entry in agent_tools.entries()}
    assert PROCESS_DB_TOOLS <= set(entries)
    for name in PROCESS_DB_TOOLS:
        toolset = entries[name].toolset
        assert toolset.name == "process_db"
        assert toolset.agents == ("investigator",)
        assert toolset.read_only is True


def test_a_disabled_toolset_is_not_served(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert not (_enabled_names() & PROCESS_DB_TOOLS)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    assert PROCESS_DB_TOOLS <= _enabled_names()


def test_any_enabled_follows_the_switches(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert agent_tools.any_enabled() is False
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    assert agent_tools.any_enabled() is True


def test_entries_are_in_a_stable_order():
    names = [(entry.toolset.name, entry.tool.name) for entry in agent_tools.entries()]
    assert names == sorted(names)


def test_validate_is_clean_by_default():
    assert agent_tools.validate() == []


# ----------------------------------------------------------------------
# Declaring tools
# ----------------------------------------------------------------------

def test_toolset_rejects_an_unknown_role():
    with pytest.raises(ValueError, match="unknown agent role"):
        Toolset(name="bad", agents=("investgator",))


def test_toolset_accepts_a_single_role_string():
    assert Toolset(name="single", agents="reviewer").agents == ("reviewer",)


def test_a_toolset_is_not_read_only_unless_it_says_so():
    assert Toolset(name="plain", agents=()).read_only is False


def test_a_tool_needs_a_docstring(registry_snapshot):
    toolset = Toolset(name="t_doc", agents=("reviewer",))
    with pytest.raises(ValueError, match="docstring"):
        @agent_tool(toolset)
        def undocumented(x: str) -> str:
            return x


def test_reserved_names_are_refused(registry_snapshot):
    toolset = Toolset(name="t_reserved", agents=("reviewer",))
    with pytest.raises(ValueError, match="reserved"):
        @agent_tool(toolset)
        def task(x: str) -> str:
            """Would shadow the deep-agent task tool."""
            return x


def test_one_name_from_two_modules_is_refused(registry_snapshot):
    toolset = Toolset(name="t_dupe", agents=("reviewer",))

    def lookup(x: str) -> str:
        """A lookup."""
        return x

    lookup.__module__ = "first.module"
    agent_tool(toolset, name="dupe_tool")(lookup)
    lookup.__module__ = "second.module"
    with pytest.raises(ValueError, match="registered twice"):
        agent_tool(toolset, name="dupe_tool")(lookup)


def test_decorator_returns_the_function_and_registers_a_schema(registry_snapshot):
    toolset = Toolset(name="t_schema", agents=("reviewer",))

    @agent_tool(toolset)
    def schema_probe(refid: str, limit: int = 5, paths: list[str] = None) -> str:
        """Probe."""
        return refid

    assert schema_probe("abc") == "abc"
    tool = agent_tools.get_tool("schema_probe")
    assert set(tool.args) == {"refid", "limit", "paths"}
    assert tool.args["paths"]["type"] == "array"
    assert tool.description == "Probe."


def test_an_exception_becomes_an_evidence_gap_message(registry_snapshot):
    toolset = Toolset(name="t_raise", agents=("reviewer",))

    @agent_tool(toolset)
    def exploding(refid: str) -> str:
        """Always fails."""
        raise RuntimeError("boom")

    result = agent_tools.get_tool("exploding").invoke({"refid": "r1"})
    assert "failed (RuntimeError)" in result
    assert "evidence gap" in result


def test_a_non_string_result_is_returned_as_json(registry_snapshot):
    toolset = Toolset(name="t_json", agents=("reviewer",))

    @agent_tool(toolset)
    def structured(refid: str) -> dict:
        """Returns a dict."""
        return {"refid": refid, "n": 1}

    assert json.loads(agent_tools.get_tool("structured").invoke({"refid": "r"})) == \
        {"refid": "r", "n": 1}


def test_a_raising_switch_counts_as_disabled(registry_snapshot):
    def broken():
        raise RuntimeError("cannot read the flag")

    toolset = Toolset(name="t_switch", agents=("reviewer",), enabled=broken)

    @agent_tool(toolset)
    def switched(refid: str) -> str:
        """Behind a broken switch."""
        return refid

    assert "switched" in {entry.tool.name for entry in agent_tools.entries()}
    assert "switched" not in _enabled_names()


# ----------------------------------------------------------------------
# Discovery: dropping a module in is the whole change
# ----------------------------------------------------------------------

def test_a_module_dropped_into_the_package_is_registered(tmp_path, monkeypatch,
                                                         registry_snapshot):
    (tmp_path / "extra_lookup.py").write_text(textwrap.dedent('''
        from src.tools.agent_tools import Toolset, agent_tool

        EXTRA = Toolset(name="extra", agents=("reviewer",),
                        guidance="Use extra_lookup for extra things.")

        @agent_tool(EXTRA)
        def extra_lookup(refid: str) -> str:
            """Look up something extra by refId."""
            return "extra:" + refid
    '''))
    (tmp_path / "_helper.py").write_text("raise RuntimeError('helpers are never scanned')\n")

    monkeypatch.setattr(agent_tools, "__path__", [*agent_tools.__path__, str(tmp_path)])
    monkeypatch.setattr(agent_tools, "_discovered", False)
    try:
        assert "extra_lookup" in _enabled_names()
        assert agent_tools.get_tool("extra_lookup").invoke({"refid": "r9"}) == "extra:r9"
    finally:
        sys.modules.pop("src.tools.agent_tools.extra_lookup", None)


def test_a_module_that_fails_to_import_fails_discovery_by_name(tmp_path, monkeypatch):
    (tmp_path / "broken_tool.py").write_text("import no_such_dependency_anywhere\n")
    monkeypatch.setattr(agent_tools, "__path__", [*agent_tools.__path__, str(tmp_path)])
    monkeypatch.setattr(agent_tools, "_discovered", False)
    try:
        with pytest.raises(RuntimeError, match="broken_tool"):
            agent_tools.discover()
        assert any("broken_tool" in error for error in agent_tools.validate())
    finally:
        sys.modules.pop("src.tools.agent_tools.broken_tool", None)


def test_cli_lists_and_calls_in_process(monkeypatch, capsys):
    from src.tools.agent_tools.__main__ import main

    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert main(["list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert {row["name"] for row in listed} >= PROCESS_DB_TOOLS
    assert all(row["enabled"] is False for row in listed if row["toolset"] == "process_db")

    assert main(["call", "get_parking_status", '{"refid": "r1"}']) == 0
    assert "switched off" in capsys.readouterr().out
    assert main(["call", "no_such_tool", "{}"]) == 1
    assert main(["call", "get_parking_status", "[1]"]) == 1
