"""
The agent tool registry (src/tools/agent_tools/__init__.py).

Covers what a developer relies on when adding a tool: a module dropped into
the package is discovered and offered to its roles with no other change, a
toolset switch removes its tools and guidance, AGENT_TOOLS_<ROLE> overrides a
role's selection, mistakes fail loudly, and every call is recorded as
evidence for the node that made it.
"""
import json
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor

import pytest
from langchain_core.runnables.config import ContextThreadPoolExecutor

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


def _names(role):
    return {tool.name for tool in agent_tools.tools_for(role)}


# ----------------------------------------------------------------------
# Defaults and the toolset switch
# ----------------------------------------------------------------------

def test_process_db_tools_are_offered_only_to_the_investigator_when_enabled(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    assert _names("investigator") == PROCESS_DB_TOOLS
    for role in agent_tools.AGENT_ROLES:
        if role != "investigator":
            assert _names(role) == set(), role


def test_a_disabled_toolset_offers_nothing_and_adds_no_guidance(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert agent_tools.tools_for("investigator") == []
    assert agent_tools.prompt_section("investigator") == ""


def test_prompt_section_carries_the_rules_and_the_guidance(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    section = agent_tools.prompt_section("investigator")
    assert section.startswith(agent_tools.TOOLS_HEADING)
    assert "evidence gap" in section
    assert "#### process_db" in section
    assert "get_parking_status" in section
    assert "None of these tables holds the final approve/reject verdict" in section


def test_tool_order_is_stable(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    names = [tool.name for tool in agent_tools.tools_for("investigator")]
    assert names == sorted(names)
    assert names == [tool.name for tool in agent_tools.tools_for("investigator")]


# ----------------------------------------------------------------------
# AGENT_TOOLS_<ROLE>
# ----------------------------------------------------------------------

def test_override_selects_exact_tools_for_a_role(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    monkeypatch.setenv("AGENT_TOOLS_REVIEWER", "get_parking_status, get_packet_stage_summary")
    assert _names("reviewer") == {"get_parking_status", "get_packet_stage_summary"}


def test_override_none_removes_every_tool(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "none")
    assert _names("investigator") == set()


def test_override_cannot_switch_on_a_disabled_toolset(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    monkeypatch.setenv("AGENT_TOOLS_REVIEWER", "get_parking_status")
    assert _names("reviewer") == set()


def test_override_naming_an_unknown_tool_raises(monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "get_parking_statuss")
    with pytest.raises(ValueError, match="unknown tool"):
        agent_tools.tools_for("investigator")


def test_validate_reports_unknown_tools_and_misspelt_roles(monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "no_such_tool")
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATER", "get_parking_status")
    errors = agent_tools.validate()
    assert any("no_such_tool" in error for error in errors)
    assert any("AGENT_TOOLS_INVESTIGATER" in error for error in errors)


def test_validate_is_clean_by_default():
    assert agent_tools.validate() == []


def test_unknown_role_is_refused():
    with pytest.raises(ValueError, match="Unknown agent role"):
        agent_tools.tools_for("investigatr")


# ----------------------------------------------------------------------
# Declaring tools
# ----------------------------------------------------------------------

def test_toolset_rejects_an_unknown_role():
    with pytest.raises(ValueError, match="unknown agent role"):
        Toolset(name="bad", agents=("investgator",))


def test_toolset_accepts_a_single_role_string():
    assert Toolset(name="single", agents="reviewer").agents == ("reviewer",)


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

    assert "switched" not in _names("reviewer")


# ----------------------------------------------------------------------
# Discovery: dropping a module in is the whole change
# ----------------------------------------------------------------------

def test_a_module_dropped_into_the_package_is_offered_to_its_role(tmp_path, monkeypatch,
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
        assert "extra_lookup" in _names("reviewer")
        assert "Use extra_lookup for extra things." in agent_tools.prompt_section("reviewer")
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


# ----------------------------------------------------------------------
# Recording
# ----------------------------------------------------------------------

def test_calls_are_recorded_only_inside_recording(registry_snapshot):
    toolset = Toolset(name="t_rec", agents=("reviewer",))

    @agent_tool(toolset)
    def recorded_lookup(refid: str, depth: int = 1) -> str:
        """Recorded."""
        return f"{refid}:{depth}"

    tool = agent_tools.get_tool("recorded_lookup")
    tool.invoke({"refid": "outside"})
    with agent_tools.recording() as calls:
        tool.invoke({"refid": "inside", "depth": 2})
    tool.invoke({"refid": "after"})

    assert calls == [{"tool": "recorded_lookup", "args": {"refid": "inside", "depth": 2},
                      "result": "inside:2"}]


def test_calls_on_worker_threads_land_in_the_callers_record(registry_snapshot):
    """LangGraph's tool node runs tools on a context-copying executor; a
    call made there must land in the node's record and no other's."""
    toolset = Toolset(name="t_threads", agents=("reviewer",))

    @agent_tool(toolset)
    def threaded_lookup(refid: str) -> str:
        """Threaded."""
        return refid

    tool = agent_tools.get_tool("threaded_lookup")
    with agent_tools.recording() as calls:
        with ContextThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda r: tool.invoke({"refid": r}), ["a", "b", "c"]))
    assert sorted(call["args"]["refid"] for call in calls) == ["a", "b", "c"]

    def separate_node(refid):
        with agent_tools.recording() as own:
            tool.invoke({"refid": refid})
        return own

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(separate_node, ["x", "y"])
    assert [call["args"]["refid"] for call in first] == ["x"]
    assert [call["args"]["refid"] for call in second] == ["y"]


def test_recorded_results_are_bounded(registry_snapshot):
    toolset = Toolset(name="t_big", agents=("reviewer",))

    @agent_tool(toolset)
    def huge(refid: str) -> str:
        """Huge."""
        return "x" * (agent_tools.MAX_RECORDED_RESULT_CHARS * 2)

    with agent_tools.recording() as calls:
        agent_tools.get_tool("huge").invoke({"refid": "r"})
    assert len(calls[0]["result"]) <= agent_tools.MAX_RECORDED_RESULT_CHARS


# ----------------------------------------------------------------------
# Evidence merging and rendering
# ----------------------------------------------------------------------

def _rec(tool, refid, result):
    return {"tool": tool, "args": {"refid": refid}, "result": result}


def test_merge_keeps_one_record_per_call_newest_last():
    merged = agent_tools.merge_evidence(
        [_rec("a", "1", "old"), _rec("b", "1", "b")],
        [_rec("a", "1", "new"), _rec("a", "2", "other")])
    assert [(r["tool"], r["args"]["refid"], r["result"]) for r in merged] == [
        ("b", "1", "b"), ("a", "1", "new"), ("a", "2", "other")]


def test_merge_caps_the_number_of_records():
    many = [_rec("t", str(i), "r") for i in range(agent_tools.MAX_EVIDENCE_RECORDS + 5)]
    merged = agent_tools.merge_evidence(None, many)
    assert len(merged) == agent_tools.MAX_EVIDENCE_RECORDS
    assert merged[-1]["args"]["refid"] == str(agent_tools.MAX_EVIDENCE_RECORDS + 4)


def test_render_is_empty_without_records():
    assert agent_tools.render_evidence(None) == ""
    assert agent_tools.render_evidence([]) == ""


def test_render_lists_calls_in_order_with_their_arguments():
    text = agent_tools.render_evidence([_rec("a", "1", "first"), _rec("b", "2", "second")])
    assert text.index("[1] a {\"refid\": \"1\"}") < text.index("[2] b {\"refid\": \"2\"}")
    assert "first" in text and "second" in text


def test_render_keeps_the_newest_and_says_how_many_it_left_out():
    records = [_rec("t", str(i), "r" * 100) for i in range(10)]
    text = agent_tools.render_evidence(records, max_chars=450)
    assert "left out to fit" in text
    assert "\"refid\": \"9\"" in text
    assert "\"refid\": \"0\"" not in text


def test_render_budget_falls_back_on_a_bad_setting(monkeypatch):
    monkeypatch.setenv(agent_tools.ENV_EVIDENCE_MAX_CHARS, "lots")
    assert agent_tools.evidence_max_chars() == agent_tools.DEFAULT_EVIDENCE_MAX_CHARS


# ----------------------------------------------------------------------
# Fingerprint material
# ----------------------------------------------------------------------

def test_fingerprint_material_moves_with_the_toolset_switch(monkeypatch):
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    off = agent_tools.fingerprint_material()
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    on = agent_tools.fingerprint_material()
    assert off != on
    assert on == agent_tools.fingerprint_material()


def test_cli_lists_and_calls(monkeypatch, capsys):
    from src.tools.agent_tools.__main__ import main

    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert main(["list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert {row["name"] for row in listed} >= PROCESS_DB_TOOLS
    assert all(row["agents_now"] == [] for row in listed if row["toolset"] == "process_db")

    assert main(["call", "get_parking_status", '{"refid": "r1"}']) == 0
    assert "switched off" in capsys.readouterr().out
    assert main(["call", "no_such_tool", "{}"]) == 1
    assert main(["call", "get_parking_status", "[1]"]) == 1
