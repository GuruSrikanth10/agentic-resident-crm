"""
How the agents reach their tools: src/tools/mcp_config.py and mcp_client.py.

Most of these run against the real tool server over HTTP (the `tool_server`
fixture in conftest.py starts one in-process on a free port), so listing,
calling, argument validation, recording and failure handling are exercised
exactly as an agent meets them.
"""
import asyncio
import json
import re
import time

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.tools import agent_tools, mcp_client, mcp_config
from src.tools.agent_tools import Toolset, agent_tool
from src.tools.agent_tools.enu_biometric import _process_db
from src.utils.resilience import process_db_breaker
from conftest import DOCS_TOOLS

#: The service the process DB tools are for, and the pack its agents use.
BIO = "enu-biometric"

PROCESS_DB_TOOLS = {
    "bio_get_packet_stage_summary", "bio_get_packet_stage_timeline", "bio_get_parking_status",
    "bio_list_helper_records", "bio_get_abis_candidates", "bio_get_candidate_facts",
    "bio_get_parking_match_verdicts", "bio_get_update_checker_result",
    "bio_get_helper_record_fields",
}


@pytest.fixture
def stage_db(monkeypatch):
    """The process DB tools switched on, reading a SQLite stage tracker."""
    engine = create_engine("sqlite://", poolclass=StaticPool,
                           connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE bio_stage_tracker (stage_tracker_key INTEGER PRIMARY KEY, "
            "refid TEXT, sub_stage TEXT, sub_stage_status TEXT, "
            "sub_stage_reject_reason_code TEXT, markfor_stage_resubmission INT, "
            "stage_resubmission_count INT, created_by TEXT, creation_date TEXT)"))
        conn.execute(text(
            "INSERT INTO bio_stage_tracker VALUES (1, 'R1', 'ABIS_DEDUP', "
            "'IN PROGRESS', NULL, 0, 0, 'Biometric', '2026-09-01 10:00:00')"))
    monkeypatch.setattr(_process_db.PROCESS, "get_engine", lambda: engine)
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    process_db_breaker.close()
    yield engine
    process_db_breaker.close()


@pytest.fixture
def served(stage_db, tool_server):
    """A running tool server serving the process DB tools."""
    return tool_server()


def _tool(role, name, service=BIO):
    return next(tool for tool in mcp_client.tools_for(role, service) if tool.name == name)


# ======================================================================
# Configuration (mcp_config)
# ======================================================================

def test_no_servers_by_default_when_nothing_is_served(monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    monkeypatch.setenv("AGENT_MCP_SERVE", "false")
    assert mcp_config.servers() == []


def test_auto_serves_locally_exactly_when_a_toolset_is_on(monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    monkeypatch.setenv("AGENT_MCP_SERVE", "auto")
    monkeypatch.setenv("PROCESS_DB_ENABLED", "false")
    assert mcp_config.serve_locally() is False
    assert mcp_config.servers() == []
    monkeypatch.setenv("PROCESS_DB_ENABLED", "true")
    monkeypatch.setenv("AGENT_MCP_PORT", "9123")
    assert mcp_config.serve_locally() is True
    assert mcp_config.servers() == [mcp_config.ServerConfig(
        name="agent_tools", url="http://127.0.0.1:9123/mcp")]


def test_explicit_servers_with_headers_from_the_environment(monkeypatch):
    monkeypatch.setenv("ORG_TOKEN", "s3cret")
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({
        "org_tools": {"url": "https://mcp.example/mcp",
                      "headers": {"Authorization": "Bearer ${ORG_TOKEN}"}},
        "agent_tools": "http://127.0.0.1:8765/mcp"}))
    servers = mcp_config.servers()
    assert [server.name for server in servers] == ["agent_tools", "org_tools"]
    assert servers[1].header_dict() == {"Authorization": "Bearer s3cret"}


@pytest.mark.parametrize("value, message", [
    ("{not json", "not valid JSON"),
    ('["http://x/mcp"]', "JSON object"),
    ('{"Bad-Name": "http://x/mcp"}', "server name"),
    ('{"tools": "ftp://x/mcp"}', "http(s) URL"),
    ('{"tools": {"url": "http://x/mcp", "headers": {"A": 1}}}', "headers"),
    ('{"tools": {"url": "http://x/mcp", "token": "t"}}', "unknown key"),
    ('{"tools": {"url": "http://x/mcp", "kind": "doc"}}', "kind must be one of"),
    ('{"tools": {"url": "http://x/mcp", "headers": {"A": "${NOT_SET_ANYWHERE}"}}}',
     "NOT_SET_ANYWHERE"),
])
def test_malformed_server_settings_fail_validation(monkeypatch, value, message):
    monkeypatch.setenv("AGENT_MCP_SERVERS", value)
    with pytest.raises(ValueError, match=re.escape(message)):
        mcp_config.servers()
    assert any(message in error for error in mcp_client.validate())


def test_a_server_kind_is_read_and_defaults_to_tools(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({
        "agent_tools": {"url": "http://127.0.0.1:8765/mcp"},
        "droa_docs": {"url": "http://docs.ns.svc.cluster.local:8080/mcp", "kind": "docs"}}))
    tools, docs = mcp_config.servers()
    assert (tools.kind, docs.kind) == ("tools", "docs")
    assert mcp_config.of_kind([tools, docs], mcp_config.KIND_DOCS) == (docs,)
    # Frozen, hashable and compared by kind too, so a changed kind makes a
    # catalog stale.
    assert hash(docs) == hash(mcp_config.ServerConfig(
        name="droa_docs", url=docs.url, kind="docs"))
    assert docs != mcp_config.ServerConfig(name="droa_docs", url=docs.url)


@pytest.mark.parametrize("value, roles", [
    (None, agent_tools.AGENT_ROLES),
    ("", agent_tools.AGENT_ROLES),
    ("none", ()),
    ("NONE", ()),
    ("reviewer, investigator,dlt_reviewer", ("investigator", "reviewer", "dlt_reviewer")),
])
def test_the_docs_roles_setting(monkeypatch, value, roles):
    if value is None:
        monkeypatch.delenv("AGENT_DOCS_ROLES", raising=False)
    else:
        monkeypatch.setenv("AGENT_DOCS_ROLES", value)
    assert mcp_config.docs_roles() == roles
    assert mcp_client.validate() == []


def test_an_unknown_docs_role_fails_validation(monkeypatch):
    monkeypatch.setenv("AGENT_DOCS_ROLES", "investigator,investigatr")
    with pytest.raises(ValueError, match=re.escape("['investigatr']")):
        mcp_config.docs_roles()
    assert any("AGENT_DOCS_ROLES" in error and "investigatr" in error
               for error in mcp_client.validate())


def test_validate_reports_bad_switches_and_misspelt_roles(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_SERVE", "sometimes")
    monkeypatch.setenv("AGENT_MCP_PORT", "70000")
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATER", "bio_get_parking_status")
    errors = mcp_client.validate()
    assert any("AGENT_MCP_SERVE" in error for error in errors)
    assert any("AGENT_MCP_PORT" in error for error in errors)
    assert any("AGENT_TOOLS_INVESTIGATER" in error for error in errors)


def test_validate_is_clean_by_default():
    assert mcp_client.validate() == []


def test_a_server_bound_everywhere_is_reached_on_loopback(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_HOST", "0.0.0.0")
    monkeypatch.setenv("AGENT_MCP_PORT", "8800")
    assert mcp_config.local_url() == "http://127.0.0.1:8800/mcp"
    assert mcp_config.is_loopback("http://127.0.0.1:8800/mcp")
    assert not mcp_config.is_loopback("https://mcp.example/mcp")


# ======================================================================
# The catalog, over real HTTP
# ======================================================================

def test_the_catalog_lists_the_served_tools_with_their_roles(served):
    catalog = mcp_client.current_catalog()
    assert catalog.complete
    assert {tool.name for tool in catalog.tools} == PROCESS_DB_TOOLS
    summary = next(tool for tool in catalog.tools if tool.name == "bio_get_packet_stage_summary")
    assert summary.agents == ("investigator",)
    assert summary.services == (BIO,)
    assert summary.toolset == "process_db"
    assert summary.read_only is True
    assert "None of these tables holds the final approve/reject verdict" in summary.guidance
    assert summary.input_schema["required"] == ["refid"]


def test_only_the_investigator_gets_them_by_default(served):
    assert {tool.name for tool in mcp_client.tools_for("investigator", BIO)} == PROCESS_DB_TOOLS
    for role in agent_tools.SERVICE_ROLES:
        if role != "investigator":
            assert mcp_client.tools_for(role, BIO) == [], role
    for role in agent_tools.DLT_ROLES:
        assert mcp_client.tools_for(role) == [], role


def test_only_enu_biometric_packets_get_them(served):
    """Scoped to enu-biometric: an unresolved packet's agents get none."""
    assert mcp_client.tools_for("investigator", "_default") == []


def test_a_tool_call_goes_over_mcp_and_is_recorded(served):
    tool = _tool("investigator", "bio_get_packet_stage_summary")
    with mcp_client.recording() as calls:
        result = json.loads(tool.invoke({"refid": "R1"}))
    assert result["open_substages"][0]["sub_stage"] == "ABIS_DEDUP"
    assert calls == [{"tool": "bio_get_packet_stage_summary", "args": {"refid": "R1"},
                      "result": json.dumps(result, separators=(",", ":"))}]


def test_a_tool_can_be_awaited(served):
    tool = _tool("investigator", "bio_get_packet_stage_summary")
    with mcp_client.recording() as calls:
        result = json.loads(asyncio.run(tool.ainvoke({"refid": "R1"})))
    assert result["found"] is True
    assert [call["tool"] for call in calls] == ["bio_get_packet_stage_summary"]


def test_arguments_the_server_refuses_come_back_as_a_readable_result(served):
    tool = _tool("investigator", "bio_get_helper_record_fields")
    result = tool.invoke({"refid": "R1", "record_type": "ParkingHelperRecord"})
    assert result.startswith("The tool reported an error, so nothing was read.")
    assert "json_paths" in result


def test_calls_are_recorded_only_inside_recording(served):
    tool = _tool("investigator", "bio_get_packet_stage_summary")
    tool.invoke({"refid": "R1"})
    with mcp_client.recording() as calls:
        pass
    assert calls == []


def test_the_selection_can_be_overridden_per_role(served, monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_REVIEWER", "bio_get_parking_status, bio_get_packet_stage_summary")
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "none")
    assert {tool.name for tool in mcp_client.tools_for("reviewer", BIO)} == {
        "bio_get_parking_status", "bio_get_packet_stage_summary"}
    assert mcp_client.tools_for("investigator", BIO) == []


def test_an_override_naming_an_unserved_tool_is_an_error_when_every_server_answered(
        served, monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "get_parking_statuss")
    with pytest.raises(ValueError, match="no server serves"):
        mcp_client.tools_for("investigator", BIO)


def test_prompt_sections_name_tools_as_each_consumer_sees_them(served):
    deep = mcp_client.prompt_section("investigator", BIO)
    harness = mcp_client.prompt_section("investigator", BIO, opencode=True)
    assert deep.startswith(mcp_client.TOOLS_HEADING)
    assert "#### process_db" in deep and "Tools: bio_get_abis_candidates," in deep
    assert "agent_tools_bio_get_parking_status" in harness
    assert "agent_tools_" not in deep
    assert mcp_client.prompt_section("reviewer", BIO) == ""


def test_the_fingerprint_moves_with_what_is_served(stage_db, tool_server, monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    without = mcp_client.fingerprint_material(BIO)
    tool_server()
    with_tools = mcp_client.fingerprint_material(BIO)
    assert without != with_tools
    assert with_tools == mcp_client.fingerprint_material(BIO)


def test_the_same_tool_from_two_servers_is_taken_once(stage_db, tool_server, monkeypatch):
    first = tool_server()
    second = tool_server()
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({"a_tools": first, "b_tools": second}))
    catalog = mcp_client.current_catalog()
    assert catalog.complete
    assert {tool.server.name for tool in catalog.tools} == {"a_tools"}
    assert len(catalog.tools) == len(PROCESS_DB_TOOLS)


# ======================================================================
# Outages
# ======================================================================

def test_an_unreachable_server_leaves_an_incomplete_catalog_that_goes_stale(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({"down": "http://127.0.0.1:9/mcp"}))
    monkeypatch.setenv("AGENT_MCP_RETRY_SECONDS", "0.2")
    catalog = mcp_client.current_catalog()
    assert not catalog.complete
    assert "down" in catalog.failures
    assert catalog.tools == ()
    assert not mcp_client.is_stale(catalog)
    time.sleep(0.25)
    assert mcp_client.is_stale(catalog)
    assert mcp_client.current_catalog() is not catalog


def test_an_override_is_not_an_error_while_a_server_is_down(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({"down": "http://127.0.0.1:9/mcp"}))
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "bio_get_parking_status")
    assert mcp_client.tools_for("investigator", BIO) == []


def test_a_changed_server_list_makes_a_catalog_stale(served, monkeypatch):
    catalog = mcp_client.current_catalog()
    assert not mcp_client.is_stale(catalog)
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps({"other": "http://127.0.0.1:9/mcp"}))
    assert mcp_client.is_stale(catalog)
    assert mcp_client.is_stale(None) is False


def test_a_call_to_a_server_that_went_away_is_an_evidence_gap(stage_db, monkeypatch):
    """The tool object outlives its server: calls then report the gap."""
    catalog = mcp_client.Catalog(
        servers=(), tools=(mcp_client.RemoteTool(
            server=mcp_config.ServerConfig(name="gone", url="http://127.0.0.1:9/mcp"),
            name="bio_get_parking_status", description="d", input_schema={
                "type": "object", "properties": {"refid": {"type": "string"}}}),))
    tool = mcp_client._langchain_tool(catalog.tools[0])
    with mcp_client.recording() as calls:
        result = tool.invoke({"refid": "R1"})
    assert result.startswith("The tool bio_get_parking_status could not be called on the gone server")
    assert "evidence gap" in result
    assert calls[0]["result"] == result


# ======================================================================
# Tools a server lists without this repository's metadata
# ======================================================================

def _catalog_of(*tools):
    return mcp_client.Catalog(servers=(), tools=tuple(tools))


def _remote(name, agents=(), toolset=None):
    return mcp_client.RemoteTool(
        server=mcp_config.ServerConfig(name="org_tools", url="https://mcp.example/mcp"),
        name=name, description=f"{name} description", input_schema={"type": "object"},
        agents=agents, toolset=toolset)


def test_a_tool_that_names_no_roles_goes_to_none_until_configured(monkeypatch):
    catalog = _catalog_of(_remote("org_lookup"))
    assert mcp_client.selection("dlt_investigator", catalog=catalog) == []
    monkeypatch.setenv("AGENT_TOOLS_DLT_INVESTIGATOR", "org_lookup")
    assert [tool.name for tool in mcp_client.selection("dlt_investigator", catalog=catalog)] \
        == ["org_lookup"]


def test_an_undeclared_tool_reaches_a_service_only_once_named_common(monkeypatch):
    """It names no services, so a role override alone cannot add it to a
    service's scope; AGENT_TOOLS_COMMON makes it a tool for every service."""
    catalog = _catalog_of(_remote("org_lookup"))
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "org_lookup")
    assert mcp_client.selection("investigator", BIO, catalog=catalog) == []
    monkeypatch.setenv("AGENT_TOOLS_COMMON", "org_lookup")
    for service in (BIO, "_default"):
        assert [tool.name for tool in mcp_client.selection("investigator", service,
                                                           catalog=catalog)] \
            == ["org_lookup"]


def test_listing_metadata_is_read_defensively():
    import mcp_types

    tool = mcp_types.Tool(name="t", description="d", input_schema={"type": "object"},
                          _meta={mcp_config.META_AGENTS: ["investigator", "no_such_role"],
                                 mcp_config.META_TOOLSET: 7})
    remote = mcp_client._remote_tool(mcp_config.ServerConfig(name="s", url="http://x/mcp"), tool)
    assert remote.agents == ("investigator",)
    assert remote.toolset is None
    assert remote.read_only is None
    assert remote.services == ()


def test_unknown_roles_are_refused():
    with pytest.raises(ValueError, match="Unknown agent role"):
        mcp_client.selection("investigatr", BIO, catalog=_catalog_of())


def test_a_rejection_role_needs_a_service_and_a_dlt_role_takes_one_or_none():
    with pytest.raises(ValueError, match="scoped by service"):
        mcp_client.selection("investigator", catalog=_catalog_of())
    # A DLT role is scoped by its record's service when it has one, and by
    # role alone otherwise (MULTI_SERVICE_PLAN.md Phase 8); it has no
    # `_default` pack.
    mcp_client.selection("dlt_investigator", BIO, catalog=_catalog_of())
    mcp_client.selection("dlt_investigator", catalog=_catalog_of())
    with pytest.raises(ValueError, match="has no _default pack"):
        mcp_client.selection("dlt_investigator", "_default", catalog=_catalog_of())


# ======================================================================
# opencode
# ======================================================================

def test_opencode_gets_the_servers_and_one_agent_per_harness_role_and_service(served):
    config = mcp_client.opencode_config()
    assert config["mcp"] == {"agent_tools": {"type": "remote", "url": served, "enabled": True}}
    assert set(config["agent"]) == {
        "crm_investigator__enu_biometric", "crm_investigator__default",
        "crm_reviewer__enu_biometric", "crm_reviewer__default",
        "crm_dlt_investigator", "crm_dlt_reviewer",
        "crm_dlt_investigator__enu_biometric", "crm_dlt_reviewer__enu_biometric"}
    investigator = config["agent"]["crm_investigator__enu_biometric"]
    assert investigator["mode"] == "primary"
    assert investigator["tools"]["agent_tools_*"] is False
    assert {name for name, allowed in investigator["tools"].items() if allowed} == {
        f"agent_tools_{name}" for name in PROCESS_DB_TOOLS}
    assert config["agent"]["crm_investigator__default"]["tools"] == {"agent_tools_*": False}
    assert config["agent"]["crm_dlt_investigator"]["tools"] == {"agent_tools_*": False}


def test_opencode_gets_nothing_without_servers(monkeypatch):
    monkeypatch.delenv("AGENT_MCP_SERVERS", raising=False)
    assert mcp_client.opencode_config() == {}


def test_opencode_headers_are_passed_on(monkeypatch):
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps(
        {"org_tools": {"url": "http://127.0.0.1:9/mcp", "headers": {"X-Key": "k"}}}))
    assert mcp_client.opencode_config()["mcp"]["org_tools"]["headers"] == {"X-Key": "k"}


def test_harness_tool_calls_become_evidence_under_their_own_names(served):
    records = mcp_client.evidence_from_harness([
        {"tool": "agent_tools_bio_get_parking_status", "input": {"refid": "R1"},
         "output": '{"parked_now": false}', "status": "completed"},
        {"tool": "grep", "input": {"pattern": "x"}, "output": "...", "status": "completed"},
        {"tool": "agent_tools_bio_get_abis_candidates", "input": "not a dict",
         "output": "x" * (mcp_client.MAX_RECORDED_RESULT_CHARS + 50), "status": "completed"},
    ])
    assert [(r["tool"], r["args"]) for r in records] == [
        ("bio_get_parking_status", {"refid": "R1"}), ("bio_get_abis_candidates", {})]
    assert len(records[1]["result"]) <= mcp_client.MAX_RECORDED_RESULT_CHARS


# ======================================================================
# Evidence merging and rendering
# ======================================================================

def _rec(tool, refid, result):
    return {"tool": tool, "args": {"refid": refid}, "result": result}


def test_merge_keeps_one_record_per_call_newest_last():
    merged = mcp_client.merge_evidence(
        [_rec("a", "1", "old"), _rec("b", "1", "b")],
        [_rec("a", "1", "new"), _rec("a", "2", "other")])
    assert [(r["tool"], r["args"]["refid"], r["result"]) for r in merged] == [
        ("b", "1", "b"), ("a", "1", "new"), ("a", "2", "other")]


def test_merge_caps_the_number_of_records():
    many = [_rec("t", str(i), "r") for i in range(mcp_client.MAX_EVIDENCE_RECORDS + 5)]
    merged = mcp_client.merge_evidence(None, many)
    assert len(merged) == mcp_client.MAX_EVIDENCE_RECORDS
    assert merged[-1]["args"]["refid"] == str(mcp_client.MAX_EVIDENCE_RECORDS + 4)


def test_render_is_empty_without_records():
    assert mcp_client.render_evidence(None) == ""
    assert mcp_client.render_evidence([]) == ""


def test_render_lists_calls_in_order_with_their_arguments():
    text = mcp_client.render_evidence([_rec("a", "1", "first"), _rec("b", "2", "second")])
    assert text.index('[1] a {"refid": "1"}') < text.index('[2] b {"refid": "2"}')


def test_render_keeps_the_newest_and_says_how_many_it_left_out():
    records = [_rec("t", str(i), "r" * 100) for i in range(10)]
    text = mcp_client.render_evidence(records, max_chars=450)
    assert "left out to fit" in text
    assert '"refid": "9"' in text
    assert '"refid": "0"' not in text


def test_render_budget_falls_back_on_a_bad_setting(monkeypatch):
    monkeypatch.setenv(mcp_client.ENV_EVIDENCE_MAX_CHARS, "lots")
    assert mcp_client.evidence_max_chars() == mcp_client.DEFAULT_EVIDENCE_MAX_CHARS


# ======================================================================
# A tool added to the package reaches the agents with no other change
# ======================================================================

def test_a_new_registered_tool_is_served_and_offered_to_its_role(tool_server):
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    try:
        toolset = Toolset(name="late_addition", agents=("reviewer",),
                          guidance="Use echo_refid to echo.", read_only=True,
                          services=("*",))

        @agent_tool(toolset)
        def echo_refid(refid: str) -> str:
            """Echo a refId back."""
            return f"echo:{refid}"

        tool_server()
        tools = mcp_client.tools_for("reviewer", BIO)
        assert [tool.name for tool in tools] == ["echo_refid"]
        assert tools[0].invoke({"refid": "R7"}) == "echo:R7"
        assert "Use echo_refid to echo." in mcp_client.prompt_section("reviewer", BIO)
    finally:
        agent_tools._registry.clear()
        agent_tools._registry.update(saved)


# ======================================================================
# CLI
# ======================================================================

def test_cli_lists_prompts_and_calls(served, capsys):
    assert mcp_client.main(["list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert set(listed["services"][BIO]["investigator"]) == PROCESS_DB_TOOLS
    assert listed["services"]["_default"]["investigator"] == []
    assert listed["roles"] == {"dlt_investigator": [], "dlt_reviewer": [],
                               "dlt_synthesis": []}
    assert listed["failed"] == {}

    assert mcp_client.main(["prompt", "investigator", "--service", BIO, "--opencode"]) == 0
    assert "agent_tools_bio_get_parking_status" in capsys.readouterr().out

    assert mcp_client.main(["call", "bio_get_packet_stage_summary", '{"refid": "R1"}']) == 0
    assert json.loads(capsys.readouterr().out)["found"] is True
    assert mcp_client.main(["call", "no_such_tool"]) == 1


# ======================================================================
# A documentation server (kind "docs")
# ======================================================================

DOCS = mcp_config.ServerConfig(name="droa_docs", url="http://docs.example/mcp", kind="docs")


def _listed(name, meta=None):
    import mcp_types

    return mcp_types.Tool(name=name, description=f"{name} description",
                          input_schema={"type": "object"}, _meta=meta)


def _docs_tool(name):
    """A docs tool as the catalog would take it from a listing."""
    return mcp_client._remote_tool(DOCS, _listed(name))


def _servers(monkeypatch, **urls):
    """Point AGENT_MCP_SERVERS at the tools server and the docs server."""
    config = {}
    if "tools" in urls:
        config["agent_tools"] = {"url": urls["tools"]}
    if "docs" in urls:
        config["droa_docs"] = {"url": urls["docs"], "kind": "docs"}
    monkeypatch.setenv("AGENT_MCP_SERVERS", json.dumps(config))


@pytest.fixture
def docs_only(docs_server, monkeypatch):
    """The fake documentation server, alone."""
    url = docs_server()
    _servers(monkeypatch, docs=url)
    return url


@pytest.fixture
def both(served, docs_server, monkeypatch):
    """The real tool server with the process DB tools, and the fake
    documentation server."""
    url = docs_server()
    _servers(monkeypatch, tools=served, docs=url)
    return served, url


def _names(tools):
    return [tool.name for tool in tools]


class LogRecorder:
    """Stands in for mcp_client's logger; keeps (level, event, fields)."""

    def __init__(self):
        self.lines = []

    def __getattr__(self, level):
        return lambda event, **fields: self.lines.append((level, event, fields))

    def calls(self):
        return [line for line in self.lines if line[1] == "Agent tool call"]


@pytest.fixture
def log(monkeypatch):
    recorder = LogRecorder()
    monkeypatch.setattr(mcp_client, "logger", recorder)
    return recorder


# ---------------------------------------------------------------- catalog

def test_a_docs_listing_without_metadata_is_for_every_service_and_the_docs_roles(both):
    catalog = mcp_client.current_catalog()
    assert catalog.complete
    docs = [tool for tool in catalog.tools if tool.is_docs]
    assert sorted(_names(docs)) == sorted(DOCS_TOOLS)
    for tool in docs:
        assert tool.services == ("*",)
        assert tool.agents == agent_tools.AGENT_ROLES
        assert tool.toolset == mcp_client.DOCS_TOOLSET
        assert tool.guidance == mcp_client.DOCS_GUIDANCE
    # The tools server's tools are catalogued as before.
    assert {tool.name for tool in catalog.tools if not tool.is_docs} == PROCESS_DB_TOOLS


def test_a_docs_listing_may_name_its_toolset_and_guidance_but_not_its_audience(monkeypatch):
    monkeypatch.setenv("AGENT_DOCS_ROLES", "reviewer")
    tool = mcp_client._remote_tool(DOCS, _listed("docs_search", {
        mcp_config.META_TOOLSET: "DROA corpus", mcp_config.META_GUIDANCE: "Read it.",
        mcp_config.META_AGENTS: ["investigator"], mcp_config.META_SERVICES: [BIO]}))
    assert (tool.toolset, tool.guidance) == ("DROA corpus", "Read it.")
    assert (tool.agents, tool.services) == (("reviewer",), ("*",))


def test_the_prefix_rule_accepts_a_tool_for_every_service_without_a_prefix():
    from src.utils import service_registry

    assert service_registry.tool_prefix_error("docs_search", ("*",)) is None
    assert service_registry.tool_prefix_error("bio_docs_search", ("*",)) is not None


# -------------------------------------------------------------- selection

def test_docs_tools_follow_every_tools_server_tool(both):
    assert _names(mcp_client.tools_for("investigator", BIO)) == \
        sorted(PROCESS_DB_TOOLS) + sorted(DOCS_TOOLS)
    # Every role, every scope, including the unresolved pack.
    for role in agent_tools.SERVICE_ROLES:
        for service in (BIO, "_default"):
            assert set(DOCS_TOOLS) <= set(_names(mcp_client.tools_for(role, service)))


def test_docs_tools_survive_a_role_override_and_none(both, monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "bio_get_parking_status")
    assert _names(mcp_client.tools_for("investigator", BIO)) == \
        ["bio_get_parking_status", *sorted(DOCS_TOOLS)]
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "none")
    assert _names(mcp_client.tools_for("investigator", BIO)) == sorted(DOCS_TOOLS)


def test_a_role_override_may_not_name_a_docs_tool(both, monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "bio_get_parking_status,docs_search")
    with pytest.raises(ValueError, match="controlled by AGENT_DOCS_ROLES"):
        mcp_client.tools_for("investigator", BIO)


def test_a_role_outside_the_docs_roles_gets_no_docs_tool(monkeypatch):
    monkeypatch.setenv("AGENT_DOCS_ROLES", "investigator")
    catalog = _catalog_of(_docs_tool("docs_search"))
    assert _names(mcp_client.selection("investigator", BIO, catalog=catalog)) == ["docs_search"]
    assert mcp_client.selection("reviewer", BIO, catalog=catalog) == []
    assert mcp_client.selection("dlt_investigator", catalog=catalog) == []
    monkeypatch.setenv("AGENT_DOCS_ROLES", "none")
    catalog = _catalog_of(_docs_tool("docs_search"))
    assert mcp_client.selection("investigator", BIO, catalog=catalog) == []


def test_dlt_roles_get_docs_tools_with_and_without_a_service():
    catalog = _catalog_of(_docs_tool("docs_search"))
    for role in agent_tools.DLT_ROLES:
        assert _names(mcp_client.selection(role, catalog=catalog)) == ["docs_search"]
        assert _names(mcp_client.selection(role, BIO, catalog=catalog)) == ["docs_search"]


# --------------------------------------------------------- prompt section

def test_the_docs_section_follows_the_tools_section_and_names_the_service(both):
    section = mcp_client.prompt_section("investigator", BIO)
    assert section.startswith(mcp_client.TOOLS_HEADING)
    assert section.count(mcp_client.COMMON_RULES) == 1
    docs = section[section.index(mcp_client.DOCS_HEADING):]
    assert section.index(mcp_client.COMMON_RULES) < section.index(mcp_client.DOCS_HEADING)
    assert docs.startswith(f"{mcp_client.DOCS_HEADING}\nTools: {', '.join(sorted(DOCS_TOOLS))}.")
    assert "This packet's documentation service is `enu-biometric`." in docs
    assert docs.endswith(mcp_client.DOCS_GUIDANCE)
    # The docs tools are never listed under the evidence rules.
    assert not any(name in section[:section.index(mcp_client.DOCS_HEADING)]
                   for name in DOCS_TOOLS)


def test_an_unresolved_packet_is_told_to_find_its_service(both):
    for role, service in (("investigator", "_default"), ("dlt_investigator", None)):
        section = mcp_client.prompt_section(role, service)
        assert mcp_client.DOCS_SERVICE_UNRESOLVED in section
        assert "documentation service is" not in section
    assert "documentation service is `enu-biometric`" in \
        mcp_client.prompt_section("dlt_investigator", BIO)


def test_a_role_with_only_docs_tools_gets_only_the_docs_section(both):
    section = mcp_client.prompt_section("reviewer", BIO)
    assert section.startswith(mcp_client.DOCS_HEADING)
    assert mcp_client.TOOLS_HEADING not in section
    assert mcp_client.COMMON_RULES not in section


def test_an_unreachable_docs_server_is_named_in_the_prompt(monkeypatch):
    """A docs server that could not be listed used to leave the agents with no
    docs tools and no word of it (2026-09-29: droa_docs down all day, every
    Investigator made no tool call, every Reviewer approved)."""
    _servers(monkeypatch, docs="http://127.0.0.1:9/mcp")
    mcp_client.reset()

    section = mcp_client.prompt_section("investigator", BIO)
    assert section == f"{mcp_client.DOCS_HEADING}\n{mcp_client.DOCS_UNAVAILABLE}"
    assert mcp_client.DOCS_UNAVAILABLE in mcp_client.prompt_section("reviewer", BIO)
    # The harness is given no docs server either way, so it is told nothing.
    assert mcp_client.prompt_section("investigator", BIO, opencode=True) == ""


def test_an_unreachable_docs_server_is_not_named_to_a_role_that_never_reads_it(monkeypatch):
    _servers(monkeypatch, docs="http://127.0.0.1:9/mcp")
    monkeypatch.setenv("AGENT_DOCS_ROLES", "investigator")
    mcp_client.reset()

    assert mcp_client.prompt_section("reviewer", BIO) == ""


def test_an_unreachable_tools_server_says_nothing_about_documentation(monkeypatch):
    _servers(monkeypatch, tools="http://127.0.0.1:9/mcp")
    mcp_client.reset()

    assert mcp_client.DOCS_UNAVAILABLE not in mcp_client.prompt_section("investigator", BIO)


def _every_section(opencode=False):
    sections = {}
    for role in agent_tools.SERVICE_ROLES:
        for service in (BIO, "_default"):
            sections[role, service] = mcp_client.prompt_section(role, service,
                                                                opencode=opencode)
    for role in agent_tools.DLT_ROLES:
        for service in (None, BIO):
            sections[role, service] = mcp_client.prompt_section(role, service,
                                                                opencode=opencode)
    return sections


def test_sections_are_byte_identical_for_a_role_without_docs_tools(served, docs_server,
                                                                   monkeypatch):
    without = _every_section()
    harness = _every_section(opencode=True)
    assert without[("investigator", BIO)].startswith(mcp_client.TOOLS_HEADING)

    _servers(monkeypatch, tools=served, docs=docs_server())
    monkeypatch.setenv("AGENT_DOCS_ROLES", "none")
    mcp_client.reset()
    assert _every_section() == without
    # The harness never sees docs tools, whoever gets them.
    monkeypatch.delenv("AGENT_DOCS_ROLES")
    mcp_client.reset()
    assert _every_section(opencode=True) == harness


def test_the_fingerprint_moves_with_the_docs_server(served, docs_server, monkeypatch):
    without = mcp_client.fingerprint_material(BIO)
    _servers(monkeypatch, tools=served, docs=docs_server())
    mcp_client.reset()
    with_docs = mcp_client.fingerprint_material(BIO)
    assert with_docs != without
    assert mcp_client.DOCS_HEADING in with_docs and "docs_search" in with_docs
    monkeypatch.setenv("AGENT_DOCS_ROLES", "investigator")
    mcp_client.reset()
    assert mcp_client.fingerprint_material(BIO) not in (without, with_docs)


# --------------------------------------------------------------- evidence

def test_docs_calls_are_not_evidence_and_tool_calls_still_are(both):
    docs = _tool("investigator", "docs_read")
    lookup = _tool("investigator", "bio_get_packet_stage_summary")
    with mcp_client.recording() as calls:
        assert docs.invoke({"service": BIO, "path": "flows.md"}) == \
            "enu-biometric/flows.md: the document"
        asyncio.run(docs.ainvoke({"service": BIO, "path": "flows.md"}))
        lookup.invoke({"refid": "R1"})
    assert [call["tool"] for call in calls] == ["bio_get_packet_stage_summary"]


# ---------------------------------------------------------------- harness

def test_the_harness_never_sees_a_docs_server(both):
    tools_url, _ = both
    config = mcp_client.opencode_config()
    assert config["mcp"] == {"agent_tools": {"type": "remote", "url": tools_url,
                                             "enabled": True}}
    for agent in config["agent"].values():
        assert not any(name.startswith("droa_docs_") for name in agent["tools"])
    records = mcp_client.evidence_from_harness([
        {"tool": "droa_docs_docs_search", "input": {"query": "x"}, "output": "hits"},
        {"tool": "agent_tools_bio_get_parking_status", "input": {"refid": "R1"},
         "output": "{}"},
    ])
    assert [record["tool"] for record in records] == ["bio_get_parking_status"]
    assert mcp_client.prompt_section("reviewer", BIO, opencode=True) == ""


def test_the_harness_gets_nothing_with_only_a_docs_server(docs_only):
    assert mcp_client.current_catalog().tools
    assert mcp_client.opencode_config() == {}
    assert mcp_client.evidence_from_harness([
        {"tool": "droa_docs_docs_search", "input": {}, "output": "hits"}]) == []


def test_the_cli_list_shows_each_servers_kind(both, capsys):
    assert mcp_client.main(["list"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert {server["name"]: server["kind"] for server in listed["servers"]} == {
        "agent_tools": "tools", "droa_docs": "docs"}


# ---------------------------------------------------------------- logging

def test_an_answered_call_is_logged_once_at_info(docs_only, log):
    tool = _tool("investigator", "docs_read")
    result = tool.invoke({"service": BIO, "path": "flows.md"})
    [(level, _, fields)] = log.calls()
    assert level == "info"
    assert fields["server"] == "droa_docs" and fields["server_kind"] == "docs"
    assert fields["tool"] == "docs_read" and fields["outcome"] == "ok"
    assert isinstance(fields["duration_ms"], int) and fields["duration_ms"] >= 0
    assert fields["result_chars"] == len(result)
    assert json.loads(fields["args"]) == {"service": BIO, "path": "flows.md"}
    # Outside a call context: no role, no ids, not even as None.
    assert not {"role", "event_id", "case_id", "error"} & set(fields)
    # The result body is never in an INFO line.
    assert result not in json.dumps(fields)


def test_a_tool_error_is_logged_once_at_warning(docs_only, log):
    result = _tool("investigator", "docs_read").invoke({"service": BIO, "path": "boom"})
    assert result.startswith("The tool reported an error")
    [(level, _, fields)] = log.calls()
    assert (level, fields["outcome"]) == ("warning", "tool_error")
    assert "error" not in fields


def test_a_failed_call_is_logged_once_at_error(log):
    remote = mcp_client.RemoteTool(server=mcp_config.ServerConfig(
        name="gone", url="http://127.0.0.1:9/mcp"), name="lookup", description="d",
        input_schema={"type": "object", "properties": {"refid": {"type": "string"}}})
    with mcp_client.call_context(role="investigator", event_id="e-1"):
        mcp_client._langchain_tool(remote).invoke({"refid": "R1"})
    [(level, _, fields)] = log.calls()
    assert (level, fields["outcome"]) == ("error", "call_failed")
    assert fields["server_kind"] == "tools"
    assert fields["error"].startswith(("ConnectError", "ConnectionRefusedError", "OSError"))
    assert (fields["role"], fields["event_id"]) == ("investigator", "e-1")
    assert not [line for line in log.lines if line[1] == "Agent tool call failed"]


def test_the_result_is_logged_only_at_debug_and_only_its_start(docs_only, log):
    _tool("investigator", "docs_search").invoke({"query": "q" * 1000})
    debug = [fields for level, _, fields in log.lines if level == "debug"]
    assert len(debug) == 1
    assert len(debug[0]["result"]) == mcp_client.MAX_LOGGED_RESULT_CHARS


def test_long_arguments_are_cut(docs_only, log):
    _tool("investigator", "docs_search").invoke({"query": "q" * 2000})
    [(_, _, fields)] = log.calls()
    assert len(fields["args"]) == mcp_client.MAX_LOGGED_ARGS_CHARS
    assert fields["args"].endswith("...")


def test_the_call_context_reaches_the_log_from_a_worker_thread(docs_only, log):
    """As the graphs run tools: on a worker thread with a copy of the
    caller's context -- and, with a loop running there, the call itself on
    `_run_sync`'s helper thread, which has none."""
    import contextvars
    from concurrent.futures import ThreadPoolExecutor

    tool = _tool("dlt_investigator", "docs_search", service=None)

    async def with_a_running_loop():
        return tool.invoke({"query": "q"})

    with mcp_client.call_context(role="dlt_investigator", case_id="case-7", event_id=None):
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(contextvars.copy_context().run, tool.invoke, {"query": "q"}).result()
            pool.submit(contextvars.copy_context().run,
                        lambda: asyncio.run(with_a_running_loop())).result()
    lines = log.calls()
    assert len(lines) == 2
    for _, _, fields in lines:
        assert (fields["role"], fields["case_id"]) == ("dlt_investigator", "case-7")
        assert "event_id" not in fields
