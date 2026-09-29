"""
Tools per service (MULTI_SERVICE_PLAN.md Phase 4, D7-D9).

- The selection matrix: role x service x (common, specific, undeclared,
  include, exclude, AGENT_TOOLS_<ROLE> override), over a catalog built here.
- The prefix rule and the scope checks: what fails start-up, and what a
  served tool breaking the rule costs (it is left out of the catalog).
- The opencode config gives each per-service agent exactly its tools.
- A fixture service's Investigator -- and its `task` subagent -- is offered no
  `bio_` tool, with real deep agents over the real tool server.
- The fingerprint moves per service.

Every test builds its own registry under tmp_path: enu-biometric (prefix
`bio`), a fixture service `svc-demo` (prefix `demo`) whose pack includes and
excludes tools by name, and `_default`.
"""
import json

import pytest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from src.core.agent_factory import build_agent
from src.tools import agent_tools, mcp_client, mcp_config
from src.tools.agent_tools import Toolset, agent_tool
from src.utils import paths
from src.utils import service_registry as sr

BIO = "enu-biometric"
DEMO = "svc-demo"


def _write_pack(root, document, policy="The policy of this service."):
    directory = root / document["service"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "service.json").write_text(json.dumps(document), encoding="utf-8")
    (directory / "policy.md").write_text(policy, encoding="utf-8")


def _pack(name, prefix, stages, **extra):
    document = {"schema_version": 1, "service": name, "display_name": f"{name} service",
                "tool_prefix": prefix, "match": {"stages": stages},
                "rule_source": {"type": "none"}}
    document.update(extra)
    return document


DEFAULT_PACK = {"schema_version": 1, "service": "_default", "display_name": "Default",
                "match": {}, "rule_source": {"type": "none"}}


@pytest.fixture
def estate(tmp_path, monkeypatch):
    """The registry every test here reads."""
    root = tmp_path / "packs"
    _write_pack(root, DEFAULT_PACK)
    _write_pack(root, _pack(BIO, "bio", ["Biometric"], rule_source={"type": "rules_db"}))
    _write_pack(root, _pack(DEMO, "demo", ["Demographic"],
                            tools={"include": ["org_extra"], "exclude": ["common_noisy"]}))
    monkeypatch.setattr(paths, "SERVICE_PACKS_DIR", root)
    assert not sr.load().errors
    return root


@pytest.fixture
def registry_snapshot():
    """Restore the tool registry after a test registers throwaway tools."""
    agent_tools.discover()
    saved = dict(agent_tools._registry)
    yield
    agent_tools._registry.clear()
    agent_tools._registry.update(saved)


# ======================================================================
# The selection matrix
# ======================================================================

def _remote(name, services=(), agents=("investigator",)):
    return mcp_client.RemoteTool(
        server=mcp_config.ServerConfig(name="org_tools", url="https://mcp.example/mcp"),
        name=name, description=f"{name} description", input_schema={"type": "object"},
        agents=agents, services=tuple(services), toolset=None)


CATALOG = mcp_client.Catalog(servers=(), tools=(
    _remote("common_lookup", ["*"], agents=("investigator", "reviewer")),
    _remote("common_noisy", ["*"]),
    _remote("bio_lookup", [BIO]),
    _remote("demo_lookup", [DEMO]),
    _remote("org_lookup"),
    _remote("org_extra"),
    # Naming the default pack reaches nobody: it is no service.
    _remote("odd_lookup", ["_default"]),
    _remote("dlt_lookup", ["*"], agents=("dlt_investigator",)),
))


def _names(role, service):
    return {tool.name for tool in mcp_client.selection(role, service, catalog=CATALOG)}


@pytest.mark.parametrize("role, service, expected", [
    # Common tools, the service's own, and the pack's include; less its exclude.
    ("investigator", BIO, {"common_lookup", "common_noisy", "bio_lookup"}),
    ("investigator", DEMO, {"common_lookup", "demo_lookup", "org_extra"}),
    # An unresolved packet: the tools for every service, and no other.
    ("investigator", "_default", {"common_lookup", "common_noisy"}),
    # The scope never widens the roles: only common_lookup names the reviewer,
    # and org_extra, included for svc-demo, is the investigator's alone.
    ("reviewer", BIO, {"common_lookup"}),
    ("reviewer", DEMO, {"common_lookup"}),
    ("synthesis", BIO, set()),
    ("log_filter", "_default", set()),
])
def test_the_selection_is_scoped_by_role_and_service(estate, role, service, expected):
    assert _names(role, service) == expected


def test_a_dlt_role_keeps_the_role_only_selection(estate):
    assert {tool.name for tool in mcp_client.selection("dlt_investigator",
                                                       catalog=CATALOG)} == {"dlt_lookup"}


def test_agent_tools_common_makes_an_undeclared_tool_common(estate, monkeypatch):
    monkeypatch.setenv(mcp_client.ENV_COMMON, "org_lookup, bio_lookup")
    for service in (BIO, DEMO, "_default"):
        assert "org_lookup" in _names("investigator", service), service
    # It names undeclared tools only: a tool scoped to a service stays there.
    assert "bio_lookup" not in _names("investigator", DEMO)


def test_the_role_override_narrows_but_never_widens_the_scope(estate, monkeypatch):
    monkeypatch.setenv("AGENT_TOOLS_INVESTIGATOR", "bio_lookup,demo_lookup,org_lookup")
    assert _names("investigator", BIO) == {"bio_lookup"}
    assert _names("investigator", DEMO) == {"demo_lookup"}
    assert _names("investigator", "_default") == set()
    # An override can hand a role a tool it does not name -- as always -- but
    # only inside the scope.
    monkeypatch.setenv("AGENT_TOOLS_REVIEWER", "bio_lookup")
    assert _names("reviewer", BIO) == {"bio_lookup"}
    assert _names("reviewer", DEMO) == set()


def test_a_service_with_no_pack_gets_only_what_names_it(estate):
    assert _names("investigator", "svc-unregistered") == {"common_lookup", "common_noisy"}


def test_a_packs_exclude_removes_a_docs_tool(estate):
    """A docs tool is for every service, whatever its listing says, and a
    pack's tools.exclude still removes it for that pack."""
    import mcp_types

    docs = mcp_config.ServerConfig(name="droa_docs", url="http://docs.example/mcp",
                                   kind="docs")
    catalog = mcp_client.Catalog(servers=(docs,), tools=tuple(
        mcp_client._remote_tool(docs, mcp_types.Tool(
            name=name, description="d", input_schema={"type": "object"}))
        for name in ("common_noisy", "docs_search")))
    for role in ("investigator", "reviewer"):
        assert {tool.name for tool in mcp_client.selection(role, BIO, catalog=catalog)} \
            == {"common_noisy", "docs_search"}
        assert {tool.name for tool in mcp_client.selection(role, DEMO, catalog=catalog)} \
            == {"docs_search"}


def test_the_prompt_section_lists_the_scoped_tools(estate, monkeypatch):
    monkeypatch.setattr(mcp_client, "current_catalog", lambda: CATALOG)
    demo = mcp_client.prompt_section("investigator", DEMO)
    assert "demo_lookup" in demo and "bio_lookup" not in demo
    assert "bio_lookup" in mcp_client.prompt_section("investigator", BIO)


# ======================================================================
# Declaring a scope
# ======================================================================

@pytest.mark.parametrize("services, message", [
    (("Svc_Bad",), "malformed service"),
    # The default pack is no service a tool can be for.
    (("_default",), "malformed service"),
    (("*", BIO), "stands alone"),
])
def test_a_malformed_scope_is_refused_when_declared(services, message):
    with pytest.raises(ValueError, match=message):
        Toolset(name="scoped", agents=("investigator",), services=services)


def test_a_scope_is_normalised():
    assert Toolset(name="one", agents=(), services=BIO).services == (BIO,)
    assert Toolset(name="two", agents=(), services=(BIO, DEMO, BIO)).services == (BIO, DEMO)
    assert Toolset(name="none", agents=()).services == ()


# ======================================================================
# The prefix rule and the scope checks at start-up (5.7, D8)
# ======================================================================

@pytest.mark.parametrize("name, services, expected", [
    ("bio_lookup", [BIO], None),
    ("lookup", [BIO], "must start with enu-biometric's tool prefix, 'bio_'"),
    ("demo_lookup", [BIO], "must start with enu-biometric's tool prefix, 'bio_'"),
    ("lookup", ["*"], None),
    ("bio_lookup", ["*"], "carries enu-biometric's tool prefix 'bio_'"),
    ("demo_lookup", [BIO, DEMO], "carries svc-demo's tool prefix 'demo_'"),
    ("shared_lookup", [BIO, DEMO], None),
    # Undeclared: not judged. Unregistered: reported by the scope check.
    ("bio_lookup", [], None),
    ("lookup", ["svc-ghost"], None),
    # The prefix is the prefix and an underscore, not any shared start.
    ("biometric_lookup", ["*"], None),
])
def test_the_prefix_rule(estate, name, services, expected):
    problem = sr.tool_prefix_error(name, services)
    if expected is None:
        assert problem is None
    else:
        assert expected in problem


def test_the_shipped_tools_pass_the_scope_checks():
    assert agent_tools.scope_problems() == ([], [])
    assert all(entry.tool.name.startswith("bio_") and entry.toolset.services == (BIO,)
               for entry in agent_tools.entries())


def _register(toolset, name, module="probe.module"):
    def lookup(refid: str) -> str:
        """A probe."""
        return refid

    lookup.__module__ = module
    agent_tool(toolset, name=name)(lookup)


@pytest.mark.parametrize("toolset, name, expected", [
    (Toolset(name="p1", agents=("investigator",), services=(DEMO,)), "unprefixed",
     "must start with svc-demo's tool prefix, 'demo_'"),
    (Toolset(name="p2", agents=("investigator",), services=("*",)), "bio_common",
     "carries enu-biometric's tool prefix 'bio_'"),
    (Toolset(name="p3", agents=("investigator",), services=("svc-ghost",)), "ghost_lookup",
     "names service 'svc-ghost', which has no valid pack"),
])
def test_a_scope_error_fails_start_up(estate, registry_snapshot, monkeypatch,
                                      toolset, name, expected):
    # Imported first: importing it runs the boot checks, which pass here.
    from src import main_api

    _register(toolset, name)

    errors, _warnings = sr.validate()
    assert any(expected in error for error in errors), errors
    with pytest.raises(SystemExit):
        main_api.validate_service_registry()


def test_a_local_name_clash_fails_start_up(estate, registry_snapshot, tmp_path, monkeypatch):
    """Two modules registering one name fail discovery, which the start-up
    checks report with the module's name."""
    import sys

    (tmp_path / "clash_one.py").write_text(
        "from src.tools.agent_tools import Toolset, agent_tool\n"
        "T = Toolset(name='clash', agents=('investigator',), services=('*',))\n"
        "@agent_tool(T)\n"
        "def clashing_lookup(refid: str) -> str:\n"
        "    '''One.'''\n"
        "    return refid\n", encoding="utf-8")
    (tmp_path / "clash_two.py").write_text(
        "from src.tools.agent_tools import Toolset, agent_tool\n"
        "T = Toolset(name='clash2', agents=('investigator',), services=('*',))\n"
        "@agent_tool(T)\n"
        "def clashing_lookup(refid: str) -> str:\n"
        "    '''Two.'''\n"
        "    return refid\n", encoding="utf-8")
    monkeypatch.setattr(agent_tools, "__path__", [*agent_tools.__path__, str(tmp_path)])
    monkeypatch.setattr(agent_tools, "_discovered", False)
    try:
        errors, _warnings = sr.validate()
        assert any("registered twice" in error for error in errors), errors
        assert any("registered twice" in error for error in agent_tools.validate())
    finally:
        for name in ("clash_one", "clash_two"):
            sys.modules.pop(f"src.tools.agent_tools.{name}", None)


def test_an_undeclared_toolset_for_a_rejection_role_is_a_warning(estate, registry_snapshot):
    _register(Toolset(name="undeclared", agents=("reviewer",)), "plain_lookup")
    _register(Toolset(name="dlt_only", agents=("dlt_investigator",)), "dlt_plain_lookup")

    errors, warnings = sr.validate()
    assert errors == []
    joined = "\n".join(warnings)
    assert "Toolset 'undeclared'" in joined
    assert "Toolset 'dlt_only'" not in joined


def test_subpackages_are_discovered(registry_snapshot):
    modules = {entry.module for entry in agent_tools.entries()}
    assert "src.tools.agent_tools.enu_biometric.stage_tracker" in modules
    assert agent_tools._tool_modules(agent_tools.__path__, agent_tools.__name__)[:2] == [
        "src.tools.agent_tools.common", "src.tools.agent_tools.enu_biometric"]


@pytest.mark.parametrize("change, expected", [
    ({"tools": {"include": ["org_lookup"]}}, "the default pack takes no tools.include"),
])
def test_the_default_pack_includes_no_tools(tmp_path, change, expected):
    root = tmp_path / "packs"
    document = dict(DEFAULT_PACK)
    document.update(change)
    _write_pack(root, document)
    assert any(expected in error for error in sr.load(root).errors)


def test_a_service_may_not_take_the_default_agents_name(tmp_path):
    root = tmp_path / "packs"
    _write_pack(root, DEFAULT_PACK)
    _write_pack(root, _pack("default", "dflt", ["X"]))
    assert any("is reserved" in error for error in sr.load(root).errors)


def test_service_slugs():
    assert sr.service_slug(BIO) == "enu_biometric"
    assert sr.service_slug("_default") == "default"
    assert mcp_client.opencode_agent("reviewer", BIO) == "crm_reviewer__enu_biometric"
    assert mcp_client.opencode_agent("reviewer", None) == "crm_reviewer__default"
    assert mcp_client.opencode_agent("dlt_reviewer") == "crm_dlt_reviewer"
    assert mcp_client.opencode_agent("dlt_reviewer", BIO) == "crm_dlt_reviewer__enu_biometric"


# ======================================================================
# Over the real tool server
# ======================================================================

@pytest.fixture
def served(estate, registry_snapshot, tool_server):
    """Four tools served over MCP: one for every service, one for each of the
    two services, and one breaking the prefix rule."""
    common = Toolset(name="common_probe", agents=("investigator", "reviewer"),
                     services=("*",), guidance="Common guidance.")
    bio = Toolset(name="bio_probe", agents=("investigator",), services=(BIO,),
                  guidance="Biometric guidance.")
    demo = Toolset(name="demo_probe", agents=("investigator",), services=(DEMO,))

    @agent_tool(common)
    def common_probe(refid: str) -> str:
        """Common probe."""
        return f"common:{refid}"

    @agent_tool(bio)
    def bio_probe(refid: str) -> str:
        """Biometric probe."""
        return f"bio:{refid}"

    @agent_tool(demo)
    def demo_probe(refid: str) -> str:
        """Demographic probe."""
        return f"demo:{refid}"

    @agent_tool(common)
    def bio_misnamed(refid: str) -> str:
        """For every service, but named for one."""
        return refid

    return tool_server()


def test_a_served_tool_breaking_the_prefix_rule_is_left_out(served):
    names = {tool.name for tool in mcp_client.current_catalog().tools}
    assert {"common_probe", "bio_probe", "demo_probe"} <= names
    assert "bio_misnamed" not in names
    listed = next(tool for tool in mcp_client.current_catalog().tools
                  if tool.name == "demo_probe")
    assert listed.services == (DEMO,)


def test_opencode_gives_each_per_service_agent_exactly_its_tools(served):
    config = mcp_client.opencode_config()

    def allowed(agent):
        return {name for name, on in config["agent"][agent]["tools"].items() if on}

    assert set(config["agent"]) == {
        "crm_investigator__enu_biometric", "crm_investigator__svc_demo",
        "crm_investigator__default", "crm_reviewer__enu_biometric",
        "crm_reviewer__svc_demo", "crm_reviewer__default",
        # The DLT roles: one per registered service, and one with no service
        # for a record analysed with no pack (MULTI_SERVICE_PLAN.md Phase 8).
        "crm_dlt_investigator", "crm_dlt_reviewer",
        "crm_dlt_investigator__enu_biometric", "crm_dlt_investigator__svc_demo",
        "crm_dlt_reviewer__enu_biometric", "crm_dlt_reviewer__svc_demo"}
    assert allowed("crm_investigator__enu_biometric") == {
        "agent_tools_common_probe", "agent_tools_bio_probe"}
    assert allowed("crm_investigator__svc_demo") == {
        "agent_tools_common_probe", "agent_tools_demo_probe"}
    assert allowed("crm_investigator__default") == {"agent_tools_common_probe"}
    assert allowed("crm_reviewer__svc_demo") == {"agent_tools_common_probe"}
    assert allowed("crm_dlt_investigator") == set()
    assert allowed("crm_dlt_investigator__enu_biometric") == set()
    for agent in config["agent"].values():
        assert agent["tools"]["agent_tools_*"] is False


class ScriptedModel(BaseChatModel):
    """Replies from a list, and remembers the tool names each call bound."""

    replies: list
    bound: list = []

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
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


def _through_the_subagent(pack):
    """Build `pack`'s Investigator, run it through one `task` call, and
    return (the parent's tools, the subagent's tools)."""
    model = ScriptedModel(replies=[
        AIMessage(content="", tool_calls=[{
            "name": "task", "id": "t1",
            "args": {"description": "look it up", "subagent_type": "general-purpose"}}]),
        AIMessage(content="subagent report"),
        AIMessage(content="MAIN"),
    ], bound=[])
    agent = build_agent("investigator", model, "ROLE", pack=pack)
    assert agent.invoke({"messages": [HumanMessage(content="go")]})["messages"][-1].content \
        == "MAIN"
    return model.bound[0], model.bound[1]


def test_a_fixture_service_investigator_and_its_subagent_get_no_bio_tool(served):
    parent, subagent = _through_the_subagent(DEMO)

    assert "task" in parent
    for tools in (parent, subagent):
        assert "common_probe" in tools and "demo_probe" in tools
        assert not [name for name in tools if name.startswith("bio_")], tools


def test_the_biometric_investigator_and_its_subagent_get_the_bio_tools(served):
    parent, subagent = _through_the_subagent(BIO)

    for tools in (parent, subagent):
        assert "bio_probe" in tools and "common_probe" in tools
        assert "demo_probe" not in tools


def test_an_unresolved_packets_investigator_gets_the_common_tools_alone(served):
    parent, subagent = _through_the_subagent("_default")

    for tools in (parent, subagent):
        assert "common_probe" in tools
        assert "bio_probe" not in tools and "demo_probe" not in tools


def test_the_system_prompt_describes_the_scoped_tools_only(served):
    section = mcp_client.prompt_section("investigator", DEMO)
    assert "Common guidance." in section and "demo_probe" in section
    assert "Biometric guidance." not in section and "bio_probe" not in section


def test_the_orchestrator_scopes_a_packets_agents_to_its_pack(served, monkeypatch):
    """The pooled agents are built for their pack, and the harness tools
    section names the same pack's tools."""
    from unittest.mock import MagicMock

    import src.core.agent_orchestrator as orch

    built = []

    def fake_build_agent(role, model, system_prompt, tools=(), pack=None):
        built.append((role, pack))
        agent = MagicMock()
        agent.invoke.return_value = {"messages": [AIMessage(content="findings")]}
        return agent

    monkeypatch.setattr(orch, "_agent", None)
    monkeypatch.setattr(orch, "_prompt_fingerprints", {})
    monkeypatch.setattr(orch, "get_llm", lambda _tier: MagicMock())
    monkeypatch.setattr(orch, "build_agent", fake_build_agent)
    monkeypatch.setattr(orch, "get_checkpointer", lambda: None)
    monkeypatch.setattr(orch, "get_casebook_storage", lambda: MagicMock())
    investigate = orch._build_agent().builder.nodes["investigate"].runnable.func

    investigate({"payload": {"eventId": "e-demo"}, "logs": "", "db_rule": "",
                 "retry_count": 0, "service": DEMO,
                 "service_resolution": {"service": DEMO}, "service_pack": DEMO})

    assert ("investigator", DEMO) in built
    assert ("log_filter", "_default") in built
    section = orch._with_tools_section("TASK", "investigator", DEMO)
    assert "agent_tools_demo_probe" in section and "agent_tools_bio_probe" not in section


def test_the_fingerprint_of_one_service_moves_only_with_its_tools(estate, registry_snapshot,
                                                                   tool_server, monkeypatch):
    common = Toolset(name="fp_common", agents=("investigator",), services=("*",))

    @agent_tool(common)
    def fp_common(refid: str) -> str:
        """Common."""
        return refid

    tool_server()
    before = {pack: mcp_client.fingerprint_material(pack) for pack in (BIO, DEMO)}

    demo = Toolset(name="fp_demo", agents=("investigator",), services=(DEMO,))

    @agent_tool(demo)
    def demo_fp_lookup(refid: str) -> str:
        """Demographic only."""
        return refid

    tool_server()
    after = {pack: mcp_client.fingerprint_material(pack) for pack in (BIO, DEMO)}

    assert after[BIO] == before[BIO]
    assert after[DEMO] != before[DEMO]
