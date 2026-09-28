"""
How every agent reaches its tools: over MCP, from the servers in mcp_config.

The deep agents get LangChain tools built here from the servers' listings;
the opencode harness gets the same servers, and the same per-role selection,
as an `mcp` and `agent` block in its config (`opencode_config`). Nothing here
imports a tool's code: a tool served by this repository and one served by
the organisation's own MCP server are handled identically.

The catalog
-----------
One listing of every configured server, shared by every agent built from it.
A server that cannot be listed is left out and the catalog marked
incomplete; an incomplete catalog is fetched again after
AGENT_MCP_RETRY_SECONDS, and the orchestrators rebuild their graphs from the
new one (`is_stale`). So a tool server that is down when the first packet
arrives costs that packet its tools, not every packet until a restart.

Which role gets which tool, for which service
---------------------------------------------
By default, the roles a tool's listing names under `_meta` (mcp_config.
META_AGENTS) -- what the toolset in src/tools/agent_tools declared. A tool
whose listing names no roles, which is any tool from a server that does not
know this repository, goes to no role until AGENT_TOOLS_<ROLE> names it.
AGENT_TOOLS_<ROLE> replaces a role's selection outright: a comma-separated
list of tool names, or `none`.

The rejection lane's roles are then scoped by the packet's service
(MULTI_SERVICE_PLAN.md D7, 5.6): an agent for service S gets only the tools
whose listing names S or "*" (mcp_config.META_SERVICES), the undeclared tools
AGENT_TOOLS_COMMON names, and the tools S's pack includes -- less those it
excludes. Nothing widens that scope: not AGENT_TOOLS_<ROLE>, and not a tool's
description. The `_default` pack, for unresolved packets, gets the "*" tools
alone. The DLT roles are not scoped by service yet (Phase 8), so they take
none. Tool names are global across servers, and a tool scoped to one service
carries that service's prefix (D8): a served tool that breaks the prefix rule
is left out of the catalog like a reserved name.

Calling a tool
--------------
Each call opens its own session: the servers are stateless, a call is a
couple of requests, and a server restarted between two calls is then no
concern of the agent's. Tools run synchronously for the pipeline's
synchronous graphs and asynchronously for anything else. A call that cannot
complete -- server down, timeout, a tool error -- returns a message saying
nothing was read instead of raising, because an exception would end the
whole agent run; and every call is recorded while a node holds `recording()`
open, which is how the Investigator's tool results reach the Reviewer.
"""
import asyncio
import contextlib
import contextvars
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

from langchain_core.tools import BaseTool, StructuredTool

from src.tools import mcp_config
from src.tools.agent_tools import AGENT_ROLES, DLT_ROLES, RESERVED_TOOL_NAMES, SERVICE_ROLES
from src.utils import service_registry
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

ENV_ROLE_PREFIX = "AGENT_TOOLS_"
_NONE = "none"

#: Undeclared tools -- a listing that names no services, as any tool from a
#: server that does not know this repository's metadata -- to treat as tools
#: for every service. A comma-separated list of tool names.
ENV_COMMON = "AGENT_TOOLS_COMMON"

#: The roles that run on the opencode harness, and the opencode agent each
#: one runs as there. A prefix keeps them clear of opencode's own agents.
HARNESS_ROLES = ("investigator", "reviewer", "dlt_investigator", "dlt_reviewer")
OPENCODE_AGENT_PREFIX = "crm_"

#: Heading of the system-prompt section listing a role's tools. The agent
#: prompts refer to it by this name.
TOOLS_HEADING = "### AVAILABLE TOOLS"

COMMON_RULES = """\
The tools below read live systems. What they return is evidence about this
packet, with the same standing as the logs. For every one of them:

1. Cite what you rely on: name the tool and the fields, for example
   "<tool>: <field> is <value>".
2. A result saying a lookup was switched off, refused an argument, or failed
   means nothing was read. That is an evidence gap: say so, and never treat
   it as "no rows".
3. A lookup that ran and found nothing is a finding in its own right.
4. Never state a value that no tool returned and no other evidence shows."""

#: Evidence bounds. A tool bounds its own output; these bound what is kept
#: in graph state (and so in every checkpoint) and what a prompt carries.
MAX_RECORDED_RESULT_CHARS = 16000
MAX_EVIDENCE_RECORDS = 30
DEFAULT_EVIDENCE_MAX_CHARS = 40000
ENV_EVIDENCE_MAX_CHARS = "AGENT_TOOL_EVIDENCE_MAX_CHARS"

#: How many listing pages one server may return before it is treated as
#: broken rather than followed forever.
MAX_LISTING_PAGES = 50


# ---------------------------------------------------------------------------
# The catalog
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RemoteTool:
    """One tool as a server listed it."""

    server: mcp_config.ServerConfig
    name: str
    description: str
    input_schema: dict
    toolset: Optional[str] = None
    agents: tuple = ()
    guidance: str = ""
    read_only: Optional[bool] = None
    #: The services whose packets the tool is for; ("*",) for every service,
    #: () when the listing names none.
    services: tuple = ()

    @property
    def opencode_name(self) -> str:
        """What opencode calls this tool: the server's name, then the tool's."""
        return f"{self.server.name}_{self.name}"


@dataclass(frozen=True)
class Catalog:
    servers: tuple
    tools: tuple
    #: Server name -> why it could not be listed.
    failures: dict = field(default_factory=dict)
    loaded_at: float = 0.0

    @property
    def complete(self) -> bool:
        return not self.failures


_catalog: Optional[Catalog] = None
_catalog_lock = threading.Lock()


def is_stale(catalog: Optional[Catalog]) -> bool:
    """Whether a graph built from `catalog` should be rebuilt now.

    True when the configured servers have changed, or when the catalog is
    incomplete and older than AGENT_MCP_RETRY_SECONDS. None -- a graph built
    without one -- is never stale.
    """
    if catalog is None:
        return False
    if tuple(mcp_config.servers()) != catalog.servers:
        return True
    return (not catalog.complete
            and time.monotonic() - catalog.loaded_at >= mcp_config.retry_seconds())


def current_catalog() -> Catalog:
    """The shared catalog, fetched on first use and again once stale."""
    global _catalog
    catalog = _catalog
    if catalog is not None and not is_stale(catalog):
        return catalog
    with _catalog_lock:
        if _catalog is None or is_stale(_catalog):
            _catalog = load_catalog()
        return _catalog


def reset() -> None:
    """Forget the cached catalog. For tests."""
    global _catalog
    with _catalog_lock:
        _catalog = None


def load_catalog() -> Catalog:
    """List every configured server now."""
    configured = tuple(mcp_config.servers())
    tools, failures, seen = [], {}, set()
    for server in configured:
        try:
            listed = _run_sync(lambda server=server: _list_tools(server))
        except Exception as e:
            failures[server.name] = describe_error(e)
            logger.error("Could not list an agent tool server; its tools are left "
                         "out until the next attempt", server=server.name,
                         url=server.url, error=failures[server.name],
                         retry_seconds=mcp_config.retry_seconds())
            continue
        for tool in listed:
            remote = _remote_tool(server, tool)
            if remote.name in RESERVED_TOOL_NAMES:
                logger.error("Ignoring a served tool whose name is reserved",
                             server=server.name, tool=remote.name)
                continue
            if remote.name in seen:
                logger.error("Ignoring a served tool whose name another server "
                             "already uses", server=server.name, tool=remote.name)
                continue
            # The prefix rule holds for every server's tools (D8). This
            # repository's own are checked at boot; another team's server can
            # only be checked once it lists them.
            problem = service_registry.tool_prefix_error(remote.name, remote.services)
            if problem:
                logger.error("Ignoring a served tool that breaks the tool prefix "
                             "rule", server=server.name, tool=remote.name,
                             detail=problem)
                continue
            seen.add(remote.name)
            tools.append(remote)
    logger.info("Agent tool catalog loaded", servers=[s.name for s in configured],
                tools=[t.name for t in tools], failed=sorted(failures))
    return Catalog(servers=configured, tools=tuple(tools), failures=failures,
                   loaded_at=time.monotonic())


def _remote_tool(server: mcp_config.ServerConfig, tool) -> RemoteTool:
    meta = tool.meta or {}
    agents = meta.get(mcp_config.META_AGENTS)
    agents = tuple(role for role in agents if role in AGENT_ROLES) \
        if isinstance(agents, list) else ()
    services = meta.get(mcp_config.META_SERVICES)
    services = tuple(dict.fromkeys(service.strip() for service in services
                                   if isinstance(service, str) and service.strip())) \
        if isinstance(services, list) else ()
    toolset = meta.get(mcp_config.META_TOOLSET)
    guidance = meta.get(mcp_config.META_GUIDANCE)
    annotations = tool.annotations
    return RemoteTool(
        server=server,
        name=tool.name,
        description=tool.description or "",
        input_schema=dict(tool.input_schema or {"type": "object", "properties": {}}),
        toolset=toolset if isinstance(toolset, str) else None,
        agents=agents,
        guidance=guidance if isinstance(guidance, str) else "",
        read_only=annotations.read_only_hint if annotations is not None else None,
        services=services,
    )


# ---------------------------------------------------------------------------
# Talking to a server
# ---------------------------------------------------------------------------

@contextlib.asynccontextmanager
async def _session(server: mcp_config.ServerConfig):
    """One MCP session with `server`, closed on exit."""
    import httpx2
    from mcp.client.client import Client
    from mcp.client.streamable_http import streamable_http_client
    from mcp_types import Implementation

    timeout = mcp_config.timeout_seconds()
    # Loopback traffic never goes through a proxy: a corporate proxy answering
    # for 127.0.0.1 is a failure this codebase has met twice already.
    async with httpx2.AsyncClient(headers=server.header_dict(),
                                  timeout=httpx2.Timeout(timeout),
                                  trust_env=not mcp_config.is_loopback(server.url)) as http:
        client = Client(streamable_http_client(server.url, http_client=http),
                        cache=None, read_timeout_seconds=timeout,
                        client_info=Implementation(name="agentic-resident-crm",
                                                   version="1"))
        async with client:
            yield client


async def _list_tools(server: mcp_config.ServerConfig) -> list:
    import anyio

    with anyio.fail_after(mcp_config.timeout_seconds()):
        async with _session(server) as client:
            tools, cursor = [], None
            for _ in range(MAX_LISTING_PAGES):
                page = await client.list_tools(cursor=cursor)
                tools.extend(page.tools)
                cursor = page.next_cursor
                if not cursor:
                    return tools
    raise RuntimeError(f"the server returned more than {MAX_LISTING_PAGES} listing pages")


async def _call_text(server: mcp_config.ServerConfig, name: str, arguments: dict) -> str:
    """Call one tool and return its result as text; never raises."""
    import anyio

    try:
        with anyio.fail_after(mcp_config.timeout_seconds()):
            async with _session(server) as client:
                result = await client.call_tool(name, arguments)
    except Exception as e:
        cause = root_cause(e)
        logger.error("Agent tool call failed", server=server.name, tool=name,
                     error=describe_error(e))
        return (f"The tool {name} could not be called on the {server.name} server "
                f"({type(cause).__name__}), so nothing was read. Treat this as an "
                f"evidence gap, not a finding.")
    return _result_text(result)


def root_cause(error: BaseException) -> BaseException:
    """The first leaf of an exception group: the SDK runs its transport in a
    task group, so a refused connection arrives wrapped in one or two."""
    while isinstance(error, BaseExceptionGroup) and error.exceptions:
        error = error.exceptions[0]
    return error


def describe_error(error: BaseException) -> str:
    cause = root_cause(error)
    return f"{type(cause).__name__}: {cause}"


def _result_text(result) -> str:
    from mcp_types import TextContent

    parts = []
    for item in result.content or []:
        if isinstance(item, TextContent):
            parts.append(item.text)
        else:
            parts.append(f"[{getattr(item, 'type', 'non-text')} content omitted]")
    text = "\n".join(parts)
    if not text and result.structured_content is not None:
        text = json.dumps(result.structured_content, default=str, ensure_ascii=False)
    if result.is_error:
        return ("The tool reported an error, so nothing was read. This is an "
                f"evidence gap, not a finding. {text}".rstrip())
    return text


def _run_sync(factory):
    """Run the coroutine `factory()` makes, from synchronous code.

    The graphs are synchronous and run tools on worker threads, where no
    event loop is running; should one be running on this thread, the
    coroutine runs on a helper thread instead of deadlocking it.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(factory())).result()


# ---------------------------------------------------------------------------
# Tools for a role
# ---------------------------------------------------------------------------

def _override(role: str) -> Optional[list]:
    """The AGENT_TOOLS_<ROLE> selection, or None when unset or blank."""
    raw = os.environ.get(ENV_ROLE_PREFIX + role.upper(), "").strip()
    if not raw:
        return None
    if raw.lower() == _NONE:
        return []
    return list(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))


def _common_names() -> frozenset:
    """The AGENT_TOOLS_COMMON tool names."""
    raw = os.environ.get(ENV_COMMON, "")
    return frozenset(part.strip() for part in raw.split(",") if part.strip())


def _check_scope(role: str, service: Optional[str]) -> None:
    """A rejection role is always scoped by a service. A DLT role is scoped by
    one when its record was analysed with its service's pack, and keeps the
    role-only selection otherwise (MULTI_SERVICE_PLAN.md Phase 8); it has no
    `_default` pack."""
    if role not in AGENT_ROLES:
        raise ValueError(f"Unknown agent role {role!r}; the roles are {list(AGENT_ROLES)}.")
    if role in SERVICE_ROLES and not service:
        raise ValueError(f"The {role} role's tools are scoped by service; name the "
                         f"packet's pack ({service_registry.DEFAULT_PACK} for an "
                         f"unresolved packet).")
    if role in DLT_ROLES and service == service_registry.DEFAULT_PACK:
        raise ValueError(f"The {role} role has no {service_registry.DEFAULT_PACK} "
                         f"pack; pass no service for an unplaced record.")


def in_scope(tool: RemoteTool, service: str) -> bool:
    """Whether `tool` may be offered for a packet analysed with the pack
    `service` (MULTI_SERVICE_PLAN.md 5.6).

    Yes when its listing names every service ("*") or this one, or it names
    none and AGENT_TOOLS_COMMON lists it, or the pack's `tools.include` lists
    it; never when the pack's `tools.exclude` does. The `_default` pack is no
    service a tool can name, so an unresolved packet gets the tools for every
    service alone.
    """
    include, exclude = service_registry.tool_scope(service)
    if tool.name in exclude:
        return False
    if tool.name in include or service_registry.ALL_SERVICES in tool.services:
        return True
    if service != service_registry.DEFAULT_PACK and service in tool.services:
        return True
    return not tool.services and tool.name in _common_names()


def selection(role: str, service: Optional[str] = None, *,
              catalog: Optional[Catalog] = None) -> list:
    """The catalog's tools that `role` gets for a packet analysed with the pack
    `service`, in a stable order.

    `service` is required for the rejection roles (MULTI_SERVICE_PLAN.md
    5.6). For the DLT roles it is optional: with one they are scoped the same
    way; with none they keep the role-only selection (Phase 8).
    """
    _check_scope(role, service)
    catalog = catalog or current_catalog()
    chosen_names = _override(role)
    if chosen_names is None:
        chosen = [tool for tool in catalog.tools if role in tool.agents]
    else:
        known = {tool.name: tool for tool in catalog.tools}
        unknown = [name for name in chosen_names if name not in known]
        if unknown and catalog.complete:
            # Every server answered and none serves these: a typo, not an outage.
            raise ValueError(f"{ENV_ROLE_PREFIX}{role.upper()} names tool(s) {unknown} "
                             f"that no server serves; served: {sorted(known)}.")
        if unknown:
            logger.warning("Configured tools are unavailable while a server is "
                           "unreachable", role=role, tools=unknown,
                           failed=sorted(catalog.failures))
        chosen = [known[name] for name in chosen_names if name in known]
    if service is not None:
        # After the override, never before it: AGENT_TOOLS_<ROLE> can narrow
        # or replace a role's list, but not add a tool outside the scope.
        chosen = [tool for tool in chosen if in_scope(tool, service)]
    return sorted(chosen, key=lambda tool: (tool.toolset or "", tool.name))


def tools_for(role: str, service: Optional[str] = None) -> list:
    """LangChain tools for `role` and the pack `service`, each calling its
    server over MCP."""
    return [_langchain_tool(tool) for tool in selection(role, service)]


def _langchain_tool(remote: RemoteTool) -> BaseTool:
    def run(**arguments) -> str:
        text = _run_sync(lambda: _call_text(remote.server, remote.name, arguments))
        _record(remote.name, arguments, text)
        return text

    async def arun(**arguments) -> str:
        text = await _call_text(remote.server, remote.name, arguments)
        _record(remote.name, arguments, text)
        return text

    # The listing's JSON schema is used as it is: the server validates the
    # arguments, and its refusal comes back as a result the model can read.
    return StructuredTool(name=remote.name, description=remote.description,
                          args_schema=remote.input_schema, func=run, coroutine=arun)


def prompt_section(role: str, service: Optional[str] = None, *,
                   opencode: bool = False) -> str:
    """The system-prompt section describing the tools `role` gets for the
    pack `service`, or "" for none.

    `opencode` names the tools the way opencode exposes them
    (`<server>_<tool>`), for the harness prompts.
    """
    tools = selection(role, service)
    if not tools:
        return ""
    groups: dict = {}
    for tool in tools:
        key = tool.toolset or f"tools from the {tool.server.name} server"
        groups.setdefault(key, []).append(tool)

    parts = [TOOLS_HEADING, COMMON_RULES]
    if opencode:
        parts.append("Here each tool's name carries its server's prefix: "
                     f"{tools[0].name} is {tools[0].opencode_name}.")
    for heading, members in groups.items():
        names = [tool.opencode_name if opencode else tool.name for tool in members]
        body = f"Tools: {', '.join(names)}."
        guidance = next((tool.guidance for tool in members if tool.guidance.strip()), "")
        if guidance:
            body += "\n\n" + guidance.strip()
        parts.append(f"#### {heading}\n{body}")
    return "\n\n".join(parts)


def _fingerprint_scope(role: str, service: Optional[str]):
    """(included, scope) for `role` in the fingerprint of the pack `service`.

    The LogFilter is one agent for every packet, built with the `_default`
    scope, so it is hashed with that scope whatever the pack. With no pack,
    the pooled roles are left out: there is no service to scope them by.
    """
    if role in DLT_ROLES:
        return True, None
    if role == "log_filter":
        return True, service_registry.DEFAULT_PACK
    return service is not None, service


def fingerprint_material(service: Optional[str] = None) -> str:
    """Everything tool-related the agents of the pack `service` see, for its
    prompt fingerprint (MULTI_SERVICE_PLAN.md D13).

    The tool names, descriptions, argument schemas and prompt sections decide
    what the agents can look up and how they are told to read it -- a change
    to any of them is a prompt change and must move the fingerprint. Scoped
    per service, so a tool added for one service moves only that service's
    fingerprint.
    """
    lines = []
    for role in AGENT_ROLES:
        included, scope = _fingerprint_scope(role, service)
        if not included:
            continue
        for tool in selection(role, scope):
            schema = json.dumps(tool.input_schema, sort_keys=True, default=str)
            lines.append(f"{role}\ttool\t{tool.server.name}\t{tool.name}\t"
                         f"{tool.description}\t{schema}")
        lines.append(f"{role}\tsection\t{prompt_section(role, scope)}")
    return "\n".join(lines)


def scopes() -> tuple:
    """Every pack a rejection agent can be built for: each registered
    service, then `_default`."""
    return (*service_registry.load().services(), service_registry.DEFAULT_PACK)


def describe() -> dict:
    """The catalog and the selections, for the CLI and diagnostics: per pack
    for the rejection roles, per role for the DLT roles."""
    catalog = current_catalog()
    return {
        "servers": [{"name": server.name, "url": server.url} for server in catalog.servers],
        "failed": dict(catalog.failures),
        "tools": [{"name": tool.name, "server": tool.server.name, "toolset": tool.toolset,
                   "default_agents": list(tool.agents), "services": list(tool.services),
                   "read_only": tool.read_only,
                   "summary": tool.description.splitlines()[0] if tool.description else ""}
                  for tool in catalog.tools],
        "services": {service: {role: [tool.name
                                      for tool in selection(role, service, catalog=catalog)]
                               for role in SERVICE_ROLES}
                     for service in scopes()},
        "roles": {role: [tool.name for tool in selection(role, catalog=catalog)]
                  for role in DLT_ROLES},
    }


def validate() -> list:
    """Configuration errors, for `validate_config()` at boot.

    The tool names in AGENT_TOOLS_<ROLE> cannot be checked here -- the
    servers are not up yet at boot -- and are checked when the first agent is
    built instead. The variable names can be.
    """
    errors = list(mcp_config.validate())
    known = {ENV_ROLE_PREFIX + role.upper() for role in AGENT_ROLES}
    for variable in sorted(os.environ):
        if variable.startswith(ENV_ROLE_PREFIX) and variable not in known \
                and variable != ENV_COMMON:
            errors.append(f"{variable} is not a known setting; per-role tool "
                          f"selections are {sorted(known)}, and {ENV_COMMON} "
                          f"lists undeclared tools for every service.")
    return errors


# ---------------------------------------------------------------------------
# opencode
# ---------------------------------------------------------------------------

def opencode_agent(role: str, service: Optional[str] = None) -> str:
    """The opencode agent a harness task for `role` runs as.

    `crm_<role>__<service_slug>` for a rejection role -- `crm_<role>__default`
    for the `_default` pack, and for no pack at all. For a DLT role,
    `crm_<role>__<service_slug>` for a record analysed with its service's
    pack, and `crm_<role>` for one analysed with none.
    """
    if role in SERVICE_ROLES:
        slug = service_registry.service_slug(service or service_registry.DEFAULT_PACK)
        return f"{OPENCODE_AGENT_PREFIX}{role}__{slug}"
    if service:
        return f"{OPENCODE_AGENT_PREFIX}{role}__{service_registry.service_slug(service)}"
    return OPENCODE_AGENT_PREFIX + role


def opencode_config() -> dict:
    """The `mcp` and `agent` blocks opencode needs, or {} with no servers.

    One opencode agent per harness role and scope, each allowed exactly the
    tools that role gets here for that scope: for a rejection role, one per
    registered service and one for `_default`; for a DLT role, one per
    registered service and one with no service. So a
    harness task can no more reach a tool outside its role and service than a
    deep agent can. opencode deep-merges this with its other config, so the
    provider block it already has is left alone. Built when `opencode serve`
    starts, so a pack added later needs a restart -- it ships with a deploy
    anyway.
    """
    catalog = current_catalog()
    if not catalog.servers:
        return {}
    servers = {server.name: {"type": "remote", "url": server.url, "enabled": True,
                             **({"headers": server.header_dict()} if server.headers else {})}
               for server in catalog.servers}
    agents = {}
    for role in HARNESS_ROLES:
        for service in (scopes() if role in SERVICE_ROLES
                        else (None, *service_registry.load().services())):
            tools = {f"{server.name}_*": False for server in catalog.servers}
            for tool in selection(role, service, catalog=catalog):
                tools[tool.opencode_name] = True
            agents[opencode_agent(role, service)] = {
                "mode": "primary",
                "description": (f"Agentic Resident CRM harness task: {role}"
                                + (f", {service} packets." if service else ".")),
                "tools": tools,
            }
    return {"mcp": servers, "agent": agents}


def evidence_from_harness(calls) -> list:
    """Evidence records from the tool calls of an opencode task.

    Keeps only calls to the configured servers' tools, named as the agents
    know them (the server prefix removed): the file reads and searches a
    harness task also makes are not evidence about the packet.
    """
    prefixes = {f"{server.name}_": server.name for server in mcp_config.servers()}
    records = []
    for call in calls or []:
        name = str(call.get("tool") or "")
        prefix = next((p for p in prefixes if name.startswith(p)), None)
        if prefix is None:
            continue
        records.append({"tool": name[len(prefix):],
                        "args": call.get("input") if isinstance(call.get("input"), dict) else {},
                        "result": _bounded(str(call.get("output") or ""),
                                           MAX_RECORDED_RESULT_CHARS)})
    return records


# ---------------------------------------------------------------------------
# Evidence: the record of what the tools returned during one node's run
# ---------------------------------------------------------------------------

#: The call list of the node currently recording, if any. A ContextVar rather
#: than a global: packets run concurrently on separate threads, and LangGraph
#: and the tool node copy the context into the threads they run tools on, so
#: a tool call -- including one made inside a `task` subagent -- lands in the
#: list of the node that started the run, and only that node.
_recorder: contextvars.ContextVar = contextvars.ContextVar(
    "agent_tool_recorder", default=None)


@contextlib.contextmanager
def recording():
    """Collect every tool call made until the block exits.

    Yields the list the calls are appended to, as
    {"tool": name, "args": {...}, "result": text}, in completion order.
    """
    calls: list = []
    token = _recorder.set(calls)
    try:
        yield calls
    finally:
        _recorder.reset(token)


def _record(tool_name: str, arguments: dict, result: str) -> None:
    calls = _recorder.get()
    if calls is None:
        return
    calls.append({"tool": tool_name, "args": dict(arguments),
                  "result": _bounded(result, MAX_RECORDED_RESULT_CHARS)})


def _bounded(text: str, limit: int) -> str:
    text = str(text)
    if len(text) <= limit:
        return text
    marker = f"\n... [{len(text)} characters in total; the rest was cut]"
    return text[:max(0, limit - len(marker))] + marker


def _evidence_key(record: dict) -> tuple:
    return (record.get("tool"),
            json.dumps(record.get("args"), sort_keys=True, default=str))


def merge_evidence(existing, new) -> list:
    """`existing` plus `new`, one record per (tool, arguments), newest kept.

    Kept across Investigator retries: a retry that relies on what an earlier
    attempt looked up without repeating the call still has its evidence in
    front of the Reviewer.
    """
    merged = [dict(record) for record in (existing or []) if isinstance(record, dict)]
    for record in new or []:
        if not isinstance(record, dict):
            continue
        key = _evidence_key(record)
        merged = [kept for kept in merged if _evidence_key(kept) != key]
        merged.append(dict(record))
    return merged[-MAX_EVIDENCE_RECORDS:]


def evidence_max_chars() -> int:
    """AGENT_TOOL_EVIDENCE_MAX_CHARS, read at call time. An unusable value
    falls back rather than raising -- a typo in a tunable must not fail a
    packet."""
    raw = os.environ.get(ENV_EVIDENCE_MAX_CHARS, "")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_EVIDENCE_MAX_CHARS
    return value if value > 0 else DEFAULT_EVIDENCE_MAX_CHARS


def render_evidence(records, max_chars: Optional[int] = None) -> str:
    """The records as prompt text, in call order, or "" when there are none.

    When they do not all fit in `max_chars`, the most recent ones are kept
    and the number left out is stated, so a reader never mistakes a cut list
    for a complete one.
    """
    records = [record for record in (records or []) if isinstance(record, dict)]
    if not records:
        return ""
    budget = max_chars if max_chars and max_chars > 0 else evidence_max_chars()

    blocks = []
    for index, record in enumerate(records, start=1):
        arguments = json.dumps(record.get("args") or {}, sort_keys=True,
                               default=str, ensure_ascii=False)
        blocks.append(f"[{index}] {record.get('tool')} {arguments}\n"
                      f"{record.get('result', '')}")

    kept: list = []
    used = 0
    for block in reversed(blocks):
        cost = len(block) + 2
        if used + cost > budget:
            if not kept:
                kept.append(_bounded(block, budget))
            break
        kept.append(block)
        used += cost
    kept.reverse()

    omitted = len(blocks) - len(kept)
    note = (f"({omitted} earlier tool result(s) left out to fit "
            f"{ENV_EVIDENCE_MAX_CHARS}.)\n\n") if omitted else ""
    return note + "\n\n".join(kept)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    """What the agents see through MCP.

        python3 -m src.tools.mcp_client list
        python3 -m src.tools.mcp_client prompt investigator --service enu-biometric
        python3 -m src.tools.mcp_client call bio_get_parking_status '{"refid": "..."}'
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(prog="python3 -m src.tools.mcp_client",
                                     description="Inspect and call the agent tools over MCP.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="the servers, their tools, and the selections")
    prompt = commands.add_parser("prompt", help="the tools section of a role's prompt")
    prompt.add_argument("role", choices=AGENT_ROLES)
    prompt.add_argument("--service", default=None,
                        help="the pack whose agent it is (a rejection role needs one; "
                             f"{service_registry.DEFAULT_PACK} for unresolved packets)")
    prompt.add_argument("--opencode", action="store_true", help="name tools as opencode does")
    call = commands.add_parser("call", help="call one tool as an agent would")
    call.add_argument("tool")
    call.add_argument("arguments", nargs="?", default="{}")
    args = parser.parse_args(argv)

    try:
        if args.command == "list":
            print(json.dumps(describe(), indent=2))
            return 0
        if args.command == "prompt":
            print(prompt_section(args.role, args.service, opencode=args.opencode)
                  or f"(no tools for {args.role})")
            return 0
        arguments = json.loads(args.arguments)
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be a JSON object")
        remote = next((tool for tool in current_catalog().tools if tool.name == args.tool), None)
        if remote is None:
            raise ValueError(f"no configured server serves a tool named {args.tool!r}")
        print(_langchain_tool(remote).invoke(arguments))
        return 0
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
