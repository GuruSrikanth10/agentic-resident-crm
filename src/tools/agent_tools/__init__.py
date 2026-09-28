"""
The tools this repository serves to agents, discovered from this package.

Every tool here is served over MCP by `src/tools/mcp_server.py`, and every
agent -- the deep agents in both lanes and the opencode harness -- reaches it
only through MCP (`src/tools/mcp_client.py`). This package is therefore the
*server side*: it declares tools. Which agent gets which tool, how the tools
are described in a prompt, and how their results are kept as evidence are
decided by the client, from what the server lists.

Adding a tool
-------------
Create a module -- any name that does not start with "_" -- in the
subpackage of the service it is for, `agent_tools/<service_slug>/` (the
service name with every character outside [a-z0-9] turned into "_", since a
Python package name cannot hold a hyphen: enu-biometric's tools live in
`agent_tools/enu_biometric/`), or in `agent_tools/common/` for a tool whose
meaning holds for every service. Then decorate plain functions:

    import json

    from src.tools.agent_tools import Toolset, agent_tool

    MY_TOOLS = Toolset(
        name="my_tools",
        agents=("investigator",),
        services=("enu-biometric",),
        guidance="When to use these tools and how to read their results. "
                 "Added to the system prompt of every agent that gets one.",
        read_only=True,
    )

    @agent_tool(MY_TOOLS)
    def bio_lookup_something(refid: str) -> str:
        '''What this returns and when to use it. The model reads this.'''
        return json.dumps({"refid": refid})

That is the whole change. The next time the tool server starts it serves the
new tool, and every role in `agents` gets it for a packet of a service in
`services` -- the deep agents and the opencode harness alike. A toolset's
`enabled` callable (default: always on) is checked when the server starts; a
disabled toolset is not served at all. Several modules can share one toolset
by importing it; a module whose name starts with "_" is a helper, never
scanned, and so is a subpackage whose name does.

Which services a tool is for (MULTI_SERVICE_PLAN.md D7, D8)
------------------------------------------------------------
`services` names the services whose packets may be investigated with the
toolset's tools, or is `("*",)` for a tool whose meaning holds for every
service. The scope is enforced when an agent is built, never left to a tool's
description. A toolset that names no services reaches no service's agents
unless AGENT_TOOLS_COMMON or a pack's `tools.include` names its tools; it
still reaches the DLT roles, which are not scoped by service.

A toolset scoped to exactly one service names every tool with that service's
`tool_prefix` and an underscore (`bio_...` for enu-biometric); any other
toolset's tool names carry no registered service's prefix. Tool names are
global across servers, so the prefix is what keeps one service's tool from
silently replacing another's. Boot validation (`scope_problems`, run by
main_api) refuses a toolset naming an unregistered service and a tool
breaking the prefix rule; two modules registering one tool name fail
discovery itself.

`python3 -m src.tools.agent_tools list` shows what is registered here, and
`... call <tool> '<json>'` runs one tool in-process, without a server, which
is the quickest way to develop one. `python3 -m src.tools.mcp_client` shows
what the agents actually see through MCP.

What the decorator gives every tool
-----------------------------------
- The docstring is the description the model sees and the signature is its
  argument schema, so both are written for the model.
- An exception inside the tool becomes a result saying the tool failed and
  read nothing, rather than an error that could fail a whole agent run.
- The toolset's roles, services, guidance and read-only flag travel with the
  tool in its MCP listing (`_meta` and annotations), which is how a client
  that has none of this code still knows who the tool is for.
- The decorated function is returned unchanged, for Python callers and tests.

The DLT roles' prompts forbid per-packet evidence (their narrative is served
to every record with the same failure signature), so a tool that reads one
packet's data does not belong on a dlt_* role without a prompt change.
"""
import functools
import importlib
import inspect
import json
import pkgutil
import re
import threading
from dataclasses import dataclass
from typing import Callable, Optional

from langchain_core.tools import BaseTool, StructuredTool

from src.utils.logging_config import get_logger
from src.utils.service_registry import ALL_SERVICES, is_service_name

logger = get_logger(__name__)

#: The roles that investigate one rejection packet. Their tools are scoped by
#: the packet's service (MULTI_SERVICE_PLAN.md D7).
SERVICE_ROLES = ("investigator", "reviewer", "synthesis", "log_filter")

#: The DLT lane's roles. Scoped by service when a record is analysed with its
#: service's pack, and by role alone when it is analysed with none
#: (MULTI_SERVICE_PLAN.md Phase 8).
DLT_ROLES = ("dlt_investigator", "dlt_reviewer", "dlt_synthesis")

#: Every agent the pipeline builds: the rejection lane's four, then the DLT
#: lane's three. These are also the `node` labels on the LLM metrics, and the
#: roles a tool's listing names in its `_meta`.
AGENT_ROLES = SERVICE_ROLES + DLT_ROLES

#: Names a tool may not take: the deep-agent built-ins every agent already
#: has, and the two tools the orchestrator passes explicitly. A clash would
#: silently shadow one of them.
RESERVED_TOOL_NAMES = frozenset({
    "write_todos", "ls", "read_file", "write_file", "edit_file", "glob",
    "grep", "execute", "task",
    "queue_for_replay", "add_learning_rule",
})

#: Provider-safe function names (OpenAI allows [a-zA-Z0-9_-]{1,64}).
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


def _always() -> bool:
    return True


@dataclass(frozen=True)
class Toolset:
    """A group of tools that share a default audience, a switch and guidance.

    `agents` are the roles that get the tools by default. `services` are the
    services whose packets they are for, or `("*",)` for every service; empty
    means undeclared (see the module docstring). `guidance` is added to the
    system prompt of every agent that gets at least one of them. `enabled` is
    checked when the server starts. `read_only` is published to clients as
    the MCP read-only hint; it defaults to False, because claiming a tool
    cannot change anything is the one mistake a client may act on.
    """

    name: str
    agents: tuple
    guidance: str = ""
    enabled: Callable[[], bool] = _always
    read_only: bool = False
    services: tuple = ()

    def __post_init__(self):
        if not isinstance(self.name, str) or not _NAME.match(self.name):
            raise ValueError(f"Toolset name {self.name!r} must match {_NAME.pattern}.")
        agents = (self.agents,) if isinstance(self.agents, str) else tuple(self.agents)
        unknown = [role for role in agents if role not in AGENT_ROLES]
        if unknown:
            raise ValueError(f"Toolset {self.name!r} names unknown agent role(s) "
                             f"{unknown}; the roles are {list(AGENT_ROLES)}.")
        object.__setattr__(self, "agents", agents)

        services = (self.services,) if isinstance(self.services, str) \
            else tuple(self.services)
        malformed = [service for service in services
                     if service != ALL_SERVICES and not is_service_name(service)]
        if malformed:
            raise ValueError(f"Toolset {self.name!r} names malformed service(s) "
                             f"{malformed}; name services as their packs do, or "
                             f"{ALL_SERVICES!r} for every service.")
        if ALL_SERVICES in services and len(services) > 1:
            raise ValueError(f"Toolset {self.name!r}: {ALL_SERVICES!r} already means "
                             f"every service, so it stands alone.")
        object.__setattr__(self, "services", tuple(dict.fromkeys(services)))


@dataclass(frozen=True)
class Entry:
    """One registered tool: the callable the server runs, and its metadata."""

    tool: BaseTool
    toolset: Toolset
    module: str
    function: Callable


_registry: dict = {}
_lock = threading.RLock()
_discovered = False


def agent_tool(toolset: Toolset, *, name: Optional[str] = None):
    """Register the decorated function as a tool of `toolset`.

    Returns the function unchanged. What is served wraps it: the result is
    returned as text, and an exception becomes a failure message.
    """
    if not isinstance(toolset, Toolset):
        raise TypeError("agent_tool() takes the Toolset the tool belongs to, "
                        "e.g. @agent_tool(MY_TOOLS).")

    def decorate(func):
        tool_name = name or func.__name__
        if not _NAME.match(tool_name):
            raise ValueError(f"Tool name {tool_name!r} must match {_NAME.pattern}.")
        if tool_name in RESERVED_TOOL_NAMES:
            raise ValueError(f"Tool name {tool_name!r} is reserved: every agent "
                             f"already has a tool by that name.")
        description = inspect.getdoc(func)
        if not description:
            raise ValueError(f"Tool {tool_name!r} needs a docstring: it is the "
                             f"description the model reads.")

        @functools.wraps(func)
        def call(*args, **kwargs) -> str:
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error("Agent tool failed", tool=tool_name,
                             error=f"{type(e).__name__}: {e}")
                result = (f"The tool {tool_name} failed ({type(e).__name__}) and "
                          f"returned no data. Treat this as an evidence gap, "
                          f"not as a result.")
            return result if isinstance(result, str) \
                else json.dumps(result, default=str, ensure_ascii=False)

        tool = StructuredTool.from_function(func=call, name=tool_name,
                                            description=description)
        _register(Entry(tool=tool, toolset=toolset, module=func.__module__,
                        function=call))
        return func

    return decorate


def _register(entry: Entry) -> None:
    with _lock:
        existing = _registry.get(entry.tool.name)
        # The same module registering again is a reload; anything else is two
        # tools claiming one name.
        if existing is not None and existing.module != entry.module:
            raise ValueError(f"Tool {entry.tool.name!r} is registered twice: by "
                             f"{existing.module} and by {entry.module}.")
        _registry[entry.tool.name] = entry


def _tool_modules(search_path, package: str) -> list:
    """The qualified names of the tool modules under `package`, in a stable
    order: every module and subpackage whose name does not start with "_",
    and, for a subpackage, the modules inside it. A subpackage is imported to
    be searched, so its own `__init__` counts as one of its modules."""
    found = []
    for module in sorted(pkgutil.iter_modules(search_path), key=lambda m: m.name):
        if module.name.startswith("_"):
            continue
        qualified = f"{package}.{module.name}"
        found.append(qualified)
        if module.ispkg:
            subpackage = _import(qualified)
            found.extend(_tool_modules(subpackage.__path__, qualified))
    return found


def _import(qualified: str):
    try:
        return importlib.import_module(qualified)
    except Exception as e:
        raise RuntimeError(f"Could not import agent tool module "
                           f"{qualified}: {type(e).__name__}: {e}") from e


def discover() -> None:
    """Import every tool module in this package and its subpackages, once.

    A module that fails to import raises: a tool silently missing from the
    server is worse than a boot that fails with the module's name.
    `validate()` runs this at boot for exactly that reason.
    """
    global _discovered
    if _discovered:
        return
    with _lock:
        if _discovered:
            return
        for qualified in _tool_modules(__path__, __name__):
            _import(qualified)
        _discovered = True


def is_enabled(toolset: Toolset) -> bool:
    """Whether `toolset` is switched on. A switch that raises is off: serving
    a tool whose backing system may be misconfigured is worse than not."""
    try:
        return bool(toolset.enabled())
    except Exception as e:
        logger.error("Toolset switch raised; treating the toolset as disabled",
                     toolset=toolset.name, error=f"{type(e).__name__}: {e}")
        return False


def entries() -> list:
    """Every registered tool, enabled or not, in a stable order."""
    discover()
    return sorted(_registry.values(),
                  key=lambda entry: (entry.toolset.name, entry.tool.name))


def enabled_entries() -> list:
    """The registered tools whose toolset is switched on: what gets served."""
    switches: dict = {}
    active = []
    for entry in entries():
        if entry.toolset not in switches:
            switches[entry.toolset] = is_enabled(entry.toolset)
        if switches[entry.toolset]:
            active.append(entry)
    return active


def any_enabled() -> bool:
    """Whether there is anything to serve at all."""
    return bool(enabled_entries())


def get_tool(name: str) -> BaseTool:
    """One registered tool by name, as an in-process LangChain tool.

    For the CLI and tests. Agents never call this: they reach every tool
    through the MCP server.
    """
    discover()
    try:
        return _registry[name].tool
    except KeyError:
        raise ValueError(f"No agent tool named {name!r}; the registered tools "
                         f"are {sorted(_registry)}.") from None


def describe() -> list:
    """Every registered tool, for the CLI."""
    return [{
        "name": entry.tool.name,
        "toolset": entry.toolset.name,
        "enabled": is_enabled(entry.toolset),
        "default_agents": list(entry.toolset.agents),
        "services": list(entry.toolset.services),
        "read_only": entry.toolset.read_only,
        "module": entry.module,
        "summary": entry.tool.description.splitlines()[0],
    } for entry in entries()]


def validate() -> list:
    """Errors in the tool modules, for `validate_config()` at boot."""
    try:
        discover()
    except Exception as e:
        return [str(e)]
    return []


def scope_problems(registry=None) -> tuple:
    """(errors, warnings) in the local tools' service scopes, for
    `service_registry.validate()` -- which main_api runs at boot -- since they
    can only be judged against the registry (MULTI_SERVICE_PLAN.md 5.7).

    Every registered tool is checked, enabled or not: a scope that is wrong
    must fail the deploy that ships it, not the later one that switches the
    toolset on. Errors: a toolset names a service with no valid pack; a tool
    breaks the prefix rule (D8); two modules register one tool name (which
    fails discovery). Warning: a toolset for a service-scoped role declares no
    services, so it reaches no service unless configured to.
    """
    from src.utils import service_registry

    registry = registry or service_registry.load()
    try:
        discover()
    except Exception as e:
        return [str(e)], []

    errors, warnings, checked = [], [], set()
    for entry in entries():
        toolset = entry.toolset
        if toolset not in checked:
            checked.add(toolset)
            for service in toolset.services:
                if service != ALL_SERVICES and not registry.is_registered(service):
                    errors.append(f"Toolset {toolset.name!r} ({entry.module}) names "
                                  f"service {service!r}, which has no valid pack "
                                  f"in {registry.root}.")
            scoped_roles = [role for role in toolset.agents if role in SERVICE_ROLES]
            if not toolset.services and scoped_roles:
                warnings.append(f"Toolset {toolset.name!r} ({entry.module}) is for "
                                f"{scoped_roles} but names no services, so it "
                                f"reaches no service's packets unless "
                                f"AGENT_TOOLS_COMMON or a pack's tools.include "
                                f"names its tools.")
        problem = service_registry.tool_prefix_error(entry.tool.name,
                                                     toolset.services, registry)
        if problem:
            errors.append(f"{problem} ({entry.module})")
    return errors, warnings
