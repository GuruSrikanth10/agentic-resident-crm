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
Create a module in this package -- any name that does not start with "_" --
and decorate plain functions:

    import json

    from src.tools.agent_tools import Toolset, agent_tool

    MY_TOOLS = Toolset(
        name="my_tools",
        agents=("investigator",),
        guidance="When to use these tools and how to read their results. "
                 "Added to the system prompt of every agent that gets one.",
        read_only=True,
    )

    @agent_tool(MY_TOOLS)
    def lookup_something(refid: str) -> str:
        '''What this returns and when to use it. The model reads this.'''
        return json.dumps({"refid": refid})

That is the whole change. The next time the tool server starts it serves the
new tool, and every role in `agents` gets it -- the deep agents and the
opencode harness alike. A toolset's `enabled` callable (default: always on)
is checked when the server starts; a disabled toolset is not served at all.
Several modules can share one toolset by importing it; a module whose name
starts with "_" is a helper, never scanned.

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
- The toolset's roles, guidance and read-only flag travel with the tool in
  its MCP listing (`_meta` and annotations), which is how a client that has
  none of this code still knows who the tool is for.
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

logger = get_logger(__name__)

#: Every agent the pipeline builds: the rejection lane's four, then the DLT
#: lane's three. These are also the `node` labels on the LLM metrics, and the
#: roles a tool's listing names in its `_meta`.
AGENT_ROLES = (
    "investigator",
    "reviewer",
    "synthesis",
    "log_filter",
    "dlt_investigator",
    "dlt_reviewer",
    "dlt_synthesis",
)

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

    `agents` are the roles that get the tools by default. `guidance` is added
    to the system prompt of every agent that gets at least one of them.
    `enabled` is checked when the server starts. `read_only` is published to
    clients as the MCP read-only hint; it defaults to False, because claiming
    a tool cannot change anything is the one mistake a client may act on.
    """

    name: str
    agents: tuple
    guidance: str = ""
    enabled: Callable[[], bool] = _always
    read_only: bool = False

    def __post_init__(self):
        if not isinstance(self.name, str) or not _NAME.match(self.name):
            raise ValueError(f"Toolset name {self.name!r} must match {_NAME.pattern}.")
        agents = (self.agents,) if isinstance(self.agents, str) else tuple(self.agents)
        unknown = [role for role in agents if role not in AGENT_ROLES]
        if unknown:
            raise ValueError(f"Toolset {self.name!r} names unknown agent role(s) "
                             f"{unknown}; the roles are {list(AGENT_ROLES)}.")
        object.__setattr__(self, "agents", agents)


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


def discover() -> None:
    """Import every tool module in this package, once.

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
        for module in sorted(pkgutil.iter_modules(__path__), key=lambda m: m.name):
            if module.name.startswith("_"):
                continue
            qualified = f"{__name__}.{module.name}"
            try:
                importlib.import_module(qualified)
            except Exception as e:
                raise RuntimeError(f"Could not import agent tool module "
                                   f"{qualified}: {type(e).__name__}: {e}") from e
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
