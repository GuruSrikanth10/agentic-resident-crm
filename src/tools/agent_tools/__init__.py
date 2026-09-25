"""
Tools the pipeline's agents can call, discovered from this package.

Every agent is built by `src.core.agent_factory.build_agent`, which asks this
registry which tools the agent's role gets. Nothing outside this package has
to change to give an agent a new tool.

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
    )

    @agent_tool(MY_TOOLS)
    def lookup_something(refid: str) -> str:
        '''What this returns and when to use it. The model reads this.'''
        return json.dumps({"refid": refid})

That is the whole change. The module is imported the first time an agent is
built, and every role in `agents` gets the tool. A toolset's `enabled`
callable (default: always on) is checked when an agent is built, so a
toolset behind a feature flag drops out cleanly -- its tools are not offered
and its guidance is not added. Several modules can share one toolset by
importing it; a module whose name starts with "_" is a helper, never scanned.
`python3 -m src.tools.agent_tools list` shows what each role gets.

What the decorator gives every tool
-----------------------------------
- The docstring is the description the model sees and the signature is its
  argument schema, so both are written for the model.
- An exception inside the tool becomes a result saying the tool failed and
  read nothing. Without that, one bad lookup would raise out of the agent run
  and fail the whole packet.
- Each call is recorded while a node holds `recording()` open, which is how
  the Investigator's tool results reach the Reviewer as evidence.
- The decorated function is returned unchanged, for Python callers and tests.

Choosing tools per role without a code change
---------------------------------------------
AGENT_TOOLS_<ROLE> (e.g. AGENT_TOOLS_REVIEWER) replaces the default selection
for one role: a comma-separated list of tool names, or `none`. Unset or blank
keeps the defaults. Names are checked at boot by `validate()`. A tool whose
toolset is disabled stays off either way.

The DLT roles' prompts forbid per-packet evidence (their narrative is served
to every record with the same failure signature), so a tool that reads one
packet's data does not belong on a dlt_* role without a prompt change.
"""
import contextlib
import contextvars
import functools
import importlib
import inspect
import json
import os
import pkgutil
import re
import threading
from dataclasses import dataclass
from typing import Callable, Optional

from langchain_core.tools import BaseTool, StructuredTool

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Every agent the pipeline builds: the rejection lane's four, then the DLT
#: lane's three. These are also the `node` labels on the LLM metrics.
AGENT_ROLES = (
    "investigator",
    "reviewer",
    "synthesis",
    "log_filter",
    "dlt_investigator",
    "dlt_reviewer",
    "dlt_synthesis",
)

#: Names a registered tool may not take: the deep-agent built-ins every agent
#: already has, and the two tools the orchestrator passes explicitly. A clash
#: would silently shadow one of them.
RESERVED_TOOL_NAMES = frozenset({
    "write_todos", "ls", "read_file", "write_file", "edit_file", "glob",
    "grep", "execute", "task",
    "queue_for_replay", "add_learning_rule",
})

#: Provider-safe function names (OpenAI allows [a-zA-Z0-9_-]{1,64}).
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")

ENV_PREFIX = "AGENT_TOOLS_"
_NONE = "none"

#: Heading of the system-prompt section listing a role's tools. The agent
#: prompts refer to it by this name.
TOOLS_HEADING = "### AVAILABLE TOOLS"

_COMMON_RULES = """\
The tools below read live systems. What they return is evidence about this
packet, with the same standing as the logs. For every one of them:

1. Cite what you rely on: name the tool and the fields, for example
   "get_parking_status: parked_now is false".
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


def _always() -> bool:
    return True


@dataclass(frozen=True)
class Toolset:
    """A group of tools that share a default audience, a switch and guidance.

    `agents` are the roles that get the tools by default. `guidance` is added
    to the system prompt of every agent that gets at least one of them.
    `enabled` is checked each time an agent is built.
    """

    name: str
    agents: tuple
    guidance: str = ""
    enabled: Callable[[], bool] = _always

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
class _Entry:
    tool: BaseTool
    toolset: Toolset
    module: str


_registry: dict = {}
_lock = threading.RLock()
_discovered = False

#: The call list of the node currently recording, if any. A ContextVar rather
#: than a global: packets run concurrently on separate threads, and LangGraph
#: and the tool node copy the context into the threads they run tools on, so
#: a tool call -- including one made inside a `task` subagent -- lands in the
#: list of the node that started the run, and only that node.
_recorder: contextvars.ContextVar = contextvars.ContextVar(
    "agent_tool_recorder", default=None)


def agent_tool(toolset: Toolset, *, name: Optional[str] = None):
    """Register the decorated function as a tool of `toolset`.

    Returns the function unchanged. The registered tool wraps it: the result
    is returned as text, an exception becomes a failure message, and the call
    is recorded for the node that is recording.
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
        signature = inspect.signature(func)

        @functools.wraps(func)
        def call(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error("Agent tool failed", tool=tool_name,
                             error=f"{type(e).__name__}: {e}")
                result = (f"The tool {tool_name} failed ({type(e).__name__}) and "
                          f"returned no data. Treat this as an evidence gap, "
                          f"not as a result.")
            text = result if isinstance(result, str) \
                else json.dumps(result, default=str, ensure_ascii=False)
            _record(tool_name, _arguments(signature, args, kwargs), text)
            return text

        tool = StructuredTool.from_function(func=call, name=tool_name,
                                            description=description)
        _register(_Entry(tool=tool, toolset=toolset, module=func.__module__))
        return func

    return decorate


def _register(entry: _Entry) -> None:
    with _lock:
        existing = _registry.get(entry.tool.name)
        # The same module registering again is a reload; anything else is two
        # tools claiming one name.
        if existing is not None and existing.module != entry.module:
            raise ValueError(f"Tool {entry.tool.name!r} is registered twice: by "
                             f"{existing.module} and by {entry.module}.")
        _registry[entry.tool.name] = entry


def _arguments(signature: inspect.Signature, args, kwargs) -> dict:
    try:
        return dict(signature.bind_partial(*args, **kwargs).arguments)
    except TypeError:
        return {"args": list(args), **kwargs}


def discover() -> None:
    """Import every tool module in this package, once.

    A module that fails to import raises: a tool silently missing from an
    agent is worse than a boot that fails with the module's name.
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


def _check_role(role: str) -> None:
    if role not in AGENT_ROLES:
        raise ValueError(f"Unknown agent role {role!r}; the roles are "
                         f"{list(AGENT_ROLES)}.")


def _override(role: str) -> Optional[list]:
    """The AGENT_TOOLS_<ROLE> selection, or None when unset or blank."""
    raw = os.environ.get(ENV_PREFIX + role.upper(), "").strip()
    if not raw:
        return None
    if raw.lower() == _NONE:
        return []
    return list(dict.fromkeys(part.strip() for part in raw.split(",") if part.strip()))


def _is_enabled(toolset: Toolset) -> bool:
    try:
        return bool(toolset.enabled())
    except Exception as e:
        # A switch that cannot be read is off: offering a tool whose backing
        # system may be misconfigured is worse than not offering it.
        logger.error("Toolset switch raised; treating the toolset as disabled",
                     toolset=toolset.name, error=f"{type(e).__name__}: {e}")
        return False


def _entries_for(role: str) -> list:
    _check_role(role)
    discover()
    selection = _override(role)
    if selection is None:
        chosen = [entry for entry in _registry.values() if role in entry.toolset.agents]
    else:
        unknown = [name for name in selection if name not in _registry]
        if unknown:
            raise ValueError(f"{ENV_PREFIX}{role.upper()} names unknown tool(s) "
                             f"{unknown}; the registered tools are {sorted(_registry)}.")
        chosen = [_registry[name] for name in selection]

    switches = {}
    active = []
    for entry in chosen:
        if entry.toolset not in switches:
            switches[entry.toolset] = _is_enabled(entry.toolset)
        if switches[entry.toolset]:
            active.append(entry)
    # Sorted, so the tool list and the prompt section are identical on every
    # build and a prompt fingerprint cannot move on import order alone.
    return sorted(active, key=lambda entry: (entry.toolset.name, entry.tool.name))


def tools_for(role: str) -> list:
    """The registered tools `role` gets, in a stable order."""
    return [entry.tool for entry in _entries_for(role)]


def prompt_section(role: str) -> str:
    """The system-prompt section describing `role`'s tools, or "" for none."""
    entries = _entries_for(role)
    if not entries:
        return ""
    by_toolset: dict = {}
    for entry in entries:
        by_toolset.setdefault(entry.toolset, []).append(entry.tool.name)

    parts = [TOOLS_HEADING, _COMMON_RULES]
    for toolset, names in by_toolset.items():
        body = f"Tools: {', '.join(names)}."
        if toolset.guidance.strip():
            body += "\n\n" + inspect.cleandoc(toolset.guidance)
        parts.append(f"#### {toolset.name}\n{body}")
    return "\n\n".join(parts)


def get_tool(name: str) -> BaseTool:
    """One registered tool by name, whatever its roles or switch."""
    discover()
    try:
        return _registry[name].tool
    except KeyError:
        raise ValueError(f"No agent tool named {name!r}; the registered tools "
                         f"are {sorted(_registry)}.") from None


def describe() -> list:
    """Every registered tool, with the roles that get it now. For the CLI."""
    discover()
    active = {role: {entry.tool.name for entry in _entries_for(role)}
              for role in AGENT_ROLES}
    rows = []
    for name in sorted(_registry):
        entry = _registry[name]
        rows.append({
            "name": name,
            "toolset": entry.toolset.name,
            "enabled": _is_enabled(entry.toolset),
            "default_agents": list(entry.toolset.agents),
            "agents_now": [role for role in AGENT_ROLES if name in active[role]],
            "module": entry.module,
            "summary": entry.tool.description.splitlines()[0],
        })
    return rows


def fingerprint_material() -> str:
    """Everything tool-related an agent sees, for the prompt fingerprint.

    The tool names, descriptions, argument schemas and prompt sections decide
    what the agents can look up and how they are told to read it -- a change
    to any of them is a prompt change and must move the fingerprint.
    """
    lines = []
    for role in AGENT_ROLES:
        for entry in _entries_for(role):
            schema = json.dumps(entry.tool.args, sort_keys=True, default=str)
            lines.append(f"{role}\ttool\t{entry.tool.name}\t"
                         f"{entry.tool.description}\t{schema}")
        lines.append(f"{role}\tsection\t{prompt_section(role)}")
    return "\n".join(lines)


def validate() -> list:
    """Configuration errors, for `validate_config()` at boot."""
    try:
        discover()
    except Exception as e:
        return [str(e)]

    errors = []
    overrides = {ENV_PREFIX + role.upper() for role in AGENT_ROLES}
    for variable in sorted(os.environ):
        if variable.startswith(ENV_PREFIX) and variable not in overrides:
            errors.append(f"{variable} is not a known setting; per-role tool "
                          f"selections are {sorted(overrides)}.")
    for role in AGENT_ROLES:
        try:
            _entries_for(role)
        except ValueError as e:
            errors.append(str(e))
    return errors


# ---------------------------------------------------------------------------
# Evidence: the record of what the tools returned during one node's run
# ---------------------------------------------------------------------------

@contextlib.contextmanager
def recording():
    """Collect every registered-tool call made until the block exits.

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
    calls.append({"tool": tool_name, "args": arguments,
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
