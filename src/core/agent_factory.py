"""
Every agent in the pipeline is a deep agent (`deepagents.create_deep_agent`).

One builder for both lanes, so every agent gets the same things:

- the tools its role gets from the MCP tool servers (src/tools/mcp_client.py;
  src/tools/mcp_config.py says which servers) -- for a rejection role, only
  those in scope for the service pack the agent is built for
  (MULTI_SERVICE_PLAN.md D7) -- with the system-prompt section describing
  them, beside any tools the orchestrator passes itself (queue_for_replay,
  add_learning_rule);
- when a docs server is configured (mcp_config, kind "docs"), the service
  documentation tools, for the AGENT_DOCS_ROLES roles, with a SERVICE
  DOCUMENTATION TOOLS section of their own after AVAILABLE TOOLS that names
  the pack's documentation service. Their results are not evidence about the
  packet and are not recorded. Reading documentation costs tool and model
  calls, so a deployment with a docs server raises the limits below
  (.env.example);
- the deep-agent built-ins: planning (write_todos), a scratch filesystem held
  in the run's own state -- nothing is written to disk -- and `task`
  subagents;
- hard limits on tool calls and on model calls per run. The deep-agent
  default is a recursion limit of 9,999, which does not bound cost at all,
  and a runaway loop would outlive the server-side timeout that stops
  waiting for it;
- a note that the run is unattended and only its final message is read.

The general-purpose subagent behind `task` is configured here rather than
left to the deepagents default, for two reasons. The default copies every
tool the parent has -- which would hand queue_for_replay and
add_learning_rule, the two tools with side effects, to a subagent none of the
prompts mention -- and it gets none of the parent's middleware, so it would
run without the limits. Here it gets the parent's MCP tools -- the same
service-scoped list, never the role's full one, or the subagent would be a way
around the scope (D4) -- and limits of its own.

The system prompt is fixed when the agent is built, so a node invokes the
agent with the user message alone: {"messages": [HumanMessage(...)]}.
"""
import os
from typing import Optional, Sequence

from deepagents import create_deep_agent
from deepagents.middleware.subagents import GENERAL_PURPOSE_SUBAGENT
from langchain.agents.middleware import (
    ModelCallLimitMiddleware,
    ToolCallLimitMiddleware,
)

from src.tools import mcp_client
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: Tool calls one run may make, counting the deep-agent built-ins. Past it,
#: further calls are refused with a message telling the model to stop and
#: answer from what it has.
DEFAULT_MAX_TOOL_CALLS = 20

#: Model calls one run may make. Past it the run raises
#: ModelCallLimitExceededError and the node fails like any other agent
#: failure, rather than returning a limit notice as if it were the answer.
DEFAULT_MAX_MODEL_CALLS = 25

OPERATING_MODE = """\
### OPERATING MODE

You run unattended inside an automated pipeline. Nobody will answer a
question, so never ask one: work from the evidence you have and say plainly
what is missing. Only your final message is read. Put your complete answer in
it, in exactly the format the instructions above require -- not in a file and
not in a todo list. The file tools are private scratch space, discarded when
you finish; they hold nothing you were not given."""


def _limit(variable: str, default: int) -> int:
    """A positive integer setting. An unusable value falls back rather than
    raising -- a typo in a tunable must not fail every packet."""
    raw = os.environ.get(variable, "")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def max_tool_calls() -> int:
    return _limit("AGENT_MAX_TOOL_CALLS", DEFAULT_MAX_TOOL_CALLS)


def max_model_calls() -> int:
    return _limit("AGENT_MAX_MODEL_CALLS", DEFAULT_MAX_MODEL_CALLS)


def _limits() -> list:
    """Fresh limit middleware; each agent and subagent counts its own run."""
    return [
        ToolCallLimitMiddleware(run_limit=max_tool_calls(), exit_behavior="continue"),
        ModelCallLimitMiddleware(run_limit=max_model_calls(), exit_behavior="error"),
    ]


def system_prompt_for(role: str, system_prompt: str, pack: Optional[str] = None) -> str:
    """The role's prompt, its AVAILABLE TOOLS and SERVICE DOCUMENTATION TOOLS
    sections, then the operating note.

    deepagents appends its own base prompt and the built-in tools'
    instructions after this.
    """
    parts = [(system_prompt or "").rstrip(), mcp_client.prompt_section(role, pack),
             OPERATING_MODE]
    return "\n\n".join(part for part in parts if part)


def build_agent(role: str, model, system_prompt: str, tools: Sequence = (), *,
                pack: Optional[str] = None):
    """Build the deep agent for `role` (one of agent_tools.AGENT_ROLES).

    `pack` is the service pack the agent is built for: a rejection role's MCP
    tools are the ones in scope for it, and it is required for those roles;
    a DLT role takes none (mcp_client.selection). `tools` are the role's
    explicit tools; its MCP tools are added here. Returns a compiled graph to
    invoke with {"messages": [...]}.
    """
    registered = mcp_client.tools_for(role, pack)
    explicit = list(tools)
    clash = sorted({tool.name for tool in explicit} & {tool.name for tool in registered})
    if clash:
        raise ValueError(f"Tool name(s) {clash} are both passed explicitly and "
                         f"served over MCP for role {role!r}.")

    # The parent's own scoped list: a subagent given the role's full list
    # would reach tools the parent's service may not use.
    general_purpose = {
        **GENERAL_PURPOSE_SUBAGENT,
        "tools": registered,
        "middleware": _limits(),
    }
    agent = create_deep_agent(
        model=model,
        tools=[*explicit, *registered],
        system_prompt=system_prompt_for(role, system_prompt, pack),
        middleware=_limits(),
        subagents=[general_purpose],
        name=role,
    )
    logger.info("Agent built", role=role, service_pack=pack,
                explicit_tools=[tool.name for tool in explicit],
                mcp_tools=[tool.name for tool in registered],
                max_tool_calls=max_tool_calls(),
                max_model_calls=max_model_calls())
    return agent
