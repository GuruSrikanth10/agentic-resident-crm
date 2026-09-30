"""Cooperative cancellation for an agent run the API has stopped waiting for.

`_investigate_packet` bounds `agent.invoke()` with `asyncio.wait_for`, but a
Python thread cannot be interrupted: when the budget expired, the route
recorded FAILED_TIMEOUT and returned while the graph ran on to the end in its
executor thread. On 2026-09-29 that did three things. The abandoned runs held
every `agent-invoke` worker, so the next batch queued behind them and timed
out without starting (10 of 21 timeouts never reached the log fetcher). They
ran Reviewer and Synthesis for packets already recorded as failed, with
`queue_for_replay` and `add_learning_rule` in reach. And their outcome was
never read, so one that died mid-run did so without a line in the log.

The route now hands each run a `threading.Event` and sets it on timeout.
Everything the run does below `scope()` sees it through a context variable
(LangGraph and LangChain copy the context into the threads they run nodes
and tools on), and `CancellationMiddleware`, which every agent carries,
refuses the next model call and the next tool call. The model call already in
flight still finishes -- nothing can stop it -- but no further one is made,
so the worker is back within one call instead of at the end of the graph.
"""
import contextlib
import contextvars
import threading
from typing import Optional

from langchain.agents.middleware import AgentMiddleware


class RunCancelled(Exception):
    """The run was abandoned by its caller and must stop.

    Not a transient failure: `retry_transient` does not retry it, and
    `llm_breaker` excludes it, so an abandoned run never counts towards
    opening the LLM circuit for every other packet.
    """


_cancel: contextvars.ContextVar[Optional[threading.Event]] = contextvars.ContextVar(
    "agent_run_cancel", default=None)


@contextlib.contextmanager
def scope(event: threading.Event):
    """Run the block as a run that `event` cancels."""
    token = _cancel.set(event)
    try:
        yield
    finally:
        _cancel.reset(token)


def run_in_scope(event: threading.Event, fn):
    """`fn()` as a run that `event` cancels. For an executor to call."""
    with scope(event):
        return fn()


def cancelled() -> bool:
    """Whether the current run has been abandoned. False outside a run."""
    event = _cancel.get()
    return event is not None and event.is_set()


def check() -> None:
    """Raise RunCancelled if the current run has been abandoned."""
    if cancelled():
        raise RunCancelled("The investigation was abandoned after its time budget ran out.")


class CancellationMiddleware(AgentMiddleware):
    """Refuse the next model call and tool call of an abandoned run."""

    def wrap_model_call(self, request, handler):
        check()
        return handler(request)

    async def awrap_model_call(self, request, handler):
        check()
        return await handler(request)

    def wrap_tool_call(self, request, handler):
        check()
        return handler(request)

    async def awrap_tool_call(self, request, handler):
        check()
        return await handler(request)
