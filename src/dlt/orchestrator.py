"""Phase 8 of DLT_PLAN.md -- the DLT analysis lane.

The only place in this flow that calls an LLM, and it calls one only for a
novel Class A fingerprint or a corroboration discrepancy. Everything else is
answered by `canned.py` or by a group's stored recommendation.

Investigate -> Review -> Synthesise, reusing the rejection orchestrator's
proven shape: a bounded retry loop when the Reviewer rejects, a single repair
attempt when the model's JSON does not satisfy the contract, and the circuit
breaker and transient-retry decorators around every call.

The prompts are deliberately narrow. With no source access and no database
access, the Investigator's job is to check the trace against the logs and to
say clearly what the evidence cannot establish -- not to explain a bug it
cannot see.
"""
import json
import os
import threading
from typing import Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import create_react_agent
from typing_extensions import TypedDict

from src.dlt.corroborate import Corroboration
from src.models.dlt_synthesis import DltFinding
from src.utils import metrics
from src.utils.llm_utils import get_llm
from src.utils.logging_config import get_logger
from src.utils.resilience import llm_breaker, retry_transient

logger = get_logger(__name__)

_agent = None

#: Guards the lazy build. `/analyze-dlt` is a sync endpoint dispatched on
#: Starlette's threadpool, so two concurrent DLT cases can already reach the
#: builder at once today -- this one was never masked by the event loop.
_agent_lock = threading.Lock()

MAX_EVIDENCE_CHARS = int(os.environ.get("DLT_MAX_EVIDENCE_CHARS", "40000"))


class DltGraphState(TypedDict, total=False):
    case_id: str
    failure: dict
    corroboration: dict
    logs: str
    payload_summary: Optional[str]
    investigation: str
    reviewer_feedback: str
    retry_count: int
    finding: Optional[dict]
    parse_error: Optional[str]


def is_approved(feedback: str) -> bool:
    """Reuses the rejection Reviewer's approval rule verbatim."""
    from src.core.agent_orchestrator import is_reviewer_approved

    return is_reviewer_approved(feedback)


def _evidence_block(state: DltGraphState) -> str:
    """The context both the Investigator and the Reviewer see."""
    failure = state.get("failure") or {}
    corroboration = state.get("corroboration") or {}
    logs = state.get("logs") or "(no logs were fetched)"

    chain = "\n".join(
        f"  {i}. {link.get('fqcn')}: {link.get('message', '')[:300]}"
        for i, link in enumerate(failure.get("chain") or [], start=1)
    )
    frames = "\n".join(f"  - {frame}" for frame in (failure.get("frames") or []))

    # The payload is evidence, not just a place the refId lives. For the
    # ABIS dedupe failures this lane sees most, the frames name a loop over
    # matched candidates and the payload holds the candidates being looped
    # over -- which is the difference between "a row was missing" and "a row
    # was missing while resolving these specific candidates".
    payload_summary = state.get("payload_summary") or "(no payload was captured)"

    registry = failure.get("registry_description")
    category = failure.get("registry_category")
    category_line = (f"\nCategory: {category}"
                     f" ({failure.get('registry_category_source') or 'unknown provenance'})"
                     if category else "")
    registry_line = (f"{registry}{category_line}\n(This is the entire registry "
                     f"entry. It is one line. Do not extrapolate beyond it.)"
                     if registry else
                     "(No registry entry exists for this code.)")

    return (
        f"### Case\n{state.get('case_id')}\n\n"
        f"### Declared failure\n"
        f"Class: {failure.get('failure_class')} ({failure.get('class_reason')})\n"
        f"Root exception: {failure.get('root_fqcn')}\n"
        f"Root message: {failure.get('root_message')}\n"
        f"Business code: {failure.get('business_code')}\n"
        f"Trace truncated: {failure.get('truncated')}\n\n"
        f"### Registry description\n{registry_line}\n\n"
        f"### Exception chain (outermost first; the LAST entry is the root)\n"
        f"{chain or '  (none parsed)'}\n\n"
        f"### Application frames at the failure site\n{frames or '  (none)'}\n\n"
        f"### Message payload\n{payload_summary}\n\n"
        f"### Corroboration\n"
        f"Verdict: {corroboration.get('verdict')}\n"
        f"Reason: {corroboration.get('reason')}\n"
        f"Unexplained exceptions in the logs: "
        f"{', '.join(corroboration.get('unexplained') or []) or 'none'}\n\n"
        f"### Logs\n{logs[:MAX_EVIDENCE_CHARS]}\n"
    )


def get_dlt_agent():
    """Build (and cache) the DLT analysis graph."""
    global _agent
    if _agent is not None:
        return _agent

    with _agent_lock:
        if _agent is not None:
            return _agent
        return _build_dlt_agent()


def _build_dlt_agent():
    """Construct the graph. Caller must hold `_agent_lock`."""
    global _agent

    logger.info("Building the DLT agent graph")
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prompts_dir = os.path.join(base_dir, "prompts")

    def load_prompt(filename: str) -> str:
        with open(os.path.join(prompts_dir, filename), "r", encoding="utf-8") as handle:
            return handle.read()

    investigator_prompt = load_prompt("DltInvestigatorAgent.md")
    reviewer_prompt = load_prompt("DltReviewerAgent.md")
    synthesis_prompt = load_prompt("DltSynthesisAgent.md")

    llm = get_llm("complex")
    investigator_agent = create_react_agent(llm, tools=[])
    reviewer_agent = create_react_agent(llm, tools=[])
    synthesis_agent = create_react_agent(llm, tools=[])

    def investigator_node(state: DltGraphState):
        log = logger.bind(case_id=state.get("case_id"))
        log.info("DLT investigator started", state="DLT_INVESTIGATING")

        prompt = _evidence_block(state)
        feedback = state.get("reviewer_feedback", "")
        if feedback and not is_approved(feedback):
            prompt += (f"\n### Reviewer feedback on your previous attempt\n"
                       f"{feedback}\n\nRevise your findings to address it.\n")

        @llm_breaker
        @retry_transient
        def invoke():
            return investigator_agent.invoke({"messages": [
                SystemMessage(content=investigator_prompt),
                HumanMessage(content=prompt),
            ]})

        res = invoke()
        metrics.record_llm_usage("dlt_investigator", res)
        metrics.LLM_CALLS.labels(node="dlt_investigator", outcome="ok").inc()
        return {"investigation": res["messages"][-1].content}

    def reviewer_node(state: DltGraphState):
        log = logger.bind(case_id=state.get("case_id"))
        log.info("DLT reviewer started", state="DLT_REVIEWING")

        prompt = (f"{_evidence_block(state)}\n"
                  f"### Investigator findings to validate\n"
                  f"{state.get('investigation', '')}\n")

        @llm_breaker
        @retry_transient
        def invoke():
            return reviewer_agent.invoke({"messages": [
                SystemMessage(content=reviewer_prompt),
                HumanMessage(content=prompt),
            ]})

        res = invoke()
        metrics.record_llm_usage("dlt_reviewer", res)
        metrics.LLM_CALLS.labels(node="dlt_reviewer", outcome="ok").inc()
        return {"reviewer_feedback": res["messages"][-1].content,
                "retry_count": state.get("retry_count", 0) + 1}

    def check_approval(state: DltGraphState):
        feedback = state.get("reviewer_feedback", "")
        retries = state.get("retry_count", 0)
        limit = int(os.environ.get("DLT_MAX_INVESTIGATION_RETRIES", "3"))

        if is_approved(feedback):
            return "approved"
        if retries >= limit:
            # Synthesise the best available findings rather than dropping the
            # case: an unreviewed narrative with a capped confidence is more
            # useful than nothing, and the casebook records that it was never
            # approved.
            logger.bind(case_id=state.get("case_id")).warning(
                "DLT reviewer never approved; synthesising anyway",
                retries=retries)
            return "approved"
        return "retry"

    def synthesis_node(state: DltGraphState):
        log = logger.bind(case_id=state.get("case_id"))
        log.info("DLT synthesis started", state="DLT_SYNTHESISING")

        prompt = (f"Convert these approved findings into the JSON contract.\n\n"
                  f"{state.get('investigation', '')}\n")

        @llm_breaker
        @retry_transient
        def invoke():
            return synthesis_agent.invoke({"messages": [
                SystemMessage(content=synthesis_prompt),
                HumanMessage(content=prompt),
            ]})

        res = invoke()
        metrics.record_llm_usage("dlt_synthesis", res)
        metrics.LLM_CALLS.labels(node="dlt_synthesis", outcome="ok").inc()
        raw = res["messages"][-1].content

        finding, error = parse_finding(raw)
        if finding is not None:
            return {"finding": finding.model_dump(), "parse_error": None}

        # One repair attempt, mirroring the rejection path.
        log.warning("DLT synthesis output failed the contract; repairing",
                    error=error)

        @llm_breaker
        @retry_transient
        def invoke_repair():
            return synthesis_agent.invoke({"messages": [
                SystemMessage(content=synthesis_prompt),
                HumanMessage(content=(
                    f"Your previous reply did not satisfy the contract: {error}\n\n"
                    f"Previous reply:\n{raw}\n\n"
                    f"Reply again with ONLY the JSON object.")),
            ]})

        repaired = invoke_repair()
        metrics.record_llm_usage("dlt_synthesis_repair", repaired)
        finding, error = parse_finding(repaired["messages"][-1].content)
        if finding is not None:
            return {"finding": finding.model_dump(), "parse_error": None}

        log.error("DLT synthesis failed the contract after repair", error=error)
        return {"finding": None, "parse_error": error}

    graph = StateGraph(DltGraphState)
    graph.add_node("investigate", investigator_node)
    graph.add_node("review", reviewer_node)
    graph.add_node("synthesise", synthesis_node)

    graph.add_edge(START, "investigate")
    graph.add_edge("investigate", "review")
    graph.add_conditional_edges("review", check_approval,
                                {"approved": "synthesise", "retry": "investigate"})
    graph.add_edge("synthesise", END)

    _agent = graph.compile()
    return _agent


def parse_finding(text: str):
    """Parse a synthesis reply into a `DltFinding`. Returns (finding, error)."""
    from src.models.synthesis import extract_json_block

    block = extract_json_block(text or "")
    if not block:
        return None, "Response contained no JSON object."
    try:
        return DltFinding(**json.loads(block)), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def investigate(case_id: str, failure: dict, corroboration: Corroboration,
                logs: str, payload_summary: Optional[str] = None) -> tuple:
    """Run the analysis lane. Returns (finding, parse_error).

    `payload_summary` defaults to None so a caller without one still works --
    a header-only case has no payload to describe.
    """
    agent = get_dlt_agent()
    result = agent.invoke({
        "case_id": case_id,
        "failure": failure,
        "payload_summary": payload_summary,
        "corroboration": {
            "verdict": corroboration.verdict.value,
            "reason": corroboration.reason,
            "unexplained": list(corroboration.unexplained),
        },
        "logs": logs,
        "retry_count": 0,
    })

    finding = result.get("finding")
    if finding is None:
        return None, result.get("parse_error")
    return DltFinding(**finding), None


def reset_agent_cache() -> None:
    """Drop the cached graph. For tests."""
    global _agent
    _agent = None
