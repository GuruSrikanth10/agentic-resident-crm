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

The three agents are deep agents built by `core/agent_factory.py`, which
gives each the tools its role gets from the MCP tool servers. No tool is
meant for the dlt_* roles: the narrative here is stored per error code and
re-served to every record with the same failure signature, and a tool that
reads one packet's data would put that packet's facts into it. Should a tool
be given to them, what it returns joins the evidence block below, so the
Reviewer checks against it like any other evidence.
"""
import json
import os
import threading
from typing import Optional

from langchain_core.messages import HumanMessage
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from src.core.agent_factory import build_agent
from src.dlt.corroborate import Corroboration
from src.tools import mcp_client
from src.models.dlt_synthesis import DltFinding
from src.utils import metrics
from src.utils.llm_utils import get_llm
from src.utils.logging_config import get_logger
from src.utils.resilience import llm_breaker, retry_transient

logger = get_logger(__name__)

_agent = None

#: The tool catalog `_agent` was built from; see agent_orchestrator's.
_agent_catalog = None

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
    #: What the Investigator's MCP tools returned (mcp_client
    #: records), merged across attempts. Empty while the dlt_* roles have no
    #: tools, which is the default.
    tool_evidence: list


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

    # The payload is evidence, not just a place the refId lives. Where the
    # frames name a loop and the payload holds what was being looped over,
    # this is the difference between "a row was missing" and "a row was
    # missing while resolving the entries this input carried".
    #
    # It is also the only per-structure context the prompts get. The prompts
    # are one file each, shared by every original topic; the payload structure
    # is not shared, so the type-specific facts -- which field is the
    # correlation id, which ids belong to other records -- travel here, in
    # text `summarise_payload` renders for whichever `__TypeId__` this record
    # actually has. The topic and type are named alongside it so a summary is
    # never read against the wrong structure.
    payload_summary = state.get("payload_summary") or "(no payload was captured)"
    origin_topic = failure.get("origin_topic") or "(unknown)"
    type_id = failure.get("type_id") or "(no __TypeId__ header)"

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
        f"### Case\n{state.get('case_id')}\n"
        f"Original topic: {origin_topic}\n"
        f"Payload type (__TypeId__): {type_id}\n\n"
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
        f"### Message payload (structure: {type_id})\n{payload_summary}\n\n"
        f"### Corroboration\n"
        f"Verdict: {corroboration.get('verdict')}\n"
        f"Reason: {corroboration.get('reason')}\n"
        f"Unexplained exceptions in the logs: "
        f"{', '.join(corroboration.get('unexplained') or []) or 'none'}\n\n"
        f"### Logs\n{logs[:MAX_EVIDENCE_CHARS]}\n"
        f"{_tool_evidence_block(state)}"
    )


def _tool_evidence_block(state: DltGraphState) -> str:
    """The Investigator's tool results, as a final evidence section, or ""."""
    rendered = mcp_client.render_evidence(state.get("tool_evidence"))
    return f"\n### Evidence retrieved with tools\n{rendered}\n" if rendered else ""


def _write_harness_case_files(case_dir, state: DltGraphState) -> None:
    """Write the evidence the harness Investigator and Reviewer read from disk.

    DLT cases live under dlt_cases/ in the casebook store, but the opencode
    agent can only read local files. Both harness nodes call this, so the
    Reviewer never depends on the Investigator's pass having left the
    directory in place.
    """
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / "dlt_evidence.txt").write_text(_evidence_block(state), encoding="utf-8")
    (case_dir / "dlt_failure.json").write_text(
        json.dumps(state.get("failure") or {}, indent=2, ensure_ascii=False),
        encoding="utf-8")


def get_dlt_agent():
    """Build (and cache) the DLT analysis graph."""
    global _agent
    if _agent is not None and not mcp_client.is_stale(_agent_catalog):
        return _agent

    with _agent_lock:
        if _agent is not None and not mcp_client.is_stale(_agent_catalog):
            return _agent
        return _build_dlt_agent()


def _build_dlt_agent():
    """Construct the graph. Caller must hold `_agent_lock`."""
    global _agent, _agent_catalog

    logger.info("Building the DLT agent graph")
    _agent_catalog = mcp_client.current_catalog()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prompts_dir = os.path.join(base_dir, "prompts")

    def load_prompt(filename: str) -> str:
        with open(os.path.join(prompts_dir, filename), "r", encoding="utf-8") as handle:
            return handle.read()

    investigator_prompt = load_prompt("DltInvestigatorAgent.md")
    reviewer_prompt = load_prompt("DltReviewerAgent.md")
    synthesis_prompt = load_prompt("DltSynthesisAgent.md")

    # Deep agents, each with the MCP tools its role gets -- none by default
    # (see the module docstring). The system prompts are fixed here,
    # so every node below sends only its user message.
    llm = get_llm("complex")
    investigator_agent = build_agent("dlt_investigator", llm, investigator_prompt)
    reviewer_agent = build_agent("dlt_reviewer", llm, reviewer_prompt)
    synthesis_agent = build_agent("dlt_synthesis", llm, synthesis_prompt)

    def investigator_node(state: DltGraphState):
        log = logger.bind(case_id=state.get("case_id"))
        log.info("DLT investigator started", state="DLT_INVESTIGATING")

        feedback = state.get("reviewer_feedback", "")
        is_retry = bool(feedback) and not is_approved(feedback)

        # Check if the DLT lane runs on the opencode harness (per lane).
        from src.utils.opencode_runner import lane_enabled
        use_harness = lane_enabled("dlt") and not is_retry

        if use_harness:
            from src.utils import opencode_runner, docs_loader
            from src.utils.paths import LOCAL_CASESHEETS_DIR

            ref_id = state.get("case_id", "unknown")
            case_dir = LOCAL_CASESHEETS_DIR / f"casebook_{ref_id}"
            _write_harness_case_files(case_dir, state)

            output_path = str(case_dir / "dlt_investigation.json")

            # Wait for corpus if not yet available
            if not docs_loader.corpus_available():
                for _ in range(60):
                    if docs_loader.corpus_available():
                        break
                    import time
                    time.sleep(1)

            from src.utils.prompt_loader import render as render_prompt
            harness_prompt = render_prompt(
                "DltInvestigator",
                ref_id=ref_id,
                output_path=output_path,
            )
            tools_section = mcp_client.prompt_section("dlt_investigator", opencode=True)
            if tools_section:
                harness_prompt = f"{harness_prompt.rstrip()}\n\n{tools_section}\n"

            try:
                result = opencode_runner.run_task_json(
                    prompt=harness_prompt,
                    output_path=output_path,
                    node="dlt_investigator",
                )
                investigation = result["result"].get("investigation", "")
                log.info("DLT investigator finished (opencode harness)",
                         elapsed=result.get("seconds"),
                         **(result.get("trace") or {}))
                calls = mcp_client.evidence_from_harness(result.get("tool_calls"))
                return {"investigation": investigation,
                        "tool_evidence": mcp_client.merge_evidence(
                            state.get("tool_evidence"), calls)}
            except Exception as e:
                log.warning("opencode harness failed for DLT investigator; falling back to direct LLM",
                            error=f"{type(e).__name__}: {e}")
                # Fall through to the direct LLM path below

        # Direct LLM path (original)
        prompt = _evidence_block(state)
        if is_retry:
            prompt += (f"\n### Reviewer feedback on your previous attempt\n"
                       f"{feedback}\n\nRevise your findings to address it.\n")

        @llm_breaker
        @retry_transient
        def invoke():
            # Recorded per attempt, as in the rejection lane.
            with mcp_client.recording() as calls:
                result = investigator_agent.invoke({"messages": [
                    HumanMessage(content=prompt),
                ]})
            return result, calls

        res, calls = invoke()
        metrics.record_llm_usage("dlt_investigator", res)
        metrics.LLM_CALLS.labels(node="dlt_investigator", outcome="ok").inc()
        return {"investigation": res["messages"][-1].content,
                "tool_evidence": mcp_client.merge_evidence(
                    state.get("tool_evidence"), calls)}

    def reviewer_node(state: DltGraphState):
        log = logger.bind(case_id=state.get("case_id"))
        log.info("DLT reviewer started", state="DLT_REVIEWING")

        investigation = state.get("investigation", "")

        # Check if the DLT lane runs on the opencode harness (per lane).
        from src.utils.opencode_runner import lane_enabled
        use_harness = lane_enabled("dlt")

        if use_harness:
            from src.utils import opencode_runner
            from src.utils.paths import LOCAL_CASESHEETS_DIR

            ref_id = state.get("case_id", "unknown")
            case_dir = LOCAL_CASESHEETS_DIR / f"casebook_{ref_id}"
            output_path = str(case_dir / "dlt_review.json")

            from src.utils.prompt_loader import render as render_prompt
            reviewer_harness_prompt = render_prompt(
                "DltReviewer",
                ref_id=ref_id,
                output_path=output_path,
            )
            tools_section = mcp_client.prompt_section("dlt_reviewer", opencode=True)
            if tools_section:
                reviewer_harness_prompt = (f"{reviewer_harness_prompt.rstrip()}"
                                           f"\n\n{tools_section}\n")

            try:
                # Inside the try, and the evidence rewritten rather than
                # assumed: a case directory that is missing or unwritable is a
                # harness failure like any other and falls back to the direct
                # LLM, instead of raising out of the node.
                _write_harness_case_files(case_dir, state)
                (case_dir / "dlt_investigation_text.txt").write_text(
                    investigation, encoding="utf-8")

                result = opencode_runner.run_task_json(
                    prompt=reviewer_harness_prompt,
                    output_path=output_path,
                    node="dlt_reviewer",
                )
                verdict = result["result"].get("verdict", "REJECTED").upper()
                feedback = result["result"].get("feedback", "")
                if verdict == "APPROVED":
                    feedback = "APPROVED"
                log.info("DLT reviewer finished (opencode harness)",
                         elapsed=result.get("seconds"), verdict=verdict,
                         **(result.get("trace") or {}))
                return {"reviewer_feedback": feedback,
                        "retry_count": state.get("retry_count", 0) + 1}
            except Exception as e:
                log.warning("opencode harness failed for DLT reviewer; falling back to direct LLM",
                            error=f"{type(e).__name__}: {e}")
                # Fall through to the direct LLM path below

        # Direct LLM path (original)
        prompt = (f"{_evidence_block(state)}\n"
                  f"### Investigator findings to validate\n"
                  f"{investigation}\n")

        @llm_breaker
        @retry_transient
        def invoke():
            return reviewer_agent.invoke({"messages": [
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


def investigate(ref_id: str, failure: dict, corroboration: Corroboration,
                logs: str, payload_summary: Optional[str] = None) -> tuple:
    """Run the analysis lane. Returns (finding, parse_error).

    `payload_summary` defaults to None so a caller without one still works --
    a header-only case has no payload to describe.
    """
    agent = get_dlt_agent()
    result = agent.invoke({
        "case_id": ref_id,
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
    global _agent, _agent_catalog
    _agent = None
    _agent_catalog = None
