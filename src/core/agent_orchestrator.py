import os
import json
import contextvars
import threading
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from src.utils import metrics
from src.utils.llm_utils import get_llm
from src.tools.tool_registry import (
    fetch_and_persist_logs,
    get_tool_by_name,
    lookup_rule_for,
    lookup_rule_text,
    get_error_description,
)
from src.storage.factory import get_casebook_storage
from src.models.synthesis import (
    ACTIONS,
    RESIDENT_ACTIONS,
    apply_confidence_policy,
    parse_synthesis,
)
from src.utils.resilience import retry_transient, llm_breaker
from src.utils.runbook_validator import validate_learning_rule
from src.utils.logging_config import get_logger
from src.core.checkpointer import get_checkpointer
from src.utils.runbook_store import (
    generate_rule_fingerprint,
    get_runbook,
    is_serve_allowed,
)

logger = get_logger(__name__)

# Per-packet context for the add_learning_rule tool, which is now built once
# at graph-construction time instead of once per review call (2.1). Each
# packet is processed on its own dedicated thread (see routes.py), and
# contextvars are thread-local by default, so setting these at the top of
# reviewer_node is safe under concurrent packets.
_current_event_id: contextvars.ContextVar[str] = contextvars.ContextVar("current_event_id", default="unknown")
_current_investigation: contextvars.ContextVar[str] = contextvars.ContextVar("current_investigation", default="")

def _counted(node: str, invoke):
    """Run an LLM invocation, counting failures as well as successes.

    Every LLM_CALLS call site recorded a success-shaped outcome (ok, invalid,
    unrepairable, abstained). A call that raised propagated through
    @retry_transient and @llm_breaker and was counted nowhere, so LLM error
    rate -- and with it the breaker-trip frequency ENHANCEMENT_PLAN section 4.5
    lists as unknowable -- could not be computed at all (G17).
    """
    try:
        return invoke()
    except Exception:
        metrics.LLM_CALLS.labels(node=node, outcome="error").inc()
        raise


def is_reviewer_approved(feedback: str) -> bool:
    """Return True only if the Reviewer's verdict is an unqualified APPROVED.

    A naive `"APPROVED" in feedback.upper()` substring check also matches
    "NOT APPROVED", "DISAPPROVED", or prose like "this is not approved
    because...", silently skipping the QC loop. The Reviewer is instructed to
    reply with exactly 'APPROVED' when findings are valid, so requiring the
    (markdown/whitespace-stripped) verdict to *start with* that token is both
    correct for the happy path and closed against negation phrasing.
    """
    normalized = (feedback or "").strip().strip("*_`\"' \t\n\r").upper()
    return normalized.startswith("APPROVED")

class GraphState(TypedDict):
    payload: dict
    logs: str
    db_rule: str
    investigation: str
    reviewer_feedback: str
    synthesis: str
    messages: list
    retry_count: int
    runbook_id: str
    resolution_source: str
    shadow_runbook_resolution: str
    #: What the shadowed runbook would have decided, and whether it agreed
    #: with the agents. Carried out of the graph so it reaches the casebook
    #: and the outcome record, which is what turns shadow mode into evidence
    #: instead of log noise (G18).
    shadow_comparison: dict

_agent = None

#: Guards the lazy build below. Two concurrent first-callers each built a full
#: graph -- two LLM clients, four react agents, and two `get_checkpointer()`
#: calls, the second of which opens another connection and re-runs setup()'s
#: DDL. This was masked while `get_agent()` ran on the event loop, which
#: serialised it; moving the call off the loop removes that accidental
#: protection, so the lock has to be real. Same shape as
#: `core/checkpointer.py` and `sources/k8s/client.py`.
_agent_lock = threading.Lock()

#: Hash of the prompts and policy the cached graph was built from.
#: Written into every casebook so an accuracy movement can be attributed to a
#: prompt change rather than merely coinciding with one. The rule side of this
#: is already solved by `rule_fingerprint`; the prompt side had no equivalent,
#: so after Phase D there was an accuracy figure per reason code and no way to
#: tell what moved it (G23).
_prompt_fingerprint = "unknown"

PROMPT_FILES = (
    "InvestigatorAgent.md",
    "ReviewerAgent.md",
    "SynthesisAgent.md",
    "LogFilterAgent.md",
)


def compute_prompt_fingerprint(base_dir: str) -> str:
    """SHA256 over the four agent prompts plus the shared policy context.

    Sorted and length-prefixed so the digest cannot be changed by reordering
    or by content shifting across a boundary.
    """
    import hashlib

    digest = hashlib.sha256()
    paths = [os.path.join(base_dir, "prompts", name) for name in PROMPT_FILES]
    paths.append(os.path.join(os.path.dirname(base_dir), "agent_policy_context.md"))

    for path in sorted(paths):
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            # A missing prompt is itself a meaningful configuration, so record
            # its absence rather than skipping it and colliding with present.
            body = b""
        digest.update(os.path.basename(path).encode("utf-8"))
        digest.update(str(len(body)).encode("utf-8"))
        digest.update(body)

    return "sha256:" + digest.hexdigest()


def prompt_fingerprint() -> str:
    return _prompt_fingerprint


def get_agent():
    global _agent, _prompt_fingerprint

    # Fast path without the lock: once built, `_agent` never changes, and an
    # unsynchronised read of an already-published reference is safe.
    if _agent is not None:
        logger.info("Returning the cached agent graph")
        return _agent

    with _agent_lock:
        # Re-check: another thread may have built it while we waited.
        if _agent is not None:
            return _agent
        return _build_agent()


def _build_agent():
    """Construct the graph. Caller must hold `_agent_lock`."""
    global _agent, _prompt_fingerprint

    logger.info("Building the agent graph")
    base_dir = os.path.dirname(os.path.dirname(__file__))
    _prompt_fingerprint = compute_prompt_fingerprint(base_dir)
    logger.info("Prompt fingerprint computed", prompt_fingerprint=_prompt_fingerprint)
    llm = get_llm("complex")
    # DELIBERATE DEVIATION (ENHANCEMENT_PLAN section 7.1, AUDIT_2026_08 G6).
    # This reads "complex" on purpose. The Reviewer is a bounded verdict task
    # and the cheaper "simple" tier would suit it -- that is the documented
    # recommendation, and it is roughly a third of all LLM calls -- but the
    # change was explicitly declined by the requester and has not been
    # re-authorised. Flipping it is a one-word edit here; the regression test
    # (tests/test_phase2_fixes.py::test_reviewer_built_once_with_simple_llm)
    # is marked xfail(strict=True) against this exact line, so applying the
    # fix turns that test green again and the xfail marker must then come off.
    simple_llm = get_llm("complex")
    
    def load_prompt(filename, with_policy: bool = True):
        """Read an agent prompt, optionally appending the business policy.

        `with_policy` exists because the policy document was appended to every
        prompt including the LogFilter's. That agent strips log lines not
        belonging to a target event id -- a mechanical text operation with no
        use for business policy -- so the whole document rode along in the
        context window of every filter call for nothing (G24).
        """
        with open(os.path.join(base_dir, "prompts", filename), "r", encoding="utf-8") as f:
            prompt = f.read()

        if not with_policy:
            return prompt

        policy_path = os.path.join(os.path.dirname(base_dir), "agent_policy_context.md")
        if os.path.exists(policy_path):
            with open(policy_path, "r", encoding="utf-8") as f:
                policy = f.read()
            # Inject policy context directly into the prompt so the LLM sees it.
            prompt += "\n\n### GLOBAL BUSINESS POLICY CONTEXT\n" + policy

        return prompt

    investigator_prompt = load_prompt("InvestigatorAgent.md")
    reviewer_prompt = load_prompt("ReviewerAgent.md")
    synthesis_prompt = load_prompt("SynthesisAgent.md")
    log_filter_prompt = load_prompt("LogFilterAgent.md", with_policy=False)
    
    def fetch_logs_node(state: GraphState):
        payload = state.get("payload", {})
        event_id = payload.get("eventId", "")
        log = logger.bind(event_id=event_id)
        log.info("Log fetcher node started", state="LOG_FETCHER")

        # Cache-first: POST /fetch-logs (the fast consumer's route) already
        # fetched and persisted this event's logs before /analyze-rejection
        # ever invokes the graph, so the normal path here is a storage read,
        # not a live Elasticsearch/Kubernetes round trip. Presence of the
        # artifact -- regardless of its content, including the "disabled"/"no
        # logs found" sentinels fetch_and_persist_logs persists verbatim -- is
        # the unambiguous signal that a fetch was already attempted.
        #
        # Falling back to a live fetch when the artifact is absent keeps
        # every caller that invokes the graph directly (POST
        # /process-rejection, local_run.py, a checkpoint resume that predates
        # this split, or /analyze-rejection racing ahead of /fetch-logs)
        # working exactly as before -- the graph's node set, edges, and
        # thread_id=event_id checkpoint keying are unchanged either way.
        cached = get_casebook_storage().load_artifact(event_id, "fetched_logs.txt")
        if cached is not None:
            log.info("Using logs persisted by the fast consumer", state="LOG_FETCHER")
            return {"logs": cached}

        log.info("No persisted logs found; fetching live", state="LOG_FETCHER")
        logs = fetch_and_persist_logs(event_id, payload)
        log.info("Logs retrieved")
        return {"logs": logs}

    def runbook_lookup_node(state: GraphState):
        mode = os.environ.get("RUNBOOK_MODE", "off").lower()
        if mode == "off":
            return {"resolution_source": "agent"}

        payload = state.get("payload", {})
        event_id = payload.get("eventId", "unknown")
        log = logger.bind(event_id=event_id)

        # Every outcome is counted, not just hits. A counter that only ever
        # records "hit" has no denominator, so the runbook hit RATE -- named in
        # ENHANCEMENT_PLAN section 4.5 as one of the unknowables and the primary
        # input to the section 4.2 rollout decision -- stayed unknowable (G16).
        def _miss(reason: str):
            metrics.RUNBOOK_LOOKUPS.labels(outcome=reason).inc()
            return {"resolution_source": "agent"}

        # The runbook path is an optimisation, never a correctness
        # requirement: falling back to the agents always produces a valid
        # result. Any failure here must therefore degrade to "agent", not
        # propagate -- an uncaught TypeError from the fingerprint check used
        # to fail agent.invoke() outright and DLQ every runbook-matching
        # packet (F2).
        try:
            exec_summary = payload.get("packetExecutionSummary") or {}
            error_data = exec_summary.get("errorData") or []
            reason_code = None
            for err in error_data:
                if err and err.get("errorReasonCode"):
                    reason_code = err.get("errorReasonCode")
                    break

            if not reason_code:
                return _miss("no_reason_code")

            packet_type = payload.get("packetMetaData", {}).get("enrolmentType", "")
            runbook = get_runbook(reason_code, packet_type)
            if not runbook:
                return _miss("miss")

            runbook_id = runbook["runbook_id"]
            version = runbook["version"]

            # Staleness check: serve the runbook only while the DB rule it was
            # derived from is unchanged. Fingerprint the *parsed* rows, not the
            # raw to_json string (F2).
            rules = lookup_rule_for(reason_code, packet_type)
            if rules:
                current_fp = generate_rule_fingerprint(rules)
                if current_fp != runbook["rule_fingerprint"]:
                    log.warning("Fingerprint mismatch", runbook_id=runbook_id,
                                expected=runbook["rule_fingerprint"], actual=current_fp)
                    return _miss("fingerprint_mismatch")

            res_source = f"runbook:{runbook_id}@v{version}"
            synthesis_json = json.dumps(runbook["resolution"])

            # A reason code not on the allowlist still runs the agents, but
            # its runbook is compared against them -- which is how it earns
            # its place on the allowlist (4.2).
            if mode == "shadow" or not is_serve_allowed(reason_code):
                if mode != "shadow":
                    log.info("Runbook not yet cleared to serve; shadowing instead",
                             runbook_id=runbook_id)
                log.info("Runbook shadowed", runbook_id=runbook_id, version=version, mode=mode)
                metrics.RUNBOOK_LOOKUPS.labels(outcome="shadow").inc()
                return {
                    "resolution_source": "agent",
                    "shadow_runbook_resolution": synthesis_json,
                    "runbook_id": runbook_id,
                }

            log.info("Runbook hit", runbook_id=runbook_id, version=version, mode=mode)
            metrics.RUNBOOK_LOOKUPS.labels(outcome="hit").inc()
            return {"resolution_source": res_source, "synthesis": synthesis_json, "runbook_id": runbook_id}
        except Exception as e:
            log.error("Runbook lookup failed; falling through to the agents",
                      error=f"{type(e).__name__}: {e}", exc_info=True)
            return _miss("error")

    def check_runbook_hit(state: GraphState):
        if state.get("resolution_source", "").startswith("runbook:"):
            return "end"
        if os.environ.get("ENABLE_LOG_FILTER_AGENT", "false").lower() == "true":
            return "filter"
        return "investigate"

    # Create agents once during graph construction, not per invocation.
    investigator_agent = create_react_agent(llm, tools=[])
    log_filter_agent = create_react_agent(llm, tools=[])
    queue_tool = get_tool_by_name("queue_for_replay")
    synthesis_agent = create_react_agent(llm, tools=[queue_tool])

    from langchain_core.tools import tool
    from datetime import datetime
    from filelock import FileLock

    @tool
    def add_learning_rule(rule_text: str, reasoning: str) -> str:
        """Propose a new permanent rule to fix Investigator mistakes."""
        target_file = os.path.join(base_dir, "prompts", "pending_rules.jsonl")
        lock_file = target_file + ".lock"

        # Validate at the point of proposal, not only at promotion.
        #
        # This argument is LLM-generated text derived from log content, and
        # log content is influenced by upstream request data. Whatever lands
        # here can be appended verbatim to InvestigatorAgent.md by
        # promote_rules.py -- labelled "CRITICAL RULE" -- and becomes part of
        # the system prompt for every future packet. Rejecting here keeps
        # instruction-shaped text out of the operator's queue entirely,
        # rather than relying on one interactive y/N to catch it (G19).
        violations = validate_learning_rule(rule_text)
        if violations:
            logger.warning(
                "Rejected a proposed learning rule",
                event_id=_current_event_id.get(),
                violations=violations,
            )
            return (
                "Rule rejected and NOT queued. Fix these and try again: "
                + "; ".join(violations)
            )

        entry = {
            "eventId": _current_event_id.get(),
            "timestamp": datetime.now().isoformat(),
            "proposed_rule": rule_text,
            "reviewer_reasoning": reasoning,
            "investigator_original_output": _current_investigation.get(),
        }
        try:
            with FileLock(lock_file, timeout=10):
                with open(target_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
            return f"Successfully queued rule for human review: {rule_text}"
        except Exception as e:
            return f"Failed to queue rule: {e}"

    # 2.1: built once here (not per-review) now that the tool reads its
    # per-packet context from contextvars instead of a closure over
    # event_id/investigation -- rebuilding a React agent (with the tool
    # schema binding that implies) on every single review call, and every
    # retry loop, was pure waste.
    # 2.2: the Reviewer would be a natural fit for the cheaper "simple" tier.
    # It is NOT on that tier today -- `simple_llm` is bound to "complex" by the
    # deliberate deviation documented at its assignment above. The name is kept
    # so the one-word fix stays a one-word fix.
    reviewer_agent = create_react_agent(simple_llm, tools=[add_learning_rule])

    def filter_logs_node(state: GraphState):
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        
        logs = state.get("logs", "")
        if not logs or logs == "Log fetching disabled.":
            return {}
            
        log.info("Log Filter node started", state="FILTERING")
        
        prompt = (
            f"Target Event ID: {event_id}\n\n"
            f"Raw Logs:\n{logs}\n\n"
            "Return ONLY the clean log string, stripping out any errors that do not belong to the Target Event ID."
        )
        
        @llm_breaker
        @retry_transient
        def invoke_filter():
            return log_filter_agent.invoke({"messages": [
                SystemMessage(content=log_filter_prompt),
                HumanMessage(content=prompt)
            ]})
            
        res = _counted("log_filter", invoke_filter)
        filtered_logs = res["messages"][-1].content
        
        try:
            get_casebook_storage().save_artifact(event_id, "filtered_logs.txt", filtered_logs)
            log.info("Persisted filtered logs to local artifact for testing", artifact="filtered_logs.txt")
        except Exception as e:
            log.warning("Failed to persist filtered logs artifact", error=str(e))
            
        log.info("Log Filter node finished")
        return {"logs": filtered_logs}

    def investigator_node(state: GraphState):
        payload = state.get("payload", {})
        event_id = payload.get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        log.info("Investigator node started", state="INVESTIGATING")
        logs = state.get("logs", "")
        feedback = state.get("reviewer_feedback", "")
        db_rule = state.get("db_rule", "")
        investigation = state.get("investigation", "")

        # Optimize DB Calls: Fetch rule in Python if not already fetched
        if not db_rule:
            exec_summary = payload.get("packetExecutionSummary") or {}
            error_data = exec_summary.get("errorData") or []
            reason_code = None
            for err in error_data:
                if err and err.get("errorReasonCode"):
                    reason_code = err.get("errorReasonCode")
                    break
            
            if reason_code:
                # Lookup + enrolment-type filtering now live together in
                # tool_registry.lookup_rule_text; this node previously carried
                # its own copy of the filter, which drifted from the three
                # runbook call sites' copy (F2).
                packet_type = payload.get("packetMetaData", {}).get("enrolmentType", "")
                try:
                    db_rule = lookup_rule_text(reason_code, packet_type)
                    # If db_rule is empty or indicates failure, fallback to hardcoded description
                    if db_rule.startswith("Rule not found") or db_rule == "[]":
                        fallback_desc = get_error_description(reason_code)
                        if fallback_desc != "Unknown error code.":
                            db_rule += f"\n\nSystem Error Description: {fallback_desc}"
                except Exception as e:
                    log.warning("Rule lookup failed", error=f"{type(e).__name__}: {e}")
                    db_rule = f"Rule lookup failed for reason code {reason_code}: {e}"
                    fallback_desc = get_error_description(reason_code)
                    if fallback_desc != "Unknown error code.":
                        db_rule += f"\n\nSystem Error Description: {fallback_desc}"
            else:
                db_rule = "No errorReasonCode found in payload."

        is_retry = bool(feedback)
        if is_retry:
            # Retry: send the delta plus the evidence.
            #
            # 2.3 dropped the payload, the rule AND the logs on retry, on the
            # reasoning that none had changed since attempt one. That holds
            # for the payload and the rule -- both are static and both are
            # already reflected in the prior investigation. It does not hold
            # for the logs: the Reviewer's most common rejection is that the
            # findings are not grounded in the log evidence, and the retry
            # then asked the Investigator to fix a citation problem with the
            # citations removed from its context. It could not comply, so the
            # loop ran to MAX_INVESTIGATION_RETRIES and escalated -- saving
            # one log payload and spending three LLM round-trips plus a manual
            # review to do it (G12).
            prompt = (
                f"Your previous analysis:\n{investigation}\n\n"
                f"Reviewer Feedback (You MUST fix your previous analysis): {feedback}\n\n"
            )
            if logs and logs != "Log fetching disabled.":
                prompt += f"Elasticsearch Logs (cite these):\n{logs}\n\n"
        else:
            # Project the payload down to only the fields the prompt
            # actually needs -- the full nested Kafka message carries many
            # fields (sourceTopic, callbackTopic, taskMetaData, rejectBits,
            # resubmissionSummary, uidV2DataArray, ...) the Investigator
            # never uses (2.3).
            flow_meta = payload.get("flowMetaData") or {}
            projected_payload = {
                "eventId": payload.get("eventId"),
                "packetMetaData": payload.get("packetMetaData"),
                "packetExecutionSummary": payload.get("packetExecutionSummary"),
                "flowMetaData": {"stage": flow_meta.get("stage")},
            }
            prompt = f"Kafka Payload: {json.dumps(projected_payload)}\n\n"
            if logs and logs != "Log fetching disabled.":
                prompt += f"Elasticsearch Logs: {logs}\n\n"
            prompt += f"Database Rule Configuration:\n{db_rule}\n\n"

        @llm_breaker
        @retry_transient
        def invoke_investigator():
            return investigator_agent.invoke({"messages": [
                SystemMessage(content=investigator_prompt),
                HumanMessage(content=prompt)
            ]})

        res = _counted("investigator", invoke_investigator)
        metrics.record_llm_usage("investigator", res)
        metrics.LLM_CALLS.labels(node="investigator", outcome="ok").inc()
        log.info("Investigator finished analysis")
        return {"investigation": res["messages"][-1].content, "db_rule": db_rule}

    def reviewer_node(state: GraphState):
        investigation = state.get("investigation", "")
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        log.info("Reviewer node started", state="REVIEWING")

        # Set the per-packet context the module-scope add_learning_rule tool
        # reads (see its definition above) instead of closing over these
        # values directly.
        _current_event_id.set(event_id)
        _current_investigation.set(investigation)

        prompt = f"Validate this investigation:\n{investigation}\n\nIf it's perfect, reply with exactly 'APPROVED'. If not, explain what is wrong."

        @llm_breaker
        @retry_transient
        def invoke_reviewer():
            return reviewer_agent.invoke({"messages": [
                SystemMessage(content=reviewer_prompt),
                HumanMessage(content=prompt)
            ]})

        res = _counted("reviewer", invoke_reviewer)
        metrics.record_llm_usage("reviewer", res)
        metrics.LLM_CALLS.labels(node="reviewer", outcome="ok").inc()
        feedback = res["messages"][-1].content
        log.info("Reviewer finished assessment")
        return {"reviewer_feedback": feedback, "retry_count": state.get("retry_count", 0) + 1}

    def check_approval(state: GraphState):
        feedback = state.get("reviewer_feedback", "")
        retry_count = state.get("retry_count", 0)
        max_retries = int(os.environ.get("MAX_INVESTIGATION_RETRIES", 3))
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id, retry_count=retry_count)

        if is_reviewer_approved(feedback):
            log.info("Reviewer APPROVED findings", transition="synthesis")
            return "synthesis"
        elif retry_count >= max_retries:
            log.warning("Maximum retries reached", max_retries=max_retries, transition="escalate", state="NEEDS_MANUAL_REVIEW")
            return "escalate"
        else:
            log.info("Reviewer REJECTED findings", transition="investigator", state="RETRYING")
            return "investigator"

    def escalate_node(state: GraphState):
        event_id = state.get("payload", {}).get("eventId", "unknown")
        logger.bind(event_id=event_id).info("Generating escalation casebook", state="ESCALATING")

        # Observed here as well as in synthesis_node. This node is reached
        # exactly when retry_count >= MAX_INVESTIGATION_RETRIES -- the packets
        # with the MOST Reviewer rejections -- and recording only the
        # successful path gave the histogram a hard ceiling at max_retries - 1
        # and hid the tail it exists to measure.
        #
        # `retry_count`, NOT `retry_count - 1`. The histogram counts Reviewer
        # REJECTIONS, and `retry_count` counts reviews. On the synthesis path
        # the final review approved, so rejections are one fewer than reviews.
        # On this path every review rejected, so they are equal.
        metrics.INVESTIGATOR_RETRIES.observe(max(0, state.get("retry_count", 0)))

        # We manually construct a fake synthesis payload that forces the routes.py to mark it NEEDS_MANUAL_REVIEW
        investigation = state.get("investigation", "")
        feedback = state.get("reviewer_feedback", "")

        # We format it to match the expected JSON structure so routes.py parses it
        escalation_result = {
            "rejection_description": f"ESCALATED: The automated agents could not agree on a resolution after multiple attempts.\nLast Investigation:\n{investigation}\n\nLast Reviewer Feedback:\n{feedback}",
            "synthesis": "ESCALATED TO HUMAN REVIEW. The system encountered a complex edge case and exceeded the maximum allowed retries for agentic resolution.",
            "action": "MANUAL_REVIEW",
            "resident_action": "PENDING"
        }
        # Dump to JSON so routes.py can parse it
        return {"synthesis": json.dumps(escalation_result)}

    def synthesis_node(state: GraphState):
        event_id = state.get("payload", {}).get("eventId", "unknown")
        log = logger.bind(event_id=event_id)
        log.info("Synthesis node started", state="SYNTHESIZING")
        investigation = state.get("investigation", "")
        prompt = f"Create the final JSON casebook based strictly on this approved investigation:\n{investigation}"

        @llm_breaker
        @retry_transient
        def invoke_synthesis():
            return synthesis_agent.invoke({"messages": [
                SystemMessage(content=synthesis_prompt),
                HumanMessage(content=prompt)
            ]})

        res = _counted("synthesis", invoke_synthesis)
        metrics.record_llm_usage("synthesis", res)
        metrics.LLM_CALLS.labels(node="synthesis", outcome="ok").inc()
        synthesis_content = res["messages"][-1].content

        # Validate against the declared contract, and repair once. A malformed
        # response used to fall back to {"rejection_description": <raw text>},
        # producing a casebook with action: null that looked identical to a
        # packet the agents genuinely could not classify (4.3).
        parsed, error = parse_synthesis(synthesis_content)
        if parsed is None:
            log.warning("Synthesis output failed validation; requesting a repair",
                        error=error)
            metrics.LLM_CALLS.labels(node="synthesis", outcome="invalid").inc()

            repair_prompt = (
                f"Your previous response was rejected: {error}\n\n"
                f"Previous response:\n{synthesis_content}\n\n"
                f"Return ONLY the corrected JSON object. `action` must be one "
                f"of {list(ACTIONS)} and `resident_action` one of "
                f"{list(RESIDENT_ACTIONS)}."
            )

            @llm_breaker
            @retry_transient
            def invoke_repair():
                return synthesis_agent.invoke({"messages": [
                    SystemMessage(content=synthesis_prompt),
                    HumanMessage(content=repair_prompt)
                ]})

            res = _counted("synthesis", invoke_repair)
            metrics.record_llm_usage("synthesis", res)
            synthesis_content = res["messages"][-1].content
            parsed, error = parse_synthesis(synthesis_content)

        if parsed is None:
            # Two failures. Escalating is the only honest outcome: we have no
            # validated action, and inventing one would be worse than saying so.
            log.error("Synthesis output invalid after repair; escalating",
                      error=error)
            metrics.LLM_CALLS.labels(node="synthesis", outcome="unrepairable").inc()
            synthesis_content = json.dumps({
                "rejection_description": (
                    "ESCALATED: the Synthesis agent did not produce a valid "
                    f"resolution after a repair attempt. Last error: {error}"
                ),
                "synthesis": "ESCALATED TO HUMAN REVIEW (invalid agent output).",
                "action": "MANUAL_REVIEW",
                "resident_action": "PENDING",
            })
        else:
            parsed, abstained, reason = apply_confidence_policy(
                parsed, logs=state.get("logs", "")
            )
            if reason:
                log.warning("Confidence policy applied", detail=reason,
                            abstained=abstained)
            if abstained:
                metrics.LLM_CALLS.labels(node="synthesis", outcome="abstained").inc()
            synthesis_content = parsed.model_dump_json()

        log.info("Synthesis finished")
        # How many Reviewer rejections this packet needed. A rising
        # distribution is the earliest signal of prompt or model regression.
        #
        # Minus one because reaching synthesis means the LAST review approved:
        # `retry_count` counts reviews, and one of them was not a rejection.
        # escalate_node observes `retry_count` undecremented, for the mirror
        # reason -- see there.
        metrics.INVESTIGATOR_RETRIES.observe(max(0, state.get("retry_count", 1) - 1))
        
        # Shadow comparison. The verdict is RETURNED, not just logged.
        #
        # It used to exist only as a warning line. `outcomes.summarise` groups
        # by resolution_source, which for a shadowed packet is "agent", so
        # accuracy_report could only compare runbooks that were ALREADY being
        # served -- and a runbook cannot be cleared to serve until it has been
        # compared. That closed loop had no entry point, which made section 4.2's
        # "shadow, measure, then serve" sequence unimplementable as built (G18).
        shadow = None
        shadow_res = state.get("shadow_runbook_resolution")
        if shadow_res:
            runbook_id = state.get("runbook_id", "unknown")
            try:
                agent_res = json.loads(synthesis_content)
                rb_res = json.loads(shadow_res)
                agreed = agent_res.get("action") == rb_res.get("action")

                shadow = {
                    "runbook_id": runbook_id,
                    "action": rb_res.get("action"),
                    "resident_action": rb_res.get("resident_action"),
                    "synthesis": rb_res.get("synthesis"),
                    "agreed": agreed,
                }

                if not agreed:
                    metrics.SHADOW_DIVERGENCE.labels(runbook_id=runbook_id).inc()
                    log.warning(
                        "Shadow divergence",
                        runbook_id=runbook_id,
                        runbook_action=rb_res.get("action"),
                        agent_action=agent_res.get("action"),
                        runbook_synthesis=rb_res.get("synthesis"),
                        agent_synthesis=agent_res.get("synthesis")
                    )
            except Exception as e:
                log.error("Failed to compare the shadow runbook resolution", error=f"{type(e).__name__}: {e}")

        return {
            "synthesis": synthesis_content,
            "messages": res["messages"],
            "resolution_source": "agent",
            "shadow_comparison": shadow,
        }

    # Build Graph
    workflow = StateGraph(GraphState)
    workflow.add_node("fetch_logs", fetch_logs_node)
    workflow.add_node("runbook_lookup", runbook_lookup_node)
    workflow.add_node("filter_logs", filter_logs_node)
    workflow.add_node("investigate", investigator_node)
    workflow.add_node("review", reviewer_node)
    workflow.add_node("synthesize", synthesis_node)
    workflow.add_node("escalate", escalate_node)
    
    workflow.add_edge(START, "fetch_logs")
    workflow.add_edge("fetch_logs", "runbook_lookup")
    workflow.add_conditional_edges("runbook_lookup", check_runbook_hit, {"end": END, "filter": "filter_logs", "investigate": "investigate"})
    workflow.add_edge("filter_logs", "investigate")
    workflow.add_edge("investigate", "review")
    workflow.add_conditional_edges("review", check_approval, {"synthesis": "synthesize", "investigator": "investigate", "escalate": "escalate"})
    workflow.add_edge("synthesize", END)
    workflow.add_edge("escalate", END)
    
    # Backend is selectable so two API replicas can share checkpoints (4.7).
    _agent = workflow.compile(checkpointer=get_checkpointer())
    
    logger.info("Agent graph constructed")
    return _agent
