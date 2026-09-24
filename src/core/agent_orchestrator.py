import os
import json
import contextvars
import threading
import time
from typing import TypedDict
from langgraph.graph import StateGraph, START, END
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from src.utils import metrics, reason_code_docs
from src.core import rejection_context
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
from src.utils.env import get_bool_env
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
    put its verdict on the FIRST line, so that line -- stripped of markdown and
    whitespace -- must start with the token.

    Only the first non-empty line is read, and that matters more than it
    looks. A react agent's final message can carry a leading blank line or a
    fenced wrapper, and once the Reviewer was given the full evidence
    (REJECTION_REVIEWER_EVIDENCE) it began writing its reasoning out at
    length. Scanning the whole string for a leading token would then fail on
    any reply that opened with a newline, and reading further than the first
    line would let a discussion of what "approved" would require count as an
    approval. One line, at the top, is the contract ReviewerAgent.md states.
    """
    for line in (feedback or "").splitlines():
        candidate = line.strip().strip("*_`\"' \t\r")
        if candidate:
            return candidate.upper().startswith("APPROVED")
    return False


#: How each payload `enrolmentType` is described to the Investigator. One map
#: for both Investigator paths: the harness and direct paths each carried
#: their own, and they disagreed -- the harness path had no "Z", the direct
#: path had no "E" (the code the runbooks are keyed on), and they described
#: "U" differently. The descriptions follow InvestigatorAgent.md and
#: agent_policy_context.md: an update is checked 1:N against other residents
#: as well as 1:1 against its own parent.
ENROLMENT_TYPE_DISPLAY = {
    "N": "New Enrolment (1:N deduplication)",
    "E": "New Enrolment (1:N deduplication)",
    "U": "Biometric Update (1:N deduplication and 1:1 authentication and append)",
    "Z": "Reactivation (1:N deduplication and 1:1 authentication and append)",
}


def enrolment_type_display(payload: dict) -> str:
    raw = (payload.get("packetMetaData") or {}).get("enrolmentType", "")
    return ENROLMENT_TYPE_DISPLAY.get(str(raw).strip().upper(), raw or "Unknown")


def _reason_code_of(payload: dict):
    """The packet's first non-empty `errorReasonCode`, or None.

    Tolerant of None at every level: a payload that carries no execution
    summary, a null `errorData`, or a null entry inside it is a packet with
    no reason code, not a packet that fails the node.
    """
    exec_summary = payload.get("packetExecutionSummary") or {}
    for error in exec_summary.get("errorData") or []:
        if error and error.get("errorReasonCode"):
            return error.get("errorReasonCode")
    return None


def _doc_match_label(doc_state: dict) -> str:
    """The `match` label for REASON_CODE_DOC_LOOKUPS.

    `exact` and `any` are counted apart because they say different things
    about the store: `any` means the code is documented but not for this
    packet's enrolment type, which is a narrower gap than a miss and a
    different piece of authoring work.
    """
    matched = doc_state.get("matched_type")
    if not matched:
        return "none"
    return "exact" if matched == doc_state.get("requested_type") else "any"


def _resolve_reason_code_doc(state: dict, payload: dict, log):
    """The documentation for this packet, looked up at most once (D9).

    Retries, the Reviewer and Synthesis all read the stored result, so a
    document edited mid-packet cannot produce an investigation reasoned from
    one version and a review reasoned from another. A checkpoint written
    before this feature existed carries no entry, so a retry resuming from one
    does the lookup itself.
    """
    doc_state = state.get("reason_code_doc")
    if doc_state is not None:
        return doc_state

    if not reason_code_docs.docs_enabled():
        return {"outcome": "disabled"}

    reason_code = _reason_code_of(payload)
    raw_type = (payload.get("packetMetaData") or {}).get("enrolmentType")
    try:
        doc_state = reason_code_docs.lookup(reason_code, raw_type)
    except Exception as e:
        # `lookup` never raises; this guards a bug in it, because a
        # documentation problem must not be able to fail a packet.
        log.warning("Reason-code document lookup raised",
                    error=f"{type(e).__name__}: {e}")
        doc_state = reason_code_docs.error_state(
            reason_code, f"{type(e).__name__}: {e}")

    metrics.REASON_CODE_DOC_LOOKUPS.labels(
        outcome=doc_state.get("outcome", "error"),
        match=_doc_match_label(doc_state)).inc()
    # The sha256, never the text: the text is large, identical for every
    # packet with this reason code, and the digest already says which version
    # the model was shown.
    log.info("Reason-code documentation resolved",
             outcome=doc_state.get("outcome"), reason_code=reason_code,
             requested_type=doc_state.get("requested_type"),
             matched_type=doc_state.get("matched_type"),
             doc_sha256=doc_state.get("sha256"),
             detail=doc_state.get("detail"))
    return doc_state


#: What Synthesis is told about the documentation's own recommendation. Its
#: own switch, off by default (D12), so the effect on `action` and
#: `resident_action` can be measured apart from everything else this feature
#: changes. The values are already validated against synthesis.ACTIONS and
#: RESIDENT_ACTIONS by the document validator, so the model is not being
#: offered an action the contract would then reject.
_SYNTHESIS_GUIDANCE_HEADER = (
    "\n\n### Resolution guidance from the reason code documentation\n")
_SYNTHESIS_GUIDANCE_FOOTER = (
    "\nUse this guidance to choose action and resident_action, unless the "
    "approved investigation shows this packet does not fit it. In that case "
    "follow the investigation and say why in the synthesis.\n")


def _synthesis_doc_guidance(doc_state) -> str:
    """The guidance block to append to the Synthesis prompt, or "".

    Silent unless the switch is on, the lookup hit, and the document actually
    carries guidance. A header with nothing under it would read as an empty
    recommendation rather than as an absent one.
    """
    if not get_bool_env("REJECTION_SYNTHESIS_DOC_GUIDANCE", False):
        return ""
    if not doc_state or doc_state.get("outcome") != "hit":
        return ""
    guidance = doc_state.get("resolution_guidance") or []
    if not guidance:
        return ""

    lines = []
    for item in guidance:
        line = (f"- action: {item.get('action')} | "
                f"resident_action: {item.get('resident_action')}")
        if item.get("when"):
            line += f" | when: {item['when']}"
        lines.append(line)
    return (_SYNTHESIS_GUIDANCE_HEADER + "\n".join(lines)
            + _SYNTHESIS_GUIDANCE_FOOTER)


def _project_payload(payload: dict) -> dict:
    """Only the payload fields the Investigator uses.

    The full nested Kafka message carries many fields (sourceTopic,
    callbackTopic, taskMetaData, rejectBits, resubmissionSummary,
    uidV2DataArray, ...) the Investigator never reads (2.3).
    """
    flow_meta = payload.get("flowMetaData") or {}
    return {
        "eventId": payload.get("eventId"),
        "packetMetaData": payload.get("packetMetaData"),
        "packetExecutionSummary": payload.get("packetExecutionSummary"),
        "flowMetaData": {"stage": flow_meta.get("stage")},
    }


def _write_harness_case_files(case_dir, payload: dict, logs: str, db_rule: str) -> None:
    """Write the evidence the harness Investigator and Reviewer read from disk.

    Written to the LOCAL filesystem directly -- not through the storage
    abstraction -- because the opencode agent can only read local files, and
    under CASEBOOK_STORAGE_BACKEND=s3 the storage layer writes to S3, not to
    disk. Both harness nodes call this, so the Reviewer never depends on the
    Investigator's pass having left the directory in place.
    """
    case_dir.mkdir(parents=True, exist_ok=True)
    if logs and logs != "Log fetching disabled.":
        (case_dir / "supported_logs.txt").write_text(logs, encoding="utf-8")
    with open(case_dir / "context.json", "w", encoding="utf-8") as f:
        json.dump({
            "payload": _project_payload(payload),
            "enrolment_type": enrolment_type_display(payload),
            "db_rule": db_rule,
        }, f, indent=2, ensure_ascii=False)


class GraphState(TypedDict):
    payload: dict
    logs: str
    #: Which stored artifact holds the text in `logs`, as a name relative to
    #: the casebook root. The casebook records this rather than the text, so
    #: the evidence is referenced where it already sits instead of being
    #: written a second time under a third name (`supported_logs.txt`, which
    #: was a byte-for-byte duplicate nothing read back).
    logs_artifact: str
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
    #: The reason-code documentation this packet was reasoned from, in the
    #: shape `utils/reason_code_docs.lookup` returns. Looked up once, on the
    #: Investigator's first pass, and reused by every retry, by the Reviewer
    #: and by Synthesis: one packet must never be reasoned about with two
    #: versions of a document. LangGraph only carries keys declared here.
    reason_code_doc: dict
    #: Which path actually produced each half of the investigation --
    #: `harness` or `direct`. A harness task that fails falls back to the
    #: direct LLM silently, so without this a comparison between the two
    #: paths would be scoring runs that were not on the path they claim.
    investigator_path: str
    reviewer_path: str

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
    "DltInvestigatorAgent.md",
    "DltReviewerAgent.md",
    "DltSynthesisAgent.md",
    "harness/RejectionInvestigator.md",
    "harness/RejectionReviewer.md",
    "harness/DltInvestigator.md",
    "harness/DltReviewer.md",
    # Inlined into the harness templates by `{{> rules/...}}`, so an edit to
    # either is a prompt change and has to move the fingerprint too.
    "harness/rules/rejection.md",
    "harness/rules/dlt.md",
)


def compute_prompt_fingerprint(base_dir: str) -> str:
    """SHA256 over the agent system prompts, harness templates and rules,
    the policy, and AGENTS.md.

    Sorted and length-prefixed so the digest cannot be changed by reordering
    or by content shifting across a boundary.
    """
    import hashlib

    digest = hashlib.sha256()
    paths = [os.path.join(base_dir, "prompts", name) for name in PROMPT_FILES]
    paths.append(os.path.join(os.path.dirname(base_dir), "agent_policy_context.md"))
    # opencode loads the root AGENTS.md into every harness session.
    paths.append(os.path.join(os.path.dirname(base_dir), "AGENTS.md"))

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
            return {"logs": cached, "logs_artifact": "fetched_logs.txt"}

        log.info("No persisted logs found; fetching live", state="LOG_FETCHER")
        logs = fetch_and_persist_logs(event_id, payload)
        log.info("Logs retrieved")
        # Both branches name the same artifact: `fetch_and_persist_logs` writes
        # `fetched_logs.txt` on the live path, which is the object the cache
        # branch above just read.
        return {"logs": logs, "logs_artifact": "fetched_logs.txt"}

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

    def queue_learning_rule(rule_text: str, reasoning: str) -> str:
        """Validate a proposed rule and queue it for human review.

        Shared by the direct Reviewer (through the `add_learning_rule` tool)
        and the harness Reviewer (through its JSON output), so a rule reaches
        pending_rules.jsonl by one validated path whichever one proposed it.
        """
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

    @tool
    def add_learning_rule(rule_text: str, reasoning: str) -> str:
        """Propose a new permanent rule to fix Investigator mistakes."""
        return queue_learning_rule(rule_text, reasoning)

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

        # On a failed save the pointer stays on `fetched_logs.txt`: that object
        # exists and holds a superset of this text, which is a worse but
        # readable answer. Naming `filtered_logs.txt` here regardless would
        # leave the casebook pointing at a key that was never written.
        artifact = "fetched_logs.txt"
        try:
            get_casebook_storage().save_artifact(event_id, "filtered_logs.txt", filtered_logs)
            artifact = "filtered_logs.txt"
            log.info("Persisted filtered logs to local artifact for testing", artifact="filtered_logs.txt")
        except Exception as e:
            log.warning("Failed to persist filtered logs artifact", error=str(e))

        log.info("Log Filter node finished")
        return {"logs": filtered_logs, "logs_artifact": artifact}

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
            reason_code = _reason_code_of(payload)

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

        # Before the harness branch, so the harness path records which
        # document this packet would have been given and a fallback to the
        # direct LLM already has it in hand.
        doc_state = _resolve_reason_code_doc(state, payload, log)

        # Check if the rejection lane runs on the opencode harness. Per lane,
        # not global: the DLT lane can stay on opencode while this one moves
        # to the direct path.
        from src.utils.opencode_runner import lane_enabled
        use_harness = lane_enabled("rejection")

        if use_harness and not is_retry:
            # opencode harness path: the agent reads files from disk and
            # writes its output to a file. See _write_harness_case_files for
            # why the context goes to local disk, not CasebookStorage.
            from src.utils import opencode_runner, docs_loader

            # Wait for the corpus download to finish if it's still running.
            # The download happens in a background thread at API startup;
            # if a packet arrives before it completes, the agent would read
            # a partial corpus. 60s is generous for a corpus that's usually
            # already on disk from a previous run.
            if not docs_loader.corpus_available():
                log.info("Waiting for documentation corpus download to complete...")
                for _ in range(60):
                    if docs_loader.corpus_available():
                        break
                    time.sleep(1)
                if not docs_loader.corpus_available():
                    log.warning("Corpus not available; proceeding without docs")

            # Resolve the local casebook directory (independent of the
            # storage backend — always on disk).
            from src.utils.paths import LOCAL_CASESHEETS_DIR
            case_dir = LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"
            _write_harness_case_files(case_dir, payload, logs, db_rule)
            etype_display = enrolment_type_display(payload)

            output_path = str(case_dir / "investigation.json")

            from src.utils.prompt_loader import render as render_prompt
            harness_prompt = render_prompt(
                "RejectionInvestigator",
                event_id=event_id,
                etype_display=etype_display,
                output_path=output_path,
            )

            try:
                # No `timeout=` here: opencode_runner._task_timeout() is the
                # single reader of OPENCODE_TASK_TIMEOUT_SECONDS. This site used
                # to default to 120s while the other three defaulted to 300s, so
                # with the variable unset the heaviest task of the four -- the
                # one that reads the docs corpus from cold -- got the shortest
                # budget, timed out, and fell back to the direct LLM.
                result = opencode_runner.run_task_json(
                    prompt=harness_prompt,
                    output_path=output_path,
                    node="investigator",
                )
                investigation = result["result"].get("investigation", "")
                log.info("Investigator finished (opencode harness)",
                         elapsed=result.get("seconds"),
                         **(result.get("trace") or {}))
                # The harness is deliberately NOT given the documents (D13):
                # it explores the corpus itself, and leaving its prompt alone
                # keeps the two paths comparable. The state is carried anyway
                # so the Reviewer and the casebook see the same shape on both.
                return {"investigation": investigation, "db_rule": db_rule,
                        "reason_code_doc": doc_state,
                        "investigator_path": "harness"}
            except Exception as e:
                log.warning("opencode harness failed; falling back to direct LLM",
                            error=f"{type(e).__name__}: {e}")
                # Fall through to the direct LLM path below

        if reason_code_docs.docs_enabled():
            # Assembled in rejection_context, in a fixed order and under
            # REJECTION_PROMPT_MAX_CHARS. Both branches send the same
            # document: the retry reuses the one already in state.
            if is_retry:
                prompt, log_trimmed = rejection_context.build_retry_prompt(
                    previous_investigation=investigation, feedback=feedback,
                    doc_state=doc_state, db_rule=db_rule,
                    enrolment_display=enrolment_type_display(payload),
                    logs=logs)
            else:
                prompt, log_trimmed = rejection_context.build_investigation_prompt(
                    doc_state=doc_state, db_rule=db_rule,
                    enrolment_display=enrolment_type_display(payload),
                    payload_projection=_project_payload(payload), logs=logs)
            if log_trimmed:
                metrics.REJECTION_PROMPT_TRIMS.labels(node="investigator").inc()
                log.warning("Trimmed the logs to fit REJECTION_PROMPT_MAX_CHARS",
                            node="investigator")
        elif is_retry:
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
            prompt = f"Kafka Payload: {json.dumps(_project_payload(payload))}\n\n"

            # Enrolment type is the single most important framing fact for a
            # rejection: New Enrolment (N) follows 1:N dedup rules, Biometric
            # Update (U) follows 1:1 auth-and-append rules. Stating it
            # explicitly prevents the LLM from missing it inside the JSON.
            prompt += f"Enrolment Type: {enrolment_type_display(payload)}\n\n"

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
        return {"investigation": res["messages"][-1].content, "db_rule": db_rule,
                "reason_code_doc": doc_state, "investigator_path": "direct"}

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

        # Check if the rejection lane runs on the opencode harness. Per lane,
        # not global: the DLT lane can stay on opencode while this one moves
        # to the direct path.
        from src.utils.opencode_runner import lane_enabled
        use_harness = lane_enabled("rejection")

        if use_harness:
            from src.utils import opencode_runner
            from src.utils.paths import LOCAL_CASESHEETS_DIR

            case_dir = LOCAL_CASESHEETS_DIR / f"casebook_{event_id}"
            output_path = str(case_dir / "review.json")

            from src.utils.prompt_loader import render as render_prompt
            harness_prompt = render_prompt(
                "RejectionReviewer",
                event_id=event_id,
                output_path=output_path,
            )

            try:
                # Inside the try, and the evidence rewritten rather than
                # assumed: a case directory that is missing or unwritable is a
                # harness failure like any other and falls back to the direct
                # LLM, instead of raising out of the node and failing the
                # packet.
                _write_harness_case_files(case_dir, state.get("payload", {}),
                                          state.get("logs", ""),
                                          state.get("db_rule", ""))
                (case_dir / "investigation_text.txt").write_text(
                    investigation, encoding="utf-8")

                result = opencode_runner.run_task_json(
                    prompt=harness_prompt,
                    output_path=output_path,
                    node="reviewer",
                )
                verdict = result["result"].get("verdict", "REJECTED").upper()
                feedback = result["result"].get("feedback", "")
                if verdict == "APPROVED":
                    feedback = "APPROVED"
                else:
                    # The harness Reviewer has no tool to call, so it returns
                    # its proposed rule in the JSON instead of calling
                    # add_learning_rule; without this the self-learning loop
                    # received nothing while the harness was on.
                    rule = result["result"].get("learning_rule")
                    if isinstance(rule, dict) and rule.get("rule_text"):
                        outcome = queue_learning_rule(str(rule["rule_text"]),
                                                      str(rule.get("reasoning") or ""))
                        log.info("Harness Reviewer proposed a learning rule",
                                 queued=outcome.startswith("Successfully"))
                log.info("Reviewer finished (opencode harness)",
                         elapsed=result.get("seconds"), verdict=verdict,
                         **(result.get("trace") or {}))
                return {"reviewer_feedback": feedback,
                        "retry_count": state.get("retry_count", 0) + 1,
                        "reviewer_path": "harness"}
            except Exception as e:
                log.warning("opencode harness failed for reviewer; falling back to direct LLM",
                            error=f"{type(e).__name__}: {e}")
                # Fall through to the direct LLM path below

        # The Reviewer sees the evidence the Investigator had (D8, on by
        # default). It was previously given the investigation text alone,
        # while ReviewerAgent.md asked it to check that text against the
        # payload and the evidence-gaps banner -- so its most common
        # rejection, "this is not grounded in the logs", was one it had no
        # way to verify either direction. Setting the variable to false
        # restores the old prompt exactly.
        #
        # The builder leaves the documentation section out for a `disabled`
        # or missing state, so this needs no separate check on the docs
        # switch.
        if get_bool_env("REJECTION_REVIEWER_EVIDENCE", True):
            payload = state.get("payload", {})
            prompt, log_trimmed = rejection_context.build_review_prompt(
                investigation=investigation,
                doc_state=state.get("reason_code_doc"),
                db_rule=state.get("db_rule", ""),
                enrolment_display=enrolment_type_display(payload),
                payload_projection=_project_payload(payload),
                logs=state.get("logs"))
            if log_trimmed:
                metrics.REJECTION_PROMPT_TRIMS.labels(node="reviewer").inc()
                log.warning("Trimmed the logs to fit REJECTION_PROMPT_MAX_CHARS",
                            node="reviewer")
        else:
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
        # The verdict text, truncated. Without it a rejection loop is
        # undiagnosable from the logs alone -- "Reviewer REJECTED findings"
        # says nothing about what it objected to, and the only other copy is
        # inside an escalation casebook that only exists once the loop has
        # already burned every retry.
        log.info("Reviewer finished assessment",
                 approved=is_reviewer_approved(feedback),
                 verdict=(feedback or "").strip()[:600])
        return {"reviewer_feedback": feedback,
                "retry_count": state.get("retry_count", 0) + 1,
                "reviewer_path": "direct"}

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
            log.warning("Maximum retries reached", max_retries=max_retries,
                        transition="escalate", state="NEEDS_MANUAL_REVIEW",
                        verdict=(feedback or "").strip()[:600])
            return "escalate"
        else:
            log.info("Reviewer REJECTED findings", transition="investigator",
                     state="RETRYING", verdict=(feedback or "").strip()[:600])
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
        prompt += _synthesis_doc_guidance(state.get("reason_code_doc"))

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
