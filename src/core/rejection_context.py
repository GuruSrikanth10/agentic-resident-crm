"""
The direct rejection lane's prompts, assembled in one place
(REASON_CODE_DOCS_PLAN.md Phase 3).

`investigator_node` and `reviewer_node` each built their own prompt inline, in
their own order, and the two had drifted apart: the Investigator was given the
rule, the payload and the logs, and the Reviewer was given the investigation
text alone -- while `ReviewerAgent.md` asked it to check that text against the
payload and the evidence gaps banner. A reviewer that cannot see the evidence
cannot check a citation, which is the thing it most often rejects for.

So the three prompts are built here, from the same sections, in a fixed order:

* the documentation and the rule first, because they say what the policy is;
* the payload and the logs after, because they say what this packet did;
* the task restated last, after the logs, because the logs are by far the
  longest section and an instruction ahead of 120000 characters of trace is an
  instruction the model has to reach back for.

Nothing here calls an LLM, touches the filesystem, or records a metric. It
reads one environment variable, `REJECTION_PROMPT_MAX_CHARS`, and returns
strings -- so the callers stay responsible for their own metrics and every
prompt shape is testable without a graph.
"""
import json
import os
from typing import Optional

from src.models.synthesis import classify_logs

#: Cap on the user message of one direct Investigator or Reviewer call.
#:
#: Roughly `(max_model_len - max_output_tokens - 4000) * 3`: the 4000 tokens
#: cover the system prompt with the policy appended (about 11000 characters),
#: and 3 characters per token is conservative for log text. For a
#: 131072-token window with 8192 output tokens that is about 357000, so this
#: default sits well below it. A normal worst case is about 160000 characters
#: -- 120000 of logs (LOG_MAX_REDUCED_CHARS), 16000 of documentation, plus the
#: rule, the payload, the investigation and the task -- so the trim below is
#: reached only in unusual cases. Lower it with the same formula for a model
#: with a smaller window.
DEFAULT_PROMPT_MAX_CHARS = 200000

#: Below this much room, trimming the logs to fit leaves a trace too
#: fragmentary to cite from, so they are replaced outright and the model is
#: told they were. A head and a tail of a few hundred characters each is worse
#: than an honest absence: it invites a citation from a window the model
#: cannot see the rest of.
MIN_LOG_ROOM = 2000

# ---------------------------------------------------------------------------
# Section labels. These are the names the prompt files already use, so the
# system prompt and the user message refer to the same things.
# ---------------------------------------------------------------------------
DOCUMENTATION = "Reason Code Documentation"
DATABASE_RULE = "Database Rule Configuration"
ENROLMENT_TYPE = "Enrolment Type"
KAFKA_PAYLOAD = "Kafka Payload"
LOGS = "Elasticsearch Logs"
PREVIOUS_ANALYSIS = "Your previous analysis"
REVIEWER_FEEDBACK = "Reviewer Feedback (You MUST fix your previous analysis)"
INVESTIGATION = "Investigation to validate"
TASK = "Task"

#: What the documentation section says when there is nothing to send. Said
#: plainly rather than omitted, so the model knows the gap is the store's and
#: does not go looking for a section that is not there.
NO_DOCUMENTATION = (
    "No documentation is available for this reason code. Reason from the "
    "Database Rule Configuration and the policy context.")

#: What the logs section says when there is no trace. The alternative -- an
#: empty section, or the bare `Log fetching disabled.` sentinel -- leaves the
#: model to infer both the fact and its consequences, and what it infers is
#: often a confident packet-specific claim with nothing behind it.
NO_LOGS = (
    "No logs are available for this packet (log fetching was disabled or the "
    "fetch failed). Do not cite log lines, and do not state packet-specific "
    "facts that only logs could show.")

#: Said when the logs would not fit at all. Distinct from NO_LOGS: the trace
#: exists, and knowing that it was dropped for size is different from knowing
#: it was never fetched.
LOGS_OMITTED = ("The logs were omitted: the rest of this prompt already fills "
                "REJECTION_PROMPT_MAX_CHARS.")

_TRIM_MARKER = ("\n\n... {} characters omitted from the middle of this trace "
                "(REJECTION_PROMPT_MAX_CHARS) ...\n\n")

_TASK_INVESTIGATION = (
    "Explain why this packet was rejected. Apply {sources} to this packet; "
    "take packet-specific facts from the logs and quote the exact lines you "
    "rely on. If no logs are available, say so plainly and do not invent "
    "packet-specific details.")
_TASK_RETRY = ("Revise your previous analysis to address the Reviewer "
               "Feedback, using the evidence above.")
_TASK_REVIEW = ("Validate the investigation above against this evidence. If it "
                "is correct, reply with exactly 'APPROVED'. If not, explain "
                "what is wrong.")


def prompt_max_chars() -> int:
    """The cap, read at call time. An unusable value falls back rather than
    raising -- a typo in a tunable must not fail a packet."""
    raw = os.environ.get("REJECTION_PROMPT_MAX_CHARS", "")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_PROMPT_MAX_CHARS
    return value if value > 0 else DEFAULT_PROMPT_MAX_CHARS


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def _documentation_body(doc_state) -> Optional[str]:
    """The documentation section, or None to leave it out entirely.

    `disabled` and a missing state both mean the feature is off, and a prompt
    with a documentation section that says "no documentation" reads as a gap
    in the store rather than as a switch nobody turned on. Every read uses
    `.get()`: the disabled state is stored as just `{"outcome": "disabled"}`,
    and a checkpoint written before this feature existed has no state at all.
    """
    if not doc_state:
        return None
    outcome = doc_state.get("outcome")
    if outcome == "disabled":
        return None
    if outcome == "hit":
        return doc_state.get("text") or NO_DOCUMENTATION
    return NO_DOCUMENTATION


def _logs_body(logs) -> str:
    """The logs section. A `silent` trace keeps its "No logs found" line and
    any gaps banner -- both are things the prompt files already explain, and
    both say something an empty section does not."""
    return NO_LOGS if classify_logs(logs) == "unavailable" else str(logs)


def _render(sections) -> str:
    """`### Label` then the body, one blank line between sections."""
    return "\n\n".join(f"### {label}\n{body}" for label, body in sections)


def _fit(sections, logs_index: int, logs_body: str):
    """Assemble, trimming only the logs, and say whether they were trimmed.

    The budget is spent on everything else first and the logs take what is
    left. That ordering is the point: the documentation, the rule and the
    investigation are what the model reasons *with*, and cutting any of them
    to make room for more trace would remove the argument to keep the
    evidence for it.
    """
    skeleton = _render([(label, "" if index == logs_index else body)
                        for index, (label, body) in enumerate(sections)])
    room = prompt_max_chars() - len(skeleton)

    if len(logs_body) <= room:
        final, trimmed = logs_body, False
    elif room >= MIN_LOG_ROOM:
        # Sized against the widest the count can be, so one pass is exact and
        # the result can never exceed `room`: a shorter count only shortens
        # the marker. Same shape as log_pipeline's `_bound_total_size` -- head
        # and tail kept, middle dropped, because the head says what the flow
        # attempted and the tail says how it ended.
        widest = _TRIM_MARKER.format(len(logs_body))
        keep = max(0, (room - len(widest)) // 2)
        marker = _TRIM_MARKER.format(len(logs_body) - 2 * keep)
        final = logs_body[:keep] + marker + logs_body[-keep:] if keep \
            else LOGS_OMITTED
        trimmed = True
    else:
        final, trimmed = LOGS_OMITTED, True

    resolved = list(sections)
    resolved[logs_index] = (resolved[logs_index][0], final)
    return _render(resolved), trimmed


def _with_documentation(sections, doc_body):
    """Prepend the documentation section when there is one to send."""
    return ([(DOCUMENTATION, doc_body)] + sections) if doc_body is not None \
        else sections


# ---------------------------------------------------------------------------
# The three prompts
# ---------------------------------------------------------------------------

def build_investigation_prompt(*, doc_state, db_rule, enrolment_display,
                               payload_projection, logs):
    """The Investigator's first pass. Returns (prompt, logs_were_trimmed)."""
    doc_body = _documentation_body(doc_state)
    sources = ("the Reason Code Documentation and the Database Rule "
               "Configuration") if doc_body is not None \
        else "the Database Rule Configuration"
    sections = _with_documentation([
        (DATABASE_RULE, db_rule or ""),
        (ENROLMENT_TYPE, enrolment_display or ""),
        (KAFKA_PAYLOAD, json.dumps(payload_projection)),
        (LOGS, ""),
        (TASK, _TASK_INVESTIGATION.format(sources=sources)),
    ], doc_body)
    return _fit(sections, _index_of(sections, LOGS), _logs_body(logs))


def build_retry_prompt(*, previous_investigation, feedback, doc_state, db_rule,
                       enrolment_display, logs):
    """The Investigator's retry. Returns (prompt, logs_were_trimmed).

    No payload, as on the retry path today: it is static and already reflected
    in the prior investigation. The logs are NOT dropped with it -- the
    Reviewer's most common rejection is that the findings are not grounded in
    the evidence, and a retry that asks for better citations with the
    citations removed cannot comply (G12).
    """
    sections = _with_documentation([
        (DATABASE_RULE, db_rule or ""),
        (ENROLMENT_TYPE, enrolment_display or ""),
        (LOGS, ""),
        (TASK, _TASK_RETRY),
    ], _documentation_body(doc_state))
    sections = [(PREVIOUS_ANALYSIS, previous_investigation or ""),
                (REVIEWER_FEEDBACK, feedback or "")] + sections
    return _fit(sections, _index_of(sections, LOGS), _logs_body(logs))


def build_review_prompt(*, investigation, doc_state, db_rule, enrolment_display,
                        payload_projection, logs):
    """The Reviewer, with the evidence the Investigator had.

    The investigation comes after the evidence, not before it: the Reviewer's
    job is to check the claims against the evidence, and reading the claims
    first is how a reviewer ends up looking for support for them instead.
    """
    sections = _with_documentation([
        (DATABASE_RULE, db_rule or ""),
        (ENROLMENT_TYPE, enrolment_display or ""),
        (KAFKA_PAYLOAD, json.dumps(payload_projection)),
        (LOGS, ""),
        (INVESTIGATION, investigation or ""),
        (TASK, _TASK_REVIEW),
    ], _documentation_body(doc_state))
    return _fit(sections, _index_of(sections, LOGS), _logs_body(logs))


def _index_of(sections, label) -> int:
    return next(index for index, (name, _) in enumerate(sections)
                if name == label)
