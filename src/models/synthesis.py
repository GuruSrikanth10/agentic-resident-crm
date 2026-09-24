"""
The Synthesis output contract (ENHANCEMENT_PLAN.md 4.3, 4.4).

`SynthesisAgent.md` specifies exact enums for `action` and `resident_action`,
and nothing validated them: routes.py ran a regex, then json.loads, then fell
back to `{"rejection_description": str(...)}`. A malformed response therefore
produced a casebook with `action: null` that was indistinguishable, downstream,
from a packet the agents genuinely could not classify -- and an LLM returning
"REPLAY_PACKET" instead of "REPLAY" was accepted verbatim.

Parsing lives here, beside the schema, so the orchestrator (which repairs) and
routes.py (which persists) apply exactly the same rules.
"""
import json
import os
import re
from typing import Literal, Optional

from pydantic import BaseModel, Field, ValidationError

#: Operational actions. MANUAL_REVIEW is included because escalate_node emits
#: it directly when the Reviewer never approves.
ACTIONS = (
    "REPLAY",
    "WHITELISTING",
    "QC_REPLAY",
    "RO_APPROVAL",
    "RESIDENT_PACKET_RESUBMIT",
    "MANUAL_REVIEW",
)

#: What the resident must do. PENDING pairs with MANUAL_REVIEW.
RESIDENT_ACTIONS = (
    "NEW_PACKET",
    "NEW_PACKET_WITH_DIFFERENT_ARTIFACTS",
    "RO_APPLICATION",
    "PENDING",
)


class SynthesisResult(BaseModel):
    """The strict JSON contract the Synthesis agent must produce."""

    rejection_description: str = ""
    synthesis: str = ""
    action: Literal[ACTIONS]  # type: ignore[valid-type]
    resident_action: Literal[RESIDENT_ACTIONS]  # type: ignore[valid-type]

    #: How well the evidence supported this conclusion. Optional so a model
    #: that ignores the instruction still validates -- absent is honest,
    #: whereas defaulting to 1.0 would manufacture false certainty.
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)

    #: True when `apply_confidence_policy` overrode the model's own action and
    #: routed to manual review. Set by the policy, never by the LLM.
    #:
    #: It lives on the model so it survives `model_dump_json()` and reaches the
    #: casebook. Previously the policy returned it as a separate value that the
    #: orchestrator logged and dropped, so nothing downstream could tell an
    #: abstention apart from a genuine MANUAL_REVIEW verdict (G5).
    abstained: bool = False


def extract_json_block(text: str) -> Optional[str]:
    """Pull the JSON object out of an LLM response.

    Handles a fenced block, a bare object, or the whole string being JSON.
    """
    if not text:
        return None

    fenced = re.search(r"```(?:json)?\s*([\{\[].*?[\}\]])\s*```", text, re.DOTALL)
    if fenced:
        return fenced.group(1)

    braced = re.search(r"(\{.*\})", text, re.DOTALL)
    if braced:
        return braced.group(1)

    stripped = text.strip()
    return stripped if stripped.startswith(("{", "[")) else None


def parse_synthesis(text: str):
    """Return (SynthesisResult | None, error_message | None).

    Never raises: a malformed response is an expected outcome that the caller
    repairs or escalates, not an exception that DLQs the packet.
    """
    block = extract_json_block(text)
    if block is None:
        return None, "Response contained no JSON object."

    try:
        payload = json.loads(block)
    except json.JSONDecodeError as e:
        return None, f"Response was not valid JSON: {e}"

    if not isinstance(payload, dict):
        return None, f"Expected a JSON object, got {type(payload).__name__}."

    try:
        return SynthesisResult(**payload), None
    except ValidationError as e:
        return None, _describe(e)


def _describe(error: ValidationError) -> str:
    """A repair instruction the model can act on, not a stack trace."""
    parts = []
    for item in error.errors():
        field = ".".join(str(x) for x in item["loc"]) or "(root)"
        parts.append(f"field '{field}': {item['msg']}")
    return "; ".join(parts)


# ======================================================================
# 4.4 -- confidence and abstention
# ======================================================================

def confidence_threshold() -> float:
    """Below this, route to manual review instead of acting.

    Defaults to 0.0 (disabled). A confidence score nobody has checked is worse
    than none: enabling this before calibrating against recorded outcomes
    (4.1) would abstain on the wrong packets and look principled doing it.
    Turn it on once `accuracy_report` shows what a given confidence is worth.
    """
    try:
        return float(os.environ.get("SYNTHESIS_CONFIDENCE_THRESHOLD", "0"))
    except ValueError:
        return 0.0


def gap_confidence_ceiling() -> float:
    """Highest confidence permitted when the trace is known incomplete.

    This one is NOT a calibrated judgement, it is a hard safety property: the
    evidence-gap banner means we could not see part of the window, so a
    confident conclusion drawn from it is unsupported by construction.
    """
    try:
        return float(os.environ.get("SYNTHESIS_GAP_CONFIDENCE_CEILING", "0.6"))
    except ValueError:
        return 0.6


#: Ceilings for a resolution no log line corroborated. Only a trace WITH gaps
#: was capped, so a packet with no logs at all could score higher than one
#: with partial logs. The DB rule and the service documentation still say why
#: a packet was rejected, so these are not the gap ceiling; the defaults match
#: the DLT lane's DLT_LOGS_UNAVAILABLE_CEILING / DLT_LOGS_SILENT_CEILING, which
#: make the same split for the same reasons:
#:
#:   unavailable  we never looked -- fetching disabled, or the fetch failed.
#:                The rule stands alone and nothing contradicts it.
#:   silent       we looked and the log source had nothing for this packet.
#:                The logs could have spoken and did not, so it is lower.
DEFAULT_LOGS_UNAVAILABLE_CEILING = 0.75
DEFAULT_LOGS_SILENT_CEILING = 0.6

#: The sentinels the fetch stage stores in place of a trace:
#: `tool_registry.fetch_and_persist_logs` and `log_pipeline.pipeline`.
_LOGS_DISABLED = "Log fetching disabled."
_NO_LOGS_FOUND = "No logs found for ID:"


def _ceiling_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except ValueError:
        return default


def logs_unavailable_ceiling() -> float:
    return _ceiling_env("SYNTHESIS_LOGS_UNAVAILABLE_CEILING",
                        DEFAULT_LOGS_UNAVAILABLE_CEILING)


def logs_silent_ceiling() -> float:
    return _ceiling_env("SYNTHESIS_LOGS_SILENT_CEILING", DEFAULT_LOGS_SILENT_CEILING)


def classify_logs(logs: Optional[str]) -> str:
    """What the log slot in graph state actually holds.

    Three states, and the difference between the last two matters:

      unavailable  we never looked -- fetching disabled, or the fetch failed.
      silent       we looked and the log source had nothing for this packet.
      present      there is a trace, gaps banner or not.

    Public because the confidence policy is no longer the only reader: the
    rejection prompt builder needs the same three-way split, to tell the model
    that no logs are available rather than sending it an empty section or the
    bare sentinel `Log fetching disabled.` and hoping it infers the rest.
    Keeping one classifier means the prompt and the confidence ceiling can
    never disagree about what the evidence was.
    """
    text = (logs or "").strip()
    if not text or text == _LOGS_DISABLED:
        return "unavailable"
    if _NO_LOGS_FOUND in text:
        return "silent"
    return "present"


def _confidence_ceilings(logs: Optional[str]) -> list:
    """Every (ceiling, why) that the evidence behind a resolution imposes."""
    from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER

    text = (logs or "").strip()
    ceilings = []
    if BANNER_HEADER in text:
        ceilings.append((gap_confidence_ceiling(),
                         "the trace carries evidence gaps"))
    kind = classify_logs(logs)
    if kind == "unavailable":
        ceilings.append((logs_unavailable_ceiling(),
                         "no logs were fetched, so nothing corroborated the rule"))
    elif kind == "silent":
        ceilings.append((logs_silent_ceiling(),
                         "the log source had no lines for this packet, so "
                         "nothing corroborated the rule"))
    return ceilings


def apply_confidence_policy(result: SynthesisResult, logs: Optional[str] = ""):
    """Cap confidence on missing or incomplete logs, then abstain if too low.

    Returns (result, abstained, reason). The result is a copy -- callers hold
    the original for the audit trail.
    """
    confidence = result.confidence
    reason = None

    ceilings = _confidence_ceilings(logs)
    if ceilings and confidence is not None:
        ceiling, why = min(ceilings)
        if confidence > ceiling:
            reason = (
                f"confidence lowered from {confidence} to {ceiling}: {why}, "
                f"so a higher confidence is unsupported."
            )
            confidence = ceiling

    threshold = confidence_threshold()
    abstained = False
    if threshold > 0 and confidence is not None and confidence < threshold:
        abstained = True
        reason = (
            f"confidence {confidence} is below the {threshold} threshold; "
            f"routing to manual review instead of acting."
        )

    updated = result.model_copy(update={"confidence": confidence,
                                        "abstained": abstained})
    if abstained:
        updated = updated.model_copy(update={
            "action": "MANUAL_REVIEW",
            "resident_action": "PENDING",
            "synthesis": f"ESCALATED (low confidence). {updated.synthesis}",
        })

    return updated, abstained, reason
