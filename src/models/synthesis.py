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


def apply_confidence_policy(result: SynthesisResult, logs: str = ""):
    """Cap confidence on an incomplete trace, then abstain if too low.

    Returns (result, abstained, reason). The result is a copy -- callers hold
    the original for the audit trail.
    """
    from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER

    confidence = result.confidence
    reason = None

    has_gaps = bool(logs) and BANNER_HEADER in logs
    if has_gaps and confidence is not None:
        ceiling = gap_confidence_ceiling()
        if confidence > ceiling:
            reason = (
                f"confidence lowered from {confidence} to {ceiling}: the trace "
                f"carries evidence gaps, so a higher confidence is unsupported."
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
