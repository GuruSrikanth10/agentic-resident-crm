"""Phase 8 of DLT_PLAN.md -- the DLT finding contract and its confidence ceilings.

Separate from `SynthesisResult`: the rejection contract's action vocabulary
(`REPLAY`, `QC_REPLAY`, ...) describes remediation, and this system does not
remediate. Its actions describe *routing* -- who should look at this, and what
they should do about it.

The ceilings encode what the available evidence can actually support. With no
source access and no database access, a Class B narrative would be invention
and a per-packet cause for a Class A code would be guesswork; the ceilings make
that structural, not a matter of prompt discipline.
"""
import os
from typing import Literal, Optional

from pydantic import BaseModel, Field

#: What should happen next. Routing, not remediation.
DLT_ACTIONS = (
    "NEEDS_MANUAL_REVIEW",
    "ROUTE_TO_DEV",
    "REDRIVE_AFTER_RECOVERY",
    "DATA_FIX_REQUIRED",
    "NO_ACTION",
)

DEFAULT_CLASS_B_CEILING = 0.3
DEFAULT_UNVERIFIED_CEILING = 0.5
DEFAULT_CONTRADICTED_CEILING = 0.6
DEFAULT_REGISTRY_MISS_CEILING = 0.5
DEFAULT_REUSE_DECAY = 0.95


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (ValueError, TypeError):
        return default


def class_b_ceiling() -> float:
    return _float_env("DLT_CLASS_B_CEILING", DEFAULT_CLASS_B_CEILING)


def unverified_ceiling() -> float:
    return _float_env("DLT_UNVERIFIED_CONFIDENCE_CEILING", DEFAULT_UNVERIFIED_CEILING)


def contradicted_ceiling() -> float:
    return _float_env("DLT_CONTRADICTED_CEILING", DEFAULT_CONTRADICTED_CEILING)


def registry_miss_ceiling() -> float:
    return _float_env("DLT_REGISTRY_MISS_CEILING", DEFAULT_REGISTRY_MISS_CEILING)


def reuse_decay() -> float:
    return _float_env("DLT_REUSE_DECAY", DEFAULT_REUSE_DECAY)


class DltFinding(BaseModel):
    """The strict JSON contract the DLT synthesis step must produce."""

    #: What the evidence shows. One or two paragraphs, plain language.
    narrative: str = ""

    #: Populated only when corroboration came back CONTRADICTED or PARTIAL.
    #: This is the highest-value field in the whole system -- it is the thing
    #: a developer reading the trace in Kafka UI cannot see.
    discrepancy: Optional[str] = None

    #: What a human should do. Per-code for Class A, not per-packet.
    recommendation: str = ""

    action: Literal[DLT_ACTIONS]  # type: ignore[valid-type]

    #: Optional, for the same reason as `SynthesisResult.confidence`: absent is
    #: honest, whereas defaulting to 1.0 manufactures false certainty.
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)

    #: Names every ceiling that was applied, so a capped score is auditable
    #: rather than mysteriously low.
    ceilings_applied: list = Field(default_factory=list)

    abstained: bool = False


def apply_dlt_confidence_policy(finding: DltFinding,
                                failure_class: str,
                                corroboration: str,
                                registry_hit: bool,
                                reused: bool = False,
                                logs: str = "") -> DltFinding:
    """Cap a finding's confidence at what its evidence supports.

    Ceilings compose by taking the minimum. `ceilings_applied` names every
    ceiling this case triggered -- not only the tightest -- so the record
    answers "why is this untrusted?" rather than "which single rule produced
    the number?". Reuses the rejection pipeline's evidence-gap ceiling
    unchanged, so a DLT case built on a gapped trace is capped exactly as a
    rejection would be.
    """
    from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER
    from src.models.synthesis import gap_confidence_ceiling

    applied = []
    ceiling = 1.0

    def cap(value: float, label: str):
        """Lower the effective ceiling, and record that this one applied.

        `applied` deliberately names every ceiling the case TRIGGERED, not
        only the single tightest one. Recording just the tightest would make
        the list order-dependent -- Class B (0.3) is evaluated before
        UNVERIFIABLE (0.5), so an unverifiable Class B case would silently
        stop reporting that it was unverifiable at all -- and a reader
        auditing a capped confidence wants every reason it is untrusted, not
        whichever reason happened to be checked first.
        """
        nonlocal ceiling
        ceiling = min(ceiling, value)
        applied.append(label)

    if failure_class in ("B", "U"):
        # No source access, so any narrative about *why* would be invention.
        cap(class_b_ceiling(), f"class_{failure_class.lower()}")

    if corroboration == "UNVERIFIABLE":
        cap(unverified_ceiling(), "unverifiable")
    elif corroboration == "CONTRADICTED":
        # We know the trace is wrong. We do not know what is right.
        cap(contradicted_ceiling(), "contradicted")

    if failure_class == "A" and not registry_hit:
        cap(registry_miss_ceiling(), "registry_miss")

    if logs and BANNER_HEADER in logs:
        cap(gap_confidence_ceiling(), "evidence_gap")

    confidence = finding.confidence
    if confidence is not None:
        if reused:
            confidence *= reuse_decay()
            applied.append("reuse_decay")
        confidence = min(confidence, ceiling)

    updated = finding.model_copy(update={
        "confidence": confidence,
        "ceilings_applied": applied,
    })

    # Class B and U are routed, never diagnosed -- the action is forced
    # regardless of what the model proposed.
    if failure_class in ("B", "U") and updated.action not in (
            "NEEDS_MANUAL_REVIEW", "ROUTE_TO_DEV"):
        updated = updated.model_copy(update={"action": "NEEDS_MANUAL_REVIEW"})

    return updated
