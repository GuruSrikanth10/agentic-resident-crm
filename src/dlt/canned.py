"""Phase 8 of DLT_PLAN.md -- fixed treatments for Class B, C and U.

No LLM is involved. Class B has no source to reason about, Class C has one
answer, and Class U has nothing parseable -- none of them can produce a
narrative worth paying tokens for.

**What this adds over Kafka UI is aggregation, not diagnosis.** "This is
occurrence 47 of this fingerprint, first seen five days ago, here are the
affected refIds" is information a developer cannot get from the topic, and it
is what makes a Class B case worth opening at all. The diagnosis itself stays
with the developer, and the finding says so plainly rather than guessing.
"""
from datetime import datetime, timezone
from typing import Optional

from src.models.dlt_synthesis import DltFinding


def _seen_summary(group: Optional[dict]) -> str:
    if not group or not group.get("occurrence_count"):
        return "This is the first recorded occurrence of this failure signature."

    count = group["occurrence_count"]
    first = group.get("first_seen")
    when = ""
    if first:
        when = (" first seen "
                + datetime.fromtimestamp(first, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    plural = "occurrence" if count == 1 else "occurrences"
    return f"{count} {plural} of this signature recorded{when}."


def _catalog_note(failure: dict) -> str:
    """Explain a reclassification the exception type alone would contradict.

    A reader looking at `BusinessException[KAFKA_PRODUCER_EXCEPTION]` filed as a
    technical fault will reasonably ask why, since the exception says
    "business". The answer is that the organisation's own reject-code source
    declares the code technical -- so say it, name where it came from, and let
    the reader disagree if the catalog is wrong.
    """
    code = failure.get("business_code")
    category = failure.get("registry_category")
    if not code or category != "TECHNICAL_EXCEPTION":
        return ""

    origin = failure.get("registry_category_source")
    provenance = {
        "declared": "declared as such in the BusinessReasonCode source",
        "inferred": ("inferred from its numeric id range -- weaker evidence "
                     "than a declared category, so treat this classification "
                     "as provisional"),
    }.get(origin, "categorised by the reason-code catalog")

    description = failure.get("registry_description")
    detail = f' ("{description}")' if description else ""

    return (f"\n\nThis was raised as a business exception, but the reason code "
            f"{code}{detail} is a technical fault: {provenance}. It is treated "
            f"as Class C for that reason, which is why no per-packet "
            f"investigation was run.")


def build(failure_class: str,
          failure: dict,
          corroboration,
          group: Optional[dict] = None) -> DltFinding:
    """Build the fixed finding for a non-Class-A failure."""
    signature = failure.get("signature") or failure.get("root_fqcn") or "unknown failure"
    seen = _seen_summary(group)

    if failure_class == "C":
        narrative = (
            f"Technical fault: {signature}. The root exception is a transient "
            f"or infrastructure-level failure, not an application defect. "
            f"Spring exhausted its retries before the dependency recovered. {seen}"
            f"{_catalog_note(failure)}"
        )
        recommendation = (
            "Confirm the dependency named in the trace is healthy, then redrive "
            "this record from the dead-letter topic. No code or data change is "
            "expected."
        )
        action = "REDRIVE_AFTER_RECOVERY"

    elif failure_class == "B":
        narrative = (
            f"Application defect: {signature}. The root exception indicates a "
            f"bug in service code rather than a business rule or a transient "
            f"fault. {seen}\n\n"
            "No diagnosis is offered: this system has no access to the source of "
            "the failing service, so any explanation of *why* the code failed "
            "would be speculation. The stack trace, the normalised frames and "
            "the affected records are attached for the development team."
        )
        recommendation = (
            "Route to the team owning the service in the trace. The frames below "
            "name the failure site; the occurrence count indicates how widely it "
            "is firing."
        )
        action = "ROUTE_TO_DEV"

    else:  # U
        narrative = (
            f"Unclassified failure: {signature}. "
            f"{failure.get('class_reason', '')} {seen}\n\n"
            "The root exception is not in the known classification map, so no "
            "treatment can be assigned automatically. The verbatim stack trace "
            "is attached."
        )
        recommendation = (
            "A human should read the attached trace and, once its treatment is "
            "known, add the root exception to DLT_CLASS_MAP so future "
            "occurrences classify automatically."
        )
        action = "NEEDS_MANUAL_REVIEW"

    discrepancy = None
    if corroboration is not None and getattr(corroboration, "is_discrepancy", False):
        discrepancy = (
            f"The logs do not support the declared exception: {corroboration.reason}. "
            "Treat the declared root cause with suspicion."
        )

    return DltFinding(
        narrative=narrative,
        discrepancy=discrepancy,
        recommendation=recommendation,
        action=action,
        # No model produced this, so there is no model confidence to report.
        # The ceilings in apply_dlt_confidence_policy still bound it.
        confidence=None,
    )
