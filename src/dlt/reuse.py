"""Phase 7 of DLT_PLAN.md -- deciding whether a message needs the LLM.

The operative decision from DLT_PLAN.md 5.7: **never serve a cached
recommendation blind.** Every message gets its logs fetched and its
corroboration run; only the *LLM* is skipped.

That is what keeps the mis-cast detector live on every occurrence. Blind cache
reuse would disable it: a fingerprint whose usual cause is a genuine missing
record, but which occasionally wraps a timeout, would be mislabelled on exactly
the occurrences that matter most -- and those are the findings worth having.

Logs are cheap and already fetched in the fast stage. The LLM is the expensive
component, and it runs only on novel fingerprints and on discrepancies.

    LLM_REQUIRED   novel fingerprint, or a discrepancy worth explaining
    REUSE_GROUP    known fingerprint, corroborated, recommendation on file
    CANNED         Class B/C/U -- a fixed treatment, no model involved

Pure functions, no I/O.
"""
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.dlt.classify import FailureClass
from src.dlt.corroborate import Verdict
from src.dlt.groups import has_usable_recommendation
from src.utils.env import get_bool_env


class Decision(str, Enum):
    LLM_REQUIRED = "LLM_REQUIRED"
    REUSE_GROUP = "REUSE_GROUP"
    CANNED = "CANNED"


@dataclass(frozen=True)
class ReuseDecision:
    decision: Decision
    reason: str

    @property
    def calls_llm(self) -> bool:
        return self.decision is Decision.LLM_REQUIRED


def reuse_enabled() -> bool:
    return get_bool_env("DLT_REUSE_ENABLED", True)


def decide(failure_class: str,
           verdict: str,
           group: Optional[dict]) -> ReuseDecision:
    """Choose the treatment for one message.

    `failure_class` is the Phase 2 letter, `verdict` the Phase 6 corroboration
    verdict, `group` the Phase 7 record for this fingerprint (None if novel).
    """
    # Class B has no source to reason about, C has a fixed answer, U has
    # nothing parseable. None of them can produce a narrative worth paying
    # for, at any occurrence count.
    if failure_class != FailureClass.BUSINESS.value:
        return ReuseDecision(
            Decision.CANNED,
            f"class {failure_class} has a fixed treatment; no model involved")

    # A discrepancy is the finding. It overrides any cached answer, because
    # the cached answer is precisely what the logs are contradicting.
    if verdict in (Verdict.CONTRADICTED.value, Verdict.PARTIAL.value):
        return ReuseDecision(
            Decision.LLM_REQUIRED,
            f"corroboration came back {verdict}; the discrepancy leads the finding")

    if not reuse_enabled():
        return ReuseDecision(Decision.LLM_REQUIRED,
                             "reuse is disabled by DLT_REUSE_ENABLED")

    if not has_usable_recommendation(group):
        return ReuseDecision(
            Decision.LLM_REQUIRED,
            "novel fingerprint; no recommendation on file for this group")

    if verdict == Verdict.CORROBORATED.value:
        return ReuseDecision(
            Decision.REUSE_GROUP,
            "known fingerprint, logs corroborate the trace; serving the "
            "group's recommendation")

    # UNVERIFIABLE with a recommendation on file: serve it, but Phase 8 caps
    # the confidence via DLT_UNVERIFIED_CONFIDENCE_CEILING. Re-running the LLM
    # on the same evidence that produced the cached answer, minus the logs,
    # would cost tokens to reach a worse conclusion.
    return ReuseDecision(
        Decision.REUSE_GROUP,
        "known fingerprint but corroboration was unverifiable; serving the "
        "group's recommendation at a capped confidence")


def llm_calls_avoided(messages: int, groups: int) -> float:
    """Share of LLM calls the reuse policy avoids. Used by the operator CLI to
    report the measurement Phase 7's exit criteria asks for."""
    if messages <= 0:
        return 0.0
    return max(0.0, 1.0 - (groups / messages))
