"""
Phase 10 of KUBERNETES_LOGS_PLAN.md -- prompt guidance for evidence gaps.

Prompt content is mostly unverifiable by machine, but one property is not:
the prompts tell the agents to look for a specific banner, and the code emits
one. If those two strings drift apart, the agents are watching for a marker
that never appears and every incomplete trace is silently treated as complete
-- the exact failure this project exists to prevent.
"""
from pathlib import Path

import pytest

from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER
from src.log_pipeline.types import GapType

PROMPTS = Path(__file__).resolve().parent.parent / "src" / "prompts"
INVESTIGATOR = (PROMPTS / "InvestigatorAgent.md").read_text(encoding="utf-8")
REVIEWER = (PROMPTS / "ReviewerAgent.md").read_text(encoding="utf-8")


@pytest.mark.parametrize("prompt,name", [
    (INVESTIGATOR, "InvestigatorAgent.md"),
    (REVIEWER, "ReviewerAgent.md"),
])
def test_prompt_mentions_evidence_gaps(prompt, name):
    assert "EVIDENCE GAPS" in prompt, f"{name} has no gap guidance"


def test_investigator_quotes_the_exact_banner_the_code_emits():
    """The literal string must match, not merely resemble."""
    assert BANNER_HEADER in INVESTIGATOR, (
        "InvestigatorAgent.md does not contain the banner emitted by "
        "gaps.render_banner(); the agent would never recognise it."
    )


@pytest.mark.parametrize("gap_type", list(GapType))
def test_every_gap_type_is_explained_to_the_investigator(gap_type):
    """A gap the agent cannot interpret is a gap it will ignore."""
    assert gap_type.value in INVESTIGATOR, (
        f"{gap_type.value} can be emitted but is not explained in "
        "InvestigatorAgent.md"
    )


def test_investigator_is_told_absence_is_not_evidence_of_absence():
    lowered = INVESTIGATOR.lower()
    assert "absence of evidence" in lowered
    assert "evidence of absence" in lowered


def test_reviewer_is_told_to_reject_unqualified_conclusions():
    lowered = REVIEWER.lower()
    assert "reject" in lowered
    assert "absence of evidence" in lowered


def test_reviewer_permits_an_honest_non_answer():
    """Otherwise the Reviewer would reject correct 'insufficient evidence'
    findings and push the Investigator toward fabricating a cause."""
    lowered = REVIEWER.lower()
    assert "insufficient" in lowered or "non-answer" in lowered
    assert "approvable" in lowered or "valid" in lowered


def test_level_parse_degraded_carries_its_specific_warning():
    """This one is subtle enough to need spelling out: with levels unparsed,
    the absence of ERROR lines carries no information at all."""
    assert "LEVEL_PARSE_DEGRADED" in INVESTIGATOR
    assert "no information" in INVESTIGATOR.lower() or \
           "tells you nothing" in INVESTIGATOR.lower()


def test_investigator_no_longer_claims_elasticsearch_is_the_only_source():
    """Logs may now come from Kubernetes; the prompt should not hardcode ES."""
    assert "IF logs are provided" in INVESTIGATOR
