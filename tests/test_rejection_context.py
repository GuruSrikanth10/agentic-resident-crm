"""
The direct rejection lane's prompt builders (REASON_CODE_DOCS_PLAN.md Phase 3).

These are pure functions, so everything about a prompt's shape is testable
here without a graph, an LLM or a checkpoint. What the tests are protecting:

* **Order.** The documentation and the rule say what the policy is and come
  first; the logs are the longest section and come last, with the task
  restated after them.
* **The size limit trims logs and nothing else.** A cap that could silently
  drop the documentation or the investigation would remove the argument the
  evidence is being kept for.
* **A missing evidence slot is stated, not left empty.** An empty section is
  a gap the model fills by inference, and what it infers is a confident
  packet-specific claim with nothing behind it.
"""
import json

import pytest

from src.core import rejection_context as ctx
from src.models.synthesis import classify_logs

PAYLOAD = {"eventId": "evt-1", "packetMetaData": {"enrolmentType": "E"}}


def _hit(text="THE DOCUMENT TEXT"):
    return {"outcome": "hit", "text": text, "sha256": "sha256:x",
            "refs": [], "resolution_guidance": [], "truncated": False}


def _investigation(**overrides):
    kwargs = {"doc_state": _hit(), "db_rule": "THE RULE",
              "enrolment_display": "New Enrolment (1:N deduplication)",
              "payload_projection": PAYLOAD, "logs": "THE LOGS"}
    kwargs.update(overrides)
    return ctx.build_investigation_prompt(**kwargs)


def _retry(**overrides):
    kwargs = {"previous_investigation": "THE PREVIOUS ANALYSIS",
              "feedback": "THE FEEDBACK", "doc_state": _hit(),
              "db_rule": "THE RULE", "enrolment_display": "New Enrolment",
              "logs": "THE LOGS"}
    kwargs.update(overrides)
    return ctx.build_retry_prompt(**kwargs)


def _review(**overrides):
    kwargs = {"investigation": "THE INVESTIGATION", "doc_state": _hit(),
              "db_rule": "THE RULE", "enrolment_display": "New Enrolment",
              "payload_projection": PAYLOAD, "logs": "THE LOGS"}
    kwargs.update(overrides)
    return ctx.build_review_prompt(**kwargs)


def _labels(prompt):
    return [line[4:] for line in prompt.splitlines() if line.startswith("### ")]


# ---------------------------------------------------------------------------
# Section order.
# ---------------------------------------------------------------------------

def test_the_investigation_prompt_puts_policy_first_and_logs_last():
    prompt, _ = _investigation()
    assert _labels(prompt) == [
        ctx.DOCUMENTATION, ctx.DATABASE_RULE, ctx.ENROLMENT_TYPE,
        ctx.KAFKA_PAYLOAD, ctx.LOGS, ctx.TASK]


def test_the_retry_prompt_leads_with_the_delta_and_drops_the_payload():
    """The payload is static and already reflected in the prior analysis; the
    logs are not, and a retry about citations needs them (G12)."""
    prompt, _ = _retry()
    assert _labels(prompt) == [
        ctx.PREVIOUS_ANALYSIS, ctx.REVIEWER_FEEDBACK, ctx.DOCUMENTATION,
        ctx.DATABASE_RULE, ctx.ENROLMENT_TYPE, ctx.LOGS, ctx.TASK]
    assert ctx.KAFKA_PAYLOAD not in prompt


def test_the_review_prompt_shows_the_evidence_before_the_claims():
    """Reading the claims first is how a reviewer ends up looking for support
    for them rather than checking them."""
    prompt, _ = _review()
    assert _labels(prompt) == [
        ctx.DOCUMENTATION, ctx.DATABASE_RULE, ctx.ENROLMENT_TYPE,
        ctx.KAFKA_PAYLOAD, ctx.LOGS, ctx.INVESTIGATION, ctx.TASK]


def test_sections_are_separated_by_exactly_one_blank_line():
    prompt, _ = _investigation()
    assert "\n\n### " in prompt
    assert "\n\n\n" not in prompt
    assert prompt.startswith(f"### {ctx.DOCUMENTATION}\nTHE DOCUMENT TEXT")


@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_every_prompt_carries_its_evidence_verbatim(build):
    prompt, trimmed = build()
    assert trimmed is False
    assert "THE DOCUMENT TEXT" in prompt
    assert "THE RULE" in prompt
    assert "THE LOGS" in prompt


def test_the_payload_is_sent_as_json():
    prompt, _ = _investigation()
    assert json.dumps(PAYLOAD) in prompt


# ---------------------------------------------------------------------------
# The documentation section: every outcome in the state shape.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("outcome", ["miss", "error", "no_reason_code"])
@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_a_lookup_that_found_nothing_says_so(build, outcome):
    prompt, _ = build(doc_state={"outcome": outcome, "text": None})
    assert ctx.NO_DOCUMENTATION in prompt
    assert ctx.DOCUMENTATION in _labels(prompt)


@pytest.mark.parametrize("doc_state", [{"outcome": "disabled"}, None, {}])
@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_the_section_is_left_out_when_the_feature_is_off(build, doc_state):
    """A "no documentation available" line with the feature switched off
    reads as a gap in the store rather than as a switch nobody turned on."""
    prompt, _ = build(doc_state=doc_state)
    assert ctx.DOCUMENTATION not in _labels(prompt)
    assert ctx.NO_DOCUMENTATION not in prompt


def test_a_hit_with_no_text_degrades_to_the_no_documentation_line():
    prompt, _ = _investigation(doc_state={"outcome": "hit", "text": None})
    assert ctx.NO_DOCUMENTATION in prompt


def test_the_task_names_the_documentation_only_when_it_is_present():
    with_docs, _ = _investigation()
    without, _ = _investigation(doc_state=None)

    assert "Apply the Reason Code Documentation and the Database Rule " \
           "Configuration to this packet" in with_docs
    assert "Apply the Database Rule Configuration to this packet" in without
    assert "Reason Code Documentation" not in without


# ---------------------------------------------------------------------------
# The logs section: every sentinel the fetch stage can store.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("logs", [None, "", "   ", "\n\t ",
                                  "Log fetching disabled."])
@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_an_absent_trace_is_stated_and_its_consequences_spelled_out(build, logs):
    prompt, _ = build(logs=logs)
    assert ctx.NO_LOGS in prompt
    assert "Log fetching disabled." not in prompt


def test_a_silent_trace_keeps_its_own_words():
    """"No logs found" is evidence: the source was asked and had nothing."""
    logs = "No logs found for ID: <refId>"
    prompt, _ = _investigation(logs=logs)
    assert logs in prompt
    assert ctx.NO_LOGS not in prompt


def test_a_gaps_banner_reaches_the_model_intact():
    from src.log_pipeline.sources.k8s.gaps import BANNER_HEADER

    logs = f"{BANNER_HEADER}\nLOG_ROTATION: ...\nNo logs found for ID: <refId>"
    prompt, _ = _investigation(logs=logs)
    assert BANNER_HEADER in prompt


@pytest.mark.parametrize("logs,expected", [
    (None, "unavailable"), ("", "unavailable"), ("  ", "unavailable"),
    ("Log fetching disabled.", "unavailable"),
    (" Log fetching disabled. ", "unavailable"),
    ("No logs found for ID: x", "silent"),
    ("a real trace line", "present"),
])
def test_classify_logs_covers_every_sentinel(logs, expected):
    assert classify_logs(logs) == expected


# ---------------------------------------------------------------------------
# The size limit.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    (None, 200000), ("", 200000), ("nonsense", 200000), ("0", 200000),
    ("-1", 200000), ("5000", 5000), (" 5000 ", 5000),
])
def test_prompt_max_chars_falls_back_on_an_unusable_value(monkeypatch, raw,
                                                          expected):
    if raw is None:
        monkeypatch.delenv("REJECTION_PROMPT_MAX_CHARS", raising=False)
    else:
        monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", raw)
    assert ctx.prompt_max_chars() == expected


def test_oversized_logs_are_trimmed_from_the_middle(monkeypatch):
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "4000")
    logs = "HEAD" + ("x" * 20000) + "TAIL"

    prompt, trimmed = _investigation(logs=logs)

    assert trimmed is True
    assert len(prompt) <= 4000
    assert "characters omitted from the middle of this trace" in prompt
    assert "HEAD" in prompt and "TAIL" in prompt


def test_nothing_but_the_logs_is_ever_trimmed(monkeypatch):
    """The cap must not be able to remove the documentation, the rule or the
    task -- they are what the model reasons with."""
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "4000")
    document = "D" * 1500
    prompt, trimmed = _investigation(doc_state=_hit(document), logs="x" * 50000)

    assert trimmed is True
    assert document in prompt
    assert "THE RULE" in prompt
    assert json.dumps(PAYLOAD) in prompt
    assert prompt.rstrip().endswith("do not invent packet-specific details.")


def test_a_prompt_with_no_room_left_drops_the_logs_and_says_so(monkeypatch):
    """A few hundred characters of head and tail is worse than an honest
    absence: it invites a citation from a window the model cannot see."""
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "1500")
    prompt, trimmed = _investigation(logs="x" * 50000)

    assert trimmed is True
    assert ctx.LOGS_OMITTED in prompt
    assert "xxxx" not in prompt


def test_the_other_sections_are_sent_in_full_even_past_the_cap(monkeypatch):
    """The cap is a budget for the logs, not a guillotine for the prompt: a
    truncated rule would make the investigation wrong rather than short."""
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "100")
    document, rule = "D" * 2000, "R" * 2000
    prompt, trimmed = _investigation(doc_state=_hit(document), db_rule=rule,
                                     logs="x" * 5000)

    assert trimmed is True
    assert document in prompt and rule in prompt
    assert len(prompt) > 100
    assert ctx.LOGS_OMITTED in prompt


def test_logs_that_fit_exactly_are_not_trimmed(monkeypatch):
    prompt, trimmed = _investigation(logs="x" * 100)
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", str(len(prompt)))

    again, trimmed = _investigation(logs="x" * 100)
    assert trimmed is False
    assert again == prompt


@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_every_builder_reports_its_own_trim(monkeypatch, build):
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "4000")
    _, untrimmed = build(logs="short")
    _, trimmed = build(logs="x" * 50000)
    assert untrimmed is False and trimmed is True


def test_the_trim_marker_counts_what_it_actually_dropped(monkeypatch):
    """The figure is a number the model reads, so it has to be the real one
    and not the shortfall against the budget -- the marker takes room too."""
    monkeypatch.setenv("REJECTION_PROMPT_MAX_CHARS", "4000")
    import re

    # A filler character that appears nowhere in the prompt's own prose, so
    # the count below is the kept trace and nothing else.
    logs = "Q" * 20000
    prompt, _ = _investigation(logs=logs)
    stated = int(re.search(r"\.\.\. (\d+) characters omitted", prompt).group(1))
    kept = prompt.count("Q")
    assert stated == len(logs) - kept


# ---------------------------------------------------------------------------
# Which rule description wins.
# ---------------------------------------------------------------------------
#
# The documentation is generated from the PRODUCTION rule base and from the
# service source; the Database Rule Configuration is read live from whichever
# rules database is configured, which may be a staging copy. On top of that,
# some reason codes are raised in the service source and will never have a
# database rule at all. A single fixed precedence was wrong for both cases, so
# the prompt states which one applies to this packet.

def _rule_hit(kind, refs=1):
    return {"outcome": "hit", "text": "DOC TEXT", "sha256": "sha256:x",
            "refs": [{"source": "services/s.json", "kind": kind, "ref": "r"}] * refs,
            "resolution_guidance": [], "truncated": False}


@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_a_documented_rule_outranks_a_possibly_stale_database(build):
    prompt, _ = build(doc_state=_rule_hit("rule"), db_rule="THE RULE")
    assert ctx.RULE_NOTE_DOC_FROM_RULES in prompt
    assert "prefer the documentation" in prompt


@pytest.mark.parametrize("build", [_investigation, _retry, _review])
def test_a_source_raised_code_expects_no_database_rule(build):
    """These codes are thrown in the Java source, not by the rule engine. A
    database miss is the expected result and must not read as missing
    evidence."""
    prompt, _ = build(doc_state=_rule_hit("code"),
                      db_rule="Rule not found for reason code: X in live DB.")
    assert ctx.RULE_NOTE_DOC_FROM_CODE in prompt
    assert "not expected to hold a rule" in prompt


def test_a_mixed_document_uses_the_rule_note():
    """A rule entry makes a claim about the rule base and so is the one that
    can disagree with the database; a code entry cannot."""
    state = _rule_hit("code")
    state["refs"].append({"source": "services/s.json", "kind": "rule", "ref": "r2"})
    prompt, _ = _investigation(doc_state=state)
    assert ctx.RULE_NOTE_DOC_FROM_RULES in prompt


def test_an_undocumented_code_with_a_database_rule_trusts_the_database():
    """The signal that the store needs regenerating: production has a rule the
    documentation has never seen."""
    prompt, _ = _investigation(doc_state={"outcome": "miss"}, db_rule="THE RULE")
    assert ctx.RULE_NOTE_RULE_ONLY in prompt
    assert "added to production after the documentation was generated" in prompt


@pytest.mark.parametrize("db_rule", [
    "", "[]", "   ",
    "Rule not found for reason code: X in mock DB (Searched column: y).",
    "Rule lookup failed for reason code X: timeout",
])
def test_with_neither_source_the_model_is_told_to_invent_nothing(db_rule):
    prompt, _ = _investigation(doc_state={"outcome": "miss"}, db_rule=db_rule)
    assert ctx.RULE_NOTE_NEITHER in prompt
    assert "do not invent a rule" in prompt


@pytest.mark.parametrize("doc_state", [None, {"outcome": "disabled"}])
def test_no_precedence_note_when_the_documentation_is_switched_off(doc_state):
    """With nothing to weigh the rule against there is nothing to say, and a
    note would only add tokens."""
    prompt, _ = _investigation(doc_state=doc_state, db_rule="THE RULE")
    assert "Provenance:" not in prompt


@pytest.mark.parametrize("db_rule,present", [
    ("[{\"rule_id\": 1}]", True),
    ("some rule text", True),
    ("", False), (None, False), ("   ", False), ("[]", False),
    ("Rule not found for reason code: X in live DB.", False),
    ("Rule lookup failed for reason code X: boom", False),
])
def test_rule_is_present_recognises_the_lookup_s_miss_messages(db_rule, present):
    """Both misses arrive as prose in the same slot a real rule would, so
    emptiness is not the test."""
    assert ctx.rule_is_present(db_rule) is present


def test_the_note_never_displaces_the_rule_itself():
    prompt, _ = _investigation(doc_state=_rule_hit("rule"), db_rule="THE RULE")
    assert "THE RULE" in prompt


def test_the_review_task_asks_for_a_verdict_on_the_first_line():
    """`is_reviewer_approved` reads the first line; the Reviewer began writing
    prose first once it was given the full evidence, and every such reply
    counted as a rejection."""
    prompt, _ = _review()
    assert "on the first line" in prompt
    assert "nothing before it" in prompt
