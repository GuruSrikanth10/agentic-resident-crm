import re

# UUID regex
UUID_REGEX = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)

# Basic ISO-8601 / Date literal regexes
ISO_TIMESTAMP_REGEX = re.compile(r"\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
DATE_REGEX = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

# Any digit run of 10 or more (SRN, EID, UID, Aadhaar, etc)
LONG_DIGIT_RUN_REGEX = re.compile(r"\d{10,}")

# Maximum length of a proposed learning rule. A rule is a sentence, not an
# essay: an unbounded string here is appended verbatim to InvestigatorAgent.md
# and becomes part of the system prompt for every future packet (G19).
MAX_RULE_LENGTH = 500

# Phrases that have no place in a rule and every place in a prompt-injection
# payload. Log content is influenced by upstream request data, the Reviewer
# derives its proposal from that content, and promote_rules.py appends the
# result to the system prompt -- so a crafted log line can reach the prompt of
# every future investigation if it survives one interactive approval (G19).
INJECTION_MARKERS = (
    "ignore previous",
    "ignore all previous",
    "disregard previous",
    "disregard all",
    "ignore the above",
    "system:",
    "assistant:",
    "</system",
    "<|im_start|",
    "<|im_end|",
    "new instructions",
    "override the",
    "you are now",
    "forget everything",
)


def validate_learning_rule(rule_text: str) -> list[str]:
    """Check a Reviewer-proposed rule before it is queued for promotion.

    Returns a list of violation messages; empty means acceptable.

    This is deliberately conservative. The cost of rejecting a good rule is
    that an operator re-words it. The cost of accepting a bad one is a
    permanent, privileged modification to the prompt driving every future
    investigation.
    """
    violations = []

    text = (rule_text or "").strip()
    if not text:
        violations.append("Rule is empty.")
        return violations

    if len(text) > MAX_RULE_LENGTH:
        violations.append(
            f"Rule is {len(text)} characters; the limit is {MAX_RULE_LENGTH}."
        )

    lowered = text.lower()
    for marker in INJECTION_MARKERS:
        if marker in lowered:
            violations.append(f"Contains instruction-shaped text: {marker!r}")

    if "```" in text:
        violations.append("Contains a fenced code block.")

    # A rule is guidance, not data. Packet-specific identifiers in a permanent
    # rule mean it was generalised from one case incorrectly.
    violations.extend(validate_generic_text(text, []))

    return violations


def validate_generic_text(text: str, source_values: list[str]) -> list[str]:
    """
    Check that text contains no packet-specific identifiers.
    Returns a list of violation messages. Empty list means validation passed.
    """
    violations = []
    
    if UUID_REGEX.search(text):
        violations.append("Contains a UUID.")
        
    if ISO_TIMESTAMP_REGEX.search(text) or DATE_REGEX.search(text):
        violations.append("Contains a timestamp or date literal.")
        
    if LONG_DIGIT_RUN_REGEX.search(text):
        violations.append("Contains a digit run of 10 or more (e.g. SRN, EID).")
        
    for val in source_values:
        if val and len(val) > 3 and val in text:  # Ignore very short strings that might coincidentally match
            violations.append(f"Contains source value: {val}")
            
    return violations
