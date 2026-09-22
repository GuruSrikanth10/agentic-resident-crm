"""A mechanical check that a finding is safe to cache against a fingerprint.

An agent finding is stored as the group's recommendation and re-served,
verbatim, to every later packet with the same stack trace. The prompt says so
and forbids packet-specific detail. This module checks the output rather than
trusting the instruction, because a finding that slips through is not wrong
once -- it is wrong for every packet the group serves afterwards.

Two severities, because they fail differently:

  identifier  A UUID, or a run of 12+ digits (a UID is 12, an EID longer, a
              millisecond timestamp 13). Another packet's identifier served
              as though it described this one is actively misleading, so a
              finding carrying one is **withheld from the cache**: this packet
              still gets it, the next one re-investigates. The 12-digit floor
              is deliberate -- configuration values such as a 129700000 ms
              retry ceiling are per-code and legitimate.

  phrasing    "this packet"-style wording, or a source line number
              (`Foo.java:185`, "line 190"). Not a leak, but it reads wrongly
              when re-served and line numbers go stale on every release. The
              finding is cached and the violation counted, so the rate is
              visible without paying an LLM call per occurrence for it.

Set `DLT_PER_CODE_WITHHOLD=false` to count identifier violations without
withholding.
"""
import re
from dataclasses import dataclass, field

from src.utils.env import get_bool_env

IDENTIFIER = "identifier"
PHRASING = "phrasing"

_PATTERNS = (
    ("uuid", IDENTIFIER, re.compile(
        r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")),
    ("long_number", IDENTIFIER, re.compile(r"(?<![\w.])\d{12,}(?![\w.])")),
    ("this_packet", PHRASING, re.compile(
        r"\bthis (?:specific |particular )?(?:packet|request|applicant|enrolment|record)\b",
        re.IGNORECASE)),
    ("source_line", PHRASING, re.compile(
        r"\b\w+\.java:\d+|\(line \d+\)|\bline \d+\b|\blines \d+\s*[-–]\s*\d+\b",
        re.IGNORECASE)),
)


@dataclass(frozen=True)
class Violation:
    pattern: str
    severity: str
    field: str
    text: str


@dataclass
class Check:
    violations: list = field(default_factory=list)

    @property
    def withhold(self) -> bool:
        """Should this finding be kept out of the group cache?"""
        return (withhold_enabled()
                and any(v.severity == IDENTIFIER for v in self.violations))

    def as_dict(self) -> list:
        return [{"pattern": v.pattern, "severity": v.severity,
                 "field": v.field, "text": v.text} for v in self.violations]


def withhold_enabled() -> bool:
    return get_bool_env("DLT_PER_CODE_WITHHOLD", True)


def check(finding) -> Check:
    """Scan the cacheable text of a finding. Never raises."""
    result = Check()
    for field_name in ("narrative", "recommendation", "discrepancy"):
        text = getattr(finding, field_name, None) or ""
        for name, severity, pattern in _PATTERNS:
            for match in pattern.finditer(text):
                result.violations.append(Violation(name, severity, field_name,
                                                   match.group(0)))
    return result
