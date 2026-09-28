#!/usr/bin/env python3
"""
Redaction audit: what personal data would survive redaction in a service's
logs (MULTI_SERVICE_PLAN.md Phase 6).

Runs every line of a sample of one service's logs through the same redaction
the log pipeline applies (`redaction.redact_text`), then counts what is left
of the known personal-data keys and patterns. The count must be zero before
the service is enabled or piloted.

The residual check is deliberately looser than the redaction. Redaction
rewrites the value of a JSON key (`"name": ...`, in any escaping); the audit
also counts the same keys written any other way it can recognise -- `name=`,
`'name': `, `name: ` -- because that is exactly the personal data redaction
would miss. A count on those means the service logs a form redaction does not
cover, and redaction must be extended (`K8S_REDACT_EXTRA_PATTERNS`, or
`REDACT_JSON_KEYS`) before its logs are read. A value that is empty, `null`,
or already a `[REDACTED:...]` placeholder is not counted.

Usage:
    python -m src.tools.redaction_audit --service enu-demographic samples/enu-demographic/
    python -m src.tools.redaction_audit --service enu-demographic raw_logs.txt --json

Every file under a directory is read, one log line per text line. Findings
name the file, the line and the key or pattern, never the value.

Exit status: 0 when nothing is left, 1 when anything is, 2 when there was
nothing to read.
"""
import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.log_pipeline import redaction  # noqa: E402

#: How many findings to list per key or pattern; the counts are always whole.
MAX_EXAMPLES = 5

#: A value that carries no personal data: an empty string, a redaction
#: placeholder, null or a boolean, or nothing at all before the next separator.
_EMPTY_VALUE = re.compile(
    r"""(?:(?P<quote>\\*["'])(?:(?P=quote)|\[REDACTED:)"""
    r"""|\[REDACTED:|(?:null|none|true|false)\b|[,;}\])]|$)""",
    re.IGNORECASE)


def _residual_key_pattern(keys: tuple[str, ...]) -> re.Pattern:
    """A known key in any common spelling of `key: value` / `key=value`."""
    alternatives = "|".join(re.escape(key) for key in sorted(keys, key=len, reverse=True))
    return re.compile(
        rf"""(?<![A-Za-z0-9_.-])\\*["']?(?P<key>{alternatives})\\*["']?\s*[:=]\s*""",
        re.IGNORECASE)


@dataclass
class AuditResult:
    service: str
    files: int = 0
    lines: int = 0
    #: Redactions applied, per label, as the pipeline would count them.
    redacted: dict = field(default_factory=dict)
    #: What is left after redaction, per key ("key:<name>") or pattern
    #: ("pattern:<label>").
    residual: dict = field(default_factory=dict)
    #: {finding: ["<file>:<line>", ...]}, at most MAX_EXAMPLES each.
    examples: dict = field(default_factory=dict)

    @property
    def residual_total(self) -> int:
        return sum(self.residual.values())

    def as_dict(self) -> dict:
        return {"service": self.service, "files": self.files, "lines": self.lines,
                "redacted": dict(sorted(self.redacted.items())),
                "residual": dict(sorted(self.residual.items())),
                "residual_total": self.residual_total,
                "examples": dict(sorted(self.examples.items()))}


def residual_findings(text: str, key_pattern: re.Pattern) -> list:
    """The findings left in already-redacted text: ("key:<name>" or
    "pattern:<label>") once per occurrence."""
    findings = []
    for match in key_pattern.finditer(text):
        if not _EMPTY_VALUE.match(text, match.end()):
            findings.append(f"key:{match.group('key').lower()}")
    for label, pattern in redaction.active_patterns():
        findings.extend(f"pattern:{label}" for _ in pattern.finditer(text))
    return findings


def audit_lines(result: AuditResult, lines, source: str,
                key_pattern: re.Pattern) -> None:
    for number, line in enumerate(lines, start=1):
        line = line.rstrip("\n")
        if not line:
            continue
        result.lines += 1
        redacted = redaction.redact_text(line)
        for label, count in redacted.counts.items():
            result.redacted[label] = result.redacted.get(label, 0) + count
        for finding in residual_findings(redacted.text, key_pattern):
            result.residual[finding] = result.residual.get(finding, 0) + 1
            examples = result.examples.setdefault(finding, [])
            if len(examples) < MAX_EXAMPLES:
                examples.append(f"{source}:{number}")


def _files(paths: list) -> list:
    found = []
    for path in paths:
        if os.path.isdir(path):
            for directory, _dirs, names in os.walk(path):
                found.extend(os.path.join(directory, name) for name in sorted(names))
        elif os.path.isfile(path):
            found.append(path)
    return sorted(found)


def audit(service: str, paths: list) -> AuditResult:
    result = AuditResult(service=service)
    key_pattern = _residual_key_pattern(redaction.json_keys())
    for path in _files(paths):
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            audit_lines(result, handle, path, key_pattern)
        result.files += 1
    return result


def _print_report(result: AuditResult) -> None:
    print(f"Service: {result.service}")
    print(f"Read {result.lines} lines from {result.files} files.")
    if result.redacted:
        print("Redacted:")
        for label, count in sorted(result.redacted.items()):
            print(f"  {label}: {count}")
    if not result.residual_total:
        print("Left after redaction: 0. The audit passes.")
        return
    print(f"Left after redaction: {result.residual_total}. The audit fails.")
    for finding, count in sorted(result.residual.items()):
        print(f"  {finding}: {count}")
        for where in result.examples.get(finding, []):
            print(f"      {where}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Count the personal data left in a service's sample logs "
                    "after redaction. Must be zero before the service is enabled.")
    parser.add_argument("paths", nargs="+",
                        help="Log files, or directories whose every file is read.")
    parser.add_argument("--service", required=True,
                        help="The service the sample was taken from.")
    parser.add_argument("--json", action="store_true",
                        help="Print the result as JSON.")
    args = parser.parse_args(argv)

    if not redaction.is_enabled():
        print("K8S_REDACT_ENABLED is off, so the pipeline redacts nothing; "
              "the audit measures redaction as it runs when on.", file=sys.stderr)

    result = audit(args.service, args.paths)
    if not result.files:
        print(f"No files found under {args.paths}.", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result.as_dict(), indent=2))
    else:
        _print_report(result)
    return 1 if result.residual_total else 0


if __name__ == "__main__":
    sys.exit(main())
