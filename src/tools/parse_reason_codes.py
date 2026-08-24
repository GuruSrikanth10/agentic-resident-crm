#!/usr/bin/env python3
"""Turn the `BusinessReasonCode` Java source into the reason-code catalog.

    python -m src.tools.parse_reason_codes            # regenerate the CSV
    python -m src.tools.parse_reason_codes --report   # show what it found

The organisation publishes its reject codes as Java source, not as data. This
tool is the seam: it parses that source once into `reason_codes.csv`, which is
what the running system reads. Keeping the parser (rather than hand-editing the
CSV) means the next drop of the Java file is a re-run, not a merge.

**Two structures live in one file**, and they are not equivalent:

1. `enum BusinessReasonCode` -- `CODE("CODE", "description", <Category>.<VALUE>)`.
   Each entry *declares* its category. This is ground truth.
2. `bioDedupReasonCodes` -- `tempMap.put(<id>, new ReasonCode("CODE", "desc"))`.
   No category is declared anywhere. Anything we assign is **inference** from
   the numeric id range and the section comment above it.

That distinction is recorded per row as `category_source`, because a category
we inferred and one the source declared must not be indistinguishable to
anything downstream. Three codes appear in both structures with identical
descriptions -- `37004`, `37005`, `37006` are all declared
`TECHNICAL_EXCEPTION` in the enum -- which is the evidence the `37xxx ->
technical` inference rests on. It is evidence, not proof.

**Section comments do not carry across the two structures.** `// Bio Fraud
Stage` sits 340 lines above the first `tempMap.put`, in the enum block; letting
it leak forward would file ten bio-fraud codes under a stage they have nothing
to do with. Context resets at the block boundary.
"""
import argparse
import csv
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_SOURCE = REPO_ROOT / "reason_codes.txt"
DEFAULT_OUTPUT = REPO_ROOT / "reason_codes.csv"

#: `CODE("CODE", "description", ReasonCategory.VALUE),`
_ENUM_RE = re.compile(
    r'^\s*([A-Z][A-Z0-9_]*)\s*\(\s*"([^"]+)"\s*,\s*"([^"]*)"\s*,\s*'
    r'((?:Error)?ReasonCategory)\.([A-Z_]+)\s*\)')

#: `tempMap.put(37004, new ReasonCode("CODE", "description"));`
_MAP_RE = re.compile(
    r'^\s*tempMap\.put\(\s*(\d+)\s*,\s*new ReasonCode\(\s*"([^"]+)"\s*,\s*'
    r'"([^"]*)"\s*\)\s*\)')

#: Where the enum ends and the id-keyed map begins.
_MAP_BLOCK_MARKER = "tempMap = new HashMap"

#: A `// ## Name` line: the processing stage a block of codes belongs to.
_STAGE_RE = re.compile(r'^//\s*##\s*(.+?)\s*$')

# --- our category vocabulary -------------------------------------------------
BUSINESS_VALIDATION_ERROR = "BUSINESS_VALIDATION_ERROR"
BUSINESS_EXCEPTION = "BUSINESS_EXCEPTION"
TECHNICAL_EXCEPTION = "TECHNICAL_EXCEPTION"

DECLARED = "declared"
INFERRED = "inferred"

#: Numeric id prefix -> category, for the map block only. Derived from the
#: section comments the ids sit under, and corroborated by the three codes that
#: also appear in the enum with a declared category.
_ID_PREFIX_CATEGORY = {
    "17": BUSINESS_EXCEPTION,     # "// Business Reason Codes"
    "37": TECHNICAL_EXCEPTION,    # "// TechnicalReasonCodes", "// MDD and CRE Phase 2"
    "12": TECHNICAL_EXCEPTION,    # "// DATA EXCEPTION" -- redis/packet data access
    # 23xxx is deliberately absent. Those ten codes sit under no section
    # header and mix business rejects with technical processing failures;
    # guessing a category for them would be worse than having none, because
    # `classify()` acts on whatever this file says.
}

#: Category -> the DLT failure class it implies (DLT_PLAN.md section 4).
_CATEGORY_CLASS = {
    BUSINESS_VALIDATION_ERROR: "A",
    BUSINESS_EXCEPTION: "A",
    TECHNICAL_EXCEPTION: "C",
}

FIELDNAMES = ("code", "description", "category", "category_source",
              "failure_class", "stage", "section", "numeric_id")


@dataclass
class ReasonCode:
    code: str
    description: str = ""
    category: str = ""
    category_source: str = ""
    failure_class: str = ""
    stage: str = ""
    section: str = ""
    numeric_id: str = ""

    def as_row(self) -> dict:
        return {name: getattr(self, name) for name in FIELDNAMES}


@dataclass
class ParseReport:
    enum_entries: int = 0
    map_entries: int = 0
    merged: int = 0
    skipped_commented: int = 0
    categories: Counter = field(default_factory=Counter)
    stages: Counter = field(default_factory=Counter)
    uncategorised: list = field(default_factory=list)
    corroborated: list = field(default_factory=list)


def _clean_section(text: str) -> str:
    """Strip a `//` comment down to its label, or "" if it is not one.

    `:: DONE` and similar working notes are dropped -- they are the author's
    progress markers, not part of the name.
    """
    body = text.lstrip("/").strip()
    body = body.split("::")[0].strip()
    return body


def parse(source: Path) -> tuple:
    """Parse the Java source into (list[ReasonCode], ParseReport)."""
    by_code = {}
    order = []
    report = ParseReport()

    in_map_block = False
    stage = ""
    section = ""

    for raw in source.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()
        stripped = line.strip()

        # The block boundary. Section context from the enum must not describe
        # codes in the map, so both reset here.
        if not in_map_block and _MAP_BLOCK_MARKER in stripped:
            in_map_block = True
            stage = ""
            section = ""
            continue

        if stripped.startswith("//"):
            # A commented-out entry is not a section header. Skipping it is the
            # point: `// tempMap.put(17041, ...RESIDENT_MAN_DEDUP_REJECT_NON_TD)`
            # was deliberately retired and must not come back as live data.
            if "tempMap.put" in stripped or _ENUM_RE.match(stripped.lstrip("/")):
                report.skipped_commented += 1
                continue
            stage_match = _STAGE_RE.match(stripped)
            if stage_match:
                stage = _clean_section(stage_match.group(1))
                section = ""
            else:
                section = _clean_section(stripped)
            continue

        match = _ENUM_RE.match(line)
        if match:
            _, code, description, _enum_class, value = match.groups()
            entry = ReasonCode(
                code=code,
                description=description.strip(),
                category=value,
                category_source=DECLARED,
                failure_class=_CATEGORY_CLASS.get(value, ""),
                stage=stage,
                section=section,
            )
            if code not in by_code:
                order.append(code)
            by_code[code] = entry
            report.enum_entries += 1
            continue

        match = _MAP_RE.match(line)
        if match:
            numeric_id, code, description = match.groups()
            report.map_entries += 1

            existing = by_code.get(code)
            if existing is not None:
                # Same code in both structures. The enum declared a category;
                # keep it and record that the inference agreed, which is the
                # only corroboration the id-range rule has.
                existing.numeric_id = numeric_id
                inferred = _ID_PREFIX_CATEGORY.get(numeric_id[:2], "")
                if inferred and inferred == existing.category:
                    report.corroborated.append((numeric_id, code, existing.category))
                continue

            category = _ID_PREFIX_CATEGORY.get(numeric_id[:2], "")
            entry = ReasonCode(
                code=code,
                description=description.strip(),
                category=category,
                category_source=INFERRED if category else "",
                failure_class=_CATEGORY_CLASS.get(category, ""),
                stage=stage,
                section=section,
                numeric_id=numeric_id,
            )
            order.append(code)
            by_code[code] = entry

    entries = [by_code[code] for code in order]
    report.merged = len(entries)
    for entry in entries:
        report.categories[(entry.category or "(none)", entry.category_source or "-")] += 1
        report.stages[entry.stage or "(none)"] += 1
        if not entry.category:
            report.uncategorised.append(entry.code)

    return entries, report


def write_csv(entries, output: Path) -> None:
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry.as_row())


def print_report(report: ParseReport, output: Path) -> None:
    print(f"enum entries parsed : {report.enum_entries}")
    print(f"map entries parsed  : {report.map_entries}")
    print(f"commented-out lines skipped: {report.skipped_commented}")
    print(f"unique codes written: {report.merged} -> {output}")

    print("\ncategory (source):")
    for (category, origin), count in report.categories.most_common():
        implied = _CATEGORY_CLASS.get(category, "-")
        print(f"  {count:5d}  {category:26} [{origin}]  -> class {implied}")

    print("\nstage:")
    for stage, count in report.stages.most_common():
        print(f"  {count:5d}  {stage}")

    if report.corroborated:
        print("\ninference corroborated by a declared category:")
        for numeric_id, code, category in report.corroborated:
            print(f"  {numeric_id}  {code} -> {category}")

    if report.uncategorised:
        print(f"\nuncategorised ({len(report.uncategorised)}) -- these carry no "
              f"opinion and leave classification to the stacktrace:")
        for code in report.uncategorised:
            print(f"  {code}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE,
                        help=f"Java source to parse (default {DEFAULT_SOURCE.name})")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"CSV to write (default {DEFAULT_OUTPUT.name})")
    parser.add_argument("--report", action="store_true",
                        help="print what was found")
    parser.add_argument("--check", action="store_true",
                        help="parse and report without writing")
    args = parser.parse_args()

    if not args.source.exists():
        print(f"source not found: {args.source}", file=sys.stderr)
        return 1

    entries, report = parse(args.source)
    if not entries:
        print("no reason codes parsed; refusing to write an empty catalog",
              file=sys.stderr)
        return 1

    if not args.check:
        write_csv(entries, args.output)

    if args.report or args.check:
        print_report(report, args.output)
    else:
        print(f"wrote {len(entries)} reason codes to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
