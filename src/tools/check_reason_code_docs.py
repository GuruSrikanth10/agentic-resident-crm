#!/usr/bin/env python3
"""
Operator CLI: check the reason-code documentation store.

    python3 -m src.tools.check_reason_code_docs
    python3 -m src.tools.check_reason_code_docs --coverage
    python3 -m src.tools.check_reason_code_docs --dir /mnt/reason_code_docs

Exits 1 when the store has any error, 0 otherwise, so it works as a
pre-commit or CI gate. The same check runs at API boot when
`REJECTION_REASON_CODE_DOCS_ENABLED=true`, which is the reason a broken store
must be caught here first: there it exits the process.

`--coverage` additionally lists reason codes that already have a runbook but
no documentation. Those are the documents worth writing next -- a runbook
exists because the pipeline meets that code often enough to be worth a stored
answer.
"""
import argparse
import sys

from dotenv import load_dotenv

# Before utils.paths resolves REASON_CODE_DOCS_DIR: without this the CLI
# checks the default in-repo store while the deployment it is being run for
# reads a mounted one.
load_dotenv()

from src.utils.reason_code_docs import validate  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the reason-code documentation store.")
    parser.add_argument(
        "--dir", dest="directory", default=None,
        help="Store root to check. Defaults to REASON_CODE_DOCS_DIR.")
    parser.add_argument(
        "--coverage", action="store_true",
        help="Also warn about reason codes that have a runbook but no docs.")
    args = parser.parse_args(argv)

    errors, warnings = validate(root=args.directory, coverage=args.coverage)

    for warning in warnings:
        print(f"WARNING: {warning}")
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)

    if errors:
        print(f"\n{len(errors)} error(s), {len(warnings)} warning(s).",
              file=sys.stderr)
        return 1

    print(f"\nThe store is valid. {len(warnings)} warning(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
