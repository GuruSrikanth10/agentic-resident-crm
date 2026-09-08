import os
import csv
import sys

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# A dummy representation of the expected "DB Schema" policies/columns
KNOWN_DB_COLUMNS = {
    "rule_data",
    "reject_reason_code",
    "rule_description",
    "is_active"
}

def check_drift():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    rules_file = os.path.join(base_dir, "rules.csv")
    
    if not os.path.exists(rules_file):
        logger.error("Cannot find the rules file", path=rules_file)
        sys.exit(1)
        
    print(f"Checking {rules_file} against the expected schema.")
    
    with open(rules_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            logger.error("Rules file is empty", path=rules_file)
            sys.exit(1)
        first_data_row = next(reader, None)

    # Normalize headers
    headers = [h.strip().lower() for h in headers if h.strip()]

    # A CSV that parses down to a single column when several are expected is
    # usually not a genuine schema change -- it's the tell-tale sign of an
    # export with unescaped commas/newlines inside a field (e.g. a raw JSON
    # blob dumped into rule_data without quoting), which drags the row's
    # other real columns into that one cell and desyncs the column count for
    # the rest of the file. Surface that distinctly instead of reporting it
    # as if the DB schema had changed (1.15).
    if len(headers) == 1 and len(KNOWN_DB_COLUMNS) > 1:
        sample = (first_data_row[0].strip().lower() if first_data_row else "")
        looks_like_broken_export = sample in KNOWN_DB_COLUMNS or sample == headers[0]
        print(
            f"ALERT: {rules_file} parsed as a single column ('{headers[0]}') "
            f"but {len(KNOWN_DB_COLUMNS)} columns are expected ({sorted(KNOWN_DB_COLUMNS)})."
        )
        if looks_like_broken_export:
            print(
                "This looks like a malformed export (unescaped commas/newlines inside a "
                "field, likely rule_data's JSON, spilled into the row) rather than a real "
                "schema change. Re-export rules.csv with proper CSV quoting -- do not "
                "change KNOWN_DB_COLUMNS to match this file."
            )
        sys.exit(2)

    unknown_columns = [h for h in headers if h not in KNOWN_DB_COLUMNS]
    missing_columns = [k for k in KNOWN_DB_COLUMNS if k not in headers]

    drift_detected = False
    
    if unknown_columns:
        print(f"ALERT: drift detected. Undocumented columns in rules.csv: {unknown_columns}")
        drift_detected = True
        
    if missing_columns:
        print(f"ALERT: drift detected. Expected columns missing from rules.csv: {missing_columns}")
        drift_detected = True
        
    if drift_detected:
        print("Please update the agent_policy_context.md and KNOWN_DB_COLUMNS to reflect these schema changes.")
        sys.exit(2)
    else:
        print("No drift detected. rules.csv matches expected DB schema.")
        sys.exit(0)

if __name__ == "__main__":
    check_drift()