import json
import argparse
import subprocess
import getpass
import datetime
from pathlib import Path
from filelock import FileLock, Timeout

from src.utils.runbook_store import (
    binding_of,
    list_draft_runbooks,
    list_final_runbooks,
    load_draft_runbook,
    promote_draft_to_final,
    get_runbook,
    service_of_path,
    RUNBOOK_ROOT,
)
from src.utils.runbook_validator import validate_generic_text
from src.utils.logging_config import get_logger
from src.tools.build_runbooks import binding_for

logger = get_logger(__name__)

def git_is_clean(path: Path) -> bool:
    try:
        # Check if there are uncommitted changes in the runbooks directory
        res = subprocess.run(
            ["git", "status", "--porcelain", str(path)],
            capture_output=True,
            text=True,
            check=True
        )
        return not bool(res.stdout.strip())
    except subprocess.CalledProcessError:
        return False

def git_commit_runbooks(message: str):
    subprocess.run(["git", "add", str(RUNBOOK_ROOT)], check=True)
    subprocess.run(["git", "commit", "-m", message], check=True)

def main():
    parser = argparse.ArgumentParser(description="Promote draft runbooks to final.")
    parser.add_argument("--service", type=str, help="Filter by service")
    parser.add_argument("--reason-code", type=str, help="Filter by reason code")
    parser.add_argument("--list", action="store_true", help="List drafts and staleness without promoting")
    parser.add_argument("--dry-run", action="store_true", help="Dry run only")
    args = parser.parse_args()
    
    lock_path = RUNBOOK_ROOT / ".promote.lock"
    
    try:
        with FileLock(lock_path, timeout=5):
            
            if not args.list and not args.dry_run:
                if not git_is_clean(RUNBOOK_ROOT):
                    print("Error: Uncommitted changes in src/runbooks. Please commit or stash them first.")
                    return
            
            drafts = list_draft_runbooks()
            if not drafts:
                print("No draft runbooks found.")
            
            for draft_path in drafts:
                data = load_draft_runbook(draft_path)
                # A draft belongs to the service whose directory holds it.
                service = service_of_path(draft_path)
                reason_code = data["reason_code"]
                etype = data["enrolment_type"]
                
                if args.service and service != args.service:
                    continue
                if args.reason_code and reason_code != args.reason_code:
                    continue
                    
                print(f"\n{'='*60}")
                print(f"Draft: {service} {reason_code} ({etype})")
                print(f"Sources: {data['provenance']['source_casebook_count']} casebooks, max retries: {data['provenance']['max_retry_count_in_sources']}")
                print(f"Resolution:")
                print(json.dumps(data["resolution"], indent=2))
                
                # Compare to existing final runbook
                existing = get_runbook(service, reason_code, etype)
                if existing:
                    print(f"\nExisting version: {existing['version']}")
                else:
                    print("\nExisting version: None (New)")
                    
                if args.list:
                    continue
                    
                while True:
                    resp = input(f"\nPromote this runbook? [y/N/edit]: ").strip().lower()
                    if resp in ("y", "n", "", "edit"):
                        break
                        
                if resp == "edit":
                    print(f"Please edit the draft manually at {draft_path} and run this tool again.")
                    continue
                    
                if resp == "y":
                    # Re-validate
                    draft_str = json.dumps(data["resolution"])
                    specific_values = data["provenance"].get("source_event_ids", [])
                    violations = validate_generic_text(draft_str, specific_values)
                    
                    if violations:
                        print("Error: Validation failed! Cannot promote.")
                        for v in violations:
                            print(f" - {v}")
                        continue
                        
                    # Update fields
                    data["status"] = "final"
                    data["approved_by"] = getpass.getuser()
                    data["approved_at"] = datetime.datetime.utcnow().isoformat()
                    
                    if existing:
                        data["version"] = existing.get("version", 0) + 1
                    else:
                        data["version"] = 1
                        
                    if not args.dry_run:
                        promote_draft_to_final(draft_path, data)
                        git_commit_runbooks(f"Add runbook {service} {reason_code} {etype} v{data['version']}")
                        print(f"Promoted to final as version {data['version']}.")
                    else:
                        print("Dry run: would promote.")
            
            # Check staleness if list mode
            if args.list:
                print(f"\n{'='*60}")
                print("Checking staleness for all final runbooks...")
                for final_path in list_final_runbooks():
                    with open(final_path, "r", encoding="utf-8") as f:
                        final_data = json.load(f)

                    service = service_of_path(final_path)
                    rc = final_data["reason_code"]
                    et = final_data["enrolment_type"]

                    # What the runbook is bound to now, computed the way it
                    # was when the draft was written (MULTI_SERVICE_PLAN.md D11).
                    bound = binding_of(final_data)
                    current = binding_for(service, rc, et)
                    if current is None:
                        print(f"WARNING: {service} {rc} ({et}) - nothing to check it "
                              f"against: no rule in the DB, or no documentation "
                              f"of the service's own")
                    elif current["type"] != bound["type"]:
                        print(f"STALE: {service} {rc} ({et}) - bound to {bound['type']}, "
                              f"but the service's rule source is now {current['type']}.")
                    elif current["fingerprint"] != bound["fingerprint"]:
                        print(f"STALE: {service} {rc} ({et}) - Fingerprint mismatch! "
                              f"The {current['type']} has changed.")
            
    except Timeout:
        print("Another promotion is currently in progress. Exiting.")

if __name__ == "__main__":
    main()
