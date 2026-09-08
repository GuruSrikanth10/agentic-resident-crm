"""Promote Reviewer-proposed learning rules into the Investigator prompt.

Every rule approved here is appended to InvestigatorAgent.md and becomes part
of the system prompt for every future investigation. That text originates from
an LLM reading log content, and log content is influenced by upstream request
data -- so this is the last gate on a path that runs from a log line to a
permanent, privileged instruction (G19).

The gate is therefore deliberately awkward:
  - each rule is re-validated here, not only when it was proposed;
  - the exact diff is shown before anything is written;
  - approval requires typing the word `promote`, not `y`;
  - the git commit is a separate, opt-in step.
"""
import argparse
import json
import os
import subprocess

from filelock import FileLock

from src.utils.runbook_validator import validate_learning_rule


def promote_rules(auto_commit: bool = False):
    base_dir = os.path.dirname(os.path.dirname(__file__))
    prompts_dir = os.path.join(base_dir, "prompts")
    pending_file = os.path.join(prompts_dir, "pending_rules.jsonl")
    file_lock_path = pending_file + ".lock"
    promo_lock_path = os.path.join(prompts_dir, "promotion.lock")
    target_file = os.path.join(prompts_dir, "InvestigatorAgent.md")
    
    # 1. Top-level lock to prevent concurrent promotions by multiple humans/scripts
    promo_lock = FileLock(promo_lock_path, timeout=0)
    try:
        promo_lock.acquire()
    except Exception:
        print("Another promotion process is currently running. Exiting.")
        return
        
    try:
        # 2. Git status check
        result = subprocess.run(["git", "status", "--porcelain", prompts_dir], capture_output=True, text=True)
        if result.stdout.strip():
            print(f"Refusing to promote: uncommitted changes exist in {prompts_dir}")
            print(result.stdout)
            return

        if not os.path.exists(pending_file):
            print("No pending rules to promote.")
            return
            
        # 3. Read pending rules holding the file lock briefly
        with FileLock(file_lock_path, timeout=10):
            with open(pending_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                
        if not lines:
            print("No pending rules to promote.")
            return
            
        print(f"Found {len(lines)} pending rules.")

        # We process them sequentially. Only lines that are actually
        # promoted get removed below -- skipped rules, rules that errored,
        # and anything a running agent appends to pending_file during this
        # (potentially long, interactive) loop must all survive (1.7).
        promoted_count = 0
        promoted_raw_lines = set()
        for i, line in enumerate(lines):
            try:
                entry = json.loads(line.strip())
                proposed = entry.get("proposed_rule") or ""

                print(f"\nRule {i+1}")
                print(f"Event ID: {entry.get('eventId')}")
                print(f"Reasoning: {entry.get('reviewer_reasoning')}")

                # Re-validate at promotion. A rule may have been queued before
                # the validator existed, or by a different code path.
                violations = validate_learning_rule(proposed)
                if violations:
                    print("REJECTED by validation, cannot be promoted:")
                    for violation in violations:
                        print(f"  - {violation}")
                    continue

                # The exact diff, so approval is informed rather than nominal.
                addition = f"\n- CRITICAL RULE: {proposed}\n"
                print(f"\nThis will append to {os.path.basename(target_file)}:")
                print("-" * 70)
                for diff_line in addition.strip("\n").splitlines():
                    print(f"+ {diff_line}")
                print("-" * 70)
                print("It becomes part of the system prompt for EVERY future packet.")

                choice = input("Type 'promote' to apply, anything else to skip: ").strip()
                if choice == "promote":
                    with open(target_file, "a", encoding="utf-8") as f_target:
                        f_target.write(addition)

                    print("Rule promoted.")
                    promoted_count += 1
                    promoted_raw_lines.add(line.strip())

                    if auto_commit:
                        commit_msg = f"Add learning rule from event {entry.get('eventId')}"
                        subprocess.run(["git", "add", target_file], check=True)
                        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
                        print("Committed.")
                else:
                    print("Rule skipped.")
            except Exception as e:
                print(f"Error processing rule {i+1}: {e}")

        # Rewrite the pending file, keeping every entry that was not
        # promoted. Re-read fresh (rather than reusing the stale `lines`
        # from the initial read) so anything appended concurrently during
        # this interactive session is preserved too.
        with FileLock(file_lock_path, timeout=10):
            with open(pending_file, "r", encoding="utf-8") as f:
                current_lines = f.readlines()
            remaining_lines = [ln for ln in current_lines if ln.strip() not in promoted_raw_lines]
            with open(pending_file, "w", encoding="utf-8") as f:
                f.writelines(remaining_lines)

        print(f"\nFinished. Promoted {promoted_count} rules.")
    finally:
        promo_lock.release()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Review and promote Reviewer-proposed learning rules."
    )
    parser.add_argument(
        "--commit", action="store_true",
        help="Also git-commit each promotion. Off by default so promoting and "
             "committing stay separate decisions -- an auto-commit makes an "
             "unreviewed prompt change look reviewed.",
    )
    promote_rules(auto_commit=parser.parse_args().commit)
