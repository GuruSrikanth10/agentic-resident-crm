"""Promote Reviewer-proposed learning rules into the Investigator prompt.

Every rule carries a scope (MULTI_SERVICE_PLAN.md D11), proposed by the
Reviewer and shown here for the operator to keep or change:
  - `service` (the default): appended to the `learned_rules.md` of the
    service pack it was learned under, and part of the Investigator's system
    prompt for every future packet analysed with that pack;
  - `generic`: appended to `src/prompts/learned_rules.md`, and part of the
    Investigator's system prompt for every service.
Never to InvestigatorAgent.md: a rule learned under one service's policy can
be wrong under another's, which is also why `service` is the default -- a
rule wrongly marked generic reaches every service, one wrongly kept to its
service merely fails to spread. That text originates from an LLM reading log content, and
log content is influenced by upstream request data -- so this is the last
gate on a path that runs from a log line to a permanent, privileged
instruction (G19).

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

from src.core import prompt_composer
from src.utils import paths, service_registry
from src.utils.runbook_validator import validate_learning_rule

SCOPE_SERVICE = "service"
SCOPE_GENERIC = "generic"


def target_pack_for(entry: dict) -> str:
    """The pack a pending rule is promoted into: the one it was learned under.

    An entry queued before rules recorded their pack was learned under the
    pre-registry pack, the only one that existed, so it goes there -- and so
    does one naming a pack this registry no longer has, rather than being
    promoted somewhere it was not learned.
    """
    pack = entry.get("service_pack")
    if isinstance(pack, str) and pack in service_registry.load().packs:
        return pack
    return service_registry.PRE_REGISTRY_PACK


def scope_of(entry: dict) -> str:
    """The scope a pending rule was proposed with. An entry queued before
    rules carried one, or carrying anything but `generic`, is `service`."""
    return SCOPE_GENERIC if entry.get("scope") == SCOPE_GENERIC else SCOPE_SERVICE


def generic_rules_file() -> str:
    return str(prompt_composer._prompts_dir() / prompt_composer.GENERIC_LEARNED_RULES)


def target_file_for(entry: dict, scope: str = None) -> str:
    """The file a pending rule is appended to under `scope` (by default the
    scope it was proposed with)."""
    if (scope or scope_of(entry)) == SCOPE_GENERIC:
        return generic_rules_file()
    return str(service_registry.packs_dir() / target_pack_for(entry)
               / service_registry.LEARNED_RULES_FILE)


def _show(entry: dict, scope: str, addition: str) -> None:
    """The exact diff for `scope`, so approval is informed rather than nominal."""
    pack = target_pack_for(entry)
    target_file = target_file_for(entry, scope)
    where = (os.path.relpath(target_file, paths.REPO_ROOT) if scope == SCOPE_GENERIC
             else f"{pack}/{os.path.basename(target_file)}")
    print(f"Scope: {scope}" + (" (as proposed)" if scope == scope_of(entry)
                                else f" (proposed: {scope_of(entry)})"))
    print(f"\nThis will append to {where}:")
    print("-" * 70)
    for diff_line in addition.strip("\n").splitlines():
        print(f"+ {diff_line}")
    print("-" * 70)
    if scope == SCOPE_GENERIC:
        print("It becomes part of the Investigator's system prompt for EVERY "
              "future packet of EVERY service. Promote it as generic only if it "
              "concerns evidence handling, citations or output format and names "
              "no concept of any one service.")
    else:
        print(f"It becomes part of the Investigator's system prompt for "
              f"EVERY future packet analysed with the {pack} pack.")


def promote_rules(auto_commit: bool = False):
    # The directory the composer reads the generic learned rules from.
    prompts_dir = str(prompt_composer._prompts_dir())
    packs_dir = str(service_registry.packs_dir())
    pending_file = os.path.join(prompts_dir, "pending_rules.jsonl")
    file_lock_path = pending_file + ".lock"
    promo_lock_path = os.path.join(prompts_dir, "promotion.lock")
    
    # 1. Top-level lock to prevent concurrent promotions by multiple humans/scripts
    promo_lock = FileLock(promo_lock_path, timeout=0)
    try:
        promo_lock.acquire()
    except Exception:
        print("Another promotion process is currently running. Exiting.")
        return
        
    try:
        # 2. Git status check, over both places a promotion reads or writes.
        result = subprocess.run(["git", "status", "--porcelain", prompts_dir, packs_dir],
                                capture_output=True, text=True)
        if result.stdout.strip():
            print(f"Refusing to promote: uncommitted changes exist in {prompts_dir} "
                  f"or {packs_dir}")
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

                pack = target_pack_for(entry)
                addition = f"\n- CRITICAL RULE: {proposed}\n"
                print(f"Learned under service {entry.get('service') or 'unrecorded'}, "
                      f"pack {pack}.")

                # The operator may change the proposed scope; the diff is
                # shown again for the scope that would be applied.
                scope = scope_of(entry)
                while True:
                    _show(entry, scope, addition)
                    other = SCOPE_SERVICE if scope == SCOPE_GENERIC else SCOPE_GENERIC
                    choice = input(f"Type 'promote' to apply, '{other}' to change the "
                                   f"scope, anything else to skip: ").strip()
                    if choice != other:
                        break
                    scope = other
                target_file = target_file_for(entry, scope)
                if choice == "promote":
                    with open(target_file, "a", encoding="utf-8") as f_target:
                        f_target.write(addition)

                    print(f"Rule promoted ({scope}).")
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
