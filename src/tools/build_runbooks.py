import json
import argparse
from pathlib import Path
from collections import defaultdict
import datetime
from langchain_core.messages import SystemMessage, HumanMessage
from src.utils.llm_utils import get_llm
from src.utils.runbook_validator import validate_generic_text
from src.utils.runbook_store import write_draft_runbook, generate_rule_fingerprint
from src.utils.logging_config import get_logger
from src.tools.tool_registry import lookup_rule_for

logger = get_logger(__name__)

PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "RunbookGenerator.md"

def main():
    parser = argparse.ArgumentParser(description="Draft generic runbooks from casebooks.")
    parser.add_argument("--reason-code", type=str, help="Filter by specific reason code")
    parser.add_argument("--min-samples", type=int, default=3, help="Minimum eligible casebooks required")
    parser.add_argument("--any-enrolment-type", action="store_true", help="Generate ANY enrolment_type runbook")
    parser.add_argument("--overwrite-drafts", action="store_true", help="Overwrite existing drafts")
    parser.add_argument("--dry-run", action="store_true", help="Do not save drafts")
    args = parser.parse_args()
    
    # 1. Gather terminal casebooks through CasebookStorage.
    #
    # This used to walk local_casesheets/ directly, so under
    # CASEBOOK_STORAGE_BACKEND=s3 it found nothing, generated no drafts, and
    # reported success -- the whole runbook-learning loop was silently dead on
    # exactly the deployment it is meant to scale with. Same failure as G2.
    groups = defaultdict(list)

    from src.storage.factory import get_casebook_storage

    storage = get_casebook_storage()
    try:
        event_ids = storage.list_events()
    except Exception as e:
        logger.error("Could not enumerate casebooks",
                     error=f"{type(e).__name__}: {e}")
        event_ids = []

    for event_id in event_ids:
        try:
            cb = storage.load(event_id, filename="casebook.json")
            if not cb:
                continue

            status = cb.get("packet_status", {}).get("status")
            # Exclude non-clean outcomes
            if status not in ("COMPLETED", "REJECTED"):
                continue

            synthesis = cb.get("resolution", {}).get("synthesis", "")
            if "ESCALATED TO HUMAN REVIEW" in synthesis:
                continue

            # We need rejection_code and packet_type
            rejection_data = cb.get("packet_status", {}).get("rejection_data", {})
            reason_code = (rejection_data.get("rejection_code")
                           or rejection_data.get("reason_code"))
            packet_type = cb.get("packet_metadata", {}).get("packet_type")

            if not reason_code or not packet_type:
                continue

            if args.reason_code and reason_code != args.reason_code:
                continue

            etype = "ANY" if args.any_enrolment_type else str(packet_type).strip().upper()
            groups[(reason_code, etype)].append(cb)

        except Exception as e:
            logger.error("Failed to read casebook", event_id=event_id,
                         error=f"{type(e).__name__}: {e}")
            continue

    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        system_prompt = f.read()

    # Built on first use, not up front. `get_llm` resolves provider
    # credentials and raises without them, so constructing it unconditionally
    # meant `--dry-run` -- and any run where no group reaches --min-samples --
    # failed on a machine that needs no model at all.
    _llm = None

    def llm_client():
        nonlocal _llm
        if _llm is None:
            _llm = get_llm(tier="simple").with_config(tags=["runbook_generator"])
        return _llm

    for (reason_code, etype), casebooks in groups.items():
        if len(casebooks) < args.min_samples:
            logger.info("Skipping group due to insufficient samples", reason_code=reason_code, enrolment_type=etype, samples=len(casebooks))
            continue
            
        logger.info("Drafting runbook", reason_code=reason_code, enrolment_type=etype, samples=len(casebooks))
        
        # Build specific values list for validator
        specific_values = []
        for cb in casebooks:
            pm = cb.get("packet_metadata", {})
            specific_values.extend([pm.get("eid", ""), pm.get("srn", ""), pm.get("ref_id", "")])
            
        # The rule fingerprint pins this runbook to the DB rule it was derived
        # from. `lookup_rule_for` normalises the enrolment type internally and
        # returns parsed rows, so the fingerprint no longer folds DataFrame
        # column order into the hash (F2). "ANY" normalises to None, meaning
        # "do not filter" -- correct for a cross-type runbook.
        rules = lookup_rule_for(reason_code, etype)
        if not rules:
            logger.warning("No rule found in DB for group", reason_code=reason_code)
            continue

        fingerprint = generate_rule_fingerprint(rules)
        
        # Prompt LLM
        examples = []
        max_retries = 0
        for idx, cb in enumerate(casebooks):
            examples.append(f"--- Casebook {idx+1} ---\n{json.dumps(cb.get('resolution', {}), indent=2)}")
            # No easy way to get retries from final casebook right now unless stored, so default 0
            
        human_msg = f"Reason Code: {reason_code}\nEnrolment Type: {etype}\n\nCasebook Resolutions:\n\n" + "\n\n".join(examples)
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=human_msg)
        ]
        
        try:
            response = llm_client().invoke(messages)
            text = response.content
            
            # Extract JSON block
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].strip()
                
            draft_res = json.loads(text)
            
            # Validate
            draft_str = json.dumps(draft_res)
            violations = validate_generic_text(draft_str, specific_values)
            if violations:
                logger.error("Draft failed generic-text validation", reason_code=reason_code, violations=violations)
                continue
                
            # Build full draft
            runbook_id = f"{reason_code}__{etype}"
            draft = {
                "schema_version": "1.1",
                "runbook_id": runbook_id,
                "reason_code": reason_code,
                "enrolment_type": etype,
                "status": "draft",
                "version": 1,
                "rule_fingerprint": fingerprint,
                "resolution": draft_res,
                "provenance": {
                    "source_event_ids": [cb.get("packet_metadata", {}).get("eid") for cb in casebooks],
                    "source_casebook_count": len(casebooks),
                    "generated_at": datetime.datetime.utcnow().isoformat(),
                    "generated_by_model": "simple_tier_llm",
                    "max_retry_count_in_sources": max_retries
                },
                "approved_by": None,
                "approved_at": None
            }
            
            if not args.dry_run:
                write_draft_runbook(reason_code, etype, draft)
                logger.info("Draft written", runbook_id=runbook_id)
            else:
                logger.info("Dry run: would write draft", runbook_id=runbook_id)
                
        except Exception as e:
            logger.error("Failed to generate draft", reason_code=reason_code, error=f"{type(e).__name__}: {e}")
            continue

if __name__ == "__main__":
    main()
