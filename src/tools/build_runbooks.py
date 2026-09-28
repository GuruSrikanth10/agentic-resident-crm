import json
import argparse
from pathlib import Path
from collections import defaultdict
import datetime
from langchain_core.messages import SystemMessage, HumanMessage
from src.utils.llm_utils import get_llm
from src.utils.runbook_validator import validate_generic_text
from src.utils import reason_code_docs, service_registry
from src.utils.runbook_store import (
    BINDING_DB_RULE,
    BINDING_REASON_CODE_DOC,
    SCHEMA_VERSION,
    doc_binding_fingerprint,
    generate_rule_fingerprint,
    write_draft_runbook,
)
from src.utils.logging_config import get_logger
from src.tools.tool_registry import lookup_rule_for

logger = get_logger(__name__)

PROMPT_PATH = Path(__file__).resolve().parent.parent / "prompts" / "RunbookGenerator.md"


def casebook_service(cb: dict, registry=None):
    """The registered service a casebook's runbook belongs to, or None.

    `packet_metadata.service` since Phase 1 of MULTI_SERVICE_PLAN.md. A
    casebook written before then records no service; it is assigned to the
    pre-registry service only when its stage (`packet_status.service`, which
    holds `flowMetaData.stage`) matches that service's `match` rules, and
    otherwise left out: a casebook of unknown service must not teach one
    service's runbook.

    A casebook analysed with another pack than its service's -- a packet the
    gate only recorded, analysed with the pre-registry pack -- is left out
    too: its resolution was reasoned from the other service's knowledge.
    """
    registry = registry or service_registry.load()
    meta = cb.get("packet_metadata") or {}
    service = meta.get("service")
    if service:
        if not registry.is_registered(service):
            return None
        used = ((cb.get("resolution") or {}).get("provenance") or {}).get("service_pack")
        used = used.get("service") if isinstance(used, dict) else None
        if used and used != service:
            return None
        return service

    stage = (cb.get("packet_status") or {}).get("service")
    pre = registry.packs.get(service_registry.PRE_REGISTRY_PACK)
    if pre is not None and isinstance(stage, str) and stage.strip() \
            and pre.matches_stage(stage.strip().lower(), None):
        return pre.name
    return None


def binding_for(service: str, reason_code: str, etype: str):
    """The binding for a runbook of `service`, or None when there is nothing
    to bind it to (MULTI_SERVICE_PLAN.md D11).

    A service with a rules table binds to the fingerprint of the rule; one
    without binds to its own documentation entries for the code, looked up
    the way a packet of that service and type is.
    """
    lookup_type = None if etype == "ANY" else etype
    if service_registry.rule_source_of(service) == service_registry.RULES_DB:
        # `lookup_rule_for` normalises the enrolment type internally and
        # returns parsed rows, so the fingerprint no longer folds DataFrame
        # column order into the hash (F2). "ANY" normalises to None, meaning
        # "do not filter" -- correct for a cross-type runbook.
        rules = lookup_rule_for(reason_code, etype,
                                type_filter=service_registry.rule_type_filter(service))
        if not rules:
            return None
        return {"type": BINDING_DB_RULE,
                "fingerprint": generate_rule_fingerprint(rules)}

    doc_state = reason_code_docs.lookup(
        reason_code, lookup_type, **service_registry.docs_lookup_options(service))
    # Only the service's own documentation: a runbook bound to another
    # service's entries would be checked against the wrong service.
    if doc_state.get("scope") != reason_code_docs.SCOPE_OWN:
        return None
    fingerprint = doc_binding_fingerprint(doc_state)
    if fingerprint is None:
        return None
    return {"type": BINDING_REASON_CODE_DOC, "fingerprint": fingerprint}

def main():
    parser = argparse.ArgumentParser(description="Draft generic runbooks from casebooks.")
    parser.add_argument("--service", type=str, help="Filter by service")
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
    registry = service_registry.load()

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

            # Runbooks are kept per service (MULTI_SERVICE_PLAN.md D11).
            service = casebook_service(cb, registry)
            if service is None:
                logger.info("Skipping casebook with no registered service",
                            event_id=event_id,
                            service=(cb.get("packet_metadata") or {}).get("service"))
                continue
            if args.service and service != args.service:
                continue

            etype = "ANY" if args.any_enrolment_type else str(packet_type).strip().upper()
            groups[(service, reason_code, etype)].append(cb)

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

    for (service, reason_code, etype), casebooks in groups.items():
        if len(casebooks) < args.min_samples:
            logger.info("Skipping group due to insufficient samples", service=service, reason_code=reason_code, enrolment_type=etype, samples=len(casebooks))
            continue
            
        logger.info("Drafting runbook", service=service, reason_code=reason_code, enrolment_type=etype, samples=len(casebooks))
        
        # Build specific values list for validator
        specific_values = []
        for cb in casebooks:
            pm = cb.get("packet_metadata", {})
            specific_values.extend([pm.get("eid", ""), pm.get("srn", ""), pm.get("ref_id", "")])
            
        # The binding pins this runbook to what it was derived from: the DB
        # rule, or for a service without a rules table its documentation.
        binding = binding_for(service, reason_code, etype)
        if binding is None:
            logger.warning("Nothing to bind the runbook to; no rule in the DB "
                           "or no documentation of the service's own",
                           service=service, reason_code=reason_code)
            continue
        
        # Prompt LLM
        examples = []
        max_retries = 0
        for idx, cb in enumerate(casebooks):
            examples.append(f"--- Casebook {idx+1} ---\n{json.dumps(cb.get('resolution', {}), indent=2)}")
            # No easy way to get retries from final casebook right now unless stored, so default 0
            
        human_msg = f"Service: {service}\nReason Code: {reason_code}\nEnrolment Type: {etype}\n\nCasebook Resolutions:\n\n" + "\n\n".join(examples)
        
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
                logger.error("Draft failed generic-text validation", service=service, reason_code=reason_code, violations=violations)
                continue
                
            # Build full draft
            runbook_id = f"{reason_code}__{etype}"
            draft = {
                "schema_version": SCHEMA_VERSION,
                "service": service,
                "runbook_id": runbook_id,
                "reason_code": reason_code,
                "enrolment_type": etype,
                "status": "draft",
                "version": 1,
                "binding": binding,
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
                write_draft_runbook(service, reason_code, etype, draft)
                logger.info("Draft written", service=service, runbook_id=runbook_id)
            else:
                logger.info("Dry run: would write draft", service=service, runbook_id=runbook_id)
                
        except Exception as e:
            logger.error("Failed to generate draft", service=service, reason_code=reason_code, error=f"{type(e).__name__}: {e}")
            continue

if __name__ == "__main__":
    main()
