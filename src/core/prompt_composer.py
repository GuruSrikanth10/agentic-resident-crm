"""
The rejection lane's prompts, composed per service pack
(MULTI_SERVICE_PLAN.md D3, D4 and section 5.5).

Each role prompt in `src/prompts/` says what every service shares: how to use
the evidence, the gaps banner, the output contract. Everything one service
knows and no other shares -- its glossary, what its enrolment types mean, its
policy -- is in its pack under `src/service_packs/<service>/`. This module
puts the two together, in one fixed order, for one role and one pack:

    <generic role prompt>

    ### SERVICE CONTEXT -- <display name> [<pack>]
    <the pack's file for this role>

    ### SERVICE POLICY -- <display name> [<pack>]
    <the pack's policy.md>

    ### LEARNED RULES                        (the Investigator only)
    <src/prompts/learned_rules.md, then the pack's learned_rules.md>

`agent_factory.build_agent` then appends the AVAILABLE TOOLS section and the
operating note, as it always has.

Services' vocabularies contradict each other -- enu-biometric's "demo" is the
face modality, and must not be read as demographic -- so no prompt ever
carries two packs. Nothing here reads a model or the network: the text is
decided by the pack a packet was placed in, and nothing else.

    python3 -m src.core.prompt_composer investigator --pack enu-biometric
"""
from typing import Optional

from src.utils import paths, service_registry

#: The generic prompt of each role that has a pack section.
ROLE_PROMPTS = {"investigator": "InvestigatorAgent.md",
                "reviewer": "ReviewerAgent.md",
                "synthesis": "SynthesisAgent.md"}

#: Learned rules that hold for every service. Promotion writes a service's own
#: rules to its pack; this file is for the ones about evidence and output
#: that name no service concept. Absent until the first is promoted.
GENERIC_LEARNED_RULES = "learned_rules.md"

#: Said when a pack has no file for a role, so the prompts that point at the
#: SERVICE CONTEXT never point at nothing.
NO_ROLE_INSTRUCTIONS = "No service-specific instructions are configured for this role."

#: How each way of placing a packet is described to the agents.
_PLACED_BY = {
    service_registry.SOURCE_FLOW_STAGE: "its flowMetaData.stage",
    service_registry.SOURCE_SOURCE_TOPIC: "its sourceTopic",
    service_registry.SOURCE_REASON_CODE_DOCS:
        "its reason code, which only this service's documentation documents",
}


def _prompts_dir():
    return paths.REPO_ROOT / "src" / "prompts"


def generic_prompt(role: str) -> str:
    with open(_prompts_dir() / ROLE_PROMPTS[role], "r", encoding="utf-8") as handle:
        return handle.read()


def _generic_learned_rules() -> str:
    path = _prompts_dir() / GENERIC_LEARNED_RULES
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def _require_pack(name: str):
    found = service_registry.pack(name)
    if found is None:
        # Reaching here means a pack that is not in the registry: boot
        # validation refuses a registry missing the packs it needs, so this is
        # a bug, not a configuration a packet should limp through.
        raise ValueError(f"No valid service pack named {name!r}.")
    return found


def service_context(role: str, pack_name: str, lead: Optional[str] = None) -> str:
    """The pack's sections for `role`: context, policy and, for the
    Investigator, learned rules. `lead` opens the context section -- the
    harness uses it to say which service the packet was placed in."""
    found = _require_pack(pack_name)
    title = f"{found.display_name} [{found.name}]"

    context = [f"### SERVICE CONTEXT -- {title}"]
    if lead:
        context.append(lead.strip())
    context.append((found.texts.get(service_registry.ROLE_FILES[role], "").strip()
                    or NO_ROLE_INSTRUCTIONS))
    sections = ["\n\n".join(context),
                f"### SERVICE POLICY -- {title}\n"
                f"{found.texts[service_registry.POLICY_FILE].strip()}"]

    if role == "investigator":
        learned = [text.strip() for text in (
            _generic_learned_rules(),
            found.texts.get(service_registry.LEARNED_RULES_FILE, ""))
            if text.strip()]
        if learned:
            sections.append("### LEARNED RULES\n" + "\n".join(learned))
    return "\n\n".join(sections)


def compose_system_prompt(role: str, pack_name: str) -> str:
    """The system prompt `role` is built with for packets using `pack_name`."""
    return f"{generic_prompt(role).rstrip()}\n\n{service_context(role, pack_name)}"


def _placed(resolution: dict, pack_name: str) -> Optional[str]:
    """How the packet came to be in `pack_name`, or None when it did not: in
    `record` mode a packet the gate would skip is analysed with the
    pre-registry pack, and saying it belongs there would be untrue."""
    if resolution.get("service") != pack_name:
        return None
    phrase = _PLACED_BY.get(str(resolution.get("source") or ""), "its payload")
    placed = f"placed there by {phrase} '{resolution.get('matched')}'"
    other = (resolution.get("conflict") or {}).get("reason_code_docs")
    if other:
        placed += (f". The reason code is documented by {other}, not by "
                   f"{pack_name}: if the evidence points to {other}, say so, "
                   f"but do not apply its policy")
    return placed


def service_note(resolution: Optional[dict], pack_name: str) -> Optional[str]:
    """The `### Service` section of a direct prompt's user message, or None
    to leave it out."""
    resolution = resolution or {}
    if pack_name == service_registry.DEFAULT_PACK:
        return ("Not resolved: no rule placed this packet in a known service, "
                "so no service-specific policy applies. Reason only from the "
                "documentation, the rule and the evidence.")
    placed = _placed(resolution, pack_name)
    if placed is None:
        return None
    found = _require_pack(pack_name)
    return f"This packet belongs to {pack_name} ({found.display_name}), {placed}."


def harness_lead(resolution: Optional[dict], pack_name: str) -> Optional[str]:
    """The opening of a harness task's SERVICE CONTEXT, or None."""
    resolution = resolution or {}
    if pack_name == service_registry.DEFAULT_PACK:
        return ("This packet's service could not be resolved. Identify the "
                "service from the evidence, and apply no other service's "
                "policy to it.")
    placed = _placed(resolution, pack_name)
    if placed is None:
        return None
    found = _require_pack(pack_name)
    return (f"This packet belongs to {pack_name}, {placed}. Its documentation "
            f"is in docs_cache/{found.droa_corpus_dir}/.")


#: The case file a harness task reads the documentation from, for a pack with
#: no rules database (MULTI_SERVICE_PLAN.md Phase 3).
HARNESS_RULE_DOC_FILE = "reason_code_doc.md"


def harness_rule_source(pack_name: str, event_id: str) -> Optional[str]:
    """The `### RULE SOURCE` section of a harness task, or None.

    None for a pack with a rules database, whose harness prompt is then
    exactly what it was. The templates tell the agent to read the DB rule in
    context.json; for a pack without one there is none there, and its
    documentation is the only account of the rule, so this says where it is.
    """
    if service_registry.rule_source_of(pack_name) == service_registry.RULES_DB:
        return None
    path = f"local_casesheets/casebook_{event_id}/{HARNESS_RULE_DOC_FILE}"
    return ("### RULE SOURCE\n"
            "This service has no rules database, so context.json holds no DB "
            "rule. Wherever this task says \"DB rule\", read the reason code "
            f"documentation in {path} instead: it is the only account of the "
            "rule that fired. If it says no documentation describes this "
            "reason code, say so plainly and do not invent a rule.")


def harness_service_context(role: str, resolution: Optional[dict],
                            pack_name: str, event_id: Optional[str] = None) -> str:
    """What a harness task appends after its template: the lead, the pack's
    sections for `role`, then, for a pack with no rules database and a known
    `event_id`, where the rule is."""
    block = service_context(role, pack_name, lead=harness_lead(resolution, pack_name))
    rule_source = harness_rule_source(pack_name, event_id) if event_id else None
    return f"{block}\n\n{rule_source}" if rule_source else block


def main(argv=None) -> int:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        prog="python3 -m src.core.prompt_composer",
        description="Print the system prompt a role is built with for a pack "
                    "(without the AVAILABLE TOOLS section and the operating "
                    "note, which build_agent adds).")
    parser.add_argument("role", choices=sorted(ROLE_PROMPTS))
    parser.add_argument("--pack", default=service_registry.PRE_REGISTRY_PACK)
    args = parser.parse_args(argv)
    try:
        print(compose_system_prompt(args.role, args.pack))
    except Exception as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
