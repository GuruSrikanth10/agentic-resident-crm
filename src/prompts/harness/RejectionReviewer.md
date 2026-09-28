Review the investigation for event {{event_id}}.

Read the investigation:
- local_casesheets/casebook_{{event_id}}/investigation_text.txt

Read the case evidence:
- local_casesheets/casebook_{{event_id}}/context.json (payload, enrolment type, DB rule)
- local_casesheets/casebook_{{event_id}}/supported_logs.txt (the log trace — may be absent or incomplete)
- local_casesheets/casebook_{{event_id}}/reason_code_doc.md (the reason code documentation — present only for a service with no rules database, and then it is the rule)
- local_casesheets/casebook_{{event_id}}/tool_evidence.txt (what the Investigator's tools returned — present only when it used tools)

## Reasoning hierarchy

The PRIMARY source of truth for WHY a packet was rejected is the combination
of the DB rule and the service documentation. Logs are corroborating evidence.
An investigation that correctly reasons from the code and DB rule is valid
even when logs are unavailable — the Reviewer must not reject it merely for
lacking log citations when no logs were available.

## STEP 1 — Confirm which service(s) the investigation references

- List the available services: Glob docs_cache/*/docs/architecture/components.md
- Read docs_cache/MANIFEST.json for the full service list
- Check that the investigation identified the correct service(s) from the
  evidence (payload flowMetaData.stage, Kafka topics, reason code)

## STEP 2 — Verify claims against the documentation

- Read architecture: docs_cache/<service>/docs/architecture/components.md
- Read dataflow: docs_cache/<service>/docs/architecture/dataflow.md
- Read packet flows: docs_cache/<service>/docs/ontology/flows.md
- Read error paths: docs_cache/<service>/docs/ontology/error_paths.md
- Use Glob to find module docs by class name:
  Glob docs_cache/<service>/docs/modules/*<ClassName>*
- Use Grep to search for the reason code, error codes, or method names
  across the entire corpus

## STEP 3 — Check for these common errors

1. Reason code misapplication: verify the investigation's explanation of the
   reason code matches what the DB rule and service documentation say.
2. Terminology violations: a term used contrary to the SERVICE CONTEXT and
   SERVICE POLICY at the end of this task, or given another service's meaning.
3. Enrolment type misapplication: rules applied that the SERVICE CONTEXT gives
   for a different enrolment type.
4. Claims not grounded in docs: verify service behaviour claims against the
   documentation. Every claim about what the code does must cite the doc.
5. Claims not grounded in logs (when logs ARE available): if logs were
   available, verify cited log lines actually exist in supported_logs.txt.
   If logs were NOT available, do NOT reject the investigation for lacking
   log citations — verify instead that it stated this plainly.
6. Wrong service identified: verify the investigation attributed the failure
   to the correct service.
7. Claims grounded in tool results: when tool_evidence.txt exists, a
   packet-specific fact it supports is grounded, and one it contradicts is
   wrong. A tool result saying the lookup was switched off or failed read
   nothing — reject a finding that treats it as "no rows".

## STEP 4 — Propose a learning rule (only when rejecting)

If you reject because of a mistake the Investigator is likely to repeat on
other packets, propose one permanent rule to correct the behaviour: a strict,
single-line constraint, for example "Always ensure that the solution maps
exactly to the rule's suggested resolution." It is queued for human review,
not applied directly. It must be general: no identifiers, values or other
details from this packet. Use null when approving, or when the mistake is
not one worth a permanent rule.

Set its `scope` to "service" (the default) unless the rule is generic. A
rule is generic only when it concerns evidence handling, citations or output
format, and names no term, rule, enrolment type or data source of any one
service. A generic rule reaches the Investigator for every service; a
"service" rule reaches only this packet's service. When in doubt, use
"service".

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"verdict": "APPROVED" or "REJECTED", "feedback": "<if rejected, explain what is wrong; if approved, empty string>", "learning_rule": {"rule_text": "<single-line rule>", "reasoning": "<why the rule is needed>", "scope": "service" or "generic"} or null}

Follow the rules in AGENTS.md, and these rules for this flow:

{{> rules/rejection}}
