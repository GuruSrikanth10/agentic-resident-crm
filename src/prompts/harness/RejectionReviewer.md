Review the investigation for event {{event_id}}.

Read the investigation:
- local_casesheets/casebook_{{event_id}}/investigation_text.txt

Read the case evidence:
- local_casesheets/casebook_{{event_id}}/context.json (payload, enrolment type, DB rule)
- local_casesheets/casebook_{{event_id}}/supported_logs.txt (the log trace — may be absent or incomplete)

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
2. Glossary violations: 'demo' = face modality, 'nonDemo' = fingerprints and
   iris. 'TD' = all nonDemo matched.
3. Enrolment type misapplication: N / E = 1:N dedup, U = 1:N dedup plus 1:1
   auth and append against its own parent, MBU = treated as 1:N.
4. Claims not grounded in docs: verify service behaviour claims against the
   documentation. Every claim about what the code does must cite the doc.
5. Claims not grounded in logs (when logs ARE available): if logs were
   available, verify cited log lines actually exist in supported_logs.txt.
   If logs were NOT available, do NOT reject the investigation for lacking
   log citations — verify instead that it stated this plainly.
6. Wrong service identified: verify the investigation attributed the failure
   to the correct service.

## STEP 4 — Propose a learning rule (only when rejecting)

If you reject because of a mistake the Investigator is likely to repeat on
other packets, propose one permanent rule to correct the behaviour: a strict,
single-line constraint, for example "Always ensure that the solution maps
exactly to the rule's suggested resolution." It is queued for human review,
not applied directly. It must be general: no identifiers, values or other
details from this packet. Use null when approving, or when the mistake is
not one worth a permanent rule.

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"verdict": "APPROVED" or "REJECTED", "feedback": "<if rejected, explain what is wrong; if approved, empty string>", "learning_rule": {"rule_text": "<single-line rule>", "reasoning": "<why the rule is needed>"} or null}

Follow the rules in AGENTS.md, and these rules for this flow:

{{> rules/rejection}}
