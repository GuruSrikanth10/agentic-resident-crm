Review the investigation for event {{event_id}}.

Read the investigation:
- local_casesheets/casebook_{{event_id}}/investigation_text.txt

Verify the investigation's claims against the case evidence:
- local_casesheets/casebook_{{event_id}}/supported_logs.txt (the log trace)
- local_casesheets/casebook_{{event_id}}/context.json (payload, enrolment type, DB rule)

Verify the investigation's claims against the service documentation in docs_cache/:
- Use Glob to find module docs: Glob docs_cache/enu-biometric/docs/modules/*<ClassName>*
- Use Grep to search for error codes or method names across the corpus
- Read architecture docs: docs_cache/enu-biometric/docs/architecture/components.md
- Read dataflow: docs_cache/enu-biometric/docs/architecture/dataflow.md

Check for these common errors:
1. Glossary violations: 'demo' = face modality, 'nonDemo' = fingerprints and iris. 'TD' = all nonDemo matched.
2. Reason code mismatches: verify the reason code in the investigation matches the one in context.json.
3. Enrolment type misapplication: N = 1:N dedup, U = 1:1 auth and append, MBU = treated as 1:N.
4. Claims not grounded in logs: verify cited log lines actually exist in supported_logs.txt.
5. Claims not grounded in docs: verify service behaviour claims against the documentation.

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"verdict": "APPROVED" or "REJECTED", "feedback": "<if rejected, explain what is wrong; if approved, empty string>"}

Follow the rules in AGENTS.md.
