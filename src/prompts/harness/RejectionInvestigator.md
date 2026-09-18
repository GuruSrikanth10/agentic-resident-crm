Investigate the rejection for event {{event_id}}.

Read these files for the case evidence:
- local_casesheets/casebook_{{event_id}}/supported_logs.txt (the log trace)
- local_casesheets/casebook_{{event_id}}/context.json (payload, enrolment type, DB rule)

Understand the service using the documentation corpus in docs_cache/:
- Use Glob to find module docs: Glob docs_cache/enu-biometric/docs/modules/*<ClassName>*
- Use Grep to search for error codes or method names across the corpus
- Read architecture docs: docs_cache/enu-biometric/docs/architecture/components.md
- Read dataflow: docs_cache/enu-biometric/docs/architecture/dataflow.md

Enrolment Type: {{etype_display}}

Analyze why the packet was rejected. Cross-reference the logs with the
service documentation to pinpoint the exact failure. Cite specific log
lines and documentation references.

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename (not findings.json, not output.json).
The file MUST be named investigation.json at the path above.
Write a JSON object with this schema:
{"investigation": "<your detailed analysis text>", "citations": [<list of cited evidence>]}

Follow the rules in AGENTS.md.
