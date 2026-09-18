Investigate the DLT failure for case {{ref_id}}.

Read the case evidence:
- local_casesheets/casebook_{{ref_id}}/dlt_evidence.txt (full evidence block)
- local_casesheets/casebook_{{ref_id}}/dlt_failure.json (parsed failure details)

Understand the service using the documentation corpus in docs_cache/:
- Use Glob to find module docs: Glob docs_cache/enu-biometric/docs/modules/*<ClassName>*
- Use Grep to search for error codes or method names across the corpus
- Read architecture docs: docs_cache/enu-biometric/docs/architecture/components.md
- Read dataflow: docs_cache/enu-biometric/docs/architecture/dataflow.md

Analyze why the dead-lettered message failed. Cross-reference the stack trace
and logs with the service documentation to pinpoint the exact failure. Cite
specific log lines and documentation references.

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"investigation": "<your detailed analysis text>", "citations": [<list of cited evidence>]}

Follow the rules in AGENTS.md.
