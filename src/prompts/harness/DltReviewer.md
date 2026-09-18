Review the DLT investigation for case {{ref_id}}.

Read the investigation:
- local_casesheets/casebook_{{ref_id}}/dlt_investigation_text.txt

Verify the investigation's claims against the case evidence:
- local_casesheets/casebook_{{ref_id}}/dlt_evidence.txt (full evidence block)
- local_casesheets/casebook_{{ref_id}}/dlt_failure.json (parsed failure details)

Verify the investigation's claims against the service documentation in docs_cache/:
- Use Glob to find module docs: Glob docs_cache/enu-biometric/docs/modules/*<ClassName>*
- Use Grep to search for error codes or method names across the corpus
- Read architecture docs: docs_cache/enu-biometric/docs/architecture/components.md
- Read dataflow: docs_cache/enu-biometric/docs/architecture/dataflow.md

Check for these common errors:
1. Claims not grounded in the stack trace or logs.
2. Misinterpretation of the exception chain (the LAST entry is the root cause).
3. Claims about service behaviour that contradict the documentation.
4. Corroboration verdict ignored (CORROBORATED vs CONTRADICTED vs UNVERIFIABLE).

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"verdict": "APPROVED" or "REJECTED", "feedback": "<if rejected, explain what is wrong; if approved, empty string>"}

Follow the rules in AGENTS.md.
