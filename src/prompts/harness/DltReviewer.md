Review the DLT investigation for case {{ref_id}}.

Read the investigation:
- local_casesheets/casebook_{{ref_id}}/dlt_investigation_text.txt

Read the case evidence:
- local_casesheets/casebook_{{ref_id}}/dlt_failure.json (parsed failure details)
- local_casesheets/casebook_{{ref_id}}/dlt_evidence.txt (full evidence block, including logs)

## Reasoning hierarchy

The PRIMARY source of truth for WHY a packet crashed is the combination of
the stack trace and the service documentation. Logs are corroborating evidence.
An investigation that correctly reasons from the code and stack trace is valid
even when logs are unavailable — the Reviewer must not reject it merely for
lacking log citations when no logs were available.

## STEP 1 — Confirm which service(s) the investigation references

- List the available services: Glob docs_cache/*/docs/architecture/components.md
- Read docs_cache/MANIFEST.json for the full service list
- Check that the investigation identified the correct service(s) from the
  evidence (stack trace packages, origin topic, exception FQCN)

## STEP 2 — Verify claims against the documentation

- Read architecture: docs_cache/<service>/docs/architecture/components.md
- Read dataflow: docs_cache/<service>/docs/architecture/dataflow.md
- Read packet flows: docs_cache/<service>/docs/ontology/flows.md
- Read error paths: docs_cache/<service>/docs/ontology/error_paths.md
- Use Glob to find module docs by class name from the stack trace:
  Glob docs_cache/<service>/docs/modules/*<ClassName>*
- Use Grep to search for the exception type, business code, or method names
  across the entire corpus

## STEP 3 — Check for these common errors

1. Misinterpretation of the exception chain (the LAST entry is the root cause).
2. Claims not grounded in the stack trace or documentation. Every claim about
   what the code does must cite the doc.
3. Claims about service behaviour that contradict the documentation.
4. Corroboration verdict ignored (CORROBORATED vs CONTRADICTED vs UNVERIFIABLE).
5. Claims not grounded in logs (when logs ARE available): if logs were
   available, verify cited log lines actually exist. If logs were NOT
   available, do NOT reject the investigation for lacking log citations —
   verify instead that it stated this plainly.
6. Wrong service identified: verify the investigation attributed the failure
   to the correct service.
7. **Packet-specific values in the narrative or recommendation**: the finding
   will be cached and re-served verbatim to every future packet with the same
   fingerprint. REJECT if the narrative or recommendation names specific
   identifiers from the payload, exact counts, specific data values, or any
   other value that belongs to this particular packet and would be wrong for
   a different one. The narrative must describe the *shape* of the failure
   ("the response contained multiple items, and at least one item's database
   record was absent"), not the values ("3 items, item abc-123 was missing").
   The recommendation must be actionable for any packet with this fingerprint.

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"verdict": "APPROVED" or "REJECTED", "feedback": "<if rejected, explain what is wrong; if approved, empty string>"}

Follow the rules in AGENTS.md.
