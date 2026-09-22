Investigate the DLT failure for case {{ref_id}}.

Read the case evidence:
- local_casesheets/casebook_{{ref_id}}/dlt_failure.json (parsed failure details — exception chain, business code, frames)
- local_casesheets/casebook_{{ref_id}}/dlt_evidence.txt (full evidence block, including logs)

## Reasoning hierarchy

The PRIMARY source of truth for WHY a packet crashed is the combination of
the stack trace and the service documentation — NOT the logs. The stack trace
tells you which exception was thrown and where. The service documentation in
docs_cache/ tells you what the code does at that location, what conditions
trigger that exception, and what the code was doing when it failed. Logs are
CORROBORATING evidence: they confirm what happened at runtime, may reveal
contributing factors (timeouts, missing data, dependency failures) that the
code alone cannot surface, and may contradict the declared exception (the
mis-cast detector). But the reasoning chain — what broke and why — comes
from the stack trace and the service documentation.

This means you can and should produce a complete investigation even when logs
are unavailable, incomplete, or absent. In that case, state plainly that the
reasoning is based on the stack trace and service documentation, and that
runtime logs were not available to corroborate it.

## STEP 1 — Read the case evidence

Read dlt_failure.json first. It contains:
- The root exception FQCN and message
- The business code (if present)
- The exception chain (outermost first; the LAST entry is the root cause)
- Application frames at the failure site
- The origin topic and payload type

Then read dlt_evidence.txt, which includes the logs. If the logs section is
absent, empty, or says "(no logs were fetched)" — proceed without them.

## STEP 2 — Discover which service(s) are involved

- List the available services: Glob docs_cache/*/docs/architecture/components.md
- Read docs_cache/MANIFEST.json for the full service list with file counts
- Identify the relevant service(s) from the evidence:
  - The stack trace's Java package names (e.g. com.uidai.enu.biometric -> enu-biometric)
  - The origin topic and DLT topic names (e.g. ENU.BIO.* -> enu-biometric)
  - The exception FQCN and the classes in the call chain
  - If logs are available, the log lines' app_name or pod names
  - A packet may traverse multiple services; investigate all that appear in the evidence

## STEP 3 — Explore the documentation for the identified service(s)

- Read architecture: docs_cache/<service>/docs/architecture/components.md
  (module-level decomposition — every module and its responsibility)
- Read dataflow: docs_cache/<service>/docs/architecture/dataflow.md
  (Kafka topic chains — entrypoint to sink)
- Read packet flows: docs_cache/<service>/docs/ontology/flows.md
  (the packet's journey through the service — which step it was at when it failed)
- Read error paths: docs_cache/<service>/docs/ontology/error_paths.md
  (known failure paths, DLT triggers, retry chains — directly relevant to DLT analysis)
- Use Glob to find module docs by class name from the stack trace:
  Glob docs_cache/<service>/docs/modules/*<ClassName>*
- Use Grep to search for the exception type, business code, method names, or
  error messages across the entire corpus (all services at once)

The documentation will tell you:
- What the code does at the frame where the exception was thrown
- What conditions cause that exception type to be raised
- What the business code means (if the registry description is present)
- Which module and method handles that stage of the pipeline

## STEP 4 — Analyse and corroborate

Build your reasoning from the stack trace and service documentation first:
- What exception was thrown, and what does the code do at that location?
- What condition triggered it (based on the documented behaviour)?
- What was the code trying to do when it failed?
- Is this a business exception (expected rejection) or a technical fault (bug)?

Then, IF logs are available, corroborate your reasoning:
- Do the logs show the failure happening at the stage the documentation predicts?
- Do the log lines confirm the specific condition that the code checks?
- Are there contributing factors (timeouts, missing data, dependency failures)
  visible in the logs that the code alone would not reveal?
- Do the logs AGREE with the declared exception, or do they CONTRADICT it?
  A contradiction (the mis-cast case) is the single most valuable finding.

## STEP 5 — Write the investigation

### PER-CODE, NOT PER-PACKET — READ THIS BEFORE WRITING

Your narrative and recommendation will be stored against this failure's
fingerprint and **re-served verbatim to every future packet** that hits the
same stack trace. A different packet will have different data values,
different identifiers, and different counts. If your narrative names any
value from this specific packet, it will be **wrong** for every packet
after this one.

Rules:
- **Never include packet-specific values**: identifiers from the payload
  (refIds, record keys, UIDs), counts and quantities (number of items,
  matches, retries), specific data values (scores, thresholds, field
  values), or timestamps from this packet's processing.
- **Describe the shape, not the values.** Instead of "the response contained
  3 items and item abc-123 was missing from the database", write "the
  response contained multiple items, and at least one item's database
  record was absent". Instead of "the service processed 5 records before
  failing on the 3rd", write "the service processes a collection of records
  and fails when one does not meet a condition".
- **The recommendation must be actionable for ANY packet with this
  fingerprint.** Instead of "Query the database for items abc-123, def-456
  and check which is missing", write "Query the database for each item
  referenced in the response and confirm which record is absent".
- **You may use the current packet's values to understand the failure**,
  but strip them from the narrative and recommendation before writing.

### Output format

If logs WERE available: cite both the documentation references and the specific
log lines that corroborate them.

If logs were NOT available: state your reasoning from the stack trace and
service documentation, and end with a clear statement such as:
"Note: Runtime logs were not available for this packet. This analysis is based
on the stack trace and the service documentation. The failure cause is derived
from the code's documented behaviour."

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename.
Write a JSON object with this schema:
{"investigation": "<your detailed analysis text>", "citations": [<list of cited evidence>]}

Follow the rules in AGENTS.md.
