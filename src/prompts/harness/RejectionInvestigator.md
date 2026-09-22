Investigate the rejection for event {{event_id}}.

Read these files for the case evidence:
- local_casesheets/casebook_{{event_id}}/context.json (payload, enrolment type, DB rule)
- local_casesheets/casebook_{{event_id}}/supported_logs.txt (the log trace — may be absent or incomplete)

## Reasoning hierarchy

The PRIMARY source of truth for WHY a packet was rejected is the combination
of the DB rule and the service documentation — NOT the logs. The reason code
in the payload and the DB rule in context.json tell you which business rule
fired. The service documentation in docs_cache/ tells you what the code does,
what conditions trigger that rule, and what the code was doing when it fired.
Logs are CORROBORATING evidence: they confirm what happened at runtime, pin
down the exact timestamp, and may reveal contributing factors the code alone
cannot surface. But the reasoning chain — why this rule exists, what it
checks, and why the packet failed it — comes from the code and the DB rule.

This means you can and should produce a complete investigation even when logs
are unavailable, incomplete, or disabled. In that case, state plainly that
the reasoning is based on the service documentation and DB rule, and that
runtime logs were not available to corroborate it.

## STEP 1 — Read the case evidence

Read context.json first. It contains:
- The Kafka payload (eventId, packetMetaData, flowMetaData.stage, etc.)
- The enrolment type (N = new enrolment, U = biometric update)
- The DB rule configuration — this is the business rule that rejected the packet

Then read supported_logs.txt if it exists. If it is absent, empty, or contains
"Log fetching disabled." / "No logs found" — proceed without it.

## STEP 2 — Discover which service(s) are involved

- List the available services: Glob docs_cache/*/docs/architecture/components.md
- Read docs_cache/MANIFEST.json for the full service list with file counts
- Identify the relevant service(s) from the evidence:
  - The payload's flowMetaData.stage or service field
  - The Kafka topic names in the payload (e.g. ENU.BIO.* points to enu-biometric)
  - The reason code and DB rule — they reference the service that rejected the packet
  - If logs are available, the log trace's app_name or pod names
  - A packet may traverse multiple services; investigate all that appear in the evidence

## STEP 3 — Explore the documentation for the identified service(s)

- Read architecture: docs_cache/<service>/docs/architecture/components.md
  (module-level decomposition — every module and its responsibility)
- Read dataflow: docs_cache/<service>/docs/architecture/dataflow.md
  (Kafka topic chains — entrypoint to sink)
- Read packet flows: docs_cache/<service>/docs/ontology/flows.md
  (the packet's journey through the service — which step it was at when it failed)
- Read error paths: docs_cache/<service>/docs/ontology/error_paths.md
  (known failure paths, DLT triggers, retry chains)
- Use Glob to find module docs by class name:
  Glob docs_cache/<service>/docs/modules/*<ClassName>*
- Use Grep to search for the reason code, error codes, method names, or exception
  types across the entire corpus (all services at once)

The documentation will tell you:
- What the code does at the stage where the packet was rejected
- What conditions cause the reason code to be thrown
- What the DB rule means and how it is applied
- Which module and method handles that stage

## STEP 4 — Analyse and corroborate

Build your reasoning from the DB rule and service documentation first:
- What does the reason code mean?
- What does the DB rule configuration say?
- What was the code doing when it applied this rule?
- Why would a packet with this enrolment type hit this rule?

Then, IF logs are available, corroborate your reasoning:
- Do the logs show the rejection happening at the stage the documentation predicts?
- Do the log lines confirm the specific condition the rule checks?
- Are there contributing factors (timeouts, missing data, dependency failures)
  visible in the logs that the code alone would not reveal?

Enrolment Type: {{etype_display}}

## STEP 5 — Write the investigation

If logs WERE available: cite both the documentation references and the specific
log lines that corroborate them.

If logs were NOT available: state your reasoning from the DB rule and service
documentation, and end with a clear statement such as:
"Note: Runtime logs were not available for this packet. This analysis is based
on the service documentation and the DB rule configuration. The rejection
reason and its cause are derived from the code's documented behaviour."

CRITICAL: You MUST write your output to EXACTLY this file path:
  {{output_path}}
Do NOT write to any other filename (not findings.json, not output.json).
The file MUST be named investigation.json at the path above.
Write a JSON object with this schema:
{"investigation": "<your detailed analysis text>", "citations": [<list of cited evidence>]}

Follow the rules in AGENTS.md, and these rules for this flow:

{{> rules/rejection}}
