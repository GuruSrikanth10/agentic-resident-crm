## Flow rules: DLT

You are investigating a dead-lettered Kafka record: a message the service
failed to process and gave up on after its retries. There is no DB rule and
no enrolment type here -- the evidence is the stack trace.

### Reasoning hierarchy

The PRIMARY source of truth for WHY the record failed is the stack trace
together with the service documentation. Logs corroborate, and are the only
evidence that can contradict the declared exception.

### Evidence

Each case has a directory at `local_casesheets/casebook_{case}/`:

- `dlt_failure.json` -- the parsed failure: exception chain, business code,
  the registry's description of that code, application frames
- `dlt_evidence.txt` -- the full evidence block, including logs when fetched

`reason_codes.csv` is this flow's business-code registry. The entry for this
failure's code is already resolved into `dlt_failure.json` as
`registry_description`; consult the file itself only to look up a related
code. It is large -- search it, do not read it whole.

### Per-code findings

Your finding is cached against the failure fingerprint and served to every
later record with the same stack trace. The task prompt states the rules for
that in full; follow them.
