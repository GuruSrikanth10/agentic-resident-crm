# Rejection Lane Without opencode -- Reason-Code Documentation Plan

- **Date:** 2026-09-22
- **Status:** Proposed. Nothing in this plan is implemented yet.
- **Scope:** the rejection lane. The DLT lane keeps the opencode harness; the
  only DLT change is that its switch gets its own name (Phase 1).
- **Audience:** the agent or engineer implementing this, one phase at a time.

---

## 0. How to use this plan

1. Read the whole plan before changing anything. Sections 1-5 are reference
   material; section 6 is the work, in phases.
2. Do the phases in order. Each ends with a **checkpoint**: stop, report what
   changed and the test results, and wait for the owner before the next phase.
3. The repository rules in `.agents/AGENTS.md` apply throughout:
   - Update `ARCHITECTURE.md` in the same phase as the code it describes.
   - Never run `git commit` or `git push` unless the owner explicitly asks.
   - No emojis anywhere: code, logs, comments, prompts, documents.
4. Section 2 was verified on 2026-09-22. Re-check each fact before relying on
   it (grep hints are given), and refer to code by symbol name, not line
   number; line numbers drift.
5. Match the surrounding code: its comment density, its naming, the
   `get_bool_env` helper, the `_counter` metric helper, and the argparse shape
   of the CLIs in `src/tools/`.
6. If a fact in section 2 is wrong, or a decision in section 3 cannot be
   implemented as written, stop and ask. Do not improvise around it.

### Running the tests

- Command: `.venv/bin/python -m pytest -q -p no:cacheprovider -rf`
- About 56 tests fail **before** any of this work (DLT lane tests, a `park()`
  signature change, casebooks loading as `None`). It is 58 on a tree without
  the uncommitted harness fixes of 2026-09-22. Save the failing test ids
  before Phase 1 and compare after every phase. A phase is done when no test
  that passed before now fails, and the phase's own new tests pass. Do not try
  to fix the pre-existing failures as part of this plan.
- `tests/test_resilience.py::test_concurrent_add_learning_rule` and
  `tests/test_phase2_fixes.py::test_add_learning_rule_uses_per_packet_contextvars`
  delete the tracked file `src/prompts/pending_rules.jsonl`. After every full
  run: `git checkout -- src/prompts/pending_rules.jsonl`.

---

## 1. Goal

With the rejection lane's opencode harness switched off, the rejection
pipeline should still reason from documentation. It should use curated
per-reason-code documents rather than an agent exploring the whole corpus:

- The **Investigator** receives the document mapped to the packet's reason
  code and enrolment type, plus the DB rule, the payload and the logs, in one
  direct LLM call.
- The **Reviewer** receives the same evidence, so it can check the
  investigation instead of only reading it.
- Optionally, **Synthesis** receives the document's resolution guidance.
- The prompt stays inside the model's context window, the casebook records
  exactly which document was used, and every behaviour change sits behind a
  switch with a safe default.

Non-goals (see section 9):
- Changing the DLT lane beyond its switch.
- Removing opencode or the rejection harness code.
- Deterministic extraction of facts from logs.
- Writing the real reason-code documents; the owner authors them.
- Hosting the documents in S3, or vector search.

---

## 2. Current state (verified 2026-09-22)

### Harness switch
- There is one switch, `USE_OPENCODE_HARNESS`, read through
  `src/utils/opencode_runner.py::is_enabled()`. It is true only when the value,
  lowercased, is exactly `true`; surrounding whitespace is not stripped.
- It is read by:
  - `src/core/agent_orchestrator.py`: `investigator_node` and `reviewer_node`.
  - `src/dlt/orchestrator.py`: `investigator_node` and `reviewer_node`.
  - `src/main_api.py::lifespan`, which starts the corpus download and
    `opencode serve` on a background thread.
  - The readiness handler in `src/api/routes.py`, which returns 503 while the
    corpus downloads or the server boots.
  - `start.py`, which reads the variable itself and waits on `/ready`.
  - `entrypoint.sh`, which writes `~/.config/opencode/config.json`. It compares
    against the exact lowercase string `true`, so `True` turns the Python side
    on and leaves the shell side off.
- `run_task()` refuses to run unless `is_enabled()` is true.
- Grep: `grep -rn "USE_OPENCODE_HARNESS\|is_enabled as harness_enabled\|harness_enabled()" src start.py entrypoint.sh`

### Corpus
- The DROA corpus is downloaded (`docs_loader.download_corpus`) only inside
  the harness start-up thread in `main_api.lifespan`. With the harness off it
  never reaches the pod.
- This plan does not need the corpus: the reason-code documents live in the
  repository (decision D2).

### Direct (non-harness) rejection prompts, built in `agent_orchestrator._build_agent`
- **Investigator, first pass:** `Kafka Payload: ...`, `Enrolment Type: ...`,
  `Elasticsearch Logs: ...`, then `Database Rule Configuration:\n...`, in that
  order. The logs part is left out when the logs are empty or equal
  `Log fetching disabled.`.
- **Investigator, retry:** `Your previous analysis`, `Reviewer Feedback`,
  `Elasticsearch Logs`. It has no rule and no payload.
  `tests/test_phase2_fixes.py::test_investigator_prompt_trims_context_on_retry`
  pins this shape.
- **Reviewer:** `Validate this investigation:\n{investigation}` plus the
  APPROVED instruction.
  - It receives no rule, no payload and no logs.
  - Yet `ReviewerAgent.md` asks it to check against the payload and the
    evidence-gaps banner.
- **Synthesis:** receives only the approved investigation.
- **System prompts:** the Investigator, Reviewer and Synthesis get
  `agent_policy_context.md` appended (`load_prompt(..., with_policy=True)`).
  Line 34 of that file says "Use the `lookup_rule_by_reason_code` tool", but
  the Investigator has no tools.
- **Existing helpers:** `_project_payload`, `enrolment_type_display` /
  `ENROLMENT_TYPE_DISPLAY`, `is_reviewer_approved` (the verdict must start
  with `APPROVED`), `_counted`, `llm_breaker`, `retry_transient`.
- **Graph node names:** `investigate`, `review`, `synthesize`, `escalate`.

### Graph state
- `GraphState` is a `TypedDict`. LangGraph only carries keys declared on it,
  so **every new state key must be declared there**.
- The checkpointer is keyed by eventId. Re-running the same eventId resumes
  the stored state.

### Logs
- The log pipeline caps its output at `LOG_MAX_REDUCED_CHARS` (default 120000)
  by trimming the middle (`src/log_pipeline/pipeline.py::_bound_total_size`).
- Values stored in place of a trace:
  - `Log fetching disabled.` (from `tool_registry.fetch_and_persist_logs`).
  - `No logs found for ID: <id>` (from the pipeline), possibly preceded by the
    evidence-gaps banner.
  - `None`, when the fetch failed.
- `src/models/synthesis.py::_confidence_ceilings` already classifies these
  for the confidence policy (no logs, logs had nothing, gaps).

### Enrolment type
- `src/tools/tool_registry.py::normalize_enrolment_type` maps `N`, `E`,
  `ENROLMENT` and `ENROLLMENT` to `ENROLMENT`; `U` and `UPDATE` to `UPDATE`;
  anything else (for example `Z`) to `None`.
- Runbooks (`src/utils/runbook_store.py`):
  - Files are keyed by the raw uppercased type (`E`, `U`, `N`), with an `ANY`
    fallback.
  - Reason codes are checked against `REASON_CODE_PATTERN`
    (`^[A-Za-z0-9_.:-]{1,128}$`).
  - 37 reason codes have draft runbooks: 15 `E` files and 24 `U` files.

### Provenance, outcomes, metrics, validation, tests
- **Casebook provenance:** built in `src/api/routes.py` as
  `casebook_data["resolution"]["provenance"] = {"prompt_fingerprint": prompt_fingerprint()}`.
  The `result` used there is the final graph state dict.
- **Outcomes:** `src/utils/outcomes.py` copies `prompt_fingerprint` into each
  outcome record.
- **Metrics:** defined in `src/utils/metrics.py` with
  `_counter(name, doc, labels)`, named `agentic_resident_crm_<thing>_total`.
- **Text checks:** `src/utils/runbook_validator.py::validate_generic_text(text, source_values)`
  rejects UUIDs, timestamps and dates, and runs of 10 or more digits. The same
  file defines `INJECTION_MARKERS`.
- **Allowed action values:** `src/models/synthesis.py::ACTIONS` and
  `RESIDENT_ACTIONS`.
- **Start-up validation:** `src/utils/config_validator.py::validate_config()`
  collects errors and calls `sys.exit(1)`. It runs when `main_api` is imported
  and in every consumer.
- **Test environment:** `tests/conftest.py` stubs `load_dotenv` and removes
  `ISOLATED_ENV_VARS` for the session. `USE_OPENCODE_HARNESS` is not in that
  list.
- **Test pattern for driving one graph node:** see `tests/test_opencode_harness.py`
  (`_StubAgent`, `_rejection_reviewer`, `_harness_on`). It builds the graph
  with `get_llm`, `create_react_agent` and `get_checkpointer` patched, then
  calls `graph.builder.nodes["<node>"].runnable.func(state)`.

### Known issue that blocks Phase 9 (not fixed by this plan)
- `src/api/routes.py` calls `storage.save_terminal(...)` and then
  `cleanup_casebook_dir(event_id)`.
- With `CASEBOOK_STORAGE_BACKEND=local` (the default),
  `LocalFilesystemCasebookStorage` stores casebooks in
  `LOCAL_CASESHEETS_DIR/casebook_<id>/`. That is exactly the directory
  `cleanup_casebook_dir` deletes.
- So under the local backend a finished casebook appears to be deleted as soon
  as it is saved. This is probably behind many of the pre-existing `None`
  casebook test failures.
- Phase 0 verifies it; the owner decides the fix.

---

## 3. Decisions

**D1. One switch per lane, falling back to the old switch.**
- The new switches are `USE_OPENCODE_HARNESS_REJECTION` and
  `USE_OPENCODE_HARNESS_DLT`.
- A lane's own switch wins when its value is non-empty after stripping
  whitespace. Otherwise the lane inherits `USE_OPENCODE_HARNESS`; if that is
  unset too, the lane is off.
- A value counts as on when, stripped and lowercased, it equals `true`. The
  one change from today is that surrounding whitespace is now ignored, on both
  the Python side and the shell side.
- `is_enabled()` keeps its name and now means "some lane uses the harness".
  That is exactly the condition for starting the server, downloading the
  corpus, gating `/ready`, and writing the provider config.
- Deployments and tests that set only `USE_OPENCODE_HARNESS` behave as before.

**D2. The documents live in the repository, under `src/reason_code_docs/`.**
- They are reviewed in pull requests and versioned with the code that reads
  them.
- They ship through the existing `COPY src /app/src`, need no S3 access or
  start-up download, and can be tested in CI.
- The cost is that editing a document needs a deploy. That is acceptable:
  the DROA corpus they are derived from also changes on deploy.
- `REASON_CODE_DOCS_DIR` can point elsewhere (for example a mounted volume)
  without code changes. Hosting in S3 is a follow-up (section 9).

**D3. One index file maps reason codes to documents.**
- The file is `src/reason_code_docs/index.json`. It is JSON because PyYAML is
  not a dependency.
- Each reason code has a list of entries. Each entry names the enrolment types
  it covers and the documents to send.
- This covers every shape the owner asked for:
  - reason code + E, or reason code + U: an entry with one type;
  - reason code + E/U: one entry listing both;
  - reason code for any type: an entry with `ANY`.

**D4. Enrolment types are matched by family, not by raw value.**
- The payload value is normalised with `tool_registry.normalize_enrolment_type`:
  - `N`, `E`, `ENROLMENT`, `ENROLLMENT` become `E`;
  - `U`, `UPDATE` become `U`;
  - any other value is used raw and uppercased (for example `Z`);
  - a missing value matches only `ANY`.
- The lookup tries the normalised type first, then `ANY`. Authors only ever
  write `E`, `U` or `ANY` (or a raw type such as `Z`), and never need an `N`
  entry.
- This deliberately differs from runbook file keys, which are raw. A runbook
  is a stored answer, while a document describes a policy, and `N` and `E` are
  the same policy (the rule lookup already treats them as one).
- `N`, `ENROLMENT`, `ENROLLMENT` and `UPDATE` are rejected in the index,
  because they could never match.

**D5. A document reference is a file, or a file plus one section.**
Sections are chosen by exact heading text. Authors should write one
self-contained file per entry, and use sections only to pull a shared passage
from a common file.

**D6. One direct LLM call per step, no agent.**
Finding the document is a lookup done in code. The Investigator, Reviewer and
Synthesis each stay a single call, as they are today on the direct path.

**D7. With the switch off, the Investigator's prompts are exactly today's.**
With `REJECTION_REASON_CODE_DOCS_ENABLED=false`, the Investigator's first-pass
and retry prompts are character-for-character what they are now. The only
exception to "off means today" is the Reviewer (D8).

**D8. The Reviewer sees the evidence by default.**
- A Reviewer that sees only the investigation text is a defect in the direct
  path, not a feature.
- `REJECTION_REVIEWER_EVIDENCE` defaults to `true`; setting it to `false`
  restores today's Reviewer prompt.
- This is the only change that is on by default. It roughly doubles the
  Reviewer's input tokens, because the logs are included.
- The owner confirms this default (section 10).

**D9. The document is chosen once per packet.**
- The lookup runs on the Investigator's first pass. Its result, including the
  rendered text, is stored in graph state.
- Retries, the Reviewer and Synthesis reuse it, so one packet is never
  reasoned about with two versions of a document.
- If a retry finds no entry in state (a checkpoint written before this
  change), it does the lookup then.

**D10. A document problem never fails a packet; a broken index stops start-up.**
- At run time, any lookup problem becomes outcome `error`. It is logged and
  counted, and the packet carries on without the document.
- At start-up, when `REJECTION_REASON_CODE_DOCS_ENABLED=true`, the API
  validates the index and exits on errors, following `validate_config`'s
  convention.
- Only the API does this check, because the consumers never read documents.

**D11. The size limit trims logs, never documentation or rules.**
`REJECTION_PROMPT_MAX_CHARS` caps the user message. When the prompt is too
big, only the logs are trimmed, from the middle, with a marker. The document,
the rule and the investigation are never cut.

**D12. Synthesis guidance has its own switch, off by default.**
`REJECTION_SYNTHESIS_DOC_GUIDANCE` exists so that its effect on the action and
resident action can be measured on its own.

**D13. The rejection harness path is not changed.**
When `USE_OPENCODE_HARNESS_REJECTION=true`, the harness runs exactly as it
does now and is not given the documents. The direct path uses them, including
the fallback after a harness failure. This keeps the comparison in Phase 9
clean.

**D14. The document's fingerprint is recorded per packet, not folded into the prompt fingerprint.**
- Each casebook records the SHA-256 of the exact document text the model
  received.
- `compute_prompt_fingerprint` is left as it is, apart from reflecting the
  prompt files this plan edits.
- That way a document edit and a prompt edit stay distinguishable.

---

## 4. Configuration

| Variable | Default | Read by | Meaning |
|---|---|---|---|
| `USE_OPENCODE_HARNESS` | `false` | `opencode_runner`, `entrypoint.sh` | The old single switch. Used by either lane whose own switch is unset. |
| `USE_OPENCODE_HARNESS_REJECTION` | inherits the old switch | `opencode_runner.lane_enabled("rejection")` | Rejection Investigator and Reviewer run on opencode. |
| `USE_OPENCODE_HARNESS_DLT` | inherits the old switch | `opencode_runner.lane_enabled("dlt")` | DLT Investigator and Reviewer run on opencode. |
| `REJECTION_REASON_CODE_DOCS_ENABLED` | `false` | `reason_code_docs.docs_enabled()` | The direct Investigator uses the reason-code documents. |
| `REASON_CODE_DOCS_DIR` | `<repo>/src/reason_code_docs` | `src/utils/paths.py` | Folder holding `index.json` and `docs/`. |
| `REASON_CODE_DOC_MAX_CHARS` | `16000` | `reason_code_docs` | Cap on one entry's rendered text. The validator enforces it; at run time it truncates as a safety net. |
| `REJECTION_PROMPT_MAX_CHARS` | `200000` | `rejection_context` | Cap on the user message of each direct Investigator or Reviewer call. |
| `REJECTION_REVIEWER_EVIDENCE` | `true` | `agent_orchestrator.reviewer_node` | The direct Reviewer gets the rule, payload, logs and document. |
| `REJECTION_SYNTHESIS_DOC_GUIDANCE` | `false` | `agent_orchestrator.synthesis_node` | Synthesis gets the document's resolution guidance. |

- The three new on/off switches (`REJECTION_REASON_CODE_DOCS_ENABLED`,
  `REJECTION_REVIEWER_EVIDENCE`, `REJECTION_SYNTHESIS_DOC_GUIDANCE`) use
  `src/utils/env.py::get_bool_env`, which accepts `true`, `1` and `yes`, like
  the other `ENABLE_*` switches.
- The harness switches follow D1 instead, because `entrypoint.sh` must reach
  the same answer as Python.
- Add every new variable to `tests/conftest.py::ISOLATED_ENV_VARS` in the
  phase that introduces it.

**Choosing `REJECTION_PROMPT_MAX_CHARS`**
- Formula: roughly `(max_model_len - max_output_tokens - 4000) * 3`.
  - The 4000 tokens cover the system prompt with the policy appended, which is
    about 11000 characters.
  - 3 characters per token is a conservative figure for logs.
- For a 131072-token window with 8192 output tokens, that gives about 357000.
  The default of 200000 is deliberately below it.
- A normal worst case is about 160000 characters: 120000 of logs, 16000 of
  documentation, plus the rule, the payload, the investigation (for the
  Reviewer) and the task. So the default only trims in unusual cases.
- For a smaller model window, lower the value using the formula.

---

## 5. The reason-code document contract

### 5.1 Layout

```
src/reason_code_docs/
  index.json            the mapping (5.2)
  README.md             authoring guide; never referenced by the index
  docs/                 every referenced file lives under here
    <REASON_CODE>__E.md
    <REASON_CODE>__U.md
    <REASON_CODE>.md
    shared/<topic>.md   passages referenced by several codes
```

The file names are a convention for people; only the index decides what is
sent to the model.

### 5.2 `index.json`

```json
{
  "schema_version": 1,
  "reason_codes": {
    "RESIDENT_MAN_DEDUP_REJECT_TD": [
      {
        "enrolment_types": ["E"],
        "docs": [
          {"path": "docs/RESIDENT_MAN_DEDUP_REJECT_TD__E.md"},
          {"path": "docs/shared/biometric_dedup.md", "section": "Modality terminology"}
        ]
      },
      {
        "enrolment_types": ["U"],
        "docs": [{"path": "docs/RESIDENT_MAN_DEDUP_REJECT_TD__U.md"}]
      }
    ],
    "PKT_URI_AND_ENRCODE_MISMATCH": [
      {"enrolment_types": ["ANY"], "docs": [{"path": "docs/PKT_URI_AND_ENRCODE_MISMATCH.md"}]}
    ]
  }
}
```

The validator (Phase 2) treats each of these as an **error**:
1. `schema_version` is not `1`, or there are top-level keys other than
   `schema_version` and `reason_codes`.
2. A reason code key does not match `runbook_store.REASON_CODE_PATTERN`.
3. A reason code maps to an empty list; or an entry does not have exactly the
   keys `enrolment_types` and `docs`; or either list is empty.
4. An enrolment type is neither `ANY` nor a match for `^[A-Z]{1,16}$`, or is
   one of `N`, `ENROLMENT`, `ENROLLMENT`, `UPDATE` (D4).
5. Within one reason code, the same type appears in more than one entry.
6. A document reference has a key other than `path` and the optional
   `section`, or its `path` breaks any of these rules:
   - relative POSIX path starting with `docs/` and ending in `.md`;
   - no `..` segment, no backslash, no leading `/`;
   - after `realpath` it is still inside `<root>/docs`, which catches
     symlinks pointing outside.
7. The file does not exist or is not UTF-8. Or `section` is given and the file
   does not have exactly one heading with that text (5.4).
8. The rendered entry (5.6) is longer than `REASON_CODE_DOC_MAX_CHARS`.
9. The rendered entry breaks the content rules in 5.5.

**Warnings**, not errors:
- A `.md` file under `docs/` that no entry references.
- With `--coverage`: reason codes that have a draft or final runbook but no
  index entry.

### 5.3 Lookup

**Input:** the reason code (the first non-empty
`packetExecutionSummary.errorData[].errorReasonCode`) and the raw
`packetMetaData.enrolmentType`.

1. Switch off: no lookup; the state gets `{"outcome": "disabled"}`.
2. No reason code: outcome `no_reason_code`.
3. The reason code fails `REASON_CODE_PATTERN`: outcome `miss`, with detail
   `invalid reason code` (runbooks treat it the same way).
4. Normalise the enrolment type (D4) into `requested_type`, which may be None.
   A raw value that is neither a known alias nor a match for `^[A-Z]{1,16}$`
   after uppercasing (for example `B/D`) also gives None, so only `ANY` can
   match it.
5. Candidates are `[requested_type, "ANY"]`, dropping None and duplicates.
6. The first candidate listed in some entry's `enrolment_types` picks that
   entry, and that candidate becomes `matched_type`. If no entry matches:
   outcome `miss`.
7. Render the entry (5.6).

Any exception while reading the index or the files gives outcome `error`,
with the exception type and message in `detail`. **The lookup never raises.**

The index and the files are read on every lookup, as `prompt_loader.render`
does. They are small, and edits in a mounted `REASON_CODE_DOCS_DIR` then take
effect without a restart.

### 5.4 Section extraction

- Only ATX headings count: a line matching `^(#{1,6})\s+(.+?)\s*#*\s*$`. The
  heading text is capture group 2, compared exactly (case-sensitive).
- Lines inside fenced code blocks are never headings. A fence opens and closes
  on a line starting with three backticks or three tildes.
- A section starts at its heading line (included). It ends just before the
  next heading of the same or a higher level (the same number of `#` or
  fewer), or at the end of the file.
- If no heading or more than one heading matches, that is an error.

### 5.5 Document template and content rules

This template is also copied into `src/reason_code_docs/README.md`:

```markdown
# <REASON_CODE> -- <New Enrolment (E) | Biometric Update (U) | all types>

## Summary
What this rejection means, in two or three plain sentences.

## Policy
The business rule behind it and why it exists: which success criterion the
packet failed (see agent_policy_context.md, "What does SUCCESS look like?").

## Trigger conditions
When the service raises this code, in terms of the Database Rule
Configuration fields, for example `numberOfCandidatesWithDifferentParent > 0`.

## Evidence to look for in logs
Which service logs the decision, and the shape of the lines that carry the
packet-specific facts, using placeholders only, for example:
`<timestamp> INFO ... refId=<refId> candidate=<candidateRefId> parent=<same|different> modality=<finger|iris|face> score=<score>`
What each fact means for the policy.

## Without logs
What can still be concluded from the rule and this document, and what cannot.

## Resolution guidance
- action: RESIDENT_PACKET_RESUBMIT | resident_action: NEW_PACKET | when: <condition>

## Sources
The DROA documents this was derived from (service and module paths).
```

Content rules:
1. **Generic.** Nothing from a particular packet. The validator runs
   `validate_generic_text(text, [])`, so the document can have no UUIDs, no
   dates or timestamps, and no runs of ten or more digits. Examples use
   placeholders such as `<refId>` and `<timestamp>`.
2. **No prompt-injection phrases.** None of `runbook_validator.INJECTION_MARKERS`,
   compared case-insensitively. Some of them are ordinary phrases ("override
   the"); reword those.
3. **Terminology** follows `agent_policy_context.md`: "demo" is the face
   modality, "nonDemo" is fingerprints and iris, and "TD" means all nonDemo
   modalities matched.
4. **Biometric Update documents** cover the Mandatory Biometric Update case
   (a first-time biometric update is deduplicated like a new enrolment)
   wherever it changes the answer.
5. **Resolution guidance** lines, if present:
   - must match exactly `^- action:\s*([A-Z_]+)\s*\|\s*resident_action:\s*([A-Z_]+)\s*(?:\|\s*when:\s*(.+))?$`;
   - must use values from `synthesis.ACTIONS` and `synthesis.RESIDENT_ACTIONS`;
   - may appear only under a `Resolution guidance` heading.

   Any line starting with `- action:` that does not match is an error.
6. **Short.** The cap is 16000 characters per entry, shared passages included.
   Aim for well under half of that.

### 5.6 Rendering

Each document reference becomes a block, and the blocks are joined with one
blank line:

```
[Source: docs/RESIDENT_MAN_DEDUP_REJECT_TD__E.md]
<the whole file>

[Source: docs/shared/biometric_dedup.md, section "Modality terminology"]
<that section, heading included>
```

- If the joined text is longer than `REASON_CODE_DOC_MAX_CHARS`:
  - keep the start;
  - append `\n\n... <n> characters omitted from the end of the reason-code documentation (REASON_CODE_DOC_MAX_CHARS) ...`;
  - set `truncated`, and log a warning.
- `sha256` is `"sha256:" + hexdigest` of the final text, which is exactly what
  the model sees.
- `resolution_guidance` is parsed from the final text (content rule 5) into a
  list of `{"action", "resident_action", "when"}`.

### 5.7 State and provenance shapes

Stored in `GraphState.reason_code_doc`:

```json
{
  "outcome": "hit",
  "reason_code": "RESIDENT_MAN_DEDUP_REJECT_TD",
  "requested_type": "E",
  "matched_type": "E",
  "refs": [{"path": "docs/RESIDENT_MAN_DEDUP_REJECT_TD__E.md", "section": null}],
  "text": "<rendered text>",
  "sha256": "sha256:...",
  "truncated": false,
  "resolution_guidance": [{"action": "...", "resident_action": "...", "when": "..."}],
  "detail": null
}
```

- `outcome` is one of `hit`, `miss`, `error`, `no_reason_code`, `disabled`.
- For any outcome other than `hit`: `text` and `sha256` are null, and `refs`
  and `resolution_guidance` are empty lists.
- The casebook's provenance copy is the same object without `text` and
  `resolution_guidance`. **The document text is never written to a casebook
  or a log line.**
- The `disabled` state is stored as just `{"outcome": "disabled"}`. So every
  reader (the builders, `provenance`, Synthesis) must use `.get()` with
  defaults, and must treat a partial dict the same as the full shape with
  empty values.

---

## 6. Phases

### Phase 0 -- Prerequisites and baseline

**Goal:** a known starting point.

1. The working tree has uncommitted work on `fix/dlt-group-state-and-dedupe`:
   the opencode harness fixes, the no-logs confidence caps, and this plan file.
   Ask the owner which branch to build on. Do not commit.
2. Run the full suite, save the failing test ids to a scratch file, and restore
   `src/prompts/pending_rules.jsonl`.
3. Re-check the facts in section 2 with the grep hints, and report anything
   that differs.
4. Verify the local-backend cleanup issue (end of section 2):
   - Read `LocalFilesystemCasebookStorage` and `cleanup_casebook_dir`.
   - Process one packet through `/process-rejection` with
     `CASEBOOK_STORAGE_BACKEND=local`.
   - Check whether `casebook_<id>/casebook.json` still exists afterwards.

   Report the result. While the issue exists, Phase 9 cannot use the local
   backend. The owner decides whether to fix it first, or to run Phase 9
   against S3. A likely fix is to skip `cleanup_casebook_dir` when the
   casebook backend is local, because that folder is the store itself.

**Checkpoint:** report the baseline failure count, anything that differs from
section 2, and the cleanup finding.

### Phase 1 -- A separate opencode switch for each lane

**Goal:** the rejection lane can be switched off while the DLT lane stays on
opencode.

**Changes**

1. `src/utils/opencode_runner.py`
   - Keep `ENV_DISABLE = "USE_OPENCODE_HARNESS"` (the old switch; the constant
     keeps its name for compatibility).
   - Add `ENV_LANES = {"rejection": "USE_OPENCODE_HARNESS_REJECTION", "dlt": "USE_OPENCODE_HARNESS_DLT"}`.
   - Add `lane_enabled(lane: str) -> bool`:
     - raise `ValueError` for an unknown lane;
     - if the lane's variable, stripped, is non-empty, return whether it
       lowercases to `true`;
     - otherwise apply the same test to `USE_OPENCODE_HARNESS`, stripped,
       treating unset as `false`.
   - `is_enabled()` returns `any(lane_enabled(lane) for lane in ENV_LANES)`.
     Update its docstring and the module docstring to say it now means "the
     server is needed".
   - `run_task` keeps its `is_enabled()` guard. Update the error text to name
     the lane switches.
2. `src/core/agent_orchestrator.py`: in `investigator_node` and
   `reviewer_node`, replace the `is_enabled as harness_enabled` import and call
   with `lane_enabled("rejection")`.
3. `src/dlt/orchestrator.py`: the same, with `lane_enabled("dlt")`.
4. `src/main_api.py` and the readiness handler in `src/api/routes.py`: keep
   `is_enabled()`, and update the comments to say "any lane".
5. `start.py`: replace the inline `os.environ.get("USE_OPENCODE_HARNESS", ...)`
   with `from src.utils.opencode_runner import is_enabled`, imported inside
   `main()`. The module already calls `load_dotenv()` when imported, before
   `main()` runs. Confirm that the import of `src` works both for
   `python start.py` run from the repo root and in the image, where the working
   directory is `/app` and `PYTHONPATH=/app`.
6. `entrypoint.sh`: resolve the lanes the same way Python does. Put the block
   between marker comments so a test can run it:

   ```bash
   # BEGIN harness-lanes
   norm() { printf '%s' "$1" | tr -d '[:space:]' | tr '[:upper:]' '[:lower:]'; }
   HARNESS_LEGACY="$(norm "${USE_OPENCODE_HARNESS:-}")"
   HARNESS_REJECTION="$(norm "${USE_OPENCODE_HARNESS_REJECTION:-}")"
   HARNESS_DLT="$(norm "${USE_OPENCODE_HARNESS_DLT:-}")"
   [ -n "$HARNESS_REJECTION" ] || HARNESS_REJECTION="$HARNESS_LEGACY"
   [ -n "$HARNESS_DLT" ] || HARNESS_DLT="$HARNESS_LEGACY"
   # END harness-lanes

   if [ "$HARNESS_REJECTION" = "true" ] || [ "$HARNESS_DLT" = "true" ]; then
   ```

   Update the header comment. The script uses `#!/bin/bash` and `set -e`; both
   are fine with this block.
7. `.env.example`, in the opencode section:
   - describe the old switch as the fallback;
   - add both lane switches set to `false`, with a comment giving the target
     production values (section 7).

   Keep them `false` here: a lane switched on without the opencode binary
   leaves `/ready` waiting for a server that never starts.
8. `tests/conftest.py`: add `USE_OPENCODE_HARNESS`,
   `USE_OPENCODE_HARNESS_REJECTION` and `USE_OPENCODE_HARNESS_DLT` to
   `ISOLATED_ENV_VARS`.

**Tests** (in `tests/test_opencode_harness.py`)
- A parametrised table: the old switch unset, `true` or `false`, crossed with
  the lane switch unset, empty, `true`, `TRUE`, ` true ` or `false`. Assert
  `lane_enabled` for both lanes.
- `is_enabled()` is true when either lane is on, and false when both are off.
- `lane_enabled("other")` raises `ValueError`.
- An AST check, in the style of `test_no_harness_call_site_passes_its_own_timeout`:
  - `agent_orchestrator.py` calls `lane_enabled("rejection")` and never calls
    `is_enabled`;
  - `dlt/orchestrator.py` calls `lane_enabled("dlt")` and never calls
    `is_enabled`.
- Shell parity:
  - extract the `harness-lanes` block from `entrypoint.sh`;
  - for each case in the table above, run it with `bash -c` followed by
    `echo "$HARNESS_REJECTION $HARNESS_DLT"`, using that case's environment;
  - assert the result matches `lane_enabled`.

  Skip the test if `bash` is not available.
- `bash -n entrypoint.sh` succeeds.
- Existing tests that set only `USE_OPENCODE_HARNESS` still pass without
  changes.

**Docs:** in `ARCHITECTURE.md` section 3.2.1, describe the lane switches and
what "any lane" starts. In `SYSTEM_OVERVIEW.md` section 7, add one sentence
saying the switch is per lane.

**Done when:** the rejection lane can be off while DLT stays on opencode, the
tests above pass, and nothing new fails.

### Phase 2 -- Document store, loader and validator

**Goal:** documents can be written, validated and looked up. Nothing in the
pipeline uses them yet.

**Changes**

1. `src/utils/paths.py`: add
   `REASON_CODE_DOCS_DIR = Path(os.environ.get("REASON_CODE_DOCS_DIR", REPO_ROOT / "src" / "reason_code_docs"))`,
   with a comment in the style of the neighbouring paths.
2. `src/reason_code_docs/index.json`: `{"schema_version": 1, "reason_codes": {}}`.
   **Do not invent real documents.** The owner writes them.
3. `src/reason_code_docs/README.md`: the authoring guide. It covers sections
   5.1, 5.2, 5.4 and 5.5 of this plan, and how to run the validator.
4. `src/reason_code_docs/docs/.gitkeep`, so the folder exists.
5. `src/utils/reason_code_docs.py` (new), exposing:
   - `SCHEMA_VERSION = 1`, `ANY = "ANY"`.
   - `docs_enabled() -> bool`: `get_bool_env("REJECTION_REASON_CODE_DOCS_ENABLED", False)`,
     read at call time.
   - `max_chars() -> int`: reads `REASON_CODE_DOC_MAX_CHARS`; uses the default
     of 16000 when the value is not a positive integer.
   - `normalize_doc_type(raw) -> Optional[str]`, per D4.
   - `extract_section(markdown: str, heading: str) -> str`, per 5.4. Raises
     `ReasonCodeDocError` when no heading or several headings match.
   - `parse_resolution_guidance(text: str) -> list[dict]`, per content rule 5
     in 5.5.
   - `lookup(reason_code, raw_enrolment_type, root=None) -> dict`: returns the
     shape in 5.7 and never raises (5.3).
   - `provenance(doc_state) -> Optional[dict]`: the state shape without `text`
     and `resolution_guidance`; `None` for `None`.
   - `validate(root=None, coverage=False) -> tuple[list[str], list[str]]`:
     returns (errors, warnings), per 5.2.

   Notes for this module:
   - Read the root at call time as `paths.REASON_CODE_DOCS_DIR` (import the
     module, not the name) so tests can patch it.
   - Reuse `runbook_store.REASON_CODE_PATTERN`,
     `tool_registry.normalize_enrolment_type`,
     `runbook_validator.validate_generic_text` and `INJECTION_MARKERS`, and
     `synthesis.ACTIONS` / `RESIDENT_ACTIONS`.
   - `tool_registry` imports a lot. If importing it at module level creates an
     import cycle or noticeable import cost, import `normalize_enrolment_type`
     inside the function.
6. `src/tools/check_reason_code_docs.py` (new):
   - usage: `python -m src.tools.check_reason_code_docs [--dir PATH] [--coverage]`;
   - prints the errors and warnings;
   - exits 1 if there is any error, 0 otherwise;
   - uses argparse, like the other tools.
7. `src/main_api.py`: immediately after `validate_config()`, if
   `docs_enabled()`, call `validate()`.
   - On errors: log each one, print it to stderr, and `sys.exit(1)`, the same
     way `validate_config` does.
   - Warnings are only logged.
   - Put the check in a small helper function so it can be tested without
     importing `main_api`.
   - Do not add it to `config_validator.validate_config`: the consumers call
     that too, and they never read documents.
8. `src/utils/metrics.py`: add
   `REASON_CODE_DOC_LOOKUPS = _counter("agentic_resident_crm_reason_code_doc_lookups_total", "Reason-code document lookups by outcome and by which enrolment type matched.", ["outcome", "match"])`.
   - `match` is `exact`, `any` or `none`.
   - The Investigator increments it (Phase 4); `lookup` itself stays free of
     side effects.
9. `tests/fixtures/reason_code_docs/`: a small valid folder for tests, with:
   - an `E` entry, a `U` entry and an `ANY` entry;
   - one reference to a section of a shared file;
   - one resolution guidance line.
10. `.env.example`: add `REJECTION_REASON_CODE_DOCS_ENABLED`,
    `REASON_CODE_DOCS_DIR` and `REASON_CODE_DOC_MAX_CHARS`, each with a
    comment. Add the three to `ISOLATED_ENV_VARS`.

**Tests** (in `tests/test_reason_code_docs.py`)
- **Committed folder:** `src/reason_code_docs` validates with no errors. This
  is the CI gate for every future document edit.
- **Fixture folder:** it validates with no errors, and `lookup` against it
  returns:
  - `hit` with the correct `matched_type` for `E`, `N` (normalised to `E`),
    `ENROLMENT`, `U` and `UPDATE`;
  - the `ANY` entry for `Z` (with no `Z` entry present) and for a missing type;
  - `miss` for an unknown reason code and for a string that fails the pattern;
  - `no_reason_code` for `None`;
  - `error`, not an exception, when a referenced file is deleted after
    validation.
- **One test per validator error in 5.2**, each using a temporary folder:
  - invalid JSON; wrong schema version; an unknown top-level key;
  - a bad reason code; the unreachable type `N`; a type repeated across two
    entries; an empty `docs` list;
  - an absolute path; a `..` segment; a path outside `docs/`; a symlink
    escaping the root; a missing file; a non-`.md` file;
  - a missing section; a duplicated heading; a heading inside a code fence
    (must not count);
  - an entry over the size cap;
  - a UUID; a timestamp; a ten-digit run; an injection phrase;
  - an invalid action value; a malformed `- action:` line.
- **Section extraction:** stops at a heading of the same or higher level,
  continues through deeper headings, and includes its own heading line.
- **Rendering:**
  - the `[Source: ...]` headers and the blank-line join;
  - the truncation marker, and the `truncated` flag;
  - `sha256` is the same for the same text and different for different text.
- **Start-up check:** with the switch on and an invalid index, the helper from
  change 7 exits. With the switch off, it does nothing.

**Docs:** add a new subsection "Reason-code documentation" to `ARCHITECTURE.md`
(the store, index, lookup and validation), and add the new files to its file
tree.

**Done when:** the validator and the lookup behave as specified, and the
pipeline has not been touched.

### Phase 3 -- Prompt builder and size limit

**Goal:** one module without side effects that assembles the Investigator,
retry and Reviewer prompts in a fixed order, under a size limit.

**Changes**

1. `src/models/synthesis.py`: make the log classification public:
   - `classify_logs(logs) -> str` returns:
     - `unavailable` for `None`, an empty or all-whitespace value, or exactly
       `Log fetching disabled.`;
     - `silent` when the value contains `No logs found for ID:`;
     - `present` otherwise.
   - `_confidence_ceilings` uses it.
   - No change in behaviour: the existing confidence tests must pass untouched.
2. `src/core/rejection_context.py` (new). Its functions make no LLM calls and
   do no I/O apart from reading `REJECTION_PROMPT_MAX_CHARS`:
   - `prompt_max_chars() -> int` (default 200000; invalid values fall back to
     the default).
   - `build_investigation_prompt(*, doc_state, db_rule, enrolment_display, payload_projection, logs) -> tuple[str, bool]`.
     The bool says whether the logs were trimmed.
   - `build_retry_prompt(*, previous_investigation, feedback, doc_state, db_rule, enrolment_display, logs) -> tuple[str, bool]`.
   - `build_review_prompt(*, investigation, doc_state, db_rule, enrolment_display, payload_projection, logs) -> tuple[str, bool]`.

   Each section is written as `### <Label>\n<body>`, and sections are separated
   by one blank line. The labels are the ones the prompt files already refer
   to. Section order:

   | Prompt | Sections, in order |
   |---|---|
   | Investigation | Reason Code Documentation, Database Rule Configuration, Enrolment Type, Kafka Payload, Elasticsearch Logs, Task |
   | Retry | Your previous analysis, Reviewer Feedback (You MUST fix your previous analysis), Reason Code Documentation, Database Rule Configuration, Enrolment Type, Elasticsearch Logs, Task |
   | Review | Reason Code Documentation, Database Rule Configuration, Enrolment Type, Kafka Payload, Elasticsearch Logs, Investigation to validate, Task |

   The retry leaves out the payload, as it does today (the 2.3 decision
   recorded in the code).

   Section contents:
   - **Reason Code Documentation**
     - `hit`: the rendered text.
     - `miss`, `error`, `no_reason_code`:
       `No documentation is available for this reason code. Reason from the Database Rule Configuration and the policy context.`
     - `disabled` or `None`: leave the section out entirely.
   - **Kafka Payload:** `json.dumps(payload_projection)`, as today.
   - **Elasticsearch Logs**
     - `present` or `silent` (from `classify_logs`): the logs as given. A
       `silent` value keeps its "No logs found" line and any gaps banner, both
       of which the prompts already explain.
     - `unavailable`:
       `No logs are available for this packet (log fetching was disabled or the fetch failed). Do not cite log lines, and do not state packet-specific facts that only logs could show.`
   - **Task (investigation), when the documentation section is present:**
     `Explain why this packet was rejected. Apply the Reason Code Documentation and the Database Rule Configuration to this packet; take packet-specific facts from the logs and quote the exact lines you rely on. If no logs are available, say so plainly and do not invent packet-specific details.`
   - **Task (investigation), when the documentation section is left out:** the
     same text without the words "the Reason Code Documentation and".
   - **Task (retry):**
     `Revise your previous analysis to address the Reviewer Feedback, using the evidence above.`
   - **Task (review):**
     `Validate the investigation above against this evidence. If it is correct, reply with exactly 'APPROVED'. If not, explain what is wrong.`

   **Size limit**
   1. Build everything except the logs' contents, and compute
      `room = prompt_max_chars() - len(everything else)`.
   2. If the logs fit in `room`, use them as they are.
   3. If not, and `room >= 2000`: trim the logs from the middle down to `room`
      characters, inserting
      `\n\n... <n> characters omitted from the middle of this trace (REJECTION_PROMPT_MAX_CHARS) ...\n\n`.
      This is the same shape as `_bound_total_size`, but implemented in this
      module; do not change the log pipeline.
   4. If `room < 2000`: replace the logs' contents with
      `The logs were omitted: the rest of this prompt already fills REJECTION_PROMPT_MAX_CHARS.`
   5. Never trim any other section. In cases 3 and 4, return `trimmed=True`.
3. `src/utils/metrics.py`: add
   `REJECTION_PROMPT_TRIMS = _counter("agentic_resident_crm_rejection_prompt_trims_total", "Direct rejection prompts whose logs were trimmed to fit REJECTION_PROMPT_MAX_CHARS.", ["node"])`.
   The callers increment it.
4. `.env.example` and `ISOLATED_ENV_VARS`: add `REJECTION_PROMPT_MAX_CHARS`.

**Tests** (in `tests/test_rejection_context.py`)
- Section order and exact labels for all three builders.
- Every documentation variant: `hit`, `miss`, `error`, `no_reason_code`,
  `disabled`, `None`.
- Every logs variant: present; silent with a gaps banner; unavailable for
  `None`, empty, whitespace-only, and the disabled sentinel.
- The Task text with and without the documentation section.
- Size limit:
  - the logs are trimmed with the marker, every other section is unchanged
    character for character, and `trimmed` is true;
  - a very small limit replaces the logs' contents;
  - when the other sections alone exceed the limit, they are still sent in
    full, the logs are replaced, and `trimmed` is true.
- `classify_logs` covers every sentinel, and the existing confidence tests
  still pass.

**Done when:** the builders are fully tested and the pipeline has not been
touched.

### Phase 4 -- The Investigator uses the documents

**Goal:** with the switch on, the direct Investigator's first pass and its
retries use the builder and the document.

**Changes** (in `src/core/agent_orchestrator.py` unless stated)

1. `GraphState`: add `reason_code_doc: dict`, `investigator_path: str` and
   `reviewer_path: str`.
2. Add `_reason_code_of(payload) -> Optional[str]`: the first non-empty
   `errorReasonCode` in `packetExecutionSummary.errorData`, tolerating `None`
   at every level. Use it in `investigator_node` in place of the inline loop.
   Behaviour must not change.
3. In `investigator_node`, before the harness branch:

   ```python
   doc_state = state.get("reason_code_doc")
   if doc_state is None:  # first pass, or a checkpoint from before this change
       if reason_code_docs.docs_enabled():
           try:
               doc_state = reason_code_docs.lookup(_reason_code_of(payload), raw_enrolment_type)
           except Exception as e:  # lookup never raises; this guards a bug in it
               doc_state = {"outcome": "error", "detail": f"{type(e).__name__}: {e}"}
           # increment REASON_CODE_DOC_LOOKUPS with outcome and match
           # log outcome, reason code, matched type, sha256 -- never the text
       else:
           doc_state = {"outcome": "disabled"}
   ```

   - `raw_enrolment_type` is `(payload.get("packetMetaData") or {}).get("enrolmentType")`.
   - `match` label: `exact` when `matched_type == requested_type`; `any` when
     the `ANY` entry was used for a different or missing requested type;
     `none` when nothing matched.
   - When the fallback `error` state is built here, fill in the rest of the
     5.7 shape: `null` for the scalar fields and empty lists for `refs` and
     `resolution_guidance`.
4. **Harness branch** (`lane_enabled("rejection") and not is_retry`): unchanged,
   except that its successful return also includes
   `"reason_code_doc": doc_state` and `"investigator_path": "harness"`.
5. **Direct path:**
   - If `reason_code_docs.docs_enabled()`:
     - the first pass uses `build_investigation_prompt(doc_state=doc_state, db_rule=db_rule, enrolment_display=enrolment_type_display(payload), payload_projection=_project_payload(payload), logs=logs)`;
     - a retry uses `build_retry_prompt(...)` with the same document, rule,
       enrolment type and logs;
     - increment `REJECTION_PROMPT_TRIMS.labels(node="investigator")` whenever
       the logs were trimmed.
   - If the switch is off, the existing prompt code runs unchanged (D7).
   - The direct return also includes `"reason_code_doc": doc_state` and
     `"investigator_path": "direct"`.
6. `src/prompts/InvestigatorAgent.md`: add the section in Appendix A, right
   after the opening paragraph and before "Modality Terminology".
7. `agent_policy_context.md`: replace
   "Use the `lookup_rule_by_reason_code` tool to fetch the exact conditions that failed, and reverse-engineer the violation."
   with
   "The orchestrator supplies the exact conditions as the \"Database Rule Configuration\"; reverse-engineer the violation from them."
   This file is appended to the Investigator, Reviewer and Synthesis prompts
   and is part of the prompt fingerprint. Both effects are intended.

**Tests** (in a new file `tests/test_rejection_docs_pipeline.py`)

Use the node-driving pattern from `tests/test_opencode_harness.py`, with a stub
agent that records the messages it receives.
- **Switch off:** the first-pass and retry prompts equal, character for
  character, what the code produced before this change. Build the expected
  strings in the test from the old format.
  `test_investigator_prompt_trims_context_on_retry` must still pass unchanged.
- **Switch on, hit:** the prompt contains the document text before the rule,
  and the logs last before the Task. The returned state has
  `reason_code_doc.outcome == "hit"` and `investigator_path == "direct"`.
- **Switch on, miss:** the prompt contains the "No documentation is available"
  text, and the outcome is `miss`.
- **Switch on, retry:** the retry prompt contains the previous analysis, the
  feedback, the document, the rule and the logs, and no `### Kafka Payload`
  section.
- A retry reuses the stored document: patch `lookup` to count its calls, and
  check it is called once across the first pass and the retry.
- A retry with no `reason_code_doc` in state does the lookup.
- A `lookup` that raises gives outcome `error`, and the packet carries on.
- A harness success sets `investigator_path == "harness"`. A harness failure
  falls back and sets `direct`. Reuse `_harness_on`.
- `GraphState.__annotations__` contains the three new keys.
- **End to end**, in the `tests/test_end_to_end.py` style: with the switch on
  and `paths.REASON_CODE_DOCS_DIR` pointed at the fixture folder, the packet
  is processed and the Investigator's prompt contains the fixture document.

**Docs:** in `ARCHITECTURE.md`, update the Investigator node description and
the Investigator note in the state diagram.

**Done when:** with the switch off, the prompts are identical to today; with it
on, the document is sent.

### Phase 5 -- The Reviewer sees the evidence

**Changes** (in `src/core/agent_orchestrator.py`, `reviewer_node`, direct path)

1. Read `evidence = get_bool_env("REJECTION_REVIEWER_EVIDENCE", True)`.
2. If `evidence` is on: use
   `build_review_prompt(investigation=investigation, doc_state=state.get("reason_code_doc"), db_rule=state.get("db_rule", ""), enrolment_display=enrolment_type_display(payload), payload_projection=_project_payload(payload), logs=state.get("logs"))`.
   - The builder leaves out the document section for `disabled` or `None`, so
     no separate check on the docs switch is needed.
   - Increment `REJECTION_PROMPT_TRIMS.labels(node="reviewer")` whenever the
     logs were trimmed.
3. If it is off: today's prompt, unchanged.
4. Both the harness return and the direct return include `reviewer_path`
   (`harness` or `direct`).
5. Leave unchanged: the `add_learning_rule` tool, the contextvars, and
   `is_reviewer_approved`. The Task text keeps "reply with exactly 'APPROVED'".
6. `src/prompts/ReviewerAgent.md`: add the section in Appendix B, after the
   opening instructions and before "EVIDENCE GAPS".
7. `.env.example` and `ISOLATED_ENV_VARS`: add `REJECTION_REVIEWER_EVIDENCE`.

**Tests**
- With the variable unset, the Reviewer's prompt contains the rule, the
  payload, the logs and the investigation, plus the document when the docs
  switch is on.
- With `false`, the prompt is today's, character for character.
- An oversized log is trimmed, and the trim is counted.
- `reviewer_path` is set on both paths.
- An `APPROVED` reply still leads to synthesis; the existing tests of
  `check_approval` cover this.

**Docs:** in `ARCHITECTURE.md`, update the Reviewer node description and its
state diagram note.

**Done when:** the direct Reviewer can check claims against the evidence, and a
single switch restores today's behaviour.

### Phase 6 -- Provenance, outcomes and metrics

1. `src/api/routes.py`, where `casebook_data["resolution"]["provenance"]` is
   built, add:
   - `"reason_code_doc": reason_code_docs.provenance(result.get("reason_code_doc"))`
   - `"investigator_path": result.get("investigator_path")`
   - `"reviewer_path": result.get("reviewer_path")`

   Packets answered by a runbook never reach the Investigator, so these are
   `None` for them. Never write the document text into the casebook.
2. `src/utils/outcomes.py`: add `reason_code_doc_outcome`,
   `reason_code_doc_sha256` and `investigator_path` next to
   `prompt_fingerprint`.
3. Metrics were added in Phases 2 and 3. Confirm both counters appear on
   `/metrics`.

**Tests**
- The casebook provenance has the three fields.
- A JSON dump of the casebook does not contain the fixture document's text.
- Outcome records have the new fields. Extend the pattern of
  `tests/test_audit_phase2.py::test_outcome_denormalises_the_new_grouping_keys`.
- Escalated packets (`escalate_node`) also carry the provenance.

**Docs:** in `ARCHITECTURE.md`, extend the paragraph that describes
`resolution.provenance.prompt_fingerprint`.

### Phase 7 -- Resolution guidance for Synthesis (own switch, off by default)

1. `synthesis_node`: when `get_bool_env("REJECTION_SYNTHESIS_DOC_GUIDANCE", False)`
   is on, the stored document has `outcome == "hit"`, and its
   `resolution_guidance` is non-empty, append this to the prompt:

   ```
   ### Resolution guidance from the reason code documentation
   - action: X | resident_action: Y | when: Z
   Use this guidance to choose action and resident_action, unless the approved
   investigation shows this packet does not fit it. In that case follow the
   investigation and say why in the synthesis.
   ```

   - The repair prompt is unchanged.
   - With the switch off, the prompt is today's.
2. `src/prompts/SynthesisAgent.md`: add one short paragraph explaining how to
   use that section when it is present, and noting that the values in it are
   already valid.
3. `.env.example` and `ISOLATED_ENV_VARS`: add `REJECTION_SYNTHESIS_DOC_GUIDANCE`.

**Tests**
- With the switch off, the prompt is unchanged character for character.
- With it on and guidance present, the section is added.
- With it on but a miss, or a hit with no guidance, nothing is added.

**Docs:** in `ARCHITECTURE.md`, update the Synthesis node description.

### Phase 8 -- Documentation pass

- `ARCHITECTURE.md`:
  - re-read every section touched in Phases 1-7 and make them consistent;
  - add the new files and folders to the file tree: `src/reason_code_docs/`,
    `src/utils/reason_code_docs.py`, `src/core/rejection_context.py`,
    `src/tools/check_reason_code_docs.py`, and the new tests;
  - describe the direct path with documents alongside the harness.
- `SYSTEM_OVERVIEW.md` section 7: the rejection lane now has three modes
  (harness, direct, and direct with documents), chosen per lane.
- `README.md`: add the new files to its file tree.
- `.env.example`: every variable in section 4 is present and commented.
- `src/reason_code_docs/README.md`: a final check against section 5.

### Phase 9 -- Evaluation and rollout

**Prerequisite:** the local-backend cleanup issue from Phase 0 is fixed, or
these runs use S3 (or an S3-compatible store) as the casebook backend.

1. The owner writes documents for the most frequent reason codes, and the
   validator passes.
2. Build an evaluation set:
   - captured rejection payloads whose correct outcome is known (recorded with
     `src/tools/record_outcome.py`);
   - offline log fixtures (`src/tools/build_log_fixture.py`, then
     `ES_MOCK_FILE` or `K8S_FIXTURE_DIR`), so every configuration sees the same
     logs.

   Real resident data stays in the gitignored fixture folders.
3. Run each configuration below on the same payloads and the same LLM endpoint,
   with `RUNBOOK_MODE=off`. Give each run its own casebook location and its
   own `LOCAL_CHECKPOINTS_DIR`: the checkpointer is keyed by eventId, so a
   shared checkpoint store would resume the previous run's state.

   | Run | Settings |
   |---|---|
   | A | `USE_OPENCODE_HARNESS_REJECTION=true` (today) |
   | B | harness off, `REJECTION_REASON_CODE_DOCS_ENABLED=true`, `REJECTION_REVIEWER_EVIDENCE=true` |
   | C | harness off, both of those switches off (today's fallback) |
   | B-nolog | B with `ENABLE_LOG_FETCHING=false` |
   | B+S | B with `REJECTION_SYNTHESIS_DOC_GUIDANCE=true` |

   Drive the runs with `local_run.py` (or a small loop around it), and record
   the wall time per packet.
4. For each packet, compare: action, resident action, confidence, abstention,
   number of Reviewer rejections, `investigator_path`, and
   `reason_code_doc.outcome`.

   An optional small tool can help: `src/tools/compare_rejection_runs.py`. It
   would be read-only, read casebooks from the run folders, and print a table
   plus agreement rates against the recorded correct outcomes.
5. Read a sample of B-nolog casebooks by hand. They must not state
   packet-specific facts, such as which candidates matched or their scores,
   that only logs could show.
6. The owner decides go or no-go. Suggested bar:
   - B is at least as correct as A on the reason codes that have documents;
   - B-nolog invents nothing;
   - B's Reviewer rejection and escalation rates are no worse than A's;
   - B's time per packet is clearly lower.
7. Rollout: set the values in section 7, then watch:
   - `reason_code_doc_lookups_total`: the miss rate by reason code shows which
     documents to write next;
   - `rejection_prompt_trims_total`;
   - the Investigator retries histogram;
   - abstentions and escalations.

---

## 7. Target production configuration

```
USE_OPENCODE_HARNESS_REJECTION=false
USE_OPENCODE_HARNESS_DLT=true
REJECTION_REASON_CODE_DOCS_ENABLED=true
REJECTION_REVIEWER_EVIDENCE=true
REJECTION_SYNTHESIS_DOC_GUIDANCE=false   # until Phase 9 shows B+S is better
REJECTION_PROMPT_MAX_CHARS=<from section 4 and the model's context window>
```

Because the DLT lane stays on opencode, the opencode server still starts, the
corpus still downloads, and `/ready` still waits for both. That is expected.

---

## 8. Risks

| Risk | Mitigation |
|---|---|
| A document goes stale after a rule change | The Investigator is told that the rule wins and to report the disagreement (Appendix A). Each casebook records the document's hash. Documents change only through reviewed pull requests. |
| Long, noisy logs: the model misses the facts that matter | Logs come last, the task is restated after them, exact-line quotes are required, and the Reviewer can now check those quotes. If accuracy still falls short, the follow-up is deterministic fact extraction. |
| The Reviewer becomes much stricter | Retries are capped by `MAX_INVESTIGATION_RETRIES`. `REJECTION_REVIEWER_EVIDENCE=false` restores today's Reviewer. Retries and escalations are watched in Phase 9. |
| Higher token use from the Reviewer seeing the evidence | Bounded by `REJECTION_PROMPT_MAX_CHARS`, and measured in Phase 9. |
| Packet identifiers or instructions slip into a document | The validator's content rules, which CI runs on every change. |
| Python and the shell script disagree about the switches | One normalisation rule, plus a test that runs the shell block and compares it with Python. |
| Old checkpoints without the new state keys | Every read uses `state.get`, and a missing document entry triggers a lookup. |
| Local-backend casebooks are deleted after completion | Verified in Phase 0; blocks Phase 9 until the owner decides. |

---

## 9. Out of scope, and follow-ups

- **Deleting the rejection harness code** once Phase 9 passes:
  - `src/prompts/harness/RejectionInvestigator.md`, `RejectionReviewer.md`
    and `rules/rejection.md`, and their `PROMPT_FILES` entries;
  - the harness branches in `investigator_node` and `reviewer_node`, and
    `_write_harness_case_files`;
  - the rejection parts of `tests/test_opencode_harness.py`.
- **Deterministic fact extraction:** pull out the facts each document's
  "Evidence to look for in logs" names, as a compact table placed ahead of the
  raw logs.
- **The DLT lane without opencode:** business-code documents plus a
  stack-frame to module-doc lookup, with a small tool loop for new
  fingerprints.
- **Other document locations:** documents hosted in S3, or index entries that
  point into the downloaded DROA corpus.
- **Giving the documents to the rejection harness** as well.
- **Drafting runbooks from the documents.**

---

## 10. Questions for the owner (answer before or during Phase 0)

1. Which branch to build on, given the uncommitted work on
   `fix/dlt-group-state-and-dedupe`.
2. The model server's `max-model-len` and maximum output tokens, to set
   `REJECTION_PROMPT_MAX_CHARS`.
3. Whether `REJECTION_REVIEWER_EVIDENCE` should default to `true` (D8).
4. The local-backend cleanup issue: fix it first, or run Phase 9 on S3.
5. Who writes and reviews the documents, and which reason codes come first.

---

## Appendix A -- Addition to `InvestigatorAgent.md` (draft)

The wording can be polished; the seven rules must be kept.

```
### REASON CODE DOCUMENTATION -- READ THIS FIRST WHEN IT IS PRESENT

The prompt may include a "Reason Code Documentation" section. When it does:

1. It is the authoritative description of the policy behind this reason code
   and of what triggers it. Use it together with the "Database Rule
   Configuration" to explain WHY the packet was rejected.
2. The logs supply the packet-specific facts: for example which candidates
   matched, whether they share this packet's parent, which modality matched,
   the scores, and when. The documentation's "Evidence to look for in logs"
   says what to look for. Quote the exact log lines you rely on.
3. If no logs are available, still give the complete explanation from the
   documentation and the rule, state plainly that runtime logs were not
   available to corroborate it, and do not state packet-specific facts that
   only logs could show.
4. If the logs contradict the documentation, report the contradiction
   explicitly. Do not silently prefer one of them.
5. If the documentation and the "Database Rule Configuration" disagree, the
   rule is what actually fired. Follow the rule and say that the
   documentation appears to be out of date.
6. The documentation uses placeholders such as <refId> in its examples. Never
   present a placeholder or an example value as a fact about this packet.
7. If the section says no documentation is available, reason from the rule
   and the policy context as usual.
```

## Appendix B -- Addition to `ReviewerAgent.md` (draft)

The wording can be polished; the rules must be kept.

```
### THE EVIDENCE YOU ARE GIVEN

You receive the evidence the Investigator had: the Database Rule
Configuration, the Enrolment Type, the Kafka Payload, the logs, and the
Reason Code Documentation when there is one. Check the investigation against
it. REJECT the investigation if:

1. It misstates what the reason code or the rule means, or contradicts the
   Reason Code Documentation without saying why.
2. It applies the rules for the wrong enrolment type.
3. It quotes a log line that does not appear in the supplied logs, or states a
   packet-specific fact (a candidate, a score, a timestamp) that neither the
   logs nor the payload support.
4. It presents a placeholder or an example value from the documentation as a
   fact about this packet.

If no logs were available, do NOT reject the investigation for lacking log
citations. Check instead that it says logs were unavailable and invents no
packet-specific facts.
```
