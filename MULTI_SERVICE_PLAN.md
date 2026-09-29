# Multi-Service Rejection Lane -- Implementation Plan

- **Date:** 2026-09-25
- **Status:** Phases 1 to 8 implemented on 2026-09-28 (Phase 7 as its pilot
  mechanism only; no service has been onboarded); `ARCHITECTURE.md`
  sections 3.2.2, 3.2.3 and 3.5.1 describe what was built. Phase 0 needs production access and is the
  owner's; of it, only 0.6 (the failing-test baseline: 56 pre-existing
  failures) has been done. Implementing Phase 1 corrected four points of this
  plan, all now reflected below:
  - `policy.md` becomes required in Phase 2, when packs are first read for
    prompts.
  - Pack validation runs in `main_api`, not `validate_config`.
  - `_service_of(state)` and `pack()` arrive in Phase 2 with their first
    callers, and `pilot_services()` in Phase 7.
  - Each metric gains its `service` label in the phase that changes the code
    emitting it.

  Phase 2 was implemented on 2026-09-28 too; its owner-run parity check has
  not been run. Implementing it changed five more points, also reflected
  below:
  - **`record` mode is allowed with Phase 2 code** (D5, 5.7). The plan made
    it a boot error, which would have taken the lane down on the default
    setting. Instead, `record` analyses a packet the gate would skip with the
    pre-registry pack (`enu-biometric`), exactly as before, so it still
    changes nothing about analysis.
  - **The harness receives the pack by appending**, not by template
    placeholders (5.5). The SERVICE CONTEXT block is added after the rendered
    template, the way the tools section already is.
  - **Section headings name the pack in brackets:**
    `### SERVICE CONTEXT -- <display name> [<pack>]`.
  - **Tool plumbing moves to Phase 4.** The per-service opencode agents and a
    `service` argument to `build_agent` would only carry an unused value until
    tools are scoped.
  - **Rule promotion moves earlier.** Learned rules are routed to the pack
    they were learned under now, rather than in Phase 5. Otherwise the next
    promotion would write a biometric rule into the now-generic
    `InvestigatorAgent.md`. The `scope` choice stays in Phase 5.

  Phase 3 was implemented on 2026-09-28 as well; see its own "As built"
  section. It changed two points of this plan:
  - **`lookup` takes the pack's documentation file and enrolment-type words,
    not a service name** (D10), so it stays a function of the store alone and
    the registry stays out of it.
  - **The harness receives the rule source by appending** too, as Phase 2 does
    for the service context, rather than by editing the templates for it. The
    templates only list the new evidence file.

  Phase 4 was implemented on 2026-09-28 too; see its own "As built" section.
  It settled four points the plan left open, all reflected there:
  - **The LogFilter is built with the `_default` scope**: one agent serves
    every service, so it may use only the tools for every service.
  - **`selection(role, None)` is an error for a rejection role**, as 5.6 says,
    and so is a service for a DLT role; `build_agent` takes `pack=`.
  - **A harness task with no opencode agent of its own is refused** while tool
    servers are configured, and falls back to its direct path, because
    opencode's default agent would have every tool.
  - **The `_default` pack takes no `tools.include`, and no service may be
    named `default`**, so D7's "only the `*` tools" and 5.6's opencode names
    both hold by construction.

  Phase 5 was implemented on 2026-09-28 too; see its own "As built" section.
  It settled four points the plan left open:
  - **Runbooks are looked up under the packet's pack**, not its service, so
    `record` mode still analyses a skipped packet exactly as before (D5).
  - **A documentation-bound runbook with no documentation to compare against
    is not served** (`binding_unavailable`), where a rules-table runbook with
    no rule still is, as before.
  - **`build_runbooks.py` also leaves out a casebook analysed with another
    pack than its service's**, and binds a draft only to its service's own
    documentation.
  - **An unknown learned-rule scope is queued as `service`**, on both
    Reviewer paths, rather than refused.

  Phase 6 was implemented on 2026-09-28 too; see its own "As built" section.
  It settled four points the plan left open:
  - **Logs follow the pack only when it is the packet's own.** A packet
    `record` mode analyses with the pre-registry pack because the gate would
    skip it is fetched as before (the environment's lists), so `record` still
    changes nothing (D5). A `_default` packet fetches nothing.
  - **enu-biometric keeps the unscoped catalog and parse tree** until it has
    a catalog of its own, so its reduction is unchanged; every other service
    without one gets no filtering.
  - **The generic decision vocabulary lost its four biometric terms** to
    enu-biometric's pack; callers with no service still match them.
  - **`_project_payload` is unchanged**: the plan makes narrowing it
    conditional on Phase 0, which has not been run.

  Phase 7's one-time addition, pilot mode, was implemented on 2026-09-28
  too; see its own "As built" section. It settled four points the plan left
  open:
  - **The pilot decision is made once per packet, with the pack**, and
    carried in `GraphState.pilot`. The Synthesis agent and the casebook flag
    therefore always agree, even across a checkpoint resume.
  - **A pilot Synthesis prompt gains a `### PILOT MODE` section.** The
    generic prompt tells Synthesis to call `queue_for_replay` before
    answering REPLAY. Without the section, the pilot agent would reach for a
    tool it does not have.
  - **A service named in both lists is a boot error**, and a pilot where
    validation has not run.
  - **Accuracy is grouped by service everywhere**, not only under
    `--service`, so one reason code raised by two services is reported as
    two rows.

  Phase 8 was implemented on 2026-09-28 too, once the owner settled the DLT
  contract: **every service dead-letters its records with the rejection
  lane's Kafka payload and key, and only the headers differ, carrying the
  stack trace.** See Phase 8's own "As built" section. It settled five points
  the plan left open:
  - **The DLT lane has its own gate**, `DLT_SERVICE_GATE` and
    `DLT_SERVICES_ENABLED`, since a service's crashes are onboarded apart
    from its rejections. It has no `_default` pack: an unresolved record is a
    skip reason, and in `record` mode it is analysed with no pack, as before.
  - **The consumer group decides first**, before the stage: it is the failing
    consumer's own identity.
  - **Cases are stored per record**, `<refId>__<digest of case_id>`, which
    fixes section 10's refId dedupe and keeps the refId findable as a prefix.
  - **Only services other than enu-biometric get namespaced fingerprints**,
    so its groups and cached recommendations are unchanged.
  - **The DLT agents get a pack's optional `dlt.md`, not its `policy.md`**,
    which is written for rejections.
- **Scope:** the rejection lane. Every service publishes its rejection events
  to one Kafka topic, in the structure the lane already parses. The DLT lane
  is deferred until its message contract is final (section 10); the registry
  reserves the fields it will need (D15).
- **Audience:** the engineer or agent implementing this, one phase at a time,
  and the owner, who answers section 11 and approves each checkpoint.

### Where a new session resumes (2026-09-28)

**Done:** Phases 1 to 6, and Phase 7's pilot mechanism, each with an "As
built" section (Phases 1 and 2 describe theirs under "Changes, as built")
under its own heading in section 6. They record where the implementation
differs from what the phase text says, and the later phases build on the
differences, not on the original text.

**Next:** onboarding the first service other than enu-biometric (section 7),
which needs Phase 0's values and a pack from the service's experts -- its
`dlt` section too, for the DLT lane. Of Phase 6, only the `_project_payload`
narrowing waits, on Phase 0's answer to question 11.9.

**State of the branch:** Phases 1-6 are committed on `feature/multi-service`.
Phases 7 and 8 are in the working tree, not committed.

**Baseline to compare against:** 56 pre-existing failures on the full suite,
re-measured before Phase 4 and unchanged after it. Re-measure before the next
phase rather than trusting this number, and remember the
`pending_rules.jsonl` restore in "Running the tests" below.

**Still the owner's, and still not done:** Phase 0 (sampling the shared topic,
which is what confirms every service's `flowMetaData.stage` and supplies the
other services' values) and the Phase 2 parity check. Until Phase 0 is done,
`REJECTION_SERVICE_GATE` stays `record` and `REJECTION_SERVICES_ENABLED` stays
`enu-biometric`: no second service can be onboarded (Phase 7) without the
values Phase 0 produces.

**Deliberately not built yet, so do not read its absence as an oversight:**
per-service rules tables (there is one table, and it is enu-biometric's --
D6), the named-field payload projection (Phase 6, waiting on Phase 0), pilot
mode for the DLT lane, and a change of the DLT replay identity from the refId
to the eventId (unconfirmed with OIS).

---

## 0. How to use this plan

1. Read the whole plan before changing anything. Sections 1-5 are the answers,
   the facts and the decisions; section 6 is the work, in phases.
2. Do the phases in order. Each ends with a **checkpoint**: stop, report what
   changed and the test results, and wait for the owner before the next phase.
3. The rules in `.agents/AGENTS.md` apply throughout:
   - Update `ARCHITECTURE.md` (and `SYSTEM_OVERVIEW.md` where it describes the
     same thing) in the same phase as the code it describes.
   - Never run `git commit` or `git push` unless the owner explicitly asks.
   - No emojis anywhere: code, logs, comments, prompts, documents.
4. Section 2 was verified on 2026-09-25 against the working tree of branch
   `fix/dlt-group-state-and-dedupe`, which carries uncommitted changes to the
   MCP tool layer. Re-check each fact before relying on it, and refer to code
   by symbol name, not line number; line numbers drift.
5. If a fact in section 2 is wrong, or a decision in section 4 cannot be
   implemented as written, stop and ask. Do not improvise around it.

### Running the tests

- Command: `.venv/bin/python -m pytest -q -p no:cacheprovider -rf`
- Save the failing test ids before Phase 1 and compare after every phase. A
  phase is done when no test that passed before now fails, and the phase's own
  new tests pass. Do not fix pre-existing failures as part of this plan.
- Some tests delete the tracked `src/prompts/pending_rules.jsonl`
  (REASON_CODE_DOCS_PLAN.md section 0). After every full run:
  `git checkout -- src/prompts/pending_rules.jsonl`.

---

## 1. Short answers

### 1.1 Update the current prompts, or write dedicated prompts for each service?

Neither, as such. Keep **one** set of role prompts (Investigator, Reviewer,
Synthesis, and the two harness templates), rewritten so they contain nothing
service-specific. Add one **service pack** per service holding everything that
is service-specific:

- the business policy and glossary;
- what each enrolment type means;
- extra checks for the Reviewer;
- guidance for Synthesis;
- the learned rules for that service.

Code builds each agent's prompt from the role prompt plus the packet's own
service pack (D3, D4).

- **Why not one prompt covering every service:** services' vocabularies
  contradict each other. `agent_policy_context.md` says "demo" means the face
  modality and MUST NOT be read as demographic. For a demographic service,
  "demo" means demographic. One prompt carrying every service's policy cannot
  be right for both. Every packet would also pay for about ten policies that do
  not apply to it, and an edit to one service's policy would change every
  service's prompt fingerprint.
- **Why not a full prompt set per service:** about 35 of the 296 lines in
  `InvestigatorAgent.md`, `ReviewerAgent.md` and `SynthesisAgent.md` are
  biometric-specific. The rest (evidence gaps, log noise, the output contract,
  approval criteria, confidence calibration) would be copied into eleven sets
  of five files.
  - Every fix to shared behaviour would become eleven edits, and the copies
    would drift. This codebase has already seen that happen with two copies of
    one table: the comment on `ENROLMENT_TYPE_DISPLAY` records that the harness
    copy had no `Z` and the direct copy had no `E`.
  - A generic lesson learned on one service could not reach the others.
- **This matches the architecture's own principle:** "New services join by
  being configured and documented, not by being coded for"
  (`SYSTEM_OVERVIEW.md` section 1).

### 1.2 Should the rules-table lookup become a tool described "use only for enu-biometric"?

Recommended against. Keep the lookup **deterministic**, but run it only for
services whose registry entry declares a rules database. Today that is only
enu-biometric. For every other service:

- the lookup never runs;
- the "Database Rule Configuration" section is left out;
- the prompt says the reason-code documentation is the rule source.

This achieves what the tool description was trying to achieve: the biometric
table never reaches another service's packet. It does so by construction
rather than by asking the model. D6 gives the five reasons in full. In short:

- The rule is the primary evidence for a rejection.
- The runbook staleness check needs it before any model runs.
- The Reviewer checks against the same rule.
- A description is only advice.
- The other services lose nothing, because their documentation files carry
  their rule-engine rules in `rules.rules[]`.

### 1.3 Many tools: some common, some per service, some databases shared

- **Scope every toolset by service as well as by agent role, and filter in
  code** (D7). The agent that investigates a packet is built with the common
  tools plus that packet's service's tools, and nothing else. A tool's
  description is never the guard.
- **Service-specific tool names carry the service's prefix, and the prefix is
  enforced** (D8). Tool names are global across servers, and today a duplicate
  is silently dropped.
- **Database connections are shared per database, not per toolset** (D9).
  "The database is common" and "the tool is common" are different facts: a
  biometric-only tool can read a shared database.

### 1.4 Reason-code documentation from S3

- Keep the one-file-per-service format, and make the file name the service's
  canonical name.
- The lookup reads the packet's own service file first. Other services' entries
  are used only as a labelled fallback (D10).
- The same files give a last-resort way to work out a packet's service from
  its reason code (D1).

### 1.5 The piece everything depends on: which service does this packet belong to?

Nothing works that out today. Every later choice depends on it: policy, tools,
documentation, rule source, runbooks and logs. So it is resolved once per
packet, deterministically, and recorded with how it was resolved (D1). It is
never decided by a model.

---

## 2. Current state (verified 2026-09-25)

### 2.1 Intake

- All services publish rejections to one topic, in the structure
  `MessagePayload` parses (`src/models/schemas.py`).
- `RejectionAdapter.should_skip` filters only on `packetStatus == "REJECTED"`
  and on whether a finished casebook exists. Nothing identifies the service.
  (grep: `class RejectionAdapter`)
- The fast consumer treats any 2xx from `/fetch-logs` as done and commits the
  offset (`_handle_one_message` and the forwarding call in
  `src/utils/kafkaConsumer.py`). So the API can decline a packet by returning
  200 with a skip status.
- `MessagePayload` declares no `extra` setting, so pydantic ignores unknown
  top-level keys. A field added to the payload before it is republished to the
  analysis queue is dropped on the slow side. Anything the analysis stage needs
  from the fetch stage must travel as a stored artifact, as
  `fetched_logs.txt` does.
- `flowMetaData.stage` and `subStage` are the payload's own statement of where
  the packet failed. A past biometric investigation quoted in
  `src/prompts/pending_rules.jsonl` reports the stage as `Biometric`. The values
  for other services are not known yet (Phase 0).

### 2.2 Agents and prompts

- `_build_agent` in `src/core/agent_orchestrator.py` compiles the graph once
  and, with it, builds one deep agent per role (`build_agent` in
  `src/core/agent_factory.py`).
  - An agent's system prompt and its tools are fixed when it is built; nodes
    send only the user message.
  - So a packet can have different system instructions or different tools only
    if it uses a different agent.
- `load_prompt` appends `agent_policy_context.md` under the heading
  `### GLOBAL BUSINESS POLICY CONTEXT` to the Investigator, Reviewer and
  Synthesis prompts. The LogFilter prompt does not get it.
- Service-specific content today (all of it biometric):

| File | Service-specific content |
|---|---|
| `agent_policy_context.md` | The whole file: the glossary ("demo" = face, MUST NOT be read as demographic), success criteria for enrolment, update and MBU, how to read rule conditions, and the resolution strategy |
| `src/prompts/InvestigatorAgent.md` | "Enrolment Type -- READ THIS FIRST", "Aadhaar Biometric Processing Rules", "Modality Terminology -- BINDING", the biometric examples in documentation rule 2, and CRITICAL INSTRUCTION 1 naming `agent_policy_context.md` |
| `src/prompts/ReviewerAgent.md` | CRITICAL INSTRUCTION: reject findings that contradict the glossary ("demo", "nonDemo") |
| `src/prompts/SynthesisAgent.md` | The glossary, "Aadhaar Biometric Processing Rules", and the instruction to refer to `agent_policy_context.md` |
| `src/prompts/harness/rules/rejection.md` | "Rejection Investigator for the Aadhaar Biometric Enrolment/Update system", and the enrolment-type rules |
| `src/prompts/harness/RejectionReviewer.md` | STEP 3 checks 2 and 3 (glossary, enrolment types) |
| `src/prompts/harness/RejectionInvestigator.md` | "(N or E = new enrolment, U = biometric update)" |
| `src/prompts/RunbookGenerator.md` | "rejected biometric packets" |
| `AGENTS.md` (root; opencode loads it into every session) | "investigation agent for the Aadhaar Biometric Enrolment/Update pipeline" |
| `agent_orchestrator.ENROLMENT_TYPE_DISPLAY`, `reason_code_docs._TYPE_DISPLAY` | `U` shown as "Biometric Update" |
| `src/main_api.py` app description | "rejected biometric packets" |

- `compute_prompt_fingerprint` hashes one fixed list of files plus
  `mcp_client.fingerprint_material()`. There is one fingerprint for the whole
  deployment.
- `agent_policy_context.md` is also:
  - copied into the image on its own line in the `Dockerfile`
    (`COPY agent_policy_context.md ...`);
  - written by `tests/test_audit_phase2.py::test_prompt_fingerprint_is_stable_and_sensitive`;
  - named in a message in `src/tools/check_drift.py`.

  The `Dockerfile` copies `src/` whole, so anything added under `src/` ships
  without a Dockerfile change.

### 2.3 Tools

- `Toolset(name, agents, guidance, enabled, read_only)` in
  `src/tools/agent_tools/__init__.py` scopes tools by agent role only.
  `mcp_client.selection(role)` picks tools by role; `AGENT_TOOLS_<ROLE>`
  replaces a role's selection.
- `agent_tools.discover()` imports the top-level modules of the package only
  (`pkgutil.iter_modules`), not its subpackages.
- Tool names are global across servers. `mcp_client.load_catalog` drops any
  tool whose name another server already serves, and only logs an error. Two
  services each serving a tool named `get_parking_status` would silently lose
  one.
- The only toolset, `process_db`, reads enu-biometric's `bio_*` tables in
  `uidprocessv2_2`. Its helper is `_process_db.py`; its tool modules are
  `stage_tracker.py`, `parking_queue.py` and `helper_cache.py`. When
  `PROCESS_DB_ENABLED=true`, it goes to every Investigator run, whatever the
  service.
- The harness has one opencode agent per role, `crm_<role>`
  (`mcp_client.opencode_config`). It is chosen by
  `opencode_runner._task_agent(node, config)`. The config is generated when the
  opencode server starts.
- MCP metadata keys: `uidai.crm/toolset`, `uidai.crm/agents` and
  `uidai.crm/guidance` (`mcp_config.META_*`). Tools from other teams' servers
  are supported through `AGENT_MCP_SERVERS`.

### 2.4 The rules database

- `_lookup_rule_by_reason_code_impl` (`src/tools/tool_registry.py`) runs
  `SELECT * FROM rules WHERE reject_reason_code = :reason_code` against
  `DB_NAME` (default `uidmasterv1_1`), or the mock export.
  - The table holds enu-biometric rules only (owner, 2026-09-25).
  - The TTL cache is keyed on the reason code alone.
- It is called deterministically, before any model sees the packet, in two
  places:
  - `investigator_node`, which fills the "Database Rule Configuration" section;
  - `runbook_lookup_node`, through `lookup_rule_for`, whose fingerprint decides
    whether a runbook is stale.
- `_parse_and_filter_rules` filters rules by
  `rule_data.statement.Condition.StringEquals.enrolmentType`, mapping through
  `_ENROLMENT_TYPE_ALIASES` (U -> UPDATE, N/E -> ENROLMENT). This is specific to
  this table's rule format.
- `get_error_description` is a fallback of eleven hardcoded biometric
  descriptions, used when the lookup misses.
- `rejection_context._rule_note` picks one of four provenance notes, from the
  documentation outcome and whether a rule came back. None of them covers "this
  service has no rules database".
- `src/prompts/pending_rules.jsonl` line 1: a Reviewer caught an Investigator
  that had looked up `RESIDENT_MAN_MAN_DEDUP_DUPLICATE` (an extra `MAN_`) and
  concluded "rule not found".

### 2.5 Reason-code documentation

- There is one JSON file per service under `<REASON_CODE_DOCS_DIR>/services/`.
  Each carries `codes[]` and, optionally, `rules.rules[]`: the rule-engine rules
  that raise a code, with their conditions.
- `enu-biometric.json` has 98 code entries and 58 rule entries. The rule
  entries are generated from the production rule base: the same rules the
  rules table holds.
- `_entries_for` returns every entry from every file for a code. Nothing
  prefers the packet's own service. Every lookup re-reads and re-parses every
  file.
- `download_service_docs()` downloads nothing; it is a placeholder. The files
  are read from `REASON_CODE_DOCS_DIR`, which defaults to the store in the
  repository.
- `validate()` reports an error when two files declare the same `service`. It
  does not check the file name against that field.
- `REJECTION_REASON_CODE_DOCS_ENABLED` defaults to false.

### 2.6 Runbooks and learned rules

- Runbooks live at `src/runbooks/{draft,final}/<CODE>__<TYPE>.json`, keyed on
  reason code and enrolment type (`runbook_store._resolve_runbook_path`).
  - There are 39 drafts, all `RESIDENT_MAN_DEDUPE_REJECT_*` (biometric).
  - `RUNBOOK_SERVE_ALLOWLIST` lists bare reason codes.
- Learned rules: `queue_learning_rule` appends to
  `src/prompts/pending_rules.jsonl` (3 entries, all biometric).
  `src/tools/promote_rules.py` appends approved rules to `InvestigatorAgent.md`,
  which every packet reads.

### 2.7 Logs and privacy

- The services searched come from static lists (`ES_APP_NAMES`,
  `K8S_APP_NAMES`, `K8S_SERVICE_MAP`). `FetchContext.app` is never set per
  packet (`log_pipeline/pipeline.py`), so every packet searches every configured
  service.
- The template catalog (built by `build_catalog.py`, one file at
  `CATALOG_PATH`) turns boilerplate phrases into `must_not` clauses on every
  Elasticsearch query. It was built from biometric refIds.
- `DECISION_VOCABULARY_REGEX` is biometric-flavoured (`biometric.*match`,
  `MAN_DEDUP`).
- Redaction (`redaction.DEFAULT_PATTERNS`) covers VID, Aadhaar, mobile and
  email. It has nothing for name, date of birth, gender or address.

### 2.8 Things that cannot be used

- `reason_codes.csv` cannot map a reason code to a service. Of the 136 codes in
  `enu-biometric.json`, 87 are in the CSV, and 66 of those have an empty
  `stage`; another 4 say `PostEnrolment`.

---

## 3. Goal and non-goals

**Goal.** Every rejection event on the shared topic is analysed with its own
service's policy, documentation, rule source and tools, and never with another
service's. Services are switched on one at a time. enu-biometric's analysis
does not change until the owner accepts a measured difference (Phase 2).

**Non-goals.**
- The DLT lane (section 10).
- Changing the Synthesis contract (`ACTIONS`, `RESIDENT_ACTIONS`) (D14).
- A different graph or flow per service. The flow is the same; only the
  knowledge differs.
- Writing the other services' packs. Their subject-matter experts author them;
  this plan defines the contract, and extracts the enu-biometric pack from
  today's prompts.
- Vector search or retrieval over the documentation.

---

## 4. Decisions

**D1. A packet's service is resolved once, deterministically, from the
payload, and recorded with how it was resolved.**
- Order of evidence:
  1. `flowMetaData.stage` (and `subStage`, where a service needs it) matched
     against the registry;
  2. a `sourceTopic` pattern;
  3. the reason code appearing in exactly one service's documentation file;
  4. otherwise, unresolved.
- The stage comes first because it is the producer's own statement of where
  the packet failed. A reason code can be raised from a shared library, so it
  can appear under more than one service.
- If the stage and the documentation disagree, the stage wins. The
  disagreement is recorded and shown to the Investigator. This is the same
  treatment the DLT lane gives a refId on which the record key and the payload
  disagree.
- No model chooses the service. Policy, tools, documentation, rule source and
  runbooks all depend on it; if a model chose it, every one of them would stop
  being deterministic.

**D2. The unit of per-service knowledge is a service pack in the repository,
`src/service_packs/<service>/`.**
- `<service>` is the canonical service name. It must equal the stem of that
  service's S3 documentation file and, by default, its DROA corpus directory in
  `docs_cache/`.
- Packs live in the repository, not S3, because they are prompts: they are
  reviewed, versioned, hashed into the prompt fingerprint, and changed by the
  learning loop's commits. The generated reason-code documentation stays in S3.
- Environment-specific values do not go in a pack: namespaces, database hosts
  and credentials stay in the environment. They differ between staging and
  production, and a pack does not.

**D3. Prompts are composed from three parts: a generic role prompt, the service
pack, and the per-packet evidence.**

| Option | Verdict |
|---|---|
| A. Extend the one prompt to cover every service | Rejected. The glossaries contradict each other; every packet carries about eleven policies; one service's edit moves every service's fingerprint |
| B. A full prompt set per service | Rejected. About 55 files sharing most of their text; shared fixes need eleven edits and drift apart; generic learned rules cannot reach other services |
| C. Generic role prompts plus a service pack, composed in code | **Chosen** |

**D4. Service-specific instructions go into the system prompt of an agent built
for that (role, service) pair. The agent comes from a pool inside the one
compiled graph.**
- A per-(role, service) agent is needed anyway:
  - tools must differ per service (D7);
  - a deep agent's tools are fixed when it is built (2.2).
- Putting the pack into that same agent's system prompt keeps instructions
  (system prompt) apart from evidence (user message). It also gives each
  service a stable prompt prefix, which suits prefix caching.
- The pool covers the Investigator, the Reviewer and Synthesis. The LogFilter
  stays a single agent, because it uses no policy and no tools.
- Agents for enabled and pilot services are built when the graph is built;
  others are built on first use and kept. That is at most 3 roles x (registered
  services + 1) agents.
- The graph, its nodes, its edges and the checkpointer are unchanged. A node
  picks its agent by `state["service"]`.
- When the tool catalog goes stale, the graph is rebuilt as it is today, and
  the pool goes with it.
- The deep agent's `task` subagent must receive the same service-scoped tool
  list as its parent. If it received the role's full list, the subagent would
  be a way around the scope.
- Considered and rejected:
  - Putting the service text in the user message and keeping one agent per
    role: this cannot change the tools.
  - Middleware that filters tools on every model call: the `task` subagent and
    the opencode harness would each need their own filter, and the AVAILABLE
    TOOLS section of the system prompt would still list the wrong tools.
  - One graph per service: more checkpointer wiring, for a flow that is
    identical.
  - Choosing tools by semantic similarity: this is not deterministic, and the
    service is already known exactly.

**D5. Services are switched on one at a time. A packet of a service that is not
switched on is acknowledged without being analysed.**
- `REJECTION_SERVICES_ENABLED` defaults to `enu-biometric`.
- A packet that is unresolved, belongs to an unregistered service, or belongs
  to a service that is not enabled is acknowledged:
  - without analysis;
  - without any casebook or status file;
  - counted by service and reason.
- No casebook, because a finished casebook would stop the packet being analysed
  after its service is switched on: the duplicate checks read it.
- Packets that arrived before their service was switched on are analysed only
  if they are replayed from Kafka within its retention period. This is
  accepted.
- Unresolved packets are skipped by default rather than run with a generic
  pack, because using the wrong pack is the failure this plan exists to
  prevent. The skip counter shows which mappings are missing.
  - `REJECTION_UNRESOLVED_SERVICE=default_pack` runs them with the `_default`
    pack instead, and caps their confidence at 0.6.
- Phase 1 ships this gate in `record` mode: the service is resolved and
  recorded, and nothing is skipped. The owner switches it to `enforce` once the
  recorded resolutions are known to be right.
- **`record` mode analyses nothing differently, in any phase** (revised
  2026-09-28, replacing "from Phase 2 on, `enforce` is required"). A packet
  the gate would skip is analysed with the pre-registry pack, `enu-biometric`
  -- the pack every packet used before packs existed.
  - So a wrong mapping found while recording costs nothing, and deploying
    Phase 2 does not force the switch to `enforce` before the recorded
    resolutions have been checked.
  - A boot error for `record` with Phase 2 code would have stopped the API
    on its default setting.
  - `service_registry.pack_for` makes this decision, the same way `gate`
    decides.

**D6. The rules database stays a deterministic lookup. It runs only for
services whose registry entry declares `rule_source.type = "rules_db"`, which
today is only enu-biometric. It does not become a tool the model decides
whether to call.**

Five reasons:
1. For a rejection, the rule is the primary evidence. The architecture resolves
   primary evidence before the model runs (`SYSTEM_OVERVIEW.md` section 2,
   decision 2), so it cannot be skipped, mistyped, or looked up for the wrong
   code. Line 1 of `pending_rules.jsonl` is this repository's own record of a
   reason code typed by the model (`RESIDENT_MAN_MAN_DEDUP_DUPLICATE`)
   producing a false "rule not found".
2. `runbook_lookup_node` needs the rule before any model runs, to decide
   whether a runbook is stale. A tool the model may or may not call cannot
   provide that.
3. The Reviewer checks the investigation against the same rule. As a tool, the
   Reviewer would see the rule only if the Investigator happened to call it.
4. A description is only advice. With eleven services and many tools, a model
   will sometimes call a biometric-only tool on another service's packet. A
   "Rule not found" from the biometric table then reads like a finding about
   that packet. Not offering the lookup at all makes the mistake impossible.
5. Other services lose nothing. Their documentation files carry the
   rule-engine rules in `rules.rules[]` (for enu-biometric, the same 58 rules
   the table holds). For a service without a rules database, the documentation
   is the rule source.

For a service with `rule_source.type = "none"`:
- the rules database is not called;
- there is no "Database Rule Configuration" section; a `### Rule source`
  section carries a new provenance note instead (Appendix B.10);
- `get_error_description` is not used; it stays on the `rules_db` path only;
- runbooks bind to a hash of the documentation instead of the rule's
  fingerprint (D11).

If a second service ever gets a rules database, it declares its own
`rule_source` with its own connection settings and enrolment-type filter, and
the rule cache key gains the service.

**D7. Tools are scoped by (role, service), and the scope is enforced when the
agent is built.**
- `Toolset` gains `services`: a tuple of service names, or `("*",)` for a tool
  whose meaning holds for every service. It is published in the tool's MCP
  `_meta` as `uidai.crm/services`.
- The scope is never bypassed: `AGENT_TOOLS_<ROLE>` can still narrow or
  replace a role's list, but it cannot add a tool outside the service's scope.
- A tool whose listing declares no services goes to no service unless it is
  named in `AGENT_TOOLS_COMMON` or in a pack's `tools.include`. This covers a
  server that does not know this metadata, and follows today's rule for a tool
  that names no roles.
- Unresolved packets (when D5 lets them through) get only the `"*"` tools.
- The harness gets one opencode agent per (harness role, service), with exactly
  that selection (5.6).

**D8. Service-specific tool names carry the service's prefix, and the prefix
is enforced.**
- Each service declares a `tool_prefix`, for example `bio`. Prefixes are unique
  across the registry.
- A toolset scoped to exactly one service must name every tool
  `<prefix>_...`. Common tools take no service prefix.
- For this repository's own tools, a prefix violation or a name clash is an
  error at start-up. Today a clash is only logged, and the second tool
  silently disappears.
- The biometric process-DB tools are renamed with `bio_` (for example,
  `get_parking_status` becomes `bio_get_parking_status`) in the same phase
  that scopes them. Their guidance text names them, so the rename and the
  scoping are one change.
- Considered and rejected: prefixing only new tools and leaving the biometric
  ones as they are. The unprefixed names are exactly the ones another service
  is most likely to want, and a rename costs more once casebooks, dashboards
  and tests refer to them.

**D9. Database connections are shared per database. Each toolset says which
database it reads.**
- `_process_db.py` becomes a general read-only database layer: one engine and
  one circuit breaker per database key, each with its own `enabled` switch and
  settings.
- It keeps every guarantee the process-DB tools have today:
  - a read-only session;
  - `max_execution_time` on every query;
  - capped rows and output size;
  - redaction;
  - indexed predicates only, with no SQL taken from the model.
- A toolset is served only while its database is enabled. One database's
  outage opens only that database's breaker.
- A common database does not make a common tool. A toolset for one service can
  read a shared database, for example that service's own tables in a shared
  schema.
- Service teams may run their own MCP servers (already supported through
  `AGENT_MCP_SERVERS`), publishing `uidai.crm/agents` and `uidai.crm/services`
  in each tool's metadata. The prefix rule applies to them too.

**D10. The reason-code documentation lookup reads the packet's own service
first.**
- Entries come from `services/<service>.json`.
- Only when that file has no entry for the code are other services' entries
  included. They go under a heading saying they belong to another service, and
  why that can happen: a shared library, or a misattributed service.
- `lookup` keeps outcome `hit` for such a result and adds `scope`, either
  `own` or `other_service`, so every existing reader of the outcome keeps
  working.
- A file's name must equal its `service` field; a mismatch is a validation
  error.
- Parsed files are cached on (path, mtime, size), so a refreshed file still
  takes effect without a restart.
- The S3 download follows `docs_loader.download_corpus`:
  1. list the files;
  2. download them into a temporary directory;
  3. validate them;
  4. swap them into place, keeping the last good copy if anything fails.

  If the fetch already happens outside the application (question 11.3), keep
  it there and only validate.
- The enrolment-type labels in the documentation's title line come from the
  service pack.

**D11. Runbooks and learned rules are keyed by service.**
- Runbooks:
  - Location: `src/runbooks/{draft,final}/<service>/<CODE>__<TYPE>.json`.
  - Schema 1.2 adds `service`, and a `binding` field of the form
    `{"type": "db_rule" | "reason_code_doc", "fingerprint": ...}`.
  - A documentation binding hashes the selected entries only, not the rendered
    title, so editing a label in a pack does not make every runbook stale.
  - `RUNBOOK_SERVE_ALLOWLIST` entries become `service:CODE`. A bare `CODE`
    still means enu-biometric, with a deprecation warning.
- Learned rules:
  - Each proposal carries `service`, and a `scope` of `service` (the default)
    or `generic`.
  - `promote_rules.py` writes `service` rules to the pack's `learned_rules.md`
    and `generic` rules to `src/prompts/learned_rules.md`.
  - The default is `service` because the costs are lopsided. A rule wrongly
    marked generic contaminates every service; a rule wrongly marked
    service-only merely fails to spread.

**D12. Redaction is global, not per service.** The union of every service's
sensitive fields is removed from every packet's logs. Redaction must not depend
on the service having been resolved correctly.

**D13. The prompt fingerprint is per service.**
- It hashes the generic prompts, the pack's files, its `service.json`, and
  that (role, service)'s tool material.
- The casebook records it together with the service.
- Every fingerprint changes once, when Phase 2 lands. Record the date, so the
  accuracy reports can separate before from after.

**D14. The Synthesis contract stays global.** `ACTIONS` and `RESIDENT_ACTIONS`
are read by every casebook consumer. A service that needs a new action is a
contract change, decided separately.

**D15. DLT: the registry reserves `dlt.consumer_groups`, `dlt.original_topics`
and `dlt.java_packages`.** This lets the DLT lane later resolve services the
same way. Nothing reads these fields until the DLT contract is final.

---

## 5. Contracts

### 5.1 Layout

```
src/service_packs/
  README.md                 this contract, for pack authors
  _default/
    service.json            used for unresolved packets when D5 allows them
    policy.md               Appendix C
  enu-biometric/
    service.json            identity, matching, enrolment types, rule source, logs, tools
    policy.md               business policy and glossary (today's agent_policy_context.md)
    investigator.md         Investigator rules: enrolment types, terminology
    reviewer.md             checks the Reviewer applies for this service
    synthesis.md            how findings map to actions for this service
    learned_rules.md        promoted learned rules for this service (starts empty)
  <service>/
    ...
```

`service.json` and `policy.md` are required. The other files are optional.

### 5.2 `service.json`

The example is enu-biometric. The `match` values are to be confirmed in
Phase 0.

```json
{
  "schema_version": 1,
  "service": "enu-biometric",
  "display_name": "ENU Biometric (BIO stage, BIO_DEDUP)",
  "tool_prefix": "bio",
  "match": {
    "stages": ["Biometric"],
    "sub_stages": [],
    "source_topics": []
  },
  "enrolment_types": {
    "payload": {
      "N": {"family": "E", "label": "New Enrolment (1:N deduplication)"},
      "E": {"family": "E", "label": "New Enrolment (1:N deduplication)"},
      "U": {"family": "U", "label": "Biometric Update (1:N deduplication and 1:1 authentication and append)"},
      "Z": {"family": "Z", "label": "Reactivation (1:N deduplication and 1:1 authentication and append)"}
    },
    "family_labels": {"E": "New Enrolment (E)", "U": "Biometric Update (U)"},
    "doc_aliases": {"ENROLMENT": "E", "ENROLLMENT": "E", "UPDATE": "U"}
  },
  "rule_source": {
    "type": "rules_db",
    "enrolment_type_filter": {"N": "ENROLMENT", "E": "ENROLMENT", "U": "UPDATE"}
  },
  "reason_code_docs_file": "enu-biometric",
  "droa_corpus_dir": "enu-biometric",
  "logs": {
    "app_names": ["enu-biometric"],
    "k8s_match": {"name_contains": "enu-biometric"},
    "also_search": [],
    "decision_vocabulary": "biometric.*match|MAN_DEDUP|dedup.*reject|quality.*check.*fail"
  },
  "tools": {"include": [], "exclude": []},
  "dlt": {"consumer_groups": [], "original_topics": [], "java_packages": ["com.uidai.enu.biometric"]}
}
```

| Field | Meaning |
|---|---|
| `service` | Canonical name. Must equal the directory name and the S3 file stem |
| `tool_prefix` | Unique across the registry. Pattern `^[a-z][a-z0-9]{1,11}$` |
| `match.stages` / `match.sub_stages` | Values of `flowMetaData.stage` / `subStage`, compared case-insensitively. `sub_stages` empty means any |
| `match.source_topics` | Anchored regexes over `sourceTopic`. Used only when the stage does not decide |
| `enrolment_types.payload` | Raw `packetMetaData.enrolmentType` value -> its family and the label the prompts show. Replaces `ENROLMENT_TYPE_DISPLAY` |
| `enrolment_types.family_labels` | The label in the documentation's title line. Replaces `_TYPE_DISPLAY` |
| `enrolment_types.doc_aliases` | The type names the documentation's rule conditions use -> family. Replaces the `_TYPE_FAMILY` mapping inside `normalize_doc_type` |
| `rule_source.type` | `rules_db` or `none` (D6). `enrolment_type_filter` replaces `_ENROLMENT_TYPE_ALIASES` |
| `reason_code_docs_file`, `droa_corpus_dir` | Default to `service` |
| `logs.app_names`, `logs.k8s_match` | This service's Elasticsearch `application_name` values and pod match. The namespace stays in `K8S_SERVICE_MAP` / `K8S_DEFAULT_NAMESPACE` |
| `logs.also_search` | Other registered services whose logs are worth reading for this service's packets |
| `logs.decision_vocabulary` | Added to the generic decision-vocabulary regex |
| `tools.include` / `tools.exclude` | Widen or narrow this service's tool scope by name. Never widens the roles a tool is for |
| `dlt.*` | Reserved (D15) |

### 5.3 Rules for pack content

- Packs are generic. No refId, eventId, UUID, timestamp or long digit run. This
  is checked with `validate_generic_text` and `INJECTION_MARKERS`, the same
  checks the reason-code documentation passes.
- A pack describes what the service does and what its terms mean. It does not
  repeat the generic rules (evidence gaps, output format, citations).
- A pack must define every term its documents and logs use in a way another
  service might not. For example, enu-biometric's `policy.md` keeps its
  "demo means face" override.
- Size cap: `SERVICE_PACK_MAX_CHARS`, default 20000, applies to the largest
  composed pack text for any role. Today's biometric policy file is about
  4.8 KB. Exceeding the cap is an error at start-up.

### 5.4 Resolving the service

```
resolve(payload) -> ServiceResolution

stage, sub_stage = flowMetaData.stage, flowMetaData.subStage   (stripped, case-insensitive)
1. by_stage = services with stage in match.stages
              and (match.sub_stages empty or sub_stage in match.sub_stages)
   exactly one -> candidate, source "flow_stage"
2. else by_topic = services whose match.source_topics matches sourceTopic
   exactly one -> candidate, source "source_topic"
   more than one -> log an error, treat as no match at this step
3. else by_code = reason_code_docs.services_for_code(first errorReasonCode)
   exactly one -> candidate, source "reason_code_docs"
4. else -> "_unresolved", source "none"

conflict: when 1 or 2 found a candidate, and by_code names exactly one OTHER
          service, keep the candidate and record {"reason_code_docs": <other>}
registry: a candidate with no pack -> skip reason "service_not_registered"
```

Stored shape (`service_resolution.json`, graph state, casebook):

```json
{"service": "enu-biometric", "source": "flow_stage", "matched": "Biometric",
 "conflict": null,
 "detail": {"stage": "Biometric", "sub_stage": "BIO_DEDUP",
            "source_topic": null, "reason_code": "RESIDENT_MAN_DEDUP_DUPLICATE"},
 "registry_sha256": "sha256:..."}
```

Skip reasons: `service_unresolved`, `service_not_registered`,
`service_not_enabled`.

### 5.5 Composing the prompts

The system prompt for role R (investigator, reviewer or synthesis) and service
S, in this order:

As built in Phase 2 (`src/core/prompt_composer.py`):

```
<generic role prompt: src/prompts/<Role>Agent.md>

### SERVICE CONTEXT -- <display_name> [<pack>]
<pack/<role>.md, or a line saying there are no service-specific instructions>

### SERVICE POLICY -- <display_name> [<pack>]
<pack/policy.md>

### LEARNED RULES                      (Investigator only, when any exist)
<src/prompts/learned_rules.md>
<pack/learned_rules.md>

### AVAILABLE TOOLS                    mcp_client.prompt_section(R, S)   (per service since Phase 4)
### OPERATING MODE                     agent_factory.OPERATING_MODE
```

- The heading `### SERVICE POLICY` replaces `### GLOBAL BUSINESS POLICY
  CONTEXT`. Every prompt that referred to the old heading is updated.
- **The user message** of the docs-on and review prompts gains a first
  section, `### Service`: "This packet belongs to `<pack>` (`<display_name>`),
  placed there by its flowMetaData.stage `'<matched>'`."
  - On a conflict, it adds: "The reason code is documented by `<other>`, not
    by `<pack>`: if the evidence points to `<other>`, say so, but do not apply
    its policy."
  - For `_default` it says the packet was not resolved.
  - It is left out when the pack is not the packet's own, because the
    `record` mode fallback would make the statement untrue.
  - The docs-off Investigator prompt is unchanged.
- **The harness** appends a SERVICE CONTEXT block after the rendered template
  (`prompt_composer.harness_service_context`), the way the tools section is
  appended.
  - Its first line says which service the packet was placed in and where that
    service's `docs_cache/` directory is.
  - The templates tell the agent to look for "the SERVICE CONTEXT at the end
    of this task".
  - No template placeholders were added, so every existing `render` call is
    unchanged.

### 5.6 Selecting tools

```
selection(role, service):
  candidates = tools whose agents include role      (or the AGENT_TOOLS_<ROLE> list when set)
  in_scope(t) = "*" in t.services
                or service in t.services
                or (t.services is empty and t.name in AGENT_TOOLS_COMMON)
                or t.name in pack(service).tools.include
  return [t for t in candidates if in_scope(t) and t.name not in pack(service).tools.exclude]

selection(role, None) is allowed only for the DLT roles, and keeps today's
role-only behaviour.
```

opencode agents:
- `crm_<role>__<service_slug>` for each harness rejection role (investigator,
  reviewer) and each registered service. `service_slug` is the service name
  with every character outside `[a-z0-9]` turned into `_`.
- `crm_<role>__default`, with the `"*"` tools only.
- `_task_agent(node, service, config)` falls back to `__default`, never to a
  wider set.
- The DLT roles keep `crm_dlt_investigator` and `crm_dlt_reviewer`.

### 5.7 Validation at start-up

`main_api.validate_service_registry()` checks these at boot and exits on an
error, the way `validate_config` does. It is API-only, like
`validate_reason_code_docs`, because the consumers never read a pack. Each
check lands in the phase that introduces what it checks.

Errors:
- Phase 1:
  - `service.json` fails the schema, or has an unknown key at any level;
  - `service` differs from the directory name;
  - a `tool_prefix` or a `reason_code_docs_file` is shared;
  - `_default/` is missing, matches something, or has a `tool_prefix`;
  - `logs.also_search` names a service that is not registered;
  - two services share a (stage, sub_stage) pair;
  - a name in `REJECTION_SERVICES_ENABLED` is not registered, or is
    `_default` or `_unresolved`;
  - `REJECTION_SERVICE_GATE` or `REJECTION_UNRESOLVED_SERVICE` has an unknown
    value.
- Phase 2 (implemented; each failure leaves the pack out of the registry):
  - `policy.md` is missing or empty;
  - a content rule (5.3) fails;
  - the composed size exceeds `SERVICE_PACK_MAX_CHARS`.

  `REJECTION_SERVICE_GATE=record` with Phase 2 code is **not** an error; see
  D5.
- Phase 3:
  - an enabled service has `rule_source.type = "none"`, but
    `REJECTION_REASON_CODE_DOCS_ENABLED` is off;
  - an enabled service has `rule_source.type = "rules_db"`, but no database
    settings are configured and `USE_MOCK_DB` is off.
- Phase 4 (implemented): a local toolset's `services` names an unregistered
  service; a tool breaks the prefix rule (D8); or two local tools share a
  name. Also, when a pack is loaded: `_default` has a `tools.include`, or a
  service is named `default`. `validate_config` checks every database that is
  on has its connection settings.
- Phase 7 (implemented): a name in `REJECTION_SERVICES_PILOT` is not
  registered, is `_default` or `_unresolved`, or is also in
  `REJECTION_SERVICES_ENABLED`. The Phase 3 rule-source errors cover pilot
  services too.

Warnings:
- Phase 1:
  - a registered service can be matched by neither stage nor topic;
  - a documentation file exists for an unregistered service;
  - a registered service has no documentation file.
- Phase 2: the harness is on and an enabled service's `droa_corpus_dir` is
  absent from `docs_cache/`. This is checked at readiness, because the corpus
  downloads in the background.
- Phase 4: a local toolset for a rejection role declares no services, so it
  reaches no service unless `AGENT_TOOLS_COMMON` or a pack includes its tools.

### 5.8 State, artifacts, casebook, metrics

- `GraphState` gains `service: str` and `service_resolution: dict`. LangGraph
  only carries keys that are declared.
- `/fetch-logs` writes the artifact `service_resolution.json`, and
  `_investigate_packet` reads it back (or resolves and writes it, on the
  `/process-rejection` path). The route passes the resolution into the graph
  with the payload. `fetch_logs_node` fills the two keys only for an
  invocation that did not pass them.
- A checkpoint written before Phase 1 resumes past `fetch_logs_node` without
  the keys. Nothing reads them in Phase 1. From Phase 2, the helper
  `_service_of(state)` resolves from the payload when the key is missing.
- Casebook additions:
  - `packet_metadata.service`;
  - `packet_metadata.service_resolution`;
  - `resolution.provenance.prompt_fingerprint`, now per service;
  - `resolution.provenance.service_pack` = `{"service", "sha256"}`;
  - `pilot: true` on pilot services' casebooks.
- `packet_status.service` keeps holding `flowMetaData.stage`, for its existing
  readers. Its name is misleading; say so in `ARCHITECTURE.md`.
- New metrics:
  - `SERVICE_RESOLUTIONS{service, source, conflict}`;
  - `REJECTIONS_SKIPPED{service, reason}`.
- Metrics gaining a `service` label, each in the phase that changes the code
  emitting it: `PACKETS_TOTAL` and `PACKET_DURATION` (Phase 1), `LLM_CALLS`
  (Phase 2), `REASON_CODE_DOC_LOOKUPS` (Phase 3, which also adds `scope`) and
  `RUNBOOK_LOOKUPS` (Phase 5). The registry and the documentation files bound
  the label's cardinality: a resolution never names anything else.
- `accuracy_report.py` gains `--service`, and outcome records a `service`
  field, in Phase 7 (implemented).

---

## 6. Phases

| Phase | What | Changes enu-biometric's analysis? | Rough size |
|---|---|---|---|
| 0 | Discovery and baseline | No | No code |
| 1 | Registry, resolution, intake gate | No (`record` mode) | Medium |
| 2 | Prompt layering, agent pool, per-service fingerprint | Prompt text reorganised; gated on parity | Large |
| 3 | Rule source and documentation per service | No | Medium |
| 4 | Tools per service | The biometric tools are renamed `bio_` | Large |
| 5 | Runbooks and learned rules per service (implemented) | Runbook paths move | Medium |
| 6 | Logs and privacy (implemented) | Wider redaction; logs searched per packet | Large |
| 7 | Onboarding a service (repeat for each; pilot mode implemented) | No | Small each, plus SME time |
| 8 | DLT lane (implemented) | No: its fingerprints, version read and prompts are unchanged; its logs follow Phase 6 | Large |

- Phases 3 to 6 depend on 1 and 2, but not on each other. They can run in
  parallel, each with its own checkpoint.
- Phase 6 must be complete before any service other than enu-biometric is
  enabled or piloted.

### Phase 0 -- Discovery and baseline (no code change)

- **0.1 Sample the shared topic for about a week.** Use a new consumer group,
  never the production one. Count by (`flowMetaData.stage`, `subStage`,
  `sourceTopic`, `category`, `eventType`). For each stage, record:
  - the reason codes and their volumes;
  - the `enrolmentType` values;
  - the top-level keys and the `packetMetaData` keys, as a PII check.

  Output: the proposed `match` rules for each service, and the volume per
  service.
- **0.2 The documentation.**
  - Confirm where the S3 fetch runs.
  - Download the files and run
    `.venv/bin/python -m src.tools.check_reason_code_docs --dir <root containing services/>`.
  - For each service, measure what share of its sampled reason-code volume its
    file documents.
- **0.3 Service names.** Confirm that the canonical names are the S3 file
  stems, and that they match the directories in `docs_cache/MANIFEST.json`.
- **0.4 Tool and database inventory, per service.** For each database:
  - the database and its tables;
  - whether `refid` (or the key the tool would use) is indexed;
  - a read-only account;
  - the owner;
  - whether it is shared with other services.
- **0.5 Current exposure.** Does the running deployment already consume the
  shared topic? If it does, non-biometric rejections are being analysed with
  the biometric policy today. Ship Phase 1 straight into `enforce` mode,
  ahead of everything else.
- **0.6 Baseline.**
  - Record the failing test ids.
  - Fix a parity set of about 50 biometric packets that have a stored
    `fetched_logs.txt`, their current casebooks, and recorded outcomes where
    they exist. Phase 2's parity check replays these.

**Checkpoint:** the owner confirms the service list, the `match` rules and the
answers to section 11.

### Phase 1 -- Registry, resolution and intake gate (implemented 2026-09-28)

Goal: every packet carries its service and how it was resolved. Packets of
services that are not enabled can be skipped. enu-biometric's analysis is
unchanged.

Changes, as built:
- **Packs.** `src/service_packs/enu-biometric/service.json` (5.2),
  `src/service_packs/_default/` (`service.json` and the Appendix C
  `policy.md`), and `src/service_packs/README.md`.
- **`src/utils/paths.py`:** `SERVICE_PACKS_DIR`.
- **New module `src/utils/service_registry.py`:**
  - `load()` builds the registry once per process and keeps it; `reset()`
    exists for tests. A pack with any error is left out.
  - `validate()` covers the Phase 1 checks in 5.7.
  - `resolve(payload) -> ServiceResolution` (5.4).
  - `gate(resolution) -> GateDecision`, `skip_reason()`,
    `enabled_services()`, `gate_mode()` and `unresolved_policy()`. A blank
    `REJECTION_SERVICES_ENABLED` means the default, never "nothing".
  - `load_or_resolve(storage, event_id, payload)` and `persist(...)`: a
    stored resolution wins over a fresh one.
- **`reason_code_docs`:**
  - `services_for_code(code)`: a code -> file-stem index over every
    documentation file, memoised on the files' (name, mtime, size), never
    raising;
  - `documented_services()`.
- **The gate, in two routes** (`routes.fetch_logs` and `_investigate_packet`,
  which serves both `/analyze-rejection` and `/process-rejection`):
  - It runs after the terminal check and before anything is fetched or
    written. In `enforce` mode it returns 200 with
    `{"status": "skipped", "reason": ..., "service": ..., "event_id": ...}`,
    and a skipped packet leaves nothing behind.
  - Otherwise `/fetch-logs` stores `service_resolution.json`. In `record`
    mode, what `enforce` would do is logged.
- **Graph state.** `GraphState` gains `service` and `service_resolution`. The
  route passes them in with the payload for a fresh invocation.
  `fetch_logs_node` fills them (`_service_update`) only for an invocation that
  did not pass them.
- **Casebook:** `packet_metadata.service` and
  `packet_metadata.service_resolution`.
- **Metrics:**
  - new `SERVICE_RESOLUTIONS` (fresh resolutions only) and
    `REJECTIONS_SKIPPED`;
  - a `service` label on `PACKETS_TOTAL` and `PACKET_DURATION`
    (`tests/test_audit_phase2.py` updated for it).
- **Boot:** `main_api.validate_service_registry()`.
- **Settings:** `SERVICE_PACKS_DIR`, `REJECTION_SERVICE_GATE` (`record` |
  `enforce`, default `record`), `REJECTION_SERVICES_ENABLED` (default
  `enu-biometric`), `REJECTION_UNRESOLVED_SERVICE` (default `skip`). All four
  are isolated in `tests/conftest.py`, which also resets the registry around
  every test.
- **Documentation:** `ARCHITECTURE.md` (section 3.2.3, the 1.3.1 master-flow
  diagram, 4.2, the directory tree, and a section 5 entry) and `.env.example`.

Tests, as built: `tests/test_service_registry.py` and
`tests/test_service_gate.py`.
- **Resolution, table-driven:**
  - stage, case-insensitive and stripped;
  - stage with subStage;
  - topic, matched against the whole string;
  - two topics matching;
  - the documentation fallback, through codes and rules;
  - `reason_code_docs_file` mapping;
  - an unregistered documentation file;
  - a conflict, and the agreeing and ambiguous non-conflicts;
  - unresolved;
  - malformed and missing fields.
- **Validation:** every Phase 1 check in 5.7; the shipped registry validates
  clean and places a biometric packet; the load-once contract.
- **The gate:**
  - `record` skips nothing but reports the reason; `enforce` skips each kind;
  - an enabled service goes through;
  - the `default_pack` policy;
  - a blank enabled list.
- **Routes:**
  - a skipped packet fetches no logs and leaves no artifact, `status.json` or
    casebook, and is neither published nor claimed;
  - `record` mode stores the resolution;
  - a resolution is counted once across both stages;
  - the stored resolution wins at the analysis stage.
- **Graph:**
  - the resolution reaches the graph and is not replaced;
  - a direct invocation resolves one;
  - a real checkpoint written without the keys resumes, and its casebook
    still records the service.
- **Full suite:** the same 56 failures as the baseline, and no new ones.

**Checkpoint:** after a few days in `record` mode, the owner reviews
`SERVICE_RESOLUTIONS` and a sample of the recorded resolutions, then switches
the gate to `enforce`.

### Phase 2 -- Prompt layering, agent pool, per-service fingerprint (implemented 2026-09-28; parity check not run)

Goal: the prompts contain nothing service-specific, and each packet's agents
are built from its own service pack. For enu-biometric, the composed prompts
carry the same instructions as before.

Changes, as built:
- **Carried from Phase 1:**
  - `service_registry.pack(name)`, `pack_for(resolution)`,
    `packs_to_prebuild()` and `enrolment_labels(pack)`;
  - `_service_of(state)`, `_pack_of(state)` and `_service_resolution_of(state)`
    in the orchestrator, for nodes reading a checkpoint written before these
    keys existed;
  - the Phase 2 checks in 5.7, run when the pack is loaded;
  - a `service` label on `LLM_CALLS` (`unknown` on the DLT lane's sites).
- **The biometric text was extracted, byte for byte**, from the saved
  originals (`tests/fixtures/prompts_before_service_packs/`) into
  `src/service_packs/enu-biometric/{investigator,reviewer,synthesis}.md`.
  `agent_policy_context.md` was moved unchanged to
  `src/service_packs/enu-biometric/policy.md`.
  - The role prompts and harness templates were rewritten per Appendix B,
    keeping every generic line as it was.
  - The `Dockerfile` no longer copies the old root file, and `check_drift.py`
    names the pack policy instead.
- **Composition:** `src/core/prompt_composer.py` --
  `compose_system_prompt(role, pack)`, `service_context`, `service_note`,
  `harness_lead` and `harness_service_context`, per 5.5. It has a CLI.
- **The pool** lives inside `_build_agent` (D4).
  - `agent_for(role, pack)` builds on first use and keeps the agent.
  - The prebuilt packs are built with the graph, in the order the agents
    always were (investigator, log_filter, synthesis, reviewer), so the
    existing build-order and build-count tests still hold.
  - `build_agent`'s signature is unchanged.
- **Graph state** gains `service_pack`, decided once per packet: by the route
  (`pack_for`, next to the gate), or by `fetch_logs_node` for an invocation
  that did not pass it.
- **User message:**
  - the docs-on Investigator, retry and review builders take `service_note`,
    rendered as a first `### Service` section (5.5);
  - `enrolment_type_display(payload, pack)` reads the pack and replaces
    `ENROLMENT_TYPE_DISPLAY`; `_write_harness_case_files` passes the pack.
- **Harness:**
  - `harness/rules/rejection.md` is generic.
  - The templates point at "the SERVICE CONTEXT at the end of this task".
  - The block is appended by `_with_service_context`, before the tools
    section.
  - The root `AGENTS.md` opening and "Project purpose" are service-neutral.
- **Fingerprint and provenance:**
  - `prompt_fingerprint(pack)` is cached per pack, emptied when the graph is
    rebuilt, and defaults to the pre-registry pack.
  - `compute_prompt_fingerprint(base_dir, pack)` folds in the pack's digest
    and the generic `learned_rules.md`.
  - The casebook records `provenance.prompt_fingerprint` and
    `provenance.service_pack` for the pack the graph actually used.
- **Learned rules:**
  - a proposal records `service` and `service_pack`;
  - `promote_rules.py` appends to that pack's `learned_rules.md`, or to the
    pre-registry pack's for an older entry, and checks both `src/prompts/` and
    `src/service_packs/` for uncommitted changes;
  - the Investigator's prompt gets a `### LEARNED RULES` section when either
    the generic or the pack file has any.
- **`_default` confidence cap:** `apply_confidence_policy(..., default_pack=)`
  caps at `SYNTHESIS_UNRESOLVED_SERVICE_CONFIDENCE_CEILING` (0.6).
- **Corpus check:** after the documentation corpus downloads, `main_api` logs
  a warning for an enabled service with no `docs_cache/` directory.
- **Wording:** `RunbookGenerator.md` and the API description are
  service-neutral.
- **Deferred to Phase 4:** per-service opencode agents and the `service`
  argument to the tool selection. In Phase 2 they would only carry an unused
  value.

Tests, as built: `tests/test_service_prompts.py`, plus updates to
`test_audit_phase2.py` (fingerprint, `LLM_CALLS` labels),
`test_opencode_harness.py` (enrolment labels, fingerprint cache) and the
registry and gate tests (every pack has a `policy.md`).
- **Content preservation**, direct and harness:
  - every non-blank line of the saved originals appears verbatim in the
    composed enu-biometric prompt for the matching role, or its listed
    replacements do;
  - the list is the reviewable diff of meaning.
- **Neutrality:**
  - the generic rejection prompts, `RunbookGenerator.md` and the root
    `AGENTS.md` contain none of: biometric, dedup, de-duplication, demo,
    nonDemo, face, iris, MBU, ABIS, True Duplicate, parent Aadhaar;
  - words are matched case-insensitively on word boundaries, after service
    names, Java packages and topic patterns are stripped;
  - "fingerprint" is not on the list, because the DLT prompts use it for
    failure fingerprints.
- **Snapshots:**
  - `tests/fixtures/composed_prompts/<pack>/` holds every role's system prompt
    and the harness blocks, for enu-biometric and `_default`;
  - regenerate with `UPDATE_PROMPT_SNAPSHOTS=1` and review the diff.
- **Pool:**
  - the prebuilt pack is built in order with the graph;
  - another pack is built once on first use;
  - a stale catalog rebuilds the pool.
- **Fingerprint:** it differs per pack, and an edit to one pack moves only
  that pack's fingerprint.
- **The pack decision** in every gate mode and unresolved policy.
- **Service notes:**
  - placed, conflict and `_default` cases;
  - the record-mode fallback says nothing;
  - the Service section comes first, only when given.
- **Validation:** a missing or empty `policy.md`, a UUID, instruction-shaped
  text, and the size cap.
- **Learned rules** reach the Investigator only.
- **The `_default` cap**, and the promotion target of a learned rule.
- **Full suite:** the same 56 failures as the baseline, no new ones, and the
  strict xfail still holds.

Parity check (run by the owner; not yet run): replay the Phase 0 parity set
through the code before Phase 2 and after it. Compare:
- the agreement on `action` and `resident_action`;
- the Reviewer retry counts;
- the escalation rate.

Compare distributions, not exact text; the models are not deterministic.
Accept only when the owner has an explanation for every difference. The
changes worth watching:
- the biometric sections now follow the generic ones, where they used to sit
  in the middle of the Investigator prompt;
- the Reviewer's glossary instruction moved into the pack;
- a `### Service` section opens the docs-on and review prompts.

**Checkpoint.**

### Phase 3 -- Rule source and documentation per service (implemented 2026-09-28)

Changes:
- **Rule lookup.** `investigator_node` and `runbook_lookup_node` read
  `pack.rule_source`.
  - `rules_db`: today's path. The enrolment-type filter comes from the pack
    instead of `_ENROLMENT_TYPE_ALIASES`. `get_error_description` is called
    only on this path.
  - `none`: no lookup; `db_rule = ""`.
- **Prompt sections.**
  - `rejection_context` leaves out the "Database Rule Configuration" section
    for a `none` service, and adds a `### Rule source` section holding the
    Appendix B.10 note.
  - `_rule_note` takes the rule-source type.
  - The task text names only "the Reason Code Documentation".
  - The same applies to the retry and review builders.
- **Documentation lookup.**
  `reason_code_docs.lookup(reason_code, raw_type, service)` reads the service's
  own file first, with the `scope` fallback (D10). Labels come from the pack.
  Parsed files are cached. `validate()` gains the file-name check.
- **S3 download** (D10), and a `/ready` check that waits for the documentation
  while it is enabled but not yet on disk.
- **Harness.** `context.json` has no `db_rule` for a `none` service, and the
  generic harness rules say the rule may be absent.
  - For such a service, `_write_harness_case_files` also writes the
    documentation text to `reason_code_doc.md` in the case directory, and the
    harness templates list it among the evidence files.
  - This departs on purpose from REASON_CODE_DOCS_PLAN.md D13, which kept the
    documentation out of the harness prompt. Without the rules database, the
    documentation is the only rule source, so a harness run without it would
    have no rule at all.
  - For enu-biometric (`rules_db`), the harness stays exactly as it is.
- **Metrics.** `REASON_CODE_DOC_LOOKUPS` gains `service` and `scope`.

Tests:
- A `none` service never calls the rules database (assert on the mock).
- Its prompts have no rule section and do have the `### Rule source` note.
- Every provenance note appears in the right case.
- The own-service lookup is used first, and the fallback is labelled.
- A file whose name does not match its `service` fails validation.
- S3 download: the swap works, and a failed download keeps the last good copy
  (use `tests/s3_fakes.py`).
- The enu-biometric prompts are unchanged from Phase 2.

**Checkpoint.**

#### As built (2026-09-28)

Everything above, with these differences and these details.

- **The rule source is read through the registry**, not from the document at
  each call site: `rule_source_of(pack)` (`rules_db` or `none` -- `none` for a
  pack that is not in the registry, because guessing would query one service's
  rules table for another's packet), `rule_type_filter(pack)` and
  `docs_lookup_options(pack)`. `RULES_DB` and `NO_RULES_DB` are constants in
  both `service_registry` and `rejection_context`.
- **`investigator_node`** looks the rule up only when
  `rule_source_of(pack) == rules_db`, passing `type_filter=rule_type_filter(pack)`.
  `lookup_rule_for`, `lookup_rule_text` and `_parse_and_filter_rules` take that
  keyword and fall back to the built-in normalisation without it, so the three
  existing runbook call sites are untouched.
  - The biometric pack's `enrolment_type_filter` was set to
    `tool_registry._ENROLMENT_TYPE_ALIASES` exactly, and
    `test_service_rules.py` asserts they are still equal.
- **`runbook_lookup_node`** returns `_miss("rule_source_none")` for a `none`
  pack, before reading the reason code: a runbook is checked against the
  rules-table rule it was derived from, so for such a service it could only be
  another service's. `RUNBOOK_LOOKUPS` gains that outcome; the `service` label
  is still Phase 5's.
- **The prompts.** `rejection_context` gained `RULE_SOURCE`, the three
  `RULE_NOTE_NO_DB_*` notes, `no_rules_db_note(doc_state)`, and a
  `rule_source=RULES_DB` argument on all three builders. `_rule_section`
  returns `(RULE_SOURCE, note)` for a `none` service instead of an empty
  Database Rule Configuration, `_documentation_body` uses
  `NO_DOCUMENTATION_NO_RULE`, and `_sources` names "the Reason Code
  Documentation" alone (or "the SERVICE POLICY" when there is no document).
  The docs-off inline prompt carries `Rule source: <note>`.
- **The documentation lookup** takes `service_file`, `type_families` and
  `type_labels` rather than a service name, so it stays a pure function of the
  store and needs no registry: `docs_lookup_options(pack)` supplies all three.
  - `scope` is `own` / `other_service` / `all`, and `OTHER_SERVICE_NOTE` opens
    the text in the fallback case. A code the own file documents only for
    another enrolment type stays a miss.
  - The default label for `U` is now the neutral "Update (U)"; the biometric
    wording comes from the pack's `family_labels`.
  - Parsed files are cached on `(mtime_ns, size)`.
  - `validate()` requires a file to be named after its service, and renders
    each file with `service_file=<stem>`.
- **The S3 download.** `REASON_CODE_DOCS_S3_DOWNLOAD` and
  `REASON_CODE_DOCS_REFRESH_SECONDS`, started from the API lifespan in a
  background thread. Staged in a sibling directory, validated with the same
  validator, swapped in with two renames; a failure keeps the last good copy.
  `docs_available()` gates `/ready` only while nothing is on disk.
  - Boot validation does not read the disk when the download is on. It instead
    refuses to start when `REASON_CODE_DOCS_DIR` is unset -- the swap would
    replace the files shipped in `src/` -- or when no bucket is configured.
- **The harness** gets `reason_code_doc.md` for a `none` pack (the text, or
  the note when there is no document -- never an empty file), `context.json`
  without `db_rule`, and an appended `### RULE SOURCE` section naming the file.
  The section is added by `prompt_composer.harness_rule_source(pack, event_id)`,
  so a `rules_db` pack's task is byte-for-byte what it was. A stale
  `reason_code_doc.md` is removed when the same case is written again under a
  `rules_db` pack.
  - The three harness templates list the file, saying it is present only for a
    service with no rules database. Additions only.
- **5.7 gained** the two Phase 3 errors and, beyond the plan, one warning:
  `default_pack` with the documents off, where an unresolved packet would have
  neither a policy nor a document.
- **Tests:** `tests/test_service_rules.py` (15) is new -- it wires the rules
  table to fail the test if a `none` service reaches it. `test_reason_code_docs.py`
  gained the scope, file-name, download, readiness and boot cases.
  `test_service_registry.py` gained the 5.7 cases, and its fixture now gives
  `enu-biometric` a rules table, as the shipped pack has.
- **Full suite:** the same 56 failures as the baseline, no new ones.

### Phase 4 -- Tools per service (implemented 2026-09-28)

Changes:
- **Carried from Phase 2:**
  - `build_agent` takes the pack and passes it to `mcp_client.tools_for` and
    `prompt_section`. The pool in `_build_agent` already builds one agent per
    (role, pack), so only the call changes. Every test fake of
    `build_agent(role, llm, system_prompt, tools=())` must then accept the new
    argument.
  - The general-purpose subagent gets the same scoped list as its parent.
  - `_with_tools_section` takes the pack too, for the harness.
- **Scope plumbing:**
  - `Toolset.services`;
  - `mcp_config.META_SERVICES = "uidai.crm/services"`, published by
    `mcp_server.py`;
  - `RemoteTool.services`;
  - `selection(role, service)`, `tools_for(role, service)`,
    `prompt_section(role, service)`, `fingerprint_material(service)`;
  - the `AGENT_TOOLS_COMMON` setting.
- **Package layout.** `discover()` walks subpackages:
  `agent_tools/common/` and `agent_tools/<service_slug>/`. Python package names
  cannot contain hyphens, so `enu-biometric` becomes `enu_biometric`. Modules
  whose names start with `_` are still helpers.
- **The biometric toolset:**
  - move it under `agent_tools/enu_biometric/`;
  - give it `services=("enu-biometric",)`;
  - rename its tools `bio_*`;
  - update its GUIDANCE text.
- **Prefix and clash checks** at start-up (D8).
- **Database layer** (D9): generalise `_process_db.py`. The existing
  `PROCESS_DB_*` settings keep working, as the settings of the `process`
  database key. `metrics.sample_breaker_states` iterates the per-database
  breakers.
- **opencode:** the per-service agents now carry the scoped selection.
  `_task_agent` takes the service.

Tests:
- The selection matrix: role x service x (common, specific, undeclared,
  include, exclude, `AGENT_TOOLS_<ROLE>` override).
- A prefix violation fails start-up, and so does a local name clash.
- The opencode config gives each per-service agent exactly its tools.
- A fixture-service packet's Investigator, and its `task` subagent, receive no
  `bio_` tool.
- The existing process-DB tests pass under the new names.

**Checkpoint.**

#### As built (2026-09-28)

Everything above, with these differences and these details.

- **Roles and scopes.** `agent_tools.SERVICE_ROLES` (investigator, reviewer,
  synthesis, log_filter) are scoped by service; `DLT_ROLES` are not.
  `mcp_client.selection(role, service=None, *, catalog=None)` refuses a
  rejection role without a service and a DLT role with one, applies
  `AGENT_TOOLS_<ROLE>` first and the scope (`mcp_client.in_scope`) after it.
  `tools_for`, `prompt_section` and `fingerprint_material` take the service
  the same way; `describe()` and the CLI list the selection per pack
  (`prompt <role> --service <pack>`).
  - The pack's `tools.include`/`exclude` come from
    `service_registry.tool_scope(pack)`. A tool naming `_default` reaches
    nobody: the default pack is no service.
  - `Toolset.services` defaults to `()`; a malformed name, `_default`, or
    `"*"` beside another name is refused when the toolset is declared.
- **Agents.** `build_agent(role, model, system_prompt, tools=(), *, pack=None)`
  and `system_prompt_for(role, prompt, pack)`. The pool's agents are built for
  their pack; the LogFilter for `_default`, since one agent serves every
  service. The subagent already received `registered`, now the scoped list.
- **Harness.** `opencode_agent(role, service)` gives
  `crm_<role>__<service_slug(service)>` for the rejection roles (`_default`
  and no service give `__default`) and `crm_<role>` for the DLT roles.
  `opencode_config` builds one agent per rejection harness role for every
  registered service and `_default`. `_task_agent(node, service, config)`
  falls back to `__default`; with an `mcp` block but no usable agent it raises
  `OpencodeUnavailable` rather than run as opencode's default agent, which
  would have every tool, and the node falls back to its direct path.
  `run_task`/`run_task_json` take `service=`, and the orchestrator passes the
  pack, as it does to `_with_tools_section(prompt, role, pack)`.
- **Names.** `service_registry.tool_prefix_error(name, services)` holds the
  rule: a tool scoped to exactly one registered service starts with
  `<prefix>_`; a declared tool scoped otherwise starts with no registered
  prefix; an undeclared tool is not judged. `agent_tools.scope_problems()`
  applies it to every local tool, enabled or not, from
  `service_registry.validate()`; `mcp_client.load_catalog` applies it to every
  served tool and leaves a violator out with an error log.
  `service_registry.service_slug` is shared by the opencode names and the
  package layout.
- **Package layout.** `discover()` walks subpackages (`_tool_modules`), a
  subpackage's `__init__` counting as one of its modules. `common/` ships
  empty. The biometric modules are in `enu_biometric/`, the functions renamed
  `bio_*`, and every guidance and docstring reference with them.
  `mcp_client.COMMON_RULES` cited a biometric tool as its example; it now says
  `"<tool>: <field> is <value>"`, since every service's prompt reads it.
- **Database layer.** `agent_tools/_database.py`: `declare(key, label=,
  default_port=, default_name=)` returns the one `Database` for a key
  (declaring it again differently is an error); each has its settings
  (`PROCESS_DB_*` for `process`, `AGENT_DB_<KEY>_*` otherwise), engine,
  breaker (`process_db_breaker` for `process`), `query`, `run_lookup`,
  `finish` and `missing_settings`. The generic value, argument and redaction
  helpers moved there; `enu_biometric/_process_db.py` declares `process`,
  defines the toolset and re-exports what its modules use. Every message the
  process DB tools returned is unchanged. `config_validator` checks the
  declared databases' settings after the tool modules import.
  `metrics.sample_breaker_states` adds every declared database's breaker --
  in the API process these are that process's own, not the tool server's
  (`ARCHITECTURE.md` section 5).
- **Settings.** `AGENT_TOOLS_COMMON` (isolated in `tests/conftest.py`, and no
  longer reported as an unknown `AGENT_TOOLS_*` setting) and the
  `AGENT_DB_<KEY>_*` family.
- **Tests:** `tests/test_service_tools.py` (44) is new -- the matrix, the
  declaration and prefix checks, the boot failure, a local clash, the
  per-service opencode agents, and a fixture service's Investigator and its
  `task` subagent built as real deep agents over the real tool server.
  `test_process_db_tools.py` gained the database-layer cases. Every existing
  tool test moved to the new names, and every fake of `build_agent` and
  `run_task_json` accepts the new argument.
- **Full suite:** the same 56 failures as the baseline, no new ones.

### Phase 5 -- Runbooks and learned rules per service (implemented 2026-09-28)

Changes:
- **Carried from Phase 1:** a `service` label on `RUNBOOK_LOOKUPS`.
- **Runbook store:**
  - service layout, schema 1.2 and `binding` (D11);
  - `git mv` the 39 drafts into `draft/enu-biometric/` and add
    `"service": "enu-biometric"` to each;
  - the new allowlist format.
- **One documentation lookup per packet.** Resolve the documentation in
  `runbook_lookup_node` before its `RUNBOOK_MODE` check, and return it in every
  branch. The documentation binding and the investigation then use the same
  documentation state. `investigator_node`'s existing reuse of
  `state["reason_code_doc"]` covers older checkpoints.
  - A casebook answered by a runbook then records the documentation state in
    its provenance too. Until now it recorded `None` there.
  - Update the comment in `routes.py` that says these fields are `None` for a
    packet a runbook answered.
- **`build_runbooks.py`** groups casebooks by (service, code, type). A casebook
  from before Phase 1 has no `packet_metadata.service`. It is assigned to
  enu-biometric only when its `packet_status.service` (the stage) matches
  enu-biometric's `match` rules; otherwise it is skipped.
- **`promote_runbooks.py`** and `check_reason_code_docs --coverage` walk the
  service directories.
- **Learned rules.** Phase 2 already records `service` and `service_pack` on
  each proposal, and promotes into the pack's `learned_rules.md`. What
  remains is the scope:
  - `queue_learning_rule` records `scope`.
  - The direct Reviewer's `add_learning_rule` tool gains a `scope` argument.
  - The harness Reviewer's JSON gains `learning_rule.scope`.
  - Both Reviewer prompts say when a rule is generic: only when it concerns
    evidence handling, citations or output format, and names no service
    concept.
  - `promote_rules.py` shows the scope and lets the operator change it. A
    `generic` rule goes to `src/prompts/learned_rules.md`, which the composer
    already reads.
  - The 3 existing pending entries carry no pack and are promoted into
    `enu-biometric`, the pack they were learned under; they need no tagging.

Tests:
- Runbook lookup by service, including the ANY fallback within one service.
- A 1.1 runbook in the enu-biometric directory still loads.
- A bare allowlist code maps to enu-biometric, with a warning.
- A documentation binding stays the same across a pack label change, and
  changes when the entries change.
- Learned rules land in the right file, and the fingerprint moves.

**Checkpoint.**

#### As built (2026-09-28)

Everything above, with these differences and these details.

- **The store** (`utils/runbook_store.py`). Every function takes the service
  first: `get_runbook(service, code, type)`, `write_draft_runbook(service,
  ...)`, `runbook_cache_key(service, code, type)` (`"<service>/<CODE>__<TYPE>"`)
  and `is_serve_allowed(service, code)`. `promote_draft_to_final` takes the
  service from the draft's directory and refuses a draft naming another.
  `list_draft_runbooks` / `list_final_runbooks` walk `<dir>/<service>/*.json`
  and ignore, with a warning, a runbook left at the top level. A service
  directory must be a service name, so `_default` has none.
  - `check_runbook(data, service)`: a 1.2 runbook needs `service` equal to its
    directory and a `binding` of a known type with a fingerprint. A 1.0 or 1.1
    runbook loads only from `enu-biometric/`, and `binding_of` reads its
    `rule_fingerprint` as a `db_rule` binding. Test fakes that return a runbook
    dict with only `rule_fingerprint` therefore keep working.
  - The 39 drafts stay schema 1.1, now with `"service": "enu-biometric"`; they
    are upgraded only if they are redrafted.
- **The documentation binding.** `reason_code_docs.lookup` adds
  `entries_sha256` to every state (`entries_digest` over each selected
  entry's source, kind, ref, enrolment type and body); the rendered title and
  the other-service note are not in it. `runbook_store.doc_binding_fingerprint`
  reads it, None for anything but a hit.
- **The node.** `runbook_lookup_node` calls `_resolve_reason_code_doc` first and
  returns `reason_code_doc` from every branch, `off` included. It looks up
  under `_pack_of(state)`, and counts `RUNBOOK_LOOKUPS{outcome, service}` with
  `_service_of(state)`.
  - Outcomes: `rule_source_none` is removed; `no_service` is added (the
    `_default` pack, before any lookup) and `binding_unavailable` (a
    `reason_code_doc` binding and no documentation hit).
  - A binding of the other type than the pack's rule source is
    `fingerprint_mismatch`. A `db_rule` binding is still compared only when
    the rules table returns rows, as before.
- **The allowlist.** An entry is `service:CODE` when the part before the first
  `:` has the shape of a service name, since a reason code may itself contain
  `:`. Anything else is a bare code, read as the pre-registry service's, with
  a warning logged once per entry per process.
- **`build_runbooks.py`** gained `--service`. `casebook_service(cb)` decides the
  group's service as the plan says, and also leaves out a casebook whose
  `provenance.service_pack` names another pack than its service (a packet the
  gate only recorded, analysed with the pre-registry pack): its resolution was
  reasoned from another service's knowledge. `binding_for(service, code, type)`
  binds a `rules_db` service to the rule, with the pack's type filter, and a
  `none` service to its own documentation only (`scope` `own`); anything else
  writes no draft. The generator's message now opens with `Service:`.
- **`promote_runbooks.py`** gained `--service`, and `--list` checks each final
  runbook's binding through `build_runbooks.binding_for`, reporting a change
  of rule source as stale. `check_reason_code_docs --coverage` checks each
  runbook against its own service's documentation file (the pack's
  `reason_code_docs_file`).
- **Learned rules.**
  - `queue_learning_rule(rule_text, reasoning, scope=None)` records `scope`;
    `normalize_rule_scope` makes anything but `generic` (any case) `service`
    and logs an unknown value. `add_learning_rule` has `scope="service"`; the
    harness passes `learning_rule.scope`.
  - The queue path is `agent_orchestrator.PENDING_RULES_FILE`, read at call
    time, so the new tests write to a temporary file rather than the tracked
    one.
  - `ReviewerAgent.md` and `harness/RejectionReviewer.md` each gained one
    paragraph on when a rule is generic, and the harness JSON schema line
    gained `"scope"` (listed as a replacement in `test_service_prompts.py`).
    The Reviewer snapshots were regenerated; the diff is that paragraph.
  - `promote_rules.py`: `scope_of(entry)`, `target_file_for(entry, scope)`
    and `generic_rules_file()`. The prompt accepts `promote`, or the other
    scope's name to switch and show the diff again. It reads the prompts
    directory through `prompt_composer`, so it follows `paths.REPO_ROOT`.
- **`accuracy_report --shadow`** says "add `<service>:<CODE>`"; outcome records
  carry no service until Phase 7, so the operator supplies it.
- **Documentation:** `ARCHITECTURE.md` (section 3.2.3's new "Runbooks and
  learned rules per service", 3.3, 3.7, 3.8, 3.9, the tree and a section 5
  entry), `SYSTEM_OVERVIEW.md` sections 9 and 10, `.env.example` and
  `src/service_packs/README.md`.
- **Tests:** `tests/test_service_runbooks.py` (49) is new: the store per
  service and its ANY fallback, legacy and malformed runbooks, the shipped
  drafts, the allowlist, the binding across a label change and an entry
  change, every node outcome, drafting and coverage per service, and the
  scope from proposal to promotion and into the fingerprint.
  `test_opencode_harness.py` gained the harness scope case and
  `test_end_to_end.py` the runbook-hit provenance. Every existing runbook test
  moved to the new signatures and the `service` label, and the
  `test_operator_tools.py` casebooks now record their service.
- **Full suite:** the same 56 failures as the baseline, no new ones.

### Phase 6 -- Logs and privacy (required before any other service is enabled; implemented 2026-09-28)

Changes:
- **Logs searched per packet.**
  - `FetchContext` gains `apps`, filled from the pack's `logs.app_names` plus
    `also_search`.
  - `discovery.discover_targets` accepts `apps=`. The Elasticsearch terms
    filter uses the same list.
  - Namespaces still come from the environment.
  - `ES_APP_NAMES` and `K8S_APP_NAMES` remain the fallback for callers with no
    service (the DLT lane, the CLIs).
- **A template catalog per service.**
  - `build_catalog.py --service <name>` writes `template_catalog.<service>.json`
    and keeps Drain3 state per service.
  - A service with no catalog of its own gets no `must_not` filtering: losing
    an evidence line costs more than a longer trace.
- **Decision vocabulary** = the generic regex OR the pack's
  `logs.decision_vocabulary`.
- **Redaction.**
  - Add key-based redaction of JSON fields: the value of `"<key>": ...` is
    replaced for each key in `REDACT_JSON_KEYS`. The default list covers name
    fields, `dob`/`dateOfBirth`, gender, address fields, pincode and relative
    names. The exact keys come from the Phase 0 sample.
  - If Phase 0 found demographic fields in any service's `packetMetaData`,
    `_project_payload` sends only named fields. The projected payload reaches
    the model, the checkpoint and the harness `context.json`.
- **Redaction audit.** A CLI runs over sampled logs from each service and
  counts what is left of the known keys and patterns after redaction. The
  count must be zero before a service is enabled.

Tests:
- Routing: a packet of service S searches S and its `also_search` services
  only.
- Unresolved or skipped packets fetch nothing.
- A service's catalog is used when present; with no catalog, no `must_not`
  clauses are sent.
- Key redaction of nested JSON, escaped quotes and arrays.
- The audit CLI's counting.

**Checkpoint.**

#### As built (2026-09-28)

Everything above, except the payload projection, with these differences and
details.

- **The scope** (`src/log_pipeline/scope.py`). `for_service(name)` returns a
  `LogScope`: `apps`, `pod_matches`, `catalog_path`, `drain3_state_file`,
  `decision_vocabulary` and `searchable`. `unscoped()` is the behaviour from
  before Phase 6, and `for_service(None)` is it. `service_to_search(resolution,
  pack)` decides which a packet gets:
  - its pack, when that is its own service's pack;
  - `_default`, which searches nothing: `reduce_logs` returns
    `not_searched_message`, which is persisted as `fetched_logs.txt` like any
    other fetch;
  - None (unscoped) otherwise, which is a packet `record` mode analyses with
    the pre-registry pack. The plan's "skipped packets fetch nothing" holds
    under `enforce`, where the gate returns before any fetch (Phase 1).
- **The plumbing.** `service` is threaded through
  `fetch_and_persist_logs(..., service=)`, `fetch_logs_for` and
  `reduce_logs`. `/fetch-logs` computes it from the resolution and
  `pack_for`; `fetch_logs_node`'s live fetch from the state's resolution and
  pack.
  - `FetchContext` gained `apps` and `pod_matches`. `apps` None means the
    environment's lists.
  - `fetcher.fetch_logs(apps=)` puts them in the terms filter.
  - `discovery.discover_targets(apps=, pod_matches=)` searches exactly those.
    `resolve_service(match=)` takes a pack's pod match, below an operator's
    `K8S_SERVICE_MAP` entry and above the app name.
  - `service_registry.log_options(name)` reads a pack's `logs`. A service
    that declares no `app_names` searches its own name, as its documentation
    file and corpus directory default to it. `also_search` is followed one
    level, and its services' own vocabularies are not added.
- **The catalog.** `template_catalog.<service>.json` beside the old catalog,
  and `drain3_state/drain3_state.<service>.bin`. The reducer keeps a
  `TemplateMiner` per state file, and the pipeline a catalog per path, each
  read once per process. The pre-registry pack uses the unscoped pair until
  `template_catalog.enu-biometric.json` exists, since that catalog was built
  from its packets; without this, Phase 6 would have removed its `must_not`
  clauses and boilerplate classifications. `build_catalog.py --service`
  always builds the service's own pair, and refuses an unregistered service.
- **The vocabulary.** `config.DECISION_VOCABULARY_REGEX` is now
  `GENERIC_DECISION_VOCABULARY_REGEX`, without `biometric.*match`,
  `MAN_DEDUP`, `dedup.*reject` and `quality.*check.*fail`, which are
  enu-biometric's `logs.decision_vocabulary`. `scope.DecisionVocabulary`
  matches the two as separate patterns, because the generic one's inline
  `(?i)` cannot be nested in an alternation; the pack's is compiled
  case-insensitively. The unscoped vocabulary adds the pre-registry pack's,
  so it is the old regex. `apply_evidence_guardrails(vocabulary=)` falls back
  to the generic words alone.
- **Redaction** (`redaction.py`). `DEFAULT_JSON_KEYS`, `json_keys()`,
  `redact_json_fields(text)`, run by `redact_text` before the patterns and
  counted as `JSON_FIELD`. The value after `"<key>":` is replaced whatever
  it is (string, number, object, array), in plain JSON and in JSON escaped
  one or more levels inside a string, honouring escaped quotes and brackets
  inside strings. A cut-off value is redacted to the end of the line.
  `null`, booleans, empty strings and existing placeholders are left, which
  keeps redaction idempotent. `_active_patterns` became `active_patterns`,
  for the audit.
  - The default list includes `name`, as the plan says. It also redacts an
    operational JSON field called `name`; the Phase 0 sample should decide
    whether to narrow it.
- **The payload projection is not changed.** It is conditional on Phase 0
  finding demographic fields in `packetMetaData` (question 11.9), and Phase 0
  has not been run. It remains to be done before a service whose payload
  carries them is enabled.
- **The audit** (`src/tools/redaction_audit.py`). `--service <name>` over
  files or directories. Each line goes through `redact_text`; what is left is
  counted per key (`key:<name>`) and per pattern (`pattern:<label>`). Keys
  are looked for in any spelling it can recognise (`"key":`, `'key':`,
  `key=`, `key:`), not only JSON, because those are what key redaction
  misses. Empty, `null`, boolean and placeholder values are not counted.
  Findings give the file and line, never the value. Exit 0 when nothing is
  left, 1 when anything is, 2 when there was nothing to read.
- **Documentation:** `ARCHITECTURE.md` (section 3.2.3's new "Logs and privacy
  per service", 3.6, 3.10, 4.3, the tree and a section 5 entry),
  `SYSTEM_OVERVIEW.md` sections 4.2 and 6, `.env.example`
  (`REDACT_JSON_KEYS`, and `ES_APP_NAMES` / `K8S_APP_NAMES` as a fallback)
  and `src/service_packs/README.md`.
- **Tests:** `tests/test_service_logs.py` (43) is new: the scope per service
  and its `also_search`, the `_default` and unscoped cases, the service each
  kind of packet is fetched for through `/fetch-logs` and the graph, the
  Elasticsearch filter and Kubernetes discovery, the pod-match precedence,
  the catalog and parse tree per service, `build_catalog --service`, the
  vocabulary, key redaction, and the audit. `test_evidence_integrity.py`'s
  fake fetchers accept `apps`, and `test_phase_c_fixes.py`'s fake
  `reduce_logs` accepts `service` and checks it is passed through.
- **Full suite:** the same 56 failures as the baseline, no new ones.

### Phase 7 -- Onboarding a service (repeat for each)

Follow the checklist in section 7. Pilot mode needs one small addition, made
the first time:
- `REJECTION_SERVICES_PILOT` lists services that are analysed normally, with
  two differences:
  - their casebooks carry `pilot: true`;
  - their Synthesis agent is built without `queue_for_replay`.

  `service_registry.pilot_services()` reads the setting, the gate lets pilot
  services through, and start-up validation requires every listed name to be
  registered.
- The service's experts record verdicts through `POST /outcome/{event_id}`.
- `accuracy_report.py --service <name>` measures the result. For that,
  `outcomes.record_outcome` copies `packet_metadata.service` into each outcome
  record. An outcome recorded earlier has no service; read it as
  enu-biometric, the only service analysed before then.
- The owner moves the service from `REJECTION_SERVICES_PILOT` to
  `REJECTION_SERVICES_ENABLED` once it meets the agreed bar (question 11.7).

#### As built (2026-09-28)

The one-time addition above, with these details. No service has been
onboarded: that needs Phase 0's `match` values and a pack from the service's
experts (section 7, items 1 and 2).

- **The registry.** `ENV_PILOT`, `pilot_services()` (blank means none; there
  is no default), `is_pilot(pack)` and `analysed_services()`, which is
  enabled or pilot.
  - `skip_reason` lets an analysed service through.
  - `packs_to_prebuild` and `missing_corpus_dirs` cover pilot services.
  - `validate()` adds the Phase 7 errors in 5.7, and applies the Phase 3
    rule-source errors to every analysed service.
  - A service in both lists is a pilot wherever validation has not run: the
    more cautious reading.
- **The graph.**
  - `GraphState.pilot` is decided with the pack: by the route
    (`is_pilot(service_pack)`), or by `_service_update` for an invocation
    that did not pass it. `_pilot_of(state)` reads it, and falls back to the
    pack for a checkpoint written before the key existed.
  - The pool's key is (role, pack, pilot), where pilot is only ever true for
    Synthesis. A pilot Synthesis is built with no tools
    (`tools=[] if pilot else [queue_tool]`).
  - Its system prompt gains `prompt_composer.PILOT_SYNTHESIS_SECTION`
    (`compose_system_prompt(role, pack, pilot=)`, `--pilot` on the CLI).
    Without it the generic instruction to call `queue_for_replay` before
    answering REPLAY would send the agent after a tool it does not have.
- **The fingerprint.** `compute_prompt_fingerprint(base_dir, pack, pilot=)`
  hashes the PILOT MODE section when pilot. `prompt_fingerprint(pack,
  pilot=None)` defaults to the pack's setting now and caches a pilot's under
  `<pack>+pilot`. An enabled pack's fingerprint is unchanged.
- **The casebook.** The top-level `pilot: true` appears only on a pilot's
  casebook. The fingerprint recorded with it is the pilot one.
- **Outcomes.** `record_outcome` records `service` (from
  `packet_metadata.service`) and `pilot`. `outcome_service(outcome)` reads a
  missing service as the pre-registry pack. `for_service(outcomes, service)`
  filters. `summarise` and `summarise_shadow` group by service first, and
  each row carries `service`.
- **`accuracy_report`** gained `--service`, and a SERVICE column in both
  tables. The `--shadow` readiness line now names the whole
  `<service>:<CODE>` allowlist entry, where Phase 5 left `<service>` for the
  operator.
- **Settings:** `REJECTION_SERVICES_PILOT`, isolated in `tests/conftest.py`,
  and documented in `.env.example`.
- **Documentation:** `ARCHITECTURE.md` (section 3.2.3's new "Pilot mode and
  accuracy per service", 3.3, 4, the tree and a section 5 entry),
  `SYSTEM_OVERVIEW.md` sections 8 and 11, and `src/service_packs/README.md`.
- **Tests:** `tests/test_service_pilot.py` (29) is new. It covers:
  - the setting, and the gate letting a pilot through;
  - prebuilding and the corpus check;
  - every validation error;
  - the prompt section, which only a pilot Synthesis gets;
  - the fingerprint;
  - a pilot Synthesis built without the tool;
  - the casebook flag, present and absent;
  - the per-packet decision;
  - outcomes recording the service, the legacy reading, grouping and
    filtering;
  - the report's `--service` and its allowlist line.
- **Full suite:** the same 56 failures as the baseline, no new ones.

### Phase 8 -- DLT lane (implemented 2026-09-28)

The contract, as the owner settled it on 2026-09-28: a DLT record's payload
and key are the rejection lane's Kafka payload and key; its headers carry the
exception and the stack trace. Section 10 lists what the lane needed.

#### As built (2026-09-28)

- **The contract** (`dlt/payload.py`).
  - `rejection_contract(payload)` validates a payload as `MessagePayload`.
  - `resolve_ref_id` reads such a payload's refId from
    `packetMetaData.refId`, with source `contract`, ahead of the key. The key
    is only checked: it is a mismatch when it equals none of
    `contract_identifiers` (eventId, refId, srn, sid).
  - `DltMessage.event_id` carries the eventId.
  - `summarise_rejection_contract` describes the payload by the fields the
    rejection Investigator is shown, and labels its identifiers.
  - Other payloads keep the four older layers.
- **Identity** (`dlt/identity.py`).
  - `derive_case_id(..., consumer_group)` appends `-g<digest>`.
  - `storage_key(ref_id, case_id)` = `<refId>__<digest of case_id>`, or the
    case id without a usable refId.
  - Both routes, `DltAdapter.identity_of` and the claim's finished check use
    the storage key. The claim is still taken under the delivery's refId.
  - `case_storage.keys_for_ref_id` finds a packet's cases, and
    `dlt_report --case` takes a key, a refId or a case id.
- **The registry.**
  - The `dlt` section is parsed into `ServicePack.consumer_groups`,
    `original_topics` (compiled) and `java_packages`.
  - Boot errors: a malformed topic or package; a group or package shared by
    two services; any `dlt` signal on `_default`.
  - `resolve_dlt` resolves: consumer group, stage, original topic, Java
    package (longest prefix, first claimed frame), source topic, reason code.
    Later disagreeing steps go in `conflict`, keyed by source.
  - `load_or_resolve` takes a `resolver`.
  - `dlt_gate_mode`, `dlt_enabled_services`, `dlt_skip_reason`, `dlt_gate`,
    `dlt_pack_for` and `fingerprint_service`.
  - Validation of the two settings. A warning is logged for an enabled
    service nothing can place.
  - The shipped enu-biometric pack names `com.uidai.enu.biometric`.
- **The routes** (`api/dlt_routes.py`).
  - `/fetch-dlt-logs` parses and classifies first, then resolves and gates,
    before the claim and before any evidence is written. It stores
    `service_resolution.json`.
  - Logs are fetched for `log_scope.service_to_search(resolution, pack)`,
    and the running version through `deployed.for_service`, which keeps
    enu-biometric's default read.
  - `/analyze-dlt` reads the stored resolution, gates again, and passes
    `service_resolution` and `service_pack` to the orchestrator.
  - `fingerprinted(failure, resolution)` recomputes the fingerprint with the
    service, for services other than enu-biometric only
    (`compute_fingerprint(service=)`, appended last).
  - Casebook schema 1.3.
- **The agents** (`dlt/orchestrator.py`).
  - One agent per (role, pack). The no-pack agents are prebuilt, in the
    order they always were.
  - The system prompt is `prompt_composer.compose_dlt_system_prompt`, with
    the pack's `dlt.md` when there is one.
  - The evidence block opens with `dlt_service_note`.
  - Harness tasks append `dlt_harness_service_context`, then the tools
    section, and run as the pack's opencode agent. The two DLT harness
    templates gained a sentence pointing at the context.
  - `LLM_CALLS` carries the resolved service.
- **Tools** (`mcp_client`).
  - A DLT role may take a service, and is then scoped by it; with none it
    keeps the role-only selection; `_default` is refused.
  - `opencode_agent(dlt_role, service)` is `crm_<role>__<slug>`.
    `opencode_config` builds one per registered service, plus the unscoped
    one. `_task_agent` does not fall back from a service's DLT agent to the
    wider unscoped one.
  - `deployed.running_version` and `discovery.list_pods_for_service` take a
    pod `match`.
- **Metrics:** `DLT_SERVICE_RESOLUTIONS` and `DLT_SKIPPED`.
- **Settings:** `DLT_SERVICE_GATE` and `DLT_SERVICES_ENABLED`, isolated in
  `tests/conftest.py` and documented in `.env.example`.
- **Documentation:**
  - `ARCHITECTURE.md`: section 3.2.3's new "The DLT lane per service", 4.4,
    4.4.2, the tree, and a section 5 entry;
  - `SYSTEM_OVERVIEW.md` section 12;
  - `src/service_packs/README.md`.
- **Tests:**
  - `tests/test_service_dlt.py` (48) is new. It covers:
    - the contract, the key check, the summary and the adapter;
    - the case id and the storage key;
    - the `dlt` section's parsing and boot errors;
    - resolution in every order, with conflicts;
    - the gate, the pack and the fingerprint rule;
    - the settings' validation;
    - the prompts, the service note and the harness context;
    - tool scoping, and the agents built per pack;
    - the routes: a skip leaves nothing; `record` mode analyses as before;
      an enabled service gets its own pack, logs and pods; enu-biometric
      keeps its fingerprint; a second record of one packet is analysed; the
      stored resolution is acted on.
  - Updated to the new contract: `test_dlt_abis_payload.py` (the case id),
    `test_dlt_flow_fixes.py` (per-record keys, and the `investigate` fakes),
    `test_dlt_analysis.py` (its `investigate` fake), `test_service_tools.py`
    and `test_mcp_client.py` (the DLT opencode agents and scoping).
- **Full suite:** 54 failures, all from the 56-failure baseline, none new.
  Two baseline failures in `test_dlt_analysis.py` now pass.

---

## 7. Onboarding checklist for one service

1. **Registry entry.** `src/service_packs/<service>/service.json`, with
   `match` rules taken from the Phase 0 sample and a unique `tool_prefix`.
2. **Pack.** `policy.md` (glossary, success criteria, what each enrolment type
   means), plus `investigator.md`, `reviewer.md` and `synthesis.md` as needed.
   - Authored with the service's experts.
   - Passes 5.3.
   - Every term the service uses differently from another service is defined.
3. **Documentation.**
   - `<service>.json` is in S3, and its stem equals the service name.
   - `check_reason_code_docs` is clean.
   - The reason codes covering most of the service's sampled volume are
     documented.
4. **Rule source.** `none`, unless the service has a rules database of its own.
5. **Corpus.** `docs_cache/<droa_corpus_dir>/` exists, if the harness is on.
6. **Logs.**
   - `logs.app_names` and `logs.k8s_match` are set; the namespace is in the
     environment.
   - The service's catalog is built.
   - The redaction audit passes on its sample logs.
7. **Tools** (optional).
   - A toolset under `agent_tools/<service_slug>/` with `services=(service,)`
     and prefixed names, or a team-owned MCP server that publishes the same
     metadata.
   - A read-only database account, and indexed predicates.
8. **Runbooks.** None at first. They are mined from casebooks later.
9. **Pilot.** Add the service to `REJECTION_SERVICES_PILOT`, collect verdicts,
   and check `accuracy_report --service`.
10. **Enable.** Move the service to `REJECTION_SERVICES_ENABLED`.
11. **Documentation of the change.** `ARCHITECTURE.md` lists the service.

---

## 8. Configuration

| Variable | Default | Phase | Meaning |
|---|---|---|---|
| `SERVICE_PACKS_DIR` | `src/service_packs` | 1 | Where the packs live |
| `REJECTION_SERVICE_GATE` | `record` | 1 | `record`: resolve and record only. `enforce`: skip packets of services that are not enabled. Must be `enforce` from Phase 2 |
| `REJECTION_SERVICES_ENABLED` | `enu-biometric` | 1 | Services analysed. Blank means the default, never "none", so a copied blank value cannot switch every service off under `enforce` |
| `REJECTION_UNRESOLVED_SERVICE` | `skip` | 1 | `skip`, or `default_pack` (with a confidence cap of 0.6) |
| `SERVICE_PACK_MAX_CHARS` | `20000` | 2 | Cap on a composed pack |
| `SYNTHESIS_UNRESOLVED_SERVICE_CONFIDENCE_CEILING` | `0.6` | 2 | Highest confidence for a packet analysed with the `_default` pack |
| `REASON_CODE_DOCS_S3_PREFIX` | `nalanda/reason-codes` (exists) | 3 | Now actually used |
| `REASON_CODE_DOCS_S3_DOWNLOAD` | `false` | 3 | Fetch the documentation store from that prefix at start-up. Needs `REASON_CODE_DOCS_DIR` set and a bucket, or the API refuses to boot |
| `REASON_CODE_DOCS_REFRESH_SECONDS` | `0` (start-up only) | 3 | Periodic refresh of the documentation |
| `AGENT_TOOLS_COMMON` | empty | 4 | Undeclared tools from other teams' servers to treat as common |
| `AGENT_DB_<KEY>_*` | -- | 4 | Per-database settings. `PROCESS_DB_*` remain the settings of key `process` |
| `RUNBOOK_SERVE_ALLOWLIST` | unchanged | 5 | Entries become `service:CODE`; a bare `CODE` means enu-biometric |
| `REDACT_JSON_KEYS` | the default list | 6 | JSON keys whose values are redacted |
| `REJECTION_SERVICES_PILOT` | empty | 7 | Services analysed in pilot mode |
| `DLT_SERVICE_GATE` | `record` | 8 | The DLT lane's gate: `record` or `enforce` |
| `DLT_SERVICES_ENABLED` | `enu-biometric` | 8 | Services whose dead-lettered records are analysed with their pack. Blank means the default |

Changed meaning: from Phase 6, `ES_APP_NAMES` and `K8S_APP_NAMES` are only a
fallback, for callers with no service.

---

## 9. Risks

| Risk | Mitigation |
|---|---|
| A wrong stage-to-service mapping sends a packet through the wrong pack | Phase 0 sampling; `record` mode before `enforce`; conflicts with the documentation are detected and counted; pilot per service |
| The prompt refactor lowers enu-biometric accuracy | Content-preservation and snapshot tests; the Phase 2 parity check against a fixed set |
| A tool name clash silently drops a tool | Enforced prefixes; local clashes stop start-up (D8) |
| Volume rises about tenfold, raising LLM cost and queue lag | Services enabled one by one; `MAX_CONCURRENT_INVESTIGATIONS` sized per phase; runbooks per service as volume builds |
| A learned rule from one service leaks into the others | `service` scope by default (D11) |
| Personal data in demographic logs reaches storage and the model | Phase 6 is a precondition for enabling another service; the redaction audit must pass |
| A service's documentation file is missing or invalid, leaving it with no rule source | Start-up validation blocks enabling it; a failed download keeps the last good copy |
| Packets are in flight during a deploy | `_service_of(state)` resolves when the key is missing; graph topology is unchanged |
| Every fingerprint changes at Phase 2 | Record the date; compare accuracy per period |
| Adding a service needs an opencode restart | Accepted: a pack ships with a deploy anyway |

---

## 10. Out of scope, and the DLT lane

- **DLT lane.** Built in Phase 8, against the contract the owner settled on
  2026-09-28. Each issue listed here was addressed there, as its "As built"
  section says. Known issues it had to address:
  - Terminal dedupe is keyed on the refId (`DltAdapter.identity_of` and the
    terminal check in `/fetch-dlt-logs`, which runs before the per-record
    claim). A second dead-lettered record for the same refId, from another
    service or another stage, is acknowledged and never analysed. **This is
    independent of the final contract**, and worth fixing on its own.
  - `deployed.running_version()` is called with no service, so it reads
    `K8S_DEFAULT_APP` or `enu-biometric`.
  - The failure fingerprint has no service in it. Under a shared contract,
    every service sends the same `__TypeId__`, so `DLT_FINGERPRINT_TYPE_ID`
    cannot separate services. The consumer group can.
  - `case_id` is topic-partition-offset, with no consumer group in it.
- Changing the Synthesis contract (D14).
- A different graph per service.
- Writing other services' packs.
- Retrieval or vector search over the documentation.

---

## 11. Questions for the owner

Answer these before or during Phase 0.

1. What are the `flowMetaData.stage`, `subStage` and `sourceTopic` values for
   each service? Phase 0.1 measures them; the owner confirms the mapping.
2. Does the running deployment already consume the shared topic? If it does,
   the Phase 1 gate should go straight to `enforce`.
3. Where does the S3 fetch of the reason-code documentation run today: in the
   application, or in an init container or job?
4. Are the S3 file stems the canonical service names, and do they match the
   `docs_cache/` directories?
5. Which databases are shared, and which belong to one service? Who owns each
   service's tools? Will any service team run its own MCP server?
6. Is `queue_for_replay` (OIS `forceReplay`) valid for every service? Are
   today's `ACTIONS` and `RESIDENT_ACTIONS` enough for all of them?
7. What accuracy bar moves a service from pilot to enabled? (For example, a
   share of CORRECT verdicts over at least N outcomes.)
8. Who authors each service's pack?
9. Do other services' payloads carry personal data in `packetMetaData`?
10. Do other services use enrolment types other than N, E, U and Z?

---

## Appendix A -- Where today's biometric-specific text goes

| Today | Moves to | Replaced in the generic file by |
|---|---|---|
| `agent_policy_context.md` (whole file) | `enu-biometric/policy.md`, verbatim | Nothing: the file is deleted and its `Dockerfile` line removed |
| `InvestigatorAgent.md`, "Enrolment Type -- READ THIS FIRST" (the N/U definitions) | `enu-biometric/investigator.md` | B.2 |
| `InvestigatorAgent.md`, "Aadhaar Biometric Processing Rules" | `enu-biometric/investigator.md` | Nothing |
| `InvestigatorAgent.md`, "Modality Terminology -- BINDING" | `enu-biometric/investigator.md` | B.2, "Service terminology" |
| `InvestigatorAgent.md`, the biometric examples in documentation rule 2 | `enu-biometric/investigator.md` | B.4 |
| `InvestigatorAgent.md`, CRITICAL INSTRUCTION 1 | -- | B.3 |
| `ReviewerAgent.md`, CRITICAL INSTRUCTION (the glossary example) | `enu-biometric/reviewer.md` | B.5 |
| `SynthesisAgent.md`, "Organization Terminology Glossary" and "Aadhaar Biometric Processing Rules" | `enu-biometric/synthesis.md` | Nothing |
| `SynthesisAgent.md`, "refer to the `agent_policy_context.md` document" | -- | B.6 |
| `harness/rules/rejection.md`, the role line and "Enrolment type rules" | `enu-biometric/investigator.md` (the harness receives the same file) | B.7 |
| `harness/RejectionReviewer.md`, STEP 3 checks 2 and 3 | `enu-biometric/reviewer.md` | B.8 |
| `harness/RejectionInvestigator.md`, "(N or E = new enrolment, U = biometric update)" | -- | B.9 |
| `RunbookGenerator.md`, "rejected biometric packets" | -- | "rejected packets"; `build_runbooks.py` names the service in its message |
| Root `AGENTS.md`, first paragraph | -- | B.1 |
| `agent_orchestrator.ENROLMENT_TYPE_DISPLAY` | `enu-biometric/service.json`, `enrolment_types.payload` | A pack lookup |
| `reason_code_docs._TYPE_DISPLAY` | `enu-biometric/service.json`, `enrolment_types.family_labels` | A pack lookup |
| `tool_registry._ENROLMENT_TYPE_ALIASES` | `enu-biometric/service.json`, `rule_source.enrolment_type_filter` and `enrolment_types.doc_aliases` | A pack lookup |
| `tool_registry.get_error_description` | Stays; called only on the `rules_db` path | -- |
| `main_api.py`, the app description | -- | "rejected packets" |

---

## Appendix B -- Service-neutral replacement text (drafts)

**B.1 Root `AGENTS.md`, first paragraph.**
You are an investigation agent for the Aadhaar enrolment and update pipeline,
which runs as many services. This file is loaded into every session, whatever
the task or the service, so it holds only what is true of every investigation.
Your task prompt names your flow, the service the packet belongs to, and that
service's own rules. Where they differ from this file, the task prompt wins.

**B.2 `InvestigatorAgent.md`, replacing "Enrolment Type", "Aadhaar Biometric
Processing Rules" and "Modality Terminology".**

```
### Enrolment Type -- READ THIS FIRST
The prompt includes an "Enrolment Type" field taken from
`packetMetaData.enrolmentType`. What each type means for this packet's service,
and which rules apply to it, is set out in the SERVICE CONTEXT section below.
You MUST state the enrolment type in your findings and apply that service's
rules for it. A rejection reason that is valid for one enrolment type may not
apply to another.

### Service terminology
The SERVICE POLICY section below defines the terms this service uses. Use them
exactly as defined there. Services use some of the same words differently;
never carry a meaning over from another service.
```

**B.3 `InvestigatorAgent.md`, CRITICAL INSTRUCTION 1.**
"1. You MUST use the SERVICE POLICY section (appended below) to interpret the
rule you are given -- the "Database Rule Configuration" when there is one,
otherwise the rules in the Reason Code Documentation."

**B.4 `InvestigatorAgent.md`, documentation rule 2.**
"2. The logs -- and the tool results, when you have tools -- supply the
packet-specific facts: what the service did with this packet, what it found,
what it decided, and when. Where the documentation names the evidence to look
for, look for exactly that. Quote the exact log lines you rely on."
The biometric examples move to `enu-biometric/investigator.md`: "For this
service those facts are, for example, which candidates matched, whether they
share this packet's parent, which modality matched, and the scores."

**B.5 `ReviewerAgent.md`, CRITICAL INSTRUCTION.**
"**CRITICAL INSTRUCTION**: You must validate their findings against the
SERVICE POLICY section appended at the bottom of this prompt, including its
terminology. If the investigator uses a term in a sense the SERVICE POLICY
rules out, or gives it another service's meaning, you must reject their
findings."
`enu-biometric/reviewer.md` keeps: "Pay special attention to the Organization
Terminology Glossary. If the investigator misinterprets "demo" or "nonDemo",
reject the findings."

**B.6 `SynthesisAgent.md`.**
"When generating the synthesis, you MUST use the SERVICE POLICY section
appended below to translate the Investigator's raw conditions into
human-readable resolutions for the operator."

**B.7 `harness/rules/rejection.md`.**
"You are the Rejection Investigator for the Aadhaar enrolment and update
pipeline. A packet was rejected by a business rule in the service named in
your task; your job is to explain why." And, under "Enrolment type rules":
"Its meaning for this service, and the rules that apply to each type, are in
the service context of your task. You MUST explicitly state the enrolment type
in your findings and apply that service's rules for it." The evidence list
says: "`context.json` -- the Kafka payload, the enrolment type, and the DB rule
configuration (absent for a service with no rules database)."

**B.8 `harness/RejectionReviewer.md`, STEP 3 checks 2 and 3.**
"2. Terminology violations: a term used contrary to the service context below.
3. Enrolment type misapplication: rules applied that the service context gives
for a different enrolment type."

**B.9 `harness/RejectionInvestigator.md`.**
- STEP 1: "The enrolment type (its meaning for this service is in the service
  context below)".
- STEP 2 opens with: "This packet belongs to {{service}} ({{service_display}}).
  Start from docs_cache/{{droa_dir}}/. Read other services' documentation only
  when the evidence shows the failure involved them."
- A new section before STEP 5:

  ```
  ## Service context
  {{service_context}}
  ```

**B.10 `rejection_context`, the new provenance notes for a service with no
rules database.**
- Documentation hit: "Provenance: this service has no rules database. The
  Reason Code Documentation above, including any rule-engine rules it lists,
  is the only account of the rule. Reason from it."
- Documentation miss: "Provenance: this service has no rules database, and no
  documentation describes this reason code. Say so plainly, reason from the
  reason code, the payload and the logs alone, and do not invent a rule."

---

## Appendix C -- The `_default` pack

`src/service_packs/_default/policy.md`:

```
No service-specific policy is configured for this packet's service. Reason
only from the Reason Code Documentation, the rule source, the payload and the
logs. Do not apply any other service's terminology or success criteria: a term
you know from another service may mean something different here. Where the
evidence does not establish the cause, say so and recommend escalation.
```

`src/service_packs/_default/service.json`:
- `rule_source.type` is `none`;
- `tools` is empty, so only the `"*"` tools apply;
- `match` is empty; the pack is never matched, only used by D5.
