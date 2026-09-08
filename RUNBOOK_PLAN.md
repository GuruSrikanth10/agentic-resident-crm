# Agentic Resident CRM: Standard Runbook Implementation Plan

Design date: 2026-08-11. Status: **planned, not implemented.**

Rejections repeat. The same `errorReasonCode` produces the same business-rule
analysis and the same remediation across thousands of packets, but today every
packet pays for a full Investigator -> Reviewer -> Synthesis LLM cycle (and a
rejected packet pays for the Investigator loop up to `MAX_INVESTIGATION_RETRIES`
times). This plan introduces a **runbook**: a human-approved, packet-agnostic
resolution template keyed by reason code, served in place of the agent pipeline
when one exists.

---

## 1. Goals

1. **Cost.** Skip the LLM cycle entirely on a runbook hit. With roughly 100
   reason codes and a volume distribution that is almost certainly
   front-loaded onto a handful of them, most packets should hit.
2. **Latency.** A multi-minute agent run becomes a file read.
3. **Consistency.** Two residents rejected for the identical reason receive the
   identical explanation, instead of two LLM paraphrases of the same finding.
4. **Reviewability.** A human-vetted template is strictly more trustworthy than
   a per-packet LLM inference, and it is reviewable once instead of never.

## 2. Non-goals

- **Not per-packet caching.** The existing idempotency guard in `routes.py`
  already prevents the *same* packet being reprocessed. Runbooks dedupe across
  *different* packets that share a rejection cause. The two are complementary
  and independent.
- **Not a replacement for the rules DB.** `lookup_rule_by_reason_code` remains
  the source of truth for business rules; a runbook caches the *analysis of* a
  rule, not the rule.
- **Not auto-approval.** Nothing an LLM generates is ever served without a
  human promoting it (Section 10).
- **Not a runtime writer.** The serving path only ever reads the runbook store.

---

## 3. Design overview

Three separable pieces, each mirroring a pattern already established in this
codebase:

| Piece | Runs | Mirrors |
|---|---|---|
| **Serving** -- return a finalized runbook instead of invoking the agents | In-graph, request path | `escalate_node`'s "produce a resolution without the LLM" shape |
| **Drafting** -- LLM generates generic runbooks from completed casebooks | Offline CLI | `src/tools/build_catalog.py` (offline Stage 0 builder) |
| **Promotion** -- human reviews drafts, moves to final, git-commits | Offline CLI | `src/tools/promote_rules.py` |

**Drafting is deliberately offline rather than inline.** Generating a template
from a single packet's casebook is the worst possible way to get generic text:
the model has exactly one example and will bake that packet's timestamps and
identifiers into the output. Mining several completed casebooks for the same
reason code and asking for the *common* resolution generalises far better,
directly serves the "no packet specifics" requirement, and keeps the request
path untouched. Operators here already run CLIs for `promote_rules`,
`approve_replays`, `check_drift`, `build_catalog`, and `prune_checkpoints`, so
this adds no new operational concept.

---

## 4. Storage layout

Data (git-committed):

```text
src/runbooks/
├── draft/          # LLM-generated, awaiting human review. Never served.
│   └── RESIDENT_MAN_DEDUP_REJECT_TD__U.json
└── final/          # human-approved. The only directory the graph reads.
    └── RESIDENT_MAN_DEDUP_REJECT_TD__U.json
```

Code: `src/utils/runbook_store.py`.

This split matches the existing convention where `src/db/` and `src/prompts/`
hold data and `src/utils/` holds the modules that operate on it. JSON is not
matched by any `.gitignore` rule, so runbooks are versioned and every promotion
shows up as a readable diff. At ~100 reason codes and at most a couple of
enrolment types each, the directory stays small enough to scan without an index.

---

## 5. Runbook schema

```json
{
  "schema_version": "1.0",
  "runbook_id": "RESIDENT_MAN_DEDUP_REJECT_TD__U",
  "reason_code": "RESIDENT_MAN_DEDUP_REJECT_TD",
  "enrolment_type": "U",
  "status": "final",
  "version": 1,
  "rule_fingerprint": "sha256:2f9c...",
  "resolution": {
    "rejection_description": "<generic>",
    "synthesis": "<generic>",
    "action": "REPLAY",
    "resident_action": "<generic>"
  },
  "provenance": {
    "source_event_ids": ["...", "..."],
    "source_casebook_count": 3,
    "generated_at": "2026-08-11T10:00:00",
    "generated_by_model": "mistral-large-latest",
    "max_retry_count_in_sources": 0
  },
  "approved_by": null,
  "approved_at": null
}
```

Field notes:

- `resolution` carries exactly the four keys `routes.py` already parses out of
  the graph's `synthesis` output, so serving requires no change to that parsing
  logic.
- `status` is `draft` or `final`. The graph reads only `final/`, and the
  `status` field is a redundant second check against a file being moved by hand.
- `max_retry_count_in_sources` is a confidence signal for the human reviewer: a
  runbook derived from investigations that needed three retries to pass QC
  deserves more scrutiny than one approved first time.
- `version` increments on each re-promotion, and appears in the casebook
  provenance string so a specific revision can be traced or recalled.

---

## 6. Key derivation

Key is `(reason_code, enrolmentType)`; filename `{reason_code}__{enrolment_type}.json`.

- `reason_code` comes from `packetExecutionSummary.errorData[].errorReasonCode`
  and is the first non-empty value, matching how `investigator_node` already
  selects it.
- `enrolmentType` comes from `packetMetaData.enrolmentType` and is stored
  **raw and uppercased** (`U`, `E`, `N`). Note the orchestrator separately maps
  `U -> UPDATE` and `E -> ENROLMENT` when *filtering rules*; that mapping is
  not used for the runbook key, because the raw value is what distinguishes the
  file and mapping it would collapse unmapped types.
- **Path safety.** `reason_code` arrives off a Kafka topic and is interpolated
  into a filename. It must be validated against `^[A-Za-z0-9_.:-]{1,128}$`
  before use, and the resolved path must be asserted to stay under the runbook
  root -- the same two-layer discipline applied to `eventId` in remediation
  items 0.11 and 1.17. A rejected key is logged and treated as a miss.
- **Fallback.** After an exact-match miss, try `{reason_code}__ANY.json`. This
  covers unmapped or missing enrolment types without forcing a file per variant.
  `build_runbooks` writes an `ANY` runbook only when explicitly asked
  (`--any-enrolment-type`); it never infers one.

---

## 7. Rule fingerprint and invalidation

"The rules will not change" is nearly true but not exactly: remediation item
1.1 replaced a process-lifetime `lru_cache` with a TTL cache specifically so
live rule edits take effect, and `src/tools/check_drift.py` exists to detect
policy drift. Rules change rarely, not never -- and a runbook that outlives its
rule is a silent correctness failure spread across every packet with that code.

- `rule_fingerprint` is `sha256` over the **post-`enrolmentType`-filter** rule
  JSON -- exactly the `db_rule` string the Investigator saw -- serialised
  canonically (`json.dumps(..., sort_keys=True, separators=(",", ":"))`) so
  key ordering can't produce spurious mismatches.
- On serve, the fingerprint is recomputed from the current rule and compared.
  A mismatch logs a warning and **falls through to the full pipeline**.
- The serving path does not rewrite, quarantine, or auto-refresh the stale file.
  Mutating a git-committed artifact from the request path would make production
  state diverge from the repo. Staleness is surfaced to operators via
  `promote_runbooks --list` and fixed by re-drafting.

---

## 8. Serving path

A new node sits between `fetch_logs` and `investigate`:

```text
START -> fetch_logs -> runbook_lookup --(hit)--> END
                            |
                         (miss)
                            v
                       investigate -> review -> synthesize/escalate -> END
```

- `runbook_lookup_node` performs the lookup and, on a hit, writes `synthesis`
  as a JSON string carrying the runbook's four resolution keys -- the same
  shape `escalate_node` already emits, so `routes.py` parses it through the
  existing path unchanged.
- A conditional edge reads state and routes to `END` or `investigate`. The node
  does the work and the edge only reads it, matching the existing
  `reviewer_node` / `check_approval` split.
- New `GraphState` fields: `runbook_id: str`, `resolution_source: str`.

**Preconditions -- all must hold for a hit:**

1. `RUNBOOK_MODE == "serve"`.
2. A reason code is present and passes the key pattern.
3. A **final** runbook exists for the exact key, or for the `ANY` fallback.
4. The runbook parses and carries all four resolution keys.
5. `rule_fingerprint` matches the current rule.

Any failure logs the reason and falls through. A malformed or unreadable
runbook file is never fatal -- it is a miss.

**Stuck packets need no special handling.** They do not carry a reason code, so
precondition 2 fails and they take the normal path by construction. This is why
the log-pipeline ERROR branch requires no runbook-specific guard.

**Logs are still fetched on a hit.** The node sits *after* `fetch_logs`, so
`rejection_logs`, `raw_logs.txt`, and `reduced_logs.txt` are all still produced
and attached. The LLM calls are the expensive component, not the ES query, so
this keeps essentially all of the saving while losing none of the audit trail.

---

## 9. Configuration

| Variable | Default | Meaning |
|---|---|---|
| `RUNBOOK_MODE` | `off` | `off` \| `shadow` \| `serve` |
| `RUNBOOK_CACHE_TTL_SECONDS` | `600` | In-process cache TTL for loaded runbooks |

- `off` -- lookup never runs. Identical to today's behaviour.
- `shadow` -- the full pipeline runs, its result is compared against the
  runbook, the divergence is logged, and **the pipeline result is returned**.
  This is how confidence is built on real traffic before enabling `serve`.
- `serve` -- a hit short-circuits the agents.

Shadow comparison is only meaningful on the crisp field: `action` is compared
exactly and any mismatch logged at warning level. `synthesis`,
`rejection_description`, and `resident_action` are prose, so both versions are
logged side by side for human reading rather than string-compared.

Runbooks are loaded lazily into a module-level dict behind a TTL, mirroring the
`TTLCache` pattern introduced for rule lookups in 1.1. With ~100 small files
this is trivial, and it avoids a filesystem read per packet.

Both variables must be added to `.env.example` with comments, per the
convention re-established in remediation item 1.4.

---

## 10. Drafting CLI -- `src/tools/build_runbooks.py`

1. Scan `local_casesheets/` for terminal casebooks.
2. Group by `(rejection_data.rejection_code, packet_metadata.packet_type)`.
3. **Exclude non-clean outcomes.** A casebook is an eligible source only if
   `packet_status.status` is `COMPLETED` or `REJECTED`, and its synthesis does
   not carry the `ESCALATED TO HUMAN REVIEW` marker written by `escalate_node`.
   An escalation is by definition the case where the agents could not agree; it
   is the last thing that should become a reusable template. If a reason code's
   genuine remediation really is manual review, a human authors that runbook by
   hand -- the CLI will not draft it.
4. For each group with at least `--min-samples` (default 3) eligible casebooks,
   send them to the LLM with a new `src/prompts/RunbookGenerator.md` prompt
   whose core instruction is: *produce the resolution that applies to every
   packet rejected for this reason code and enrolment type; include no
   packet-specific values.*
5. Run the generic-text validator (Section 11). A draft that fails is reported
   and not written.
6. Write to `src/runbooks/draft/` only. The tool has no ability to write to
   `final/`.

Flags: `--reason-code CODE`, `--min-samples N`, `--any-enrolment-type`,
`--overwrite-drafts`, `--dry-run`.

Uses the `simple` LLM tier, consistent with the tiering established in 2.2 --
this is a bounded summarisation task, not deep reasoning, and it runs at most
~100 times ever.

---

## 11. Generic-text validator

"No packet specifics" is a hard requirement, so it is enforced mechanically
rather than by trusting the prompt. Draft resolution text is rejected if it
contains:

- a UUID,
- an ISO-8601 timestamp or a date literal,
- a digit run of 10 or more (SRN, EID, UID, Aadhaar number),
- any literal `eventId`, `srn`, or `refId` value drawn from the source
  casebooks used to generate it.

The validator runs in **both** `build_runbooks` (before writing a draft) and
`promote_runbooks` (before writing to `final/`), so no packet-specific string
can reach the served set even if a draft is hand-edited.

---

## 12. Promotion CLI -- `src/tools/promote_runbooks.py`

Mirrors `src/tools/promote_rules.py`, including the correctness fixes applied
to it in remediation item 1.7 -- the new tool should be written correctly from
the start rather than repeating them:

- Top-level lock so two operators cannot promote concurrently.
- Refuses to run if `src/runbooks/` has uncommitted changes.
- For each draft, prints the reason code, enrolment type, the full proposed
  resolution, provenance (source count, whether any source needed retries), and
  a diff against the existing final runbook if one is present.
- On approval: re-runs the validator, sets `status: final`, stamps
  `approved_by` / `approved_at`, bumps `version`, writes to `final/`, removes
  the draft, and git-commits.
- **Only promoted entries are removed.** Skipped drafts, errored drafts, and
  drafts written by a concurrent `build_runbooks` run all survive, and the
  pending set is re-read fresh immediately before the rewrite.

Flags: `--reason-code CODE`, `--list`, `--dry-run`.

`--list` doubles as the staleness report: it flags any final runbook whose
`rule_fingerprint` no longer matches the current rule.

---

## 13. Casebook provenance

Add `resolution.source` to every casebook:

- `"agent"` -- produced by the full pipeline.
- `"runbook:<runbook_id>@v<version>"` -- served from a runbook.

This is non-negotiable for auditability. Without it there is no way to
distinguish a reasoned casebook from a templated one after the fact, and no way
to scope a recall if a runbook is later found to be wrong. It is an additive
change; bump the storage layer's enforced `schema_version` to `"1.1"` and
record the change in `ARCHITECTURE.md`.

---

## 14. Observability

All of the following are logged through the `structlog` logger with `event_id`
bound, per the standardisation in 2.8:

| Event | Level | Fields |
|---|---|---|
| Runbook hit | info | `runbook_id`, `version`, `mode` |
| Runbook miss | info | `reason_code`, `enrolment_type`, `miss_reason` |
| Fingerprint mismatch | warning | `runbook_id`, `expected`, `actual` |
| Malformed runbook | error | `path`, parse error |
| Shadow divergence | warning | `runbook_id`, `runbook_action`, `agent_action` |

`miss_reason` should be an enum-ish string (`no_reason_code`,
`invalid_key`, `not_found`, `stale_fingerprint`, `malformed`) so hit rate and
miss causes are aggregatable straight from the logs.

---

## 15. Phasing

Nothing changes production behaviour until Phase D, and Phase D ships dark.

| Phase | Contents | Behaviour change | Done when |
|---|---|---|---|
| **A** | `src/utils/runbook_store.py`: schema, load/save, key derivation + validation, path guard, fingerprinting, TTL cache. Directory scaffolding. Config flags + `.env.example`. | None -- inert | Store unit tests green; nothing imports it from the request path yet |
| **B** | `build_runbooks.py`, `RunbookGenerator.md`, generic-text validator | None -- offline | Drafts generate from existing casesheets and all pass the validator |
| **C** | `promote_runbooks.py` | None -- offline | A draft can be promoted, git-committed, and appears in `final/`; skipped drafts survive |
| **D** | `runbook_lookup_node`, `GraphState` fields, conditional edge, `resolution.source`, `RUNBOOK_MODE` gating, schema bump to 1.1 | Live, **defaults to `off`** | Full suite green with mode `off` and mode `serve`; agents provably not invoked on a hit |
| **E** *(optional)* | Shadow mode + divergence reporting | Observability only | Divergence rate measurable from logs on real traffic |

`ARCHITECTURE.md` is updated as part of Phase D (and again for E if built), per
the standing requirement in `.agents/AGENTS.md` that it be updated before any
commit that changes design or flow.

---

## 16. Tests -- `tests/test_runbooks.py`

Following the existing per-phase test-file convention:

**Store and keys**
- Key derivation for `U`, `E`, and the `ANY` fallback.
- A reason code containing `../` is rejected, and the resolved path is asserted
  to stay under the runbook root.
- A malformed JSON runbook yields a miss, not an exception.
- Fingerprint is stable across key reordering in the rule JSON.

**Serving**
- `RUNBOOK_MODE=off` -- agents always invoked even when a final runbook exists.
- `RUNBOOK_MODE=serve` + hit -- agents **not** invoked; logs still fetched;
  `rejection_logs` still populated; `resolution.source` stamped with id and
  version.
- A **draft** runbook is never served.
- Fingerprint mismatch -- falls through to the full pipeline and logs a warning.
- No reason code (stuck packet) -- takes the normal path.

**Drafting**
- Escalated, `DLQ`, `FAILED_TIMEOUT`, and `NEEDS_MANUAL_REVIEW` casebooks are
  excluded as sources.
- A group below `--min-samples` is skipped.
- Validator catches UUIDs, ISO timestamps, 10+ digit runs, and a literal source
  `eventId`.

**Promotion**
- Skipped drafts and drafts added mid-session survive the rewrite.
- Promotion bumps `version` and sets `status`, `approved_by`, `approved_at`.

---

## 17. Accepted tradeoffs

1. **`rejection_description` becomes generic on a hit.** Per the "no packet
   specifics" requirement, a served description no longer names the specific
   microservice and timestamp for that packet, which
   `src/prompts/InvestigatorAgent.md` instruction 3 currently asks for. The
   reduced logs are still attached to the casebook, so the evidence itself is
   not lost -- only its prose summary is generalised. That instruction remains
   in force on the miss path.
2. **Bootstrapping requires volume.** `build_runbooks` needs at least
   `--min-samples` eligible casebooks per key, so coverage will be thin until
   traffic accumulates. Self-correcting, and `--min-samples 1` is available for
   deliberate early seeding with correspondingly higher review scrutiny.
3. **A wrong approved runbook has a wide blast radius.** This is the reason for
   human promotion, the mechanical validator, shadow mode, `resolution.source`
   provenance, and per-code rollout. Rollback is a git revert of the file in
   `final/`, picked up within `RUNBOOK_CACHE_TTL_SECONDS`.

---

## 18. Future guards (not in scope for v1)

- **Log-shape check.** Given that logs follow a consistent pattern per reason
  code, a runbook could store an expected template signature and fall through
  to full analysis when a packet's reduced logs diverge from it -- catching the
  case where a familiar reason code is masking an unfamiliar failure.
- **Per-code enablement.** A `serve`-listed subset of reason codes, so
  well-understood codes can go live while messier ones stay on the agent path.
  Currently achievable by simply not promoting the messy ones.
- **Storage backend.** The store module should keep its file layout behind a
  small interface so a move to MySQL or the `CasebookStorage` abstraction later
  does not touch the graph.
