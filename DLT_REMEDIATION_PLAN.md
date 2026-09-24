# DLT lane -- remediation plan

**Status:** proposed, not started.
**Amends:** `DLT_PLAN.md` (phases 1-9 and C0-C8).
**Written:** 2026-09-23.

---

## 1. Purpose

`DLT_PLAN.md` describes what the dead-letter lane was built to do. This
document records what a deep read of the built lane found wrong with it, and
stages the work to fix it.

It is deliberately **not** a restatement of that plan's own open questions and
risks. Where a finding here overlaps one of them, it names it and says what
has changed since -- usually that machinery which did not exist when the
trade-off was accepted now does exist, which changes the cost of closing it.

The lane is in several respects better engineered than the rejection lane: the
create-only/blind-PUT group store is correct against S3-compatible stores that
refuse `If-Match`, the `provenance` block records what actually happened
rather than what was meant to, and four separate layers (claims, canned
treatment, group reuse, single-flight) exist to avoid paying for an LLM call.
The findings below are not a verdict on the design. They are the gap between
the design and the code.

## 2. How to read this

Every finding carries a verification status:

| Status | Meaning |
|---|---|
| **CONFIRMED** | Traced end to end in the source and verified at the cited line. |
| **REPRODUCED** | A background audit executed the real repository functions and produced the failing output; the mechanism has been independently re-read here, but the reproduction itself was not re-run. |
| **NEEDS CHECK** | Reported by the audit and plausible on reading, but not independently verified. Must be confirmed before any code is written against it. |

Nothing below is presented as fact on the strength of an agent report alone.
The four highest-impact findings (S1.1, S1.2, S2.1, S2.2) were each re-read
and confirmed directly at the cited lines.

---

## 3. Findings

### S1 -- A cached recommendation can be served for a different bug

This is the most serious class in the lane, because it is silent and it
scales: a fingerprint is the cache key, so a fingerprint that collides serves
one bug's narrative to another bug, for every later occurrence, with the
casebook asserting a provenance of `group_reuse` as though that were fine.

`DLT_PLAN.md` R2 ("fingerprint over-groups") anticipated exactly this and
mitigated it with a Trap-1 regression test on frame normalisation. All three
findings below sit *outside* what that test covers.

**S1.1 -- The wrapper fallback fingerprints on the wrapper's class name.
CONFIRMED.**
`src/dlt/classify.py:326-348`. When the root FQCN is unrecognised but its
message names a business exception and carries a code, both return paths set
`root_fqcn=fqcn` -- and `fqcn` is the *wrapper's* class, not the business
exception named in the message. The comment immediately above says the
wrapper's message "carries the real exception's FQCN and business code as
text", and the code then extracts only the code, never the FQCN.

Where it bites: a framework wrapper (`ListenerExecutionFailedException` and
friends) has no application-package frames, so `normalise_frames()` correctly
filters all of them out and the frame tuple is empty. The fingerprint
degenerates to `(generic wrapper FQCN, business code)` -- identical for every
call site in the estate that raises that code through that wrapper. The audit
reproduced two traces with different messages and different Java line numbers
producing a byte-identical fingerprint.

This is `stacktrace.py`'s own documented Trap 1 ("key on the wrapper exception
and every failure in every Spring Kafka consumer collapses into one bucket"),
occurring on a path that docstring does not guard.

An application-level wrapper is less severe: its own frame survives
normalisation, so some discrimination remains -- but `root_fqcn`, the
signature and the casebook still name the wrong exception.

**S1.2 -- `ParsedTrace.truncated` is computed and then never consulted.
CONFIRMED.**
`src/dlt/stacktrace.py:302` promises that a truncated trace "downstream phases
treat as Class U rather than fingerprinting a wrapper by mistake". Grepping
every reader of the flag:

```
src/dlt/classify.py:178      a comment, not a read
src/api/dlt_routes.py:199    stored on the failure record
src/api/dlt_routes.py:445    copied into the casebook for display
```

`classify()`, `compute_fingerprint()`, `reuse.decide()` and `groups` never
read it. **The documented safety contract is not implemented.** Where it
appears to hold, it holds by accident of the fallthrough, not by a check.

Consequence: a business exception whose frames were cut off by header
truncation fingerprints on `(BusinessException, code)` with an empty frame
tuple -- the same degenerate key for every failure site sharing that code.
Note this also fires *without* truncation whenever normalisation legitimately
empties the frame list, which `stacktrace.py:34-46` says is routine for a
`BusinessException` whose only app frame is `CommonErrorFactory`.

This is `DLT_PLAN.md` R5, whose mitigation was stated as "parser degrades to
Class U rather than fingerprinting a wrapper". That degradation was never
built.

**S1.3 -- `Suppressed:` blocks splice their frames into the root's.
REPRODUCED.**
`src/dlt/stacktrace.py:50`: `_FRAME_RE = re.compile(r"^\s*at\s+...")` matches
an `at ...` line at any indentation, and the string `Suppressed` does not
appear anywhere in the module. Java has printed indented `Suppressed:`
sub-traces since Java 7 (try-with-resources close failures,
`Throwable.addSuppressed`). The parser has no concept of that block boundary,
so those frames append to the current chain link.

Consequences: unrelated cleanup frames occupy slots in the 5-frame
fingerprint window and displace genuinely distinguishing ones; the same bug
fingerprints differently depending on whether a close-failure happened to fire
alongside it (R3, under-grouping); and the single scalar `elided` field is
overwritten by whichever `... N more` line is seen last, conflating the
suppressed block's count with the primary trace's.

**Why S1 matters more than its individual parts.** `reuse.decide()`
(`src/dlt/reuse.py:57`) has no independent way to tell that two occurrences of
one fingerprint are not one bug -- its entire correctness rests on the
fingerprint. `per_code.py` checks only that a cached finding carries no
packet-*identifying* text; it cannot detect that a finding describes a
different failure. Neither module can see any of S1.1-S1.3.

### S2 -- An operator report that cannot report

**S2.1 -- `--code-check-accuracy` always reports a 0% recurrence rate.
CONFIRMED.**
`src/tools/dlt_report.py:266-308`. `cases` is built by iterating
`storage.list_events()` and loading one casebook per id (`:267-276`). The DLT
case store is keyed by `ref_id`, one document per id. `by_ref` therefore maps
each `ref_id` to a list containing **exactly one** casebook -- itself. The
recurrence test is then:

```python
recurred = any(later.get("detected_at", 0) > detected
               for later in by_ref.get(ref_id, []))
```

which reduces to `casebook.detected_at > casebook.detected_at` -- always
`False`, for every case, always.

This report exists specifically to answer whether Trap T9's estimated 5%
false-positive rate is real, and therefore whether
`DLT_CODE_CHECK_GATES_REPLAY` is safe to enable. It will answer "0% recurrence,
no false positives" regardless of what production is doing. An operator
turning on an auto-replay gate on the strength of this output would be acting
on a number that is structurally incapable of being anything else.

The test suite does not catch it because `tests/test_dlt_report.py:371` seeds
via `save(casebook["case_id"], ...)` -- keying by `case_id` -- which creates a
data shape the real write path cannot produce, since production always
collapses to one document per `ref_id`.

**S2.2 -- `--group` prints a follow-up command that cannot work. CONFIRMED.**
`src/tools/dlt_report.py:148` prints `Inspect one with --case {members[-1]}`.
`members` holds **case ids** (`src/dlt/group_store.py:380`:
`[r.get("case_id") or r["_name"] for r in fresh]`). `cmd_case` at `:151` does
`storage.load(ref_id)` against the ref-id-keyed store. Following the tool's
own printed instruction raises `SystemExit: No casebook for '<case_id>'` for
every case that has a resolvable refId -- the documented common path.

`claims.aliases_of(case_id)` (`src/dlt/claims.py:162`) exists to map a case id
back to the refIds it was seen under. `dlt_report.py` never imports `claims`.
The fix is half-built and unwired.

**S2.3 -- A stale operator hint. CONFIRMED.**
`src/tools/dlt_sample.py:430` tells the operator to run
`dlt_report.py --corpus`. No such flag exists in that tool's argparse.

### S3 -- State and checkpointing

**S3.1 -- The DLT graph has no checkpointer. CONFIRMED.**
`src/dlt/orchestrator.py:396` compiles with a bare `graph.compile()`; `:421`
invokes with no `config`, so no `thread_id`. State lives only in the Python
frame. There is no `get_state`, no active-checkpoint guard, no resume. A pod
killed mid-analysis loses the work and re-runs the LLM on redelivery. Compare
`src/core/agent_orchestrator.py:955` and `src/api/routes.py:718`, which have
all three.

**S3.2 -- `DltGraphState.case_id` holds the `ref_id`. CONFIRMED.**
`investigate(ref_id, ...)` (`src/dlt/orchestrator.py:413`) passes
`{"case_id": ref_id, ...}` at `:422`, and four `logger.bind` sites (`:177`,
`:248`, `:331`, `:338`) then emit `case_id=<a ref_id>`. The lane is otherwise
scrupulous about this distinction -- `src/api/dlt_routes.py:260-264` explains
that claims key on `case_id` precisely because it is the record's idempotent
identity while `ref_id` is only the storage key. Correlating graph logs
against `dlt_claims/` by `case_id` silently matches nothing.

**S3.3 -- A checkpointer needs a thread-id namespace. CONFIRMED (design).**
The rejection lane uses a bare `event_id`. The `checkpoints` table is keyed
`(thread_id, checkpoint_ns, checkpoint_id)` and shared by every graph using
the store, so a DLT `ref_id` equal to a rejection `event_id` collides.
`checkpoint_ns` is LangGraph's own subgraph-nesting mechanism and should not be
repurposed; a prefixed `dlt:{ref_id}` is the safe form.

**S3.4 -- Keeping the logs out of the checkpoint. CONFIRMED (mechanism).**
`langgraph.channels.UntrackedValue.checkpoint()` returns `MISSING`, so such a
channel is never persisted -- but `from_checkpoint` returns an empty channel,
so the value is gone after a resume. A reload path is required either way, so
the simpler design is to keep `logs` out of the state entirely and have the
single consumer (`_evidence_block`, `src/dlt/orchestrator.py:124`) load
`fetched_logs.txt` on demand.

**S3.5 -- `DLT_MAX_EVIDENCE_CHARS` bounds the prompt, not the state.
CONFIRMED.** `src/dlt/orchestrator.py:124` truncates at 40,000 chars when
building the evidence block; `state["logs"]` holds the whole artifact. Only
matters once state is checkpointed, which is what S3.1 changes.

### S4 -- Nothing prunes any DLT root

**S4.1 -- The pruner cannot reach them. CONFIRMED.**
`src/tools/prune_casesheets.py:87` iterates the immediate children of its root
and `_casebook_status` (`:47`) requires a literal `casebook.json` directly
inside each child. `dlt_cases/`, `dlt_groups/`, `dlt_claims/`,
`dlt_parked_replays/` and `pending_replays/` are children whose real documents
live one level deeper, so each returns `None`, is counted as `skipped_active`,
and is never recursed into. The operator sees them reported as "active or
non-terminal" when they are in fact subtrees the tool cannot enter.

`dlt_cases` alone could be pruned by passing `--root .../dlt_cases`, since its
inner layout does match. The other three never can: their documents are
`meta.json` / `claim.json` / `parked_replay.json`, never `casebook.json`.

**S4.2 -- And could not, because storage has no delete. CONFIRMED.**
`CasebookStorage` (`src/storage/base.py`) defines `save`, `save_terminal`,
`load`, `exists`, `terminal_status`, `save_artifact`, `load_artifact`,
`artifact_exists`, `update_json`, `create_json`, `list_json`, `list_events` --
and no removal method of any kind. The only `delete_object` in the codebase
(`src/storage/s3.py:580`) is cleanup inside the conditional-write probe. A
pruner written against the abstraction could not have been written. This is
why both pruners are filesystem-only, and it is why making them
backend-aware is a larger change than it looks.

**S4.3 -- `prune_checkpoints.py` can never prune anything. CONFIRMED.**
It keeps thread ids whose `local_casesheets/casebook_<id>/casebook.json`
exists (`:46-50`), but `cleanup_casebook_dir()` `rmtree`s that directory
immediately after `save_terminal()` on every terminal path. The file it tests
for is always already gone, so the eligible set is always empty. It is also
`sqlite3`-only. Adding a DLT checkpointer (S3.1) makes this worse: a second
lane's threads begin accumulating in a table nothing prunes.

**S4.4 -- The reaper reaches DLT scratch but not DLT cases. CONFIRMED.**
`reap_stale_casebooks` (`src/utils/case_cleanup.py:77`) filters on
`name.startswith("casebook_")`. The DLT harness scratch dir is
`casebook_<ref_id>` at the top level and is reaped correctly; `dlt_cases/`
does not match the prefix and never is.

**S4.5 -- Parked replays are logically bounded, physically unbounded.
NEEDS CHECK.** `parked.py`'s docstring claims bounding on both axes, and the
*active* queue genuinely is capped (`park()` refuses past `cap()`, default
500) with a 30-day TTL. But `_mark()` (`:217`) only mutates `status` in place
via `update_json`; it never removes the document. One file per ever-parked
ref_id accumulates forever. The docstring's specific claim is about the
operational queue and is true; the physical growth is a separate gap.

### S5 -- There is no feedback loop

**S5.1 -- `STATE_FINAL` is a dead terminal state. CONFIRMED.**
Defined at `src/dlt/groups.py:53`, read by `has_usable_recommendation()` at
`:232`, written by nothing. `attach_recommendation` defaults to `STATE_DRAFT`
(`:190`) and its only production call site passes it explicitly
(`src/api/dlt_routes.py:761`).

**S5.2 -- No DLT equivalent of `outcome.json`. CONFIRMED.**
`src/api/dlt_routes.py` defines exactly two routes. The rejection lane has
`POST /outcome/{event_id}`, `src/utils/outcomes.py`, `record_outcome.py` and
`accuracy_report.py`. The DLT lane has `dlt_report.py`, which is read-only.

This is `DLT_PLAN.md` Open Question 5, mitigated there by "never writing
`final`, so every reuse is explicitly marked unreviewed". That mitigation
labels the problem rather than closing it. What has changed is that the
machinery is already half-built and wired -- only the operator surface and the
write are missing.

**S5.3 -- The existing guard test does not block a fix. CONFIRMED.**
`tests/test_dlt_analysis.py:352` drives `analyze_dlt` only. An
operator-initiated promotion does not touch that path, so the guarantee the
test actually protects -- *the agent can never promote its own
recommendation* -- survives unchanged.

### S6 -- Concurrency (all NEEDS CHECK)

Reported by the audit, plausible on reading, **not independently verified**.
Each must be confirmed before code is written against it.

**S6.1 -- `create_json` does not take the lock its siblings take.**
`src/storage/local.py`: `save`, `load`, `save_artifact`, `load_artifact` and
`update_json` all wrap their body in `FileLock`. `create_json` (`:194-224`)
does not; it relies on `O_CREAT|O_EXCL`, which makes a zero-byte file visible
before `json.dump` writes into it. A concurrent `load()` can then read an
empty file and hit its bare `except Exception: return None`. Applied to
`claims._claim`, a torn read makes `holder` `None`, which takes the blind
overwrite branch -- and both deliveries believe they won the claim. S3 is
unaffected (genuine conditional PUT). Local backend is the default.

**S6.2 -- Claim TTL does not account for queue backlog.** The claim is taken
at fetch time and never renewed when the analysis lane picks the message up.
The TTL is `max(1800, 2 × analysis budget)`, which models the analysis, not an
unbounded wait in `dlt-analysis-queue` -- the very backlog the analysis
consumer exists to absorb. Sustained backlog past 30 minutes makes a live
claim look abandoned.

**S6.3 -- Single-flight can erode the 30s safety margin.** The server budget
is deliberately `consumer_budget - 30` so the API always resolves before the
consumer's client timeout. A waiter that exhausts its wait still gets a 30s
floor (`src/api/dlt_routes.py:680`), so worst case is roughly wait + floor ≈
the full consumer budget -- reintroducing the race the margin exists to
prevent.

### S7 -- Efficiency and hygiene

**S7.1 -- `group_store._cache` is unbounded. CONFIRMED.**
`src/dlt/group_store.py:96` is a plain dict. `CACHE_TTL_SECONDS` governs
whether `_cache_get` treats an entry as *fresh*; nothing evicts a stale one.
Its sibling `_records` (`:145`) is a bounded `LRUCache(50000)` with a comment
explaining why bounding matters. Keyed by fingerprint, so it grows with the
number of distinct stack traces the estate ever produces.

**S7.2 -- Blocking `rmtree` on the event loop. CONFIRMED.**
`cleanup_casebook_dir()` is called synchronously inside `async def` handlers at
`src/api/dlt_routes.py:714` and `:831`, and `src/api/routes.py:793`, `:813`,
`:1008`. Both modules already have an `_off_loop` helper used for every other
blocking call in the same functions. (`routes.py:175` is in the synchronous
drain path and is correct.)

**S7.3 -- The frame list is unbounded everywhere except the hash.
NEEDS CHECK.** `compute_fingerprint` slices to `DLT_FINGERPRINT_FRAMES`
(default 5), but `normalise_frames()` returns everything and the full list is
passed to `corroborate()` and persisted in the casebook. `corroborate` scans
the whole log haystack once per frame. A `StackOverflowError` (a mapped
Class B exception) inside application packages can produce thousands of
frames, and corroboration runs unconditionally even when no LLM will.

**S7.4 -- `class_map()` rebuilds on every call. CONFIRMED.**
`src/dlt/classify.py:143` does `dict(DEFAULT_CLASS_MAP)` plus a fresh
`json.loads` of `DLT_CLASS_MAP` per invocation, and `classify()` runs twice per
case by design. `registry.load_catalog()` caches on `(path, mtime, size)` with
a comment explaining the trade-off; this sibling has no cache. Small, but free
to fix.

**S7.5 -- Two gaps in the metrics surface. CONFIRMED.**
Twelve `record_dlt_*` helpers exist, but nothing records the analysis-timeout
rate (`src/api/dlt_routes.py:704`) or parked-replay releases. The timeout path
is the one that silently converts a case into manual review.

**S7.6 -- A split write can mislabel its own failure. NEEDS CHECK.**
`group_store.record_occurrence` has no try/except around the `meta.json` write
that follows the occurrence write. If the occurrence persists and the meta
write throws, the caller records `record_dlt_group_write("occurrence", False)`
and `group_state = "occurrence_not_recorded"` even though the occurrence is on
disk and will be counted correctly on the next read. Harmless to data,
misleading in the metric.

### S8 -- The replay / parking chain

**S8.1 -- `parked.py` currently has no effective test coverage, and the
failures are a known regression rather than background noise. CONFIRMED.**

Commit `5de2c2f` ("...caseid to refid for dlt flow...", 2026-09-09) changed
the parking API:

```
park(case_id, ref_id, code_check, finding)        ->  park(ref_id, code_check, finding)
maybe_park(case_id, ref_id, code_check, finding)  ->  maybe_park(ref_id, message_ref_id, code_check, finding)
```

and dropped the `"case_id"` key from the stored entry. `tests/test_dlt_parked.py`
was never updated and still calls the old shape -- `parked.park(CASE, "REF-1",
verdict(), finding())` against a three-parameter function, and asserts
`entries[0]["case_id"]` against a dict that no longer has the key. Verified
directly: `park` is `def park(ref_id, code_check, finding=None)` at
`src/dlt/parked.py:118`, and the test calls it with four positional arguments.

**14 of 22 tests in `tests/test_dlt_parked.py` fail**, and they are every TTL
expiry test, every cap test, every release/idempotency test, and the
numeric-vs-lexical release test. A further **12 of 17 in
`tests/test_dlt_analysis_replay.py`** fail, mostly because the suite loads the
persisted casebook by `case_id` while `/analyze-dlt` persists under `ref_id`.

So the entire behavioural surface `parked.py`'s docstring claims to guarantee
-- bounded queue, expiry, release-once -- is currently unverified by anything.
Production source is fully migrated (`grep case_id` across `parked.py`,
`release_parked_replays.py`, `auto_replay.py` returns nothing); the drift is
confined to tests. That is the good news and the bad news: the code is
probably right, and nothing would tell us if it stopped being.

**These failures account for roughly 26 of the ~56 failures in the current
suite baseline.** Anyone treating that baseline as inert background noise --
as earlier work in this repo reasonably did, since the failures predate it --
should know that a quarter of it is one identifiable, fixable regression in
the lane this document covers.

**S8.2 -- `maybe_park` refuses exactly the packets `maybe_replay` accepts.
CONFIRMED.**
`src/dlt/parked.py:178`:

```python
if not message_ref_id:
    return {"parked": False, "reason": "no refId; nothing to replay later"}
```

versus `src/dlt/auto_replay.py:299`:

```python
decision = decide(finding, message_ref_id or ref_id, code_check)
```

`ref_id` at the call site is already `message.ref_id or case_id`
(`src/api/dlt_routes.py:536`), so `maybe_replay` falls back to the case id and
proceeds when the payload carries no refId. `maybe_park` hard-refuses on the
same input.

`EnrolmentEventResponse.ref_id` can legitimately be `None`
(`src/models/dlt_payload_schemas.py:140-150`, documented as "never falls back
to `event_id`"), so this is reachable. The consequence is the precise failure
the module exists to prevent: a packet whose replay is withheld *only* because
the fix has not deployed is refused parking on an unrelated check, is never
replayed, and never comes back when the fix ships. `parked.py:2-9` --
"withholding without coming back to it would simply lose the packet, so this
is where it waits."

The same commit added the `message_ref_id or ref_id` fallback to the replay
path and left the parking gate as a straight rename of the pre-fallback check.

**S8.3 -- `release_ready()` is not idempotent, contrary to its docstring.
NEEDS CHECK.** `src/tools/release_parked_replays.py:11` claims "it is
idempotent, and an entry it releases is marked so it is not released twice".
`src/dlt/parked.py:254-287` decides to call `auto_replay.attempt()` -- which
can POST to the live OIS replay endpoint -- from a read that may be stale, and
marks the entry `STATUS_RELEASED` only *after* the call, with no
compare-and-swap. `CasebookStorage.create_json` is exactly the claim-once
primitive for this, and `claims.claim_case` already uses it; `release_ready`
does not. No scheduler invokes this tool anywhere in the repo, so today it is
operator-run and the race is theoretical -- but the docstring's guarantee is
not backed by the code.

**S8.4 -- A redelivery can reset a resolved replay record to pending.
NEEDS CHECK.** `src/tools/tool_registry.py:569` writes the pending-replay
record with a plain `save()`, not `create_json`. `approve_replays._resolve()`
deliberately marks a record `replayed`/`discarded` in place ("an audit record,
not scratch" -- intentional). A second `queue_for_replay` for the same id
silently resets that audit record to `pending`, and the operator is asked
again. Trigger: a crash between the replay-queue call
(`src/api/dlt_routes.py:783`) and the terminal casebook write (`:828`) leaves
the case non-terminal, and a same-`ref_id` redelivery is treated as a
legitimate retry rather than blocked.

**S8.5 -- An unreadable commit timestamp disables a filter silently.
NEEDS CHECK.** `src/dlt/code_check.py:299` passes
`since_ms=commit.timestamp_ms`, which `bitbucket.py:467-473` deliberately
allows to be `None`. `commits_touching` returns the *entire* unfiltered branch
history when `since_ms is None`, and the caller then takes `bumps[-1]` on the
comment "the oldest entry is the first bump after the commit" -- true only if
the list was filtered. The audit traced the consequence forward and found it
cannot produce a false `FIX_DEPLOYED`: the understated version fails the
Trap-T9 baseline-ahead check and downgrades to `UNKNOWN`. So the defect is a
wrong `UNKNOWN`, which is safe but still wrong. No test exercises a `None`
timestamp reaching this path.

**S8.6 -- `bitbucket._cache` is a second unbounded process-global cache.
NEEDS CHECK.** `src/dlt/bitbucket.py:84` is a plain dict; `_cached()` skips
expired entries on read but never removes them, and there is no maxsize. Its
sibling `deployed.py:227` uses a bounded `TTLCache(maxsize=32, ttl=...)`.
Same shape as S7.1, different module -- which suggests the pattern, not the
module, is what needs a convention.

**S8.7 -- Minor. NEEDS CHECK.** `_probe_path`'s de-dup cache
(`src/dlt/code_check.py:252-273`) is keyed on the path string alone, not
`(repo, path)`; the common case is masked by `evaluate()`'s multi-repo guard.
A negative `DLT_PARKED_REPLAY_TTL_SECONDS` clamps to `0`, which *disables*
expiry rather than expiring immediately (the `0` case is deliberate and
tested; the negative case silently joins it). `park()`'s cap check is a
read-then-write race that can overshoot the soft cap slightly.

**Checked and found correct**, per the audit and not re-reported as defects:
the three independent code-check flags and the two independent replay flags
are deliberately separate and documented as such; every replay/park/code-check
call site in `dlt_routes.py` is wrapped in `_off_loop`; the numeric-vs-lexical
version comparisons in `versions.py` are correct and match their Trap T5-T9
comments. `auto_replay.py`, `code_check.py`, `deployed.py`, `versions.py`,
`bitbucket.py` and `code_check_probe.py` all pass their own unit suites at
100%.

---

## 4. Phases

Ordered so each is independently shippable. Suggested sequence:
**0 → 1 → 2a → 3 → 4 → 5 → 6 → 7.**

### Phase 0 -- Restore the parking lane's test coverage (S8.1)

**Do this before anything else, and before touching `parked.py` for S8.2.**
Changing a module whose entire test suite is failing is changing it blind.

1. Update `tests/test_dlt_parked.py` to the current API: `park(ref_id,
   code_check, finding)`, `maybe_park(ref_id, message_ref_id, code_check,
   finding)`, and assertions against the entry shape the code actually stores.
   The tests' *intent* is sound and worth preserving verbatim -- they cover
   expiry, the cap, release-once and numeric-vs-lexical ordering, which is
   exactly the surface S8.3 and S8.7 touch.
2. Fix `tests/test_dlt_analysis_replay.py` to load casebooks by the key the
   route actually writes (`ref_id`), and to seed `seed_baseline()` under the
   same key. This is the same class of fixture bug as S2.1's: a test that
   describes a state the system cannot reach.
3. Investigate the one failure the audit attributed upstream: a
   `SocketTimeoutException` log line against the reference trace classifies
   `UNVERIFIABLE` where the test expects `CONTRADICTED`. That is in
   `corroborate.py`, and it may be a genuine S1-class finding rather than a
   test bug -- resolve which before changing either side.

Exit criteria: `tests/test_dlt_parked.py` and
`tests/test_dlt_analysis_replay.py` green, and the suite baseline drops by
roughly 26 failures.

### Phase 1 -- Stop the cache serving the wrong bug (S1)

The highest-severity class, and the one whose cost grows with every day the
lane runs, because wrong recommendations accumulate in the group store.

1. **S1.1** -- extract the real FQCN from the wrapper's message. The message
   already contains it; `extract_business_code` proves the text is parseable.
   Set `root_fqcn` to the nested exception, not the wrapper. Where it cannot
   be extracted, classify Class U rather than fingerprinting the wrapper.
2. **S1.2** -- implement the documented contract: a `truncated` trace, or one
   whose normalised frame tuple is empty, does not produce a reusable
   fingerprint. Either classify Class U or mark the group non-cacheable. The
   empty-frame case is the one that matters and it fires without truncation.
3. **S1.3** -- teach `_parse_link` the `Suppressed:` boundary: stop consuming
   frames into the current link at a `Suppressed:` marker, and keep the
   primary trace's `elided` count.
4. Pin the reference fixture's fingerprint as a literal and assert it is
   byte-identical, exactly as Trap T10 already requires for C2. Any change to
   fingerprint inputs must be shown not to move existing groups.

**Migration consideration.** Changing fingerprint derivation re-keys the group
store: existing groups keep their old fingerprints and new occurrences land on
new ones. That is acceptable -- the old groups are precisely the ones that may
be poisoned -- but it must be a deliberate, announced cutover, not a silent
one. Record the change in the group `meta.json` schema version.

### Phase 2 -- Retention

**2a -- S3 lifecycle rules. No code.** Expiry by key suffix (`raw_logs*`,
`trace.txt`, `parsed_trace.json`) with `casebook.json`, `status.json` and
`outcome.json` exempt, mirroring the `PRESERVED_ON_PRUNE` reasoning at
`src/tools/prune_casesheets.py:44`. Highest value per unit of effort in this
document, and it needs no delete API because the bucket does the deleting.
Ship it first.

**2b -- A delete capability, then backend-aware pruners.** Fixes S4.1-S4.4.

1. Add `delete(event_id, filename)` and `delete_case(event_id, *, preserve=())`
   to the protocol and both backends. `preserve` carries the
   `PRESERVED_ON_PRUNE` semantics into the abstraction so no caller can forget
   them. Refuse to delete a case whose recorded status is not terminal -- in
   the storage layer, not only in the caller. This is the one method that can
   destroy the system's output; the guards belong where they cannot be
   bypassed.
2. `prune_casesheets` enumerates via `list_events()` and takes a `--root`
   naming the scoped store, so the DLT roots become reachable.
3. `prune_checkpoints` tests terminality via
   `storage.exists(..., terminal_only=True)` and branches on
   `checkpointer.backend_name()`.
4. Physical removal for released/expired parked entries (S4.5).

If 2b is judged too large for now, 2a alone removes most of the growth -- but
S4.3 stands, and the checkpoint table keeps growing whether or not Phase 3
ships.

### Phase 3 -- Checkpoint the lane, without the logs (S3)

1. Rename the state field so it says what it holds (S3.2). Do this first: it
   is a prerequisite for a `thread_id` that means what it says and it has no
   design surface.
2. Remove `logs` from `DltGraphState`; add `ref_id`. `_evidence_block` loads
   `fetched_logs.txt` via `get_dlt_storage().load_artifact` and caches it in a
   local for the node's duration (S3.4, S3.5).
3. Compile with `get_checkpointer()`; invoke with
   `thread_id=f"dlt:{ref_id}"` (S3.3).
4. Reset `retry_count` on a fresh invoke, as `src/api/routes.py:764` does and
   for the reason documented there.
5. Add the resume guard in `/analyze-dlt`, mirroring `routes.py:718-740`.

The trade-off to write down: the lane becomes resumable at the cost of
checkpoint rows it did not previously pay for. Keeping `logs` out is what makes
that cost acceptable -- it is the field that dominates the rejection lane's
checkpoint size.

### Phase 4 -- Make the operator reports true (S2)

1. **S2.1** -- recurrence cannot be derived from a store that holds one
   document per ref id. Either record replay outcomes in their own append-only
   store keyed by `(ref_id, attempt)`, or derive recurrence from the group's
   `occurrences/` subtree, which *does* retain one object per case. Until one
   of those exists, the subcommand should refuse to print a number rather than
   print zero.
2. **S2.2** -- have `--case` accept either identity, resolving a case id
   through `claims.aliases_of()`; or print the ref id in `--group`'s member
   list. The former is friendlier and the helper already exists.
3. **S2.3** -- fix the stale `--corpus` hint.
4. Fix the test fixture that masks S2.1: seed through the same key derivation
   production uses, so a test cannot describe a state the system cannot reach.

### Phase 5 -- Efficiency and hygiene (S7)

Bound `group_store._cache` to match its sibling; route `cleanup_casebook_dir`
through `_off_loop` at all five async sites; cache `class_map()`; cap the
persisted/scanned frame list; add the two metrics; tighten the split-write
metric.

### Phase 6 -- Concurrency and the replay chain (S6, S8.2-S8.7)

**Confirm each NEEDS CHECK finding first.**

The one item here that is confirmed and should not wait: **S8.2**, the
`maybe_park` / `maybe_replay` asymmetry. It loses packets, it is a two-line
fix (`message_ref_id or ref_id`, matching the replay path), and Phase 0 gives
it the coverage to be changed safely.

Then: take the lock in `create_json`, or write-then-rename to match `save`'s
atomicity (S6.1); renew the claim when the analysis lane picks a message up,
or key the TTL off queue-entry time (S6.2); make the single-flight floor come
out of the budget rather than on top of it (S6.3); give `release_ready` a
claim-once via `create_json` before it calls `attempt()` (S8.3); make the
pending-replay write `create_json` so it cannot resurrect a resolved audit
record (S8.4); treat a `None` commit timestamp as "cannot filter" rather than
"no filter" (S8.5).

Bound `bitbucket._cache` alongside `group_store._cache` in Phase 5 -- two
unbounded process-global caches with bounded siblings in the same package is a
convention problem, not two separate bugs.

### Phase 7 -- The feedback loop (S5)

1. A DLT outcome record mirroring `src/utils/outcomes.py`: written under the
   case's key, denormalised so a report can group without re-reading
   casebooks, exempt from pruning.
2. `POST /dlt-outcome/{ref_id}` mirroring `routes.py:557`, with the same
   verdict constraint and id-pattern guard.
3. Promotion through the `groups` facade (`src/dlt/groups.py:189`), not
   `group_store` directly, so the legacy layout is handled.
4. An operator CLI beside `record_outcome.py`, and a `--pending-review`
   listing in `dlt_report.py`.

**The decision this phase turns on.** `has_usable_recommendation()` treats
`STATE_DRAFT` and `STATE_FINAL` identically, so promotion alone changes
nothing -- it is a label. A feedback loop worth building must act on a
*negative* verdict:

- **Recommended:** add `STATE_REJECTED`, excluded by
  `has_usable_recommendation`, written when an operator records INCORRECT. The
  next occurrence re-investigates instead of re-serving a recommendation a
  human has already rejected. That is the actual value -- correction, not
  labelling.
- Keep drafts reusable, so enabling the loop cannot suddenly multiply LLM
  calls for every unreviewed fingerprint. `STATE_FINAL` stays a review marker
  and may exempt the finding from the unverified confidence decay.

---

## 5. Verification

- `.venv/bin/python -m pytest -q`. The suite carries a baseline of roughly 56
  failures. **Roughly 26 of those are S8.1** -- the `5de2c2f` signature drift
  in `test_dlt_parked.py` and `test_dlt_analysis_replay.py` -- and Phase 0
  removes them. The rest sit in `test_resilience`, `test_audit_phase1`,
  `test_phase1_fixes`, `test_phase2_fixes`, `test_dlt_lane_parity` and
  `test_dlt_report`, and are out of scope here.
  The bar for every phase is **no new failures**, established by diffing the
  failing test-id set against a clean-HEAD worktree run -- not by the raw
  count. Do not read the count alone as "known noise": a quarter of it was one
  fixable regression that had been sitting in the baseline unexamined.
- `.venv/bin/python -m ruff check src tests`.
- Phase 1: the reference fingerprint pinned as a literal; a wrapper trace and
  a truncated trace each asserted *not* to collide with a different failure;
  a `Suppressed:` fixture asserted to yield the primary trace's frames only.
  Trap T10's existing discipline applies unchanged.
- Phase 3: a resumed analysis asserted to reload its evidence rather than
  resume empty; a DLT thread id asserted not to collide with a rejection one.
- Phase 4: a test seeded through the production key derivation, asserting a
  real recurrence is counted.
- Phase 7: the analysis path asserted still unable to write `STATE_FINAL`; an
  operator promotion asserted to.
- Per `AGENTS.md`, `ARCHITECTURE.md` must be updated before any commit --
  section 3.8's storage inventory and the DLT sections in particular.
- Note: a full `pytest` run deletes tracked `src/prompts/pending_rules.jsonl`
  as a side effect. Restore it with `git checkout --` before committing.

## 6. What this plan does not cover

- `DLT_PLAN.md`'s genuinely open external questions -- branch naming across
  repos (14.5 Q1), whether the 5% version-bump error is uniform (14.5 Q4) --
  which need answers from outside the codebase.
- Anything in the rejection lane except where it shares code with this one
  (the storage protocol in Phase 2b, `cleanup_casebook_dir` in Phase 5).
