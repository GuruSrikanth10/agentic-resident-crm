# Service packs

One directory per service the rejection lane knows. A pack says how to
recognise the service's packets, and carries everything about the service
that the agents need and no other service shares: its policy, its glossary,
what its enrolment types mean -- and, as `MULTI_SERVICE_PLAN.md` proceeds,
its rule source and its tools.

```
src/service_packs/
  README.md                this file
  _default/                used for unresolved packets when
    service.json             REJECTION_UNRESOLVED_SERVICE=default_pack
    policy.md
  <service>/
    service.json           required
    policy.md              required: the SERVICE POLICY section of the
                             Investigator's, Reviewer's and Synthesis's prompts
    investigator.md        optional: the SERVICE CONTEXT section of that role's
    reviewer.md              prompt; a role with no file is told there are no
    synthesis.md             service-specific instructions for it
    learned_rules.md       optional: promoted learned rules, appended to the
                             Investigator's prompt; written by promote_rules.py
                             for a rule of `service` scope (a `generic` rule
                             goes to src/prompts/learned_rules.md instead)
    dlt.md                 optional: the SERVICE CONTEXT of the DLT lane's
                             three agents for this service's dead-lettered
                             records
```

A prompt is the role's generic prompt from `src/prompts/` followed by these
sections of **one** pack -- never two, because services' terms contradict
each other. `python3 -m src.core.prompt_composer <role> --pack <service>`
prints what a role is built with.

`src/utils/service_registry.py` reads the packs -- `service.json` and the text
files together. It loads them once per process, so a change takes effect on
the next deploy or restart. The API validates them at boot
(`main_api.validate_service_registry`) and refuses to start on an error: at
run time a pack with errors would be silently left out, and under
`REJECTION_SERVICE_GATE=enforce` its packets would then be skipped.

## The service name

The directory name, the `service` field, the stem of the service's reason-code
documentation file (`<service>.json` in the S3 store) and, by default, its
directory in `docs_cache/` are one and the same name. Lower case, digits and
hyphens, starting with a letter: `enu-biometric`.

## `service.json`

```json
{
  "schema_version": 1,
  "service": "enu-biometric",
  "display_name": "ENU Biometric (BIO stage, BIO_DEDUP)",
  "tool_prefix": "bio",
  "match": {"stages": ["Biometric"], "sub_stages": [], "source_topics": []},
  "enrolment_types": {
    "payload": {"U": {"family": "U", "label": "Biometric Update (...)"}},
    "family_labels": {"U": "Biometric Update (U)"},
    "doc_aliases": {"UPDATE": "U"}
  },
  "rule_source": {"type": "rules_db", "enrolment_type_filter": {"U": "UPDATE"}},
  "reason_code_docs_file": "enu-biometric",
  "droa_corpus_dir": "enu-biometric",
  "logs": {"app_names": ["enu-biometric"],
           "k8s_match": {"name_contains": "enu-biometric"},
           "also_search": [], "decision_vocabulary": "MAN_DEDUP"},
  "tools": {"include": [], "exclude": []},
  "dlt": {"consumer_groups": [], "original_topics": [], "java_packages": []}
}
```

Required: `schema_version` (1), `service`, `display_name`, `match`,
`rule_source`, and `tool_prefix` for every pack but `_default`. Unknown keys
are errors, at every level: a misspelled key would otherwise configure
nothing.

| Field | Meaning |
|---|---|
| `tool_prefix` | Unique across the registry, `^[a-z][a-z0-9]{1,11}$`. Every tool scoped to this service alone is named `<prefix>_...`, and no other tool may start with it (Phase 4) |
| `match.stages` | Values of `flowMetaData.stage`, compared case-insensitively |
| `match.sub_stages` | Values of `flowMetaData.subStage`. Empty means any. Non-empty narrows `stages`, so it needs at least one stage |
| `match.source_topics` | Regular expressions, each matched against the whole `sourceTopic`. Consulted only when the stage decides nothing |
| `enrolment_types.payload` | Raw `packetMetaData.enrolmentType` value -> its family and the label the prompts show. A type the pack does not describe is shown as it arrived |
| `enrolment_types.family_labels` | Family -> the label in the reason-code documentation's title line (Phase 3) |
| `enrolment_types.doc_aliases` | Type names the documentation's rule conditions use -> family (Phase 3) |
| `rule_source.type` | `rules_db` when the service's rules are in the rules database, otherwise `none`: the reason-code documentation is then the rule source (Phase 3). `enrolment_type_filter` belongs to `rules_db` only |
| `reason_code_docs_file` | Stem of the reason-code documentation file. Defaults to `service`; no two services may share one |
| `droa_corpus_dir` | The service's directory in `docs_cache/`. Defaults to `service` |
| `logs.app_names`, `logs.k8s_match` | The service's Elasticsearch `application_name` values, which are also its Kubernetes app names, and how its pods are found (`name_contains` or `label_selector`, not both). A packet of the service searches these apps and nothing else of its own. `app_names` defaults to the service name. A `K8S_SERVICE_MAP` entry for an app overrides `k8s_match`, and namespaces stay in the environment |
| `logs.also_search` | Other registered services whose apps are searched too for this service's packets. Their own `also_search` is not followed |
| `logs.decision_vocabulary` | A regular expression, matched case-insensitively, OR-ed with the generic decision vocabulary for this service's packets and its catalog |
| `tools.include`, `tools.exclude` | Widen or narrow the service's tool scope by tool name (Phase 4). Never widen the roles a tool is for; `_default` takes no `include` |
| `dlt.consumer_groups` | Kafka consumer groups whose dead-lettered records are this service's, compared exactly. Unique across services |
| `dlt.original_topics` | Anchored regexes over a dead-lettered record's original topic |
| `dlt.java_packages` | Java package prefixes of this service's code. The failure site's first frame a pack claims places the record, longest prefix first. Unique across services |

Environment-specific values -- namespaces, hosts, credentials -- never go in a
pack. They differ between staging and production; the pack does not.

## How a packet is placed

In order, the first step that names exactly one service decides:

1. the packet's reason code, when the reason-code service map lists it under
   exactly one service (below);
2. `flowMetaData.stage` (and `subStage`, for a service that lists sub-stages);
3. `sourceTopic`, against `match.source_topics`;
4. the packet's reason code, when exactly one documentation file documents it;
5. otherwise the packet is `_unresolved`.

When a later step names a different service than the one chosen, the chosen
one still wins, and the resolution records a `conflict` keyed by that step's
source (`flow_stage`, `source_topic`, `reason_code_docs`).

### The reason-code service map

A rejection's `edata.stage` is `REJECTINTERCEPTOR` -- the service that
publishes every rejection -- so it does not say which service rejected the
packet. The map says so, and places the packet before anything else:

```json
{
  "schema_version": 1,
  "description": "optional free text",
  "services": {
    "enu-biometric": ["RESIDENT_MAN_DEDUP_REJECT_ANOMALOUS", "..."],
    "enu-qc": ["RESIDENT_QC_POA_DOCUMENT_NOT_APPROVED", "..."]
  }
}
```

- Keys are service names (the pack names); each lists the reason codes that
  service raises, exactly as `errorReasonCode` carries them.
- A service need not have a pack. Its packets resolve to it and the gate
  reports them `service_not_registered`, so list every service whose codes you
  know, not only the analysed ones.
- A code listed under two services decides nothing; the next step places it.
- Any error rejects the whole file: unknown keys, a bad service name or code,
  or a service key written twice. A code under two services, or twice under
  one, is a warning.

It is `reason_code_services.json` in `REASON_CODE_DOCS_DIR`, or
`REASON_CODE_SERVICE_MAP_FILE`. With `REASON_CODE_SERVICE_MAP_S3_KEY` set it
is fetched from S3 at start-up (and every
`REASON_CODE_SERVICE_MAP_REFRESH_SECONDS`), validated before it replaces the
copy on disk, and `/ready` fails until the first copy is there. It is
re-read when it changes; no restart is needed. Without a file, packets are
placed from step 2 on, as before the map existed. The API validates it at
boot with the registry. The DLT lane does not read it (below).

To analyse only some services' rejections, list them in
`REJECTION_SERVICES_ENABLED` and set `REJECTION_SERVICE_GATE=enforce`.

Two services may share a stage only on disjoint, non-empty `sub_stages`; any
other overlap is a boot error. Topic patterns cannot be checked for overlap in
advance, so two patterns matching one topic are logged at run time and decide
nothing.

## Which pack a packet is analysed with

The same decision the gate makes:

- A packet of an enabled or pilot service uses its own pack.
- An unresolved packet let through by
  `REJECTION_UNRESOLVED_SERVICE=default_pack` uses `_default`, with its
  confidence capped at 0.6.
- A packet the gate would skip is analysed at all only when the gate is in
  `record` mode, and then with the `enu-biometric` pack -- exactly as every
  packet was before packs existed.

A service is not analysed with its own pack until it is in
`REJECTION_SERVICES_PILOT` or `REJECTION_SERVICES_ENABLED`.

## Piloting a new service

A new service is piloted before it is enabled (`MULTI_SERVICE_PLAN.md`
section 7). Name it in `REJECTION_SERVICES_PILOT`. Its packets are then
analysed with its pack, like an enabled service's, except that:

- their casebooks carry `"pilot": true`;
- their Synthesis agent has no `queue_for_replay` tool, and its prompt ends
  with a `### PILOT MODE` section saying so. Nothing a pilot concludes is
  replayed.

The service's experts record verdicts with `POST /outcome/{event_id}`, and
`python3 -m src.tools.accuracy_report --service <name>` gives the result.
Once that meets the agreed bar, move the name from `REJECTION_SERVICES_PILOT`
to `REJECTION_SERVICES_ENABLED`. A name in both lists is a boot error.

## Which tools a packet's agents get

A tool's toolset names the services it is for (`services`, published as
`uidai.crm/services`), or `"*"` for every service. An agent built for a pack
is offered a tool its role gets only when the tool names `"*"` or the pack's
service, or names no services and `AGENT_TOOLS_COMMON` lists it, or the pack's
`tools.include` lists it -- and never when its `tools.exclude` does. The
opencode harness gets the same scope, as one agent per role and service
(`crm_<role>__<service with non-alphanumerics as _>`). A service's own tools
live in `src/tools/agent_tools/<service_slug>/`; `enu-biometric`'s are the
`bio_*` process DB tools. No service may be named `default`: that name is the
unresolved packets' agents'.

## `_default`

It matches nothing, serves no tools of its own (`tool_prefix` is not allowed,
nor is `tools.include`: its agents get the `"*"` tools alone) and has
`rule_source.type` `none`. Its `policy.md` tells the agents that no
service-specific policy applies. It is used only when
`REJECTION_UNRESOLVED_SERVICE=default_pack` lets an unresolved packet through.

## Writing the markdown files

Checked at load; a failing file is a boot error:

- Generic: no refId, eventId, UUID, timestamp or long digit run -- the checks
  the reason-code documentation passes -- and no instruction-shaped text
  (`runbook_validator.INJECTION_MARKERS`).
- `policy.md` present and not empty.
- The text composed for any one role -- its file, `policy.md` and, for the
  Investigator, `learned_rules.md` -- within `SERVICE_PACK_MAX_CHARS`
  (default 20000).

And by convention:

- Say what the service does and what its terms mean. Do not repeat the generic
  rules the role prompts already carry (evidence gaps, output format,
  citations).
- Define every term the service uses in a sense another service might not --
  enu-biometric's "demo" means the face modality, which is not what a
  demographic service means by it.
- The generic prompts point at "the SERVICE CONTEXT section" for what each
  enrolment type means, and at "the SERVICE POLICY" for the terms. Put those
  there.

## Dead-lettered records (the DLT lane)

Every service dead-letters its records with the rejection lane's payload and
key; the headers carry the stack trace (`MULTI_SERVICE_PLAN.md` Phase 8). A
record is placed in a service by, in order:

1. its consumer group (`dlt.consumer_groups`);
2. `flowMetaData.stage` -- the AUDIT payload's `edata.stage` and `subStage`,
   which on a dead-lettered record are the failing service's own (`QC` /
   `SMART_QC`), so list those values in `match.stages` / `match.sub_stages`;
3. its original topic (`dlt.original_topics`);
4. its failure site's Java package (`dlt.java_packages`);
5. `sourceTopic`;
6. its reason code's documentation file.

The reason-code service map is not consulted: a dead-lettered record's code
is usually a generic one such as `UNHANDLED_EXCEPTION`, and its stage already
names the service.

A service's dead-lettered records are analysed with its pack only once it is
in `DLT_SERVICES_ENABLED`; with `DLT_SERVICE_GATE=enforce`, other services'
records are acknowledged without analysis. Its agents then get:

- the pack's optional `dlt.md` as their SERVICE CONTEXT: what its crashes
  usually mean, where its code and data live. It is checked like the other
  markdown files.
- the tools in its scope.

`policy.md` is not given to the DLT agents: it is written for rejections.
