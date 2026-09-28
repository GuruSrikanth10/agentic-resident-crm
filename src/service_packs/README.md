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
| `tool_prefix` | Unique across the registry, `^[a-z][a-z0-9]{1,11}$`. Every tool scoped to this service alone is named `<prefix>_...` (Phase 4) |
| `match.stages` | Values of `flowMetaData.stage`, compared case-insensitively |
| `match.sub_stages` | Values of `flowMetaData.subStage`. Empty means any. Non-empty narrows `stages`, so it needs at least one stage |
| `match.source_topics` | Regular expressions, each matched against the whole `sourceTopic`. Consulted only when the stage decides nothing |
| `enrolment_types.payload` | Raw `packetMetaData.enrolmentType` value -> its family and the label the prompts show. A type the pack does not describe is shown as it arrived |
| `enrolment_types.family_labels` | Family -> the label in the reason-code documentation's title line (Phase 3) |
| `enrolment_types.doc_aliases` | Type names the documentation's rule conditions use -> family (Phase 3) |
| `rule_source.type` | `rules_db` when the service's rules are in the rules database, otherwise `none`: the reason-code documentation is then the rule source (Phase 3). `enrolment_type_filter` belongs to `rules_db` only |
| `reason_code_docs_file` | Stem of the reason-code documentation file. Defaults to `service`; no two services may share one |
| `droa_corpus_dir` | The service's directory in `docs_cache/`. Defaults to `service` |
| `logs.app_names`, `logs.k8s_match` | The service's Elasticsearch `application_name` values, and how its pods are found (`name_contains` or `label_selector`, not both). Namespaces stay in the environment (Phase 6) |
| `logs.also_search` | Other registered services whose logs are worth reading for this service's packets (Phase 6) |
| `logs.decision_vocabulary` | A regular expression added to the generic decision vocabulary (Phase 6) |
| `tools.include`, `tools.exclude` | Widen or narrow the service's tool scope by tool name (Phase 4) |
| `dlt.*` | Reserved for the DLT lane; nothing reads it yet |

Environment-specific values -- namespaces, hosts, credentials -- never go in a
pack. They differ between staging and production; the pack does not.

## How a packet is placed

In order, the first step that names exactly one service decides:

1. `flowMetaData.stage` (and `subStage`, for a service that lists sub-stages);
2. `sourceTopic`, against `match.source_topics`;
3. the packet's reason code, when exactly one documentation file documents it;
4. otherwise the packet is `_unresolved`.

When the documentation names a different service than the stage or topic did,
the stage or topic still wins, and the resolution records a `conflict`.

Two services may share a stage only on disjoint, non-empty `sub_stages`; any
other overlap is a boot error. Topic patterns cannot be checked for overlap in
advance, so two patterns matching one topic are logged at run time and decide
nothing.

## Which pack a packet is analysed with

The same decision the gate makes:

- A packet of an enabled service uses its own pack.
- An unresolved packet let through by
  `REJECTION_UNRESOLVED_SERVICE=default_pack` uses `_default`, with its
  confidence capped at 0.6.
- A packet the gate would skip is analysed at all only when the gate is in
  `record` mode, and then with the `enu-biometric` pack -- exactly as every
  packet was before packs existed.

A service is not analysed with its own pack until it is in
`REJECTION_SERVICES_ENABLED`.

## `_default`

It matches nothing, serves no tools of its own (`tool_prefix` is not allowed)
and has `rule_source.type` `none`. Its `policy.md` tells the agents that no
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
