# Reason-code documentation

This is the documentation the rejection Investigator reasons from when it runs
on the direct path instead of the opencode harness. It replaces an agent
exploring the whole DROA corpus with a lookup done in Python: the packet's
reason code and enrolment type select the text, and that text goes into the
prompt alongside the Database Rule Configuration, the payload and the logs.

The Reviewer is given the same text, so it can check the investigation against
the documentation rather than only reading the investigation.

```
src/reason_code_docs/
  README.md                  this file
  services/
    enu-biometric.json       one file per service
    <service>.json
```

There is no separate index. A service file is keyed by reason code already,
so the mapping is intrinsic and there is no second file to keep in step.

Read `REASON_CODE_DOCS_PLAN.md` at the repository root for why the store looks
like this; sections 5 and 6 are the contract this file summarises.

## The file format

One JSON object per service. The generator that produces it traces each code
through the service source, so the file is machine-generated: edit the
generator, not the output.

```json
{
  "schema_version": 1,
  "title": "ENU Biometric Stage - Rejection/Reason Codes (Code-Verified)",
  "description": "All rejection and reason codes for the enu-biometric service...",
  "service": "enu-biometric",
  "total_codes": 105,
  "codes": [
    {
      "numeric_code": 2315,
      "reason_code": "ABIS_MW_RESPONSE_PROCESSING_FAILED",
      "description": "Thrown as an ApplicationException when ...",
      "category": "Technical",
      "is_retryable": true,
      "resolution_guidance": [
        {"action": "REPLAY", "resident_action": "PENDING",
         "when": "the downstream service has recovered"}
      ]
    }
  ],
  "rules": {
    "description": "Policy/rule-based rejection rules evaluated by the rule engine...",
    "total_rules": 58,
    "rules": [
      {
        "rule_id": "cre-14f9c766",
        "reject_reason_code": "RESIDENT_MAN_DEDUPE_REJECT_WL_DEMOMATCH_TD",
        "module": "MDD_POLICY_BATCH_1",
        "description": "CRE rejection rule in MDD_POLICY_BATCH_1. ... This rule fires when <condition>. The APPLICANT is REJECTED ...",
        "condition_description": "the enrolment type is 'ENROLMENT' (New Enrolment); AND ..."
      }
    ]
  }
}
```

Both sections are documentation, and a reason code may appear in either or
both:

- **`codes[]`** says what a reason code *means*: its numeric code, its
  category, whether the failure is retryable, and prose describing the code
  path that raises it.
- **`rules.rules[]`** says what *fires* a code: the rule engine condition, and
  what happens to the applicant and the candidates as a result.

`resolution_guidance` is optional and is the only field an author is expected
to add by hand. It is what Synthesis is shown when
`REJECTION_SYNTHESIS_DOC_GUIDANCE=true`.

## How a lookup picks its text

Given the packet's first non-empty `errorReasonCode` and its raw
`packetMetaData.enrolmentType`:

1. The enrolment type is normalised to a family: `N`, `E`, `ENROLMENT` and
   `ENROLLMENT` give `E`; `U` and `UPDATE` give `U`; another value that looks
   like a type code is used as-is (`Z`); anything else gives nothing.
2. Every entry any service publishes for that reason code is collected. A
   `codes[]` entry applies to every type. A `rules.rules[]` entry applies to
   the type its `condition_description` names -- `the enrolment type is
   'ENROLMENT'` means `E`, `'UPDATE'` means `U` -- and to every type when it
   names none.
3. If some entry matches the packet's own type, that type is the match and the
   document holds those entries plus the type-agnostic ones. Otherwise the
   match is `ANY` and the document holds the type-agnostic entries alone.
4. With nothing left to include, the lookup is a miss and the Investigator is
   told plainly that no documentation is available.

A reason code documented only for `U` is therefore correctly a miss for an
enrolment packet, rather than being answered with rules that cannot have
fired.

## Rules the validator enforces

Run it before committing a change to this store:

```
python -m src.tools.check_reason_code_docs            # errors and warnings
python -m src.tools.check_reason_code_docs --coverage # also: codes with a runbook and no docs
python -m src.tools.check_reason_code_docs --dir <path>
```

It exits 1 on any error, and CI runs it through
`tests/test_reason_code_docs.py`. The API runs it at boot when
`REJECTION_REASON_CODE_DOCS_ENABLED=true` and exits rather than starting with
a store it cannot read.

**Errors**

1. The file is not valid UTF-8 JSON, or is not an object.
2. `schema_version` is not `1`, or the file carries an unknown top-level key.
3. `service` is missing or empty, or two files declare the same service.
4. A `codes[]` or `rules.rules[]` entry carries an unknown key, is missing a
   required one, or repeats a `reason_code` within one file.
5. A rule's `description` does not contain its `condition_description`
   verbatim. The renderer splits the rule's outcome off at the condition, and
   without it the outcome cannot be separated from the condition.
6. A rendered document is longer than `REASON_CODE_DOC_MAX_CHARS` (16000).
7. A rendered document breaks a content rule (below).
8. A file under `services/` resolves outside the store, for example through a
   symlink.

**Warnings** -- reported, never fatal

- A reason code that cannot match a payload `errorReasonCode`, such as
  `(CRE_REJECT_APPLICANT)`. The generator emits these where it could not
  resolve a rule's reject reason code; every entry under such a key is
  skipped. This is data, not a typo, so it must not stop a deploy.
- A service file that publishes no addressable reason code at all.
- With `--coverage`: a reason code that has a draft or final runbook but no
  documentation. Those are the documents worth having next.

## Content rules

These apply to the **rendered** document -- the exact text the model is shown
-- not to any one field.

1. **Generic.** Nothing from a particular packet. The validator runs
   `runbook_validator.validate_generic_text`, so no UUIDs, no dates or
   timestamps, and no runs of ten or more digits. Examples use placeholders
   such as `<refId>` and `<timestamp>`.
2. **No prompt-injection phrases.** None of
   `runbook_validator.INJECTION_MARKERS`, compared case-insensitively. Some
   are ordinary phrases ("override the"); reword those.
3. **Terminology** follows `agent_policy_context.md`: "demo" is the face
   modality, "nonDemo" is fingerprints and iris, and "TD" means all nonDemo
   modalities matched.
4. **Biometric Update entries** cover the Mandatory Biometric Update case -- a
   first-time biometric update is deduplicated like a new enrolment --
   wherever it changes the answer.
5. **`resolution_guidance`** uses values from `synthesis.ACTIONS` and
   `synthesis.RESIDENT_ACTIONS`. It renders as
   `- action: X | resident_action: Y | when: Z` under a `Resolution guidance`
   heading, and a line of that shape anywhere else in a document is an error.
6. **Short.** 16000 characters per rendered document, and well under half of
   that is the target.

## Where the files come from

They ship inside the image through the same `COPY src /app/src` as the rest of
the package, so they are reviewed in pull requests and versioned with the code
that reads them. The cost is that editing one needs a deploy, which is
acceptable: the service source they are generated from also changes on deploy.

`REASON_CODE_DOCS_DIR` points the store somewhere else -- a mounted volume,
for instance -- with no code change. Hosting the files in S3 is a follow-up:
`reason_code_docs.s3_prefix()` and `download_service_docs()` reserve the path
and the entry point, and `utils/docs_loader.py` is the working implementation
to copy. Nothing downloads today.
