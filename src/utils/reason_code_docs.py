"""
Reason-code documentation: the store, the lookup, and the validator
(REASON_CODE_DOCS_PLAN.md sections 5 and 6, Phase 2).

With the rejection lane's opencode harness switched off, the direct
Investigator can no longer explore the DROA corpus for itself. It is handed
curated documentation for the packet's own reason code instead, chosen in
Python rather than by an agent.

The store is one JSON file per service under `<root>/services/`, in the shape
the service teams generate from their own source (see `README.md` in the store
for the schema). Each file carries two kinds of entry, and both are documentation:

* `codes[]`      -- what a reason code means: its numeric code, category,
                    whether it is retryable, and a prose description traced
                    through the service source.
* `rules.rules[]` -- the CRE policy rules that raise a code: the condition
                    that fires them and what happens to the applicant and the
                    candidates as a result.

The two are keyed by reason code, so the mapping the plan's `index.json`
carried is intrinsic here and there is no second file to keep in step with the
first. Enrolment type is intrinsic too: a rule whose condition names
`'ENROLMENT'` documents the E family and one naming `'UPDATE'` documents U,
while a rule that names neither fires for every type and is always included.

Three properties this module is built around:

* **`lookup` never raises.** A documentation problem degrades one packet's
  prompt; it must never fail the packet. Every failure becomes outcome
  `error` and the pipeline carries on without the document.
* **Nothing here has side effects.** No metrics, no state. The Investigator
  counts the lookup outcome, so a validator run and a CLI run cannot inflate
  the pipeline's counters.
* **The files are read on every lookup**, as `prompt_loader.render` does. They
  are small, and edits in a mounted `REASON_CODE_DOCS_DIR` then take effect
  without a restart.
"""
import json
import os
import re
from typing import Optional

from src.models.synthesis import ACTIONS, RESIDENT_ACTIONS
from src.utils import paths
from src.utils.env import get_bool_env
from src.utils.logging_config import get_logger
from src.utils.runbook_store import REASON_CODE_PATTERN
from src.utils.runbook_validator import INJECTION_MARKERS, validate_generic_text

logger = get_logger(__name__)

SCHEMA_VERSION = 1

#: The enrolment type that matches any packet. Authors never write `N`,
#: `ENROLMENT`, `ENROLLMENT` or `UPDATE`: those are normalised to a family
#: letter before anything is compared (D4).
ANY = "ANY"

#: Cap on the rendered documentation for one reason code and enrolment type.
DEFAULT_MAX_CHARS = 16000

#: Where the per-service files live, relative to the store root.
SERVICES_DIRNAME = "services"

#: Reserved for hosting the store outside the image. `docs_loader` already
#: does this for the DROA corpus and is the model to follow: list the prefix,
#: download into a temp directory, swap it into place, and degrade to whatever
#: is on disk when S3 is unreachable. Nothing downloads yet -- the files ship
#: in `src/reason_code_docs/` and `REASON_CODE_DOCS_DIR` can already point at
#: a mounted volume, so this is a placeholder for the path, not a promise that
#: the path works.
DEFAULT_S3_PREFIX = "nalanda/reason-codes"

#: A raw enrolment type that is not a known alias is used as-is when it looks
#: like a type code (for example `Z`). `B/D` does not, so it matches only ANY.
_RAW_TYPE_PATTERN = re.compile(r"^[A-Z]{1,16}$")

#: The DB-side type names `tool_registry.normalize_enrolment_type` returns,
#: mapped to the family letter this module compares on. `N` and `E` are the
#: same policy, and the rule lookup already treats them as one.
_TYPE_FAMILY = {"ENROLMENT": "E", "UPDATE": "U"}

#: How a CRE rule declares the enrolment type it applies to, inside its
#: `condition_description`. A rule that declares none fires for every type.
_RULE_TYPE_PATTERN = re.compile(r"the enrolment type is '([A-Za-z_]+)'")

#: Content rule 5: a resolution-guidance line, and the heading it may appear
#: under. Anything that starts like one and does not match is an error, so a
#: typo is reported rather than silently dropped.
_GUIDANCE_LINE = re.compile(
    r"^- action:\s*([A-Z_]+)\s*\|\s*resident_action:\s*([A-Z_]+)\s*"
    r"(?:\|\s*when:\s*(.+))?$")
_GUIDANCE_PREFIX = "- action:"
_GUIDANCE_HEADING = "Resolution guidance"

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")

#: How a matched enrolment type is described in the document's title line.
_TYPE_DISPLAY = {
    "E": "New Enrolment (E)",
    "U": "Biometric Update (U)",
    ANY: "all enrolment types",
}

#: The keys a service file may carry at the top level, and the keys one
#: `codes[]` / `rules.rules[]` entry may carry. Unknown keys are an error:
#: a misspelled `conditon_description` would otherwise silently document
#: nothing.
_FILE_KEYS = {"schema_version", "title", "description", "service",
              "total_codes", "codes", "rules"}
_CODE_KEYS = {"numeric_code", "reason_code", "description", "category",
              "is_retryable", "resolution_guidance"}
_RULES_KEYS = {"description", "total_rules", "rules"}
_RULE_KEYS = {"rule_id", "reject_reason_code", "module", "description",
              "condition_description"}
_GUIDANCE_KEYS = {"action", "resident_action", "when"}


class ReasonCodeDocError(Exception):
    """A document could not be read or rendered."""


# ======================================================================
# Configuration
# ======================================================================

def docs_enabled() -> bool:
    """Whether the direct Investigator reasons from these documents.

    Read at call time, not at import: the switch is flipped per deployment
    and the tests drive both sides of it against one imported module.
    """
    return get_bool_env("REJECTION_REASON_CODE_DOCS_ENABLED", False)


def max_chars() -> int:
    """Cap on one rendered document. An unusable value falls back rather than
    raising -- a typo in a tunable must not stop the pipeline."""
    raw = os.environ.get("REASON_CODE_DOC_MAX_CHARS", "")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return DEFAULT_MAX_CHARS
    return value if value > 0 else DEFAULT_MAX_CHARS


def s3_prefix() -> str:
    """Where the service files would be hosted. See DEFAULT_S3_PREFIX."""
    return os.environ.get("REASON_CODE_DOCS_S3_PREFIX",
                          DEFAULT_S3_PREFIX).strip("/")


def download_service_docs() -> bool:
    """Placeholder for fetching the store from S3. Downloads nothing yet.

    The files ship in the image, so there is nothing to fetch and returning
    False here is honest rather than degraded: it says "no download happened",
    which is exactly true. Wiring it up means following
    `docs_loader.download_corpus` against `s3_prefix()`; until then a
    deployment that wants the files from elsewhere points
    `REASON_CODE_DOCS_DIR` at a volume someone else populated.
    """
    logger.info("Reason-code document download is not wired up; using the "
                "files on disk", prefix=s3_prefix(), directory=str(_root()))
    return False


def _root(root=None):
    """The store root, resolved at call time so tests can patch it.

    `paths.REASON_CODE_DOCS_DIR` is read through the module rather than
    imported by name, so `monkeypatch.setattr(paths, ...)` is visible here.
    """
    from pathlib import Path
    return Path(root) if root is not None else paths.REASON_CODE_DOCS_DIR


# ======================================================================
# Enrolment types
# ======================================================================

def normalize_doc_type(raw) -> Optional[str]:
    """The enrolment-type family a raw payload value belongs to (D4).

    `N`, `E`, `ENROLMENT` and `ENROLLMENT` give `E`; `U` and `UPDATE` give
    `U`; another value that looks like a type code is used as-is (`Z`);
    anything else, including a missing value, gives None and can therefore
    only match ANY.
    """
    if raw is None:
        return None
    text = str(raw).strip().upper()
    if not text:
        return None

    # Imported here rather than at module scope: tool_registry pulls in the
    # DB layer, the log pipeline and the LangChain tool decorators, and this
    # module is imported by the config validator and by a CLI.
    from src.tools.tool_registry import normalize_enrolment_type

    known = normalize_enrolment_type(text)
    if known:
        return _TYPE_FAMILY[known]
    return text if _RAW_TYPE_PATTERN.match(text) else None


def _rule_enrolment_type(condition_description: str) -> Optional[str]:
    """The type a CRE rule applies to, or None when it applies to every type.

    A condition that somehow names two different types is treated as naming
    none: including it everywhere is the safe reading, because the rule
    demonstrably fires for more than one.
    """
    found = {normalize_doc_type(name)
             for name in _RULE_TYPE_PATTERN.findall(condition_description or "")}
    found.discard(None)
    return found.pop() if len(found) == 1 else None


# ======================================================================
# Reading the store
# ======================================================================

def _service_files(root) -> list:
    """Every service file in the store, in a stable order.

    The containment check catches a symlink under `services/` pointing at a
    file outside the store, which globbing alone follows without complaint.
    """
    directory = root / SERVICES_DIRNAME
    if not directory.is_dir():
        return []
    inside = os.path.realpath(directory)
    kept = []
    for path in sorted(directory.glob("*.json")):
        if os.path.commonpath([inside, os.path.realpath(path)]) != inside:
            raise ReasonCodeDocError(
                f"{path.name} resolves outside {SERVICES_DIRNAME}/")
        kept.append(path)
    return kept


def _load_service_file(path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            document = json.load(handle)
    except UnicodeDecodeError as error:
        raise ReasonCodeDocError(f"{path.name} is not UTF-8: {error}") from error
    except ValueError as error:
        raise ReasonCodeDocError(f"{path.name} is not valid JSON: {error}") from error
    except OSError as error:
        raise ReasonCodeDocError(f"{path.name} could not be read: {error}") from error
    if not isinstance(document, dict):
        raise ReasonCodeDocError(
            f"{path.name} holds a {type(document).__name__}, not an object.")
    return document


def _render_guidance(guidance) -> list:
    """Structured guidance as the `- action:` lines content rule 5 defines.

    One writer and one parser for guidance, so a line the validator accepts
    is exactly a line `parse_resolution_guidance` reads back.
    """
    lines = []
    for item in guidance or []:
        if not isinstance(item, dict):
            continue
        line = (f"- action: {item.get('action')} | "
                f"resident_action: {item.get('resident_action')}")
        when = item.get("when")
        if when:
            line += f" | when: {when}"
        lines.append(line)
    return lines


def _code_entry(document: dict, source: str, code: dict) -> dict:
    """One `codes[]` entry as a documentation block.

    A code entry describes the reason code itself, which is true whatever the
    packet's enrolment type, so it carries no type and is always included.
    """
    numeric = code.get("numeric_code")
    retryable = code.get("is_retryable")
    retryable_text = ("unknown" if retryable is None
                      else ("yes" if retryable else "no"))
    body = [
        f"## Reason code -- {document.get('service')}"
        + (f", numeric code {numeric}" if numeric is not None else ""),
        "",
        f"Category: {code.get('category') or 'unspecified'}. "
        f"Retryable: {retryable_text}.",
        "",
        str(code.get("description") or "").strip(),
    ]
    lines = _render_guidance(code.get("resolution_guidance"))
    if lines:
        body += ["", f"## {_GUIDANCE_HEADING}", ""] + lines
    return {
        "kind": "code",
        "ref": str(numeric) if numeric is not None else "-",
        "source": source,
        "enrolment_type": None,
        "body": "\n".join(body).rstrip(),
    }


def _rule_entry(source: str, rule: dict) -> dict:
    """One `rules.rules[]` entry as a documentation block.

    The rule's `description` is its `condition_description` wrapped in
    generated prose -- a boilerplate preamble in front and the outcome behind.
    The preamble repeats the module and the reason code, which the heading
    already names, so only the outcome is kept. Every shipped rule contains
    its condition verbatim, and the validator enforces that, so the split is
    exact rather than a guess.
    """
    condition = str(rule.get("condition_description") or "").strip()
    description = str(rule.get("description") or "").strip()
    outcome = ""
    if condition and condition in description:
        outcome = description.split(condition, 1)[1].lstrip(". ").strip()
    else:
        outcome = description

    enrolment_type = _rule_enrolment_type(condition)
    scope = (f"enrolment type {enrolment_type}" if enrolment_type
             else "all enrolment types")
    body = [
        f"## Rule {rule.get('rule_id')} -- {rule.get('module')}, {scope}",
        "",
        f"Fires when: {condition}.",
    ]
    if outcome:
        body += ["", f"Outcome: {outcome}"]
    return {
        "kind": "rule",
        "ref": str(rule.get("rule_id") or "-"),
        "source": source,
        "enrolment_type": enrolment_type,
        "body": "\n".join(body).rstrip(),
    }


def _entries_for(root, reason_code: str) -> list:
    """Every documentation block any service publishes for this reason code."""
    entries = []
    for path in _service_files(root):
        document = _load_service_file(path)
        source = f"{SERVICES_DIRNAME}/{path.name}"
        for code in document.get("codes") or []:
            if isinstance(code, dict) and code.get("reason_code") == reason_code:
                entries.append(_code_entry(document, source, code))
        rules = document.get("rules") or {}
        for rule in (rules.get("rules") or []) if isinstance(rules, dict) else []:
            if isinstance(rule, dict) and rule.get("reject_reason_code") == reason_code:
                entries.append(_rule_entry(source, rule))
    return entries


# ======================================================================
# Rendering
# ======================================================================

def _render(reason_code: str, matched_type: str, entries: list):
    """The exact text the model is shown, and whether it had to be cut.

    Each block is attributed, so a claim in an investigation can be traced to
    the service file and entry it came from.
    """
    header = (f"# {reason_code} -- "
              f"{_TYPE_DISPLAY.get(matched_type, f'enrolment type {matched_type}')}")
    blocks = [header]
    for entry in entries:
        blocks.append(f"[Source: {entry['source']}, {entry['kind']} "
                      f"{entry['ref']}]\n{entry['body']}")
    text = "\n\n".join(blocks)

    limit = max_chars()
    truncated = False
    if len(text) > limit:
        marker = ("\n\n... {} characters omitted from the end of the "
                  "reason-code documentation (REASON_CODE_DOC_MAX_CHARS) ...")
        # The marker counts against the limit, so the kept prefix leaves room
        # for it -- otherwise the "capped" text is longer than the cap.
        omitted = len(text) - limit
        rendered = marker.format(omitted)
        keep = max(0, limit - len(rendered))
        text = text[:keep] + marker.format(len(text) - keep)
        truncated = True
    return text, truncated


def parse_resolution_guidance(text: str) -> list:
    """The `- action:` lines under a `Resolution guidance` heading.

    Content rule 5: a guidance line elsewhere in the document is not guidance,
    and a line that starts like one but does not parse is reported by the
    validator rather than being read here.
    """
    guidance, in_section = [], False
    for line in (text or "").splitlines():
        heading = _HEADING.match(line)
        if heading:
            in_section = heading.group(2).strip() == _GUIDANCE_HEADING
            continue
        if not in_section:
            continue
        match = _GUIDANCE_LINE.match(line.strip())
        if match:
            guidance.append({"action": match.group(1),
                             "resident_action": match.group(2),
                             "when": (match.group(3) or "").strip() or None})
    return guidance


# ======================================================================
# Lookup
# ======================================================================

def _state(outcome: str, reason_code=None, requested_type=None, detail=None) -> dict:
    """The 5.7 shape for every outcome other than `hit`.

    Filled in completely rather than partially: every reader uses `.get()`
    with a default, but a state that is missing half its keys makes a
    provenance record that silently differs from packet to packet.
    """
    return {
        "outcome": outcome,
        "reason_code": reason_code,
        "requested_type": requested_type,
        "matched_type": None,
        "refs": [],
        "text": None,
        "sha256": None,
        "truncated": False,
        "resolution_guidance": [],
        "detail": detail,
    }


def error_state(reason_code=None, detail=None) -> dict:
    """The 5.7 shape for a failure a caller detected rather than `lookup` did.

    `lookup` never raises, so the Investigator's guard around it exists only
    against a bug in this module. It still needs a complete state to store,
    because a half-filled one makes a provenance record that differs from
    every other packet's.
    """
    return _state("error", reason_code, None, detail)


def lookup(reason_code, raw_enrolment_type, root=None) -> dict:
    """The documentation for one packet, as the state shape in 5.7.

    Never raises: the caller is a graph node, and a documentation problem is
    a degraded prompt, not a failed packet.
    """
    import hashlib

    requested_type = None
    try:
        if not reason_code:
            return _state("no_reason_code")
        reason_code = str(reason_code)
        if not REASON_CODE_PATTERN.match(reason_code):
            return _state("miss", reason_code, detail="invalid reason code")

        requested_type = normalize_doc_type(raw_enrolment_type)
        entries = _entries_for(_root(root), reason_code)
        if not entries:
            return _state("miss", reason_code, requested_type)

        # Candidates in order (5.3 step 5): the packet's own type, then ANY.
        # A type-agnostic entry applies whichever candidate wins, because the
        # rule it documents fires for every type.
        if requested_type and any(e["enrolment_type"] == requested_type
                                  for e in entries):
            matched_type = requested_type
        else:
            matched_type = ANY
        selected = [e for e in entries
                    if e["enrolment_type"] in (None, matched_type)]
        if not selected:
            return _state("miss", reason_code, requested_type)

        text, truncated = _render(reason_code, matched_type, selected)
        if truncated:
            logger.warning("Reason-code documentation was truncated",
                           reason_code=reason_code, matched_type=matched_type,
                           limit=max_chars())
        return {
            "outcome": "hit",
            "reason_code": reason_code,
            "requested_type": requested_type,
            "matched_type": matched_type,
            "refs": [{"source": e["source"], "kind": e["kind"], "ref": e["ref"]}
                     for e in selected],
            "text": text,
            "sha256": "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "truncated": truncated,
            "resolution_guidance": parse_resolution_guidance(text),
            "detail": None,
        }
    except Exception as error:  # noqa: BLE001 - the contract is "never raises"
        return _state("error", reason_code if reason_code else None,
                      requested_type, f"{type(error).__name__}: {error}")


def provenance(doc_state) -> Optional[dict]:
    """The casebook's copy of a lookup: everything except the document itself.

    The text never reaches a casebook or a log line. It is large, it is the
    same for every packet with this reason code, and the `sha256` already
    identifies exactly which version the model was shown.
    """
    if doc_state is None:
        return None
    return {
        "outcome": doc_state.get("outcome"),
        "reason_code": doc_state.get("reason_code"),
        "requested_type": doc_state.get("requested_type"),
        "matched_type": doc_state.get("matched_type"),
        "refs": doc_state.get("refs") or [],
        "sha256": doc_state.get("sha256"),
        "truncated": doc_state.get("truncated", False),
        "detail": doc_state.get("detail"),
    }


# ======================================================================
# Validation
# ======================================================================

def _check_content(label: str, text: str, errors: list) -> None:
    """Content rules 1 and 2 of section 5.5, over one rendered document.

    Documentation is generic by construction -- it describes a policy, not a
    packet -- so a UUID, a timestamp or a long digit run in it means a
    specific case leaked into the store. The injection markers matter for the
    same reason the learning-rule validator checks them: this text is placed
    in the Investigator's prompt.
    """
    for violation in validate_generic_text(text, []):
        errors.append(f"{label}: {violation}")
    lowered = text.lower()
    for marker in INJECTION_MARKERS:
        if marker in lowered:
            errors.append(f"{label}: contains instruction-shaped text: {marker!r}")


def _check_guidance(label: str, text: str, errors: list) -> None:
    """Content rule 5: guidance lines parse, sit under their heading, and use
    values the Synthesis contract accepts."""
    in_section = False
    for line in text.splitlines():
        heading = _HEADING.match(line)
        if heading:
            in_section = heading.group(2).strip() == _GUIDANCE_HEADING
            continue
        stripped = line.strip()
        if not stripped.startswith(_GUIDANCE_PREFIX):
            continue
        if not in_section:
            errors.append(f"{label}: a resolution-guidance line sits outside a "
                          f"'{_GUIDANCE_HEADING}' section: {stripped!r}")
            continue
        match = _GUIDANCE_LINE.match(stripped)
        if not match:
            errors.append(f"{label}: malformed resolution-guidance line: "
                          f"{stripped!r}")
            continue
        if match.group(1) not in ACTIONS:
            errors.append(f"{label}: unknown action {match.group(1)!r}; "
                          f"expected one of {list(ACTIONS)}")
        if match.group(2) not in RESIDENT_ACTIONS:
            errors.append(f"{label}: unknown resident_action {match.group(2)!r}; "
                          f"expected one of {list(RESIDENT_ACTIONS)}")


def _check_keys(label: str, value, allowed: set, errors: list) -> bool:
    if not isinstance(value, dict):
        errors.append(f"{label} is a {type(value).__name__}, not an object.")
        return False
    unknown = sorted(set(value) - allowed)
    if unknown:
        errors.append(f"{label} has unknown key(s) {unknown}; "
                      f"expected a subset of {sorted(allowed)}.")
    return True


def _validate_file(path, errors: list, warnings: list, skipped: set) -> dict:
    """Check one service file's shape. Returns its reason codes by type."""
    name = path.name
    try:
        document = _load_service_file(path)
    except ReasonCodeDocError as error:
        errors.append(str(error))
        return {}

    if not _check_keys(name, document, _FILE_KEYS, errors):
        return {}
    if document.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"{name}: schema_version is "
                      f"{document.get('schema_version')!r}, expected "
                      f"{SCHEMA_VERSION}.")
    service = document.get("service")
    if not isinstance(service, str) or not service.strip():
        errors.append(f"{name}: 'service' must be a non-empty string.")

    codes = document.get("codes")
    if codes is None:
        codes = []
    if not isinstance(codes, list):
        errors.append(f"{name}: 'codes' is a {type(codes).__name__}, not a list.")
        codes = []

    seen = set()
    published = {}
    for index, code in enumerate(codes):
        label = f"{name} codes[{index}]"
        if not _check_keys(label, code, _CODE_KEYS, errors):
            continue
        reason_code = code.get("reason_code")
        if not isinstance(reason_code, str) or not reason_code.strip():
            errors.append(f"{label}: 'reason_code' must be a non-empty string.")
            continue
        if reason_code in seen:
            errors.append(f"{label}: duplicate reason_code {reason_code!r} in "
                          f"this file's codes.")
        seen.add(reason_code)
        if not isinstance(code.get("description"), str) or not code["description"].strip():
            errors.append(f"{label}: 'description' must be a non-empty string.")
        for item in code.get("resolution_guidance") or []:
            _check_keys(f"{label} resolution_guidance", item, _GUIDANCE_KEYS, errors)
        if _addressable(reason_code, label, warnings, skipped):
            published.setdefault(reason_code, set()).add(None)

    rules_block = document.get("rules")
    if rules_block is not None:
        if _check_keys(f"{name} rules", rules_block, _RULES_KEYS, errors):
            rules = rules_block.get("rules")
            if rules is None:
                rules = []
            if not isinstance(rules, list):
                errors.append(f"{name} rules.rules is a "
                              f"{type(rules).__name__}, not a list.")
                rules = []
            for index, rule in enumerate(rules):
                label = f"{name} rules.rules[{index}]"
                if not _check_keys(label, rule, _RULE_KEYS, errors):
                    continue
                reason_code = rule.get("reject_reason_code")
                if not isinstance(reason_code, str) or not reason_code.strip():
                    errors.append(f"{label}: 'reject_reason_code' must be a "
                                  f"non-empty string.")
                    continue
                condition = rule.get("condition_description")
                description = rule.get("description")
                for field, value in (("condition_description", condition),
                                     ("description", description)):
                    if not isinstance(value, str) or not value.strip():
                        errors.append(f"{label}: {field!r} must be a non-empty "
                                      f"string.")
                if isinstance(condition, str) and isinstance(description, str) \
                        and condition.strip() and condition not in description:
                    # The renderer splits the outcome off the description at
                    # the condition. Without the condition verbatim it would
                    # repeat the whole description instead.
                    errors.append(f"{label}: 'description' does not contain "
                                  f"'condition_description' verbatim, so the "
                                  f"rule's outcome cannot be separated from "
                                  f"its condition.")
                if _addressable(reason_code, label, warnings, skipped):
                    published.setdefault(reason_code, set()).add(
                        _rule_enrolment_type(condition))

    if not published:
        warnings.append(f"{name}: publishes no addressable reason code.")
    return published


def _addressable(reason_code: str, label: str, warnings: list,
                 skipped: set) -> bool:
    """Whether a payload's `errorReasonCode` could ever equal this key.

    A key that fails REASON_CODE_PATTERN is a warning, not an error. These
    files are generated from service source, and a key such as
    `(CRE_REJECT_APPLICANT)` is a rule whose reject reason code the generator
    could not resolve -- real data, not a typo. It can never match a payload
    value, so it is skipped and reported; failing start-up over it would mean
    the file could never ship.
    """
    if REASON_CODE_PATTERN.match(reason_code):
        return True
    # Warned about once, not once per entry: 17 of the shipped rules carry the
    # same unresolved key, and repeating it drowns out every other warning.
    if reason_code not in skipped:
        skipped.add(reason_code)
        warnings.append(f"{label}: reason code {reason_code!r} cannot match a "
                        f"payload errorReasonCode; every entry under it is "
                        f"skipped.")
    return False


def validate(root=None, coverage: bool = False):
    """Check the whole store. Returns (errors, warnings).

    Errors are what `main_api` exits on when the documents are switched on: a
    store that cannot be read or that would put a packet identifier into a
    prompt is a configuration failure, and failing at boot is the convention
    `validate_config` already sets. Warnings are only logged.
    """
    errors, warnings = [], []
    store = _root(root)

    if not store.is_dir():
        errors.append(f"The reason-code document store {store} does not exist.")
        return errors, warnings

    try:
        files = _service_files(store)
    except ReasonCodeDocError as error:
        return [str(error)], warnings

    if not files:
        errors.append(f"No service files found in {store / SERVICES_DIRNAME}; "
                      f"expected at least one <service>.json.")
        return errors, warnings

    services, published, skipped = {}, {}, set()
    for path in files:
        entries = _validate_file(path, errors, warnings, skipped)
        try:
            service = _load_service_file(path).get("service")
        except ReasonCodeDocError:
            service = None
        if service:
            if service in services:
                errors.append(f"{path.name}: service {service!r} is already "
                              f"declared by {services[service]}.")
            services[service] = path.name
        for reason_code, types in entries.items():
            published.setdefault(reason_code, set()).update(types)

    # Render every reason code the store publishes, for every type it could be
    # asked about, and check the text a model would actually be shown. A file
    # whose fields are individually fine can still render a document that is
    # over the cap or that carries a date.
    for reason_code, types in sorted(published.items()):
        candidates = sorted(t for t in types if t) or [ANY]
        for requested in candidates:
            state = lookup(reason_code, requested, root=store)
            label = f"{reason_code} [{requested}]"
            if state["outcome"] == "error":
                errors.append(f"{label}: {state['detail']}")
                continue
            if state["outcome"] != "hit":
                errors.append(f"{label}: the store publishes this reason code "
                              f"but the lookup returned {state['outcome']!r}.")
                continue
            if state["truncated"]:
                errors.append(f"{label}: the rendered documentation exceeds "
                              f"REASON_CODE_DOC_MAX_CHARS ({max_chars()}).")
            _check_content(label, state["text"], errors)
            _check_guidance(label, state["text"], errors)

    if coverage:
        _check_coverage(published, warnings)

    return errors, warnings


def _check_coverage(published: dict, warnings: list) -> None:
    """Reason codes that already have a runbook but no documentation.

    A runbook is a stored answer for a code the pipeline meets often, so a
    code with one and no documentation is the next document worth having.
    """
    from src.utils.runbook_store import RUNBOOK_DRAFT_DIR, RUNBOOK_FINAL_DIR

    for directory in (RUNBOOK_FINAL_DIR, RUNBOOK_DRAFT_DIR):
        if not directory.is_dir():
            continue
        for path in sorted(directory.glob("*.json")):
            reason_code = path.stem.rsplit("__", 1)[0]
            if reason_code not in published:
                warnings.append(f"{reason_code} has a runbook ({directory.name}/"
                                f"{path.name}) but no documentation.")
