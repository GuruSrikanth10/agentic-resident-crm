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
* **Nothing here has side effects.** No metrics, and no state beyond the memo
  behind `services_for_code`. The Investigator counts the lookup outcome, so a
  validator run and a CLI run cannot inflate the pipeline's counters.
* **The files are read on every lookup**, as `prompt_loader.render` does. They
  are small, and edits in a mounted `REASON_CODE_DOCS_DIR` then take effect
  without a restart. The memo keeps that property: it is keyed on each file's
  size and modification time, so an edited file is indexed again.
"""
import json
import os
import re
import threading
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

#: Where the store is hosted outside the image: `<prefix>/<service>.json`, or
#: `<prefix>/services/<service>.json`. Fetched by `download_service_docs` when
#: REASON_CODE_DOCS_S3_DOWNLOAD is on; otherwise the files ship in
#: `src/reason_code_docs/`, or REASON_CODE_DOCS_DIR points at a volume
#: someone else populated.
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

#: How a matched enrolment type is described in the document's title line when
#: the caller brings no labels of its own. A packet's service pack brings its
#: own (`enrolment_types.family_labels`), so these are deliberately neutral:
#: they are what a packet with no service-specific policy sees
#: (MULTI_SERVICE_PLAN.md D10).
_TYPE_DISPLAY = {
    "E": "New Enrolment (E)",
    "U": "Update (U)",
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
    """Where the service files are hosted. See DEFAULT_S3_PREFIX."""
    return os.environ.get("REASON_CODE_DOCS_S3_PREFIX",
                          DEFAULT_S3_PREFIX).strip("/")


ENV_S3_DOWNLOAD = "REASON_CODE_DOCS_S3_DOWNLOAD"
ENV_REFRESH = "REASON_CODE_DOCS_REFRESH_SECONDS"


def s3_download_enabled() -> bool:
    """Whether this process fetches the store from S3 (MULTI_SERVICE_PLAN.md
    D10). Off by default: the files ship in the image, and a deployment whose
    files are fetched by something else only points REASON_CODE_DOCS_DIR at
    them."""
    return get_bool_env(ENV_S3_DOWNLOAD, False)


def refresh_seconds() -> float:
    """How often to fetch the store again after the first time; 0 means only
    at start-up. An unusable value is 0 rather than an error."""
    try:
        value = float(os.environ.get(ENV_REFRESH, "").strip() or 0)
    except ValueError:
        return 0.0
    return value if value > 0 else 0.0


def _s3_bucket() -> Optional[str]:
    return os.environ.get("CASEBOOK_S3_BUCKET") or os.environ.get("S3_LOGS_BUCKET")


def _s3_client():
    from src.storage.s3 import _get_client
    return _get_client()


#: The part of a key after the prefix that names a service file: the file
#: itself, or the file under `services/` for a store uploaded with its layout.
#: Anything else under the prefix is not a service file and is ignored.
_S3_SERVICE_KEY = re.compile(r"^(?:services/)?([A-Za-z0-9][A-Za-z0-9._-]*)\.json$")

#: One download at a time: the start-up fetch and a refresh must never write
#: the same staging directory together.
_download_lock = threading.Lock()


def download_service_docs() -> bool:
    """Fetch the store from S3 into REASON_CODE_DOCS_DIR. True when a new copy
    is in place.

    The way `docs_loader.download_corpus` fetches the DROA corpus, with one
    addition: the downloaded store is validated before it replaces the one on
    disk. A download that fails, finds nothing, or does not validate changes
    nothing -- the last good copy keeps serving -- and is logged. Never raises:
    a documentation problem must not take the API down.
    """
    if not s3_download_enabled():
        logger.info("Reason-code document download is off; using the files on "
                    "disk", directory=str(_root()))
        return False
    bucket = _s3_bucket()
    if not bucket:
        logger.warning("Reason-code document download is on, but no S3 bucket "
                       "is configured (CASEBOOK_S3_BUCKET or S3_LOGS_BUCKET)")
        return False
    with _download_lock:
        try:
            return _download(bucket)
        except Exception as error:  # noqa: BLE001 - the contract is "never raises"
            logger.warning("Reason-code document download failed; keeping the "
                           "files on disk", error=f"{type(error).__name__}: {error}")
            return False


def _download(bucket: str) -> bool:
    import shutil

    prefix = s3_prefix()
    listed = f"{prefix}/" if prefix else ""
    root = _root()
    staging = root.parent / f".{root.name}.download"
    if staging.exists():
        shutil.rmtree(staging)
    (staging / SERVICES_DIRNAME).mkdir(parents=True)

    try:
        client = _s3_client()
        count = 0
        for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=listed):
            for item in page.get("Contents", []):
                match = _S3_SERVICE_KEY.match(item["Key"][len(listed):])
                if not match:
                    continue
                target = staging / SERVICES_DIRNAME / f"{match.group(1)}.json"
                if target.exists():
                    raise ReasonCodeDocError(
                        f"two objects under {listed or 'the bucket root'} are "
                        f"both {target.name}")
                body = client.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
                target.write_bytes(body)
                count += 1

        if count == 0:
            logger.warning("Reason-code document download found no service files; "
                           "keeping the files on disk", bucket=bucket, prefix=prefix)
            return False

        errors, warnings = validate(root=staging)
        for warning in warnings:
            logger.warning("Downloaded reason-code documentation warning",
                           detail=warning)
        if errors:
            logger.error("Downloaded reason-code documentation failed validation; "
                         "keeping the files on disk", errors=errors)
            return False

        _swap_in(staging / SERVICES_DIRNAME, root)
        logger.info("Reason-code documentation downloaded", files=count,
                    bucket=bucket, prefix=prefix, directory=str(root))
        return True
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _swap_in(new_services, root) -> None:
    """Put a validated `services/` in place, keeping the old one until the new
    one is there. Two renames on one filesystem: a lookup between them finds
    no files and reads as a miss, which `lookup` already survives."""
    import shutil

    root.mkdir(parents=True, exist_ok=True)
    current = root / SERVICES_DIRNAME
    previous = root / f".{SERVICES_DIRNAME}.previous"
    if previous.exists():
        shutil.rmtree(previous)
    if current.exists():
        current.rename(previous)
    try:
        new_services.rename(current)
    except Exception:
        if previous.exists() and not current.exists():
            previous.rename(current)
        raise
    shutil.rmtree(previous, ignore_errors=True)


def docs_available() -> bool:
    """Whether /ready may pass as far as this store is concerned.

    Always, unless the download is on and there is no copy on disk yet. A
    copy already there -- from an earlier run or a refresh -- serves while a
    new one is fetched, so a refresh never makes a ready pod unready.
    """
    if not s3_download_enabled():
        return True
    try:
        return bool(_service_files(_root()))
    except ReasonCodeDocError:
        return False


def start_background_download():
    """Fetch the store now, off the caller's thread, and again every
    REASON_CODE_DOCS_REFRESH_SECONDS when that is set. Returns the thread, or
    None when the download is off."""
    import time

    if not s3_download_enabled():
        return None

    def run():
        download_service_docs()
        while refresh_seconds() > 0:
            time.sleep(refresh_seconds())
            download_service_docs()

    thread = threading.Thread(target=run, name="reason-code-docs-download",
                              daemon=True)
    thread.start()
    return thread


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

def normalize_doc_type(raw, families: Optional[dict] = None) -> Optional[str]:
    """The enrolment-type family a raw payload value belongs to (D4).

    `families` is a service pack's own map ({RAW OR ALIAS: family}, keys
    upper-case) and is consulted first. Without it, or for a value it does not
    name: `N`, `E`, `ENROLMENT` and `ENROLLMENT` give `E`; `U` and `UPDATE`
    give `U`; another value that looks like a type code is used as-is (`Z`);
    anything else, including a missing value, gives None and can therefore
    only match ANY.
    """
    if raw is None:
        return None
    text = str(raw).strip().upper()
    if not text:
        return None
    if families and text in families:
        return families[text]

    # Imported here rather than at module scope: tool_registry pulls in the
    # DB layer, the log pipeline and the LangChain tool decorators, and this
    # module is imported by the config validator and by a CLI.
    from src.tools.tool_registry import normalize_enrolment_type

    known = normalize_enrolment_type(text)
    if known:
        return _TYPE_FAMILY[known]
    return text if _RAW_TYPE_PATTERN.match(text) else None


def _rule_enrolment_type(condition_description: str,
                         families: Optional[dict] = None) -> Optional[str]:
    """The type a CRE rule applies to, or None when it applies to every type.

    A condition that somehow names two different types is treated as naming
    none: including it everywhere is the safe reading, because the rule
    demonstrably fires for more than one.
    """
    found = {normalize_doc_type(name, families)
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


#: Parsed service files, {path: ((mtime, size), document)}. Every lookup used
#: to re-read and re-parse every file, which with one file per service is a
#: dozen parses per packet. Keyed on the file's mtime and size, so an edited
#: or replaced file is parsed again -- the store keeps the property that an
#: edit takes effect without a restart (MULTI_SERVICE_PLAN.md D10). Callers
#: only read the documents; nothing may mutate one.
_parsed_cache: dict = {}
_parsed_lock = threading.Lock()


def _parsed(path) -> dict:
    try:
        stat = path.stat()
    except OSError as error:
        raise ReasonCodeDocError(f"{path.name} could not be read: {error}") from error
    signature = (stat.st_mtime_ns, stat.st_size)
    key = str(path)
    with _parsed_lock:
        cached = _parsed_cache.get(key)
    if cached is not None and cached[0] == signature:
        return cached[1]
    document = _load_service_file(path)
    with _parsed_lock:
        _parsed_cache[key] = (signature, document)
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
        # The reason code when there is no numeric one: 18 of the shipped
        # enu-biometric codes have `numeric_code: null`, and `[Source: ...,
        # code -]` names nothing a reader could look up.
        "ref": str(numeric) if numeric is not None else str(code.get("reason_code") or "-"),
        "source": source,
        "enrolment_type": None,
        "body": "\n".join(body).rstrip(),
    }


def _rule_entry(source: str, rule: dict, families: Optional[dict] = None) -> dict:
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

    enrolment_type = _rule_enrolment_type(condition, families)
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


def _file_entries(path, reason_code: str, families: Optional[dict] = None) -> list:
    """The documentation blocks one service file publishes for this code."""
    document = _parsed(path)
    source = f"{SERVICES_DIRNAME}/{path.name}"
    entries = []
    for code in document.get("codes") or []:
        if isinstance(code, dict) and code.get("reason_code") == reason_code:
            entries.append(_code_entry(document, source, code))
    rules = document.get("rules") or {}
    for rule in (rules.get("rules") or []) if isinstance(rules, dict) else []:
        if isinstance(rule, dict) and rule.get("reject_reason_code") == reason_code:
            entries.append(_rule_entry(source, rule, families))
    return entries


# ======================================================================
# Which services document a code (MULTI_SERVICE_PLAN.md D1)
# ======================================================================
#
# The last-resort way of telling which service a packet belongs to: a reason
# code that exactly one service's file documents. A file is named after its
# service, so the answer is the file's stem.

#: Store root -> (signature, {reason_code: stems}). The signature is every
#: service file's (name, mtime, size), so an edited, added or removed file
#: rebuilds the index while an unchanged store is not re-parsed per packet.
_code_index_cache: dict = {}
_code_index_lock = threading.Lock()


def _codes_in(document: dict) -> set:
    """Every reason code a service file publishes, from codes[] and rules[]."""
    codes = set()
    for code in document.get("codes") or []:
        if isinstance(code, dict) and isinstance(code.get("reason_code"), str):
            codes.add(code["reason_code"])
    rules = document.get("rules") or {}
    for rule in (rules.get("rules") or []) if isinstance(rules, dict) else []:
        if isinstance(rule, dict) and isinstance(rule.get("reject_reason_code"), str):
            codes.add(rule["reject_reason_code"])
    return codes


def _code_index(root) -> dict:
    files = _service_files(root)
    signature = tuple((path.name, path.stat().st_mtime_ns, path.stat().st_size)
                      for path in files)
    key = str(root)
    with _code_index_lock:
        cached = _code_index_cache.get(key)
    if cached is not None and cached[0] == signature:
        return cached[1]

    index: dict = {}
    for path in files:
        for code in _codes_in(_parsed(path)):
            index.setdefault(code, set()).add(path.stem)
    frozen = {code: tuple(sorted(stems)) for code, stems in index.items()}
    with _code_index_lock:
        _code_index_cache[key] = (signature, frozen)
    return frozen


def services_for_code(reason_code, root=None) -> tuple:
    """The stems of the service files that document `reason_code`, sorted.

    Never raises, for the same reason `lookup` does not: an unreadable store
    costs the caller one way of resolving a packet's service, never the
    packet. Any failure is logged and reads as "no service documents it".
    """
    if not reason_code:
        return ()
    try:
        return _code_index(_root(root)).get(str(reason_code), ())
    except Exception as error:  # noqa: BLE001 - the contract is "never raises"
        logger.warning("Could not index the reason-code documentation by code",
                       error=f"{type(error).__name__}: {error}")
        return ()


def documented_services(root=None) -> tuple:
    """The stem of every service file in the store, sorted. Never raises."""
    try:
        return tuple(path.stem for path in _service_files(_root(root)))
    except Exception as error:  # noqa: BLE001 - the contract is "never raises"
        logger.warning("Could not list the reason-code documentation files",
                       error=f"{type(error).__name__}: {error}")
        return ()


# ======================================================================
# Rendering
# ======================================================================

#: Said above entries taken from other services' files, because the packet's
#: own service documents nothing for its code (MULTI_SERVICE_PLAN.md D10).
OTHER_SERVICE_NOTE = (
    "[Not documented by {service}. The entries below come from other "
    "services' documentation: the code may be raised from a shared library "
    "those services use, or this packet may have been placed in the wrong "
    "service. Weigh them with that in mind.]")


def _type_label(matched_type: str, type_labels: Optional[dict]) -> str:
    if matched_type != ANY and type_labels and type_labels.get(matched_type):
        return type_labels[matched_type]
    return _TYPE_DISPLAY.get(matched_type, f"enrolment type {matched_type}")


def _render(reason_code: str, matched_type: str, entries: list,
            type_labels: Optional[dict] = None, note: Optional[str] = None):
    """The exact text the model is shown, and whether it had to be cut.

    Each block is attributed, so a claim in an investigation can be traced to
    the service file and entry it came from. `type_labels` are the packet's
    service pack's words for its enrolment types; `note` opens the text when
    the entries are not the packet's own service's.
    """
    header = f"# {reason_code} -- {_type_label(matched_type, type_labels)}"
    blocks = [header] + ([note] if note else [])
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
        "entries_sha256": None,
        "truncated": False,
        "resolution_guidance": [],
        "detail": detail,
        "scope": None,
    }


#: Where a hit's entries came from (MULTI_SERVICE_PLAN.md D10): the packet's
#: own service's file, other services' files because its own documents
#: nothing for the code, or every file because no service was named.
SCOPE_OWN = "own"
SCOPE_OTHER_SERVICE = "other_service"
SCOPE_ALL = "all"


def error_state(reason_code=None, detail=None) -> dict:
    """The 5.7 shape for a failure a caller detected rather than `lookup` did.

    `lookup` never raises, so the Investigator's guard around it exists only
    against a bug in this module. It still needs a complete state to store,
    because a half-filled one makes a provenance record that differs from
    every other packet's.
    """
    return _state("error", reason_code, None, detail)


def lookup(reason_code, raw_enrolment_type, root=None, service_file=None,
           type_labels=None, type_families=None) -> dict:
    """The documentation for one packet, as the state shape in 5.7.

    `service_file` is the stem of the packet's own service's file
    (MULTI_SERVICE_PLAN.md D10). Its entries are used when it has any for the
    code; only when it has none are the other files' entries used, under a
    note saying whose they are. Without a `service_file`, every file's
    entries are used together, as before services were known. `type_labels`
    and `type_families` are the service pack's enrolment-type words and
    families; without them the neutral defaults apply.

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

        requested_type = normalize_doc_type(raw_enrolment_type, type_families)
        files = _service_files(_root(root))
        note = None
        if service_file is None:
            scope = SCOPE_ALL
            entries = [entry for path in files
                       for entry in _file_entries(path, reason_code)]
        else:
            scope = SCOPE_OWN
            entries = [entry for path in files if path.stem == service_file
                       for entry in _file_entries(path, reason_code, type_families)]
            if not entries:
                # The service's own file says nothing about this code: not
                # "documented, but not for this enrolment type", which stays
                # a miss below.
                scope = SCOPE_OTHER_SERVICE
                note = OTHER_SERVICE_NOTE.format(service=service_file)
                entries = [entry for path in files if path.stem != service_file
                           for entry in _file_entries(path, reason_code)]
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

        text, truncated = _render(reason_code, matched_type, selected,
                                  type_labels=type_labels, note=note)
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
            "entries_sha256": entries_digest(selected),
            "truncated": truncated,
            "resolution_guidance": parse_resolution_guidance(text),
            "detail": None,
            "scope": scope,
        }
    except Exception as error:  # noqa: BLE001 - the contract is "never raises"
        return _state("error", reason_code if reason_code else None,
                      requested_type, f"{type(error).__name__}: {error}")


def entries_digest(entries: list) -> str:
    """SHA256 over the selected entries themselves: where each came from and
    what it says, not the rendered text around them.

    What a runbook of a service with no rules table is bound to
    (MULTI_SERVICE_PLAN.md D11). The rendered text also carries the title
    line, whose enrolment-type label comes from the service pack, and the
    note heading another service's entries; hashing that would make every
    runbook stale whenever a pack's label was reworded.
    """
    import hashlib

    canonical = json.dumps(
        [{"source": e["source"], "kind": e["kind"], "ref": e["ref"],
          "enrolment_type": e["enrolment_type"], "body": e["body"]}
         for e in entries],
        sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
        # own, other_service or all: whether the text was this packet's own
        # service's documentation (MULTI_SERVICE_PLAN.md D10).
        "scope": doc_state.get("scope"),
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

    services, published, by_file, skipped = {}, {}, [], set()
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
            # The file name is how a packet's service finds its own
            # documentation (MULTI_SERVICE_PLAN.md D10). A file named after
            # anything else would be read as another service's.
            if service != path.stem:
                errors.append(f"{path.name}: 'service' is {service!r}, but a "
                              f"file must be named after its service "
                              f"({service}.json).")
        by_file.append((path, entries))
        # Per file: a runbook is checked against its own service's
        # documentation, not against every service's.
        published[path.stem] = set(entries)

    # Render every reason code each file publishes, for every type it could be
    # asked about, and check the text a model would actually be shown: the
    # file's own entries, as a packet of that service sees them. A file whose
    # fields are individually fine can still render a document that is over
    # the cap or that carries a date.
    for path, entries in by_file:
        for reason_code, types in sorted(entries.items()):
            candidates = sorted(t for t in types if t) or [ANY]
            for requested in candidates:
                state = lookup(reason_code, requested, root=store,
                               service_file=path.stem)
                label = f"{path.name} {reason_code} [{requested}]"
                if state["outcome"] == "error":
                    errors.append(f"{label}: {state['detail']}")
                    continue
                if state["outcome"] != "hit":
                    errors.append(f"{label}: the store publishes this reason "
                                  f"code but the lookup returned "
                                  f"{state['outcome']!r}.")
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
    `published` is {file stem: reason codes it documents}; each service's
    runbooks are checked against that service's own file
    (MULTI_SERVICE_PLAN.md D10, D11).
    """
    from src.utils import runbook_store, service_registry

    registry = service_registry.load()
    for directory in (runbook_store.RUNBOOK_FINAL_DIR, runbook_store.RUNBOOK_DRAFT_DIR):
        if not directory.is_dir():
            continue
        for path in runbook_store._service_runbooks(directory):
            service = runbook_store.service_of_path(path)
            found = registry.packs.get(service)
            stem = found.reason_code_docs_file if found else service
            reason_code = path.stem.rsplit("__", 1)[0]
            if reason_code not in published.get(stem, ()):
                warnings.append(f"{reason_code} has a runbook ({directory.name}/"
                                f"{service}/{path.name}) but no documentation "
                                f"in {SERVICES_DIRNAME}/{stem}.json.")
