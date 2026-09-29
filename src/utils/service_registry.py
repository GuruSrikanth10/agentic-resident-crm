"""
The service registry: which services the rejection lane knows, and which one a
packet belongs to (MULTI_SERVICE_PLAN.md D1, D2, D5 and section 5).

Every service publishes its rejection events to one topic, in one structure.
Nothing in that structure names the service outright, and every later choice
-- policy, tools, documentation, rule source, runbooks -- depends on knowing
it. So the service is resolved once per packet, here, by rules a person wrote
down, and never by a model:

1. `flowMetaData.stage` (and `subStage`, for a service that lists sub-stages)
   against each pack's `match.stages` / `match.sub_stages`;
2. otherwise `sourceTopic`, against `match.source_topics`;
3. otherwise the reason code, when exactly one service's reason-code
   documentation file documents it;
4. otherwise unresolved.

The stage comes first because it is the producer's own statement of where the
packet failed. A reason code can be raised from a shared library, so the
documentation is only a fallback -- and when it names a different service
than the stage did, the stage still wins and the disagreement is recorded as
a `conflict`, never silently resolved.

The registry is one directory per service under SERVICE_PACKS_DIR, each with a
`service.json` (see `src/service_packs/README.md`). It is loaded once per
process and kept: a pack is configuration that ships with a deploy, and
re-reading it per packet would let a half-edited file on a mounted volume
change which service packets belong to while they are in flight. A pack whose
file has errors is left out of the registry entirely rather than half-used;
`validate()`, which main_api runs at boot, is what makes such a pack stop the
API instead of quietly shrinking the registry.

The gate (D5) decides what happens to a packet whose service is unresolved,
unregistered, or neither enabled nor piloted. With
REJECTION_SERVICE_GATE=record (the default) the decision is only recorded;
with `enforce` the packet is acknowledged without analysis. A pilot service
(REJECTION_SERVICES_PILOT, Phase 7) is let through like an enabled one.
"""
import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Optional

from src.utils import paths
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

SCHEMA_VERSION = 1
SERVICE_FILE = "service.json"

#: The pack used for unresolved packets when REJECTION_UNRESOLVED_SERVICE
#: allows them through. It is registered but never matched, and never a
#: service a packet resolves to.
DEFAULT_PACK = "_default"

#: The service of a packet no rule could place.
UNRESOLVED = "_unresolved"

#: In a tool's `services`, every service: the tool's meaning holds whatever
#: the packet's service (MULTI_SERVICE_PLAN.md D7).
ALL_SERVICES = "*"

#: The slug that names the `_default` pack's opencode agents
#: (`crm_<role>__default`). No service may take it.
DEFAULT_SLUG = "default"

#: The pack every packet was analysed with before packs existed. In `record`
#: mode a packet the gate would skip is analysed with it -- exactly as it was
#: before -- so that `record` changes how nothing is analysed, only what is
#: recorded. MULTI_SERVICE_PLAN.md D5.
PRE_REGISTRY_PACK = "enu-biometric"

#: A pack's prompt text (MULTI_SERVICE_PLAN.md 5.1, 5.3). `policy.md` is
#: required; a role file is added to that role's system prompt when present;
#: `learned_rules.md` to the Investigator's.
POLICY_FILE = "policy.md"
LEARNED_RULES_FILE = "learned_rules.md"
ROLE_FILES = {"investigator": "investigator.md", "reviewer": "reviewer.md",
              "synthesis": "synthesis.md"}
#: What the DLT lane's three agents are told about this service
#: (MULTI_SERVICE_PLAN.md Phase 8). Optional; without it their prompts are
#: the generic ones. `policy.md` is not given to them: it is written for
#: business-rule rejections, and a dead-lettered record is a code failure.
DLT_FILE = "dlt.md"
PACK_TEXT_FILES = (POLICY_FILE, *ROLE_FILES.values(), LEARNED_RULES_FILE, DLT_FILE)

ENV_PACK_MAX_CHARS = "SERVICE_PACK_MAX_CHARS"
DEFAULT_PACK_MAX_CHARS = 20000

#: Where the fetch stage records its resolution, beside `fetched_logs.txt`,
#: so the analysis stage reads the same answer rather than re-deriving it.
RESOLUTION_ARTIFACT = "service_resolution.json"

#: How a service was resolved.
SOURCE_FLOW_STAGE = "flow_stage"
SOURCE_SOURCE_TOPIC = "source_topic"
SOURCE_REASON_CODE_DOCS = "reason_code_docs"
SOURCE_NONE = "none"
#: How a dead-lettered record's service was resolved, beyond the above
#: (MULTI_SERVICE_PLAN.md Phase 8, from the pack's reserved `dlt` fields).
SOURCE_CONSUMER_GROUP = "consumer_group"
SOURCE_ORIGINAL_TOPIC = "original_topic"
SOURCE_JAVA_PACKAGE = "java_package"

#: Why the gate would skip a packet.
SKIP_UNRESOLVED = "service_unresolved"
SKIP_NOT_REGISTERED = "service_not_registered"
SKIP_NOT_ENABLED = "service_not_enabled"

GATE_RECORD = "record"
GATE_ENFORCE = "enforce"
GATE_MODES = (GATE_RECORD, GATE_ENFORCE)

UNRESOLVED_SKIP = "skip"
UNRESOLVED_DEFAULT_PACK = "default_pack"
UNRESOLVED_POLICIES = (UNRESOLVED_SKIP, UNRESOLVED_DEFAULT_PACK)

#: Used when REJECTION_SERVICES_ENABLED is unset or blank. Blank does not mean
#: "nothing": `.env.example` ships keys blank so operators can see them, and a
#: copied blank value must not switch every service off under `enforce`.
DEFAULT_ENABLED = ("enu-biometric",)

#: Where a pack's rules are (D6): the rules table, or only its documentation.
RULES_DB = "rules_db"
NO_RULES_DB = "none"
RULE_SOURCE_TYPES = (RULES_DB, NO_RULES_DB)

ENV_GATE = "REJECTION_SERVICE_GATE"
ENV_ENABLED = "REJECTION_SERVICES_ENABLED"
ENV_PILOT = "REJECTION_SERVICES_PILOT"
ENV_UNRESOLVED = "REJECTION_UNRESOLVED_SERVICE"
#: The DLT lane's own gate (MULTI_SERVICE_PLAN.md Phase 8). Its own settings,
#: because a service's rejections and its crashes are onboarded apart: the
#: DLT lane needs the pack's `dlt` fields, and its source mappings, first.
ENV_DLT_GATE = "DLT_SERVICE_GATE"
ENV_DLT_ENABLED = "DLT_SERVICES_ENABLED"

#: A service name is also a directory name, a documentation file stem and a
#: metric label value, so it is kept to what all of them accept.
_SERVICE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_TOOL_PREFIX = re.compile(r"^[a-z][a-z0-9]{1,11}$")
_DIR_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SLUG_UNSAFE = re.compile(r"[^a-z0-9]")
#: A Java package prefix: dotted identifiers, e.g. `com.uidai.enu.biometric`.
_JAVA_PACKAGE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

_FILE_KEYS = {"schema_version", "service", "display_name", "tool_prefix",
              "match", "enrolment_types", "rule_source",
              "reason_code_docs_file", "droa_corpus_dir", "logs", "tools",
              "dlt"}
_REQUIRED_KEYS = ("schema_version", "service", "display_name", "match",
                  "rule_source")
_MATCH_KEYS = {"stages", "sub_stages", "source_topics"}
_ENROLMENT_KEYS = {"payload", "family_labels", "doc_aliases"}
_ENROLMENT_ENTRY_KEYS = {"family", "label"}
_RULE_SOURCE_KEYS = {"type", "enrolment_type_filter"}
_LOGS_KEYS = {"app_names", "k8s_match", "also_search", "decision_vocabulary"}
_K8S_MATCH_KEYS = {"name_contains", "label_selector"}
_TOOLS_KEYS = {"include", "exclude"}
_DLT_KEYS = {"consumer_groups", "original_topics", "java_packages"}


# ======================================================================
# Packs and the registry
# ======================================================================

@dataclass(frozen=True)
class ServicePack:
    """One parsed, valid `service.json`."""

    name: str
    display_name: str
    tool_prefix: Optional[str]
    #: Lower-cased, so matching is case-insensitive.
    stages: tuple
    sub_stages: tuple
    #: Compiled; matched against the whole `sourceTopic`.
    source_topics: tuple
    rule_source_type: str
    reason_code_docs_file: str
    droa_corpus_dir: str
    #: The whole parsed file. The later phases read their sections from it.
    document: dict = field(compare=False, repr=False)
    #: The pack's prompt text, read and validated with the rest of the pack:
    #: {file name: text} for each PACK_TEXT_FILES entry that exists.
    texts: dict = field(default_factory=dict, compare=False, repr=False)
    #: Digest of service.json and every text file, recorded with each
    #: casebook so it says which version of the pack its agents were shown.
    sha256: str = ""
    #: The `dlt` section (MULTI_SERVICE_PLAN.md D15, read from Phase 8):
    #: consumer groups compared exactly, original topics compiled and matched
    #: against the whole topic, Java package prefixes.
    consumer_groups: tuple = ()
    original_topics: tuple = ()
    java_packages: tuple = ()

    def matches_stage(self, stage: str, sub_stage: Optional[str]) -> bool:
        if stage not in self.stages:
            return False
        return not self.sub_stages or (sub_stage is not None
                                       and sub_stage in self.sub_stages)

    def matches_topic(self, topic: str) -> bool:
        return any(pattern.fullmatch(topic) for pattern in self.source_topics)

    def matches_original_topic(self, topic: str) -> bool:
        return any(pattern.fullmatch(topic) for pattern in self.original_topics)

    def package_match(self, frame: str) -> int:
        """The length of the longest of this pack's Java packages `frame`
        (a fully qualified method) sits in, or 0."""
        return max((len(package) for package in self.java_packages
                    if frame == package or frame.startswith(package + ".")),
                   default=0)


@dataclass(frozen=True)
class Registry:
    root: Path
    #: Name -> ServicePack, `_default` included. Packs with errors are absent.
    packs: dict
    errors: tuple = ()
    warnings: tuple = ()
    #: Digest of every service.json read, recorded with each resolution so a
    #: casebook says which version of the registry placed it.
    sha256: str = ""

    def services(self) -> tuple:
        """Every registered service a packet can belong to, sorted."""
        return tuple(sorted(name for name in self.packs if name != DEFAULT_PACK))

    def is_registered(self, name: Optional[str]) -> bool:
        return bool(name) and name != DEFAULT_PACK and name in self.packs

    def by_stage(self, stage: str, sub_stage: Optional[str]) -> list:
        return [name for name in self.services()
                if self.packs[name].matches_stage(stage, sub_stage)]

    def by_topic(self, topic: str) -> list:
        return [name for name in self.services()
                if self.packs[name].matches_topic(topic)]

    def by_consumer_group(self, group: str) -> list:
        return [name for name in self.services()
                if group in self.packs[name].consumer_groups]

    def by_original_topic(self, topic: str) -> list:
        return [name for name in self.services()
                if self.packs[name].matches_original_topic(topic)]

    def by_java_package(self, frames) -> tuple:
        """(services, frame): the services owning the first application frame
        any pack's Java packages claim, by longest prefix. The frames are the
        failure site's, innermost first, so the first claimed frame is the
        code that failed rather than a caller of it."""
        for frame in frames or ():
            lengths = {name: self.packs[name].package_match(str(frame))
                       for name in self.services()}
            best = max(lengths.values(), default=0)
            if best:
                return [name for name, length in lengths.items() if length == best], frame
        return [], None

    def service_for_docs_file(self, stem: str) -> str:
        """The service whose documentation file is `stem`.

        A file with no pack still names a service -- its file is named after
        it -- so the stem itself is returned, and the gate then reports that
        service as unregistered rather than the packet as unresolved.
        """
        for name in self.services():
            if self.packs[name].reason_code_docs_file == stem:
                return name
        return stem


_cache: dict = {}
_cache_lock = threading.Lock()


def packs_dir(root=None) -> Path:
    """The registry root, read through `paths` at call time so tests can patch
    `paths.SERVICE_PACKS_DIR`."""
    return Path(root) if root is not None else paths.SERVICE_PACKS_DIR


def load(root=None) -> Registry:
    """The registry for `root`, built on first use and kept. Never raises."""
    directory = packs_dir(root)
    key = str(directory)
    with _cache_lock:
        registry = _cache.get(key)
        if registry is None:
            registry = _build(directory)
            _cache[key] = registry
            if registry.errors:
                logger.error("The service registry has errors; the packs they "
                             "belong to are left out", directory=key,
                             errors=list(registry.errors))
    return registry


def reset() -> None:
    """Forget every loaded registry. For tests."""
    with _cache_lock:
        _cache.clear()


def _build(directory: Path) -> Registry:
    errors, warnings, packs, texts = [], [], {}, []

    if not directory.is_dir():
        errors.append(f"The service pack directory {directory} does not exist.")
        return Registry(root=directory, packs={}, errors=tuple(errors),
                        sha256=_digest(texts))

    for entry in sorted(directory.iterdir(), key=lambda path: path.name):
        if not entry.is_dir() or entry.name.startswith((".", "__")):
            continue
        service_file = entry / SERVICE_FILE
        if not service_file.is_file():
            errors.append(f"{entry.name}/: has no {SERVICE_FILE}.")
            continue
        label = f"{entry.name}/{SERVICE_FILE}"
        try:
            text = service_file.read_text(encoding="utf-8")
            document = json.loads(text)
        except UnicodeDecodeError as error:
            errors.append(f"{label} is not UTF-8: {error}")
            continue
        except ValueError as error:
            errors.append(f"{label} is not valid JSON: {error}")
            continue
        except OSError as error:
            errors.append(f"{label} could not be read: {error}")
            continue
        texts.append((entry.name, text))

        before = len(errors)
        pack = _parse_pack(entry.name, label, document, errors, warnings)
        pack_texts = _read_pack_texts(entry, errors)
        if pack is not None and len(errors) == before:
            packs[pack.name] = replace(
                pack, texts=pack_texts,
                sha256=_digest([(SERVICE_FILE, text), *pack_texts.items()]))

    _check_registry(packs, errors)
    return Registry(root=directory, packs=packs, errors=tuple(errors),
                    warnings=tuple(warnings), sha256=_digest(texts))


def _digest(texts: list) -> str:
    digest = hashlib.sha256()
    for name, text in sorted(texts):
        encoded = text.encode("utf-8")
        digest.update(name.encode("utf-8"))
        digest.update(str(len(encoded)).encode("utf-8"))
        digest.update(encoded)
    return "sha256:" + digest.hexdigest()


# ======================================================================
# Parsing one service.json
# ======================================================================

def _check_keys(label: str, value, allowed: set, errors: list) -> bool:
    if not isinstance(value, dict):
        errors.append(f"{label} is a {type(value).__name__}, not an object.")
        return False
    unknown = sorted(set(value) - allowed)
    if unknown:
        errors.append(f"{label} has unknown key(s) {unknown}; expected a "
                      f"subset of {sorted(allowed)}.")
    return True


def _string_list(label: str, value, errors: list) -> list:
    """A list of non-empty strings, or [] after reporting what is wrong."""
    if value is None:
        return []
    if not isinstance(value, list) or not all(
            isinstance(item, str) and item.strip() for item in value):
        errors.append(f"{label} must be a list of non-empty strings.")
        return []
    return [item.strip() for item in value]


def _string_map(label: str, value, errors: list) -> None:
    if value is None:
        return
    if not isinstance(value, dict) or not all(
            isinstance(key, str) and key.strip() and isinstance(item, str)
            and item.strip() for key, item in value.items()):
        errors.append(f"{label} must map non-empty strings to non-empty strings.")


def _optional_name(label: str, value, pattern, default: str,
                   errors: list) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or not pattern.match(value):
        errors.append(f"{label} must match {pattern.pattern}; got {value!r}.")
        return default
    return value


def _parse_pack(dir_name: str, label: str, document, errors: list,
                warnings: list) -> Optional[ServicePack]:
    if not _check_keys(label, document, _FILE_KEYS, errors):
        return None
    missing = [key for key in _REQUIRED_KEYS if key not in document]
    if missing:
        errors.append(f"{label} is missing required key(s) {missing}.")
        return None

    if document.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"{label}: schema_version is "
                      f"{document.get('schema_version')!r}, expected "
                      f"{SCHEMA_VERSION}.")

    name = document.get("service")
    if name != dir_name:
        errors.append(f"{label}: 'service' is {name!r}, but a pack's service "
                      f"must equal its directory name, {dir_name!r}.")
        return None
    is_default = name == DEFAULT_PACK
    if not is_default and not _SERVICE_NAME.match(name):
        errors.append(f"{label}: service name {name!r} must match "
                      f"{_SERVICE_NAME.pattern}.")
    elif not is_default and service_slug(name) == DEFAULT_SLUG:
        errors.append(f"{label}: service name {name!r} is reserved: its opencode "
                      f"agents would be the unresolved packets' "
                      f"(crm_<role>__{DEFAULT_SLUG}).")

    display_name = document.get("display_name")
    if not isinstance(display_name, str) or not display_name.strip():
        errors.append(f"{label}: 'display_name' must be a non-empty string.")
        display_name = name

    tool_prefix = document.get("tool_prefix")
    if is_default:
        if tool_prefix is not None:
            errors.append(f"{label}: the default pack serves no tools of its "
                          f"own, so it takes no 'tool_prefix'.")
    elif not isinstance(tool_prefix, str) or not _TOOL_PREFIX.match(tool_prefix):
        errors.append(f"{label}: 'tool_prefix' is required and must match "
                      f"{_TOOL_PREFIX.pattern}; got {tool_prefix!r}.")

    stages, sub_stages, topics = _parse_match(label, document.get("match"),
                                              errors)
    if is_default and (stages or sub_stages or topics):
        errors.append(f"{label}: the default pack must match nothing; it is "
                      f"used only for packets no service matched.")
    elif not is_default and not stages and not topics:
        warnings.append(f"{label}: no match.stages and no "
                        f"match.source_topics, so {name} can be resolved only "
                        f"from a reason code its documentation file alone "
                        f"documents.")

    _parse_enrolment_types(label, document.get("enrolment_types"), errors)
    rule_source_type = _parse_rule_source(label, document.get("rule_source"),
                                          errors)
    _parse_logs(label, document.get("logs"), errors)
    _parse_lists(f"{label} tools", document.get("tools"), _TOOLS_KEYS, errors)
    if is_default and isinstance(document.get("tools"), dict) \
            and document["tools"].get("include"):
        # An unresolved packet gets the tools every service may use and no
        # other (MULTI_SERVICE_PLAN.md D7): which service's tool would fit a
        # packet nobody could place is exactly what is not known.
        errors.append(f"{label}: the default pack takes no tools.include; an "
                      f"unresolved packet gets only the tools for every "
                      f"service ({ALL_SERVICES!r}).")
    consumer_groups, original_topics, java_packages = _parse_dlt(
        label, document.get("dlt"), errors)
    if is_default and (consumer_groups or original_topics or java_packages):
        errors.append(f"{label}: the default pack must match nothing, so its "
                      f"dlt section must be empty.")

    return ServicePack(
        name=name,
        display_name=display_name.strip(),
        tool_prefix=None if is_default else tool_prefix,
        stages=tuple(stage.lower() for stage in stages),
        sub_stages=tuple(stage.lower() for stage in sub_stages),
        source_topics=tuple(topics),
        rule_source_type=rule_source_type,
        reason_code_docs_file=_optional_name(
            f"{label}: 'reason_code_docs_file'",
            document.get("reason_code_docs_file"), _SERVICE_NAME, name, errors),
        droa_corpus_dir=_optional_name(
            f"{label}: 'droa_corpus_dir'", document.get("droa_corpus_dir"),
            _DIR_NAME, name, errors),
        document=document,
        consumer_groups=tuple(consumer_groups),
        original_topics=tuple(original_topics),
        java_packages=tuple(java_packages),
    )


def _parse_dlt(label: str, value, errors: list) -> tuple:
    """(consumer groups, compiled original topics, Java packages) from the
    `dlt` section, reporting what is wrong with it."""
    where = f"{label} dlt"
    if value is None:
        return [], [], []
    if not _check_keys(where, value, _DLT_KEYS, errors):
        return [], [], []
    groups = _string_list(f"{where}.consumer_groups", value.get("consumer_groups"), errors)
    topics = []
    for pattern in _string_list(f"{where}.original_topics",
                                value.get("original_topics"), errors):
        try:
            topics.append(re.compile(pattern))
        except re.error as error:
            errors.append(f"{where}.original_topics entry {pattern!r} is not a "
                          f"valid regular expression: {error}")
    packages = []
    for package in _string_list(f"{where}.java_packages",
                                value.get("java_packages"), errors):
        if _JAVA_PACKAGE.match(package):
            packages.append(package)
        else:
            errors.append(f"{where}.java_packages entry {package!r} is not a "
                          f"Java package name.")
    return groups, topics, packages


def _parse_match(label: str, match, errors: list) -> tuple:
    if not _check_keys(f"{label} match", match, _MATCH_KEYS, errors):
        return [], [], []
    stages = _string_list(f"{label} match.stages", match.get("stages"), errors)
    sub_stages = _string_list(f"{label} match.sub_stages",
                              match.get("sub_stages"), errors)
    if sub_stages and not stages:
        errors.append(f"{label}: match.sub_stages narrows match.stages, so it "
                      f"needs at least one stage.")
    topics = []
    for pattern in _string_list(f"{label} match.source_topics",
                                match.get("source_topics"), errors):
        try:
            topics.append(re.compile(pattern))
        except re.error as error:
            errors.append(f"{label}: match.source_topics entry {pattern!r} is "
                          f"not a valid regular expression: {error}")
    return stages, sub_stages, topics


def _parse_enrolment_types(label: str, value, errors: list) -> None:
    if value is None:
        return
    where = f"{label} enrolment_types"
    if not _check_keys(where, value, _ENROLMENT_KEYS, errors):
        return
    payload = value.get("payload")
    if payload is not None:
        if not isinstance(payload, dict):
            errors.append(f"{where}.payload must be an object.")
        else:
            for raw_type, entry in payload.items():
                entry_label = f"{where}.payload[{raw_type!r}]"
                if not _check_keys(entry_label, entry, _ENROLMENT_ENTRY_KEYS,
                                   errors):
                    continue
                for key in sorted(_ENROLMENT_ENTRY_KEYS):
                    item = entry.get(key)
                    if not isinstance(item, str) or not item.strip():
                        errors.append(f"{entry_label}.{key} must be a "
                                      f"non-empty string.")
    _string_map(f"{where}.family_labels", value.get("family_labels"), errors)
    _string_map(f"{where}.doc_aliases", value.get("doc_aliases"), errors)


def _parse_rule_source(label: str, value, errors: list) -> str:
    where = f"{label} rule_source"
    if not _check_keys(where, value, _RULE_SOURCE_KEYS, errors):
        return "none"
    source_type = value.get("type")
    if source_type not in RULE_SOURCE_TYPES:
        errors.append(f"{where}.type must be one of {list(RULE_SOURCE_TYPES)}; "
                      f"got {source_type!r}.")
        return "none"
    if "enrolment_type_filter" in value:
        if source_type != "rules_db":
            errors.append(f"{where}.enrolment_type_filter applies only to "
                          f"type 'rules_db'.")
        _string_map(f"{where}.enrolment_type_filter",
                    value.get("enrolment_type_filter"), errors)
    return source_type


def _parse_logs(label: str, value, errors: list) -> None:
    if value is None:
        return
    where = f"{label} logs"
    if not _check_keys(where, value, _LOGS_KEYS, errors):
        return
    _string_list(f"{where}.app_names", value.get("app_names"), errors)
    _string_list(f"{where}.also_search", value.get("also_search"), errors)
    k8s_match = value.get("k8s_match")
    if k8s_match is not None and _check_keys(f"{where}.k8s_match", k8s_match,
                                             _K8S_MATCH_KEYS, errors):
        if len(k8s_match) > 1:
            errors.append(f"{where}.k8s_match takes one of name_contains or "
                          f"label_selector, not both.")
        for key, item in k8s_match.items():
            if not isinstance(item, str) or not item.strip():
                errors.append(f"{where}.k8s_match.{key} must be a non-empty "
                              f"string.")
    vocabulary = value.get("decision_vocabulary")
    if vocabulary is not None:
        if not isinstance(vocabulary, str) or not vocabulary.strip():
            errors.append(f"{where}.decision_vocabulary must be a non-empty "
                          f"string.")
        else:
            try:
                re.compile(vocabulary)
            except re.error as error:
                errors.append(f"{where}.decision_vocabulary is not a valid "
                              f"regular expression: {error}")


def _parse_lists(where: str, value, allowed: set, errors: list) -> None:
    """A section whose every key holds a list of strings (tools, dlt)."""
    if value is None:
        return
    if not _check_keys(where, value, allowed, errors):
        return
    for key in sorted(set(value) & allowed):
        _string_list(f"{where}.{key}", value.get(key), errors)


# ======================================================================
# A pack's prompt text (MULTI_SERVICE_PLAN.md 5.3)
# ======================================================================

def pack_max_chars() -> int:
    """SERVICE_PACK_MAX_CHARS. An unusable value falls back rather than
    raising -- a typo in a tunable must not stop the pipeline."""
    try:
        value = int(os.environ.get(ENV_PACK_MAX_CHARS, "").strip())
    except ValueError:
        return DEFAULT_PACK_MAX_CHARS
    return value if value > 0 else DEFAULT_PACK_MAX_CHARS


def _read_pack_texts(directory: Path, errors: list) -> dict:
    """Read and check a pack's text files. Every problem is an error.

    The text goes into a system prompt, so it passes the checks the
    reason-code documentation passes: generic -- no packet identifier, date
    or long digit run -- and free of instruction-shaped text.
    """
    from src.utils.runbook_validator import INJECTION_MARKERS, validate_generic_text

    name = directory.name
    texts = {}
    for filename in PACK_TEXT_FILES:
        path = directory / filename
        if not path.is_file():
            continue
        label = f"{name}/{filename}"
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            errors.append(f"{label} could not be read: {error}")
            continue
        texts[filename] = text
        for violation in validate_generic_text(text, []):
            errors.append(f"{label}: {violation}")
        lowered = text.lower()
        for marker in INJECTION_MARKERS:
            if marker in lowered:
                errors.append(f"{label}: contains instruction-shaped text: "
                              f"{marker!r}")

    if not texts.get(POLICY_FILE, "").strip():
        errors.append(f"{name}/{POLICY_FILE} is missing or empty; every pack "
                      f"needs one.")

    limit = pack_max_chars()
    if len(texts.get(DLT_FILE, "")) > limit:
        errors.append(f"{name}/{DLT_FILE} is {len(texts[DLT_FILE])} characters, "
                      f"over {ENV_PACK_MAX_CHARS} ({limit}).")
    shared = len(texts.get(POLICY_FILE, ""))
    for role, filename in ROLE_FILES.items():
        size = shared + len(texts.get(filename, ""))
        if role == "investigator":
            size += len(texts.get(LEARNED_RULES_FILE, ""))
        if size > limit:
            errors.append(f"{name}: the pack text composed for the {role} is "
                          f"{size} characters, over {ENV_PACK_MAX_CHARS} "
                          f"({limit}).")
    return texts


# ======================================================================
# Checks across packs
# ======================================================================

def _check_registry(packs: dict, errors: list) -> None:
    if DEFAULT_PACK not in packs:
        errors.append(f"There is no valid {DEFAULT_PACK}/ pack. It ships with "
                      f"the repository; a SERVICE_PACKS_DIR pointing elsewhere "
                      f"must include it.")

    services = sorted(name for name in packs if name != DEFAULT_PACK)

    # Two services matching one (stage, sub-stage) would make the stage --
    # the strongest evidence there is -- decide nothing.
    for index, first in enumerate(services):
        for second in services[index + 1:]:
            a, b = packs[first], packs[second]
            for stage in sorted(set(a.stages) & set(b.stages)):
                if not a.sub_stages or not b.sub_stages:
                    errors.append(f"{first} and {second} both match stage "
                                  f"{stage!r}, and at least one of them for "
                                  f"every sub-stage.")
                    continue
                shared = sorted(set(a.sub_stages) & set(b.sub_stages))
                if shared:
                    errors.append(f"{first} and {second} both match stage "
                                  f"{stage!r} with sub-stage(s) {shared}.")

    for field_name, describe in (("tool_prefix", "tool prefix"),
                                 ("reason_code_docs_file",
                                  "reason-code documentation file")):
        owners: dict = {}
        for name in services:
            value = getattr(packs[name], field_name)
            owners.setdefault(value, []).append(name)
        for value, names in sorted(owners.items(), key=lambda item: str(item[0])):
            if value is not None and len(names) > 1:
                errors.append(f"Services {names} share the {describe} "
                              f"{value!r}; each must have its own.")

    # A consumer group or a Java package claimed by two services would decide
    # nothing for a dead-lettered record (MULTI_SERVICE_PLAN.md Phase 8).
    # Nested packages are allowed -- the longest prefix decides -- but one
    # package named by two services is not.
    for field_name, describe in (("consumer_groups", "DLT consumer group"),
                                 ("java_packages", "Java package")):
        owners = {}
        for name in services:
            for value in getattr(packs[name], field_name):
                owners.setdefault(value, []).append(name)
        for value, names in sorted(owners.items()):
            if len(names) > 1:
                errors.append(f"Services {names} share the {describe} "
                              f"{value!r}; each must have its own.")

    for name in services:
        logs = packs[name].document.get("logs") or {}
        for other in logs.get("also_search") or []:
            if other == name or other not in services:
                errors.append(f"{name}/{SERVICE_FILE}: logs.also_search names "
                              f"{other!r}, which is not another registered "
                              f"service.")


# ======================================================================
# Resolution
# ======================================================================

@dataclass(frozen=True)
class ServiceResolution:
    service: str
    source: str = SOURCE_NONE
    #: The payload value that decided it: the stage, the topic or the code.
    matched: Optional[str] = None
    #: {"reason_code_docs": <service>} when the documentation named a
    #: different service than the one chosen.
    conflict: Optional[dict] = None
    detail: dict = field(default_factory=dict)
    registry_sha256: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "service": self.service,
            "source": self.source,
            "matched": self.matched,
            "conflict": dict(self.conflict) if self.conflict else None,
            "detail": dict(self.detail),
            "registry_sha256": self.registry_sha256,
        }


def _text(value) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _reason_code_of(payload: dict) -> Optional[str]:
    """The first non-empty `errorReasonCode` -- the same rule the orchestrator
    uses, restated here so resolving a service does not import the graph."""
    summary = payload.get("packetExecutionSummary")
    if not isinstance(summary, dict):
        return None
    for error in summary.get("errorData") or []:
        if isinstance(error, dict):
            code = _text(error.get("errorReasonCode"))
            if code:
                return code
    return None


def resolve(payload, registry: Optional[Registry] = None,
            docs_root=None) -> ServiceResolution:
    """Which service `payload` belongs to, and how that was decided.

    Tolerant of any payload shape: a missing or malformed field is simply
    evidence that is not there.
    """
    registry = registry or load()
    payload = payload if isinstance(payload, dict) else {}
    flow = payload.get("flowMetaData")
    flow = flow if isinstance(flow, dict) else {}

    stage = _text(flow.get("stage"))
    sub_stage = _text(flow.get("subStage"))
    topic = _text(payload.get("sourceTopic"))
    code = _reason_code_of(payload)
    detail = {"stage": stage, "sub_stage": sub_stage, "source_topic": topic,
              "reason_code": code}

    candidate, source, matched = None, SOURCE_NONE, None

    if stage:
        found = registry.by_stage(stage.lower(),
                                  sub_stage.lower() if sub_stage else None)
        if len(found) == 1:
            candidate, source, matched = found[0], SOURCE_FLOW_STAGE, stage
        elif found:
            # Boot validation refuses overlapping stages, so this is a registry
            # that was never validated. Deciding between them would be a guess.
            logger.error("Several services match one stage; the stage decides "
                         "nothing", stage=stage, sub_stage=sub_stage,
                         services=found)

    if candidate is None and topic:
        found = registry.by_topic(topic)
        if len(found) == 1:
            candidate, source, matched = found[0], SOURCE_SOURCE_TOPIC, topic
        elif found:
            # Topic patterns are regular expressions, so overlap cannot be
            # proved at boot. Reported here instead, every time it happens.
            logger.error("Several services' source_topics match one topic; "
                         "the topic decides nothing", source_topic=topic,
                         services=found)

    documented_by = _documented_by(code, registry, docs_root)

    conflict = None
    if candidate is None:
        if len(documented_by) == 1:
            candidate, source, matched = (documented_by[0],
                                          SOURCE_REASON_CODE_DOCS, code)
    elif len(documented_by) == 1 and documented_by[0] != candidate:
        conflict = {"reason_code_docs": documented_by[0]}

    return ServiceResolution(
        service=candidate or UNRESOLVED,
        source=source,
        matched=matched,
        conflict=conflict,
        detail=detail,
        registry_sha256=registry.sha256,
    )


def _documented_by(code: Optional[str], registry: Registry, docs_root=None) -> tuple:
    """The registered services whose documentation file documents `code`."""
    if not code:
        return ()
    from src.utils import reason_code_docs

    return tuple(sorted({
        registry.service_for_docs_file(stem)
        for stem in reason_code_docs.services_for_code(code, root=docs_root)}))


def resolve_dlt(payload, *, consumer_group: Optional[str] = None,
                original_topic: Optional[str] = None, frames=(),
                business_code: Optional[str] = None,
                registry: Optional[Registry] = None,
                docs_root=None) -> ServiceResolution:
    """Which service a dead-lettered record belongs to (MULTI_SERVICE_PLAN.md
    Phase 8, D15), and how that was decided.

    The record's payload is in the rejection lane's contract, so every step
    `resolve` takes is available; the DLT headers and the stack trace add
    three more. In order, the first step that names exactly one service
    decides:

    1. the consumer group that gave up on the record, against each pack's
       `dlt.consumer_groups` -- the failing consumer's own identity, as the
       stage is the rejecting producer's;
    2. `flowMetaData.stage` (and `subStage`), as for a rejection;
    3. the original topic, against `dlt.original_topics`;
    4. the failure site's first application frame claimed by any pack's
       `dlt.java_packages`, longest prefix first;
    5. `sourceTopic`, against `match.source_topics`;
    6. the reason code -- the payload's, else the one the trace carries --
       when exactly one documentation file documents it.

    Every later step that names exactly one other service is recorded in
    `conflict`, keyed by its source, and never overrides the decision.
    """
    registry = registry or load()
    payload = payload if isinstance(payload, dict) else {}
    flow = payload.get("flowMetaData")
    flow = flow if isinstance(flow, dict) else {}

    group = _text(consumer_group)
    origin = _text(original_topic)
    stage = _text(flow.get("stage"))
    sub_stage = _text(flow.get("subStage"))
    topic = _text(payload.get("sourceTopic"))
    code = _reason_code_of(payload) or _text(business_code)
    package_services, package_frame = registry.by_java_package(frames)

    steps = [
        (SOURCE_CONSUMER_GROUP, group,
         registry.by_consumer_group(group) if group else []),
        (SOURCE_FLOW_STAGE, stage,
         registry.by_stage(stage.lower(), sub_stage.lower() if sub_stage else None)
         if stage else []),
        (SOURCE_ORIGINAL_TOPIC, origin,
         registry.by_original_topic(origin) if origin else []),
        (SOURCE_JAVA_PACKAGE, package_frame, package_services),
        (SOURCE_SOURCE_TOPIC, topic, registry.by_topic(topic) if topic else []),
        (SOURCE_REASON_CODE_DOCS, code, list(_documented_by(code, registry, docs_root))),
    ]

    candidate, source, matched, conflict = None, SOURCE_NONE, None, {}
    for step_source, value, found in steps:
        if len(found) > 1 and step_source != SOURCE_REASON_CODE_DOCS:
            logger.error("Several services match one piece of DLT evidence; it "
                         "decides nothing", source=step_source, value=value,
                         services=found)
        if len(found) != 1:
            continue
        if candidate is None:
            candidate, source, matched = found[0], step_source, value
        elif found[0] != candidate:
            conflict[step_source] = found[0]

    return ServiceResolution(
        service=candidate or UNRESOLVED,
        source=source,
        matched=matched,
        conflict=conflict or None,
        detail={"consumer_group": group, "original_topic": origin,
                "stage": stage, "sub_stage": sub_stage, "source_topic": topic,
                "java_frame": package_frame, "reason_code": code},
        registry_sha256=registry.sha256,
    )


# ======================================================================
# The gate (D5)
# ======================================================================

def gate_mode() -> str:
    raw = os.environ.get(ENV_GATE, "").strip().lower()
    return raw if raw in GATE_MODES else GATE_RECORD


def unresolved_policy() -> str:
    raw = os.environ.get(ENV_UNRESOLVED, "").strip().lower()
    return raw if raw in UNRESOLVED_POLICIES else UNRESOLVED_SKIP


def enabled_services() -> frozenset:
    """The services analysed. Read at call time, like every other switch."""
    raw = os.environ.get(ENV_ENABLED, "")
    names = [name.strip() for name in raw.split(",") if name.strip()]
    return frozenset(names or DEFAULT_ENABLED)


def pilot_services() -> frozenset:
    """The services analysed in pilot mode (MULTI_SERVICE_PLAN.md Phase 7):
    analysed like an enabled service, except that their casebooks carry
    `pilot: true` and their Synthesis agent cannot stage a replay. Blank
    means none -- unlike the enabled list, there is no default to fall back
    to."""
    raw = os.environ.get(ENV_PILOT, "")
    return frozenset(name.strip() for name in raw.split(",") if name.strip())


def is_pilot(name: Optional[str]) -> bool:
    """Whether the pack `name` is a pilot service's. A service named in both
    lists is a pilot -- boot validation refuses that setting, and where it
    has not run, the more cautious reading wins."""
    return bool(name) and name in pilot_services()


def analysed_services() -> frozenset:
    """Every service whose packets the gate lets through: enabled or pilot."""
    return enabled_services() | pilot_services()


@dataclass(frozen=True)
class GateDecision:
    #: Whether to acknowledge the packet without analysing it.
    skip: bool
    #: What `enforce` would skip it for, or None. Set in `record` mode too,
    #: which is how that mode is reviewed before it is switched to `enforce`.
    reason: Optional[str]
    mode: str


def skip_reason(resolution: dict, registry: Optional[Registry] = None) -> Optional[str]:
    """Why the gate would skip a packet with this resolution, or None."""
    registry = registry or load()
    service = (resolution or {}).get("service") or UNRESOLVED
    if service == UNRESOLVED:
        if unresolved_policy() == UNRESOLVED_DEFAULT_PACK:
            return None
        return SKIP_UNRESOLVED
    if not registry.is_registered(service):
        return SKIP_NOT_REGISTERED
    if service not in analysed_services():
        return SKIP_NOT_ENABLED
    return None


def gate(resolution: dict, registry: Optional[Registry] = None) -> GateDecision:
    reason = skip_reason(resolution, registry)
    mode = gate_mode()
    return GateDecision(skip=bool(reason) and mode == GATE_ENFORCE,
                        reason=reason, mode=mode)


# ======================================================================
# Which pack a packet is analysed with (MULTI_SERVICE_PLAN.md D4, D5)
# ======================================================================

def pack(name: Optional[str], registry: Optional[Registry] = None) -> Optional[ServicePack]:
    """The named pack, `_default` included, or None."""
    registry = registry or load()
    return registry.packs.get(name) if name else None


def pack_for(resolution: dict, registry: Optional[Registry] = None) -> str:
    """The name of the pack a packet with this resolution is analysed with.

    Decided the way the gate decides. A packet the gate lets through uses its
    own service's pack, or `_default` when it is an unresolved packet the
    `default_pack` policy admitted. A packet the gate would skip is analysed
    at all only in `record` mode, and then with the pack every packet used
    before packs existed -- so `record` stays a mode that changes nothing but
    what is recorded.
    """
    registry = registry or load()
    service = (resolution or {}).get("service") or UNRESOLVED
    if skip_reason(resolution, registry) is None:
        return DEFAULT_PACK if service == UNRESOLVED else service
    if registry.is_registered(PRE_REGISTRY_PACK):
        return PRE_REGISTRY_PACK
    return DEFAULT_PACK


def packs_to_prebuild(registry: Optional[Registry] = None) -> tuple:
    """The packs whose agents are built with the graph rather than on first
    use: every enabled and pilot service's, and in `record` mode the
    pre-registry pack that packets the gate would skip are analysed with."""
    registry = registry or load()
    names = {name for name in analysed_services() if registry.is_registered(name)}
    if gate_mode() == GATE_RECORD and registry.is_registered(PRE_REGISTRY_PACK):
        names.add(PRE_REGISTRY_PACK)
    return tuple(sorted(names))


# ======================================================================
# The DLT lane's gate and pack (MULTI_SERVICE_PLAN.md Phase 8)
# ======================================================================

def dlt_gate_mode() -> str:
    raw = os.environ.get(ENV_DLT_GATE, "").strip().lower()
    return raw if raw in GATE_MODES else GATE_RECORD


def dlt_enabled_services() -> frozenset:
    """The services whose dead-lettered records are analysed. Blank means the
    default, never "nothing", as for the rejection lane."""
    raw = os.environ.get(ENV_DLT_ENABLED, "")
    names = [name.strip() for name in raw.split(",") if name.strip()]
    return frozenset(names or DEFAULT_ENABLED)


def dlt_skip_reason(resolution: dict, registry: Optional[Registry] = None) -> Optional[str]:
    """Why the DLT gate would skip a record with this resolution, or None.

    An unresolved record is always a skip reason: the DLT lane has no
    `_default` pack to analyse it with, and in `record` mode it is analysed
    exactly as before, with no pack at all.
    """
    registry = registry or load()
    service = (resolution or {}).get("service") or UNRESOLVED
    if service == UNRESOLVED:
        return SKIP_UNRESOLVED
    if not registry.is_registered(service):
        return SKIP_NOT_REGISTERED
    if service not in dlt_enabled_services():
        return SKIP_NOT_ENABLED
    return None


def dlt_gate(resolution: dict, registry: Optional[Registry] = None) -> GateDecision:
    reason = dlt_skip_reason(resolution, registry)
    mode = dlt_gate_mode()
    return GateDecision(skip=bool(reason) and mode == GATE_ENFORCE,
                        reason=reason, mode=mode)


def dlt_pack_for(resolution: dict, registry: Optional[Registry] = None) -> Optional[str]:
    """The pack a dead-lettered record is analysed with: its own service's
    when the DLT gate lets it through, otherwise none -- which is how every
    record was analysed before the DLT lane resolved services, so `record`
    mode changes nothing about analysis here either."""
    registry = registry or load()
    if dlt_skip_reason(resolution, registry) is None:
        return (resolution or {}).get("service")
    return None


def fingerprint_service(resolution: dict, registry: Optional[Registry] = None) -> Optional[str]:
    """The service a DLT failure fingerprint is namespaced by, or None.

    The resolved service, when it is registered and is not the pre-registry
    one: so two services' records never share a group, while every
    enu-biometric and every unresolved fingerprint -- and the groups and
    recommendations stored under them -- is what it was. Independent of the
    gate: a group describes a service's failure whether or not that service
    is analysed yet.
    """
    registry = registry or load()
    service = (resolution or {}).get("service")
    if registry.is_registered(service) and service != PRE_REGISTRY_PACK:
        return service
    return None


def enrolment_labels(name: Optional[str], registry: Optional[Registry] = None) -> dict:
    """{RAW TYPE: label} from a pack's `enrolment_types.payload`, keys upper-case.

    Empty for a pack that declares none -- `_default` among them -- so a
    caller shows the raw value rather than another service's description.
    """
    found = pack(name, registry)
    payload = ((found.document.get("enrolment_types") or {}).get("payload") or {}) \
        if found else {}
    return {str(raw).strip().upper(): entry["label"]
            for raw, entry in payload.items()
            if isinstance(entry, dict) and isinstance(entry.get("label"), str)}


def rule_source_of(name: Optional[str], registry: Optional[Registry] = None) -> str:
    """`rules_db` or `none` (MULTI_SERVICE_PLAN.md D6). A pack that is not in
    the registry has no rule source: guessing one would query a service's
    rules table for a packet that is not that service's."""
    found = pack(name, registry)
    return found.rule_source_type if found else NO_RULES_DB


def rule_type_filter(name: Optional[str], registry: Optional[Registry] = None) -> Optional[dict]:
    """{RAW TYPE: rules-table type} from the pack's
    `rule_source.enrolment_type_filter`, keys upper-case, or None when it
    declares none (then the rules lookup applies no pack-specific filter)."""
    found = pack(name, registry)
    source = (found.document.get("rule_source") or {}) if found else {}
    mapping = source.get("enrolment_type_filter")
    if not isinstance(mapping, dict):
        return None
    return {str(raw).strip().upper(): value for raw, value in mapping.items()}


def docs_lookup_options(name: Optional[str], registry: Optional[Registry] = None) -> dict:
    """The keyword arguments `reason_code_docs.lookup` takes from a pack:

    * `service_file` -- the stem of the pack's own documentation file;
    * `type_families` -- {RAW TYPE OR ALIAS: family}, from
      `enrolment_types.payload` and `enrolment_types.doc_aliases`;
    * `type_labels` -- {family: label}, from `enrolment_types.family_labels`.

    Empty for a name with no pack, which then reads every file together.
    """
    found = pack(name, registry)
    if found is None:
        return {}
    types = found.document.get("enrolment_types") or {}
    families = {}
    for raw, entry in (types.get("payload") or {}).items():
        if isinstance(entry, dict) and isinstance(entry.get("family"), str):
            families[str(raw).strip().upper()] = entry["family"]
    for alias, family in (types.get("doc_aliases") or {}).items():
        families.setdefault(str(alias).strip().upper(), family)
    return {"service_file": found.reason_code_docs_file,
            "type_families": families or None,
            "type_labels": dict(types.get("family_labels") or {}) or None}


def log_options(name: Optional[str], registry: Optional[Registry] = None) -> dict:
    """A pack's `logs` section (MULTI_SERVICE_PLAN.md Phase 6):

    * `app_names` -- its Elasticsearch `application_name` values, which are
      also its Kubernetes app names; the service's own name when it declares
      none, as its documentation file and corpus directory default to it;
    * `k8s_match` -- {"name_contains" | "label_selector": value}, or None;
    * `also_search` -- the other registered services whose logs are read
      for its packets;
    * `decision_vocabulary` -- its regex, or None.

    No app names for a name with no registered pack, `_default` included.
    """
    registry = registry or load()
    found = pack(name, registry)
    logs = (found.document.get("logs") or {}) if found else {}
    apps = tuple(logs.get("app_names") or ())
    if not apps and registry.is_registered(name):
        apps = (name,)
    return {"app_names": apps,
            "k8s_match": dict(logs["k8s_match"]) if logs.get("k8s_match") else None,
            "also_search": tuple(logs.get("also_search") or ()),
            "decision_vocabulary": logs.get("decision_vocabulary")}


# ======================================================================
# Tools per service (MULTI_SERVICE_PLAN.md D7, D8)
# ======================================================================

def is_service_name(name) -> bool:
    """Whether `name` has the shape of a service name. Says nothing about
    whether such a service is registered."""
    return isinstance(name, str) and bool(_SERVICE_NAME.match(name))


def service_slug(name: str) -> str:
    """`name` with every character outside [a-z0-9] turned into "_".

    What a service is called where a hyphen is not allowed: its opencode
    agents (`crm_<role>__<slug>`) and its tool package
    (`src/tools/agent_tools/<slug>/`). The `_default` pack's slug is
    `default`.
    """
    if name == DEFAULT_PACK:
        return DEFAULT_SLUG
    return _SLUG_UNSAFE.sub("_", name)


def tool_scope(name: Optional[str], registry: Optional[Registry] = None) -> tuple:
    """(include, exclude): the tool names the pack's `tools` section adds to
    and removes from its scope, as frozensets. Empty for a name with no pack."""
    found = pack(name, registry)
    tools = (found.document.get("tools") or {}) if found else {}
    return (frozenset(tools.get("include") or ()),
            frozenset(tools.get("exclude") or ()))


def tool_prefix_error(tool_name: str, services, registry: Optional[Registry] = None) -> Optional[str]:
    """Why a tool's name breaks the prefix rule (D8), or None.

    A tool scoped to exactly one registered service is named
    `<that service's tool_prefix>_...`. A tool that declares a scope and is
    not scoped to one service alone -- every service, or several -- carries
    no registered service's prefix: the name would claim a service the scope
    does not. A tool that declares no services is not judged: it reaches no
    service unless configured to, and it may come from a server that knows
    nothing of this registry. A scope naming an unregistered service is
    reported where the scope is checked, not here.
    """
    registry = registry or load()
    services = tuple(services or ())
    if not services:
        return None
    named = [service for service in services if service != ALL_SERVICES]
    if ALL_SERVICES not in services and len(named) == 1:
        owner = named[0]
        if not registry.is_registered(owner):
            return None
        prefix = registry.packs[owner].tool_prefix
        if not tool_name.startswith(f"{prefix}_"):
            return (f"Tool {tool_name!r} is scoped to {owner} alone, so its name "
                    f"must start with {owner}'s tool prefix, '{prefix}_'.")
        return None
    for service in registry.services():
        prefix = registry.packs[service].tool_prefix
        if prefix and tool_name.startswith(f"{prefix}_"):
            return (f"Tool {tool_name!r} carries {service}'s tool prefix "
                    f"'{prefix}_' but is not scoped to {service} alone "
                    f"(services {list(services)}).")
    return None


def missing_corpus_dirs(docs_dir, registry: Optional[Registry] = None) -> list:
    """Enabled and pilot services whose DROA corpus directory is absent from
    `docs_dir`.

    Checked once the corpus has been downloaded, not at boot: the download
    runs in the background after the API has started.
    """
    registry = registry or load()
    root = Path(docs_dir)
    return [name for name in sorted(analysed_services())
            if registry.is_registered(name)
            and not (root / registry.packs[name].droa_corpus_dir).is_dir()]


# ======================================================================
# Validation, for main_api at boot
# ======================================================================

def validate(root=None, docs_root=None) -> tuple:
    """(errors, warnings) for the registry and the settings that read it.

    Errors are what main_api exits on. Validates the registry this process
    will actually use -- the cached one -- so what passed at boot is what
    places the packets.
    """
    registry = load(root)
    errors, warnings = list(registry.errors), list(registry.warnings)

    for variable, allowed in ((ENV_GATE, GATE_MODES),
                              (ENV_UNRESOLVED, UNRESOLVED_POLICIES)):
        raw = os.environ.get(variable, "").strip().lower()
        if raw and raw not in allowed:
            errors.append(f"{variable} must be one of {list(allowed)}; got "
                          f"{raw!r}.")

    for name in sorted(enabled_services()):
        if name in (DEFAULT_PACK, UNRESOLVED):
            errors.append(f"{ENV_ENABLED} names {name!r}, which is not a "
                          f"service. Unresolved packets are governed by "
                          f"{ENV_UNRESOLVED}.")
        elif not registry.is_registered(name):
            errors.append(f"{ENV_ENABLED} names {name!r}, which has no valid "
                          f"pack in {registry.root}.")

    # MULTI_SERVICE_PLAN.md Phase 7. A service in both lists is a move from
    # pilot to enabled done halfway: whether its Synthesis may stage replays
    # would then depend on which list was read, so it is refused.
    for name in sorted(pilot_services()):
        if name in (DEFAULT_PACK, UNRESOLVED):
            errors.append(f"{ENV_PILOT} names {name!r}, which is not a "
                          f"service. Unresolved packets are governed by "
                          f"{ENV_UNRESOLVED}.")
        elif not registry.is_registered(name):
            errors.append(f"{ENV_PILOT} names {name!r}, which has no valid "
                          f"pack in {registry.root}.")
        elif name in enabled_services():
            errors.append(f"{name} is named in both {ENV_PILOT} and "
                          f"{ENV_ENABLED}; a service is piloted or enabled, "
                          f"not both.")

    from src.utils import reason_code_docs
    from src.utils.env import get_bool_env

    # Where each analysed service's rules come from has to be a place that
    # exists (MULTI_SERVICE_PLAN.md 5.7): a service with neither a rules
    # database nor the documentation has no account of the rule at all, and
    # one with a rules database it cannot reach has no rule either. A pilot
    # service is analysed in full, so it is held to the same.
    docs_on = reason_code_docs.docs_enabled()
    for name in sorted(analysed_services()):
        if not registry.is_registered(name):
            continue  # already reported above
        source = registry.packs[name].rule_source_type
        if source == NO_RULES_DB and not docs_on:
            errors.append(f"{name} has rule_source.type 'none', so its "
                          f"documentation is its only rule source, but "
                          f"REJECTION_REASON_CODE_DOCS_ENABLED is off.")
        elif source == RULES_DB and not get_bool_env("USE_MOCK_DB", True) \
                and not os.environ.get("DB_HOST", "").strip():
            errors.append(f"{name} has rule_source.type 'rules_db' and "
                          f"USE_MOCK_DB is off, but DB_HOST is not set.")

    for variable in (ENV_DLT_GATE,):
        raw = os.environ.get(variable, "").strip().lower()
        if raw and raw not in GATE_MODES:
            errors.append(f"{variable} must be one of {list(GATE_MODES)}; got "
                          f"{raw!r}.")
    for name in sorted(dlt_enabled_services()):
        if name in (DEFAULT_PACK, UNRESOLVED):
            errors.append(f"{ENV_DLT_ENABLED} names {name!r}, which is not a "
                          f"service.")
        elif not registry.is_registered(name):
            errors.append(f"{ENV_DLT_ENABLED} names {name!r}, which has no valid "
                          f"pack in {registry.root}.")
        elif not (registry.packs[name].consumer_groups
                  or registry.packs[name].original_topics
                  or registry.packs[name].java_packages
                  or registry.packs[name].stages
                  or registry.packs[name].source_topics):
            warnings.append(f"{name} is in {ENV_DLT_ENABLED} but neither its dlt "
                            f"section nor its match section can place a "
                            f"dead-lettered record in it.")

    if unresolved_policy() == UNRESOLVED_DEFAULT_PACK and not docs_on:
        warnings.append(f"{ENV_UNRESOLVED} is {UNRESOLVED_DEFAULT_PACK!r} and "
                        f"REJECTION_REASON_CODE_DOCS_ENABLED is off, so an "
                        f"unresolved packet is analysed with no service policy "
                        f"and no documentation.")

    documented = set(reason_code_docs.documented_services(root=docs_root))
    claimed = set()
    for name in registry.services():
        stem = registry.packs[name].reason_code_docs_file
        claimed.add(stem)
        if stem not in documented:
            warnings.append(f"{name} has no reason-code documentation file "
                            f"({stem}.json), so its reason codes cannot place "
                            f"a packet and will have no documentation.")
    for stem in sorted(documented - claimed):
        warnings.append(f"The reason-code documentation file {stem}.json has "
                        f"no service pack; a packet placed by one of its codes "
                        f"is reported as {SKIP_NOT_REGISTERED}.")

    # The local tools' service scopes and names (MULTI_SERVICE_PLAN.md 5.7,
    # Phase 4). Judged here because only the registry can say whether a
    # service is registered and what its prefix is.
    from src.tools import agent_tools

    tool_errors, tool_warnings = agent_tools.scope_problems(registry)
    errors.extend(tool_errors)
    warnings.extend(tool_warnings)

    return errors, warnings


# ======================================================================
# The stored resolution
# ======================================================================

def load_or_resolve(storage, event_id: str, payload, resolver=None) -> tuple:
    """(resolution dict, fresh) for one packet.

    `resolver(payload)` resolves afresh; `resolve` by default. The DLT lane
    passes `resolve_dlt` with the record's headers and trace bound.

    The stored resolution wins when there is one: it is what the fetch stage
    decided, and the analysis stage must act on the same answer even if the
    registry has been redeployed in between. `storage` is passed in rather
    than looked up, because every caller already holds the store it is
    working in.
    """
    try:
        raw = storage.load_artifact(event_id, RESOLUTION_ARTIFACT)
    except Exception as error:
        logger.warning("Could not read the stored service resolution; "
                       "resolving again", event_id=event_id,
                       error=f"{type(error).__name__}: {error}")
        raw = None
    if raw:
        try:
            stored = json.loads(raw)
        except (TypeError, ValueError):
            stored = None
        if isinstance(stored, dict) and isinstance(stored.get("service"), str) \
                and stored["service"]:
            return stored, False
        logger.warning("Ignoring an unreadable stored service resolution",
                       event_id=event_id)
    return (resolver or resolve)(payload).as_dict(), True


def persist(storage, event_id: str, resolution: dict) -> bool:
    """Store the resolution beside the packet's other artifacts.

    Best effort, like the tool evidence: a failed write costs the analysis
    stage a re-resolution, never the packet.
    """
    try:
        storage.save_artifact(event_id, RESOLUTION_ARTIFACT,
                              json.dumps(resolution, indent=2, sort_keys=True,
                                         ensure_ascii=False))
        return True
    except Exception as error:
        logger.warning("Could not store the service resolution",
                       event_id=event_id,
                       error=f"{type(error).__name__}: {error}")
        return False
