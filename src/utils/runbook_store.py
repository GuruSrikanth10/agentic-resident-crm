import os
import json
import re
import hashlib
import threading
from typing import Optional
from cachetools import TTLCache
from pathlib import Path
from src.utils.atomic import replace_with_retry
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

RUNBOOK_ROOT = Path(__file__).resolve().parent.parent.parent / "src" / "runbooks"
RUNBOOK_FINAL_DIR = RUNBOOK_ROOT / "final"
RUNBOOK_DRAFT_DIR = RUNBOOK_ROOT / "draft"

# Ensure directories exist
os.makedirs(RUNBOOK_FINAL_DIR, exist_ok=True)
os.makedirs(RUNBOOK_DRAFT_DIR, exist_ok=True)

# Validation pattern for reason_code (0.11, 1.17 convention)
REASON_CODE_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")

#: Runbooks are kept per service (MULTI_SERVICE_PLAN.md D11):
#: `{draft,final}/<service>/<CODE>__<TYPE>.json`. Schema 1.2 records the
#: service and what the runbook is bound to; 1.0 and 1.1 predate services and
#: load only from the pre-registry service's directory, the only service
#: runbooks were written for before then.
SCHEMA_VERSION = "1.2"
LEGACY_SCHEMA_VERSIONS = ("1.0", "1.1")

#: What a runbook is checked against before it is served (D11). A service
#: with a rules table binds to the fingerprint of the rule it was derived
#: from; a service without one binds to a hash of the documentation entries.
BINDING_DB_RULE = "db_rule"
BINDING_REASON_CODE_DOC = "reason_code_doc"
BINDING_TYPES = (BINDING_DB_RULE, BINDING_REASON_CODE_DOC)

# Module-level TTL cache for runbook loads
CACHE_TTL = int(os.environ.get("RUNBOOK_CACHE_TTL_SECONDS", "600"))
_runbook_cache = TTLCache(maxsize=1024, ttl=CACHE_TTL)
# TTLCache is not thread-safe and this is read from every concurrent agent
# thread; expiry mutates internal state during lookup (F15).
_runbook_cache_lock = threading.Lock()

#: Bare allowlist entries already warned about, so a deprecated entry is
#: reported once per process rather than on every lookup.
_warned_bare_codes: set = set()
_warned_lock = threading.Lock()


def _pre_registry_service() -> str:
    # Imported here: service_registry reaches reason_code_docs, which imports
    # this module for REASON_CODE_PATTERN.
    from src.utils.service_registry import PRE_REGISTRY_PACK
    return PRE_REGISTRY_PACK


def _is_service_name(name) -> bool:
    from src.utils.service_registry import is_service_name
    return is_service_name(name)


def normalize_enrolment_type(enrolment_type) -> str:
    """Normalise an enrolment type for use in a filename or cache key.

    A missing type means the runbook applies to any type.
    """
    return str(enrolment_type).strip().upper() if enrolment_type else "ANY"


def runbook_cache_key(service: str, reason_code: str, enrolment_type) -> str:
    """The one place a runbook cache key is constructed.

    `promote_draft_to_final` used to build this from the *raw* enrolment type
    while `get_runbook` built it from the normalised one, so a draft carrying
    "e" or " E " invalidated a key lookups never read -- and the stale runbook
    kept being served for up to RUNBOOK_CACHE_TTL_SECONDS (F13). The service
    is part of the key: two services' runbooks for one code are different
    runbooks.
    """
    return f"{service}/{reason_code}__{normalize_enrolment_type(enrolment_type)}"


def _parse_allowlist_entry(entry: str) -> tuple:
    """(service, code) for one RUNBOOK_SERVE_ALLOWLIST entry.

    `service:CODE` names both. A bare `CODE` was written before runbooks were
    kept per service and so means the pre-registry service; it still works,
    with a deprecation warning. A reason code may itself contain ":", so the
    part before the first ":" is read as a service only when it has the shape
    of a service name.
    """
    service, sep, code = entry.partition(":")
    if sep and code and _is_service_name(service):
        return service, code
    pre = _pre_registry_service()
    with _warned_lock:
        first = entry not in _warned_bare_codes
        _warned_bare_codes.add(entry)
    if first:
        logger.warning("RUNBOOK_SERVE_ALLOWLIST entry names no service; read as "
                       f"{pre}. Write it as '{pre}:{entry}'.", entry=entry)
    return pre, entry


def serve_allowlist() -> Optional[set]:
    """(service, reason code) pairs cleared to short-circuit the agents (4.2).

    RUNBOOK_MODE is a global switch: flipping it to `serve` turns runbooks on
    for every reason code at once, including ones with no accuracy evidence
    behind them. This narrows it, so a high-volume, well-understood code can
    go first while everything else keeps running the agents. Entries are
    `service:CODE` (MULTI_SERVICE_PLAN.md D11); a code cleared for one
    service is not cleared for another.

    Returns None when unset, meaning "no per-code restriction" -- the previous
    behaviour, so existing deployments are unaffected.
    """
    raw = os.environ.get("RUNBOOK_SERVE_ALLOWLIST", "").strip()
    if not raw:
        return None
    return {_parse_allowlist_entry(entry.strip())
            for entry in raw.split(",") if entry.strip()}


def is_serve_allowed(service: str, reason_code: str) -> bool:
    allowed = serve_allowlist()
    return True if allowed is None else (service, reason_code) in allowed


def generate_rule_fingerprint(rule) -> str:
    """Generate a stable SHA256 fingerprint from parsed rule row(s).

    Accepts the dict or list-of-dicts that `lookup_rule_for` returns. It must
    NOT be handed the raw `DataFrame.to_json()` string: hashing that folds
    column order and float formatting into the fingerprint, so a harmless
    re-export of the rules table invalidated every runbook (F2).
    """
    if isinstance(rule, str):
        raise TypeError(
            "generate_rule_fingerprint expects parsed rule rows (dict or list), "
            "not a raw JSON string -- use tool_registry.lookup_rule_for()."
        )
    canonical_json = json.dumps(rule, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def doc_binding_fingerprint(doc_state) -> Optional[str]:
    """The documentation fingerprint a runbook is bound to, or None.

    The hash of the entries the lookup selected (`entries_sha256`), never of
    the rendered text: the title line carries the pack's enrolment-type
    label, and editing a label must not make every runbook stale (D11). None
    when the lookup did not hit, so there is nothing to bind to.
    """
    if not isinstance(doc_state, dict) or doc_state.get("outcome") != "hit":
        return None
    return doc_state.get("entries_sha256") or None


def binding_of(runbook: dict) -> dict:
    """{"type", "fingerprint"}: what `runbook` is checked against.

    A 1.2 runbook states it. One written before 1.2 carries only
    `rule_fingerprint`, which is a rules-table binding.
    """
    binding = runbook.get("binding")
    if isinstance(binding, dict) and binding.get("type"):
        return {"type": binding.get("type"), "fingerprint": binding.get("fingerprint")}
    return {"type": BINDING_DB_RULE, "fingerprint": runbook.get("rule_fingerprint")}


def service_dir(directory: Path, service: str) -> Path:
    """`directory/<service>`, refusing anything that is not a service name --
    `_default` included, since an unresolved packet has no service to keep
    runbooks for."""
    if not _is_service_name(service):
        raise ValueError(f"Invalid service name: {service!r}")
    return directory / service


def _resolve_runbook_path(reason_code: str, enrolment_type: str, directory: Path) -> Path:
    """Resolve and validate the runbook file path safely inside `directory`,
    which is one service's directory."""
    if not REASON_CODE_PATTERN.match(reason_code):
        raise ValueError(f"Invalid reason_code format: {reason_code}")

    # Enrolment type defaults to ANY if missing
    etype = normalize_enrolment_type(enrolment_type)

    filename = f"{reason_code}__{etype}.json"
    target_path = (directory / filename).resolve()

    # Path traversal guard
    if not str(target_path).startswith(str(directory.resolve())):
        raise ValueError(f"Resolved path escapes runbook directory: {target_path}")

    return target_path


def check_runbook(data, service: str) -> None:
    """Raise ValueError when `data` is not a servable runbook of `service`."""
    if not isinstance(data, dict):
        raise ValueError("Runbook is not a JSON object")
    version = data.get("schema_version")
    if version == SCHEMA_VERSION:
        if data.get("service") != service:
            raise ValueError(f"Runbook names service {data.get('service')!r} "
                             f"but is kept under {service!r}")
        binding = data.get("binding")
        if not isinstance(binding, dict) or binding.get("type") not in BINDING_TYPES \
                or not isinstance(binding.get("fingerprint"), str) \
                or not binding["fingerprint"]:
            raise ValueError(f"Invalid binding: {binding!r}")
    elif version in LEGACY_SCHEMA_VERSIONS:
        # Written before services were known, so a runbook of the
        # pre-registry service; under another service it could only have
        # been copied there by mistake.
        if service != _pre_registry_service():
            raise ValueError(f"A schema {version} runbook predates services and "
                             f"is valid only under {_pre_registry_service()!r}")
        if data.get("service") not in (None, service):
            raise ValueError(f"Runbook names service {data.get('service')!r} "
                             f"but is kept under {service!r}")
    else:
        raise ValueError(f"Unsupported schema version: {version}")
    if data.get("status") != "final":
        raise ValueError(f"Status is not final: {data.get('status')}")

    resolution = data.get("resolution", {})
    required_keys = {"rejection_description", "synthesis", "action", "resident_action"}
    if not required_keys.issubset(resolution.keys()):
        raise ValueError(f"Missing resolution keys: {required_keys - set(resolution.keys())}")


def get_runbook(service: str, reason_code: str, enrolment_type: str) -> Optional[dict]:
    """
    Fetch `service`'s final runbook for the given reason code and enrolment
    type. Falls back to that service's ANY runbook if the exact match fails,
    never to another service's. Uses TTLCache. Returns None on miss.
    """
    if not reason_code:
        logger.info("Runbook miss", miss_reason="no_reason_code")
        return None

    if not REASON_CODE_PATTERN.match(reason_code):
        logger.info("Runbook miss", reason_code=reason_code, enrolment_type=enrolment_type, miss_reason="invalid_key")
        return None

    try:
        directory = service_dir(RUNBOOK_FINAL_DIR, service)
    except ValueError:
        logger.info("Runbook miss", service=service, reason_code=reason_code,
                    miss_reason="invalid_service")
        return None

    # Determine types to try (exact match, then ANY fallback)
    etype = normalize_enrolment_type(enrolment_type)
    candidates = [(reason_code, etype)]
    if etype != "ANY":
        candidates.append((reason_code, "ANY"))

    for r_code, e_type in candidates:
        cache_key = runbook_cache_key(service, r_code, e_type)
        with _runbook_cache_lock:
            cached = _runbook_cache.get(cache_key)
        if cached is not None:
            return cached

        try:
            target_path = _resolve_runbook_path(r_code, e_type, directory)
        except ValueError as e:
            logger.info("Runbook miss", service=service, reason_code=r_code, enrolment_type=e_type, miss_reason="invalid_key")
            continue

        if not target_path.exists():
            continue

        try:
            with open(target_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            check_runbook(data, service)

            with _runbook_cache_lock:
                _runbook_cache[cache_key] = data
            return data

        except Exception as e:
            logger.error("Malformed runbook", path=str(target_path), error=f"{type(e).__name__}: {e}")
            logger.info("Runbook miss", service=service, reason_code=r_code, enrolment_type=e_type, miss_reason="malformed")

    logger.info("Runbook miss", service=service, reason_code=reason_code, enrolment_type=enrolment_type, miss_reason="not_found")
    return None

def write_draft_runbook(service: str, reason_code: str, enrolment_type: str, data: dict):
    """Write a draft runbook to `service`'s draft directory. Cannot write to final."""
    directory = service_dir(RUNBOOK_DRAFT_DIR, service)
    directory.mkdir(parents=True, exist_ok=True)
    target_path = _resolve_runbook_path(reason_code, enrolment_type, directory)

    # Use atomic write via temp file
    tmp_path = target_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    replace_with_retry(tmp_path, target_path)


def _service_runbooks(directory: Path) -> list[Path]:
    """Every `<service>/<CODE>__<TYPE>.json` under `directory`, sorted.

    A runbook left at the top level, where they were kept before services
    were known, is reported and not listed: it names no service, and
    guessing one is exactly what keeping them per service prevents.
    """
    if not directory.exists():
        return []
    for stray in sorted(directory.glob("*.json")):
        logger.warning("Runbook outside any service directory is ignored; move "
                       "it under <service>/", path=str(stray))
    return sorted(path for path in directory.glob("*/*.json")
                  if _is_service_name(path.parent.name))


def list_draft_runbooks() -> list[Path]:
    """Return all draft runbook paths, across every service's directory."""
    return _service_runbooks(RUNBOOK_DRAFT_DIR)


def list_final_runbooks() -> list[Path]:
    """Return all final runbook paths, across every service's directory."""
    return _service_runbooks(RUNBOOK_FINAL_DIR)


def service_of_path(path: Path) -> str:
    """The service a runbook file is kept under: its directory's name."""
    return Path(path).parent.name


def load_draft_runbook(path: Path) -> dict:
    """Load a draft runbook."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def promote_draft_to_final(draft_path: Path, data: dict):
    """Save to the same service's final/ directory and remove the draft.

    The service is the draft's directory: a draft is promoted within the
    service it was written for, never into another.
    """
    service = service_of_path(draft_path)
    if data.get("service") not in (None, service):
        raise ValueError(f"Draft names service {data.get('service')!r} but is "
                         f"kept under {service!r}")
    reason_code = data["reason_code"]
    enrolment_type = data["enrolment_type"]

    directory = service_dir(RUNBOOK_FINAL_DIR, service)
    directory.mkdir(parents=True, exist_ok=True)
    final_path = _resolve_runbook_path(reason_code, enrolment_type, directory)
    tmp_path = final_path.with_suffix(".json.tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    replace_with_retry(tmp_path, final_path)

    # Invalidate cache. Built through the same normalizer get_runbook() reads
    # from, so a draft carrying "e" invalidates the "E" key lookups use (F13).
    with _runbook_cache_lock:
        _runbook_cache.pop(runbook_cache_key(service, reason_code, enrolment_type), None)

    # Remove draft
    os.remove(draft_path)
