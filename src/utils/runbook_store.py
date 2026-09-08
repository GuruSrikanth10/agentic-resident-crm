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

# Module-level TTL cache for runbook loads
CACHE_TTL = int(os.environ.get("RUNBOOK_CACHE_TTL_SECONDS", "600"))
_runbook_cache = TTLCache(maxsize=1024, ttl=CACHE_TTL)
# TTLCache is not thread-safe and this is read from every concurrent agent
# thread; expiry mutates internal state during lookup (F15).
_runbook_cache_lock = threading.Lock()

def normalize_enrolment_type(enrolment_type) -> str:
    """Normalise an enrolment type for use in a filename or cache key.

    A missing type means the runbook applies to any type.
    """
    return str(enrolment_type).strip().upper() if enrolment_type else "ANY"


def runbook_cache_key(reason_code: str, enrolment_type) -> str:
    """The one place a runbook cache key is constructed.

    `promote_draft_to_final` used to build this from the *raw* enrolment type
    while `get_runbook` built it from the normalised one, so a draft carrying
    "e" or " E " invalidated a key lookups never read -- and the stale runbook
    kept being served for up to RUNBOOK_CACHE_TTL_SECONDS (F13).
    """
    return f"{reason_code}__{normalize_enrolment_type(enrolment_type)}"


def serve_allowlist() -> Optional[set]:
    """Reason codes cleared to short-circuit the agents (4.2).

    RUNBOOK_MODE is a global switch: flipping it to `serve` turns runbooks on
    for every reason code at once, including ones with no accuracy evidence
    behind them. This narrows it, so a high-volume, well-understood code can
    go first while everything else keeps running the agents.

    Returns None when unset, meaning "no per-code restriction" -- the previous
    behaviour, so existing deployments are unaffected.
    """
    raw = os.environ.get("RUNBOOK_SERVE_ALLOWLIST", "").strip()
    if not raw:
        return None
    return {code.strip() for code in raw.split(",") if code.strip()}


def is_serve_allowed(reason_code: str) -> bool:
    allowed = serve_allowlist()
    return True if allowed is None else reason_code in allowed


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

def _resolve_runbook_path(reason_code: str, enrolment_type: str, directory: Path) -> Path:
    """Resolve and validate the runbook file path safely."""
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

def get_runbook(reason_code: str, enrolment_type: str) -> Optional[dict]:
    """
    Fetch a final runbook for the given reason code and enrolment type.
    Falls back to ANY enrolment_type if exact match fails.
    Uses TTLCache. Returns None on miss.
    """
    if not reason_code:
        logger.info("Runbook miss", miss_reason="no_reason_code")
        return None
        
    if not REASON_CODE_PATTERN.match(reason_code):
        logger.info("Runbook miss", reason_code=reason_code, enrolment_type=enrolment_type, miss_reason="invalid_key")
        return None

    # Determine types to try (exact match, then ANY fallback)
    etype = normalize_enrolment_type(enrolment_type)
    candidates = [(reason_code, etype)]
    if etype != "ANY":
        candidates.append((reason_code, "ANY"))

    for r_code, e_type in candidates:
        cache_key = runbook_cache_key(r_code, e_type)
        with _runbook_cache_lock:
            cached = _runbook_cache.get(cache_key)
        if cached is not None:
            return cached
            
        try:
            target_path = _resolve_runbook_path(r_code, e_type, RUNBOOK_FINAL_DIR)
        except ValueError as e:
            logger.info("Runbook miss", reason_code=r_code, enrolment_type=e_type, miss_reason="invalid_key")
            continue
            
        if not target_path.exists():
            continue
            
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            # Basic validation
            if not isinstance(data, dict):
                raise ValueError("Runbook is not a JSON object")
            if data.get("schema_version") not in ("1.0", "1.1"):
                raise ValueError(f"Unsupported schema version: {data.get('schema_version')}")
            if data.get("status") != "final":
                raise ValueError(f"Status is not final: {data.get('status')}")
            
            resolution = data.get("resolution", {})
            required_keys = {"rejection_description", "synthesis", "action", "resident_action"}
            if not required_keys.issubset(resolution.keys()):
                raise ValueError(f"Missing resolution keys: {required_keys - set(resolution.keys())}")
                
            with _runbook_cache_lock:
                _runbook_cache[cache_key] = data
            return data
            
        except Exception as e:
            logger.error("Malformed runbook", path=str(target_path), error=f"{type(e).__name__}: {e}")
            logger.info("Runbook miss", reason_code=r_code, enrolment_type=e_type, miss_reason="malformed")
            
    logger.info("Runbook miss", reason_code=reason_code, enrolment_type=enrolment_type, miss_reason="not_found")
    return None

def write_draft_runbook(reason_code: str, enrolment_type: str, data: dict):
    """Write a draft runbook to disk. Cannot write to final."""
    target_path = _resolve_runbook_path(reason_code, enrolment_type, RUNBOOK_DRAFT_DIR)
    
    # Use atomic write via temp file
    tmp_path = target_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    replace_with_retry(tmp_path, target_path)

def list_draft_runbooks() -> list[Path]:
    """Return all draft runbook paths."""
    if not RUNBOOK_DRAFT_DIR.exists():
        return []
    return list(RUNBOOK_DRAFT_DIR.glob("*.json"))

def load_draft_runbook(path: Path) -> dict:
    """Load a draft runbook."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def promote_draft_to_final(draft_path: Path, data: dict):
    """Save to final/ and remove the draft."""
    reason_code = data["reason_code"]
    enrolment_type = data["enrolment_type"]
    
    final_path = _resolve_runbook_path(reason_code, enrolment_type, RUNBOOK_FINAL_DIR)
    tmp_path = final_path.with_suffix(".json.tmp")
    
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    replace_with_retry(tmp_path, final_path)
    
    # Invalidate cache. Built through the same normalizer get_runbook() reads
    # from, so a draft carrying "e" invalidates the "E" key lookups use (F13).
    with _runbook_cache_lock:
        _runbook_cache.pop(runbook_cache_key(reason_code, enrolment_type), None)
    
    # Remove draft
    os.remove(draft_path)
