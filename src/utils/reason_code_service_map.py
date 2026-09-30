"""
The reason-code -> service map: which service raises each reject reason code.

Under the AUDIT contract a rejection's `edata.stage` is the reject
interceptor's (`REJECTINTERCEPTOR`) -- the service that publishes every
rejection -- not the stage of the service that rejected the packet. Nothing
else in the payload names that service either, so the rejection lane places
a packet by its reason code, from this map, before anything else
(`service_registry.resolve`). The DLT lane does not read it: a dead-lettered
record's `edata.stage` is the failing service's own.

The file, `reason_code_services.json`:

    {
      "schema_version": 1,
      "description": "optional free text",
      "services": {
        "enu-biometric": ["RESIDENT_MAN_DEDUP_REJECT_ANOMALOUS", ...],
        "enu-qc":        ["RESIDENT_QC_POA_DOCUMENT_NOT_APPROVED", ...]
      }
    }

Grouped by service because each service's team owns its list. A code listed
under two services is allowed -- a shared library can raise it -- but then
the map decides nothing for it and the next step of the resolution does. A
service need not have a pack: its packets resolve to it and the gate reports
them `service_not_registered`.

Where it comes from: REASON_CODE_SERVICE_MAP_S3_KEY, when set, is fetched at
start-up and every REASON_CODE_SERVICE_MAP_REFRESH_SECONDS into the file on
disk; a copy that does not validate never replaces the one there. Without a
key, the file on disk is used as it is, and without a file there is no map
and packets are placed exactly as before it existed. The file is re-read
when it changes, so a refresh takes effect without a restart; a packet's
resolution is stored at the fetch stage, so a refresh between its two stages
does not move it.
"""
import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from src.utils import paths
from src.utils.logging_config import get_logger
from src.utils.runbook_store import REASON_CODE_PATTERN

logger = get_logger(__name__)

SCHEMA_VERSION = 1

FILE_STEM = "reason_code_services"
FILENAME = f"{FILE_STEM}.json"

ENV_S3_KEY = "REASON_CODE_SERVICE_MAP_S3_KEY"
ENV_S3_BUCKET = "REASON_CODE_SERVICE_MAP_S3_BUCKET"
ENV_REFRESH = "REASON_CODE_SERVICE_MAP_REFRESH_SECONDS"

#: The shape of a service name, as `service_registry` requires of a pack.
_SERVICE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")

_FILE_KEYS = {"schema_version", "description", "services"}


@dataclass(frozen=True)
class ServiceMap:
    #: {reason code: (service, ...)}, services sorted. A code with more than
    #: one service decides nothing.
    by_code: dict = field(default_factory=dict)
    #: Every service the map names, sorted.
    services: tuple = ()
    #: Digest of the file, recorded with each resolution it made.
    sha256: Optional[str] = None

    def services_for(self, code) -> tuple:
        return self.by_code.get(code, ()) if code else ()


EMPTY = ServiceMap()


# ======================================================================
# Configuration
# ======================================================================

def map_path() -> Path:
    """The file on disk, resolved at call time so tests can patch `paths`."""
    return (paths.REASON_CODE_SERVICE_MAP_FILE
            or paths.REASON_CODE_DOCS_DIR / FILENAME)


def s3_key() -> Optional[str]:
    """The object to fetch the map from, or None when it is not fetched."""
    return os.environ.get(ENV_S3_KEY, "").strip().lstrip("/") or None


def s3_bucket() -> Optional[str]:
    return (os.environ.get(ENV_S3_BUCKET, "").strip()
            or os.environ.get("CASEBOOK_S3_BUCKET", "").strip()
            or os.environ.get("S3_LOGS_BUCKET", "").strip() or None)


def refresh_seconds() -> float:
    """How often to fetch the map again after start-up; 0 means only at
    start-up. An unusable value is 0 rather than an error."""
    try:
        value = float(os.environ.get(ENV_REFRESH, "").strip() or 0)
    except ValueError:
        return 0.0
    return value if value > 0 else 0.0


# ======================================================================
# Parsing
# ======================================================================

class _DuplicateKey(ValueError):
    pass


def _no_duplicate_keys(pairs):
    """json.loads keeps the last of two equal keys without a word, which in a
    hand-edited map silently drops one service's whole list."""
    document = {}
    for key, value in pairs:
        if key in document:
            raise _DuplicateKey(f"the key {key!r} appears twice in one object")
        document[key] = value
    return document


def parse(raw, label: str = FILENAME) -> tuple:
    """(ServiceMap or None, errors, warnings) for the file's bytes or text.

    Any error rejects the whole file: a map that is half right would place
    the other half's packets wrongly without saying so.
    """
    errors, warnings = [], []
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        document = json.loads(text, object_pairs_hook=_no_duplicate_keys)
    except UnicodeDecodeError as error:
        return None, [f"{label} is not UTF-8: {error}"], warnings
    except _DuplicateKey as error:
        return None, [f"{label}: {error}"], warnings
    except ValueError as error:
        return None, [f"{label} is not valid JSON: {error}"], warnings

    if not isinstance(document, dict):
        return None, [f"{label} holds a {type(document).__name__}, not an object."], warnings
    unknown = sorted(set(document) - _FILE_KEYS)
    if unknown:
        errors.append(f"{label} has unknown key(s) {unknown}; expected a subset "
                      f"of {sorted(_FILE_KEYS)}.")
    if document.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"{label}: schema_version is "
                      f"{document.get('schema_version')!r}, expected {SCHEMA_VERSION}.")
    services = document.get("services")
    if not isinstance(services, dict):
        errors.append(f"{label}: 'services' must be an object of service name "
                      f"-> list of reason codes.")
        return None, errors, warnings

    owners: dict = {}
    for service, codes in services.items():
        if not _SERVICE_NAME.match(service):
            errors.append(f"{label}: service name {service!r} must match "
                          f"{_SERVICE_NAME.pattern}.")
            continue
        if not isinstance(codes, list):
            errors.append(f"{label}: services.{service} must be a list of "
                          f"reason codes.")
            continue
        seen = set()
        for code in codes:
            if not isinstance(code, str) or not REASON_CODE_PATTERN.match(code):
                errors.append(f"{label}: services.{service} has {code!r}, which "
                              f"is not a reason code ({REASON_CODE_PATTERN.pattern}).")
                continue
            if code in seen:
                warnings.append(f"{label}: services.{service} lists {code} twice.")
                continue
            seen.add(code)
            owners.setdefault(code, []).append(service)

    if errors:
        return None, errors, warnings
    for code, names in sorted(owners.items()):
        if len(names) > 1:
            warnings.append(f"{label}: {code} is listed under {sorted(names)}, so "
                            f"the map does not place its packets.")

    encoded = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    return ServiceMap(
        by_code={code: tuple(sorted(names)) for code, names in owners.items()},
        services=tuple(sorted(services)),
        sha256="sha256:" + hashlib.sha256(encoded).hexdigest(),
    ), errors, warnings


# ======================================================================
# Reading the map on disk
# ======================================================================

#: str(path) -> ((mtime_ns, size), ServiceMap). Keyed on the file's signature
#: so a refreshed file is read again, and an unchanged one is not re-parsed
#: per packet.
_cache: dict = {}
_cache_lock = threading.Lock()


def current(path=None) -> ServiceMap:
    """The map on disk, or EMPTY when there is none. Never raises.

    A file that does not parse keeps the last good map, if this process had
    one: only a hand edit puts such a file there, since a download is
    validated before it is written, and boot validation refuses one.
    """
    path = Path(path) if path is not None else map_path()
    key = str(path)
    with _cache_lock:
        cached = _cache.get(key)
    try:
        stat = path.stat()
    except FileNotFoundError:
        return EMPTY
    except OSError as error:
        logger.warning("Could not read the reason-code service map",
                       path=key, error=f"{type(error).__name__}: {error}")
        return cached[1] if cached else EMPTY
    signature = (stat.st_mtime_ns, stat.st_size)
    if cached is not None and cached[0] == signature:
        return cached[1]

    try:
        service_map, errors, _warnings = parse(path.read_bytes(), path.name)
    except OSError as error:
        errors, service_map = [f"{path.name} could not be read: {error}"], None
    if errors or service_map is None:
        logger.error("The reason-code service map on disk is invalid; keeping "
                     "the last good copy", path=key, errors=errors,
                     kept=bool(cached and cached[1].sha256))
        service_map = cached[1] if cached else EMPTY
    with _cache_lock:
        _cache[key] = (signature, service_map)
    return service_map


def reset() -> None:
    """Forget every map read. For tests."""
    with _cache_lock:
        _cache.clear()


# ======================================================================
# The download from S3
# ======================================================================

_download_lock = threading.Lock()


def download() -> bool:
    """Fetch the map from S3 onto disk. True when a valid copy is in place.

    A fetch that fails or does not validate changes nothing: the last good
    copy keeps serving. Never raises: a map problem must not take the API
    down.
    """
    key = s3_key()
    if not key:
        return False
    bucket = s3_bucket()
    if not bucket:
        logger.warning(f"{ENV_S3_KEY} is set but no S3 bucket is configured "
                       f"({ENV_S3_BUCKET}, CASEBOOK_S3_BUCKET or S3_LOGS_BUCKET)")
        return False
    with _download_lock:
        try:
            return _download(bucket, key)
        except Exception as error:  # noqa: BLE001 - the contract is "never raises"
            logger.warning("Reason-code service map download failed; keeping the "
                           "copy on disk", bucket=bucket, key=key,
                           error=f"{type(error).__name__}: {error}")
            return False


def _download(bucket: str, key: str) -> bool:
    from src.storage.s3 import _get_client
    from src.utils.atomic import replace_with_retry

    body = _get_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    service_map, errors, warnings = parse(body, key)
    for warning in warnings:
        logger.warning("Downloaded reason-code service map warning", detail=warning)
    if errors:
        logger.error("Downloaded reason-code service map failed validation; "
                     "keeping the copy on disk", bucket=bucket, key=key, errors=errors)
        return False

    target = map_path()
    if target.is_file() and target.read_bytes() == body:
        return True
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.download")
    try:
        staging.write_bytes(body)
        replace_with_retry(staging, target)
    finally:
        staging.unlink(missing_ok=True)
    logger.info("Reason-code service map downloaded", bucket=bucket, key=key,
                path=str(target), codes=len(service_map.by_code),
                services=list(service_map.services), sha256=service_map.sha256)
    try:
        for warning in coverage_warnings(service_map):
            logger.warning("Reason-code service map warning", detail=warning)
    except Exception as error:  # noqa: BLE001 - the copy is already in place
        logger.warning("Could not check the reason-code service map against the "
                       "registry", error=f"{type(error).__name__}: {error}")
    return True


def available() -> bool:
    """Whether /ready may pass as far as the map is concerned: always, unless
    it is fetched from S3 and no copy is on disk yet. Placing packets before
    the map is there would, under an enforcing gate, skip them unanalysed."""
    return not s3_key() or map_path().is_file()


def start_background_download():
    """Fetch the map now, off the caller's thread, and again every
    REASON_CODE_SERVICE_MAP_REFRESH_SECONDS when that is set. Returns the
    thread, or None when the map is not fetched from S3."""
    if not s3_key():
        return None

    def run():
        download()
        while refresh_seconds() > 0:
            time.sleep(refresh_seconds())
            download()

    thread = threading.Thread(target=run, name="reason-code-service-map-download",
                              daemon=True)
    thread.start()
    return thread


# ======================================================================
# Validation, for main_api at boot (through service_registry.validate)
# ======================================================================

def coverage_warnings(service_map: ServiceMap, registry=None) -> list:
    """What the map leaves out or names that the registry does not know."""
    from src.utils import service_registry

    registry = registry or service_registry.load()
    warnings = []
    unregistered = [name for name in service_map.services
                    if not registry.is_registered(name)]
    if unregistered:
        warnings.append(f"The reason-code service map names services with no "
                        f"pack: {unregistered}. Their packets are reported as "
                        f"{service_registry.SKIP_NOT_REGISTERED}; check for a typo.")
    mapped = {names[0] for names in service_map.by_code.values() if len(names) == 1}
    for name in sorted(service_registry.analysed_services()):
        if registry.is_registered(name) and name not in mapped:
            warnings.append(f"{name} is analysed, but the reason-code service map "
                            f"places none of its reason codes with it alone, so "
                            f"its rejections are placed only by stage, topic or "
                            f"documentation.")
    return warnings


def validate(registry=None) -> tuple:
    """(errors, warnings) for the map this process will use."""
    errors, warnings = [], []
    if s3_key() and not s3_bucket():
        errors.append(f"{ENV_S3_KEY} is set but no S3 bucket is configured; set "
                      f"{ENV_S3_BUCKET}, CASEBOOK_S3_BUCKET or S3_LOGS_BUCKET.")
    path = map_path()
    if not path.is_file():
        # Not there yet when it is fetched from S3; /ready waits for it.
        return errors, warnings
    try:
        raw = path.read_bytes()
    except OSError as error:
        return errors + [f"{path} could not be read: {error}"], warnings
    service_map, parse_errors, parse_warnings = parse(raw, str(path))
    errors.extend(parse_errors)
    warnings.extend(parse_warnings)
    if service_map is not None:
        warnings.extend(coverage_warnings(service_map, registry))
    return errors, warnings
