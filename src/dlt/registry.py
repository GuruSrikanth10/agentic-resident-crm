"""Phase 2 of DLT_PLAN.md -- the reason-code catalog.

Maps an enumerated error code (`UID_ORIGIN_TRACKER_DATA_NOT_FOUND`) to the
one-line description the organisation publishes for it, and -- since the
`BusinessReasonCode` source was imported -- to the **category** that source
declares for it.

The category is the part that changes behaviour. `BusinessReasonCode implements
IRejectCode`, so every one of these codes can arrive inside a
`BusinessException`, and 198 of them are declared `TECHNICAL_EXCEPTION`. Without
the catalog, `BusinessException: [KAFKA_PRODUCER_EXCEPTION]` reads as a business
failure, goes to the expensive lane, and comes back with a narrative about
business rules for what is actually a Kafka publish error whose treatment is
"redrive once the broker recovers". `class_for()` is what lets `classify()` tell
those apart. See `src/tools/parse_reason_codes.py` for how the catalog is built.

**A seed, not an answer.** One line is enough to anchor and classify; it is not
enough to recommend. Phase 8's prompt treats a description as context and is
explicitly forbidden from inventing detail beyond it -- with no source and no
database access, a per-packet cause would be invention (DLT_PLAN.md 9.1).

**`category_source` is load-bearing.** A category the Java source *declared* and
one this project *inferred* from a numeric id range are not the same evidence,
and a row records which it is. Nothing here silently promotes an inference.

Every failure mode is a *miss*, never an exception: a missing file, an
unreadable file, an unparseable row, an unknown code, a column that is not
there. A catalog problem must degrade the confidence of a finding, not cost us
the message. Risk R7 in the plan is that the file arrives in an unexpected
format -- which is why the whole loader is isolated behind `lookup()`, and why
a catalog with only `code,description` columns still loads and simply carries
no category.
"""
import csv
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from src.utils.logging_config import get_logger
from src.utils.paths import REPO_ROOT

logger = get_logger(__name__)

DEFAULT_REGISTRY_FILENAME = "reason_codes.csv"

#: Accepted spellings for the code column, lower-cased and stripped.
_CODE_COLUMNS = ("code", "reason_code", "reasoncode", "error_code", "errorcode",
                 "business_code", "businesscode", "key")
#: Accepted spellings for the description column.
_DESC_COLUMNS = ("description", "desc", "message", "reason", "text", "detail",
                 "error_description")
#: Optional columns. Absent ones leave the entry's field empty, which every
#: consumer already treats as "no opinion".
_CATEGORY_COLUMNS = ("category", "reason_category", "error_reason_category")
_CATEGORY_SOURCE_COLUMNS = ("category_source", "categorysource")
_CLASS_COLUMNS = ("failure_class", "class", "failureclass")
_STAGE_COLUMNS = ("stage",)
_NUMERIC_ID_COLUMNS = ("numeric_id", "id", "numericid")

#: Failure classes a catalog row is allowed to assert. `B` is excluded on
#: purpose: a code defect is identified by its *exception type*, never by a
#: reject code, and a catalog that could assert B would let a data file route
#: cases to the "no diagnosis possible" lane.
_ALLOWED_CLASSES = ("A", "C")


@dataclass(frozen=True)
class ReasonCode:
    """One catalog row. Every field beyond `code` may legitimately be empty."""

    code: str
    description: str = ""

    #: `BUSINESS_VALIDATION_ERROR`, `BUSINESS_EXCEPTION`, `TECHNICAL_EXCEPTION`,
    #: or "" when the catalog carries no category for this code.
    category: str = ""

    #: "declared" (the Java source said so) or "inferred" (this project derived
    #: it from a numeric id range). Never treat the two as equivalent.
    category_source: str = ""

    #: The DLT failure class the category implies: "A", "C", or "".
    failure_class: str = ""

    stage: str = ""
    numeric_id: str = ""

    @property
    def is_declared(self) -> bool:
        return self.category_source == "declared"

_cache: dict = {}
_cache_lock = threading.Lock()


def registry_path() -> Path:
    """Where the registry lives. Relative values resolve against the repo root."""
    raw = os.environ.get("DLT_REGISTRY_PATH", "").strip() or DEFAULT_REGISTRY_FILENAME
    path = Path(raw)
    return path if path.is_absolute() else REPO_ROOT / path


def _pick_column(fieldnames, candidates) -> Optional[str]:
    normalised = {(name or "").strip().lower(): name for name in (fieldnames or [])}
    for candidate in candidates:
        if candidate in normalised:
            return normalised[candidate]
    return None


def _class_from(row_value: str, category: str) -> str:
    """The failure class for a row: the column if usable, else the category.

    A catalog is data, and data can say anything. An unrecognised class is
    dropped rather than propagated -- `classify()` acts on this value, so a
    typo'd column must not be able to route cases into a lane nobody chose.
    """
    text = (row_value or "").strip().upper()
    if text in _ALLOWED_CLASSES:
        return text
    if text:
        logger.warning("Ignoring an unusable failure_class in the reason-code "
                       "catalog", value=row_value)
    return _CLASS_BY_CATEGORY.get((category or "").strip().upper(), "")


#: Fallback when a catalog carries a category but no `failure_class` column.
_CLASS_BY_CATEGORY = {
    "BUSINESS_VALIDATION_ERROR": "A",
    "BUSINESS_EXCEPTION": "A",
    "TECHNICAL_EXCEPTION": "C",
}


def _parse(path: Path) -> dict:
    """Read the CSV into {code: ReasonCode}.

    Tries named columns first, then falls back to positional (first column is
    the code, second the description), skipping a header row if the first cell
    looks like one. Only `code` is required; a two-column file from before the
    catalog existed still loads and simply carries no category.
    """
    entries = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(8192)
        handle.seek(0)
        if not sample.strip():
            return entries

        reader = csv.DictReader(handle)
        code_col = _pick_column(reader.fieldnames, _CODE_COLUMNS)
        desc_col = _pick_column(reader.fieldnames, _DESC_COLUMNS)
        cat_col = _pick_column(reader.fieldnames, _CATEGORY_COLUMNS)
        src_col = _pick_column(reader.fieldnames, _CATEGORY_SOURCE_COLUMNS)
        class_col = _pick_column(reader.fieldnames, _CLASS_COLUMNS)
        stage_col = _pick_column(reader.fieldnames, _STAGE_COLUMNS)
        id_col = _pick_column(reader.fieldnames, _NUMERIC_ID_COLUMNS)

        if code_col:
            for row in reader:
                code = (row.get(code_col) or "").strip()
                if not code:
                    continue

                def cell(column):
                    return (row.get(column) or "").strip() if column else ""

                category = cell(cat_col)
                entries[code] = ReasonCode(
                    code=code,
                    description=cell(desc_col),
                    category=category,
                    category_source=cell(src_col),
                    failure_class=_class_from(cell(class_col), category),
                    stage=cell(stage_col),
                    numeric_id=cell(id_col),
                )
            return entries

        # No recognisable header -- treat it as positional data.
        handle.seek(0)
        for index, row in enumerate(csv.reader(handle)):
            if not row:
                continue
            code = (row[0] or "").strip()
            if not code:
                continue
            if index == 0 and code.lower() in _CODE_COLUMNS:
                continue
            entries[code] = ReasonCode(
                code=code,
                description=(row[1] or "").strip() if len(row) > 1 else "")
    return entries


def load_catalog(path: Optional[Path] = None) -> dict:
    """Return {code: ReasonCode}, cached on the file's identity and mtime.

    Keying the cache on (path, mtime, size) means an operator can regenerate
    the catalog without restarting the consumers, while the steady state costs
    one `stat`.
    """
    target = Path(path) if path else registry_path()

    try:
        stat = target.stat()
        key = (str(target), stat.st_mtime_ns, stat.st_size)
    except OSError:
        logger.warning("Reason-code catalog not readable; every lookup "
                       "will miss", path=str(target))
        return {}

    with _cache_lock:
        cached = _cache.get(key)
        if cached is not None:
            return cached

    try:
        entries = _parse(target)
    except (OSError, csv.Error, UnicodeDecodeError, IndexError) as exc:
        logger.warning("Reason-code catalog could not be parsed; every "
                       "lookup will miss", path=str(target), error=str(exc))
        entries = {}
    else:
        categorised = sum(1 for e in entries.values() if e.failure_class)
        logger.info("Loaded reason-code catalog", path=str(target),
                    codes=len(entries), with_failure_class=categorised)

    with _cache_lock:
        _cache[key] = entries
    return entries


def load_registry(path: Optional[Path] = None) -> dict:
    """Return {code: description}. The description-only view of the catalog."""
    return {code: entry.description
            for code, entry in load_catalog(path).items()}


def lookup(code: Optional[str], path: Optional[Path] = None) -> Optional[str]:
    """Description for a code, or None.

    Case-exact after trimming: these are enumerated constants, and quietly
    matching `uid_origin_tracker_data_not_found` against a differently-cased
    entry would hide a real registry mismatch.
    """
    entry = lookup_entry(code, path)
    return entry.description if entry is not None else None


def lookup_entry(code: Optional[str],
                 path: Optional[Path] = None) -> Optional[ReasonCode]:
    """The full catalog row for a code, or None. Case-exact after trimming."""
    if not code:
        return None
    return load_catalog(path).get(str(code).strip())


def class_for(code: Optional[str], path: Optional[Path] = None) -> Optional[str]:
    """The failure class the catalog asserts for a business code, or None.

    Passed into `classify()` as its `code_class` hook. Returning None for an
    unknown code is what keeps an absent or partial catalog a no-op: the
    stacktrace-based classification stands, exactly as before this file existed.
    """
    entry = lookup_entry(code, path)
    return (entry.failure_class or None) if entry is not None else None


def clear_cache() -> None:
    """Drop the cached registry. For tests and for operator CLIs."""
    with _cache_lock:
        _cache.clear()
