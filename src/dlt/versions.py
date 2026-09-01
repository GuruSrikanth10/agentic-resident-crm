"""Phase C3 of DLT_PLAN.md section 14 -- parsing and ordering version strings.

The version number is the join key between a commit on `release` and the
process actually running, which makes this module the load-bearing arithmetic
of the whole precheck. It is also the single most likely place for the feature
to be quietly wrong for months, because a bad comparison looks exactly like a
good one: `"1.0.10" < "1.0.9"` is true as strings, wrong as versions, and
invisible until somebody audits a specific case. That is Trap T7, and it is
why this is its own module with its own hostile test table.

**The governing rule is that unparseable means unknown, never a guess.**
`parse()` returns None for anything without a numeric core, and every
comparison involving a None yields None, which C5 turns into an `UNKNOWN`
verdict. A wrong ordering causes a replay that fails again; an `UNKNOWN` only
costs a verdict.

Three shapes are in use:

    1.0.0-release.42     the image tag
    1.0.0                the pom's <version>
    1.0.1-SNAPSHOT       the pom mid-development

**Equal cores with an ambiguous qualifier compare equal, not ordered.** The
pom says `1.0.0` and the image says `1.0.0-release.42`; the second is a build
*of* the first, and nothing in either string says whether that build predates
or postdates a given commit. Reporting them equal is honest, and C5 treats
equality as `UNKNOWN` rather than as "deployed" -- which is what keeps Trap T9
(a fix merged with no version bump) from producing a false `FIX_DEPLOYED`.

Pure functions, no I/O.
"""
import os
import re
from dataclasses import dataclass
from typing import Optional, Sequence

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

#: A leading numeric core: `1`, `1.0`, `1.0.0`, `1.0.0.4`.
_CORE_RE = re.compile(r"^(\d+(?:\.\d+)*)")

#: A build ordinal at the very end of a qualifier: `release.42` -> 42.
#: Requires a separator, so `rc1` is not read as ordinal 1 -- `rc1` is a name,
#: `release.42` is a counter, and only the second one orders.
_ORDINAL_RE = re.compile(r"[.\-_](\d+)$")

#: Compiled `DLT_VERSION_PATTERN`, keyed on the raw setting so a reconfigured
#: value takes effect without a restart and a bad one is only logged once.
_pattern_cache = {}


def custom_pattern():
    """An optional operator-supplied regex naming where the version sits.

    The default parser wants the version at the front of the tag. A team
    tagging `enu-biometric-1.0.0` or `release-1.0.0` instead would otherwise
    get `UNKNOWN` on every case, and the fix would be a code change. With
    this, it is a config change:

        DLT_VERSION_PATTERN=^enu-biometric-(?P<version>.+)$

    A pattern without a `version` group, or one that does not compile, is
    logged and ignored -- a broken override degrades to the default parser
    rather than refusing every version in the system.
    """
    raw = os.environ.get("DLT_VERSION_PATTERN", "").strip()
    if not raw:
        return None
    if raw in _pattern_cache:
        return _pattern_cache[raw]

    compiled = None
    try:
        candidate = re.compile(raw)
        if "version" not in (candidate.groupindex or {}):
            logger.warning("DLT_VERSION_PATTERN has no (?P<version>...) group; "
                           "ignoring", pattern=raw)
        else:
            compiled = candidate
    except re.error as exc:
        logger.warning("DLT_VERSION_PATTERN is not a valid regex; ignoring",
                       pattern=raw, error=str(exc))

    _pattern_cache[raw] = compiled
    return compiled


@dataclass(frozen=True)
class Version:
    """One parsed version. Compared through `compare`, never with `<`.

    Deliberately not `order=True`: dataclass ordering would compare the fields
    in declaration order, starting with `raw`, which is a string comparison --
    exactly the bug this module exists to prevent.
    """

    raw: str
    #: The numeric core, e.g. `(1, 0, 0)`.
    core: tuple
    #: Everything after the core, `-` stripped. "" when there is none.
    qualifier: str = ""
    snapshot: bool = False
    #: The trailing build counter, when the qualifier ends in one.
    build: Optional[int] = None

    def __str__(self) -> str:
        return self.raw


# ---------------------------------------------------------------------------
# Extraction and parsing
# ---------------------------------------------------------------------------

def version_of(reference: Optional[str]) -> str:
    """Pull the version out of a container image reference.

    Both shapes in use here must work:

        mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric/1.0.0-release.4
        mndc-prod.harbor.uidai.net.in/ankalan/enu-biometric:1.0.0

    A trailing `@sha256:...` digest is stripped first. The digest identifies
    image *content* and says nothing about which commit built it, so it must
    never be mistaken for a version. The colon is only read as a tag separator
    when it appears in the last path segment, so a registry port
    (`host:5000/ns/app`) is not misread as one.

    Returns "" rather than None for anything unusable, so callers building a
    set of versions stay total.
    """
    if not reference:
        return ""
    head = str(reference).split("@", 1)[0].strip()
    if not head:
        return ""
    last = head.rsplit("/", 1)[-1]
    if ":" in last:
        return last.rsplit(":", 1)[-1]
    return last


def parse(text: Optional[str]) -> Optional[Version]:
    """Parse a version string, or an image reference containing one.

    None for anything without a numeric core -- `latest`, a branch name, a
    bare digest, empty text. Never raises, and never invents a core.
    """
    if text is None:
        return None

    raw = str(text).strip()
    if not raw:
        return None

    # Accept a full image reference so no caller has to remember to extract
    # first. Harmless for a bare version: `version_of("1.0.0")` is "1.0.0".
    raw = version_of(raw) or raw

    pattern = custom_pattern()
    if pattern is not None:
        found = pattern.search(raw)
        if found:
            raw = (found.group("version") or "").strip() or raw

    candidate = raw.lstrip("vV") if re.match(r"^[vV]\d", raw) else raw

    match = _CORE_RE.match(candidate)
    if not match:
        # No numeric core: `latest`, a branch name, a bare digest. Refusing
        # these is the point -- `latest` moves under your feet.
        return None

    core = tuple(int(part) for part in match.group(1).split("."))

    # The ordinal is searched on the tail *before* separators are stripped,
    # so `1.0.0-42` keeps its counter. Stripping first would leave "42" with
    # no separator for _ORDINAL_RE to anchor on.
    tail = candidate[match.end():]
    ordinal = _ORDINAL_RE.search(tail) if tail else None

    return Version(
        raw=raw,
        core=core,
        qualifier=tail.lstrip("-_."),
        snapshot="snapshot" in tail.lower(),
        build=int(ordinal.group(1)) if ordinal else None,
    )


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------

def _pad(a: tuple, b: tuple) -> tuple:
    """`(1, 0)` vs `(1, 0, 0)` must compare equal, not shorter-is-less."""
    width = max(len(a), len(b))
    return (a + (0,) * (width - len(a)), b + (0,) * (width - len(b)))


def compare(left, right) -> Optional[int]:
    """-1, 0 or 1 -- or None when either side could not be parsed.

    `left` and `right` may be strings, image references or `Version` objects.
    None is not an error to be swallowed: it means "no ordering is known", and
    a caller that treats it as 0 has just invented one.
    """
    a = left if isinstance(left, Version) else parse(left)
    b = right if isinstance(right, Version) else parse(right)
    if a is None or b is None:
        return None

    left_core, right_core = _pad(a.core, b.core)
    if left_core != right_core:
        return -1 if left_core < right_core else 1

    # Same core. A snapshot is the work leading up to that version, so it
    # precedes the released one.
    if a.snapshot != b.snapshot:
        return -1 if a.snapshot else 1

    # Build counters order only against each other. A bare `1.0.0` carries no
    # counter, so it is neither ahead of nor behind `1.0.0-release.42`.
    if a.build is not None and b.build is not None and a.build != b.build:
        return -1 if a.build < b.build else 1

    return 0


def at_least(candidate, required) -> Optional[bool]:
    """Is `candidate` at or beyond `required`? None when unknown.

    The question C5 asks of a running version against a fix's first-containing
    version. Kept separate from `compare` so a caller cannot accidentally
    coerce a None into False, which would silently read "unknown" as "the fix
    is not deployed" and park packets forever.
    """
    result = compare(candidate, required)
    return None if result is None else result >= 0


def is_ahead(candidate, baseline) -> Optional[bool]:
    """Strictly ahead. The safe reading for Trap T9.

    A fix merged without a version bump leaves the pom reading the same number
    that is already running; `at_least` would call that deployed and cause a
    replay that fails again. Requiring strictly-ahead gives up the boundary
    case and nothing else.
    """
    result = compare(candidate, baseline)
    return None if result is None else result > 0


def lowest(values: Sequence) -> Optional[Version]:
    """The lowest of several versions, or None if any cannot be parsed.

    Mid-rollout the pods disagree about what is running, and a replay may land
    on any of them -- so the lowest is the only safe reading. One unparseable
    entry poisons the answer rather than being skipped: the version that could
    not be parsed might be the low one, and quietly ignoring it would report a
    higher floor than actually exists.
    """
    if not values:
        return None

    parsed = []
    for value in values:
        version = value if isinstance(value, Version) else parse(value)
        if version is None:
            return None
        parsed.append(version)

    winner = parsed[0]
    for version in parsed[1:]:
        result = compare(version, winner)
        if result is None:
            return None
        if result < 0:
            winner = version
    return winner
