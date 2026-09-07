"""Phase 1 of DLT_PLAN.md -- stacktrace parsing, frame normalisation, fingerprinting.

The load-bearing module. Everything downstream -- grouping, recommendation
reuse, cost control -- depends on the fingerprint being *stable* across
occurrences of one bug and *distinct* across genuinely different bugs.

Two failure modes, both silent, both fatal:

* Over-grouping. Key on the wrapper exception and every failure in every
  Spring Kafka consumer collapses into one bucket, and one wrong
  recommendation is then served to all of them. This is why the root is taken
  from the *last* `Caused by:` and never from `kafka_exception-cause-fqcn`
  (DLT_PLAN.md 3.2, Trap 1).

* Under-grouping. Leave a line number or a JVM-generated class name in the
  fingerprint and no two occurrences of the same bug ever match, so the cache
  never hits and LLM cost scales with the full 2,000/day message rate.

Pure functions, no I/O.
"""
import hashlib
import os
import re
from dataclasses import dataclass
from typing import Optional, Sequence

#: Packages considered "ours". Frames outside these are framework or JDK noise
#: and are dropped before fingerprinting -- the reference sample's chain holds
#: 60+ frames, of which 9 are application frames.
DEFAULT_APP_PACKAGES = ("com.uidai.", "in.gov.uidai.")

DEFAULT_FINGERPRINT_FRAMES = 5

#: Application frames that are *exception plumbing* rather than failure sites.
#:
#: Found by running Phase 1 against the reference sample: the top application
#: frame of a `BusinessException` is `CommonErrorFactory.instantiateException`,
#: because that factory constructs every business exception in the codebase.
#: It is therefore identical for every Class A failure -- it contributes no
#: discriminating power to the fingerprint, displaces a frame that would, and
#: makes the human-readable signature name the factory instead of the code
#: that actually failed.
#:
#: Inferred from one sample. Phase 0's corpus should confirm it and reveal any
#: siblings; override with `DLT_BOILERPLATE_FRAMES` (empty value disables).
DEFAULT_BOILERPLATE_FRAMES = ("in.gov.uidai.common.factory.CommonErrorFactory",)

#: `\tat com.foo.Bar.baz(Bar.java:42)`. The location group is optional so a
#: frame written without one still parses.
_FRAME_RE = re.compile(r"^\s*at\s+(?P<target>[^\s(]+)(?:\((?P<location>.*)\))?\s*$")

#: `\t... 14 more` -- frames shared with the enclosing exception.
_ELIDED_RE = re.compile(r"^\s*\.\.\.\s+(?P<count>\d+)\s+more\s*$")

#: A plausible Java binary name, inner classes included.
_FQCN_RE = re.compile(r"^[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*)*$")

#: JVM- and framework-generated classes whose names carry a counter that
#: changes between runs, deployments, or even class-loads. Every one of these
#: appears in the reference sample. Left in, they guarantee that no two
#: occurrences of the same bug ever fingerprint alike.
#:
#: Note this deliberately requires a doubled `$$`, so a genuine application
#: lambda method (`AbstractBaseConsumer.lambda$executeConsumption$0`) survives:
#: its index is assigned at compile time and is stable across runs.
_SYNTHETIC_RE = re.compile(
    r"\$\$(?:SpringCGLIB|EnhancerBySpringCGLIB|FastClassBySpringCGLIB|Lambda)"
    r"|GeneratedMethodAccessor\d+"
    r"|GeneratedConstructorAccessor\d+"
    r"|\$Proxy\d+"
)

_CAUSED_BY = "\nCaused by: "


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def app_packages() -> tuple:
    raw = os.environ.get("DLT_APP_PACKAGES", "").strip()
    if not raw:
        return DEFAULT_APP_PACKAGES
    parsed = tuple(p.strip() for p in raw.split(",") if p.strip())
    return parsed or DEFAULT_APP_PACKAGES


def boilerplate_frames() -> tuple:
    """Frame prefixes to drop as exception plumbing.

    Unset uses the default; explicitly empty disables the filter entirely, so
    an operator who disagrees with the default can turn it off without a code
    change (the fingerprints then shift, which is why it is config).
    """
    raw = os.environ.get("DLT_BOILERPLATE_FRAMES")
    if raw is None:
        return DEFAULT_BOILERPLATE_FRAMES
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def fingerprint_frame_count() -> int:
    try:
        return max(1, int(os.environ.get("DLT_FINGERPRINT_FRAMES",
                                         str(DEFAULT_FINGERPRINT_FRAMES))))
    except (ValueError, TypeError):
        return DEFAULT_FINGERPRINT_FRAMES


def fingerprint_includes_type() -> bool:
    """Whether `__TypeId__` is a fingerprint dimension.

    A group's recommendation is written once and re-served verbatim to every
    later member, and the narrative describes the payload the failure occurs
    on. That is safe while the DLT carries one payload structure. It stops
    being safe with several: two topics failing in a shared library or a
    common consumer base class share a root exception, a business code and
    their top application frames, so they collapse into one group -- and the
    first topic's payload description is then served against the second's
    records, where it is simply wrong.

    Off by default, because turning it on re-hashes every failure mode: live
    groups fragment once, occurrence counts restart from zero and stored
    recommendations are orphaned. That is Risk R3 paid deliberately, and it
    buys nothing until a second structure actually reaches the DLT -- so it is
    a deployment decision, not a release. Turn it on with the second topic.

    Reads `os.environ` directly rather than through `src.utils.env`, like the
    two settings above it: that module calls `load_dotenv()` on import, and
    this one is imported by pure-parsing tools that should not acquire a
    dotenv read as a side effect.
    """
    raw = os.environ.get("DLT_FINGERPRINT_TYPE_ID", "").strip().lower()
    return raw in ("true", "1", "yes")


# ---------------------------------------------------------------------------
# Parsed shapes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FrameLocation:
    """Where one application frame sits in the source (phase C2).

    Kept beside the fingerprint's frame list, never inside it. `BioDeDup-
    licationServiceImpl` is a 4,000-line class whose line numbers shift on
    every release, so a line number in the fingerprint would fragment a group
    that should be stable (9.3, Risk R3). A line number is nonetheless exactly
    what a source lookup wants, hence two projections of one parse.
    """

    #: The full frame target, e.g. `com.foo.BarService.doWork`.
    target: str
    #: `BarService.java`, or None for `Native Method` / `Unknown Source`.
    file: Optional[str] = None
    #: None when the trace was written without one, or the frame is native.
    line: Optional[int] = None

    @property
    def class_fqcn(self) -> str:
        """The declaring class, inner-class marker included."""
        return self.target.rsplit(".", 1)[0] if "." in self.target else self.target

    @property
    def package(self) -> str:
        outer = self.class_fqcn.split("$", 1)[0]
        return outer.rsplit(".", 1)[0] if "." in outer else ""

    @property
    def source_path_suffix(self) -> str:
        """`com/foo/BarService.java` -- the tail of the repository path.

        Built from the package plus the *file name the JVM reported*, not from
        the class name, because those differ for an inner class: a frame in
        `com.foo.Outer$Inner.run` reports `Outer.java`, which is the file that
        actually exists. Falls back to the outer class name when the trace
        carried no file (a native or synthetic frame).
        """
        name = self.file
        if not name or not name.endswith(".java"):
            outer = self.class_fqcn.split("$", 1)[0]
            simple = outer.rsplit(".", 1)[-1] if "." in outer else outer
            if not simple:
                return ""
            name = f"{simple}.java"
        package = self.package
        return f"{package.replace('.', '/')}/{name}" if package else name

    def as_dict(self) -> dict:
        return {"target": self.target, "file": self.file, "line": self.line}


@dataclass(frozen=True)
class ExceptionLink:
    """One `Caused by:` level."""

    fqcn: str
    message: str
    #: Raw frame targets, module prefix and location stripped, in trace order.
    frames: tuple
    #: The `... N more` count, when the link ends with one.
    elided: Optional[int] = None
    #: Raw `(File.java:42)` text per frame, index-parallel with `frames`. A
    #: frame written without a location holds None, so the two tuples stay
    #: aligned rather than silently shifting.
    locations: tuple = ()

    @property
    def simple_name(self) -> str:
        return self.fqcn.rsplit(".", 1)[-1] if self.fqcn else ""


@dataclass(frozen=True)
class ParsedTrace:
    """A full exception chain, outermost link first."""

    chain: tuple
    truncated: bool

    @property
    def root(self) -> Optional[ExceptionLink]:
        """The innermost cause -- the only link worth fingerprinting."""
        return self.chain[-1] if self.chain else None

    @property
    def root_frames(self) -> tuple:
        root = self.root
        return root.frames if root else ()

    @property
    def root_locations(self) -> tuple:
        """Index-parallel with `root_frames`. Phase C2."""
        root = self.root
        return root.locations if root else ()

    @property
    def depth(self) -> int:
        return len(self.chain)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _strip_module(target: str) -> str:
    """`java.base/java.lang.Thread.run` -> `java.lang.Thread.run`."""
    return target.split("/", 1)[-1] if "/" in target else target


def _parse_link(block: str) -> ExceptionLink:
    """Parse one chain link: a header, then frames, then an optional elision.

    The header may span several lines -- an exception message is free text and
    can contain newlines -- so it is everything up to the first frame line.
    """
    header_lines = []
    frames = []
    locations = []
    elided = None
    in_frames = False

    for line in block.split("\n"):
        frame_match = _FRAME_RE.match(line)
        if frame_match:
            in_frames = True
            frames.append(_strip_module(frame_match.group("target")))
            # Kept index-parallel with `frames`. The location group is
            # optional, so a frame written without one appends None rather
            # than nothing -- otherwise the two lists drift apart and every
            # location after the first bare frame refers to the wrong frame.
            locations.append(frame_match.group("location"))
            continue

        elided_match = _ELIDED_RE.match(line)
        if elided_match:
            in_frames = True
            elided = int(elided_match.group("count"))
            continue

        if not in_frames:
            header_lines.append(line)

    header = "\n".join(header_lines).strip()

    fqcn, message = "", header
    if header:
        candidate_fqcn, _, candidate_message = header.partition(": ")
        if _FQCN_RE.match(candidate_fqcn):
            fqcn, message = candidate_fqcn, candidate_message.strip()
        elif _FQCN_RE.match(header):
            # A class name with no message at all.
            fqcn, message = header, ""

    return ExceptionLink(fqcn=fqcn, message=message,
                         frames=tuple(frames), elided=elided,
                         locations=tuple(locations))


def parse_stacktrace(text: Optional[str]) -> ParsedTrace:
    """Split a Java stacktrace into its `Caused by:` chain.

    Never raises. A stacktrace that is absent, empty, or cut mid-frame yields
    a `ParsedTrace` flagged `truncated`, which downstream phases treat as
    Class U rather than fingerprinting a wrapper by mistake.
    """
    if not text or not text.strip():
        return ParsedTrace(chain=(), truncated=True)

    chain = tuple(_parse_link(block) for block in text.split(_CAUSED_BY))

    # A well-formed link ends in either frames or a `... N more` elision.
    # A final link with neither means the header cut the trace short.
    last = chain[-1]
    truncated = not last.frames and last.elided is None

    return ParsedTrace(chain=chain, truncated=truncated)


# ---------------------------------------------------------------------------
# Normalisation and fingerprinting
# ---------------------------------------------------------------------------

def is_synthetic(target: str) -> bool:
    """Is this frame a JVM/framework-generated class with an unstable name?"""
    return bool(_SYNTHETIC_RE.search(target or ""))


def normalise_frames(frames: Sequence,
                     packages: Optional[Sequence] = None,
                     boilerplate: Optional[Sequence] = None) -> tuple:
    """Reduce raw frame targets to the stable application-code subset.

    Three filters, in order: keep only application packages, drop
    JVM/framework-generated classes, drop exception plumbing.

    Line numbers are already absent -- `_parse_link` keeps only the target,
    discarding the `(File.java:4067)` location. `BioDeDuplicationServiceImpl`
    is a 4,000+ line class whose line numbers shift on every release, so
    keeping them would fragment a group that should be stable
    (DLT_PLAN.md 9.3, an accepted tradeoff: two distinct bugs in one method
    will merge).
    """
    prefixes, noise = _filters(packages, boilerplate)
    return tuple(frame for frame in frames if _is_app_frame(frame, prefixes, noise))


def _filters(packages: Optional[Sequence],
             boilerplate: Optional[Sequence]) -> tuple:
    prefixes = tuple(packages) if packages else app_packages()
    noise = tuple(boilerplate) if boilerplate is not None else boilerplate_frames()
    return prefixes, noise


def _is_app_frame(frame: Optional[str], prefixes: tuple, noise: tuple) -> bool:
    """The single keep/drop rule. Shared so the frame list the fingerprint is
    built from and the location list a source lookup uses can never disagree
    about which frames are ours."""
    return bool(
        frame
        and frame.startswith(prefixes)
        and not is_synthetic(frame)
        and not (noise and frame.startswith(noise))
    )


def split_location(text: Optional[str]) -> tuple:
    """`BarService.java:42` -> `("BarService.java", 42)`.

    `Native Method`, `Unknown Source` and a bare file name all yield a None
    line rather than a guess. A non-numeric suffix is kept as part of the file
    name, since inventing a line number is worse than having none.
    """
    if not text:
        return None, None
    value = text.strip()
    if not value:
        return None, None
    name, separator, tail = value.rpartition(":")
    if separator and tail.isdigit():
        return (name or None), int(tail)
    return value, None


def normalise_frame_locations(frames: Sequence,
                              locations: Sequence,
                              packages: Optional[Sequence] = None,
                              boilerplate: Optional[Sequence] = None) -> tuple:
    """`FrameLocation` per application frame, in the same order and under the
    same filter as `normalise_frames` (phase C2).

    The result is index-parallel with `normalise_frames(frames)`, so
    `locations[0]` describes the failure site that `build_signature` names.
    """
    prefixes, noise = _filters(packages, boilerplate)
    kept = []
    for index, frame in enumerate(frames):
        if not _is_app_frame(frame, prefixes, noise):
            continue
        raw = locations[index] if index < len(locations) else None
        file_name, line = split_location(raw)
        kept.append(FrameLocation(target=frame, file=file_name, line=line))
    return tuple(kept)


def compute_fingerprint(root_fqcn: Optional[str],
                        normalised_frames: Sequence,
                        business_code: str = "",
                        limit: Optional[int] = None,
                        type_id: Optional[str] = None) -> str:
    """Stable SHA256 identity for one failure mode.

    `business_code` is supplied by Phase 2's classifier; Phase 1 computes
    fingerprints without one. It is a distinct dimension rather than part of
    the message because two different codes raised from the same frame are
    genuinely different failures.

    `type_id` is the payload's `__TypeId__`, and it is a dimension only when
    `fingerprint_includes_type()` says so -- see that function for why the
    default is off. It is **appended** to the hashed string rather than
    interleaved, so a fingerprint computed without it stays byte-identical to
    every fingerprint this function has ever returned. Do not reorder these
    parts to tidy them up: the order is the compatibility guarantee.
    """
    count = fingerprint_frame_count() if limit is None else max(1, limit)
    parts = [
        (root_fqcn or "").strip(),
        (business_code or "").strip(),
        "\n".join(normalised_frames[:count]),
    ]

    normalised_type = (type_id or "").strip()
    if normalised_type and fingerprint_includes_type():
        parts.append(normalised_type)

    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def build_signature(root_fqcn: Optional[str],
                    normalised_frames: Sequence,
                    business_code: str = "") -> str:
    """A human-readable label for a fingerprint.

    `BusinessException[UID_ORIGIN_TRACKER_DATA_NOT_FOUND] @ BioDataBaseHelperServiceImpl.getUidOriginTrackerData`

    An operator reads this in `dlt_report`; the hash is for machines.
    """
    simple = root_fqcn.rsplit(".", 1)[-1] if root_fqcn else "UnknownException"
    code = f"[{business_code}]" if business_code else ""

    if not normalised_frames:
        return f"{simple}{code}"

    top = normalised_frames[0]
    parts = top.rsplit(".", 2)
    location = ".".join(parts[-2:]) if len(parts) >= 2 else top
    return f"{simple}{code} @ {location}"
