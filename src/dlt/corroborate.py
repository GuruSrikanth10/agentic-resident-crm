"""Phase 6 of DLT_PLAN.md -- checking the declared trace against the logs.

**The stack trace is a claim, not ground truth.** Application code catches a
technical fault and rethrows it as a business exception; when that happens the
trace confidently names the wrong root cause and every consumer of it -- human
or machine -- inherits the error. Logs from the same pod at the same instant
are the only available check.

Surfacing that discrepancy is the highest-value output of this system, because
it is the one thing a developer reading the trace in Kafka UI structurally
cannot see. It is also the entire justification for the log lane: without it,
the headers alone would do.

    CORROBORATED   the declared root appears in the logs
    PARTIAL        it appears, but so do unexplained errors
    CONTRADICTED   it does not appear, and something else failed instead
    UNVERIFIABLE   no logs, no identifier, or nothing error-level matched

A verdict never adjudicates on its own. `CONTRADICTED` escalates to the LLM
lane with the discrepancy as the framing question, and every verdict carries
the specific lines it relied on so a human can check the machine's reading.

**Not every error line in the window belongs to this packet.** The Kubernetes
source cannot grep server-side, so it emits each identifier match plus the
lines around it, and on a pod serving packets concurrently those neighbours
are other refIds' lines. Flattened to text they are indistinguishable from
this packet's, and a neighbour's exception read as "an error the trace does
not explain" turns every busy-pod case into PARTIAL -- or, when the declared
root happens not to sit on an error-level line, into a CONTRADICTED that tells
a developer their trace is lying about a failure that was never theirs.

So the pipeline marks those lines (`types.CONTEXT_LINE_MARKER`) and this
module reads the mark, asymmetrically:

  * the declared root may be matched anywhere in the window, context lines
    included -- generous, for the same reason the FQCN/simple-name/code
    matching is generous;
  * an *unexplained* exception counts only from lines that carried the
    identifier, because that is the claim the verdict actually makes.

Text with no marks at all -- Elasticsearch, which filters server-side, and
artifacts written before the mark existed -- is read exactly as before.

**No real mis-cast example exists yet** (Open Question 2). The thresholds are
deliberately conservative and the verdict is advisory until real samples
validate it.

Pure functions, no I/O.
"""
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import NamedTuple, Optional, Sequence

from src.log_pipeline.types import CONTEXT_LINE_MARKER

#: Lines strong enough to *accuse the trace with*. Matching on word boundaries
#: so "TERROR" or "error_code=0" do not count.
_ERROR_LINE = re.compile(r"\b(ERROR|FATAL|SEVERE)\b")

#: Lines that may *corroborate* the trace. Wider than `_ERROR_LINE` on
#: purpose, and the asymmetry is the point.
#:
#: Spring services routinely catch a fault, log it at WARN, and rethrow it as
#: a business exception -- which is precisely the shape this module exists to
#: inspect. Requiring ERROR to find the declared root therefore manufactures
#: CONTRADICTED verdicts on the most ordinary logging convention there is: the
#: root is right there in the window at WARN, we decline to look at it, some
#: unrelated ERROR is present, and we tell a developer their trace is lying.
#:
#: The reverse widening would be a mistake. A Spring WARN stream is mostly
#: *recovered* faults -- retry notices, circuit-breaker transitions, "caught
#: X, falling back" -- and counting their exceptions as errors the trace fails
#: to explain would make PARTIAL the default verdict, which `reuse.decide`
#: turns into an LLM call per occurrence. So a WARN line can confirm the
#: declared root and can never convict it.
#:
#: `WARNING` spelled out as well: the Kubernetes parser normalises it to WARN,
#: but Elasticsearch passes `level` through untouched.
_MATCHABLE_LINE = re.compile(r"\b(ERROR|FATAL|SEVERE|WARN(?:ING)?)\b")

#: A Java exception FQCN appearing in free log text.
_FQCN_IN_TEXT = re.compile(r"\b((?:[a-z][\w$]*\.){2,}[A-Z][\w$]*(?:Exception|Error|Throwable))\b")

#: A rendered log line opens with its bracketed prefix -- `[ts] [origin]
#: [LEVEL]`, optionally behind the ERROR branch's ` *** ` gutter. Anything
#: else continues the line above it: a stack trace inside one record's
#: message renders as several physical lines, and only the first carries the
#: prefix and therefore the context mark. A continuation inherits its
#: opener's attribution rather than defaulting to "this packet's".
_LINE_START = re.compile(r"^\s*(?:\*\*\*\s*)?\[")

#: How many cited lines to keep per verdict. Enough for a human to judge,
#: bounded so a 4,000-line retry storm cannot land in the casebook.
MAX_CITATIONS = 20


class Verdict(str, Enum):
    CORROBORATED = "CORROBORATED"
    PARTIAL = "PARTIAL"
    CONTRADICTED = "CONTRADICTED"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class Corroboration:
    verdict: Verdict
    reason: str
    #: Log lines the verdict rests on, so a human can check the reading.
    citations: tuple = ()
    #: Exception FQCNs seen in the logs that the trace does not explain.
    unexplained: tuple = ()
    matched_declared: bool = False
    error_lines_seen: int = 0
    #: Distinguishes "could not look" from "looked and found nothing", the
    #: same distinction `FetchResult.ok` makes in the log pipeline. Collapsing
    #: them would let a finding read as "no errors occurred" when the truth is
    #: "we could not read the logs".
    could_not_look: bool = False
    details: dict = field(default_factory=dict)

    @property
    def is_discrepancy(self) -> bool:
        return self.verdict in (Verdict.CONTRADICTED, Verdict.PARTIAL)


#: Text the fetch stage writes when it deliberately skipped the fetch. These
#: mean "could not look", not "looked and found nothing".
_SKIP_MARKERS = (
    "No refId available",
    "No usable timestamp",
    "Log window too old",
    "Log fetch failed",
    "Log fetching disabled",
)

_EMPTY_MARKERS = ("No logs found for ID:",)


def _simple_name(fqcn: Optional[str]) -> str:
    return fqcn.rsplit(".", 1)[-1] if fqcn else ""


def _matches_any(text: str, *needles) -> bool:
    """Does any spelling of the declared root appear in `text`?

    Shared by the verdict's match test and by the check for whether that match
    landed only on a context line, so the two cannot answer differently.
    """
    return any(needle and needle in text for needle in needles)


class ScannedLine(NamedTuple):
    """One line of the window, with the two things that bound what it can prove.

    Both flags narrow the same way: a line may corroborate the declared root
    however weak it is, and may only convict it when strong on both axes.
    """

    text: str
    #: Did the line carry the identifier we searched for, rather than arriving
    #: as surrounding context? See CONTEXT_LINE_MARKER.
    attributed: bool
    #: Was it error-level, rather than a WARN that may well be a handled fault?
    error_level: bool


def _scan_lines(logs: str) -> list:
    """Return a `ScannedLine` for every error- or warning-level line, in order.

    `attributed` is False only for a line the pipeline marked as context --
    kept because it sat near an identifier match, not because it carried the
    id. A line with no prefix of its own continues the line above it and
    inherits that line's attribution; text containing no marks at all is
    entirely attributed, which is what makes this a no-op for Elasticsearch
    and for artifacts written before the mark existed.
    """
    scanned = []
    inherited = True
    for line in logs.splitlines():
        if _LINE_START.match(line):
            inherited = CONTEXT_LINE_MARKER not in line
        if _MATCHABLE_LINE.search(line):
            scanned.append(ScannedLine(line.strip(), inherited,
                                       bool(_ERROR_LINE.search(line))))
    return scanned


def _error_lines(logs: str) -> list:
    """Every error-level line, attributed or not. Citations show both: a human
    checking the reading needs to see what was set aside as much as what was
    counted."""
    return [line.text for line in _scan_lines(logs) if line.error_level]


def corroborate(logs: Optional[str],
                root_fqcn: Optional[str],
                business_code: Optional[str] = None,
                frames: Optional[Sequence] = None) -> Corroboration:
    """Compare a declared root cause against the fetched logs.

    Matching is deliberately generous about *how* the declared root appears --
    the full FQCN, its simple name, or its business code all count, at ERROR
    or at WARN, on a line carrying our identifier or on one that merely sat
    beside it. Services log exceptions in all those shapes, and a false
    CONTRADICTED is far more damaging than a missed one: it would tell a
    developer their trace is lying when it is not.

    Accusing the trace is the strict half. An exception counts as one the
    trace fails to explain only when it appears on a line that is both
    error-level and carrying this packet's identifier -- the two axes tracked
    on `ScannedLine`. What the weaker lines held is reported rather than
    discarded, so a human can see what was set aside and why.
    """
    if not logs or not logs.strip():
        return Corroboration(Verdict.UNVERIFIABLE, "no logs were fetched",
                             could_not_look=True)

    if any(marker in logs for marker in _SKIP_MARKERS):
        return Corroboration(Verdict.UNVERIFIABLE,
                             "the log fetch was skipped or failed",
                             could_not_look=True)

    if any(marker in logs for marker in _EMPTY_MARKERS):
        return Corroboration(Verdict.UNVERIFIABLE,
                             "the log source returned no lines for this identifier",
                             could_not_look=False)

    scanned = _scan_lines(logs)
    error_lines = [line.text for line in scanned if line.error_level]
    if not scanned:
        return Corroboration(
            Verdict.UNVERIFIABLE,
            "logs were fetched but contain no error- or warning-level lines",
            error_lines_seen=0,
            could_not_look=False,
        )

    # Four readings of the same window, from the widest to the narrowest.
    # `haystack` answers "does the declared root appear anywhere here" and so
    # takes everything; `accusable` answers "what else failed *to this
    # packet*" and so takes only lines strong on both axes. Answering the
    # second question from the first is the bug this split exists to fix. The
    # two set-aside buckets are what the narrowing dropped, kept so the
    # verdict can say what it did not count.
    haystack = "\n".join(line.text for line in scanned)
    accusable = "\n".join(line.text for line in scanned
                          if line.attributed and line.error_level)
    set_aside_context = "\n".join(line.text for line in scanned
                                  if not line.attributed)
    set_aside_warn = "\n".join(line.text for line in scanned
                               if line.attributed and not line.error_level)
    simple = _simple_name(root_fqcn)

    # Per line rather than over the joined text, so the verdict knows which
    # line it matched on and can cite it. A CORROBORATED whose citations do
    # not contain the match is not checkable -- and once WARN lines can carry
    # the match, citing only error lines would leave it uncitable outright.
    matched = False
    matched_on = None
    matched_line = None
    for needle, label in ((root_fqcn, "fqcn"), (simple, "simple name"),
                          (business_code, "business code")):
        if not needle:
            continue
        for line in scanned:
            if needle in line.text:
                matched, matched_on, matched_line = True, label, line
                break
        if matched:
            break

    # Frames are corroborating, not decisive: seeing the failing method in the
    # logs supports the trace without proving which exception left it.
    frame_hits = tuple(
        frame for frame in (frames or [])
        if frame and frame.rsplit(".", 1)[-1] and frame.rsplit(".", 1)[-1] in haystack
    )

    def _unexplained_in(text: str) -> tuple:
        return tuple(sorted(
            fqcn for fqcn in set(_FQCN_IN_TEXT.findall(text))
            if fqcn != root_fqcn and _simple_name(fqcn) != simple
        ))

    seen_fqcns = set(_FQCN_IN_TEXT.findall(accusable))
    unexplained = _unexplained_in(accusable)
    # Held separately rather than discarded. These are real exceptions that
    # really happened in the window; they are just not evidence *about this
    # packet* -- one belongs to another transaction, the other was logged at a
    # level that usually means the service handled it. A human checking a
    # verdict needs to see what was set aside as much as what was counted.
    set_aside = {
        "exceptions_on_context_lines": tuple(
            fqcn for fqcn in _unexplained_in(set_aside_context)
            if fqcn not in unexplained),
        "exceptions_on_warn_lines": tuple(
            fqcn for fqcn in _unexplained_in(set_aside_warn)
            if fqcn not in unexplained),
    }

    # The matched line leads, then the error lines in order. Deduped, so a
    # match that was already an error line is not cited twice, and bounded as
    # a whole so a retry storm still cannot land in the casebook.
    cited = ([matched_line.text] if matched_line else []) + error_lines
    citations = tuple(dict.fromkeys(cited))[:MAX_CITATIONS]

    details = {
        "matched_on": matched_on,
        "frame_hits": list(frame_hits),
        "exceptions_in_logs": sorted(seen_fqcns),
        # What the narrowing dropped, and how much of it there was.
        "exceptions_on_context_lines": list(set_aside["exceptions_on_context_lines"]),
        "exceptions_on_warn_lines": list(set_aside["exceptions_on_warn_lines"]),
        "context_error_lines": sum(1 for line in scanned if not line.attributed),
        "warn_lines_seen": sum(1 for line in scanned if not line.error_level),
        # Two ways a match can be weaker than it looks. Both still count as
        # matches -- a false CONTRADICTED is the more damaging error -- but
        # neither passes silently.
        #
        # Context-only: the root turned up on a line belonging to another
        # transaction, most likely a sibling packet hitting the same bug at
        # the same instant.
        "matched_in_context_only": bool(
            matched and not _matches_any(
                "\n".join(line.text for line in scanned if line.attributed),
                root_fqcn, simple, business_code)
        ),
        # WARN-only: the service logged the root as a handled fault. Ordinary
        # Spring practice, and the reason `_MATCHABLE_LINE` is wider than
        # `_ERROR_LINE` at all.
        "matched_on_warn_only": bool(
            matched and not _matches_any(
                "\n".join(line.text for line in scanned if line.error_level),
                root_fqcn, simple, business_code)
        ),
    }

    if matched and not unexplained:
        qualifier = ""
        if details["matched_on_warn_only"]:
            qualifier = ", logged at warning level"
        if details["matched_in_context_only"]:
            qualifier += ", on a line that does not carry this packet's identifier"
        return Corroboration(
            Verdict.CORROBORATED,
            f"the declared root appears in the logs (matched on its "
            f"{matched_on}{qualifier})",
            citations=citations, matched_declared=True,
            error_lines_seen=len(error_lines), details=details)

    if matched and unexplained:
        return Corroboration(
            Verdict.PARTIAL,
            "the declared root appears, but this packet's own log lines also "
            "hold errors it does not explain",
            citations=citations, unexplained=unexplained, matched_declared=True,
            error_lines_seen=len(error_lines), details=details)

    if unexplained:
        # The mis-cast case: the trace claims one thing, the logs show another
        # at the same instant.
        return Corroboration(
            Verdict.CONTRADICTED,
            f"the declared root ({simple or 'unknown'}) does not appear in the "
            f"logs, but {', '.join(_simple_name(f) for f in unexplained[:3])} did",
            citations=citations, unexplained=unexplained, matched_declared=False,
            error_lines_seen=len(error_lines), details=details)

    if any(set_aside.values()):
        # Exceptions did occur in this window, but every one of them is on a
        # line too weak to convict with: another transaction's, or one the
        # service logged as handled. Both used to return CONTRADICTED, telling
        # a developer their trace was lying about a failure that was either
        # not theirs or not a failure. There is nothing here to contradict the
        # trace with, and saying so is the honest answer.
        because = " and ".join(filter(None, (
            f"{len(set_aside['exceptions_on_context_lines'])} on context lines "
            f"from other transactions on the same pod"
            if set_aside["exceptions_on_context_lines"] else "",
            f"{len(set_aside['exceptions_on_warn_lines'])} logged at warning "
            f"level, which usually means the service handled them"
            if set_aside["exceptions_on_warn_lines"] else "",
        )))
        return Corroboration(
            Verdict.UNVERIFIABLE,
            f"the declared root ({simple or 'unknown'}) does not appear on any "
            f"error line belonging to this packet, and the exceptions this "
            f"window does hold are {because}",
            citations=citations, error_lines_seen=len(error_lines),
            could_not_look=False, details=details)

    # Errors present, but nothing named -- not enough to contradict anything.
    return Corroboration(
        Verdict.UNVERIFIABLE,
        "error lines were found but none names an exception type",
        citations=citations, error_lines_seen=len(error_lines),
        could_not_look=False, details=details)
