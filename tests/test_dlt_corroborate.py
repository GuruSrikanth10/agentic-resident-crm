"""
Phase 6 of DLT_PLAN.md -- trace-vs-log corroboration.

`test_business_exception_hiding_a_timeout_is_contradicted` is the mis-cast case
and the reason the log lane exists at all. Without it, the headers alone would
be enough and none of Phase 5 would be worth building.

The other property that matters is the one the log pipeline already makes with
`FetchResult.ok`: "could not look" and "looked and found nothing" must stay
distinguishable. Collapsing them lets a finding read as "no errors occurred"
when the truth is "we could not read the logs".
"""
import pytest

from src.dlt.corroborate import Verdict, corroborate
from src.log_pipeline.types import CONTEXT_LINE_MARKER

BUSINESS_FQCN = "in.gov.uidai.common.exception.BusinessException"
CODE = "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"
FRAMES = ("com.uidai.enu.biometric.service.impl.BioDataBaseHelperServiceImpl."
          "getUidOriginTrackerData",)


def logs(*lines):
    return "--- Log Trace ---\n" + "\n".join(lines) + "\n--- End ---"


# ======================================================================
# CORROBORATED
# ======================================================================

def test_declared_root_present_by_fqcn():
    result = corroborate(
        logs(f"[2026-08-18T02:20:08Z] [enu-biometric] [ERROR] {BUSINESS_FQCN}: [{CODE}] absent"),
        BUSINESS_FQCN, CODE, FRAMES)
    assert result.verdict is Verdict.CORROBORATED
    assert result.matched_declared is True
    assert result.citations


def test_declared_root_present_by_simple_name():
    result = corroborate(
        logs("[2026-08-18T02:20:08Z] [ERROR] BusinessException thrown during dedup"),
        BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.CORROBORATED
    assert result.details["matched_on"] == "simple name"


def test_declared_root_present_by_business_code_only():
    """Services log the code without the exception type often enough that
    requiring the FQCN would produce false contradictions."""
    result = corroborate(
        logs(f"[2026-08-18T02:20:08Z] [ERROR] lookup failed: {CODE}"),
        BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.CORROBORATED
    assert result.details["matched_on"] == "business code"


def test_frame_hits_are_recorded_as_supporting_evidence():
    result = corroborate(
        logs(f"[ERROR] {BUSINESS_FQCN}: [{CODE}] in getUidOriginTrackerData"),
        BUSINESS_FQCN, CODE, FRAMES)
    assert result.details["frame_hits"]


# ======================================================================
# CONTRADICTED -- the mis-cast case
# ======================================================================

def test_business_exception_hiding_a_timeout_is_contradicted():
    """THE case this whole lane exists for: the catch block rethrows an infra
    fault as a business error, and the trace confidently reports the wrong
    root cause."""
    result = corroborate(
        logs("[2026-08-18T02:20:07Z] [ERROR] java.net.SocketTimeoutException: "
             "Read timed out talking to the uid-origin datasource",
             "[2026-08-18T02:20:08Z] [ERROR] transaction rolled back"),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CONTRADICTED
    assert result.is_discrepancy
    assert result.matched_declared is False
    assert "java.net.SocketTimeoutException" in result.unexplained
    assert "SocketTimeoutException" in result.reason
    assert result.citations, "a contradiction must cite what it saw"


def test_contradiction_names_the_declared_root_it_could_not_find():
    result = corroborate(
        logs("[ERROR] java.sql.SQLRecoverableException: connection lost"),
        BUSINESS_FQCN, CODE)
    assert "BusinessException" in result.reason


# ======================================================================
# PARTIAL
# ======================================================================

def test_declared_root_plus_unexplained_errors_is_partial():
    result = corroborate(
        logs(f"[ERROR] {BUSINESS_FQCN}: [{CODE}] absent",
             "[ERROR] java.net.SocketTimeoutException: Read timed out"),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.PARTIAL
    assert result.is_discrepancy
    assert result.matched_declared is True
    assert "java.net.SocketTimeoutException" in result.unexplained


# ======================================================================
# UNVERIFIABLE -- and the could-not-look distinction
# ======================================================================

@pytest.mark.parametrize("value", [None, "", "   "])
def test_no_logs_is_unverifiable_and_could_not_look(value):
    result = corroborate(value, BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is True


@pytest.mark.parametrize("marker", [
    "No refId available; logs were not fetched.",
    "No usable timestamp; logs were not fetched.",
    "Log window too old to fetch: ...",
    "Log fetch failed: RuntimeError: cluster unreachable",
])
def test_skipped_fetches_are_could_not_look(marker):
    result = corroborate(marker, BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is True


def test_source_returned_nothing_is_not_could_not_look():
    """'Looked and found nothing' is a different state from 'could not look',
    exactly as FetchResult.ok distinguishes them."""
    result = corroborate("No logs found for ID: REF-1", BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is False


def test_logs_without_error_lines_are_unverifiable():
    result = corroborate(
        logs("[INFO] started", "[INFO] finished"), BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.could_not_look is False
    assert result.error_lines_seen == 0


def test_errors_naming_no_exception_type_do_not_contradict():
    """Not enough signal to accuse the trace of lying."""
    result = corroborate(
        logs("[ERROR] something went wrong", "[ERROR] retrying"),
        BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.error_lines_seen == 2


# ======================================================================
# Context lines -- other refIds caught in the Kubernetes context window
#
# The Kubernetes source has no server-side grep, so it emits each identifier
# match plus K8S_CONTEXT_LINES_BEFORE/_AFTER lines around it. On a pod serving
# packets concurrently those neighbours belong to other refIds. Counting their
# exceptions against this packet made every busy-pod case a discrepancy.
# ======================================================================

TS = "[2026-08-18T02:20:08Z] [enu-biometric@enu-bio-7d9] "


def mine(level, message):
    """A line that carried the searched identifier."""
    return f" *** {TS}[{level}] {message}"


def theirs(level, message):
    """A line kept only as surrounding context -- another transaction's."""
    return f" *** {TS}[{level}] {CONTEXT_LINE_MARKER} {message}"


def test_a_neighbours_error_does_not_make_this_packet_partial():
    """The bug this split exists to fix. Before the mark, a concurrent
    packet's timeout landed in `unexplained` and every occurrence on a busy
    pod came back PARTIAL -- which forces a fresh LLM call per occurrence and
    defeats group reuse entirely."""
    result = corroborate(
        logs(mine("ERROR", f"{BUSINESS_FQCN}: [{CODE}] absent"),
             theirs("ERROR", "java.net.SocketTimeoutException: Read timed out")),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CORROBORATED
    assert result.unexplained == ()
    assert result.details["exceptions_on_context_lines"] == [
        "java.net.SocketTimeoutException"]
    assert result.details["context_error_lines"] == 1
    assert result.citations, "the set-aside line is still shown to a human"


def test_this_packets_own_unexplained_error_still_reports_partial():
    """The true positive has to survive the fix."""
    result = corroborate(
        logs(mine("ERROR", f"{BUSINESS_FQCN}: [{CODE}] absent"),
             mine("ERROR", "java.net.SocketTimeoutException: Read timed out")),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.PARTIAL
    assert "java.net.SocketTimeoutException" in result.unexplained


def test_a_neighbours_error_alone_does_not_contradict():
    """The expensive false positive: nothing of ours is in the window, a
    neighbour's exception is, and the old reading told a developer their trace
    was lying about a failure that was never theirs."""
    result = corroborate(
        logs(mine("ERROR", "dedup stage did not complete"),
             theirs("ERROR", "java.net.SocketTimeoutException: Read timed out")),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.is_discrepancy is False
    assert result.unexplained == ()
    assert "other transactions" in result.reason


def test_declared_root_seen_only_on_a_context_line_still_matches():
    """Most likely a sibling packet hitting the same bug at the same instant.
    Still a match -- a false CONTRADICTED is the more damaging error -- but
    recorded, not silent."""
    result = corroborate(
        logs(theirs("ERROR", f"{BUSINESS_FQCN}: [{CODE}] absent")),
        BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.CORROBORATED
    assert result.details["matched_in_context_only"] is True


def test_a_continuation_line_inherits_its_openers_attribution():
    """A stack trace inside one record's message renders as several physical
    lines and only the first carries the mark. Without inheritance the
    continuation reads as this packet's."""
    result = corroborate(
        logs(mine("ERROR", "dedup stage did not complete"),
             theirs("ERROR", "handler failed"),
             "Caused by: java.net.SocketTimeoutException: ERROR Read timed out"),
        BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.unexplained == ()
    assert result.details["exceptions_on_context_lines"] == [
        "java.net.SocketTimeoutException"]


def test_unmarked_text_is_read_exactly_as_before():
    """Elasticsearch filters server-side, so every record it returns carries
    the id and nothing is marked -- as is every artifact written before the
    mark existed. Both must keep contradicting."""
    result = corroborate(
        logs(f"{TS}[ERROR] java.net.SocketTimeoutException: Read timed out"),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CONTRADICTED
    assert "java.net.SocketTimeoutException" in result.unexplained
    assert result.details["context_error_lines"] == 0


# ======================================================================
# Warning-level lines -- caught exceptions in ordinary Spring services
#
# A service that catches a fault, logs it at WARN and rethrows it as a
# business exception is the exact shape this module inspects. Reading only
# ERROR lines meant the declared root was invisible in that entirely ordinary
# case, so any unrelated ERROR in the window produced CONTRADICTED -- the most
# damaging verdict the system can emit, on the most common logging convention
# there is.
#
# Matching therefore reads WARN; accusing does not. A Spring WARN stream is
# mostly recovered faults, and counting those as errors the trace fails to
# explain would make PARTIAL the default and put an LLM call behind every
# occurrence.
# ======================================================================

def test_a_root_logged_at_warn_is_corroborated_not_contradicted():
    """The case that would have destroyed trust in the feature: the root is
    right there in the window, at WARN, and an unrelated ERROR is present."""
    result = corroborate(
        logs(mine("WARN", f"{BUSINESS_FQCN}: [{CODE}] absent"),
             mine("ERROR", "dedup stage aborted")),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CORROBORATED
    assert result.matched_declared is True
    assert result.details["matched_on_warn_only"] is True
    assert "warning level" in result.reason


def test_the_warn_line_the_match_rests_on_is_cited():
    """A CORROBORATED whose citations do not contain the match is not
    checkable -- and citing only error lines would leave a WARN match with no
    supporting evidence at all."""
    result = corroborate(
        logs(mine("WARN", f"{BUSINESS_FQCN}: [{CODE}] absent"),
             *[mine("ERROR", f"retry {i}") for i in range(50)]),
        BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.CORROBORATED
    assert any(CODE in line for line in result.citations)
    assert len(result.citations) <= 20


@pytest.mark.parametrize("spelling", ["WARN", "WARNING"])
def test_both_spellings_of_the_warning_level_are_read(spelling):
    """The Kubernetes parser normalises WARNING to WARN; Elasticsearch passes
    the level through untouched."""
    result = corroborate(
        logs(mine(spelling, f"{BUSINESS_FQCN}: [{CODE}] absent")),
        BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.CORROBORATED


def test_an_exception_logged_at_warn_does_not_accuse_the_trace():
    """A handled fault is not an unexplained error. Counting it would make
    PARTIAL the default verdict on any service that logs its retries."""
    result = corroborate(
        logs(mine("ERROR", f"{BUSINESS_FQCN}: [{CODE}] absent"),
             mine("WARN", "org.springframework.web.client.ResourceAccessException: "
                          "retrying attempt 2 of 3")),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CORROBORATED
    assert result.unexplained == ()
    assert result.details["exceptions_on_warn_lines"] == [
        "org.springframework.web.client.ResourceAccessException"]


def test_a_warn_only_exception_cannot_contradict_on_its_own():
    """Root absent, and the only exception in the window is a handled one.
    Not enough to accuse anything."""
    result = corroborate(
        logs(mine("ERROR", "dedup stage did not complete"),
             mine("WARN", "java.net.SocketTimeoutException: retrying")),
        BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.unexplained == ()
    assert "warning level" in result.reason


def test_an_error_level_exception_still_contradicts():
    """The mis-cast detector has to survive the widening."""
    result = corroborate(
        logs(mine("WARN", "cache miss, falling back"),
             mine("ERROR", "java.net.SocketTimeoutException: Read timed out")),
        BUSINESS_FQCN, CODE, FRAMES)

    assert result.verdict is Verdict.CONTRADICTED
    assert "java.net.SocketTimeoutException" in result.unexplained


def test_warn_lines_do_not_count_as_error_lines_seen():
    """`error_lines_seen` is a reported field and a metric; widening what the
    matcher reads must not quietly change what it counts."""
    result = corroborate(
        logs(mine("WARN", f"{BUSINESS_FQCN}: [{CODE}] absent"),
             mine("ERROR", "dedup stage aborted")),
        BUSINESS_FQCN, CODE)

    assert result.error_lines_seen == 1
    assert result.details["warn_lines_seen"] == 1


def test_a_trace_with_neither_errors_nor_warnings_is_unverifiable():
    result = corroborate(logs(mine("INFO", "started"), mine("INFO", "finished")),
                         BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.UNVERIFIABLE
    assert result.error_lines_seen == 0
    assert result.could_not_look is False


# ======================================================================
# Matching hygiene
# ======================================================================

def test_error_matching_is_word_bounded():
    """'TERROR' and 'error_code' must not register as error-level lines."""
    result = corroborate(
        logs("[INFO] TERROR movie night", "[INFO] error_code=0 all good"),
        BUSINESS_FQCN, CODE)
    assert result.error_lines_seen == 0


@pytest.mark.parametrize("level", ["ERROR", "FATAL", "SEVERE"])
def test_all_error_levels_are_recognised(level):
    result = corroborate(logs(f"[{level}] {BUSINESS_FQCN}: [{CODE}] x"),
                         BUSINESS_FQCN, CODE)
    assert result.verdict is Verdict.CORROBORATED


def test_citations_are_bounded():
    """A 4,000-line retry storm must not land whole in the casebook."""
    storm = logs(*[f"[ERROR] java.net.SocketTimeoutException: attempt {i}"
                   for i in range(4000)])
    result = corroborate(storm, BUSINESS_FQCN, CODE)
    assert len(result.citations) <= 20
    assert result.error_lines_seen == 4000


def test_unknown_declared_root_still_reports_what_it_saw():
    result = corroborate(
        logs("[ERROR] java.net.SocketTimeoutException: Read timed out"), None, None)
    assert result.verdict is Verdict.CONTRADICTED
    assert "java.net.SocketTimeoutException" in result.unexplained


def test_same_exception_in_a_different_package_is_not_unexplained():
    """Matching on the simple name keeps a repackaged type from reading as a
    contradiction."""
    result = corroborate(
        logs("[ERROR] com.other.pkg.BusinessException: [CODE] boom"),
        BUSINESS_FQCN, "CODE")
    assert result.verdict is Verdict.CORROBORATED


def test_verdict_is_total():
    for value in (None, "", "garbage", "[ERROR]", "\n\n"):
        assert corroborate(value, BUSINESS_FQCN, CODE).verdict in set(Verdict)
