"""Context-line attribution: telling this packet's log lines from its
neighbours'.

The Kubernetes API has no server-side grep, so the source emits each
identifier match plus K8S_CONTEXT_LINES_BEFORE/_AFTER lines around it. On a
pod serving packets concurrently those neighbours are other refIds' lines, and
once everything is flattened to text they are indistinguishable from this
packet's own.

Corroboration read all of them. A concurrent packet's timeout therefore landed
in `unexplained` and the verdict came back PARTIAL -- which `reuse.decide`
turns into a fresh LLM call per occurrence, defeating group reuse on exactly
the busy services that generate the most cases. Worse, when the declared root
was not itself on an error-level line, the same neighbour produced
CONTRADICTED: an accusation that a developer's stack trace is lying, about a
failure that was never theirs.

These tests follow one context line along the whole chain -- selector, record,
rendered text, verdict -- because the mark is only useful if it survives every
hop.
"""
import json
from datetime import datetime, timezone

import pytest

from src.dlt.corroborate import Verdict, corroborate
from src.log_pipeline.pipeline import _format_error_path
from src.log_pipeline.sources.k8s import client as k8s_client_module
from src.log_pipeline.sources.k8s import retrieval
from src.log_pipeline.sources.k8s.discovery import PodTarget
from src.log_pipeline.sources.k8s.filtering import (
    ContextWindowSelector,
    build_matcher,
)
from src.log_pipeline.types import CONTEXT_LINE_MARKER, TimeWindow

REF_ID = "c5d21184-08f4-4c32-9e5e-5c108c33eb14"
BUSINESS_FQCN = "in.gov.uidai.common.exception.BusinessException"
CODE = "UID_ORIGIN_TRACKER_DATA_NOT_FOUND"

#: A concurrent packet's infrastructure failure, logged microseconds before
#: ours on the same pod. It is the whole problem in one line.
NEIGHBOUR = ("2026-01-01T10:15:29.000000000Z ERROR [http-nio-8080-exec-3] "
             "java.net.SocketTimeoutException: Read timed out")
OURS = (f"2026-01-01T10:15:30.000000000Z ERROR [http-nio-8080-exec-9] "
        f"refId={REF_ID} {BUSINESS_FQCN}: [{CODE}] absent")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.delenv("K8S_FIXTURE_DIR", raising=False)
    k8s_client_module.reset_client()
    yield
    k8s_client_module.reset_client()


def _pod(root, lines):
    pod_dir = root / "enu" / "pod-a"
    pod_dir.mkdir(parents=True, exist_ok=True)
    (pod_dir / "current.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (pod_dir / "meta.json").write_text(json.dumps({
        "phase": "Running", "labels": {"app": "enu-biometric"},
        "containers": ["app"], "restart_counts": {},
    }), encoding="utf-8")
    return PodTarget(
        namespace="enu", pod_name="pod-a", container="app",
        restart_count=0, phase="Running",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _read(monkeypatch, tmp_path, lines, before=1, after=0):
    target = _pod(tmp_path, lines)
    monkeypatch.setenv("K8S_FIXTURE_DIR", str(tmp_path))
    selector = ContextWindowSelector(build_matcher([REF_ID]),
                                     before=before, after=after)
    return retrieval.read_pod_logs(target, TimeWindow.default(),
                                   selector=selector).records


# ======================================================================
# Retrieval -- the flag is stamped where the matcher ran
# ======================================================================

def test_retrieval_flags_the_line_that_carried_the_identifier(monkeypatch, tmp_path):
    records = _read(monkeypatch, tmp_path, [NEIGHBOUR, OURS])

    assert [r["identifier_match"] for r in records] == [False, True]
    assert REF_ID in records[1]["message"]


def test_trailing_context_is_flagged_too(monkeypatch, tmp_path):
    """A neighbour landing just *after* ours is pulled in by the after-window
    and is no more ours for it."""
    records = _read(monkeypatch, tmp_path, [OURS, NEIGHBOUR],
                    before=0, after=1)

    assert [r["identifier_match"] for r in records] == [True, False]


# ======================================================================
# Rendering -- the flag survives the flattening to text
# ======================================================================

def test_the_rendered_trace_marks_context_lines_and_explains_the_mark():
    records = [
        {"timestamp": "T1", "level": "ERROR", "app_name": "enu", "pod_name": "p",
         "message": "someone else timed out", "identifier_match": False},
        {"timestamp": "T2", "level": "ERROR", "app_name": "enu", "pod_name": "p",
         "message": "ours failed", "identifier_match": True},
    ]

    text = _format_error_path("evt-1", records, 2, "some/path")
    marked = [line for line in text.splitlines()
              if CONTEXT_LINE_MARKER in line and "someone else" in line]

    assert marked, "the context line must carry the mark"
    assert "ours failed" in text
    assert CONTEXT_LINE_MARKER not in [
        line for line in text.splitlines() if "ours failed" in line][0]
    assert "may belong to another transaction" in text, "the mark needs a legend"


def test_a_trace_with_no_context_lines_renders_no_mark_and_no_legend():
    """Nothing to warn about, so nothing is said -- and Elasticsearch records,
    which have no flag at all because the source filters server-side, render
    exactly as they did before."""
    records = [
        {"timestamp": "T1", "level": "ERROR", "app_name": "enu",
         "message": "ours failed", "identifier_match": True},
        {"timestamp": "T2", "level": "ERROR", "app_name": "enu",
         "message": "ours failed again"},  # no flag: an Elasticsearch record
    ]

    text = _format_error_path("evt-1", records, 2, "some/path")

    assert CONTEXT_LINE_MARKER not in text
    assert "may belong to another transaction" not in text


# ======================================================================
# End to end -- what the verdict does with it
# ======================================================================

def test_a_neighbours_timeout_no_longer_makes_this_packet_partial(
        monkeypatch, tmp_path):
    """The chain, whole: a real pod log with two transactions interleaved,
    read through the selector, rendered, and judged."""
    records = _read(monkeypatch, tmp_path, [NEIGHBOUR, OURS])
    text = _format_error_path(REF_ID, records, len(records), "some/path")

    result = corroborate(text, BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.CORROBORATED
    assert result.unexplained == ()
    assert result.details["exceptions_on_context_lines"] == [
        "java.net.SocketTimeoutException"]


def test_the_same_trace_without_the_flag_is_what_the_bug_looked_like(
        monkeypatch, tmp_path):
    """The control. Identical lines, flag stripped -- which is what every
    Kubernetes-sourced case produced before this change, and what an
    Elasticsearch-sourced case still produces because there the neighbour
    would never have been fetched in the first place."""
    records = _read(monkeypatch, tmp_path, [NEIGHBOUR, OURS])
    for record in records:
        del record["identifier_match"]
    text = _format_error_path(REF_ID, records, len(records), "some/path")

    result = corroborate(text, BUSINESS_FQCN, CODE)

    assert result.verdict is Verdict.PARTIAL
    assert "java.net.SocketTimeoutException" in result.unexplained
