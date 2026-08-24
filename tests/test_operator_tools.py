"""Operator CLIs must read through CasebookStorage, not the local filesystem.

G2 fixed exactly this in `outcomes.iter_outcomes`: walking
`local_casesheets/` meant that under CASEBOOK_STORAGE_BACKEND=s3 the tool
found nothing, said so cheerfully, and the operator had no way to tell an
empty queue from a blind one. The same walk survived in several tools.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.storage.factory import get_casebook_storage, reset_storage_cache


@pytest.fixture(autouse=True)
def _isolated_store(tmp_path, monkeypatch):
    monkeypatch.delenv("CASEBOOK_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr("src.utils.paths.LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr("src.storage.local.LOCAL_CASESHEETS_DIR", tmp_path)
    reset_storage_cache()
    yield
    reset_storage_cache()


def _terminal_casebook(storage, event_id, reason_code="RC_1", packet_type="U"):
    storage.save(event_id, {
        "packet_metadata": {"eid": event_id, "packet_type": packet_type},
        "packet_status": {
            "status": "COMPLETED",
            "rejection_data": {"rejection_code": reason_code},
        },
        "resolution": {"synthesis": "replay it", "action": "REPLAY",
                       "source": "agent"},
    })


def test_record_outcome_lists_pending_from_storage(capsys):
    """The operator's work queue. Empty output here means the accuracy
    dataset never gets fed, and nothing says why."""
    from src.tools import record_outcome

    storage = get_casebook_storage()
    _terminal_casebook(storage, "evt-pending-1")
    _terminal_casebook(storage, "evt-pending-2")

    record_outcome._list_pending()

    out = capsys.readouterr().out
    assert "evt-pending-1" in out
    assert "evt-pending-2" in out


def test_record_outcome_skips_events_already_judged(capsys):
    from src.tools import record_outcome
    from src.utils.outcomes import OUTCOME_FILENAME

    storage = get_casebook_storage()
    _terminal_casebook(storage, "evt-judged")
    storage.save("evt-judged", {"event_id": "evt-judged", "verdict": "CORRECT"},
                 filename=OUTCOME_FILENAME)

    record_outcome._list_pending()

    assert "evt-judged" not in capsys.readouterr().out


def test_record_outcome_reports_an_enumeration_failure(capsys):
    """"No casebooks found" must not be printed when the truth is "could not
    look" -- the distinction this codebase exists to preserve."""
    from src.tools import record_outcome

    broken = MagicMock()
    broken.list_events.side_effect = RuntimeError("bucket unreachable")

    with patch("src.storage.factory.get_casebook_storage", return_value=broken):
        record_outcome._list_pending()

    out = capsys.readouterr().out
    assert "Could not enumerate" in out
    assert "No casebooks found" not in out


def test_build_runbooks_groups_casebooks_from_storage():
    """The runbook learning loop was silently dead on the S3 backend: no
    casebooks found, no drafts generated, exit 0."""
    from src.tools import build_runbooks

    storage = get_casebook_storage()
    for index in range(3):
        _terminal_casebook(storage, f"evt-rb-{index}", reason_code="RC_SHARED")

    args = MagicMock()
    args.reason_code = None
    args.any_enrolment_type = False
    args.min_samples = 99          # stop before drafting
    args.dry_run = True

    with patch.object(build_runbooks, "get_llm") as llm, \
         patch("argparse.ArgumentParser.parse_args", return_value=args), \
         patch.object(build_runbooks, "logger") as log:
        build_runbooks.main()

    # No group cleared --min-samples, so no model was ever constructed.
    llm.assert_not_called()

    # It found the group and declined only because of --min-samples.
    skipped = [c for c in log.info.call_args_list
               if "insufficient samples" in str(c)]
    assert skipped, "build_runbooks saw no casebooks at all"
    assert skipped[0].kwargs["samples"] == 3
    assert skipped[0].kwargs["reason_code"] == "RC_SHARED"


def test_build_runbooks_builds_no_llm_when_there_is_nothing_to_draft():
    """`get_llm` resolves provider credentials and raises without them, so
    constructing it up front made --dry-run fail on a machine that needs no
    model at all."""
    from src.tools import build_runbooks

    args = MagicMock()
    args.reason_code = None
    args.any_enrolment_type = False
    args.min_samples = 1
    args.dry_run = True

    with patch("argparse.ArgumentParser.parse_args", return_value=args), \
         patch.object(build_runbooks, "get_llm",
                      side_effect=ValueError("HF_TOKEN must be set")), \
         patch.object(build_runbooks, "logger"):
        build_runbooks.main()   # no casebooks at all -> must not raise


def test_build_runbooks_ignores_escalated_and_non_terminal_casebooks():
    from src.tools import build_runbooks

    storage = get_casebook_storage()
    storage.save("evt-escalated", {
        "packet_metadata": {"eid": "evt-escalated", "packet_type": "U"},
        "packet_status": {"status": "COMPLETED",
                          "rejection_data": {"rejection_code": "RC_X"}},
        "resolution": {"synthesis": "ESCALATED TO HUMAN REVIEW. nope"},
    })
    storage.save("evt-inflight", {
        "packet_metadata": {"eid": "evt-inflight", "packet_type": "U"},
        "packet_status": {"status": "IN_PROGRESS",
                          "rejection_data": {"rejection_code": "RC_X"}},
        "resolution": {"synthesis": "working"},
    })

    args = MagicMock()
    args.reason_code = None
    args.any_enrolment_type = False
    args.min_samples = 1
    args.dry_run = True

    with patch("argparse.ArgumentParser.parse_args", return_value=args), \
         patch.object(build_runbooks, "get_llm") as llm, \
         patch.object(build_runbooks, "logger"):
        build_runbooks.main()

    llm.assert_not_called()
