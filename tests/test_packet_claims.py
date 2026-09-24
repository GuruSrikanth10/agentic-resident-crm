"""One investigation per packet, even under concurrent duplicates.

The regression these pin is not hypothetical. On 2026-09-24 five deliveries of
one event reached `/analyze-rejection` within six milliseconds; all five read
`status.json` before any wrote it, all five passed the IN_PROGRESS guard, and
all five invoked the graph against the same `thread_id` for nineteen minutes.

`test_only_one_of_many_concurrent_claims_wins` is the one that matters: it
runs the claims genuinely in parallel, because a sequential test passes
against the broken check-then-act guard too.
"""
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.storage import factory, local as local_storage
from src.utils import packet_claims, paths


@pytest.fixture(autouse=True)
def claim_store(tmp_path, monkeypatch):
    """A fresh claim store AND casebook store per test.

    Both bindings have to be patched, and for different reasons. The scoped
    claim store resolves `paths.LOCAL_CASESHEETS_DIR` at call time, so
    patching the module attribute is enough. The unscoped casebook store --
    which `_is_finished` reads -- gets it from a module-level `from ... import`
    in `storage/local.py`, so that name has to be patched separately.

    Patching only the first is not a smaller version of this: it writes real
    casebooks into the repository's own `local_casesheets/`, where they
    survive the test and make the NEXT test's first claim come back
    `finished`.
    """
    monkeypatch.setattr(paths, "LOCAL_CASESHEETS_DIR", tmp_path)
    monkeypatch.setattr(local_storage, "LOCAL_CASESHEETS_DIR", tmp_path)
    factory.reset_storage_cache()
    monkeypatch.delenv("PACKET_CLAIM_ENABLED", raising=False)
    monkeypatch.delenv("PACKET_CLAIM_TTL_SECONDS", raising=False)
    yield
    factory.reset_storage_cache()


def _terminal(event_id, status="COMPLETED"):
    """Write a terminal casebook the way a finished run would."""
    factory.get_casebook_storage().save_terminal(
        event_id, {"packet_metadata": {"eid": event_id},
                   "packet_status": {"status": status}})


# ---------------------------------------------------------------------------
# The race.
# ---------------------------------------------------------------------------

def test_the_first_claim_wins():
    assert packet_claims.claim_packet("evt-1").outcome == "won"


def test_a_second_claim_while_the_first_runs_is_a_duplicate():
    packet_claims.claim_packet("evt-1")
    second = packet_claims.claim_packet("evt-1")

    assert second.won is False
    assert second.outcome == "duplicate"


def test_only_one_of_many_concurrent_claims_wins():
    """The actual failure mode: five deliveries arriving together.

    Sequential claims would pass even against the check-then-act guard this
    replaces, so these have to genuinely overlap.
    """
    barrier = __import__("threading").Barrier(5)

    def claim():
        barrier.wait(timeout=5)
        return packet_claims.claim_packet("evt-concurrent")

    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(lambda _: claim(), range(5)))

    assert sum(1 for r in results if r.won) == 1
    assert sum(1 for r in results if r.outcome == "duplicate") == 4


def test_each_packet_has_its_own_claim():
    assert packet_claims.claim_packet("evt-a").won is True
    assert packet_claims.claim_packet("evt-b").won is True


# ---------------------------------------------------------------------------
# Getting the claim back.
# ---------------------------------------------------------------------------

def test_a_duplicate_of_a_finished_packet_is_reported_as_finished():
    packet_claims.claim_packet("evt-1")
    _terminal("evt-1")

    second = packet_claims.claim_packet("evt-1")
    assert second.won is False
    assert second.outcome == "finished"


def test_a_dead_run_s_claim_is_taken_over_after_the_ttl(monkeypatch):
    """A run that died without writing a terminal status must not lock the
    packet out forever.

    The TTL is patched rather than set through the environment because
    `claim_ttl_seconds` floors an explicit value at one second -- deliberately,
    so a misconfiguration cannot make every live claim look abandoned -- and
    sleeping past that floor would put a second into the suite for nothing.
    The environment path has its own test below.
    """
    monkeypatch.setattr(packet_claims, "claim_ttl_seconds", lambda: 0.0)
    packet_claims.claim_packet("evt-1")

    retaken = packet_claims.claim_packet("evt-1")
    assert retaken.won is True
    assert retaken.outcome == "reclaimed"


def test_an_explicit_ttl_is_floored_at_one_second(monkeypatch):
    """A fat-fingered zero must not disable the claim entirely."""
    monkeypatch.setenv("PACKET_CLAIM_TTL_SECONDS", "0")
    assert packet_claims.claim_ttl_seconds() == 1.0


def test_a_live_run_s_claim_is_not_taken_over(monkeypatch):
    """The TTL must outlast a legitimate investigation: reclaiming early puts
    two of them in flight, which is the failure this prevents."""
    monkeypatch.setattr(packet_claims, "claim_ttl_seconds", lambda: 3600.0)
    packet_claims.claim_packet("evt-1")
    assert packet_claims.claim_packet("evt-1").won is False


@pytest.mark.parametrize("budget,expected", [
    ("3600", 7200.0),      # twice the invoke budget
    ("300", 1800.0),       # ...but never below the floor
    ("nonsense", 3600.0),  # an unusable value falls back
])
def test_the_ttl_tracks_the_invoke_budget(monkeypatch, budget, expected):
    monkeypatch.delenv("PACKET_CLAIM_TTL_SECONDS", raising=False)
    monkeypatch.setenv("AGENT_INVOKE_TIMEOUT_SECONDS", budget)
    assert packet_claims.claim_ttl_seconds() == expected


# ---------------------------------------------------------------------------
# Failing open.
# ---------------------------------------------------------------------------

def test_an_unreachable_claim_store_lets_the_packet_through(monkeypatch):
    """Dedupe saves an LLM call. It must never cost availability."""
    def explode():
        raise RuntimeError("store is down")

    monkeypatch.setattr(packet_claims, "get_claim_storage", explode)
    claim = packet_claims.claim_packet("evt-1")

    assert claim.won is True
    assert claim.outcome == "error"


def test_the_claim_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("PACKET_CLAIM_ENABLED", "false")
    assert packet_claims.claim_packet("evt-1").outcome == "disabled"
    assert packet_claims.claim_packet("evt-1").outcome == "disabled"


def test_every_outcome_is_counted(monkeypatch):
    seen = []

    monkeypatch.setattr(packet_claims.metrics, "record_packet_claim",
                        lambda outcome: seen.append(outcome))
    packet_claims.claim_packet("evt-1")
    packet_claims.claim_packet("evt-1")

    assert seen == ["won", "duplicate"]
