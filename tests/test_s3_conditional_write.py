"""S3 conditional writes against stores that do not implement them faithfully.

The DLT run on 2026-09-22 produced five bursts of exactly eight
"Conditional write lost a race" lines, each ending in a dropped group update.
Two of those bursts were on fingerprints with a single member case: there was
no other writer in existence, yet every If-Match was refused. The store
accepted If-None-Match creates and refused If-Match overwrites -- so every
group record was created once and never updated again.

These tests pin the behaviour that makes that failure loud and, where the
store's quirk is recoverable, recover from it.
"""
import pytest

from s3_fakes import FakeS3, S3Error, install
from src.storage import s3 as s3_module


def _store(monkeypatch, fake):
    install(monkeypatch, fake)
    return s3_module.S3CasebookStorage(bucket="b", prefix="p")


def bump(current):
    return {"n": (current or {}).get("n", 0) + 1}


# ----------------------------------------------------------------------
# The endpoint that refuses If-Match outright
# ----------------------------------------------------------------------

def test_a_store_that_refuses_if_match_fails_fast_and_says_why(monkeypatch):
    fake = FakeS3(if_match="refuse")
    store = _store(monkeypatch, fake)

    store.update_json("k", "doc.json", bump)            # create: works

    with pytest.raises(s3_module.ConditionalWriteUnsupported) as err:
        store.update_json("k", "doc.json", bump)        # overwrite: never can

    assert "does not support conditional overwrite" in str(err.value)
    # Two reads that returned the same ETag are enough to know. The old loop
    # spent all eight attempts and then blamed "another writer".
    overwrite_puts = fake.conditional_puts - 1
    assert overwrite_puts <= 4, overwrite_puts


def test_losing_a_genuine_race_is_still_retried(monkeypatch):
    """The fast-fail must not fire on real contention, where every re-read
    returns a *different* ETag because someone else did write."""
    fake = FakeS3()
    store = _store(monkeypatch, fake)
    store.update_json("k", "doc.json", bump)

    real_put = fake.put_object
    interference = {"left": 3}

    def contended_put(**kwargs):
        if kwargs.get("IfMatch") is not None and interference["left"]:
            interference["left"] -= 1
            fake._store(kwargs["Key"], b'{"n": 100}')   # another writer lands
        return real_put(**kwargs)

    monkeypatch.setattr(fake, "put_object", contended_put)
    result = store.update_json("k", "doc.json", bump)

    assert result["n"] == 101, "applied on top of the winner's state"


# ----------------------------------------------------------------------
# The endpoint that wants the bare ETag
# ----------------------------------------------------------------------

def test_a_store_that_wants_the_bare_etag_is_negotiated(monkeypatch):
    fake = FakeS3(if_match="bare_only")
    store = _store(monkeypatch, fake)

    store.update_json("k", "doc.json", bump)
    assert store.update_json("k", "doc.json", bump)["n"] == 2
    assert s3_module._etag_style_resolved == "bare"


def test_the_negotiated_style_is_used_directly_afterwards(monkeypatch):
    fake = FakeS3(if_match="bare_only")
    store = _store(monkeypatch, fake)
    store.update_json("k", "doc.json", bump)
    store.update_json("k", "doc.json", bump)            # negotiates

    before = fake.conditional_puts
    store.update_json("k", "doc.json", bump)
    assert fake.conditional_puts - before == 1, "no wasted round trip once latched"


def test_a_faithful_store_keeps_the_quoted_form(monkeypatch):
    fake = FakeS3()
    store = _store(monkeypatch, fake)
    store.update_json("k", "doc.json", bump)
    store.update_json("k", "doc.json", bump)
    assert s3_module._etag_style_resolved == "quoted"


def test_a_pinned_style_is_respected(monkeypatch):
    fake = FakeS3(if_match="bare_only")
    store = _store(monkeypatch, fake)
    monkeypatch.setattr(s3_module, "_etag_style_resolved", "quoted")

    store.update_json("k", "doc.json", bump)
    with pytest.raises(s3_module.ConditionalWriteUnsupported):
        store.update_json("k", "doc.json", bump)


# ----------------------------------------------------------------------
# Reads
# ----------------------------------------------------------------------

def test_an_unreadable_object_is_not_treated_as_an_absent_one(monkeypatch):
    """Falling through to If-None-Match against a key that exists can never
    succeed -- it used to burn the whole retry budget and drop the write."""
    fake = FakeS3()
    store = _store(monkeypatch, fake)
    store.update_json("k", "doc.json", bump)
    fake.fail_reads = True

    with pytest.raises(s3_module.StorageReadError):
        store.update_json("k", "doc.json", bump)
    assert fake.conditional_puts == 1, "no doomed create was attempted"


def test_a_corrupt_document_is_rebuilt_under_its_etag(monkeypatch):
    fake = FakeS3()
    store = _store(monkeypatch, fake)
    fake._store("p/casebook_k/doc.json", b"{not json")

    assert store.update_json("k", "doc.json", bump) == {"n": 1, "schema_version": "1.2"}


def test_a_corrupt_document_still_loses_to_a_concurrent_writer(monkeypatch):
    """Rebuilding from corruption must not become a way to clobber."""
    fake = FakeS3(if_match="refuse")
    store = _store(monkeypatch, fake)
    fake._store("p/casebook_k/doc.json", b"{not json")

    with pytest.raises(s3_module.ConditionalWriteUnsupported):
        store.update_json("k", "doc.json", bump)


# ----------------------------------------------------------------------
# create_json / list_json
# ----------------------------------------------------------------------

def test_create_json_creates_once(monkeypatch):
    fake = FakeS3(if_match="refuse")                   # irrelevant to creates
    store = _store(monkeypatch, fake)

    assert store.create_json("k", "sub/a.json", {"v": 1}) is True
    assert store.create_json("k", "sub/a.json", {"v": 2}) is False
    assert fake.json("p/casebook_k/sub/a.json")["v"] == 1


def test_list_json_lists_one_level(monkeypatch):
    fake = FakeS3()
    store = _store(monkeypatch, fake)
    store.create_json("k", "sub/a.json", {})
    store.create_json("k", "sub/b.json", {})
    store.create_json("k", "sub/deeper/c.json", {})
    store.save("k", {}, filename="other.json")

    assert store.list_json("k", "sub") == ["a.json", "b.json"]
    assert store.list_json("k", "absent") == []


def test_list_json_raises_rather_than_reporting_empty(monkeypatch):
    """An empty listing would read as "no occurrences" -- a novel fingerprint."""
    fake = FakeS3()
    store = _store(monkeypatch, fake)

    def broken(name):
        raise S3Error("SlowDown", 503)

    monkeypatch.setattr(fake, "get_paginator", broken)
    with pytest.raises(S3Error):
        store.list_json("k", "sub")


# ----------------------------------------------------------------------
# The capability probe
# ----------------------------------------------------------------------

@pytest.mark.parametrize("mode,overwrite,style", [
    ("honour", True, "quoted"),
    ("bare_only", True, "bare"),
    ("refuse", False, None),
])
def test_the_probe_reports_what_the_endpoint_does(monkeypatch, mode, overwrite, style):
    fake = FakeS3(if_match=mode)
    install(monkeypatch, fake)

    result = s3_module.probe_conditional_write("b", "p")

    assert result["error"] is None
    assert result["overwrite"] is overwrite
    assert result["etag_style"] == style
    assert result["create_only"] is True
    assert not any("_probe" in key for key in fake.objects), "sentinel cleaned up"


def test_the_probe_notices_a_store_with_no_create_only_either(monkeypatch):
    fake = FakeS3(if_match="refuse", if_none_match="ignore")
    install(monkeypatch, fake)
    assert s3_module.probe_conditional_write("b", "p")["create_only"] is False


def test_the_startup_probe_logs_an_error_for_a_refusing_store(monkeypatch):
    fake = FakeS3(if_match="refuse")
    install(monkeypatch, fake)
    monkeypatch.setattr(s3_module, "SKIP_CAS_PROBE", False)
    errors = []
    monkeypatch.setattr(s3_module.logger, "error",
                        lambda msg, **kw: errors.append(msg))

    result = s3_module.conditional_write_capability("b", "p")

    assert result["overwrite"] is False
    assert errors and "refuses conditional overwrite" in errors[0]


def test_the_startup_probe_is_cached(monkeypatch):
    fake = FakeS3()
    install(monkeypatch, fake)
    monkeypatch.setattr(s3_module, "SKIP_CAS_PROBE", False)

    s3_module.conditional_write_capability("b", "p")
    puts = fake.puts
    s3_module.conditional_write_capability("b", "p")
    assert fake.puts == puts


def test_the_startup_probe_can_be_skipped(monkeypatch):
    fake = FakeS3()
    install(monkeypatch, fake)
    monkeypatch.setattr(s3_module, "SKIP_CAS_PROBE", True)

    assert s3_module.conditional_write_capability("b", "p")["error"] == "probe skipped"
    assert fake.puts == 0


# ----------------------------------------------------------------------
# Backoff
# ----------------------------------------------------------------------

def test_the_first_retry_does_not_sleep_zero():
    """zero-based `attempt * 0.05` gave a ceiling of exactly zero on the first
    retry, so the first two rounds of any real burst were a guaranteed tie."""
    ceilings = []
    for attempt in range(8):
        samples = [s3_module._backoff_seconds(attempt) for _ in range(200)]
        ceilings.append(max(samples))
    assert ceilings[0] > 0
    assert all(c <= s3_module.UPDATE_MAX_BACKOFF for c in ceilings)
    assert ceilings[-1] > ceilings[0]
