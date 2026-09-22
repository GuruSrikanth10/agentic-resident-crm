"""S3 fakes that misbehave the way real S3-compatible stores do.

The fake in test_multipod_state.py implements conditional writes perfectly,
which is precisely why `test_s3_update_json_uses_a_conditional_write_every_time`
passed while every DLT group update in production was being refused. A fake
that is better-behaved than the dependency it stands in for proves nothing
about the dependency. These model the two failure modes seen in the field:

  refuse     accepts `If-None-Match: *`, refuses every `If-Match`
  bare_only  compares `If-Match` against the unquoted ETag, so the quoted form
             `get_object` returns never matches
"""
import io
import json


class S3Error(Exception):
    def __init__(self, code, status):
        super().__init__(code)
        self.response = {"Error": {"Code": code},
                         "ResponseMetadata": {"HTTPStatusCode": status}}


class _Paginator:
    def __init__(self, fake):
        self.fake = fake

    def paginate(self, Bucket, Prefix="", Delimiter=None):
        contents, prefixes = [], set()
        for key in sorted(self.fake.objects):
            if not key.startswith(Prefix):
                continue
            rest = key[len(Prefix):]
            if Delimiter and Delimiter in rest:
                prefixes.add(Prefix + rest[:rest.index(Delimiter) + 1])
            else:
                contents.append({"Key": key})
        yield {"Contents": contents,
               "CommonPrefixes": [{"Prefix": p} for p in sorted(prefixes)]}


class FakeS3:
    """`if_match` is one of "honour", "refuse", "bare_only"."""

    def __init__(self, if_match: str = "honour", if_none_match: str = "honour",
                 fail_reads: bool = False):
        self.objects = {}
        self.etags = {}
        self.if_match = if_match
        self.if_none_match = if_none_match
        self.fail_reads = fail_reads
        self.puts = 0
        self.conditional_puts = 0
        self._version = 0

    def _store(self, key, body):
        self._version += 1
        self.objects[key] = body if isinstance(body, bytes) else body.encode("utf-8")
        self.etags[key] = f'"etag{self._version}"'

    def get_object(self, Bucket, Key):
        if self.fail_reads:
            raise S3Error("AccessDenied", 403)
        if Key not in self.objects:
            raise S3Error("NoSuchKey", 404)
        return {"Body": io.BytesIO(self.objects[Key]), "ETag": self.etags[Key]}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise S3Error("404", 404)
        return {"ETag": self.etags[Key]}

    def put_object(self, Bucket, Key, Body, ContentType=None,
                   IfMatch=None, IfNoneMatch=None):
        self.puts += 1
        if IfMatch is not None or IfNoneMatch is not None:
            self.conditional_puts += 1

        if IfNoneMatch == "*" and Key in self.objects and self.if_none_match == "honour":
            raise S3Error("PreconditionFailed", 412)

        if IfMatch is not None:
            current = self.etags.get(Key)
            if self.if_match == "refuse":
                raise S3Error("PreconditionFailed", 412)
            if self.if_match == "bare_only":
                ok = current is not None and IfMatch == current.strip('"')
            else:
                ok = IfMatch == current
            if not ok:
                raise S3Error("PreconditionFailed", 412)

        self._store(Key, Body)

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.etags.pop(Key, None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _Paginator(self)

    def json(self, key):
        return json.loads(self.objects[key].decode("utf-8"))


def install(monkeypatch, fake, bucket="b", prefix="agentic"):
    """Point the S3 backend, and every scoped store built from it, at `fake`."""
    from src.dlt import case_storage
    from src.storage import factory
    from src.storage import s3 as s3_module

    monkeypatch.setenv("CASEBOOK_STORAGE_BACKEND", "s3")
    monkeypatch.setenv("CASEBOOK_S3_BUCKET", bucket)
    monkeypatch.setenv("CASEBOOK_S3_PREFIX", prefix)
    monkeypatch.setattr(s3_module, "_get_client", lambda: fake)
    # Module-level negotiation state must not leak between tests.
    monkeypatch.setattr(s3_module, "ETAG_STYLE", "auto")
    monkeypatch.setattr(s3_module, "_etag_style_resolved", None)
    monkeypatch.setattr(s3_module, "_capability", None)
    monkeypatch.setattr(s3_module, "_backoff_seconds", lambda attempt: 0.0)
    factory.reset_storage_cache()
    case_storage.reset_cache()
    return fake
