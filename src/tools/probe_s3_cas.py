"""Does the configured S3 endpoint support the conditional writes we rely on?

Run this against the real endpoint before trusting `update_json` on it:

    python -m src.tools.probe_s3_cas

`update_json` needs two things, and an endpoint can provide one without the
other:

  create-only   `If-None-Match: *` must REFUSE a PUT over a key that exists.
                `src/dlt/claims.py` and `src/dlt/group_store.py` both depend
                on this for idempotency.
  overwrite     `If-Match: <etag>` must ACCEPT a PUT when the object is still
                the one we read.

An endpoint that gives the first but not the second looks healthy: every
record is created successfully and then silently never updated again. That is
what produced DLT group records stuck at `occurrence_count: 1` with a null
`recommendation` -- five bursts of eight refused writes with no contending
writer anywhere in the run.

Exit codes: 0 both supported, 1 overwrite unsupported, 2 the probe itself
could not run.
"""
import os
import sys


def main() -> int:
    from src.storage.s3 import probe_conditional_write

    bucket = (os.environ.get("CASEBOOK_S3_BUCKET")
              or os.environ.get("S3_LOGS_BUCKET"))
    if not bucket:
        print("Set CASEBOOK_S3_BUCKET (or S3_LOGS_BUCKET) first.")
        return 2

    prefix = (os.environ.get("CASEBOOK_S3_PREFIX") or "casebooks").strip("/")
    endpoint = (os.environ.get("S3_ENDPOINT_URL")
                or os.environ.get("AWS_ENDPOINT_URL") or "(AWS default)")

    print(f"endpoint : {endpoint}")
    print(f"bucket   : {bucket}")
    print(f"prefix   : {prefix}")
    print()

    result = probe_conditional_write(bucket, prefix)

    if result["error"]:
        print(f"probe failed: {result['error']}")
        return 2

    print(f"create-only (If-None-Match: *) : "
          f"{'supported' if result['create_only'] else 'NOT SUPPORTED'}")
    print(f"overwrite   (If-Match: <etag>) : "
          f"{'supported' if result['overwrite'] else 'NOT SUPPORTED'}")
    if result["etag_style"]:
        print(f"ETag form accepted             : {result['etag_style']}")
    print()

    if not result["overwrite"]:
        print("This endpoint cannot support read-modify-write through")
        print("update_json. DLT group records must use the decomposed store")
        print("(src/dlt/group_store.py), which needs only create-only and")
        print("blind PUT. Set DLT_GROUP_STORE=v2.")
        if not result["create_only"]:
            print()
            print("It does not honour If-None-Match either, so the case claim")
            print("in src/dlt/claims.py cannot dedupe across processes. Set")
            print("DLT_CLAIM_ENABLED=false and dedupe upstream instead.")
        return 1

    if result["etag_style"] == "bare":
        print("Note: this endpoint wants the UNQUOTED ETag on If-Match, which")
        print("is not what get_object returns. S3_ETAG_STYLE=auto handles it;")
        print("pin S3_ETAG_STYLE=bare to skip the negotiation round trip.")

    print("Both preconditions are honoured. update_json is safe here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
