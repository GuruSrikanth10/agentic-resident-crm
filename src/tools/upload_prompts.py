"""
Upload the repository's prompts to Langfuse Prompt Management, where the API
reads them under PROMPT_SOURCE=langfuse (src/utils/prompt_store.py).

    python -m src.tools.upload_prompts --list     each prompt file's Langfuse name (no network)
    python -m src.tools.upload_prompts            compare with Langfuse; changes nothing
    python -m src.tools.upload_prompts --apply    publish what differs, labelled `production`

Run it from the repository root, on a machine that reaches the self-hosted
Langfuse, with LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY and LANGFUSE_BASE_URL
set (the repository's .env is read). `--apply` creates a new version of each
prompt whose labelled version differs from its file -- a prompt Langfuse does
not have yet is created -- and moves the label to it. A prompt that already
matches is left alone, so running it twice creates nothing. It then reads
every prompt back and compares.

Every text is checked first, exactly as the API checks what it fetches
(prompt_store.check_texts): a pack text with a packet identifier or
instruction-shaped text, an empty prompt or a broken harness include stops
the run before anything is uploaded. Trailing whitespace is not a difference.

Exit status: 0 when every prompt matches (or was made to); 1 when differences
were found without --apply; 2 when a check, the connection or an upload
failed.
"""
import argparse
import contextlib
import logging
import os
import subprocess
import sys
from typing import Optional

from src.utils import env  # noqa: F401  -- reads .env before the client is built
from src.utils import paths, prompt_store

SAME, NEW, CHANGED = "same", "new", "changed"


def _relative(path) -> str:
    """`path` from the repository root, with forward slashes on Windows too,
    so a version's commit message reads the same whichever machine wrote it."""
    try:
        return path.relative_to(paths.REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _commit() -> Optional[str]:
    """The repository's commit, to say in each version where it came from."""
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=paths.REPO_ROOT,
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except Exception:
        return None


def _current(client, name: str, label: str):
    """(text, version) of `name`'s `label` version in Langfuse, or None when
    there is none. Read past the SDK's cache. Anything but "not found"
    raises."""
    try:
        prompt = client.get_prompt(name, label=label, type="text",
                                   cache_ttl_seconds=0, max_retries=1)
    except Exception as e:
        if getattr(e, "status_code", None) == 404:
            return None
        raise
    text = getattr(prompt, "prompt", None)
    if not isinstance(text, str):
        raise RuntimeError(f"{name} is not a text prompt in Langfuse; this tool "
                           f"publishes text prompts only.")
    return text, getattr(prompt, "version", None)


def _same(left: str, right: str) -> bool:
    return left.rstrip() == right.rstrip()


def _connection_errors() -> list:
    """What must be set before reaching Langfuse. The server is required:
    without it the SDK would reach Langfuse Cloud."""
    errors = [f"{name} is not set." for name in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")
              if not os.environ.get(name)]
    if not (os.environ.get("LANGFUSE_BASE_URL") or os.environ.get("LANGFUSE_HOST")):
        errors.append("LANGFUSE_BASE_URL is not set: the self-hosted Langfuse to upload to.")
    return errors


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Upload the repository's prompts to Langfuse Prompt Management.")
    parser.add_argument("--apply", action="store_true",
                        help="publish every prompt that differs (default: compare only)")
    parser.add_argument("--list", action="store_true",
                        help="list each prompt file's Langfuse name, without connecting")
    parser.add_argument("--label", default=prompt_store.label(),
                        help="the label to compare with and publish under "
                             "(default: LANGFUSE_PROMPT_LABEL, else production)")
    args = parser.parse_args(argv)

    catalog = prompt_store.catalog()
    if args.list:
        for entry in catalog:
            print(f"{entry.name:48} {_relative(entry.path)}")
        print(f"\n{len(catalog)} prompts.")
        return 0

    # Text mode, as the API reads a file: a Windows checkout's line endings
    # arrive as they do on Linux.
    texts = {entry.name: entry.path.read_text(encoding="utf-8") for entry in catalog}
    errors = prompt_store.check_texts(texts)
    if errors:
        print("These prompts fail the checks the service applies; nothing was uploaded:")
        for error in errors:
            print(f"  - {error}")
        return 2

    problems = _connection_errors()
    if problems:
        for problem in problems:
            print(f"Cannot connect: {problem}")
        return 2

    server = os.environ.get("LANGFUSE_BASE_URL") or os.environ.get("LANGFUSE_HOST")
    print(f"Langfuse: {server}   label: {args.label}\n")
    with _quiet_sdk():
        return _sync(args, catalog, texts)


@contextlib.contextmanager
def _quiet_sdk():
    """A prompt Langfuse does not have yet is a 404, which the SDK logs as a
    warning; here it is the expected "new", reported in the table. Its errors
    still show."""
    sdk = logging.getLogger("langfuse")
    previous = sdk.level
    sdk.setLevel(logging.ERROR)
    try:
        yield
    finally:
        sdk.setLevel(previous)


def _sync(args, catalog, texts: dict) -> int:
    """Compare every prompt with Langfuse and, with --apply, publish what
    differs and read it all back. `main`'s exit status."""
    from src.utils import tracing

    try:
        client = tracing.langfuse_client()
        rows = []
        for entry in catalog:
            current = _current(client, entry.name, args.label)
            if current is None:
                rows.append((entry, NEW, None))
            else:
                status = SAME if _same(current[0], texts[entry.name]) else CHANGED
                rows.append((entry, status, current[1]))
    except Exception as e:
        print(f"Could not read the prompts from Langfuse: {type(e).__name__}: {e}")
        return 2

    for entry, status, version in rows:
        shown = f"v{version}" if version is not None else "-"
        print(f"{status:8} {entry.name:48} {shown:6} {_relative(entry.path)}")
    pending = [(entry, status) for entry, status, _ in rows if status != SAME]
    counts = {status: sum(1 for _, s, _ in rows if s == status) for status in (SAME, CHANGED, NEW)}
    print(f"\n{counts[SAME]} same, {counts[CHANGED]} changed, {counts[NEW]} new.")

    if not args.apply:
        if pending:
            print("Nothing was uploaded. Run with --apply to publish them.")
            return 1
        return 0

    commit = _commit()
    for entry, status in pending:
        message = f"Uploaded from {_relative(entry.path)}" + (f" at {commit}" if commit else "")
        try:
            created = client.create_prompt(name=entry.name, prompt=texts[entry.name],
                                           labels=[args.label], type="text",
                                           commit_message=message)
        except Exception as e:
            print(f"Could not publish {entry.name}: {type(e).__name__}: {e}")
            return 2
        print(f"published {entry.name} v{getattr(created, 'version', '?')} ({status})")

    # Read back what the service will read.
    mismatched = []
    for entry in catalog:
        try:
            current = _current(client, entry.name, args.label)
        except Exception as e:
            print(f"Could not read back {entry.name}: {type(e).__name__}: {e}")
            return 2
        if current is None or not _same(current[0], texts[entry.name]):
            mismatched.append(entry.name)
    if mismatched:
        print(f"These prompts do not match after publishing: {', '.join(mismatched)}")
        return 2
    print(f"\nAll {len(catalog)} prompts in Langfuse match the repository "
          f"(label {args.label}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
