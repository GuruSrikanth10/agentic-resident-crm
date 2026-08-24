import os
import json
from pathlib import Path
from typing import Optional
from filelock import FileLock
from src.utils.atomic import replace_with_retry
from src.storage.base import (
    CASEBOOK_SCHEMA_VERSION,
    CasebookStorage,
    TERMINAL_STATUSES,
)
from src.utils.paths import LOCAL_CASESHEETS_DIR

class LocalFilesystemCasebookStorage(CasebookStorage):
    def __init__(self, base_dir: str = None):
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            self.base_dir = LOCAL_CASESHEETS_DIR


        os.makedirs(self.base_dir, exist_ok=True)
        
    def _resolve_dir(self, event_id: str) -> Path:
        """Resolve the casebook directory for event_id without creating it."""
        base_resolved = self.base_dir.resolve()
        target_dir = (self.base_dir / f"casebook_{event_id}").resolve()

        # Defense in depth alongside the Pydantic eventId pattern (0.11):
        # never create or touch a path that resolves outside the storage root.
        try:
            target_dir.relative_to(base_resolved)
        except ValueError:
            raise ValueError(f"Resolved casebook directory escapes storage root: {target_dir}")

        return target_dir

    def _get_dir(self, event_id: str) -> Path:
        """Resolve and create the casebook directory for event_id.

        Only save() should create directories -- load()/exists() calling
        this used to create an empty casebook_<id>/ directory for every
        existence check, including events that were skipped entirely (1.17).
        """
        target_dir = self._resolve_dir(event_id)
        os.makedirs(target_dir, exist_ok=True)
        return target_dir

    def save(self, event_id: str, casebook: dict, filename: str = "casebook.json") -> None:
        target_dir = self._get_dir(event_id)
        final_path = target_dir / filename
        tmp_path = target_dir / f"{filename}.tmp"
        lock_path = target_dir / f"{filename}.lock"

        # Enforce schema version for backwards compatibility
        if "schema_version" not in casebook:
            casebook["schema_version"] = CASEBOOK_SCHEMA_VERSION

        with FileLock(str(lock_path), timeout=10):
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(casebook, f, indent=4, ensure_ascii=False)
            replace_with_retry(tmp_path, final_path)

    def save_terminal(self, event_id: str, casebook: dict) -> None:
        """Write casebook.json then status.json for one terminal outcome.

        casebook.json is written first: if the process dies between the two
        writes, a stale IN_PROGRESS status.json goes stale on its own after
        MAX_IN_PROGRESS_AGE_SECONDS, whereas a terminal status.json with no
        casebook behind it would suppress reprocessing forever.
        """
        self.save(event_id, casebook)

        status = (casebook.get("packet_status") or {}).get("status")
        self.save(event_id, {
            "packet_metadata": {"eid": event_id},
            "packet_status": {"status": status},
            "resolution": {
                "synthesis": (casebook.get("resolution") or {}).get("synthesis")
            },
        }, filename="status.json")

    def terminal_status(self, event_id: str,
                        filenames: tuple = ("status.json", "casebook.json")) -> Optional[str]:
        """Return the terminal status found in either file, or None."""
        for filename in filenames:
            data = self.load(event_id, filename=filename)
            if not data:
                continue
            status = (data.get("packet_status") or {}).get("status")
            if status in TERMINAL_STATUSES:
                return status
        return None

    def load(self, event_id: str, filename: str = "casebook.json") -> Optional[dict]:
        target_dir = self._resolve_dir(event_id)
        final_path = target_dir / filename
        lock_path = target_dir / f"{filename}.lock"
        
        if not final_path.exists():
            return None
            
        with FileLock(str(lock_path), timeout=10):
            try:
                with open(final_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None
                
    def exists(self, event_id: str, terminal_only: bool = False, filename: str = "casebook.json") -> bool:
        data = self.load(event_id, filename=filename)
        if not data:
            return False

        if terminal_only:
            return data.get("packet_status", {}).get("status") in TERMINAL_STATUSES

        return True

    # ------------------------------------------------------------------
    # Blob artifacts (G3)
    # ------------------------------------------------------------------

    def save_artifact(self, event_id: str, filename: str, content: str) -> str:
        """Atomic write, same discipline as save(): a crash mid-write must not
        leave a half-written trace for a later run to trust."""
        target_dir = self._get_dir(event_id)
        final_path = target_dir / filename
        tmp_path = target_dir / f"{filename}.tmp"
        lock_path = target_dir / f"{filename}.lock"

        with FileLock(str(lock_path), timeout=10):
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)
            replace_with_retry(tmp_path, final_path)

        return str(final_path)

    def load_artifact(self, event_id: str, filename: str) -> Optional[str]:
        final_path = self._resolve_dir(event_id) / filename
        if not final_path.exists():
            return None
        lock_path = self._resolve_dir(event_id) / f"{filename}.lock"
        with FileLock(str(lock_path), timeout=10):
            try:
                return final_path.read_text(encoding="utf-8")
            except Exception:
                return None

    def artifact_exists(self, event_id: str, filename: str) -> bool:
        return (self._resolve_dir(event_id) / filename).exists()

    def update_json(self, event_id: str, filename: str, mutate) -> dict:
        """Read-modify-write under the file's own lock.

        The same `FileLock` that `save()` takes, held across the read AND the
        write rather than once for each -- which is the whole point. filelock
        coordinates processes on a shared filesystem, so this is safe for
        several processes on one host; it is NOT safe across hosts, which is
        what the S3 backend's conditional write is for.
        """
        target_dir = self._get_dir(event_id)
        final_path = target_dir / filename
        tmp_path = target_dir / f"{filename}.tmp"
        lock_path = target_dir / f"{filename}.lock"

        with FileLock(str(lock_path), timeout=10):
            current = None
            if final_path.exists():
                try:
                    with open(final_path, "r", encoding="utf-8") as f:
                        current = json.load(f)
                except Exception:
                    current = None

            updated = mutate(current)
            if "schema_version" not in updated:
                updated["schema_version"] = CASEBOOK_SCHEMA_VERSION

            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(updated, f, indent=4, ensure_ascii=False)
            replace_with_retry(tmp_path, final_path)

        return updated

    def list_events(self) -> list:
        if not self.base_dir.exists():
            return []
        return sorted(
            path.name[len("casebook_"):]
            for path in self.base_dir.iterdir()
            if path.is_dir() and path.name.startswith("casebook_")
        )
