"""
Stage 0 -- Offline Template Catalog.

Maintains a JSON catalog that maps Drain3 template IDs to classifications:
  - boilerplate:      appears in ~100% of flows regardless of outcome.
                      Safe to collapse to count-only or filter via must_not.
  - informative:      presence/count varies by outcome. Keep count + examples.
  - decision-marker:  matches decision vocabulary. Always keep full text.
  - unknown:          not yet catalogued. Default -- always keep in full.

New templates that the catalog has never seen default to 'unknown' and are
*never* dropped.  This is the safety net that makes aggressive filtering safe.
"""
import json
import os
from typing import Optional

from src.log_pipeline.config import CATALOG_PATH
from src.utils.logging_config import get_logger

logger = get_logger(__name__)


class TemplateCatalog:
    """Load, query, and persist the template classification catalog."""

    def __init__(self, path: Optional[str] = None):
        self._path = path or CATALOG_PATH
        self._entries: dict = {}  # template_id -> entry dict
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load(self):
        if os.path.exists(self._path):
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Expect a list of entry dicts keyed by template_id
                for entry in data:
                    tid = entry.get("template_id")
                    if tid:
                        self._entries[tid] = entry
                logger.info("Template catalog loaded", template_count=len(self._entries), path=str(self._path))
            except Exception as e:
                logger.error("Failed to load the template catalog", path=str(self._path), error=f"{type(e).__name__}: {e}")
        else:
            logger.info("No template catalog found; every template classifies as unknown", path=str(self._path))

    def save(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(list(self._entries.values()), f, indent=2)
        logger.info("Template catalog saved", template_count=len(self._entries), path=str(self._path))

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------
    def get_classification(self, template_id: str) -> str:
        """Return the classification for a template, defaulting to 'unknown'."""
        entry = self._entries.get(template_id)
        if entry:
            return entry.get("classification", "unknown")
        return "unknown"

    def get_boilerplate_phrases(self) -> list[str]:
        """Return raw template strings classified as boilerplate.
        
        These can be used to build Elasticsearch must_not filters.
        """
        phrases = []
        for entry in self._entries.values():
            if entry.get("classification") == "boilerplate":
                template = entry.get("template", "")
                # Strip Drain3 placeholders to get a matchable phrase
                clean = template.replace("<*>", "").replace("<NUM>", "").strip()
                if len(clean) > 10:  # only use reasonably specific phrases
                    phrases.append(clean)
        return phrases

    # ------------------------------------------------------------------
    # Catalog building (used by build_catalog.py)
    # ------------------------------------------------------------------
    def upsert(self, template_id: str, template: str,
               classification: str = "unknown",
               seen_in_pct_of_flows: float = 0.0,
               outcome_correlation: str = "none"):
        self._entries[template_id] = {
            "template_id": template_id,
            "template": template,
            "classification": classification,
            "seen_in_pct_of_flows": round(seen_in_pct_of_flows, 4),
            "outcome_correlation": outcome_correlation,
        }

    @property
    def size(self) -> int:
        return len(self._entries)
