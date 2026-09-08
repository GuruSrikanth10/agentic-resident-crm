"""
The `LogSource` protocol (KUBERNETES_LOGS_PLAN.md 4.2).

Mirrors the `CasebookStorage` Protocol in `src/storage/base.py`: a structural
interface with no base class to inherit, so a source is anything with the
right shape.
"""
from typing import Protocol, runtime_checkable

from src.log_pipeline.types import FetchContext, FetchResult, TimeWindow


@runtime_checkable
class LogSource(Protocol):
    """A Stage 1 log source.

    `fetch` returns a `FetchResult` rather than a bare list because gaps are
    first-class: a source that cannot express "I returned less than you asked
    for" structurally cannot satisfy design principle 2.
    """

    #: Stable identifier, stamped onto every record's `source` field.
    name: str

    def fetch(self, identifier: str, window: TimeWindow,
              ctx: FetchContext) -> FetchResult:
        """Fetch log records matching `identifier` within `window`."""
        ...
