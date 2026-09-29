"""
enu-biometric's own tools (MULTI_SERVICE_PLAN.md D7, D8).

Every toolset here is scoped to `services=("enu-biometric",)`, so no other
service's packet is offered these tools, and every tool is named with the
service's `tool_prefix`, `bio_`. The package name is the service name with
its hyphen turned into an underscore, which a Python package name needs.

- `_process_db.py`: the `process_db` toolset and the process database
  (uidprocessv2_2) it reads, through the shared read-only layer.
- `stage_tracker.py`: bio_stage_tracker -- stage summary and timeline.
- `parking_queue.py`: bio_parking_queue_store -- parking status.
- `helper_cache.py`: bio_helper_cache_store -- ABIS candidates, candidate
  facts, parking verdicts, the Update Checker result, and any field by
  JSON_EXTRACT.
"""
