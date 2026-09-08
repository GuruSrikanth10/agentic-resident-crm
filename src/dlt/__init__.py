"""Dead-letter topic (DLT) analysis.

A parallel flow to the rejection pipeline, not an extension of it. See
DLT_PLAN.md for the full design. Shares the log pipeline, storage, consumer
scaffolding and confidence policy; shares neither `MessagePayload`, the
rejection casebook schema, `rules.csv`, nor the runbook key space.
"""
