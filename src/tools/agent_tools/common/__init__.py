"""
Tools whose meaning holds for every service (MULTI_SERVICE_PLAN.md D7).

A toolset here declares `services=("*",)`, and its tool names carry no
service's prefix (D8). Such a tool is offered to every service's packets --
unresolved ones admitted with the `_default` pack included -- so it must not
read, or describe, anything one service alone has. A tool for one service
belongs in that service's own package, `agent_tools/<service_slug>/`, even
when the database it reads is shared.

None ship yet.
"""
