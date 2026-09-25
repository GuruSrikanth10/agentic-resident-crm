"""
Operator CLI for the agent tools.

    python3 -m src.tools.agent_tools list
    python3 -m src.tools.agent_tools prompt investigator
    python3 -m src.tools.agent_tools call get_packet_stage_summary '{"refid": "<refId>"}'

`list` shows every registered tool, whether its toolset is switched on, and
which roles get it now (after AGENT_TOOLS_<ROLE> overrides). `prompt` prints
the AVAILABLE TOOLS section a role's system prompt receives. `call` runs one
tool exactly as an agent would -- same argument validation, same output --
which is how to check a new tool against a real system before an agent uses
it. Touches nothing in the packet path.

Exit codes:
    0  done
    1  could not run (unknown tool or role, arguments that do not validate)
"""
import argparse
import json
import sys

from src.tools.agent_tools import AGENT_ROLES, describe, get_tool, prompt_section


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python3 -m src.tools.agent_tools",
                                     description="Inspect and run the agent tools.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="every registered tool and who gets it")
    prompt = commands.add_parser("prompt", help="the tools section of a role's prompt")
    prompt.add_argument("role", choices=AGENT_ROLES)
    call = commands.add_parser("call", help="run one tool as an agent would")
    call.add_argument("tool")
    call.add_argument("arguments", nargs="?", default="{}",
                      help='a JSON object, e.g. \'{"refid": "..."}\'')
    args = parser.parse_args(argv)

    try:
        if args.command == "list":
            print(json.dumps(describe(), indent=2))
            return 0
        if args.command == "prompt":
            print(prompt_section(args.role) or f"(no tools for {args.role})")
            return 0

        tool = get_tool(args.tool)
        arguments = json.loads(args.arguments)
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be a JSON object")
        print(tool.invoke(arguments))
        return 0
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
