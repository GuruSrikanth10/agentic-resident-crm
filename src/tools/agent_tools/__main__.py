"""
Operator CLI for the tools registered in this package, in-process.

    python3 -m src.tools.agent_tools list
    python3 -m src.tools.agent_tools call bio_get_packet_stage_summary '{"refid": "<refId>"}'

`list` shows every registered tool, its toolset, whether that toolset is
switched on, and the roles and services it names. `call` runs one tool in
this process -- same argument validation, same output as the server -- which
is the quickest way to develop a tool against a real system. No MCP server is
involved; to see what the agents actually get through MCP, use
`python3 -m src.tools.mcp_client`.

Exit codes:
    0  done
    1  could not run (unknown tool, arguments that do not validate)
"""
import argparse
import json
import sys

from src.tools.agent_tools import describe, get_tool


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python3 -m src.tools.agent_tools",
                                     description="Inspect and run the registered tools in-process.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="every registered tool")
    call = commands.add_parser("call", help="run one tool in-process")
    call.add_argument("tool")
    call.add_argument("arguments", nargs="?", default="{}",
                      help='a JSON object, e.g. \'{"refid": "..."}\'')
    args = parser.parse_args(argv)

    try:
        if args.command == "list":
            print(json.dumps(describe(), indent=2))
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
