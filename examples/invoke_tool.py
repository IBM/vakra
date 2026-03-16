#!/usr/bin/env python3
"""
Invoke a specific MCP tool with JSON arguments.

Connects to the MCP server for a given capability and domain, then calls the
requested tool and prints the result.

Usage:
    python examples/invoke_tool.py --capability-id 2 --domain hockey --tool <tool_name>
    python examples/invoke_tool.py --capability-id 2 --domain hockey --tool <tool_name> --args '{"key": "value"}'
    python examples/invoke_tool.py --capability-id 2 --domain hockey --list
"""
import asyncio
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from benchmark.mcp_client import load_mcp_config, create_client_and_connect, MCPConnectionConfig

DEFAULT_CONFIG = "benchmark/mcp_connection_config.yaml"


async def _run(capability_id: int, domain: str, tool_name: str,
               tool_args: dict, config_path: str, list_only: bool):
    configs = load_mcp_config(config_path)
    cfg = configs.get(capability_id)
    if cfg is None:
        raise ValueError(
            f"Capability {capability_id} not found in {config_path}. "
            f"Available: {sorted(configs.keys())}"
        )

    async with create_client_and_connect(cfg, domain) as session:
        tools_result = await session.list_tools()
        tools = {t.name: t for t in tools_result.tools}

        if list_only:
            print(f"\nAvailable tools for Capability {capability_id} / domain {domain!r} ({len(tools)}):\n")
            for name, tool in tools.items():
                desc = (tool.description or "").split("\n")[0].strip()
                print(f"  {name}")
                if desc:
                    print(f"    {desc}")
            return

        if tool_name not in tools:
            close = [n for n in tools if tool_name.lower() in n.lower()]
            hint = f"  Did you mean: {close}" if close else ""
            raise ValueError(
                f"Tool {tool_name!r} not found.{hint}\n"
                f"Available tools: {sorted(tools.keys())}"
            )

        tool = tools[tool_name]
        schema = tool.inputSchema or {}
        props = schema.get("properties", {})
        required = set(schema.get("required", []))

        print(f"Tool:   {tool_name}")
        if tool.description:
            print(f"Desc:   {tool.description.strip().split(chr(10))[0]}")
        if props:
            print(f"Params: {json.dumps({k: v.get('type', '?') for k, v in props.items()}, indent=2)}")
        if required:
            print(f"Required: {sorted(required)}")
        print(f"\nCalling with args: {json.dumps(tool_args, indent=2)}\n")

        result = await session.call_tool(tool_name, tool_args)

        print("Result:")
        if result.content:
            for item in result.content:
                if hasattr(item, "text"):
                    # Try to pretty-print JSON
                    try:
                        parsed = json.loads(item.text)
                        print(json.dumps(parsed, indent=2))
                    except (json.JSONDecodeError, TypeError):
                        print(item.text)
                else:
                    print(repr(item))
        else:
            print("(empty response)")

        if result.isError:
            print("\nNote: tool returned an error flag", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Invoke an MCP tool and print the result.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available tools first
  python examples/invoke_tool.py --capability-id 2 --domain hockey --list

  # Call a tool with no arguments
  python examples/invoke_tool.py --capability-id 2 --domain hockey --tool get_hockey_teams

  # Call a tool with JSON arguments
  python examples/invoke_tool.py --capability-id 2 --domain hockey \\
      --tool get_hockey_schedule --args '{"season": "2023", "team_id": 5}'

  # Pipe args from a file
  python examples/invoke_tool.py --capability-id 3 --domain bpo \\
      --tool get_sla_report --args "$(cat args.json)"
        """,
    )
    parser.add_argument("--capability-id", type=int, required=True,
                        help="Capability ID (1, 2, 3, or 4)")
    parser.add_argument("--domain", type=str, default="",
                        help="Domain name (e.g. hockey, bpo, superhero)")
    parser.add_argument("--tool", type=str, default=None,
                        help="Tool name to invoke")
    parser.add_argument("--args", type=str, default="{}",
                        help="Tool arguments as a JSON string (default: {})")
    parser.add_argument("--list", action="store_true",
                        help="List available tools and exit (no tool invocation)")
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG,
                        help=f"Path to MCP connection config YAML (default: {DEFAULT_CONFIG})")
    args = parser.parse_args()

    if not args.list and not args.tool:
        parser.error("Provide --tool <name> to invoke a tool, or --list to see available tools.")

    try:
        tool_args = json.loads(args.args)
    except json.JSONDecodeError as e:
        print(f"Error parsing --args JSON: {e}", file=sys.stderr)
        sys.exit(1)

    domain_label = args.domain or "(no domain filter)"
    print(f"Connecting to Capability {args.capability_id} MCP server  [domain: {domain_label}] ...")

    try:
        asyncio.run(_run(
            capability_id=args.capability_id,
            domain=args.domain,
            tool_name=args.tool,
            tool_args=tool_args,
            config_path=args.config,
            list_only=args.list,
        ))
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
