#!/usr/bin/env python3
"""
Simple example: connect to a running MCP server via WebSocket and list / call tools.

The MCP server must already be running in WebSocket mode before you run this script.

── Starting the MCP server in WebSocket mode ────────────────────────────────

Option A — run locally (no Docker):
    MCP_DOMAIN=hockey python -m environment.m3.python_tools.mcp \
        --transport websocket --port 8000

Option B — inside a running Docker container (expose the port first):
    docker run -d --name cap2_ws -p 8000:8000 \
        -e MCP_DB_ROOT=/app/db -e CAPABILITY_ID=2 -e MCP_DOMAIN=hockey \
        -e SLOT_FILLING_MCP_TRANSPORT=websocket -e SLOT_FILLING_MCP_PORT=8000 \
        benchmark_environ python /app/mcp_dispatch.py

─────────────────────────────────────────────────────────────────────────────

Usage:
    pip install mcp

    python examples/m3_quickstart/simple_websocket.py
    python examples/m3_quickstart/simple_websocket.py --server-url ws://localhost:8000/mcp
    python examples/m3_quickstart/simple_websocket.py --server-url ws://remotehost:9000/mcp \\
        --header "Authorization: Bearer mytoken"
"""

import argparse
import asyncio
import importlib
import sys


async def main(server_url: str, headers: dict) -> None:
    # Lazy import so the script fails with a clear message if the package is missing
    try:
        ws_mod = importlib.import_module("mcp.client.websocket")
        core   = importlib.import_module("mcp")
    except ModuleNotFoundError:
        sys.exit("Install the mcp package first:  pip install mcp")

    websocket_client = ws_mod.websocket_client
    ClientSession    = core.ClientSession

    print(f"Connecting to: {server_url}")
    if headers:
        print(f"Headers: {headers}")
    print()

    async with websocket_client(server_url, headers=headers) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # ── List tools ────────────────────────────────────────────────────
            resp  = await session.list_tools()
            tools = resp.tools
            print(f"Tools ({len(tools)}):")
            for t in tools:
                desc = (t.description or "").split("\n")[0]
                print(f"  {t.name:40s}  {desc[:60]}")

            print()

            # ── Call a tool ───────────────────────────────────────────────────
            # Replace this block with your own tool call.
            # Use `session.call_tool(tool_name, {arg: value, ...})` to invoke any tool.
            #
            # Example (uncomment and adapt):
            #
            # if tools:
            #     first_tool = tools[0].name
            #     print(f"Calling: {first_tool}")
            #     result = await session.call_tool(first_tool, {})
            #     for item in result.content:
            #         print(getattr(item, "text", repr(item)))

            print("Done. Wire in your agent or tool calls above.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simple WebSocket MCP example.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python examples/m3_quickstart/simple_websocket.py
  python examples/m3_quickstart/simple_websocket.py --server-url ws://localhost:8000/mcp
  python examples/m3_quickstart/simple_websocket.py \\
      --server-url ws://myserver/mcp --header "Authorization: Bearer token"
        """,
    )
    parser.add_argument("--server-url", type=str, default="ws://localhost:8000/mcp",
                        help="WebSocket URL of the MCP server (default: ws://localhost:8000/mcp)")
    parser.add_argument("--header", type=str, action="append", metavar="KEY: VALUE",
                        help="Extra HTTP header for WebSocket handshake (repeat for multiple)")
    args = parser.parse_args()

    headers: dict = {}
    for hdr in (args.header or []):
        if ":" in hdr:
            k, v = hdr.split(":", 1)
            headers[k.strip()] = v.strip()

    try:
        asyncio.run(main(args.server_url, headers))
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
