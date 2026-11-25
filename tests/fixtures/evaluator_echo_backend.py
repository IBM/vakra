"""Minimal MCP fixture for testing evaluator transports without environment data."""
import asyncio
import json
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool


def make_server():
    server = Server('evaluator-echo')
    @server.list_tools()
    async def tools():
        return [Tool(name='echo', description='Return the input value', inputSchema={
            'type': 'object', 'properties': {'value': {'type': 'string'}}, 'required': ['value']})]
    @server.call_tool()
    async def call(name, arguments):
        return [TextContent(type='text', text=json.dumps({'value': arguments['value']}))]
    return server


if __name__ == '__main__':
    async def main():
        server = make_server()
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())
    asyncio.run(main())
