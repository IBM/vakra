#!/usr/bin/env python3
"""
Test script for MCP server domain filtering.

Usage:
1. First, start the FastAPI server:
   cd /Users/anu/Documents/GitHub/routing/EnterpriseBenchmark/apis/m3/rest
   uvicorn app:app --host 0.0.0.0 --port 8000

2. Then run this test script:
   python test_mcp_filtering.py
"""
import asyncio
from mcp_server import FastAPIMCPServer


async def test_filtering():
    """Test different filtering configurations"""

    fastapi_url = "http://localhost:8000"

    print("=" * 60)
    print("MCP Server Domain Filtering Test")
    print("=" * 60)

    # Test 1: No filtering (all endpoints)
    print("\n[Test 1] No filtering - ALL endpoints")
    print("-" * 40)
    server_all = FastAPIMCPServer(
        fastapi_base_url=fastapi_url,
        server_name="all-endpoints"
    )
    try:
        await server_all.initialize()
        print(f"✓ Total tools discovered: {len(server_all.tools_cache)}")

        # Show unique path prefixes
        prefixes = set()
        for tool in server_all.tools_cache:
            path = tool._metadata["path"]
            # Extract /v1/domain/ prefix
            parts = path.split("/")
            if len(parts) >= 3:
                prefixes.add(f"/v1/{parts[2]}")
        print(f"✓ Unique domains found: {len(prefixes)}")
        print(f"  Sample domains: {sorted(list(prefixes))[:10]}...")
    except Exception as e:
        print(f"✗ Error: {e}")
        print("  Make sure FastAPI server is running on http://localhost:8000")
        return

    # Test 2: Filter by single path prefix (hockey)
    print("\n[Test 2] Filter by path prefix: /v1/hockey")
    print("-" * 40)
    server_hockey = FastAPIMCPServer(
        fastapi_base_url=fastapi_url,
        server_name="hockey-only",
        path_prefixes=["/v1/hockey"]
    )
    await server_hockey.initialize()
    print(f"✓ Hockey tools: {len(server_hockey.tools_cache)}")
    if server_hockey.tools_cache:
        print(f"  Sample tools: {[t.name for t in server_hockey.tools_cache[:3]]}")

    # Test 3: Filter by multiple path prefixes (sports domain)
    print("\n[Test 3] Filter by multiple prefixes (sports domain)")
    print("-" * 40)
    sports_prefixes = [
        "/v1/hockey",
        "/v1/ice_hockey_draft",
        "/v1/professional_basketball",
        "/v1/european_football",
        "/v1/soccer",
        "/v1/olympics",
        "/v1/formula_1"
    ]
    server_sports = FastAPIMCPServer(
        fastapi_base_url=fastapi_url,
        server_name="sports-mcp",
        path_prefixes=sports_prefixes
    )
    await server_sports.initialize()
    print(f"✓ Sports tools: {len(server_sports.tools_cache)}")

    # Test 4: Filter by path prefix with exclusions
    print("\n[Test 4] Filter with exclusions")
    print("-" * 40)
    server_movies_partial = FastAPIMCPServer(
        fastapi_base_url=fastapi_url,
        server_name="movies-partial",
        path_prefixes=["/v1/movie", "/v1/movielens", "/v1/movies_4"],
        excluded_paths=["/v1/movielens"]  # Exclude movielens
    )
    await server_movies_partial.initialize()
    print(f"✓ Movie tools (excluding movielens): {len(server_movies_partial.tools_cache)}")

    # Test 5: Geographic/Address domain
    print("\n[Test 5] Geographic domain")
    print("-" * 40)
    geo_prefixes = ["/v1/address", "/v1/world", "/v1/mondial_geo"]
    server_geo = FastAPIMCPServer(
        fastapi_base_url=fastapi_url,
        server_name="geography-mcp",
        path_prefixes=geo_prefixes
    )
    await server_geo.initialize()
    print(f"✓ Geography tools: {len(server_geo.tools_cache)}")

    # Test 6: Financial domain
    print("\n[Test 6] Financial domain")
    print("-" * 40)
    finance_prefixes = ["/v1/financial", "/v1/coinmarketcap", "/v1/debit_card"]
    server_finance = FastAPIMCPServer(
        fastapi_base_url=fastapi_url,
        server_name="finance-mcp",
        path_prefixes=finance_prefixes
    )
    await server_finance.initialize()
    print(f"✓ Finance tools: {len(server_finance.tools_cache)}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total endpoints available:     {len(server_all.tools_cache)}")
    print(f"Hockey domain:                 {len(server_hockey.tools_cache)}")
    print(f"Sports domain (combined):      {len(server_sports.tools_cache)}")
    print(f"Geography domain:              {len(server_geo.tools_cache)}")
    print(f"Finance domain:                {len(server_finance.tools_cache)}")
    print("\nEach MCP server can be run independently with different")
    print("environment variables to serve different domains.")


async def test_tool_call():
    """Test calling a filtered tool"""
    print("\n" + "=" * 60)
    print("Testing Tool Call")
    print("=" * 60)

    server = FastAPIMCPServer(
        fastapi_base_url="http://localhost:8000",
        server_name="hockey-test",
        path_prefixes=["/v1/hockey"]
    )
    await server.initialize()

    if server.tools_cache:
        # Find a simple tool to test
        tool = server.tools_cache[0]
        print(f"\nTesting tool: {tool.name}")
        print(f"Description: {tool.description[:100]}...")
        print(f"Path: {tool._metadata['path']}")
        print(f"Method: {tool._metadata['method']}")

        # Try calling it (may need arguments)
        try:
            # This is just a demonstration - actual call may need args
            print("\n(To test actual calls, you'd need to provide required arguments)")
        except Exception as e:
            print(f"Note: {e}")


if __name__ == "__main__":
    asyncio.run(test_filtering())
    asyncio.run(test_tool_call())
