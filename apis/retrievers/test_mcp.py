"""
Test runner for MCP server domain filtering and query execution.

Starts the MCP server (in-process), lists tools, runs queries through
the MCP tool, and reports results.

Prerequisites:
    1. FastAPI retriever server must be running:
       cd apis/retrievers && python run.py

    2. ChromaDB must be indexed:
       python index_all_domains.py /path/to/docs_by_domains_20k

Usage:
    # Test a single domain
    python test_mcp.py address

    # Test multiple domains
    python test_mcp.py address hockey

    # Test all domains (auto-discovers from queries/ directory)
    python test_mcp.py

    # Limit number of queries per domain
    python test_mcp.py address --max-queries 5

    # Custom FastAPI server URL
    python test_mcp.py address --base-url http://localhost:8001
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from mcp_server import FastAPIMCPServer

QUERIES_DIR = "./queries"


def load_queries(domain: str) -> list[dict]:
    path = Path(QUERIES_DIR) / f"{domain}_queries.json"
    if not path.exists():
        print(f"  WARNING: No query file found at {path}")
        return []
    with open(path) as f:
        return json.load(f)


async def test_domain(domain: str, base_url: str, max_queries: int | None = None) -> dict:
    """Test MCP server for a single domain."""
    print(f"\n{'=' * 60}")
    print(f"MCP Test: domain={domain}")
    print(f"{'=' * 60}")

    # Create MCP server instance for this domain
    mcp = FastAPIMCPServer(
        fastapi_base_url=base_url,
        server_name=f"retriever-mcp-{domain}",
        domains=[domain],
    )

    # Step 1: Initialize and list tools
    print("\n[1] Discovering tools...")
    try:
        await mcp.initialize()
    except Exception as e:
        print(f"  ERROR: Could not connect to FastAPI server at {base_url}")
        print(f"  {e}")
        print(f"  Make sure the retriever server is running: python run.py")
        return {"domain": domain, "tools": 0, "total": 0, "success": 0, "errors": 0, "topic_matches": 0, "topic_queries": 0, "topic_rate": 0, "avg_time_s": 0}

    tools = await mcp.list_tools()
    print(f"  Found {len(tools)} tool(s):")
    for tool in tools:
        print(f"    - {tool.name}: {tool.description}")
        print(f"      Path: {tool._metadata['path']}  Method: {tool._metadata['method']}")

    if not tools:
        print(f"  ERROR: No tools found for domain '{domain}'")
        print(f"  Check that the domain exists (GET /domains on the FastAPI server)")
        return {"domain": domain, "tools": 0, "total": 0, "success": 0, "errors": 0, "topic_matches": 0, "topic_queries": 0, "topic_rate": 0, "avg_time_s": 0}

    # Use the first (and typically only) query tool
    query_tool = tools[0]

    # Step 2: Load and run queries
    queries = load_queries(domain)
    if not queries:
        return {"domain": domain, "tools": len(tools), "total": 0, "success": 0, "errors": 0, "topic_matches": 0, "topic_queries": 0, "topic_rate": 0, "avg_time_s": 0}

    if max_queries:
        queries = queries[:max_queries]

    print(f"\n[2] Running {len(queries)} queries via MCP tool '{query_tool.name}'...\n")

    success = 0
    errors = 0
    topic_matches = 0
    total_time = 0.0

    for i, q in enumerate(queries, 1):
        question = q["question"]
        expected_topic = q.get("expected_topic", "")

        try:
            start = time.time()
            # Domain is pre-filled in the tool when MCP_DOMAIN is set,
            # so we only pass the request body
            results = await mcp.call_tool(
                query_tool.name,
                {
                    "body": {"question": question, "n_results": 3},
                },
            )
            elapsed = time.time() - start
            total_time += elapsed

            # Parse the response
            response_text = results[0].text if results else ""
            if response_text.startswith("Error"):
                errors += 1
                print(f"  Q{i:02d} ({elapsed:.3f}s): {question[:70]}")
                print(f"       {response_text}")
                continue

            response = json.loads(response_text)
            result_list = response.get("results", [])

            if result_list:
                success += 1
                top = result_list[0]
                top_text = top["text"][:80] + "..." if len(top["text"]) > 80 else top["text"]
                top_dist = top["distance"]

                topic_found = ""
                if expected_topic:
                    for r in result_list:
                        if expected_topic.lower() in r["text"].lower():
                            topic_found = " [TOPIC MATCH]"
                            topic_matches += 1
                            break

                print(f"  Q{i:02d} ({elapsed:.3f}s): {question[:70]}")
                print(f"       Top (dist={top_dist:.4f}): {top_text}{topic_found}")
            else:
                errors += 1
                print(f"  Q{i:02d} ({elapsed:.3f}s): {question[:70]}")
                print(f"       NO RESULTS")

        except Exception as e:
            errors += 1
            print(f"  Q{i:02d}: {question[:70]}")
            print(f"       ERROR: {e}")

    # Summary
    avg_time = total_time / len(queries) if queries else 0
    queries_with_topic = sum(1 for q in queries if q.get("expected_topic"))
    topic_rate = (topic_matches / queries_with_topic * 100) if queries_with_topic else 0

    print(f"\n--- {domain} MCP test summary ---")
    print(f"  Tools discovered: {len(tools)}")
    print(f"  Queries: {len(queries)} | Success: {success} | Errors: {errors}")
    print(f"  Topic match: {topic_matches}/{queries_with_topic} ({topic_rate:.1f}%)")
    print(f"  Avg query time: {avg_time:.3f}s")

    return {
        "domain": domain,
        "tools": len(tools),
        "total": len(queries),
        "success": success,
        "errors": errors,
        "topic_matches": topic_matches,
        "topic_queries": queries_with_topic,
        "topic_rate": round(topic_rate, 1),
        "avg_time_s": round(avg_time, 3),
    }


async def run(args):
    # Discover domains
    if args.domains:
        domains = args.domains
    else:
        query_files = sorted(Path(QUERIES_DIR).glob("*_queries.json"))
        if not query_files:
            print(f"No query files found in {QUERIES_DIR}/")
            print("Run index_all_domains.py first to generate them.")
            sys.exit(1)
        domains = [f.stem.replace("_queries", "") for f in query_files]

    print(f"Testing {len(domains)} domain(s): {', '.join(domains)}")
    print(f"FastAPI server: {args.base_url}")

    all_stats = []
    for domain in domains:
        stats = await test_domain(domain, args.base_url, max_queries=args.max_queries)
        all_stats.append(stats)

    # Overall summary
    print(f"\n{'=' * 60}")
    print("MCP TEST OVERALL SUMMARY")
    print(f"{'=' * 60}")

    header = f"{'Domain':<25} {'Tools':>5} {'Queries':>7} {'OK':>5} {'Err':>5} {'TopicMatch':>11} {'Time':>7}"
    print(header)
    print("-" * len(header))
    for s in all_stats:
        topic = f"{s['topic_matches']}/{s['topic_queries']}" if s.get("topic_queries") else "N/A"
        avg_t = f"{s['avg_time_s']:.3f}s" if s["total"] > 0 else "N/A"
        print(f"{s['domain']:<25} {s['tools']:>5} {s['total']:>7} {s['success']:>5} {s['errors']:>5} {topic:>11} {avg_t:>7}")

    print("-" * len(header))
    total_q = sum(s["total"] for s in all_stats)
    total_s = sum(s["success"] for s in all_stats)
    total_e = sum(s["errors"] for s in all_stats)
    total_tm = sum(s["topic_matches"] for s in all_stats)
    total_tq = sum(s.get("topic_queries", 0) for s in all_stats)
    overall_topic = f"{total_tm}/{total_tq}" if total_tq else "N/A"
    print(f"{'TOTAL':<25} {'':>5} {total_q:>7} {total_s:>5} {total_e:>5} {overall_topic:>11}")

    if total_tq:
        print(f"\nOverall topic match rate: {total_tm / total_tq * 100:.1f}%")


def main():
    parser = argparse.ArgumentParser(description="Test MCP server for retriever domains")
    parser.add_argument("domains", nargs="*", help="Domains to test (default: all)")
    parser.add_argument("--max-queries", type=int, default=None, help="Max queries per domain")
    parser.add_argument("--base-url", default="http://localhost:8001", help="FastAPI base URL (default: http://localhost:8001)")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
