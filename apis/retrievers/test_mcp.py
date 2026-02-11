"""
Test runner for MCP server in two modes:

1. Direct mode (--mode direct):
   In-process FastAPIMCPServer, calls tools via HTTP to FastAPI.
   Requires: FastAPI server running locally (python run.py)

2. Docker mode (--mode docker):
   Connects to MCP server inside Docker container via stdio protocol.
   Requires: docker compose up (container running)

Usage:
    # Direct mode (default) - requires FastAPI server running
    python test_mcp.py address --max-queries 5
    python test_mcp.py address hockey

    # Docker mode - requires container running
    python test_mcp.py --mode docker address --max-queries 5
    python test_mcp.py --mode docker --container-name retriever-mcp-server address

    # Test all domains
    python test_mcp.py
    python test_mcp.py --mode docker
"""

import argparse
import asyncio
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

QUERIES_DIR = "./queries"
DEFAULT_CONTAINER_NAME = "retriever-mcp-server"

EMPTY_STATS = {
    "tools": 0, "total": 0, "success": 0, "errors": 0,
    "topic_matches": 0, "topic_queries": 0, "topic_rate": 0, "avg_time_s": 0,
}


def load_queries(domain: str) -> list[dict]:
    path = Path(QUERIES_DIR) / f"{domain}_queries.json"
    if not path.exists():
        print(f"  WARNING: No query file found at {path}")
        return []
    with open(path) as f:
        return json.load(f)


def detect_container_runtime() -> str:
    """Detect available container runtime (podman or docker)."""
    if shutil.which("podman"):
        return "podman"
    if shutil.which("docker"):
        return "docker"
    raise RuntimeError("Neither podman nor docker found in PATH.")


def stop_mcp_server(container_runtime: str, container_name: str):
    """Stop any running mcp_server.py processes inside the container."""
    try:
        subprocess.run(
            [container_runtime, "exec", container_name, "pkill", "-f", "python mcp_server.py"],
            capture_output=True, timeout=5,
        )
    except Exception:
        pass


# --------------- Query execution (shared by both modes) ---------------

def process_query_result(response_text: str, expected_topic: str) -> dict:
    """Parse a query response and check for topic match.

    Returns dict with: success, topic_match, top_text, top_dist, error
    """
    if not response_text or response_text.startswith("Error"):
        return {"success": False, "error": response_text or "Empty response"}

    try:
        response = json.loads(response_text)
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON: {e}"}

    result_list = response.get("results", [])
    if not result_list:
        return {"success": False, "error": "NO RESULTS"}

    top = result_list[0]
    top_text = top["text"][:80] + "..." if len(top["text"]) > 80 else top["text"]
    top_dist = top["distance"]

    topic_match = False
    if expected_topic:
        for r in result_list:
            if expected_topic.lower() in r["text"].lower():
                topic_match = True
                break

    return {
        "success": True,
        "topic_match": topic_match,
        "top_text": top_text,
        "top_dist": top_dist,
    }


def print_query_result(i: int, question: str, elapsed: float, result: dict):
    """Print a single query result line."""
    print(f"  Q{i:02d} ({elapsed:.3f}s): {question[:70]}")
    if result["success"]:
        topic_str = " [TOPIC MATCH]" if result.get("topic_match") else ""
        print(f"       Top (dist={result['top_dist']:.4f}): {result['top_text']}{topic_str}")
    else:
        print(f"       {result.get('error', 'UNKNOWN ERROR')}")


def print_domain_summary(domain: str, stats: dict):
    """Print per-domain summary."""
    print(f"\n--- {domain} MCP test summary ---")
    print(f"  Mode: {stats.get('mode', 'unknown')}")
    print(f"  Tools discovered: {stats['tools']}")
    print(f"  Queries: {stats['total']} | Success: {stats['success']} | Errors: {stats['errors']}")
    print(f"  Topic match: {stats['topic_matches']}/{stats['topic_queries']} ({stats['topic_rate']:.1f}%)")
    print(f"  Avg query time: {stats['avg_time_s']:.3f}s")


# --------------- Direct mode ---------------

async def test_domain_direct(domain: str, base_url: str, max_queries: int | None = None) -> dict:
    """Test using in-process FastAPIMCPServer (no MCP protocol, direct HTTP)."""
    from mcp_server import FastAPIMCPServer

    print(f"\n{'=' * 60}")
    print(f"MCP Test [direct]: domain={domain}")
    print(f"{'=' * 60}")

    mcp = FastAPIMCPServer(
        fastapi_base_url=base_url,
        server_name=f"retriever-mcp-{domain}",
        domains=[domain],
    )

    print("\n[1] Discovering tools...")
    try:
        await mcp.initialize()
    except Exception as e:
        print(f"  ERROR: Could not connect to FastAPI server at {base_url}")
        print(f"  {e}")
        print(f"  Make sure the retriever server is running: python run.py")
        return {"domain": domain, "mode": "direct", **EMPTY_STATS}

    tools = await mcp.list_tools()
    print(f"  Found {len(tools)} tool(s):")
    for tool in tools:
        print(f"    - {tool.name}: {tool.description}")

    if not tools:
        print(f"  ERROR: No tools found for domain '{domain}'")
        return {"domain": domain, "mode": "direct", **EMPTY_STATS}

    query_tool = tools[0]

    queries = load_queries(domain)
    if not queries:
        return {"domain": domain, "mode": "direct", "tools": len(tools),
                "total": 0, "success": 0, "errors": 0, "topic_matches": 0,
                "topic_queries": 0, "topic_rate": 0, "avg_time_s": 0}

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
            results = await mcp.call_tool(
                query_tool.name,
                {"body": {"question": question, "n_results": 3}},
            )
            elapsed = time.time() - start
            total_time += elapsed

            response_text = results[0].text if results else ""
            result = process_query_result(response_text, expected_topic)
            print_query_result(i, question, elapsed, result)

            if result["success"]:
                success += 1
                if result.get("topic_match"):
                    topic_matches += 1
            else:
                errors += 1
        except Exception as e:
            errors += 1
            print(f"  Q{i:02d}: {question[:70]}")
            print(f"       ERROR: {e}")

    queries_with_topic = sum(1 for q in queries if q.get("expected_topic"))
    topic_rate = (topic_matches / queries_with_topic * 100) if queries_with_topic else 0
    avg_time = total_time / len(queries) if queries else 0

    stats = {
        "domain": domain,
        "mode": "direct",
        "tools": len(tools),
        "total": len(queries),
        "success": success,
        "errors": errors,
        "topic_matches": topic_matches,
        "topic_queries": queries_with_topic,
        "topic_rate": round(topic_rate, 1),
        "avg_time_s": round(avg_time, 3),
    }
    print_domain_summary(domain, stats)
    return stats


# --------------- Docker mode ---------------

async def test_domain_docker(
    domain: str,
    container_runtime: str,
    container_name: str,
    max_queries: int | None = None,
) -> dict:
    """Test using MCP client over stdio via docker exec.

    Follows the same pattern as benchmark_runner.py: spawns
    `docker exec -i -e MCP_DOMAIN={domain} {container} python mcp_server.py`
    and communicates via the MCP stdio protocol.
    """
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    print(f"\n{'=' * 60}")
    print(f"MCP Test [docker]: domain={domain}")
    print(f"{'=' * 60}")

    queries = load_queries(domain)
    if not queries:
        return {"domain": domain, "mode": "docker", **EMPTY_STATS}

    if max_queries:
        queries = queries[:max_queries]

    exec_args = [
        "exec", "-i",
        "-e", f"MCP_DOMAIN={domain}",
        container_name,
        "python", "mcp_server.py",
    ]
    print(f"\n[1] Starting MCP server: {container_runtime} {' '.join(exec_args)}")

    server_params = StdioServerParameters(
        command=container_runtime,
        args=exec_args,
        env=None,
    )

    success = 0
    errors = 0
    topic_matches = 0
    total_time = 0.0
    num_tools = 0

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()

                # List tools
                response = await session.list_tools()
                num_tools = len(response.tools)
                print(f"  Found {num_tools} tool(s):")
                for tool in response.tools:
                    print(f"    - {tool.name}: {tool.description or ''}")

                if not response.tools:
                    print(f"  ERROR: No tools found for domain '{domain}'")
                    return {"domain": domain, "mode": "docker", **EMPTY_STATS}

                query_tool_name = response.tools[0].name

                # Run queries
                print(f"\n[2] Running {len(queries)} queries via MCP tool '{query_tool_name}'...\n")

                for i, q in enumerate(queries, 1):
                    question = q["question"]
                    expected_topic = q.get("expected_topic", "")

                    try:
                        start = time.time()
                        result = await session.call_tool(
                            query_tool_name,
                            {"body": {"question": question, "n_results": 3}},
                        )
                        elapsed = time.time() - start
                        total_time += elapsed

                        response_text = result.content[0].text if result.content else ""
                        parsed = process_query_result(response_text, expected_topic)
                        print_query_result(i, question, elapsed, parsed)

                        if parsed["success"]:
                            success += 1
                            if parsed.get("topic_match"):
                                topic_matches += 1
                        else:
                            errors += 1
                    except Exception as e:
                        errors += 1
                        print(f"  Q{i:02d}: {question[:70]}")
                        print(f"       ERROR: {e}")

        print("  Server stopped.")
    except ExceptionGroup as eg:
        print(f"  Warning: Cleanup error (ignored): {eg}")
    except Exception as e:
        if "TaskGroup" in str(type(e).__name__) or "TaskGroup" in str(e):
            print(f"  Warning: Cleanup error (ignored): {e}")
        else:
            stop_mcp_server(container_runtime, container_name)
            raise

    queries_with_topic = sum(1 for q in queries if q.get("expected_topic"))
    topic_rate = (topic_matches / queries_with_topic * 100) if queries_with_topic else 0
    avg_time = total_time / len(queries) if queries else 0

    stats = {
        "domain": domain,
        "mode": "docker",
        "tools": num_tools,
        "total": len(queries),
        "success": success,
        "errors": errors,
        "topic_matches": topic_matches,
        "topic_queries": queries_with_topic,
        "topic_rate": round(topic_rate, 1),
        "avg_time_s": round(avg_time, 3),
    }
    print_domain_summary(domain, stats)
    return stats


# --------------- Main ---------------

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

    print(f"Mode: {args.mode}")
    print(f"Testing {len(domains)} domain(s): {', '.join(domains)}")

    if args.mode == "docker":
        container_runtime = args.container_runtime or detect_container_runtime()
        print(f"Container runtime: {container_runtime}")
        print(f"Container name: {args.container_name}")
    else:
        print(f"FastAPI server: {args.base_url}")

    all_stats = []
    for domain in domains:
        if args.mode == "docker":
            stats = await test_domain_docker(
                domain,
                container_runtime,
                args.container_name,
                max_queries=args.max_queries,
            )
        else:
            stats = await test_domain_direct(
                domain,
                args.base_url,
                max_queries=args.max_queries,
            )
        all_stats.append(stats)

    # Overall summary
    print(f"\n{'=' * 60}")
    print(f"MCP TEST OVERALL SUMMARY (mode: {args.mode})")
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
    parser.add_argument(
        "--mode", choices=["direct", "docker"], default="direct",
        help="Test mode: 'direct' uses in-process MCP server, "
             "'docker' connects via stdio to container (default: direct)",
    )
    parser.add_argument("--max-queries", type=int, default=None, help="Max queries per domain")

    # Direct mode options
    parser.add_argument(
        "--base-url", default="http://localhost:8001",
        help="FastAPI base URL for direct mode (default: http://localhost:8001)",
    )

    # Docker mode options
    parser.add_argument(
        "--container-runtime", type=str, default=None,
        help="Container runtime: docker or podman (default: auto-detect)",
    )
    parser.add_argument(
        "--container-name", type=str, default=DEFAULT_CONTAINER_NAME,
        help=f"Container name for docker mode (default: {DEFAULT_CONTAINER_NAME})",
    )

    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
