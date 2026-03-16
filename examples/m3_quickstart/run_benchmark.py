#!/usr/bin/env python3
"""
Enterprise Benchmark Quickstart — run your agent against the Docker benchmark environment.

Supports two connection modes:
  docker    (default) — connect to a running Docker/Podman container via docker exec + stdio
  websocket           — connect to an MCP server already running in WebSocket mode

Usage:
    # ── Docker mode (default) ─────────────────────────────────────────────────
    # Single domain smoke test
    python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey

    # Just list tools (no agent run)
    python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --list-tools

    # Run all domains for a capability
    python examples/m3_quickstart/run_benchmark.py --capability-id 2

    # Limit to first N questions
    python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --max-samples 3

    # Choose LLM provider
    python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --provider openai

    # Save results
    python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --out results.json

    # ── WebSocket mode ────────────────────────────────────────────────────────
    # Start the MCP server in WebSocket mode first (inside or outside Docker), then:
    python examples/m3_quickstart/run_benchmark.py \\
        --mode websocket --server-url ws://localhost:8000/mcp \\
        --capability-id 2 --domain hockey

    # With authentication header
    python examples/m3_quickstart/run_benchmark.py \\
        --mode websocket --server-url ws://localhost:8000/mcp \\
        --capability-id 2 --domain hockey --ws-header "Authorization: Bearer <token>"
"""

import argparse
import asyncio
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

import yaml
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ── project root on path ────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

# ── paths ────────────────────────────────────────────────────────────────────
CONFIG_FILE = Path(__file__).parent / "server.yaml"


# ─────────────────────────────────────────────────────────────────────────────
# Docker / Podman helpers
# ─────────────────────────────────────────────────────────────────────────────

def _detect_runtime(preferred: str = "docker") -> str:
    if shutil.which(preferred):
        return preferred
    fallback = "podman" if preferred == "docker" else "docker"
    if shutil.which(fallback):
        print(f"  [{preferred} not found — using {fallback}]")
        return fallback
    sys.exit("Error: neither docker nor podman found on PATH. Install one first.")


def _make_server_params(cfg: dict, domain: str, runtime: str) -> StdioServerParameters:
    """Build StdioServerParameters for `docker exec -i` into the capability container."""
    env_args = []
    for k, v in cfg.get("env", {}).items():
        env_args += ["-e", f"{k}={v}"]
    if domain:
        env_args += ["-e", f"MCP_DOMAIN={domain}"]
    container = cfg["container"]
    command   = cfg["command"]
    args = ["exec", "-i"] + env_args + [container] + command
    return StdioServerParameters(command=runtime, args=args, env=None)


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark data loading
# ─────────────────────────────────────────────────────────────────────────────

def _load_items(capability_id: int, domain: str) -> list[dict]:
    """Load benchmark questions for a domain using the project's data loader."""
    try:
        from benchmark.runner_helpers import load_benchmark_data
        items, _ = load_benchmark_data(capability_id=capability_id, domains=[domain])
    except SystemExit:
        print(
            f"  [!] Benchmark data not found for capability {capability_id}, domain '{domain}'.\n"
            f"      Run `make download` to download it.\n"
            f"      Continuing with tool listing only."
        )
        return []
    return [
        {
            "uuid":   item.uuid,
            "domain": item.domain,
            "query":  item.query,
        }
        for item in items
    ]


def _discover_domains(capability_id: int) -> list[str]:
    """Return sorted list of available domains from the data directory."""
    try:
        from benchmark.runner_helpers import load_benchmark_data
        _, domain_names = load_benchmark_data(capability_id=capability_id, domain_names_only=True)
        return domain_names
    except SystemExit:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Tool printing
# ─────────────────────────────────────────────────────────────────────────────

def _print_tools(tools: list) -> None:
    for t in tools:
        print(f"  {t.name}")
        if t.description:
            first_line = t.description.strip().split("\n")[0]
            print(f"    {first_line}")
        props    = t.inputSchema.get("properties", {}) if t.inputSchema else {}
        required = set((t.inputSchema or {}).get("required", []))
        if props:
            print("    Parameters:")
            for param, schema in props.items():
                req   = " *" if param in required else "  "
                ptype = schema.get("type", "any")
                desc  = schema.get("description", "").split("\n")[0][:60]
                print(f"     {req} {param}: {ptype}  {desc}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Core runner
# ─────────────────────────────────────────────────────────────────────────────

import contextlib
import importlib

@contextlib.asynccontextmanager
async def _connect(mode: str, cfg: dict, domain: str, runtime: str, server_url: str, ws_headers: dict):
    """Yield an initialised ClientSession for either docker-exec or websocket mode."""
    if mode == "websocket":
        ws_mod = importlib.import_module("mcp.client.websocket")
        websocket_client = ws_mod.websocket_client
        print(f"  Connecting via WebSocket: {server_url}")
        async with websocket_client(server_url, headers=ws_headers) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session
    else:
        params = _make_server_params(cfg, domain, runtime)
        print(f"  Command: {runtime} {' '.join(params.args)}")
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session


async def run_domain(
    capability_id: int,
    domain:        str,
    cfg:           dict,
    mode:          str,
    runtime:       str,
    server_url:    str,
    ws_headers:    dict,
    provider:      str,
    model:         Optional[str],
    max_samples:   Optional[int],
    list_only:     bool,
) -> list[dict]:
    """Connect to the MCP server for one domain, optionally run the agent."""
    print(f"\n{'='*60}")
    print(f"Capability {capability_id} | Domain: {domain!r} | Mode: {mode}")

    results = []

    async with _connect(mode, cfg, domain, runtime, server_url, ws_headers) as session:
        resp  = await session.list_tools()
        tools = resp.tools
        print(f"Tools ({len(tools)}):")
        _print_tools(tools)

        if list_only:
            return results

        # ── load benchmark items ──────────────────────────────────────
        items = _load_items(capability_id, domain)
        if not items:
            return results
        if max_samples:
            items = items[:max_samples]

        print(f"Running {len(items)} question(s) with provider={provider!r} ...\n")

        # ── import agent ──────────────────────────────────────────────
        from my_agent import run_query  # noqa: PLC0415

        for i, item in enumerate(items, 1):
            print(f"  [{i}/{len(items)}] {item['query'][:100]}")
            try:
                answer = await run_query(session, item["query"], provider, model or "")
                status = "success"
                error  = ""
            except Exception as exc:
                answer = ""
                status = "error"
                error  = str(exc)
                print(f"    x {error[:80]}")

            result = {
                "uuid":   item["uuid"],
                "domain": domain,
                "query":  item["query"],
                "answer": answer,
                "status": status,
                "error":  error,
            }
            if status == "success":
                print(f"    > {answer[:120]}")
            results.append(result)

    return results


async def main(args) -> None:
    config       = yaml.safe_load(CONFIG_FILE.read_text())
    capabilities = config["capabilities"]

    cap_key = str(args.capability_id)
    if cap_key not in capabilities:
        sys.exit(
            f"Capability {args.capability_id} not found in {CONFIG_FILE}. "
            f"Available: {sorted(capabilities.keys())}"
        )
    cfg = capabilities[cap_key]

    runtime = _detect_runtime(args.runtime) if args.mode == "docker" else ""

    ws_headers: dict = {}
    if args.ws_header:
        for hdr in args.ws_header:
            if ":" in hdr:
                k, v = hdr.split(":", 1)
                ws_headers[k.strip()] = v.strip()

    if args.mode == "websocket" and not args.server_url:
        sys.exit("--server-url is required when --mode websocket")

    # Determine domains to run
    if args.domain:
        domains = [args.domain]
    else:
        domains = _discover_domains(args.capability_id)
        if not domains:
            sys.exit(
                f"No benchmark data found for capability {args.capability_id}.\n"
                f"Either pass --domain <name> or run:  make download"
            )
        print(f"Found {len(domains)} domain(s) for Capability {args.capability_id}: "
              f"{domains[:5]}{'...' if len(domains)>5 else ''}")

    all_results = []
    for domain in domains:
        results = await run_domain(
            capability_id = args.capability_id,
            domain        = domain,
            cfg           = cfg,
            mode          = args.mode,
            runtime       = runtime,
            server_url    = args.server_url or "",
            ws_headers    = ws_headers,
            provider      = args.provider,
            model         = args.model,
            max_samples   = args.max_samples,
            list_only     = args.list_tools,
        )
        all_results.extend(results)

    # Summary
    if all_results:
        ok  = sum(1 for r in all_results if r["status"] == "success")
        err = len(all_results) - ok
        print(f"\n{'='*60}")
        print(f"Done: {ok} success, {err} error(s)  ({len(all_results)} total)")

    # Save results
    if args.out and all_results:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"Results saved to: {out_path}")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Run your agent against the Enterprise Benchmark Docker environment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick smoke test — single domain, first 2 questions
  python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --max-samples 2

  # Just list available tools (no agent run)
  python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --list-tools

  # Run all Capability 2 domains with Anthropic Claude
  python examples/m3_quickstart/run_benchmark.py --capability-id 2 --provider anthropic

  # Save results
  python examples/m3_quickstart/run_benchmark.py --capability-id 2 --domain hockey --out out/hockey.json

  # WebSocket mode (start MCP server with WebSocket transport first)
  python examples/m3_quickstart/run_benchmark.py \\
      --mode websocket --server-url ws://localhost:8000/mcp \\
      --capability-id 2 --domain hockey
        """,
    )
    parser.add_argument("--capability-id", type=int, required=True, choices=[1, 2, 3, 4],
                        help="Which capability to run (1, 2, 3, or 4)")
    parser.add_argument("--domain", type=str, default=None,
                        help="Domain to test (e.g. hockey). "
                             "Omit to run all domains discovered from the benchmark data.")
    parser.add_argument("--mode", type=str, default="docker", choices=["docker", "websocket"],
                        help="Connection mode: docker (default) or websocket")
    parser.add_argument("--server-url", type=str, default=None,
                        help="WebSocket URL, e.g. ws://localhost:8000/mcp (required for --mode websocket)")
    parser.add_argument("--ws-header", type=str, action="append", metavar="KEY: VALUE",
                        help="Extra HTTP header for WebSocket handshake. Repeat for multiple.")
    parser.add_argument("--provider", type=str, default="anthropic",
                        choices=["anthropic", "openai", "ollama", "litellm"],
                        help="LLM provider (default: anthropic)")
    parser.add_argument("--model", type=str, default=None,
                        help="Model name (default: provider-specific default)")
    parser.add_argument("--max-samples", type=int, default=None,
                        help="Stop after this many questions per domain (default: all)")
    parser.add_argument("--list-tools", action="store_true",
                        help="Print available tools and exit without running the agent")
    parser.add_argument("--out", type=str, default=None,
                        help="Save results as JSON to this path")
    parser.add_argument("--runtime", type=str, default="docker", choices=["docker", "podman"],
                        help="Container runtime for docker mode (default: docker; auto-falls back to podman)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
