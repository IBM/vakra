#!/usr/bin/env python3
"""
Single entrypoint for all M3 benchmark MCP servers.

Reads CAPABILITY_ID and exec's the appropriate server — zero proxy overhead
(same os.execv pattern used by apis/bpo/mcp/task3_router.py).

  CAPABILITY_ID=1  → python -m apis.m3.python_tools.mcp        (capability_1_bi_apis: Slot-fill/Selection router)
  CAPABILITY_ID=2  → python /app/m3-rest/mcp_server.py          (capability_2_dashboard_apis: M3 REST MCP)
  CAPABILITY_ID=3  → python /app/apis/bpo/mcp/task3_router.py   (capability_3_multihop_reasoning: BPO ↔ M3 REST router)
  CAPABILITY_ID=4  → python /app/retrievers/task5_mcp_server.py (capability_4_multiturn: M3 REST + Retriever)

Usage inside container:
    docker exec -i -e CAPABILITY_ID=2 <container> python /app/mcp_dispatch.py
"""
import os
import sys

CAPABILITY_ID = os.environ.get("CAPABILITY_ID", "").strip()

_ROUTES = {
    "1": [sys.executable, "-m", "apis.m3.python_tools.mcp"],
    "2": [sys.executable, "/app/m3-rest/mcp_server.py"],
    "3": [sys.executable, "/app/apis/bpo/mcp/task3_router.py"],
    "4": [sys.executable, "/app/retrievers/task5_mcp_server.py"],
}

if CAPABILITY_ID not in _ROUTES:
    print(
        f"mcp_dispatch: CAPABILITY_ID must be one of {list(_ROUTES)}; got {CAPABILITY_ID!r}",
        file=sys.stderr,
    )
    sys.exit(1)

cmd = _ROUTES[CAPABILITY_ID]
os.execv(cmd[0], cmd)
