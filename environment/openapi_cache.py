"""Shared OpenAPI spec loader for the FastAPI-wrapping MCP servers.

Every MCP server process fetches the backend's /openapi.json at startup. Under the
Code Engine HTTP bridge a fresh process is spawned per request, so with many domains
running in parallel the single-process FastAPI backend ends up serving the full
(multi-MB, all-domains) spec several times a second, and fetches start timing out.

The spec never changes while the container runs, so the entrypoint snapshots it once
into $OPENAPI_SPEC_DIR/openapi-<port>.json and every MCP process reads that file
instead. Without OPENAPI_SPEC_DIR (e.g. the local docker-exec flow) behaviour is the
same as before, just with a 30s instead of the 5s httpx default timeout.
"""
import json
import os
from typing import Dict, Optional
from urllib.parse import urlparse

import httpx

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0"}


def _snapshot_path(base_url: str) -> Optional[str]:
    spec_dir = os.environ.get("OPENAPI_SPEC_DIR")
    if not spec_dir:
        return None
    url = urlparse(base_url)
    if url.hostname not in _LOCAL_HOSTS:
        return None
    return os.path.join(spec_dir, f"openapi-{url.port or 80}.json")


async def fetch_openapi(base_url: str, timeout: float = 30.0) -> Dict:
    """Return the OpenAPI spec for base_url, from the snapshot file when available."""
    path = _snapshot_path(base_url)
    if path and os.path.isfile(path):
        with open(path) as f:
            return json.load(f)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(f"{base_url.rstrip('/')}/openapi.json")
        response.raise_for_status()
        return response.json()
