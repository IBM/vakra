"""Shared OpenAPI spec loader for the FastAPI-wrapping MCP servers.

Every MCP server process fetches the backend's /openapi.json at startup. Under the
Code Engine HTTP bridge many such processes run at once, and the M3 spec is ~6 MB
covering all 62 domains, so fetching it live made the single-process FastAPI
backend the bottleneck, and holding a parsed copy costs ~100 MB per process.

The spec never changes while the container runs, so the entrypoint snapshots it once
into $OPENAPI_SPEC_DIR and slices it per domain
(`python -m environment.openapi_cache slice <file>`):

    openapi-<port>.json                 full spec
    openapi-<port>.d/<domain>.json      that domain's /v1/<domain>/* paths + shared
                                        components (a few KB to ~300 KB)

A server pinned to one MCP_DOMAIN loads only its slice; tool lists are identical
because every wrapper filters to /v1/<domain> paths anyway. Without
OPENAPI_SPEC_DIR (e.g. the local docker-exec flow) the spec is fetched live as
before, just with a 30s instead of the 5s httpx default timeout.
"""
import json
import os
import sys
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
    full = os.path.join(spec_dir, f"openapi-{url.port or 80}.json")
    domain = os.environ.get("MCP_DOMAIN", "").strip()
    if domain and "," not in domain:
        sliced = os.path.join(spec_dir, f"openapi-{url.port or 80}.d", f"{domain}.json")
        if os.path.isfile(sliced):
            return sliced
    return full


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


def write_domain_slices(full_path: str) -> int:
    """Split openapi-<port>.json into openapi-<port>.d/<domain>.json; return count."""
    with open(full_path) as f:
        spec = json.load(f)
    paths = spec.get("paths", {})
    domains = {p.split("/")[2] for p in paths if p.startswith("/v1/") and len(p.split("/")) > 2}
    # The wrappers select endpoints with path.startswith(f"/v1/{domain}") -- no
    # trailing slash -- so e.g. "movie" also exposes /v1/movie_3/* and /v1/movies_4/*.
    # Slices must use the same rule or tool lists (and checksums) would change.
    by_domain: Dict[str, Dict] = {
        d: {p: item for p, item in paths.items() if p.startswith(f"/v1/{d}")}
        for d in domains if d
    }
    out_dir = full_path[: -len(".json")] + ".d"
    os.makedirs(out_dir, exist_ok=True)
    shared = {k: v for k, v in spec.items() if k != "paths"}
    for domain, paths in by_domain.items():
        tmp = os.path.join(out_dir, f".{domain}.json.tmp")
        with open(tmp, "w") as f:
            json.dump({**shared, "paths": paths}, f)
        os.replace(tmp, os.path.join(out_dir, f"{domain}.json"))
    return len(by_domain)


if __name__ == "__main__":
    # python -m environment.openapi_cache slice <spec_dir>/openapi-<port>.json
    if len(sys.argv) == 3 and sys.argv[1] == "slice":
        print(f"{sys.argv[2]}: {write_domain_slices(sys.argv[2])} domain slices")
    else:
        sys.exit("usage: python -m environment.openapi_cache slice <openapi-PORT.json>")
