import contextlib
from dataclasses import dataclass
import logging
import os
import subprocess
import sys
import yaml
from uuid import uuid4
from urllib.parse import quote, urlsplit

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from typing import AsyncGenerator, Dict, List, Optional

from benchmark.utils import _assert_container_running, detect_container_runtime

logger = logging.getLogger(__name__)

# Default settings
DEFAULT_CONTAINER_NAME = "fastapi-mcp-server"


@dataclass
class MCPConnectionConfig:
    """Connection settings for a single task's MCP server."""
    mode: str = "stdio"
    container_name: str = DEFAULT_CONTAINER_NAME
    container_runtime: Optional[str] = None  # None = auto-detect
    container_command: Optional[List[str]] = None
    container_env: Optional[Dict[str, str]] = None
    command: Optional[str] = None
    args: Optional[List[str]] = None
    server_url: Optional[str] = None
    require_persistent_backend: bool = False
    # Optional second MCP server launched in the same container (e.g. Task 3:
    # primary=BPO server, secondary=M3 REST server).  When set, benchmark_runner
    # opens a second docker-exec session and merges the tool lists.
    secondary_container_command: Optional[List[str]] = None


def load_mcp_config(config_path: str) -> Dict[int, MCPConnectionConfig]:
    """Load MCP connection config from YAML file.

    Returns a dict mapping capability_id (int) to MCPConnectionConfig.
    """
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    tasks = data.get("tasks", {})
    result = {}
    for k, v in tasks.items():
        cmd = v.get("command", None)
        # "python" in config means the current Python executable (virtualenv-safe)
        if cmd == "python":
            cmd = sys.executable
        result[int(k)] = MCPConnectionConfig(
            mode=v.get("mode", "stdio"),
            container_name=v.get("container_name", DEFAULT_CONTAINER_NAME),
            container_runtime=v.get("container_runtime", None),
            container_command=v.get("container_command", None),
            container_env=v.get("container_env", None),
            command=cmd,
            args=v.get("args", None),
            server_url=os.path.expandvars(v["server_url"]) if v.get("server_url") else None,
            secondary_container_command=v.get("secondary_container_command", None),
        )
    return result


def select_evaluator_mcp_config(capability_id: int, backend: str = "local", config_path: Optional[str] = None) -> MCPConnectionConfig:
    """Select a transport without changing the existing local configuration."""
    if backend not in ("local", "code-engine"):
        raise ValueError(f"Unknown tool backend: {backend}")
    path = config_path or ("benchmark/mcp_connection_config.ce.yaml" if backend == "code-engine" else "benchmark/mcp_connection_config.yaml")
    configs = load_mcp_config(path)
    if capability_id not in configs:
        raise ValueError(f"No MCP configuration for capability {capability_id} in {path}")
    cfg = configs[capability_id]
    cfg.require_persistent_backend = capability_id == 1 and cfg.mode in ("http", "streamable-http")
    if backend == "code-engine":
        if cfg.mode not in ("http", "streamable-http"):
            raise ValueError("code-engine requires an HTTP MCP configuration")
        resolve_http_url(cfg, "validation")
    return cfg


def resolve_http_url(cfg: MCPConnectionConfig, domain: str) -> str:
    if not cfg.server_url or "$" in cfg.server_url:
        raise ValueError("Set the selected VAKRA_CAPn_URL environment variable or provide server_url in --mcp-config")
    if not domain:
        raise ValueError("HTTP MCP connections require a domain")
    base = cfg.server_url
    url = base.replace("{domain}", quote(domain, safe="")) if "{domain}" in base else f"{base.rstrip('/')}/mcp/{quote(domain, safe='')}"
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("server_url must be an absolute HTTP(S) URL")
    return url


@contextlib.asynccontextmanager
async def _http_session(url: str, domain: str, *, require_persistent_backend: bool = False):
    """One private bridge backend for this connection; release it on close."""
    headers = {"X-MCP-Domain": domain, "X-Vakra-Session": uuid4().hex}
    if require_persistent_backend:
        import httpx
        health_url = url.split("/mcp/", 1)[0] + "/healthz"
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(health_url)
            response.raise_for_status()
            if response.json().get("persistent_backends") is not True:
                raise RuntimeError("Capability 1 needs the session-preserving Code Engine bridge; redeploy docker/mcp_http_bridge.py before evaluating")
    # MCP 1.x and 2.x expose different HTTP transport constructors.
    import mcp.client.streamable_http as transport
    legacy_client = getattr(transport, "streamablehttp_client", None)
    try:
        async with contextlib.AsyncExitStack() as stack:
            if legacy_client is not None:
                connection = legacy_client(url, headers=headers, timeout=600, sse_read_timeout=1800)
            else:
                import httpx2
                client = await stack.enter_async_context(httpx2.AsyncClient(headers=headers, timeout=600))
                connection = transport.streamable_http_client(url, http_client=client)
            read, write, _ = await stack.enter_async_context(connection)
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            yield session
    finally:
        await _release_http_backend(url, headers)


async def _release_http_backend(url: str, headers: dict) -> None:
    import httpx
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.delete(url, headers=headers)
    except Exception:
        logger.warning("Could not release remote backend; bridge idle expiry will clean it up", exc_info=True)


@contextlib.asynccontextmanager
async def create_client_and_connect(
    cfg: MCPConnectionConfig, domain: str = ""
) -> AsyncGenerator[ClientSession, None]:
    """Async context manager yielding an initialized ClientSession.

    Connection mode is determined by cfg.mode:
    - "stdio" without cfg.command: container exec (default)
    - "stdio" with cfg.command: local subprocess
    - "http" / "streamable-http": remote domain-scoped HTTP connection
    - "websocket": WebSocket connection

    Yields:
        ClientSession — an initialized MCP client session ready to use
    """
    if cfg.mode == "stdio":
        if cfg.command:
            # Local subprocess mode
            logger.info("Starting MCP server: %s %s", cfg.command, " ".join(cfg.args or []))
            env = os.environ.copy()
            if domain:
                env["MCP_DOMAIN"] = domain
            server_params = StdioServerParameters(
                command=cfg.command,
                args=cfg.args or [],
                env=env,
            )
        else:
            # Container exec mode (FastAPIMCPServer)
            runtime = cfg.container_runtime
            if not runtime:
                try:
                    runtime = detect_container_runtime()
                except RuntimeError as e:
                    raise RuntimeError(
                        f"Cannot start MCP server in container mode: {e}"
                    ) from e
                logger.info("Auto-detected container runtime: %s", runtime)
            if not cfg.container_name:
                raise ValueError(
                    "stdio mode requires either command or container_name"
                )
            _assert_container_running(runtime, cfg.container_name)
            exec_env = {"MCP_DOMAIN": domain} if domain else {}
            if cfg.container_env:
                exec_env.update(cfg.container_env)
            env_args = []
            for k, v in exec_env.items():
                env_args += ["-e", f"{k}={v}"]
            cmd = cfg.container_command or ["python", "mcp_server.py"]
            full_args = ["exec", "-i"] + env_args + [cfg.container_name] + cmd
            logger.info("Starting MCP server: %s %s", runtime, " ".join(full_args))
            server_params = StdioServerParameters(
                command=runtime,
                args=["exec", "-i"] + env_args + [cfg.container_name] + cmd,
                env=None,
            )
        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    yield session
        except FileNotFoundError as e:
            cmd = server_params.command
            raise RuntimeError(
                f"MCP server command not found: {cmd!r}. "
                "Ensure the command is installed and available in PATH."
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to MCP server via stdio: {e}"
            ) from e

    elif cfg.mode in ("http", "streamable-http"):
        url = resolve_http_url(cfg, domain)
        logger.info("Connecting to remote MCP server: %s", url)
        async with _http_session(url, domain, require_persistent_backend=cfg.require_persistent_backend) as session:
            yield session

    elif cfg.mode == "websocket":
        if not cfg.server_url:
            raise ValueError("websocket mode requires server_url")
        logger.info("Connecting to MCP server via websocket: %s", cfg.server_url)
        from mcp.client.websocket import websocket_client
        try:
            async with websocket_client(cfg.server_url) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    yield session
        except Exception as e:
            raise RuntimeError(
                f"Failed to connect to MCP server at {cfg.server_url!r}: {e}"
            ) from e

    else:
        raise ValueError(
            f"Unknown mode: {cfg.mode!r}. Must be 'stdio', 'http', 'streamable-http', or 'websocket'"
        )
    
def stop_mcp_server(cfg: MCPConnectionConfig):
    """Stop a running MCP server process.

    For container stdio mode, force-kills the mcp_server.py process inside the
    container.  For subprocess stdio and websocket modes the transport's own
    context manager handles teardown, so this is a no-op.
    """
    if cfg.mode in ("websocket", "http", "streamable-http"):
        # WebSocket connection is closed by the context manager; nothing to do.
        return

    if cfg.mode == "stdio" and not cfg.command and cfg.container_name:
        # Container exec mode: pkill the server process inside the container.
        runtime = cfg.container_runtime or detect_container_runtime()
        try:
            kill_cmd = [
                runtime, "exec", cfg.container_name,
                "pkill", "-f", "python mcp_server.py"
            ]
            subprocess.run(kill_cmd, capture_output=True, timeout=5)
            logger.info("Server stopped.")
        except subprocess.TimeoutExpired:
            logger.warning("Timeout while stopping server")
        except Exception:
            pass
    # Local subprocess stdio: the stdio_client context manager terminates the
    # child process on exit, so no explicit kill is needed here.

