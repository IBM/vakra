import contextlib
from dataclasses import dataclass
import logging
import os
import shutil
import subprocess
import sys
import yaml

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from typing import AsyncGenerator, Dict, List, Optional

logger = logging.getLogger(__name__)

# Default settings
DEFAULT_CONTAINER_NAME = "fastapi-mcp-server"

def _runtime_works(name: str) -> bool:
    """Return True if `name --version` exits successfully."""
    try:
        result = subprocess.run(
            [name, "--version"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _assert_container_running(runtime: str, container_name: str) -> None:
    """Check that container_name exists and is running; raise RuntimeError if not."""
    try:
        result = subprocess.run(
            [runtime, "inspect", "--format", "{{.State.Status}}", container_name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"Container runtime {runtime!r} not found or not executable. "
            "If 'docker' is a symlink to podman, set "
            "'container_runtime: podman' in your MCP connection config."
        )
    if result.returncode != 0:
        raise RuntimeError(
            f"Container {container_name!r} not found. "
            f"Start it with: {runtime} start {container_name}\n"
            f"({runtime} inspect output: {result.stderr.strip()})"
        )
    status = result.stdout.strip()
    if status != "running":
        raise RuntimeError(
            f"Container {container_name!r} exists but is not running "
            f"(status: {status!r}). "
            f"Start it with: {runtime} start {container_name}"
        )


def detect_container_runtime() -> str:
    """
    Detect available container runtime (podman or docker).
    Returns 'podman' if available and working, otherwise 'docker'.
    Raises RuntimeError if neither runtime works.
    """
    for name in ("podman", "docker"):
        if shutil.which(name) and _runtime_works(name):
            if name == "docker":
                logger.warning("podman not found or not working, using docker instead")
            return name

    raise RuntimeError(
        "Neither podman nor docker found in PATH. "
        "Please install one of them."
    )


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


def load_mcp_config(config_path: str) -> Dict[int, MCPConnectionConfig]:
    """Load MCP connection config from YAML file.

    Returns a dict mapping task_id (int) to MCPConnectionConfig.
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
            server_url=v.get("server_url", None),
        )
    return result


@contextlib.asynccontextmanager
async def create_client_and_connect(
    cfg: MCPConnectionConfig, domain: str = ""
) -> AsyncGenerator[ClientSession, None]:
    """Async context manager yielding an initialized ClientSession.

    Connection mode is determined by cfg.mode:
    - "stdio" without cfg.command: container exec (default)
    - "stdio" with cfg.command: local subprocess
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
            logger.info(
                "Starting MCP server: %s exec -i -e MCP_DOMAIN=%s %s python mcp_server.py",
                runtime, domain, cfg.container_name,
            )
            exec_env = {"MCP_DOMAIN": domain} if domain else {}
            if cfg.container_env:
                exec_env.update(cfg.container_env)
            env_args = []
            for k, v in exec_env.items():
                env_args += ["-e", f"{k}={v}"]
            cmd = cfg.container_command or ["python", "mcp_server.py"]
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
            f"Unknown mode: {cfg.mode!r}. Must be 'stdio' or 'websocket'"
        )
    
def stop_mcp_server(cfg: MCPConnectionConfig):
    """Stop a running MCP server process.

    For container stdio mode, force-kills the mcp_server.py process inside the
    container.  For subprocess stdio and websocket modes the transport's own
    context manager handles teardown, so this is a no-op.
    """
    if cfg.mode == "websocket":
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

