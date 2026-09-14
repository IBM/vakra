#!/usr/bin/env python3
from __future__ import annotations

"""
Setup script for the Enterprise Benchmark.

Downloads benchmark data from HuggingFace and manages benchmark containers
via docker compose.

Usage:
    # Install with init dependencies
    pip install -e ".[init]"

    # Run full setup (download data + start containers)
    python benchmark_setup.py

    # Individual steps
    python benchmark_setup.py --check-hf-auth
    python benchmark_setup.py --download-data
    python benchmark_setup.py --start-containers
    python benchmark_setup.py --stop-containers
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.resolve()
DATA_DIR = PROJECT_ROOT / "data"

PUBLIC_DATASET_REPO = "ibm-research/VAKRA"
GATED_TEST_REPO = "ibm-research/VAKRA-GatedTest"
GATED_TEST_ACCESS_ISSUE_URL = (
    "https://github.com/IBM/vakra/issues/new?template=gated_test_access.yml"
)

# Container names must match benchmark/mcp_connection_config.yaml
CONTAINERS = [
    "capability_1_bi_apis",
    "capability_2_dashboard_apis",
    "capability_3_multihop_reasoning",   # BPO + REST (no retriever)
    "capability_4_multiturn",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _runtime() -> str:
    """Return 'docker' or 'podman', whichever is available."""
    for rt in ["docker", "podman"]:
        if shutil.which(rt):
            return rt
    print("Error: neither docker nor podman found on PATH.")
    sys.exit(1)


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a command, printing it first."""
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, **kwargs)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
def _load_metadata(path: Path) -> dict:
    """Load locally stored file metadata (filename -> blob sha)."""
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _hf_token() -> str | None:
    """Return a Hugging Face token from env vars or the CLI cache."""
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if token:
        return token

    try:
        from huggingface_hub import get_token
    except ImportError:
        return None
    return get_token()


def check_hf_auth() -> None:
    """Exit successfully only when Hugging Face auth is available."""
    token = _hf_token()
    if not token:
        raise SystemExit(
            "ERROR: Hugging Face authentication was not found.\n"
            "Set HF_TOKEN/HUGGING_FACE_HUB_TOKEN or run 'huggingface-cli login'."
        )
    print("Hugging Face authentication found.")


def _repo_files(api, repo: str, token: str | None) -> dict[str, str]:
    from huggingface_hub import RepoFile

    return {
        item.path: item.blob_id
        for item in api.list_repo_tree(
            repo_id=repo,
            repo_type="dataset",
            recursive=True,
            token=token,
        )
        if isinstance(item, RepoFile)
    }


def _has_split_prefix(path: str, split: str) -> bool:
    parts = Path(path).parts
    return len(parts) > 0 and parts[0] == split


def _split_local_path(remote_path: str, split: str) -> str:
    """Map repo paths into data/<split>/..., preserving capability folder names."""
    parts = Path(remote_path).parts
    if parts and parts[0] == split:
        return remote_path
    return str(Path(split) / remote_path)


def _support_local_path(remote_path: str) -> str:
    return remote_path


def _sync_hf_files(
    *,
    repo: str,
    remote_files: dict[str, str],
    local_path_for,
    metadata_name: str,
    token: str | None,
) -> int:
    """Sync selected repo files into DATA_DIR and return number downloaded."""
    from huggingface_hub import hf_hub_download

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    metadata_path = DATA_DIR / metadata_name
    local_metadata = _load_metadata(metadata_path)
    desired_metadata = {
        local_path_for(remote_path): sha for remote_path, sha in remote_files.items()
    }

    to_download = [
        remote_path
        for remote_path, sha in remote_files.items()
        if local_metadata.get(local_path_for(remote_path)) != sha
    ]
    to_delete = [
        local_path
        for local_path in local_metadata
        if local_path not in desired_metadata
    ]

    if not to_download and not to_delete:
        print("  [up to date]")
        return 0

    for local_path in to_delete:
        path = DATA_DIR / local_path
        if path.exists() and path.is_file():
            path.unlink()
        print(f"  [deleted] {local_path}")

    print(f"  Downloading {len(to_download)} file(s)...")
    for remote_path in to_download:
        downloaded = Path(
            hf_hub_download(
                repo_id=repo,
                filename=remote_path,
                repo_type="dataset",
                token=token,
                local_dir=str(DATA_DIR),
            )
        )
        local_path = DATA_DIR / local_path_for(remote_path)
        if downloaded != local_path:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(downloaded), str(local_path))
        print(f"  [ok] {local_path.relative_to(DATA_DIR)}")

    metadata_path.write_text(json.dumps(desired_metadata, indent=2))
    return len(to_download)


def _sync_gated_test(api, token: str | None) -> bool:
    print(f"\n--- {GATED_TEST_REPO} -> data/test/ ---")
    try:
        remote_files = _repo_files(api, GATED_TEST_REPO, token)
    except Exception as exc:
        print(f"  Could not access gated test set: {type(exc).__name__}: {exc}")
        return False

    remote_files = {
        path: sha
        for path, sha in remote_files.items()
        if path not in {".gitattributes", "README.md"} and not path.endswith("/")
    }
    if not remote_files:
        print("  Gated test repo is accessible, but no files were found.")
        return False

    _sync_hf_files(
        repo=GATED_TEST_REPO,
        remote_files=remote_files,
        local_path_for=lambda path: _split_local_path(path, "test"),
        metadata_name=".hf_metadata_gated_test.json",
        token=token,
    )
    return any((DATA_DIR / "test").glob("capability_*/*/*.json"))


def _sync_public_support_data(api) -> None:
    print(f"\n--- {PUBLIC_DATASET_REPO} shared files -> data/ ---")
    remote_files = {
        path: sha
        for path, sha in _repo_files(api, PUBLIC_DATASET_REPO, None).items()
        if not _has_split_prefix(path, "train")
        and not _has_split_prefix(path, "test")
        and path not in {".gitattributes", "README.md", "vakra.py"}
    }
    _sync_hf_files(
        repo=PUBLIC_DATASET_REPO,
        remote_files=remote_files,
        local_path_for=_support_local_path,
        metadata_name=".hf_metadata_public_support.json",
        token=None,
    )


def _sync_public_train(api) -> bool:
    print(f"\n--- {PUBLIC_DATASET_REPO} train split -> data/train/ ---")
    remote_files = {
        path: sha
        for path, sha in _repo_files(api, PUBLIC_DATASET_REPO, None).items()
        if _has_split_prefix(path, "train")
    }
    if not remote_files:
        print("  No train split files were found in the public repo.")
        return False
    _sync_hf_files(
        repo=PUBLIC_DATASET_REPO,
        remote_files=remote_files,
        local_path_for=lambda path: _split_local_path(path, "train"),
        metadata_name=".hf_metadata_public_train.json",
        token=None,
    )
    return any((DATA_DIR / "train").glob("capability_*/*/*.json"))


def download_data() -> None:
    """Download gated test data when available, otherwise public train data."""
    try:
        from huggingface_hub import HfApi
    except ImportError:
        print("Error: huggingface_hub is not installed.")
        print("  pip install -e '.[init]'")
        sys.exit(1)

    token = _hf_token()
    api = HfApi(token=token)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "test").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "train").mkdir(parents=True, exist_ok=True)

    print(f"\n=== Syncing data into {DATA_DIR} ===")
    if token:
        test_available = _sync_gated_test(api, token)
    else:
        test_available = False
        print(f"\n--- {GATED_TEST_REPO} -> data/test/ ---")
        print("  No Hugging Face auth found; skipping gated test set.")
        print("  Public train split will be downloaded instead.")
    _sync_public_support_data(api)

    train_available = False
    if not test_available:
        train_available = _sync_public_train(api)

    print("\n" + "=" * 72)
    print("Data sync complete.")
    if test_available:
        print(f"TEST SET DOWNLOADED: data/test was populated from {GATED_TEST_REPO}.")
    elif train_available:
        print("GATED TEST SET NOT DOWNLOADED.")
        if token:
            print("Your Hugging Face auth did not provide gated test access.")
            print("Request access to VAKRA-GatedTest by opening a GitHub issue:")
            print(f"  {GATED_TEST_ACCESS_ISSUE_URL}")
        else:
            print("No Hugging Face auth was found.")
        print("Public train split was downloaded anonymously to data/train instead.")
        if not token:
            print("Request gated test access by opening a GitHub issue:")
            print(f"  {GATED_TEST_ACCESS_ISSUE_URL}")
    else:
        print("WARNING: gated test and public train splits were not downloaded.")
        print("Request gated test access by opening a GitHub issue:")
        print(f"  {GATED_TEST_ACCESS_ISSUE_URL}")
    print("=" * 72)


def start_containers() -> None:
    """Start all benchmark containers via docker compose."""
    rt = _runtime()
    print("\n=== Starting containers ===")

    db_dir = DATA_DIR / "databases"
    if not db_dir.exists() or not any(db_dir.iterdir()):
        raise SystemExit(
            f"\nERROR: Database directory '{db_dir}' is missing or empty.\n"
            "Run 'make download' (or 'python benchmark_setup.py --download-data') first.\n"
        )

    _run([rt, "compose", "up", "-d"], check=True)

    # Wait for the internal FastAPI servers to come up
    print("\nWaiting for services to initialize (up to 120s) ...")
    deadline = time.time() + 120
    ready = set()
    while time.time() < deadline and len(ready) < len(CONTAINERS):
        for name in CONTAINERS:
            if name in ready:
                continue
            result = _run(
                [rt, "exec", name, "curl", "-sf", "http://localhost:8000/openapi.json"],
                capture_output=True,
            )
            if result.returncode == 0:
                ready.add(name)
                print(f"  [ready] {name}")
        if len(ready) < len(CONTAINERS):
            time.sleep(5)

    not_ready = set(CONTAINERS) - ready
    if not_ready:
        print(f"\nWarning: these containers did not become ready: {not_ready}")
        print("Check logs with:  docker logs <container_name>")
    else:
        print("\nAll containers are ready.")


def stop_containers() -> None:
    """Stop and remove all benchmark containers."""
    rt = _runtime()
    print("\n=== Stopping and removing benchmark containers ===")
    _run([rt, "compose", "down", "--remove-orphans"], capture_output=True)
    print("Done.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Setup the Enterprise Benchmark environment",
    )
    parser.add_argument(
        "--download-data", action="store_true",
        help="Download benchmark data from HuggingFace",
    )
    parser.add_argument(
        "--check-hf-auth", action="store_true",
        help="Check whether Hugging Face authentication is configured",
    )
    parser.add_argument(
        "--start-containers", action="store_true",
        help="Start Docker containers for all tasks via docker compose",
    )
    parser.add_argument(
        "--stop-containers", action="store_true",
        help="Stop and remove all benchmark containers",
    )

    args = parser.parse_args()

    # If no specific step requested, run all setup steps
    explicit = (
        args.check_hf_auth
        or args.download_data
        or args.start_containers
        or args.stop_containers
    )

    if args.check_hf_auth:
        check_hf_auth()
        return

    if args.stop_containers:
        stop_containers()
        return

    if not explicit or args.download_data:
        download_data()

    if not explicit or args.start_containers:
        start_containers()

    if not explicit:
        print("\n" + "=" * 60)
        print("Setup complete! You can now run the benchmark:")
        print("=" * 60)
        print()
        print("  # Single task, single domain")
        print("  python benchmark_runner.py --capability_id 2 --run-agent --domain address")
        print()
        print("  # All three tasks for one domain")
        print("  python benchmark_runner.py --capability_id 1 2 4 --run-agent --domain address")
        print()
        print("  # Parallel execution")
        print("  python benchmark_runner.py --capability_id 1 2 4 --run-agent --domain address --parallel")
        print()
        print("  # Stop containers when done")
        print("  python benchmark_setup.py --stop-containers")
        print()


if __name__ == "__main__":
    main()
