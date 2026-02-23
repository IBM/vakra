# =============================================================================
# M3 Benchmark — Docker image lifecycle
# =============================================================================
# Targets:
#   make download    Download benchmark data from HuggingFace
#   make build       Build the m3_environ image from source
#   make test        Smoke-test the locally built image (file checks + MCP handshakes)
#   make validate    Validate live MCP connections against running containers
#   make tag         Tag the local image for Docker Hub
#   make push        Push the tagged image to Docker Hub
#   make release     build → test → tag → push  (full publish workflow)
#   make setup       download → build → test → start → validate  (first-time setup)
#   make start       Start all benchmark containers (pulls latest image)
#   make stop        Stop and remove all benchmark containers
#   make logs        Tail logs for all running benchmark containers
# =============================================================================

REGISTRY   := docker.io/amurthi44g1wd
IMAGE_NAME := m3_environ
REMOTE     := $(REGISTRY)/$(IMAGE_NAME):latest
DOCKERFILE := docker/Dockerfile.unified

.PHONY: download build test validate tag push release setup start stop logs

# ---------------------------------------------------------------------------
# Download benchmark data from HuggingFace  (prompts for HF token if not set)
# ---------------------------------------------------------------------------
download:
	python m3_setup.py --download-data

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
build:
	docker build -t $(IMAGE_NAME) -f $(DOCKERFILE) .

# ---------------------------------------------------------------------------
# Smoke-test  (spins up a temp container, no volume mounts needed)
# Checks: required files exist, M3 REST /openapi.json, BPO + M3 MCP handshakes
# ---------------------------------------------------------------------------
test:
	bash docker/smoke_test.sh $(IMAGE_NAME)

# ---------------------------------------------------------------------------
# Validate live MCP connections against the running benchmark containers
# Requires: make start (or m3_setup.py --start-containers) run first
# ---------------------------------------------------------------------------
validate:
	python benchmark/validate_clients.py

# ---------------------------------------------------------------------------
# Tag
# ---------------------------------------------------------------------------
tag:
	docker tag $(IMAGE_NAME) $(REMOTE)

# ---------------------------------------------------------------------------
# Push  (requires: docker login docker.io)
# ---------------------------------------------------------------------------
push:
	docker push $(REMOTE)

# ---------------------------------------------------------------------------
# Full release workflow  (image only, no data download)
# ---------------------------------------------------------------------------
release: build test tag push
	@echo ""
	@echo "Released $(REMOTE)"
	@echo "Run 'make start && make validate' to restart containers and verify live connections."

# ---------------------------------------------------------------------------
# First-time setup: data + image + containers
# ---------------------------------------------------------------------------
setup: download build test start validate
	@echo ""
	@echo "Setup complete. Run 'python benchmark_runner.py --m3_task_id 3 --domain bpo' to start benchmarking."

# ---------------------------------------------------------------------------
# Container lifecycle  (delegates to m3_setup.py)
# ---------------------------------------------------------------------------
start:
	python m3_setup.py --start-containers

stop:
	python m3_setup.py --stop-containers

logs:
	@for c in task_1_m3_environ task_2_m3_environ task_3_m3_environ task_5_m3_environ; do \
		echo ""; \
		echo "=== $$c ==="; \
		docker logs --tail 20 $$c 2>/dev/null || echo "  (not running)"; \
	done
