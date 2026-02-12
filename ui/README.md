---
title: Agent Performance Leaderboard
emoji: 🏆
colorFrom: indigo
colorTo: green
sdk: docker
pinned: false
---

# Agent Performance Leaderboard UI

A Streamlit dashboard for comparing agent performance across different benchmark task types.

## Quick Start

```bash
# Install dependencies
pip install -r ui/requirements.txt

# Run the app
cd ui
streamlit run app.py
```

The app opens at **http://localhost:8501** by default.

## Features

### Leaderboard Table

The core of the UI is a grouped leaderboard table with three header levels:

| Level | Examples | Purpose |
|-------|----------|---------|
| **Turn Type** | Single Turn, Multi-turn | Top-level grouping by interaction mode |
| **Task Type** | API Styles, Reasoning, Joint Structured/Unstructured Reasoning | Category within a turn type |
| **Metric** | Business Intelligence, Dashboard APIs, Multi-hop, Policy Adherence | Individual scored dimension |

Each row shows an agent's **name**, **model**, per-metric scores, and an **Overall** score (mean of all metrics). Agents are ranked by Overall, highest first.

### Sortable Columns

Click any metric column header or the Overall header to sort the table:

- First click sorts **descending** (highest score first).
- Click the same column again to toggle to **ascending**.
- The active sort column shows a highlighted arrow indicator.
- Rank badges update automatically after each sort.

Sorting is client-side JavaScript — instant, no page reload.

### Score Color Coding

| Range | Color | Label |
|-------|-------|-------|
| 90 -- 100 | Green | Excellent |
| 80 -- 89 | Blue | Good |
| 70 -- 79 | Yellow | Fair |
| < 70 | Red | Needs Improvement |

### Filtering

Two dropdowns at the top of the page:

- **Turn Type** -- show only Single Turn, Multi-turn, or All.
- **Task Type** -- show only API Styles, Reasoning, Joint Structured/Unstructured Reasoning, or All Tasks.

Filters narrow the visible metric columns; the Overall score always remains visible.

### Adding Agents

Two ways to add an agent:

1. **+ Agent button** -- opens an inline form where you enter an agent name, model, and scores for each metric.
2. **Upload JSON** -- drag-and-drop a benchmark summary JSON file. The file contents are previewed; integration with the evaluation pipeline can auto-populate scores.

## Data Format

All agent data lives in `leaderboard_data.json`. The file is an array of agent objects:

```json
[
  {
    "agent": "Agent Y",
    "model": "GPT-4o",
    "scores": {
      "Single Turn|API Styles|Business Intelligence": 90.0,
      "Single Turn|API Styles|Dashboard APIs": 87.0,
      "Single Turn|Reasoning|Multi-hop": 82.0,
      "Single Turn|Reasoning|Policy Adherence": 91.0,
      "Multi-turn|Joint Structured/ Unstructured Reasoning|Multi-hop": 85.0
    }
  }
]
```

Score keys are **pipe-delimited** strings in the format `Turn Type|Task Type|Metric`. This mirrors the `COLUMN_HIERARCHY` defined in `data.py` and keeps the JSON simple (no tuple workarounds).

## File Structure

```
ui/
  app.py                  # Streamlit application (entry point)
  data.py                 # Data layer: schema, DataFrame builder, load/save
  leaderboard_data.json   # Agent data (primary data source)
  requirements.txt        # Python dependencies
  Dockerfile              # Container image (for local Docker or HF Spaces)
  README.md               # This file
```

### `app.py`

Main Streamlit application. Key sections:

| Section | What it does |
|---------|--------------|
| **Inline CSS** | Styles embedded in the HTML iframe for the table, rank badges, score colors, pills, and legend. Explicit light backgrounds prevent dark-mode bleed-through. |
| **`score_class` / `rank_class`** | Map numeric values to CSS class names for color coding. |
| **`build_leaderboard_html`** | Renders the full HTML `<table>` with three grouped header rows (turn, task, metric), data rows with colored scores, and client-side JavaScript for column sorting. |
| **`main`** | Wires up the header, filters, table, legend, add-agent form, and file uploader. |
| **`render_add_agent_form`** | Renders text inputs for agent name and model, number inputs for each metric, and persists the new agent on save. |

### `data.py`

Data definitions and utilities.

| Symbol | Purpose |
|--------|---------|
| `COLUMN_HIERARCHY` | List of `(turn_type, task_type, metric)` tuples defining every scored column. Edit this list to add or remove metrics. |
| `_score_key(turn, task, metric)` | Builds the pipe-delimited key (`"Turn\|Task\|Metric"`) used in JSON score dicts. |
| `load_agents()` / `save_agents(agents)` | Read/write `leaderboard_data.json`. Returns an empty list if the file doesn't exist. |
| `build_dataframe(agents)` | Converts agent dicts into a pandas DataFrame with a 3-level `MultiIndex` on columns (`Turn Type`, `Task Type`, `Metric`). Adds Rank, Agent, Model, and Overall columns. |

## Customization

### Adding a new metric column

1. Append a tuple to `COLUMN_HIERARCHY` in `data.py`:

```python
COLUMN_HIERARCHY = [
    ...
    ("Single Turn", "Reasoning", "Chain-of-Thought"),  # new
]
```

2. Add the corresponding score key to each agent in `leaderboard_data.json`:

```json
"Single Turn|Reasoning|Chain-of-Thought": 88.0
```

The table, filters, sorting, and add-agent form all derive from `COLUMN_HIERARCHY`, so no other code changes are needed.

### Changing the scoring thresholds

Edit `score_class()` in `app.py`:

```python
def score_class(val: float) -> str:
    if val >= 90:
        return "score-excellent"   # green
    elif val >= 80:
        return "score-good"        # blue
    elif val >= 70:
        return "score-fair"        # yellow
    return "score-poor"            # red
```

### Connecting to real benchmark results

The upload section in `app.py` currently previews the JSON. To auto-populate:

1. Parse the uploaded JSON into the agent dict format shown in [Data Format](#data-format).
2. Call `save_agents(agents)` and `st.rerun()`.

The benchmark runner (`benchmark_runner.py` in the project root) writes per-domain output files. A future integration can read those files, compute aggregate scores, and feed them directly into the leaderboard.

## Container Build

The image needs to support two architectures:

| Architecture | Who uses it |
|---|---|
| `linux/amd64` | Intel/AMD Macs, Linux servers, Windows, **HF Spaces** |
| `linux/arm64` | Apple Silicon (M1/M2/M3/M4) Macs |

Choose the instructions for your container tool below.

---

### Option A — Docker (with buildx)

#### Prerequisites

Docker Desktop includes `buildx` by default. Verify:

```bash
docker buildx version
```

If you don't have a builder that supports multi-platform, create one:

```bash
docker buildx create --name multiarch --use
docker buildx inspect --bootstrap
```

#### Build & push multi-arch image

```bash
cd ui
docker login docker.io
docker buildx build --platform linux/amd64,linux/arm64 \
  -t docker.io/amurthi44g1wd/agent-leaderboard \
  --push .
```

> **Note:** `--push` is required with multi-platform builds because the resulting manifest list cannot be stored in the local Docker daemon. The image is pushed directly to Docker Hub.

#### Build single-platform image (local testing only)

```bash
cd ui
docker buildx build --platform linux/amd64 \
  -t docker.io/amurthi44g1wd/agent-leaderboard \
  --load .
```

---

### Option B — Podman (with manifest)

Podman does not have `buildx`. Instead, build each platform separately and combine them into a manifest list.

#### Build both architectures

```bash
cd ui
podman build --platform linux/amd64 -t docker.io/amurthi44g1wd/agent-leaderboard:amd64 .
podman build --platform linux/arm64 -t docker.io/amurthi44g1wd/agent-leaderboard:arm64 .
```

#### Create manifest list and push

```bash
podman login docker.io

podman manifest create docker.io/amurthi44g1wd/agent-leaderboard
podman manifest add docker.io/amurthi44g1wd/agent-leaderboard \
  docker.io/amurthi44g1wd/agent-leaderboard:amd64
podman manifest add docker.io/amurthi44g1wd/agent-leaderboard \
  docker.io/amurthi44g1wd/agent-leaderboard:arm64
podman manifest push docker.io/amurthi44g1wd/agent-leaderboard
```

#### Build single-platform image (local testing only)

```bash
cd ui
podman build --platform linux/amd64 -t docker.io/amurthi44g1wd/agent-leaderboard .
```

---

### Test locally

Run the container (works with both `docker` and `podman`):

```bash
docker run -d --name leaderboard-test -p 7860:7860 docker.io/amurthi44g1wd/agent-leaderboard
```

Open http://localhost:7860 and verify the leaderboard renders correctly.

Check the health endpoint:

```bash
curl http://localhost:7860/_stcore/health
# Expected: ok
```

To mount your own data file (so edits persist outside the container):

```bash
docker run -d --name leaderboard-test -p 7860:7860 \
  -v ./leaderboard_data.json:/app/leaderboard_data.json \
  docker.io/amurthi44g1wd/agent-leaderboard
```

### Stop and clean up

```bash
docker stop leaderboard-test && docker rm leaderboard-test
```

## Deploy to Hugging Face Spaces

HF Spaces supports Docker-based apps natively.

### Step 1 — Create the Space

Go to [huggingface.co/new-space](https://huggingface.co/new-space):

- **SDK**: Docker
- **Name**: e.g. `agent-leaderboard`

### Step 2 — Clone and add files

```bash
git clone https://huggingface.co/spaces/<your-username>/agent-leaderboard
cd agent-leaderboard
```

Copy all UI files into the Space repo:

```bash
cp /path/to/enterprise-benchmark-1/ui/Dockerfile .
cp /path/to/enterprise-benchmark-1/ui/app.py .
cp /path/to/enterprise-benchmark-1/ui/data.py .
cp /path/to/enterprise-benchmark-1/ui/requirements.txt .
cp /path/to/enterprise-benchmark-1/ui/leaderboard_data.json .
cp /path/to/enterprise-benchmark-1/ui/README.md .
```

The `README.md` must have the YAML frontmatter at the top (already included):

```yaml
---
title: Agent Performance Leaderboard
emoji: 🏆
colorFrom: indigo
colorTo: green
sdk: docker
pinned: false
---
```

### Step 3 — Push

```bash
git add -A
git commit -m "Initial deploy"
git push
```

HF detects the `Dockerfile`, builds the image, and serves the app at:

```
https://huggingface.co/spaces/<your-username>/agent-leaderboard
```

### Updating the Space

After code changes, rebuild the multi-arch image and push:

**Docker:**
```bash
cd ui
docker buildx build --platform linux/amd64,linux/arm64 \
  -t docker.io/amurthi44g1wd/agent-leaderboard \
  --push .
```

**Podman:**
```bash
cd ui
podman build --platform linux/amd64 -t docker.io/amurthi44g1wd/agent-leaderboard:amd64 .
podman build --platform linux/arm64 -t docker.io/amurthi44g1wd/agent-leaderboard:arm64 .
podman manifest rm docker.io/amurthi44g1wd/agent-leaderboard || true
podman manifest create docker.io/amurthi44g1wd/agent-leaderboard
podman manifest add docker.io/amurthi44g1wd/agent-leaderboard \
  docker.io/amurthi44g1wd/agent-leaderboard:amd64
podman manifest add docker.io/amurthi44g1wd/agent-leaderboard \
  docker.io/amurthi44g1wd/agent-leaderboard:arm64
podman manifest push docker.io/amurthi44g1wd/agent-leaderboard
```

Or push updated files directly to the Space repo and HF rebuilds automatically.

> **Note:** The container filesystem is ephemeral on HF Spaces. Agents added via the "+ Agent" button persist during the session but reset on container restart. For durable writes, swap `save_agents` to use the HF Datasets API or a database backend.

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `streamlit` | >= 1.30.0 | Web UI framework |
| `pandas` | >= 2.0.0 | DataFrame with MultiIndex for grouped columns |
