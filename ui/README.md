---
title: Agent Performance Leaderboard
emoji: 🏆
colorFrom: indigo
colorTo: green
sdk: docker
pinned: false
---

# Agent Performance Leaderboard UI

A Streamlit dashboard for comparing agent performance across benchmark task types.

## Quick Start (local, no Docker)

```bash
pip install -r ui/requirements.txt
cd ui && streamlit run app.py
```

Opens at http://localhost:8501.

## Quick Start (Docker)

### 1. Build

```bash
cd ui
docker build -t agent-leaderboard .
```

### 2. Test locally

```bash
docker run -d --name leaderboard-test -p 7860:7860 agent-leaderboard
curl http://localhost:7860/_stcore/health   # expect: ok
```

Clean up: `docker stop leaderboard-test && docker rm leaderboard-test`

## Deploy to Hugging Face Spaces

Push the source files and let HF build for you:

```bash
git clone https://huggingface.co/spaces/<your-username>/agent-leaderboard
cd agent-leaderboard
cp /path/to/enterprise-benchmark-mar13/ui/{Dockerfile,app.py,data.py,requirements.txt,leaderboard_data.json,README.md} .
git add -A && git commit -m "Initial deploy" && git push
```

HF detects the Dockerfile, builds on amd64, and serves at `https://huggingface.co/spaces/<your-username>/agent-leaderboard`.

The `README.md` **must** have the YAML frontmatter at the top (`sdk: docker`) — already included in this file.

### Redeploy

- **Any `git push`** to the Space repo triggers a rebuild.
- **Factory reboot**: Space Settings → Factory reboot (clears cache).

## Data Format

Agent data lives in `leaderboard_data.json`:

```json
[
  {
    "agent": "LangGraph React Agent",
    "model": "gpt-4o",
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

Score keys are pipe-delimited: `Turn Type|Task Type|Metric`, matching `COLUMN_HIERARCHY` in `data.py`.

## File Structure

```
ui/
  app.py                  # Streamlit app (entry point)
  data.py                 # Data layer: schema, DataFrame builder, load/save
  leaderboard_data.json   # Agent scores
  requirements.txt        # Python dependencies
  Dockerfile              # Container image
  README.md               # This file
```

## Customization

Add a new metric: append a tuple to `COLUMN_HIERARCHY` in `data.py` and add the matching score key to your agents in `leaderboard_data.json`. Everything else derives from the hierarchy automatically.
