# Setup Guide

> **Note:** These steps create a clean environment so your existing branch and code are not affected.

## Prerequisites

- Docker or Podman running (`docker ps` / `podman ps`)
- If using Podman, alias it: `alias docker=podman`

---

## 1. Clone the Repository

```bash
git clone git@github.ibm.com:AI4BA/enterprise-benchmark.git
cd enterprise-benchmark
```

---

## 2. Create a Python Environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[init]"
```

---

## 3. Run Setup Script

```bash
python m3_setup.py
```

> **You will be prompted for a Hugging Face token.**
>
> **Warning:** This step downloads ~30 GB of data. This will be reduced in a future release.

---

## 4. Verify Docker Containers

Once setup completes, confirm three containers are running:

```bash
docker ps
```

You should see **3 containers** listed.

---

## 5. Run a Benchmark

Install benchmark dependencies:

```bash
pip install -r requirements_benchmark.txt
# or manually:
pip install langchain-openai langchain mcp langchain-anthropic langgraph langchain-ollama
```

Set your API keys:

```bash
export OPENAI_API_KEY=<your-key>
export LITELLM_API_KEY=<your-key>
```

Run a sample benchmark:

```bash
python benchmark_runner.py \
  --task_id 2 \
  --run-agent \
  --domain airline \
  --max-samples-per-domain 1 \
  --provider openai
```
