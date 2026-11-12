# VAKRA Evaluator

The `evaluator/` package replays predicted tool trajectories against the live VAKRA MCP environment and scores whether the final answer is both correct and grounded in tool outputs.

Rather than grading only the final text, the evaluator executes the submitted tool calls, injects fresh tool responses, and then combines deterministic checks with LLM-based judging to produce per-dialogue and per-domain scores.

Detailed evaluation is explained in our blog - https://huggingface.co/blog/ibm-research/vakra-benchmark-analysis#evaluation-framework

## What the Evaluator Does

For each capability and domain, the evaluator:

- loads ground-truth and prediction files
- pairs examples by `uuid`
- replays predicted and ground-truth tool calls through MCP
- injects tool responses back into each dialogue
- scores the predicted final turn using exact-match, correctness, and groundedness checks
- writes detailed per-dialogue results plus aggregated summaries

The current scorer evaluates the **last turn only** for each dialogue, even when the source dialogue contains multiple turns.

## Scoring Pipeline

Each paired dialogue is scored in the following order:

1. **Exact-match tool-response check**
   The evaluator first checks whether the expected ground-truth tool responses are present in the predicted tool responses.
2. **Answer correctness check**
   If exact match fails, an LLM judge compares the predicted answer against the ground-truth answer for the final turn. The correctness prompt is adapted from CRAG and is defined in [`prompt.py`](/vakra/evaluator/prompt.py).
3. **Groundedness check**
   If the answer is acceptable, another LLM judge verifies that the predicted answer is grounded in the executed tool outputs.
4. **PolicyAdherenceJudge**
   Optional: for the multi-hop multi-source capability, the evaluator can programmatically check policy application when `--policy-judge-path /path/to/policy_judge.py` is provided. By default this argument is unset and policy judging is disabled. If you need policy evaluation, open a GitHub issue requesting access to the PolicyJudge file.

Turn scores are then aggregated into a dialogue score. The default aggregation policy in this repo is `mean`.

## Requirements

You will typically need the same setup used for running the benchmark itself:

- Python 3.10+
- benchmark dependencies installed with `pip install -r requirements_benchmark.txt`
- benchmark containers or MCP servers running and reachable via [`benchmark/mcp_connection_config.yaml`](/vakra/benchmark/mcp_connection_config.yaml)
- an OpenAI-compatible LLM configuration for the correctness and groundedness judges. We would be using `openai/gpt-oss-120b` internally for evaluation.

If you are setting up the benchmark from scratch, use the top-level guide in [`setup.md`](/vakra/setup.md).

## Input Layout

The evaluator expects one JSON file per domain in both the ground-truth and prediction directories.

```text
capability_dashboard_apis/
├── groundtruth/
│   ├── hockey.json
│   ├── movie.json
│   └── ...
└── prediction/
    ├── hockey.json
    ├── movie.json
    └── ...
```

Each file must be a JSON list of dialogue objects matched by `uuid`.

## Dialogue Format

Ground-truth and prediction files use the same top-level dialogue shape:

```json
{
  "uuid": "example_001",
  "domain": "finance",
  "additional_instructions": "Optional capability-specific instructions",
  "output": [
    {
      "turn_id": 0,
      "query": "What is the revenue of company X?",
      "answer": ["Company X reported $10M in revenue."],
      "sequence": {
        "tool_call": [
          {
            "name": "get_revenue",
            "arguments": {
              "company_name": "x"
            }
          }
        ]
      }
    }
  ]
}
```

Notes:

- prediction files should include the agent's generated `answer` and `sequence.tool_call`
- tool responses in predictions are not treated as authoritative; the evaluator re-executes the tool calls and injects fresh MCP outputs
- examples are paired by `uuid`
- missing predictions and extra predictions are reported in the results summary
- if a file contains duplicate `uuid` values, the evaluator keeps the last occurrence and silently overwrites earlier ones during pairing
- judge-based scoring considers at most the last 20 tool responses for a turn across all capabilities

## Running Evaluation

Run the evaluator from the repository root:

1) export `API_KEY=GROQ_API_KEY`

2) Run the evaluation script using the following command

```bash
python evaluator/evaluator.py \
  --capability_name capability_dashboard_apis \
  --gt_root data/test/capability_2_dashboard_apis/output \
  --pred_root output/capability_2_dashboard_apis/prediction \
  --output output/capability_2_dashboard_apis/results.json
```

Supported capability names:

- `capability_bi_apis`
- `capability_dashboard_apis`
- `capability_multihop_reasoning`
- `capability_multiturn`

Useful options:

```bash
python evaluator/evaluator.py \
  --capability_name capability_multiturn \
  --gt_root data/test/capability_4_multiturn/input \
  --pred_root output/capability_4_multiturn \
  --mcp-config benchmark/mcp_connection_config.yaml \
  --domains airline coffee \
  --output output/capability_4_multiturn/results.json
```

CLI arguments:

- `--capability_name`: capability registry key to evaluate
- `--gt_root`: directory containing ground-truth domain JSON files. One file per domain.
- `--pred_root`: directory containing prediction domain JSON files. One file per domain.
- `--output`: output file path; if omitted, defaults to `results.json` next to the capability directory
- `--mcp-config`: MCP connection YAML, defaulting to [`benchmark/mcp_connection_config.yaml`](/vakra/benchmark/mcp_connection_config.yaml)
- `--domains`: optional list of domain names to evaluate
- `--policy-judge-path`: optional path to a PolicyJudge Python file. If omitted, policy judging is disabled.

## Output Format

The evaluator writes a single JSON report containing per-domain details and an overall summary.

```json
{
  "capability_name": "capability_dashboard_apis",
  "groundtruth_dir": "data/test/capability_2_dashboard_apis/input",
  "prediction_dir": "output/capability_2_dashboard_apis",
  "domains": {
    "hockey": {
      "domain": "hockey",
      "n_groundtruth": 100,
      "n_prediction": 100,
      "n_paired": 100,
      "missing_prediction_uuids": [],
      "extra_prediction_uuids": [],
      "dialogues": [
        {
          "uuid": "example_001",
          "score": 1.0,
          "metadata": {
            "capability": "capability_dashboard_apis",
            "domain": "hockey"
          },
          "details": {
            "dialogue_score": 1.0,
            "per_turn": []
          }
        }
      ],
      "summary": {
        "num_samples": 100,
        "num_correct": 81.0,
        "mean_dialogue_score": 0.81,
        "min_dialogue_score": 0.0,
        "max_dialogue_score": 1.0
      }
    }
  },
  "summary": {
    "n_domains": 1,
    "n_paired_dialogues": 100,
    "n_missing_predictions": 0,
    "n_extra_predictions": 0,
    "n_samples": 100,
    "n_correct": 81.0,
    "mean_dialogue_score": 0.81,
    "min_dialogue_score": 0.0,
    "max_dialogue_score": 1.0
  }
}
```

## Resume Behavior

The evaluator writes intermediate results after each domain. If the output file already exists and contains completed domains, rerunning the command will skip those domains and continue from the remaining ones.

## File Map

```text
evaluator/
├── evaluator.py     # CLI entry point and capability/domain evaluation loop
├── scorer.py        # Turn-level and dialogue-level scoring logic
├── judge.py         # Exact-match, correctness, and groundedness judges
├── mcp_tools.py     # Tool extraction, MCP execution, and response injection helpers
├── utils.py         # Data loading, pairing, and evaluator dataclasses
├── constant.py      # Shared schema keys
├── prompt.py        # Judge prompts
└── README.md        # This file
```

## Related Docs

- [`README.md`](/vakra/README.md)
- [`setup.md`](/vakra/setup.md)
- [`benchmark/mcp_connection_config.yaml`](/vakra/benchmark/mcp_connection_config.yaml)
- [`evaluator/evaluator.py`](/vakra/evaluator/evaluator.py)

## Local tools or Code Engine tools

Use `--tool-backend local` (the default) for the existing Docker/subprocess
configuration. Use `--tool-backend code-engine` for MCP over streamable HTTP.
Local input files, tool replay, and judging remain the same.

Set/export the capability app URLs supplied by the deployer, for example by
loading their `.ce_urls.env`. The browser explorer URL is not a tool endpoint:

```bash
export VAKRA_CAP2_URL=https://vakra-cap2.YOUR_DEPLOYMENT.us-east.codeengine.appdomain.cloud
python evaluator/evaluator.py \
  --capability_name capability_dashboard_apis \
  --gt_root /path/to/groundtruth --pred_root /path/to/predictions \
  --domains california_schools --tool-backend code-engine \
  --output /tmp/ce-dashboard-results.json
```

`benchmark/mcp_connection_config.ce.yaml` maps capabilities 1–4 to
`VAKRA_CAP1_URL` through `VAKRA_CAP4_URL`. Only the selected capability's URL
must be set. Each connection uses `/mcp/<domain>`. `--mcp-config PATH` overrides
the selected backend's default YAML. A custom HTTP config can specify an
absolute base URL or a URL containing `{domain}`.

Results record the backend, transport and server URL. Resuming with a different
backend/URL is rejected; choose a separate `--output`. Older results without
connection metadata can be resumed only with the local backend.

### Capability 1: sample state and transforms

Prediction and ground-truth replay run consecutively in one private connection
per sample. Both begin with `get_data(tool_universe_id=UUID, reset_handles=True)`.
The first call selects the sample's universe. The second clears intermediate
handles and counters while reusing the same initial dataset. Changing the
universe loads a new dataset. All turns in one dialogue retain their state.

Transforms receive deep copies of their input datasets before executing. Their
results get new handles; neither earlier handles nor the initial dataset are
modified, even if a transform fails after changing its copy. Read-only filters,
getters and aggregations do not require this extra copy. Prediction handles are
released before ground-truth replay, and the backend closes after the sample.

**Rebuild/redeploy the backend for these semantics.** Capability 1 checks that
`get_data` advertises `reset_handles` and fails with an upgrade message if it
is missing. Hosted capability 1 also checks `/healthz` for `persistent_backends: true`
before replay, so an old stateless bridge is rejected. This applies to local Docker images as well as hosted images.

### Deploying the session-preserving HTTP bridge

The bridge in `docker/mcp_http_bridge.py` uses the existing capability dispatcher.
Build the unified image and set `SERVE_MODE=http`, `CAPABILITY_ID=1` (or 2–4),
and the existing database/retriever mounts. It listens on `PORT` (default 8080).
The evaluator supplies a random `X-Vakra-Session` header. Calls with that header
share one domain-scoped stdio backend, owned by a dedicated worker task. A
DELETE on the same path/header releases it; otherwise idle workers expire after
`BRIDGE_IDLE_TIMEOUT` seconds (default 900). `BRIDGE_MAX_SESSIONS` defaults to 16.
Tool results preserve MCP error flags and structured content.

**Use one replica per capability app and one bridge process.** Worker state is
in process memory. Unknown/expired session IDs fail explicitly rather than
silently creating an empty backend. Multi-replica deployment requires routing
all requests for a session to its owning replica. This implementation does not
provide distributed session state or automatically restart a sample after loss.

Clients without `X-Vakra-Session` retain the original bridge's stateless per-call
behavior, suitable for independent tool calls. Stateful capability 1 trajectories
must use the updated evaluator and bridge. The existing Code Engine deployment
must be updated by its owner; changing the evaluator alone cannot preserve
handles on a bridge that starts a fresh backend for every call.

The server API targets `mcp>=1.27,<2`, as declared in the MCP dependencies.

## Environment file

Copy `.env.example` to `.env` in the repository root and replace the dummy values
for your selected LLM provider and tool backend. Both `evaluator/evaluator.py`
and `benchmark_runner.py` load that file automatically before importing helpers
that read environment settings. Already-exported shell variables take precedence.
The `.env` file is git-ignored; `.env.example` contains placeholders for sharing.

The evaluator defaults to `JUDGE_BACKEND=rits` and uses `RITS_API_KEY`. For the
Groq judge, set `JUDGE_BACKEND=groq` and `API_KEY`. The runner's `--provider`
selects which provider credentials it needs. Code Engine tools additionally need
the selected capability's `VAKRA_CAPn_URL`; local tools do not use those URLs.
Backend and input/output selection remain CLI/configuration options.
