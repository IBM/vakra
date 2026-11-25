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

## Code Engine tools (capabilities 2–4)

The evaluator supports `--tool-backend local|code-engine`; local remains the
existing default. The connection adapter is confined to the evaluator; its YAML
lives beside the other connection configs in `benchmark/`. Environment servers,
containers, benchmark client/runner code and benchmark execution are unchanged.

Copy `.env.example` to `.env` in the repository root and replace the selected
capability's dummy `VAKRA_CAP2_URL`, `VAKRA_CAP3_URL` or `VAKRA_CAP4_URL` with its
app URL (not the browser explorer URL). The evaluator loads this file without
overriding exported shell variables. Existing judge credentials are still needed.

```bash
python evaluator/evaluator.py \
  --capability_name capability_dashboard_apis \
  --gt_root /path/to/groundtruth --pred_root /path/to/predictions \
  --domains california_schools --tool-backend code-engine \
  --output /tmp/ce-dashboard-results.json
```

`benchmark/mcp_connection_config.ce.yaml` maps capabilities 2–4 to streamable HTTP
endpoints `/mcp/<domain>`. `--mcp-config PATH` overrides the selected backend's
configuration. Explicit HTTP overrides also work with the default backend flag.
Local connections delegate to the existing `benchmark/mcp_client.py` unchanged.

### Capability 1 fallback

Capability 1's hosted stateful tool sequences are not supported in this version.
Requesting `--tool-backend code-engine` with `capability_bi_apis` logs:

```text
Code Engine tool calling for capability 1 is not supported in this version; falling back to local execution. No Code Engine connection will be opened.
```

Execution then uses the existing local capability 1 configuration. No hosted URL
or server upgrade is required for that fallback. An HTTP override cannot bypass
this restriction. A custom local capability 1 entry is honored; otherwise the
default `benchmark/mcp_connection_config.yaml` supplies the local settings.
Normal local requirements (including a running configured container) still apply;
local failures are reported normally, with no second attempt to use Code Engine.

Results record the **effective** backend: capability 1 fallback is recorded as
`local`, even when `code-engine` was requested. Resume checks reject results from
a different backend/URL. Legacy results without connection metadata may be
resumed locally, including through the capability 1 fallback.

The HTTP client supports the installed MCP 1.x and 2.x transport constructors.
No benchmark/environment SDK dependency or Docker image changes are required by
this evaluator-only PR. Capability 1 state handling belongs to the stacked PR.

## Groundedness K

The evaluator defaults to **K=3**, so runs without `--k` report Pass@3 and Pass^3.
Use `--k 1` for the previous single-judgment behavior, or another positive integer
for a different number of groundedness judgments. The judge constructor and
default capability registry also use K=3.

Existing results produced with K=1 cannot be resumed as K=3. Use a new `--output`
file for the new default, or pass `--k 1` to resume a compatible K=1 run.

## Concurrent Code Engine domains

For capabilities 2–4, `--tool-backend code-engine` evaluates up to 20 domains
concurrently. Use `--num-domains N` (or `--num_domains N`) to change this positive
integer limit. This limits simultaneous domains; all selected domains are queued
and evaluated. `--domains` still selects which domains to run. Local execution
and capability 1's local fallback remain sequential.

Each domain runs in a separate process with its own MCP connection and judge
clients. Tool calls within a domain retain their existing order. For example:

```bash
python evaluator/evaluator.py \
  --capability_name capability_dashboard_apis \
  --gt_root /path/to/groundtruth --pred_root /path/to/predictions \
  --tool-backend code-engine --num-domains 20 \
  --output /tmp/ce-dashboard-results.json
```

The overall log tags progress with capability, domain, stage and sample UUID.
Detailed stdout, stderr, tool progress and tracebacks go to a separate log for
each domain, under `<output_stem>_logs/<run_id>/` beside the output file. Override
the log root with `--log-dir PATH`. Results record these paths in `domain_logs`.

Only the coordinator writes the aggregate results, using atomic replacement
after each completed or failed domain. `execution`, `domain_progress` and
`failed_domains` describe run status; metrics include completed domains only.
A domain exception does not stop other domains. Such runs print `INCOMPLETE`,
record status `partial`, and exit with code 1. Individual tool errors retain the
existing replay/scoring behavior; they do not automatically fail the domain.

Rerunning the command skips completed domains and retries failed or unfinished
ones with new log files. Concurrency can change when resuming; backend, endpoint
and K compatibility checks still apply. Ctrl-C or SIGTERM stops active workers
and checkpoints completed results before exiting.

## Ground-truth retries and error report

Ground-truth tool replay retries failed calls **three additional times** (four
attempts total), with 1, 2 and 4 seconds between attempts. This applies to local
and Code Engine execution for all capabilities. Prediction calls retain one
attempt. Retry detection includes raised exceptions, either MCP error flag, and
explicit error text/JSON payloads. Cancellation interrupts retries immediately.
Capability 1's reference resolution uses the final successful retry response.

Every evaluation writes a separate `<output_stem>.errors.json` beside its results.
For example, `--output results.json` produces `results.errors.json`. The final
console summary prints ground-truth and LLM error counts and the report path.
The coordinator atomically checkpoints the report after each domain, including
collected errors if the domain fails. Workers do not write the shared report.

The report groups records under `domains.<domain>.samples.<uuid>.errors`, with
counts at sample, domain and overall levels. Two record types are collected:

- `ground_truth_tool_call`: tool name, turn/tool indices, attempt count, failed
  attempt count, each attempt's error message, and `recovered` status.
- `llm_call`: correctness/groundedness judge name, request or response-parsing
  phase, error message, and groundedness trial number when applicable.

`ground_truth_tool_call_errors` counts affected tool calls, including recovered
calls. `failed_ground_truth_attempts` counts their failed attempts separately;
`unresolved_ground_truth_calls` and `recovered_ground_truth_calls` distinguish the
outcomes. `llm_call_errors` counts failed judge invocations/trials, including
invalid responses; a K=3 sample may therefore have three groundedness errors.
SDK-internal retries are not counted separately. Normal negative judgments are
not errors. The existing scoring behavior is preserved for unresolved calls.

Results retain diagnostic records in `evaluation_errors` so resume can preserve
completed domains' reports. Retried unfinished domains replace their prior
records. `domains_without_error_tracking` lists completed domains resumed from
older results that did not record diagnostics; their historical errors cannot
be reconstructed as zero errors. Existing active workers must finish or be
restarted before these changes apply to their evaluation.
