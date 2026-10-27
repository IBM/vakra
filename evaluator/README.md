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

## Kev / Jev Judge (optional)

LLM judges can return different verdicts for the same input even at temperature 0. As an alternative, [`kev_judge.py`](/vakra/evaluator/kev_judge.py) provides drop-in correctness, groundedness and exact-match judges backed by a System One decision model: [Kev](https://github.com/jaredpalmer/kev) (open weights, self-hosted) or TypeSafe's hosted [Jev](https://docs.typesafe.ai/introduction). Each judge sends the turn as structured state with one yes/no question to `POST /v1/systemone`, and scores `1.0` when `p(yes) >= 0.5`, `0.0` otherwise. The model generates no text, so there is nothing to parse, and repeated runs return the same score. The idea follows [jev-as-a-judge](https://github.com/danielgshea/jev-as-a-judge).

The judges take the same `JudgeInput` and return the same `JudgeOutput` as the LLM judges, so scoring and the output format are unchanged. The explanation field holds the probability, e.g. `p(yes)=0.973 (kev:kev-latest)`.

Kev (self-hosted):

```bash
# in a Kev checkout (https://github.com/jaredpalmer/kev)
uv run --extra serve python -m kev.serve --run jaredpalmer/kev-4b --port 8009

# in VAKRA
export JUDGE_BACKEND=kev
python evaluator/evaluator.py --capability_name ... --gt_root ... --pred_root ... --output ...
```

Jev (hosted):

```bash
export JUDGE_BACKEND=jev
export TYPESAFE_API_KEY=...
python evaluator/evaluator.py ...
```

| Variable | Purpose | Default |
| --- | --- | --- |
| `JUDGE_BACKEND` | `kev` or `jev` selects these judges; unset (or `rits` / `groq`) keeps the LLM judges | unset |
| `KEV_END_POINT` / `JEV_END_POINT` | Server base URL | `http://127.0.0.1:8009` / `https://api.typesafe.ai` |
| `KEV_MODEL` / `JEV_MODEL` | Model name sent in the request | `kev-latest` / `jev-latest` |
| `KEV_API_KEY` | Bearer token, if the Kev server was started with one | unset |
| `TYPESAFE_API_KEY` | Bearer token for Jev (required) | — |
| `KEV_MAX_STATE_CHARS` | Characters of long state fields (answers, tool responses) sent per call; longer ones lose their middle. `0` sends them whole | `100000` |

Notes:

- Kev refuses states over its context limit (65,536 tokens) with a 422. The judges therefore cut the middle out of long state fields (tool responses and the response for groundedness, ground truth and prediction for correctness) once together they pass `KEV_MAX_STATE_CHARS`; short fields stay whole. If the server still refuses, the budget is halved and the call retried, twice at most. The explanation says when fields were cut, e.g. `p(yes)=0.723 (kev:kev-latest); long fields cut to 100000 of 299211 chars`. Kev-0.8B/4B/9B were trained on states up to about 7.5k tokens; Kev-27B up to 32k, so the smaller models may do better with a lower limit.
- Scores from Kev or Jev are not comparable to leaderboard numbers produced with the default `gpt-oss-120b` judge. Report which judge you used.

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
├── kev_judge.py     # Optional Kev / Jev (System One) versions of the same judges
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
