# Evaluator execution and error reporting

See the [main evaluator README](README.md) for setup, input formats,
scoring and groundedness K. This guide covers hosted execution, domain
concurrency and diagnostics. Ground-truth retries and error reporting apply
to both local and Code Engine execution.

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
outcomes. `failed_llm_call_attempts` counts every failed judge invocation/trial,
including invalid responses. `llm_call_errors` counts distinct
`(domain, UUID, judge)` combinations with failures, grouping repeated K trials.
For example, three failed groundedness trials for one sample produce three
failed attempts and one LLM-call error. If two judges fail on the same sample,
there are two LLM-call errors but only one affected sample.

`samples_with_errors` counts unique `(domain, UUID)` pairs across both error
types. `samples_with_llm_errors` and `samples_with_ground_truth_errors` provide
the corresponding per-type counts. Their sum can exceed `samples_with_errors`
when the same sample has both types. Counts appear in overall, domain and sample
summaries; the original detailed trial records remain available. `error_count`
continues to count raw error records rather than unique samples.
`error_messages` lists unique, nonempty error messages in sorted order. It
includes both ground-truth and LLM errors, including messages from earlier GT
retry attempts. This list appears in overall, domain and sample summaries;
deduplication does not remove detailed records or change error counts.
SDK-internal retries are not counted separately. Normal negative judgments are
not errors. The existing scoring behavior is preserved for unresolved calls.
Resuming from checkpoints with retained error records recomputes these counts.

Results retain diagnostic records in `evaluation_errors` so resume can preserve
completed domains' reports. Retried unfinished domains replace their prior
records. `domains_without_error_tracking` lists completed domains resumed from
older results that did not record diagnostics; their historical errors cannot
be reconstructed as zero errors. Existing active workers must finish or be
restarted before these changes apply to their evaluation.
