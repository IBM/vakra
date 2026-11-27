# VAKRA #71 offline pilot

This prototype reads per-dialogue evaluator JSON and compares three policies on
the same simulated groundedness failures. It uses only the Python standard
library and does not call a model or the MCP environment.

## Run

From the repository root:

```sh
python examples/judge_failure_sensitivity/replay.py --demo
python examples/judge_failure_sensitivity/replay.py system_a=run_a.json system_b=run_b.json \
  --failure-rate 0.10 --retry-budget 3 --retry-success 0.50 --repeats 200
python -m unittest discover -s examples/judge_failure_sensitivity -p 'test_*.py' -v
```

Use outputs for the same capability and task set. One input is enough to
measure score and coverage sensitivity. With multiple inputs, the replay uses
the intersection of capability, domain and UUID, and reports dropped rows.
The input schema is documented in the [VAKRA evaluator README](https://github.com/IBM/vakra/blob/main/evaluator/README.md).

## What the replay measures

- A legitimate zero remains a score. A `Judge Error` explanation marks an error.
  An uncalled groundedness judge and rule-based credit remain valid dialogue
  results, and neither is eligible for fault injection.
- Observed errors and rows with no scored turn are counted. A row with any such
  result in the compared systems is excluded from the shared reference set
  because its original successful score is unknown.
- Random failures mask a fixed rounded fraction of the eligible dialogues in
  each run. Per-task random priorities are shared across systems, but selected
  failures can differ when eligibility differs. This coupling is a simulation
  choice, not an assumption that real failures have this dependence structure.
- The denominator is the matched set of dialogues with known reference scores
  across all systems. This complete-reference selection can be unrepresentative.
  Score-denominator coverage and successful-judgment coverage are reported
  separately: injected failures assigned zero enter the former, not the latter.
- The zero policy assigns zero and includes the row in the score denominator.
  Abstention excludes masked rows from that denominator. Simulated retries
  restore reference scores with a supplied probability, up to the retry budget
  per system and trial. Remaining failures are treated as abstentions.
- A ranking reversal is a strict sign change in a pairwise score gap. Ties and
  trials with an undefined mean are reported separately.

`demo.json` and `--demo` use synthetic records. They are not evidence about
VAKRA systems. The parser targets final-turn evaluator output as documented at
upstream commit `00dc68febdc28f5c3498ac00e2cd7d7b89dee315`; it has not been
validated against a real evaluator output. One offline test exercises the
upstream `DialogueScorer` using a stubbed turn scorer and a synthetic dialogue;
this checks output-shape compatibility, not the real judge or MCP pipeline.
The first version simulates
random masking. Trace-length-dependent masking requires a suitable length
field for each dialogue and is not implemented. Retry probability is a scenario
assumption, and successful retries restore the pre-existing reference score;
the replay does not measure retry outcomes, rerun judges, or validate labels.
