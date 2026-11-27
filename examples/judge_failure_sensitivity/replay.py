"""Offline sensitivity analysis of VAKRA's groundedness judge failures."""

import argparse
from collections import Counter
from dataclasses import dataclass
from itertools import combinations
import json
import math
from pathlib import Path
import random


@dataclass(frozen=True)
class Dialogue:
    score: float
    status: str


def judge_error(explanation):
    return isinstance(explanation, str) and explanation.startswith("Judge Error ")


def score_value(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Expected a numeric score")
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ValueError("Score must be finite and in [0, 1]")
    return float(value)


def parse(output):
    records = {}
    for domain, group in output["domains"].items():
        for entry in group["dialogues"]:
            uuid = entry["uuid"]
            if not isinstance(uuid, str) or not uuid:
                raise ValueError("Expected a nonempty task UUID")
            key = (output["capability_name"], domain, uuid)
            if key in records:
                raise ValueError(f"Duplicate dialogue: {key}")
            score = score_value(entry["score"])
            turns = entry["details"]["per_turn"]
            if not turns:
                records[key] = Dialogue(score, "no_scored_turn")
                continue
            if len(turns) != 1:
                raise ValueError(f"Expected final-turn-only scoring: {key}")
            turn = turns[0]
            if not math.isclose(score, score_value(turn["score"]), rel_tol=0, abs_tol=1e-9):
                raise ValueError(f"Dialogue and final-turn scores differ: {key}")
            metadata = turn["metadata"]
            evidence = metadata["score_explanation"]
            groundedness = metadata["groundedness_score"]
            groundedness_note = evidence.get("groundedness")
            if judge_error(groundedness_note):
                status = "groundedness_error"
            elif any(judge_error(evidence.get(name)) for name in ("answer", "exactmatch", "policy")):
                status = "other_judge_error"
            elif groundedness is None:
                status = "groundedness_not_called"
            elif isinstance(groundedness_note, str) and groundedness_note.startswith(
                "Special case applied - GT indicates unanswerable"
            ):
                status = "groundedness_rule_credit"
            else:
                score_value(groundedness)
                status = "scored"
            records[key] = Dialogue(score, status)
    if not records:
        raise ValueError("No dialogue rows")
    return records


def summarize(records, excluded, zeroed):
    included = records.keys() - excluded
    return {
        "mean_score": (
            sum(records[key].score for key in sorted(included - zeroed)) / len(included)
            if included else None
        ),
        "included_in_mean": len(included),
        "total_dialogues": len(records),
        "score_denominator_coverage": len(included) / len(records),
        "successful_judgment_coverage": len(included - zeroed) / len(records),
    }


def sign(gap):
    if math.isclose(gap, 0, rel_tol=0, abs_tol=1e-12):
        return 0
    return 1 if gap > 0 else -1


def replay(systems, failure_rate, retry_budget, retry_success, seed, repeats):
    key_sets = [set(records) for records in systems.values()]
    common = set.intersection(*key_sets)
    known = {"scored", "groundedness_not_called", "groundedness_rule_credit"}
    matched = sorted(key for key in common if all(rows[key].status in known for rows in systems.values()))
    if not matched:
        raise ValueError("No shared dialogue rows with known reference scores")
    reports = {}
    trial_policies = {}
    for name, rows in systems.items():
        records = {key: rows[key] for key in matched}
        eligible = {key for key in matched if records[key].status == "scored"}
        count = int(failure_rate * len(eligible) + 0.5)
        trials = []
        for attempt in range(repeats):
            rng = random.Random(seed + attempt)
            # Generate common per-task priorities and recovery draws across systems.
            failure_priority = {key: rng.random() for key in matched}
            retry_priority = {key: rng.random() for key in matched}
            recovery = {key: rng.random() for key in matched}
            failed = set(sorted(eligible, key=lambda key: (failure_priority[key], key))[:count])
            attempted = sorted(failed, key=lambda key: (retry_priority[key], key))[:retry_budget]
            restored = {key for key in attempted if recovery[key] < retry_success}
            unresolved = failed - restored
            trials.append({
                "zero": summarize(records, set(), failed),
                "abstain": summarize(records, failed, set()),
                "retry_then_abstain": summarize(records, unresolved, set()),
            })
        trial_policies[name] = trials
        reports[name] = {
            "source_dialogues": len(rows),
            "unmatched_dialogues": len(rows) - len(common),
            "observed_status_counts": dict(Counter(x.status for x in rows.values())),
            "reference": summarize(records, set(), set()),
            "eligible_for_failure": len(eligible),
            "masked_per_trial": count,
            "retry_attempted_per_trial": min(count, retry_budget),
            "policies": {},
        }
        for policy in trials[0]:
            results = [trial[policy] for trial in trials]
            means = [result["mean_score"] for result in results if result["mean_score"] is not None]
            mean = sum(means) / len(means) if means else None
            reports[name]["policies"][policy] = {
                "mean_score": mean,
                "delta_from_reference": mean - reports[name]["reference"]["mean_score"] if means else None,
                "mean_score_denominator_coverage": sum(r["score_denominator_coverage"] for r in results) / repeats,
                "mean_successful_judgment_coverage": sum(r["successful_judgment_coverage"] for r in results) / repeats,
                "trials_with_score": len(means),
            }
    pairs = []
    for left, right in combinations(systems, 2):
        reference_gap = reports[left]["reference"]["mean_score"] - reports[right]["reference"]["mean_score"]
        pair = {"systems": [left, right], "reference_gap": reference_gap, "policies": {}}
        for policy in trial_policies[left][0]:
            scores = [(l[policy]["mean_score"], r[policy]["mean_score"])
                      for l, r in zip(trial_policies[left], trial_policies[right])]
            gaps = [l - r for l, r in scores if l is not None and r is not None]
            pair["policies"][policy] = {
                "mean_gap": sum(gaps) / len(gaps) if gaps else None,
                "strict_rank_reversals": sum(sign(gap) * sign(reference_gap) == -1 for gap in gaps),
                "ties": sum(sign(gap) == 0 for gap in gaps),
                "trials_with_both_scores": len(gaps),
            }
        pairs.append(pair)
    return {
        "common_dialogues": len(common),
        "matched_reference_dialogues": len(matched),
        "excluded_common_dialogues": len(common) - len(matched),
        "failure_rate_among_eligible": failure_rate,
        "retry_budget_per_system": retry_budget,
        "assumed_retry_success": retry_success,
        "repeats": repeats,
        "seed": seed,
        "systems": reports,
        "pairwise": pairs,
    }


def fixture(rows):
    return {"capability_name": "synthetic", "domains": {"demo": {"dialogues": [
        {"uuid": key, "score": score, "details": {"per_turn": [{"score": score, "metadata": {
            "groundedness_score": groundedness, "score_explanation": {"groundedness": note},
        }}]}} for key, score, groundedness, note in rows
    ]}}}


def demo():
    return {
        "synthetic_a": parse(fixture([
            ("task-1", 1, 1, "yes"), ("task-2", 0, 0, "no"),
            ("task-3", 0, 0, "Judge Error simulated timeout"),
            ("task-4", 0, None, None), ("task-5", 1, 1, "yes"),
        ])),
        "synthetic_b": parse(fixture([
            ("task-1", 0, 0, "no"), ("task-2", 1, 1, "yes"),
            ("task-3", 1, 1, "yes"), ("task-4", 0, None, None),
            ("task-5", 0, 0, "no"),
        ])),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", metavar="NAME=RESULTS.json", nargs="*")
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--failure-rate", type=float, default=0.25)
    parser.add_argument("--retry-budget", type=int, default=1)
    parser.add_argument("--retry-success", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=71)
    parser.add_argument("--repeats", type=int, default=200)
    args = parser.parse_args()
    if args.demo == bool(args.results):
        parser.error("Supply --demo or one or more named evaluator outputs")
    if not (0 <= args.failure_rate <= 1 and 0 <= args.retry_success <= 1):
        parser.error("Rates must be in [0, 1]")
    if args.retry_budget < 0 or args.repeats < 1:
        parser.error("Retry budget must be nonnegative and repeats must be positive")
    try:
        systems = demo() if args.demo else {}
        for argument in args.results:
            name, separator, filename = argument.partition("=")
            if not (separator and name and filename) or name in systems:
                raise ValueError(f"Expected a unique NAME=RESULTS.json argument: {argument}")
            systems[name] = parse(json.loads(Path(filename).read_text(encoding="utf-8")))
        report = replay(systems, args.failure_rate, args.retry_budget, args.retry_success,
                        args.seed, args.repeats)
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.error(str(error))
    print(json.dumps({"synthetic_demo": args.demo, **report}, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
