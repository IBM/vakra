from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List, Callable
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from judge import CorrectnessJudge, GroundednessJudge, ExactMatchJudge
from tool_backends import (
    MCPConnectionConfig,
    select_evaluator_mcp_config,
    create_client_and_connect,
)
from mcp_tools import (
    execute_tools_batch,
    extract_toolcalls_for_mcp,
    inject_mcp_responses,
)
from scorer import (
    DialogueScorer,
    DialogueScorerConfig,
    TurnScorer,
    TurnScorerConfig,
)
from constant import PRED_OUTPUT_KEY, PRED_OUTPUT_SEQUENCE_KEY
from constant import N_TOOL_CALLS_PER_TURN
from utils import read_domain_file, pair_dialogues_by_uuid
from evaluation_errors import llm_error_context, write_error_report, error_report_path

CAPABILITY_MCP_TOOL_MAP={
    "capability_bi_apis": 1,
    "capability_dashboard_apis": 2,
    "capability_multihop_reasoning": 3,
    "capability_multiturn": 4,
}

# -----------------------------
# Capability policy / registry
# -----------------------------


@dataclass(frozen=True)
class CapabilityPolicy:

    dialogue_aggregate: str = "mean"        # "mean" | "sum" | "min"

    # --- MCP execution ---
    execute_mcp_tools: bool = True

    # --- Judges ---
    correctness_judge: Any = field(default_factory=CorrectnessJudge)
    groundedness_judge: Any = field(default_factory=GroundednessJudge)
    exactmatch_judge: Any = field(default_factory=ExactMatchJudge)



def build_default_capability_registry(k: int = 3) -> Dict[str, CapabilityPolicy]:
    return {
        "capability_bi_apis": CapabilityPolicy(
            execute_mcp_tools=True,
            correctness_judge=CorrectnessJudge(config={}),
            groundedness_judge=GroundednessJudge(config={}, k=k),
            exactmatch_judge=ExactMatchJudge(config={})
        ),

        "capability_dashboard_apis": CapabilityPolicy(
            execute_mcp_tools=True,
            correctness_judge=CorrectnessJudge(config={}),
            groundedness_judge=GroundednessJudge(config={}, k=k),
            exactmatch_judge=ExactMatchJudge(config={})            
        ),

        "capability_multihop_reasoning": CapabilityPolicy(
            execute_mcp_tools=True,
            correctness_judge=CorrectnessJudge(config={}),
            groundedness_judge=GroundednessJudge(config={}, k=k),
            exactmatch_judge=ExactMatchJudge(config={})            
        ),
        "capability_multiturn": CapabilityPolicy(
            execute_mcp_tools=True,
            correctness_judge=CorrectnessJudge(config={}),
            groundedness_judge=GroundednessJudge(config={}, k=k),
            exactmatch_judge=ExactMatchJudge(config={})            
        ),        
    }


# -----------------------------
# capability_bi_apis helpers
# -----------------------------


def _prepend_get_data_to_batch(
    batch_tools: List[List[List[Dict[str, Any]]]],
    uuids: List[str],
    skip_initialize_active_data: bool = False,
) -> List[List[List[Dict[str, Any]]]]:
    """Prepend get_data(tool_universe_id=uuid) to first turn of each dialogue.

    Optionally removes initialize_active_data from first turn (for ground truth).
    """
    result = []
    for dialogue_tools, uuid in zip(batch_tools, uuids):
        if not dialogue_tools:
            result.append(dialogue_tools)
            continue
        first_turn = list(dialogue_tools[0])
        initial_label = None
        if skip_initialize_active_data and first_turn and (
            first_turn[0].get("name") == "initialize_active_data"
            or first_turn[0].get("name") == "get_data"
        ):
            initial_label = first_turn[0].get("label")
            first_turn = first_turn[1:]
        initial_tool = {"name": "get_data", "arguments": {"tool_universe_id": uuid}}
        if initial_label:
            initial_tool["label"] = initial_label
        first_turn = [initial_tool] + first_turn
        result.append([first_turn] + list(dialogue_tools[1:]))
    return result


def _update_dialogue_toolcall_for_get_data(
    dialogue: Dict[str, Any],
    uuid: str,
    skip_initialize_active_data: bool = False,
) -> None:
    """Update dialogue's sequence.tool_call in-place to match modified execution sequence.

    Must be called before inject_mcp_responses to keep tool_call and tool_response aligned.
    """
    turns = dialogue.get(PRED_OUTPUT_KEY, [])
    if not turns:
        return
    seq = turns[0].get(PRED_OUTPUT_SEQUENCE_KEY) or {}
    if not isinstance(seq, dict):
        return
    tool_calls = list(seq.get("tool_call", []))
    if skip_initialize_active_data and tool_calls and tool_calls[0].get("name") == "initialize_active_data":
        tool_calls = tool_calls[1:]
    tool_calls = [{"name": "get_data", "arguments": {"tool_universe_id": uuid}}] + tool_calls
    seq["tool_call"] = tool_calls
    turns[0][PRED_OUTPUT_SEQUENCE_KEY] = seq


# -----------------------------
# Evaluator core
# -----------------------------


async def evaluate_domain(
    domain: str,
    gt_path: Path,
    pred_path: Path,
    policy: CapabilityPolicy,
    mcp_config: Optional[MCPConnectionConfig],
    capability_name: str,
    policy_judge_path: Optional[str] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    error_records: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Any], List[float]]:
    """
    Evaluate a single domain (async version).

    Returns:
        Tuple of (domain_out dict, dialogue_scores list)
    """
    if error_records is None:
        error_records = []
    # Read data files
    gt_list = read_domain_file(gt_path)
    pred_list = read_domain_file(pred_path) if pred_path.exists() else []

    # Pair dialogues
    paired, missing_pred, extra_pred = pair_dialogues_by_uuid(gt_list, pred_list)    

    if len(pred_list) == 0:
        domain_out: Dict[str, Any] = {
            "domain": domain,
            "n_groundtruth": len(gt_list),
            "n_prediction": len(pred_list),
            "n_paired": len(paired),
            "missing_prediction_uuids": missing_pred,
            "extra_prediction_uuids": extra_pred,
            "dialogues": [],
            "summary":{
                    "num_samples": len(gt_list),
                    "num_correct": 0.0,
                    "mean_dialogue_score": 0.0,
                    "min_dialogue_score":  0.0,
                    "max_dialogue_score": 0.0,
                    "pass_at_k": 0.0,
                    "pass_power_k": 0.0,
                        }
            }
        return domain_out, []

    if progress_callback:
        progress_callback({'event': 'stage', 'stage': 'loaded', 'total': len(paired)})

    def replay_progress(stage):
        if progress_callback is None:
            return None
        def report(event):
            index = event.get('dialogue_index')
            uuid = str(paired[index][0].get('uuid')) if index is not None else None
            progress_callback({**event, 'stage': stage, 'uuid': uuid})
        return report

    def record_gt_error(event):
        index = event['dialogue_index']
        record = {**event, 'capability': capability_name, 'domain': domain,
                  'uuid': str(paired[index][0].get('uuid')),
                  'time': datetime.now(timezone.utc).isoformat()}
        error_records.append(record)

    def score_dialogue(gt_raw, pr_raw):
        with llm_error_context(error_records, capability=capability_name, domain=domain,
                               uuid=str(gt_raw.get('uuid'))):
            return dialogue_scorer.score(gt_dialogue=gt_raw, pred_dialogue=pr_raw,
                                         pred_key=PRED_OUTPUT_KEY)

    # Build scorers per domain
    turn_cfg = TurnScorerConfig(
        capability=capability_name,
        domain=domain,
        policy_judge_path=policy_judge_path,
    )
    turn_scorer = TurnScorer(
        cfg=turn_cfg,
        correctness_judge=policy.correctness_judge,
        groundedness_judge=policy.groundedness_judge,
        exactmatch_judge=policy.exactmatch_judge
    )
    dialogue_scorer = DialogueScorer(
        turn_scorer=turn_scorer,
        cfg=DialogueScorerConfig(
            aggregate=policy.dialogue_aggregate,
        ),
    )

    domain_out: Dict[str, Any] = {
        "domain": domain,
        "n_groundtruth": len(gt_list),
        "n_prediction": len(pred_list),
        "n_paired": len(paired),
        "missing_prediction_uuids": missing_pred,
        "extra_prediction_uuids": extra_pred,
        "dialogues": [],
    }

    dialogue_scores: List[float] = []

    # MCP execution and scoring
    if policy.execute_mcp_tools and mcp_config and paired:
        if progress_callback:
            progress_callback({'event': 'stage', 'stage': 'connecting'})
        async with create_client_and_connect(mcp_config, domain) as session:
            # Get schema from tools
            tools_result = await session.list_tools()
            schema_map = {tool.name: getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", {}) for tool in tools_result.tools}

            # Batch execute tools
            batch_tools_pred = [extract_toolcalls_for_mcp(pr, limit_last_n=N_TOOL_CALLS_PER_TURN) for _, pr in paired]
            batch_tools_gt = [extract_toolcalls_for_mcp(gt) for gt, _ in paired]

            # capability_bi_apis: replace initialize_active_data with get_data (GT),
            # and prepend get_data (pred), then sync dialogue tool_call sequences
            if capability_name == "capability_bi_apis":
                uuids = [str(gt.get("uuid")) for gt, _ in paired]
                batch_tools_gt = _prepend_get_data_to_batch(batch_tools_gt, uuids, skip_initialize_active_data=True)
                batch_tools_pred = _prepend_get_data_to_batch(batch_tools_pred, uuids, skip_initialize_active_data=False)
                for (gt_raw, pr_raw), uuid in zip(paired, uuids):
                    _update_dialogue_toolcall_for_get_data(gt_raw, uuid, skip_initialize_active_data=True)
                    _update_dialogue_toolcall_for_get_data(pr_raw, uuid, skip_initialize_active_data=False)

            mcp_batch_responses_pred = await execute_tools_batch(session, batch_tools_pred, schema_map,
                                                                 progress_callback=replay_progress("prediction"))
            mcp_batch_responses_gt = await execute_tools_batch(
                session,
                batch_tools_gt,
                schema_map,
                is_gt=capability_name == "capability_bi_apis",
                progress_callback=replay_progress("ground_truth"),
                gt_retries=3,
                error_callback=record_gt_error,
            )

            if progress_callback:
                progress_callback({'event': 'stage', 'stage': 'scoring', 'total': len(paired)})
            # Score each paired dialogue
            for idx, (gt_raw, pr_raw) in enumerate(
                tqdm(paired, desc=f"[{capability_name}][{domain}]", leave=False, disable=progress_callback is not None)
            ):
                uuid = str(gt_raw.get("uuid"))

                # Inject fresh responses so groundedness judge uses tool outputs
                inject_mcp_responses(pr_raw, mcp_batch_responses_pred[idx], type="pred", capability_name=capability_name)
                if capability_name != "capability_bi_apis" and mcp_batch_responses_gt is not None:
                    inject_mcp_responses(gt_raw, mcp_batch_responses_gt[idx], type="gt",capability_name=capability_name)

                # Score and store details
                dialogue_score, dialogue_details = score_dialogue(gt_raw, pr_raw)
                dialogue_scores.append(float(dialogue_score))
                if progress_callback:
                    progress_callback({'event': 'sample_scored', 'stage': 'scoring', 'uuid': uuid,
                                       'completed': idx + 1, 'total': len(paired)})

                domain_out["dialogues"].append({
                    "uuid": uuid,
                    "score": float(dialogue_score),
                    "metadata": {
                        "capability": capability_name,
                        "domain": domain,
                        "policy": {
                            "dialogue_aggregate": policy.dialogue_aggregate,
                            "execute_mcp_tools": policy.execute_mcp_tools,
                        },
                    },
                    "details": dialogue_details,
                })
    else:
        # No MCP tools - just score based on predictions as-is
        for idx, (gt_raw, pr_raw) in enumerate(
            tqdm(paired, desc=f"[{capability_name}][{domain}]", leave=False, disable=progress_callback is not None)
        ):
            uuid = str(gt_raw.get("uuid"))

            dialogue_score, dialogue_details = score_dialogue(gt_raw, pr_raw)
            dialogue_scores.append(float(dialogue_score))
            if progress_callback:
                progress_callback({'event': 'sample_scored', 'stage': 'scoring', 'uuid': uuid,
                                   'completed': idx + 1, 'total': len(paired)})

            domain_out["dialogues"].append({
                "uuid": uuid,
                "score": float(dialogue_score),
                "metadata": {
                    "capability": capability_name,
                    "domain": domain,
                    "policy": {
                        "dialogue_aggregate": policy.dialogue_aggregate,
                        "execute_mcp_tools": policy.execute_mcp_tools,
                    },
                },
                "details": dialogue_details,
            })

    # Domain summary
    domain_scores = [d["score"] for d in domain_out["dialogues"]]
    domain_out["summary"] = {
        "num_samples": len(gt_list),
        "num_correct": sum(domain_scores),
        "mean_dialogue_score": (sum(domain_scores) / len(domain_scores)) if domain_scores else 0.0,
        "min_dialogue_score": min(domain_scores) if domain_scores else 0.0,
        "max_dialogue_score": max(domain_scores) if domain_scores else 0.0,
    }

    domain_out["summary"].update(_pass_metrics(domain_out["dialogues"], len(gt_list)))
    return domain_out, dialogue_scores

def _pass_metrics(dialogues: List[Dict[str, Any]], count: int) -> Dict[str, float]:
    return {
        name: sum(d["details"][name] for d in dialogues) / count if count else 0.0
        for name in ("pass_at_k", "pass_power_k")
    }


def _load_existing_results(out_path: Path) -> Optional[Dict[str, Any]]:
    """
    Load existing results file if it exists.

    Returns:
        Existing results dict, or None if file doesn't exist or is invalid.
    """
    if not out_path.exists():
        return None

    try:
        data = json.loads(out_path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "domains" in data:
            print(f"Found existing results with {len(data['domains'])} completed domain(s)")
            return data
    except (json.JSONDecodeError, OSError) as e:
        print(f"Warning: Could not load existing results from {out_path}: {e}")

    return None


def _write_intermediate_results(
    results: Dict[str, Any],
    all_dialogue_scores: List[float],
    out_path: Path,
    *, atomic: bool = False,
) -> None:
    """
    Write intermediate results to disk.

    Updates the summary with current statistics and writes to the output file.
    This allows recovery of partial results if evaluation is interrupted.
    """
    # Calculate totals from completed domains
    total_paired = sum(
        d["n_paired"] for d in results["domains"].values()
    )
    total_missing = sum(
        len(d["missing_prediction_uuids"])
        for d in results["domains"].values()
    )
    total_extra = sum(
        len(d["extra_prediction_uuids"])
        for d in results["domains"].values()
    )

    total_samples = sum(
        d["summary"]["num_samples"] for d in results["domains"].values()
    )

    # Update summary with current progress
    num_correct=(sum(all_dialogue_scores) if all_dialogue_scores else 0.0)
    results["summary"] = {
        "n_domains": len(results["domains"]),
        "n_paired_dialogues": total_paired,
        "n_missing_predictions": total_missing,
        "n_extra_predictions": total_extra,
        "n_samples": total_samples,
        "n_correct": num_correct,
        "mean_dialogue_score": (num_correct/total_samples
            if all_dialogue_scores
            else 0.0
        ),
        "min_dialogue_score": (
            min(all_dialogue_scores) if all_dialogue_scores else 0.0
        ),
        "max_dialogue_score": (
            max(all_dialogue_scores) if all_dialogue_scores else 0.0
        ),
    }

    results["summary"].update(_pass_metrics(
        [dialogue for domain in results["domains"].values() for dialogue in domain["dialogues"]],
        total_samples,
    ))

    # The coordinator owns both files; persist diagnostics with every checkpoint.
    results['error_report_file'] = str(error_report_path(out_path))
    write_error_report(results, out_path)
    # Write to disk
    if atomic:
        from domain_runner import atomic_write_json
        atomic_write_json(out_path, results)
    else:
        out_path.write_text(
            json.dumps(results, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return results


def _evaluate_parallel_domains(results, all_dialogue_scores, gt_files, pred_dir, out_path,
                               policy, mcp_config, capability_name, policy_judge_path,
                               num_domains, log_dir):
    from domain_runner import DomainJob, pack_policy, run_domains

    selected = [path.stem for path in gt_files]
    pending = [path for path in gt_files if path.stem not in results["domains"]]
    root = Path(log_dir) if log_dir is not None else out_path.parent / f"{out_path.stem}_logs"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex[:8]
    run_path = root.resolve() / run_id
    failures = results.setdefault("failed_domains", {})
    logs = results.setdefault("domain_logs", {})
    progress = results.setdefault("domain_progress", {})
    domain_errors = results.setdefault('evaluation_errors', {})
    for domain in selected:
        failures.pop(domain, None)
        if domain not in results['domains']:
            domain_errors.pop(domain, None)
        progress[domain] = {"status": "completed" if domain in results["domains"] else "pending"}
    execution = results["execution"] = {
        "mode": "parallel-code-engine", "num_domains": num_domains,
        "max_workers": min(num_domains, len(pending)), "run_id": run_id,
        "log_directory": str(run_path), "selected_domains": selected, "status": "running",
    }
    def checkpoint():
        execution["completed_domains"] = [d for d in selected if d in results["domains"]]
        execution["failed_domains"] = [d for d in selected if d in failures]
        execution["pending_domains"] = [d for d in selected if d not in results["domains"] and d not in failures]
        # Completion order must not change output ordering or aggregation order.
        results["domains"] = dict(sorted(results["domains"].items()))
        all_dialogue_scores[:] = [float(dialogue["score"])
            for domain in results["domains"].values() for dialogue in domain["dialogues"]]
        _write_intermediate_results(results, all_dialogue_scores, out_path, atomic=True)

    def on_event(event):
        domain = event['domain']
        state = progress[domain]
        if state['status'] in ('completed', 'failed'):
            return
        if event['event'] == 'domain_started':
            state['status'] = 'running'
            logs[domain] = event['log_path']
        state.update(event)
        # Worker logs retain tool-start messages too; the overall log uses one
        # concise line per completion/stage, never multiple concurrent bars.
        if event['event'] == 'tool_started':
            return
        count = f" {event['completed']}/{event['total']}" if 'completed' in event else ''
        tool = f" {event['tool']}" if 'tool' in event else ''
        uuid = f" uuid={event['uuid']}" if event.get('uuid') else ''
        error = ' ERROR' if event.get('error') else ''
        print(f"[{capability_name}][{domain}][{event.get('stage', 'worker')}] "
              f"{event['event']}{count}{tool}{uuid}{error}", flush=True)

    def on_result(outcome):
        domain = outcome.domain
        domain_errors[domain] = outcome.errors
        logs[domain] = outcome.log_path
        progress[domain].update(status='failed' if outcome.error else 'completed', seconds=outcome.seconds)
        if outcome.error:
            failures[domain] = {"error": outcome.error, "log_path": outcome.log_path,
                                "run_id": run_id, "seconds": outcome.seconds}
            print(f"[{capability_name}][{domain}] FAILED: {outcome.error!r}; log={outcome.log_path}", flush=True)
        else:
            results["domains"][domain] = outcome.data
            failures.pop(domain, None)
            print(f"[{capability_name}][{domain}] COMPLETED in {outcome.seconds:.1f}s; log={outcome.log_path}", flush=True)
        checkpoint()
        print(f"[{capability_name}] Domains: completed={len(execution['completed_domains'])} "
              f"failed={len(execution['failed_domains'])} pending={len(execution['pending_domains'])}", flush=True)

    for path in gt_files:
        if path.stem in results["domains"]:
            print(f"Skipping already-completed domain: {path.stem}", flush=True)
    jobs = []
    if pending:
        spec = pack_policy(policy)
        run_path.mkdir(parents=True, exist_ok=True)
        jobs = [DomainJob(path.stem, path, pred_dir / path.name, capability_name, mcp_config,
                          spec, policy_judge_path, run_path / f"{path.stem}.log",
                          run_path / f"{path.stem}.result.json") for path in pending]
    checkpoint()
    print(f"[{capability_name}] Code Engine domain workers: {execution['max_workers']} "
          f"(limit={num_domains}); logs={run_path}", flush=True)
    try:
        run_domains(jobs, num_domains, on_event, on_result)
    except KeyboardInterrupt:
        execution["status"] = "interrupted"
        for domain in execution["pending_domains"]:
            if progress[domain]['status'] == 'running':
                progress[domain]['status'] = 'interrupted'
        checkpoint()
        print(f"[{capability_name}] Interrupted; completed results saved to {out_path}", flush=True)
        raise
    execution["status"] = "partial" if failures else "completed"
    checkpoint()


def evaluate_capability(
    capability_name: str,
    gt_dir: Path,
    pred_dir: Path,
    out_path: Path,
    registry: Dict[str, CapabilityPolicy],
    mcp_config: Optional[MCPConnectionConfig] = None,
    selected_domains: Optional[set[str]] = None,
    policy_judge_path: Optional[str] = None,
    tool_backend: str = "local",
    num_domains: int = 20,
    log_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    if isinstance(num_domains, bool) or not isinstance(num_domains, int) or num_domains < 1:
        raise ValueError("num_domains must be a positive integer")
    if capability_name not in registry:
        raise ValueError(
            f"Capability '{capability_name}' not found in registry. Add it in build_default_capability_registry()."
        )

    policy = registry[capability_name]

    # Discover domain files from groundtruth folder
    all_gt_files = sorted([p for p in gt_dir.glob("*.json") if p.is_file()])

    if selected_domains:
        gt_files = [p for p in all_gt_files if p.stem in selected_domains]

        missing = selected_domains - {p.stem for p in all_gt_files}
        if missing:
            raise ValueError(
                f"Requested domains not found in groundtruth_dir: {sorted(missing)}"
            )
    else:
        gt_files = all_gt_files

    if not gt_files:
        raise ValueError("No matching domain files found for evaluation.")

    # Load existing results if available (for resume capability)
    existing_results = _load_existing_results(out_path)
    k = policy.groundedness_judge.k
    if existing_results and (existing_results.get("groundedness_k") != k
                             or existing_results.get("metrics_version") != 1):
        raise ValueError("Existing results use different K or metric schema; choose a new --output file.")
    connection_identity = {
        "backend": tool_backend,
        "mode": mcp_config.mode if mcp_config else None,
        "server_url": mcp_config.server_url if mcp_config else None,
    }
    if existing_results:
        previous = existing_results.get("tool_connection")
        if previous is None and (tool_backend != "local" or (mcp_config and mcp_config.mode in ("http", "streamable-http"))):
            raise ValueError("Existing results have no backend metadata; choose a new --output for Code Engine")
        if previous is not None and previous != connection_identity:
            raise ValueError("Existing results use a different tool backend/endpoint; choose a new --output")
        results = existing_results
        results["tool_connection"] = connection_identity
        # Reconstruct all_dialogue_scores from existing results
        all_dialogue_scores: List[float] = []
        for domain_data in results["domains"].values():
            all_dialogue_scores.extend([d["score"] for d in domain_data["dialogues"]])
    else:
        results: Dict[str, Any] = {
            "groundedness_k": k,
            "metrics_version": 1,
            "capability_name": capability_name,
            "tool_connection": connection_identity,
            "groundtruth_dir": str(gt_dir),
            "prediction_dir": str(pred_dir),
            "domains": {},
            "summary": {},
        }
        all_dialogue_scores: List[float] = []

    # Get list of already-completed domains
    completed_domains = set(results["domains"].keys())

    use_workers = (
        tool_backend == "code-engine"
        and capability_name in ("capability_dashboard_apis", "capability_multihop_reasoning", "capability_multiturn")
        and mcp_config is not None and mcp_config.mode in ("http", "streamable-http")
    )
    if use_workers:
        _evaluate_parallel_domains(results, all_dialogue_scores, gt_files, pred_dir, out_path,
                                   policy, mcp_config, capability_name, policy_judge_path,
                                   num_domains, log_dir)
    else:
        for gt_path in gt_files:
            domain = gt_path.stem

            # Skip already-completed domains
            if domain in completed_domains:
                print(f"Skipping already-completed domain: {domain}")
                continue

            pred_path = pred_dir / gt_path.name

            print(f"\nEvaluating domain: {domain}")
            # Run async evaluation for this domain
            records = []
            try:
                domain_out, domain_scores = asyncio.run(evaluate_domain(
                    domain=domain,
                    gt_path=gt_path,
                    pred_path=pred_path,
                    policy=policy,
                    mcp_config=mcp_config,
                    capability_name=capability_name,
                    policy_judge_path=policy_judge_path,
                    error_records=records,
                ))
            except BaseException:
                results.setdefault('evaluation_errors', {})[domain] = records
                _write_intermediate_results(results, all_dialogue_scores, out_path)
                raise
            results.setdefault('evaluation_errors', {})[domain] = records

            all_dialogue_scores.extend(domain_scores)
            results["domains"][domain] = domain_out

            # Write intermediate results after each domain
            # This ensures partial results are saved if evaluation is interrupted
            _write_intermediate_results(results, all_dialogue_scores, out_path)

        # Final write (summary already updated by last intermediate write)
        # This is technically redundant but ensures the final state is written
        results=_write_intermediate_results(results, all_dialogue_scores, out_path)

    print("=========================================================================")
    print("==================================[RESULTS]==============================")
    print("=========================================================================")

    num_samples = results["summary"].get("n_samples", 0)
    correct_sum = results["summary"].get("n_correct", 0.0)
    accuracy = results["summary"].get("mean_dialogue_score", 0.0)

    if results.get("execution", {}).get("status") in ("partial", "interrupted"):
        print("INCOMPLETE evaluation: metrics below cover completed domains only.", flush=True)
    print("Number of samples evaluated:", num_samples)
    print("Number of correct dialogues:", correct_sum)
    print("Accuracy:", accuracy)
    print(f"Pass@{k} accuracy:", results["summary"]["pass_at_k"])
    print(f"Pass^{k} accuracy:", results["summary"]["pass_power_k"])
    error_report = write_error_report(results, out_path)
    error_summary = error_report['summary']
    print(f"Ground-truth calls with errors: {error_summary['ground_truth_tool_call_errors']} "
          f"(recovered={error_summary['recovered_ground_truth_calls']}, "
          f"unresolved={error_summary['unresolved_ground_truth_calls']}); "
          f"LLM errors: {error_summary['llm_call_errors']}")
    print(f"Error report: {error_report_path(out_path)}")
    if error_report['domains_without_error_tracking']:
        print('Historical error tracking unavailable for resumed domains: ' +
              ', '.join(error_report['domains_without_error_tracking']))
    print("=========================================================================")

    return results


# -----------------------------
# CLI
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=3, help="Positive number of groundedness judgments (default: 3)")
    ap.add_argument("--capability_name", required=True, help="Capability name (must exist in registry)")
    ap.add_argument("--gt_root", required=True, help="Path to capability_name/groundtruth/")
    ap.add_argument("--pred_root", required=True, help="Path to capability_name/prediction/")
    ap.add_argument("--output", default=None, help="Output results.json path (default: <capability_root>/results.json)")
    ap.add_argument(
        "--mcp-config",
        default=None,
        help="Override the selected backend configuration YAML; capability 1 ignores HTTP overrides and falls back locally"
    )
    ap.add_argument(
        "--domains",
        nargs="+",
        default=None,
        help="Optional list of domain names to evaluate (without .json extension). "
             "If omitted, all domains are evaluated."
    )
    ap.add_argument(
        "--policy-judge-path",
        default=None,
        help="Optional path to a PolicyJudge Python file. If omitted, policy judging is disabled.",
    )
    ap.add_argument("--tool-backend", choices=("local", "code-engine"), default="local",
                    help="Tool environment. Code Engine supports capabilities 2–4; capability 1 logs a fallback to local.")
    ap.add_argument("--num-domains", "--num_domains", dest="num_domains", type=int, default=20,
                    help="Maximum simultaneous Code Engine domains for capabilities 2–4 (default: 20). Local execution stays sequential.")
    ap.add_argument("--log-dir", type=Path, default=None,
                    help="Parallel domain logs (default: <output_stem>_logs/<run_id>/)")
    args = ap.parse_args()
    if args.num_domains < 1:
        ap.error("--num-domains must be a positive integer")
    if args.k < 1:
        ap.error("--k must be a positive integer")

    capability_name = args.capability_name
    gt_dir = Path(args.gt_root)
    pred_dir = Path(args.pred_root)
    selected_domains = set(args.domains) if args.domains else None

    if not gt_dir.exists():
        raise SystemExit(f"groundtruth_dir does not exist: {gt_dir}")
    if not pred_dir.exists():
        raise SystemExit(f"prediction_dir does not exist: {pred_dir}")

    # Default output location: sibling of groundtruth/prediction under capability root
    if args.output:
        out_path = Path(args.output)
        # If output is a directory, append results.json
        if out_path.is_dir():
            out_path = out_path / "results.json"
    else:
        # assume structure: capability_name/groundtruth and capability_name/prediction
        capability_root = gt_dir.parent
        out_path = capability_root / "results.json"

    if capability_name not in CAPABILITY_MCP_TOOL_MAP:
        ap.error(f"Unknown capability: {capability_name}")
    try:
        mcp_config = select_evaluator_mcp_config(
            CAPABILITY_MCP_TOOL_MAP[capability_name], args.tool_backend, args.mcp_config
        )
    except (ValueError, OSError) as exc:
        ap.error(str(exc))

    if capability_name == "capability_bi_apis" and args.tool_backend == "code-engine" and args.num_domains > 1:
        logging.getLogger(__name__).warning("Capability 1 uses local sequential execution; --num-domains is ignored.")
    registry = build_default_capability_registry(k=args.k)
    results = evaluate_capability(
        capability_name=capability_name,
        gt_dir=gt_dir,
        pred_dir=pred_dir,
        out_path=out_path,
        registry=registry,
        mcp_config=mcp_config,
        selected_domains=selected_domains,
        tool_backend=mcp_config.backend,
        num_domains=args.num_domains,
        log_dir=args.log_dir,
        policy_judge_path=args.policy_judge_path,
    )

    print(f"Wrote: {out_path}")
    if isinstance(results, dict) and results.get("execution", {}).get("status") == "partial":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
