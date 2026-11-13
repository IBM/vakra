"""Repeated groundedness judgments preserve first-trial accuracy."""
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evaluator"))
from judge import GroundednessJudge, LLMJudge
from utils import JudgeInput, JudgeOutput
from scorer import TurnScorer, TurnScorerConfig
from evaluator import _pass_metrics, _write_intermediate_results, evaluate_capability, CapabilityPolicy


@pytest.fixture
def inp():
    return JudgeInput("capability_dashboard_apis", "test", "query", "", "answer", "answer", [], [], [], [])


def make_judge(monkeypatch, responses):
    monkeypatch.setattr(LLMJudge, "_build_llm", lambda *args: None)
    judge = GroundednessJudge(k=len(responses))
    judge.invoke = Mock(side_effect=responses)
    return judge


@pytest.mark.parametrize("responses,first,any_pass,all_pass", [
    (["yes\nok"], 1, 1, 1),
    (["no\nfail", "yes\nok", "yes\nok"], 0, 1, 0),
    (["yes", "yes", "yes"], 1, 1, 1),
    (["no", "partial", "unsure"], 0, 0, 0),
    ([RuntimeError("offline"), "yes", "invalid"], 0, 1, 0),
])
def test_judgments(monkeypatch, inp, responses, first, any_pass, all_pass):
    judge = make_judge(monkeypatch, responses)
    result = judge.judge(inp)
    assert (result.score, result.pass_at_k, result.pass_power_k) == (first, any_pass, all_pass)
    assert len(result.trials) == len(responses)
    assert judge.invoke.call_count == len(responses)


@pytest.mark.parametrize("k", [0, -1, True, 1.5])
def test_invalid_k(k):
    with pytest.raises(ValueError, match="positive integer"):
        GroundednessJudge(k=k)


@pytest.mark.parametrize("exact,correct,expected_calls", [(1, 1, 0), (0, 1, 1), (0, 0, 1)])
def test_scoring_gates(monkeypatch, inp, exact, correct, expected_calls):
    groundedness = make_judge(monkeypatch, ["no", "yes", "yes"])
    correctness = Mock()
    correctness.judge.return_value = JudgeOutput(correct, "correctness")
    exactmatch = Mock()
    exactmatch.judge.return_value = JudgeOutput(exact, "exact")
    scorer = TurnScorer(TurnScorerConfig(inp.capability, inp.domain), correctness, groundedness, exactmatch)
    score, details = scorer.compare(inp.query, "", inp.gt_answer, inp.pred_answer, [], [], [], [])
    assert score == 0
    assert correctness.judge.call_count == expected_calls
    assert exactmatch.judge.call_count == 1
    assert groundedness.invoke.call_count == (3 if exact or correct else 0)
    if exact or correct:
        assert details["groundedness_pass_at_k"] == 1
        assert details["groundedness_pass_power_k"] == 0
        assert len(details["groundedness_trials"]) == 3


def test_summary_and_resume(tmp_path):
    dialogues = [{"score": 0, "details": {"pass_at_k": 1, "pass_power_k": 0}}]
    assert _pass_metrics(dialogues, 2) == {"pass_at_k": .5, "pass_power_k": 0}
    results = {"groundedness_k": 3, "metrics_version": 1, "domains": {"test": {
        "n_paired": 1, "missing_prediction_uuids": ["missing"], "extra_prediction_uuids": [],
        "summary": {"num_samples": 2}, "dialogues": dialogues,
    }}}
    output = tmp_path / "results.json"
    _write_intermediate_results(results, [0], output)
    assert results["summary"]["mean_dialogue_score"] == 0
    assert results["summary"]["pass_at_k"] == .5
    (tmp_path / "test.json").write_text("[]")
    policy = CapabilityPolicy(correctness_judge=None, groundedness_judge=Mock(k=2), exactmatch_judge=None)
    with pytest.raises(ValueError, match="different K"):
        evaluate_capability("test", tmp_path, tmp_path, output, {"test": policy})


@pytest.mark.parametrize("special", [False, True])
def test_dialogue_metrics_and_bypass(monkeypatch, special):
    from scorer import DialogueScorer
    groundedness = make_judge(monkeypatch, ["no", "yes", "yes"])
    exact = Mock()
    exact.judge.return_value = JudgeOutput(1, "match")
    scorer = DialogueScorer(TurnScorer(
        TurnScorerConfig("capability_multiturn", "test"), Mock(), groundedness, exact,
    ))
    turn = {"turn_id": 0, "query": "query", "answer": "i can not answer" if special else "answer",
            "sequence": {"tool_call": [], "tool_response": []}}
    score, details = scorer.score({"output": [turn]}, {"output": [turn]})
    assert score == float(special)
    assert details["pass_at_k"] == 1
    assert details["pass_power_k"] == float(special)
    assert groundedness.invoke.call_count == (0 if special else 3)


def test_correctness_error_includes_exception(monkeypatch, inp):
    from judge import CorrectnessJudge
    monkeypatch.setattr(LLMJudge, "_build_llm", lambda *args: None)
    judge = CorrectnessJudge()
    judge.invoke = Mock(side_effect=RuntimeError("provider unavailable"))
    result = judge.judge(inp)
    assert result.score == 0.0
    assert result.explanation == "Judge Error provider unavailable."
