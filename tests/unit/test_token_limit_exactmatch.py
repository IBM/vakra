"""Exact-match fallback resolves token-limit failures without hiding other errors."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
from openai import BadRequestError
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'evaluator'))
from evaluation_errors import llm_error_context, build_error_report
from judge import LLMJudge, GroundednessJudge, ExactMatchJudge
from scorer import TurnScorer, TurnScorerConfig, DialogueScorer
from utils import JudgeOutput
import evaluator as entry

MESSAGE = 'max_tokens must be at least 1, got -19171.'


def token_error():
    return BadRequestError('Error code: 400 - '+MESSAGE,
        response=httpx.Response(400, request=httpx.Request('POST', 'https://test.invalid')),
        body={'error': {'message': MESSAGE, 'code': 400}})


@pytest.fixture
def make(monkeypatch):
    monkeypatch.setattr(LLMJudge, '_build_llm', lambda *args: None)
    def create(responses, exact=1):
        groundedness = GroundednessJudge(k=len(responses))
        groundedness.invoke = Mock(side_effect=responses)
        exactmatch = ExactMatchJudge() if exact == 1 else SimpleNamespace(judge=lambda inp: JudgeOutput(0, 'different'))
        correct = SimpleNamespace(judge=lambda inp: JudgeOutput(1, 'correct'))
        scorer = TurnScorer(TurnScorerConfig('capability_dashboard_apis', 'alpha'), correct,
                            groundedness, exactmatch)
        dialogue = {'output': [{'turn_id': 0, 'query': 'query', 'answer': '42',
            'sequence': {'tool_call': [{'name': 'echo', 'arguments': {}}], 'tool_response': ['42']}}]}
        return scorer, DialogueScorer(scorer), dialogue
    return create


@pytest.mark.parametrize('k', [1, 3])
def test_exactmatch_token_failures_pass_and_are_recorded_as_resolved(make, k):
    turn_scorer, scorer, dialogue = make([token_error() for _ in range(k)])
    errors = []
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        score, details = scorer.score(dialogue, dialogue)
    assert score == details['dialogue_score'] == 1
    assert details['pass_at_k'] == details['pass_power_k'] == 1
    metadata = details['per_turn'][0]['metadata']
    assert metadata['exactmatch_score'] == 1
    assert metadata['score_resolution'] == 'exactmatch_token_limit'
    assert metadata['original_groundedness_score'] == 0
    assert metadata['groundedness_score'] == 1
    assert len(metadata['groundedness_trials']) == k
    assert all(t['score'] == 0 and MESSAGE in t['explanation'] for t in metadata['groundedness_trials'])
    assert len(errors) == k
    assert all(e['resolved'] and e['resolution'] == 'exactmatch_token_limit' for e in errors)
    report = build_error_report({'evaluation_errors': {'alpha': errors}}, Path('results.json'))
    counts = report['summary']
    assert counts['resolved_llm_errors'] == 1
    assert counts['resolved_llm_call_attempts'] == k
    assert counts['failed_llm_call_attempts'] == counts['error_count'] == k
    assert counts['llm_call_errors'] == counts['samples_with_errors'] == counts['samples_with_llm_errors'] == 0
    assert counts['error_messages'] == []
    assert MESSAGE in counts['resolved_error_messages'][0]
    assert list(counts)[-1] == 'error_messages'


def test_no_fallback_without_exactmatch(make):
    turn_scorer, scorer, dialogue = make([token_error()] * 3, exact=0)
    errors = []
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        score, details = scorer.score(dialogue, dialogue)
    assert score == 0
    assert not any(e.get('resolved') for e in errors)
    counts = build_error_report({'evaluation_errors': {'alpha': errors}}, Path('results.json'))['summary']
    assert counts['samples_with_errors'] == counts['llm_call_errors'] == 1
    assert counts['resolved_llm_errors'] == 0


@pytest.mark.parametrize('responses', [
    [RuntimeError('provider offline')] * 3,
    ['no'] * 3,
    ['no\nThe response quotes max_tokens must be at least 1.'] * 3,
])
def test_other_errors_and_real_negative_judgments_are_not_overridden(make, responses):
    turn_scorer, scorer, dialogue = make(responses)
    errors = []
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        score, details = scorer.score(dialogue, dialogue)
    assert score == 0
    assert 'score_resolution' not in details['per_turn'][0]['metadata']
    assert not any(e.get('resolved') for e in errors)


def test_unrelated_errors_keep_the_sample_in_error_counts(make):
    turn_scorer, scorer, dialogue = make([token_error(), RuntimeError('rate limited'), 'no'])
    errors = []
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        score, details = scorer.score(dialogue, dialogue)
    assert score == 1
    assert errors[0]['resolved'] is True
    assert not errors[1].get('resolved', False)
    counts = build_error_report({'evaluation_errors': {'alpha': errors}}, Path('results.json'))['summary']
    assert counts['samples_with_errors'] == counts['samples_with_llm_errors'] == 1
    assert counts['llm_call_errors'] == 1
    assert counts['resolved_llm_errors'] == 0  # This judge group still has an unresolved failure.
    assert counts['resolved_llm_call_attempts'] == 1
    assert counts['failed_llm_call_attempts'] == 2


def test_gt_error_is_not_hidden_by_resolved_llm_errors(make):
    turn_scorer, scorer, dialogue = make([token_error()] * 3)
    errors = [{'type': 'ground_truth_tool_call', 'domain': 'alpha', 'uuid': 'sample',
               'message': 'tool failed', 'recovered': False, 'failed_attempt_count': 4}]
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        scorer.score(dialogue, dialogue)
    counts = build_error_report({'evaluation_errors': {'alpha': errors}}, Path('results.json'))['summary']
    assert counts['samples_with_errors'] == counts['samples_with_ground_truth_errors'] == 1
    assert counts['samples_with_llm_errors'] == 0
    assert counts['resolved_llm_errors'] == 1


def test_resolution_does_not_leak_to_another_sample_or_judge(make):
    turn_scorer, scorer, dialogue = make([token_error()] * 3)
    errors = [{'type': 'llm_call', 'domain': 'alpha', 'uuid': uuid, 'judge': judge,
               'message': MESSAGE} for uuid, judge in [('other', 'GroundednessJudge'), ('sample', 'CorrectnessJudge')]]
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        scorer.score(dialogue, dialogue)
    assert not any(e.get('resolved') for e in errors[:2])
    assert all(e.get('resolved') for e in errors[2:])


def test_old_scoring_checkpoints_are_rejected(tmp_path):
    (tmp_path / 'domain.json').write_text('[]')
    output = tmp_path / 'results.txt'
    output.write_text(json.dumps({'domains': {}, 'groundedness_k': 3, 'metrics_version': 1}))
    policy = SimpleNamespace(groundedness_judge=SimpleNamespace(k=3))
    with pytest.raises(ValueError, match='new --output'):
        entry.evaluate_capability('capability_dashboard_apis', tmp_path, tmp_path, output,
                                  {'capability_dashboard_apis': policy})


def test_policy_failure_does_not_get_overridden(make):
    turn_scorer, scorer, dialogue = make([token_error()] * 3)
    turn_scorer.cfg = TurnScorerConfig('capability_multiturn', 'alpha')
    turn_scorer.policy_judge = SimpleNamespace(judge=lambda inp: JudgeOutput(0, 'policy failed'))
    errors = []
    with llm_error_context(errors, domain='alpha', uuid='sample'):
        score, details = scorer.score(dialogue, dialogue)
    assert score == 0
    assert details['per_turn'][0]['metadata']['exactmatch_score'] is None
    turn_scorer.groundedness_judge.invoke.assert_not_called()
    assert errors == []
