"""Unit tests for CorrectnessJudge response parsing (no LLM call)."""

import sys
from pathlib import Path

import pytest

pytest.importorskip("langchain_openai")

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "evaluator"))

from judge import CorrectnessJudge  # noqa: E402
from utils import JudgeInput  # noqa: E402


@pytest.fixture
def judge():
    # Skip __init__, which builds an LLM client.
    return CorrectnessJudge.__new__(CorrectnessJudge)


@pytest.mark.parametrize(
    "text",
    [
        '{"explanation": "ok", "score": 1}',
        'Here is my grading: {"explanation": "ok", "score": 1}',
        '```json\n{"explanation": "ok", "score": 1}\n```',
        '{"explanation": "ok", "score": 1}\nThe prediction matches.',
    ],
)
def test_parse_response_finds_json(judge, text):
    assert judge._parse_response(text) == {"explanation": "ok", "score": 1}


def test_judge_scores_wrapped_json(judge, monkeypatch):
    monkeypatch.setattr(
        judge,
        "invoke",
        lambda prompt: 'Grading: {"explanation": "same answer", "score": 1}',
    )
    inp = JudgeInput(
        capability="",
        domain="",
        query="q",
        additional_instructions="",
        gt_answer="42",
        pred_answer="42",
        gt_tool_calls=[],
        pred_tool_calls=[],
        gt_tool_responses=[],
        pred_tool_responses=[],
    )
    out = judge.judge(inp)
    assert out.score == 1
    assert out.explanation == "same answer"
