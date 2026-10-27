"""Unit tests for evaluator/kev_judge.py against a fake POST /v1/systemone server."""

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "evaluator"))

from kev_judge import (  # noqa: E402
    KevCorrectnessJudge,
    KevExactMatchJudge,
    KevGroundednessJudge,
    fit_texts,
    truncate_middle,
)
from utils import JudgeInput, JudgeOutput  # noqa: E402


class FakeSystemOne:
    """Records requests; answers with `noul` or fails with the queued status codes first."""

    def __init__(self):
        self.requests = []
        self.noul = 0.9
        self.fail_with = []
        self.max_state_chars = None  # answer 422 "token limit" to longer states

        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fake.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
                if fake.max_state_chars and len(json.dumps(body["state"])) > fake.max_state_chars:
                    self.send_response(422)
                    self.end_headers()
                    self.wfile.write(b'{"detail": "state is over the 65536 token limit"}')
                    return
                if fake.fail_with:
                    self.send_response(fake.fail_with.pop(0))
                    self.end_headers()
                    self.wfile.write(b'{"detail": "nope"}')
                    return
                answers = {qid: {"type": "noul", "noul": fake.noul} for qid in body["questions"]}
                payload = json.dumps({"model": body["model"], "answers": answers}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture
def fake():
    f = FakeSystemOne()
    yield f
    f.server.shutdown()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ("JUDGE_BACKEND", "KEV_API_KEY", "KEV_END_POINT", "KEV_MODEL", "KEV_MAX_STATE_CHARS", "TYPESAFE_API_KEY"):
        monkeypatch.delenv(var, raising=False)


def _inp(**kw):
    base = dict(
        capability="capability_dashboard_apis",
        domain="hockey",
        query="How many goals did Wayne Gretzky score in 1982?",
        additional_instructions="",
        gt_answer="92",
        pred_answer="Wayne Gretzky scored 92 goals.",
        gt_tool_calls=[],
        pred_tool_calls=[],
        gt_tool_responses=["92"],
        pred_tool_responses=["92"],
    )
    base.update(kw)
    return JudgeInput(**base)


def _judge(cls, fake, **cfg):
    return cls(config={"end_point": fake.url, "max_retries": 2, **cfg})


def test_correctness_wire_format_and_score(fake):
    out = _judge(KevCorrectnessJudge, fake).judge(_inp())
    assert isinstance(out, JudgeOutput)
    assert out.score == 1.0
    assert out.explanation == "p(yes)=0.900 (kev:kev-latest)"

    req = fake.requests[0]
    assert req["path"] == "/v1/systemone"
    assert req["body"]["model"] == "kev-latest"
    assert req["body"]["state"] == {
        "question": "how many goals did wayne gretzky score in 1982?",
        "ground_truth_answer": "92",
        "prediction": "wayne gretzky scored 92 goals.",
    }
    q = req["body"]["questions"]["verdict"]
    assert q["type"] == "noul" and set(q["criteria"]) == {"true", "false"}
    assert "Authorization" not in req["headers"]


@pytest.mark.parametrize("p,expected", [(0.0, 0.0), (0.49, 0.0), (0.5, 1.0), (1.0, 1.0)])
def test_threshold_gives_binary_scores(fake, p, expected):
    fake.noul = p
    assert _judge(KevCorrectnessJudge, fake).judge(_inp()).score == expected


def test_custom_threshold(fake):
    fake.noul = 0.7
    assert _judge(KevCorrectnessJudge, fake, threshold=0.8).judge(_inp()).score == 0.0


def test_correctness_both_empty_skips_the_model(fake):
    out = _judge(KevCorrectnessJudge, fake).judge(_inp(gt_answer=" ", pred_answer=""))
    assert out == JudgeOutput(score=1.0, explanation="Both empty.")
    assert fake.requests == []


def test_groundedness_state_keeps_last_20_responses(fake):
    responses = [f"Row {i}" for i in range(25)]
    out = _judge(KevGroundednessJudge, fake).judge(_inp(pred_tool_responses=responses))
    assert out.score == 1.0
    state = fake.requests[0]["body"]["state"]
    assert state["tool_responses"] == [f"row {i}" for i in range(5, 25)]
    assert state["user_query"] == "how many goals did wayne gretzky score in 1982?"
    assert state["response"] == "wayne gretzky scored 92 goals."


def test_repeated_calls_are_identical(fake):
    judge = _judge(KevGroundednessJudge, fake)
    outs = {judge.judge(_inp()) for _ in range(5)}
    assert len(outs) == 1
    bodies = {json.dumps(r["body"], sort_keys=True) for r in fake.requests}
    assert len(bodies) == 1


def test_retries_then_succeeds(fake, monkeypatch):
    monkeypatch.setattr("kev_judge.time.sleep", lambda s: None)
    fake.fail_with = [503, 429]
    assert _judge(KevCorrectnessJudge, fake).judge(_inp()).score == 1.0
    assert len(fake.requests) == 3


def test_http_error_scores_zero(fake):
    fake.fail_with = [422]
    out = _judge(KevCorrectnessJudge, fake).judge(_inp())
    assert out.score == 0.0
    assert out.explanation.startswith("Judge Error HTTP 422")
    assert len(fake.requests) == 1


def test_unreachable_server_scores_zero(monkeypatch):
    monkeypatch.setattr("kev_judge.time.sleep", lambda s: None)
    judge = KevCorrectnessJudge(config={"end_point": "http://127.0.0.1:9", "max_retries": 0, "timeout": 2})
    out = judge.judge(_inp())
    assert out.score == 0.0 and out.explanation.startswith("Judge Error")


def test_env_config_and_auth(fake, monkeypatch):
    monkeypatch.setenv("KEV_END_POINT", fake.url)
    monkeypatch.setenv("KEV_MODEL", "kev-4b")
    monkeypatch.setenv("KEV_API_KEY", "s3cret")
    KevCorrectnessJudge(config={}).judge(_inp())
    req = fake.requests[0]
    assert req["body"]["model"] == "kev-4b"
    assert req["headers"]["Authorization"] == "Bearer s3cret"


def test_jev_backend(fake, monkeypatch):
    with pytest.raises(ValueError, match="TYPESAFE_API_KEY"):
        KevCorrectnessJudge(config={"backend": "jev"})
    monkeypatch.setenv("TYPESAFE_API_KEY", "ts-key")
    out = _judge(KevCorrectnessJudge, fake, backend="jev").judge(_inp())
    assert out.explanation.endswith("(jev:jev-latest)")
    req = fake.requests[0]
    assert req["body"]["model"] == "jev-latest"
    assert req["headers"]["Authorization"] == "Bearer ts-key"


def test_unknown_backend():
    with pytest.raises(ValueError, match="Unsupported"):
        KevCorrectnessJudge(config={"backend": "rits"})


def test_exact_match_needs_no_llm():
    judge = KevExactMatchJudge(config={})
    assert judge.llm is None
    assert judge.judge(_inp(gt_tool_responses=["92"], pred_tool_responses=["x", "92"])).score == 1.0
    assert judge.judge(_inp(gt_tool_responses=["93"], pred_tool_responses=["92"])).score == 0.0


def test_truncate_middle_keeps_head_and_tail():
    assert truncate_middle("abc", 10) == "abc"
    cut = truncate_middle("a" * 50 + "b" * 50, 40)
    assert len(cut) == 40 and cut.startswith("a") and cut.endswith("b") and "[truncated]" in cut


def test_fit_texts_keeps_short_texts_whole():
    texts = ["x" * 10, "y" * 1000, "z" * 500]
    fitted = fit_texts(texts, 400)
    assert fitted[0] == texts[0]
    assert sum(len(t) for t in fitted) <= 400
    assert len(fitted[1]) == len(fitted[2]) == 195
    assert fit_texts(texts, 10_000) == texts


def test_groundedness_cuts_long_tool_responses(fake):
    responses = ["short", "a" * 5000 + "b" * 5000]
    out = _judge(KevGroundednessJudge, fake, max_state_chars=1000).judge(_inp(pred_tool_responses=responses))
    assert out.score == 1.0
    assert out.explanation == "p(yes)=0.900 (kev:kev-latest); long fields cut to 1000 of 10035 chars"
    sent = fake.requests[0]["body"]["state"]["tool_responses"]
    assert sent[0] == "short" and sent[1].startswith("a") and sent[1].endswith("b")


def test_groundedness_halves_budget_when_state_is_too_long(fake):
    fake.max_state_chars = 3000
    judge = _judge(KevGroundednessJudge, fake, max_state_chars=10_000)
    out = judge.judge(_inp(pred_tool_responses=["r" * 20_000]))
    assert out.score == 1.0
    assert [len(r["body"]["state"]["tool_responses"][0]) for r in fake.requests] == [9970, 4970, 2470]


def test_groundedness_gives_up_after_three_too_long_states(fake):
    fake.max_state_chars = 10
    out = _judge(KevGroundednessJudge, fake).judge(_inp(pred_tool_responses=["r" * 1000]))
    assert out.score == 0.0 and "token limit" in out.explanation
    assert len(fake.requests) == 3


def test_correctness_cuts_long_answers(fake):
    gt = ", ".join(str(i) for i in range(5000))
    out = _judge(KevCorrectnessJudge, fake, max_state_chars=2000).judge(_inp(gt_answer=gt, pred_answer=gt))
    assert out.score == 1.0 and "long fields cut to 2000" in out.explanation
    state = fake.requests[0]["body"]["state"]
    assert len(state["ground_truth_answer"]) == len(state["prediction"]) == 1000
    assert state["ground_truth_answer"].startswith("0, 1, 2") and state["ground_truth_answer"].endswith("4999")


def test_max_state_chars_zero_sends_everything(fake, monkeypatch):
    monkeypatch.setenv("KEV_MAX_STATE_CHARS", "0")
    out = _judge(KevGroundednessJudge, fake).judge(_inp(pred_tool_responses=["r" * 200_000]))
    assert "cut" not in out.explanation
    assert len(fake.requests[0]["body"]["state"]["tool_responses"][0]) == 200_000
