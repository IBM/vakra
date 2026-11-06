"""
Kev / Jev judges: drop-in replacements for CorrectnessJudge, GroundednessJudge and ExactMatchJudge.

Instead of asking an autoregressive LLM to write a verdict and parsing it, each judge sends a structured
state and one yes/no ("noul") question to a System One decision model and thresholds the returned
probability. The model reads its answer off one forward pass and generates no text, so scores barely
change between runs. List answers are compared value by value in code first (list_check.py).

Backends (both speak POST /v1/systemone, https://docs.typesafe.ai/api):
  - "kev": a self-hosted Kev server (https://github.com/jaredpalmer/kev), e.g.
        uv run --extra serve python -m kev.serve --run jaredpalmer/kev-4b --port 8009
  - "jev": TypeSafe's hosted Jev (needs TYPESAFE_API_KEY).

Select with JUDGE_BACKEND=kev or JUDGE_BACKEND=jev when running evaluator.py.
Inspired by https://github.com/danielgshea/jev-as-a-judge.
"""
from __future__ import annotations

import http.client
import json
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any, Dict

from constant import N_TOOL_CALLS_PER_TURN
from judge import ExactMatchJudge
from list_check import check_grounded, compare_answers
from utils import JudgeInput, JudgeOutput

_BACKENDS = {
    # backend: (default end point, default model, api key env var)
    "kev": ("http://127.0.0.1:8009", "kev-latest", "KEV_API_KEY"),
    "jev": ("https://api.typesafe.ai", "jev-latest", "TYPESAFE_API_KEY"),
}

_RETRY_STATUS = {429, 500, 502, 503, 504}
_TRUNCATED = " ... [truncated] ... "


class StateTooLongError(RuntimeError):
    """The server refused the state as over its token limit (HTTP 422)."""


def truncate_middle(text: str, limit: int) -> str:
    """Keep the head and tail of `text`, at most `limit` characters in all."""
    if len(text) <= limit:
        return text
    if limit <= len(_TRUNCATED):
        return text[:limit]
    head = (limit - len(_TRUNCATED) + 1) // 2
    tail = limit - len(_TRUNCATED) - head
    return text[:head] + _TRUNCATED + (text[-tail:] if tail else "")


def fit_texts(texts: list, budget: int) -> list:
    """
    Shorten `texts` to `budget` characters in all. Short texts are kept whole; the long ones share what is left
    equally and lose their middle (tool output usually puts the schema first and the last rows at the end).
    """
    if sum(len(t) for t in texts) <= budget:
        return list(texts)
    cap, left = 0, budget
    for i, n in enumerate(sorted(len(t) for t in texts)):
        cap = left // (len(texts) - i)
        if n > cap:
            break
        left -= n
    return [truncate_middle(t, cap) for t in texts]


class SystemOneJudge:
    """
    Base class for judges backed by a System One decision model (Kev or Jev).

    config keys (all optional; env vars are the fallback):
      backend      "kev" | "jev"            (JUDGE_BACKEND, default "kev")
      end_point    server base URL         (KEV_END_POINT / JEV_END_POINT)
      model_name   model to request        (KEV_MODEL / JEV_MODEL)
      threshold    p(yes) needed for 1.0   (default 0.5)
      timeout      seconds per request     (default 120)
      max_retries  retries on 429/5xx, timeouts and dropped connections  (default 3)
      max_state_chars  characters sent per call (KEV_MAX_STATE_CHARS, default 100000; 0 means no limit). Tool
                       responses lose their middle to fit; answers are never cut, an answer that does not fit
                       scores 0 as too long to judge
    """

    question: Dict[str, Any] = {}

    _ws = re.compile(r"\s+")

    def __init__(self, config: dict = {}):
        self.model_config = config
        backend = (config.get("backend") or os.getenv("JUDGE_BACKEND") or "kev").lower()
        if backend not in _BACKENDS:
            raise ValueError(f"Unsupported System One judge backend: {backend!r}. Expected 'kev' or 'jev'.")
        default_end_point, default_model, key_env = _BACKENDS[backend]
        prefix = backend.upper()
        self.backend = backend
        self.end_point = (config.get("end_point") or os.getenv(f"{prefix}_END_POINT") or default_end_point).rstrip("/")
        self.model_name = config.get("model_name") or os.getenv(f"{prefix}_MODEL") or default_model
        self.api_key = os.getenv(key_env)
        if backend == "jev" and not self.api_key:
            raise ValueError("TYPESAFE_API_KEY is required for the jev judge backend")
        self.threshold = float(config.get("threshold", 0.5))
        self.timeout = float(config.get("timeout", 120))
        self.max_retries = int(config.get("max_retries", 3))
        self.max_state_chars = int(config.get("max_state_chars", os.getenv("KEV_MAX_STATE_CHARS", 100_000)))

    def _norm(self, s: str) -> str:
        return self._ws.sub(" ", str(s).strip().lower())

    def _post(self, body: dict) -> dict:
        data = json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        url = self.end_point + "/v1/systemone"
        for attempt in range(self.max_retries + 1):
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", errors="replace")
                if e.code == 422 and "token" in detail.lower():
                    raise StateTooLongError(f"HTTP 422 from {url}: {detail[:500]}") from e
                if e.code not in _RETRY_STATUS or attempt == self.max_retries:
                    raise RuntimeError(f"HTTP {e.code} from {url}: {detail[:500]}") from e
            except (OSError, http.client.HTTPException) as e:
                if attempt == self.max_retries:
                    raise RuntimeError(f"Could not reach {url}: {e}") from e
            time.sleep(min(2 ** attempt, 30))

    def probability(self, state: dict) -> float:
        """p(yes) for this judge's question about `state`."""
        body = {"model": self.model_name, "state": state, "questions": {"verdict": self.question}}
        answer = self._post(body)["answers"]["verdict"]
        return float(answer["noul"])

    def output(self, p: float, note: str = "") -> JudgeOutput:
        score = 1.0 if p >= self.threshold else 0.0
        return JudgeOutput(score=score, explanation=f"p(yes)={p:.3f} ({self.backend}:{self.model_name}){note}")

    def decide(self, state: dict) -> JudgeOutput:
        try:
            return self.output(self.probability(state))
        except Exception as e:
            return JudgeOutput(score=0.0, explanation=f"Judge Error {e}.")

    def judge(self, inp: JudgeInput) -> JudgeOutput:
        raise NotImplementedError


class KevCorrectnessJudge(SystemOneJudge):
    """
    Does the predicted answer match the ground truth answer for the query?
    Same rules as CorrectnessPrompt (CRAG), asked as one yes/no question.
    """

    question = {
        "type": "noul",
        "instructions": (
            "Does the prediction correctly answer the question, matching the ground_truth_answer? "
            "Treat the ground_truth_answer as always correct. The prediction matches if it states the same answer, "
            "exactly or as a concise and correct summary. If the ground truth is a number, the prediction must "
            "contain exactly that number. If the ground truth is a set of items, the prediction must contain exactly "
            "the same items. The prediction does not match if it says it is unsure, contradicts itself, does not "
            "answer the question, or gives a different answer."
        ),
        "criteria": {
            "true": "The prediction gives the same answer as the ground truth",
            "false": "The prediction is wrong, incomplete, unsure, self-contradictory or does not answer the question",
        },
    }

    def judge(self, inp: JudgeInput) -> JudgeOutput:
        query = self._norm(inp.query)
        gt = self._norm(inp.gt_answer)
        pr = self._norm(inp.pred_answer)
        if not gt and not pr:
            return JudgeOutput(score=1.0, explanation="Both empty.")
        checked = compare_answers(inp.gt_answer, inp.pred_answer, inp.gt_tool_responses)
        if checked:
            return JudgeOutput(score=checked[0], explanation=checked[1])
        if self.max_state_chars and len(gt) + len(pr) > self.max_state_chars:
            return JudgeOutput(score=0.0, explanation=f"Judge Error too long to judge: {len(gt) + len(pr)} chars.")
        return self.decide({"question": query, "ground_truth_answer": gt, "prediction": pr})


class KevGroundednessJudge(SystemOneJudge):
    """
    Is the predicted answer grounded in the query and the turn's tool responses?
    Same rules as GroundednessPrompt; "partial" and "unsure" count as not grounded, as in GroundednessJudge.
    """

    question = {
        "type": "noul",
        "instructions": (
            "Is every substantial claim in the response supported by the user_query and the tool_responses? "
            "Ignore greetings and self-descriptions. A claim is supported if it rephrases information in the "
            "tool_responses or follows from it by trivial common-sense reasoning. A claim that the information is "
            "not available is supported only if the tool_responses indeed lack it. If any claim needs information "
            "or non-trivial reasoning that is not in the tool_responses, the response is not grounded. Containing "
            "keywords from the tool_responses is not enough."
        ),
        "criteria": {
            "true": "Every substantial claim is supported by the tool responses",
            "false": "At least one claim is unsupported, invented or needs non-trivial outside knowledge",
        },
    }

    def judge(self, inp: JudgeInput) -> JudgeOutput:
        responses = inp.pred_tool_responses[-N_TOOL_CALLS_PER_TURN:]
        checked = check_grounded(inp.pred_answer, responses, inp.query, inp.gt_answer)
        if checked:
            return JudgeOutput(score=checked[0], explanation=checked[1])
        return self.decide_cutting_tools(
            self._norm(inp.query), [self._norm(str(t)) for t in responses], self._norm(inp.pred_answer)
        )

    def decide_cutting_tools(self, query: str, responses: list, response: str) -> JudgeOutput:
        """
        decide() with the tool responses cut to what the answer leaves of max_state_chars. If the server still finds
        the state too long, the limit is halved and the call retried, twice at most.
        """
        tool_chars = sum(len(t) for t in responses)
        total = tool_chars + len(response)
        budget = self.max_state_chars or total
        error = None
        for attempt in range(3):
            if budget < len(response):
                return JudgeOutput(score=0.0, explanation=f"Judge Error too long to judge: {len(response)} chars.")
            fitted = fit_texts(responses, budget - len(response))
            try:
                p = self.probability({"user_query": query, "tool_responses": fitted, "response": response})
            except StateTooLongError as e:
                error, budget = e, min(budget, total) // 2
                continue
            except Exception as e:
                return JudgeOutput(score=0.0, explanation=f"Judge Error {e}.")
            n = sum(len(t) for t in fitted)
            return self.output(p, f"; tool responses cut to {n} of {tool_chars} chars" if n < tool_chars else "")
        return JudgeOutput(score=0.0, explanation=f"Judge Error {error}.")


class KevExactMatchJudge(ExactMatchJudge):
    """ExactMatchJudge without building an LLM client (it never calls one)."""

    def __init__(self, config: dict = {}):
        self.model_config = config
        self.llm = None
