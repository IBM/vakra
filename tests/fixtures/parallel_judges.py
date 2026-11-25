"""Deterministic, importable judge doubles; each spawned domain constructs its own."""
import os
import time
from utils import JudgeOutput, GroundednessOutput
from judge import GroundednessJudge


class ErrorGroundedness(GroundednessJudge):
    """One provider failure in each group of three trials; no network client."""
    def _build_llm(self, config):
        return None

    def invoke(self, prompt):
        self.calls = getattr(self, 'calls', 0) + 1
        if self.calls % 3 == 1:
            raise RuntimeError('judge unavailable')
        return 'yes'


class FixedJudge:
    def __init__(self, config=None):
        self.model_config = config or {}
        self.llm = object()  # Live clients must not be passed through the job.
        print(f'JUDGE_CREATED {type(self).__name__} pid={os.getpid()}', flush=True)
    def judge(self, inp):
        return JudgeOutput(score=1.0, explanation='fixed')


class FixedGroundedness(FixedJudge):
    def __init__(self, config=None, k=3):
        super().__init__(config)
        self.k = k
    def judge(self, inp):
        time.sleep(self.model_config.get('delay', 0))
        scores = self.model_config.get('trial_scores', [1.0] * self.k)
        assert len(scores) == self.k
        return GroundednessOutput(score=scores[0], explanation='fixed',
                                  pass_at_k=float(any(scores)), pass_power_k=float(all(scores)),
                                  trials=tuple(JudgeOutput(score, 'fixed') for score in scores))
