import unittest
import replay


class ReplayChecks(unittest.TestCase):
    def test_error_and_valid_zero_are_distinct(self):
        rows = replay.parse(replay.fixture([('a', 0, 0, 'valid'), ('b', 0, 0, 'Judge Error timeout')]))
        self.assertEqual(rows[('synthetic', 'demo', 'a')].status, 'scored')
        self.assertEqual(rows[('synthetic', 'demo', 'b')].status, 'groundedness_error')

    def test_zero_policy_distinguishes_judgment_and_denominator_coverage(self):
        data = replay.parse(replay.fixture([('a', 1, 1, 'yes'), ('b', 0, 0, 'no')]))
        report = replay.replay({'x': data}, 1, 0, 0, 71, 2)
        zero = report['systems']['x']['policies']['zero']
        abstain = report['systems']['x']['policies']['abstain']
        self.assertEqual(zero['mean_score'], 0)
        self.assertEqual(zero['mean_score_denominator_coverage'], 1)
        self.assertEqual(zero['mean_successful_judgment_coverage'], 0)
        self.assertIsNone(abstain['mean_score'])
        self.assertEqual(abstain['trials_with_score'], 0)

    def test_retry_limits_and_known_recovery(self):
        data = replay.parse(replay.fixture([('a', 1, 1, 'yes'), ('b', 1, 1, 'yes')]))
        r = replay.replay({'x': data}, 1, 1, 1, 7, 1)['systems']['x']
        self.assertEqual(r['retry_attempted_per_trial'], 1)
        self.assertEqual(r['policies']['retry_then_abstain']['mean_successful_judgment_coverage'], .5)
        self.assertEqual(r['policies']['retry_then_abstain']['mean_score'], 1)

    def test_known_errors_excluded_from_shared_reference(self):
        report = replay.replay(replay.demo(), 0, 1, .5, 71, 2)
        self.assertEqual(report['common_dialogues'], 5)
        self.assertEqual(report['matched_reference_dialogues'], 4)
        self.assertEqual(report['excluded_common_dialogues'], 1)

    def test_unmatched_and_duplicate(self):
        a = replay.parse(replay.fixture([('a', 1, 1, 'yes'), ('b', 0, 0, 'no')]))
        b = replay.parse(replay.fixture([('a', 1, 1, 'yes')]))
        out = replay.replay({'a': a, 'b': b}, 0, 0, 0, 0, 1)
        self.assertEqual(out['systems']['a']['unmatched_dialogues'], 1)
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            replay.parse(replay.fixture([('a', 1, 1, 'yes'), ('a', 0, 0, 'no')]))

    def test_sign_reversal_and_ties(self):
        self.assertEqual(replay.sign(-.1)*replay.sign(.1), -1)
        self.assertEqual(replay.sign(0), 0)
        a = replay.parse(replay.fixture([('a', 1, 1, 'yes')]))
        b = replay.parse(replay.fixture([('a', .5, None, None)]))
        result = replay.replay({'a': a, 'b': b}, 1, 0, 0, 1, 1)
        self.assertEqual(result['pairwise'][0]['policies']['zero']['strict_rank_reversals'], 1)
        self.assertEqual(result['pairwise'][0]['policies']['abstain']['trials_with_both_scores'], 0)

    def test_invalid_scores_and_repeat_determinism(self):
        for v in (True, float('nan'), -1, 2):
            with self.assertRaises(ValueError): replay.score_value(v)
        a = replay.replay(replay.demo(), .5, 1, .5, 91, 5)
        self.assertEqual(a, replay.replay(replay.demo(), .5, 1, .5, 91, 5))


if __name__ == '__main__':
    unittest.main()

class EvaluatorContractChecks(unittest.TestCase):
    def test_actual_dialogue_scorer_output_with_stub_judge(self):
        # The upstream scorer imports provider clients at module import time.
        # Substitute only that dependency; no provider or MCP call is made.
        import importlib
        import sys
        import types
        from pathlib import Path
        from unittest.mock import patch
        evaluator = str(Path(__file__).resolve().parents[2] / 'evaluator')
        fake_judge = types.ModuleType('judge')
        for name in ('CorrectnessJudge', 'GroundednessJudge', 'LLMJudge'):
            setattr(fake_judge, name, type(name, (), {}))
        fake_judge.JudgeValidationError = type('JudgeValidationError', (ValueError,), {})
        class StubTurn:
            def compare(self, **kwargs):
                return 0., {'groundedness_score': 0., 'score_explanation': {'groundedness': 'Judge Error timeout'}}
        with patch.dict(sys.modules, {'judge': fake_judge}):
            sys.path.insert(0, evaluator)
            try:
                sys.modules.pop('scorer', None)
                scorer = importlib.import_module('scorer')
                turn = {'turn_id': 1, 'query': 'q', 'answer': 'a', 'sequence': {'tool_call': [], 'tool_response': []}}
                score, details = scorer.DialogueScorer(StubTurn()).score({'output': [turn]}, {'output': [turn]})
                envelope = {'capability_name': 'c', 'domains': {'d': {'dialogues': [{'uuid': 'u', 'score': score, 'details': details}]}}}
                self.assertEqual(replay.parse(envelope)[('c','d','u')].status, 'groundedness_error')
            finally:
                sys.path.remove(evaluator)
                sys.modules.pop('scorer', None)
