"""Unit tests for evaluator/list_check.py. Most inputs are shapes seen in real VAKRA answers and tool output."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "evaluator"))

from list_check import Evidence, canon, check_grounded, compare_answers, same_values, split_items, unsupported, values  # noqa: E402


def tool(**columns):
    return json.dumps(columns, indent=2)


TITLES = [f"Movie number {i}" for i in range(30)]


@pytest.mark.parametrize(
    "a,b",
    [
        ("40000", "40000.0"),
        ("1,234", "1234"),
        ("$12.50", "12.5"),
        ("12.5%", "12.5"),
        ("None", None),
        ("null", "NULL"),
        ("MASH", "M*A*S*H"),
        ("Purée of peas", '"PURÉE OF PEAS"'),
        ("bande &agrave; part", "Bande à Part"),
        ("2016-05-01 00:27:30", "2016-05-01T00:27:30"),
        ("1991-06-15 00:00:00", "1991-06-15 00:00:00.0"),
        ("1994-09-14", "1994-09-14 00:00:00.0"),
        ("2016-05-01 21:57:13", "2016-05-01 21:57:13.0"),
        ("FHR 5-Tier 2", "FHR 5-Tier 2.0"),
        ("57.142857142857146", "57.14285714"),
    ],
)
def test_canon_treats_as_equal(a, b):
    assert canon(a) == canon(b)


@pytest.mark.parametrize(
    "a,b",
    [
        ("7311663864768030840", "7311663864768030841"),
        ("3325", "13325"),
        ("quality 2", "quality 2.5"),
        ("Mission:  - Ranked", "Mission: Impossible - Ranked"),
    ],
)
def test_canon_keeps_apart(a, b):
    assert canon(a) != canon(b)


@pytest.mark.parametrize(
    "text,items",
    [
        ('["a", "b, c", 3]', ["a", "b, c", "3"]),
        ("[[7820, 40.9], [7823, 40.8]]", ["7820", "40.9", "7823", "40.8"]),
        ("[16, 17, 18, 1199", ["16", "17", "18", "1199"]),
        ('["Alpha, ranked","Beta","Gamma","Delta","Germany', ["Alpha, ranked", "Beta", "Gamma", "Delta", "Germany"]),
        ("a, b\nc", ["a", "b", "c"]),
        ("- a\n- b\n3. c", ["a", "b", "c"]),
        ("a; b; c", ["a", "b", "c"]),
        ("C &amp; Y Cafe, Foo &#39;s bar", ["C &amp; Y Cafe", "Foo &#39;s bar"]),
        ("a, b, c ... (list continues)", ["a", "b", "c"]),
        ("a, b, Horserad ... (list continues for all remaining dish names)", ["a", "b", "Horserad"]),
        ("a, b, c (Note: The answer was truncated due to length)", ["a", "b", "c"]),
        ('[[\\u0022a\\u0022], [\\u0022b\\u0022]]', ["a", "b"]),
        ("7,158.12", ["7,158.12"]),
        ("", []),
    ],
)
def test_split_items(text, items):
    assert split_items(text) == items


def test_evidence_matches_whole_values_only():
    ev = Evidence([tool(titles=["when we get there it will still be now", "KIM KI-DUK, ranked"], ids=[13325, 16])])
    assert ev.found("when we get there it will still be now")
    assert not ev.found("when we get")
    assert not ev.found("3325") and ev.found("16") and ev.found("16.0")
    assert ev.found("KIM KI-DUK, ranked") and ev.found("ranked") and ev.found("KIM KI-DUK")


def test_evidence_splits_tool_values_as_answers_are_split():
    ev = Evidence([tool(text=["My lord, most humbly on my knee I beg\nThe leading of the vaward"])])
    assert ev.found("most humbly on my knee I beg") and ev.found("The leading of the vaward")


def test_evidence_finds_fields_of_one_row():
    ev = Evidence([tool(players=[{"first": "Antti", "last": "Aalto"}, {"first": "Peter", "last": "Ahola"}])])
    assert ev.found("Antti Aalto") and ev.found("Aalto Antti")
    assert not ev.found("Peter Aalto")


def test_evidence_reads_python_literals_and_plain_text():
    assert Evidence(["[['pollywog stew ep'], ['tool and die']]"]).found("tool and die")
    assert Evidence(["Sir Toby Belch enters in scene three."]).found("toby belch enters")


def test_unsupported_lets_a_long_answer_stop_short():
    titles = [f"A long title for movie number {i}" for i in range(60)]
    ev = Evidence([tool(titles=titles)])
    assert unsupported(titles[:59] + ["A long title for mo"], ev) == []
    assert unsupported(["A long title for mo"] + titles[:59], ev) == ["A long title for mo"]
    assert unsupported(["A long title for..."] + titles[:59], ev) == []


def test_unsupported_does_not_forgive_a_short_answer():
    ev = Evidence([tool(ids=[10, 158, 47, 40, 542], names=["Rutgers University at New Brunswick"])])
    assert unsupported(["Rutgers University at New"], ev) == ["Rutgers University at New"]
    assert unsupported(["10", "158", "47", "40", "5"], ev) == ["5"]


def test_same_values_counts_repeats_unless_each_is_listed_once():
    assert same_values(["a", "a", "b"], ["a", "b", "a"])
    assert same_values(["a", "a", "b"], ["a", "b"])
    assert not same_values(["a", "a", "a", "b"], ["a", "a", "b"])


def test_compare_passes_same_items_in_any_order_and_form():
    gt = ", ".join(TITLES)
    assert compare_answers(gt, gt)[0] == 1.0
    assert compare_answers(gt, json.dumps(list(reversed(TITLES))))[0] == 1.0


def test_compare_passes_identical_short_answers_without_a_model():
    assert compare_answers("652", "652.0")[0] == 1.0
    assert compare_answers("Burlison, Randolph", "Randolph, Burlison")[0] == 1.0


def test_compare_finds_one_wrong_item_in_the_middle_of_a_long_list():
    gt = [str(i) for i in range(100_000)]
    pred = list(gt)
    pred[50_000] = "x1"
    score, why = compare_answers(", ".join(gt), ", ".join(pred))
    assert score == 0.0 and "1 of 100000" in why


def test_compare_fails_an_answer_that_stops_early():
    gt = ", ".join(TITLES)
    assert compare_answers(gt, ", ".join(TITLES[:10]))[0] == 0.0


def test_compare_fails_a_list_that_dropped_only_repeats_part_way():
    gt = ["a"] * 10 + ["b"] * 10
    assert compare_answers(", ".join(gt), ", ".join(["a"] * 9 + ["b"] * 9))[0] == 0.0


def test_compare_passes_other_columns_of_the_ground_truth_rows():
    rows = [[f"id{i}", 40.0 + i, -70.0 - i] for i in range(10)]
    gt = ", ".join(r[0] for r in rows)
    assert compare_answers(gt, json.dumps(rows), [rows])[0] == 1.0


def test_compare_fails_extra_values_that_are_in_no_ground_truth_row():
    rows = [[f"id{i}", 40.0 + i] for i in range(10)]
    pred = [v for r in rows for v in r] + ["id99"]
    assert compare_answers(", ".join(r[0] for r in rows), ", ".join(map(str, pred)), [rows])[0] == 0.0


def test_compare_fails_values_of_different_rows_put_together():
    rows = [[f"id{i}", 40.0 + i] for i in range(10)]
    pred = [list(r) for r in rows]
    pred[3][1], pred[4][1] = pred[4][1], pred[3][1]
    score, why = compare_answers(", ".join(r[0] for r in rows), json.dumps(pred), [rows])
    assert score == 0.0 and "different ground truth rows" in why


@pytest.mark.parametrize(
    "gt,pred",
    [
        ("AUS, CHN, HKG, IDN, JPN, KOR", "Australia, China, Hong Kong, Indonesia, Japan, Korea"),
        ("BRN, GUM, HKG, MAC, MNP, NCL", "BRN (BN), GUM (GU), HKG (HK), MAC (MO), MNP (MP), NCL (NC)"),
        ("92", "Wayne Gretzky scored 92 goals."),
        ("a, b, c", "a, b, d"),
    ],
)
def test_compare_leaves_other_forms_to_the_model(gt, pred):
    assert compare_answers(gt, pred) is None


def test_compare_needs_ground_truth():
    assert compare_answers("", "a") is None


def test_compare_fails_an_empty_answer_unless_nothing_can_be_answered():
    assert compare_answers("a", "")[0] == 0.0 and compare_answers("a, b", "[]")[0] == 0.0
    assert compare_answers("I can not answer.", "") is None


@pytest.mark.parametrize(
    "gt,pred,score",
    [
        ("173", "61", 0.0),
        ("1105", "67", 0.0),
        ("25.916666666666668", "38.875", 0.0),
        ("25.916666666666668", "25.92", 1.0),
        ("25.916666666666668", "25.9", 1.0),
        ("2140.0", "2140", 1.0),
        ("100", "99.5", 0.0),
        ("65.46242774566474", "65.46242704566474", 0.0),
        ("7206704.0", "7206704.7", 0.0),
    ],
)
def test_compare_decides_two_clearly_read_numbers(gt, pred, score):
    assert compare_answers(gt, pred)[0] == score


@pytest.mark.parametrize(
    "gt,pred",
    [("0.302", "30.2"), ("102.12765957446808", "1.0212765957446808"), ("6'1\"", "73"), ("0.18197691557631693", "0"), ("76.83333333333333", "77")],
)
def test_compare_leaves_rescaled_or_heavily_rounded_numbers_to_the_model(gt, pred):
    assert compare_answers(gt, pred) is None


@pytest.mark.parametrize("gt,pred", [("Google Photos", "Google Photas"), ("Distr. Columbia", "Distr. Columuia"), ("101 Dalmatians, The Aristocats", "101 Dalmatians, The Aristocbts")])
def test_compare_fails_a_changed_word_in_a_short_answer(gt, pred):
    assert compare_answers(gt, pred)[0] == 0.0


@pytest.mark.parametrize("gt,pred", [("Studio Entertainment[NI 1]", "Studio Entertainment"), ("No", "false"), ("CDN, USA", "Canada, United States")])
def test_compare_leaves_shorter_or_other_forms_to_the_model(gt, pred):
    assert compare_answers(gt, pred) is None


def test_compare_fails_a_short_list_that_omits_most_of_the_ground_truth():
    gt = "SUFFOLK, HAMPDEN, HAMPSHIRE, WORCESTER, BERKSHIRE, ESSEX"
    assert compare_answers(gt, "SUFFOLK, HAMPDEN, HAMPSHIRE")[0] == 0.0


def test_compare_splits_one_value_holding_two_names():
    gt = "Charles Judels, Cliff Edwards; Eddie Carroll, Mel Blanc, Walter Catlett, Dickie Jones"
    pred = "Charles Judels, Cliff Edwards, Eddie Carroll, Mel Blanc, Walter Catlett, Dickie Jones"
    assert compare_answers(gt, pred)[0] == 1.0


def test_grounded_passes_a_list_copied_from_the_tool():
    assert check_grounded(", ".join(TITLES), [tool(titles=TITLES)])[0] == 1.0
    assert check_grounded(json.dumps(TITLES), [tool(titles=TITLES)])[0] == 1.0


def test_grounded_passes_a_partial_list():
    assert check_grounded(", ".join(TITLES[:9]), [tool(titles=TITLES)])[0] == 1.0


def test_grounded_passes_a_short_value_only_when_it_is_also_the_ground_truth():
    assert check_grounded("652", [tool(zip=[652])], gt_answer="652")[0] == 1.0
    assert check_grounded("0", [tool(counts=[0, 37])], gt_answer="37") is None


def test_grounded_fails_a_slip_in_a_short_answer():
    assert check_grounded("Google Photas", [tool(apps=["Google Photos", "Maps"])])[0] == 0.0
    assert check_grounded("14.544901446672062", [tool(delay=[14.544901546672062])])[0] == 0.0
    assert check_grounded("The Many of Winnie the Pooh", [tool(titles=["The Many Adventures of Winnie the Pooh"])])[0] == 0.0


def test_grounded_does_not_take_a_number_cut_at_its_comma_for_a_slip():
    answer = "The total trip duration for trips in Palo Alto is 7,158.120555555555 hours."
    assert check_grounded(answer, [tool(total_duration_hours=7158.120555555555)]) is None


def test_compare_does_not_take_a_number_cut_at_its_comma_for_a_slip():
    assert compare_answers("7158.120555555555", "The total trip duration in Palo Alto is 7,158.120555555555 hours.") is None
    assert compare_answers("7158.12", "7,158.12")[0] == 1.0


def test_grounded_does_not_take_a_percentage_for_a_slip():
    assert check_grounded("78.2608695652174", [tool(share=[0.782608695652174])]) is None


def test_grounded_leaves_a_count_around_a_list_to_the_model():
    assert check_grounded("30, " + ", ".join(TITLES), [tool(titles=TITLES)]) is None
    assert check_grounded(", ".join(TITLES[:15]) + ", 30, " + ", ".join(TITLES[15:]), [tool(titles=TITLES)])[0] == 0.0


def test_grounded_takes_the_row_width_that_fits_best():
    rows = [["Frank", "Brimsek", 1938], ["Frank", "Brimsek", 1939], ["Frank", "Brimsek", 1940]]
    rows += [["Tiny", "Thompson", y] for y in (1928, 1929, 1930, 1932, 1934, 1937, 1938)]
    answer = json.dumps(rows)
    assert check_grounded(answer, [tool(goalies=rows)])[0] == 1.0


def test_grounded_does_not_take_a_shifted_list_for_mixed_rows():
    rows = [[f"site {i}", 1000 + i] for i in range(100)]
    flat = [str(v) for r in rows for v in r]
    del flat[40]  # one empty field: every pair after it reads one place off
    assert check_grounded(", ".join(flat), [tool(rows=rows)])[0] == 1.0


def test_grounded_leaves_a_shorter_form_to_the_model():
    assert check_grounded("Studio Entertainment", [tool(segment=["Studio Entertainment[NI 1]"])]) is None


@pytest.mark.parametrize("slip", ["Movie number", "Movie Sarah Wolf", "Mvie number 7", "12345"])
def test_grounded_fails_one_slip_in_a_list(slip):
    answer = TITLES[:15] + [slip] + TITLES[15:]
    score, why = check_grounded(", ".join(answer), [tool(titles=TITLES)])
    assert score == 0.0 and "1 of 31" in why


def test_grounded_finds_one_slip_among_thousands_beyond_any_cut():
    ids = list(range(100_000))
    answer = [str(i) for i in ids]
    answer[50_000] = "100001"
    assert check_grounded(", ".join(answer), [tool(ids=ids)])[0] == 0.0


def test_grounded_fails_values_of_different_rows_put_together():
    rows = [{"name": f"car {i}", "price": 1000 + i} for i in range(10)]
    answer = [f"{r['name']}, {r['price']}" for r in rows]
    assert check_grounded(", ".join(answer), [tool(cars=rows)])[0] == 1.0
    answer[4], answer[5] = "car 4, 1005", "car 5, 1004"
    score, why = check_grounded(", ".join(answer), [tool(cars=rows)])
    assert score == 0.0 and "different tool rows" in why


@pytest.mark.parametrize(
    "answer",
    [
        "Wayne Gretzky scored 92 goals.",
        "Sir Toby Belch appears in scenes 1, 2, 3, 4 and 5 of Twelfth Night.",
        "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, which are all of the scenes that Sir Toby is part of",
    ],
)
def test_grounded_leaves_sentences_to_the_model(answer):
    assert check_grounded(answer, [tool(scenes=list(range(1, 11)), name=["Sir Toby Belch"])]) is None


def test_grounded_leaves_an_unreadable_format_to_the_model():
    answer = ", ".join(f"{t} - Gary Trousdale" for t in TITLES)
    assert check_grounded(answer, [tool(titles=TITLES)]) is None


def test_grounded_leaves_an_empty_answer_to_the_model():
    assert check_grounded("", [tool(titles=TITLES)]) is None


def test_values_cut_at_commas_like_the_ground_truth_text():
    assert values(["KIM KI-DUK, ranked", "2010"]) == ["kimkiduk", "ranked", "2010"]
