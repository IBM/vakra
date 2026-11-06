"""
Value-by-value checks for list answers.

Long VAKRA answers are lists of values copied from tool output, and tool output is JSON, so code can compare them at any
length and see one wrong item among thousands, which a model reading the list does not. The Kev / Jev judges ask these
checks first. A check returns a verdict only when it is sure; None means the answer is not something code can read
(a sentence, codes against names) and the model decides.
"""
from __future__ import annotations

import ast
import html
import json
import re
import unicodedata
from bisect import bisect_left
from collections import Counter, defaultdict
from difflib import get_close_matches
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

Verdict = Tuple[float, str]

MIN_ITEMS = 5  # one wrong item fails a list this long; a shorter answer fails only on a clear slip
MAX_ROW_WIDTH = 6  # widest row looked for in an answer like "name, price, name, price"
MAX_NEAR_CHECK = 50_000  # most values searched for a near miss
MIN_ROUNDED_DIGITS = 3  # 25.9 can be 25.92 rounded; 0 is not 0.18 rounded
MIN_CLIPPED_CHARS = 1000  # only an answer this long can have run out of room
MIN_ROW_FIT = 0.8  # share of the answer that must lie in tool rows before its rows are checked at all
MAX_MIXED_ROWS = (3, 0.05)  # a swap breaks a few rows (this many, or this share); more means the rows were misread
MAX_LITERAL_CHARS = 20_000_000

_EDGE = " \t\r\n\"'`[](){}<>;:$%\u20ac\u00a3"
_WORD = re.compile(r"\w+")
_NUMBER = re.compile(r"^[+-]?(\d{1,3}(,\d{3})+|\d+)(\.\d+)?$")
_DATE_T = re.compile(r"(?<=\d)t(?=\d)")
_DOT_ZERO = re.compile(r"(?<![\d.])(\d+)\.0+(?!\d)")
_QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')
_ESCAPED = re.compile(r"\\u([0-9a-fA-F]{4})")
_BULLET = re.compile(r"^(?:[-*•]\s+|\d{1,4}[.)]\s+)")
_ITEM_SPLIT = re.compile(r"\n+|,\s*")
_VALUE_SPLIT = re.compile(r"\n+|[,;]")
_TIME_ZERO = re.compile(r"(:\d\d)\.0+$")
_MIDNIGHT = re.compile(r"[ t]00:00:00$")
_NOT_ANSWERABLE = "can not answer"
# what an agent adds when it stops early: "... (list continues)", "...", "(Note: truncated due to length)"
_STOP_NOTE = re.compile(r"\s*(?:\.{3}|…)\s*\(?[^()\[\]]{0,60}(?:continu|remain|more|truncat|omitted)[^()\[\]]{0,60}\)?[\"'\]\s]*$", re.I)
_DOTS = re.compile(r"\s*(?:\.{3}|…)[\"'\]\s]*$")
_SAID = re.compile(r"\s*\(?note:[^()\n]{0,120}\)?\s*$", re.I)


def squash(value: Any) -> str:
    """Lower-case letters and digits only, so text that differs in case, spacing, quotes or punctuation compares equal."""
    s = str(value)
    if "&" in s:
        s = html.unescape(s)
    if "\\" in s:
        s = s.replace("\\n", " ").replace("\\t", " ")  # line breaks and tabs written out
    s = _DOT_ZERO.sub(r"\1", s)  # agents' answer adapters turn "Tier 2.0" into "Tier 2"
    return " ".join(_WORD.findall(_DATE_T.sub(" ", unicodedata.normalize("NFKC", s).lower())))


def is_number(value: Any) -> bool:
    return bool(_NUMBER.match(str(value).strip().strip(_EDGE)))


def canon(value: Any) -> str:
    """One value in comparable form: numbers by value, text squashed without spaces ("M*A*S*H" equals "MASH")."""
    if value is None:
        return "null"
    s = _MIDNIGHT.sub("", _TIME_ZERO.sub(r"\1", str(value).strip().strip(_EDGE).lower()))
    if s == "none":
        return "null"
    if is_number(s):
        t = s.replace(",", "")
        if "." not in t:
            return str(int(t))  # exact: float() would merge 19-digit IDs
        number = float(t)
        return str(int(number)) if number.is_integer() else f"{number:.10g}"
    return squash(s).replace(" ", "")


def parse(text: str) -> Any:
    """The JSON value or Python literal in `text`, or None."""
    s = text.strip()
    if s[:1] not in ("[", "{"):
        return None
    try:
        return json.loads(s)
    except ValueError:
        pass
    if len(s) <= MAX_LITERAL_CHARS:
        try:
            return ast.literal_eval(s)
        except (ValueError, SyntaxError, MemoryError, RecursionError):
            pass
    return None


def leaves(obj: Any, keys: Optional[List[Any]] = None) -> List[Any]:
    """Every scalar in a parsed value, in order; dictionary keys go to `keys` when given."""
    stack, out = [obj], []
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if keys is not None:
                keys.extend(o)
            stack.extend(reversed(list(o.values())))
        elif isinstance(o, (list, tuple)):
            stack.extend(reversed(o))
        else:
            out.append(o)
    return out


def rows_of(obj: Any) -> List[List[Any]]:
    """Records in a parsed tool output: dictionaries, short lists of scalars, and same-length lists side by side."""
    stack, out = [obj], []
    while stack:
        o = stack.pop()
        if not isinstance(o, (dict, list, tuple)):
            continue
        members = list(o.values()) if isinstance(o, dict) else list(o)
        nested = [v for v in members if isinstance(v, (dict, list, tuple))]
        flat = [v for v in members if not isinstance(v, (dict, list, tuple))]
        if 2 <= len(flat) <= 12 and (isinstance(o, dict) or not nested):
            out.append(flat)
        if isinstance(o, dict):
            columns = [v for v in nested if isinstance(v, list) and v and not isinstance(v[0], (dict, list, tuple))]
            if len(columns) >= 2 and len({len(c) for c in columns}) == 1:
                out.extend([list(r) for r in zip(*columns)])
        stack.extend(nested)
    return out


def split_items(text: Any) -> List[str]:
    """The values an answer lists, in order: JSON, a JSON list the agent stopped half-way, lines or commas."""
    s = str(text).strip()
    if "\\u" in s:
        s = _ESCAPED.sub(lambda m: chr(int(m[1], 16)), s)
    s = _DOTS.sub("", _STOP_NOTE.sub("", _SAID.sub("", s)))
    if not s or is_number(s):
        return [s] if s else []
    obj = parse(s)
    if obj is not None:
        return [str(v) for v in leaves(obj)]
    if s[0] in "[{":
        quoted = _QUOTED.findall(s)
        if quoted and sum(len(q) + 3 for q in quoted) > 0.8 * len(s):
            tail = s[s.rfind('"') + 1:] if s.count('"') % 2 else ""
            return quoted + ([tail] if tail.strip() else [])
        s = s.strip("[]{}")
    # ";" only when nothing else separates the values: it also ends HTML entities like "&amp; "
    parts = _ITEM_SPLIT.split(s) if ("\n" in s or "," in s) else re.split(r";\s+", s)
    return [item for item in (_BULLET.sub("", p.strip()) for p in parts) if item]


def parts_of(value: Any) -> List[str]:
    """`value` cut where an answer could cut it: VAKRA's ground truth text joins values with commas."""
    s = str(value)
    return [s] if is_number(s) else _VALUE_SPLIT.split(html.unescape(s) if "&" in s else s)


def values(items: Iterable[Any]) -> List[str]:
    """Comparable values of `items`, each cut into its parts."""
    return [c for item in items for c in (canon(p) for p in parts_of(item)) if c]


def is_list(items: Sequence[Any], answer: str) -> bool:
    """Is this a list of MIN_ITEMS or more values, not sentences cut at their commas?"""
    if len(items) < MIN_ITEMS:
        return False
    if parse(str(answer)) is not None:
        return True
    words = sorted(len(str(i).split()) for i in items)
    return words[len(words) // 2] <= 8 and sum(w >= 15 for w in words) <= 0.1 * len(words)


class Evidence:
    """Every value in a turn's tool responses, searchable by whole value: a shortened item does not pass as found."""

    def __init__(self, responses: Iterable[Any], query: str = ""):
        self.values: List[Any] = []
        self.rows: List[List[Any]] = []
        keys: List[Any] = []
        texts: List[str] = []
        for r in responses:
            obj = r if isinstance(r, (list, dict)) else parse(str(r))
            if obj is None:
                texts.append(squash(r))
                continue
            self.values.extend(leaves(obj, keys))
            self.rows.extend(rows_of(obj))
        self.known: Set[str] = {canon(k) for k in keys}
        for v in self.values:
            self.known.add(canon(v))
            if isinstance(v, str) and ("," in v or "\n" in v or ";" in v):
                self.known.update(canon(p) for p in parts_of(v))
        self.known.discard("")
        self.text = "\n".join(texts)
        self.query = squash(query)
        self._squashed: Optional[List[str]] = None
        self._rows_by_value: Optional[Dict[str, Set[int]]] = None
        self._rows_by_word: Optional[Dict[str, List[int]]] = None
        self._values_by_word: Optional[Dict[str, List[int]]] = None

    def has(self, item: Any) -> bool:
        c = canon(item)
        if not c or c in self.known:
            return True
        s = squash(item)
        return len(c) > 3 and (s in self.text or (" " in s and s in self.query))

    def found(self, item: Any) -> bool:
        return self.has(item) or all(self.has(p) for p in parts_of(item)) or self._fills_a_row(item)

    def starts(self, item: Any) -> bool:
        """Is `item` the start of a value? An answer that ran out of room can stop inside its last item."""
        if self._squashed is None:
            self._squashed = sorted({squash(p) for v in self.values for p in [v, *parts_of(v)]})
        s = squash(item)
        k = bisect_left(self._squashed, s)
        return bool(s) and ((k < len(self._squashed) and self._squashed[k].startswith(s)) or s in self.text)

    def similar(self, item: Any) -> bool:
        """Does a value share half the words of `item`? Then `item` is a slip in copying, not a sentence."""
        if self._values_by_word is None:
            self._values_by_word = defaultdict(list)
            for k, v in enumerate(self.values):
                for w in set(squash(v).split()):
                    self._values_by_word[w].append(k)
        words = set(squash(item).split())
        counts = Counter(k for w in words for k in self._values_by_word.get(w, ())[:20_000])
        return bool(counts) and 2 * max(counts.values()) >= len(words)

    def rows_with(self, item: Any) -> Set[int]:
        if self._rows_by_value is None:
            self._rows_by_value = defaultdict(set)
            for k, row in enumerate(self.rows):
                for v in row:
                    self._rows_by_value[canon(v)].add(k)
        return self._rows_by_value.get(canon(item), set())

    def _fills_a_row(self, item: Any) -> bool:
        """Is `item` whole fields of one record, like "Antti Aalto" for first and last name in separate fields?"""
        s = squash(item)
        words = s.split()
        if len(words) < 2 or not self.rows:
            return False
        if self._rows_by_word is None:
            self._rows_by_word = defaultdict(list)
            for k, row in enumerate(self.rows):
                for w in {w for v in row for w in squash(v).split()}:
                    self._rows_by_word[w].append(k)
        candidates = [self._rows_by_word.get(w, ()) for w in words]
        if not all(candidates):
            return False
        for k in min(candidates, key=len)[:2000]:
            rest = f" {s} "
            for field in sorted((squash(v) for v in self.rows[k]), key=len, reverse=True):
                if field and f" {field} " in rest:
                    rest = rest.replace(f" {field} ", " ", 1)
            if not rest.strip():
                return True
        return False


def _unique(xs: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(xs))


def near_miss(item: Any, others: Sequence[Any]) -> Optional[str]:
    """
    The value in `others` that `item` nearly equals: a slip in copying ("Google Photas", "The Many of Winnie the Pooh").
    A value that holds `item` whole is not one: "Studio Entertainment" is a fair copy of "Studio Entertainment[NI 1]".
    """
    if len(others) > MAX_NEAR_CHECK:
        return None
    s = squash(item)
    numbers = {squash(v): v for v in others if is_number(v)} if is_number(item) else {}
    for match in get_close_matches(s, _unique(squash(v) for v in others), n=3, cutoff=0.8):
        if s in match or match in s:
            continue
        if is_number(item) and (match not in numbers or abs(float(canon(item)) - float(canon(numbers[match]))) > 0.001 * abs(float(canon(item)))):
            continue  # 78.26 and 0.7826 share their digits, but that is a percentage, not a slip
        return match
    return None


def slip_in(items: Sequence[Any], others: Sequence[Any]) -> Optional[Tuple[Any, str]]:
    """A near miss among the items of a short answer, with what it misses. A sentence cut at its commas has none."""
    matches = [(i, near_miss(i, others)) for i in items]
    if any(len(str(i).split()) >= 4 and not match for i, match in matches):
        return None
    return next(((i, match) for i, match in matches if match), None)


def unsupported(items: Sequence[str], evidence: Evidence) -> List[str]:
    """Items not in the tool output. In a long answer, a last item cut short or one ending in "..." counts by its start."""
    missing = _unique(str(i) for i in items if not evidence.found(i))
    if sum(len(str(i)) for i in items) < MIN_CLIPPED_CHARS:
        return missing
    if missing and missing[-1] == str(items[-1]) and evidence.starts(missing[-1]):
        missing.pop()
    return [m for m in missing if m.rstrip(" .…") == m or not evidence.starts(m.rstrip(" .…"))]


def misplaced(items: Sequence[str], evidence: Evidence) -> List[List[str]]:
    """
    Values of different rows put side by side. In "name, price, name, price" every value can be in the tool output and
    the pairs still be wrong. The row width that fits the answer best says where its rows are; a run of that width that
    lies in no tool row is a mistake, unless so many do that the rows were misread (an empty field shifts all that follow).
    """
    rows = [evidence.rows_with(i) for i in items]
    best_fit, broken, runs_seen = 0.0, [], 0
    for width in range(2, MAX_ROW_WIDTH + 1):
        runs = [range(k, k + width) for k in range(0, len(items) - width + 1, width)]
        if len(runs) < 3:
            break
        known = [r for r in runs if all(rows[k] for k in r)]
        apart = [r for r in known if not set.intersection(*(rows[k] for k in r))]
        fit = (len(known) - len(apart)) / len(runs)
        if fit > best_fit:
            best_fit, broken, runs_seen = fit, apart, len(runs)
    if best_fit < MIN_ROW_FIT or len(broken) > max(MAX_MIXED_ROWS[0], MAX_MIXED_ROWS[1] * runs_seen):
        return []
    return [[str(items[k]) for k in r] for r in broken]


def same_values(gt: Sequence[str], pred: Sequence[str]) -> bool:
    """The same values, as often as the ground truth has them unless the prediction lists each one once."""
    gt_counts, pred_counts = Counter(gt), Counter(pred)
    return set(gt_counts) == set(pred_counts) and (gt_counts == pred_counts or max(pred_counts.values()) == 1)


def compare_numbers(gt: str, pred: str) -> Optional[Verdict]:
    """
    Two numbers: the prediction is the ground truth to the precision it gives, or it is not. Rounded too far to say
    (0 for 0.18), or a factor of 100 (a share and a percentage), is for the model.
    """
    a, b = float(gt), float(pred)
    decimals = len(pred.split(".")[1]) if "." in pred else 0
    if abs(a - b) <= 0.5 * 10.0 ** -decimals:
        digits = len(pred.lstrip("+-0").replace(".", "").lstrip("0"))
        return (1.0, f"{pred} is {gt} rounded") if digits >= MIN_ROUNDED_DIGITS else None
    if a and b and any(abs(x - 100) <= 1 for x in (abs(a / b), abs(b / a))):
        return None
    return 0.0, f"{pred} is not {gt}"


def compare_answers(gt_answer: str, pred_answer: str, gt_tool_responses: Sequence[Any] = ()) -> Optional[Verdict]:
    """Do ground truth and prediction list the same items? None when code cannot tell."""
    gt_items, pred_items = split_items(gt_answer), split_items(pred_answer)
    gt_values, pred_values = values(gt_items), values(pred_items)
    if not gt_values:
        return None
    if not pred_values:
        return None if _NOT_ANSWERABLE in squash(gt_answer) else (0.0, "the prediction is empty")
    if same_values(gt_values, pred_values):
        return 1.0, f"same {len(set(gt_values))} items"
    if len(gt_items) == len(pred_items) == 1 and is_number(gt_items[0]) and is_number(pred_items[0]):
        return compare_numbers(gt_values[0], pred_values[0])
    if not is_list(gt_items, gt_answer):
        slip = slip_in([p for p in pred_items if canon(p) not in gt_values], gt_items) if len(pred_items) < MIN_ITEMS else None
        return (0.0, f"{slip[0]!r} is close to the ground truth {slip[1]!r} but not the same") if slip else None
    gt_set, pred_set = set(gt_values), set(pred_values)
    shared = len(gt_set & pred_set)
    if not is_list(pred_items, pred_answer) and 2 * shared < len(pred_set):
        return None  # a sentence, not a shorter list
    if 2 * shared < min(len(gt_set), len(pred_set)):
        return None  # mostly different values: another form of the answer (codes for names), for the model to read
    missing = gt_set - pred_set
    if missing:
        return 0.0, f"{len(missing)} of {len(gt_set)} ground truth items missing from the prediction"
    extra = _unique(v for v in pred_values if v not in gt_set)
    if not extra:
        return 0.0, f"same {len(gt_set)} items, but some repeated a different number of times"
    # VAKRA's ground truth text keeps the first column of each row; the full rows are in its last tool response
    rows = Evidence(list(gt_tool_responses)[-1:])
    wrong = [v for v in extra if not rows.has(v)]
    if wrong:
        return 0.0, f"{len(wrong)} items in the prediction are not in the ground truth, e.g. {wrong[:3]}"
    mixed = misplaced(pred_items, rows)
    if mixed:
        return 0.0, f"{len(mixed)} rows mix values of different ground truth rows, e.g. {mixed[:2]}"
    return 1.0, f"same {len(gt_set)} items, with {len(extra)} values from other columns of their rows"


def check_grounded(answer: str, tool_responses: Sequence[Any], query: str = "", gt_answer: str = "") -> Optional[Verdict]:
    """Is every listed item in the tool output? None when code cannot tell."""
    items = split_items(answer)
    if not items:
        return None
    evidence = Evidence(tool_responses, query)
    missing = unsupported(items, evidence)
    if not missing:
        mixed = misplaced(items, evidence)
        if mixed:
            return 0.0, f"{len(mixed)} rows mix values of different tool rows, e.g. {mixed[:2]}"
        # a list copied from the tool is grounded; a short value can be in the tool output by chance
        if len(items) >= MIN_ITEMS or same_values(values(split_items(gt_answer)), values(items)):
            return 1.0, f"all {len(items)} items are in the tool responses"
        return None
    slip = slip_in(missing, evidence.values) if len(items) < MIN_ITEMS else None
    if slip:
        return 0.0, f"{slip[0]!r} is close to the tool value {slip[1]!r} but not the same"
    if not is_list(items, answer) or 2 * len(missing) >= len(items):
        return None  # sentences, or a format the code does not read
    if any(len(m.split()) >= 4 and not evidence.similar(m) for m in missing):
        return None  # text that resembles no value: a sentence around the list, for the model to read
    if any(is_number(m) and m in (str(items[0]), str(items[-1])) for m in missing):
        return None  # a number before or after the list: a count or total the agent worked out
    return 0.0, f"{len(missing)} of {len(items)} items are not in the tool responses, e.g. {missing[:3]}"
