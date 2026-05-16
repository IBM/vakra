# 🧪 Query Evaluation UI

Lightweight Flask UI for **human evaluation of LLM-generated multi-hop queries** with:

* Side-by-side input + evaluation view
* Keyboard shortcuts ⚡
* Auto-advance across metrics
* Hover tooltips for scoring

---

# 🚀 Setup & Run

```bash
pip install flask
python helpers/human_evaluation/task3_eval_ui.py --data task3_data.json --out annotations.json
```

Open:

```
http://127.0.0.1:5000
```

---

# 📂 Data Format & Usage

### Input JSON (`task3_data.json`)
Get the input file from https://ibm.box.com/s/2b9g7nq1r2h1ll866sebw5cuaf1az2xj

```json
[
  {
    "uuid": "ex-001",
    "domain": "movies",
    "component_queries": ["..."],
    "query": "...",
    "gt_tools": [...],
    "gt_responses": [...],
    "gt_answer": "..."
  }
]
```

### Annotation Flow

* Press **1–4** → score metric
* It auto-advances to next metric
* Press **N** → next example
* Press **P** → previous
* Press **T** → jump to notes

### Output

Saved to:

```
annotations.json
```
annotations.json saved at the end of every sample.

Contains scores + notes + timestamps.

### Metrics

```
METRICS = {
    "faithfulness": {
        "title": "Faithfulness Score",
        "desc": "Does the merged query contain only information grounded in the component queries, without introducing unsupported facts?",
        "scale": {
            1: "Completely hallucinated: major unsupported entities or relations introduced",
            2: "Harmful hallucination: incorrect info that breaks reasoning",
            3: "Minor hallucination: small issues but mostly correct",
            4: "No hallucination: fully grounded in component queries"
        }
    },
    "naturalness": {
        "title": "Naturalness",
        "desc": "Is the merged query fluent, natural, and phrased in a way a human would realistically understand?",
        "scale": {
            1: "Completely unnatural or broken",
            2: "Awkward and clearly stitched",
            3: "Mostly natural but slightly awkward",
            4: "Fully natural and fluent"
        }
    },
    "logical_consistency": {
        "title": "Logical Consistency",
        "desc": "Is the merged query logically consistent, with no contradictions between its components, and aligned with the reasoning chain implied by intermediate queries?",
        "scale": {
            1: "Completely inconsistent and contradictory leading it to be unanswerable",
            2: "Major inconsistency to the level of being misleading",
            3: "Minor inconsistency not affecting answerability",
            4: "Fully consistent"
        }
    },
    "answer_leakage": {
        "title": "Answer Leakage",
        "desc": "Does the merged query explicitly or implicitly reveal the answer entity, making the reasoning task trivial? (i.e., Is the answer entity accidentally mentioned in the query, or does the merged query leak any answer to the hop question?)",
        "scale": {
            1: "Complete leakage (answer directly present)",
            2: "Strong leakage (very obvious answer)",
            3: "Minor hints present",
            4: "No leakage"
        }
    },
    "context_sufficiency": {
        "title": "Context Sufficiency",
        "desc": "Does the merged query contain sufficient context and constraints to be answerable via the intended tool or retrieval pipeline? (i.e., Does the merged question have enough context present to be answered as well as does the query have enough context to populate the arguments of the tools?)",
        "scale": {
            1: "Not answerable",
            2: "Major missing context",
            3: "Mostly sufficient",
            4: "Fully sufficient"
        }
    },
    "reasoning_hops": {
        "title": "Reasoning Steps / Hops",
        "desc": "If you had to solve this query, how many reasoning steps would be required according to you?",
        "scale": {
            1: "Single hop",
            2: "Two hops",
            3: "Moderate multi-hop",
            4: "Complex multi-hop",
            5: "Very complex reasoning"
        }
    }
}
```
