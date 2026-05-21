# 🧪 Query Evaluation UI

Lightweight Flask UI for **human evaluation of LLM-generated multi-hop queries**.

---

# 🚀 Setup & Run

### Input JSON (`task<N>_data.json`)
Get the input file from https://ibm.box.com/s/2b9g7nq1r2h1ll866sebw5cuaf1az2xj

From this folder:

```bash
cd vakra/scripts/human_evaluation
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python task<N>_eval_ui.py --data task<N>_data.json --out task<N>_annotations.json
```

Open:

```
http://127.0.0.1:5000
```

### Annotation Flow

* Press **1–4** → score metric
* Press **0** → mark `retrieval_sufficiency` or `cross_hop_entity_consistency` as not applicable in Task 4
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

### Task 3 Metrics

```python
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

### Task 4 Metrics

```python
METRICS = {
    "faithfulness": {
        "title": "Faithfulness Score",
        "desc": "Does the merged query contain only information grounded in the component queries, without introducing unsupported facts?",
        "scale": {
            1: "Completely hallucinated: major unsupported entities or relations",
            2: "Harmful hallucination: incorrect info that breaks reasoning",
            3: "Minor hallucination: small issues but mostly correct",
            4: "No hallucination: fully grounded in component queries"
        }
    },
    "naturalness": {
        "title": "Naturalness",
        "desc": "Is the merged query fluent, natural, and phrased in a way a human would understand the ask? (i.e., check grammaticality, fluency, and overall natural phrasing of the merged query)",
        "scale": {
            1: "Cannot understand what is being asked at all",
            2: "Highly awkward phrasing; clearly stitched but can understand the ask from the query",
            3: "Slightly awkward phrasing but can understand the ask from the query",
            4: "Fully natural and fluent"
        }
    },
    "logical_consistency": {
        "title": "Logical Consistency",
        "desc": (
        "Does the merged query avoid logically incompatible conditions or contradictions? "
        "The different parts of the query should be simultaneously satisfiable and should "
        "form a valid reasoning chain. For example, a question like "
        "'Name a city on Earth which lies above the equator and is in Australia?' "
        "contains logically incompatible constraints."
    ),
        "scale": {
            1: "Completely inconsistent: logically incompatible component queries or conditions that cannot be satisfied together were merged",
            2: "Major inconsistency: the query contains impossible or directly contradictory conditions that make the question invalid or unanswerable",
            3: "Minor inconsistency: the query is mostly logically valid but contains small ambiguities, weak conflicts, or mildly confusing constraints",
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
            1: "Not answerable: entities missing in the merged query or insufficient context to answer the question",
            2: "Major missing context: all entities present but insufficient context significantly hindering answerability",
            3: "Mostly sufficient: minor missing context that does not significantly hinder answerability",
            4: "Fully sufficient"
        }
    },
    "retrieval_sufficiency": {
        "title": "Retrieval Sufficiency Score",
        "desc": "Do the ground truth documents have sufficient information to answer the RAG query? (Mark '0' if no RAG component in query.)",
        "scale": {
            0: "Not applicable (e.g., no retrieval needed for this query)",            
            1: "GT document have no relevant information",
            2: "GT document have some missing information",
            3: "GT document have some missing information which is common sense knowledge",
            4: "No information missing"
        },
    },
    "cross_hop_entity_consistency": {
        "title": "Cross-Hop Entity Consistency Score",
        "desc": "Are entities required by the arguments of the succeeding or preceding API tool calls correctly inferred and grounded in the retrieved documents or retriever questions? (Mark '0' if no RAG component in query.)",
        "scale": {
            0: "Not applicable (e.g., no retrieval needed for this query)",
            1: "Not answerable",
            2: "Majorly missing context / entities cannot be answered without these entities",
            3: "Mostly sufficient have some missing information which is common sense knowledge",
            4: "Fully sufficient",
        },
    }
}
```
