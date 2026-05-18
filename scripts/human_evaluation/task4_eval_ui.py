import argparse
import json
from datetime import datetime
from pathlib import Path

from flask import Flask, redirect, render_template_string, request, url_for


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
    # "reasoning_hops": {
    #     "title": "Reasoning Steps / Hops",
    #     "desc": "If you had to solve this query, how many reasoning steps would be required according to you?",
    #     "scale": {
    #         1: "Single step",
    #         2: "Two steps",
    #         3: "Moderate multi-hop",
    #         4: "Complex multi-hop",
    #         5: "Very complex reasoning",
    #     },
    # },
    "retrieval_sufficiency": {
        "title": "Retrieval Sufficiency Score",
        "desc": "Do the ground truth documents have sufficient information to answer the RAG query?",
        "scale": {
            1: "GT document have no relevant information",
            2: "GT document have some missing information",
            3: "GT document have some missing information which is common sense knowledge",
            4: "No information missing",
        },
    },
    "cross_hop_entity_consistency": {
        "title": "Cross-Hop Entity Consistency Score",
        "desc": "Are entities required by the arguments of the succeeding or preceding API tool calls correctly inferred and grounded in the retrieved documents or retriever questions?",
        "scale": {
            1: "Not answerable",
            2: "Majorly missing context / entities cannot be answered without these entities",
            3: "Mostly sufficient have some missing information which is common sense knowledge",
            4: "Fully sufficient",
        },
    },
}

QUALITY_KEYS = [
    "faithfulness",
    "naturalness",
    "logical_consistency",
    "answer_leakage",
    "context_sufficiency",
    "retrieval_sufficiency",
    "cross_hop_entity_consistency",
]

HTML = """
<!doctype html>
<html>
<head>
  <title>Task 4 Query Evaluation</title>
  <style>
    body {
      font-family: Arial;
      margin: 0;
      background: #fafafa;
    }

    .container {
      display: flex;
      height: 100vh;
      overflow: visible;
    }

    .left, .right {
      width: 50%;
      overflow-y: auto;
      overflow-x: visible;
      padding: 20px;
      position: relative;
    }

    .left {
      border-right: 2px solid #ddd;
      background: #ffffff;
      z-index: 1;
    }

    .right {
      background: #f9f9f9;
      z-index: 2;
    }

    .card {
      background: white;
      border: 1px solid #ddd;
      border-radius: 10px;
      padding: 15px;
      margin-bottom: 15px;
    }

    .active-metric {
      border: 2px solid #333;
    }

    pre {
      white-space: pre-wrap;
      background: #f4f4f4;
      padding: 10px;
      border-radius: 8px;
    }

    .metric {
      margin-bottom: 20px;
    }

    .desc {
      color: #555;
      font-size: 14px;
      margin-bottom: 6px;
    }

    .progress {
      margin-bottom: 15px;
      font-size: 14px;
    }

    button {
      padding: 10px 16px;
      margin-right: 10px;
      border-radius: 6px;
      border: 1px solid #999;
      cursor: pointer;
    }

    .tooltip {
      position: relative;
      display: inline-block;
      margin-right: 15px;
      cursor: pointer;
    }

    .tooltip .tooltiptext {
      visibility: hidden;
      width: 260px;
      background-color: #333;
      color: #fff;
      text-align: left;
      border-radius: 6px;
      padding: 8px;
      position: absolute;
      z-index: 9999;
      top: 140%;
      left: 0;
      opacity: 0;
      transition: opacity 0.2s;
      font-size: 12px;
      line-height: 1.4;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
      pointer-events: none;
    }

    .tooltip:hover .tooltiptext {
      visibility: visible;
      opacity: 1;
    }

    .hint {
      font-size: 12px;
      color: #777;
      margin-bottom: 10px;
    }

    .turn {
      border-top: 1px solid #eee;
      padding-top: 10px;
      margin-top: 10px;
    }

    .empty {
      color: #777;
      font-style: italic;
    }
  </style>
</head>

<body>

<div class="container">

  <div class="left">
    <h2>Input Data</h2>

    <div class="card">
      <b>UUID:</b> {{ item.uuid }} <br>
      <b>Domain:</b> {{ item.domain }} <br>
      <b>Type:</b> {{ item.type }} <br>
      <b>Question Type:</b> {{ item.question_type }}
    </div>

    <div class="card">
      <h3>Dialogue Context</h3>
      {% if item.dialogue %}
        {% for turn in item.dialogue %}
          <div class="turn">
            <b>Turn {{ turn.turn_id }}</b>
            <pre>{{ turn.query }}</pre>
            {% if turn.answer is defined %}
              <b>Answer</b>
              <pre>{{ turn.answer | tojson(indent=2) }}</pre>
            {% endif %}
          </div>
        {% endfor %}
      {% else %}
        <div class="empty">No prior dialogue turns.</div>
      {% endif %}
    </div>

    <div class="card">
      <h3>Component Queries</h3>
      {% if item.component_queries %}
        <ul>
          {% for q in item.component_queries %}
            <li>{{ q }}</li>
          {% endfor %}
        </ul>
      {% else %}
        <div class="empty">No component queries for this sample.</div>
      {% endif %}
    </div>

    <div class="card">
      <h3>Final Query</h3>
      <pre>{{ item.query }}</pre>
    </div>

    <div class="card">
      <h3>Ground Truth Tools</h3>
      <pre>{{ item.gt_tools | tojson(indent=2) }}</pre>
    </div>

    <div class="card">
      <h3>Tool Responses / Documents</h3>
      <pre>{{ item.gt_responses | tojson(indent=2) }}</pre>
    </div>

    <div class="card">
      <h3>Final Answer</h3>
      <pre>{{ item.gt_answer }}</pre>
    </div>
  </div>

  <div class="right">
    <h2>Evaluation</h2>

    <div class="progress">
      Item {{ idx + 1 }} / {{ total }} | Completed: {{ completed }}
    </div>

    <div class="hint">
      Hover over numbers for score explanations. Keyboard: 1-5 = score active metric, N = Save & Next, P = Save & Previous.
    </div>

    <form id="eval-form" method="post" action="{{ url_for('save', idx=idx) }}">

    {% for key, metric in metrics.items() %}
    <div class="card metric" data-metric-key="{{ key }}">
      <h3>{{ loop.index }}. {{ metric.title }}</h3>
      <div class="desc">{{ metric.desc }}</div>

      {% for score, explanation in metric.scale.items() %}
      <label class="tooltip">
        <input type="radio" name="{{ key }}" value="{{ score }}"
          {% if existing.get(key) == score %}checked{% endif %} required>
        <b>{{ score }}</b>
        <span class="tooltiptext">{{ explanation }}</span>
      </label>
      {% endfor %}
    </div>
    {% endfor %}

      <div class="card">
        <h3>Notes</h3>
        <textarea id="notes" name="notes" rows="4" style="width:100%;">{{ existing.get("notes","") }}</textarea>
      </div>

      <button name="action" value="prev">Prev</button>
      <button name="action" value="next">Next</button>
      <button name="action" value="stay">Save</button>

    </form>
  </div>

</div>

<script>
  const metricCards = Array.from(document.querySelectorAll(".metric"));
  let activeIndex = 0;

  function setActiveMetric(index) {
    if (index < 0 || index >= metricCards.length) return;

    metricCards.forEach(card => card.classList.remove("active-metric"));
    metricCards[index].classList.add("active-metric");
    metricCards[index].scrollIntoView({ behavior: "smooth", block: "center" });
    activeIndex = index;
  }

  function moveToNextMetric() {
    if (activeIndex < metricCards.length - 1) {
      setActiveMetric(activeIndex + 1);
    } else {
      metricCards[activeIndex].style.border = "2px solid green";
    }
  }

  function selectScore(score) {
    const activeCard = metricCards[activeIndex];
    const input = activeCard.querySelector(`input[type="radio"][value="${score}"]`);

    if (input) {
      input.checked = true;
      moveToNextMetric();
    }
  }

  metricCards.forEach((card, index) => {
    card.addEventListener("click", () => {
      setActiveMetric(index);
    });

    card.querySelectorAll('input[type="radio"]').forEach(input => {
      input.addEventListener("change", () => {
        setActiveMetric(index);
        setTimeout(moveToNextMetric, 120);
      });
    });
  });

  document.addEventListener("keydown", function(event) {
    const activeElement = document.activeElement;
    const isTyping =
      activeElement &&
      (activeElement.tagName === "TEXTAREA" || activeElement.tagName === "INPUT");

    if (isTyping && activeElement.type !== "radio") {
      return;
    }

    if (["1", "2", "3", "4", "5"].includes(event.key)) {
      event.preventDefault();
      selectScore(event.key);
    }

    if (event.key.toLowerCase() === "n") {
      event.preventDefault();
      const btn = document.querySelector('button[value="next"]');
      btn.click();
    }

    if (event.key.toLowerCase() === "p") {
      event.preventDefault();
      const btn = document.querySelector('button[value="prev"]');
      btn.click();
    }
  });

  window.addEventListener("load", () => {
    const firstUnanswered = metricCards.findIndex(card => {
      return !card.querySelector('input[type="radio"]:checked');
    });

    setActiveMetric(firstUnanswered === -1 ? 0 : firstUnanswered);
  });
</script>

</body>
</html>
"""


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


def make_app(data_path, output_path):
    app = Flask(__name__)

    data = load_json(data_path)

    if output_path.exists():
        results = load_json(output_path)
    else:
        results = {}

    def completed_count():
        return sum(1 for item in data if item["uuid"] in results)

    @app.route("/")
    def home():
        return redirect(url_for("annotate", idx=0))

    @app.route("/item/<int:idx>")
    def annotate(idx):
        idx = max(0, min(idx, len(data) - 1))
        item = data[idx]
        existing = results.get(item["uuid"], {})

        return render_template_string(
            HTML,
            item=item,
            idx=idx,
            total=len(data),
            metrics=METRICS,
            existing=existing,
            completed=completed_count(),
        )

    @app.route("/save/<int:idx>", methods=["POST"])
    def save(idx):
        item = data[idx]
        uuid = item["uuid"]

        annotation = {
            "uuid": uuid,
            "domain": item.get("domain"),
            "type": item.get("type"),
            "question_type": item.get("question_type"),
            "query": item.get("query"),
            "notes": request.form.get("notes", ""),
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

        for key in METRICS:
            annotation[key] = int(request.form[key])

        annotation["mean_quality_score_1_to_4"] = sum(
            annotation[k] for k in QUALITY_KEYS
        ) / len(QUALITY_KEYS)

        results[uuid] = annotation
        save_json(output_path, results)

        action = request.form.get("action")
        if action == "prev":
            return redirect(url_for("annotate", idx=max(0, idx - 1)))
        if action == "next":
            return redirect(url_for("annotate", idx=min(len(data) - 1, idx + 1)))
        return redirect(url_for("annotate", idx=idx))

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="task4_data.json", help="Input JSON file")
    parser.add_argument("--out", default="task4_annotations.json", help="Output JSON file")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5001, type=int)
    args = parser.parse_args()

    app = make_app(Path(args.data), Path(args.out))
    app.run(host=args.host, port=args.port, debug=True)
