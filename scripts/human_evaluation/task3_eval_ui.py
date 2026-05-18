import json
import argparse
from pathlib import Path
from datetime import datetime

from flask import Flask, request, redirect, render_template_string, url_for

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
    }
}

HTML = """
<!doctype html>
<html>
<head>
  <title>Query Evaluation</title>
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
  </style>
</head>

<body>

<div class="container">

  <div class="left">
    <h2>Input Data</h2>

    <div class="card">
      <b>UUID:</b> {{ item.uuid }} <br>
      <b>Domain:</b> {{ item.domain }}
    </div>

    <div class="card">
      <h3>Component Queries</h3>
      <ul>
        {% for q in item.component_queries %}
          <li>{{ q }}</li>
        {% endfor %}
      </ul>
    </div>

    <div class="card">
      <h3>Merged Query</h3>
      <pre>{{ item.query }}</pre>
    </div>

    <div class="card">
      <h3>Ground Truth Tools</h3>
      <pre>{{ item.gt_tools | tojson(indent=2) }}</pre>
    </div>

    <div class="card">
      <h3>Tool Responses</h3>
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
      Hover over numbers for score explanations. Keyboard: 1-4 = score active metric, N = Save & Next, P = Save & Previous.
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
      // subtle visual cue
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

    if (["1", "2", "3", "4"].includes(event.key)) {
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
            "query": item.get("query"),

            "faithfulness": int(request.form["faithfulness"]),
            "naturalness": int(request.form["naturalness"]),
            "logical_consistency": int(request.form["logical_consistency"]),
            "answer_leakage": int(request.form["answer_leakage"]),
            "context_sufficiency": int(request.form["context_sufficiency"]),

            "notes": request.form.get("notes", ""),
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }

        quality_keys = [
            "faithfulness",
            "naturalness",
            "logical_consistency",
            "answer_leakage",
            "context_sufficiency",
        ]

        annotation["mean_quality_score_1_to_4"] = sum(
            annotation[k] for k in quality_keys
        ) / len(quality_keys)

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
    parser.add_argument("--data", required=True, help="Input JSON file")
    parser.add_argument("--out", default="annotations.json", help="Output JSON file")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    args = parser.parse_args()

    app = make_app(Path(args.data), Path(args.out))
    app.run(host=args.host, port=args.port, debug=True)
