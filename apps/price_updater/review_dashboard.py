import os
import pandas as pd
import sys
import subprocess
import threading
import time
from datetime import datetime
from flask import Flask, render_template, render_template_string, request, redirect, url_for, Response

app = Flask(__name__, template_folder="templates", static_folder="static")

REVIEW_CSV = "price_updates_needs_review.csv"
PUSHED_CSV = "price_updates_pushed.csv"
MISSING_CSV = "price_updates_missing_listing.csv"
UNTOUCHED_CSV = "price_updates_untouched.csv"

if not os.path.exists(REVIEW_CSV):
    pd.DataFrame(columns=[
        "name", "tcgplayer_id", "shopify_price", "suggested_price", "price_to_upload",
        "shopify_qty", "variant_id", "shopify_inventory_id", "pending_shopify_update",
        "price_last_updated", "notes"
    ]).to_csv(REVIEW_CSV, index=False)
TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <title>Pack Fresh | Price Review Dashboard</title>
  <style>
    body {
        font-family: 'Segoe UI', sans-serif;
        background-color: #fcf7e1;
        color: #2a361c;
        padding: 40px;
    }
    h1, h2 {
        color: #616d39;
    }
    .nav a {
        margin-right: 20px;
        text-decoration: none;
        color: #2a361c;
        font-weight: bold;
    }
    .nav {
        margin-bottom: 20px;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        font-size: 14px;
        margin-top: 10px;
    }
    th, td {
        border: 1px solid #ba6b29;
        padding: 8px;
        text-align: left;
    }
    th {
        background-color: #dfa260;
        color: #000;
        cursor: pointer;
    }
    tr:nth-child(even) {
        background-color: #fff9ef;
    }
    tr:hover {
        background-color: #ffeacc;
    }
    input[type="text"] {
        width: 80px;
        padding: 4px;
    }
    button {
        background-color: #616d39;
        color: white;
        padding: 8px 12px;
        border: none;
        cursor: pointer;
        font-weight: bold;
    }
    button:hover {
        background-color: #2a361c;
    }
    #log {
        white-space: pre-wrap;
        background: #111;
        color: #0f0;
        padding: 1em;
        margin-top: 1em;
        height: 40vh;
        overflow-y: auto;
    }
  </style>
</head>
<body>

  <h1>📊 Pack Fresh Price Sync Dashboard</h1>

  <div class="nav">
      <a href="{{ url_for('dashboard', view='review') }}">Needs Review</a>
      <a href="{{ url_for('dashboard', view='pushed') }}">Auto-Updated</a>
      <a href="{{ url_for('dashboard', view='missing') }}">Missing Listings</a>
      <a href="{{ url_for('dashboard', view='untouched') }}">Untouched</a>
  </div>

  <h2>{{ title }}</h2>

  {% if df.empty %}
    <p>No entries found.</p>
  {% else %}
  <form method="post" action="{{ url_for('save_csv', view=view) }}">
    <table>
      <thead>
        <tr>
          {% for col in df.columns %}
            <th>{{ col.replace('_', ' ').title() }}</th>
          {% endfor %}
          {% if view in ['review', 'missing'] %}
            <th>Edit</th>
          {% endif %}
        </tr>
      </thead>
      <tbody>
        {% for i, row in df.iterrows() %}
        <tr>
          {% for col in df.columns %}
            {% if col == 'tcgplayer_id' %}
              <td><a href="https://www.tcgplayer.com/product/{{ row[col] }}?Language=English" target="_blank">{{ row[col] }}</a></td>
            {% else %}
              <td>{{ row[col] }}</td>
            {% endif %}
          {% endfor %}
          <td>DEBUG: {{ row.get('suggested_price') }}</td>
          {% if view in ['review', 'missing'] %}
            <td>
              <input type="text" name="price_to_upload_{{ i }}" value="{{ row.get('price_to_upload', '') }}" id="price_input_{{ i }}">
              {% if row.get('suggested_price') is not none %}
                <button type="button" onclick="document.getElementById('price_input_{{ i }}').value = '{{ row.get('suggested_price') }}'">✔ Accept</button>
              {% endif %}
            </td>
          {% endif %}
        </tr>
        {% endfor %}
      </tbody>
    </table>

    {% if view in ['review', 'missing'] %}
      <br>
      <button type="submit">💾 Save Changes</button>
    {% endif %}
  </form>

  <form method="POST" action="/run" style="margin-top:10px;">
    <input type="hidden" name="action" value="upload">
    <input type="hidden" name="source" value="{{ view }}">
    <button type="submit">🚀 Push Reviewed Prices to Shopify</button>
  </form>

  <div id="log"></div>
  {% endif %}
</body>
</html>
"""

def load_csv(path):
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

@app.route("/")
def home():
    return redirect("/dashboard/review")

@app.route("/dashboard/runlog")
def runlog():
    return render_template("runlog.html")


@app.route('/run-dailyrunner', methods=["GET", "POST"])
def run_dailyrunner():
    def launch_script():
        with open("run_output.log", "w") as f:
            subprocess.Popen(
                [sys.executable, "dailyrunner.py"],
                stdout=f,
                stderr=f
            )

    threading.Thread(target=launch_script).start()

    # If it's a browser visit, go to logs page
    if request.method == "GET":
        return redirect(url_for("runlog_page"))

    # If it's a cron POST, just return OK
    return "✅ dailyrunner.py triggered\n", 200

@app.route('/stream-log')
def stream_log():
    def generate():
        with open("run_output.log", "r") as f:
            f.seek(0, 2)  # Go to end of file
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.25)
                    continue
                yield f"data: {line.strip()}\n\n"
    return Response(generate(), mimetype="text/event-stream")

@app.route("/dashboard/<view>")
def dashboard(view):
    files = {
        "review": REVIEW_CSV,
        "pushed": PUSHED_CSV,
        "missing": MISSING_CSV,
        "untouched": UNTOUCHED_CSV
    }
    titles = {
        "review": "Needs Review",
        "pushed": "Auto-Updated Items",
        "missing": "Missing Listings",
        "untouched": "Untouched Listings"
    }
    df = load_csv(files.get(view, REVIEW_CSV))
    df["shopify_qty"] = pd.to_numeric(df["shopify_qty"], errors="coerce").fillna(0)
    df = df[df["shopify_qty"] > 0]

    df["tcgplayer_id"] = df.get("tcgplayer_id", "").astype(str).str.replace(".0", "", regex=False)
    if "price_to_upload" in df.columns:
        df["price_to_upload"] = df["price_to_upload"].fillna("")
    if "suggested_price" in df.columns:
        df["suggested_price"] = df["suggested_price"].fillna("")

    df = df.drop(columns=[col for col in ["handle", "variant_id"] if col in df.columns and col != "shopify_qty"])
    return render_template("review.html", df=df, title=titles.get(view, "Needs Review"), view=view)


@app.route("/save/<view>", methods=["POST"], endpoint='save_csv')
def save_csv(view):
    file_map = {
        "review": REVIEW_CSV,
        "missing": MISSING_CSV
    }
    filepath = file_map.get(view)
    if not filepath or not os.path.exists(filepath):
        return f"Invalid or missing file for view: {view}", 400

    df = pd.read_csv(filepath)
    for idx in df.index:
        val = request.form.get(f"price_to_upload_{idx}")
        if val is not None:
            df.at[idx, "price_to_upload"] = val
    df.to_csv(filepath, index=False)
    return redirect(f"/dashboard/{view}")

@app.route("/run", methods=["POST"])
def run():
    action = request.form.get("action")
    source = request.form.get("source", "review")  # default to review

    python_exec = sys.executable
    cmd = [python_exec, "dailyrunner.py"]

    if action == "upload":
        if source == "review":
            cmd.append("--upload-reviewed")
        elif source == "missing":
            cmd.append("--upload-missing")

    subprocess.Popen(cmd)
    return redirect(f"/dashboard/{source}")

@app.route("/ignore", methods=["POST"])
def ignore_sku():
    sku = request.form.get("sku")
    if not sku:
        return "Missing SKU", 400

    with open(".venv/Scripts/ignore_skus.txt", "a") as f:
        f.write(sku.strip() + "\n")

    return "OK", 200
@app.route("/run-live/upload")
def run_live_upload():
    def generate():
        process = subprocess.Popen(
            [sys.executable, "dailyrunner.py", "--upload-reviewed"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,  # <-- enables string mode instead of bytes
            encoding='utf-8',  # <-- force UTF-8 decoding
            bufsize=1
        )

        for line in iter(process.stdout.readline, ''):
            yield f"data: {line.strip()}\n\n"

        yield "data: ✅ Upload complete.\n\n"

    return Response(generate(), mimetype="text/event-stream")

from apscheduler.schedulers.background import BackgroundScheduler
import requests
import time
import os
from datetime import datetime

def call_dailyrunner():
    try:
        print(f"⏰ Auto-triggering /run-dailyrunner at {datetime.utcnow().isoformat()} UTC")
        requests.get("http://localhost:5000/run-dailyrunner", timeout=30)
    except Exception as e:
        print(f"🔥 Scheduled /run-dailyrunner failed: {e}")

if __name__ == "__main__":
    app.run(debug=True, port=5000)

if os.environ.get("ENABLE_CRON", "").lower() == "true":
    scheduler = BackgroundScheduler()
    scheduler.add_job(call_dailyrunner, "cron", hour=3)  # run at 3 AM UTC
    scheduler.start()
    print("✅ Scheduler started — /run-dailyrunner will fire daily at 3 AM UTC")
