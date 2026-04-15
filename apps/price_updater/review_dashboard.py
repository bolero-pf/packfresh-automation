import pandas as pd
import sys
from functools import wraps
import subprocess
import threading
from flask import Flask, render_template, request, redirect, url_for, Response
from apscheduler.schedulers.background import BackgroundScheduler
import time
import os
import io
import csv
from pathlib import Path

import socket

_old_getaddrinfo = socket.getaddrinfo

def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _old_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

socket.getaddrinfo = ipv4_only_getaddrinfo
from dailyrunner import get_shopify_products, get_shopify_products_for_feed



app = Flask(__name__, template_folder="templates", static_folder="static")
from inventory.routes import bp as inventory_bp
app.register_blueprint(inventory_bp)

from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner"],
                    public_paths=('/health', '/ping', '/favicon.ico', '/reddit-feed.csv'),
                    public_prefixes=('/static', '/pf-static'),
                    skip_jwt_prefixes=('/price_update',))

REDDIT_FEED_USER = os.environ["REDDIT_USER_NAME"]
REDDIT_FEED_PASS = os.environ["REDDIT_USER_PASS"]
ROOT = Path(__file__).resolve().parent  # == .../price_updater
REVIEW_CSV    = ROOT / "price_updates_needs_review.csv"
PUSHED_CSV    = ROOT / "price_updates_pushed.csv"
MISSING_CSV   = ROOT / "price_updates_missing_listing.csv"
UNTOUCHED_CSV = ROOT / "price_updates_untouched.csv"
RUN_LOG = ROOT / "run_output.log"
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "price-updater-fallback-key")


@app.post("/price_update")
def price_update():
    """Trigger nightly price sync (dailyrunner.py) in background."""
    try:
        SCRIPT = ROOT / "dailyrunner.py"
        LOG = ROOT / "run_output.log"

        def launch():
            LOG.parent.mkdir(parents=True, exist_ok=True)
            with open(LOG, "a", buffering=1, encoding="utf-8") as f:
                f.write(f"\n=== RUN {__import__('datetime').datetime.now().isoformat()} ===\n")
                p = subprocess.Popen(
                    [sys.executable, "-u", str(SCRIPT)],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    cwd=str(ROOT), text=True, bufsize=1,
                    env={**os.environ, "PYTHONUNBUFFERED": "1"},
                )
                for line in p.stdout:
                    print(line, end="")
                    f.write(line)
                p.wait()
                f.write(f"=== EXIT code={p.returncode} at {__import__('datetime').datetime.now().isoformat()} ===\n")

        threading.Thread(target=launch, daemon=True).start()
        from flask import jsonify
        return jsonify({"ok": True, "started": True}), 200
    except Exception as e:
        from flask import jsonify
        return jsonify({"ok": False, "error": str(e)}), 500

FEED_COLUMNS = [
    "id",
    "title",
    "description",
    "link",
    "image_link",
    "price",
    "item_group_id",
    "gtin",
    "mpn",
    "google_product_category",
    "product_type",
    "brand",
    "adult",
    "is_bundle",
    "sale_price",
    "sale_price_effective_date",
    "cost_of_goods_sold",
    "mobile_link",
    "platform_specific_link",
    "additional_image_links",
    "lifestyle_image_link",
    "availability",
    "expiration_date",
    "condition",
    "age_group",
    "gender",
    "color",
    "size",
    "size_type",
    "material",
    "pattern",
    "product_detail",
    "product_highlight",
    "average_review_rating",
    "number_of_ratings",
    "custom_label_0",
    "custom_label_1",
    "custom_label_2",
    "custom_label_3",
    "custom_label_4",
    "custom_number_0",
    "custom_number_1",
    "custom_number_2",
    "custom_number_3",
    "custom_number_4",
]


if not os.path.exists(REVIEW_CSV):
    pd.DataFrame(columns=[
        "name", "tcgplayer_id", "shopify_price", "suggested_price", "price_to_upload",
        "shopify_qty", "variant_id", "shopify_inventory_id", "pending_shopify_update",
        "price_last_updated", "notes"
    ]).to_csv(REVIEW_CSV, index=False)


def call_dailyrunner():
    try:
        import requests
        auth = (os.environ.get("DASHBOARD_USER"), os.environ.get("DASHBOARD_PASS"))
        response = requests.get("http://localhost:5000/run-dailyrunner", auth=auth)
        print(f"🔁 Cron called /run-dailyrunner, status: {response.status_code}")
    except Exception as e:
        print(f"❌ Cron call failed: {e}")


def run_scrydex_sync():
    """Run the Scrydex nightly cache sync (active expansions only)."""
    try:
        scrydex_key = os.environ.get("SCRYDEX_API_KEY", "")
        scrydex_team = os.environ.get("SCRYDEX_TEAM_ID", "")
        if not scrydex_key or not scrydex_team:
            print("⏭ Scrydex sync skipped — SCRYDEX_API_KEY/SCRYDEX_TEAM_ID not set")
            return

        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
        from scrydex_client import ScrydexClient
        from scrydex_nightly import sync_expansion
        import db as shared_db

        shared_db.init_pool()
        client = ScrydexClient(scrydex_key, scrydex_team, db=shared_db)

        rows = shared_db.query("SELECT expansion_id FROM scrydex_sync_log WHERE active = TRUE")
        expansion_ids = [r["expansion_id"] for r in rows]
        if not expansion_ids:
            print("⏭ Scrydex sync: no active expansions in sync_log")
            return

        print(f"🔄 Scrydex sync: {len(expansion_ids)} active expansions")
        totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0}
        import time as _time
        t_start = _time.time()

        for i, eid in enumerate(expansion_ids):
            try:
                stats = sync_expansion(client, eid, shared_db)
                for k in totals:
                    totals[k] += stats.get(k, 0)
                if (i + 1) % 20 == 0:
                    print(f"  ... {i+1}/{len(expansion_ids)} done ({totals['credits']} credits)")
            except Exception as e:
                print(f"  ❌ {eid}: {e}")
            _time.sleep(0.05)

        elapsed = int(_time.time() - t_start)
        print(f"✅ Scrydex sync done in {elapsed}s — {totals['cards']} cards, "
              f"{totals['sealed']} sealed, {totals['prices']} prices, {totals['credits']} credits")
    except Exception as e:
        print(f"❌ Scrydex sync failed: {e}")
        import traceback
        traceback.print_exc()


if os.environ.get("ENABLE_CRON", "").lower() == "true":
    scheduler = BackgroundScheduler()
    scheduler.add_job(call_dailyrunner, "cron", hour=3)  # UTC
    scheduler.add_job(run_scrydex_sync, "cron", hour=4)  # UTC — after dailyrunner + analytics snapshot
    scheduler.start()
    print("✅ Scheduler started — dailyrunner at 3 AM UTC, Scrydex sync at 4 AM UTC")

def load_csv(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        # File exists but has no rows/headers → treat as empty table
        return pd.DataFrame()
def check_reddit_auth(username, password):
    return username == REDDIT_FEED_USER and password == REDDIT_FEED_PASS

def requires_reddit_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_reddit_auth(auth.username, auth.password):
            return Response(
                "Unauthorized",
                401,
                {"WWW-Authenticate": 'Basic realm="Reddit Product Feed"'},
            )
        return f(*args, **kwargs)
    return wrapper
@app.route("/")
def home():
    return redirect("/dashboard/review")

@app.route("/dashboard/runlog")
def runlog():
    return render_template("runlog.html")


@app.route('/run-scrydex-sync', methods=["GET", "POST"])
def trigger_scrydex_sync():
    """Manually trigger Scrydex cache sync."""
    from flask import jsonify
    threading.Thread(target=run_scrydex_sync, daemon=True).start()
    if request.method == "GET":
        return redirect("/dashboard/runlog")
    return jsonify({"ok": True, "started": True}), 200


@app.route('/run-dailyrunner', methods=["GET", "POST"])
def run_dailyrunner():
    def launch_script():
        with open(RUN_LOG, "w") as f:
            subprocess.Popen(
                [sys.executable, "dailyrunner.py"],
                stdout=f,
                stderr=f,
                cwd=str(ROOT)
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
        with open(RUN_LOG, "r") as f:
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
    if "shopify_qty" in df.columns:
        df["shopify_qty"] = pd.to_numeric(df["shopify_qty"], errors="coerce").fillna(0)
        df = df[df["shopify_qty"] > 0]
    else:
        df["shopify_qty"] = 0
    df["shopify_qty"] = pd.to_numeric(df["shopify_qty"], errors="coerce").fillna(0)
    df = df[df["shopify_qty"] > 0]

    if "tcgplayer_id" in df.columns:
        df["tcgplayer_id"] = df["tcgplayer_id"].astype(str).str.replace(".0", "", regex=False)
    else:
        df["tcgplayer_id"] = ""
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

    subprocess.Popen(cmd, cwd=str(ROOT))
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
            bufsize=1,
            cwd=str(ROOT)
        )

        for line in iter(process.stdout.readline, ''):
            yield f"data: {line.strip()}\n\n"

        yield "data: ✅ Upload complete.\n\n"

    return Response(generate(), mimetype="text/event-stream")

def map_variant_to_row(product, variant):
    # Strip HTML if needed
    import re
    body_html = product.get("body_html") or ""
    description = re.sub("<[^<]+?>", "", body_html)
    description = description.strip()
    if not description:
        description = product["title"]

    handle = product["handle"]
    variant_id = variant["id"]
    image_link = (
            (variant.get("image") or {}).get("src")
            or (product.get("image") or {}).get("src")
            or ""
    )
    if not image_link:
        return None  # or just skip this variant

    row = {
        "id": variant_id,
        "title": product["title"],
        "description": description[:5000],  # keep it sane
        "link": f"https://pack-fresh.com/products/{handle}?variant={variant_id}",
        "image_link": image_link,
        "price": f"{variant['price']} USD",
        "item_group_id": product["id"],
        "gtin": variant.get("barcode") or "",
        "mpn": "",
        "google_product_category": "Toys & Games > Games > Card Games > Collectible Card Games",
        "product_type": "TCG > Pokémon > Sealed",
        "brand": "Pack Fresh",
        "adult": "no",
        "is_bundle": "no",
        "sale_price": "",
        "sale_price_effective_date": "",
        "cost_of_goods_sold": "",
        "mobile_link": "",
        "platform_specific_link": "",
        "additional_image_links": "",
        "lifestyle_image_link": "",
        "availability": "in stock" if variant["inventory_quantity"] > 0 else "out of stock",
        "expiration_date": "",
        "condition": "new",
        "age_group": "",
        "gender": "",
        "color": "",
        "size": "",
        "size_type": "",
        "material": "",
        "pattern": "",
        "product_detail": "",
        "product_highlight": "",
        "average_review_rating": "",
        "number_of_ratings": "",
        "custom_label_0": "",
        "custom_label_1": "",
        "custom_label_2": "",
        "custom_label_3": "",
        "custom_label_4": "",
        "custom_number_0": "",
        "custom_number_1": "",
        "custom_number_2": "",
        "custom_number_3": "",
        "custom_number_4": "",
    }
    return row

@app.get("/reddit-feed.csv")
@requires_reddit_auth
def reddit_feed():
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=FEED_COLUMNS)
    writer.writeheader()

    for product in get_shopify_products_for_feed():
        for variant in product["variants"]:
            # optional: only include in-stock / visible variants
            if variant.get("inventory_quantity", 0) <= 0:
                continue
            row = map_variant_to_row(product, variant)
            if not row:
                continue
            writer.writerow(row)

    csv_data = output.getvalue()
    output.close()
    return Response(csv_data, mimetype="text/csv")

if __name__ == "__main__":
    app.run(debug=True, port=5000)


