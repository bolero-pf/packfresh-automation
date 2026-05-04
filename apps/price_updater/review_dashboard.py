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

from auth import register_auth_hooks, get_current_user
register_auth_hooks(app, roles=["owner"],
                    public_paths=('/health', '/ping', '/favicon.ico', '/reddit-feed.csv'),
                    public_prefixes=('/static', '/pf-static'),
                    skip_jwt_prefixes=('/price_update', '/run-raw-updater',
                                       '/run-scrydex-sync', '/run-slab-updater'))


def _authorized_trigger():
    """Allow authenticated owners OR valid X-Flow-Secret (Shopify Flow cron)."""
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    if get_current_user():
        return True
    return bool(flow_secret) and secret == flow_secret

REDDIT_FEED_USER = os.environ["REDDIT_USER_NAME"]
REDDIT_FEED_PASS = os.environ["REDDIT_USER_PASS"]
ROOT = Path(__file__).resolve().parent  # == .../price_updater
RUN_LOG = ROOT / "run_output.log"
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "price-updater-fallback-key")


def _shared_db():
    """Lazy-import the shared db module + ensure pool initialized."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()
    return shared_db


_PRICING = None


def _pricing():
    """Lazy-init the shared PriceProvider (cache + Scrydex). Used by the
    raw-rebind search modal so it shares one source of truth with intake's
    /api/search/cards — see shared/price_provider.py:search_cards."""
    global _PRICING
    if _PRICING is None:
        db = _shared_db()
        from price_provider import create_price_provider
        _PRICING = create_price_provider(db=db)
    return _PRICING


# ── Runner launcher ──────────────────────────────────────────────────────
#
# Every "Run X now" button used to either spawn a subprocess (sealed) OR
# kick off threading.Thread(target=...) (slab/raw/scrydex). Two problems
# with the threading path: (a) the runners' print() output went to the
# Flask process's stdout — Railway logs but NOT RUN_LOG — so /dashboard/
# runlog only ever showed sealed output, and (b) clicking "Run X" while
# X was already running would happily start a second copy in parallel,
# burning Scrydex credits and racing on Shopify writes.
#
# The launcher below subprocesses every runner (Popen + line-buffered
# tee into RUN_LOG) and holds a per-runner-key entry in _RUNNER_PIDS for
# the duration of the run. Re-clicks while a runner is live get a 409.

_RUNNER_PIDS: dict[str, int] = {}
_RUNNER_LOCK = threading.Lock()


def _runner_is_alive(key: str) -> bool:
    """Return True iff a previous launch for this key is still running.
    We don't poll Popen.poll() here because we don't keep the Popen
    handle around — the launcher thread cleans up _RUNNER_PIDS itself."""
    return key in _RUNNER_PIDS


def _launch_runner(*, key: str, label: str, cmd: list[str]):
    """Spawn `cmd` as a subprocess, tee stdout/stderr to RUN_LOG, and
    track its PID under `key`. Idempotent: returns False if already
    running. Always returns True if a new run was kicked off."""
    with _RUNNER_LOCK:
        if key in _RUNNER_PIDS:
            return False
        _RUNNER_PIDS[key] = -1  # placeholder so a racing caller sees us busy

    def _go():
        RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(RUN_LOG, "a", buffering=1, encoding="utf-8") as f:
                ts = __import__('datetime').datetime.now().isoformat(timespec='seconds')
                f.write(f"\n=== {label} START {ts} ===\n")
                try:
                    p = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        cwd=str(ROOT), text=True, bufsize=1,
                        env={**os.environ, "PYTHONUNBUFFERED": "1"},
                    )
                except Exception as e:
                    f.write(f"!!! {label} failed to spawn: {e}\n")
                    return
                with _RUNNER_LOCK:
                    _RUNNER_PIDS[key] = p.pid
                for line in p.stdout:
                    print(line, end="")
                    f.write(line)
                p.wait()
                ts2 = __import__('datetime').datetime.now().isoformat(timespec='seconds')
                f.write(f"=== {label} EXIT code={p.returncode} {ts2} ===\n")
        finally:
            with _RUNNER_LOCK:
                _RUNNER_PIDS.pop(key, None)

    threading.Thread(target=_go, daemon=True).start()
    return True


@app.get("/api/runner-status")
def api_runner_status():
    """Snapshot of which runners are currently live. Cheap to poll."""
    from flask import jsonify
    with _RUNNER_LOCK:
        snapshot = dict(_RUNNER_PIDS)
    return jsonify({"running": snapshot})


@app.post("/price_update")
def price_update():
    """Trigger nightly sealed price sync (dailyrunner.py) in background.
    Idempotent — re-firing while a sealed run is live returns 409."""
    if not _authorized_trigger():
        from flask import jsonify
        return jsonify({"error": "Unauthorized"}), 401
    started = _launch_runner(
        key="sealed", label="SEALED",
        cmd=[sys.executable, "-u", str(ROOT / "dailyrunner.py")],
    )
    from flask import jsonify
    if not started:
        return jsonify({"ok": False, "error": "sealed updater is already running"}), 409
    return jsonify({"ok": True, "started": True}), 200

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


def call_dailyrunner():
    """APScheduler entrypoint for the nightly sealed run. Routes through
    the same launcher as the manual button so it gets the same lock +
    log tee, and a manually-triggered run still in progress at cron
    time is left alone (logged as skipped) instead of doubled up."""
    started = _launch_runner(
        key="sealed", label="SEALED CRON",
        cmd=[sys.executable, "-u", str(ROOT / "dailyrunner.py")],
    )
    if not started:
        print("[cron] sealed updater already running — skipping cron fire")


def _cron_scrydex_sync():
    started = _launch_runner(
        key="scrydex", label="SCRYDEX CRON",
        cmd=[sys.executable, "-u", str(ROOT / "run_scrydex_sync.py")],
    )
    if not started:
        print("[cron] scrydex sync already running — skipping cron fire")


def _cron_slab_updater():
    started = _launch_runner(
        key="slab", label="SLAB CRON",
        cmd=[sys.executable, "-u", str(ROOT / "slab_updater.py")],
    )
    if not started:
        print("[cron] slab updater already running — skipping cron fire")


def _cron_raw_updater():
    started = _launch_runner(
        key="raw", label="RAW CRON",
        cmd=[sys.executable, "-u", str(ROOT / "raw_card_updater.py")],
    )
    if not started:
        print("[cron] raw updater already running — skipping cron fire")


if os.environ.get("ENABLE_CRON", "").lower() == "true":
    scheduler = BackgroundScheduler()
    scheduler.add_job(call_dailyrunner,    "cron", hour=3)  # UTC
    scheduler.add_job(_cron_scrydex_sync,  "cron", hour=4)  # after dailyrunner
    scheduler.add_job(_cron_slab_updater,  "cron", hour=5)  # after Scrydex sync
    # Raw card updater is fired by an external Shopify Flow today, but a
    # local cron entry would slot in here at hour=6 if you ever wanted to
    # decouple it from Flow.
    scheduler.start()
    print("✅ Scheduler started — sealed 3AM, Scrydex 4AM, slabs 5AM UTC")

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
    """Landing page is a static nav hub — works whether or not the
    sealed_price_runs migration has been applied yet, and whether or not
    a run has populated data."""
    return render_template("home.html")

@app.route("/dashboard/runlog")
def runlog():
    return render_template("runlog.html")


@app.route('/run-scrydex-sync', methods=["GET", "POST"])
def trigger_scrydex_sync():
    """Manually trigger Scrydex cache sync. Subprocess so output streams
    into RUN_LOG (visible at /dashboard/runlog). 409 if already running."""
    from flask import jsonify
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    started = _launch_runner(
        key="scrydex", label="SCRYDEX",
        cmd=[sys.executable, "-u", str(ROOT / "run_scrydex_sync.py")],
    )
    if not started:
        if request.method == "GET":
            return redirect("/dashboard/runlog?msg=already-running")
        return jsonify({"ok": False, "error": "scrydex sync is already running"}), 409
    if request.method == "GET":
        return redirect("/dashboard/runlog")
    return jsonify({"ok": True, "started": True}), 200


@app.route('/run-slab-updater', methods=["GET", "POST"])
def trigger_slab_updater():
    """Manually trigger the graded slab price updater. Subprocess so output
    streams into RUN_LOG. 409 if already running."""
    from flask import jsonify
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    started = _launch_runner(
        key="slab", label="SLAB",
        cmd=[sys.executable, "-u", str(ROOT / "slab_updater.py")],
    )
    if not started:
        if request.method == "GET":
            return redirect("/dashboard/slab-runs?msg=already-running")
        return jsonify({"ok": False, "error": "slab updater is already running"}), 409
    if request.method == "GET":
        return redirect("/dashboard/slab-runs")
    return jsonify({"ok": True, "started": True}), 200


@app.route('/dashboard/slab-runs')
def slab_runs_list():
    """List recent slab_updater runs with summary counts per action."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()
    # Counts only consider in-stock rows (qty > 0). OOS rows are kept in the
    # DB for the audit trail but aren't part of Sean's review queue.
    rows = shared_db.query("""
        SELECT run_id,
               MIN(started_at) AS started_at,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0) AS total,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0 AND action = 'adjusted')         AS adjusted,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0 AND action = 'flag_overpriced')  AS flag_over,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0 AND action = 'flag_underpriced') AS flag_under,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0 AND action = 'ok')               AS ok,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0 AND action = 'skip')             AS skipped,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) > 0 AND action = 'error')            AS errors,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) = 0)                                 AS oos_hidden
        FROM slab_price_runs
        GROUP BY run_id
        ORDER BY started_at DESC
        LIMIT 60
    """)
    return render_template("slab_runs.html", runs=rows)


@app.route('/dashboard/slab-runs/<run_id>')
def slab_run_detail(run_id):
    """Show rows from one slab_updater run.
    Default hides qty=0 (sold/inactive) — Sean keeps those listings for
    bookkeeping but they're noise in the price-review queue. Pass
    ?include_oos=1 to show them anyway."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    action_filter = (request.args.get("action") or "").strip()
    include_oos   = (request.args.get("include_oos") or "").strip() == "1"

    where = ["run_id = %s"]
    params: list = [run_id]
    if action_filter:
        where.append("action = %s"); params.append(action_filter)
    if not include_oos:
        where.append("COALESCE(qty, 0) > 0")
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT id, title, sku, qty, company, grade, tcgplayer_id,
               old_price, new_price, suggested_price, median, low_comp, high_comp,
               comps_count, delta_pct, trend_7d, action, reason,
               product_gid, variant_gid, started_at,
               apply_status, applied_at, applied_price
        FROM slab_price_runs
        WHERE {where_sql}
        ORDER BY
            CASE action
                WHEN 'error'            THEN 0
                WHEN 'adjusted'         THEN 1
                WHEN 'flag_underpriced' THEN 2
                WHEN 'flag_overpriced'  THEN 3
                WHEN 'skip'             THEN 4
                WHEN 'ok'               THEN 5
                ELSE 6
            END,
            ABS(COALESCE(delta_pct, 0)) DESC NULLS LAST,
            title
    """
    rows = shared_db.query(sql, tuple(params))

    summary = shared_db.query_one(f"""
        SELECT MIN(started_at) AS started_at, COUNT(*) AS total,
               COUNT(*) FILTER (WHERE COALESCE(qty,0) = 0) AS oos_count
        FROM slab_price_runs WHERE run_id = %s
    """, (run_id,))

    return render_template(
        "slab_run_detail.html",
        run_id=run_id,
        rows=rows,
        summary=summary,
        action_filter=action_filter,
        include_oos=include_oos,
        store_domain=os.environ.get("SHOPIFY_STORE", ""),
    )


@app.route('/dashboard/slab-runs/row/<int:row_id>/apply', methods=["POST"])
def slab_run_apply_row(row_id):
    """Apply a single flagged row's suggested price to Shopify and mark
    the row as applied. Body may include {price: 12.99} to override the
    suggested charm-rounded price."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import db as shared_db
    shared_db.init_pool()
    from slab_updater import update_variant_price

    row = shared_db.query_one("""
        SELECT id, product_gid, variant_gid, suggested_price, old_price,
               apply_status, title
        FROM slab_price_runs WHERE id = %s
    """, (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if row["apply_status"] in ("applied", "dismissed"):
        return jsonify({"ok": False, "error": f"row already {row['apply_status']}"}), 409
    if not row["product_gid"] or not row["variant_gid"]:
        return jsonify({"ok": False, "error": "row has no Shopify identifiers"}), 400

    body = request.get_json(silent=True) or {}
    override = body.get("price")
    target_price = float(override) if override is not None else float(row["suggested_price"] or 0)
    if target_price <= 0:
        return jsonify({"ok": False, "error": "no valid price to apply"}), 400

    try:
        update_variant_price(row["product_gid"], row["variant_gid"], target_price)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Shopify update failed: {e}"}), 502

    shared_db.execute("""
        UPDATE slab_price_runs
        SET apply_status = 'applied', applied_at = NOW(), applied_price = %s
        WHERE id = %s
    """, (target_price, row_id))
    return jsonify({
        "ok": True, "applied_price": target_price, "title": row["title"],
    })


@app.route('/dashboard/slab-runs/row/<int:row_id>/suggest-tcg', methods=["POST"])
def slab_run_suggest_tcg(row_id):
    """Suggest candidate TCGPlayer IDs for a skipped 'no tcg_id' row.

    Reuses slab_backfill's title parser + cache scorer — no Scrydex credits
    consumed when the cache already covers the card. Returns up to 5
    candidates sorted by match score.
    """
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import db as shared_db
    shared_db.init_pool()

    row = shared_db.query_one(
        "SELECT id, title FROM slab_price_runs WHERE id = %s", (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404

    from slab_backfill import parse_title, _candidates_from_cache, _score_candidates
    parsed = parse_title(row["title"])
    cands = _candidates_from_cache(parsed, shared_db)
    scored = _score_candidates(parsed, cands)

    # Enrich top 5 with image + card_number from cache
    out = []
    seen_tcg = set()
    for score, r in scored[:12]:
        tcg = r.get("tcgplayer_id")
        if not tcg or tcg in seen_tcg:
            continue
        seen_tcg.add(tcg)
        img = shared_db.query_one("""
            SELECT image_small, card_number FROM scrydex_price_cache
            WHERE scrydex_id = %s AND tcgplayer_id = %s LIMIT 1
        """, (r["scrydex_id"], tcg))
        out.append({
            "tcgplayer_id":   tcg,
            "scrydex_id":     r["scrydex_id"],
            "product_name":   r["product_name"],
            "expansion_name": r["expansion_name"],
            "variant":        r.get("variant"),
            "card_number":    (img or {}).get("card_number") or r.get("printed_number"),
            "image_small":    (img or {}).get("image_small"),
            "score":          score,
        })
        if len(out) >= 5:
            break

    return jsonify({
        "ok": True,
        "parsed": {"game": parsed["game"], "card_number": parsed["card_number"]},
        "candidates": out,
    })


@app.route('/dashboard/slab-runs/row/<int:row_id>/set-tcg', methods=["POST"])
def slab_run_set_tcg(row_id):
    """Write a TCGPlayer ID metafield on the Shopify product, then re-price
    this one variant inline. Body: {tcgplayer_id: int}.

    On success the slab_price_runs row is updated with fresh pricing (action
    flips from 'skip' to ok/flag_under/flag_over) and the new state is
    returned so the UI can render Apply/Dismiss buttons in place.
    """
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import db as shared_db
    shared_db.init_pool()

    body = request.get_json(silent=True) or {}
    try:
        tcg_id = int(body.get("tcgplayer_id"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "tcgplayer_id required (int)"}), 400
    if tcg_id <= 0:
        return jsonify({"ok": False, "error": "tcgplayer_id must be positive"}), 400

    row = shared_db.query_one("""
        SELECT id, product_gid, variant_gid, title, company, grade,
               old_price, qty, cost_basis
        FROM slab_price_runs WHERE id = %s
    """, (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if not row["product_gid"]:
        return jsonify({"ok": False, "error": "row has no Shopify product"}), 400

    # Write metafield
    from slab_backfill import _set_tcg_metafield
    try:
        _set_tcg_metafield(row["product_gid"], tcg_id)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Shopify metafield write failed: {e}"}), 502

    # Re-price this one variant using the same logic as slab_updater.run()
    from graded_pricing import get_live_graded_comps
    from slab_updater import charm_ceil

    company = row["company"]
    grade   = row["grade"]
    if not company or not grade:
        # Row was skipped for "no grade in title" — can't price without a grade
        shared_db.execute("""
            UPDATE slab_price_runs SET tcgplayer_id = %s WHERE id = %s
        """, (tcg_id, row_id))
        return jsonify({
            "ok": True, "tcgplayer_id": tcg_id,
            "action": "skip", "reason": "no grade in title — fix title then rerun",
        })

    comps = get_live_graded_comps(tcg_id, company, grade, shared_db)
    if not comps or not comps.get("market"):
        shared_db.execute("""
            UPDATE slab_price_runs SET tcgplayer_id = %s,
                action = 'skip', reason = 'no comp data'
            WHERE id = %s
        """, (tcg_id, row_id))
        return jsonify({
            "ok": True, "tcgplayer_id": tcg_id,
            "action": "skip", "reason": "no comp data",
        })

    current = float(row["old_price"] or 0)
    market  = float(comps["market"])
    cost    = float(row["cost_basis"] or 0)
    comps_n = comps.get("comps_count")

    # Decision target is charm_ceil(market) — see slab_updater.run() for why.
    safe_price  = max(market, cost) if cost else market
    charm_price = charm_ceil(safe_price)
    target      = charm_price or market
    delta_pct   = ((current - target) / target * 100) if target > 0 else 0

    if abs(delta_pct) <= 10:
        action, reason, suggested = "ok", f"within 10% of target ${target:.2f} (delta {delta_pct:+.1f}%)", None
    elif delta_pct > 10:
        action = "flag_overpriced"
        reason = f"{delta_pct:+.1f}% over target ${target:.2f} — review"
        suggested = charm_price
    else:
        action = "flag_underpriced"
        reason = f"{delta_pct:+.1f}% below target ${target:.2f} — review"
        suggested = charm_price

    shared_db.execute("""
        UPDATE slab_price_runs
           SET tcgplayer_id = %s,
               median = %s, low_comp = %s, high_comp = %s,
               comps_count = %s, trend_7d = %s, delta_pct = %s,
               suggested_price = %s, action = %s, reason = %s
         WHERE id = %s
    """, (
        tcg_id, market, comps.get("low"), comps.get("high"),
        comps_n, comps.get("trend_7d_pct"), round(delta_pct, 1),
        suggested, action, reason, row_id,
    ))

    return jsonify({
        "ok": True, "tcgplayer_id": tcg_id,
        "action": action, "reason": reason,
        "median": market, "low": comps.get("low"), "high": comps.get("high"),
        "comps_count": comps_n, "delta_pct": round(delta_pct, 1),
        "trend_7d": comps.get("trend_7d_pct"),
        "suggested_price": suggested,
    })


@app.route('/run-raw-updater', methods=["GET", "POST"])
def trigger_raw_updater():
    """Manually trigger the raw card price updater. Subprocess so output
    streams into RUN_LOG (visible at /dashboard/runlog). 409 if already
    running. Designed to be hit nightly from a Shopify Flow."""
    from flask import jsonify
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    started = _launch_runner(
        key="raw", label="RAW",
        cmd=[sys.executable, "-u", str(ROOT / "raw_card_updater.py")],
    )
    if not started:
        if request.method == "GET":
            return redirect("/dashboard/raw-runs?msg=already-running")
        return jsonify({"ok": False, "error": "raw updater is already running"}), 409
    if request.method == "GET":
        return redirect("/dashboard/raw-runs")
    return jsonify({"ok": True, "started": True}), 200


@app.route('/dashboard/raw-runs')
def raw_runs_list():
    """List recent raw_card_updater runs with summary counts per action."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()
    rows = shared_db.query("""
        SELECT run_id,
               MIN(started_at)                                      AS started_at,
               COUNT(*)                                             AS total,
               COUNT(*) FILTER (WHERE action = 'auto_applied')      AS auto_applied,
               COUNT(*) FILTER (WHERE action = 'flag_overpriced')   AS flag_over,
               COUNT(*) FILTER (WHERE action = 'flag_underpriced')  AS flag_under,
               COUNT(*) FILTER (WHERE action = 'ok')                AS ok,
               COUNT(*) FILTER (WHERE action = 'skip')              AS skipped,
               COUNT(*) FILTER (WHERE action = 'error')             AS errors
        FROM raw_card_price_runs
        GROUP BY run_id
        ORDER BY started_at DESC
        LIMIT 60
    """)
    return render_template("raw_runs.html", runs=rows)


@app.route('/dashboard/raw-runs/<run_id>')
def raw_run_detail(run_id):
    """Grouped view of one raw card pricing run.

    Identical raw cards (same card_name + set_name + card_number + variant +
    condition) collapse into a single row even if there are 25 copies — they
    all point to the same SKU semantically and must price together. The per-
    row apply endpoint still exists (defense in depth); the UI defaults to
    group-apply via /dashboard/raw-runs/group/apply.

    Cost basis can vary across copies in a group (bought at different times),
    so we surface min/max/avg rather than averaging silently. current_price
    *should* be uniform across the group; if it's not, the group is flagged
    as inconsistent so staff can decide whether to uniformize.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    action_filter = (request.args.get("action") or "").strip()
    params = [run_id]

    # Group identity: (card_name, set_name, card_number, variant, condition).
    # NULL variant collapses to '' so single-printing cards group cleanly. We
    # carry the full set of per-row primitives (id, raw_card_id, old_price,
    # apply_status, bin_label) inside aggregate arrays so the UI can:
    #   - show how many copies and where they live
    #   - detect inconsistent old_price within the group
    #   - issue one apply call covering every row_id in the group
    where_action = ""
    if action_filter:
        where_action = "AND action = %s"
        params.append(action_filter)

    sql = f"""
        WITH base AS (
            SELECT r.id, r.raw_card_id, r.barcode, r.card_name, r.set_name,
                   r.card_number, r.condition, r.variant, r.cost_basis,
                   r.tcgplayer_id, r.scrydex_id,
                   r.old_price, r.new_price, r.suggested_price,
                   r.cache_market, r.cache_low, r.delta_pct,
                   r.action, r.reason, r.apply_status, r.applied_at,
                   r.applied_price, r.started_at,
                   sl.bin_label AS bin_label
            FROM raw_card_price_runs r
            LEFT JOIN raw_cards rc      ON rc.id = r.raw_card_id
            LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
            WHERE r.run_id = %s
              {where_action}
        )
        SELECT
            -- group identity (used as a stable key in the UI)
            card_name, set_name, card_number, condition,
            COALESCE(variant, '') AS variant,
            -- one representative action / pricing snapshot for the group.
            -- All rows in a group share suggested_price (same scrydex lookup).
            -- For action we pick the most-actionable (worst delta) pillar
            -- so a group with mixed flag_over + ok still surfaces as a flag.
            (ARRAY_AGG(action ORDER BY
                CASE action
                    WHEN 'error'            THEN 0
                    WHEN 'flag_underpriced' THEN 1
                    WHEN 'flag_overpriced'  THEN 2
                    WHEN 'auto_applied'     THEN 3
                    WHEN 'ok'               THEN 4
                    WHEN 'skip'             THEN 5
                    ELSE 6
                END))[1] AS action,
            MAX(reason)          AS reason,
            MAX(suggested_price) AS suggested_price,
            MAX(cache_market)    AS cache_market,
            MAX(cache_low)       AS cache_low,
            MAX(delta_pct)       AS delta_pct,
            MAX(new_price)       AS new_price,
            -- Identity for the Block button. Groups share identity by
            -- construction so MAX is just "pick the non-null one".
            MAX(scrydex_id)      AS scrydex_id,
            MAX(tcgplayer_id)    AS tcgplayer_id,
            -- per-copy spread
            COUNT(*)             AS copies,
            MIN(cost_basis)      AS cost_basis_min,
            MAX(cost_basis)      AS cost_basis_max,
            AVG(cost_basis)      AS cost_basis_avg,
            MIN(old_price)       AS old_price_min,
            MAX(old_price)       AS old_price_max,
            COUNT(DISTINCT old_price) AS distinct_old_prices,
            -- arrays the UI / apply endpoint need
            ARRAY_AGG(id ORDER BY id)            AS row_ids,
            ARRAY_AGG(raw_card_id ORDER BY id)   AS raw_card_ids,
            ARRAY_AGG(barcode ORDER BY id)       AS barcodes,
            ARRAY_AGG(old_price ORDER BY id)     AS old_prices,
            ARRAY_AGG(cost_basis ORDER BY id)    AS cost_bases,
            ARRAY_AGG(apply_status ORDER BY id)  AS apply_statuses,
            ARRAY_AGG(applied_price ORDER BY id) AS applied_prices,
            ARRAY_AGG(DISTINCT bin_label) FILTER (WHERE bin_label IS NOT NULL) AS bin_labels
        FROM base
        GROUP BY card_name, set_name, card_number, condition,
                 COALESCE(variant, '')
        ORDER BY
            CASE (ARRAY_AGG(action ORDER BY
                CASE action
                    WHEN 'error'            THEN 0
                    WHEN 'flag_underpriced' THEN 1
                    WHEN 'flag_overpriced'  THEN 2
                    WHEN 'auto_applied'     THEN 3
                    WHEN 'ok'               THEN 4
                    WHEN 'skip'             THEN 5
                    ELSE 6
                END))[1]
                WHEN 'error'            THEN 0
                WHEN 'flag_underpriced' THEN 1
                WHEN 'flag_overpriced'  THEN 2
                WHEN 'auto_applied'     THEN 3
                WHEN 'ok'               THEN 4
                WHEN 'skip'             THEN 5
                ELSE 6
            END,
            ABS(COALESCE(MAX(delta_pct), 0)) DESC NULLS LAST,
            card_name
    """
    groups = shared_db.query(sql, tuple(params))

    summary = shared_db.query_one("""
        SELECT MIN(started_at) AS started_at, COUNT(*) AS total
        FROM raw_card_price_runs WHERE run_id = %s
    """, (run_id,))

    return render_template(
        "raw_run_detail.html",
        run_id=run_id, groups=groups, summary=summary,
        action_filter=action_filter,
    )


@app.route('/dashboard/raw-runs/row/<int:row_id>/apply', methods=["POST"])
def raw_run_apply_row(row_id):
    """Apply a single flagged row — write the suggested price (or override)
    to raw_cards.current_price. Body may include {price: 12.99}."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    row = shared_db.query_one("""
        SELECT id, raw_card_id, suggested_price, apply_status, card_name
        FROM raw_card_price_runs WHERE id = %s
    """, (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if row["apply_status"] in ("applied", "dismissed"):
        return jsonify({"ok": False, "error": f"row already {row['apply_status']}"}), 409
    if not row["raw_card_id"]:
        return jsonify({"ok": False, "error": "row has no raw_card_id"}), 400

    body = request.get_json(silent=True) or {}
    override = body.get("price")
    target = float(override) if override is not None else float(row["suggested_price"] or 0)
    if target <= 0:
        return jsonify({"ok": False, "error": "no valid price to apply"}), 400

    try:
        shared_db.execute(
            "UPDATE raw_cards SET current_price = %s, last_price_update = NOW() WHERE id = %s",
            (round(target, 2), row["raw_card_id"]))
        shared_db.execute("""
            UPDATE raw_card_price_runs
            SET apply_status='applied', applied_at=NOW(), applied_price=%s
            WHERE id = %s
        """, (target, row_id))
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB update failed: {e}"}), 502
    return jsonify({"ok": True, "applied_price": target, "card_name": row["card_name"]})


@app.route('/dashboard/raw-runs/group/apply', methods=["POST"])
def raw_run_apply_group():
    """Apply a single price to every copy in a group.

    Body: {row_ids: [int, ...], price?: float}
      - row_ids: every raw_card_price_runs.id in the group (frontend reads
        these from the rendered group's data attribute).
      - price (optional): override; otherwise uses the group's
        suggested_price (uniform across the group by construction).

    Writes raw_cards.current_price for every linked raw_card_id and marks
    every run row applied — in one transaction so a 25-copy group is one
    click. Already-applied or already-dismissed rows in the group are
    skipped (idempotent), not failed.
    """
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    body = request.get_json(silent=True) or {}
    row_ids = body.get("row_ids") or []
    if not isinstance(row_ids, list) or not row_ids:
        return jsonify({"ok": False, "error": "row_ids required (non-empty list)"}), 400
    try:
        row_ids = [int(x) for x in row_ids]
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "row_ids must be ints"}), 400

    rows = shared_db.query("""
        SELECT id, raw_card_id, suggested_price, apply_status, card_name
        FROM raw_card_price_runs
        WHERE id = ANY(%s)
    """, (row_ids,))
    if not rows:
        return jsonify({"ok": False, "error": "no matching rows"}), 404

    # Filter out already-resolved + rows missing raw_card_id (skip rows that
    # never matched a real card don't have one). Idempotent — caller may
    # re-click the group apply button without it failing on partial state.
    pending = [r for r in rows
               if r["apply_status"] not in ("applied", "dismissed")
               and r["raw_card_id"]]
    if not pending:
        return jsonify({
            "ok": True, "applied_count": 0, "skipped_count": len(rows),
            "reason": "all rows already resolved",
        })

    override = body.get("price")
    if override is not None:
        try:
            target = float(override)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "price must be numeric"}), 400
    else:
        # All rows in a real group share suggested_price. Defensive: take
        # the first non-null. If they actually differ (caller fed wrong
        # row_ids), reject — don't silently apply the wrong price.
        suggesteds = {float(r["suggested_price"]) for r in pending
                      if r["suggested_price"] is not None}
        if not suggesteds:
            return jsonify({"ok": False, "error": "no suggested_price on group"}), 400
        if len(suggesteds) > 1:
            return jsonify({
                "ok": False,
                "error": f"group has divergent suggested prices {sorted(suggesteds)} — "
                         f"pass an explicit price",
            }), 400
        target = next(iter(suggesteds))

    if target <= 0:
        return jsonify({"ok": False, "error": "no valid price to apply"}), 400

    target = round(target, 2)
    raw_card_ids = [r["raw_card_id"] for r in pending]
    pending_ids  = [r["id"] for r in pending]

    try:
        shared_db.execute(
            "UPDATE raw_cards SET current_price = %s, last_price_update = NOW() "
            "WHERE id = ANY(%s::uuid[])",
            (target, raw_card_ids),
        )
        shared_db.execute("""
            UPDATE raw_card_price_runs
               SET apply_status='applied', applied_at=NOW(), applied_price=%s
             WHERE id = ANY(%s)
        """, (target, pending_ids))
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB update failed: {e}"}), 502

    return jsonify({
        "ok": True,
        "applied_count": len(pending_ids),
        "skipped_count": len(rows) - len(pending_ids),
        "applied_price": target,
        "card_name": pending[0]["card_name"],
    })


@app.route('/dashboard/raw-runs/group/dismiss', methods=["POST"])
def raw_run_dismiss_group():
    """Mark every pending row in a group as dismissed in one call.
    Body: {row_ids: [int, ...]}. Already-resolved rows are left alone."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    body = request.get_json(silent=True) or {}
    row_ids = body.get("row_ids") or []
    if not isinstance(row_ids, list) or not row_ids:
        return jsonify({"ok": False, "error": "row_ids required (non-empty list)"}), 400
    try:
        row_ids = [int(x) for x in row_ids]
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "row_ids must be ints"}), 400

    try:
        shared_db.execute("""
            UPDATE raw_card_price_runs
               SET apply_status='dismissed', applied_at=NOW()
             WHERE id = ANY(%s)
               AND apply_status NOT IN ('applied', 'dismissed')
        """, (row_ids,))
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB update failed: {e}"}), 502
    return jsonify({"ok": True})


@app.route('/dashboard/raw-runs/row/<int:row_id>/dismiss', methods=["POST"])
def raw_run_dismiss_row(row_id):
    """Mark a flagged row as dismissed — leaves raw_cards.current_price alone."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()
    row = shared_db.query_one(
        "SELECT id, apply_status FROM raw_card_price_runs WHERE id = %s", (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if row["apply_status"] in ("applied", "dismissed"):
        return jsonify({"ok": False, "error": f"row already {row['apply_status']}"}), 409
    shared_db.execute(
        "UPDATE raw_card_price_runs SET apply_status='dismissed', applied_at=NOW() WHERE id = %s",
        (row_id,))
    return jsonify({"ok": True})


# ── Manual scrydex rebind for raw_cards the updater can't price ──────────
#
# Surfaces in-stock raw_cards whose (scrydex_id|tcgplayer_id, variant,
# condition) doesn't resolve in scrydex_price_cache. The candidate query
# mirrors raw_card_updater._lookup_cache_price exactly, so a row appears
# here iff the nightly run would skip it. Operator picks the right
# (scrydex_id, variant) via the search modal; we update raw_cards in
# place for every physical copy of that identity.

_REBIND_CANDIDATES_SQL = """
-- A raw_card needs a rebind only when its (scrydex_id|tcgplayer_id, variant)
-- has no raw market_price row in cache AT ALL. Per-condition rows aren't
-- required: raw_card_updater._lookup_cache_price falls back to NM ×
-- FALLBACK_MULTIPLIERS[condition] (DMG=0.25, HP=0.45, MP=0.65, LP=0.80)
-- when the exact condition is missing. So a JP card whose cache only has
-- NM=12000 JPY still gets a derived DMG price overnight; surfacing it
-- here would be a misleading "checklist that doesn't update" — operator
-- rebinds, list still shows the card, nightly run prices it correctly.
-- Stay in lockstep with raw_card_updater._lookup_cache_price.
SELECT
    i.card_name,
    i.set_name,
    i.card_number,
    i.condition,
    COALESCE(i.variant, '') AS variant,
    i.tcgplayer_id,
    i.scrydex_id,
    COUNT(*)                          AS copies,
    ARRAY_AGG(i.id::text ORDER BY i.created_at) AS raw_card_ids,
    MIN(i.current_price)              AS price_min,
    MAX(i.current_price)              AS price_max,
    MAX(i.image_url)                  AS image_url
FROM raw_cards i
WHERE i.state IN ('STORED', 'DISPLAY')
  AND i.current_hold_id IS NULL
  AND i.is_graded = FALSE
  AND NOT EXISTS (
    SELECT 1 FROM scrydex_price_cache c
    WHERE c.product_type = 'card' AND c.price_type = 'raw'
      AND c.market_price IS NOT NULL
      AND CASE WHEN c.variant IS NULL
                 OR regexp_replace(LOWER(c.variant), '[^a-z0-9]', '', 'g') IN ('normal','holofoil')
               THEN ''
               ELSE regexp_replace(LOWER(c.variant), '[^a-z0-9]', '', 'g')
          END
        = CASE WHEN i.variant IS NULL
                 OR regexp_replace(LOWER(i.variant), '[^a-z0-9]', '', 'g') IN ('normal','holofoil')
               THEN ''
               ELSE regexp_replace(LOWER(i.variant), '[^a-z0-9]', '', 'g')
          END
      AND ((i.scrydex_id IS NOT NULL AND c.scrydex_id = i.scrydex_id)
           OR (i.scrydex_id IS NULL AND i.tcgplayer_id IS NOT NULL
               AND c.tcgplayer_id = i.tcgplayer_id))
  )
GROUP BY i.card_name, i.set_name, i.card_number, i.condition,
         COALESCE(i.variant, ''), i.tcgplayer_id, i.scrydex_id
ORDER BY i.card_name, i.set_name, i.card_number
"""


@app.route('/dashboard/raw-rebind')
def raw_rebind_list():
    """List in-stock raw_cards the nightly updater can't price, grouped by
    identity, so an operator can manually pick the right scrydex variant."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()
    groups = shared_db.query(_REBIND_CANDIDATES_SQL)
    return render_template("raw_rebind.html", groups=groups)


@app.route('/api/raw-rebind/search', methods=["GET"])
def api_raw_rebind_search():
    """Search the Scrydex catalog for candidate printings the operator can
    bind a raw_card to. Single source of truth — delegates to the shared
    pricing.search_cards() that intake (/api/search/cards) and ingestion use,
    so JP sets, printed_number-style queries (OP14-041, 026/SVC), and
    multi-token name+set+number all work the same here as everywhere else.

    Accepts a single freeform `q` (preferred) OR legacy name/set/number
    fields (concatenated). Returns whole-card rows with a `variants` map so
    the modal can render a card+variant chip picker."""
    from flask import jsonify

    q = (request.args.get("q") or "").strip()
    if not q:
        # Back-compat: combine the legacy three-field form into one query —
        # the shared cache search is multi-token so name/set/number all hit.
        parts = [
            (request.args.get("name") or "").strip(),
            (request.args.get("set") or "").strip(),
            (request.args.get("number") or "").strip(),
        ]
        q = " ".join(p for p in parts if p)
    if not q:
        return jsonify({"ok": False, "error": "query required"}), 400

    try:
        # cache_only — these are cards that already failed auto-link, and a
        # rebind can only point at a scrydex_id that already exists in the
        # cache. The live PPT fallback would never help (PPT has no JP cards
        # and no scrydex_ids) and would surface 401s on services without
        # PPT_API_KEY.
        results = _pricing().search_cards(q, limit=20, all_games=True, cache_only=True)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    return jsonify({"ok": True, "results": results or []})


@app.route('/api/raw-rebind/apply', methods=["POST"])
def api_raw_rebind_apply():
    """Bind a list of raw_cards to a chosen (scrydex_id, variant). Writes
    raw_cards.scrydex_id and raw_cards.variant; leaves tcgplayer_id alone
    so we don't lose provenance for the original PPT mapping."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    body = request.get_json(silent=True) or {}
    raw_card_ids = body.get("raw_card_ids") or []
    scrydex_id   = (body.get("scrydex_id") or "").strip()
    variant      = (body.get("variant") or "").strip()

    if not raw_card_ids or not isinstance(raw_card_ids, list):
        return jsonify({"ok": False, "error": "raw_card_ids required"}), 400
    if not scrydex_id or not variant:
        return jsonify({"ok": False, "error": "scrydex_id and variant required"}), 400

    try:
        updated = shared_db.execute(
            """UPDATE raw_cards
                  SET scrydex_id = %s, variant = %s
                WHERE id = ANY(%s::uuid[])
                  AND state IN ('STORED','DISPLAY')""",
            (scrydex_id, variant, raw_card_ids),
        )
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB update failed: {e}"}), 502
    return jsonify({"ok": True, "updated": updated})


@app.route('/dashboard/slab-backfill')
def slab_backfill_page():
    """Page that runs the slab tcg_id backfill in dry-run mode and shows
    proposed matches. User clicks "Run backfill" to actually write metafields,
    or picks individual rows for the ambiguous set."""
    return render_template("slab_backfill.html")


@app.route('/api/slab-backfill/preview', methods=["GET"])
def api_slab_backfill_preview():
    """Run the matcher in dry-run mode (no Shopify writes) and return JSON."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import db as shared_db
    shared_db.init_pool()
    from slab_backfill import run as backfill_run
    out = backfill_run(apply=False, db_module=shared_db)
    return jsonify(out)


@app.route('/api/slab-backfill/apply', methods=["POST"])
def api_slab_backfill_apply():
    """Apply ALL confident + collapsed-variant matches (writes Shopify
    metafields). Ambiguous + no_match rows are returned for visibility but
    not written."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import db as shared_db
    shared_db.init_pool()
    from slab_backfill import run as backfill_run
    out = backfill_run(apply=True, db_module=shared_db)
    return jsonify(out)


@app.route('/api/slab-backfill/manual', methods=["POST"])
def api_slab_backfill_manual():
    """Write a manually-picked tcgplayer_id metafield for one ambiguous slab.
    Body: {product_gid, tcgplayer_id}."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from slab_backfill import _set_tcg_metafield
    body = request.get_json(silent=True) or {}
    pgid = body.get("product_gid")
    tcg  = body.get("tcgplayer_id")
    if not pgid or not tcg:
        return jsonify({"ok": False, "error": "product_gid + tcgplayer_id required"}), 400
    try:
        _set_tcg_metafield(pgid, int(tcg))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502
    return jsonify({"ok": True, "tcgplayer_id": int(tcg)})


@app.route('/dashboard/slab-runs/row/<int:row_id>/dismiss', methods=["POST"])
def slab_run_dismiss_row(row_id):
    """Mark a flagged row as dismissed without changing Shopify."""
    from flask import jsonify
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    import db as shared_db
    shared_db.init_pool()

    row = shared_db.query_one(
        "SELECT id, apply_status FROM slab_price_runs WHERE id = %s", (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if row["apply_status"] in ("applied", "dismissed"):
        return jsonify({"ok": False, "error": f"row already {row['apply_status']}"}), 409
    shared_db.execute(
        "UPDATE slab_price_runs SET apply_status='dismissed', applied_at=NOW() WHERE id = %s",
        (row_id,))
    return jsonify({"ok": True})


@app.route('/stream-log')
def stream_log():
    """SSE tail of RUN_LOG. Tolerates the file not existing yet (fresh
    deploy) by creating it before tailing — without this the EventSource
    on /dashboard/runlog would error out before any runner has fired."""
    def generate():
        RUN_LOG.parent.mkdir(parents=True, exist_ok=True)
        if not RUN_LOG.exists():
            RUN_LOG.touch()
        with open(RUN_LOG, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)  # tail mode — start at current end of file
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.25)
                    continue
                yield f"data: {line.rstrip()}\n\n"
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

# ── Sealed price runs (replaces the four CSV-backed dashboards) ──────────
#
# dailyrunner.py writes one row per Shopify variant per nightly run into
# sealed_price_runs. This block surfaces those rows: list of runs, detail
# page with per-action filtering, per-row apply/dismiss, and the headline
# bulk-approve form for "approve every <X% review row in one click".

@app.route('/run-sealed-updater', methods=["GET", "POST"])
def trigger_sealed_updater():
    """Manually kick off dailyrunner.py from the dashboard. Same launcher
    as /price_update; this route is JWT-gated for human owners."""
    from flask import jsonify
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    started = _launch_runner(
        key="sealed", label="SEALED",
        cmd=[sys.executable, "-u", str(ROOT / "dailyrunner.py")],
    )
    if not started:
        if request.method == "GET":
            return redirect("/dashboard/sealed-runs?msg=already-running")
        return jsonify({"ok": False, "error": "sealed updater is already running"}), 409
    if request.method == "GET":
        return redirect("/dashboard/sealed-runs")
    return jsonify({"ok": True, "started": True}), 200


@app.route('/dashboard/sealed-runs')
def sealed_runs_list():
    """One row per nightly run with per-action counts. If the migration
    hasn't been applied yet (or no run has happened), still render the
    page with nav + an empty/instructional state instead of 500ing."""
    db = _shared_db()
    rows = []
    table_missing = False
    try:
        rows = db.query("""
            SELECT run_id,
                   MIN(started_at)                                              AS started_at,
                   COUNT(*)                                                     AS total,
                   COUNT(*) FILTER (WHERE action = 'updated')                   AS updated,
                   COUNT(*) FILTER (WHERE action = 'review')                    AS review,
                   COUNT(*) FILTER (WHERE action = 'missing')                   AS missing,
                   COUNT(*) FILTER (WHERE action = 'error')                     AS errors,
                   COUNT(*) FILTER (WHERE action = 'untouched')                 AS untouched,
                   COUNT(*) FILTER (WHERE action = 'skip')                      AS skipped
            FROM sealed_price_runs
            GROUP BY run_id
            ORDER BY started_at DESC
            LIMIT 60
        """)
    except Exception as e:
        msg = str(e).lower()
        if "sealed_price_runs" in msg and ("does not exist" in msg or "undefined" in msg):
            table_missing = True
        else:
            raise
    return render_template("sealed_runs.html", runs=rows, table_missing=table_missing)


_SEALED_DETAIL_SORTS = {
    # default: action priority then magnitude
    "":            "CASE action WHEN 'error' THEN 0 WHEN 'review' THEN 1 "
                   "  WHEN 'updated' THEN 2 WHEN 'missing' THEN 3 "
                   "  WHEN 'skip' THEN 4 WHEN 'untouched' THEN 5 ELSE 6 END, "
                   "ABS(COALESCE(delta_pct, 0)) DESC NULLS LAST, title",
    "delta_desc":  "delta_pct DESC NULLS LAST, title",
    "delta_asc":   "delta_pct ASC NULLS LAST, title",
    "abs_delta":   "ABS(COALESCE(delta_pct, 0)) DESC NULLS LAST, title",
    "title":       "title ASC",
    "qty_desc":    "qty DESC NULLS LAST, title",
    "qty_asc":     "qty ASC NULLS LAST, title",
}


def _is_charm_drop(row) -> bool:
    """A pending review row qualifies as a 'charm-tier drop' if its
    suggested price is lower than current AND the dollar delta sits
    inside the charm-rounding tier for the suggested price. Same
    predicate the bulk-apply endpoint uses, mirrored here so the
    dashboard can preview which rows the button would hit."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    from price_rounding import charm_drop_auto_threshold
    if row.get("action") != "review" or row.get("apply_status") != "pending":
        return False
    old, new = row.get("old_price"), row.get("suggested_price")
    if old is None or new is None:
        return False
    old, new = float(old), float(new)
    return new < old and (old - new) <= charm_drop_auto_threshold(new)


@app.route('/dashboard/sealed-runs/<run_id>')
def sealed_run_detail(run_id):
    """Detail page for one sealed run with per-action filter, OOS toggle,
    sort control, and bulk-approve."""
    db = _shared_db()
    action_filter = (request.args.get("action") or "").strip()
    include_oos   = (request.args.get("include_oos") or "").strip() == "1"
    charm_only    = (request.args.get("charm") or "").strip() == "1"
    sort_key      = (request.args.get("sort") or "").strip()
    if sort_key not in _SEALED_DETAIL_SORTS:
        sort_key = ""
    order_by_sql = _SEALED_DETAIL_SORTS[sort_key]

    where = ["run_id = %s"]
    params: list = [run_id]
    if action_filter:
        where.append("action = %s"); params.append(action_filter)
    if not include_oos:
        # Only hide rows we're confident are OOS — qty=0 explicitly. NULL
        # qty (very rare; would be a Shopify metafield gap) stays visible.
        where.append("(qty IS NULL OR qty > 0)")
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT id, product_gid, variant_id, sku, title, handle, tcgplayer_id,
               qty, old_price, tcg_price, suggested_price, new_price, delta_pct,
               action, reason, apply_status, applied_at, applied_price,
               started_at
        FROM sealed_price_runs
        WHERE {where_sql}
        ORDER BY {order_by_sql}
    """
    rows = db.query(sql, tuple(params))

    # Tag each row with charm eligibility so the template can render a
    # badge on the row + filter to just those rows when ?charm=1 is set.
    for r in rows:
        r["is_charm_drop"] = _is_charm_drop(r)
    if charm_only:
        rows = [r for r in rows if r["is_charm_drop"]]

    summary = db.query_one("""
        SELECT MIN(started_at) AS started_at, COUNT(*) AS total,
               COUNT(*) FILTER (WHERE action = 'review' AND apply_status = 'pending') AS pending_review,
               COUNT(*) FILTER (WHERE qty = 0) AS oos_count
        FROM sealed_price_runs WHERE run_id = %s
    """, (run_id,))

    # Charm-eligible count is independent of the active filter — always
    # shows the run-wide total so Sean can see what 'Approve charm-tier
    # drops' would do regardless of what he's currently filtered to.
    pending_for_charm = db.query("""
        SELECT old_price, suggested_price, action, apply_status
          FROM sealed_price_runs
         WHERE run_id = %s AND action = 'review'
           AND apply_status = 'pending'
           AND suggested_price IS NOT NULL
           AND old_price IS NOT NULL
           AND product_gid IS NOT NULL
           AND variant_id IS NOT NULL
    """, (run_id,))
    charm_eligible_count = sum(1 for r in pending_for_charm if _is_charm_drop(r))

    return render_template(
        "sealed_run_detail.html",
        run_id=run_id, rows=rows, summary=summary,
        action_filter=action_filter,
        include_oos=include_oos,
        sort_key=sort_key,
        charm_only=charm_only,
        charm_eligible_count=charm_eligible_count,
        store_domain=os.environ.get("SHOPIFY_STORE", ""),
    )


@app.route('/dashboard/sealed-runs/row/<int:row_id>/apply', methods=["POST"])
def sealed_run_apply_row(row_id):
    """Apply one flagged row's suggested price (or override) to Shopify."""
    from flask import jsonify
    db = _shared_db()
    from dailyrunner import update_variant_price

    row = db.query_one("""
        SELECT id, product_gid, variant_id, suggested_price, old_price,
               apply_status, title
        FROM sealed_price_runs WHERE id = %s
    """, (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if row["apply_status"] in ("applied", "dismissed"):
        return jsonify({"ok": False, "error": f"row already {row['apply_status']}"}), 409
    if not row["product_gid"] or not row["variant_id"]:
        return jsonify({"ok": False, "error": "row has no Shopify identifiers"}), 400

    body = request.get_json(silent=True) or {}
    override = body.get("price")
    target = float(override) if override is not None else float(row["suggested_price"] or 0)
    if target <= 0:
        return jsonify({"ok": False, "error": "no valid price to apply"}), 400

    try:
        update_variant_price(row["product_gid"], row["variant_id"], target)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Shopify update failed: {e}"}), 502

    db.execute("""
        UPDATE sealed_price_runs
           SET apply_status = 'applied', applied_at = NOW(), applied_price = %s,
               new_price = %s
         WHERE id = %s
    """, (target, target, row_id))
    return jsonify({"ok": True, "applied_price": target, "title": row["title"]})


@app.route('/dashboard/sealed-runs/row/<int:row_id>/dismiss', methods=["POST"])
def sealed_run_dismiss_row(row_id):
    """Mark a flagged row as dismissed without changing Shopify."""
    from flask import jsonify
    db = _shared_db()
    row = db.query_one(
        "SELECT id, apply_status FROM sealed_price_runs WHERE id = %s", (row_id,))
    if not row:
        return jsonify({"ok": False, "error": "row not found"}), 404
    if row["apply_status"] in ("applied", "dismissed"):
        return jsonify({"ok": False, "error": f"row already {row['apply_status']}"}), 409
    db.execute(
        "UPDATE sealed_price_runs SET apply_status='dismissed', applied_at=NOW() WHERE id = %s",
        (row_id,))
    return jsonify({"ok": True})


@app.route('/dashboard/sealed-runs/<run_id>/bulk-apply', methods=["POST"])
def sealed_run_bulk_apply(run_id):
    """Approve every pending review row matching the bulk filter.
    Body modes:
      {mode: 'pct', threshold_pct: 2}     -> |delta_pct| <= 2 (default)
      {mode: 'charm_tier'}                -> drop_dollars <= charm tier
                                              for that row's suggested price

    The 'charm_tier' mode mirrors the per-product policy in
    process_product: a $1.49 -> $0.99 drop is 33% but only 50¢, which
    is just charm-rounding noise — auto-apply, don't flag.

    Each Shopify mutation runs sequentially (no fan-out) so one failure
    doesn't poison the rest; per-row results are reported back."""
    from flask import jsonify
    db = _shared_db()
    from dailyrunner import update_variant_price
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    from price_rounding import charm_drop_auto_threshold

    body = request.get_json(silent=True) or {}
    mode = (body.get("mode") or "pct").strip()

    if mode not in ("pct", "charm_tier"):
        return jsonify({"ok": False, "error": "mode must be 'pct' or 'charm_tier'"}), 400

    if mode == "pct":
        try:
            threshold = float(body.get("threshold_pct") if body.get("threshold_pct") is not None else 2.0)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "threshold_pct must be numeric"}), 400
        if threshold < 0 or threshold > 100:
            return jsonify({"ok": False, "error": "threshold_pct must be 0-100"}), 400
        rows = db.query("""
            SELECT id, product_gid, variant_id, suggested_price, title, delta_pct, old_price
            FROM sealed_price_runs
            WHERE run_id = %s
              AND action = 'review'
              AND apply_status = 'pending'
              AND suggested_price IS NOT NULL
              AND product_gid IS NOT NULL
              AND variant_id IS NOT NULL
              AND ABS(COALESCE(delta_pct, 0)) <= %s
        """, (run_id, threshold))
        filter_label = f"|Δ%| ≤ {threshold}"
    else:
        # Pull every pending review row, then filter in Python by per-row tier.
        candidates = db.query("""
            SELECT id, product_gid, variant_id, suggested_price, title, delta_pct, old_price
            FROM sealed_price_runs
            WHERE run_id = %s
              AND action = 'review'
              AND apply_status = 'pending'
              AND suggested_price IS NOT NULL
              AND old_price IS NOT NULL
              AND product_gid IS NOT NULL
              AND variant_id IS NOT NULL
        """, (run_id,))
        rows = []
        for r in candidates:
            old = float(r["old_price"])
            new = float(r["suggested_price"])
            if new >= old:
                continue  # not a drop — should have been auto-applied at run time
            drop_dollars = old - new
            if drop_dollars <= charm_drop_auto_threshold(new):
                rows.append(r)
        filter_label = "drop ≤ charm tier"

    applied, failed = [], []
    for r in rows:
        target = float(r["suggested_price"])
        try:
            update_variant_price(r["product_gid"], r["variant_id"], target)
            db.execute("""
                UPDATE sealed_price_runs
                   SET apply_status = 'applied', applied_at = NOW(),
                       applied_price = %s, new_price = %s
                 WHERE id = %s
            """, (target, target, r["id"]))
            applied.append({"id": r["id"], "title": r["title"], "price": target})
        except Exception as e:
            failed.append({"id": r["id"], "title": r["title"], "error": str(e)})

    return jsonify({
        "ok": True, "mode": mode, "filter": filter_label,
        "candidates": len(rows), "applied": len(applied), "failed": len(failed),
        "failed_rows": failed[:20],
    })


# ── Block list (price_auto_block) management ─────────────────────────────
#
# A row in any of the three run-detail pages can be muted with one click.
# The updater consults the block list at the start of each run and treats
# blocked rows as action='skip', reason='auto-block'. Permanent until
# removed via /dashboard/price-blocks.

@app.route('/api/price-block/add', methods=["POST"])
def api_price_block_add():
    """Body: {domain: 'raw'|'slab'|'sealed', block_key: str, label?, reason?}."""
    from flask import jsonify
    db = _shared_db()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    from price_auto_block import add_block

    body = request.get_json(silent=True) or {}
    domain = (body.get("domain") or "").strip()
    block_key = (body.get("block_key") or "").strip()
    if domain not in ("raw", "slab", "sealed") or not block_key:
        return jsonify({"ok": False, "error": "domain (raw|slab|sealed) and block_key required"}), 400

    user = get_current_user() or {}
    inserted = add_block(
        db, domain=domain, block_key=block_key,
        label=body.get("label"), reason=body.get("reason"),
        blocked_by=user.get("email") or user.get("username"),
    )
    return jsonify({"ok": True, "inserted": inserted})


@app.route('/api/price-block/remove', methods=["POST"])
def api_price_block_remove():
    """Body: {domain, block_key}."""
    from flask import jsonify
    db = _shared_db()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    from price_auto_block import remove_block

    body = request.get_json(silent=True) or {}
    domain = (body.get("domain") or "").strip()
    block_key = (body.get("block_key") or "").strip()
    if not domain or not block_key:
        return jsonify({"ok": False, "error": "domain and block_key required"}), 400
    removed = remove_block(db, domain=domain, block_key=block_key)
    return jsonify({"ok": True, "removed": removed})


@app.route('/dashboard/price-blocks')
def price_blocks_list():
    """All current blocks across raw/slab/sealed with a Remove button."""
    db = _shared_db()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
    from price_auto_block import list_blocks
    blocks = []
    table_missing = False
    try:
        blocks = list_blocks(db)
    except Exception as e:
        msg = str(e).lower()
        if "price_auto_block" in msg and ("does not exist" in msg or "undefined" in msg):
            table_missing = True
        else:
            raise
    return render_template("price_blocks.html", blocks=blocks, table_missing=table_missing)


# ── Big Movers ───────────────────────────────────────────────────────────
#
# Single page showing the latest run from each updater filtered to
# auto-applied rows whose |delta_pct| >= threshold. Quick eyeball check
# for what moved overnight; one-click Block per row.

def _safe_query_one(db, sql, params=()):
    """Return None if the underlying table hasn't been created yet
    instead of bubbling a 500 to the user."""
    try:
        return db.query_one(sql, params)
    except Exception as e:
        msg = str(e).lower()
        if "does not exist" in msg or "undefined" in msg:
            return None
        raise


def _safe_query(db, sql, params=()):
    try:
        return db.query(sql, params)
    except Exception as e:
        msg = str(e).lower()
        if "does not exist" in msg or "undefined" in msg:
            return []
        raise


@app.route('/dashboard/big-movers')
def big_movers():
    db = _shared_db()
    try:
        threshold = float(request.args.get("threshold", 20))
    except ValueError:
        threshold = 20.0
    threshold = max(0.0, min(threshold, 500.0))

    # Latest run id per source
    sealed_run = _safe_query_one(db,
        "SELECT run_id, MIN(started_at) AS started_at FROM sealed_price_runs "
        "WHERE run_id = (SELECT run_id FROM sealed_price_runs "
        "                ORDER BY started_at DESC LIMIT 1) "
        "GROUP BY run_id")
    slab_run = _safe_query_one(db,
        "SELECT run_id, MIN(started_at) AS started_at FROM slab_price_runs "
        "WHERE run_id = (SELECT run_id FROM slab_price_runs "
        "                ORDER BY started_at DESC LIMIT 1) "
        "GROUP BY run_id")
    raw_run = _safe_query_one(db,
        "SELECT run_id, MIN(started_at) AS started_at FROM raw_card_price_runs "
        "WHERE run_id = (SELECT run_id FROM raw_card_price_runs "
        "                ORDER BY started_at DESC LIMIT 1) "
        "GROUP BY run_id")

    sealed_rows = []
    if sealed_run:
        sealed_rows = _safe_query(db, """
            SELECT id, title, sku, variant_id, old_price, new_price,
                   suggested_price, delta_pct, reason
            FROM sealed_price_runs
            WHERE run_id = %s AND action = 'updated'
              AND ABS(COALESCE(delta_pct, 0)) >= %s
            ORDER BY ABS(delta_pct) DESC LIMIT 200
        """, (sealed_run["run_id"], threshold))

    slab_rows = []
    if slab_run:
        slab_rows = _safe_query(db, """
            SELECT id, title, sku, variant_gid, old_price, new_price,
                   suggested_price, delta_pct, reason
            FROM slab_price_runs
            WHERE run_id = %s AND action = 'adjusted'
              AND ABS(COALESCE(delta_pct, 0)) >= %s
            ORDER BY ABS(delta_pct) DESC LIMIT 200
        """, (slab_run["run_id"], threshold))

    raw_rows = []
    if raw_run:
        raw_rows = _safe_query(db, """
            SELECT id, raw_card_id, scrydex_id, tcgplayer_id, card_name,
                   set_name, card_number, variant, condition,
                   old_price, new_price, suggested_price, delta_pct, reason
            FROM raw_card_price_runs
            WHERE run_id = %s AND action = 'auto_applied'
              AND ABS(COALESCE(delta_pct, 0)) >= %s
            ORDER BY ABS(delta_pct) DESC LIMIT 200
        """, (raw_run["run_id"], threshold))

    return render_template(
        "big_movers.html",
        threshold=threshold,
        sealed_run=sealed_run, sealed_rows=sealed_rows,
        slab_run=slab_run,     slab_rows=slab_rows,
        raw_run=raw_run,       raw_rows=raw_rows,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)


