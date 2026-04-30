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
REVIEW_CSV    = ROOT / "price_updates_needs_review.csv"
PUSHED_CSV    = ROOT / "price_updates_pushed.csv"
MISSING_CSV   = ROOT / "price_updates_missing_listing.csv"
UNTOUCHED_CSV = ROOT / "price_updates_untouched.csv"
RUN_LOG = ROOT / "run_output.log"
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "price-updater-fallback-key")


@app.post("/price_update")
def price_update():
    """Trigger nightly price sync (dailyrunner.py) in background."""
    if not _authorized_trigger():
        from flask import jsonify
        return jsonify({"error": "Unauthorized"}), 401
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
    """Run the Scrydex nightly cache sync for all configured games."""
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

        # Sync all configured games
        games = [g.strip() for g in os.environ.get("SCRYDEX_GAMES", "pokemon").split(",") if g.strip()]
        import time as _time
        grand_start = _time.time()
        grand_totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0}

        for game in games:
            client = ScrydexClient(scrydex_key, scrydex_team, db=shared_db, game=game)

            rows = shared_db.query(
                "SELECT expansion_id FROM scrydex_sync_log WHERE game = %s AND active = TRUE", (game,))
            expansion_ids = [r["expansion_id"] for r in rows]
            if not expansion_ids:
                print(f"⏭ {game}: no active expansions in sync_log")
                continue

            print(f"🔄 {game}: {len(expansion_ids)} active expansions")
            totals = {"cards": 0, "sealed": 0, "prices": 0, "credits": 0}
            failures = []
            t_start = _time.time()

            for i, eid in enumerate(expansion_ids):
                try:
                    stats = sync_expansion(client, eid, shared_db)
                    for k in totals:
                        totals[k] += stats.get(k, 0)
                    if (i + 1) % 20 == 0:
                        print(f"  ... {i+1}/{len(expansion_ids)} done ({totals['credits']} credits)")
                except Exception as e:
                    print(f"  ❌ {game}/{eid}: {e}")
                    failures.append((eid, str(e)))
                _time.sleep(0.05)

            # Retry failures
            if failures:
                print(f"  🔁 Retrying {len(failures)} failed expansions...")
                for eid, original_error in failures:
                    try:
                        stats = sync_expansion(client, eid, shared_db)
                        for k in totals:
                            totals[k] += stats.get(k, 0)
                        print(f"    ✅ Retry OK: {game}/{eid}")
                    except Exception as e:
                        print(f"    ❌ Still failed: {game}/{eid}: {e}")
                    _time.sleep(0.1)

            elapsed = int(_time.time() - t_start)
            print(f"✅ {game} done in {elapsed}s — {totals['cards']} cards, "
                  f"{totals['sealed']} sealed, {totals['credits']} credits")
            for k in grand_totals:
                grand_totals[k] += totals[k]

        grand_elapsed = int(_time.time() - grand_start)
        print(f"✅ All games done in {grand_elapsed}s — {grand_totals['credits']} total credits")
    except Exception as e:
        print(f"❌ Scrydex sync failed: {e}")
        import traceback
        traceback.print_exc()


def run_slab_updater():
    """Nightly slab price sync — runs after Scrydex sync so cache is fresh."""
    try:
        from slab_updater import run as slab_run
        # apply=False — slabs never auto-adjust. Every over/underpriced
        # finding lands in the dashboard at /dashboard/slab-runs as a flag
        # for human review. Charm pricing is applied at click-to-apply time
        # (handled in the dashboard, not here).
        results = slab_run(apply=False, csv_path="slab_updates.csv")
        adjusted = sum(1 for r in results if r.get("action") == "adjusted")
        flagged  = sum(1 for r in results if "flag" in (r.get("action") or ""))
        print(f"✅ Slab updater done: {len(results)} slabs, {adjusted} adjusted, {flagged} flagged")
    except Exception as e:
        print(f"❌ Slab updater failed: {e}")
        import traceback
        traceback.print_exc()


if os.environ.get("ENABLE_CRON", "").lower() == "true":
    scheduler = BackgroundScheduler()
    scheduler.add_job(call_dailyrunner, "cron", hour=3)  # UTC
    scheduler.add_job(run_scrydex_sync, "cron", hour=4)  # UTC — after dailyrunner + analytics snapshot
    scheduler.add_job(run_slab_updater, "cron", hour=5)  # UTC — after Scrydex sync
    scheduler.start()
    print("✅ Scheduler started — dailyrunner 3AM, Scrydex 4AM, slab updater 5AM UTC")

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
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    threading.Thread(target=run_scrydex_sync, daemon=True).start()
    if request.method == "GET":
        return redirect("/dashboard/runlog")
    return jsonify({"ok": True, "started": True}), 200


@app.route('/run-slab-updater', methods=["GET", "POST"])
def trigger_slab_updater():
    """Manually trigger the graded slab price updater. Always runs in apply
    mode — it auto-adjusts overpriced in-stock slabs and persists every row
    (adjusted, flagged, skipped) to slab_price_runs for review."""
    from flask import jsonify
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    threading.Thread(target=run_slab_updater, daemon=True).start()
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
    """Manually trigger the raw card price updater.
    Always runs in apply_auto=True mode — small drifts auto-apply, larger
    deltas land in /dashboard/raw-runs as flags for human review.
    Designed to be hit nightly from a Shopify Flow."""
    from flask import jsonify
    if not _authorized_trigger():
        return jsonify({"error": "Unauthorized"}), 401
    def _go():
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "shared"))
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        try:
            from raw_card_updater import run as raw_run
            import db as shared_db
            shared_db.init_pool()
            raw_run(apply_auto=True, db_module=shared_db)
        except Exception as e:
            print(f"❌ raw_card_updater failed: {e}")
            import traceback; traceback.print_exc()
    threading.Thread(target=_go, daemon=True).start()
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
            "WHERE id = ANY(%s)",
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


