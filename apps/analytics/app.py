"""
analytics — analytics.pack-fresh.com (or internal)
SKU sell-through analytics: daily order ingestion + velocity metrics.

Triggered daily via Shopify Flow webhook to /run.
Also exposes /api/analytics for batch lookups from other services.
"""

import os
import logging
import threading
from flask import Flask, request, jsonify, render_template_string, g

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()


from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner"], public_prefixes=('/static', '/api/'),
                    skip_jwt_prefixes=('/run',))


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "analytics"})


@app.route("/run", methods=["POST"])
def run_analytics():
    """
    Trigger the daily analytics pipeline.
    Called by Shopify Flow or manually.
    Runs in background thread, returns immediately.
    """
    # Allow authenticated owners OR valid Flow secret
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    try:
        from auth import get_current_user
        user = get_current_user()
    except Exception:
        user = None
    if not user and (not flow_secret or secret != flow_secret):
        return jsonify({"error": "Unauthorized"}), 401

    def _run():
        try:
            from compute import run_full_pipeline
            result = run_full_pipeline()
            logger.info(f"Analytics pipeline complete: {result}")
        except Exception as e:
            logger.exception(f"Analytics pipeline failed: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "started": True})


@app.route("/run/backfill", methods=["POST"])
def run_backfill():
    """Force a full backfill — 90d orders + 365d customers (slower, use sparingly)."""
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    try:
        from auth import get_current_user
        user = get_current_user()
    except Exception:
        user = None
    if not user and (not flow_secret or secret != flow_secret):
        return jsonify({"error": "Unauthorized"}), 401

    def _run():
        try:
            from compute import ingest_orders, recompute_analytics, snapshot_inventory
            from price_history import snapshot_scrydex_prices
            from taxonomy import classify_taxonomy
            from customers import sync_customer_orders, recompute_customer_summaries, backfill_daily_summaries
            from margins import compute_realized_margins

            snapshot_scrydex_prices()
            snapshot_inventory()
            ingest_orders(full_backfill=True)
            recompute_analytics()
            classify_taxonomy()
            sync_customer_orders(full_backfill=True)
            recompute_customer_summaries()
            backfill_daily_summaries(days=365)
            compute_realized_margins()
            logger.info("Full backfill complete")
        except Exception as e:
            logger.exception(f"Backfill failed: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "started": True, "mode": "backfill"})


@app.route("/run/migrate", methods=["POST"])
def run_migrate():
    """Run the v2 migration script to create new analytics tables."""
    try:
        from auth import get_current_user
        user = get_current_user()
    except Exception:
        user = None
    if not user:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        import subprocess
        import sys
        result = subprocess.run(
            [sys.executable, "migrate_analytics_v2.py"],
            capture_output=True, text=True, timeout=30
        )
        return jsonify({
            "ok": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/analytics", methods=["POST"])
def batch_analytics():
    """
    Batch lookup SKU analytics by shopify_variant_ids or tcgplayer_ids.
    Body: { "variant_ids": [123, 456] } or { "tcgplayer_ids": [789, 101] }
    """
    data = request.get_json(silent=True) or {}

    variant_ids = data.get("variant_ids")
    tcgplayer_ids = data.get("tcgplayer_ids")

    if variant_ids:
        ph = ",".join(["%s"] * len(variant_ids))
        rows = db.query(
            f"SELECT * FROM sku_analytics WHERE shopify_variant_id IN ({ph})",
            tuple(int(v) for v in variant_ids)
        )
    elif tcgplayer_ids:
        ph = ",".join(["%s"] * len(tcgplayer_ids))
        rows = db.query(
            f"SELECT * FROM sku_analytics WHERE tcgplayer_id IN ({ph})",
            tuple(int(t) for t in tcgplayer_ids)
        )
    else:
        return jsonify({"error": "Provide variant_ids or tcgplayer_ids"}), 400

    result = {}
    for r in rows:
        key = r["tcgplayer_id"] or r["shopify_variant_id"]
        result[key] = _ser(r)

    return jsonify({"analytics": result})


@app.route("/api/analytics/summary")
def analytics_summary():
    """Quick stats for the admin dashboard."""
    stats = db.query_one("""
        SELECT
            COUNT(*) AS total_skus,
            COUNT(*) FILTER (WHERE units_sold_90d > 0) AS active_skus,
            AVG(velocity_score) FILTER (WHERE units_sold_90d > 0) AS avg_velocity,
            MAX(computed_at) AS last_computed
        FROM sku_analytics
    """)
    return jsonify(_ser(stats) if stats else {})


@app.route("/api/browse")
def browse_analytics():
    """Browse SKU analytics with search, sort, pagination."""
    q = (request.args.get("q") or "").strip()
    sort = request.args.get("sort", "velocity_desc")
    page = max(1, int(request.args.get("page", 1)))
    limit = 50
    offset = (page - 1) * limit
    show = request.args.get("show", "all")  # all, selling, stale

    filters = []
    params = []

    if q:
        filters.append("title ILIKE %s")
        params.append(f"%{q}%")
    if show == "selling":
        filters.append("units_sold_90d > 0")
    elif show == "stale":
        filters.append("(units_sold_90d = 0 OR units_sold_90d IS NULL)")

    where = "WHERE " + " AND ".join(filters) if filters else ""

    sort_map = {
        "velocity_desc": "velocity_score DESC NULLS LAST",
        "velocity_asc": "velocity_score ASC NULLS LAST",
        "sold_desc": "units_sold_90d DESC NULLS LAST",
        "sold_asc": "units_sold_90d ASC NULLS LAST",
        "price_desc": "current_price DESC NULLS LAST",
        "price_asc": "current_price ASC NULLS LAST",
        "days_asc": "avg_days_to_sell ASC NULLS LAST",
        "days_desc": "avg_days_to_sell DESC NULLS LAST",
        "title_asc": "title ASC",
    }
    order = sort_map.get(sort, "velocity_score DESC NULLS LAST")

    count_row = db.query_one(f"SELECT COUNT(*) AS total FROM sku_analytics {where}", tuple(params))
    total = count_row["total"] if count_row else 0

    rows = db.query(f"""
        SELECT * FROM sku_analytics {where}
        ORDER BY {order}
        LIMIT %s OFFSET %s
    """, tuple(params) + (limit, offset))

    return jsonify({
        "items": [_ser(r) for r in rows],
        "total": total,
        "page": page,
        "pages": max(1, (total + limit - 1) // limit),
    })


@app.route("/api/status")
def pipeline_status():
    """Show pipeline run status."""
    meta_rows = db.query("SELECT * FROM analytics_meta ORDER BY key")
    daily_count = db.query_one("SELECT COUNT(*) AS c FROM sku_daily_sales")
    analytics_count = db.query_one("SELECT COUNT(*) AS c FROM sku_analytics")
    active = db.query_one("SELECT COUNT(*) AS c FROM sku_analytics WHERE units_sold_90d > 0")
    return jsonify({
        "meta": {r["key"]: {"value": r["value"], "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None} for r in meta_rows},
        "daily_sales_records": daily_count["c"] if daily_count else 0,
        "analytics_skus": analytics_count["c"] if analytics_count else 0,
        "active_skus": active["c"] if active else 0,
    })


# ── Inventory Flow ───────────────────────────────────────────────────────────
# Two jobs this powers:
#   A. Buying / breakdown — are we sitting on capital that does not move?
#   B. Reorder-from-distro — which board games / supplies / sealed are running low?
# Every "value" / "stale" metric is gated on current_qty > 0 — items we no longer
# own (qty=0) are not dead capital, just not owned.

# Whitelisted group-by dimensions (column on product_taxonomy t).
_FLOW_DIMS = {
    "product_type": "t.product_type",
    "ip":           "t.ip",
    "form_factor":  "t.form_factor",
    "set_name":     "t.set_name",
    "era":          "t.era",
}


@app.route("/api/inventory/flow")
def inventory_flow():
    """KPI strip + group-by roll-up with per-group velocity-band value split."""
    dim = request.args.get("dim", "product_type")
    col = _FLOW_DIMS.get(dim, "t.product_type")

    kpi = db.query_one("""
        SELECT
          COUNT(*) FILTER (WHERE s.current_qty > 0)                       AS in_stock_skus,
          COALESCE(SUM(GREATEST(s.current_qty,0)),0)                      AS units,
          COALESCE(SUM(GREATEST(s.current_qty,0)*COALESCE(s.current_price,0)),0) AS inv_value,
          COALESCE(SUM(CASE WHEN s.current_qty>0 AND COALESCE(s.units_sold_90d,0)=0
                            THEN s.current_qty*COALESCE(s.current_price,0) END),0)  AS dead_value,
          COUNT(*) FILTER (WHERE s.current_qty>0 AND COALESCE(s.units_sold_90d,0)=0) AS dead_skus,
          COALESCE(SUM(s.units_sold_90d),0)                              AS sold_90d
        FROM sku_analytics s
    """)

    rows = db.query(f"""
        SELECT
          COALESCE({col}, '(unclassified)')                              AS grp,
          COUNT(*) FILTER (WHERE s.current_qty>0)                        AS skus,
          COALESCE(SUM(GREATEST(s.current_qty,0)),0)                     AS units,
          COALESCE(SUM(GREATEST(s.current_qty,0)*COALESCE(s.current_price,0)),0) AS inv_value,
          COALESCE(SUM(s.units_sold_90d),0)                             AS sold_90d,
          COALESCE(SUM(s.units_sold_30d),0)                             AS sold_30d,
          COALESCE(SUM(s.units_sold_7d),0)                              AS sold_7d,
          COALESCE(SUM(CASE WHEN s.current_qty>0 AND COALESCE(s.units_sold_90d,0)/90.0 >= 0.3
                            THEN s.current_qty*COALESCE(s.current_price,0) END),0) AS val_fast,
          COALESCE(SUM(CASE WHEN s.current_qty>0 AND COALESCE(s.units_sold_90d,0)/90.0 >= 0.1
                            AND COALESCE(s.units_sold_90d,0)/90.0 < 0.3
                            THEN s.current_qty*COALESCE(s.current_price,0) END),0) AS val_med,
          COALESCE(SUM(CASE WHEN s.current_qty>0 AND s.units_sold_90d > 0
                            AND COALESCE(s.units_sold_90d,0)/90.0 < 0.1
                            THEN s.current_qty*COALESCE(s.current_price,0) END),0) AS val_slow,
          COALESCE(SUM(CASE WHEN s.current_qty>0 AND COALESCE(s.units_sold_90d,0)=0
                            THEN s.current_qty*COALESCE(s.current_price,0) END),0) AS val_dead
        FROM sku_analytics s
        JOIN product_taxonomy t USING (shopify_variant_id)
        GROUP BY 1
        HAVING SUM(GREATEST(s.current_qty,0)) > 0 OR SUM(s.units_sold_90d) > 0
        ORDER BY inv_value DESC NULLS LAST
    """)

    return jsonify({"kpi": _ser(kpi) if kpi else {}, "groups": [_ser(r) for r in rows]})


@app.route("/api/inventory/dead")
def inventory_dead():
    """Job A — capital that is not working: in stock and either zero sales in 90d
    or more than ~6 months of stock at the current rate. Markdown / breakdown / stop-buying."""
    ptype = (request.args.get("ptype") or "").strip()
    params = []
    where = ["s.current_qty > 0",
             "(COALESCE(s.units_sold_90d,0) = 0 OR s.current_qty / (NULLIF(s.units_sold_90d,0)/90.0) > 180)"]
    if ptype:
        where.append("t.product_type = %s")
        params.append(ptype)

    rows = db.query(f"""
        SELECT s.title, s.tcgplayer_id, t.product_type, t.ip, t.form_factor,
               s.current_qty, s.current_price, s.units_sold_90d, s.units_sold_30d,
               (s.current_qty * COALESCE(s.current_price,0)) AS tied_value,
               CASE WHEN COALESCE(s.units_sold_90d,0) > 0
                    THEN s.current_qty / (s.units_sold_90d/90.0) END AS days_inv
        FROM sku_analytics s
        JOIN product_taxonomy t USING (shopify_variant_id)
        WHERE {' AND '.join(where)}
        ORDER BY tied_value DESC NULLS LAST
        LIMIT 60
    """, tuple(params))
    return jsonify({"items": [_ser(r) for r in rows]})


@app.route("/api/inventory/restock")
def inventory_restock():
    """Job B — reorder signals: in stock with under ~30 days left at the current rate,
    plus out-of-stock non-singles that are still selling (sealed / board games / supplies)."""
    ptype = (request.args.get("ptype") or "").strip()
    params = []
    cond = ("(s.current_qty > 0 AND COALESCE(s.units_sold_90d,0) > 0 "
            "     AND s.current_qty / (s.units_sold_90d/90.0) <= 30) "
            "OR (s.current_qty = 0 AND COALESCE(s.units_sold_30d,0) > 0 AND t.product_type <> 'card')")
    where = [f"({cond})"]
    if ptype:
        where.append("t.product_type = %s")
        params.append(ptype)

    rows = db.query(f"""
        SELECT s.title, s.tcgplayer_id, t.product_type, t.ip,
               s.current_qty, s.current_price, s.units_sold_90d, s.units_sold_30d, s.units_sold_7d,
               CASE WHEN COALESCE(s.units_sold_90d,0) > 0
                    THEN s.current_qty / (s.units_sold_90d/90.0) END AS days_inv
        FROM sku_analytics s
        JOIN product_taxonomy t USING (shopify_variant_id)
        WHERE {' AND '.join(where)}
        ORDER BY (s.current_qty = 0) DESC, days_inv ASC NULLS LAST
        LIMIT 60
    """, tuple(params))
    return jsonify({"items": [_ser(r) for r in rows]})


def _ser(d):
    out = {}
    for k, v in dict(d).items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
            out[k] = float(v)
        else:
            out[k] = v
    return out


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Analytics · Pack Fresh</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>📈</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
.header h1 { font-size:1.3rem; }
.main { max-width:1100px; margin:0 auto; padding:20px; }
.stats { display:flex; gap:16px; margin-bottom:20px; flex-wrap:wrap; }
.stat { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px 18px; min-width:140px; }
.controls { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; align-items:center; }
.controls input, .controls select { height:38px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 12px; font-size:0.85rem; font-family:inherit; outline:none; }
.controls input:focus { border-color:var(--accent); }
.controls input { flex:1; min-width:200px; }
th { cursor:pointer; }
th:hover { color:var(--text); }
.vel-fast { color:var(--green); font-weight:700; }
.vel-med { color:var(--amber); font-weight:600; }
.vel-slow { color:var(--red); }
.vel-none { color:var(--dim); }
.pg { display:flex; gap:4px; justify-content:center; margin-top:16px; }
.pg button { height:32px; min-width:32px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); cursor:pointer; font-size:0.8rem; }
.pg button.active { background:var(--accent); border-color:var(--accent); color:#fff; }
.pg button:disabled { opacity:0.3; }
.empty { text-align:center; padding:40px; color:var(--dim); }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:40px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
/* tabs */
.tabs { display:flex; gap:2px; }
.tab { height:34px; padding:0 16px; background:transparent; border:1px solid var(--border); border-radius:8px; color:var(--dim); cursor:pointer; font:inherit; font-size:0.85rem; font-weight:600; }
.tab.active { background:var(--accent); border-color:var(--accent); color:#fff; }
/* inventory flow */
.section-head { display:flex; align-items:center; justify-content:space-between; gap:12px; margin:22px 0 10px; flex-wrap:wrap; }
.section-head h2 { font-size:1rem; }
.section-head select { height:34px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 10px; font:inherit; font-size:0.82rem; outline:none; }
.legend { display:flex; gap:16px; font-size:0.74rem; color:var(--dim); margin-bottom:6px; }
.legend i.sw { display:inline-block; width:11px; height:11px; border-radius:2px; margin-right:5px; vertical-align:-1px; }
.bar { display:flex; height:14px; border-radius:3px; overflow:hidden; background:var(--s2); min-width:40px; }
.bar > span { display:block; height:100%; }
.flow-cols { display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-top:10px; }
@media (max-width:860px) { .flow-cols { grid-template-columns:1fr; } }
.hint { font-size:0.76rem; color:var(--dim); margin:0 0 10px; }
.lst { display:flex; flex-direction:column; gap:6px; }
.row { display:flex; align-items:center; gap:10px; padding:8px 10px; background:var(--surface); border:1px solid var(--border); border-radius:8px; }
.row .nm { flex:1; min-width:0; font-size:0.82rem; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.row .nm small { display:block; font-weight:400; color:var(--dim); }
.row .mv { text-align:right; font-size:0.78rem; white-space:nowrap; }
.row .mv b { font-size:0.9rem; }
</style>
</head>
<body>
<div class="header">
  <h1>Analytics</h1>
  <div class="tabs">
    <button class="tab active" data-tab="skus" onclick="switchTab('skus')">SKUs</button>
    <button class="tab" data-tab="flow" onclick="switchTab('flow')">Inventory Flow</button>
  </div>
  <button class="btn btn-secondary btn-sm" onclick="runPipeline()" id="run-btn">▶ Run Now</button>
  <button class="btn btn-secondary btn-sm" onclick="runBackfill()" id="bf-btn">↻ Full Backfill</button>
  <span id="status-label" style="font-size:0.78rem;color:var(--dim);margin-left:auto;"></span>
</div>

<div class="main" id="tab-skus">
  <div class="stats" id="stats"><div class="spinner"></div></div>

  <div class="controls">
    <input type="text" id="q" placeholder="Search by product name..." oninput="debounce()">
    <select id="show-filter" onchange="doSearch()">
      <option value="all">All SKUs</option>
      <option value="selling">Has Sales</option>
      <option value="stale">No Sales (90d)</option>
    </select>
    <select id="sort-select" onchange="doSearch()">
      <option value="velocity_asc">Fastest Selling (fewest days left)</option>
      <option value="sold_desc">Units Sold (high)</option>
      <option value="sold_asc">Units Sold (low)</option>
      <option value="days_asc">Avg Days to Sell (fast)</option>
      <option value="price_desc">Price (high)</option>
      <option value="title_asc">Name A-Z</option>
    </select>
  </div>

  <div id="results"><div class="spinner"></div></div>
  <div class="pg" id="pagination"></div>
</div>

<div class="main" id="tab-flow" style="display:none;">
  <div class="stats" id="flow-kpis"><div class="spinner"></div></div>

  <div class="section-head">
    <h2>Where capital sits vs. where it sells</h2>
    <select id="flow-dim" onchange="loadFlow()">
      <option value="product_type">Group by Product Type</option>
      <option value="ip">Group by Game / IP</option>
      <option value="form_factor">Group by Form Factor</option>
      <option value="set_name">Group by Set</option>
      <option value="era">Group by Era</option>
    </select>
  </div>
  <div class="legend">
    <span><i class="sw" style="background:var(--green)"></i>Fast</span>
    <span><i class="sw" style="background:var(--amber)"></i>Medium</span>
    <span><i class="sw" style="background:#d98a4b"></i>Slow</span>
    <span><i class="sw" style="background:var(--dim)"></i>No sales</span>
  </div>
  <div id="flow-rollup"><div class="spinner"></div></div>

  <div class="flow-cols">
    <div class="flow-col">
      <div class="section-head">
        <h2>🟡 Buying &amp; breakdown</h2>
        <select id="dead-ptype" onchange="loadDead()">
          <option value="">All types</option>
          <option value="sealed">Sealed</option>
          <option value="card">Cards</option>
          <option value="board_game">Board games</option>
          <option value="accessory">Supplies</option>
        </select>
      </div>
      <p class="hint">In stock but not moving — zero sales in 90d, or 6+ months of stock at the current rate. Stop buying, or break it down.</p>
      <div id="dead-list"><div class="spinner"></div></div>
    </div>
    <div class="flow-col">
      <div class="section-head">
        <h2>🔵 Reorder from distro</h2>
        <select id="restock-ptype" onchange="loadRestock()">
          <option value="">All types</option>
          <option value="sealed">Sealed</option>
          <option value="board_game">Board games</option>
          <option value="accessory">Supplies</option>
        </select>
      </div>
      <p class="hint">Under ~30 days of stock left at the current rate (out-of-stock non-singles shown first). Reorder soon.</p>
      <div id="restock-list"><div class="spinner"></div></div>
    </div>
  </div>
</div>

<script>
let _page = 1, _timer = null;

function debounce() { clearTimeout(_timer); _timer = setTimeout(() => doSearch(), 400); }

async function doSearch(page) {
  _page = page || 1;
  const q = document.getElementById('q').value.trim();
  const show = document.getElementById('show-filter').value;
  const sort = document.getElementById('sort-select').value;
  const el = document.getElementById('results');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch(`/api/browse?q=${encodeURIComponent(q)}&show=${show}&sort=${sort}&page=${_page}`);
    const d = await r.json();
    renderTable(d.items, d.total, d.page, d.pages);
  } catch(e) { el.innerHTML = `<div class="empty">${e.message}</div>`; }
}

function velBadge(doi, units, qty) {
  if (!units || units === 0) return '<span class="badge badge-dim">No Sales</span>';
  const daily = (units / 90).toFixed(1);
  let label, cls;
  if (daily >= 5) { label = 'Very Fast'; cls = 'badge-green'; }
  else if (daily >= 1) { label = 'Fast'; cls = 'badge-green'; }
  else if (daily >= 0.3) { label = 'Medium'; cls = 'badge-amber'; }
  else if (daily >= 0.1) { label = 'Slow'; cls = 'badge-red'; }
  else { label = 'Very Slow'; cls = 'badge-red'; }
  const stockStr = qty === 0 ? ' · <span style="color:var(--red);">OOS</span>' : doi < 9999 ? ' · ' + Math.round(doi) + 'd stock' : '';
  return '<span class="badge ' + cls + '">' + label + '</span> <small style="color:var(--dim);">' + daily + '/day' + stockStr + '</small>';
}

function renderTable(items, total, page, pages) {
  const el = document.getElementById('results');
  if (!items.length) { el.innerHTML = '<div class="empty">No data yet. Run the backfill first.</div>'; return; }
  el.innerHTML = `
    <div style="font-size:0.78rem;color:var(--dim);margin-bottom:8px;">${total} SKUs</div>
    <div style="overflow-x:auto;"><table>
      <thead><tr>
        <th>Product</th><th>Velocity</th><th>Sold 90d</th><th>Sold 30d</th><th>Sold 7d</th>
        <th>Avg Days</th><th>Qty</th><th>Price</th><th>Avg Sale</th><th>Trend</th><th>OOS Days</th>
      </tr></thead>
      <tbody>${items.map(i => {
        const trend = i.price_trend_pct || 0;
        const trendColor = trend > 5 ? 'var(--green)' : trend < -5 ? 'var(--red)' : 'var(--dim)';
        const trendStr = (trend >= 0 ? '+' : '') + trend.toFixed(1) + '%';
        const days = i.avg_days_to_sell ? i.avg_days_to_sell.toFixed(1) + 'd' : '—';
        return '<tr>' +
          '<td><strong>' + (i.title||'—') + '</strong>' +
            (i.tcgplayer_id ? '<br><small style="color:var(--dim)">TCG#' + i.tcgplayer_id + '</small>' : '') +
          '</td>' +
          '<td>' + velBadge(i.velocity_score, i.units_sold_90d, i.current_qty) + '</td>' +
          '<td style="font-weight:600;">' + (i.units_sold_90d||0) + '</td>' +
          '<td>' + (i.units_sold_30d||0) + '</td>' +
          '<td>' + (i.units_sold_7d||0) + '</td>' +
          '<td>' + days + '</td>' +
          '<td style="color:' + (i.current_qty > 0 ? 'var(--green)' : 'var(--red)') + ';font-weight:600;">' + (i.current_qty||0) + '</td>' +
          '<td>$' + (i.current_price||0).toFixed(2) + '</td>' +
          '<td>$' + (i.avg_sale_price||0).toFixed(2) + '</td>' +
          '<td style="color:' + trendColor + '">' + trendStr + '</td>' +
          '<td>' + (i.out_of_stock_days||0) + '</td>' +
        '</tr>';
      }).join('')}</tbody>
    </table></div>`;
  renderPagination(page, pages);
}

function renderPagination(page, pages) {
  const el = document.getElementById('pagination');
  if (pages <= 1) { el.innerHTML = ''; return; }
  let h = '<button ' + (page<=1?'disabled':'') + ' onclick="doSearch(' + (page-1) + ')">←</button>';
  for (let p = Math.max(1,page-2); p <= Math.min(pages,page+2); p++) {
    h += '<button class="' + (p===page?'active':'') + '" onclick="doSearch(' + p + ')">' + p + '</button>';
  }
  h += '<button ' + (page>=pages?'disabled':'') + ' onclick="doSearch(' + (page+1) + ')">→</button>';
  el.innerHTML = h;
}

async function loadStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();
    document.getElementById('stats').innerHTML = `
      <div class="stat"><div class="stat-label">Total SKUs</div><div class="stat-val">${d.analytics_skus}</div></div>
      <div class="stat"><div class="stat-label">Active (has sales)</div><div class="stat-val" style="color:var(--green);">${d.active_skus}</div></div>
      <div class="stat"><div class="stat-label">Daily Records</div><div class="stat-val">${d.daily_sales_records}</div></div>
      <div class="stat"><div class="stat-label">Last Run</div><div class="stat-val" style="font-size:0.85rem;">${d.meta?.last_order_ingest?.value ? new Date(d.meta.last_order_ingest.value).toLocaleString() : 'Never'}</div></div>
    `;
    document.getElementById('status-label').textContent = d.analytics_skus > 0 ? '' : 'No data — run backfill first';
  } catch(e) {}
}

async function runPipeline() {
  const btn = document.getElementById('run-btn');
  btn.disabled = true; btn.textContent = 'Running...';
  try { await fetch('/run', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }
  catch(e) {}
  btn.textContent = 'Started!';
  setTimeout(() => { btn.disabled = false; btn.textContent = '▶ Run Now'; loadStatus(); doSearch(); }, 5000);
}

async function runBackfill() {
  const btn = document.getElementById('bf-btn');
  btn.disabled = true; btn.textContent = 'Backfilling...';
  try { await fetch('/run/backfill', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); }
  catch(e) {}
  btn.textContent = 'Started!';
  setTimeout(() => { btn.disabled = false; btn.textContent = '↻ Full Backfill'; loadStatus(); doSearch(); }, 15000);
}

// ── Inventory Flow ──────────────────────────────────────────────
let _flowLoaded = false;

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
  document.getElementById('tab-skus').style.display = name === 'skus' ? '' : 'none';
  document.getElementById('tab-flow').style.display = name === 'flow' ? '' : 'none';
  if (name === 'flow' && !_flowLoaded) { _flowLoaded = true; loadFlow(); loadDead(); loadRestock(); }
}

function fmtMoney(n) { return '$' + Math.round(n || 0).toLocaleString(); }

async function loadFlow() {
  const dim = document.getElementById('flow-dim').value;
  const roll = document.getElementById('flow-rollup');
  roll.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/flow?dim=' + dim);
    const d = await r.json();
    renderKpis(d.kpi);
    renderRollup(d.groups);
  } catch(e) { roll.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

function renderKpis(k) {
  const units = k.units || 0, sold = k.sold_90d || 0;
  const sellThrough = (units + sold) > 0 ? (sold / (units + sold) * 100) : 0;
  const daysToClear = sold > 0 ? Math.round(units / (sold / 90)) : null;
  document.getElementById('flow-kpis').innerHTML = `
    <div class="stat"><div class="stat-label">Inventory Value</div><div class="stat-val">${fmtMoney(k.inv_value)}</div></div>
    <div class="stat"><div class="stat-label">Units in Stock</div><div class="stat-val">${(units).toLocaleString()}</div></div>
    <div class="stat"><div class="stat-label">In-Stock SKUs</div><div class="stat-val">${(k.in_stock_skus||0).toLocaleString()}</div></div>
    <div class="stat"><div class="stat-label">Dead Capital</div><div class="stat-val" style="color:var(--red);">${fmtMoney(k.dead_value)}</div><div class="stat-label">${k.dead_skus||0} SKUs</div></div>
    <div class="stat"><div class="stat-label">Sell-Through 90d</div><div class="stat-val" style="color:var(--green);">${sellThrough.toFixed(0)}%</div></div>
    <div class="stat"><div class="stat-label">Days to Clear</div><div class="stat-val">${daysToClear !== null ? daysToClear + 'd' : '—'}</div></div>`;
}

function renderRollup(groups) {
  const el = document.getElementById('flow-rollup');
  if (!groups || !groups.length) { el.innerHTML = '<div class="empty">No data.</div>'; return; }
  const maxVal = Math.max(...groups.map(g => g.inv_value || 0), 1);
  const totalVal = groups.reduce((a, g) => a + (g.inv_value || 0), 0) || 1;
  el.innerHTML = `<div style="overflow-x:auto;"><table>
    <thead><tr>
      <th style="text-align:left;">Group</th><th>Capital (by velocity)</th><th>Value</th><th>% of $</th>
      <th>Sold 90d</th><th>30d</th><th>7d</th><th>Sell-Thru</th><th>Dead $</th>
    </tr></thead><tbody>${groups.map(g => {
      const v = g.inv_value || 0;
      const seg = (x) => v > 0 ? (x / v * 100) : 0;
      const barW = (v / maxVal * 100);
      const st = (g.units + g.sold_90d) > 0 ? (g.sold_90d / (g.units + g.sold_90d) * 100) : 0;
      const bar = '<div class="bar" style="width:' + Math.max(barW, 2) + '%;">' +
        '<span style="width:' + seg(g.val_fast) + '%;background:var(--green);"></span>' +
        '<span style="width:' + seg(g.val_med) + '%;background:var(--amber);"></span>' +
        '<span style="width:' + seg(g.val_slow) + '%;background:#d98a4b;"></span>' +
        '<span style="width:' + seg(g.val_dead) + '%;background:var(--dim);"></span></div>';
      return '<tr>' +
        '<td style="text-align:left;font-weight:600;">' + g.grp + '</td>' +
        '<td style="min-width:200px;">' + bar + '</td>' +
        '<td style="font-weight:600;">' + fmtMoney(v) + '</td>' +
        '<td>' + (v / totalVal * 100).toFixed(0) + '%</td>' +
        '<td style="font-weight:600;">' + (g.sold_90d||0) + '</td>' +
        '<td>' + (g.sold_30d||0) + '</td>' +
        '<td>' + (g.sold_7d||0) + '</td>' +
        '<td>' + st.toFixed(0) + '%</td>' +
        '<td style="color:' + (g.val_dead > 0 ? 'var(--red)' : 'var(--dim)') + ';">' + fmtMoney(g.val_dead) + '</td>' +
      '</tr>';
    }).join('')}</tbody></table></div>`;
}

async function loadDead() {
  const ptype = document.getElementById('dead-ptype').value;
  const el = document.getElementById('dead-list');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/dead?ptype=' + encodeURIComponent(ptype));
    const d = await r.json();
    if (!d.items.length) { el.innerHTML = '<div class="empty">Nothing stale here.</div>'; return; }
    el.innerHTML = '<div class="lst">' + d.items.map(i => {
      const tied = (i.current_qty || 0) * (i.current_price || 0);
      const sub = (i.units_sold_90d > 0)
        ? Math.round(i.days_inv) + 'd of stock · ' + i.units_sold_90d + ' sold 90d'
        : 'no sales in 90d';
      return '<div class="row"><div class="nm">' + (i.title || '—') +
        '<small>' + (i.product_type || '?') + ' · qty ' + (i.current_qty||0) + '</small></div>' +
        '<div class="mv"><b>' + fmtMoney(tied) + '</b><br><small>' + sub + '</small></div></div>';
    }).join('') + '</div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

async function loadRestock() {
  const ptype = document.getElementById('restock-ptype').value;
  const el = document.getElementById('restock-list');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/inventory/restock?ptype=' + encodeURIComponent(ptype));
    const d = await r.json();
    if (!d.items.length) { el.innerHTML = '<div class="empty">Nothing running low.</div>'; return; }
    el.innerHTML = '<div class="lst">' + d.items.map(i => {
      const out = (i.current_qty || 0) === 0;
      const lead = out ? '<b style="color:var(--red);">OUT</b>'
                       : '<b>' + Math.round(i.days_inv) + 'd</b> left';
      return '<div class="row"><div class="nm">' + (i.title || '—') +
        '<small>' + (i.product_type || '?') + ' · qty ' + (i.current_qty||0) + '</small></div>' +
        '<div class="mv">' + lead + '<br><small>' + (i.units_sold_30d||0) + ' · 30d / ' + (i.units_sold_7d||0) + ' · 7d</small></div></div>';
    }).join('') + '</div>';
  } catch(e) { el.innerHTML = '<div class="empty">' + e.message + '</div>'; }
}

loadStatus();
doSearch();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
