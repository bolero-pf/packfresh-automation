"""
analytics — analytics.pack-fresh.com (or internal)
SKU sell-through analytics: daily order ingestion + velocity metrics.

Triggered daily via Shopify Flow webhook to /run.
Also exposes /api/analytics for batch lookups from other services.
"""

import os
import logging
import threading
from flask import Flask, request, jsonify, render_template_string

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()


@app.before_request
def _check_auth():
    """JWT auth for browser UI, skip API/webhook endpoints."""
    if request.path in ('/ping', '/health', '/run', '/run/backfill'):
        return  # webhooks + health checks
    if request.path.startswith('/api/'):
        return  # API calls from other services
    try:
        from auth import require_auth
        return require_auth(roles=["owner"])
    except Exception:
        pass

@app.after_request
def _add_admin_bar(response):
    try:
        from auth import inject_admin_bar, get_current_user
        if get_current_user():
            return inject_admin_bar(response)
    except Exception:
        pass
    return response


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
    # Verify webhook secret if present (optional — also allow manual triggers)
    secret = request.headers.get("X-Flow-Secret", "")
    flow_secret = os.environ.get("VIP_FLOW_SECRET", "")
    if flow_secret and secret and secret != flow_secret:
        return jsonify({"error": "Invalid secret"}), 401

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
    """Force a full 90-day backfill (slower, use sparingly)."""
    def _run():
        try:
            from compute import ingest_orders, recompute_analytics
            ingest_orders(full_backfill=True)
            recompute_analytics()
            logger.info("Full backfill complete")
        except Exception as e:
            logger.exception(f"Backfill failed: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "started": True, "mode": "backfill"})


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
<title>Pack Fresh — SKU Analytics</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<style>
:root { --bg:#0a0c10; --surface:#141720; --s2:#1c2030; --border:#2a2f42; --accent:#4f7df9; --green:#34d058; --amber:#f6ad55; --red:#fc5c5c; --text:#e8eaf0; --dim:#6b7280; }
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-family:'DM Sans',sans-serif; font-size:14px; }
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
.header h1 { font-size:1.3rem; }
.main { max-width:1100px; margin:0 auto; padding:20px; }
.stats { display:flex; gap:16px; margin-bottom:20px; flex-wrap:wrap; }
.stat { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px 18px; min-width:140px; }
.stat-label { font-size:0.72rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.08em; }
.stat-val { font-size:1.4rem; font-weight:700; margin-top:4px; }
.controls { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; align-items:center; }
.controls input, .controls select { height:38px; background:var(--s2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 12px; font-size:0.85rem; font-family:inherit; outline:none; }
.controls input:focus { border-color:var(--accent); }
.controls input { flex:1; min-width:200px; }
.btn { height:38px; padding:0 16px; border:none; border-radius:8px; font-family:inherit; font-size:0.85rem; font-weight:600; cursor:pointer; }
.btn-primary { background:var(--accent); color:#fff; }
.btn-secondary { background:var(--s2); border:1px solid var(--border); color:var(--text); }
.btn-sm { height:30px; padding:0 10px; font-size:0.75rem; }
table { width:100%; border-collapse:collapse; font-size:0.82rem; }
th { text-align:left; color:var(--dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.06em; padding:8px; border-bottom:1px solid var(--border); cursor:pointer; }
th:hover { color:var(--text); }
td { padding:8px; border-bottom:1px solid var(--border); }
.vel-fast { color:var(--green); font-weight:700; }
.vel-med { color:var(--amber); font-weight:600; }
.vel-slow { color:var(--red); }
.vel-none { color:var(--dim); }
.badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:0.68rem; font-weight:700; }
.badge-green { background:rgba(52,208,88,0.15); color:var(--green); }
.badge-amber { background:rgba(246,173,85,0.15); color:var(--amber); }
.badge-red { background:rgba(252,92,92,0.15); color:var(--red); }
.badge-dim { background:var(--s2); color:var(--dim); }
.pg { display:flex; gap:4px; justify-content:center; margin-top:16px; }
.pg button { height:32px; min-width:32px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); cursor:pointer; font-size:0.8rem; }
.pg button.active { background:var(--accent); border-color:var(--accent); color:#fff; }
.pg button:disabled { opacity:0.3; }
.empty { text-align:center; padding:40px; color:var(--dim); }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:40px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>
<div class="header">
  <h1>SKU Analytics</h1>
  <button class="btn btn-secondary btn-sm" onclick="runPipeline()" id="run-btn">▶ Run Now</button>
  <button class="btn btn-secondary btn-sm" onclick="runBackfill()" id="bf-btn">↻ Full Backfill</button>
  <span id="status-label" style="font-size:0.78rem;color:var(--dim);margin-left:auto;"></span>
</div>

<div class="main">
  <div class="stats" id="stats"><div class="spinner"></div></div>

  <div class="controls">
    <input type="text" id="q" placeholder="Search by product name..." oninput="debounce()">
    <select id="show-filter" onchange="doSearch()">
      <option value="all">All SKUs</option>
      <option value="selling">Has Sales</option>
      <option value="stale">No Sales (90d)</option>
    </select>
    <select id="sort-select" onchange="doSearch()">
      <option value="velocity_desc">Velocity (high first)</option>
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

function velBadge(v, units) {
  if (!units || units === 0) return '<span class="badge badge-dim">No Sales</span>';
  const daily = units / 90;
  if (daily > 1) return '<span class="badge badge-green">Very Fast</span>';
  if (daily > 0.5) return '<span class="badge badge-green">Fast</span>';
  if (daily > 0.15) return '<span class="badge badge-amber">Medium</span>';
  if (daily > 0.05) return '<span class="badge badge-red">Slow</span>';
  return '<span class="badge badge-red">Very Slow</span>';
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
          '<td>' + velBadge(i.velocity_score, i.units_sold_90d) + '</td>' +
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

loadStatus();
doSearch();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
