"""
drops — drops.pack-fresh.com
Drop planner: schedule weekly + VIP drops, manage releases, backfill history.
Replaces manual tag management + drop_updater cron.
"""

import os
import logging
from datetime import datetime, date, timezone
from flask import Flask, request, jsonify, render_template_string, make_response, g

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()


from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner"], public_prefixes=('/static', '/api/'),
                    skip_jwt_prefixes=('/release',))


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "drops"})


@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


# ═══════════════════════════════════════════════════════════════════════════════
# API: Product Search
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify({"results": []})
    from service import search_products
    return jsonify({"results": search_products(q)})


# ═══════════════════════════════════════════════════════════════════════════════
# API: Deal Candidates (high inventory items)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/candidates")
def deal_candidates():
    min_qty = int(request.args.get("min_qty", 10))
    limit = int(request.args.get("limit", 50))
    rows = db.query("""
        SELECT c.shopify_variant_id, c.shopify_product_id, c.title,
               c.shopify_qty, c.shopify_price, c.tcgplayer_id,
               a.velocity_score, a.units_sold_90d, a.avg_days_to_sell,
               sc.avg_cogs
        FROM inventory_product_cache c
        LEFT JOIN sku_analytics a ON a.shopify_variant_id = c.shopify_variant_id
        LEFT JOIN sealed_cogs sc ON sc.tcgplayer_id = c.tcgplayer_id
        WHERE c.shopify_qty >= %s AND c.is_damaged = FALSE
        ORDER BY c.shopify_qty DESC
        LIMIT %s
    """, (min_qty, limit))
    return jsonify({"candidates": [_ser(r) for r in rows]})


# ═══════════════════════════════════════════════════════════════════════════════
# API: Create Drop
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/drops", methods=["POST"])
def create_drop():
    data = request.get_json(silent=True) or {}
    product_gid = data.get("product_gid")
    variant_gid = data.get("variant_gid")
    variant_id = data.get("variant_id")
    product_id = data.get("product_id")
    title = data.get("title", "")
    drop_date = data.get("drop_date")  # YYYY-MM-DD
    drop_price = data.get("drop_price")
    original_price = data.get("original_price")
    drop_type = data.get("drop_type", "weekly")
    vip_price_cents = data.get("vip_price_cents")
    qty_offered = data.get("qty_offered")

    if not all([product_gid, variant_gid, drop_date, drop_price]):
        return jsonify({"error": "product_gid, variant_gid, drop_date, drop_price required"}), 400

    # Set up the drop in Shopify
    from service import setup_drop
    try:
        result = setup_drop(
            product_gid, variant_gid, drop_date, float(drop_price),
            drop_type=drop_type,
            vip_price_cents=int(vip_price_cents) if vip_price_cents else None,
            limit_qty=int(qty_offered) if qty_offered else None,
        )
    except Exception as e:
        return jsonify({"error": f"Shopify setup failed: {e}"}), 500

    # Record in drop_events
    db.execute("""
        INSERT INTO drop_events
            (shopify_variant_id, shopify_product_id, drop_date, drop_name,
             qty_offered, limit_qty, status, drop_type, original_price, drop_price, title)
        VALUES (%s, %s, %s, %s, %s, %s, 'scheduled', %s, %s, %s, %s)
        ON CONFLICT (shopify_variant_id, drop_date) DO UPDATE SET
            drop_name = EXCLUDED.drop_name,
            qty_offered = EXCLUDED.qty_offered,
            limit_qty = EXCLUDED.limit_qty,
            drop_type = EXCLUDED.drop_type,
            original_price = EXCLUDED.original_price,
            drop_price = EXCLUDED.drop_price,
            title = EXCLUDED.title,
            status = 'scheduled'
    """, (
        variant_id or int(result.get("variant_id", 0)),
        product_id,
        drop_date,
        title,
        qty_offered,
        qty_offered,  # limit_qty = same as qty_offered
        drop_type,
        original_price,
        float(drop_price),
        title,
    ))

    return jsonify({"ok": True, **result})


# ═══════════════════════════════════════════════════════════════════════════════
# API: Release Drops (called daily at 11 AM by Flow)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/release", methods=["POST"])
def release_drops():
    """Release all scheduled drops for today."""
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

    today = date.today().isoformat()
    drops = db.query(
        "SELECT * FROM drop_events WHERE drop_date = %s AND status = 'scheduled'",
        (today,)
    )

    if not drops:
        return jsonify({"ok": True, "released": 0, "message": "No drops scheduled for today"})

    from service import release_drop
    released = []
    errors = []
    for drop in drops:
        product_gid = f"gid://shopify/Product/{drop['shopify_product_id']}"
        try:
            result = release_drop(product_gid)
            db.execute(
                "UPDATE drop_events SET status = 'active' WHERE id = %s",
                (drop["id"],)
            )
            released.append({"title": drop["title"], **result})
        except Exception as e:
            logger.error(f"Failed to release drop {drop['title']}: {e}")
            errors.append({"title": drop["title"], "error": str(e)})

    return jsonify({"ok": True, "released": len(released), "errors": errors, "details": released})


# ═══════════════════════════════════════════════════════════════════════════════
# API: List Drops
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/drops")
def list_drops():
    status = request.args.get("status", "all")
    if status == "all":
        rows = db.query("SELECT * FROM drop_events ORDER BY drop_date DESC LIMIT 100")
    else:
        rows = db.query("SELECT * FROM drop_events WHERE status = %s ORDER BY drop_date DESC LIMIT 100", (status,))
    return jsonify({"drops": [_ser(r) for r in rows]})


# ═══════════════════════════════════════════════════════════════════════════════
# API: Backfill Past Drop
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/drops/backfill", methods=["POST"])
def backfill_drop():
    """Record a past drop for analytics exclusion."""
    data = request.get_json(silent=True) or {}
    variant_id = data.get("variant_id")
    product_id = data.get("product_id")
    drop_date = data.get("drop_date")
    title = data.get("title", "")
    units_sold = data.get("units_sold")
    drop_type = data.get("drop_type", "weekly")

    if not variant_id or not drop_date:
        return jsonify({"error": "variant_id and drop_date required"}), 400

    db.execute("""
        INSERT INTO drop_events
            (shopify_variant_id, shopify_product_id, drop_date, drop_name,
             units_sold, status, drop_type, title)
        VALUES (%s, %s, %s, %s, %s, 'completed', %s, %s)
        ON CONFLICT (shopify_variant_id, drop_date) DO UPDATE SET
            units_sold = EXCLUDED.units_sold,
            title = EXCLUDED.title,
            status = 'completed'
    """, (variant_id, product_id, drop_date, title, units_sold, drop_type, title))

    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# API: Delete Drop
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/drops/<int:drop_id>", methods=["DELETE"])
def delete_drop(drop_id):
    db.execute("DELETE FROM drop_events WHERE id = %s", (drop_id,))
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════════════════════
# API: Founders' Picks
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/founders")
def list_founders_picks():
    from service import get_founders_picks
    return jsonify({"picks": get_founders_picks()})


@app.route("/api/founders", methods=["POST"])
def create_founder_pick():
    data = request.get_json(silent=True) or {}
    product_gid = data.get("product_gid")
    founder = data.get("founder", "").strip().lower()
    note = data.get("note", "").strip()

    if not product_gid or not founder or not note:
        return jsonify({"error": "product_gid, founder, and note required"}), 400

    from service import set_founder_pick
    try:
        result = set_founder_pick(product_gid, founder, note)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Shopify update failed: {e}"}), 500

    return jsonify(result)


@app.route("/api/founders/remove", methods=["POST"])
def remove_founders_picks():
    """Bulk remove founder's pick status from products."""
    data = request.get_json(silent=True) or {}
    product_gids = data.get("product_gids", [])

    if not product_gids:
        return jsonify({"error": "product_gids required"}), 400

    from service import remove_founder_pick
    removed = []
    errors = []
    for gid in product_gids:
        try:
            remove_founder_pick(gid)
            removed.append(gid)
        except Exception as e:
            logger.error(f"Failed to remove founder pick {gid}: {e}")
            errors.append({"gid": gid, "error": str(e)})

    return jsonify({"ok": True, "removed": len(removed), "errors": errors})


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


# ═══════════════════════════════════════════════════════════════════════════════
# HTML Dashboard
# ═══════════════════════════════════════════════════════════════════════════════

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pack Fresh — Drop Planner</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
.header h1 { font-size:1.3rem; }
.main { max-width:900px; margin:0 auto; padding:20px; }
.tabs { display:flex; gap:2px; margin-bottom:20px; border-bottom:1px solid var(--border); }
.tab { background:none; border:none; padding:10px 18px; color:var(--text-dim); cursor:pointer; font-size:0.88rem; font-weight:500; border-bottom:2px solid transparent; }
.tab:hover { color:var(--text); }
.tab.active { color:var(--accent); border-bottom-color:var(--accent); }
.form-row { display:flex; gap:10px; margin-bottom:12px; flex-wrap:wrap; align-items:end; }
.form-group { display:flex; flex-direction:column; gap:4px; flex:1; min-width:140px; }
.form-label { font-size:0.7rem; color:var(--text-dim); text-transform:uppercase; letter-spacing:0.06em; }
.form-input, .form-select { height:38px; background:var(--surface-2); border:1.5px solid var(--border); border-radius:8px; color:var(--text); padding:0 12px; font-size:0.85rem; font-family:inherit; outline:none; width:100%; }
.form-input:focus { border-color:var(--accent); }
.badge-scheduled { background:rgba(79,125,249,0.15); color:var(--accent); }
.badge-active { background:rgba(52,208,88,0.15); color:var(--green); }
.badge-completed { background:var(--surface-2); color:var(--text-dim); }
.pane { display:none; }
.pane.active { display:block; }
.search-result { display:flex; align-items:center; gap:12px; padding:10px; border:1px solid var(--border); border-radius:8px; margin-bottom:6px; cursor:pointer; transition:border-color 0.15s; }
.search-result:hover { border-color:var(--accent); }
.search-result img { width:48px; height:48px; object-fit:contain; border-radius:4px; }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:20px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>
<div class="header">
  <h1>🎯 Drop Planner</h1>
  <button class="btn btn-green btn-sm" onclick="manualRelease()" style="margin-left:auto;">🚀 Release Now</button>
</div>

<div class="main">
  <div class="tabs">
    <button class="tab active" onclick="switchTab('create',this)">+ Create Drop</button>
    <button class="tab" onclick="switchTab('scheduled',this)">📅 Scheduled</button>
    <button class="tab" onclick="switchTab('history',this)">📜 History</button>
    <button class="tab" onclick="switchTab('candidates',this)">🔍 Deal Candidates</button>
    <button class="tab" onclick="switchTab('backfill',this)">↩ Backfill</button>
    <button class="tab" onclick="switchTab('founders',this)">Founders' Picks</button>
  </div>

  <!-- CREATE DROP -->
  <div class="pane active" id="pane-create">
    <div class="card">
      <div class="form-row">
        <div class="form-group" style="flex:3;">
          <span class="form-label">Search Product</span>
          <input class="form-input" id="drop-search" placeholder="Type product name..." oninput="debounceSearch()">
        </div>
      </div>
      <div id="search-results"></div>
    </div>
    <div id="drop-form" style="display:none;">
      <div class="card">
        <div style="font-weight:700;margin-bottom:12px;" id="selected-title"></div>
        <div class="form-row">
          <div class="form-group">
            <span class="form-label">Drop Date</span>
            <input class="form-input" id="drop-date" type="date">
          </div>
          <div class="form-group">
            <span class="form-label">Drop Price ($)</span>
            <input class="form-input" id="drop-price" type="number" step="0.01">
          </div>
          <div class="form-group">
            <span class="form-label">Original Price ($)</span>
            <input class="form-input" id="orig-price" type="number" step="0.01" readonly style="opacity:0.5;">
          </div>
        </div>
        <div class="form-row">
          <div class="form-group">
            <span class="form-label">Type</span>
            <select class="form-select" id="drop-type" onchange="toggleVipFields()">
              <option value="weekly">Weekly Drop</option>
              <option value="vip">VIP/MSRP Drop</option>
            </select>
          </div>
          <div class="form-group">
            <span class="form-label">Limit (sets limit-X tag)</span>
            <input class="form-input" id="drop-qty" type="number" placeholder="e.g. 72">
          </div>
          <div class="form-group" id="vip-cents-group" style="display:none;">
            <span class="form-label">VIP Price (cents)</span>
            <input class="form-input" id="vip-cents" type="number" placeholder="e.g. 4599">
          </div>
        </div>
        <button class="btn btn-primary" onclick="submitDrop()">🎯 Schedule Drop</button>
      </div>
    </div>
  </div>

  <!-- SCHEDULED -->
  <div class="pane" id="pane-scheduled"><div class="spinner"></div></div>

  <!-- HISTORY -->
  <div class="pane" id="pane-history"><div class="spinner"></div></div>

  <!-- DEAL CANDIDATES -->
  <div class="pane" id="pane-candidates">
    <div class="card">
      <div class="form-row">
        <div class="form-group">
          <span class="form-label">Min Qty</span>
          <input class="form-input" id="cand-min" type="number" value="10">
        </div>
        <button class="btn btn-primary" onclick="loadCandidates()">Search</button>
      </div>
    </div>
    <div id="candidates-list"></div>
  </div>

  <!-- FOUNDERS' PICKS -->
  <div class="pane" id="pane-founders">
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
        <span style="font-weight:700;">Current Founders' Picks</span>
        <div style="display:flex;gap:8px;">
          <button class="btn btn-sm btn-secondary" onclick="selectAllFounders()">Select All</button>
          <button class="btn btn-sm btn-red" onclick="bulkRemoveFounders()">Remove Selected</button>
          <button class="btn btn-sm btn-secondary" onclick="loadFoundersPicks()">Refresh</button>
        </div>
      </div>
      <div id="founders-list"><div class="spinner"></div></div>
    </div>
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
        <span style="font-weight:700;">Pick Candidates</span>
        <div class="form-row" style="margin-bottom:0;gap:8px;flex-wrap:nowrap;">
          <div class="form-group" style="min-width:80px;flex:0;">
            <span class="form-label">Min Qty</span>
            <input class="form-input" id="fp-cand-min" type="number" value="4" style="width:80px;">
          </div>
          <button class="btn btn-sm btn-primary" onclick="loadFpCandidates()" style="align-self:end;">Load</button>
        </div>
      </div>
      <div id="fp-candidates-list" style="color:var(--dim);font-size:0.82rem;padding:8px;">Click Load to find items with enough stock</div>
    </div>
    <div class="card">
      <span style="font-weight:700;display:block;margin-bottom:12px;">Add Founder's Pick</span>
      <div class="form-row">
        <div class="form-group" style="flex:3;">
          <span class="form-label">Search Product</span>
          <input class="form-input" id="fp-search" placeholder="Type product name or select from candidates above..." oninput="debounceFpSearch()">
        </div>
      </div>
      <div id="fp-search-results"></div>
      <div id="fp-form" style="display:none;">
        <div style="font-weight:600;margin:12px 0 8px;" id="fp-title"></div>
        <div class="form-row">
          <div class="form-group">
            <span class="form-label">Founder</span>
            <select class="form-select" id="fp-founder">
              <option value="sean">Sean</option>
              <option value="stuart">Stuart</option>
              <option value="kayla">Kayla</option>
              <option value="hayley">Hayley</option>
            </select>
          </div>
        </div>
        <div class="form-row">
          <div class="form-group" style="flex:1;">
            <span class="form-label">Note (why they love this item)</span>
            <textarea class="form-input" id="fp-note" rows="3" placeholder="Write the founder's note here..." style="height:80px;padding:10px;resize:vertical;"></textarea>
          </div>
        </div>
        <button class="btn btn-primary" onclick="submitFounderPick()">Add Founder's Pick</button>
      </div>
    </div>
  </div>

  <!-- BACKFILL -->
  <div class="pane" id="pane-backfill">
    <div class="card">
      <p style="color:var(--dim);margin-bottom:12px;">Record a past drop so analytics excludes it from velocity calculations.</p>
      <div class="form-row">
        <div class="form-group" style="flex:2;">
          <span class="form-label">Search Product</span>
          <input class="form-input" id="bf-search" placeholder="Product name..." oninput="debounceBfSearch()">
        </div>
      </div>
      <div id="bf-search-results"></div>
      <div id="bf-form" style="display:none;">
        <div style="font-weight:600;margin:12px 0 8px;" id="bf-title"></div>
        <div class="form-row">
          <div class="form-group">
            <span class="form-label">Drop Date</span>
            <input class="form-input" id="bf-date" type="date">
          </div>
          <div class="form-group">
            <span class="form-label">Units Sold (optional)</span>
            <input class="form-input" id="bf-units" type="number">
          </div>
          <div class="form-group">
            <span class="form-label">Type</span>
            <select class="form-select" id="bf-type">
              <option value="weekly">Weekly</option>
              <option value="vip">VIP/MSRP</option>
            </select>
          </div>
        </div>
        <button class="btn btn-primary" onclick="submitBackfill()">↩ Record Past Drop</button>
      </div>
    </div>
  </div>
</div>

<script>
let _selected = null; // {product_gid, variant_gid, variant_id, product_id, title, price}
let _bfSelected = null;
let _timer = null, _bfTimer = null;

// velocity_score = days of inventory remaining (lower = selling faster)
function velBadge(v) {
  if (!v || v >= 9999) return '<span style="color:var(--dim);">—</span>';
  const d = Math.round(v);
  if (d <= 30) return '<span style="color:var(--green);">' + d + 'd</span>';
  if (d <= 90) return '<span style="color:var(--amber);">' + d + 'd</span>';
  return '<span style="color:var(--red);">' + d + 'd</span>';
}

function switchTab(id, btn) {
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('pane-' + id).classList.add('active');
  btn.classList.add('active');
  if (id === 'scheduled') loadDrops('scheduled');
  if (id === 'history') loadDrops('history');
  if (id === 'founders') loadFoundersPicks();
}

// Search
function debounceSearch() { clearTimeout(_timer); _timer = setTimeout(doSearch, 400); }
async function doSearch() {
  const q = document.getElementById('drop-search').value.trim();
  const el = document.getElementById('search-results');
  if (q.length < 2) { el.innerHTML = ''; return; }
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/search?q=' + encodeURIComponent(q));
    const d = await r.json();
    el.innerHTML = (d.results||[]).map(p => {
      const v = p.variants[0] || {};
      const img = p.image_url ? `<img src="${p.image_url}">` : '';
      return `<div class="search-result" onclick='selectProduct(${JSON.stringify(p).replace(/'/g,"&#39;")})'>
        ${img}
        <div style="flex:1;">
          <div style="font-weight:600;">${p.title}</div>
          <div style="font-size:0.78rem;color:var(--dim);">$${v.price?.toFixed(2)||'?'} · qty ${p.total_inventory} · ${p.status}</div>
        </div>
      </div>`;
    }).join('') || '<div style="color:var(--dim);padding:10px;">No results</div>';
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

function selectProduct(p) {
  const v = p.variants[0];
  _selected = { product_gid: p.id, variant_gid: v.id, variant_id: v.numeric_id, product_id: p.numeric_id, title: p.title, price: v.price };
  document.getElementById('selected-title').textContent = p.title + ' ($' + v.price.toFixed(2) + ' · qty ' + p.total_inventory + ')';
  document.getElementById('orig-price').value = v.price.toFixed(2);
  document.getElementById('drop-form').style.display = '';
  document.getElementById('search-results').innerHTML = '';
}

function toggleVipFields() {
  document.getElementById('vip-cents-group').style.display = document.getElementById('drop-type').value === 'vip' ? '' : 'none';
}

async function submitDrop() {
  if (!_selected) return;
  const body = {
    product_gid: _selected.product_gid,
    variant_gid: _selected.variant_gid,
    variant_id: _selected.variant_id,
    product_id: _selected.product_id,
    title: _selected.title,
    drop_date: document.getElementById('drop-date').value,
    drop_price: parseFloat(document.getElementById('drop-price').value),
    original_price: parseFloat(document.getElementById('orig-price').value),
    drop_type: document.getElementById('drop-type').value,
    qty_offered: parseInt(document.getElementById('drop-qty').value) || null,
    vip_price_cents: parseInt(document.getElementById('vip-cents').value) || null,
  };
  if (!body.drop_date || !body.drop_price) { alert('Date and price required'); return; }
  try {
    const r = await fetch('/api/drops', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Drop scheduled! ' + _selected.title, 'green');
    document.getElementById('drop-form').style.display = 'none';
    _selected = null;
  } catch(e) { alert(e.message); }
}

// Drops list
async function loadDrops(tab) {
  const el = document.getElementById('pane-' + tab);
  el.innerHTML = '<div class="spinner"></div>';
  const status = tab === 'scheduled' ? 'scheduled' : 'all';
  try {
    const r = await fetch('/api/drops?status=' + (tab==='scheduled'?'scheduled':'all'));
    const d = await r.json();
    const drops = (tab === 'history') ? (d.drops||[]).filter(x=>x.status!=='scheduled') : (d.drops||[]);
    if (!drops.length) { el.innerHTML = '<div style="color:var(--dim);text-align:center;padding:30px;">No drops</div>'; return; }
    el.innerHTML = `<table><thead><tr>
      <th>Product</th><th>Date</th><th>Type</th><th>Price</th><th>Qty</th><th>Sold</th><th>Status</th><th></th>
    </tr></thead><tbody>${drops.map(d => `<tr>
      <td><strong>${d.title||'—'}</strong></td>
      <td>${d.drop_date||'—'}</td>
      <td><span class="badge ${d.drop_type==='vip'?'badge-active':'badge-scheduled'}">${d.drop_type||'weekly'}</span></td>
      <td>$${d.drop_price?d.drop_price.toFixed(2):'—'}</td>
      <td>${d.qty_offered||'—'}</td>
      <td>${d.units_sold||'—'}</td>
      <td><span class="badge badge-${d.status||'scheduled'}">${d.status||'scheduled'}</span></td>
      <td><button class="btn btn-sm btn-secondary" onclick="deleteDrop(${d.id})">✕</button></td>
    </tr>`).join('')}</tbody></table>`;
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

async function deleteDrop(id) {
  if (!confirm('Delete this drop record?')) return;
  await fetch('/api/drops/'+id, {method:'DELETE'});
  toast('Deleted', 'green');
  loadDrops('scheduled'); loadDrops('history');
}

// Candidates
async function loadCandidates() {
  const el = document.getElementById('candidates-list');
  const min = document.getElementById('cand-min').value || 10;
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/candidates?min_qty='+min);
    const d = await r.json();
    const items = d.candidates||[];
    if (!items.length) { el.innerHTML = '<div style="color:var(--dim);padding:20px;">No items with qty >= '+min+'</div>'; return; }
    el.innerHTML = `<table><thead><tr>
      <th>Product</th><th>Qty</th><th>Price</th><th>COGS</th><th>Margin</th><th>Sold 90d</th><th>Days to Sell</th>
    </tr></thead><tbody>${items.map(i => {
      const cogs = i.avg_cogs ? '$'+i.avg_cogs.toFixed(2) : '—';
      const margin = (i.avg_cogs && i.shopify_price) ? ((1 - i.avg_cogs/i.shopify_price)*100).toFixed(0)+'%' : '—';
      return `<tr>
        <td><strong>${i.title||'—'}</strong></td>
        <td style="font-weight:700;">${i.shopify_qty}</td>
        <td>$${(i.shopify_price||0).toFixed(2)}</td>
        <td>${cogs}</td>
        <td>${margin}</td>
        <td>${i.units_sold_90d||0}</td>
        <td>${velBadge(i.velocity_score)}</td>
      </tr>`;
    }).join('')}</tbody></table>`;
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

// Backfill
function debounceBfSearch() { clearTimeout(_bfTimer); _bfTimer = setTimeout(doBfSearch, 400); }
async function doBfSearch() {
  const q = document.getElementById('bf-search').value.trim();
  const el = document.getElementById('bf-search-results');
  if (q.length < 2) { el.innerHTML = ''; return; }
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/search?q=' + encodeURIComponent(q));
    const d = await r.json();
    el.innerHTML = (d.results||[]).map(p => {
      const v = p.variants[0]||{};
      return `<div class="search-result" onclick='selectBfProduct(${JSON.stringify({variant_id:v.numeric_id,product_id:p.numeric_id,title:p.title}).replace(/'/g,"&#39;")})'>
        <div style="flex:1;"><strong>${p.title}</strong> <span style="color:var(--dim);">· $${v.price?.toFixed(2)||'?'}</span></div>
      </div>`;
    }).join('') || '<div style="color:var(--dim);padding:10px;">No results</div>';
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

function selectBfProduct(p) {
  _bfSelected = p;
  document.getElementById('bf-title').textContent = p.title;
  document.getElementById('bf-form').style.display = '';
  document.getElementById('bf-search-results').innerHTML = '';
}

async function submitBackfill() {
  if (!_bfSelected) return;
  const body = {
    variant_id: _bfSelected.variant_id,
    product_id: _bfSelected.product_id,
    title: _bfSelected.title,
    drop_date: document.getElementById('bf-date').value,
    units_sold: parseInt(document.getElementById('bf-units').value) || null,
    drop_type: document.getElementById('bf-type').value,
  };
  if (!body.drop_date) { alert('Date required'); return; }
  try {
    const r = await fetch('/api/drops/backfill', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Past drop recorded: ' + _bfSelected.title, 'green');
    document.getElementById('bf-form').style.display = 'none';
    _bfSelected = null;
  } catch(e) { alert(e.message); }
}

// Founders' Picks
let _fpSelected = null;
let _fpTimer = null;

function debounceFpSearch() { clearTimeout(_fpTimer); _fpTimer = setTimeout(doFpSearch, 400); }
async function doFpSearch() {
  const q = document.getElementById('fp-search').value.trim();
  const el = document.getElementById('fp-search-results');
  if (q.length < 2) { el.innerHTML = ''; return; }
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/search?q=' + encodeURIComponent(q));
    const d = await r.json();
    el.innerHTML = (d.results||[]).map(p => {
      const v = p.variants[0]||{};
      const img = p.image_url ? `<img src="${p.image_url}">` : '';
      return `<div class="search-result" onclick='selectFpProduct(${JSON.stringify({product_gid:p.id,title:p.title,price:v.price,image_url:p.image_url}).replace(/'/g,"&#39;")})'>
        ${img}
        <div style="flex:1;"><strong>${p.title}</strong> <span style="color:var(--dim);">· $${v.price?.toFixed(2)||'?'}</span></div>
      </div>`;
    }).join('') || '<div style="color:var(--dim);padding:10px;">No results</div>';
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

function selectFpProduct(p) {
  _fpSelected = p;
  document.getElementById('fp-title').textContent = p.title;
  document.getElementById('fp-form').style.display = '';
  document.getElementById('fp-search-results').innerHTML = '';
}

async function submitFounderPick() {
  if (!_fpSelected) return;
  const founder = document.getElementById('fp-founder').value;
  const note = document.getElementById('fp-note').value.trim();
  if (!note) { alert('Note is required'); return; }
  try {
    const r = await fetch('/api/founders', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ product_gid: _fpSelected.product_gid, founder, note })
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast(founder.charAt(0).toUpperCase()+founder.slice(1)+"'s pick added: "+_fpSelected.title, 'green');
    document.getElementById('fp-form').style.display = 'none';
    document.getElementById('fp-note').value = '';
    _fpSelected = null;
    loadFoundersPicks();
  } catch(e) { alert(e.message); }
}

async function loadFpCandidates() {
  const el = document.getElementById('fp-candidates-list');
  const min = document.getElementById('fp-cand-min').value || 4;
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/candidates?min_qty='+min+'&limit=80');
    const d = await r.json();
    const items = d.candidates||[];
    if (!items.length) { el.innerHTML = '<div style="color:var(--dim);padding:8px;">No items with qty >= '+min+'</div>'; return; }
    el.innerHTML = `<table><thead><tr>
      <th>Product</th><th>Qty</th><th>Price</th><th>COGS</th><th>Margin</th><th>Sold 90d</th><th>Days to Sell</th><th></th>
    </tr></thead><tbody>${items.map(i => {
      const cogs = i.avg_cogs ? '$'+i.avg_cogs.toFixed(2) : '—';
      const margin = (i.avg_cogs && i.shopify_price) ? ((1 - i.avg_cogs/i.shopify_price)*100).toFixed(0)+'%' : '—';
      const gid = 'gid://shopify/Product/'+i.shopify_product_id;
      return `<tr>
        <td><strong>${i.title||'—'}</strong></td>
        <td style="font-weight:700;">${i.shopify_qty}</td>
        <td>$${(i.shopify_price||0).toFixed(2)}</td>
        <td>${cogs}</td>
        <td>${margin}</td>
        <td>${i.units_sold_90d||0}</td>
        <td>${velBadge(i.velocity_score)}</td>
        <td><button class="btn btn-sm btn-primary" onclick="pickFromCandidate('${gid}','${(i.title||'').replace(/'/g,"\\'")}')">Pick</button></td>
      </tr>`;
    }).join('')}</tbody></table>`;
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

function pickFromCandidate(gid, title) {
  _fpSelected = { product_gid: gid, title: title };
  document.getElementById('fp-title').textContent = title;
  document.getElementById('fp-form').style.display = '';
  document.getElementById('fp-form').scrollIntoView({behavior:'smooth'});
}

async function loadFoundersPicks() {
  const el = document.getElementById('founders-list');
  el.innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/founders');
    const d = await r.json();
    const picks = d.picks||[];
    if (!picks.length) { el.innerHTML = '<div style="color:var(--dim);text-align:center;padding:20px;">No founders\\'s picks set</div>'; return; }
    const colors = {sean:'var(--accent)',stuart:'var(--green)',kayla:'var(--amber)',hayley:'#e879f9'};
    el.innerHTML = picks.map(p => {
      const c = colors[p.founder.toLowerCase()]||'var(--dim)';
      const img = p.image_url ? `<img src="${p.image_url}" style="width:48px;height:48px;object-fit:contain;border-radius:4px;">` : '';
      return `<div style="display:flex;align-items:start;gap:12px;padding:10px;border:1px solid var(--border);border-radius:8px;margin-bottom:6px;">
        <input type="checkbox" class="fp-check" value="${p.product_gid}" style="margin-top:4px;">
        ${img}
        <div style="flex:1;">
          <div style="font-weight:600;">${p.title}</div>
          <div style="margin-top:4px;"><span style="color:${c};font-weight:700;font-size:0.8rem;text-transform:capitalize;">${p.founder}</span> <span style="color:var(--dim);font-size:0.78rem;">· $${p.price?.toFixed(2)||'?'} · qty ${p.total_inventory}</span></div>
          <div style="color:var(--dim);font-size:0.78rem;margin-top:4px;white-space:pre-line;">${p.founder_note||''}</div>
        </div>
        <button class="btn btn-sm btn-secondary" onclick="removeSingleFounder('${p.product_gid}')">✕</button>
      </div>`;
    }).join('');
  } catch(e) { el.innerHTML = `<div style="color:var(--red);">${e.message}</div>`; }
}

function selectAllFounders() {
  const checks = document.querySelectorAll('.fp-check');
  const allChecked = [...checks].every(c => c.checked);
  checks.forEach(c => c.checked = !allChecked);
}

async function bulkRemoveFounders() {
  const gids = [...document.querySelectorAll('.fp-check:checked')].map(c => c.value);
  if (!gids.length) { alert('Select items to remove'); return; }
  if (!confirm('Remove ' + gids.length + ' founder\\'s pick(s)?')) return;
  try {
    const r = await fetch('/api/founders/remove', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ product_gids: gids })
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Removed ' + d.removed + ' founder\\'s pick(s)', 'green');
    loadFoundersPicks();
  } catch(e) { alert(e.message); }
}

async function removeSingleFounder(gid) {
  if (!confirm('Remove this founder\\'s pick?')) return;
  try {
    const r = await fetch('/api/founders/remove', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ product_gids: [gid] })
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Founder\\'s pick removed', 'green');
    loadFoundersPicks();
  } catch(e) { alert(e.message); }
}

// Manual release
async function manualRelease() {
  if (!confirm('Release all drops scheduled for today?')) return;
  try {
    const r = await fetch('/release', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
    const d = await r.json();
    toast('Released ' + (d.released||0) + ' drops', 'green');
  } catch(e) { alert(e.message); }
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
