"""
routes/barcode_bind.py — Scan-to-bind utility for sealed product barcodes.

Flow:
  1) Scan a UPC/GTIN.
  2) Look it up in Shopify (productVariants query: "barcode:...").
     - Hit  → show the product/variant; ready for next scan.
     - Miss → search the local product cache by title, pick a product,
              pick a variant, and assign the scanned barcode to it.

Variants in Shopify each carry their own barcode field, so a "Dragon Shield
Matte (Blue)" variant can hold a different UPC than "(Black)" under the same
product listing.
"""

import os
import logging
from functools import wraps

from flask import Blueprint, request, jsonify, g
import requests

import db
from shopify_graphql import shopify_gql

logger = logging.getLogger(__name__)

bp = Blueprint("barcode_bind", __name__, url_prefix="/inventory/barcode-bind")

DRY_RUN = os.getenv("PF_DRY_RUN", "0") == "1"
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE", "")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = "2025-10"


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if getattr(g, "user", None):
            return f(*args, **kwargs)
        from flask import Response
        return Response("Unauthorized", 401)
    return decorated


def _gid_num(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


# ─── API ───────────────────────────────────────────────────────────────────────

@bp.route("/api/lookup", methods=["POST"])
@requires_auth
def api_lookup():
    """Look up a scanned barcode in Shopify. Returns matching variants."""
    barcode = ((request.json or {}).get("barcode") or "").strip()
    if not barcode:
        return jsonify({"error": "barcode required"}), 400

    query = """
    query($q: String!) {
      productVariants(first: 10, query: $q) {
        edges { node {
          id title barcode sku
          product { id title status handle }
        } }
      }
    }
    """
    data = shopify_gql(query, {"q": f"barcode:{barcode}"})
    edges = (data.get("data") or {}).get("productVariants", {}).get("edges", []) or []
    matches = []
    for e in edges:
        n = e["node"] or {}
        prod = n.get("product") or {}
        matches.append({
            "variant_id": _gid_num(n.get("id", "")),
            "variant_title": n.get("title"),
            "variant_barcode": n.get("barcode"),
            "variant_sku": n.get("sku"),
            "product_id": _gid_num(prod.get("id", "")),
            "product_title": prod.get("title"),
            "product_status": prod.get("status"),
            "product_handle": prod.get("handle"),
        })
    return jsonify({"barcode": barcode, "matches": matches})


@bp.route("/api/search", methods=["POST"])
@requires_auth
def api_search():
    """Search the local inventory cache by title, group by product."""
    q = ((request.json or {}).get("q") or "").strip()
    if not q:
        return jsonify({"products": []})
    rows = db.query("""
        SELECT shopify_product_id,
               MAX(title)               AS title,
               MAX(status)              AS status,
               COALESCE(SUM(shopify_qty), 0) AS qty,
               COUNT(*)                 AS variant_count
        FROM inventory_product_cache
        WHERE title ILIKE %s
        GROUP BY shopify_product_id
        ORDER BY MAX(title)
        LIMIT 50
    """, (f"%{q}%",))
    return jsonify({"products": [
        {"product_id": str(r["shopify_product_id"]),
         "title": r["title"],
         "status": r["status"],
         "qty": int(r["qty"] or 0),
         "variant_count": int(r["variant_count"] or 0)}
        for r in rows
    ]})


@bp.route("/api/variants", methods=["POST"])
@requires_auth
def api_variants():
    """List all variants for a product with current barcode + option values."""
    product_id = str((request.json or {}).get("product_id", "")).strip()
    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    pid_gid = f"gid://shopify/Product/{product_id}"
    query = """
    query($id: ID!) {
      product(id: $id) {
        id title handle status
        options { name }
        variants(first: 100) {
          edges { node {
            id title barcode sku
            selectedOptions { name value }
          } }
        }
      }
    }
    """
    data = shopify_gql(query, {"id": pid_gid})
    p = (data.get("data") or {}).get("product") or {}
    if not p:
        return jsonify({"error": "not_found"}), 404

    variants = []
    for e in (p.get("variants") or {}).get("edges", []) or []:
        n = e.get("node") or {}
        variants.append({
            "variant_id": _gid_num(n.get("id", "")),
            "variant_title": n.get("title"),
            "variant_barcode": n.get("barcode"),
            "variant_sku": n.get("sku"),
            "options": n.get("selectedOptions") or [],
        })
    return jsonify({
        "product": {
            "product_id": _gid_num(p.get("id", "")),
            "title": p.get("title"),
            "handle": p.get("handle"),
            "status": p.get("status"),
            "option_names": [o.get("name") for o in (p.get("options") or [])],
        },
        "variants": variants,
    })


@bp.route("/api/assign", methods=["POST"])
@requires_auth
def api_assign():
    """Assign a scanned barcode to a specific variant."""
    body = request.json or {}
    variant_id = str(body.get("variant_id", "")).strip()
    barcode = str(body.get("barcode", "")).strip()
    force = bool(body.get("force"))
    if not variant_id or not barcode:
        return jsonify({"error": "variant_id + barcode required"}), 400

    # Sanity check — flag if some OTHER variant already has this barcode.
    duplicates = []
    try:
        dup_data = shopify_gql(
            """
            query($q: String!) {
              productVariants(first: 5, query: $q) {
                edges { node { id title product { id title } } }
              }
            }
            """,
            {"q": f"barcode:{barcode}"},
        )
        for e in (dup_data.get("data") or {}).get("productVariants", {}).get("edges", []) or []:
            n = e.get("node") or {}
            if _gid_num(n.get("id", "")) == variant_id:
                continue
            duplicates.append({
                "variant_id": _gid_num(n.get("id", "")),
                "variant_title": n.get("title"),
                "product_title": (n.get("product") or {}).get("title"),
            })
    except Exception as e:
        logger.warning(f"duplicate-barcode pre-check failed: {e}")

    if duplicates and not force:
        return jsonify({"error": "duplicate", "duplicates": duplicates}), 409

    if DRY_RUN:
        logger.info(f"[DRY_RUN] would set variant {variant_id} barcode = {barcode}")
        return jsonify({"ok": True, "dry_run": True,
                        "variant_id": variant_id, "barcode": barcode})

    # REST PUT (proven pattern in shared/ai_enrichment.py)
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}/variants/{variant_id}.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}
    resp = requests.put(
        url, headers=headers, timeout=30,
        json={"variant": {"id": int(variant_id), "barcode": barcode}},
    )
    if not resp.ok:
        logger.warning(f"variant barcode update failed: {resp.status_code} {resp.text[:300]}")
        return jsonify({"error": "shopify_error", "status": resp.status_code,
                        "detail": resp.text[:500]}), 502

    # Mark the inventory cache as touched so it doesn't re-pull right away.
    try:
        db.execute(
            "UPDATE inventory_cache_meta SET last_tool_push_at = NOW() WHERE id = 1"
        )
    except Exception:
        pass

    return jsonify({"ok": True, "variant_id": variant_id, "barcode": barcode})


# ─── Page ──────────────────────────────────────────────────────────────────────

@bp.route("/", methods=["GET"])
@requires_auth
def page():
    return _PAGE_HTML


_PAGE_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Barcode Bind · Inventory</title>
<style>
:root { --bg:#0c1015; --surface:#151b24; --s2:#1b2230; --border:#2a3346;
  --text:#e9eef7; --dim:#8b94a8; --green:#2dd4a0; --red:#f05252;
  --amber:#f5a623; --accent:#dfa260; --accent2:#6ba6d9; }
* { box-sizing: border-box; }
body { background: var(--bg); color: var(--text); font-family: -apple-system, system-ui, sans-serif;
       font-size: 14px; margin: 0; padding: 18px; }
h1 { font-size: 18px; margin: 0 0 4px; }
.sub { color: var(--dim); font-size: 12px; margin-bottom: 16px; }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
        padding: 14px; margin-bottom: 12px; }
input, select, button { font-family: inherit; font-size: 14px; }
input[type=text] { background: var(--surface); color: var(--text);
       border: 1px solid var(--border); border-radius: 6px; padding: 9px 12px; width: 100%; }
input[type=text]:focus { outline: none; border-color: var(--accent); }
.scan-input { font-size: 22px !important; padding: 14px 16px !important; letter-spacing: 1px;
       border: 2px solid var(--accent) !important; }
.btn { background: var(--surface); color: var(--text); border: 1px solid var(--border);
       border-radius: 6px; padding: 7px 14px; cursor: pointer; }
.btn:hover { background: var(--s2); }
.btn-primary { background: var(--accent); color: #000; border-color: var(--accent); font-weight: 600; }
.row { display: flex; gap: 8px; align-items: center; }
.muted { color: var(--dim); font-size: 12px; }
.success-badge { display: inline-block; background: rgba(45,212,160,0.16); color: var(--green);
       padding: 4px 10px; border-radius: 4px; font-weight: 600; font-size: 12px; }
.warn-badge { display: inline-block; background: rgba(245,166,35,0.16); color: var(--amber);
       padding: 4px 10px; border-radius: 4px; font-weight: 600; font-size: 12px; }
.miss-badge { display: inline-block; background: rgba(240,82,82,0.16); color: var(--red);
       padding: 4px 10px; border-radius: 4px; font-weight: 600; font-size: 12px; }
table { width: 100%; border-collapse: collapse; margin-top: 8px; }
th, td { padding: 8px 10px; border-bottom: 1px solid var(--border); text-align: left; font-size: 13px; }
th { color: var(--dim); font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; }
tbody tr:hover { background: var(--s2); }
.product-row { cursor: pointer; }
.nav { display: flex; gap: 14px; margin-bottom: 14px; font-size: 12px; }
.nav a { color: var(--dim); text-decoration: none; }
.nav a:hover { color: var(--accent); }
.toast { position: fixed; top: 20px; right: 20px; padding: 10px 16px;
       border-radius: 6px; z-index: 2000; font-weight: 600; }
.toast.green { background: var(--green); color: #000; }
.toast.red { background: var(--red); color: #fff; }
.toast.amber { background: var(--amber); color: #000; }
.code { font-family: ui-monospace, Menlo, Consolas, monospace; }
.history { font-size: 12px; max-height: 220px; overflow: auto; }
.history-row { display: flex; justify-content: space-between; padding: 4px 0;
       border-bottom: 1px solid var(--border); }
.history-row:last-child { border-bottom: none; }
.kb { background: var(--s2); border: 1px solid var(--border); padding: 1px 6px;
       border-radius: 4px; font-size: 11px; }
</style></head>
<body>
<div class="nav">
  <a href="/inventory">← Inventory</a>
  <a href="/inventory/barcode-bind">Barcode Bind</a>
</div>

<h1>Barcode Bind</h1>
<div class="sub">Scan a sealed product. If it doesn't ring up, search the store and assign the scanned barcode to the right variant.</div>

<div class="card">
  <label class="muted">Scan barcode (focus is auto-set; press <span class="kb">Enter</span> to look up)</label>
  <input id="scan" type="text" class="scan-input" autofocus autocomplete="off"
         placeholder="Scan or type UPC/GTIN…" onkeydown="if(event.key==='Enter') lookupScan()">
  <div class="row" style="margin-top:8px;">
    <button class="btn" onclick="lookupScan()">Look up</button>
    <button class="btn" onclick="resetAll()">Clear</button>
    <div style="flex:1"></div>
    <span id="status" class="muted">Ready.</span>
  </div>
</div>

<div id="result-card" class="card" style="display:none;">
  <div id="result-body"></div>
</div>

<div id="search-card" class="card" style="display:none;">
  <label class="muted">Search store by product title</label>
  <input id="search" type="text" placeholder="e.g. Dragon Shield Matte"
         onkeydown="if(event.key==='Enter') runSearch()" autocomplete="off">
  <div class="row" style="margin-top:8px;">
    <button class="btn btn-primary" onclick="runSearch()">Search</button>
    <span id="search-status" class="muted"></span>
  </div>
  <div id="search-results"></div>
</div>

<div id="variants-card" class="card" style="display:none;">
  <div id="variants-body"></div>
</div>

<div class="card">
  <div style="font-weight:600; margin-bottom: 6px;">Recent</div>
  <div id="history" class="history"><div class="muted">No scans yet.</div></div>
</div>

<div id="toast-mount"></div>

<script>
let pendingBarcode = null;
const history = [];

function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":"&#39;"}[c]));
}

function toast(msg, kind) {
  kind = kind || 'green';
  const m = document.getElementById('toast-mount');
  const div = document.createElement('div');
  div.className = 'toast ' + kind;
  div.textContent = msg;
  m.appendChild(div);
  setTimeout(() => div.remove(), 2400);
}

function setStatus(s) { document.getElementById('status').textContent = s; }

function focusScan() {
  const el = document.getElementById('scan');
  el.focus();
  el.select();
}

function pushHistory(barcode, label, kind) {
  history.unshift({ barcode, label, kind, t: new Date() });
  if (history.length > 12) history.pop();
  renderHistory();
}

function renderHistory() {
  const h = document.getElementById('history');
  if (!history.length) { h.innerHTML = '<div class="muted">No scans yet.</div>'; return; }
  h.innerHTML = history.map(x => {
    const cls = x.kind === 'hit'    ? 'success-badge'
              : x.kind === 'assign' ? 'success-badge'
              : x.kind === 'miss'   ? 'miss-badge'
              :                       'warn-badge';
    return '<div class="history-row">'
      + '<span><span class="' + cls + '">' + esc(x.kind) + '</span> '
      + '<span class="code">' + esc(x.barcode) + '</span> — ' + esc(x.label) + '</span>'
      + '<span class="muted">' + x.t.toLocaleTimeString() + '</span>'
      + '</div>';
  }).join('');
}

async function lookupScan() {
  const barcode = document.getElementById('scan').value.trim();
  if (!barcode) return;
  setStatus('Looking up…');
  document.getElementById('result-card').style.display = 'none';
  document.getElementById('search-card').style.display = 'none';
  document.getElementById('variants-card').style.display = 'none';

  let data;
  try {
    const r = await fetch('/inventory/barcode-bind/api/lookup', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ barcode }),
    });
    data = await r.json();
  } catch (e) {
    setStatus('Network error.');
    toast('Network error', 'red');
    return;
  }

  const card = document.getElementById('result-card');
  const body = document.getElementById('result-body');
  card.style.display = '';
  if (data.matches && data.matches.length > 0) {
    pendingBarcode = null;
    const m = data.matches[0];
    body.innerHTML =
      '<div><span class="success-badge">FOUND</span> &nbsp;'
      + 'Barcode <span class="code">' + esc(barcode) + '</span> rings up.</div>'
      + '<div style="margin-top:8px; font-size:15px; font-weight:600;">' + esc(m.product_title || '') + '</div>'
      + '<div class="muted">' + esc(m.variant_title || '')
      + (m.variant_sku ? ' · SKU ' + esc(m.variant_sku) : '') + '</div>'
      + (data.matches.length > 1
          ? '<div class="warn-badge" style="margin-top:8px;">⚠ ' + data.matches.length
            + ' variants share this barcode</div>' : '');
    pushHistory(barcode, m.product_title || '', 'hit');
    setStatus('Found — ready for next scan.');
    toast('Rings up: ' + (m.product_title || ''), 'green');
    document.getElementById('scan').value = '';
    focusScan();
  } else {
    pendingBarcode = barcode;
    body.innerHTML =
      '<div><span class="miss-badge">NOT FOUND</span> &nbsp;'
      + 'Barcode <span class="code">' + esc(barcode) + '</span> isn\\'t in the store.</div>'
      + '<div class="muted" style="margin-top:6px;">Search and pick the variant to assign this barcode to.</div>';
    document.getElementById('search-card').style.display = '';
    document.getElementById('search').focus();
    pushHistory(barcode, '(not found)', 'miss');
    setStatus('Awaiting product pick…');
  }
}

async function runSearch() {
  const q = document.getElementById('search').value.trim();
  if (!q) return;
  document.getElementById('search-status').textContent = 'Searching…';
  document.getElementById('variants-card').style.display = 'none';

  let data;
  try {
    const r = await fetch('/inventory/barcode-bind/api/search', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ q }),
    });
    data = await r.json();
  } catch (e) {
    document.getElementById('search-status').textContent = 'Error.';
    return;
  }

  const out = document.getElementById('search-results');
  document.getElementById('search-status').textContent = data.products.length + ' product(s)';
  if (!data.products.length) {
    out.innerHTML = '<div class="muted" style="margin-top:8px;">No matches.</div>';
    return;
  }
  out.innerHTML =
    '<table><thead><tr><th>Title</th><th>Status</th><th>Qty</th><th>Variants</th></tr></thead><tbody>'
    + data.products.map(p =>
        '<tr class="product-row" onclick="loadVariants(\\'' + esc(p.product_id) + '\\')">'
        + '<td>' + esc(p.title || '') + '</td>'
        + '<td class="muted">' + esc(p.status || '') + '</td>'
        + '<td class="muted">' + p.qty + '</td>'
        + '<td class="muted">' + p.variant_count + '</td>'
        + '</tr>'
      ).join('')
    + '</tbody></table>';
}

async function loadVariants(productId) {
  document.getElementById('variants-card').style.display = '';
  document.getElementById('variants-body').innerHTML = '<div class="muted">Loading variants…</div>';

  let data;
  try {
    const r = await fetch('/inventory/barcode-bind/api/variants', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ product_id: productId }),
    });
    data = await r.json();
  } catch (e) {
    document.getElementById('variants-body').innerHTML = '<div class="muted">Error loading variants.</div>';
    return;
  }
  if (!data.variants) {
    document.getElementById('variants-body').innerHTML = '<div class="muted">Product not found.</div>';
    return;
  }

  const optionNames = data.product.option_names || [];
  const optionCols = optionNames.length
    ? optionNames.map(n => '<th>' + esc(n) + '</th>').join('')
    : '<th>Variant</th>';

  const rows = data.variants.map(v => {
    const optCells = optionNames.length
      ? optionNames.map(n => {
          const opt = (v.options || []).find(o => o.name === n);
          return '<td>' + esc(opt ? opt.value : '') + '</td>';
        }).join('')
      : '<td>' + esc(v.variant_title || '') + '</td>';
    const cur = v.variant_barcode
      ? '<span class="code">' + esc(v.variant_barcode) + '</span>'
      : '<span class="muted">—</span>';
    const labelTitle = v.variant_title || (v.options || []).map(o => o.value).join(' / ') || 'variant';
    const btnLabel = pendingBarcode ? 'Assign ' + esc(pendingBarcode) : 'Assign';
    return '<tr>'
      + optCells
      + '<td>' + esc(v.variant_sku || '') + '</td>'
      + '<td>' + cur + '</td>'
      + '<td><button class="btn btn-primary" onclick="assignBarcode(\\'' + esc(v.variant_id)
        + '\\', \\'' + esc(labelTitle) + '\\')">' + btnLabel + '</button></td>'
      + '</tr>';
  }).join('');

  document.getElementById('variants-body').innerHTML =
    '<div style="font-weight:600; margin-bottom:6px;">' + esc(data.product.title || '') + '</div>'
    + '<div class="muted" style="margin-bottom:8px;">Pick the variant for this physical item.</div>'
    + '<table><thead><tr>' + optionCols + '<th>SKU</th><th>Current barcode</th><th></th></tr></thead>'
    + '<tbody>' + rows + '</tbody></table>';
}

async function assignBarcode(variantId, variantTitle, force) {
  if (!pendingBarcode) { toast('No barcode pending. Re-scan.', 'amber'); return; }

  const body = { variant_id: variantId, barcode: pendingBarcode };
  if (force) body.force = true;

  let data, status;
  try {
    const r = await fetch('/inventory/barcode-bind/api/assign', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    status = r.status;
    data = await r.json();
  } catch (e) {
    toast('Network error', 'red');
    return;
  }

  if (status === 409 && data.error === 'duplicate') {
    const list = (data.duplicates || [])
      .map(d => (d.product_title || '?') + ' · ' + (d.variant_title || '')).join('\\n');
    if (confirm('Barcode ' + pendingBarcode + ' is already on:\\n\\n' + list
                + '\\n\\nAssign anyway?')) {
      return assignBarcode(variantId, variantTitle, true);
    }
    return;
  }
  if (!data.ok) {
    toast('Assign failed: ' + (data.error || status), 'red');
    return;
  }

  toast('Assigned ' + pendingBarcode + ' → ' + variantTitle, 'green');
  pushHistory(pendingBarcode, variantTitle, 'assign');
  pendingBarcode = null;
  resetAll();
}

function resetAll() {
  document.getElementById('scan').value = '';
  document.getElementById('search').value = '';
  document.getElementById('search-results').innerHTML = '';
  document.getElementById('search-status').textContent = '';
  document.getElementById('result-card').style.display = 'none';
  document.getElementById('search-card').style.display = 'none';
  document.getElementById('variants-card').style.display = 'none';
  setStatus('Ready.');
  focusScan();
}

document.body.addEventListener('click', (e) => {
  const tag = e.target.tagName;
  if (tag !== 'INPUT' && tag !== 'BUTTON' && tag !== 'TD' && tag !== 'A' && tag !== 'TR') {
    focusScan();
  }
});
</script>
</body></html>
"""
