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
          inventoryQuantity
          inventoryItem { id }
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
        inv_item = n.get("inventoryItem") or {}
        matches.append({
            "variant_id": _gid_num(n.get("id", "")),
            "variant_title": n.get("title"),
            "variant_barcode": n.get("barcode"),
            "variant_sku": n.get("sku"),
            "inventory_quantity": n.get("inventoryQuantity"),
            "inventory_item_id": _gid_num(inv_item.get("id", "")) if inv_item.get("id") else None,
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

    # Clear the barcode from any OTHER variants that had it.
    cleared = []
    for dup in duplicates:
        dup_id = dup["variant_id"]
        try:
            dup_url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}/variants/{dup_id}.json"
            dup_resp = requests.put(
                dup_url, headers=headers, timeout=30,
                json={"variant": {"id": int(dup_id), "barcode": ""}},
            )
            if dup_resp.ok:
                cleared.append(dup_id)
                logger.info(f"Cleared barcode {barcode} from variant {dup_id}")
            else:
                logger.warning(f"Failed to clear barcode from variant {dup_id}: {dup_resp.status_code}")
        except Exception as e:
            logger.warning(f"Failed to clear barcode from variant {dup_id}: {e}")

    # Mark the inventory cache as touched so it doesn't re-pull right away.
    try:
        db.execute(
            "UPDATE inventory_cache_meta SET last_tool_push_at = NOW() WHERE id = 1"
        )
    except Exception:
        pass

    return jsonify({"ok": True, "variant_id": variant_id, "barcode": barcode,
                    "cleared_from": cleared})


# ─── Inventory adjust (aisle-walk physical audit) ──────────────────────────────
#
# Sean's flow: walk the aisle, scan a sealed product, eyeball the qty on the
# shelf vs what Shopify thinks we have, click +/- to reconcile. No reason
# dropdown — when something is off it usually takes a real audit to figure
# out where it went, so we just log who/when/delta and move on. The optional
# note is for the rare "this is suspicious" moment.

_LOCATION_ID_CACHE: list[str] = []


def _get_primary_location_id() -> str | None:
    """Cache the first Shopify location's gid for the lifetime of the worker."""
    if _LOCATION_ID_CACHE:
        return _LOCATION_ID_CACHE[0]
    try:
        data = shopify_gql("{ locations(first: 1) { edges { node { id } } } }")
        edges = (data.get("data") or {}).get("locations", {}).get("edges", []) or []
        if not edges:
            return None
        loc_id = edges[0]["node"]["id"]
        _LOCATION_ID_CACHE.append(loc_id)
        return loc_id
    except Exception as e:
        logger.warning(f"location lookup failed: {e}")
        return None


@bp.route("/api/adjust", methods=["POST"])
@requires_auth
def api_adjust():
    """Adjust on-hand inventory by a delta (positive or negative). Calls
    Shopify's inventoryAdjustQuantities and writes an audit row. Sealed-only
    — raw cards are tracked one-per-barcode and don't go through this.
    """
    body = request.json or {}
    variant_id = str(body.get("variant_id", "")).strip()
    inventory_item_id = str(body.get("inventory_item_id", "")).strip()
    try:
        delta = int(body.get("delta", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "delta must be an integer"}), 400
    note = (body.get("note") or "").strip() or None
    qty_before = body.get("qty_before")
    if qty_before is not None:
        try:
            qty_before = int(qty_before)
        except (TypeError, ValueError):
            qty_before = None
    product_title = (body.get("product_title") or "").strip() or None
    variant_title = (body.get("variant_title") or "").strip() or None

    if not variant_id or not inventory_item_id:
        return jsonify({"error": "variant_id + inventory_item_id required"}), 400
    if delta == 0:
        return jsonify({"error": "delta cannot be zero"}), 400

    location_id = _get_primary_location_id()
    if not location_id:
        return jsonify({"error": "no_location"}), 502

    if DRY_RUN:
        logger.info(f"[DRY_RUN] would adjust variant {variant_id} by {delta}")
        return jsonify({"ok": True, "dry_run": True, "delta": delta,
                        "qty_after": (qty_before + delta) if qty_before is not None else None})

    mutation = """
    mutation inventoryAdjustQuantities($input: InventoryAdjustQuantitiesInput!) {
      inventoryAdjustQuantities(input: $input) {
        inventoryAdjustmentGroup { reason changes { delta quantityAfterChange } }
        userErrors { field message }
      }
    }
    """
    inv_item_gid = (
        inventory_item_id
        if inventory_item_id.startswith("gid://")
        else f"gid://shopify/InventoryItem/{inventory_item_id}"
    )
    data = shopify_gql(mutation, {
        "input": {
            "reason": "correction",
            "name": "available",
            "changes": [{
                "delta": delta,
                "inventoryItemId": inv_item_gid,
                "locationId": location_id,
            }],
        },
    })
    payload = (data.get("data") or {}).get("inventoryAdjustQuantities") or {}
    user_errs = payload.get("userErrors") or []
    if user_errs:
        logger.warning(f"inventoryAdjustQuantities user errors: {user_errs}")
        return jsonify({"error": "shopify_error", "user_errors": user_errs}), 502

    qty_after = None
    changes = (payload.get("inventoryAdjustmentGroup") or {}).get("changes") or []
    if changes:
        qty_after = changes[0].get("quantityAfterChange")
    if qty_after is None and qty_before is not None:
        qty_after = qty_before + delta

    user = getattr(g, "user", None) or {}
    try:
        db.execute("""
            INSERT INTO inventory_adjustments (
                variant_id, product_id, inventory_item_id,
                product_title, variant_title,
                user_id, user_name,
                delta, qty_before, qty_after, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(variant_id),
            int(body.get("product_id")) if body.get("product_id") else None,
            int(_gid_num(inv_item_gid)) if _gid_num(inv_item_gid).isdigit() else None,
            product_title, variant_title,
            user.get("sub") or user.get("user_id"),
            user.get("name"),
            delta, qty_before, qty_after, note,
        ))
    except Exception as e:
        # Don't fail the user-visible action if audit logging hiccups —
        # Shopify is already adjusted. Log it and move on.
        logger.warning(f"inventory_adjustments insert failed: {e}")

    try:
        db.execute("UPDATE inventory_cache_meta SET last_tool_push_at = NOW() WHERE id = 1")
    except Exception:
        pass

    return jsonify({"ok": True, "delta": delta, "qty_after": qty_after})


@bp.route("/api/adjustments/<variant_id>", methods=["GET"])
@requires_auth
def api_adjustments(variant_id):
    """Recent adjustments for a single variant — surfaces in the UI under
    the qty controls so staff can see what's been touched today."""
    try:
        vid = int(variant_id)
    except (TypeError, ValueError):
        return jsonify({"error": "bad variant_id"}), 400
    rows = db.query("""
        SELECT delta, qty_before, qty_after, note, user_name, created_at
          FROM inventory_adjustments
         WHERE variant_id = %s
         ORDER BY created_at DESC
         LIMIT 20
    """, (vid,))
    out = []
    for r in rows:
        out.append({
            "delta": int(r["delta"]),
            "qty_before": r.get("qty_before"),
            "qty_after": r.get("qty_after"),
            "note": r.get("note"),
            "user_name": r.get("user_name"),
            "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        })
    return jsonify({"adjustments": out})


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
<div class="sub">Scan a sealed product. Hits show on-hand qty with ± buttons for aisle-walk audits. Misses fall through to search-and-assign so unbound UPCs get attached to the right variant.</div>

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
let currentVariant = null;  // most recent successful lookup match
let currentMatches = [];    // all variants returned for the last barcode scan
let lastScannedBarcode = null;
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

// ─── Result render + qty adjust ──────────────────────────────────────────────

function renderResult(m, barcode, allMatches) {
  const body = document.getElementById('result-body');
  const qty = (m.inventory_quantity == null) ? null : Number(m.inventory_quantity);
  const qtyDisplay = (qty == null) ? '—' : qty;
  const canAdjust = !!m.inventory_item_id;

  let matchesHtml = '';
  if (allMatches.length > 1) {
    matchesHtml =
      '<div style="margin-top:12px; padding-top:12px; border-top:1px solid var(--border);">'
      + '<div class="warn-badge" style="margin-bottom:8px;">⚠ ' + allMatches.length + ' variants share this barcode</div>'
      + '<table><thead><tr><th>Product</th><th>Variant</th><th>SKU</th><th>Qty</th><th></th></tr></thead><tbody>'
      + allMatches.map((v, i) => {
          const isActive = v.variant_id === m.variant_id;
          return '<tr' + (isActive ? ' style="background:var(--s2);"' : '') + '>'
            + '<td>' + esc(v.product_title || '') + '</td>'
            + '<td>' + esc(v.variant_title || '') + '</td>'
            + '<td class="code">' + esc(v.variant_sku || '—') + '</td>'
            + '<td>' + (v.inventory_quantity == null ? '—' : v.inventory_quantity) + '</td>'
            + '<td>' + (isActive
                ? '<span class="success-badge">Selected</span>'
                : '<button class="btn" onclick="selectMatch(' + i + ')">Use this</button>')
            + '</td></tr>';
        }).join('')
      + '</tbody></table></div>';
  }

  body.innerHTML =
    '<div><span class="success-badge">FOUND</span> &nbsp;'
    + 'Barcode <span class="code">' + esc(barcode) + '</span> rings up.</div>'
    + '<div style="margin-top:8px; font-size:15px; font-weight:600;">' + esc(m.product_title || '') + '</div>'
    + '<div class="muted">' + esc(m.variant_title || '')
    + (m.variant_sku ? ' · SKU ' + esc(m.variant_sku) : '') + '</div>'
    + '<div style="margin-top:14px; padding-top:14px; border-top:1px solid var(--border); display:flex; align-items:center; gap:14px; flex-wrap:wrap;">'
    +   '<div>'
    +     '<div class="muted" style="font-size:11px; text-transform:uppercase; letter-spacing:0.05em;">On hand</div>'
    +     '<div id="qty-display" style="font-size:32px; font-weight:700; font-family:ui-monospace,Menlo,Consolas,monospace; line-height:1;">'
    +       qtyDisplay + '</div>'
    +   '</div>'
    +   (canAdjust
        ? '<div class="row" style="gap:6px;">'
          + '<button class="btn" onclick="adjustQty(-5)">−5</button>'
          + '<button class="btn" onclick="adjustQty(-1)">−1</button>'
          + '<button class="btn btn-primary" onclick="adjustQty(1)">+1</button>'
          + '<button class="btn" onclick="adjustQty(5)">+5</button>'
          + '</div>'
        : '<div class="muted" style="font-size:12px;">No inventory item — adjust unavailable.</div>')
    + '</div>'
    + (canAdjust
        ? '<div style="margin-top:10px;">'
          + '<input id="adjust-note" type="text" placeholder="Note (optional, only if something looks off)" autocomplete="off" style="font-size:13px;">'
          + '</div>'
          + '<div id="adjust-history" class="history" style="margin-top:10px;"></div>'
        : '')
    + matchesHtml
    + '<div style="margin-top:14px; padding-top:14px; border-top:1px solid var(--border);">'
    + '<button class="btn" style="color:var(--amber); border-color:var(--amber);" onclick="reassignBarcode()">Wrong product? Reassign this barcode</button>'
    + '</div>';
}

async function adjustQty(delta) {
  if (!currentVariant) return;
  const m = currentVariant;
  const note = (document.getElementById('adjust-note') || {}).value || '';
  const qtyEl = document.getElementById('qty-display');
  const before = qtyEl ? Number(qtyEl.textContent) : null;

  setStatus(delta > 0 ? 'Adjusting +' + delta + '…' : 'Adjusting ' + delta + '…');
  let data;
  try {
    const r = await fetch('/inventory/barcode-bind/api/adjust', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        variant_id:        m.variant_id,
        product_id:        m.product_id,
        inventory_item_id: m.inventory_item_id,
        product_title:     m.product_title,
        variant_title:     m.variant_title,
        delta:             delta,
        qty_before:        Number.isFinite(before) ? before : null,
        note:              note,
      }),
    });
    data = await r.json();
  } catch (e) {
    toast('Network error', 'red');
    setStatus('Network error.');
    return;
  }

  if (!data.ok) {
    toast(data.error || 'Adjust failed', 'red');
    setStatus('Adjust failed.');
    return;
  }

  const after = (data.qty_after != null) ? Number(data.qty_after)
              : (Number.isFinite(before) ? before + delta : null);
  if (qtyEl && after != null) qtyEl.textContent = after;
  // Update the cached match so the next adjust on the same variant uses fresh qty.
  if (currentVariant) currentVariant.inventory_quantity = after;
  toast((delta > 0 ? '+' : '') + delta + ' → ' + (after == null ? '?' : after), 'green');
  setStatus('Adjusted. Scan next or adjust again.');

  // Clear the optional note so it doesn't accidentally reuse on the next press.
  const noteEl = document.getElementById('adjust-note');
  if (noteEl) noteEl.value = '';
  loadAdjustHistory(m.variant_id);
}

async function loadAdjustHistory(variant_id) {
  const el = document.getElementById('adjust-history');
  if (!el) return;
  try {
    const r = await fetch('/inventory/barcode-bind/api/adjustments/' + encodeURIComponent(variant_id));
    const data = await r.json();
    const rows = data.adjustments || [];
    if (!rows.length) { el.innerHTML = ''; return; }
    el.innerHTML =
      '<div class="muted" style="font-size:11px; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">Recent adjustments (this variant)</div>'
      + rows.slice(0, 6).map(r => {
          const sign = r.delta > 0 ? '+' : '';
          const cls = r.delta > 0 ? 'success-badge' : 'miss-badge';
          const when = new Date(r.created_at).toLocaleString();
          const noteHtml = r.note ? ' · <span class="muted">' + esc(r.note) + '</span>' : '';
          return '<div class="history-row">'
            + '<span><span class="' + cls + '">' + sign + r.delta + '</span> '
            + esc(r.user_name || '') + noteHtml + '</span>'
            + '<span class="muted">' + esc(when) + '</span>'
            + '</div>';
        }).join('');
  } catch (e) { /* silent — history is bonus */ }
}

function selectMatch(idx) {
  if (!currentMatches[idx] || !lastScannedBarcode) return;
  const m = currentMatches[idx];
  currentVariant = m;
  renderResult(m, lastScannedBarcode, currentMatches);
  loadAdjustHistory(m.variant_id);
  toast('Switched to: ' + (m.product_title || ''), 'green');
}

function reassignBarcode() {
  if (!lastScannedBarcode) { toast('No barcode to reassign. Re-scan.', 'amber'); return; }
  pendingBarcode = lastScannedBarcode;
  document.getElementById('search-card').style.display = '';
  document.getElementById('search').focus();
  setStatus('Reassigning barcode ' + lastScannedBarcode + ' — search for the correct product.');
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
    lastScannedBarcode = barcode;
    currentMatches = data.matches;
    const m = data.matches[0];
    currentVariant = m;  // for adjust handlers
    renderResult(m, barcode, data.matches);
    pushHistory(barcode, m.product_title || '', 'hit');
    setStatus('Found — adjust qty or scan next.');
    toast('Rings up: ' + (m.product_title || ''), 'green');
    document.getElementById('scan').value = '';
    focusScan();
    loadAdjustHistory(m.variant_id);
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
                + '\\n\\nAssign anyway? (barcode will be removed from the above)')) {
      return assignBarcode(variantId, variantTitle, true);
    }
    return;
  }
  if (!data.ok) {
    toast('Assign failed: ' + (data.error || status), 'red');
    return;
  }

  const cleared = (data.cleared_from || []).length;
  const msg = 'Assigned ' + pendingBarcode + ' → ' + variantTitle
    + (cleared ? ' (removed from ' + cleared + ' other variant' + (cleared > 1 ? 's' : '') + ')' : '');
  toast(msg, 'green');
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
  currentMatches = [];
  lastScannedBarcode = null;
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
