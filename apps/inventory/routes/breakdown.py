"""
routes/breakdown.py

Inventory Breakdown page:
- Recommendations: items in store that have a saved breakdown recipe with
  neutral-to-positive value delta, sorted by child low-stock signal
- Recipe editor: create/edit recipes without executing (proxies to ingest API)
- Execute breakdown: decrement parent qty, increment children qtys in Shopify
- Ignore list: suppress specific SKUs from recommendations
"""

import os
import logging
from functools import wraps

import db
from flask import Blueprint, request, jsonify, Response
from routes.inventory import requires_auth, _get_shopify_client, _get_cache_manager, LOCATION_ID, DRY_RUN

logger = logging.getLogger(__name__)

bp = Blueprint("breakdown", __name__, url_prefix="/inventory/breakdown")

INGEST_URL = os.getenv("INGEST_INTERNAL_URL", "").rstrip("/")


# ─── Proxy helpers to ingest breakdown-cache API ──────────────────────────────

def _ingest_get(path):
    import requests as req
    if not INGEST_URL:
        return None, "INGEST_INTERNAL_URL not configured"
    try:
        r = req.get(f"{INGEST_URL}{path}", timeout=10)
        return r.json(), None
    except Exception as e:
        return None, str(e)

def _ingest_post(path, body):
    import requests as req
    if not INGEST_URL:
        return None, "INGEST_INTERNAL_URL not configured"
    try:
        r = req.post(f"{INGEST_URL}{path}", json=body, timeout=10)
        return r.json(), None
    except Exception as e:
        return None, str(e)

def _ingest_delete(path):
    import requests as req
    if not INGEST_URL:
        return None, "INGEST_INTERNAL_URL not configured"
    try:
        r = req.delete(f"{INGEST_URL}{path}", timeout=10)
        return r.json(), None
    except Exception as e:
        return None, str(e)


# ─── Recommendation engine ────────────────────────────────────────────────────

def _build_recommendations():
    """
    Join inventory_product_cache with sealed_breakdown_cache to find items
    that have a saved recipe. Enrich with:
      - parent store price / qty
      - best breakdown variant value
      - children store qty (low-stock signal)
      - ignore flag

    Returns list of dicts sorted by desirability score.
    """
    # All non-ignored inventory items that have a TCGPlayer ID and qty > 0
    inventory = db.query("""
        SELECT
            c.shopify_product_id,
            c.shopify_variant_id,
            c.title,
            c.shopify_price,
            c.shopify_qty,
            c.inventory_item_id,
            c.tcgplayer_id,
            c.status
        FROM inventory_product_cache c
        WHERE c.tcgplayer_id IS NOT NULL
          AND c.shopify_qty > 0
          AND c.is_damaged = FALSE
          AND c.tcgplayer_id NOT IN (SELECT tcgplayer_id FROM breakdown_ignore)
        ORDER BY c.title
    """)

    if not inventory:
        return []

    tcg_ids = [r["tcgplayer_id"] for r in inventory]
    if not tcg_ids:
        return []

    ph = ",".join(["%s"] * len(tcg_ids))

    # Breakdown recipes
    recipes = db.query(f"""
        SELECT sbc.tcgplayer_id, sbc.product_name AS recipe_name,
               sbc.best_variant_market, sbc.variant_count, sbc.use_count,
               sbv.id AS best_variant_id, sbv.variant_name, sbv.notes AS variant_notes,
               sbv.total_component_market, sbv.component_count
        FROM sealed_breakdown_cache sbc
        JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
          AND sbv.total_component_market = sbc.best_variant_market
        WHERE sbc.tcgplayer_id IN ({ph})
    """, tuple(tcg_ids))

    recipe_map = {}
    for r in recipes:
        tid = int(r["tcgplayer_id"])
        if tid not in recipe_map or float(r["total_component_market"]) > float(recipe_map[tid]["total_component_market"]):
            recipe_map[tid] = dict(r)

    # Component child TCGPlayer IDs for low-stock lookup
    if recipe_map:
        variant_ids = [str(r["best_variant_id"]) for r in recipe_map.values()]
        vph = ",".join(["%s"] * len(variant_ids))
        components = db.query(f"""
            SELECT sbc2.tcgplayer_id AS component_tcg_id,
                   sbc.tcgplayer_id AS parent_tcg_id,
                   sbv.id AS variant_id
            FROM sealed_breakdown_components sbcomp
            JOIN sealed_breakdown_variants sbv ON sbv.id = sbcomp.variant_id
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
            LEFT JOIN sealed_breakdown_cache sbc2 ON sbc2.tcgplayer_id = sbcomp.tcgplayer_id
            WHERE sbv.id IN ({vph}) AND sbcomp.tcgplayer_id IS NOT NULL
        """, tuple(variant_ids))

        comp_tcg_ids = list({int(c["component_tcg_id"]) for c in components if c["component_tcg_id"]})
        child_qty_map = {}
        if comp_tcg_ids:
            cph = ",".join(["%s"] * len(comp_tcg_ids))
            child_rows = db.query(f"""
                SELECT tcgplayer_id, shopify_qty, shopify_price, title
                FROM inventory_product_cache
                WHERE tcgplayer_id IN ({cph}) AND is_damaged = FALSE
            """, tuple(comp_tcg_ids))
            for cr in child_rows:
                child_qty_map[int(cr["tcgplayer_id"])] = dict(cr)

        # Map variant_id → list of component tcg_ids
        variant_comp_map = {}
        for c in components:
            vid = str(c["variant_id"])
            if vid not in variant_comp_map:
                variant_comp_map[vid] = []
            if c["component_tcg_id"]:
                variant_comp_map[vid].append(int(c["component_tcg_id"]))
    else:
        child_qty_map = {}
        variant_comp_map = {}

    results = []
    for row in inventory:
        tid = int(row["tcgplayer_id"])
        if tid not in recipe_map:
            continue

        recipe = recipe_map[tid]
        store_price = float(row["shopify_price"] or 0)
        store_qty   = int(row["shopify_qty"] or 0)
        bd_value    = float(recipe["total_component_market"] or 0)

        if store_price <= 0 or bd_value <= 0:
            continue

        delta_pct = (bd_value - store_price) / store_price * 100

        # Low-stock signal: avg qty of child components in store
        vid = str(recipe["best_variant_id"])
        child_tcg_ids = variant_comp_map.get(vid, [])
        child_qtys = [child_qty_map[cid]["shopify_qty"] for cid in child_tcg_ids if cid in child_qty_map]
        avg_child_qty = sum(child_qtys) / len(child_qtys) if child_qtys else 999
        min_child_qty = min(child_qtys) if child_qtys else 999
        children_in_store = len([q for q in child_qtys if q > 0])
        total_children   = len(child_tcg_ids)

        # Score: prefer positive delta + low child stock
        # Low child qty pulls score UP (more desirable to break down)
        low_stock_bonus = max(0, 20 - avg_child_qty) * 0.5
        score = delta_pct + low_stock_bonus

        results.append({
            "shopify_variant_id": row["shopify_variant_id"],
            "shopify_product_id": row["shopify_product_id"],
            "inventory_item_id":  row["inventory_item_id"],
            "tcgplayer_id":       tid,
            "title":              row["title"],
            "status":             row["status"],
            "store_price":        store_price,
            "store_qty":          store_qty,
            "bd_value":           bd_value,
            "delta_pct":          round(delta_pct, 1),
            "best_variant_id":    vid,
            "best_variant_name":  recipe["variant_name"],
            "variant_notes":      recipe["variant_notes"],
            "variant_count":      recipe["variant_count"],
            "avg_child_qty":      round(avg_child_qty, 1),
            "min_child_qty":      min_child_qty,
            "children_in_store":  children_in_store,
            "total_children":     total_children,
            "score":              round(score, 2),
            "use_count":          recipe["use_count"],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


# ─── Routes ───────────────────────────────────────────────────────────────────

@bp.route("/")
@requires_auth
def breakdown_page():
    return Response(_render_breakdown_page(), mimetype="text/html")


@bp.route("/api/recommendations")
@requires_auth
def recommendations():
    try:
        recs = _build_recommendations()
        return jsonify({"recommendations": recs})
    except Exception as e:
        logger.exception("recommendations failed")
        return jsonify({"error": str(e)}), 500


@bp.route("/api/ignore", methods=["POST"])
@requires_auth
def ignore_sku():
    data = request.get_json(silent=True) or {}
    tcg_id = data.get("tcgplayer_id")
    name   = data.get("product_name", "")
    reason = data.get("reason", "")
    if not tcg_id:
        return jsonify({"error": "tcgplayer_id required"}), 400
    db.execute("""
        INSERT INTO breakdown_ignore (tcgplayer_id, product_name, reason)
        VALUES (%s, %s, %s)
        ON CONFLICT (tcgplayer_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            reason = EXCLUDED.reason,
            ignored_at = CURRENT_TIMESTAMP
    """, (tcg_id, name, reason))
    return jsonify({"success": True})


@bp.route("/api/ignore/<int:tcg_id>", methods=["DELETE"])
@requires_auth
def unignore_sku(tcg_id):
    db.execute("DELETE FROM breakdown_ignore WHERE tcgplayer_id = %s", (tcg_id,))
    return jsonify({"success": True})


@bp.route("/api/ignore")
@requires_auth
def list_ignored():
    rows = db.query("SELECT * FROM breakdown_ignore ORDER BY ignored_at DESC")
    return jsonify({"ignored": [dict(r) for r in rows]})


@bp.route("/api/execute", methods=["POST"])
@requires_auth
def execute_breakdown():
    """
    Decrement parent qty by qty_to_break, increment each child by
    (qty_to_break * quantity_per_parent).

    Body: {
        parent_variant_id: int,
        parent_inventory_item_id: int,
        parent_tcgplayer_id: int,
        qty_to_break: int,
        variant_id: str  (sealed_breakdown_variants.id)
    }
    """
    data = request.get_json(silent=True) or {}
    parent_variant_id    = data.get("parent_variant_id")
    parent_inv_item_id   = data.get("parent_inventory_item_id")
    parent_tcg_id        = data.get("parent_tcgplayer_id")
    qty_to_break         = int(data.get("qty_to_break", 1))
    variant_id           = data.get("variant_id")

    if not all([parent_variant_id, parent_inv_item_id, qty_to_break, variant_id]):
        return jsonify({"error": "parent_variant_id, parent_inventory_item_id, qty_to_break, variant_id required"}), 400

    sc = _get_shopify_client()
    if sc is None:
        return jsonify({"error": "Shopify not configured"}), 503

    # Fetch current parent qty
    parent_row = db.query_one(
        "SELECT shopify_qty, title FROM inventory_product_cache WHERE shopify_variant_id = %s",
        (parent_variant_id,)
    )
    if not parent_row:
        return jsonify({"error": "Parent variant not found in cache"}), 404

    current_qty = int(parent_row["shopify_qty"] or 0)
    if qty_to_break > current_qty:
        return jsonify({"error": f"Cannot break down {qty_to_break} — only {current_qty} in stock"}), 400

    # Fetch components
    components = db.query("""
        SELECT sbc.tcgplayer_id, sbc.product_name, sbc.quantity_per_parent,
               ipc.shopify_variant_id AS child_variant_id,
               ipc.inventory_item_id  AS child_inv_item_id,
               ipc.shopify_qty        AS child_current_qty,
               ipc.title              AS child_title
        FROM sealed_breakdown_components sbc
        LEFT JOIN inventory_product_cache ipc
               ON ipc.tcgplayer_id = sbc.tcgplayer_id AND ipc.is_damaged = FALSE
        WHERE sbc.variant_id = %s
    """, (variant_id,))

    if not components:
        return jsonify({"error": "No components found for this variant"}), 404

    results = {"parent": {}, "children": [], "errors": []}

    # Decrement parent
    new_parent_qty = current_qty - qty_to_break
    if DRY_RUN:
        logger.info(f"[DRY_RUN] BD parent variant={parent_variant_id} {current_qty}→{new_parent_qty}")
        results["parent"] = {"title": parent_row["title"], "old_qty": current_qty, "new_qty": new_parent_qty, "dry_run": True}
    else:
        try:
            sc.set_inventory_level(int(parent_inv_item_id), int(LOCATION_ID), new_parent_qty)
            db.execute("UPDATE inventory_product_cache SET shopify_qty=%s WHERE shopify_variant_id=%s",
                       (new_parent_qty, parent_variant_id))
            results["parent"] = {"title": parent_row["title"], "old_qty": current_qty, "new_qty": new_parent_qty}
        except Exception as e:
            return jsonify({"error": f"Failed to decrement parent: {e}"}), 500

    # Increment children
    for comp in components:
        add_qty = int(comp["quantity_per_parent"]) * qty_to_break
        child_vid    = comp["child_variant_id"]
        child_inv_id = comp["child_inv_item_id"]
        child_qty    = int(comp["child_current_qty"] or 0) if comp["child_current_qty"] is not None else None

        if child_vid is None or child_inv_id is None:
            results["errors"].append({
                "component": comp["product_name"],
                "tcgplayer_id": comp["tcgplayer_id"],
                "error": "Not found in inventory cache — SKU may not exist in Shopify yet",
            })
            continue

        new_child_qty = (child_qty or 0) + add_qty
        if DRY_RUN:
            logger.info(f"[DRY_RUN] BD child variant={child_vid} +{add_qty} → {new_child_qty}")
            results["children"].append({
                "title": comp["child_title"], "add_qty": add_qty,
                "old_qty": child_qty, "new_qty": new_child_qty, "dry_run": True,
            })
        else:
            try:
                sc.set_inventory_level(int(child_inv_id), int(LOCATION_ID), new_child_qty)
                db.execute("UPDATE inventory_product_cache SET shopify_qty=%s WHERE shopify_variant_id=%s",
                           (new_child_qty, child_vid))
                results["children"].append({
                    "title": comp["child_title"], "add_qty": add_qty,
                    "old_qty": child_qty, "new_qty": new_child_qty,
                })
            except Exception as e:
                results["errors"].append({"component": comp["child_title"], "error": str(e)})

    if not DRY_RUN:
        _get_cache_manager().record_tool_push()

    return jsonify({"success": True, "results": results})


# ─── Proxy routes to ingest breakdown-cache API ───────────────────────────────

@bp.route("/api/cache/<int:tcg_id>")
@requires_auth
def proxy_get_cache(tcg_id):
    data, err = _ingest_get(f"/api/breakdown-cache/{tcg_id}")
    if err:
        return jsonify({"error": err}), 502
    return jsonify(data)

@bp.route("/api/cache/<int:tcg_id>/variant", methods=["POST"])
@requires_auth
def proxy_save_variant(tcg_id):
    data, err = _ingest_post(f"/api/breakdown-cache/{tcg_id}/variant", request.get_json(silent=True) or {})
    if err:
        return jsonify({"error": err}), 502
    return jsonify(data)

@bp.route("/api/cache/variant/<variant_id>", methods=["DELETE"])
@requires_auth
def proxy_delete_variant(variant_id):
    data, err = _ingest_delete(f"/api/breakdown-cache/variant/{variant_id}")
    if err:
        return jsonify({"error": err}), 502
    return jsonify(data)

@bp.route("/api/cache/search")
@requires_auth
def proxy_ppt_search():
    q = request.args.get("q", "")
    data, err = _ingest_post("/api/ppt/search-sealed", {"query": q})
    if err:
        return jsonify({"error": err}), 502
    return jsonify(data)

@bp.route("/api/store-prices", methods=["POST"])
@requires_auth
def store_prices_local():
    """Look up store prices from local inventory cache (no ingest proxy needed)."""
    body = request.get_json(silent=True) or {}
    tcg_ids = [int(x) for x in body.get("tcgplayer_ids", []) if x]
    if not tcg_ids:
        return jsonify({"prices": {}})
    ph = ",".join(["%s"] * len(tcg_ids))
    rows = db.query(
        f"SELECT tcgplayer_id, shopify_price, shopify_qty, handle, title "
        f"FROM inventory_product_cache WHERE tcgplayer_id IN ({ph}) AND is_damaged = FALSE",
        tuple(tcg_ids)
    )
    prices = {str(r["tcgplayer_id"]): dict(r) for r in rows}
    return jsonify({"prices": prices})


# ─── Page renderer ────────────────────────────────────────────────────────────

def _render_breakdown_page():
    ingest_url = INGEST_URL or ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Breakdown — PackFresh Inventory</title>
<style>
:root {{
  --bg:#0f1117; --surface:#1a1d27; --surface-2:#242736; --border:#2e3147;
  --text:#e2e8f0; --text-dim:#8892a4; --accent:#7c6af7; --green:#22c55e;
  --amber:#f59e0b; --red:#ef4444; --blue:#3b82f6;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;line-height:1.5}}
a{{color:var(--accent);text-decoration:none}}
.container{{max-width:1300px;margin:0 auto;padding:20px}}
.nav{{display:flex;align-items:center;gap:16px;padding:12px 20px;background:var(--surface);border-bottom:1px solid var(--border);margin-bottom:0}}
.nav a{{color:var(--text-dim);font-size:13px}} .nav a:hover{{color:var(--text)}} .nav .active{{color:var(--accent)}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px}}
.tabs{{display:flex;gap:4px;border-bottom:1px solid var(--border);margin-bottom:20px}}
.tab-btn{{background:none;border:none;color:var(--text-dim);padding:8px 16px;cursor:pointer;font-size:14px;border-bottom:2px solid transparent;margin-bottom:-1px}}
.tab-btn.active{{color:var(--accent);border-bottom-color:var(--accent)}}
.tab-pane{{display:none}} .tab-pane.active{{display:block}}
table{{width:100%;border-collapse:collapse}}
th{{text-align:left;padding:8px 10px;font-size:12px;color:var(--text-dim);border-bottom:1px solid var(--border);font-weight:500}}
td{{padding:8px 10px;border-bottom:1px solid var(--border);font-size:13px;vertical-align:top}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:var(--surface-2)}}
.btn{{display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:6px;border:none;cursor:pointer;font-size:13px;font-weight:500}}
.btn-primary{{background:var(--accent);color:#fff}} .btn-primary:hover{{opacity:.9}}
.btn-secondary{{background:var(--surface-2);color:var(--text);border:1px solid var(--border)}} .btn-secondary:hover{{background:var(--border)}}
.btn-success{{background:#16a34a;color:#fff}} .btn-danger{{background:var(--red);color:#fff}}
.btn-sm{{padding:3px 8px;font-size:12px}}
.badge{{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600}}
.badge-green{{background:#14532d;color:#4ade80}} .badge-amber{{background:#451a03;color:#fbbf24}}
.badge-red{{background:#450a0a;color:#f87171}} .badge-blue{{background:#1e3a5f;color:#93c5fd}}
.badge-neutral{{background:var(--surface-2);color:var(--text-dim)}}
.score-bar{{height:4px;border-radius:2px;background:var(--border);width:100%;margin-top:4px}}
.score-fill{{height:4px;border-radius:2px;background:var(--green)}}
input[type=text],input[type=number],textarea,select{{
  background:var(--surface-2);border:1px solid var(--border);color:var(--text);
  border-radius:6px;padding:6px 10px;font-size:13px;width:100%
}}
input:focus,select:focus{{outline:none;border-color:var(--accent)}}
.search-result{{padding:6px 10px;border-bottom:1px solid var(--border);cursor:pointer;border-radius:4px}}
.search-result:hover{{background:var(--surface-2)}}
.loading{{display:flex;align-items:center;gap:8px;color:var(--text-dim);padding:12px}}
.spinner{{width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
.alert{{padding:10px 14px;border-radius:6px;margin:8px 0;font-size:13px}}
.alert-success{{background:#14532d22;border:1px solid #22c55e44;color:var(--green)}}
.alert-error{{background:#450a0a22;border:1px solid #ef444444;color:var(--red)}}
.alert-warning{{background:#451a0322;border:1px solid #f59e0b44;color:var(--amber)}}
.modal-overlay{{position:fixed;inset:0;background:rgba(0,0,0,.7);display:none;align-items:center;justify-content:center;z-index:1000}}
.modal-overlay.active{{display:flex}}
.modal{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;max-width:700px;width:90%;max-height:85vh;overflow-y:auto}}
.modal-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}}
.modal-close{{background:none;border:none;color:var(--text-dim);font-size:20px;cursor:pointer}}
.chip-group{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}}
.chip{{background:var(--surface-2);border:1px solid var(--border);border-radius:16px;padding:3px 10px;font-size:12px;cursor:pointer}}
.chip.active{{background:var(--accent);border-color:var(--accent);color:#fff}}
.form-row{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}}
.form-group{{margin-bottom:12px}}
.form-group label{{display:block;font-size:12px;color:var(--text-dim);margin-bottom:4px}}
.delta-pos{{color:var(--green)}} .delta-neg{{color:var(--red)}} .delta-neutral{{color:var(--amber)}}
.low-stock{{color:var(--red);font-weight:600}}
</style>
</head>
<body>

<nav class="nav">
  <strong style="color:var(--accent)">📦 PackFresh</strong>
  <a href="/inventory/">Inventory</a>
  <a href="/inventory/breakdown/" class="active">Breakdown</a>
</nav>

<div class="container">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
    <h1 style="font-size:1.4rem">🔓 Breakdown Manager</h1>
    <button class="btn btn-secondary" onclick="openRecipeEditor(null, null)">+ New Recipe</button>
  </div>

  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab('recommendations',this)">📊 Recommendations</button>
    <button class="tab-btn" onclick="switchTab('search',this)">🔍 Manual Breakdown</button>
    <button class="tab-btn" onclick="switchTab('ignored',this)">🚫 Ignored SKUs</button>
  </div>

  <!-- ═══ RECOMMENDATIONS TAB ════════════════════════════════════════════ -->
  <div id="tab-recommendations" class="tab-pane active">
    <div class="card" style="padding:10px 16px; margin-bottom:12px">
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <div style="display:flex;gap:6px;align-items:center">
          <label style="font-size:12px;color:var(--text-dim)">Show:</label>
          <select id="rec-filter" style="width:auto" onchange="renderRecommendations()">
            <option value="all">All with recipes</option>
            <option value="positive">Positive delta only</option>
            <option value="neutral">Neutral or better (≥−10%)</option>
          </select>
        </div>
        <div style="display:flex;gap:6px;align-items:center">
          <label style="font-size:12px;color:var(--text-dim)">Sort:</label>
          <select id="rec-sort" style="width:auto" onchange="renderRecommendations()">
            <option value="score">Score (delta + low-stock)</option>
            <option value="delta">Value delta %</option>
            <option value="child_qty">Child stock (low first)</option>
            <option value="store_qty">Parent qty (high first)</option>
          </select>
        </div>
        <button class="btn btn-secondary btn-sm" onclick="loadRecommendations()" style="margin-left:auto">↻ Refresh</button>
      </div>
    </div>
    <div id="rec-panel"><div class="loading"><span class="spinner"></span> Loading recommendations...</div></div>
  </div>

  <!-- ═══ MANUAL SEARCH TAB ══════════════════════════════════════════════ -->
  <div id="tab-search" class="tab-pane">
    <div class="card">
      <h3 style="margin-bottom:12px">Search your store inventory</h3>
      <div style="display:flex;gap:8px;margin-bottom:12px">
        <input type="text" id="inv-search" placeholder="Search by product name…" style="flex:1"
               onkeydown="if(event.key==='Enter') searchInventory()">
        <button class="btn btn-primary" onclick="searchInventory()">Search</button>
      </div>
      <div id="inv-search-results"></div>
    </div>
  </div>

  <!-- ═══ IGNORED SKUs TAB ═══════════════════════════════════════════════ -->
  <div id="tab-ignored" class="tab-pane">
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <h3>Ignored SKUs</h3>
        <span style="font-size:12px;color:var(--text-dim)">These will not appear in recommendations</span>
      </div>
      <div id="ignored-panel"><div class="loading"><span class="spinner"></span></div></div>
    </div>
  </div>
</div>

<!-- ═══ RECIPE EDITOR MODAL ════════════════════════════════════════════════ -->
<div id="recipe-modal" class="modal-overlay">
  <div class="modal">
    <div class="modal-header">
      <h3 id="recipe-modal-title">Breakdown Recipe</h3>
      <button class="modal-close" onclick="closeModal()">✕</button>
    </div>
    <div id="recipe-modal-body"></div>
  </div>
</div>

<!-- ═══ EXECUTE BREAKDOWN MODAL ════════════════════════════════════════════ -->
<div id="execute-modal" class="modal-overlay">
  <div class="modal">
    <div class="modal-header">
      <h3>Execute Breakdown</h3>
      <button class="modal-close" onclick="document.getElementById('execute-modal').classList.remove('active')">✕</button>
    </div>
    <div id="execute-modal-body"></div>
  </div>
</div>

<script>
// ══════════════════════════════════════════════════════════════════
// STATE
// ══════════════════════════════════════════════════════════════════
let _allRecs = [];
let _recipeTarget = null;   // {{tcgId, name, variantId, variantId}}
let _recipeComponents = [];
let _recipeVariantId = null;
let _storePrices = {{}};
let _ingestUrl = "{ingest_url}";

// ══════════════════════════════════════════════════════════════════
// TABS
// ══════════════════════════════════════════════════════════════════
function switchTab(id, btn) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  btn.classList.add('active');
  if (id === 'ignored') loadIgnored();
}}

// ══════════════════════════════════════════════════════════════════
// RECOMMENDATIONS
// ══════════════════════════════════════════════════════════════════
async function loadRecommendations() {{
  const panel = document.getElementById('rec-panel');
  panel.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
  try {{
    const r = await fetch('/inventory/breakdown/api/recommendations');
    const d = await r.json();
    if (!r.ok) {{ panel.innerHTML = `<div class="alert alert-error">${{d.error}}</div>`; return; }}
    _allRecs = d.recommendations || [];
    renderRecommendations();
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

function renderRecommendations() {{
  const panel = document.getElementById('rec-panel');
  const filter = document.getElementById('rec-filter').value;
  const sort   = document.getElementById('rec-sort').value;

  let recs = [..._allRecs];
  if (filter === 'positive') recs = recs.filter(r => r.delta_pct >= 0);
  if (filter === 'neutral')  recs = recs.filter(r => r.delta_pct >= -10);

  if (sort === 'delta')      recs.sort((a,b) => b.delta_pct - a.delta_pct);
  if (sort === 'child_qty')  recs.sort((a,b) => a.min_child_qty - b.min_child_qty);
  if (sort === 'store_qty')  recs.sort((a,b) => b.store_qty - a.store_qty);
  // default: score already sorted server-side

  if (!recs.length) {{
    panel.innerHTML = '<div class="card" style="color:var(--text-dim); text-align:center; padding:32px">' +
      (filter === 'all'
        ? '📦 No items with saved breakdown recipes found.<br><small>Add recipes in the Manual Breakdown tab or via the Ingest app.</small>'
        : '🔍 No items match the current filter.') + '</div>';
    return;
  }}

  panel.innerHTML = `
    <div style="font-size:12px;color:var(--text-dim);margin-bottom:8px">${{recs.length}} item${{recs.length!==1?'s':''}} with recipes</div>
    <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Product</th>
        <th>In Store</th>
        <th>Store Price</th>
        <th>BD Value</th>
        <th>Delta</th>
        <th>Child Stock</th>
        <th style="width:140px"></th>
      </tr></thead>
      <tbody>
      ${{recs.map(r => recRow(r)).join('')}}
      </tbody>
    </table></div>`;
}}

function recRow(r) {{
  const deltaClass = r.delta_pct >= 5 ? 'delta-pos' : r.delta_pct >= -10 ? 'delta-neutral' : 'delta-neg';
  const deltaStr   = (r.delta_pct >= 0 ? '+' : '') + r.delta_pct.toFixed(1) + '%';
  const childStockStr = r.total_children > 0
    ? `<span class="${{r.min_child_qty === 0 ? 'low-stock' : r.min_child_qty < 5 ? 'delta-neutral' : ''}}">
        min ${{r.min_child_qty}} · avg ${{r.avg_child_qty}}</span>
        <br><small style="color:var(--text-dim)">${{r.children_in_store}}/${{r.total_children}} in store</small>`
    : '<span style="color:var(--text-dim)">—</span>';

  const varBadge = r.variant_count > 1
    ? `<span class="badge badge-blue">${{r.variant_count}} configs</span> `
    : '';

  return `<tr>
    <td>
      <strong style="font-size:13px">${{r.title}}</strong><br>
      <small style="color:var(--text-dim)">TCG#${{r.tcgplayer_id}}</small>
      ${{r.variant_notes ? `<br><small style="color:var(--amber)">📝 ${{r.variant_notes}}</small>` : ''}}
    </td>
    <td style="font-weight:600;color:${{r.store_qty > 0 ? 'var(--green)' : 'var(--red)'}}">${{r.store_qty}}</td>
    <td>$${{r.store_price.toFixed(2)}}</td>
    <td>$${{r.bd_value.toFixed(2)}}<br><small style="color:var(--text-dim)">${{r.best_variant_name}}</small></td>
    <td class="${{deltaClass}}" style="font-weight:600">${{deltaStr}}</td>
    <td>${{childStockStr}}</td>
    <td>
      <div style="display:flex;flex-direction:column;gap:4px">
        <button class="btn btn-primary btn-sm" onclick="openExecuteModal(${{JSON.stringify(r).replace(/"/g,'&quot;')}})">
          ▶ Break Down
        </button>
        <div style="display:flex;gap:4px">
          <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{r.tcgplayer_id}}, '${{r.title.replace(/'/g,'').replace(/"/g,'')}}')">
            ✎ Recipe
          </button>
          <button class="btn btn-sm" style="color:var(--text-dim);border:1px solid var(--border);background:none"
            onclick="ignoreSku(${{r.tcgplayer_id}}, '${{r.title.replace(/'/g,'').replace(/"/g,'')}}')">
            🚫
          </button>
        </div>
      </div>
    </td>
  </tr>`;
}}

// ══════════════════════════════════════════════════════════════════
// EXECUTE BREAKDOWN
// ══════════════════════════════════════════════════════════════════
function openExecuteModal(rec) {{
  if (typeof rec === 'string') rec = JSON.parse(rec.replace(/&quot;/g, '"'));
  const body = document.getElementById('execute-modal-body');
  const deltaClass = rec.delta_pct >= 0 ? 'delta-pos' : rec.delta_pct >= -10 ? 'delta-neutral' : 'delta-neg';

  body.innerHTML = `
    <div style="margin-bottom:16px">
      <div style="font-size:15px;font-weight:600;margin-bottom:4px">${{rec.title}}</div>
      <div style="display:flex;gap:16px;font-size:13px;color:var(--text-dim);flex-wrap:wrap">
        <span>Store: <strong style="color:var(--text)">$${{rec.store_price.toFixed(2)}}</strong> × ${{rec.store_qty}} in stock</span>
        <span>BD value: <strong class="${{deltaClass}}">$${{rec.bd_value.toFixed(2)}}</strong> (${{rec.delta_pct >= 0 ? '+' : ''}}${{rec.delta_pct.toFixed(1)}}%)</span>
        <span>Recipe: <strong style="color:var(--text)">${{rec.best_variant_name}}</strong></span>
      </div>
    </div>

    <div style="margin-bottom:16px">
      <label style="font-size:12px;color:var(--text-dim);display:block;margin-bottom:6px">
        How many to break down? (max ${{rec.store_qty}})
      </label>
      <div style="display:flex;gap:8px;align-items:center">
        <input type="number" id="exec-qty" value="1" min="1" max="${{rec.store_qty}}"
               style="width:80px" oninput="updateExecPreview(${{rec.store_qty}}, ${{rec.bd_value}}, ${{rec.store_price}})">
        <div style="display:flex;gap:4px">
          ${{[1,2,5,10].filter(n=>n<=rec.store_qty).map(n=>
            `<button class="btn btn-secondary btn-sm" onclick="document.getElementById('exec-qty').value=${{n}};updateExecPreview(${{rec.store_qty}},${{rec.bd_value}},${{rec.store_price}})">${{n}}</button>`
          ).join('')}}
          <button class="btn btn-secondary btn-sm" onclick="document.getElementById('exec-qty').value=${{rec.store_qty}};updateExecPreview(${{rec.store_qty}},${{rec.bd_value}},${{rec.store_price}})">All</button>
        </div>
      </div>
      <div id="exec-preview" style="margin-top:8px;font-size:12px;color:var(--text-dim)"></div>
    </div>

    <div id="exec-result"></div>

    <div style="display:flex;gap:8px;margin-top:16px">
      <button class="btn btn-primary" id="exec-confirm-btn"
        onclick="confirmExecute(${{rec.shopify_variant_id}},${{rec.inventory_item_id}},${{rec.tcgplayer_id}},'${{rec.best_variant_id}}')">
        ✓ Confirm Breakdown
      </button>
      <button class="btn btn-secondary" onclick="document.getElementById('execute-modal').classList.remove('active')">Cancel</button>
    </div>
  `;
  updateExecPreview(rec.store_qty, rec.bd_value, rec.store_price);
  document.getElementById('execute-modal').classList.add('active');
}}

function updateExecPreview(maxQty, bdValue, storePrice) {{
  const qty = Math.max(1, Math.min(parseInt(document.getElementById('exec-qty')?.value)||1, maxQty));
  const totalBd = (qty * bdValue).toFixed(2);
  const totalStore = (qty * storePrice).toFixed(2);
  const diff = (qty * bdValue - qty * storePrice).toFixed(2);
  const preview = document.getElementById('exec-preview');
  if (preview) preview.innerHTML =
    `Breaking down ${{qty}} unit${{qty!==1?'s':''}}: ` +
    `Store value $${{totalStore}} → BD value <strong style="color:var(--green)">$${{totalBd}}</strong> ` +
    `(<span style="color:${{diff>=0?'var(--green)':'var(--red)'}}">${{diff>=0?'+':''}}$${{diff}}</span>)`;
}}

async function confirmExecute(parentVariantId, parentInvItemId, parentTcgId, variantId) {{
  const qty = parseInt(document.getElementById('exec-qty')?.value) || 1;
  const btn = document.getElementById('exec-confirm-btn');
  const result = document.getElementById('exec-result');
  btn.disabled = true; btn.textContent = '⟳ Executing...';

  try {{
    const r = await fetch('/inventory/breakdown/api/execute', {{
      method: 'POST', headers: {{'Content-Type':'application/json'}},
      body: JSON.stringify({{
        parent_variant_id: parentVariantId,
        parent_inventory_item_id: parentInvItemId,
        parent_tcgplayer_id: parentTcgId,
        qty_to_break: qty,
        variant_id: variantId,
      }})
    }});
    const d = await r.json();
    if (!r.ok) {{ result.innerHTML = `<div class="alert alert-error">${{d.error}}</div>`; btn.disabled=false; btn.textContent='✓ Confirm Breakdown'; return; }}

    const res = d.results;
    let html = `<div class="alert alert-success">
      ✓ Breakdown complete — parent <strong>${{res.parent.title}}</strong>: ${{res.parent.old_qty}} → ${{res.parent.new_qty}}
    </div>`;
    if (res.children.length) {{
      html += '<div style="margin-top:8px;font-size:12px"><strong>Children updated:</strong><ul style="margin-top:4px;padding-left:16px">' +
        res.children.map(c => `<li>${{c.title}}: +${{c.add_qty}} (${{c.old_qty}} → ${{c.new_qty}})</li>`).join('') +
        '</ul></div>';
    }}
    if (res.errors.length) {{
      html += '<div class="alert alert-warning" style="margin-top:8px"><strong>⚠ Some children could not be updated:</strong><ul style="margin-top:4px;padding-left:16px">' +
        res.errors.map(e => `<li>${{e.component}}: ${{e.error}}</li>`).join('') +
        '</ul></div>';
    }}
    result.innerHTML = html;
    btn.textContent = '✓ Done';

    // Update rec in-place
    const idx = _allRecs.findIndex(r => r.shopify_variant_id === parentVariantId);
    if (idx >= 0) {{ _allRecs[idx].store_qty = res.parent.new_qty; renderRecommendations(); }}
  }} catch(e) {{
    result.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`;
    btn.disabled=false; btn.textContent='✓ Confirm Breakdown';
  }}
}}

// ══════════════════════════════════════════════════════════════════
// INVENTORY SEARCH (manual breakdown tab)
// ══════════════════════════════════════════════════════════════════
async function searchInventory() {{
  const q = document.getElementById('inv-search').value.trim().toLowerCase();
  const panel = document.getElementById('inv-search-results');
  if (!q) return;
  panel.innerHTML = '<div class="loading"><span class="spinner"></span> Searching...</div>';

  try {{
    const r = await fetch('/inventory/breakdown/api/recommendations');
    const d = await r.json();
    const all = d.recommendations || [];

    // Also need to search items WITHOUT recipes
    const r2 = await fetch('/inventory/api/all-items');
    const d2 = await r2.json();
    const allItems = d2.items || [];

    const filtered = allItems.filter(i => i.title.toLowerCase().includes(q));
    if (!filtered.length) {{ panel.innerHTML = '<div class="alert alert-warning">No items found.</div>'; return; }}

    // Merge recipe data into results
    const recMap = {{}};
    all.forEach(r => recMap[r.tcgplayer_id] = r);

    panel.innerHTML = `<div style="overflow-x:auto"><table>
      <thead><tr><th>Product</th><th>Qty</th><th>Price</th><th>BD Value</th><th></th></tr></thead>
      <tbody>
      ${{filtered.slice(0,30).map(item => {{
        const rec = item.tcgplayer_id ? recMap[item.tcgplayer_id] : null;
        return `<tr>
          <td><strong>${{item.title}}</strong><br><small style="color:var(--text-dim)">TCG#${{item.tcgplayer_id||'—'}}</small></td>
          <td>${{item.shopify_qty}}</td>
          <td>$${{parseFloat(item.shopify_price||0).toFixed(2)}}</td>
          <td>${{rec ? `<span class="${{rec.delta_pct>=0?'delta-pos':rec.delta_pct>=-10?'delta-neutral':'delta-neg'}}">$${{rec.bd_value.toFixed(2)}} (${{rec.delta_pct>=0?'+':''}}${{rec.delta_pct.toFixed(1)}}%)</span>` : '<span style="color:var(--text-dim)">No recipe</span>'}}</td>
          <td>
            ${{rec ? `<button class="btn btn-primary btn-sm" onclick="openExecuteModal(${{JSON.stringify(rec).replace(/"/g,'&quot;')}})">▶ Break Down</button>` : ''}}
            <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{item.tcgplayer_id||'null'}},'${{item.title.replace(/'/g,'').replace(/"/g,'')}}')">
              ${{rec ? '✎ Recipe' : '+ Recipe'}}
            </button>
          </td>
        </tr>`;
      }}).join('')}}
      </tbody></table></div>`;
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

// ══════════════════════════════════════════════════════════════════
// IGNORE LIST
// ══════════════════════════════════════════════════════════════════
async function loadIgnored() {{
  const panel = document.getElementById('ignored-panel');
  panel.innerHTML = '<div class="loading"><span class="spinner"></span></div>';
  try {{
    const r = await fetch('/inventory/breakdown/api/ignore');
    const d = await r.json();
    const ignored = d.ignored || [];
    if (!ignored.length) {{
      panel.innerHTML = '<p style="color:var(--text-dim);padding:8px">No ignored SKUs.</p>';
      return;
    }}
    panel.innerHTML = `<table>
      <thead><tr><th>Product</th><th>TCG ID</th><th>Reason</th><th>Ignored At</th><th></th></tr></thead>
      <tbody>${{ignored.map(i => `<tr>
        <td>${{i.product_name||'—'}}</td>
        <td style="color:var(--text-dim)">${{i.tcgplayer_id}}</td>
        <td style="color:var(--text-dim)">${{i.reason||'—'}}</td>
        <td style="color:var(--text-dim);font-size:12px">${{new Date(i.ignored_at).toLocaleDateString()}}</td>
        <td><button class="btn btn-secondary btn-sm" onclick="unignoreSku(${{i.tcgplayer_id}}, this)">Unignore</button></td>
      </tr>`).join('')}}</tbody></table>`;
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

async function ignoreSku(tcgId, name) {{
  if (!confirm(`Ignore "${{name}}" from breakdown recommendations?`)) return;
  await fetch('/inventory/breakdown/api/ignore', {{
    method: 'POST', headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify({{ tcgplayer_id: tcgId, product_name: name }})
  }});
  _allRecs = _allRecs.filter(r => r.tcgplayer_id !== tcgId);
  renderRecommendations();
}}

async function unignoreSku(tcgId, btn) {{
  btn.disabled = true; btn.textContent = '⟳';
  await fetch(`/inventory/breakdown/api/ignore/${{tcgId}}`, {{ method: 'DELETE' }});
  loadIgnored();
  loadRecommendations();
}}

// ══════════════════════════════════════════════════════════════════
// RECIPE EDITOR  (reused breakdown widget from intake/ingest)
// ══════════════════════════════════════════════════════════════════
async function openRecipeEditor(tcgId, productName) {{
  _recipeTarget = {{ tcgId, productName }};
  _recipeComponents = [];
  _recipeVariantId = null;
  _storePrices = {{}};

  const body = document.getElementById('recipe-modal-body');
  document.getElementById('recipe-modal-title').textContent = productName || 'Breakdown Recipe';
  body.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
  document.getElementById('recipe-modal').classList.add('active');

  let cache = null;
  if (tcgId) {{
    try {{
      const r = await fetch(`/inventory/breakdown/api/cache/${{tcgId}}`);
      const d = await r.json();
      if (d.found) cache = d.cache;
    }} catch(e) {{}}
    fetchRecipeStorePrices();
  }}
  renderRecipeModal(cache);
}}

function renderRecipeModal(cache) {{
  const body = document.getElementById('recipe-modal-body');
  const variants = cache?.variants || [];
  const tcgId = _recipeTarget?.tcgId;
  const productName = _recipeTarget?.productName;

  const variantCards = variants.length ? `
    <div style="margin-bottom:16px">
      <div style="font-size:12px;color:var(--green);font-weight:600;margin-bottom:8px">
        💾 ${{variants.length}} saved config${{variants.length>1?'s':''}}
      </div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">
        ${{variants.map(v => `
          <div style="background:var(--surface-2);border:1px solid var(--border);border-radius:6px;padding:8px 12px;min-width:160px">
            <div style="font-size:13px;font-weight:600">${{v.variant_name}}</div>
            <div style="font-size:12px;color:var(--text-dim)">${{v.component_count}} components · $${{parseFloat(v.total_component_market||0).toFixed(2)}}</div>
            ${{v.notes ? `<div style="font-size:11px;color:var(--amber)">📝 ${{v.notes}}</div>` : ''}}
            <div style="display:flex;gap:6px;margin-top:8px">
              <button class="btn btn-primary btn-sm" onclick="loadVariantIntoEditor(${{JSON.stringify(v).replace(/"/g,'&quot;')}})">↓ Edit</button>
              <button class="btn btn-sm" style="color:var(--red);border:1px solid var(--border);background:none"
                onclick="deleteVariant('${{v.id}}','${{v.variant_name.replace(/'/g,'').replace(/"/g,'')}}')">🗑</button>
            </div>
          </div>
        `).join('')}}
      </div>
      <div style="font-size:12px;color:var(--text-dim);margin-bottom:4px">— or add a new config below —</div>
    </div>` : `<div style="font-size:13px;color:var(--text-dim);margin-bottom:16px">
      No configs saved yet for <strong>${{productName}}</strong>. Build one below.</div>`;

  body.innerHTML = `
    <p style="font-size:12px;color:var(--text-dim);margin-bottom:12px">
      ${{productName}}${{tcgId ? ` <span style="font-size:11px;background:var(--surface-2);padding:2px 6px;border-radius:4px">TCG#${{tcgId}}</span>` : ''}}
    </p>
    ${{variantCards}}
    <div style="border-top:1px solid var(--border);padding-top:14px">
      <div style="display:flex;gap:8px;margin-bottom:10px;align-items:center;flex-wrap:wrap">
        <label style="font-size:12px;color:var(--text-dim)">Config name:</label>
        <input type="text" id="re-variant-name" value="Standard" style="width:160px">
        <span id="re-editing-badge" style="display:none;font-size:11px;color:var(--accent);background:var(--surface-2);padding:2px 8px;border-radius:4px">editing</span>
      </div>
      <div style="display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:10px">
        <div style="flex:2;min-width:180px">
          <label style="font-size:12px;color:var(--text-dim)">Search PPT for component</label>
          <input type="text" id="re-search" placeholder="e.g. Booster Pack, ETB…"
                 style="width:100%;margin-top:4px" onkeydown="if(event.key==='Enter') reSearch()">
        </div>
        <button class="btn btn-primary btn-sm" onclick="reSearch()" style="height:36px">Search</button>
      </div>
      <div id="re-search-results" style="max-height:160px;overflow-y:auto;margin-bottom:10px"></div>
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px">
        <h4 style="font-size:13px">Components</h4>
        <div id="re-value-summary" style="font-size:12px;color:var(--text-dim)"></div>
      </div>
      <div id="re-components-list"><p style="color:var(--text-dim);font-size:13px">No components yet.</p></div>
      <div id="re-notes-row" style="margin-top:12px;display:none">
        <label style="font-size:12px;color:var(--text-dim)">Notes</label>
        <input type="text" id="re-notes" placeholder="e.g. Dark Sylveon variant" style="width:100%;margin-top:4px">
      </div>
      <div style="margin-top:14px;display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-success" onclick="saveRecipeVariant()">💾 Save Config</button>
        <button class="btn btn-secondary" onclick="closeModal()">Close</button>
        <button class="btn btn-secondary btn-sm" onclick="document.getElementById('re-notes-row').style.display='block';document.getElementById('re-notes').focus()" style="font-size:11px">+ Notes</button>
      </div>
    </div>`;
  setTimeout(() => document.getElementById('re-search')?.focus(), 100);
  reRenderComponents();
}}

function loadVariantIntoEditor(v) {{
  if (typeof v === 'string') v = JSON.parse(v.replace(/&quot;/g, '"'));
  _recipeVariantId = v.id;
  _recipeComponents = (v.components||[]).map(c => ({{
    product_name: c.product_name, set_name: c.set_name||'',
    tcgplayer_id: c.tcgplayer_id,
    market_price: parseFloat(c.market_price)||0,
    quantity: parseInt(c.quantity_per_parent)||1,
  }}));
  const nameEl = document.getElementById('re-variant-name');
  if (nameEl) nameEl.value = v.variant_name;
  if (v.notes) {{
    const notesEl = document.getElementById('re-notes');
    if (notesEl) {{ notesEl.value = v.notes; document.getElementById('re-notes-row').style.display='block'; }}
  }}
  document.getElementById('re-editing-badge').style.display = 'inline';
  reRenderComponents();
  fetchRecipeStorePrices();
}}

async function deleteVariant(variantId, name) {{
  if (!confirm(`Delete the "${{name}}" config?`)) return;
  const r = await fetch(`/inventory/breakdown/api/cache/variant/${{variantId}}`, {{ method: 'DELETE' }});
  const d = await r.json();
  renderRecipeModal(d.cache);
}}

async function reSearch() {{
  const q = document.getElementById('re-search')?.value.trim();
  if (!q) return;
  const panel = document.getElementById('re-search-results');
  panel.innerHTML = '<div class="loading"><span class="spinner"></span></div>';
  try {{
    const r = await fetch(`/inventory/breakdown/api/cache/search?q=${{encodeURIComponent(q)}}`);
    const d = await r.json();
    if (!r.ok) {{ panel.innerHTML = `<div class="alert alert-error">${{d.error}}</div>`; return; }}
    const results = d.results || [];
    if (!results.length) {{ panel.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No results</p>'; return; }}
    window._reSearchResults = {{}};
    panel.innerHTML = results.slice(0,15).map((p, idx) => {{
      const name = p.name||p.productName||'';
      const setName = p.setName||p.set_name||'';
      const tcgId = p.tcgplayer_id||p.tcgplayerId||p.tcgPlayerId||p.id||0;
      const price = p.unopenedPrice||p.marketPrice||0;
      window._reSearchResults[idx] = {{ tcgId, name, setName, price }};
      return `<div class="search-result" onclick="reAddByIdx(${{idx}})">
        <strong style="font-size:13px">${{name}}</strong>
        <br><small style="color:var(--text-dim)">${{setName}} · TCG#${{tcgId}} · $${{(price||0).toFixed(2)}}</small>
      </div>`;
    }}).join('');
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

function reAddByIdx(idx) {{
  const r = (window._reSearchResults||{{}})[idx];
  if (r) reAdd(r.tcgId, r.name, r.setName, r.price);
}}

function reAdd(tcgId, name, setName, price) {{
  const existing = _recipeComponents.find(c => c.tcgplayer_id && c.tcgplayer_id === tcgId);
  if (existing) {{ existing.quantity += 1; }}
  else {{ _recipeComponents.push({{ product_name: name, set_name: setName, tcgplayer_id: tcgId, market_price: price, quantity: 1 }}); }}
  reRenderComponents();
  fetchRecipeStorePrices();
  document.getElementById('re-search-results').innerHTML = '';
  document.getElementById('re-search').value = '';
  document.getElementById('re-search').focus();
}}

async function fetchRecipeStorePrices() {{
  const compIds = _recipeComponents.map(c => c.tcgplayer_id).filter(Boolean);
  const parentId = _recipeTarget?.tcgId;
  const allIds = parentId ? [...new Set([parentId, ...compIds])] : compIds;
  if (!allIds.length) return;
  try {{
    const r = await fetch('/inventory/breakdown/api/store-prices', {{
      method: 'POST', headers: {{'Content-Type':'application/json'}},
      body: JSON.stringify({{ tcgplayer_ids: allIds }})
    }});
    const d = await r.json();
    _storePrices = d.prices || {{}};
  }} catch(e) {{ _storePrices = {{}}; }}
  reRenderComponents();
}}

function reRenderComponents() {{
  const el = document.getElementById('re-components-list');
  const summary = document.getElementById('re-value-summary');
  if (!el) return;
  const totalMkt = _recipeComponents.reduce((s,c) => s+(parseFloat(c.market_price)||0)*(parseInt(c.quantity)||1), 0);
  const storeComps = _recipeComponents.filter(c => c.tcgplayer_id && _storePrices[c.tcgplayer_id]);
  const totalStore = storeComps.reduce((s,c) => s+(_storePrices[c.tcgplayer_id].shopify_price||0)*(parseInt(c.quantity)||1), 0);

  if (summary) {{
    const parentId = _recipeTarget?.tcgId;
    const parentEntry = parentId ? _storePrices[String(parentId)] : null;
    const parentStore = parentEntry ? parseFloat(parentEntry.shopify_price)||0 : 0;
    let sh = _recipeComponents.length ? `Market: <strong>$${{totalMkt.toFixed(2)}}</strong>` : '';
    if (totalStore > 0) {{
      const diff = parentStore > 0 ? totalStore - parentStore : null;
      const col = diff !== null ? (diff >= 0 ? 'var(--green)' : diff >= -parentStore*0.1 ? 'var(--amber)' : 'var(--red)') : 'var(--text-dim)';
      const diffStr = diff !== null ? ` <small style="color:${{diff>=0?'var(--green)':'var(--red)'}}">(${{diff>=0?'+':''}}$${{diff.toFixed(2)}} vs store)</small>` : '';
      sh += ` · <span style="color:${{col}}">Store: <strong>$${{totalStore.toFixed(2)}}</strong>${{diffStr}}</span>`;
    }}
    summary.innerHTML = sh;
  }}

  if (!_recipeComponents.length) {{
    el.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No components yet.</p>';
    return;
  }}
  const hasStore = Object.keys(_storePrices).length > 0;
  el.innerHTML = `<div style="overflow-x:auto"><table style="font-size:13px"><thead><tr>
    <th>Component</th><th style="width:70px">Qty</th><th style="width:90px">Market</th>
    ${{hasStore ? '<th style="width:90px;color:var(--accent)">Store</th>' : ''}}
    <th style="width:30px"></th>
  </tr></thead><tbody>` +
  _recipeComponents.map((c, idx) => {{
    const sp = c.tcgplayer_id ? _storePrices[String(c.tcgplayer_id)] : null;
    const storeCell = hasStore
      ? `<td>${{sp
          ? `<span style="color:var(--green)">$${{parseFloat(sp.shopify_price).toFixed(2)}}${{sp.shopify_qty===0?' <small style="color:var(--red)">qty 0</small>':''}}</span>`
          : '<span style="color:var(--text-dim)">—</span>'}}</td>`
      : '';
    return `<tr>
      <td>${{c.product_name}}<br><small style="color:${{c.tcgplayer_id?'var(--text-dim)':'var(--red)'}}">${{c.tcgplayer_id?'TCG#'+c.tcgplayer_id:'⚠ No TCG ID'}}${{c.set_name?' · '+c.set_name:''}}</small></td>
      <td><input type="number" value="${{c.quantity}}" min="1" style="width:55px" onchange="_recipeComponents[${{idx}}].quantity=parseInt(this.value)||1;reRenderComponents()"></td>
      <td><input type="number" value="${{(c.market_price||0).toFixed(2)}}" min="0" step="0.01" style="width:80px" onchange="_recipeComponents[${{idx}}].market_price=parseFloat(this.value)||0;reRenderComponents()"></td>
      ${{storeCell}}
      <td><button class="btn btn-sm" style="color:var(--red);font-size:11px;padding:2px 6px;border:1px solid var(--border);background:none"
        onclick="_recipeComponents.splice(${{idx}},1);_storePrices={{}};reRenderComponents()">✕</button></td>
    </tr>`;
  }}).join('') + '</tbody></table></div>';
}}

async function saveRecipeVariant() {{
  if (!_recipeComponents.length) {{ alert('Add at least one component.'); return; }}
  if (!_recipeTarget?.tcgId) {{ alert('This product has no TCGPlayer ID — cannot save to cache.'); return; }}
  const variantName = document.getElementById('re-variant-name')?.value.trim() || 'Standard';
  const notes = document.getElementById('re-notes')?.value.trim() || null;
  try {{
    const r = await fetch(`/inventory/breakdown/api/cache/${{_recipeTarget.tcgId}}/variant`, {{
      method: 'POST', headers: {{'Content-Type':'application/json'}},
      body: JSON.stringify({{
        product_name: _recipeTarget.productName,
        variant_name: variantName,
        components: _recipeComponents,
        notes: notes,
        variant_id: _recipeVariantId || undefined,
      }})
    }});
    const d = await r.json();
    if (!r.ok) {{ alert(d.error||'Save failed'); return; }}
    _recipeComponents = [];
    _recipeVariantId = null;
    renderRecipeModal(d.cache);
    // Refresh recommendations in background
    loadRecommendations();
  }} catch(e) {{ alert(e.message); }}
}}

// ══════════════════════════════════════════════════════════════════
// UTILS
// ══════════════════════════════════════════════════════════════════
function closeModal() {{
  document.getElementById('recipe-modal').classList.remove('active');
}}

document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') {{
    document.querySelectorAll('.modal-overlay.active').forEach(m => m.classList.remove('active'));
  }}
}});

// ══════════════════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════════════════
loadRecommendations();
</script>

<!-- Inventory all-items endpoint needed by manual search -->
<script>
// Shim: fetch all inventory items for search tab
// Piggybacks on the existing inventory API
const _origSearchInventory = searchInventory;
searchInventory = async function() {{
  const q = document.getElementById('inv-search').value.trim().toLowerCase();
  const panel = document.getElementById('inv-search-results');
  if (!q) return;
  panel.innerHTML = '<div class="loading"><span class="spinner"></span> Searching...</div>';
  try {{
    // Use recommendations + fetch all cache for items without recipes
    const [r1, r2] = await Promise.all([
      fetch('/inventory/breakdown/api/recommendations').then(r=>r.json()),
      fetch('/inventory/api/items?q=' + encodeURIComponent(q)).then(r=>r.json()),
    ]);
    const recs = r1.recommendations || [];
    const items = r2.items || [];
    const recMap = {{}};
    recs.forEach(r => recMap[r.tcgplayer_id] = r);
    if (!items.length) {{ panel.innerHTML = '<div class="alert alert-warning">No items found.</div>'; return; }}
    panel.innerHTML = `<div style="overflow-x:auto"><table>
      <thead><tr><th>Product</th><th>Qty</th><th>Price</th><th>BD Value</th><th></th></tr></thead>
      <tbody>${{items.slice(0,30).map(item => {{
        const rec = item.tcgplayer_id ? recMap[parseInt(item.tcgplayer_id)] : null;
        const qty = item.shopify_qty;
        return `<tr>
          <td><strong>${{item.name||item.title}}</strong><br><small style="color:var(--text-dim)">TCG#${{item.tcgplayer_id||'—'}}</small></td>
          <td style="color:${{qty>0?'var(--green)':'var(--red)'}};font-weight:600">${{qty}}</td>
          <td>$${{parseFloat(item.shopify_price||0).toFixed(2)}}</td>
          <td>${{rec ? `<span class="${{rec.delta_pct>=0?'delta-pos':rec.delta_pct>=-10?'delta-neutral':'delta-neg'}}">$${{rec.bd_value.toFixed(2)}} (${{rec.delta_pct>=0?'+':''}}${{rec.delta_pct.toFixed(1)}}%)</span>` : '<span style="color:var(--text-dim)">No recipe</span>'}}</td>
          <td style="display:flex;gap:4px;flex-wrap:wrap">
            ${{rec && qty > 0 ? `<button class="btn btn-primary btn-sm" onclick="openExecuteModal(${{JSON.stringify(rec).replace(/"/g,'&quot;')}})">▶ Break Down</button>` : ''}}
            <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{item.tcgplayer_id||'null'}},'${{(item.name||item.title).replace(/'/g,'').replace(/"/g,'')}}')">
              ${{rec ? '✎ Recipe' : '+ Recipe'}}
            </button>
          </td>
        </tr>`;
      }}).join('')}}</tbody></table></div>`;
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}};
</script>

</body>
</html>"""
