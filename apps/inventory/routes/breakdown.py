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
try:
    from ppt_client import PPTError as _PPTError
except ImportError:
    _PPTError = Exception

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
    # All non-ignored inventory items that have a TCGPlayer ID
    # Include drafts and zero-qty if they have a breakdown recipe (stub entries)
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
          AND c.is_damaged = FALSE
          AND c.tcgplayer_id NOT IN (SELECT tcgplayer_id FROM breakdown_ignore)
          AND (
              c.shopify_qty > 0
              OR EXISTS (
                  SELECT 1 FROM sealed_breakdown_cache sbc
                  WHERE sbc.tcgplayer_id = c.tcgplayer_id
              )
          )
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
            SELECT sbcomp.tcgplayer_id AS component_tcg_id,
                   sbc.tcgplayer_id AS parent_tcg_id,
                   sbv.id AS variant_id
            FROM sealed_breakdown_components sbcomp
            JOIN sealed_breakdown_variants sbv ON sbv.id = sbcomp.variant_id
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
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
        bd_value_mkt = float(recipe["total_component_market"] or 0)

        # Prefer store prices of children for bd_value
        vid = str(recipe["best_variant_id"])
        child_tcg_ids = variant_comp_map.get(vid, [])
        child_qtys = [child_qty_map[cid]["shopify_qty"] for cid in child_tcg_ids if cid in child_qty_map]
        child_store_vals = []
        for cid in child_tcg_ids:
            if cid in child_qty_map:
                sp = float(child_qty_map[cid].get("shopify_price") or 0)
                # qty_per_parent comes from components lookup — need per-component qty
                child_store_vals.append((cid, sp))

        # Compute store-based bd value using per-component qtys from recipe
        bd_value_store = 0.0
        if child_store_vals:
            # Get quantity_per_parent for each component in this variant
            comp_qty_rows = db.query("""
                SELECT tcgplayer_id, quantity_per_parent
                FROM sealed_breakdown_components WHERE variant_id = %s
            """, (vid,))
            comp_qty_map = {int(r["tcgplayer_id"]): int(r["quantity_per_parent"]) for r in comp_qty_rows}
            all_have_store = True
            for cid, sp in child_store_vals:
                if sp > 0:
                    bd_value_store += sp * comp_qty_map.get(cid, 1)
                else:
                    all_have_store = False
            if not all_have_store:
                bd_value_store = 0.0  # partial store data — fall back to market

        bd_value = bd_value_store if bd_value_store > 0 else bd_value_mkt
        bd_value_label = "store" if bd_value_store > 0 else "market"

        if store_price <= 0 or bd_value <= 0:
            continue

        delta_pct = (bd_value - store_price) / store_price * 100

        # Low-stock signal: avg qty of child components in store
        # vid and child_tcg_ids already computed above for bd_value_store
        avg_child_qty = sum(child_qtys) / len(child_qtys) if child_qtys else 999
        min_child_qty = min(child_qtys) if child_qtys else 999
        children_in_store = len([q for q in child_qtys if q > 0])
        total_children   = len(child_tcg_ids)

        # Score: prefer positive delta + low child stock
        # Low child qty pulls score UP (more desirable to break down)
        low_stock_bonus = max(0, 20 - avg_child_qty) * 0.5
        score = delta_pct + low_stock_bonus

        # Build per-component detail list for display
        comp_details = []
        for cid in child_tcg_ids:
            info = child_qty_map.get(cid, {})
            # get quantity_per_parent from components query
            qty_per_parent = next(
                (int(comp.get("quantity_per_parent", 1))
                 for comp in db.query(
                     "SELECT quantity_per_parent FROM sealed_breakdown_components WHERE variant_id=%s AND tcgplayer_id=%s",
                     (vid, cid))
                 ), 1)
            comp_details.append({
                "tcgplayer_id":    cid,
                "title":           info.get("title", f"TCG#{cid}"),
                "shopify_qty":     int(info.get("shopify_qty") or 0) if info else None,
                "shopify_price":   float(info.get("shopify_price") or 0) if info else None,
                "qty_per_parent":  qty_per_parent,
                "in_store":        bool(info),
            })

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
            "bd_value_label":     bd_value_label,
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
            "components":         comp_details,
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


# ─── Breakdown cache routes — direct DB (same shared Postgres, no proxy needed) ─

def _load_cache_for_tcg(tcg_id):
    """Load breakdown cache + all variants + components for a tcgplayer_id."""
    row = db.query_one("SELECT * FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcg_id,))
    if not row:
        return {"found": False}
    variants = db.query(
        "SELECT * FROM sealed_breakdown_variants WHERE breakdown_id=%s ORDER BY display_order, created_at",
        (row["id"],)
    )
    result_variants = []
    for v in variants:
        comps = db.query(
            "SELECT * FROM sealed_breakdown_components WHERE variant_id=%s ORDER BY display_order",
            (v["id"],)
        )
        result_variants.append({**dict(v), "components": [dict(c) for c in comps]})
    return {"found": True, "cache": {**dict(row), "variants": result_variants}}


@bp.route("/api/cache/<int:tcg_id>")
@requires_auth
def get_cache(tcg_id):
    return jsonify(_load_cache_for_tcg(tcg_id))


@bp.route("/api/cache/<int:tcg_id>/variant", methods=["POST"])
@requires_auth
def save_variant(tcg_id):
    """Save (create or update) a breakdown variant. Proxies to ingest if available,
    otherwise writes directly to the shared DB."""
    body = request.get_json(silent=True) or {}
    # Try ingest proxy first (it handles best_variant_market recalc etc.)
    if INGEST_URL:
        data, err = _ingest_post(f"/api/breakdown-cache/{tcg_id}/variant", body)
        if not err:
            return jsonify(data)
    # Direct write fallback
    from decimal import Decimal
    import uuid as _uuid
    product_name = body.get("product_name", "")
    variant_name = body.get("variant_name", "Standard")
    notes        = body.get("notes", "")
    components   = body.get("components", [])
    variant_id   = body.get("variant_id")

    cache_row = db.query_one("SELECT id FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcg_id,))
    if cache_row:
        cache_id = cache_row["id"]
        db.execute("UPDATE sealed_breakdown_cache SET product_name=%s, last_updated=CURRENT_TIMESTAMP WHERE id=%s",
                   (product_name, cache_id))
    else:
        new_id = str(_uuid.uuid4())
        db.execute("INSERT INTO sealed_breakdown_cache (id, tcgplayer_id, product_name) VALUES (%s,%s,%s)",
                   (new_id, tcg_id, product_name))
        cache_id = new_id

    total_mkt = sum(
        Decimal(str(c.get("market_price",0))) * int(c.get("quantity_per_parent", c.get("quantity",1)))
        for c in components
    )

    if variant_id:
        db.execute("UPDATE sealed_breakdown_variants SET variant_name=%s, notes=%s, total_component_market=%s WHERE id=%s",
                   (variant_name, notes, total_mkt, variant_id))
        db.execute("DELETE FROM sealed_breakdown_components WHERE variant_id=%s", (variant_id,))
        vid = variant_id
    else:
        vid = str(_uuid.uuid4())
        db.execute("""INSERT INTO sealed_breakdown_variants
            (id, breakdown_id, variant_name, notes, total_component_market, component_count)
            VALUES (%s,%s,%s,%s,%s,%s)""",
            (vid, cache_id, variant_name, notes, total_mkt, len(components)))

    for i, comp in enumerate(components):
        db.execute("""INSERT INTO sealed_breakdown_components
            (variant_id, tcgplayer_id, product_name, set_name, quantity_per_parent, market_price, display_order, component_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (vid, comp.get("tcgplayer_id"), comp.get("product_name",""),
             comp.get("set_name",""), int(comp.get("quantity_per_parent", comp.get("quantity",1))),
             Decimal(str(comp.get("market_price",0))), i,
             comp.get("component_type", "sealed")))

    db.execute("UPDATE sealed_breakdown_variants SET component_count=%s WHERE id=%s", (len(components), vid))
    # Recalc best_variant_market
    db.execute("""UPDATE sealed_breakdown_cache SET
        best_variant_market = (SELECT MAX(total_component_market) FROM sealed_breakdown_variants WHERE breakdown_id=%s),
        variant_count = (SELECT COUNT(*) FROM sealed_breakdown_variants WHERE breakdown_id=%s)
        WHERE id=%s""", (cache_id, cache_id, cache_id))

    return jsonify({**_load_cache_for_tcg(tcg_id), "success": True})


@bp.route("/api/cache/variant/<variant_id>", methods=["DELETE"])
@requires_auth
def delete_variant(variant_id):
    v = db.query_one("SELECT sbv.breakdown_id, sbc.tcgplayer_id FROM sealed_breakdown_variants sbv JOIN sealed_breakdown_cache sbc ON sbc.id=sbv.breakdown_id WHERE sbv.id=%s", (variant_id,))
    if not v:
        return jsonify({"error": "Not found"}), 404
    db.execute("DELETE FROM sealed_breakdown_components WHERE variant_id=%s", (variant_id,))
    db.execute("DELETE FROM sealed_breakdown_variants WHERE id=%s", (variant_id,))
    cnt = db.query_one("SELECT COUNT(*) AS c FROM sealed_breakdown_variants WHERE breakdown_id=%s", (v["breakdown_id"],))
    if cnt and cnt["c"] == 0:
        db.execute("DELETE FROM sealed_breakdown_cache WHERE id=%s", (v["breakdown_id"],))
        return jsonify({"found": False})
    db.execute("""UPDATE sealed_breakdown_cache SET
        best_variant_market=(SELECT MAX(total_component_market) FROM sealed_breakdown_variants WHERE breakdown_id=%s),
        variant_count=(SELECT COUNT(*) FROM sealed_breakdown_variants WHERE breakdown_id=%s)
        WHERE id=%s""", (v["breakdown_id"], v["breakdown_id"], v["breakdown_id"]))
    return jsonify(_load_cache_for_tcg(v["tcgplayer_id"]))


@bp.route("/api/cache/search")
@requires_auth
def search_ppt():
    from routes.inventory import _get_ppt_client
    q = request.args.get("q", "")
    if not q:
        return jsonify({"results": []})
    ppt = _get_ppt_client()
    if ppt is None:
        # fallback to ingest proxy
        if INGEST_URL:
            data, err = _ingest_post("/api/ppt/search-sealed", {"query": q})
            if not err:
                return jsonify(data)
        return jsonify({"results": [], "error": "PPT not configured"}), 503
    try:
        results = ppt.search_sealed_products(q, limit=5)
        return jsonify({"results": results})
    except _PPTError as e:
        details = e.args[2] if len(e.args) > 2 else {}
        retry = details.get("retry_after", 60) if isinstance(details, dict) else 60
        return jsonify({"results": [], "error": str(e.args[0]) if e.args else str(e), "retry_after": retry}), 429

@bp.route("/api/cache/search-cards")
@requires_auth
def search_cards():
    """Search raw cards via PPT — used for promo components in breakdown recipes.
    Returns NM condition price as market_price."""
    from routes.inventory import _get_ppt_client
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"results": []})

    def _enrich_nm_price(results):
        for r in (results or []):
            if not r.get("market_price"):
                conds = (r.get("prices") or {}).get("conditions") or {}
                nm = conds.get("Near Mint") or conds.get("NM") or {}
                r["market_price"] = nm.get("price") or (r.get("prices") or {}).get("market") or 0
        return results

    ppt = _get_ppt_client()
    if ppt is None:
        if INGEST_URL:
            data, err = _ingest_post("/api/ppt/search-cards", {"query": q, "limit": 5})
            if not err:
                return jsonify({"results": _enrich_nm_price(data.get("results") or [])})
        return jsonify({"results": [], "error": "PPT not configured"}), 503

    try:
        results = ppt.search_cards(q, limit=5)
        return jsonify({"results": _enrich_nm_price(results)})
    except Exception as e:
        # Fallback to ingest proxy on local failure
        if INGEST_URL:
            data, err = _ingest_post("/api/ppt/search-cards", {"query": q, "limit": 5})
            if not err:
                return jsonify({"results": _enrich_nm_price(data.get("results") or [])})
        return jsonify({"results": [], "error": str(e)}), 502

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


@bp.route("/api/inventory-search")
@requires_auth
def inventory_search():
    """Search inventory_product_cache by title for the recipe picker.
    Only returns sealed products — slabs/graded cards cannot be broken down."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"items": []})
    rows = db.query("""
        SELECT tcgplayer_id, title, shopify_qty, shopify_price, shopify_variant_id
        FROM inventory_product_cache
        WHERE LOWER(title) LIKE %s
          AND is_damaged = FALSE
          AND shopify_qty > 0
          AND LOWER(COALESCE(tags, '')) LIKE '%%sealed%%'
          AND LOWER(COALESCE(tags, '')) NOT LIKE '%%slab%%'
          AND LOWER(COALESCE(tags, '')) NOT LIKE '%%graded%%'
        ORDER BY title
        LIMIT 30
    """, (f"%{q.lower()}%",))
    return jsonify({"items": [dict(r) for r in rows]})


@bp.route("/api/base-component", methods=["POST"])
@requires_auth
def mark_base_component():
    data = request.get_json(silent=True) or {}
    tcg_id = data.get("tcgplayer_id")
    name   = data.get("product_name", "")
    if not tcg_id:
        return jsonify({"error": "tcgplayer_id required"}), 400
    db.execute("""
        INSERT INTO breakdown_base_components (tcgplayer_id, product_name)
        VALUES (%s, %s) ON CONFLICT (tcgplayer_id) DO UPDATE SET product_name = EXCLUDED.product_name
    """, (tcg_id, name))
    return jsonify({"success": True})

@bp.route("/api/base-component/<int:tcg_id>", methods=["DELETE"])
@requires_auth
def unmark_base_component(tcg_id):
    db.execute("DELETE FROM breakdown_base_components WHERE tcgplayer_id = %s", (tcg_id,))
    return jsonify({"success": True})

@bp.route("/api/all-recipes")
@requires_auth
def all_recipes():
    """All saved breakdown recipes with their variants and store inventory status."""
    rows = db.query("""
        SELECT
            sbc.id            AS cache_id,
            sbc.tcgplayer_id,
            sbc.product_name,
            sbc.variant_count,
            sbc.use_count,
            sbc.best_variant_market,
            sbc.last_updated,
            ipc.title         AS store_title,
            ipc.shopify_qty   AS store_qty,
            ipc.shopify_price AS store_price,
            ipc.status        AS store_status
        FROM sealed_breakdown_cache sbc
        LEFT JOIN inventory_product_cache ipc ON ipc.tcgplayer_id = sbc.tcgplayer_id
          AND ipc.is_damaged = FALSE
        ORDER BY sbc.product_name
    """)
    return jsonify({"recipes": [dict(r) for r in rows]})

@bp.route("/api/no-recipe")
@requires_auth
def no_recipe_items():
    """Items in store that have no breakdown recipe and aren't base components.
    Includes draft products. Excludes damaged and slab-tagged items.
    """
    rows = db.query("""
        SELECT c.tcgplayer_id, c.title, c.shopify_qty, c.shopify_price,
               c.shopify_variant_id, c.inventory_item_id, c.status, c.tags,
               CASE WHEN bc.tcgplayer_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_base
        FROM inventory_product_cache c
        LEFT JOIN sealed_breakdown_cache sbc ON sbc.tcgplayer_id = c.tcgplayer_id
        LEFT JOIN breakdown_base_components bc ON bc.tcgplayer_id = c.tcgplayer_id
        WHERE c.is_damaged = FALSE
          AND c.tcgplayer_id IS NOT NULL
          AND c.status IN ('ACTIVE', 'DRAFT')
          AND NOT (c.tags ILIKE '%slab%')
          AND sbc.tcgplayer_id IS NULL
        ORDER BY c.title
    """)
    return jsonify({"items": [dict(r) for r in rows]})


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
    <button class="tab-btn" onclick="switchTab('recipes',this)">📖 Known Recipes</button>
    <button class="tab-btn" onclick="switchTab('ignored',this)">🚫 Ignored SKUs</button>
    <button class="tab-btn" onclick="switchTab('norecipe',this)">📋 No Recipe</button>
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

  <!-- ═══ KNOWN RECIPES TAB ════════════════════════════════════════════════ -->
  <div id="tab-recipes" class="tab-pane">
    <div class="card" style="padding:10px 16px;margin-bottom:12px">
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <input type="text" id="kr-search" placeholder="Filter by name…" style="width:260px"
               oninput="renderKnownRecipes()">
        <span id="kr-count" style="font-size:12px;color:var(--text-dim);margin-left:auto"></span>
      </div>
    </div>
    <div id="known-recipes-panel"><div class="loading"><span class="spinner"></span> Loading...</div></div>
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

  <!-- ═══ NO RECIPE TAB ════════════════════════════════════════════════════ -->
  <div id="tab-norecipe" class="tab-pane">
    <div class="card" style="padding:10px 16px;margin-bottom:12px">
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <input type="text" id="nr-search" placeholder="Filter by name…" style="width:220px"
               oninput="renderNoRecipe()">
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="nr-hide-base" onchange="renderNoRecipe()" style="width:15px;height:15px">
          Hide base components
        </label>
        <span style="font-size:12px;color:var(--text-dim);margin-left:auto">
          Items with no breakdown recipe. Mark as <em>base component</em> to suppress permanently.
        </span>
      </div>
    </div>
    <div id="norecipe-panel"><div class="loading"><span class="spinner"></span> Loading...</div></div>
  </div>
</div>

<!-- ═══ THEMED CONFIRM DIALOG ═════════════════════════════════════════════ -->
<div id="themed-confirm-overlay" class="modal-overlay">
  <div class="modal" style="max-width:420px">
    <h3 id="tc-title" style="margin-bottom:10px"></h3>
    <p id="tc-message" style="color:var(--text-dim);font-size:13px;margin-bottom:20px;line-height:1.5"></p>
    <div style="display:flex;gap:8px;justify-content:flex-end">
      <button id="tc-cancel" class="btn btn-secondary">Cancel</button>
      <button id="tc-confirm" class="btn btn-primary">Confirm</button>
    </div>
  </div>
</div>

<!-- ═══ RECIPE EDITOR MODAL ════════════════════════════════════════════════ -->
<div id="recipe-modal" class="modal-overlay">
  <div class="modal" style="max-width:860px">
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
// THEMED DIALOG  (matches intake/ingest modal style)
// ══════════════════════════════════════════════════════════════════
function themedConfirm(title, message, {{ confirmText='Confirm', dangerous=false }}={{}}) {{
  return new Promise(resolve => {{
    const overlay = document.getElementById('themed-confirm-overlay');
    document.getElementById('tc-title').textContent = title;
    document.getElementById('tc-message').textContent = message;
    const btn = document.getElementById('tc-confirm');
    btn.textContent = confirmText;
    btn.className = 'btn ' + (dangerous ? 'btn-danger' : 'btn-primary');
    overlay.classList.add('active');
    function cleanup() {{ overlay.classList.remove('active'); btn.removeEventListener('click', onOk); document.getElementById('tc-cancel').removeEventListener('click', onCancel); }}
    function onOk() {{ cleanup(); resolve(true); }}
    function onCancel() {{ cleanup(); resolve(false); }}
    btn.addEventListener('click', onOk);
    document.getElementById('tc-cancel').addEventListener('click', onCancel);
  }});
}}

// ══════════════════════════════════════════════════════════════════
// STATE
// ══════════════════════════════════════════════════════════════════
let _allRecs = [];
let _recipeTarget = null;   // {{tcgId, name, variantId, variantId}}
let _recipeComponents = [];  // each item has component_type: 'sealed'|'promo'
let _recipeVariantId = null;
let _storePrices = {{}};
let _pendingListings = new Set(); // TCG IDs listed this session, not yet in cache
let _ingestUrl = "{ingest_url}";

// ══════════════════════════════════════════════════════════════════
// TABS
// ══════════════════════════════════════════════════════════════════
function switchTab(id, btn) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + id).classList.add('active');
  if (btn) btn.classList.add('active');
  location.hash = id;
  if (id === 'ignored') loadIgnored();
  if (id === 'norecipe') loadNoRecipe();
  if (id === 'recommendations') loadRecommendations();
  if (id === 'recipes') loadKnownRecipes();
}}

function restoreTab() {{
  const hash = location.hash.replace('#', '');
  const valid = ['recommendations','search','ignored','norecipe','recipes'];
  const id = valid.includes(hash) ? hash : 'recommendations';
  const btn = document.querySelector(`.tab-btn[onclick*="'${{id}}'"]`);
  switchTab(id, btn);
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
  else if (sort === 'child_qty')  recs.sort((a,b) => a.min_child_qty - b.min_child_qty);
  else if (sort === 'store_qty')  recs.sort((a,b) => b.store_qty - a.store_qty);
  else recs.sort((a,b) => b.score - a.score); // score

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
  const childStockStr = (r.components && r.components.length > 0)
    ? r.components.map(comp => {{
        const qty = comp.shopify_qty;
        const col = qty === null ? 'var(--text-dim)' : qty === 0 ? 'var(--red)' : qty < 5 ? 'var(--amber)' : 'var(--green)';
        const qtyStr = qty === null ? '<small style="color:var(--text-dim)">not in store</small>' : `<strong style="color:${{col}}">${{qty}}</strong>`;
        const perParent = comp.qty_per_parent > 1 ? ` <small style="color:var(--text-dim)">×${{comp.qty_per_parent}}/unit</small>` : '';
        return `<div style="font-size:12px;white-space:nowrap">${{comp.title.length>32?comp.title.slice(0,30)+'…':comp.title}}${{perParent}}: ${{qtyStr}}</div>`;
      }}).join('')
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
async function openExecuteModal(rec) {{
  if (typeof rec === 'string') rec = JSON.parse(rec.replace(/&quot;/g, '"'));
  const body = document.getElementById('execute-modal-body');
  document.getElementById('execute-modal').classList.add('active');

  // If multiple configs, fetch all variants and let user pick
  if (rec.variant_count > 1) {{
    body.innerHTML = '<div class="loading"><span class="spinner"></span> Loading configs...</div>';
    try {{
      const r = await fetch(`/inventory/breakdown/api/cache/${{rec.tcgplayer_id}}`);
      const d = await r.json();
      const variants = d.cache?.variants || [];
      body.innerHTML = `
        <div style="font-size:15px;font-weight:600;margin-bottom:4px">${{rec.title}}</div>
        <p style="font-size:13px;color:var(--text-dim);margin-bottom:14px">
          Store: <strong>$${{rec.store_price.toFixed(2)}}</strong> × ${{rec.store_qty}} in stock. Choose which config to break down into:
        </p>
        <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:16px">
          ${{variants.map(v => {{
            const storeTotal = parseFloat(v.total_component_market||0);
            const delta = rec.store_price > 0 ? ((storeTotal - rec.store_price)/rec.store_price*100) : 0;
            const dc = delta >= 0 ? 'var(--green)' : delta >= -10 ? 'var(--amber)' : 'var(--red)';
            return `<div style="background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:10px 14px;cursor:pointer;transition:border-color .15s"
              onmouseover="this.style.borderColor='var(--accent)'" onmouseout="this.style.borderColor='var(--border)'"
              onclick="openExecuteWithVariant(${{JSON.stringify(rec).replace(/"/g,'&quot;')}}, '${{v.id}}', '${{v.variant_name.replace(/'/g,'')}}', ${{storeTotal}})">
              <div style="display:flex;justify-content:space-between;align-items:center">
                <strong>${{v.variant_name}}</strong>
                <span style="color:${{dc}};font-weight:600">${{delta>=0?'+':''}}${{delta.toFixed(1)}}%</span>
              </div>
              <div style="font-size:12px;color:var(--text-dim);margin-top:3px">
                ${{v.component_count}} components · $${{storeTotal.toFixed(2)}} BD value
                ${{v.notes ? ` · <em>${{v.notes}}</em>` : ''}}
              </div>
            </div>`;
          }}).join('')}}
        </div>
        <button class="btn btn-secondary" onclick="document.getElementById('execute-modal').classList.remove('active')">Cancel</button>`;
    }} catch(e) {{
      body.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`;
    }}
    return;
  }}

  renderExecuteForm(rec, rec.best_variant_id, rec.best_variant_name, rec.bd_value);
}}

async function openExecuteWithVariant(rec, variantId, variantName, bdValue) {{
  if (typeof rec === 'string') rec = JSON.parse(rec.replace(/&quot;/g, '"'));
  const delta = rec.store_price > 0 ? (bdValue - rec.store_price) / rec.store_price * 100 : 0;
  const body = document.getElementById('execute-modal-body');
  body.innerHTML = '<div class="loading"><span class="spinner"></span> Loading components...</div>';

  // Fetch component details for this specific variant with store qtys
  let components = [];
  try {{
    const r = await fetch(`/inventory/breakdown/api/cache/${{rec.tcgplayer_id}}`);
    const d = await r.json();
    const variant = (d.cache?.variants||[]).find(v => v.id === variantId);
    if (variant?.components) {{
      // Fetch store qtys for each component
      const tcgIds = variant.components.map(c => c.tcgplayer_id).filter(Boolean);
      const sr = await fetch('/inventory/breakdown/api/store-prices', {{
        method: 'POST', headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{ tcgplayer_ids: tcgIds }})
      }});
      const sd = await sr.json();
      const prices = sd.prices || {{}};
      components = variant.components.map(c => ({{
        tcgplayer_id: c.tcgplayer_id,
        title: c.product_name,
        qty_per_parent: parseInt(c.quantity_per_parent)||1,
        shopify_qty: prices[String(c.tcgplayer_id)]?.shopify_qty ?? null,
        shopify_price: prices[String(c.tcgplayer_id)]?.shopify_price ?? null,
        in_store: !!prices[String(c.tcgplayer_id)],
      }}));
    }}
  }} catch(e) {{}}

  renderExecuteForm({{...rec, bd_value: bdValue, delta_pct: delta, bd_value_label: rec.bd_value_label||'market', components}}, variantId, variantName, bdValue);
}}

function renderExecuteForm(rec, variantId, variantName, bdValue) {{
  const body = document.getElementById('execute-modal-body');
  const deltaClass = rec.delta_pct >= 0 ? 'delta-pos' : rec.delta_pct >= -10 ? 'delta-neutral' : 'delta-neg';
  const bdLabel = rec.bd_value_label === 'store' ? '(store prices)' : '(market prices)';

  body.innerHTML = `
    <div style="margin-bottom:16px">
      <div style="font-size:15px;font-weight:600;margin-bottom:4px">${{rec.title}}</div>
      <div style="display:flex;gap:16px;font-size:13px;color:var(--text-dim);flex-wrap:wrap">
        <span>Store: <strong style="color:var(--text)">$${{rec.store_price.toFixed(2)}}</strong> × ${{rec.store_qty}} in stock</span>
        <span>BD value: <strong class="${{deltaClass}}">$${{bdValue.toFixed(2)}}</strong> ${{bdLabel}} (${{rec.delta_pct >= 0 ? '+' : ''}}${{rec.delta_pct.toFixed(1)}}%)</span>
        <span>Config: <strong style="color:var(--accent)">${{variantName}}</strong></span>
      </div>
    </div>

    <div id="exec-components-preview" style="margin-bottom:16px;background:var(--surface-2);border:1px solid var(--border);border-radius:6px;padding:10px 14px">
      <div style="font-size:12px;color:var(--text-dim);margin-bottom:6px;font-weight:600">Breaking into:</div>
      ${{(rec.components||[]).length > 0
        ? rec.components.map(comp => {{
            const col = comp.shopify_qty === null ? 'var(--text-dim)' : comp.shopify_qty === 0 ? 'var(--red)' : comp.shopify_qty < 5 ? 'var(--amber)' : 'var(--green)';
            const storeStr = comp.shopify_qty !== null
              ? `<span style="color:${{col}}">${{comp.shopify_qty}} in store</span>`
              : '<span style="color:var(--text-dim)">not in store</span>';
            return `<div style="display:flex;justify-content:space-between;font-size:12px;padding:2px 0">
              <span>${{comp.qty_per_parent > 1 ? comp.qty_per_parent + '× ' : ''}}${{comp.title}}</span>
              <span>${{storeStr}}</span>
            </div>`;
          }}).join('')
        : '<p style="font-size:12px;color:var(--text-dim)">Component list unavailable</p>'
      }}
    </div>

    <div style="margin-bottom:16px">
      <label style="font-size:12px;color:var(--text-dim);display:block;margin-bottom:6px">
        How many to break down? (max ${{rec.store_qty}})
      </label>
      <div style="display:flex;gap:8px;align-items:center">
        <input type="number" id="exec-qty" value="1" min="1" max="${{rec.store_qty}}"
               style="width:80px" oninput="updateExecPreview(${{rec.store_qty}}, ${{bdValue}}, ${{rec.store_price}})">
        <div style="display:flex;gap:4px">
          ${{[1,2,5,10].filter(n=>n<=rec.store_qty).map(n=>
            `<button class="btn btn-secondary btn-sm" onclick="document.getElementById('exec-qty').value=${{n}};updateExecPreview(${{rec.store_qty}},${{bdValue}},${{rec.store_price}})">${{n}}</button>`
          ).join('')}}
          <button class="btn btn-secondary btn-sm" onclick="document.getElementById('exec-qty').value=${{rec.store_qty}};updateExecPreview(${{rec.store_qty}},${{bdValue}},${{rec.store_price}})">All</button>
        </div>
      </div>
      <div id="exec-preview" style="margin-top:8px;font-size:12px;color:var(--text-dim)"></div>
    </div>

    <div id="exec-result"></div>

    <div style="display:flex;gap:8px;margin-top:16px">
      <button class="btn btn-primary" id="exec-confirm-btn"
        onclick="confirmExecute(${{rec.shopify_variant_id}},${{rec.inventory_item_id}},${{rec.tcgplayer_id}},'${{variantId}}')">
        ✓ Confirm Breakdown
      </button>
      <button class="btn btn-secondary" onclick="document.getElementById('execute-modal').classList.remove('active')">Cancel</button>
    </div>
  `;
  updateExecPreview(rec.store_qty, bdValue, rec.store_price);
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
  const ok = await themedConfirm(
    '🚫 Ignore SKU',
    `Hide "${{name}}" from breakdown recommendations? You can unignore it anytime from the Ignored SKUs tab.`,
    {{ confirmText: 'Ignore', dangerous: true }}
  );
  if (!ok) return;
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
  _pendingListings = new Set();

  const body = document.getElementById('recipe-modal-body');
  document.getElementById('recipe-modal-title').textContent = productName || 'Breakdown Recipe';
  document.getElementById('recipe-modal').classList.add('active');

  // If no item selected yet, show inventory search picker first
  if (!tcgId) {{
    body.innerHTML = `
      <p style="color:var(--text-dim);font-size:13px;margin-bottom:12px">Search your store inventory to find the item you want to build a recipe for.</p>
      <div style="display:flex;gap:8px;margin-bottom:10px">
        <input type="text" id="re-picker-search" placeholder="Product name…" style="flex:1"
               onkeydown="if(event.key==='Enter') rePickerSearch()">
        <button class="btn btn-primary btn-sm" onclick="rePickerSearch()">Search</button>
      </div>
      <div id="re-picker-results" style="max-height:280px;overflow-y:auto"></div>`;
    setTimeout(() => document.getElementById('re-picker-search')?.focus(), 100);
    return;
  }}

  body.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
  let cache = null;
  try {{
    const r = await fetch(`/inventory/breakdown/api/cache/${{tcgId}}`);
    const d = await r.json();
    if (d.found) cache = d.cache;
  }} catch(e) {{}}
  fetchRecipeStorePrices();
  renderRecipeModal(cache);
}}

async function rePickerSearch() {{
  const q = document.getElementById('re-picker-search')?.value.trim().toLowerCase();
  const panel = document.getElementById('re-picker-results');
  if (!q) return;
  panel.innerHTML = '<div class="loading"><span class="spinner"></span></div>';
  try {{
    const r = await fetch('/inventory/breakdown/api/inventory-search?q=' + encodeURIComponent(q));
    const d = await r.json();
    const items = d.items || [];
    if (!items.length) {{ panel.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No items found.</p>'; return; }}
    window._rePickerItems = items;
    panel.innerHTML = items.slice(0,20).map((i,idx) => `
      <div class="search-result" onclick="rePickerSelect(${{idx}})">
        <strong style="font-size:13px">${{i.title}}</strong>
        <br><small style="color:var(--text-dim)">TCG#${{i.tcgplayer_id||'—'}} · qty ${{i.shopify_qty}} · $${{parseFloat(i.shopify_price||0).toFixed(2)}}</small>
      </div>`).join('');
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

async function rePickerSelect(idx) {{
  const item = (window._rePickerItems||[])[idx];
  if (!item) return;
  const tcgId = item.tcgplayer_id || null;
  const productName = item.name || item.title || '';
  _recipeTarget = {{ tcgId, productName }};
  document.getElementById('recipe-modal-title').textContent = productName;
  const body = document.getElementById('recipe-modal-body');
  body.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
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
              <button class="btn btn-primary btn-sm" onclick="loadVariantIntoEditor(${{JSON.stringify(v).replace(/\"/g,'&quot;')}})">↓ Edit</button>
              <button class="btn btn-sm" style="color:var(--red);border:1px solid var(--border);background:none"
                onclick="deleteVariant('${{v.id}}','${{v.variant_name.replace(/'/g,'').replace(/\"/g,'')}}')">🗑</button>
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
      <div style="display:flex;gap:8px;margin-bottom:12px;align-items:center;flex-wrap:wrap">
        <label style="font-size:12px;color:var(--text-dim)">Config name:</label>
        <input type="text" id="re-variant-name" value="Standard" style="width:160px">
        <span id="re-editing-badge" style="display:none;font-size:11px;color:var(--accent);background:var(--surface-2);padding:2px 8px;border-radius:4px">editing</span>
      </div>
      <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px;margin-bottom:14px">
        <div style="border:1px solid var(--border);border-radius:8px;padding:12px">
          <div style="font-size:11px;font-weight:600;color:var(--text-dim);text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px">📦 Sealed</div>
          <div style="display:flex;gap:6px;margin-bottom:6px">
            <input type="text" id="re-search" placeholder="Booster Box, ETB…"
                   style="flex:1;font-size:12px" onkeydown="if(event.key==='Enter') reSearch()">
            <button class="btn btn-primary btn-sm" onclick="reSearch()" style="height:30px;font-size:11px">Search</button>
          </div>
          <div id="re-search-results" style="max-height:130px;overflow-y:auto"></div>
        </div>
        <div style="border:1px solid rgba(79,125,249,0.4);border-radius:8px;padding:12px">
          <div style="font-size:11px;font-weight:600;color:var(--accent);text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px">✨ Promos <span style="color:var(--text-dim);font-weight:400;text-transform:none">(NM price)</span></div>
          <div style="display:flex;gap:6px;margin-bottom:6px">
            <input type="text" id="re-promo-search" placeholder="Pikachu Promo, 187/XY…"
                   style="flex:1;font-size:12px" onkeydown="if(event.key==='Enter') rePromoSearch()">
            <button class="btn btn-sm" onclick="rePromoSearch()" style="height:30px;font-size:11px;border-color:var(--accent);color:var(--accent)">Search</button>
          </div>
          <div id="re-promo-results" style="max-height:130px;overflow-y:auto"></div>
        </div>
      </div>
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
    component_type: c.component_type || 'sealed',
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
  const ok = await themedConfirm('Delete Config', `Remove the "${{name}}" config?`, {{ dangerous: true }});
  if (!ok) return;
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
    if (r.status === 429 || (d.error && !d.results?.length)) {{
      panel.innerHTML = `<div class="alert alert-warning" style="font-size:12px">⚠ ${{d.error || 'Rate limited — try again shortly.'}}</div>`;
      return;
    }}
    if (!r.ok) {{ panel.innerHTML = `<div class="alert alert-error">${{d.error}}</div>`; return; }}
    const results = d.results || [];
    if (!results.length) {{ panel.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No results</p>'; return; }}
    window._reSearchResults = {{}};
    panel.innerHTML = results.slice(0,5).map((p, idx) => {{
      const name = p.name||p.productName||'';
      const setName = p.setName||p.set_name||'';
      const tcgId = p.tcgplayer_id||p.tcgplayerId||p.tcgPlayerId||p.id||0;
      const price = p.unopenedPrice||p.marketPrice||0;
      window._reSearchResults[idx] = {{ tcgId, name, setName, price, component_type: 'sealed' }};
      return `<div class="search-result" onclick="reAddByIdx(${{idx}})">
        <strong style="font-size:13px">${{name}}</strong>
        <br><small style="color:var(--text-dim)">${{setName}} · $${{(price||0).toFixed(2)}}</small>
      </div>`;
    }}).join('');
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

async function rePromoSearch() {{
  const q = document.getElementById('re-promo-search')?.value.trim();
  if (!q) return;
  const panel = document.getElementById('re-promo-results');
  panel.innerHTML = '<div class="loading"><span class="spinner"></span></div>';
  try {{
    const r = await fetch(`/inventory/breakdown/api/cache/search-cards?q=${{encodeURIComponent(q)}}`);
    const d = await r.json();
    if (r.status === 429 || (d.error && !d.results?.length)) {{
      panel.innerHTML = `<div class="alert alert-warning" style="font-size:12px">⚠ ${{d.error || 'Rate limited — try again shortly.'}}</div>`;
      return;
    }}
    if (!r.ok) {{ panel.innerHTML = `<div class="alert alert-error">${{d.error}}</div>`; return; }}
    const results = d.results || [];
    if (!results.length) {{ panel.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No results</p>'; return; }}
    window._rePromoResults = {{}};
    panel.innerHTML = results.slice(0,5).map((p, idx) => {{
      const name = p.name||p.productName||'';
      const setName = p.setName||p.set_name||'';
      const tcgId = p.tcgPlayerId||p.tcgplayer_id||p.id||0;
      const nmCond = (p.prices?.conditions||{{}})['Near Mint'] || {{}};
      const price = parseFloat(p.market_price) || parseFloat(nmCond.price) || parseFloat(p.prices?.market) || 0;
      window._rePromoResults[idx] = {{ tcgId, name, setName, price, component_type: 'promo' }};
      return `<div class="search-result" onclick="reAddPromoByIdx(${{idx}})">
        <strong style="font-size:13px">${{name}}</strong>
        <br><small style="color:var(--text-dim)">${{setName}} · NM $${{price.toFixed(2)}}</small>
      </div>`;
    }}).join('');
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

function reAddByIdx(idx) {{
  const r = (window._reSearchResults||{{}})[idx];
  if (r) reAdd(r.tcgId, r.name, r.setName, r.price, 'sealed');
}}

function reAddPromoByIdx(idx) {{
  const r = (window._rePromoResults||{{}})[idx];
  if (r) reAdd(r.tcgId, r.name, r.setName, r.price, 'promo');
}}

function reAdd(tcgId, name, setName, price, componentType) {{
  const existing = _recipeComponents.find(c => c.tcgplayer_id && c.tcgplayer_id === tcgId && c.component_type === componentType);
  if (existing) {{ existing.quantity += 1; }}
  else {{ _recipeComponents.push({{ product_name: name, set_name: setName, tcgplayer_id: tcgId, market_price: price, quantity: 1, component_type: componentType || 'sealed' }}); }}
  reRenderComponents();
  fetchRecipeStorePrices();
  const resultEl = componentType === 'promo' ? 're-promo-results' : 're-search-results';
  const inputEl  = componentType === 'promo' ? 're-promo-search' : 're-search';
  document.getElementById(resultEl).innerHTML = '';
  document.getElementById(inputEl).value = '';
  document.getElementById(inputEl).focus();
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

  const sealedComps = _recipeComponents.filter(c => c.component_type !== 'promo');
  const promoComps  = _recipeComponents.filter(c => c.component_type === 'promo');
  const totalMkt = _recipeComponents.reduce((s,c) => s+(parseFloat(c.market_price)||0)*(parseInt(c.quantity)||1), 0);
  const storeComps = _recipeComponents.filter(c => c.tcgplayer_id && _storePrices[c.tcgplayer_id]);
  const totalStore = storeComps.reduce((s,c) => s+(_storePrices[c.tcgplayer_id].shopify_price||0)*(parseInt(c.quantity)||1), 0);

  if (summary) {{
    const parentId = _recipeTarget?.tcgId;
    const parentEntry = parentId ? _storePrices[String(parentId)] : null;
    const parentStore = parentEntry ? parseFloat(parentEntry.shopify_price)||0 : 0;
    let sh = _recipeComponents.length ? `Market: <strong>$${{totalMkt.toFixed(2)}}</strong>` : '';
    if (promoComps.length) {{
      const promoMkt = promoComps.reduce((s,c) => s+(parseFloat(c.market_price)||0)*(parseInt(c.quantity)||1), 0);
      sh += ` <span style="color:var(--accent);font-size:11px">(incl. $${{promoMkt.toFixed(2)}} promos)</span>`;
    }}
    if (totalStore > 0) {{
      const diff = parentStore > 0 ? totalStore - parentStore : null;
      const col = diff !== null ? (diff >= 0 ? 'var(--green)' : diff >= -parentStore*0.1 ? 'var(--amber)' : 'var(--red)') : 'var(--text-dim)';
      const diffStr = diff !== null ? ` <small style="color:${{diff>=0?'var(--green)':'var(--red)'}}">  ${{diff>=0?'+':''}}$${{diff.toFixed(2)}} vs store</small>` : '';
      sh += ` · <span style="color:${{col}}">Store: <strong>$${{totalStore.toFixed(2)}}</strong>${{diffStr}}</span>`;
    }}
    summary.innerHTML = sh;
  }}

  if (!_recipeComponents.length) {{
    el.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No components yet.</p>';
    return;
  }}

  const hasStore = Object.keys(_storePrices).length > 0;
  const renderRows = (comps) => comps.map((c) => {{
    const realIdx = _recipeComponents.indexOf(c);
    const sp = c.tcgplayer_id ? _storePrices[String(c.tcgplayer_id)] : null;
    const isPromo = c.component_type === 'promo';
    const storeCell = hasStore
      ? `<td>${{sp
          ? `<span style="color:var(--green)">$${{parseFloat(sp.shopify_price).toFixed(2)}}${{sp.shopify_qty===0?' <small style="color:var(--red)">qty 0</small>':''}}</span>`
          : (_pendingListings.has(String(c.tcgplayer_id))
              ? '<span style="color:var(--green);font-size:11px">✓ Draft</span>'
              : (c.tcgplayer_id && !isPromo
                  ? `<button class="btn btn-sm btn-primary" style="font-size:11px;padding:1px 5px;" onclick="reCreateListing(${{c.tcgplayer_id}},'component',this)">+ List</button>`
                  : '<span style="color:var(--text-dim)">—</span>'))}}</td>`
      : '';
    return `<tr>
      <td>
        ${{isPromo ? '<span style="font-size:10px;background:rgba(79,125,249,0.15);color:var(--accent);padding:1px 5px;border-radius:3px;margin-right:4px">PROMO</span>' : ''}}
        ${{c.product_name}}<br>
        <small style="color:${{c.tcgplayer_id?'var(--text-dim)':'var(--red)'}};">${{c.tcgplayer_id?'TCG#'+c.tcgplayer_id:'⚠ No TCG ID'}}${{c.set_name?' · '+c.set_name:''}}</small>
      </td>
      <td><input type="number" value="${{c.quantity}}" min="1" style="width:55px" onchange="_recipeComponents[${{realIdx}}].quantity=parseInt(this.value)||1;reRenderComponents()"></td>
      <td><input type="number" value="${{(c.market_price||0).toFixed(2)}}" min="0" step="0.01" style="width:80px" onchange="_recipeComponents[${{realIdx}}].market_price=parseFloat(this.value)||0;reRenderComponents()"></td>
      ${{storeCell}}
      <td><button class="btn btn-sm" style="color:var(--red);font-size:11px;padding:2px 6px;border:1px solid var(--border);background:none"
        onclick="_recipeComponents.splice(${{realIdx}},1);_storePrices={{}};reRenderComponents()">✕</button></td>
    </tr>`;
  }}).join('');

  let html = `<div style="overflow-x:auto"><table style="font-size:13px"><thead><tr>
    <th>Component</th><th style="width:70px">Qty</th><th style="width:90px">Market</th>
    ${{hasStore ? '<th style="width:90px;color:var(--accent)">Store</th>' : ''}}
    <th style="width:30px"></th>
  </tr></thead><tbody>`;
  if (sealedComps.length) {{
    if (promoComps.length) html += `<tr><td colspan="5" style="font-size:11px;color:var(--text-dim);padding:4px 0;font-weight:600;text-transform:uppercase;letter-spacing:.05em">📦 Sealed</td></tr>`;
    html += renderRows(sealedComps);
  }}
  if (promoComps.length) {{
    html += `<tr><td colspan="5" style="font-size:11px;color:var(--accent);padding:4px 0;font-weight:600;text-transform:uppercase;letter-spacing:.05em;border-top:1px solid var(--border)">✨ Promos</td></tr>`;
    html += renderRows(promoComps);
  }}
  html += '</tbody></table></div>';
  el.innerHTML = html;
}}

async function reCreateListing(tcgId, context, btn) {{
  if (!tcgId) {{ alert('No TCGPlayer ID'); return; }}
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = '⟳';
  btn.title = 'Creating draft listing... ~30-60s';
  try {{
    const r = await fetch('/inventory/api/enrich/create-listing', {{
      method: 'POST', headers: {{'Content-Type':'application/json'}},
      body: JSON.stringify({{ tcgplayer_id: tcgId, quantity: 0 }}),
    }});
    const d = await r.json();
    if (!r.ok) {{
      btn.disabled = false;
      btn.textContent = origText;
      alert('Failed: ' + (d.error || 'Unknown'));
      return;
    }}
    // Track locally so re-renders don't flip back to "+ List" before cache syncs
    _pendingListings.add(String(tcgId));
    // Refresh store prices (will re-render, but _pendingListings keeps ✓ Draft visible)
    _storePrices = {{}};
    fetchRecipeStorePrices();
  }} catch(e) {{
    btn.disabled = false;
    btn.textContent = origText;
    alert('Error: ' + e.message);
  }}
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
    // Refresh both recs and no-recipe list (remove item that now has a recipe)
    const currentTab = location.hash.replace('#','') || 'recommendations';
    loadRecommendations();
    // Remove from no-recipe list immediately so it's gone when modal closes
    _noRecipeItems = _noRecipeItems.filter(i => i.tcgplayer_id !== _recipeTarget?.tcgId);
    if (currentTab === 'norecipe') renderNoRecipe();
  }} catch(e) {{ alert(e.message); }}
}}

// ══════════════════════════════════════════════════════════════════
// NO RECIPE TAB
// ══════════════════════════════════════════════════════════════════
let _noRecipeItems = [];

async function loadNoRecipe() {{
  const panel = document.getElementById('norecipe-panel');
  panel.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
  try {{
    const r = await fetch('/inventory/breakdown/api/no-recipe');
    const d = await r.json();
    _noRecipeItems = d.items || [];
    renderNoRecipe();
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

function renderNoRecipe() {{
  const panel = document.getElementById('norecipe-panel');
  const q = (document.getElementById('nr-search')?.value || '').toLowerCase();
  const hideBase = document.getElementById('nr-hide-base')?.checked;

  let items = _noRecipeItems;
  if (q) items = items.filter(i => (i.title||'').toLowerCase().includes(q));
  if (hideBase) items = items.filter(i => !i.is_base);

  const total = items.length;
  const baseCount = _noRecipeItems.filter(i => i.is_base).length;

  if (!items.length) {{
    panel.innerHTML = `<div class="card" style="color:var(--text-dim);text-align:center;padding:32px">
      ${{q ? '🔍 No items match that filter.' : '✓ All store items have recipes or are marked as base components.'}}
    </div>`;
    return;
  }}

  panel.innerHTML = `
    <div style="font-size:12px;color:var(--text-dim);margin-bottom:8px">
      ${{total}} item${{total!==1?'s':''}} without recipes · ${{baseCount}} base component${{baseCount!==1?'s':''}}
    </div>
    <div style="overflow-x:auto"><table>
      <thead><tr>
        <th>Product</th><th>Qty</th><th>Price</th><th>Status</th><th style="width:180px"></th>
      </tr></thead>
      <tbody>
      ${{items.map(i => `<tr style="${{i.is_base ? 'opacity:0.5' : ''}}">
        <td>
          <strong>${{i.title}}</strong>
          <br><small style="color:var(--text-dim)">TCG#${{i.tcgplayer_id}}</small>
          ${{i.is_base ? '<br><span class="badge badge-neutral" style="font-size:10px">base component</span>' : ''}}
        </td>
        <td style="color:${{i.shopify_qty>0?'var(--green)':'var(--red)'}};font-weight:600">${{i.shopify_qty}}</td>
        <td>$${{parseFloat(i.shopify_price||0).toFixed(2)}}</td>
        <td><span class="badge ${{i.shopify_qty>0?'badge-green':'badge-red'}}">${{i.shopify_qty>0?'In Stock':'Out'}}</span></td>
        <td>
          <div style="display:flex;gap:4px;flex-wrap:wrap">
            <button class="btn btn-primary btn-sm" onclick="nrOpenRecipe(${{i.tcgplayer_id}})">
              + Recipe
            </button>
            ${{i.is_base
              ? `<button class="btn btn-secondary btn-sm" onclick="unmarkBase(${{i.tcgplayer_id}},this)">Unmark Base</button>`
              : `<button class="btn btn-sm" style="color:var(--text-dim);border:1px solid var(--border);background:none" onclick="nrMarkBase(${{i.tcgplayer_id}},this)" title="Mark as base component — won't show here">📌 Base</button>`
            }}
          </div>
        </td>
      </tr>`).join('')}}
      </tbody>
    </table></div>`;
}}

function nrOpenRecipe(tcgId) {{
  const item = _noRecipeItems.find(i => i.tcgplayer_id === tcgId);
  if (!item) return;
  openRecipeEditor(item.tcgplayer_id || null, item.title || '');
}}

async function nrMarkBase(tcgId, btn) {{
  const item = _noRecipeItems.find(i => i.tcgplayer_id === tcgId);
  if (!item) return;
  await markBase(item.tcgplayer_id, item.title, btn);
}}

async function markBase(tcgId, name, btn) {{
  if (btn) btn.disabled = true;
  await fetch('/inventory/breakdown/api/base-component', {{
    method: 'POST', headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify({{ tcgplayer_id: tcgId, product_name: name }})
  }});
  const item = _noRecipeItems.find(i => i.tcgplayer_id === tcgId);
  if (item) item.is_base = true;
  renderNoRecipe();
}}

async function unmarkBase(tcgId, btn) {{
  btn.disabled = true;
  await fetch(`/inventory/breakdown/api/base-component/${{tcgId}}`, {{ method: 'DELETE' }});
  const item = _noRecipeItems.find(i => i.tcgplayer_id === tcgId);
  if (item) item.is_base = false;
  renderNoRecipe();
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
restoreTab();
</script>

<!-- Known Recipes tab -->
<script>
let _allKnownRecipes = [];

async function loadKnownRecipes() {{
  const panel = document.getElementById('known-recipes-panel');
  if (!panel) return;
  panel.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
  try {{
    const r = await fetch('/inventory/breakdown/api/all-recipes');
    const d = await r.json();
    _allKnownRecipes = d.recipes || [];
    renderKnownRecipes();
  }} catch(e) {{
    panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`;
  }}
}}

function renderKnownRecipes() {{
  const panel = document.getElementById('known-recipes-panel');
  const countEl = document.getElementById('kr-count');
  if (!panel) return;
  const q = (document.getElementById('kr-search')?.value || '').toLowerCase();
  let recs = _allKnownRecipes;
  if (q) recs = recs.filter(r => (r.product_name || '').toLowerCase().includes(q));
  if (countEl) countEl.textContent = `${{recs.length}} recipe${{recs.length !== 1 ? 's' : ''}}`;
  if (!recs.length) {{
    panel.innerHTML = `<div style="color:var(--text-dim);padding:20px;text-align:center">${{
      q ? '🔍 No recipes match that filter.' : '📭 No breakdown recipes saved yet.'
    }}</div>`;
    return;
  }}
  panel.innerHTML = `<div style="overflow-x:auto"><table>
    <thead><tr>
      <th>Product</th><th>TCG ID</th><th>Variants</th>
      <th>Best Value</th><th>Store Qty</th><th>Store Price</th><th>Status</th><th></th>
    </tr></thead>
    <tbody>
    ${{recs.map(r => {{
      const storeStatus = r.store_status ? r.store_status.toLowerCase() : '—';
      const statusBadge = storeStatus === 'active'
        ? '<span class="badge badge-green">ACTIVE</span>'
        : storeStatus === 'draft'
          ? '<span class="badge" style="background:#7c3aed;color:#fff;font-size:0.65rem;">DRAFT</span>'
          : '<span class="badge badge-dim" style="font-size:0.65rem;">NOT IN STORE</span>';
      const bdVal = r.best_variant_market ? `<span style="color:var(--green);font-weight:600">$${{parseFloat(r.best_variant_market).toFixed(2)}}</span>` : '—';
      const storeQty = r.store_qty != null ? `<span style="color:${{r.store_qty > 0 ? 'var(--green)' : 'var(--red)'}};font-weight:600">${{r.store_qty}}</span>` : '—';
      const storePrice = r.store_price ? `$${{parseFloat(r.store_price).toFixed(2)}}` : '—';
      const name = r.store_title || r.product_name || '—';
      return `<tr>
        <td><strong style="font-size:13px">${{name}}</strong></td>
        <td style="color:var(--text-dim);font-size:12px">${{r.tcgplayer_id || '—'}}</td>
        <td style="text-align:center">${{r.variant_count || 0}}</td>
        <td>${{bdVal}}</td>
        <td style="text-align:center">${{storeQty}}</td>
        <td>${{storePrice}}</td>
        <td>${{statusBadge}}</td>
        <td>
          <button class="btn btn-secondary btn-sm"
            onclick="openRecipeEditor(${{r.tcgplayer_id || 'null'}},'${{(r.store_title || r.product_name || '').replace(/'/g,'').replace(/"/g,'')}}')">
            ✎ Edit
          </button>
        </td>
      </tr>`;
    }}).join('')}}
    </tbody></table></div>`;
}}
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
