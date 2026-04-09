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

def _build_recommendations(in_stock_only=True):
    """
    Join inventory_product_cache with sealed_breakdown_cache to find items
    that have a saved recipe. Enrich with:
      - parent store price / qty
      - best breakdown variant value
      - children store qty (low-stock signal)
      - ignore flag

    Returns list of dicts sorted by desirability score.
    """
    if in_stock_only:
        inventory = db.query("""
            SELECT
                c.shopify_product_id, c.shopify_variant_id, c.title,
                c.shopify_price, c.shopify_qty, c.inventory_item_id,
                c.tcgplayer_id, c.status
            FROM inventory_product_cache c
            WHERE c.tcgplayer_id IS NOT NULL
              AND c.is_damaged = FALSE
              AND c.shopify_qty > 0
              AND c.tcgplayer_id NOT IN (SELECT tcgplayer_id FROM breakdown_ignore)
            ORDER BY c.title
        """)
    else:
        inventory = db.query("""
            SELECT
                c.shopify_product_id, c.shopify_variant_id, c.title,
                c.shopify_price, c.shopify_qty, c.inventory_item_id,
                c.tcgplayer_id, c.status
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

        # JIT refresh stale component market prices
        # Use 24h staleness for recommendations (advisory view) to avoid PPT rate limits.
        # Actual breakdown execution uses 4h threshold.
        try:
            from breakdown_helpers import refresh_stale_component_prices
            from routes.inventory import _get_ppt_client
            _ppt = _get_ppt_client()
            if _ppt:
                refresh_stale_component_prices(variant_ids, db, _ppt, max_age_hours=24)
        except Exception as e:
            logger.warning(f"Component price refresh skipped: {e}")
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

        # Load ALL variants' components for deep value (not just the best variant)
        all_variant_comps = db.query(f"""
            SELECT sbcomp.tcgplayer_id AS comp_tcg_id, sbcomp.quantity_per_parent,
                   sbcomp.market_price AS comp_market, sbv.id AS variant_id,
                   sbc.tcgplayer_id AS parent_tcg_id
            FROM sealed_breakdown_components sbcomp
            JOIN sealed_breakdown_variants sbv ON sbv.id = sbcomp.variant_id
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
            WHERE sbc.tcgplayer_id IN ({ph}) AND sbcomp.tcgplayer_id IS NOT NULL
        """, tuple(tcg_ids))
        all_comp_tcg_ids = list(set(
            [int(c["component_tcg_id"]) for c in components if c["component_tcg_id"]] +
            [int(c["comp_tcg_id"]) for c in all_variant_comps if c["comp_tcg_id"]]
        ))

        # Nested breakdown lookup: which components have their own recipes?
        child_bd_map = {}       # market-based
        child_bd_store_map = {} # store-based
        if all_comp_tcg_ids:
            cph2 = ",".join(["%s"] * len(all_comp_tcg_ids))
            child_bd_rows = db.query(f"""
                SELECT tcgplayer_id, best_variant_market
                FROM sealed_breakdown_cache
                WHERE tcgplayer_id IN ({cph2})
            """, tuple(all_comp_tcg_ids))
            child_bd_map = {int(r["tcgplayer_id"]): float(r["best_variant_market"] or 0) for r in child_bd_rows}

            # Compute store-based BD value for children with recipes (grandchild store prices)
            if child_bd_map:
                try:
                    child_tcg_list = list(child_bd_map.keys())
                    gcph = ",".join(["%s"] * len(child_tcg_list))
                    gc_rows = db.query(f"""
                        SELECT sbc.tcgplayer_id AS child_tcg_id,
                               sbco.tcgplayer_id AS gc_tcg_id,
                               sbco.quantity_per_parent
                        FROM sealed_breakdown_cache sbc
                        JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                            AND sbv.total_component_market = sbc.best_variant_market
                        LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                        WHERE sbc.tcgplayer_id IN ({gcph}) AND sbco.tcgplayer_id IS NOT NULL
                    """, tuple(child_tcg_list))
                    gc_ids = list(set(r["gc_tcg_id"] for r in gc_rows if r["gc_tcg_id"]))
                    gc_store = {}
                    if gc_ids:
                        gcp = ",".join(["%s"] * len(gc_ids))
                        gc_sp = db.query(
                            f"SELECT tcgplayer_id, shopify_price FROM inventory_product_cache WHERE tcgplayer_id IN ({gcp}) AND is_damaged = FALSE",
                            tuple(gc_ids))
                        gc_store = {r["tcgplayer_id"]: float(r["shopify_price"] or 0) for r in gc_sp}
                    _gc_by_child = {}
                    for r in gc_rows:
                        _gc_by_child.setdefault(r["child_tcg_id"], []).append(r)
                    for ctid, gcs in _gc_by_child.items():
                        sv = 0.0
                        all_have = True
                        for gc in gcs:
                            sp = gc_store.get(gc["gc_tcg_id"], 0)
                            if sp > 0:
                                sv += sp * (gc["quantity_per_parent"] or 1)
                            else:
                                all_have = False
                        if all_have and sv > 0:
                            child_bd_store_map[ctid] = sv
                except Exception:
                    pass

        # Map variant_id → list of component tcg_ids
        variant_comp_map = {}
        for c in components:
            vid = str(c["variant_id"])
            if vid not in variant_comp_map:
                variant_comp_map[vid] = []
            if c["component_tcg_id"]:
                variant_comp_map[vid].append(int(c["component_tcg_id"]))
        # Pre-build quantity_per_parent lookup by variant (eliminates per-item queries)
        comp_qty_by_variant = {}
        for avc in all_variant_comps:
            _vid = str(avc["variant_id"])
            _cid = avc["comp_tcg_id"]
            if _cid:
                comp_qty_by_variant.setdefault(_vid, {})[int(_cid)] = int(avc["quantity_per_parent"] or 1)
    else:
        child_qty_map = {}
        variant_comp_map = {}
        child_bd_map = {}
        comp_qty_by_variant = {}

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

        # quantity_per_parent lookup from pre-built dict (no per-item DB query)
        comp_qty_map = comp_qty_by_variant.get(vid, {})

        # Compute store-based bd value using per-component qtys from recipe
        bd_value_store = 0.0
        # Only use store prices if ALL children are present in the store
        if child_store_vals and len(child_store_vals) == len(child_tcg_ids):
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
            qty_per_parent = comp_qty_map.get(cid, 1)
            child_bd_val = child_bd_map.get(cid, 0)
            comp_details.append({
                "tcgplayer_id":    cid,
                "title":           info.get("title", f"TCG#{cid}"),
                "shopify_qty":     int(info.get("shopify_qty") or 0) if info else None,
                "shopify_price":   float(info.get("shopify_price") or 0) if info else None,
                "qty_per_parent":  qty_per_parent,
                "in_store":        bool(info),
                "has_breakdown":   child_bd_val > 0,
                "child_bd_value":  round(child_bd_val, 2) if child_bd_val > 0 else None,
            })

        # Compute store-based deep value across ALL variants
        best_deep_value = 0.0
        _parent_var_comps = {}
        for avc in all_variant_comps:
            if int(avc["parent_tcg_id"]) == tid:
                _parent_var_comps.setdefault(str(avc["variant_id"]), []).append(avc)
        for _pvid, _pvcomps in _parent_var_comps.items():
            dv = 0.0
            dv_has_deep = False
            for vc in _pvcomps:
                cid = int(vc["comp_tcg_id"])
                qty = vc["quantity_per_parent"] or 1
                # Prefer store-based child BD value, fallback to store price, then market
                cbd_store = child_bd_store_map.get(cid, 0)
                if cbd_store > 0:
                    dv += cbd_store * qty
                    dv_has_deep = True  # this child has its own recipe
                else:
                    ci = child_qty_map.get(cid, {})
                    sp = float(ci.get("shopify_price") or 0) if ci else 0
                    if sp > 0:
                        dv += sp * qty
                    else:
                        dv += float(vc["comp_market"] or 0) * qty
            if dv_has_deep and dv > best_deep_value:
                best_deep_value = dv

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
            "deep_bd_value":      round(best_deep_value, 2) if best_deep_value > 0 else None,
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
        in_stock = request.args.get("in_stock", "true").lower() != "false"
        recs = _build_recommendations(in_stock_only=in_stock)
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

    # Fetch components (include market_price for COGS allocation)
    components = db.query("""
        SELECT sbc.tcgplayer_id, sbc.product_name, sbc.quantity_per_parent,
               sbc.market_price AS comp_market_price,
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

    # Fetch parent COGS for redistribution to children
    parent_unit_cost = None
    try:
        parent_cost_data = sc.get_inventory_item_cost_and_qty(str(parent_inv_item_id))
        parent_unit_cost = parent_cost_data[0]  # unit cost per parent
    except Exception as e:
        logger.warning(f"Could not fetch parent COGS for redistribution: {e}")

    # Pre-compute each child's share of parent COGS based on market values
    child_cogs = {}  # child_variant_id -> unit cost to set
    if parent_unit_cost and parent_unit_cost > 0:
        total_comp_market = sum(
            float(c.get("comp_market_price") or 0) * int(c["quantity_per_parent"])
            for c in components
        )
        if total_comp_market > 0:
            for comp in components:
                comp_market = float(comp.get("comp_market_price") or 0)
                comp_qty = int(comp["quantity_per_parent"])
                share = (comp_market * comp_qty) / total_comp_market
                # COGS per child unit = (parent_cost * share) / qty_per_parent
                child_unit_cost = (parent_unit_cost * share) / max(comp_qty, 1)
                if comp.get("child_variant_id"):
                    child_cogs[comp["child_variant_id"]] = round(child_unit_cost, 2)

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
                # Update COGS: weighted average of existing cost + parent's allocated cost
                if child_vid in child_cogs:
                    try:
                        existing_cost, existing_qty = sc.get_inventory_item_cost_and_qty(str(child_inv_id))
                        our_cost = child_cogs[child_vid]
                        if not existing_cost or existing_qty <= 0:
                            new_cost = our_cost
                        else:
                            new_cost = (existing_cost * existing_qty + our_cost * add_qty) / (existing_qty + add_qty)
                        sc.set_unit_cost(str(child_inv_id), new_cost)
                    except Exception as e:
                        logger.warning(f"Could not update COGS for child {child_vid}: {e}")

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


# Cache CRUD routes (get, save variant, delete variant, search, store-prices)
# now served by shared breakdown blueprint registered in app.py








@bp.route("/api/variant-values/<int:tcg_id>")
@requires_auth
def variant_values(tcg_id):
    """Compute store-based and deep store-based BD value for every variant of a product."""
    cache_row = db.query_one("SELECT id FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcg_id,))
    if not cache_row:
        return jsonify({"variants": {}})

    variants = db.query(
        "SELECT id, total_component_market FROM sealed_breakdown_variants WHERE breakdown_id=%s",
        (cache_row["id"],)
    )
    # Load all components across all variants
    var_ids = [v["id"] for v in variants]
    if not var_ids:
        return jsonify({"variants": {}})
    vph = ",".join(["%s"] * len(var_ids))
    all_comps = db.query(f"""
        SELECT variant_id, tcgplayer_id, quantity_per_parent, market_price
        FROM sealed_breakdown_components WHERE variant_id IN ({vph}) AND tcgplayer_id IS NOT NULL
    """, tuple(var_ids))

    # Fetch store prices for all components
    comp_ids = list({int(c["tcgplayer_id"]) for c in all_comps if c["tcgplayer_id"]})
    store_map = {}
    if comp_ids:
        cph = ",".join(["%s"] * len(comp_ids))
        rows = db.query(
            f"SELECT tcgplayer_id, shopify_price FROM inventory_product_cache WHERE tcgplayer_id IN ({cph}) AND is_damaged = FALSE",
            tuple(comp_ids)
        )
        store_map = {int(r["tcgplayer_id"]): float(r["shopify_price"] or 0) for r in rows}

    # Check which components have their own breakdown recipes (for deep value)
    child_bd_map = {}
    if comp_ids:
        cph = ",".join(["%s"] * len(comp_ids))
        bd_rows = db.query(f"SELECT tcgplayer_id, best_variant_market FROM sealed_breakdown_cache WHERE tcgplayer_id IN ({cph})", tuple(comp_ids))
        child_bd_map = {int(r["tcgplayer_id"]): float(r["best_variant_market"] or 0) for r in bd_rows}

    # Compute grandchild store prices for deep value
    child_bd_store_map = {}
    if child_bd_map:
        child_tcg_list = list(child_bd_map.keys())
        gcph = ",".join(["%s"] * len(child_tcg_list))
        try:
            gc_rows = db.query(f"""
                SELECT sbc.tcgplayer_id AS child_tcg_id,
                       sbco.tcgplayer_id AS gc_tcg_id,
                       sbco.quantity_per_parent
                FROM sealed_breakdown_cache sbc
                JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                    AND sbv.total_component_market = sbc.best_variant_market
                LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                WHERE sbc.tcgplayer_id IN ({gcph}) AND sbco.tcgplayer_id IS NOT NULL
            """, tuple(child_tcg_list))
            gc_ids = list(set(r["gc_tcg_id"] for r in gc_rows if r["gc_tcg_id"]))
            gc_store = {}
            if gc_ids:
                gcp = ",".join(["%s"] * len(gc_ids))
                gc_sp = db.query(
                    f"SELECT tcgplayer_id, shopify_price FROM inventory_product_cache WHERE tcgplayer_id IN ({gcp}) AND is_damaged = FALSE",
                    tuple(gc_ids))
                gc_store = {int(r["tcgplayer_id"]): float(r["shopify_price"] or 0) for r in gc_sp}
            _gc_by_child = {}
            for r in gc_rows:
                _gc_by_child.setdefault(int(r["child_tcg_id"]), []).append(r)
            for ctid, gcs in _gc_by_child.items():
                sv = 0.0
                all_have = True
                for gc in gcs:
                    sp = gc_store.get(int(gc["gc_tcg_id"]), 0)
                    if sp > 0:
                        sv += sp * (gc["quantity_per_parent"] or 1)
                    else:
                        all_have = False
                if all_have and sv > 0:
                    child_bd_store_map[ctid] = sv
        except Exception:
            pass

    # Group components by variant
    comps_by_var = {}
    for c in all_comps:
        comps_by_var.setdefault(c["variant_id"], []).append(c)

    result = {}
    for v in variants:
        vid = v["id"]
        comps = comps_by_var.get(vid, [])
        mkt_total = float(v["total_component_market"] or 0)

        # Store-based BD value
        store_total = 0.0
        all_have_store = len(comps) > 0
        for c in comps:
            cid = int(c["tcgplayer_id"])
            sp = store_map.get(cid, 0)
            qty = int(c["quantity_per_parent"] or 1)
            if sp > 0:
                store_total += sp * qty
            else:
                all_have_store = False

        # Deep store value: prefer grandchild store BD, fallback to store price, then market
        deep_val = 0.0
        has_deep = False
        for c in comps:
            cid = int(c["tcgplayer_id"])
            qty = int(c["quantity_per_parent"] or 1)
            cbd_store = child_bd_store_map.get(cid, 0)
            if cbd_store > 0:
                deep_val += cbd_store * qty
                has_deep = True
            else:
                sp = store_map.get(cid, 0)
                if sp > 0:
                    deep_val += sp * qty
                else:
                    deep_val += float(c["market_price"] or 0) * qty

        result[str(vid)] = {
            "store_value": round(store_total, 2) if all_have_store else None,
            "market_value": round(mkt_total, 2),
            "deep_store_value": round(deep_val, 2) if has_deep else None,
        }

    return jsonify({"variants": result})


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


@bp.route("/api/component-search")
@requires_auth
def component_search():
    """Search breakdown components by name and return parent products that contain them."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"results": []})
    rows = db.query("""
        SELECT sbc.tcgplayer_id AS parent_tcg_id,
               sbc.product_name AS parent_name,
               sbcomp.product_name AS component_name,
               sbcomp.quantity_per_parent,
               sbcomp.market_price AS component_market_price,
               sbv.variant_name,
               sbv.total_component_market,
               ipc.shopify_qty AS parent_store_qty,
               ipc.shopify_price AS parent_store_price,
               ipc.shopify_variant_id AS parent_variant_id,
               ipc.inventory_item_id AS parent_inv_item_id,
               ipc.status AS parent_status
        FROM sealed_breakdown_components sbcomp
        JOIN sealed_breakdown_variants sbv ON sbv.id = sbcomp.variant_id
        JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
        LEFT JOIN inventory_product_cache ipc ON ipc.tcgplayer_id = sbc.tcgplayer_id
          AND ipc.is_damaged = FALSE
        WHERE LOWER(sbcomp.product_name) LIKE %s
        ORDER BY sbc.product_name, sbcomp.quantity_per_parent DESC
    """, (f"%{q.lower()}%",))

    results = []
    for r in rows:
        parent_qty = int(r["parent_store_qty"] or 0)
        qty_per = int(r["quantity_per_parent"] or 1)
        parent_price = float(r["parent_store_price"] or 0)
        bd_value = float(r["total_component_market"] or 0)
        comp_price = float(r["component_market_price"] or 0)
        results.append({
            "parent_tcg_id": r["parent_tcg_id"],
            "parent_name": r["parent_name"],
            "component_name": r["component_name"],
            "quantity_per_parent": qty_per,
            "component_market_price": round(comp_price, 2),
            "component_total_value": round(comp_price * qty_per, 2),
            "variant_name": r["variant_name"],
            "bd_value": round(bd_value, 2),
            "parent_store_qty": parent_qty,
            "parent_store_price": round(parent_price, 2),
            "parent_variant_id": r["parent_variant_id"],
            "parent_inv_item_id": r["parent_inv_item_id"],
            "parent_status": r["parent_status"],
            "total_available": parent_qty * qty_per,
            "bd_pl": round(bd_value - parent_price, 2) if parent_price > 0 else None,
            "bd_pl_pct": round((bd_value - parent_price) / parent_price * 100, 1) if parent_price > 0 else None,
        })
    return jsonify({"results": results})


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
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Breakdown — PackFresh Inventory</title>
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.container{{max-width:1300px;margin:0 auto;padding:20px}}
.nav{{display:flex;align-items:center;gap:16px;padding:12px 20px;background:var(--surface);border-bottom:1px solid var(--border);margin-bottom:0}}
.nav a{{color:var(--text-dim);font-size:13px}} .nav a:hover{{color:var(--text)}} .nav .active{{color:var(--accent)}}
.tabs{{display:flex;gap:4px;border-bottom:1px solid var(--border);margin-bottom:20px}}
.tab-btn{{background:none;border:none;color:var(--text-dim);padding:8px 16px;cursor:pointer;font-size:14px;border-bottom:2px solid transparent;margin-bottom:-1px}}
.tab-btn.active{{color:var(--accent);border-bottom-color:var(--accent)}}
.tab-pane{{display:none}} .tab-pane.active{{display:block}}
th{{font-size:12px;font-weight:500}}
td{{font-size:13px;vertical-align:top}}
tr:last-child td{{border-bottom:none}}
.btn-primary{{background:var(--accent);color:#fff}} .btn-primary:hover{{opacity:.9}}
.btn-secondary{{background:var(--surface-2);color:var(--text);border:1px solid var(--border)}} .btn-secondary:hover{{background:var(--border)}}
.btn-success{{background:#16a34a;color:#fff}} .btn-danger{{background:var(--red);color:#fff}}
.badge-neutral{{background:var(--surface-2);color:var(--text-dim)}}
.score-bar{{height:4px;border-radius:2px;background:var(--border);width:100%;margin-top:4px}}
.score-fill{{height:4px;border-radius:2px;background:var(--green)}}
input[type=text],input[type=number],textarea,select{{
  background:var(--surface-2);border:1px solid var(--border);color:var(--text);
  border-radius:6px;padding:6px 10px;font-size:13px;width:100%
}}
.search-result{{padding:6px 10px;border-bottom:1px solid var(--border);cursor:pointer;border-radius:4px}}
.search-result:hover{{background:var(--surface-2)}}
.loading{{display:flex;align-items:center;gap:8px;color:var(--text-dim);padding:12px}}
.spinner{{width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
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
<link rel="stylesheet" href="/inventory/breakdown/api/cache/bd-static/breakdown_modal.css">
</head>
<body>
<script src="/inventory/breakdown/api/cache/bd-static/breakdown_modal.js"></script>

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
    <button class="tab-btn" onclick="switchTab('compsearch',this)">🔎 Find Components</button>
  </div>

  <!-- ═══ RECOMMENDATIONS TAB ════════════════════════════════════════════ -->
  <div id="tab-recommendations" class="tab-pane active">
    <div class="card" style="padding:10px 16px; margin-bottom:12px">
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <div style="display:flex;gap:6px;align-items:center">
          <label style="font-size:12px;color:var(--text-dim)">Show:</label>
          <select id="rec-filter" style="width:auto" onchange="saveFilter('bd_rec_filter',this.value);renderRecommendations()">
            <option value="all">All with recipes</option>
            <option value="positive">Positive delta only</option>
            <option value="neutral">Neutral or better (≥−10%)</option>
          </select>
        </div>
        <div style="display:flex;gap:6px;align-items:center">
          <label style="font-size:12px;color:var(--text-dim)">Sort:</label>
          <select id="rec-sort" style="width:auto" onchange="saveFilter('bd_rec_sort',this.value);renderRecommendations()">
            <option value="score">Score (delta + low-stock)</option>
            <option value="delta">Value delta %</option>
            <option value="child_qty">Child stock (low first)</option>
            <option value="store_qty">Parent qty (high first)</option>
          </select>
        </div>
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="rec-in-stock" checked onchange="saveFilter('bd_rec_in_stock',this.checked);loadRecommendations()" style="width:15px;height:15px;accent-color:var(--accent)">
          In Stock Only
        </label>
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
        <input type="text" id="kr-search" placeholder="Filter by name…" style="width:220px"
               oninput="renderKnownRecipes()">
        <select id="kr-status" style="width:auto" onchange="saveFilter('bd_kr_status',this.value);renderKnownRecipes()">
          <option value="all">All</option>
          <option value="in_store">In Store</option>
          <option value="not_in_store">Not In Store</option>
        </select>
        <select id="kr-sort" style="width:auto" onchange="saveFilter('bd_kr_sort',this.value);renderKnownRecipes()">
          <option value="name">Sort: Name</option>
          <option value="best_value">Sort: Best Value</option>
          <option value="store_qty">Sort: Store Qty</option>
          <option value="use_count">Sort: Use Count</option>
        </select>
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
          <input type="checkbox" id="nr-hide-base" checked onchange="saveFilter('bd_nr_hide_base',this.checked);renderNoRecipe()" style="width:15px;height:15px;accent-color:var(--accent)">
          Hide base components
        </label>
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="nr-in-stock" checked onchange="saveFilter('bd_nr_in_stock',this.checked);renderNoRecipe()" style="width:15px;height:15px;accent-color:var(--accent)">
          In Stock Only
        </label>
        <select id="nr-sort" style="width:auto" onchange="saveFilter('bd_nr_sort',this.value);renderNoRecipe()">
          <option value="name">Sort: Name</option>
          <option value="qty">Sort: Qty (high first)</option>
          <option value="price">Sort: Price</option>
        </select>
        <span style="font-size:12px;color:var(--text-dim);margin-left:auto">
          Mark as <em>base component</em> to suppress permanently.
        </span>
      </div>
    </div>
    <div id="norecipe-panel"><div class="loading"><span class="spinner"></span> Loading...</div></div>
  </div>
</div>

  <!-- ═══ FIND COMPONENTS TAB ═══════════════════════════════════════════ -->
  <div id="tab-compsearch" class="tab-pane">
    <div class="card">
      <h3 style="margin-bottom:4px">Search for components across all recipes</h3>
      <p style="font-size:12px;color:var(--text-dim);margin-bottom:12px">
        Find which products you can break down to get a specific component (e.g., "destined rivals booster")
      </p>
      <div style="display:flex;gap:8px;margin-bottom:12px">
        <input type="text" id="cs-search" placeholder="Search component name…" style="flex:1"
               onkeydown="if(event.key==='Enter') searchComponents()">
        <button class="btn btn-primary" onclick="searchComponents()">Search</button>
      </div>
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:12px" id="cs-filters" style="display:none">
        <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
          <input type="checkbox" id="cs-in-stock" checked onchange="renderComponentResults()" style="width:15px;height:15px;accent-color:var(--accent)">
          In Stock Only
        </label>
        <select id="cs-sort" style="width:auto" onchange="renderComponentResults()">
          <option value="available">Sort: Most Available</option>
          <option value="pl">Sort: Best Breakdown P/L</option>
          <option value="name">Sort: Parent Name</option>
          <option value="qty_per">Sort: Qty per Unit</option>
        </select>
        <span id="cs-count" style="font-size:12px;color:var(--text-dim);margin-left:auto"></span>
      </div>
      <div id="cs-results"></div>
    </div>
  </div>

<!-- Shared breakdown modal is injected by breakdown_modal.js -->

<script>
// ══════════════════════════════════════════════════════════════════
// FILTER PERSISTENCE
// ══════════════════════════════════════════════════════════════════
function saveFilter(k, v) {{ try {{ localStorage.setItem(k, JSON.stringify(v)); }} catch(e) {{}} }}
function loadFilter(k, fallback) {{ try {{ const v = localStorage.getItem(k); return v !== null ? JSON.parse(v) : fallback; }} catch(e) {{ return fallback; }} }}

function restoreFilters() {{
  // Recommendations
  const recInStock = loadFilter('bd_rec_in_stock', true);
  const el1 = document.getElementById('rec-in-stock');
  if (el1) el1.checked = recInStock;
  const recFilter = loadFilter('bd_rec_filter', 'all');
  const el2 = document.getElementById('rec-filter');
  if (el2) el2.value = recFilter;
  const recSort = loadFilter('bd_rec_sort', 'score');
  const el3 = document.getElementById('rec-sort');
  if (el3) el3.value = recSort;
  // Known Recipes
  const krStatus = loadFilter('bd_kr_status', 'all');
  const el4 = document.getElementById('kr-status');
  if (el4) el4.value = krStatus;
  const krSort = loadFilter('bd_kr_sort', 'name');
  const el5 = document.getElementById('kr-sort');
  if (el5) el5.value = krSort;
  // No Recipe
  const nrHideBase = loadFilter('bd_nr_hide_base', true);
  const el6 = document.getElementById('nr-hide-base');
  if (el6) el6.checked = nrHideBase;
  const nrInStock = loadFilter('bd_nr_in_stock', true);
  const el7 = document.getElementById('nr-in-stock');
  if (el7) el7.checked = nrInStock;
  const nrSort = loadFilter('bd_nr_sort', 'name');
  const el8 = document.getElementById('nr-sort');
  if (el8) el8.value = nrSort;
}}

// ══════════════════════════════════════════════════════════════════
// STATE
// ══════════════════════════════════════════════════════════════════
let _allRecs = [];

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
  const valid = ['recommendations','search','ignored','norecipe','recipes','compsearch'];
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
    const inStock = document.getElementById('rec-in-stock')?.checked ?? true;
    const r = await fetch(`/inventory/breakdown/api/recommendations?in_stock=${{inStock}}`);
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
        const bdIcon = comp.has_breakdown ? ` <span style="color:var(--accent);font-size:10px" title="Has recipe: $${{comp.child_bd_value?.toFixed(2)}}">📦</span>` : '';
        return `<div style="font-size:12px;white-space:nowrap">${{comp.title.length>32?comp.title.slice(0,30)+'…':comp.title}}${{perParent}}${{bdIcon}}: ${{qtyStr}}</div>`;
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
    <td>$${{r.bd_value.toFixed(2)}} <small style="color:var(--text-dim)">${{r.bd_value_label === 'store' ? '(store)' : '(mkt)'}}</small>${{r.deep_bd_value ? `<br><small style="color:var(--accent)" title="Value if children are also broken down">Deep: $${{r.deep_bd_value.toFixed(2)}}</small>` : ''}}<br><small style="color:var(--text-dim)">${{r.best_variant_name}}</small></td>
    <td class="${{deltaClass}}" style="font-weight:600">${{deltaStr}}</td>
    <td>${{childStockStr}}</td>
    <td>
      <div style="display:flex;flex-direction:column;gap:4px">
        <button class="btn btn-primary btn-sm" onclick="openExecuteModal(this,${{r.tcgplayer_id}},'${{r.title.replace(/'/g,'').replace(/"/g,'')}}', ${{r.store_price}}, ${{r.store_qty}}, ${{r.shopify_variant_id}}, ${{r.inventory_item_id}})">
          ▶ Break Down
        </button>
        <div style="display:flex;gap:4px">
          <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{r.tcgplayer_id}}, '${{r.title.replace(/'/g,'').replace(/"/g,'')}}', ${{r.store_price}}, ${{r.store_qty}})">
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
// EXECUTE BREAKDOWN (via shared modal)
// ══════════════════════════════════════════════════════════════════
function _bdExecuteHandler(parentVariantId, parentInvItemId, parentTcgId) {{
  return function(variantId, qty, components) {{
    return fetch('/inventory/breakdown/api/execute', {{
      method: 'POST',
      headers: {{'Content-Type':'application/json'}},
      body: JSON.stringify({{
        parent_variant_id: parentVariantId,
        parent_inventory_item_id: parentInvItemId,
        parent_tcgplayer_id: parentTcgId,
        qty_to_break: qty,
        variant_id: variantId,
      }})
    }}).then(r => r.json().then(d => {{
      if (!r.ok) throw new Error(d.error || 'Execute failed');
      // Update rec in-place
      const idx = _allRecs.findIndex(rec => rec.tcgplayer_id === parentTcgId);
      if (idx >= 0 && d.results) {{
        _allRecs[idx].store_qty = d.results.parent.new_qty;
        renderRecommendations();
      }}
      return d;
    }}));
  }};
}}

function openExecuteModal(btn, tcgId, name, storePrice, storeQty, parentVariantId, parentInvItemId) {{
  openBreakdownModal({{
    tcgplayerId: tcgId,
    productName: name,
    parentStore: storePrice || null,
    parentQty: storeQty || 1,
    apiBase: '/inventory/breakdown/api/cache',
    priceMode: 'best',
    onExecute: _bdExecuteHandler(parentVariantId, parentInvItemId, tcgId),
    onSave: function() {{ loadRecommendations(); }},
    showQtySelector: true,
  }});
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
    // Search inventory locally (no PPT calls) and use cached recommendations if available
    const r2 = await fetch('/inventory/breakdown/api/inventory-search?q=' + encodeURIComponent(q));
    const d2 = await r2.json();
    const filtered = d2.items || [];
    if (!filtered.length) {{ panel.innerHTML = '<div class="alert alert-warning">No items found.</div>'; return; }}

    // Use already-loaded recommendations for BD values (from Recommendations tab)
    const recMap = {{}};
    if (typeof _allRecs !== 'undefined' && _allRecs) {{
      _allRecs.forEach(r => recMap[r.tcgplayer_id] = r);
    }}

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
            ${{rec ? `<button class="btn btn-primary btn-sm" onclick="openExecuteModal(this,${{rec.tcgplayer_id}},'${{rec.title.replace(/'/g,'').replace(/"/g,'')}}', ${{rec.store_price}}, ${{rec.store_qty}}, ${{rec.shopify_variant_id}}, ${{rec.inventory_item_id}})">▶ Break Down</button>` : ''}}
            <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{item.tcgplayer_id||'null'}},'${{item.title.replace(/'/g,'').replace(/"/g,'')}}', ${{parseFloat(item.shopify_price||0)}}, ${{item.shopify_qty||0}})">
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
// RECIPE EDITOR  (via shared modal)
// ══════════════════════════════════════════════════════════════════
function openRecipeEditor(tcgId, productName, storePrice, storeQty) {{
  openBreakdownModal({{
    tcgplayerId: tcgId || null,
    productName: productName || '',
    parentStore: storePrice || null,
    parentQty: storeQty || 1,
    apiBase: '/inventory/breakdown/api/cache',
    priceMode: 'best',
    onExecute: null,
    onSave: function() {{ loadRecommendations(); }},
    showQtySelector: false,
  }});
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

  let items = [..._noRecipeItems];
  if (q) items = items.filter(i => (i.title||'').toLowerCase().includes(q));
  if (hideBase) items = items.filter(i => !i.is_base);
  const nrInStock = document.getElementById('nr-in-stock')?.checked;
  if (nrInStock) items = items.filter(i => i.shopify_qty > 0);
  const nrSort = document.getElementById('nr-sort')?.value || 'name';
  if (nrSort === 'qty') items.sort((a,b) => (b.shopify_qty||0) - (a.shopify_qty||0));
  else if (nrSort === 'price') items.sort((a,b) => parseFloat(b.shopify_price||0) - parseFloat(a.shopify_price||0));
  else items.sort((a,b) => (a.title||'').localeCompare(b.title||''));

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
  openRecipeEditor(item.tcgplayer_id || null, item.title || '', parseFloat(item.shopify_price||0), item.shopify_qty||0);
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
document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') {{
    document.querySelectorAll('.modal-overlay.active').forEach(m => m.classList.remove('active'));
  }}
}});

// ══════════════════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════════════════
restoreFilters();
restoreTab();
</script>

<!-- Component Search tab -->
<script>
let _compSearchResults = [];

async function searchComponents() {{
  const q = document.getElementById('cs-search').value.trim();
  const panel = document.getElementById('cs-results');
  if (!q) return;
  panel.innerHTML = '<div class="loading"><span class="spinner"></span> Searching...</div>';
  try {{
    const r = await fetch('/inventory/breakdown/api/component-search?q=' + encodeURIComponent(q));
    const d = await r.json();
    if (!r.ok) {{ panel.innerHTML = `<div class="alert alert-error">${{d.error}}</div>`; return; }}
    _compSearchResults = d.results || [];
    renderComponentResults();
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}}

function renderComponentResults() {{
  const panel = document.getElementById('cs-results');
  const countEl = document.getElementById('cs-count');
  const inStockOnly = document.getElementById('cs-in-stock')?.checked;
  const sort = document.getElementById('cs-sort')?.value || 'available';

  let items = [..._compSearchResults];
  if (inStockOnly) items = items.filter(i => i.parent_store_qty > 0);

  if (sort === 'available') items.sort((a,b) => b.total_available - a.total_available);
  else if (sort === 'pl') items.sort((a,b) => (b.bd_pl || -9999) - (a.bd_pl || -9999));
  else if (sort === 'qty_per') items.sort((a,b) => b.quantity_per_parent - a.quantity_per_parent);
  else items.sort((a,b) => (a.parent_name||'').localeCompare(b.parent_name||''));

  if (countEl) countEl.textContent = `${{items.length}} result${{items.length !== 1 ? 's' : ''}}`;

  if (!items.length) {{
    panel.innerHTML = `<div style="color:var(--text-dim);padding:20px;text-align:center">
      ${{_compSearchResults.length > 0
        ? '🔍 No in-stock results. Uncheck "In Stock Only" to see all.'
        : '🔍 No recipes contain components matching that search.'}}
    </div>`;
    return;
  }}

  panel.innerHTML = `<div style="overflow-x:auto"><table>
    <thead><tr>
      <th>Parent Product</th>
      <th>Component</th>
      <th>Qty/Unit</th>
      <th>Total Available</th>
      <th>Parent Price</th>
      <th>BD Value</th>
      <th>P/L</th>
      <th></th>
    </tr></thead>
    <tbody>
    ${{items.map(i => {{
      const plClass = i.bd_pl_pct == null ? '' : i.bd_pl_pct >= 5 ? 'delta-pos' : i.bd_pl_pct >= -10 ? 'delta-neutral' : 'delta-neg';
      const plStr = i.bd_pl != null
        ? `<span class="${{plClass}}" style="font-weight:600">${{i.bd_pl >= 0 ? '+' : ''}}$${{i.bd_pl.toFixed(2)}} <small>(${{i.bd_pl_pct >= 0 ? '+' : ''}}${{i.bd_pl_pct.toFixed(1)}}%)</small></span>`
        : '<span style="color:var(--text-dim)">—</span>';
      const qtyColor = i.parent_store_qty > 0 ? 'var(--green)' : 'var(--red)';
      const availColor = i.total_available > 0 ? 'var(--green)' : i.parent_store_qty > 0 ? 'var(--amber)' : 'var(--text-dim)';
      const safeName = (i.parent_name || '').replace(/'/g, '').replace(/"/g, '');
      return `<tr>
        <td>
          <strong style="font-size:13px">${{i.parent_name}}</strong><br>
          <small style="color:var(--text-dim)">TCG#${{i.parent_tcg_id}}</small>
          <br><small style="color:var(--text-dim)">${{i.variant_name}}</small>
        </td>
        <td>${{i.component_name}}<br><small style="color:var(--text-dim)">$${{i.component_market_price.toFixed(2)}} ea</small></td>
        <td style="font-weight:600">${{i.quantity_per_parent}}</td>
        <td style="font-weight:600;color:${{availColor}}">
          ${{i.total_available}}
          <small style="color:var(--text-dim);font-weight:400">(${{i.parent_store_qty}} × ${{i.quantity_per_parent}})</small>
        </td>
        <td>${{i.parent_store_price > 0 ? '$$' + i.parent_store_price.toFixed(2) : '<span style="color:var(--text-dim)">—</span>'}}</td>
        <td>${{i.bd_value > 0 ? '$$' + i.bd_value.toFixed(2) : '<span style="color:var(--text-dim)">—</span>'}}</td>
        <td>${{plStr}}</td>
        <td>
          <div style="display:flex;flex-direction:column;gap:4px">
            ${{i.parent_store_qty > 0 && i.parent_variant_id && i.parent_inv_item_id
              ? `<button class="btn btn-primary btn-sm" onclick="openExecuteModal(this,${{i.parent_tcg_id}},'${{safeName}}', ${{i.parent_store_price}}, ${{i.parent_store_qty}}, ${{i.parent_variant_id}}, ${{i.parent_inv_item_id}})">▶ Break Down</button>`
              : ''}}
            <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{i.parent_tcg_id}},'${{safeName}}', ${{i.parent_store_price}}, ${{i.parent_store_qty||0}})">✎ Recipe</button>
          </div>
        </td>
      </tr>`;
    }}).join('')}}
    </tbody></table></div>`;
}}
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
  let recs = [..._allKnownRecipes];
  if (q) recs = recs.filter(r => (r.product_name || '').toLowerCase().includes(q) || (r.store_title || '').toLowerCase().includes(q));
  const krStatus = document.getElementById('kr-status')?.value || 'all';
  if (krStatus === 'in_store') recs = recs.filter(r => r.store_qty != null && r.store_qty > 0);
  if (krStatus === 'not_in_store') recs = recs.filter(r => r.store_qty == null || r.store_qty <= 0);
  const krSort = document.getElementById('kr-sort')?.value || 'name';
  if (krSort === 'best_value') recs.sort((a,b) => parseFloat(b.best_variant_market||0) - parseFloat(a.best_variant_market||0));
  else if (krSort === 'store_qty') recs.sort((a,b) => (b.store_qty||0) - (a.store_qty||0));
  else if (krSort === 'use_count') recs.sort((a,b) => (b.use_count||0) - (a.use_count||0));
  else recs.sort((a,b) => (a.store_title||a.product_name||'').localeCompare(b.store_title||b.product_name||''));
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
            onclick="openRecipeEditor(${{r.tcgplayer_id || 'null'}},'${{(r.store_title || r.product_name || '').replace(/'/g,'').replace(/"/g,'')}}', ${{parseFloat(r.store_price||0)}}, ${{r.store_qty||0}})">
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
            ${{rec && qty > 0 ? `<button class="btn btn-primary btn-sm" onclick="openExecuteModal(this,${{rec.tcgplayer_id}},'${{rec.title.replace(/'/g,'').replace(/"/g,'')}}', ${{rec.store_price}}, ${{rec.store_qty}}, ${{rec.shopify_variant_id}}, ${{rec.inventory_item_id}})">▶ Break Down</button>` : ''}}
            <button class="btn btn-secondary btn-sm" onclick="openRecipeEditor(${{item.tcgplayer_id||'null'}},'${{(item.name||item.title).replace(/'/g,'').replace(/"/g,'')}}', ${{parseFloat(item.shopify_price||0)}}, ${{qty||0}})">
              ${{rec ? '✎ Recipe' : '+ Recipe'}}
            </button>
          </td>
        </tr>`;
      }}).join('')}}</tbody></table></div>`;
  }} catch(e) {{ panel.innerHTML = `<div class="alert alert-error">${{e.message}}</div>`; }}
}};

// Auto-open recipe/execute when launched from inventory page with bd_tcg param
(function() {{
  const params = new URLSearchParams(window.location.search);
  const bdTcg = params.get('bd_tcg');
  const bdAction = params.get('bd_action');
  if (!bdTcg) return;
  const tcgId = parseInt(bdTcg);

  function _autoOpen() {{
    const rec = _allRecs.find(r => r.tcgplayer_id === tcgId);
    if (bdAction === 'execute' && rec) {{
      openExecuteModal(null, rec.tcgplayer_id, rec.title, rec.store_price, rec.store_qty, rec.shopify_variant_id, rec.inventory_item_id);
    }} else {{
      openRecipeEditor(tcgId, rec ? rec.title : 'Product', rec ? rec.store_price : null, rec ? rec.store_qty : null);
    }}
  }}

  // Fire after whichever tab loads first
  const _origRecs = loadRecommendations;
  loadRecommendations = async function() {{
    await _origRecs();
    _autoOpen();
  }};
  const _origKnown = loadKnownRecipes;
  loadKnownRecipes = async function() {{
    await _origKnown();
    _autoOpen();
  }};
}})();
</script>

</body>
</html>"""
