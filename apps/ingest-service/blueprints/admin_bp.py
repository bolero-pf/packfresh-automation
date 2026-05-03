"""Auto-generated from app.py refactor. admin routes."""
import os
import json
import hashlib
import logging
import time
import requests as _requests
from decimal import Decimal, InvalidOperation

from flask import Blueprint, request, jsonify, render_template, send_file, Response, g

import db
import intake
from helpers import (
    _serialize,
    _decode_override,
    _effective_caps_from_role,
    _validate_offer_caps,
    _log_override_if_present,
    OVERRIDE_ACTION,
    ASSOCIATE_DEFAULT_CASH,
    ASSOCIATE_DEFAULT_CREDIT,
    MANAGER_CAP,
)

# Module-level service handles. Populated by configure() before routes run.
pricing = None
shopify = None
cache_mgr = None
INGEST_INTERNAL_URL = ""
SHOPIFY_STORE = ""
logger = logging.getLogger("intake.admin")


def configure(*, _pricing=None, _shopify=None, _cache_mgr=None,
              _ingest_url="", _shopify_store="", _logger=None):
    global pricing, shopify, cache_mgr, INGEST_INTERNAL_URL, SHOPIFY_STORE, logger
    pricing = _pricing
    shopify = _shopify
    cache_mgr = _cache_mgr
    INGEST_INTERNAL_URL = _ingest_url
    SHOPIFY_STORE = _shopify_store
    if _logger is not None:
        logger = _logger


bp = Blueprint("admin", __name__)

from barcode_gen import generate_barcode_image



@bp.route("/api/mappings", methods=["GET"])
def list_mappings():
    """List cached product mappings."""
    product_type = request.args.get("product_type")
    mappings = intake.get_all_mappings(product_type)
    return jsonify({"mappings": [_serialize(m) for m in mappings]})


# ==========================================
# BARCODE
# ==========================================


@bp.route("/api/barcode/<barcode_id>.png")
def get_barcode(barcode_id):
    """Generate and return a barcode label image."""
    # Optionally look up card details for the label
    card = db.query_one(
        "SELECT card_name, set_name, condition, current_price FROM raw_cards WHERE barcode = %s",
        (barcode_id,)
    )

    import io
    png = generate_barcode_image(
        barcode_id,
        card_name=card["card_name"] if card else "",
        set_name=card["set_name"] if card else "",
        condition=card["condition"] if card else "",
        price=f"${card['current_price']:.2f}" if card else "",
    )
    return send_file(io.BytesIO(png), mimetype="image/png")


# ==========================================
# HEALTH CHECK
# ==========================================


@bp.route("/api/shopify/sync", methods=["POST"])
def shopify_sync():
    """Trigger a cache refresh via CacheManager and stream a simple progress response."""
    if not shopify:
        return jsonify({"error": "Shopify not configured (set SHOPIFY_TOKEN + SHOPIFY_STORE)"}), 503

    def generate():
        import json
        try:
            yield json.dumps({"status": "starting"}) + "\n"
            cache_mgr.invalidate("manual_sync")
            # Give it a moment to start, then report done —
            # actual sync runs in background thread via CacheManager
            yield json.dumps({"status": "done", "message": "Cache refresh triggered in background"}) + "\n"
        except Exception as e:
            logger.error(f"Shopify sync trigger failed: {e}")
            yield json.dumps({"status": "error", "error": str(e)}) + "\n"

    return Response(generate(), mimetype="application/x-ndjson")


# ═══════════════════════════════════════════════════════════════════════════════
# LISTING CREATION (proxy to ingest enrichment pipeline)
# ═══════════════════════════════════════════════════════════════════════════════


@bp.route("/api/create-listing", methods=["POST"])
def proxy_create_listing():
    """
    Proxy to ingest service /api/enrich/create-listing.
    Creates a fully enriched DRAFT Shopify listing for a TCGPlayer product.

    Body: {
        "tcgplayer_id": 12345,
        "quantity": 0,        (default 0 — shell listing for price tracking)
        "offer_price": null   (optional COGS)
    }
    """
    if not INGEST_INTERNAL_URL:
        return jsonify({"error": "INGEST_INTERNAL_URL not configured — cannot create listings from intake"}), 503

    data = request.get_json() or {}
    tcgplayer_id = data.get("tcgplayer_id")
    item_id = data.get("item_id")  # optional — if provided, save resulting product ID back
    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    try:
        ingest_api_key = os.getenv("INGEST_API_KEY", "")
        headers = {"X-Ingest-Api-Key": ingest_api_key} if ingest_api_key else {}
        resp = _requests.post(
            f"{INGEST_INTERNAL_URL}/api/enrich/create-listing",
            json={
                "tcgplayer_id": tcgplayer_id,
                "quantity": int(data.get("quantity", 0)),
                "offer_price": data.get("offer_price"),
            },
            headers=headers,
            timeout=120,  # enrichment can take ~30-60s (image processing)
        )
        result = resp.json()

        # If creation succeeded and we know which intake item triggered this,
        # save the Shopify product ID back to intake_items and product_mappings
        if resp.ok and item_id and result.get("product_id"):
            shopify_product_id = int(result["product_id"])
            product_name = result.get("title", "")
            try:
                db.execute("""
                    UPDATE intake_items
                    SET shopify_product_id = %s
                    WHERE id = %s
                """, (shopify_product_id, item_id))
                # Also persist in product_mappings for future imports
                item = db.query_one("SELECT product_name, product_type, set_name, card_number, variance FROM intake_items WHERE id = %s", (item_id,))
                if item:
                    intake.save_mapping(
                        item["product_name"],
                        int(tcgplayer_id),
                        item.get("product_type", "sealed"),
                        set_name=item.get("set_name"),
                        card_number=item.get("card_number"),
                        variance=item.get("variance") or "",
                        shopify_product_id=shopify_product_id,
                        shopify_product_name=product_name or item["product_name"],
                    )
            except Exception as save_err:
                logger.warning(f"Could not persist shopify_product_id after create-listing: {save_err}")

        return jsonify(result), resp.status_code
    except _requests.Timeout:
        return jsonify({"error": "Listing creation timed out — it may still be processing"}), 504
    except Exception as e:
        logger.exception("proxy_create_listing failed")
        return jsonify({"error": str(e)}), 500



@bp.route("/api/cache/status")
def cache_status():
    """Return cache health and staleness info."""
    return jsonify(cache_mgr.get_status())



@bp.route("/api/cache/invalidate", methods=["POST"])
def cache_invalidate():
    """Explicitly invalidate and trigger cache refresh. Called by ingest after push-live."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "manual")
    cache_mgr.invalidate(reason)
    return jsonify({"success": True, "reason": reason})



@bp.route("/api/cache/refresh", methods=["POST"])
def cache_refresh():
    """Manual full cache refresh trigger from UI."""
    cache_mgr.invalidate("manual")
    return jsonify({"success": True, "message": "Refresh triggered in background"})



@bp.route("/api/shopify/status")
def shopify_status():
    """Check Shopify integration status and cache stats."""
    configured = shopify is not None
    cache_count = 0
    last_sync = None
    if configured:
        try:
            row = db.query_one("SELECT COUNT(*) as cnt, MAX(last_synced) as last_sync FROM inventory_product_cache")
            if row:
                cache_count = row["cnt"]
                last_sync = row["last_sync"].isoformat() if row["last_sync"] else None
        except Exception:
            pass  # Table might not exist yet
    return jsonify({"configured": configured, "store": SHOPIFY_STORE if configured else None,
                    "cache_count": cache_count, "last_sync": last_sync})



@bp.route("/api/shopify/session/<session_id>/store-check")
def shopify_session_store_check(session_id):
    """Check Shopify cache for inventory/price of all mapped items in a session.
    Damaged items look for damaged variants first, then fall back to 88% of normal price."""
    DAMAGED_DISCOUNT = 0.88  # We sell damaged at 12% off

    # Self-aware cache: check staleness and trigger background refresh if needed
    cache_mgr.check_and_refresh_if_stale()

    items = intake.get_session_items(session_id)
    active_items = [i for i in items if i.get("item_status") in ("good", "damaged")]
    # Items linked by tcgplayer_id (PPT-matched)
    linked_tcg = [i for i in active_items if i.get("tcgplayer_id")]
    # Items linked by shopify_product_id only (store-only link — e.g. CN products not on PPT)
    linked_shopify_only = [i for i in active_items if not i.get("tcgplayer_id") and i.get("shopify_product_id")]
    truly_unlinked = [i for i in active_items if not i.get("tcgplayer_id") and not i.get("shopify_product_id")]

    tcg_ids = list(set(i["tcgplayer_id"] for i in linked_tcg))
    shopify_ids = list(set(str(i["shopify_product_id"]) for i in linked_shopify_only))

    if not tcg_ids and not shopify_ids and not truly_unlinked:
        return jsonify({"items": [], "cache_hit_rate": 0})

    try:
        # Fetch by tcgplayer_id
        all_rows = []
        if tcg_ids:
            ph = ",".join(["%s"] * len(tcg_ids))
            all_rows += db.query(
                f"SELECT * FROM inventory_product_cache WHERE tcgplayer_id IN ({ph})",
                tuple(tcg_ids)
            )
        # Fetch by shopify_product_id for store-only linked items
        shopify_rows = []
        if shopify_ids:
            ph2 = ",".join(["%s"] * len(shopify_ids))
            shopify_rows = db.query(
                f"SELECT * FROM inventory_product_cache WHERE shopify_product_id IN ({ph2})",
                tuple(shopify_ids)
            )
    except Exception:
        return jsonify({"error": "Shopify cache table not found. Run the migration first, then sync."}), 500

    # Build separate maps for normal and damaged variants — keyed by tcgplayer_id
    normal_map = {}   # tcg_id -> {title, handle, shopify_price, shopify_qty, ...}
    damaged_map = {}  # tcg_id -> {title, handle, shopify_price, shopify_qty, ...}

    for r in all_rows:
        tcg = r["tcgplayer_id"]
        is_dmg = r.get("is_damaged") or False
        target = damaged_map if is_dmg else normal_map

        if tcg not in target:
            target[tcg] = {
                "title": r["title"], "handle": r["handle"],
                "shopify_price": float(r["shopify_price"]) if r["shopify_price"] else None,
                "shopify_qty": 0, "shopify_product_id": r["shopify_product_id"],
                "shopify_variant_id": r.get("shopify_variant_id"),
                "status": r["status"],
                "last_synced": r["last_synced"].isoformat() if r["last_synced"] else None,
                "is_damaged": is_dmg,
            }
        target[tcg]["shopify_qty"] += (r["shopify_qty"] or 0)

    # Build shopify_product_id -> cache row map for store-only items
    shopify_direct_map = {}  # shopify_product_id (str) -> cache row
    for r in shopify_rows:
        pid = str(r["shopify_product_id"])
        if pid not in shopify_direct_map:
            shopify_direct_map[pid] = {
                "title": r["title"], "handle": r["handle"],
                "shopify_price": float(r["shopify_price"]) if r["shopify_price"] else None,
                "shopify_qty": 0, "shopify_product_id": r["shopify_product_id"],
                "shopify_variant_id": r.get("shopify_variant_id"),
                "status": r["status"],
                "last_synced": r["last_synced"].isoformat() if r["last_synced"] else None,
                "is_damaged": r.get("is_damaged") or False,
                "tcgplayer_id": r.get("tcgplayer_id"),
            }
        shopify_direct_map[pid]["shopify_qty"] += (r["shopify_qty"] or 0)

    # Merge linked list — treat shopify-only as a third category
    linked = linked_tcg  # still processed via tcg map below
    unlinked = truly_unlinked

    result_items = []
    for item in linked:
        tcg_id = item["tcgplayer_id"]
        item_status = item.get("item_status", "good")
        is_damaged_item = (item_status == "damaged")

        sd = None
        damaged_variant_exists = tcg_id in damaged_map
        normal_variant = normal_map.get(tcg_id)
        store_note = None

        if is_damaged_item:
            if damaged_variant_exists:
                # Best case: we have a damaged listing in the store
                sd = damaged_map[tcg_id]
                store_note = "Matched damaged variant"
            elif normal_variant and normal_variant["shopify_price"]:
                # Fallback: use normal price × 88%
                sd = {
                    **normal_variant,
                    "shopify_price": round(normal_variant["shopify_price"] * DAMAGED_DISCOUNT, 2),
                    "shopify_qty": 0,  # we don't have damaged stock
                    "title": normal_variant["title"] + " [est. damaged]",
                    "is_damaged": True,
                }
                store_note = f"No damaged variant — estimated at {int(DAMAGED_DISCOUNT*100)}% of ${normal_variant['shopify_price']:.2f}"
            # else: sd stays None — not in store at all
        else:
            # Normal item — use non-damaged variant
            sd = normal_variant

        result_items.append({
            "item_id": item["id"], "product_name": item.get("product_name"), "tcgplayer_id": tcg_id,
            "offer_price": float(item.get("offer_price") or 0), "market_price": float(item.get("market_price") or 0),
            "quantity": item.get("quantity", 1), "item_status": item_status,
            "set_name": item.get("set_name"), "product_type": item.get("product_type", "sealed"),
            "in_store": sd is not None,
            "store_title": sd["title"] if sd else None, "store_price": sd["shopify_price"] if sd else None,
            "store_qty": sd["shopify_qty"] if sd else None, "store_handle": sd["handle"] if sd else None,
            "store_product_id": sd["shopify_product_id"] if sd else None,
            "shopify_variant_id": sd.get("shopify_variant_id") if sd else None,
            "damaged_variant_exists": damaged_variant_exists if is_damaged_item else None,
            "store_note": store_note,
        })

    # Shopify-only linked items — look up by shopify_product_id directly
    for item in linked_shopify_only:
        pid = str(item["shopify_product_id"])
        sd = shopify_direct_map.get(pid)
        result_items.append({
            "item_id": item["id"], "product_name": item.get("product_name"),
            "tcgplayer_id": sd.get("tcgplayer_id") if sd else None,
            "offer_price": float(item.get("offer_price") or 0),
            "market_price": float(item.get("market_price") or 0),
            "quantity": item.get("quantity", 1), "item_status": item.get("item_status", "good"),
            "set_name": item.get("set_name"), "product_type": item.get("product_type", "sealed"),
            "in_store": sd is not None,
            "store_title": sd["title"] if sd else item.get("shopify_product_name"),
            "store_price": sd["shopify_price"] if sd else None,
            "store_qty": sd["shopify_qty"] if sd else None,
            "store_handle": sd["handle"] if sd else None,
            "store_product_id": sd["shopify_product_id"] if sd else item["shopify_product_id"],
            "shopify_variant_id": sd.get("shopify_variant_id") if sd else None,
            "damaged_variant_exists": None, "store_note": "Store-linked" if sd else "Linked but not in cache",
            "breakdown": None,
        })

    # Append truly unlinked items
    for item in unlinked:
        result_items.append({
            "item_id": item["id"], "product_name": item.get("product_name"), "tcgplayer_id": None,
            "offer_price": float(item.get("offer_price") or 0), "market_price": float(item.get("market_price") or 0),
            "quantity": item.get("quantity", 1), "item_status": item.get("item_status", "good"),
            "set_name": item.get("set_name"), "product_type": item.get("product_type", "sealed"),
            "in_store": False,
            "store_title": None, "store_price": None, "store_qty": None,
            "store_handle": None, "store_product_id": None, "shopify_variant_id": None,
            "damaged_variant_exists": None, "store_note": "Not linked to TCGPlayer",
            "breakdown": None,
        })

    hit = sum(1 for i in result_items if i["in_store"])

    # Enrich with breakdown cache data (multi-variant schema)
    all_tcg_ids = [i["tcgplayer_id"] for i in result_items if i.get("tcgplayer_id")]
    breakdown_data = {}
    if all_tcg_ids:
        try:
            ph = ",".join(["%s"] * len(all_tcg_ids))

            # JIT refresh stale component market prices in background
            try:
                from breakdown_helpers import refresh_stale_component_prices
                import threading
                _vids = db.query(f"""
                    SELECT sbv.id AS variant_id
                    FROM sealed_breakdown_cache sbc
                    JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                        AND sbv.total_component_market = sbc.best_variant_market
                    WHERE sbc.tcgplayer_id IN ({ph})
                """, tuple(all_tcg_ids))
                if _vids:
                    threading.Thread(target=refresh_stale_component_prices,
                        args=([v["variant_id"] for v in _vids], db, pricing), daemon=True).start()
            except Exception as e:
                logger.warning(f"Component price refresh skipped: {e}")

            # Get best variant (highest total) per product for store check display
            bd_rows = db.query(f"""
                SELECT sbc.tcgplayer_id AS parent_id,
                       sbc.best_variant_market, sbc.variant_count,
                       sbv.id AS variant_id, sbv.variant_name, sbv.notes,
                       sbv.total_component_market, sbv.component_count,
                       sbco.tcgplayer_id AS comp_tcg_id, sbco.product_name AS comp_name,
                       sbco.quantity_per_parent, sbco.market_price AS comp_price
                FROM sealed_breakdown_cache sbc
                JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                    AND sbv.total_component_market = sbc.best_variant_market
                LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                WHERE sbc.tcgplayer_id IN ({ph})
                ORDER BY sbc.tcgplayer_id, sbco.display_order
            """, tuple(all_tcg_ids))

            # Check component store presence
            comp_tcg_ids = list(set(r["comp_tcg_id"] for r in bd_rows if r.get("comp_tcg_id")))
            comp_store_map = {}
            if comp_tcg_ids:
                cp = ",".join(["%s"] * len(comp_tcg_ids))
                comp_rows = db.query(
                    f"SELECT tcgplayer_id, shopify_qty, shopify_price FROM inventory_product_cache WHERE tcgplayer_id IN ({cp}) AND is_damaged = FALSE",
                    tuple(comp_tcg_ids)
                )
                for cr in comp_rows:
                    comp_store_map[cr["tcgplayer_id"]] = cr

            # Nested breakdown lookup: which components (across ALL variants) have their own recipes?
            # We check all variants, not just the best, so deep value works even when
            # the best variant's components are base items but another variant has breakdownable children
            all_comp_tcg_ids = set(comp_tcg_ids)
            try:
                all_variant_comps = db.query(f"""
                    SELECT sbco.tcgplayer_id AS comp_tcg_id, sbco.quantity_per_parent,
                           sbco.market_price, sbv.id AS variant_id,
                           sbc.tcgplayer_id AS parent_id
                    FROM sealed_breakdown_cache sbc
                    JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                    LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                    WHERE sbc.tcgplayer_id IN ({ph}) AND sbco.tcgplayer_id IS NOT NULL
                """, tuple(all_tcg_ids))
                for avc in all_variant_comps:
                    if avc["comp_tcg_id"]:
                        all_comp_tcg_ids.add(avc["comp_tcg_id"])
            except Exception:
                all_variant_comps = []

            child_bd_map = {}      # tcg_id -> market value of best variant
            child_bd_store_map = {}  # tcg_id -> store value of best variant's components
            if all_comp_tcg_ids:
                cbp = ",".join(["%s"] * len(all_comp_tcg_ids))
                child_bd_rows = db.query(
                    f"SELECT tcgplayer_id, best_variant_market FROM sealed_breakdown_cache WHERE tcgplayer_id IN ({cbp})",
                    tuple(all_comp_tcg_ids)
                )
                child_bd_map = {int(r["tcgplayer_id"]): float(r["best_variant_market"] or 0) for r in child_bd_rows}

                # For children with recipes, compute their store-based breakdown value
                # by looking up grandchild components' store prices
                if child_bd_map:
                    try:
                        child_tcg_list = list(child_bd_map.keys())
                        gcph = ",".join(["%s"] * len(child_tcg_list))
                        grandchild_rows = db.query(f"""
                            SELECT sbc.tcgplayer_id AS child_tcg_id,
                                   sbco.tcgplayer_id AS gc_tcg_id,
                                   sbco.quantity_per_parent
                            FROM sealed_breakdown_cache sbc
                            JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                                AND sbv.total_component_market = sbc.best_variant_market
                            LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                            WHERE sbc.tcgplayer_id IN ({gcph}) AND sbco.tcgplayer_id IS NOT NULL
                        """, tuple(child_tcg_list))
                        # Get store prices for all grandchild components
                        gc_ids = list(set(r["gc_tcg_id"] for r in grandchild_rows if r["gc_tcg_id"]))
                        gc_store = {}
                        if gc_ids:
                            gcp = ",".join(["%s"] * len(gc_ids))
                            gc_store_rows = db.query(
                                f"SELECT tcgplayer_id, shopify_price FROM inventory_product_cache WHERE tcgplayer_id IN ({gcp}) AND is_damaged = FALSE",
                                tuple(gc_ids))
                            gc_store = {r["tcgplayer_id"]: float(r["shopify_price"] or 0) for r in gc_store_rows}
                        # Compute store total per child recipe
                        _gc_by_child = {}
                        for r in grandchild_rows:
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

            for row in bd_rows:
                pid = row["parent_id"]
                if pid not in breakdown_data:
                    breakdown_data[pid] = {
                        "best_variant_market": float(row["best_variant_market"] or 0),
                        "variant_count": row["variant_count"],
                        "variant_name": row["variant_name"],
                        "variant_notes": row["notes"],
                        "component_count": row["component_count"],
                        "components": [],
                        "all_components_in_store": True,
                        "components_in_store_count": 0,
                    }
                if row["comp_name"]:
                    cs = comp_store_map.get(row["comp_tcg_id"])
                    in_store = cs is not None and (cs.get("shopify_qty") or 0) > 0
                    store_price = float(cs["shopify_price"]) if cs and cs.get("shopify_price") else None
                    child_bd_val = child_bd_map.get(row["comp_tcg_id"], 0)
                    breakdown_data[pid]["components"].append({
                        "tcgplayer_id": row["comp_tcg_id"],
                        "product_name": row["comp_name"],
                        "quantity_per_parent": row["quantity_per_parent"],
                        "market_price": float(row["comp_price"] or 0),
                        "store_price": store_price,
                        "in_store": in_store,
                        "has_breakdown": child_bd_val > 0,
                        "child_bd_value": round(child_bd_val, 2) if child_bd_val > 0 else None,
                    })
                    if in_store:
                        breakdown_data[pid]["components_in_store_count"] += 1
                    else:
                        breakdown_data[pid]["all_components_in_store"] = False
        except Exception as e:
            logger.warning(f"Breakdown cache lookup failed (run migrate_breakdown_cache.py?): {e}")

    for item in result_items:
        tcg_id = item.get("tcgplayer_id")
        bd = breakdown_data.get(tcg_id)
        if bd:
            # Compute store total for best variant (sum comp store_price * qty, only if all have store prices)
            comps = bd["components"]
            store_total = None
            if comps:
                comp_store_vals = [
                    (c["store_price"] or 0) * (c["quantity_per_parent"] or 1)
                    for c in comps if c.get("store_price") is not None
                ]
                if comp_store_vals:
                    store_total = sum(comp_store_vals)
                    # If not all components have store prices, mark as partial
                    if len(comp_store_vals) < len(comps):
                        store_total = None  # partial — don't use for margin math

            # Compute deep value across ALL variants (not just the best)
            # Compute BOTH market deep and store deep — different contexts need different values
            best_deep_market = 0.0
            best_deep_store = 0.0
            _var_comps = {}
            for avc in all_variant_comps:
                if avc["parent_id"] == tcg_id:
                    _var_comps.setdefault(avc["variant_id"], []).append(avc)
            for _vid, _vcomps in _var_comps.items():
                dv_mkt = 0.0
                dv_store = 0.0
                has_deep_mkt = False
                has_deep_store = False
                for vc in _vcomps:
                    cid = vc["comp_tcg_id"]
                    qty = vc["quantity_per_parent"] or 1
                    # Market deep: use child's market BD value, fallback to component market price
                    cbd_mkt = child_bd_map.get(cid, 0)
                    if cbd_mkt > 0:
                        dv_mkt += cbd_mkt * qty
                        has_deep_mkt = True
                    else:
                        dv_mkt += float(vc["market_price"] or 0) * qty
                    # Store deep: use child's store BD value, fallback to component store price
                    cbd_store = child_bd_store_map.get(cid, 0)
                    if cbd_store > 0:
                        dv_store += cbd_store * qty
                        has_deep_store = True
                    else:
                        cs = comp_store_map.get(cid)
                        sp = float(cs["shopify_price"]) if cs and cs.get("shopify_price") else 0
                        if sp > 0:
                            dv_store += sp * qty
                        else:
                            dv_store += float(vc["market_price"] or 0) * qty
                if has_deep_mkt and dv_mkt > best_deep_market:
                    best_deep_market = dv_mkt
                if has_deep_store and dv_store > best_deep_store:
                    best_deep_store = dv_store

            item["breakdown"] = {
                "best_variant_market": bd["best_variant_market"],
                "best_variant_store": store_total,
                "variant_count": bd["variant_count"],
                "variant_name": bd["variant_name"],
                "variant_notes": bd["variant_notes"],
                "component_count": bd["component_count"],
                "all_components_in_store": bd["all_components_in_store"],
                "components_in_store_count": bd["components_in_store_count"],
                "total_components": len(bd["components"]),
                "deep_bd_market": round(best_deep_market, 2) if best_deep_market > 0 else None,
                "deep_bd_store": round(best_deep_store, 2) if best_deep_store > 0 else None,
            }
        else:
            item["breakdown"] = None

    return jsonify({"items": result_items, "total": len(result_items), "in_store": hit,
                    "not_in_store": len(result_items) - hit,
                    "cache_hit_rate": round(hit / len(result_items) * 100, 1) if result_items else 0})



# Breakdown-cache, store-prices routes now served by shared breakdown blueprint



@bp.route("/api/store/search", methods=["GET"])
@bp.route("/api/store/search", methods=["GET"])
def store_search():
    """Search inventory_product_cache by title — fuzzy token matching so partial/reordered names hit."""
    import re as _re
    try:
        q = request.args.get("q", "").strip()
        if not q or len(q) < 2:
            return jsonify({"results": []})

        # Strip parenthetical suffixes like (CN), (International Version), (Japanese) etc.
        q_stripped = _re.sub(r'\s*\([^)]{1,30}\)\s*$', '', q).strip()
        q_for_tokens = q_stripped if q_stripped else q

        STOPWORDS = {"the", "a", "an", "of", "and", "or", "in", "for", "&", "-", "pokemon", "tcg", "card", "cards",
                     "collection", "set", "box", "pack"}
        tokens = [t.lower() for t in q_for_tokens.replace("-", " ").replace(":", " ").split()
                  if t.lower() not in STOPWORDS and len(t) > 2]
        if not tokens:
            tokens = [t.lower() for t in q_for_tokens.split() if len(t) > 1]

        def run_query(conditions_sql, params):
            return db.query(
                f"""SELECT tcgplayer_id, shopify_product_id, shopify_variant_id,
                          title, handle, shopify_price, shopify_qty, is_damaged
                   FROM inventory_product_cache
                   WHERE ({conditions_sql}) AND (is_damaged = false OR is_damaged IS NULL)
                   ORDER BY title ASC LIMIT 20""",
                params
            )

        rows = run_query("title ILIKE %s", (f"%{q_stripped}%",))

        if not rows and q_stripped != q:
            rows = run_query("title ILIKE %s", (f"%{q}%",))

        if not rows and tokens:
            conds = " AND ".join(["title ILIKE %s"] * len(tokens))
            rows = run_query(conds, tuple(f"%{t}%" for t in tokens))

        if not rows and len(tokens) > 2:
            for drop_count in range(1, len(tokens) - 1):
                reduced = tokens[:len(tokens) - drop_count]
                conds = " AND ".join(["title ILIKE %s"] * len(reduced))
                rows = run_query(conds, tuple(f"%{t}%" for t in reduced))
                if rows:
                    break

        if not rows and len(tokens) > 2:
            majority = sorted(tokens, key=len, reverse=True)[:-1]
            conds = " AND ".join(["title ILIKE %s"] * len(majority))
            rows = run_query(conds, tuple(f"%{t}%" for t in majority))

        results = [_serialize(dict(r)) for r in rows]
        return jsonify({"results": results, "query": q, "tokens": tokens})

    except Exception as e:
        logger.error(f"store_search error: {e}")
        return jsonify({"results": [], "error": str(e)})

# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")
