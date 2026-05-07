"""
Shared Flask Blueprint for breakdown-cache API endpoints.

Usage in any service:
    from breakdown_routes import create_breakdown_blueprint
    app.register_blueprint(create_breakdown_blueprint(db, ppt_getter=lambda: ppt))
"""

import os
import logging
import threading
from datetime import datetime, date
from decimal import Decimal
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)


def _serialize(obj):
    """JSON-safe serialization for DB rows (Decimal, datetime, UUID)."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if hasattr(obj, '__str__') and type(obj).__name__ in ('UUID', 'uuid'):
        return str(obj)
    return obj


def create_breakdown_blueprint(db_module, ppt_getter=None, url_prefix="/api/breakdown-cache", name="breakdown_cache"):
    """
    Factory: returns a Flask Blueprint with all breakdown-cache endpoints.

    Args:
        db_module: the service's db module (must have query, query_one, execute, execute_returning)
        ppt_getter: callable returning a PriceProvider instance (or None to disable provider features)
        url_prefix: where to mount the blueprint (default /api/breakdown-cache)
        name: blueprint name (must be unique per app, default "breakdown_cache")
    """
    bp = Blueprint(
        name, __name__,
        url_prefix=url_prefix,
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
        static_url_path="/bd-static",
    )

    # Import shared logic (will be on PYTHONPATH via shared/)
    import breakdown_logic as logic

    def _get_ppt():
        if ppt_getter:
            return ppt_getter()
        return None

    # ─── List all recipes ───────────────────────────────────────────

    @bp.route("/")
    def list_cache():
        limit = request.args.get("limit", 200, type=int)
        rows = logic.list_breakdown_cache(db_module, limit=limit)
        return jsonify({"caches": _serialize(rows)})

    # ─── Get full breakdown record ──────────────────────────────────

    @bp.route("/<int:tcgplayer_id>")
    def get_cache(tcgplayer_id):
        result = logic.get_breakdown_cache(tcgplayer_id, db_module)
        if not result:
            return jsonify({"found": False, "cache": None})

        # JIT refresh stale component market prices in background. cache_only:
        # this endpoint is just opening the breakdown modal — a slightly stale
        # market_price is fine, and the previous behavior burned 12s+ of PPT
        # time on cards Scrydex doesn't cover (promos from new sealed bundles).
        ppt = _get_ppt()
        if ppt:
            try:
                from breakdown_helpers import refresh_stale_component_prices
                variant_ids = [str(v["id"]) for v in result.get("variants", [])]
                if variant_ids:
                    threading.Thread(
                        target=refresh_stale_component_prices,
                        args=(variant_ids, db_module, ppt),
                        kwargs={"cache_only": True},
                        daemon=True
                    ).start()
            except Exception as e:
                logger.warning(f"Component price refresh skipped: {e}")

        return jsonify({"found": True, "cache": _serialize(result)})

    # ─── Delete entire record ───────────────────────────────────────

    @bp.route("/<int:tcgplayer_id>", methods=["DELETE"])
    def delete_cache(tcgplayer_id):
        deleted = logic.delete_breakdown_cache(tcgplayer_id, db_module)
        return jsonify({"success": deleted})

    # ─── Create/update variant ──────────────────────────────────────

    @bp.route("/<int:tcgplayer_id>/variant", methods=["POST"])
    def save_variant_route(tcgplayer_id):
        data = request.get_json(silent=True) or {}
        product_name = data.get("product_name", "Unknown")
        variant_name = data.get("variant_name", "Standard")
        components = data.get("components", [])
        notes = data.get("notes")
        variant_id = data.get("variant_id")

        if not components:
            return jsonify({"error": "components required"}), 400

        try:
            result = logic.save_variant(
                tcgplayer_id, product_name, variant_name, components,
                db_module, notes=notes, variant_id=variant_id
            )
            return jsonify({"success": True, "cache": _serialize(result)})
        except Exception as e:
            logger.exception(f"Failed to save variant for {tcgplayer_id}")
            return jsonify({"error": str(e)}), 500

    # ─── Delete variant ─────────────────────────────────────────────

    @bp.route("/variant/<variant_id>", methods=["DELETE"])
    def delete_variant_route(variant_id):
        result = logic.delete_variant(variant_id, db_module)
        return jsonify({"success": True, "cache": _serialize(result)})

    # ─── Batch summaries ────────────────────────────────────────────

    @bp.route("/batch", methods=["POST"])
    def batch_summaries():
        data = request.get_json(silent=True) or {}
        tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
        if not tcg_ids:
            return jsonify({"summaries": {}})
        ppt = _get_ppt()
        # Read endpoint — don't fall through to PPT on cache miss (avoids 12s
        # network stalls on promos Scrydex doesn't cover yet).
        summaries = logic.get_breakdown_summary_for_items(
            tcg_ids, db_module, ppt=ppt, max_age_hours=24, cache_only=True,
        )
        return jsonify({"summaries": _serialize(summaries)})

    # ─── Cache audit: which recipe components are missing from Scrydex ─
    # Sean's class of bug: recipe editor stores tcgplayer_id; cache lookup
    # is by tcgplayer_id; if Scrydex hasn't synced that ID (common for new
    # promo cards inside sealed bundles), every Collection Summary on a
    # session containing the parent box would burn 12s+ on PPT timeouts.
    # cache_only flag now suppresses the timeout, but the recipes are still
    # silently missing prices. This endpoint surfaces the broken set so
    # operators can fix at the source instead of grepping logs.
    @bp.route("/audit-missing")
    def audit_missing():
        """Components whose tcgplayer_id has no matching scrydex_price_cache row
        of the expected product_type. Sealed components expect product_type='sealed';
        promo components expect product_type='card'.

        Returns one row per (tcgplayer_id, component_type), with the parent
        recipes that wire it in so operators know what to fix.

        Optional query param ?include_any_row=1 also returns rows where the
        tcgplayer_id IS in the cache but under a different product_type
        (recipe author may have picked the wrong component_type).
        """
        include_any = request.args.get("include_any_row") == "1"

        rows = db_module.query("""
            WITH comp_recipes AS (
                SELECT sbco.tcgplayer_id,
                       COALESCE(sbco.component_type, 'sealed') AS component_type,
                       sbc.tcgplayer_id AS parent_tcg_id,
                       MAX(sbco.market_price) AS last_market_price,
                       MAX(sbco.market_price_updated_at) AS last_priced_at
                FROM sealed_breakdown_components sbco
                JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
                JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
                WHERE sbco.tcgplayer_id IS NOT NULL
                GROUP BY sbco.tcgplayer_id, sbco.component_type, sbc.tcgplayer_id
            ),
            agg AS (
                SELECT tcgplayer_id,
                       component_type,
                       array_agg(DISTINCT parent_tcg_id) AS parent_tcg_ids,
                       COUNT(DISTINCT parent_tcg_id) AS recipe_count,
                       MAX(last_market_price) AS last_market_price,
                       MAX(last_priced_at) AS last_priced_at
                FROM comp_recipes
                GROUP BY tcgplayer_id, component_type
            )
            SELECT a.tcgplayer_id,
                   a.component_type,
                   a.parent_tcg_ids,
                   a.recipe_count,
                   a.last_market_price,
                   a.last_priced_at,
                   EXISTS (
                       SELECT 1 FROM scrydex_price_cache spc
                       WHERE spc.tcgplayer_id = a.tcgplayer_id
                         AND spc.product_type = (CASE WHEN a.component_type = 'promo'
                                                       THEN 'card' ELSE 'sealed' END)
                   ) AS in_cache_correct_type,
                   EXISTS (
                       SELECT 1 FROM scrydex_price_cache spc
                       WHERE spc.tcgplayer_id = a.tcgplayer_id
                   ) AS in_cache_any_type,
                   (SELECT product_type FROM scrydex_price_cache spc
                     WHERE spc.tcgplayer_id = a.tcgplayer_id LIMIT 1) AS cached_product_type
            FROM agg a
            ORDER BY a.recipe_count DESC, a.tcgplayer_id
        """)

        # Default: only rows missing the expected type. With include_any_row=1,
        # also surface rows where the cache has a row of a *different* type —
        # those usually mean the recipe author tagged a card as sealed (or
        # vice versa) and the lookup goes to the wrong column.
        broken = []
        for r in rows:
            in_correct = r["in_cache_correct_type"]
            if include_any:
                if in_correct:
                    continue  # this one's fine
            else:
                if in_correct:
                    continue
                if r["in_cache_any_type"]:
                    # Tagged with wrong component_type — surface separately
                    pass
            broken.append({
                "tcgplayer_id": int(r["tcgplayer_id"]),
                "component_type": r["component_type"],
                "expected_cache_type": "card" if r["component_type"] == "promo" else "sealed",
                "in_cache_any_type": bool(r["in_cache_any_type"]),
                "cached_product_type": r["cached_product_type"],
                "wrong_type_in_cache": (
                    bool(r["in_cache_any_type"]) and not r["in_cache_correct_type"]
                ),
                "recipe_count": int(r["recipe_count"]),
                "parent_tcg_ids": [int(p) for p in (r["parent_tcg_ids"] or [])],
                "last_market_price": float(r["last_market_price"]) if r["last_market_price"] is not None else None,
                "last_priced_at": r["last_priced_at"].isoformat() if r["last_priced_at"] else None,
            })

        # Enrich parent TCG IDs with their Shopify titles so the UI/JSON
        # reader can identify the recipe without a second lookup.
        all_parents = sorted({p for row in broken for p in row["parent_tcg_ids"]})
        parent_titles: dict[int, str] = {}
        if all_parents:
            try:
                ph = ",".join(["%s"] * len(all_parents))
                for r in db_module.query(
                    f"SELECT tcgplayer_id, title FROM inventory_product_cache "
                    f"WHERE tcgplayer_id IN ({ph})",
                    tuple(all_parents),
                ):
                    parent_titles[int(r["tcgplayer_id"])] = r["title"] or ""
            except Exception as e:
                logger.warning(f"audit-missing: parent title enrichment failed: {e}")

        for row in broken:
            row["parents"] = [
                {"tcgplayer_id": p, "title": parent_titles.get(p)}
                for p in row["parent_tcg_ids"]
            ]
            del row["parent_tcg_ids"]

        return jsonify({
            "missing_count": len(broken),
            "wrong_type_count": sum(1 for b in broken if b["wrong_type_in_cache"]),
            "no_row_count": sum(1 for b in broken if not b["in_cache_any_type"]),
            "missing": broken,
        })

    # ─── PPT search (sealed products) ──────────────────────────────

    @bp.route("/search")
    def search_sealed():
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"results": []})
        pricing = _get_ppt()
        if not pricing:
            return jsonify({"results": [], "error": "Price provider not configured"}), 503
        try:
            results = pricing.search_sealed_products(q, limit=10) or []
            # Normalize fields — provider returns varying field names
            for r in results:
                if not r.get("tcgplayer_id"):
                    tcg_id = r.get("tcgplayerId") or r.get("tcgPlayerId") or r.get("id")
                    if tcg_id:
                        try:
                            r["tcgplayer_id"] = int(tcg_id)
                        except (TypeError, ValueError):
                            pass
                # Sealed products: price is in unopenedPrice, not market_price
                if not r.get("market_price"):
                    r["market_price"] = r.get("unopenedPrice") or r.get("marketPrice") or r.get("midPrice") or 0
            # Scrydex sealed hits don't carry tcgplayer_id — backfill from
            # inventory_product_cache when the store already carries the product.
            try:
                from sealed_tcg_enrichment import enrich_sealed_with_shopify_tcg
                enrich_sealed_with_shopify_tcg(results, db_module, price_provider=pricing)
            except Exception as e:
                logger.debug(f"Sealed-TCG enrichment skipped: {e}")
            return jsonify({"results": results})
        except Exception as e:
            details = e.args[2] if len(e.args) > 2 else {}
            retry = details.get("retry_after", 60) if isinstance(details, dict) else 60
            return jsonify({"results": [], "error": str(e.args[0]) if e.args else str(e), "retry_after": retry}), 429

    # ─── PPT search (cards/promos) ──────────────────────────────────

    @bp.route("/search-cards")
    def search_cards():
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"results": []})
        ppt = _get_ppt()
        if not ppt:
            return jsonify({"results": [], "error": "PPT not configured"}), 503
        try:
            set_name = request.args.get("set_name", "").strip() or None
            limit = request.args.get("limit", 8, type=int)
            results = ppt.search_cards(q, set_name=set_name, limit=limit)
            # Extract NM price for each card
            for r in (results or []):
                if not r.get("market_price"):
                    conds = (r.get("prices") or {}).get("conditions") or {}
                    nm = conds.get("Near Mint") or conds.get("NM") or {}
                    r["market_price"] = nm.get("price") or (r.get("prices") or {}).get("market") or 0
            return jsonify({"results": results})
        except Exception as e:
            return jsonify({"results": [], "error": str(e)}), 500

    # ─── Store prices lookup ────────────────────────────────────────

    @bp.route("/store-prices", methods=["POST"])
    def store_prices():
        data = request.get_json(silent=True) or {}
        tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
        if not tcg_ids:
            return jsonify({"prices": {}})
        prices = logic.get_store_prices(tcg_ids, db_module)

        # Enrich with velocity data from sku_analytics
        try:
            from sku_analytics import get_analytics_for_tcgplayer_ids
            analytics = get_analytics_for_tcgplayer_ids(tcg_ids, db_module)
            for tcg_id, a in analytics.items():
                if tcg_id in prices:
                    prices[tcg_id]["velocity_score"] = a.get("velocity_score")
                    prices[tcg_id]["units_sold_90d"] = a.get("units_sold_90d")
                elif tcg_id not in prices:
                    # Component not in store but has analytics — still useful
                    prices[tcg_id] = {
                        "tcgplayer_id": tcg_id,
                        "shopify_price": None, "shopify_qty": None,
                        "velocity_score": a.get("velocity_score"),
                        "units_sold_90d": a.get("units_sold_90d"),
                    }
        except Exception as e:
            logger.warning(f"Velocity enrichment skipped: {e}")

        return jsonify({"prices": _serialize(prices)})

    return bp
