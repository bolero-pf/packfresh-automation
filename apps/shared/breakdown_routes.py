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
        ppt_getter: callable returning a PPTClient instance (or None to disable PPT features)
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

        # JIT refresh stale component market prices in background
        ppt = _get_ppt()
        if ppt:
            try:
                from breakdown_helpers import refresh_stale_component_prices
                variant_ids = [str(v["id"]) for v in result.get("variants", [])]
                if variant_ids:
                    threading.Thread(
                        target=refresh_stale_component_prices,
                        args=(variant_ids, db_module, ppt),
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
        summaries = logic.get_breakdown_summary_for_items(
            tcg_ids, db_module, ppt=ppt, max_age_hours=24
        )
        return jsonify({"summaries": _serialize(summaries)})

    # ─── PPT search (sealed products) ──────────────────────────────

    @bp.route("/search")
    def search_sealed():
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"results": []})
        ppt = _get_ppt()
        if not ppt:
            return jsonify({"results": [], "error": "PPT not configured"}), 503
        try:
            results = ppt.search_sealed_products(q, limit=10)
            # Normalize tcgplayer_id field
            for r in results:
                if not r.get("tcgplayer_id"):
                    tcg_id = r.get("tcgplayerId") or r.get("tcgPlayerId") or r.get("id")
                    if tcg_id:
                        r["tcgplayer_id"] = int(tcg_id)
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
