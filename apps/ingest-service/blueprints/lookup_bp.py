"""Auto-generated from app.py refactor. lookup routes."""
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
from price_provider import PriceError
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
logger = logging.getLogger("intake.lookup")


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


bp = Blueprint("lookup", __name__)

from sealed_tcg_enrichment import enrich_sealed_with_shopify_tcg as _enrich_sealed_with_shopify_tcg_shared


def _enrich_sealed_with_shopify_tcg(results: list):
    _enrich_sealed_with_shopify_tcg_shared(results, db, price_provider=pricing)



@bp.route("/api/lookup/card", methods=["POST"])
def lookup_card():
    """Look up a raw card by tcgplayer_id. Returns card data + variant/condition prices."""
    if not pricing:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    tcgplayer_id = data.get("tcgplayer_id")
    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    # Only fetch live eBay comps for the specific (company, grade) the caller
    # asks about. Enriching every grade is a 20+ paginated Scrydex calls and
    # blocks the lookup spinner indefinitely.
    req_company = (data.get("grade_company") or "").strip().upper() or None
    req_grade   = (data.get("grade_value")   or "").strip() or None

    try:
        view = pricing.get_card_view(tcgplayer_id=int(tcgplayer_id))
        if not view:
            return jsonify({"error": "Card not found"}), 404

        logger.info(f"CARD LOOKUP {tcgplayer_id}: "
                         f"variants={list((view.get('variants') or {}).keys())} "
                         f"primary={view.get('primary_variant')}")

        live_graded = {}
        if req_company and req_grade:
            try:
                from graded_pricing import get_live_graded_comps
                live = get_live_graded_comps(int(tcgplayer_id), req_company, req_grade, db)
                if live:
                    live_graded[req_company] = {req_grade: live}
            except Exception as e:
                logger.warning(f"Live graded enrichment failed for TCG#{tcgplayer_id} {req_company} {req_grade}: {e}")

        return jsonify({
            "card": view,
            "variants": view.get("variants") or {},
            "primary_printing": view.get("primary_variant"),
            "graded_prices": view.get("graded") or {},
            "live_graded": live_graded,
        })
    except PriceError as e:
        return jsonify({"error": str(e)}), 502



@bp.route("/api/lookup/sealed", methods=["POST"])
def lookup_sealed():
    """Look up a sealed product by tcgplayer_id via PPT."""
    if not pricing:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    tcgplayer_id = data.get("tcgplayer_id")
    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    try:
        product_data = pricing.get_sealed_product_by_tcgplayer_id(int(tcgplayer_id))
        if not product_data:
            return jsonify({"error": f"Sealed product TCG#{tcgplayer_id} not found in cache, Scrydex, or PPT — may be too new to have a mapping yet"}), 404

        # Extract market price with multiple fallback paths
        market_price = None
        if isinstance(product_data.get("prices"), dict):
            market_price = product_data["prices"].get("market")
        if market_price is None:
            market_price = product_data.get("market_price") or product_data.get("marketPrice") or product_data.get("price")

        return jsonify({
            "product": product_data,
            "extracted_price": market_price,  # explicitly extracted for the frontend
        })
    except PriceError as e:
        return jsonify({"error": str(e)}), 502



@bp.route("/api/lookup/debug-card/<int:tcgplayer_id>")
def debug_card_raw(tcgplayer_id):
    """Debug: dump raw PPT response for a card — bare HTTP, no abstraction."""
    import requests as _requests
    if not pricing:
        return jsonify({"error": "PPT not configured"}), 503

    results = {}
    base = f"{pricing.base_url}/v2/cards"
    combos = {
        "bare":         {"tcgPlayerId": str(tcgplayer_id), "limit": 1},
        "includeEbay":  {"tcgPlayerId": str(tcgplayer_id), "limit": 1, "includeEbay": "true"},
        "includeBoth":  {"tcgPlayerId": str(tcgplayer_id), "limit": 1, "includeHistory": "true", "includeEbay": "true"},
    }
    for label, params in combos.items():
        try:
            r = _requests.get(base, headers=pricing.headers, params=params, timeout=15)
            results[label] = {
                "status": r.status_code,
                "url": r.url,
                "body": r.json() if r.headers.get("content-type","").startswith("application/json") else r.text[:500],
            }
        except Exception as e:
            import traceback
            results[label] = {"error": str(e), "type": type(e).__name__, "tb": traceback.format_exc()}
    return jsonify(results)



@bp.route("/api/lookup/debug-sealed/<int:tcgplayer_id>")
def debug_sealed(tcgplayer_id):
    """Debug: compare search vs direct lookup for a sealed product."""
    if not pricing:
        return jsonify({"error": "PPT not configured"}), 503
    results = {}
    
    # Test 1: Direct lookup by tcgPlayerId
    try:
        url = f"{pricing.base_url}/v2/sealed-products"
        params = {"tcgPlayerId": str(tcgplayer_id)}
        raw = pricing._get(url, params)
        results["direct_lookup"] = {
            "url": f"{url}?tcgPlayerId={tcgplayer_id}",
            "response": raw,
        }
    except PriceError as e:
        results["direct_lookup"] = {"error": str(e), "status": e.status_code}

    # Test 2: Search (to compare structure)
    try:
        url2 = f"{pricing.base_url}/v2/sealed-products"
        params2 = {"search": "Elite Trainer Box", "limit": 1}
        raw2 = pricing._get(url2, params2)
        results["search_example"] = {
            "url": f"{url2}?search=Elite+Trainer+Box&limit=1",
            "response": raw2,
        }
    except PriceError as e:
        results["search_example"] = {"error": str(e), "status": e.status_code}

    return jsonify(results)



@bp.route("/api/search/sealed", methods=["POST"])
def search_sealed():
    """Search for sealed products by name across all TCGs.
    Cache first (multi-TCG: pokemon, onepiece, lorcana, mtg, riftbound),
    then live PPT/Scrydex fallback for the configured game.
    Pass live=true to skip cache.
    """
    if not pricing:
        return jsonify({"error": "PPT API not configured"}), 503
    data = request.get_json(silent=True) or {}
    q = data.get("query", "").strip()
    if not q:
        return jsonify({"error": "No query"}), 400
    live_only = data.get("live", False)
    try:
        results = []
        cache = getattr(pricing, "cache", None)
        if cache and not live_only:
            try:
                results = cache.search_sealed_products(q, limit=5, all_games=True)
                results = pricing._stamp(results, "cache")
            except Exception as e:
                logger.warning(f"Cross-game sealed cache search failed: {e}")
                results = []
        if not results:
            live = pricing.primary.search_sealed_products(q, limit=5) or []
            results = pricing._stamp(live, pricing._primary_source)
        for r in (results or []):
            if not r.get("tcgplayer_id"):
                tcg_id = r.get("tcgplayerId") or r.get("tcgPlayerId") or r.get("id")
                if tcg_id:
                    try:
                        r["tcgplayer_id"] = int(tcg_id)
                    except (TypeError, ValueError):
                        pass
        _enrich_sealed_with_shopify_tcg(results or [])
        return jsonify({"results": results or []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@bp.route("/api/search/cards", methods=["POST"])
def search_cards():
    """Search for individual cards by name across all TCGs. Thin wrapper —
    `pricing.search_cards()` (shared/price_provider.py) owns the cache→live
    orchestration; raw-rebind in price_updater calls the same method."""
    if not pricing:
        return jsonify({"error": "PPT API not configured"}), 503
    data = request.get_json(silent=True) or {}
    q = data.get("query", "").strip()
    if not q:
        return jsonify({"error": "No query"}), 400
    try:
        return jsonify({"results": pricing.search_cards(
            q,
            set_name=data.get("set_name") or None,
            limit=int(data.get("limit") or 8),
            all_games=True,
        )})
    except PriceError as e:
        return jsonify({"error": str(e)}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@bp.route("/api/lookup/parse-title", methods=["POST"])
def parse_title():
    """Fuzzy-match a product name via PPT's parse-title endpoint (best for card titles)."""
    if not pricing:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    title = data.get("title", "").strip()
    if not title:
        return jsonify({"error": "title required"}), 400

    matches = pricing.parse_title(title)
    return jsonify({"matches": matches})


# ==========================================
# PRODUCT MAPPINGS
# ==========================================
