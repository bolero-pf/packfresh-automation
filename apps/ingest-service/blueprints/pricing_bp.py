"""Auto-generated from app.py refactor. pricing routes."""
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
    enforce_offer_caps,
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
logger = logging.getLogger("intake.pricing")


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


bp = Blueprint("pricing", __name__)


def _maybe_backfill_english_name(session_id, tcgplayer_id, english_name):
    """Rewrite intake_items.product_name + set_name to English + ' (JP)' for
    JP-set cards. Sean's rule: no Japanese characters in any UI.

    Behavior by scrydex_id:
      - JP (`*_ja-*`): swap product_name + set_name to Scrydex's
        product_name_en / expansion_name_en and append ' (JP)'. Applies
        whether the saved name still has CJK characters or not — so cards
        linked before this fix still pick up the language tag.
      - Non-JP: only rewrite when the current product_name has multibyte
        chars and a clean EN form is available (legacy behavior). Leaves
        operator-customized English labels alone.

    Scoped to the session being refreshed."""
    if not tcgplayer_id or not session_id:
        return
    try:
        row = db.query_one(
            """SELECT scrydex_id, product_name_en, expansion_name_en, product_name, expansion_name
                 FROM scrydex_price_cache
                WHERE tcgplayer_id = %s
                ORDER BY (product_name_en IS NULL), fetched_at DESC
                LIMIT 1""",
            (int(tcgplayer_id),),
        )
    except Exception as e:
        logger.warning(f"Scrydex lookup for backfill TCG#{tcgplayer_id} failed: {e}")
        row = None

    scrydex_id = (row or {}).get("scrydex_id") if row else None
    is_jp = bool(scrydex_id) and "_ja-" in scrydex_id

    if is_jp:
        en_name = (row.get("product_name_en") or "").strip() or english_name or row.get("product_name") or ""
        en_set  = (row.get("expansion_name_en") or "").strip() or row.get("expansion_name") or ""
        if not en_name:
            return
        if not en_name.endswith(" (JP)"):
            en_name = en_name + " (JP)"
        if en_set and not en_set.endswith(" (JP)"):
            en_set = en_set + " (JP)"
        try:
            if en_set:
                db.execute(
                    """UPDATE intake_items
                       SET product_name = %s, set_name = %s
                       WHERE session_id = %s
                         AND tcgplayer_id = %s
                         AND (product_name <> %s OR COALESCE(set_name,'') <> %s)""",
                    (en_name, en_set, session_id, int(tcgplayer_id), en_name, en_set),
                )
            else:
                db.execute(
                    """UPDATE intake_items
                       SET product_name = %s
                       WHERE session_id = %s
                         AND tcgplayer_id = %s
                         AND product_name <> %s""",
                    (en_name, session_id, int(tcgplayer_id), en_name),
                )
        except Exception as e:
            logger.warning(f"JP name backfill failed for TCG#{tcgplayer_id}: {e}")
        return

    # Non-JP path: legacy multibyte-detection swap. Leaves EN names alone.
    if not english_name or any(ord(c) > 127 for c in english_name):
        return
    try:
        db.execute(
            """UPDATE intake_items
               SET product_name = %s
               WHERE session_id = %s
                 AND tcgplayer_id = %s
                 AND octet_length(product_name) > char_length(product_name)
                 AND product_name <> %s""",
            (english_name, session_id, int(tcgplayer_id), english_name),
        )
    except Exception as e:
        logger.warning(f"Name backfill failed for TCG#{tcgplayer_id}: {e}")



@bp.route("/api/intake/session/<session_id>/offer-percentage", methods=["POST"])
@enforce_offer_caps
def update_offer_percentage(session_id):
    """Update offer percentages and recalculate item offers.

    Accepts any combination of:
      - `cash_percentage`     — new cash split (also mirrored to legacy
                                offer_percentage for back-compat readers)
      - `credit_percentage`   — new credit split
      - `offer_percentage`    — legacy single value, treated as cash
      - `override_token`      — manager/owner PIN-derived token if the
                                caller's role can't authorize the new
                                values (see _validate_offer_caps)

    Path is kept on `/offer-percentage` to avoid breaking the running
    frontend mid-deploy; the dashboard now POSTs cash + credit on the
    same path.
    """
    data = request.json or {}
    try:
        cash_raw = data.get("cash_percentage")
        credit_raw = data.get("credit_percentage")
        legacy_raw = data.get("offer_percentage")
        cash_pct = Decimal(str(cash_raw)) if cash_raw is not None else None
        credit_pct = Decimal(str(credit_raw)) if credit_raw is not None else None
        if cash_pct is None and legacy_raw is not None:
            cash_pct = Decimal(str(legacy_raw))
    except Exception:
        return jsonify({"error": "Invalid percentage"}), 400

    if cash_pct is None and credit_pct is None:
        return jsonify({"error": "No percentage provided"}), 400

    # Cap validation runs in the @enforce_offer_caps decorator before this body.

    try:
        session = intake.update_session_percentages(
            session_id, cash_pct=cash_pct, credit_pct=credit_pct,
        )
        _log_override_if_present(data, session_id, cash_pct, credit_pct)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/session/<session_id>/bulk-tiers", methods=["POST"])
def update_bulk_tiers(session_id):
    """Replace the session's per-dollar bracket overrides and re-price every
    item. Body: `{"tiers": [{"max": 2, "pct": 25}, ...]}` (max 3 entries,
    sorted ascending; bad rows dropped). An empty list resets to the
    legacy default."""
    data = request.json or {}
    raw = data.get("tiers")
    if raw is None or not isinstance(raw, list):
        return jsonify({"error": "tiers must be a list"}), 400
    try:
        session = intake.update_session_bulk_tiers(session_id, raw)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/session/<session_id>/refresh-prices", methods=["POST"])
def refresh_session_prices(session_id):
    """
    Fetch current PPT prices for linked items in a session.
    Supports pagination: send {"offset": N} to continue from item N.
    Fires requests until rate-limited, then returns partial results
    with retry_after and next_offset so the frontend can continue.
    """
    if not pricing:
        return jsonify({"error": "PPT API not configured"}), 503

    data = request.json or {}
    offset = int(data.get("offset", 0))

    items = intake.get_session_items(session_id)
    linked = [i for i in items if i.get("tcgplayer_id") and i.get("item_status", "good") in ("good", "damaged")]

    # Deduplicate: unique (tcg_id, ptype, is_graded, grade_company, grade_value)
    seen = set()
    unique_lookups = []
    for item in linked:
        is_graded = bool(item.get("is_graded"))
        grade_co = (item.get("grade_company") or "").upper() if is_graded else ""
        grade_val = (item.get("grade_value") or "").upper() if is_graded else ""
        key = (item["tcgplayer_id"], item.get("product_type", "sealed"), is_graded, grade_co, grade_val)
        if key not in seen:
            seen.add(key)
            unique_lookups.append(key)

    # Fetch starting from offset
    price_cache = {}
    rate_limited = False
    retry_after = None
    fetched_count = 0

    for idx in range(offset, len(unique_lookups)):
        tcg_id, ptype, is_graded, grade_co, grade_val = unique_lookups[idx]

        # Check rate limit BEFORE making the request — never trigger a 429
        if pricing.should_throttle():
            rate_info = pricing.get_rate_limit_info()
            retry_after = rate_info.get("retry_after") or 60
            rate_limited = True
            logger.info(f"PPT throttle: minute_remaining={rate_info['minute_remaining']}, "
                            f"pausing at offset {idx} (fetched {fetched_count}), retry in {retry_after}s")
            break

        ppt_price = None
        ppt_low = None
        ppt_name = None
        error = None
        source = None

        try:
            if ptype == "sealed":
                ppt_data = pricing.get_sealed_product_by_tcgplayer_id(tcg_id)
            else:
                ppt_data = pricing.get_card_by_tcgplayer_id(tcg_id)

            if is_graded and grade_co and grade_val:
                # Graded pricing doesn't depend on card metadata — Scrydex has
                # graded comps keyed off tcg_id even when the card itself isn't
                # in the cache. Run this branch independently of ppt_data so
                # slabs with no Scrydex card record still get prices.
                # Per CLAUDE.md: live eBay comps → cache aggregate. Never PPT.
                try:
                    from graded_pricing import get_live_graded_comps
                    live = get_live_graded_comps(tcg_id, grade_co, grade_val, db)
                    if live:
                        mkt = live.get("market") if live.get("market") is not None else live.get("mid")
                        if mkt is not None:
                            ppt_price = Decimal(str(mkt)).quantize(Decimal("0.01"))
                            source = "scrydex_live"
                except Exception as e:
                    logger.warning(f"Live graded comps failed for {tcg_id} {grade_co} {grade_val}: {e}")
                if ppt_price is None:
                    try:
                        cp = pricing.get_graded_price(
                            tcgplayer_id=int(tcg_id), company=grade_co, grade=grade_val,
                        )
                        if cp is not None:
                            ppt_price = cp
                            source = "cache"
                    except Exception as e:
                        logger.warning(f"Cache graded lookup failed for {tcg_id} {grade_co} {grade_val}: {e}")
                if ppt_data:
                    ppt_name = ppt_data.get("nameEn") or ppt_data.get("name")
                    if not source:
                        source = ppt_data.get("_price_source", "ppt")
                ppt_low = None  # no "low" concept for graded eBay data
            elif ppt_data:
                source = ppt_data.get("_price_source", "ppt")
                if ptype == "sealed":
                    unopened = ppt_data.get("unopenedPrice")
                    prices = ppt_data.get("prices") or {}
                    if isinstance(prices, dict):
                        prices_low = prices.get("low")
                    else:
                        prices_low = None
                    ppt_price = unopened
                    ppt_low = prices_low
                    ppt_name = ppt_data.get("nameEn") or ppt_data.get("name")
                else:
                    prices = ppt_data.get("prices", {})
                    # Use market price as default; per-condition resolved per-item in comparisons below
                    ppt_price = prices.get("market")
                    ppt_low = prices.get("low")
                    ppt_name = ppt_data.get("nameEn") or ppt_data.get("name")
                    # Store full prices dict for per-condition lookup in comparisons step
                    price_cache[(tcg_id, ptype, is_graded, grade_co, grade_val)] = {
                        "ppt_price": ppt_price, "ppt_low": ppt_low, "ppt_name": ppt_name,
                        "error": None, "raw_prices": prices, "price_source": source,
                    }
                    # Backfill JP product_name with English when available.
                    _maybe_backfill_english_name(session_id, tcg_id, ppt_name)
                    fetched_count += 1
                    continue

            # Auto-translate saved product_name from JP → EN when Scrydex
            # gave us an English form. Items linked before the JP fix kept
            # their JP product_name; rerunning refresh-prices heals them.
            _maybe_backfill_english_name(session_id, tcg_id, ppt_name)

            fetched_count += 1

        except PriceError as e:
            status_code = getattr(e, 'status_code', None)
            if status_code == 429:
                # Shouldn't happen since we check should_throttle, but handle gracefully
                body = getattr(e, 'body', {}) or {}
                retry_after = body.get("retry_after", 60) if isinstance(body, dict) else 60
                rate_limited = True
                logger.warning(f"PPT 429 despite throttle check — pausing at {idx}, retry in {retry_after}s")
                break
            elif status_code == 403:
                error = str(e)
                logger.warning(f"PPT 403 for {tcg_id}: {e}")
                price_cache[(tcg_id, ptype, is_graded, grade_co, grade_val)] = {"ppt_price": None, "ppt_low": None, "ppt_name": None, "error": error}
                rate_limited = True
                retry_after = None
                break
            else:
                error = str(e)
                logger.warning(f"PPT error for {tcg_id}: {e}")
        except Exception as e:
            error = str(e)
            logger.warning(f"Unexpected error for {tcg_id}: {e}")

        price_cache[(tcg_id, ptype, is_graded, grade_co, grade_val)] = {"ppt_price": ppt_price, "ppt_low": ppt_low, "ppt_name": ppt_name, "error": error, "price_source": source if ppt_data else None}

    # Build comparisons for ALL linked items (using whatever we've fetched so far)
    comparisons = []
    for item in linked:
        tcg_id = item["tcgplayer_id"]
        ptype = item.get("product_type", "sealed")
        is_graded = bool(item.get("is_graded"))
        grade_co = (item.get("grade_company") or "").upper() if is_graded else ""
        grade_val = (item.get("grade_value") or "").upper() if is_graded else ""
        cached = price_cache.get((tcg_id, ptype, is_graded, grade_co, grade_val))

        ppt_price = cached["ppt_price"] if cached else None
        ppt_low = cached["ppt_low"] if cached else None
        ppt_name = cached.get("ppt_name") if cached else None

        # For raw ungraded cards: resolve per-condition price from cached raw_prices
        if cached and not is_graded and ptype == "raw" and cached.get("raw_prices"):
            raw_prices = cached["raw_prices"]
            condition = item.get("condition") or item.get("listing_condition") or "NM"
            cond_map = {"NM": "Near Mint", "LP": "Lightly Played", "MP": "Moderately Played",
                        "HP": "Heavily Played", "DMG": "Damaged"}
            cond_key = cond_map.get(condition.upper(), "Near Mint")
            conditions = raw_prices.get("conditions") or {}
            cond_data = conditions.get(cond_key) or {}
            cond_price = cond_data.get("price")
            if cond_price is not None:
                ppt_price = cond_price

        collectr_price = float(item.get("market_price") or 0)
        ppt_price_f = float(ppt_price) if ppt_price is not None else None
        delta_pct = None
        if ppt_price_f and collectr_price > 0:
            delta_pct = round((ppt_price_f - collectr_price) / collectr_price * 100, 1)

        # Reflect the JP→EN backfill in this same response so the operator
        # doesn't have to refresh twice. DB is updated in the loop above;
        # mirror that choice here for the in-flight comparison row.
        saved_name = item.get("product_name") or ""
        display_name = saved_name
        if (ppt_name and saved_name
                and any(ord(c) > 127 for c in saved_name)
                and not any(ord(c) > 127 for c in ppt_name)):
            display_name = ppt_name

        comparisons.append({
            "item_id": item["id"],
            "product_name": display_name,
            "ppt_name": ppt_name,
            "tcgplayer_id": tcg_id,
            "quantity": item.get("quantity", 1),
            "collectr_price": collectr_price,
            "ppt_market": ppt_price_f,
            "ppt_low": float(ppt_low) if ppt_low is not None else None,
            "delta_pct": delta_pct,
            "significant": abs(delta_pct) > 10 if delta_pct is not None else False,
            "error": cached.get("error") if cached else None,
            "fetched": cached is not None,
            "is_graded": is_graded,
            "grade_label": f"{grade_co} {grade_val}".strip() if is_graded else None,
            "condition": item.get("condition") or item.get("listing_condition"),
            "price_source": cached.get("price_source") if cached else None,
        })

    succeeded = sum(1 for c in comparisons if c.get("ppt_market") is not None)
    next_offset = offset + fetched_count

    result = {
        "comparisons": comparisons,
        "count": len(comparisons),
        "succeeded": succeeded,
        "failed": sum(1 for c in comparisons if c.get("fetched") and c.get("ppt_market") is None),
        "pending": sum(1 for c in comparisons if not c.get("fetched")),
        "total_unique": len(unique_lookups),
        "fetched_this_batch": fetched_count,
        "next_offset": next_offset,
        "complete": next_offset >= len(unique_lookups),
    }
    if rate_limited:
        result["rate_limited"] = True
        result["retry_after"] = retry_after
    return jsonify(result)



@bp.route("/api/intake/update-item-price", methods=["POST"])
def update_item_price():
    """Update an individual item's market price (from the price comparison UI)."""
    data = request.json or {}
    item_id = data.get("item_id")
    session_id = data.get("session_id")
    new_price = data.get("new_price")

    if not all([item_id, session_id, new_price]):
        return jsonify({"error": "item_id, session_id, and new_price required"}), 400

    try:
        updated = intake.update_item_price(item_id, Decimal(str(new_price)), session_id)
        return jsonify({"success": True, "item": _serialize(updated)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/intake/item/<item_id>/accept-price", methods=["POST"])
def accept_price_no_link(item_id):
    """Mark an item as resolved (is_mapped=TRUE) without a TCGPlayer ID.
    Used when PPT has no match and user accepts Collectr/market price as-is,
    or links to a Shopify store product only."""
    data = request.json or {}
    session_id = data.get("session_id")
    override_price = data.get("override_price")  # optional new price
    store_product_id = data.get("store_product_id")  # optional shopify ref
    store_product_name = data.get("store_product_name")
    tcgplayer_id = data.get("tcgplayer_id")  # if store product has a TCGPlayer ID, link it

    item = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        return jsonify({"error": "Item not found"}), 404

    session = db.query_one(
        "SELECT offer_percentage, bulk_tiers FROM intake_sessions WHERE id = %s",
        (item["session_id"],)
    )
    if not session:
        return jsonify({"error": "Session not found"}), 404

    market_price = Decimal(str(override_price)) if override_price is not None else item["market_price"]
    offer_pct = session["offer_percentage"]
    offer_price, unit_cost_basis = intake.calc_offer_price(
        market_price, item["quantity"], offer_pct,
        product_type=item.get("product_type", "raw"),
        bulk_tiers=intake._session_bulk_tiers(session))

    updated = db.execute_returning("""
        UPDATE intake_items
        SET is_mapped = TRUE,
            market_price = %s, offer_price = %s, unit_cost_basis = %s,
            tcgplayer_id = COALESCE(%s, tcgplayer_id),
            shopify_product_id = COALESCE(%s, shopify_product_id),
            shopify_product_name = COALESCE(%s, shopify_product_name)
        WHERE id = %s
        RETURNING *
    """, (market_price, offer_price, unit_cost_basis,
          tcgplayer_id or None,
          str(store_product_id) if store_product_id else None,
          store_product_name or None,
          item_id))

    if not updated:
        return jsonify({"error": "Update failed"}), 500

    # Persist the mapping so future imports of the same product name auto-link
    intake.save_mapping(
        item["product_name"],
        tcgplayer_id or None,
        item.get("product_type", "sealed"),
        set_name=item.get("set_name"),
        card_number=item.get("card_number"),
        variance=item.get("variance") or "",
        shopify_product_id=str(store_product_id) if store_product_id else None,
        shopify_product_name=store_product_name or None,
    )

    intake._recalculate_session_totals(item["session_id"])
    return jsonify({"success": True, "item": _serialize(updated)})


# ==========================================
# RAW CARD MANUAL ENTRY
# ==========================================


@bp.route("/api/intake/item/<item_id>/override-price", methods=["POST"])
def override_price(item_id):
    """Override an item's market price with a note."""
    data = request.get_json(silent=True) or {}
    new_price = data.get("new_price")
    note = data.get("note", "")
    session_id = data.get("session_id")

    if new_price is None or not session_id:
        return jsonify({"error": "new_price and session_id required"}), 400

    try:
        item = intake.override_item_price(item_id, Decimal(str(new_price)), note, session_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/apply-breakdown-price", methods=["POST"])
def apply_breakdown_price(item_id):
    """
    Reprice an item using its breakdown value instead of whole-unit market price.
    If breakdown_qty < item quantity, splits the item first: breakdown_qty units
    get the breakdown price, the remainder stay at their original price.
    Body: {session_id, variant_name, breakdown_total, breakdown_qty}
    """
    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id")
    variant_name = data.get("variant_name", "breakdown")
    breakdown_total = data.get("breakdown_total")
    breakdown_qty = int(data.get("breakdown_qty") or 1)

    if not session_id or breakdown_total is None:
        return jsonify({"error": "session_id and breakdown_total required"}), 400

    try:
        item = intake.get_item(item_id)
        if not item:
            return jsonify({"error": "Item not found"}), 404

        current_qty = item.get("quantity", 1)
        note = f"Priced as breakdown ({variant_name})"

        if breakdown_qty >= current_qty:
            # Apply to whole item
            updated = intake.override_item_price(
                item_id, Decimal(str(breakdown_total)), note, session_id
            )
            return jsonify({"success": True, "item": _serialize(updated)})
        else:
            # Split: reduce original item to remainder qty, create new item for breakdown qty
            remainder_qty = current_qty - breakdown_qty
            intake.update_item_quantity(item_id, remainder_qty, session_id)

            # Clone the item with breakdown_qty and breakdown price
            new_item = intake.clone_item_with_overrides(
                item_id, session_id,
                quantity=breakdown_qty,
                market_price=Decimal(str(breakdown_total)),
                notes=note
            )
            return jsonify({"success": True, "split": True, "item": _serialize(new_item), "remainder_qty": remainder_qty})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
