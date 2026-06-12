"""Auto-generated from app.py refactor. items routes."""
import os
import re
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

try:
    # Variant name normalization shared with the cache layer so auto-link
    # stores the same display variant the manual picker would ("holofoil" →
    # "Holofoil") and matches the item's Collectr variance against native rows.
    from price_cache import _to_native_variant as _native_variant, VARIANT_DISPLAY as _VARIANT_DISPLAY
except Exception:  # pragma: no cover - defensive; price_cache always on path
    _VARIANT_DISPLAY = {}
    def _native_variant(name):
        return name
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
logger = logging.getLogger("intake.items")


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


bp = Blueprint("items", __name__)



@bp.route("/api/intake/map-item", methods=["POST"])
def map_item():
    """Map an intake item to a Scrydex product (preferred) and/or a TCGplayer ID,
    with optional price override.

    scrydex_id is the canonical linker; tcgplayer_id is a property used by
    the price_updater to crawl TCGplayer for current low. At least one is
    required. Sealed products and many JP cards have no TCGplayer mapping
    in Scrydex's data — those link by scrydex_id alone.
    """
    data = request.json or {}
    item_id = data.get("item_id")
    tcgplayer_id = data.get("tcgplayer_id")
    scrydex_id = (data.get("scrydex_id") or "").strip() or None

    if not item_id or (not tcgplayer_id and not scrydex_id):
        return jsonify({"error": "item_id plus tcgplayer_id or scrydex_id required"}), 400

    if tcgplayer_id:
        try:
            tcgplayer_id = int(tcgplayer_id)
        except (ValueError, TypeError):
            return jsonify({"error": "tcgplayer_id must be an integer"}), 400
    else:
        tcgplayer_id = None

    # Price override from the comparison UI (user picked Collectr, PPT, or custom)
    new_price = None
    override_price = data.get("override_price")
    if override_price is not None:
        try:
            new_price = Decimal(str(override_price))
        except Exception:
            pass

    # Legacy: verify_price still works if called directly. Only meaningful
    # when a TCGplayer ID is supplied (PPT lookups are TCG-keyed).
    if new_price is None and data.get("verify_price") and pricing and tcgplayer_id:
        item = db.query_one("SELECT product_type FROM intake_items WHERE id = %s", (item_id,))
        if item:
            try:
                if item["product_type"] == "sealed":
                    new_price = pricing.get_sealed_market_price(tcgplayer_id)
                else:
                    new_price = pricing.get_raw_condition_price(
                        tcgplayer_id=tcgplayer_id, condition="NM",
                    )
            except PriceError as e:
                logger.warning(f"Price verification failed for {tcgplayer_id}: {e}")

    try:
        updated = intake.map_item(
            item_id, tcgplayer_id, new_price,
            product_name=data.get("product_name"),
            set_name=data.get("set_name"),
            card_number=data.get("card_number"),
            rarity=data.get("rarity"),
            variance=data.get("variance"),
            scrydex_id=scrydex_id,
        )

        # Auto-link other unmapped items in the same session with the same product_name
        # AND same set_name + card_number + condition/grade
        siblings_updated = 0
        session_id = data.get("session_id") or updated.get("session_id")
        if session_id and new_price is not None:
            # Fetch the source item to know its condition/grade for sibling matching
            source_item = db.query_one(
                "SELECT is_graded, grade_company, grade_value, condition, set_name, card_number, variance FROM intake_items WHERE id = %s",
                (item_id,)
            )
            src_name = updated.get("product_name") or data.get("product_name", "")
            src_set = updated.get("set_name") or (source_item or {}).get("set_name") or ""
            src_num = updated.get("card_number") or (source_item or {}).get("card_number") or ""
            src_var = (source_item or {}).get("variance") or ""
            if source_item and source_item.get("is_graded"):
                # Graded: only auto-link siblings with same name+set+number+variance+company+grade
                siblings = db.query("""
                    SELECT id FROM intake_items
                    WHERE session_id = %s
                      AND id != %s
                      AND product_name = %s
                      AND COALESCE(set_name, '') = %s
                      AND COALESCE(card_number, '') = %s
                      AND COALESCE(variance, '') = %s
                      AND is_graded = TRUE
                      AND grade_company = %s
                      AND grade_value = %s
                      AND (tcgplayer_id IS NULL OR is_mapped = FALSE)
                      AND item_status IN ('good', 'damaged')
                """, (session_id, item_id, src_name, src_set, src_num, src_var,
                      source_item.get("grade_company", ""),
                      source_item.get("grade_value", "")))
            else:
                # Raw: only auto-link siblings with same name+set+number+variance+condition
                source_cond = (source_item or {}).get("condition") or "NM"
                siblings = db.query("""
                    SELECT id FROM intake_items
                    WHERE session_id = %s
                      AND id != %s
                      AND product_name = %s
                      AND COALESCE(set_name, '') = %s
                      AND COALESCE(card_number, '') = %s
                      AND COALESCE(variance, '') = %s
                      AND (is_graded = FALSE OR is_graded IS NULL)
                      AND COALESCE(condition, 'NM') = %s
                      AND (tcgplayer_id IS NULL OR is_mapped = FALSE)
                      AND item_status IN ('good', 'damaged')
                """, (session_id, item_id, src_name, src_set, src_num, src_var,
                      source_cond))
            for sib in siblings:
                try:
                    intake.map_item(
                        sib["id"], tcgplayer_id, new_price,
                        product_name=data.get("product_name"),
                        set_name=data.get("set_name"),
                        card_number=data.get("card_number"),
                        rarity=data.get("rarity"),
                        variance=data.get("variance"),
                        scrydex_id=scrydex_id,
                    )
                    siblings_updated += 1
                except Exception:
                    pass

        return jsonify({
            "success": True,
            "item": _serialize(updated),
            "price_updated": new_price is not None,
            "siblings_linked": siblings_updated,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


# ==========================================
# AUTO-LINK — batch-link the unambiguous raw cards
# ==========================================

# Tokens that don't help distinguish one card from another for the Tier-B
# name-overlap guard (every Pokemon card is "ex"/"V"/etc.).
_AUTOLINK_STOPWORDS = {"the", "of", "and", "ex", "gx", "v", "vmax", "vstar",
                       "delta", "species", "full", "art", "secret", "promo"}


def _autolink_cardnum_forms(raw):
    """Card-number comparison forms. Scrydex stores numbers unpadded and
    without the '/total' suffix (Collectr '011/025' → cache '11'), so compare
    against both the head token and its leading-zero-stripped form."""
    s = (raw or "").strip()
    if not s:
        return []
    head = s.split("/")[0].strip()
    if not head:
        return []
    forms = {head}
    if head.isdigit():
        forms.add(head.lstrip("0") or "0")
    return list(forms)


def _autolink_name_tokens(name):
    return {t for t in re.split(r"[^a-z0-9]+", (name or "").lower())
            if len(t) >= 3 and t not in _AUTOLINK_STOPWORDS}


def _autolink_match(item):
    """Return a confident link plan for an unmapped raw item, or None.

    Conservative on purpose — mirrors heal_raw_card_bindings' abstain-on-
    ambiguity rule. A match is returned only when BOTH resolve to exactly one
    option: (1) the scrydex_id, via exact expansion+number or number-guarded
    fuzzy search, and (2) the variant, via the item's Collectr variance or a
    lone priced printing. Anything else stays manual. Never auto-links a card
    with no card number (no anchor) or a graded-only printing (no raw price).
    """
    game = (item.get("game") or "pokemon").lower()
    item_game = (item.get("game") or "").strip().lower() or None  # known game, no pokemon default
    set_name = item.get("set_name") or ""
    num_forms = _autolink_cardnum_forms(item.get("card_number"))
    if not num_forms:
        return None

    # A card number is NOT a stable cross-catalog key. Collector numbers are
    # unique-and-stable within a Pokemon set, but MTG renumbers variant cards
    # between TCGplayer/Collectr exports and Scrydex (a Collectr "Aetherdrift #92"
    # borderless is Scrydex "Intimidation Tactics #92"). So every tier now also
    # requires a name-token overlap — without it, expansion+number alone links
    # confidently to whatever unrelated card sits at that number in Scrydex.
    tokens = sorted(_autolink_name_tokens(item.get("product_name")), key=len, reverse=True)[:3]
    if not tokens:
        return None

    sid = None
    tier = None

    # English-only: both tiers require COALESCE(language_code,'EN')='EN'. Collectr
    # imports even Japanese cards as language 'EN', so we can't trust the item's
    # language — but linking an English card to a JP printing (when only the JP
    # one is cached) is wrong, and JP cards need the (JP)-suffix handling that
    # only the manual picker applies. So auto-link sticks to English printings
    # and leaves Japanese cards for manual.

    # Tier A — exact expansion name + number + a shared name token. Still mostly
    # name-INdependent (one overlapping token is enough), so it catches the
    # Collectr-vs-Scrydex drift ("Mew (Delta Species)" vs "Mew δ" share "mew";
    # "Charizard VMAX (Rainbow)" vs "Charizard VMAX" share "charizard") while
    # refusing a number-only match to a totally different card. Drives off the
    # partial index idx_spc_lower_expansion_name — do NOT add
    # `OR lower(expansion_name_en)` here: that column is unindexed and the OR
    # forces a seq scan of the whole 1M-row cache per item (the bug that timed
    # the worker out). The number/name filters run on the few hundred rows the
    # expansion index already narrowed to.
    name_ors = []
    name_params: list = []
    for t in tokens:
        name_ors.append("product_name ILIKE %s")
        name_ors.append("product_name_en ILIKE %s")
        name_params.extend([f"%{t}%", f"%{t}%"])
    name_overlap = "(" + " OR ".join(name_ors) + ")"

    if set_name:
        game_clause = "AND game = %s " if item_game else ""
        rows = db.query(
            "SELECT DISTINCT scrydex_id FROM scrydex_price_cache "
            "WHERE product_type='card' AND price_type='raw' AND market_price IS NOT NULL "
            "AND COALESCE(language_code, 'EN') = 'EN' "
            + game_clause +
            "AND lower(expansion_name) = lower(%s) "
            "AND (card_number = ANY(%s::text[]) OR printed_number = ANY(%s::text[])) "
            "AND " + name_overlap,
            tuple(([item_game] if item_game else []) + [set_name, num_forms, num_forms] + name_params),
        )
        if len(rows) == 1:
            sid, tier = rows[0]["scrydex_id"], "set+number"

    # Tier B — same number + EVERY significant name token, game-scoped. Catches
    # sets whose Collectr name differs from Scrydex's ("Sword & Shield Promo" →
    # "SWSH Black Star Promos") while still abstaining if the number+name match
    # points at more than one card. Direct indexed query (product_name trigram +
    # number) instead of the heavy scored search_cards — same exactly-one
    # confidence, a fraction of the cost.
    if not sid:
        where = ["product_type='card'", "price_type='raw'", "market_price IS NOT NULL",
                 "COALESCE(language_code, 'EN') = 'EN'",
                 "game = %s",
                 "(card_number = ANY(%s::text[]) OR printed_number = ANY(%s::text[]))"]
        params = [game, num_forms, num_forms]
        for t in tokens:
            where.append("(product_name ILIKE %s OR product_name_en ILIKE %s)")
            params.extend([f"%{t}%", f"%{t}%"])
        rows = db.query(
            "SELECT DISTINCT scrydex_id FROM scrydex_price_cache WHERE "
            + " AND ".join(where) + " LIMIT 5",
            tuple(params),
        )
        if len(rows) == 1:
            sid, tier = rows[0]["scrydex_id"], "name+number"

    if not sid:
        return None

    # Variant resolution — distinct priced raw printings for this card.
    vrows = db.query(
        """
        SELECT variant,
               (ARRAY_AGG(tcgplayer_id) FILTER (WHERE tcgplayer_id IS NOT NULL))[1] AS tcgplayer_id
        FROM scrydex_price_cache
        WHERE scrydex_id = %s AND product_type='card' AND price_type='raw'
          AND market_price IS NOT NULL
        GROUP BY variant
        """,
        (sid,),
    )
    if not vrows:
        return None
    variants = {(r["variant"] or "normal"): r.get("tcgplayer_id") for r in vrows}
    native = _native_variant(item.get("variance"))
    if native and native in variants:
        chosen = native
    elif len(variants) == 1:
        chosen = next(iter(variants))
    else:
        # e.g. Normal vs Holofoil both exist but the item's variance doesn't
        # name one — exactly the case the operator must eyeball. Leave it.
        return None

    cond = (item.get("condition") or "NM").upper()
    try:
        price = pricing.get_raw_condition_price(scrydex_id=sid, condition=cond, variant=chosen)
        if price is None and cond != "NM":
            price = pricing.get_raw_condition_price(scrydex_id=sid, condition="NM", variant=chosen)
    except Exception:
        price = None
    if price is None:
        return None

    meta = db.query_one(
        """
        SELECT product_name, product_name_en, expansion_name, expansion_name_en,
               card_number, rarity
        FROM scrydex_price_cache
        WHERE scrydex_id = %s AND product_type='card'
        ORDER BY price_type, condition LIMIT 1
        """,
        (sid,),
    ) or {}

    return {
        "scrydex_id": sid,
        "tcgplayer_id": variants.get(chosen),
        "variant": _VARIANT_DISPLAY.get(chosen, chosen),
        "price": float(price),
        "tier": tier,
        "new_name": meta.get("product_name_en") or meta.get("product_name"),
        "new_set": meta.get("expansion_name_en") or meta.get("expansion_name"),
        "new_number": meta.get("card_number"),
        "new_rarity": meta.get("rarity"),
    }


@bp.route("/api/intake/session/<session_id>/auto-link", methods=["POST"])
def auto_link_session(session_id):
    """Batch-link the unambiguous unmapped raw cards in a session to Scrydex.

    Body: {apply: bool, limit?: int, offset?: int}. apply=false previews a
    window without writing; apply=true links the matched items via
    intake.map_item (offer recalc + re-link cache write happen there, same as
    the manual picker). Processed in windows so a 1000-card session can't time
    a single request out — the client walks the windows.

    Pagination note: in apply mode linked items leave the unmapped set, so the
    client must advance `offset` by the returned `skipped` (not `processed`);
    in preview mode advance by `processed`. Stop when `processed < limit`.
    """
    data = request.json or {}
    do_apply = bool(data.get("apply"))
    try:
        limit = max(1, min(int(data.get("limit") or 150), 500))
    except (ValueError, TypeError):
        limit = 150
    try:
        offset = max(0, int(data.get("offset") or 0))
    except (ValueError, TypeError):
        offset = 0

    total_row = db.query_one(
        """
        SELECT COUNT(*) AS n FROM intake_items
        WHERE session_id = %s AND is_mapped = FALSE
          AND (is_graded = FALSE OR is_graded IS NULL)
          AND product_type = 'raw' AND item_status IN ('good', 'damaged')
        """,
        (session_id,),
    )
    total_unmapped = (total_row or {}).get("n", 0)

    items = db.query(
        """
        SELECT * FROM intake_items
        WHERE session_id = %s AND is_mapped = FALSE
          AND (is_graded = FALSE OR is_graded IS NULL)
          AND product_type = 'raw' AND item_status IN ('good', 'damaged')
        ORDER BY created_at, id
        LIMIT %s OFFSET %s
        """,
        (session_id, limit, offset),
    )

    sample, linked, matched_n = [], 0, 0
    for it in items:
        try:
            plan = _autolink_match(it)
        except Exception as e:
            logger.warning(f"auto-link match failed for {it.get('id')}: {e}")
            plan = None
        if not plan:
            continue
        matched_n += 1
        if len(sample) < 25:
            imp = float(it.get("market_price") or 0)
            sample.append({
                "from": f"{it.get('product_name') or ''}"
                        f"{(' #' + it['card_number']) if it.get('card_number') else ''}"
                        f"{(' · ' + it['variance']) if it.get('variance') else ''}"
                        f" ({it.get('condition') or 'NM'})"
                        f"{f' — kept ${imp:.2f}' if imp > 0 else ''}",
                "to": f"{plan['new_name'] or ''} · {plan['variant']} · mkt ${plan['price']:.2f}",
                "tier": plan["tier"],
            })
        if do_apply:
            try:
                # new_market_price=None on purpose: auto-link only establishes
                # the Scrydex link + identity + variant. It KEEPS the imported
                # Collectr/CSV/HTML price as market_price so the Market Prices
                # tab can still show imported-vs-market deltas for triage —
                # overwriting it with Scrydex market would zero out that delta
                # and kill the "where were we high/low" step. Pricing stays the
                # operator's call.
                intake.map_item(
                    str(it["id"]),
                    tcgplayer_id=plan["tcgplayer_id"],
                    new_market_price=None,
                    product_name=plan["new_name"],
                    set_name=plan["new_set"],
                    card_number=plan["new_number"],
                    rarity=plan["new_rarity"],
                    variance=plan["variant"],
                    scrydex_id=plan["scrydex_id"],
                )
                linked += 1
            except Exception as e:
                logger.warning(f"auto-link apply failed for {it.get('id')}: {e}")

    return jsonify({
        "success": True,
        "applied": do_apply,
        "processed": len(items),
        "limit": limit,
        "matched": matched_n,
        "linked": linked,
        "skipped": len(items) - linked,
        "total_unmapped": total_unmapped,
        "sample": sample,
    })


# ==========================================
# ACCEPT PRICE WITHOUT TCG LINK
# ==========================================


@bp.route("/api/intake/add-raw-card", methods=["POST"])
def add_raw_card():
    """Add a single raw card to a session. tcgplayer_id is now optional so
    staff can enter cards Scrydex doesn't track (MTG PEOE promos, prerelease
    stamps, Scrydex-only JP) manually — in that case the client must supply
    card_name, condition, quantity, and market_price, and the item lands as
    unmapped for staff to relink later if the card shows up in Scrydex.
    """
    data = request.json or {}

    required = ["session_id", "card_name", "condition", "quantity"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    tcgplayer_id = None
    if data.get("tcgplayer_id"):
        try:
            tcgplayer_id = int(data["tcgplayer_id"])
        except (ValueError, TypeError) as e:
            return jsonify({"error": f"Invalid tcgplayer_id: {e}"}), 400
    scrydex_id = (data.get("scrydex_id") or "").strip() or None
    try:
        quantity = int(data["quantity"])
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid quantity: {e}"}), 400

    # No tcg_id → caller must supply market_price; we can't look it up.
    if tcgplayer_id is None and not data.get("market_price"):
        return jsonify({"error": "market_price is required when tcgplayer_id is not provided"}), 400

    # Get price — only when we have a tcg_id to look up.
    market_price = None
    if pricing and tcgplayer_id is not None:
        try:
            is_graded = bool(data.get("is_graded", False))
            grade_company = (data.get("grade_company") or "").strip()
            grade_value = (data.get("grade_value") or "").strip()

            if is_graded and grade_company and grade_value:
                # Live eBay comps via Scrydex listings — cache aggregates are unreliable
                try:
                    from graded_pricing import get_live_graded_comps
                    live = get_live_graded_comps(tcgplayer_id, grade_company, grade_value, db)
                    if live and live.get("mid"):
                        market_price = Decimal(str(live["mid"])).quantize(Decimal("0.01"))
                        logger.info(
                            f"Live graded comps for TCG#{tcgplayer_id} {grade_company} {grade_value}: "
                            f"median ${live['mid']} ({live.get('comps_count', '?')} comps, {live.get('source')})"
                        )
                except Exception as e:
                    logger.warning(f"Live graded pricing failed for TCG#{tcgplayer_id}: {e}")
                # Fallback to PPT/cache aggregate if live didn't return anything
                if market_price is None:
                    market_price = pricing.get_graded_price(
                        tcgplayer_id=int(tcgplayer_id),
                        company=grade_company, grade=grade_value,
                    )
                if market_price is None:
                    logger.warning(
                        f"No graded price for {tcgplayer_id} {grade_company} {grade_value}, "
                        "falling back to NM raw price"
                    )
                    market_price = pricing.get_raw_condition_price(
                        tcgplayer_id=int(tcgplayer_id), condition="NM",
                    )
            else:
                # Pass variance so non-Pokemon cards (One Piece Alt Art etc.)
                # get the variant-specific price, not the primary printing's
                variance_for_price = (data.get("variance") or "").strip() or None
                market_price = pricing.get_raw_condition_price(
                    tcgplayer_id=int(tcgplayer_id),
                    condition=data["condition"],
                    variant=variance_for_price,
                )

            # Enrich metadata fields the caller didn't provide.
            meta = pricing.get_card_metadata(tcgplayer_id=int(tcgplayer_id))
            if meta:
                if not data.get("set_name") and meta.get("expansion_name"):
                    data["set_name"] = meta["expansion_name"]
                if not data.get("card_number") and meta.get("card_number"):
                    data["card_number"] = meta["card_number"]
                if not data.get("rarity") and meta.get("rarity"):
                    data["rarity"] = meta["rarity"]
                if not data.get("card_name") and meta.get("name"):
                    data["card_name"] = meta["name"]
        except PriceError as e:
            logger.warning(f"PPT lookup failed for {tcgplayer_id}: {e}")

    # Allow manual price override
    if data.get("market_price"):
        try:
            market_price = Decimal(str(data["market_price"]))
        except InvalidOperation:
            return jsonify({"error": "Invalid market_price"}), 400

    if market_price is None:
        return jsonify({
            "error": "Could not determine price. PPT lookup failed and no manual price provided.",
        }), 400

    # Get session's offer percentage
    session = db.query_one(
        "SELECT offer_percentage FROM intake_sessions WHERE id = %s",
        (data["session_id"],)
    )
    if not session:
        return jsonify({"error": "Session not found"}), 404

    item = intake.add_single_raw_item(
        session_id=data["session_id"],
        product_name=data["card_name"],
        tcgplayer_id=tcgplayer_id,
        scrydex_id=scrydex_id,
        set_name=data.get("set_name", ""),
        card_number=data.get("card_number", ""),
        condition=data["condition"],
        rarity=data.get("rarity", ""),
        quantity=quantity,
        market_price=market_price,
        offer_percentage=session["offer_percentage"],
        is_graded=bool(data.get("is_graded", False)),
        grade_company=data.get("grade_company", "") or "",
        grade_value=data.get("grade_value", "") or "",
        variance=(data.get("variance") or "").strip(),
        game=(data.get("game") or "").strip().lower() or None,
    )

    # Recalculate session totals
    intake._recalculate_session_totals(data["session_id"])

    return jsonify({
        "success": True,
        "item": _serialize(item),
        "market_price": float(market_price),
    })


# ==========================================
# ITEM STATUS MANAGEMENT
# ==========================================


@bp.route("/api/intake/item/<item_id>/damage", methods=["POST"])
def damage_item(item_id):
    """Split item into good + damaged quantities."""
    data = request.get_json(silent=True) or {}
    damaged_qty = data.get("damaged_qty", 1)
    try:
        result = intake.split_damaged(item_id, int(damaged_qty))
        return jsonify({
            "success": True,
            "original_item": _serialize(result["original_item"]),
            "session": _serialize(result["session"]),
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/status", methods=["POST"])
def set_item_status(item_id):
    """Set item status to any valid value."""
    data = request.get_json(silent=True) or {}
    new_status = data.get("status")
    if new_status not in ("good", "damaged", "missing", "rejected"):
        return jsonify({"error": f"Invalid status: {new_status}"}), 400
    try:
        if new_status == "missing":
            item = intake.mark_item_missing(item_id)
        elif new_status == "rejected":
            item = intake.mark_item_rejected(item_id)
        elif new_status == "good":
            item = intake.restore_item(item_id)
        elif new_status == "damaged":
            item = intake.mark_item_damaged(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/missing", methods=["POST"])
def missing_item(item_id):
    """Mark item as missing."""
    try:
        item = intake.mark_item_missing(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/rejected", methods=["POST"])
def rejected_item(item_id):
    """Mark item as rejected (seller kept it)."""
    try:
        item = intake.mark_item_rejected(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/restore", methods=["POST"])
def restore_item(item_id):
    """Restore a missing/rejected/damaged item back to good."""
    try:
        item = intake.restore_item(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/delete", methods=["POST"])
def delete_item(item_id):
    """Permanently delete an item from a session."""
    try:
        session = intake.delete_item(item_id)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/update-quantity", methods=["POST"])
def update_quantity(item_id):
    """Update an item's quantity."""
    data = request.get_json(silent=True) or {}
    new_qty = data.get("quantity")
    session_id = data.get("session_id")
    if new_qty is None or not session_id:
        return jsonify({"error": "quantity and session_id required"}), 400
    try:
        item = intake.update_item_quantity(item_id, int(new_qty), session_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/claim-variant", methods=["POST"])
def claim_variant(item_id):
    """
    Lock an intake row to a specific breakdown variant ("seller said it's the
    Kanto one"), or clear the claim. Used by probabilistic recipes where the
    operator wants to use that variant's exact value for offer math instead
    of the recipe's average.

    Body: { "variant_id": "<uuid>" } to lock, or {} / { "variant_id": null } to clear.
    """
    data = request.get_json(silent=True) or {}
    variant_id = data.get("variant_id")
    try:
        if variant_id:
            db.execute(
                "UPDATE intake_items SET claimed_variant_id = %s WHERE id = %s",
                (variant_id, item_id),
            )
        else:
            db.execute(
                "UPDATE intake_items SET claimed_variant_id = NULL WHERE id = %s",
                (item_id,),
            )
        item = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
        if not item:
            return jsonify({"error": "Item not found"}), 404
        return jsonify({"success": True, "item": _serialize(item)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/intake/item/<item_id>/update-condition", methods=["POST"])
def update_condition(item_id):
    """Update an item's condition and re-price from PPT if possible."""
    data = request.get_json(silent=True) or {}
    new_condition = data.get("condition", "").strip().upper()
    session_id = data.get("session_id")
    if not new_condition or not session_id:
        return jsonify({"error": "condition and session_id required"}), 400
    try:
        # Update the condition
        item = intake.update_item_condition(item_id, new_condition, session_id)

        # Try to re-price from PPT if we have a tcgplayer_id
        # Skip re-pricing if caller sent skip_reprice (e.g., relink already set the price)
        tcg_id = item.get("tcgplayer_id")
        skip_reprice = data.get("skip_reprice", False)
        if tcg_id and pricing and not skip_reprice:
            try:
                # Direct scalar lookup — cache-first (USD-correct for JP),
                # PPT fallback. Variant in intake_items is stored as the
                # Scrydex-native name ('holofoil', 'altArt') so we pass it
                # straight through.
                item_variance = (item.get("variance") or "").strip() or None
                new_price = pricing.get_raw_condition_price(
                    tcgplayer_id=int(tcg_id),
                    condition=new_condition,
                    variant=item_variance,
                )
                if new_price is not None:
                    item = intake.update_item_price(item_id, new_price, session_id)
                    logger.info(
                        f"Condition change {item_id}: {new_condition} -> ${new_price}"
                    )
            except Exception as e:
                logger.warning(f"Re-price on condition change failed: {e}")
                # Condition is still updated, just price stays the same

        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/item/<item_id>/mark-graded", methods=["POST"])
def mark_item_graded(item_id):
    """Mark a raw card item as graded (PSA/BGS/CGC/SGC) and re-price from eBay data."""
    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id")
    grade_company = (data.get("grade_company") or "").strip().upper()
    grade_value = (data.get("grade_value") or "").strip()
    market_price_override = data.get("market_price")

    if not session_id or not grade_company or not grade_value:
        return jsonify({"error": "session_id, grade_company, and grade_value required"}), 400

    # Update graded fields on the item
    db.execute(
        """UPDATE intake_items
           SET is_graded = TRUE, grade_company = %s, grade_value = %s,
               condition = 'NM'
           WHERE id = %s""",
        (grade_company, grade_value, item_id),
    )
    item = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        return jsonify({"error": "Item not found"}), 404

    # Re-price: use override if provided, otherwise fetch from PPT
    new_price = None
    if market_price_override is not None:
        try:
            new_price = Decimal(str(market_price_override))
        except Exception:
            pass

    if new_price is None and item.get("tcgplayer_id"):
        # Live eBay comps first
        try:
            from graded_pricing import get_live_graded_comps
            live = get_live_graded_comps(int(item["tcgplayer_id"]), grade_company, grade_value, db)
            if live and live.get("mid"):
                new_price = Decimal(str(live["mid"])).quantize(Decimal("0.01"))
                logger.info(f"Live graded comps for mark-graded: median ${live['mid']} "
                                f"({live.get('comps_count', '?')} comps)")
        except Exception as e:
            logger.warning(f"Live graded pricing failed: {e}")
        # Fallback to cache/PPT aggregate via scalar API
        if new_price is None and pricing:
            try:
                new_price = pricing.get_graded_price(
                    tcgplayer_id=int(item["tcgplayer_id"]),
                    company=grade_company, grade=grade_value,
                )
                if new_price is None:
                    logger.warning(
                        f"No graded price for {item['tcgplayer_id']} {grade_company} {grade_value}"
                    )
            except Exception as e:
                logger.warning(f"Graded price fetch failed: {e}")

    if new_price is not None:
        item = intake.update_item_price(item_id, new_price, session_id)

    intake._recalculate_session_totals(session_id)
    return jsonify({"success": True, "item": _serialize(item), "new_price": float(new_price) if new_price else None})



@bp.route("/api/intake/add-sealed-item", methods=["POST"])
def add_sealed_item():
    """Add a sealed item to an existing session (manual add during buy)."""
    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id")
    product_name = data.get("product_name")
    tcgplayer_id = data.get("tcgplayer_id")
    market_price = data.get("market_price")
    quantity = data.get("quantity", 1)

    if not all([session_id, product_name, market_price]):
        return jsonify({"error": "session_id, product_name, and market_price required"}), 400

    try:
        item = intake.add_sealed_item(
            session_id=session_id,
            product_name=product_name,
            tcgplayer_id=int(tcgplayer_id) if tcgplayer_id else None,
            market_price=Decimal(str(market_price)),
            quantity=int(quantity),
            set_name=data.get("set_name"),
        )
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


# ==========================================
# SESSION STATUS FLOW
# in_progress → offered → accepted → received → (handed to ingest service)
#                       → rejected
# ==========================================
