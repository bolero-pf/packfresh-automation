"""Auto-generated from app.py refactor. sessions routes."""
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
import breakdown_logic as bd_logic
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
logger = logging.getLogger("intake.sessions")


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


bp = Blueprint("sessions", __name__)

from collectr_parser import parse_collectr_csv
from collectr_html_parser import parse_collectr_html
from generic_csv_parser import parse_generic_csv, detect_csv_columns
import io, csv



@bp.route("/api/intake/upload-collectr", methods=["POST"])
@enforce_offer_caps
def upload_collectr():
    """Upload and parse a Collectr CSV export."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    customer_name = request.form.get("customer_name", "").strip() or "Unknown"
    try:
        legacy_pct = request.form.get("offer_percentage")
        cash_raw = request.form.get("cash_percentage")
        credit_raw = request.form.get("credit_percentage")
        cash_pct = (Decimal(cash_raw) if cash_raw
                    else Decimal(legacy_pct) if legacy_pct
                    else ASSOCIATE_DEFAULT_CASH)
        credit_pct = Decimal(credit_raw) if credit_raw else ASSOCIATE_DEFAULT_CREDIT
        offer_pct = cash_pct  # item math stays cash-denominated
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400
    force_product_type = request.form.get("force_product_type")  # 'raw' or 'sealed' or None

    # Read file
    try:
        file_content = file.read().decode("utf-8")
    except UnicodeDecodeError:
        file_content = file.read().decode("latin-1")

    # Parse
    result = parse_collectr_csv(file_content)

    if result.errors and not result.items:
        return jsonify({"error": "Failed to parse CSV", "details": result.errors}), 400

    # Check for duplicate import (allow override with force flag)
    dup_session = intake.check_duplicate_import(result.file_hash)
    if dup_session and request.form.get("force") != "1":
        return jsonify({
            "error": "This file has already been imported",
            "existing_session_id": dup_session,
        }), 409

    # Determine session type
    if force_product_type in ("raw", "sealed"):
        session_type = force_product_type
    elif result.raw_count > 0 and result.sealed_count > 0:
        session_type = "mixed"
    elif result.raw_count > 0:
        session_type = "raw"
    else:
        session_type = "sealed"

    # Create session — CSV imports default to NOT walk-in (the customer
    # is mailing or dropping off later). Walk-in flag can be flipped from
    # the session UI if the import is actually being processed at the
    # counter.
    session = intake.create_session(
        customer_name=customer_name or result.portfolio_name,
        session_type=session_type,
        cash_percentage=cash_pct,
        credit_percentage=credit_pct,
        is_walk_in=False,
        file_name=file.filename,
        file_hash=result.file_hash,
    )

    # Set distribution flag if provided
    if request.form.get("is_distribution") == "1":
        db.execute("UPDATE intake_sessions SET is_distribution = TRUE WHERE id = %s", (session["id"],))

    # Process items: calculate offers and check for cached mappings
    effective_product_type = force_product_type or None
    processed = []
    for item in result.items:
        product_type = effective_product_type or item.product_type
        offer_price, unit_cost = intake.calc_offer_price(
            item.market_price, item.quantity, offer_pct, product_type=product_type)

        # Check for cached tcgplayer_id mapping and/or shopify link
        item_variance = getattr(item, "variance", "") or ""
        tcgplayer_id = intake.get_cached_mapping(
            item.product_name, product_type,
            set_name=item.set_name, card_number=item.card_number,
            variance=item_variance)
        shopify_link = intake.get_cached_shopify_link(item.product_name, product_type)
        # If shopify link has a tcgplayer_id that our mapping table missed, use it
        if not tcgplayer_id and shopify_link and shopify_link.get("tcgplayer_id"):
            tcgplayer_id = shopify_link["tcgplayer_id"]

        processed.append({
            "product_name": item.product_name,
            "product_type": product_type,
            "set_name": item.set_name,
            "card_number": item.card_number,
            "condition": item.condition,
            "rarity": item.rarity,
            "variance": item_variance,
            "quantity": item.quantity,
            "market_price": item.market_price,
            "offer_price": offer_price,
            "unit_cost_basis": unit_cost,
            "tcgplayer_id": tcgplayer_id,
            "is_graded": getattr(item, "is_graded", False),
            "grade_company": getattr(item, "grade_company", "") or None,
            "grade_value": getattr(item, "grade_value", "") or None,
            "shopify_product_id": shopify_link["shopify_product_id"] if shopify_link else None,
            "shopify_product_name": shopify_link["shopify_product_name"] if shopify_link else None,
        })

    # Add items to session
    intake.add_items_to_session(session["id"], processed)

    # Update session totals
    intake._recalculate_session_totals(session["id"])

    unmapped_count = sum(1 for p in processed if not p["tcgplayer_id"])
    auto_mapped = sum(1 for p in processed if p["tcgplayer_id"])

    return jsonify({
        "success": True,
        "session_id": session["id"],
        "customer_name": customer_name or result.portfolio_name,
        "session_type": session_type,
        "item_count": len(processed),
        "total_market_value": float(result.total_market_value),
        "total_offer": float(sum(p["offer_price"] for p in processed)),
        "unmapped_count": unmapped_count,
        "auto_mapped_count": auto_mapped,
        "parse_errors": result.errors[:10] if result.errors else [],
    })



@bp.route("/api/intake/upload-collectr-html", methods=["POST"])
@enforce_offer_caps
def upload_collectr_html():
    """Parse pasted Collectr HTML (from portfolio page) into a session.
    If session_id is provided, appends items to that existing session instead of creating new.
    If force_product_type is provided ('raw' or 'sealed'), overrides parser classification.
    """
    data = request.json or {}
    html_content = (data.get("html_content") or data.get("html") or "").strip()
    customer_name = data.get("customer_name", "").strip() or "Unknown"
    existing_session_id = data.get("session_id")
    force_product_type = data.get("force_product_type")  # 'raw' or 'sealed' or None
    try:
        legacy_raw = data.get("offer_percentage")
        cash_raw = data.get("cash_percentage")
        credit_raw = data.get("credit_percentage")
        cash_pct = (Decimal(str(cash_raw)) if cash_raw is not None
                    else Decimal(str(legacy_raw)) if legacy_raw is not None
                    else ASSOCIATE_DEFAULT_CASH)
        credit_pct = Decimal(str(credit_raw)) if credit_raw is not None else ASSOCIATE_DEFAULT_CREDIT
        offer_pct = cash_pct  # item math stays cash-denominated
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400

    if not html_content:
        return jsonify({"error": "No HTML content provided"}), 400

    # Parse
    result = parse_collectr_html(html_content)

    if result.errors and not result.items:
        return jsonify({"error": "Failed to parse HTML", "details": result.errors}), 400

    # Check for duplicate import (skip if appending or force override)
    if not existing_session_id:
        dup_session = intake.check_duplicate_import(result.file_hash)
        if dup_session and not data.get("force"):
            return jsonify({
                "error": "This exact HTML has already been imported",
                "existing_session_id": dup_session,
            }), 409

    # Resolve or create session
    if existing_session_id:
        session = intake.get_session(existing_session_id)
        if not session:
            return jsonify({"error": "Session not found"}), 404
        session_type = session["session_type"]
        offer_pct = Decimal(str(session["offer_percentage"]))
    else:
        if result.raw_count > 0 and result.sealed_count > 0:
            session_type = "mixed"
        elif result.raw_count > 0:
            session_type = "raw"
        else:
            session_type = "sealed"
        # If force_product_type is set, use it as the session type too
        if force_product_type in ("raw", "sealed"):
            session_type = force_product_type

        session = intake.create_session(
            customer_name=customer_name,
            session_type=session_type,
            cash_percentage=cash_pct,
            credit_percentage=credit_pct,
            is_walk_in=False,
            file_name="collectr_html_paste",
            file_hash=result.file_hash,
        )

    # Set distribution flag if provided
    if data.get("is_distribution"):
        db.execute("UPDATE intake_sessions SET is_distribution = TRUE WHERE id = %s", (session["id"],))

    # Process items — override product_type if forced or inferred from session
    effective_product_type = force_product_type or (
        "raw" if session_type == "raw" else None
    )

    # Bulk-resolve slab UUIDs to (company, grade) up front. Unknown UUIDs come
    # back missing from the dict and the row imports without a grade —
    # surfaced as "Unknown slab" in the UI for one-time identification.
    slab_uuids = list({i.slab_uuid for i in result.items if getattr(i, "slab_uuid", "")})
    slab_lookup: dict[str, dict] = {}
    if slab_uuids:
        try:
            ph = ",".join(["%s"] * len(slab_uuids))
            for r in db.query(
                f"SELECT slab_uuid, grade_company, grade_value FROM slab_grade_lookup "
                f"WHERE slab_uuid IN ({ph})",
                tuple(slab_uuids),
            ):
                slab_lookup[r["slab_uuid"]] = r
        except Exception as e:
            logger.warning(f"slab_grade_lookup read failed: {e}")

    processed = []
    for item in result.items:
        product_type = effective_product_type or item.product_type
        offer_price, unit_cost = intake.calc_offer_price(
            item.market_price, item.quantity, offer_pct, product_type=product_type)

        item_variance = getattr(item, "variance", "") or ""
        tcgplayer_id = intake.get_cached_mapping(
            item.product_name, product_type,
            set_name=item.set_name, card_number=item.card_number,
            variance=item_variance)
        shopify_link = intake.get_cached_shopify_link(item.product_name, product_type)
        if not tcgplayer_id and shopify_link and shopify_link.get("tcgplayer_id"):
            tcgplayer_id = shopify_link["tcgplayer_id"]

        # Slab UUID → grade (if known). Parser-supplied grade_company/value
        # are always empty for Collectr HTML graded — Collectr doesn't put
        # the grade in the HTML at all — so the lookup is the source.
        slab_uuid = getattr(item, "slab_uuid", "") or ""
        is_graded = bool(getattr(item, "is_graded", False))
        grade_company = None
        grade_value = None
        if is_graded and slab_uuid and slab_uuid in slab_lookup:
            grade_company = slab_lookup[slab_uuid]["grade_company"]
            grade_value = slab_lookup[slab_uuid]["grade_value"]

        processed.append({
            "product_name": item.product_name,
            "product_type": product_type,
            "set_name": item.set_name,
            "card_number": item.card_number if product_type == "raw" else "",
            "condition": item.condition,
            "rarity": item.rarity if product_type == "raw" else "",
            "variance": item_variance if product_type == "raw" else "",
            "quantity": item.quantity,
            "market_price": item.market_price,
            "offer_price": offer_price,
            "unit_cost_basis": unit_cost,
            "tcgplayer_id": tcgplayer_id,
            "is_graded": is_graded,
            "grade_company": grade_company,
            "grade_value": grade_value,
            "slab_uuid": slab_uuid or None,
            "shopify_product_id": shopify_link["shopify_product_id"] if shopify_link else None,
            "shopify_product_name": shopify_link["shopify_product_name"] if shopify_link else None,
        })

    intake.add_items_to_session(session["id"], processed)
    intake._recalculate_session_totals(session["id"])

    unmapped_count = sum(1 for p in processed if not p["tcgplayer_id"])
    auto_mapped = sum(1 for p in processed if p["tcgplayer_id"])

    return jsonify({
        "success": True,
        "session_id": session["id"],
        "customer_name": customer_name,
        "session_type": session_type,
        "item_count": len(processed),
        "total_market_value": float(result.total_market_value),
        "total_offer": float(sum(p["offer_price"] for p in processed)),
        "unmapped_count": unmapped_count,
        "auto_mapped_count": auto_mapped,
        "parse_errors": result.errors[:10] if result.errors else [],
        "appended_to_existing": bool(existing_session_id),
    })


# ==========================================
# GENERIC CSV IMPORT
# ==========================================


@bp.route("/api/intake/preview-csv", methods=["POST"])
def preview_csv():
    """Preview a generic CSV — detect columns and return mapping + sample rows."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    try:
        file_content = file.read().decode("utf-8")
    except UnicodeDecodeError:
        file.seek(0)
        file_content = file.read().decode("latin-1")

    result = detect_csv_columns(file_content)
    return jsonify(result)



@bp.route("/api/intake/upload-generic-csv", methods=["POST"])
@enforce_offer_caps
def upload_generic_csv():
    """Upload a generic CSV with flexible column mapping."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    customer_name = request.form.get("customer_name", "").strip() or "Unknown"
    try:
        legacy_pct = request.form.get("offer_percentage")
        cash_raw = request.form.get("cash_percentage")
        credit_raw = request.form.get("credit_percentage")
        cash_pct = (Decimal(cash_raw) if cash_raw
                    else Decimal(legacy_pct) if legacy_pct
                    else ASSOCIATE_DEFAULT_CASH)
        credit_pct = Decimal(credit_raw) if credit_raw else ASSOCIATE_DEFAULT_CREDIT
        offer_pct = cash_pct  # item math stays cash-denominated
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400
    force_product_type = request.form.get("force_product_type")  # 'raw' or 'sealed' or None

    # Get column overrides from form (JSON string)
    column_overrides = None
    overrides_str = request.form.get("column_mapping")
    if overrides_str:
        try:
            column_overrides = json.loads(overrides_str)
        except json.JSONDecodeError:
            pass

    # Read file
    try:
        file_content = file.read().decode("utf-8")
    except UnicodeDecodeError:
        file.seek(0)
        file_content = file.read().decode("latin-1")

    # Parse
    result = parse_generic_csv(file_content, column_overrides=column_overrides)

    if result.errors and not result.items:
        return jsonify({
            "error": "Failed to parse CSV",
            "details": result.errors,
            "column_mapping": result.column_mapping,
            "unmapped_headers": result.unmapped_headers,
        }), 400

    # Check for duplicate import (allow override with force flag)
    dup_session = intake.check_duplicate_import(result.file_hash)
    if dup_session and request.form.get("force") != "1":
        return jsonify({
            "error": "This file has already been imported",
            "existing_session_id": dup_session,
        }), 409

    # Determine session type
    if force_product_type in ("raw", "sealed"):
        session_type = force_product_type
    elif result.raw_count > 0 and result.sealed_count > 0:
        session_type = "mixed"
    elif result.raw_count > 0:
        session_type = "raw"
    else:
        session_type = "sealed"

    # Create session — generic CSV imports default to NOT walk-in.
    session = intake.create_session(
        customer_name=customer_name,
        session_type=session_type,
        cash_percentage=cash_pct,
        credit_percentage=credit_pct,
        is_walk_in=False,
        file_name=file.filename,
        file_hash=result.file_hash,
    )

    # Set distribution flag if provided
    if request.form.get("is_distribution") == "1":
        db.execute("UPDATE intake_sessions SET is_distribution = TRUE WHERE id = %s", (session["id"],))

    # Process items
    effective_product_type = force_product_type or None
    processed = []
    for item in result.items:
        product_type = effective_product_type or item.product_type
        offer_price, unit_cost = intake.calc_offer_price(
            item.market_price, item.quantity, offer_pct, product_type=product_type)

        # Check for cached tcgplayer_id mapping (or use the one from CSV)
        item_variance = getattr(item, "variance", "") or ""
        tcgplayer_id = item.tcgplayer_id or intake.get_cached_mapping(
            item.product_name, product_type,
            set_name=item.set_name, card_number=item.card_number,
            variance=item_variance)
        shopify_link = intake.get_cached_shopify_link(item.product_name, product_type)
        if not tcgplayer_id and shopify_link and shopify_link.get("tcgplayer_id"):
            tcgplayer_id = shopify_link["tcgplayer_id"]

        processed.append({
            "product_name": item.product_name,
            "product_type": product_type,
            "set_name": item.set_name,
            "card_number": item.card_number,
            "condition": item.condition,
            "rarity": item.rarity,
            "variance": item_variance,
            "quantity": item.quantity,
            "market_price": item.market_price,
            "offer_price": offer_price,
            "unit_cost_basis": unit_cost,
            "tcgplayer_id": tcgplayer_id,
            "is_graded": getattr(item, "is_graded", False),
            "grade_company": getattr(item, "grade_company", "") or None,
            "grade_value": getattr(item, "grade_value", "") or None,
            "shopify_product_id": shopify_link["shopify_product_id"] if shopify_link else None,
            "shopify_product_name": shopify_link["shopify_product_name"] if shopify_link else None,
        })

    # Add items to session
    intake.add_items_to_session(session["id"], processed)
    intake._recalculate_session_totals(session["id"])

    unmapped_count = sum(1 for p in processed if not p["tcgplayer_id"])
    auto_mapped = sum(1 for p in processed if p["tcgplayer_id"])

    return jsonify({
        "success": True,
        "session_id": session["id"],
        "customer_name": customer_name,
        "session_type": session_type,
        "item_count": len(processed),
        "total_market_value": float(result.total_market_value),
        "total_offer": float(sum(p["offer_price"] for p in processed)),
        "unmapped_count": unmapped_count,
        "auto_mapped_count": auto_mapped,
        "column_mapping": result.column_mapping,
        "parse_errors": result.errors[:10] if result.errors else [],
    })


# ==========================================
# SESSION MANAGEMENT
# ==========================================


@bp.route("/api/intake/create-session", methods=["POST"])
@enforce_offer_caps
def create_session():
    """Create an empty intake session (for manual raw card entry).

    Accepts the new cash/credit split. If only legacy `offer_percentage`
    is supplied, treat it as the cash percentage so existing callers keep
    working unchanged. `is_walk_in` defaults to TRUE for manual-entry
    sessions because that's the dominant counter use case — CSV/HTML
    upload paths pass it explicitly when they want non-walk-in.
    """
    data = request.json or {}
    customer_name = data.get("customer_name", "Walk-in")
    session_type = data.get("session_type", "raw")
    try:
        legacy_pct = data.get("offer_percentage")
        cash_raw = data.get("cash_percentage", legacy_pct if legacy_pct is not None else str(ASSOCIATE_DEFAULT_CASH))
        credit_raw = data.get("credit_percentage", str(ASSOCIATE_DEFAULT_CREDIT))
        cash_pct = Decimal(str(cash_raw)) if cash_raw is not None else None
        credit_pct = Decimal(str(credit_raw)) if credit_raw is not None else None
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400

    # Cap validation runs in the @enforce_offer_caps decorator.

    is_walk_in = data.get("is_walk_in")
    if is_walk_in is None:
        is_walk_in = True  # manual-entry default

    session = intake.create_session(
        customer_name=customer_name,
        session_type=session_type,
        cash_percentage=cash_pct,
        credit_percentage=credit_pct,
        is_walk_in=bool(is_walk_in),
        employee_id=data.get("employee_id"),
        notes=data.get("notes"),
    )
    # Set distribution flag if provided
    if data.get("is_distribution"):
        db.execute("UPDATE intake_sessions SET is_distribution = TRUE WHERE id = %s", (session["id"],))
        session["is_distribution"] = True

    # If an override was used, log it now that we have a session_id.
    _log_override_if_present(data, session["id"], cash_pct, credit_pct)

    return jsonify({"success": True, "session": _serialize(session)})



@bp.route("/api/intake/session/<session_id>", methods=["GET"])
def get_session(session_id):
    """Get session details, items, and breakdown summaries for sealed items."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    items = intake.get_session_items(session_id)

    # Attach breakdown summaries to sealed items that have a tcgplayer_id.
    # The shared summary call also triggers JIT-refresh of stale component
    # market prices internally (when ppt is passed), so no separate refresh
    # thread is needed here.
    tcg_ids = list({int(i["tcgplayer_id"]) for i in items if i.get("tcgplayer_id")})

    bd_map = {}
    if tcg_ids:
        try:
            # Read path: don't pass ppt — JIT refresh is synchronous and blocks
            # on Scrydex 5xx for known-broken sealed products (e.g. mep-23),
            # easily blowing past Railway's worker timeout. Use whatever the
            # nightly cron + the explicit /refresh-prices endpoint have written.
            bd_map = bd_logic.get_breakdown_summary_for_items(tcg_ids, db, ppt=None)
        except Exception as e:
            logger.warning(f"breakdown summary lookup failed: {e}")
            bd_map = {}

    # Attach velocity data from sku_analytics (prefer non-damaged variant with most sales)
    velocity_map = {}
    if tcg_ids:
        try:
            vph = ",".join(["%s"] * len(tcg_ids))
            vel_rows = db.query(f"""
                SELECT a.tcgplayer_id, a.units_sold_90d, a.units_sold_30d, a.units_sold_7d,
                       a.total_sold_all_time, a.first_seen_date,
                       a.velocity_score, a.current_qty, a.current_price, a.avg_days_to_sell,
                       a.out_of_stock_days, a.price_trend_pct, a.computed_at
                FROM sku_analytics a
                JOIN inventory_product_cache c ON c.shopify_variant_id = a.shopify_variant_id
                WHERE a.tcgplayer_id IN ({vph}) AND c.is_damaged = FALSE
                ORDER BY a.units_sold_90d DESC
            """, tuple(tcg_ids))
            for r in vel_rows:
                if r["tcgplayer_id"] not in velocity_map:
                    velocity_map[r["tcgplayer_id"]] = dict(r)
        except Exception:
            pass

    serialized = []
    for i in items:
        item_dict = _serialize(i)
        tcg = i.get("tcgplayer_id")
        item_dict["breakdown_summary"] = _serialize(bd_map.get(int(tcg))) if tcg and int(tcg) in bd_map else None
        vel = velocity_map.get(int(tcg)) if tcg else None
        item_dict["velocity"] = _serialize(vel) if vel else None
        serialized.append(item_dict)

    # Live cash/credit totals for the dual-offer projection. Until the
    # customer commits to an offer type, intake_items.offer_price is
    # cash-denominated; the credit total has to be computed from the
    # session's credit_percentage on the fly.
    s_out = _serialize(session)
    try:
        totals = intake.compute_offer_totals(session_id)
        s_out["total_offer_cash"] = float(totals["cash"])
        s_out["total_offer_credit"] = float(totals["credit"])
    except Exception as e:
        logger.warning(f"compute_offer_totals failed for {session_id}: {e}")

    return jsonify({
        "session": s_out,
        "items": serialized,
    })



@bp.route("/api/intake/session/<session_id>/meta-stats", methods=["GET"])
def session_meta_stats(session_id):
    """Per-category breakdown of an intake session for the Collection
    Summary card. Categories come from Shopify tags joined out of
    inventory_product_cache (see shared/product_categorize.py).

    The interesting numbers are *real* sell-side margins — what staff
    would actually make if the collection was sold through. Cost is the
    per-unit offer_price (what we paid). Sell is store_price when the
    item is currently listed, else market_price as a proxy. Mirrors the
    Store tab's effectiveSellPrice / margin math so both screens agree.

    Per category:
        sku_count             — number of intake_items rows
        qty                   — sum of quantity (units of product)
        market_value          — sum of market_price * quantity
        cash_offer            — sum of offer_price (already qty-weighted)
        credit_offer          — sum of market * qty * credit_pct/100
        store_listed_value    — sum of store_price * qty (only items listed)
        sell_value            — sum of effective_sell * qty (store fallback market)
        share_of_market_pct   — share of grand_market by category
        margin_cash_pct       — (sell - paid_cash) / sell, value-weighted
        margin_credit_pct     — (sell - paid_credit) / sell, value-weighted
        in_store_count        — # of SKUs currently listed
        items                 — list of {name, qty, market_price, offer_price,
                                store_price, in_store} for drill-in
    """
    from product_categorize import classify_item, sort_categories

    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    items = intake.get_session_items(session_id) or []
    items = [i for i in items if (i.get("item_status") or "good") in ("good", "damaged")]

    cash_pct = session.get("cash_percentage")
    if cash_pct is None:
        cash_pct = session.get("offer_percentage")
    credit_pct = session.get("credit_percentage")
    cash_pct = float(cash_pct) if cash_pct is not None else 0.0
    credit_pct = float(credit_pct) if credit_pct is not None else 0.0

    # One round-trip — pull tags + store price + qty for every linked item.
    tcg_ids = list({i["tcgplayer_id"] for i in items if i.get("tcgplayer_id")})
    shopify_ids = list({str(i["shopify_product_id"]) for i in items if i.get("shopify_product_id")})
    cache_by_tcg: dict[int, dict] = {}
    cache_by_shopify: dict[str, dict] = {}
    # bd_summary_by_tcg holds the full breakdown summary (variant_resolution,
    # variants[], best/expected/worst aggregates) so per-item offer math can
    # honor each row's claimed_variant_id via pick_offer_value().
    bd_summary_by_tcg: dict[int, dict] = {}
    _t0 = time.perf_counter()
    _t_cache_tcg = _t_bd = _t_cache_shopify = 0.0
    try:
        if tcg_ids:
            ph = ",".join(["%s"] * len(tcg_ids))
            _ts = time.perf_counter()
            for r in db.query(
                f"""SELECT tcgplayer_id, shopify_product_id, tags, shopify_price, shopify_qty, is_damaged
                    FROM inventory_product_cache WHERE tcgplayer_id IN ({ph})""",
                tuple(tcg_ids),
            ):
                tcg = r.get("tcgplayer_id")
                if not tcg:
                    continue
                # Prefer non-damaged variant when both exist for the same TCG ID.
                if tcg in cache_by_tcg and r.get("is_damaged"):
                    continue
                cache_by_tcg[tcg] = r
            _t_cache_tcg = time.perf_counter() - _ts
            try:
                _ts = time.perf_counter()
                # Sync read uses cache_only=True so the response is instant
                # even when there are cache-miss components in the mix. Then
                # below we background a cache_only=False refresh so PPT gets
                # called (1 credit per unique stale component per max_age
                # window, bounded by the staleness check) — next page load
                # picks up the freshly-populated market_price.
                bd_summary_by_tcg = bd_logic.get_breakdown_summary_for_items(
                    tcg_ids, db, ppt=pricing, cache_only=True,
                )
                _t_bd = time.perf_counter() - _ts
            except Exception as e:
                logger.warning(f"meta-stats breakdown summary lookup failed: {e}")
                bd_summary_by_tcg = {}

            # Background PPT refresh for next call. Pull the variant ids that
            # back this session's tcg_ids so we only refresh what's relevant.
            try:
                if pricing and tcg_ids:
                    import threading
                    from breakdown_helpers import refresh_stale_component_prices
                    _bg_ph = ",".join(["%s"] * len(tcg_ids))
                    _bg_vids = [
                        str(r["variant_id"]) for r in db.query(
                            f"""SELECT sbv.id AS variant_id
                                FROM sealed_breakdown_cache sbc
                                JOIN sealed_breakdown_variants sbv
                                    ON sbv.breakdown_id = sbc.id
                                WHERE sbc.tcgplayer_id IN ({_bg_ph})""",
                            tuple(tcg_ids),
                        )
                    ]
                    if _bg_vids:
                        threading.Thread(
                            target=refresh_stale_component_prices,
                            args=(_bg_vids, db, pricing),
                            daemon=True,
                        ).start()
            except Exception as e:
                logger.warning(f"meta-stats background refresh skipped: {e}")
        if shopify_ids:
            ph = ",".join(["%s"] * len(shopify_ids))
            _ts = time.perf_counter()
            for r in db.query(
                f"""SELECT shopify_product_id, tcgplayer_id, tags, shopify_price, shopify_qty, is_damaged
                    FROM inventory_product_cache WHERE shopify_product_id IN ({ph})""",
                tuple(shopify_ids),
            ):
                pid = str(r.get("shopify_product_id") or "")
                if pid and (pid not in cache_by_shopify or not r.get("is_damaged")):
                    cache_by_shopify[pid] = r
            _t_cache_shopify = time.perf_counter() - _ts
    except Exception as e:
        logger.warning(f"meta-stats: cache join failed: {e}")

    cats: dict[str, dict] = {}
    grand_market = 0.0
    grand_cash = 0.0
    grand_credit = 0.0
    grand_sell = 0.0
    grand_listed = 0.0
    grand_breakdown = 0.0
    grand_qty = 0
    grand_skus = 0
    grand_in_store = 0
    grand_with_bd = 0

    for item in items:
        cache_row = None
        if item.get("tcgplayer_id"):
            cache_row = cache_by_tcg.get(item["tcgplayer_id"])
        if not cache_row and item.get("shopify_product_id"):
            cache_row = cache_by_shopify.get(str(item["shopify_product_id"]))
        tags_csv = (cache_row or {}).get("tags") or ""

        label = classify_item(item, tags_csv)
        qty = int(item.get("quantity") or 0)
        unit_market = float(item.get("market_price") or 0)
        item_market = unit_market * qty
        item_cash = float(item.get("offer_price") or 0)
        item_credit = unit_market * qty * (credit_pct / 100.0) if credit_pct else 0.0

        store_price = None
        in_store = False
        if cache_row and cache_row.get("shopify_price") is not None:
            store_price = float(cache_row["shopify_price"])
            in_store = (cache_row.get("shopify_qty") or 0) > 0 or store_price > 0

        # Per-item breakdown value — honors variant_resolution and the row's
        # claimed_variant_id so probabilistic recipes use the avg (not max)
        # unless the seller's claim has been locked in. Collection Summary
        # is the store-side view (upside if we sell through), so prefer
        # store-priced BD with market as fallback. The Offer tab is where
        # market-priced BD belongs (negotiating against the seller).
        unit_bd = None
        if item.get("tcgplayer_id"):
            _bd_summary = bd_summary_by_tcg.get(int(item["tcgplayer_id"]))
            if _bd_summary:
                unit_bd = bd_logic.pick_offer_value(
                    _bd_summary,
                    claimed_variant_id=item.get("claimed_variant_id"),
                    prefer="store",
                )
        item_bd = (unit_bd * qty) if unit_bd else 0.0

        # 'Achievable sell-as-sealed' price: when listed, that's our
        # store price (the actual sticker we'd charge); when not listed,
        # we fall back to market_price as a planning proxy. The external
        # market_price is *not* the achievable sell for listed items —
        # we'd never sell above our own listing.
        unit_store = store_price if (store_price and store_price > 0) else 0.0
        unit_sealed_sell = unit_store if unit_store > 0 else unit_market
        unit_best = max(unit_sealed_sell, unit_bd or 0.0)
        item_sell = unit_best * qty

        # Best strategy:
        #   breakdown — bd value beats whatever we'd get sealed
        #   store     — listed and selling sealed beats breakdown
        #   market    — not listed; market_price is just an estimate
        if unit_bd and unit_bd >= unit_sealed_sell:
            best_strategy = "breakdown"
        elif unit_store > 0:
            best_strategy = "store"
        else:
            best_strategy = "market"
        item_listed = (store_price * qty) if (store_price and store_price > 0) else 0.0

        bucket = cats.setdefault(label, {
            "label": label, "sku_count": 0, "qty": 0,
            "market_value": 0.0, "cash_offer": 0.0, "credit_offer": 0.0,
            "store_listed_value": 0.0, "sell_value": 0.0,
            "breakdown_value": 0.0, "items_with_breakdown": 0,
            "in_store_count": 0,
            "items": [],
        })
        bucket["sku_count"] += 1
        bucket["qty"] += qty
        bucket["market_value"] += item_market
        bucket["cash_offer"] += item_cash
        bucket["credit_offer"] += item_credit
        bucket["sell_value"] += item_sell
        bucket["store_listed_value"] += item_listed
        if unit_bd:
            bucket["breakdown_value"] += item_bd
            bucket["items_with_breakdown"] += 1
        if in_store:
            bucket["in_store_count"] += 1
        bucket["items"].append({
            "name": item.get("product_name") or "",
            "qty": qty,
            "market_price": round(unit_market, 2),
            "offer_price": round(item_cash, 2),
            "store_price": round(store_price, 2) if store_price else None,
            "breakdown_price": round(unit_bd, 2) if unit_bd else None,
            "best_strategy": best_strategy,
            "in_store": in_store,
        })

        grand_market += item_market
        grand_cash += item_cash
        grand_credit += item_credit
        grand_sell += item_sell
        grand_listed += item_listed
        if unit_bd:
            grand_breakdown += item_bd
            grand_with_bd += 1
        grand_qty += qty
        grand_skus += 1
        if in_store:
            grand_in_store += 1

    def _margin(sell: float, paid: float) -> float:
        return round(((sell - paid) / sell * 100.0), 1) if sell > 0 else 0.0

    out_cats = []
    for label in sort_categories(cats.keys()):
        b = cats[label]
        m = b["market_value"]
        b["share_of_market_pct"] = round((m / grand_market * 100.0), 1) if grand_market > 0 else 0.0
        b["margin_cash_pct"] = _margin(b["sell_value"], b["cash_offer"])
        b["margin_credit_pct"] = _margin(b["sell_value"], b["credit_offer"])
        b["market_value"] = round(b["market_value"], 2)
        b["cash_offer"] = round(b["cash_offer"], 2)
        b["credit_offer"] = round(b["credit_offer"], 2)
        b["sell_value"] = round(b["sell_value"], 2)
        b["store_listed_value"] = round(b["store_listed_value"], 2)
        b["breakdown_value"] = round(b["breakdown_value"], 2)
        # Items already in display order (intake order). Sort biggest first
        # by market value so drill-in shows the heavy hitters first.
        b["items"].sort(key=lambda x: -(float(x["market_price"] or 0) * (x["qty"] or 0)))
        out_cats.append(b)

    _t_total = time.perf_counter() - _t0
    if _t_total > 1.0:
        logger.warning(
            "meta-stats slow: session=%s items=%d tcg_ids=%d total=%.2fs "
            "[cache_tcg=%.2fs bd_summary=%.2fs cache_shopify=%.2fs]",
            session_id, len(items), len(tcg_ids),
            _t_total, _t_cache_tcg, _t_bd, _t_cache_shopify,
        )

    return jsonify({
        "session_id": session_id,
        "totals": {
            "sku_count": grand_skus,
            "qty": grand_qty,
            "market_value": round(grand_market, 2),
            "cash_offer": round(grand_cash, 2),
            "credit_offer": round(grand_credit, 2),
            "sell_value": round(grand_sell, 2),
            "store_listed_value": round(grand_listed, 2),
            "breakdown_value": round(grand_breakdown, 2),
            "items_with_breakdown": grand_with_bd,
            "in_store_count": grand_in_store,
            "in_store_pct": round((grand_in_store / grand_skus * 100.0), 1) if grand_skus > 0 else 0.0,
            "margin_cash_pct": _margin(grand_sell, grand_cash),
            "margin_credit_pct": _margin(grand_sell, grand_credit),
            "cash_percentage": cash_pct,
            "credit_percentage": credit_pct,
        },
        "categories": out_cats,
    })



@bp.route("/api/intake/identify-slab", methods=["POST"])
def identify_slab():
    """Map a Collectr slab graphic UUID → (grading company, grade) and
    backfill any intake_items waiting on that UUID. Collectr embeds the
    grade only in the slab image (PSA 10, BGS 9.5, etc. as a PNG, not text),
    but the image URL is stable per (company, grade) — so once an operator
    identifies one slab, every future paste with that UUID auto-fills.

    Backfill scope: only items where grade_company IS NULL. We never
    overwrite a manually-set grade.
    """
    data = request.json or {}
    slab_uuid = (data.get("slab_uuid") or "").strip()
    company = (data.get("grade_company") or "").strip().upper()
    grade = (data.get("grade_value") or "").strip()
    sample_url = (data.get("sample_image_url") or "").strip() or None
    if not slab_uuid or not company or not grade:
        return jsonify({"error": "slab_uuid, grade_company, grade_value required"}), 400
    if company not in ("PSA", "BGS", "CGC", "SGC", "TAG"):
        return jsonify({"error": f"Unsupported grading company: {company}"}), 400

    user = None
    try:
        user = (g.user or {}).get("email") if hasattr(g, "user") else None
    except Exception:
        user = None
    user = user or "unknown"

    db.execute("""
        INSERT INTO slab_grade_lookup
            (slab_uuid, grade_company, grade_value, sample_image_url, identified_by)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (slab_uuid) DO UPDATE
           SET grade_company = EXCLUDED.grade_company,
               grade_value = EXCLUDED.grade_value,
               sample_image_url = COALESCE(EXCLUDED.sample_image_url, slab_grade_lookup.sample_image_url),
               identified_at = CURRENT_TIMESTAMP,
               identified_by = EXCLUDED.identified_by
    """, (slab_uuid, company, grade, sample_url, user))

    backfilled = db.execute("""
        UPDATE intake_items
           SET grade_company = %s, grade_value = %s
         WHERE slab_uuid = %s
           AND is_graded = TRUE
           AND grade_company IS NULL
    """, (company, grade, slab_uuid))

    return jsonify({"ok": True, "backfilled_count": backfilled})


@bp.route("/api/intake/sessions", methods=["GET"])
def list_sessions():
    """List sessions by status with optional filters."""
    status = request.args.get("status", "in_progress")
    limit = int(request.args.get("limit", 50))
    search = request.args.get("search", "").strip()
    days = request.args.get("days")  # e.g. 30 for last 30 days
    fulfillment = request.args.get("fulfillment")  # pickup or mail

    statuses = [s.strip() for s in status.split(",") if s.strip()]
    placeholders = ",".join(["%s"] * len(statuses))
    params = list(statuses)

    where_clauses = [f"status IN ({placeholders})"]

    if search:
        where_clauses.append("LOWER(customer_name) LIKE %s")
        params.append(f"%{search.lower()}%")

    if days:
        where_clauses.append("created_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'")
        params.append(int(days))

    if fulfillment:
        where_clauses.append("fulfillment_method = %s")
        params.append(fulfillment)

    where_sql = " AND ".join(where_clauses)
    params.append(limit)

    sessions = db.query(f"""
        SELECT * FROM intake_session_summary
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT %s
    """, tuple(params))

    return jsonify({"sessions": [_serialize(s) for s in sessions]})



@bp.route("/api/intake/session/<session_id>/walk-in", methods=["POST"])
def set_session_walk_in(session_id):
    """Toggle / set the walk-in flag on a session."""
    data = request.json or {}
    flag = bool(data.get("is_walk_in", True))
    try:
        session = intake.set_walk_in(session_id, flag)
        return jsonify({"success": True, "session": _serialize(session)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/rejuvenate-session/<session_id>", methods=["POST"])
def rejuvenate_session(session_id):
    """Restore a cancelled/rejected session back to in_progress."""
    try:
        result = intake.rejuvenate_session(session_id)
        return jsonify({"success": True, "session": result})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("rejuvenate_session error")
        return jsonify({"success": False, "error": str(e)}), 500



@bp.route("/api/intake/cancel-session/<session_id>", methods=["POST"])
def cancel_session(session_id):
    """Cancel an intake session."""
    data = request.get_json(silent=True) or {}
    try:
        result = intake.cancel_session(session_id, reason=data.get("reason"))
        return jsonify({"success": True, "session": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/merge-sessions", methods=["POST"])
def merge_sessions():
    """Merge source session into target, combining duplicate items."""
    data = request.get_json(silent=True) or {}
    target_id = data.get("target_session_id")
    source_id = data.get("source_session_id")
    if not target_id or not source_id:
        return jsonify({"error": "target_session_id and source_session_id required"}), 400
    try:
        result = intake.merge_sessions(target_id, source_id)
        return jsonify({"success": True, "session": _serialize(result), "merge_stats": result.get("merge_stats", {})})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception("merge_sessions error")
        return jsonify({"error": str(e)}), 500



@bp.route("/api/intake/session/<session_id>/offer", methods=["POST"])
def offer_session(session_id):
    """Lock in the offer. Validates all items are mapped."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("in_progress",):
        return jsonify({"error": f"Cannot offer — session is '{session['status']}'"}), 400

    items = intake.get_session_items(session_id)
    active = [i for i in items if i.get("item_status", "good") in ("good", "damaged")]
    if not active:
        return jsonify({"error": "No active items in session"}), 400
    unmapped = [i for i in active if not i.get("is_mapped")]
    if unmapped:
        names = [i["product_name"] for i in unmapped[:5]]
        return jsonify({"error": f"{len(unmapped)} items still need mapping", "unmapped_names": names}), 400

    db.execute("UPDATE intake_sessions SET status = 'offered', offered_at = CURRENT_TIMESTAMP WHERE id = %s", (session_id,))
    return jsonify({"success": True, "status": "offered"})



@bp.route("/api/intake/session/<session_id>/accept", methods=["POST"])
def accept_session(session_id):
    """Customer accepted the offer. `offer_type` ∈ {'cash','credit'} picks the
    offer; walk-in sessions short-circuit straight to 'received'."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("offered",):
        return jsonify({"error": f"Cannot accept — session is '{session['status']}'"}), 400
    data = request.get_json(silent=True) or {}
    offer_type = (data.get("offer_type") or "").lower().strip()
    if offer_type not in ("cash", "credit"):
        return jsonify({"error": "offer_type must be 'cash' or 'credit'"}), 400

    fulfillment = data.get("fulfillment_method", "pickup")  # pickup or mail
    tracking = (data.get("tracking_number") or "").strip() or None
    pickup_date = (data.get("pickup_date") or "").strip() or None

    try:
        updated = intake.accept_offer(
            session_id,
            offer_type=offer_type,
            fulfillment=fulfillment,
            tracking_number=tracking,
            pickup_date=pickup_date,
        )
        return jsonify({
            "success": True,
            "status": updated["status"],
            "accepted_offer_type": offer_type,
            "fulfillment_method": fulfillment,
            "is_walk_in": bool(updated.get("is_walk_in")),
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400



@bp.route("/api/intake/session/<session_id>/receive", methods=["POST"])
def receive_session(session_id):
    """Product received — ready for verification and eventually ingest."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("accepted",):
        return jsonify({"error": f"Cannot receive — session is '{session['status']}'"}), 400

    # Snapshot items + offer at receive time for adjustment tracking in ingest
    items = intake.get_session_items(session_id)
    snapshot = json.dumps([{
        "id": str(i["id"]),
        "product_name": i.get("product_name"),
        "tcgplayer_id": i.get("tcgplayer_id"),
        "quantity": i.get("quantity", 1),
        "market_price": float(i.get("market_price", 0)),
        "offer_price": float(i.get("offer_price", 0)),
        "item_status": i.get("item_status", "good"),
    } for i in items if i.get("item_status") in ("good", "damaged")])

    db.execute("""
        UPDATE intake_sessions
        SET status = 'received', received_at = CURRENT_TIMESTAMP,
            original_offer_amount = total_offer_amount,
            received_items_snapshot = %s
        WHERE id = %s
    """, (snapshot, session_id))
    return jsonify({"success": True, "status": "received"})



@bp.route("/api/intake/session/<session_id>/reject", methods=["POST"])
def reject_session(session_id):
    """Customer rejected the offer."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] in ("ingested",):
        return jsonify({"error": "Cannot reject — already ingested"}), 400
    db.execute("UPDATE intake_sessions SET status = 'rejected', rejected_at = CURRENT_TIMESTAMP WHERE id = %s", (session_id,))
    return jsonify({"success": True, "status": "rejected"})



@bp.route("/api/intake/session/<session_id>/reopen", methods=["POST"])
def reopen_session(session_id):
    """Reopen a session back to in_progress (for edits before ingest)."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] in ("ingested",):
        return jsonify({"error": "Cannot reopen — already ingested"}), 400
    db.execute("UPDATE intake_sessions SET status = 'in_progress' WHERE id = %s", (session_id,))
    return jsonify({"success": True, "status": "in_progress"})



@bp.route("/api/intake/session/<session_id>/toggle-distribution", methods=["POST"])
def toggle_distribution(session_id):
    """Toggle the distribution flag on a session."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    new_val = not (session.get("is_distribution") is True)
    try:
        db.execute("UPDATE intake_sessions SET is_distribution = %s WHERE id = %s", (new_val, session_id))
    except Exception as e:
        return jsonify({"error": f"Failed — run migration to add is_distribution column: {e}"}), 500
    return jsonify({"success": True, "is_distribution": new_val})



@bp.route("/api/intake/session/<session_id>/tracking", methods=["POST"])
def update_tracking(session_id):
    """Update tracking number/link for a mailed session."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    data = request.get_json(silent=True) or {}
    tracking = data.get("tracking_number", "").strip() or None
    db.execute("UPDATE intake_sessions SET tracking_number = %s WHERE id = %s", (tracking, session_id))
    return jsonify({"success": True, "tracking_number": tracking})



@bp.route("/api/intake/session/<session_id>/pickup-date", methods=["POST"])
def update_pickup_date(session_id):
    """Update pickup date for an accepted-pickup session."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    data = request.get_json(silent=True) or {}
    pickup_date = data.get("pickup_date", "").strip() or None
    db.execute("UPDATE intake_sessions SET pickup_date = %s WHERE id = %s", (pickup_date, session_id))
    return jsonify({"success": True, "pickup_date": pickup_date})




@bp.route("/api/intake/session/<session_id>/export-csv")
def export_session_csv(session_id):
    """Export session items as CSV for pen-and-paper verification."""
    import csv
    import io

    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    items = intake.get_session_items(session_id)
    active = [i for i in items if i.get("item_status", "good") in ("good", "damaged")]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Product Name", "TCGPlayer ID", "Condition", "Quantity", "Unit Price", "Damage Deduction", "Offer Total", "Present", "Notes"])
    for item in active:
        qty = item.get("quantity", 1)
        offer = float(item.get("offer_price") or 0)
        unit = offer / qty if qty > 0 else 0
        damaged_unit = unit * 0.15
        writer.writerow([
            item.get("product_name", ""),
            item.get("tcgplayer_id", ""),
            item.get("condition", ""),
            qty,
            f"${unit:.2f}",
            f"${damaged_unit:.2f}",
            f"${offer:.2f}",
            "",  # Present column — blank for checking off
            "DAMAGED" if item.get("item_status") == "damaged" else "",
        ])
    writer.writerow([])
    writer.writerow(["TOTAL", "", "", sum(i.get("quantity", 1) for i in active), "", "",
                     f"${sum(float(i.get('offer_price') or 0) for i in active):.2f}", "", ""])

    output.seek(0)
    customer = session.get("customer_name", "export")
    filename = f"offer_{customer}_{session_id[:8]}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ==========================================
# PPT INTEGRATION ENDPOINTS
# ==========================================
