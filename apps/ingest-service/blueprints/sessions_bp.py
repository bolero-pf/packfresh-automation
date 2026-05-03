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
def upload_collectr():
    """Upload and parse a Collectr CSV export."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    customer_name = request.form.get("customer_name", "").strip() or "Unknown"
    try:
        offer_pct = Decimal(request.form.get("offer_percentage", "75"))
        cash_raw = request.form.get("cash_percentage")
        credit_raw = request.form.get("credit_percentage")
        cash_pct = Decimal(cash_raw) if cash_raw else offer_pct
        credit_pct = Decimal(credit_raw) if credit_raw else None
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400
    force_product_type = request.form.get("force_product_type")  # 'raw' or 'sealed' or None

    # Item-level offer math stays denominated in cash (the operational
    # baseline) until a customer picks an offer type. The credit
    # projection is recomputed live on read.
    offer_pct = cash_pct

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
        offer_pct = Decimal(str(data.get("offer_percentage", "75")))
        cash_raw = data.get("cash_percentage")
        credit_raw = data.get("credit_percentage")
        cash_pct = Decimal(str(cash_raw)) if cash_raw is not None else offer_pct
        credit_pct = Decimal(str(credit_raw)) if credit_raw is not None else None
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400
    # Item math stays cash-denominated (see upload_collectr).
    offer_pct = cash_pct

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
            "is_graded": getattr(item, "is_graded", False),
            "grade_company": getattr(item, "grade_company", "") or None,
            "grade_value": getattr(item, "grade_value", "") or None,
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
def upload_generic_csv():
    """Upload a generic CSV with flexible column mapping."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    customer_name = request.form.get("customer_name", "").strip() or "Unknown"
    try:
        offer_pct = Decimal(request.form.get("offer_percentage", "75"))
        cash_raw = request.form.get("cash_percentage")
        credit_raw = request.form.get("credit_percentage")
        cash_pct = Decimal(cash_raw) if cash_raw else offer_pct
        credit_pct = Decimal(credit_raw) if credit_raw else None
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400
    offer_pct = cash_pct  # item math stays cash-denominated
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
        # Cash defaults 65%, credit 75% (per the role-policy spec)
        legacy_pct = data.get("offer_percentage")
        cash_raw = data.get("cash_percentage", legacy_pct if legacy_pct is not None else "65")
        credit_raw = data.get("credit_percentage", "75")
        cash_pct = Decimal(str(cash_raw)) if cash_raw is not None else None
        credit_pct = Decimal(str(credit_raw)) if credit_raw is not None else None
    except InvalidOperation:
        return jsonify({"error": "Invalid percentage"}), 400

    # Server-side role-policy check (issue #8b — see _validate_offer_caps).
    # An associate can only create with the canonical defaults; managers
    # are capped at 80; owner uncapped. Override tokens unlock as usual.
    cap_err = _validate_offer_caps(data, cash_pct, credit_pct, session_id=None)
    if cap_err:
        return jsonify(cap_err), 403

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

    # Attach breakdown summaries to sealed items that have a tcgplayer_id
    tcg_ids = list({int(i["tcgplayer_id"]) for i in items if i.get("tcgplayer_id")})

    # JIT refresh stale component market prices in background (don't block response)
    if tcg_ids and pricing:
        try:
            from breakdown_helpers import refresh_stale_component_prices
            import threading
            _ph = ",".join(["%s"] * len(tcg_ids))
            _vids = db.query(f"""
                SELECT sbv.id AS variant_id
                FROM sealed_breakdown_cache sbc
                JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                WHERE sbc.tcgplayer_id IN ({_ph})
            """, tuple(tcg_ids))
            if _vids:
                threading.Thread(target=refresh_stale_component_prices,
                    args=([v["variant_id"] for v in _vids], db, pricing), daemon=True).start()
        except Exception:
            pass

    bd_map = {}
    if tcg_ids:
        try:
            ph = ",".join(["%s"] * len(tcg_ids))
            rows = db.query(f"""
                SELECT sbc.tcgplayer_id, sbc.variant_count, sbc.best_variant_market,
                       COALESCE(
                           (SELECT STRING_AGG(variant_name, ' / ' ORDER BY display_order)
                            FROM sealed_breakdown_variants WHERE breakdown_id=sbc.id), ''
                       ) AS variant_names
                FROM sealed_breakdown_cache sbc
                WHERE sbc.tcgplayer_id IN ({ph})
            """, tuple(tcg_ids))
            bd_map = {r["tcgplayer_id"]: dict(r) for r in rows}

            # Compute deep value for each parent across all variants
            if bd_map:
                try:
                    all_comps = db.query(f"""
                        SELECT sbc.tcgplayer_id AS parent_id, sbv.id AS variant_id,
                               sbco.tcgplayer_id AS comp_tcg_id, sbco.quantity_per_parent,
                               sbco.market_price AS comp_market
                        FROM sealed_breakdown_cache sbc
                        JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                        LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                        WHERE sbc.tcgplayer_id IN ({ph}) AND sbco.tcgplayer_id IS NOT NULL
                    """, tuple(tcg_ids))
                    # Get which components have their own recipes
                    _comp_ids = list(set(c["comp_tcg_id"] for c in all_comps if c["comp_tcg_id"]))
                    _cbd_map = {}
                    if _comp_ids:
                        _cbph = ",".join(["%s"] * len(_comp_ids))
                        _cbd_rows = db.query(
                            f"SELECT tcgplayer_id, best_variant_market FROM sealed_breakdown_cache WHERE tcgplayer_id IN ({_cbph})",
                            tuple(_comp_ids))
                        _cbd_map = {int(r["tcgplayer_id"]): float(r["best_variant_market"] or 0) for r in _cbd_rows}
                    # Compute best deep value per parent across all variants
                    _by_parent_variant = {}
                    for c in all_comps:
                        key = (c["parent_id"], c["variant_id"])
                        _by_parent_variant.setdefault(key, []).append(c)
                    for (pid, _vid), vcomps in _by_parent_variant.items():
                        dv = 0.0
                        dv_has = False
                        for vc in vcomps:
                            cbd = _cbd_map.get(vc["comp_tcg_id"], 0)
                            qty = vc["quantity_per_parent"] or 1
                            if cbd > 0:
                                dv += cbd * qty
                                dv_has = True
                            else:
                                dv += float(vc["comp_market"] or 0) * qty
                        if dv_has and dv > 0 and pid in bd_map:
                            existing = bd_map[pid].get("deep_bd_market") or 0
                            if dv > existing:
                                bd_map[pid]["deep_bd_market"] = round(dv, 2)
                except Exception:
                    pass
        except Exception:
            pass  # breakdown table may not exist yet

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



@bp.route("/api/intake/finalize/<session_id>", methods=["POST"])
def finalize(session_id):
    """Legacy finalize — now means 'offer'. Kept for backward compat."""
    return offer_session(session_id)



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
    """Customer accepted the offer.

    New shape (#7):
      - `offer_type` ('cash' or 'credit') — the offer the customer chose.
        Stamps `accepted_offer_type`, re-prices items at the chosen
        percentage, and walk-in sessions short-circuit straight to
        'received' (skip the pickup/mail wait).

    Legacy shape (still supported for the deploy window):
      - No `offer_type` → behaves like the old single-offer accept
        (just flips status to 'accepted'). The dashboard switches to the
        new shape in the same deploy, so this branch only matters for
        in-flight sessions someone has open in another tab.
    """
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("offered",):
        return jsonify({"error": f"Cannot accept — session is '{session['status']}'"}), 400
    data = request.get_json(silent=True) or {}
    fulfillment = data.get("fulfillment_method", "pickup")  # pickup or mail
    tracking = (data.get("tracking_number") or "").strip() or None
    pickup_date = (data.get("pickup_date") or "").strip() or None

    offer_type = (data.get("offer_type") or "").lower().strip()
    if offer_type in ("cash", "credit"):
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

    # Legacy path — preserved for any in-flight client that hasn't picked
    # up the new dashboard. Does NOT set accepted_offer_type, does NOT
    # short-circuit walk-ins. Once the dashboard deploy is universal we
    # can drop this branch.
    db.execute("""
        UPDATE intake_sessions
        SET status = 'accepted', accepted_at = CURRENT_TIMESTAMP,
            fulfillment_method = %s, tracking_number = %s, pickup_date = %s
        WHERE id = %s
    """, (fulfillment, tracking, pickup_date, session_id))
    return jsonify({"success": True, "status": "accepted", "fulfillment_method": fulfillment})



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
