"""
TCG Store - Intake Service
Flask API for collection intake (sealed via Collectr CSV, raw via manual entry).

Endpoints:
    Dashboard:
        GET  /                              - Intake dashboard UI

    Collectr CSV Flow:
        POST /api/intake/upload-collectr    - Upload & parse Collectr CSV
        POST /api/intake/upload-collectr-html - Parse pasted Collectr HTML
        GET  /api/intake/session/<id>       - Get session details + items
        POST /api/intake/map-item           - Map item to tcgplayer_id
        POST /api/intake/finalize/<id>      - Finalize session

    Raw Card Manual Entry:
        POST /api/intake/add-raw-card       - Add single raw card to session
        POST /api/intake/create-session     - Create empty session (for manual entry)

    PPT Integration:
        POST /api/ppt/lookup-card           - Lookup raw card by tcgplayer_id
        POST /api/ppt/lookup-sealed         - Lookup sealed product by tcgplayer_id
        POST /api/ppt/parse-title           - Fuzzy match product name

    Product Mappings:
        GET  /api/mappings                  - List cached mappings

    Session Management:
        GET  /api/intake/sessions           - List sessions by status

    Barcode:
        GET  /api/barcode/<barcode_id>.png  - Get barcode label image

    Health:
        GET  /health                        - Health check
"""

import os
import json
import hashlib
import logging
import time
import requests as _requests
from decimal import Decimal, InvalidOperation

from flask import Flask, Blueprint, request, jsonify, render_template, send_file, Response, g
from flask_cors import CORS
from functools import wraps

import db
from ppt_client import PPTClient, PPTError
from collectr_parser import parse_collectr_csv
from collectr_html_parser import parse_collectr_html
from generic_csv_parser import parse_generic_csv, detect_csv_columns
from barcode_gen import generate_barcode_image
from shopify_client import ShopifyClient
from cache_manager import CacheManager
import intake

# ==========================================
# APP SETUP
# ==========================================

app = Flask(__name__)
CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

# Initialize PPT client (optional — gracefully degrades if no key)
PPT_API_KEY = os.getenv("PPT_API_KEY")
ppt: PPTClient | None = None
if PPT_API_KEY:
    ppt = PPTClient(PPT_API_KEY)
    app.logger.info("PPT client initialized")
else:
    app.logger.warning("PPT_API_KEY not set — price lookups will be unavailable")

# Initialize Shopify client (optional — for store inventory lookups)
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
shopify: ShopifyClient | None = None
if SHOPIFY_TOKEN and SHOPIFY_STORE:
    shopify = ShopifyClient(SHOPIFY_TOKEN, SHOPIFY_STORE)
    app.logger.info(f"Shopify client initialized for {SHOPIFY_STORE}")
else:
    app.logger.warning("SHOPIFY_TOKEN/SHOPIFY_STORE not set — store lookups unavailable")

cache_mgr = CacheManager(db, shopify, table_prefix="inventory_", cache_all_products=True)

# Register shared breakdown blueprint (replaces breakdown-cache, PPT search, store-prices routes)
from breakdown_routes import create_breakdown_blueprint
app.register_blueprint(create_breakdown_blueprint(db, ppt_getter=lambda: ppt))

# Serve shared static assets (pf_theme.css, pf_ui.js) at /pf-static/
_pf_static = Blueprint(
    "pf_static", __name__,
    static_folder=os.path.join(os.path.dirname(__file__), "..", "shared", "static"),
    static_url_path="/pf-static",
)
app.register_blueprint(_pf_static)

# Ingest service URL — used to proxy listing creation requests
INGEST_INTERNAL_URL = os.getenv("INGEST_INTERNAL_URL", "").rstrip("/")

# ── Auth ──────────────────────────────────────────────────────────
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "secret")

def check_auth(user, pwd):
    return user == DASHBOARD_USER and pwd == DASHBOARD_PASS

def authenticate():
    return Response(
        "🚫 Access Denied. You must provide valid credentials.", 401,
        {"WWW-Authenticate": 'Basic realm="Login Required"'}
    )

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if getattr(g, 'user', None):
            return f(*args, **kwargs)  # JWT already validated
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


@app.before_request
def _check_jwt_auth():
    """Validate JWT cookie from admin portal."""
    if request.path in ('/health', '/ping', '/favicon.ico') or request.path.startswith(('/static', '/pf-static')):
        return
    try:
        from auth import require_auth as jwt_auth
        result = jwt_auth()
        if result is None:
            return None  # JWT valid — skip old basic auth
    except Exception:
        pass

@app.after_request
def _add_admin_bar(response):
    try:
        from auth import inject_admin_bar, get_current_user
        if get_current_user():
            return inject_admin_bar(response)
    except Exception:
        pass
    return response

# Initialize DB pool on first request
@app.before_request
def ensure_db():
    try:
        db.get_pool()
    except RuntimeError:
        db.init_pool()


@app.teardown_appcontext
def close_db(exception):
    pass  # pool persists across requests


# ==========================================
# DASHBOARD
# ==========================================

@app.route("/")
@requires_auth
def index():
    return render_template("intake_dashboard.html")


# ==========================================
# COLLECTR CSV UPLOAD
# ==========================================

@app.route("/api/intake/upload-collectr", methods=["POST"])
def upload_collectr():
    """Upload and parse a Collectr CSV export."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    customer_name = request.form.get("customer_name", "").strip() or "Unknown"
    try:
        offer_pct = Decimal(request.form.get("offer_percentage", "75"))
    except InvalidOperation:
        return jsonify({"error": "Invalid offer_percentage"}), 400
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

    # Check for duplicate import
    dup_session = intake.check_duplicate_import(result.file_hash)
    if dup_session:
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

    # Create session
    session = intake.create_session(
        customer_name=customer_name or result.portfolio_name,
        session_type=session_type,
        offer_percentage=offer_pct,
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
        offer_price = item.market_price * item.quantity * (offer_pct / Decimal("100"))
        unit_cost = offer_price / item.quantity if item.quantity > 0 else Decimal("0")

        # Check for cached tcgplayer_id mapping and/or shopify link
        tcgplayer_id = intake.get_cached_mapping(item.product_name, product_type)
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


@app.route("/api/intake/upload-collectr-html", methods=["POST"])
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
    except InvalidOperation:
        return jsonify({"error": "Invalid offer_percentage"}), 400

    if not html_content:
        return jsonify({"error": "No HTML content provided"}), 400

    # Parse
    result = parse_collectr_html(html_content)

    if result.errors and not result.items:
        return jsonify({"error": "Failed to parse HTML", "details": result.errors}), 400

    # Check for duplicate import (skip if appending to existing session)
    if not existing_session_id:
        dup_session = intake.check_duplicate_import(result.file_hash)
        if dup_session:
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
            offer_percentage=offer_pct,
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
        offer_price = item.market_price * item.quantity * (offer_pct / Decimal("100"))
        unit_cost = offer_price / item.quantity if item.quantity > 0 else Decimal("0")

        tcgplayer_id = intake.get_cached_mapping(item.product_name, product_type)
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

@app.route("/api/intake/preview-csv", methods=["POST"])
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


@app.route("/api/intake/upload-generic-csv", methods=["POST"])
def upload_generic_csv():
    """Upload a generic CSV with flexible column mapping."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    customer_name = request.form.get("customer_name", "").strip() or "Unknown"
    try:
        offer_pct = Decimal(request.form.get("offer_percentage", "75"))
    except InvalidOperation:
        return jsonify({"error": "Invalid offer_percentage"}), 400
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

    # Check for duplicate import
    dup_session = intake.check_duplicate_import(result.file_hash)
    if dup_session:
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

    # Create session
    session = intake.create_session(
        customer_name=customer_name,
        session_type=session_type,
        offer_percentage=offer_pct,
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
        offer_price = item.market_price * item.quantity * (offer_pct / Decimal("100"))
        unit_cost = offer_price / item.quantity if item.quantity > 0 else Decimal("0")

        # Check for cached tcgplayer_id mapping (or use the one from CSV)
        tcgplayer_id = item.tcgplayer_id or intake.get_cached_mapping(item.product_name, product_type)
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

@app.route("/api/intake/create-session", methods=["POST"])
def create_session():
    """Create an empty intake session (for manual raw card entry)."""
    data = request.json or {}
    customer_name = data.get("customer_name", "Walk-in")
    session_type = data.get("session_type", "raw")
    try:
        offer_pct = Decimal(str(data.get("offer_percentage", "65")))
    except InvalidOperation:
        return jsonify({"error": "Invalid offer_percentage"}), 400

    session = intake.create_session(
        customer_name=customer_name,
        session_type=session_type,
        offer_percentage=offer_pct,
        employee_id=data.get("employee_id"),
        notes=data.get("notes"),
    )
    # Set distribution flag if provided
    if data.get("is_distribution"):
        db.execute("UPDATE intake_sessions SET is_distribution = TRUE WHERE id = %s", (session["id"],))
        session["is_distribution"] = True
    return jsonify({"success": True, "session": _serialize(session)})


@app.route("/api/intake/session/<session_id>", methods=["GET"])
def get_session(session_id):
    """Get session details, items, and breakdown summaries for sealed items."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    items = intake.get_session_items(session_id)

    # Attach breakdown summaries to sealed items that have a tcgplayer_id
    tcg_ids = list({int(i["tcgplayer_id"]) for i in items if i.get("tcgplayer_id")})

    # JIT refresh stale component market prices in background (don't block response)
    if tcg_ids and ppt:
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
                    args=([v["variant_id"] for v in _vids], db, ppt), daemon=True).start()
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

    return jsonify({
        "session": _serialize(session),
        "items": serialized,
    })


@app.route("/api/intake/sessions", methods=["GET"])
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


@app.route("/api/intake/session/<session_id>/offer-percentage", methods=["POST"])
def update_offer_percentage(session_id):
    """Update the offer percentage and recalculate all item offers."""
    data = request.json or {}
    try:
        new_pct = Decimal(str(data.get("offer_percentage", "65")))
    except Exception:
        return jsonify({"error": "Invalid offer_percentage"}), 400

    try:
        session = intake.update_offer_percentage(session_id, new_pct)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/session/<session_id>/refresh-prices", methods=["POST"])
def refresh_session_prices(session_id):
    """
    Fetch current PPT prices for linked items in a session.
    Supports pagination: send {"offset": N} to continue from item N.
    Fires requests until rate-limited, then returns partial results
    with retry_after and next_offset so the frontend can continue.
    """
    if not ppt:
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
        if ppt.should_throttle():
            rate_info = ppt.get_rate_limit_info()
            retry_after = rate_info.get("retry_after") or 60
            rate_limited = True
            app.logger.info(f"PPT throttle: minute_remaining={rate_info['minute_remaining']}, "
                            f"pausing at offset {idx} (fetched {fetched_count}), retry in {retry_after}s")
            break

        ppt_price = None
        ppt_low = None
        ppt_name = None
        error = None

        try:
            if ptype == "sealed":
                ppt_data = ppt.get_sealed_product_by_tcgplayer_id(tcg_id)
            else:
                ppt_data = ppt.get_card_by_tcgplayer_id(tcg_id)

            if ppt_data:
                if ptype == "sealed":
                    unopened = ppt_data.get("unopenedPrice")
                    prices = ppt_data.get("prices") or {}
                    if isinstance(prices, dict):
                        prices_low = prices.get("low")
                    else:
                        prices_low = None
                    ppt_price = unopened
                    ppt_low = prices_low
                    ppt_name = ppt_data.get("name")
                elif is_graded and grade_co and grade_val:
                    # Use eBay smartMarketPrice for graded cards
                    ppt_price = PPTClient.get_graded_price(ppt_data, grade_co, grade_val)
                    ppt_low = None  # no "low" concept for graded eBay data
                    ppt_name = ppt_data.get("name")
                else:
                    prices = ppt_data.get("prices", {})
                    # Use market price as default; per-condition resolved per-item in comparisons below
                    ppt_price = prices.get("market")
                    ppt_low = prices.get("low")
                    ppt_name = ppt_data.get("name")
                    # Store full prices dict for per-condition lookup in comparisons step
                    price_cache[(tcg_id, ptype, is_graded, grade_co, grade_val)] = {
                        "ppt_price": ppt_price, "ppt_low": ppt_low, "ppt_name": ppt_name,
                        "error": None, "raw_prices": prices,
                    }
                    fetched_count += 1
                    continue

            fetched_count += 1

        except PPTError as e:
            status_code = getattr(e, 'status_code', None)
            if status_code == 429:
                # Shouldn't happen since we check should_throttle, but handle gracefully
                body = getattr(e, 'body', {}) or {}
                retry_after = body.get("retry_after", 60) if isinstance(body, dict) else 60
                rate_limited = True
                app.logger.warning(f"PPT 429 despite throttle check — pausing at {idx}, retry in {retry_after}s")
                break
            elif status_code == 403:
                error = str(e)
                app.logger.warning(f"PPT 403 for {tcg_id}: {e}")
                price_cache[(tcg_id, ptype, is_graded, grade_co, grade_val)] = {"ppt_price": None, "ppt_low": None, "ppt_name": None, "error": error}
                rate_limited = True
                retry_after = None
                break
            else:
                error = str(e)
                app.logger.warning(f"PPT error for {tcg_id}: {e}")
        except Exception as e:
            error = str(e)
            app.logger.warning(f"Unexpected error for {tcg_id}: {e}")

        price_cache[(tcg_id, ptype, is_graded, grade_co, grade_val)] = {"ppt_price": ppt_price, "ppt_low": ppt_low, "ppt_name": ppt_name, "error": error}

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

        comparisons.append({
            "item_id": item["id"],
            "product_name": item.get("product_name"),
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


@app.route("/api/intake/update-item-price", methods=["POST"])
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

@app.route("/api/intake/map-item", methods=["POST"])
def map_item():
    """Map an intake item to a tcgplayer_id, with optional price override."""
    data = request.json or {}
    item_id = data.get("item_id")
    tcgplayer_id = data.get("tcgplayer_id")

    if not item_id or not tcgplayer_id:
        return jsonify({"error": "item_id and tcgplayer_id required"}), 400

    try:
        tcgplayer_id = int(tcgplayer_id)
    except (ValueError, TypeError):
        return jsonify({"error": "tcgplayer_id must be an integer"}), 400

    # Price override from the comparison UI (user picked Collectr, PPT, or custom)
    new_price = None
    override_price = data.get("override_price")
    if override_price is not None:
        try:
            new_price = Decimal(str(override_price))
        except Exception:
            pass

    # Legacy: verify_price still works if called directly
    if new_price is None and data.get("verify_price") and ppt:
        item = db.query_one("SELECT product_type FROM intake_items WHERE id = %s", (item_id,))
        if item:
            try:
                if item["product_type"] == "sealed":
                    ppt_data = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
                else:
                    ppt_data = ppt.get_card_by_tcgplayer_id(tcgplayer_id)
                new_price = PPTClient.extract_market_price(ppt_data)
            except PPTError as e:
                app.logger.warning(f"PPT verification failed for {tcgplayer_id}: {e}")

    try:
        updated = intake.map_item(
            item_id, tcgplayer_id, new_price,
            product_name=data.get("product_name"),
            set_name=data.get("set_name"),
            card_number=data.get("card_number"),
            rarity=data.get("rarity"),
        )

        # Auto-link other unmapped items in the same session with the same product_name
        # so you don't have to manually relink duplicates
        siblings_updated = 0
        session_id = data.get("session_id") or updated.get("session_id")
        if session_id and new_price is not None:
            siblings = db.query("""
                SELECT id FROM intake_items
                WHERE session_id = %s
                  AND id != %s
                  AND product_name = %s
                  AND (tcgplayer_id IS NULL OR is_mapped = FALSE)
                  AND item_status IN ('good', 'damaged')
            """, (session_id, item_id, updated.get("product_name") or data.get("product_name", "")))
            for sib in siblings:
                try:
                    intake.map_item(
                        sib["id"], tcgplayer_id, new_price,
                        product_name=data.get("product_name"),
                        set_name=data.get("set_name"),
                        card_number=data.get("card_number"),
                        rarity=data.get("rarity"),
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
# ACCEPT PRICE WITHOUT TCG LINK
# ==========================================

@app.route("/api/intake/item/<item_id>/accept-price", methods=["POST"])
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
        "SELECT offer_percentage FROM intake_sessions WHERE id = %s",
        (item["session_id"],)
    )
    if not session:
        return jsonify({"error": "Session not found"}), 404

    market_price = Decimal(str(override_price)) if override_price is not None else item["market_price"]
    offer_pct = session["offer_percentage"]
    offer_price = market_price * item["quantity"] * (offer_pct / Decimal("100"))
    unit_cost_basis = offer_price / item["quantity"] if item["quantity"] > 0 else Decimal("0")

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
        shopify_product_id=str(store_product_id) if store_product_id else None,
        shopify_product_name=store_product_name or None,
    )

    intake._recalculate_session_totals(item["session_id"])
    return jsonify({"success": True, "item": _serialize(updated)})


# ==========================================
# RAW CARD MANUAL ENTRY
# ==========================================

@app.route("/api/intake/add-raw-card", methods=["POST"])
def add_raw_card():
    """Add a single raw card to a session via manual form entry."""
    data = request.json or {}

    required = ["session_id", "tcgplayer_id", "card_name", "condition", "quantity"]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    try:
        tcgplayer_id = int(data["tcgplayer_id"])
        quantity = int(data["quantity"])
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"Invalid number: {e}"}), 400

    # Get price from PPT
    market_price = None
    if ppt:
        try:
            card_data = ppt.get_card_by_tcgplayer_id(tcgplayer_id)
            if card_data:
                is_graded = bool(data.get("is_graded", False))
                grade_company = (data.get("grade_company") or "").strip()
                grade_value = (data.get("grade_value") or "").strip()

                if is_graded and grade_company and grade_value:
                    # Use graded (PSA/BGS/CGC) eBay market price
                    market_price = PPTClient.get_graded_price(card_data, grade_company, grade_value)
                    if market_price is None:
                        app.logger.warning(
                            f"No graded price for {tcgplayer_id} {grade_company} {grade_value}, "
                            "falling back to NM raw price"
                        )
                        market_price = PPTClient.extract_condition_price(card_data, "NM")
                else:
                    market_price = PPTClient.extract_condition_price(
                        card_data, data["condition"]
                    )
                # Enrich with data from PPT if not provided
                if not data.get("set_name") and card_data.get("setName"):
                    data["set_name"] = card_data["setName"]
                if not data.get("card_number") and card_data.get("cardNumber"):
                    data["card_number"] = card_data["cardNumber"]
                if not data.get("rarity") and card_data.get("rarity"):
                    data["rarity"] = card_data["rarity"]
                if not data.get("card_name") and card_data.get("name"):
                    data["card_name"] = card_data["name"]
        except PPTError as e:
            app.logger.warning(f"PPT lookup failed for {tcgplayer_id}: {e}")

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

@app.route("/api/intake/rejuvenate-session/<session_id>", methods=["POST"])
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


@app.route("/api/intake/cancel-session/<session_id>", methods=["POST"])
def cancel_session(session_id):
    """Cancel an intake session."""
    data = request.get_json(silent=True) or {}
    try:
        result = intake.cancel_session(session_id, reason=data.get("reason"))
        return jsonify({"success": True, "session": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/item/<item_id>/damage", methods=["POST"])
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


@app.route("/api/intake/item/<item_id>/status", methods=["POST"])
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


@app.route("/api/intake/item/<item_id>/missing", methods=["POST"])
def missing_item(item_id):
    """Mark item as missing."""
    try:
        item = intake.mark_item_missing(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/item/<item_id>/rejected", methods=["POST"])
def rejected_item(item_id):
    """Mark item as rejected (seller kept it)."""
    try:
        item = intake.mark_item_rejected(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/item/<item_id>/restore", methods=["POST"])
def restore_item(item_id):
    """Restore a missing/rejected/damaged item back to good."""
    try:
        item = intake.restore_item(item_id)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/item/<item_id>/override-price", methods=["POST"])
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


@app.route("/api/intake/item/<item_id>/apply-breakdown-price", methods=["POST"])
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


@app.route("/api/intake/item/<item_id>/delete", methods=["POST"])
def delete_item(item_id):
    """Permanently delete an item from a session."""
    try:
        session = intake.delete_item(item_id)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/item/<item_id>/update-quantity", methods=["POST"])
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


@app.route("/api/intake/item/<item_id>/update-condition", methods=["POST"])
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
        tcg_id = item.get("tcgplayer_id")
        if tcg_id and ppt:
            try:
                card_data = ppt.get_card_by_tcgplayer_id(int(tcg_id))
                if card_data:
                    variants = PPTClient.extract_variants(card_data)
                    primary = PPTClient.get_primary_printing(card_data) or "Default"
                    if primary not in variants and variants:
                        primary = list(variants.keys())[0]
                    variant_data = variants.get(primary, {})
                    new_price = variant_data.get(new_condition)
                    if new_price is not None:
                        from decimal import Decimal
                        item = intake.update_item_price(
                            item_id, Decimal(str(new_price)), session_id
                        )
                        app.logger.info(
                            f"Condition change {item_id}: {new_condition} -> ${new_price}"
                        )
            except Exception as e:
                app.logger.warning(f"PPT re-price on condition change failed: {e}")
                # Condition is still updated, just price stays the same

        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/intake/item/<item_id>/mark-graded", methods=["POST"])
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

    if new_price is None and ppt and item.get("tcgplayer_id"):
        try:
            card_data = ppt.get_card_by_tcgplayer_id(int(item["tcgplayer_id"]))
            if card_data:
                new_price = PPTClient.get_graded_price(card_data, grade_company, grade_value)
                if new_price is None:
                    app.logger.warning(
                        f"No graded price for {item['tcgplayer_id']} {grade_company} {grade_value}"
                    )
        except Exception as e:
            app.logger.warning(f"PPT graded price fetch failed: {e}")

    if new_price is not None:
        item = intake.update_item_price(item_id, new_price, session_id)

    intake._recalculate_session_totals(session_id)
    return jsonify({"success": True, "item": _serialize(item), "new_price": float(new_price) if new_price else None})


@app.route("/api/intake/add-sealed-item", methods=["POST"])
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

@app.route("/api/intake/finalize/<session_id>", methods=["POST"])
def finalize(session_id):
    """Legacy finalize — now means 'offer'. Kept for backward compat."""
    return offer_session(session_id)


@app.route("/api/intake/session/<session_id>/offer", methods=["POST"])
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


@app.route("/api/intake/session/<session_id>/accept", methods=["POST"])
def accept_session(session_id):
    """Customer accepted the offer. Optionally set fulfillment method and tracking."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("offered",):
        return jsonify({"error": f"Cannot accept — session is '{session['status']}'"}), 400
    data = request.get_json(silent=True) or {}
    fulfillment = data.get("fulfillment_method", "pickup")  # pickup or mail
    tracking = data.get("tracking_number", "").strip() or None
    pickup_date = data.get("pickup_date", "").strip() or None
    db.execute("""
        UPDATE intake_sessions
        SET status = 'accepted', accepted_at = CURRENT_TIMESTAMP,
            fulfillment_method = %s, tracking_number = %s, pickup_date = %s
        WHERE id = %s
    """, (fulfillment, tracking, pickup_date, session_id))
    return jsonify({"success": True, "status": "accepted", "fulfillment_method": fulfillment})


@app.route("/api/intake/session/<session_id>/receive", methods=["POST"])
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


@app.route("/api/intake/session/<session_id>/reject", methods=["POST"])
def reject_session(session_id):
    """Customer rejected the offer."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] in ("ingested",):
        return jsonify({"error": "Cannot reject — already ingested"}), 400
    db.execute("UPDATE intake_sessions SET status = 'rejected', rejected_at = CURRENT_TIMESTAMP WHERE id = %s", (session_id,))
    return jsonify({"success": True, "status": "rejected"})


@app.route("/api/intake/session/<session_id>/reopen", methods=["POST"])
def reopen_session(session_id):
    """Reopen a session back to in_progress (for edits before ingest)."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] in ("ingested",):
        return jsonify({"error": "Cannot reopen — already ingested"}), 400
    db.execute("UPDATE intake_sessions SET status = 'in_progress' WHERE id = %s", (session_id,))
    return jsonify({"success": True, "status": "in_progress"})


@app.route("/api/intake/session/<session_id>/toggle-distribution", methods=["POST"])
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


@app.route("/api/intake/session/<session_id>/tracking", methods=["POST"])
def update_tracking(session_id):
    """Update tracking number/link for a mailed session."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    data = request.get_json(silent=True) or {}
    tracking = data.get("tracking_number", "").strip() or None
    db.execute("UPDATE intake_sessions SET tracking_number = %s WHERE id = %s", (tracking, session_id))
    return jsonify({"success": True, "tracking_number": tracking})


@app.route("/api/intake/session/<session_id>/pickup-date", methods=["POST"])
def update_pickup_date(session_id):
    """Update pickup date for an accepted-pickup session."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    data = request.get_json(silent=True) or {}
    pickup_date = data.get("pickup_date", "").strip() or None
    db.execute("UPDATE intake_sessions SET pickup_date = %s WHERE id = %s", (pickup_date, session_id))
    return jsonify({"success": True, "pickup_date": pickup_date})



@app.route("/api/intake/session/<session_id>/export-csv")
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

@app.route("/api/ppt/lookup-card", methods=["POST"])
def ppt_lookup_card():
    """Look up a raw card by tcgplayer_id. Returns card data + variant/condition prices."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    tcgplayer_id = data.get("tcgplayer_id")
    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    try:
        card_data = ppt.get_card_by_tcgplayer_id(int(tcgplayer_id))
        if not card_data:
            return jsonify({"error": "Card not found in PPT"}), 404

        # Extract structured variant → condition → price data
        variants = PPTClient.extract_variants(card_data)
        primary_printing = PPTClient.get_primary_printing(card_data)

        # Debug logging
        prices_raw = card_data.get("prices", {})
        app.logger.info(f"CARD LOOKUP {tcgplayer_id}: prices keys={list(prices_raw.keys()) if isinstance(prices_raw, dict) else type(prices_raw)}")
        if isinstance(prices_raw, dict):
            # Log top-level conditions
            raw_conditions = prices_raw.get("conditions", {})
            if raw_conditions and isinstance(raw_conditions, dict):
                app.logger.info(f"  TOP-LEVEL conditions keys={list(raw_conditions.keys())}")
                for ck, cv in raw_conditions.items():
                    app.logger.info(f"    condition '{ck}' -> price={cv.get('price') if isinstance(cv, dict) else cv}")
            # Log variants
            raw_variants = prices_raw.get("variants", {})
            app.logger.info(f"  raw variants keys={list(raw_variants.keys()) if isinstance(raw_variants, dict) else type(raw_variants)}")
            for vname, vconds in (raw_variants.items() if isinstance(raw_variants, dict) else []):
                if isinstance(vconds, dict):
                    app.logger.info(f"  variant '{vname}': cond keys={list(vconds.keys())}")
                    for ck, cv in vconds.items():
                        app.logger.info(f"    '{ck}' -> {cv if not isinstance(cv, dict) else {k: cv[k] for k in list(cv.keys())[:3]}}")
        app.logger.info(f"  extract_variants result={variants}")
        app.logger.info(f"  primary_printing={primary_printing}")

        # Extract graded (PSA/BGS/CGC) prices
        graded_prices = PPTClient.extract_graded_prices(card_data)

        return jsonify({
            "card": card_data,
            "variants": variants,            # {"Holofoil": {"NM": 103.85, "LP": 87.80, ...}, ...}
            "primary_printing": primary_printing,  # "Holofoil"
            "graded_prices": graded_prices,  # {"PSA": {"10": {"avg": 450, ...}, "9": {...}}, ...}
        })
    except PPTError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/ppt/lookup-sealed", methods=["POST"])
def ppt_lookup_sealed():
    """Look up a sealed product by tcgplayer_id via PPT."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    tcgplayer_id = data.get("tcgplayer_id")
    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    try:
        product_data = ppt.get_sealed_product_by_tcgplayer_id(int(tcgplayer_id))
        if not product_data:
            return jsonify({"error": "Sealed product not found in PPT"}), 404

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
    except PPTError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/ppt/debug-card/<int:tcgplayer_id>")
def debug_card_raw(tcgplayer_id):
    """Debug: dump raw PPT response for a card — bare HTTP, no abstraction."""
    import requests as _requests
    if not ppt:
        return jsonify({"error": "PPT not configured"}), 503

    results = {}
    base = f"{ppt.base_url}/v2/cards"
    combos = {
        "bare":         {"tcgPlayerId": str(tcgplayer_id), "limit": 1},
        "includeEbay":  {"tcgPlayerId": str(tcgplayer_id), "limit": 1, "includeEbay": "true"},
        "includeBoth":  {"tcgPlayerId": str(tcgplayer_id), "limit": 1, "includeHistory": "true", "includeEbay": "true"},
    }
    for label, params in combos.items():
        try:
            r = _requests.get(base, headers=ppt.headers, params=params, timeout=15)
            results[label] = {
                "status": r.status_code,
                "url": r.url,
                "body": r.json() if r.headers.get("content-type","").startswith("application/json") else r.text[:500],
            }
        except Exception as e:
            import traceback
            results[label] = {"error": str(e), "type": type(e).__name__, "tb": traceback.format_exc()}
    return jsonify(results)


@app.route("/api/ppt/debug-sealed/<int:tcgplayer_id>")
def debug_sealed(tcgplayer_id):
    """Debug: compare search vs direct lookup for a sealed product."""
    if not ppt:
        return jsonify({"error": "PPT not configured"}), 503
    results = {}
    
    # Test 1: Direct lookup by tcgPlayerId
    try:
        url = f"{ppt.base_url}/v2/sealed-products"
        params = {"tcgPlayerId": str(tcgplayer_id)}
        raw = ppt._get(url, params)
        results["direct_lookup"] = {
            "url": f"{url}?tcgPlayerId={tcgplayer_id}",
            "response": raw,
        }
    except PPTError as e:
        results["direct_lookup"] = {"error": str(e), "status": e.status_code}

    # Test 2: Search (to compare structure)
    try:
        url2 = f"{ppt.base_url}/v2/sealed-products"
        params2 = {"search": "Elite Trainer Box", "limit": 1}
        raw2 = ppt._get(url2, params2)
        results["search_example"] = {
            "url": f"{url2}?search=Elite+Trainer+Box&limit=1",
            "response": raw2,
        }
    except PPTError as e:
        results["search_example"] = {"error": str(e), "status": e.status_code}

    return jsonify(results)


# PPT search-sealed + search-cards now served by shared breakdown blueprint


@app.route("/api/ppt/parse-title", methods=["POST"])
def ppt_parse_title():
    """Fuzzy-match a product name via PPT's parse-title endpoint (best for card titles)."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    title = data.get("title", "").strip()
    if not title:
        return jsonify({"error": "title required"}), 400

    matches = ppt.parse_title(title)
    return jsonify({"matches": matches})


# ==========================================
# PRODUCT MAPPINGS
# ==========================================

@app.route("/api/mappings", methods=["GET"])
def list_mappings():
    """List cached product mappings."""
    product_type = request.args.get("product_type")
    mappings = intake.get_all_mappings(product_type)
    return jsonify({"mappings": [_serialize(m) for m in mappings]})


# ==========================================
# BARCODE
# ==========================================

@app.route("/api/barcode/<barcode_id>.png")
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

@app.route("/health")
def health():
    try:
        db.query("SELECT 1")
        ppt_status = "configured" if ppt else "not configured"
        return jsonify({"status": "healthy", "database": "connected", "ppt": ppt_status})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


# ==========================================
# HELPERS
# ==========================================

def _serialize(obj):
    """Convert a dict with Decimal/datetime values to JSON-safe types."""
    if obj is None:
        return None
    # Fields that must be real JS booleans (DB may return 0/1 integers)
    BOOL_FIELDS = {"is_graded", "is_mapped", "is_distribution", "needsDetailedScrape"}
    out = {}
    for k, v in obj.items():
        if k in BOOL_FIELDS:
            out[k] = bool(v)
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ==========================================
# SHOPIFY STORE INTEGRATION
# ==========================================

@app.route("/api/shopify/sync", methods=["POST"])
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
            app.logger.error(f"Shopify sync trigger failed: {e}")
            yield json.dumps({"status": "error", "error": str(e)}) + "\n"

    return app.response_class(generate(), mimetype="application/x-ndjson")


# ═══════════════════════════════════════════════════════════════════════════════
# LISTING CREATION (proxy to ingest enrichment pipeline)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/create-listing", methods=["POST"])
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
                item = db.query_one("SELECT product_name, product_type FROM intake_items WHERE id = %s", (item_id,))
                if item:
                    intake.save_mapping(
                        item["product_name"],
                        int(tcgplayer_id),
                        item.get("product_type", "sealed"),
                        shopify_product_id=shopify_product_id,
                        shopify_product_name=product_name or item["product_name"],
                    )
            except Exception as save_err:
                app.logger.warning(f"Could not persist shopify_product_id after create-listing: {save_err}")

        return jsonify(result), resp.status_code
    except _requests.Timeout:
        return jsonify({"error": "Listing creation timed out — it may still be processing"}), 504
    except Exception as e:
        app.logger.exception("proxy_create_listing failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/cache/status")
def cache_status():
    """Return cache health and staleness info."""
    return jsonify(cache_mgr.get_status())


@app.route("/api/cache/invalidate", methods=["POST"])
def cache_invalidate():
    """Explicitly invalidate and trigger cache refresh. Called by ingest after push-live."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "manual")
    cache_mgr.invalidate(reason)
    return jsonify({"success": True, "reason": reason})


@app.route("/api/cache/refresh", methods=["POST"])
def cache_refresh():
    """Manual full cache refresh trigger from UI."""
    cache_mgr.invalidate("manual")
    return jsonify({"success": True, "message": "Refresh triggered in background"})


@app.route("/api/shopify/status")
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


@app.route("/api/shopify/session/<session_id>/store-check")
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
                        args=([v["variant_id"] for v in _vids], db, ppt), daemon=True).start()
            except Exception as e:
                app.logger.warning(f"Component price refresh skipped: {e}")

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
            app.logger.warning(f"Breakdown cache lookup failed (run migrate_breakdown_cache.py?): {e}")

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


@app.route("/api/store/search", methods=["GET"])
@app.route("/api/store/search", methods=["GET"])
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
        app.logger.error(f"store_search error: {e}")
        return jsonify({"results": [], "error": str(e)})

# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")
