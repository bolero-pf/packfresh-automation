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
import hashlib
import logging
import time
from decimal import Decimal, InvalidOperation

from flask import Flask, request, jsonify, render_template, send_file, Response
from flask_cors import CORS
from functools import wraps

import db
from ppt_client import PPTClient, PPTError
from collectr_parser import parse_collectr_csv
from collectr_html_parser import parse_collectr_html
from barcode_gen import generate_barcode_image
from shopify_client import ShopifyClient, ShopifyError
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
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


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
    if result.raw_count > 0 and result.sealed_count > 0:
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
    processed = []
    for item in result.items:
        offer_price = item.market_price * item.quantity * (offer_pct / Decimal("100"))
        unit_cost = offer_price / item.quantity if item.quantity > 0 else Decimal("0")

        # Check for cached tcgplayer_id mapping
        tcgplayer_id = intake.get_cached_mapping(item.product_name, item.product_type)

        processed.append({
            "product_name": item.product_name,
            "product_type": item.product_type,
            "set_name": item.set_name,
            "card_number": item.card_number,
            "condition": item.condition,
            "rarity": item.rarity,
            "quantity": item.quantity,
            "market_price": item.market_price,
            "offer_price": offer_price,
            "unit_cost_basis": unit_cost,
            "tcgplayer_id": tcgplayer_id,
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
    """Parse pasted Collectr HTML (from portfolio page) into a session."""
    data = request.json or {}
    html_content = data.get("html", "").strip()
    customer_name = data.get("customer_name", "").strip() or "Unknown"
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

    # Check for duplicate import
    dup_session = intake.check_duplicate_import(result.file_hash)
    if dup_session:
        return jsonify({
            "error": "This exact HTML has already been imported",
            "existing_session_id": dup_session,
        }), 409

    session_type = "sealed"  # Collectr HTML portfolios are always sealed

    # Create session
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

    # Process items
    processed = []
    for item in result.items:
        offer_price = item.market_price * item.quantity * (offer_pct / Decimal("100"))
        unit_cost = offer_price / item.quantity if item.quantity > 0 else Decimal("0")

        tcgplayer_id = intake.get_cached_mapping(item.product_name, item.product_type)

        processed.append({
            "product_name": item.product_name,
            "product_type": item.product_type,
            "set_name": item.set_name,
            "card_number": item.card_number,
            "condition": item.condition,
            "rarity": item.rarity,
            "quantity": item.quantity,
            "market_price": item.market_price,
            "offer_price": offer_price,
            "unit_cost_basis": unit_cost,
            "tcgplayer_id": tcgplayer_id,
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
    """Get session details and items."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    items = intake.get_session_items(session_id)
    return jsonify({
        "session": _serialize(session),
        "items": [_serialize(i) for i in items],
    })


@app.route("/api/intake/sessions", methods=["GET"])
def list_sessions():
    """List sessions by status."""
    status = request.args.get("status", "in_progress")
    sessions = intake.list_sessions(status)
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

    # Deduplicate: build ordered list of unique (tcg_id, ptype) to fetch
    seen = set()
    unique_lookups = []
    for item in linked:
        key = (item["tcgplayer_id"], item.get("product_type", "sealed"))
        if key not in seen:
            seen.add(key)
            unique_lookups.append(key)

    # Fetch starting from offset
    price_cache = {}
    rate_limited = False
    retry_after = None
    fetched_count = 0

    for idx in range(offset, len(unique_lookups)):
        tcg_id, ptype = unique_lookups[idx]

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
                        prices_market = prices.get("market")
                        prices_low = prices.get("low")
                    else:
                        prices_market = None
                        prices_low = None
                    ppt_price = unopened
                    ppt_low = prices_low
                    ppt_name = ppt_data.get("name")
                else:
                    prices = ppt_data.get("prices", {})
                    ppt_price = prices.get("market")
                    ppt_low = prices.get("low")
                    ppt_name = ppt_data.get("name")

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
                price_cache[(tcg_id, ptype)] = {"ppt_price": None, "ppt_low": None, "ppt_name": None, "error": error}
                rate_limited = True
                retry_after = None
                break
            else:
                error = str(e)
                app.logger.warning(f"PPT error for {tcg_id}: {e}")
        except Exception as e:
            error = str(e)
            app.logger.warning(f"Unexpected error for {tcg_id}: {e}")

        price_cache[(tcg_id, ptype)] = {"ppt_price": ppt_price, "ppt_low": ppt_low, "ppt_name": ppt_name, "error": error}

    # Build comparisons for ALL linked items (using whatever we've fetched so far)
    comparisons = []
    for item in linked:
        tcg_id = item["tcgplayer_id"]
        ptype = item.get("product_type", "sealed")
        cached = price_cache.get((tcg_id, ptype))

        ppt_price = cached["ppt_price"] if cached else None
        ppt_low = cached["ppt_low"] if cached else None
        ppt_name = cached.get("ppt_name") if cached else None

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
            "collectr_price": collectr_price,
            "ppt_market": ppt_price_f,
            "ppt_low": float(ppt_low) if ppt_low is not None else None,
            "delta_pct": delta_pct,
            "significant": abs(delta_pct) > 10 if delta_pct is not None else False,
            "error": cached.get("error") if cached else None,
            "fetched": cached is not None,
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
        return jsonify({
            "success": True,
            "item": _serialize(updated),
            "price_updated": new_price is not None,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 404


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
    db.execute("""
        UPDATE intake_sessions
        SET status = 'accepted', accepted_at = CURRENT_TIMESTAMP,
            fulfillment_method = %s, tracking_number = %s
        WHERE id = %s
    """, (fulfillment, tracking, session_id))
    return jsonify({"success": True, "status": "accepted", "fulfillment_method": fulfillment})


@app.route("/api/intake/session/<session_id>/receive", methods=["POST"])
def receive_session(session_id):
    """Product received — ready for verification and eventually ingest."""
    session = intake.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("accepted",):
        return jsonify({"error": f"Cannot receive — session is '{session['status']}'"}), 400
    db.execute("UPDATE intake_sessions SET status = 'received', received_at = CURRENT_TIMESTAMP WHERE id = %s", (session_id,))
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
    writer.writerow(["Product Name", "TCGPlayer ID", "Condition", "Quantity", "Unit Price", "Offer Total", "Present", "Notes"])
    for item in active:
        qty = item.get("quantity", 1)
        offer = float(item.get("offer_price") or 0)
        unit = offer / qty if qty > 0 else 0
        writer.writerow([
            item.get("product_name", ""),
            item.get("tcgplayer_id", ""),
            item.get("condition", ""),
            qty,
            f"${unit:.2f}",
            f"${offer:.2f}",
            "",  # Present column — blank for checking off
            "DAMAGED" if item.get("item_status") == "damaged" else "",
        ])
    writer.writerow([])
    writer.writerow(["TOTAL", "", "", sum(i.get("quantity", 1) for i in active), "",
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

        return jsonify({
            "card": card_data,
            "variants": variants,            # {"Holofoil": {"NM": 103.85, "LP": 87.80, ...}, ...}
            "primary_printing": primary_printing,  # "Holofoil"
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


@app.route("/api/ppt/search-sealed", methods=["POST"])
def ppt_search_sealed():
    """Search sealed products by name via PPT /v2/sealed-products?search=...&set=..."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    query = data.get("query", "").strip()
    set_name = data.get("set_name", "").strip() or None
    if not query:
        return jsonify({"error": "query required"}), 400

    try:
        results = ppt.search_sealed_products(query, set_name=set_name, limit=5)
        return jsonify({"results": results})
    except PPTError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/ppt/search-cards", methods=["POST"])
def ppt_search_cards():
    """Search cards by name via PPT /v2/cards?search=...&set=..."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    query = data.get("query", "").strip()
    set_name = data.get("set_name", "").strip() or None
    if not query:
        return jsonify({"error": "query required"}), 400

    try:
        results = ppt.search_cards(query, set_name=set_name, limit=5)
        return jsonify({"results": results})
    except PPTError as e:
        return jsonify({"error": str(e)}), 502


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
    out = {}
    for k, v in obj.items():
        if isinstance(v, Decimal):
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
    """Sync all Shopify products with tcgplayer_id metafield into local cache."""
    if not shopify:
        return jsonify({"error": "Shopify not configured (set SHOPIFY_TOKEN + SHOPIFY_STORE)"}), 503
    try:
        app.logger.info("Starting Shopify sync...")
        products = shopify.get_all_products()
        app.logger.info(f"Shopify returned {len(products)} total variants")
        with_tcg = [p for p in products if p.get("tcgplayer_id")]
        app.logger.info(f"  {len(with_tcg)} have tcgplayer_id")
        if products and not with_tcg:
            # Log a sample to debug metafield issue
            sample = products[:3]
            for s in sample:
                app.logger.info(f"  Sample product: {s.get('title')} tcg_id={s.get('tcgplayer_id')}")
            return jsonify({"synced": 0, "total_variants": len(products), "message": "No products with tcgplayer_id metafield found"})
        upserted = 0
        for p in with_tcg:
            db.execute("""
                INSERT INTO shopify_product_cache
                    (tcgplayer_id, shopify_product_id, shopify_variant_id,
                     title, handle, sku, shopify_price, shopify_qty, status, is_damaged, last_synced)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (tcgplayer_id, shopify_variant_id)
                DO UPDATE SET title = EXCLUDED.title, handle = EXCLUDED.handle, sku = EXCLUDED.sku,
                    shopify_price = EXCLUDED.shopify_price, shopify_qty = EXCLUDED.shopify_qty,
                    status = EXCLUDED.status, is_damaged = EXCLUDED.is_damaged, last_synced = CURRENT_TIMESTAMP
            """, (p["tcgplayer_id"], p["shopify_product_id"], p["variant_id"],
                  p["title"], p["handle"], p.get("sku"), p["shopify_price"], p["shopify_qty"],
                  p.get("status", "ACTIVE"), p.get("is_damaged", False)))
            upserted += 1
        # Backfill sealed_cogs linkage
        db.execute("""
            UPDATE sealed_cogs sc SET shopify_product_id = spc.shopify_product_id
            FROM shopify_product_cache spc
            WHERE sc.tcgplayer_id = spc.tcgplayer_id AND sc.shopify_product_id IS NULL
        """)
        app.logger.info(f"Shopify sync complete: {upserted} upserted")
        return jsonify({"synced": upserted, "total_variants": len(products), "with_tcg_id": len(with_tcg)})
    except (ShopifyError, Exception) as e:
        app.logger.error(f"Shopify sync failed: {e}")
        return jsonify({"error": str(e)}), 502


@app.route("/api/shopify/status")
def shopify_status():
    """Check Shopify integration status and cache stats."""
    configured = shopify is not None
    cache_count = 0
    last_sync = None
    if configured:
        try:
            row = db.query_one("SELECT COUNT(*) as cnt, MAX(last_synced) as last_sync FROM shopify_product_cache")
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

    items = intake.get_session_items(session_id)
    linked = [i for i in items if i.get("tcgplayer_id")]
    tcg_ids = list(set(i["tcgplayer_id"] for i in linked))
    if not tcg_ids:
        return jsonify({"items": [], "cache_hit_rate": 0})
    placeholders = ",".join(["%s"] * len(tcg_ids))

    try:
        # Fetch ALL variants (both damaged and non-damaged) for these tcgplayer_ids
        all_rows = db.query(
            f"SELECT * FROM shopify_product_cache WHERE tcgplayer_id IN ({placeholders})",
            tuple(tcg_ids)
        )
    except Exception:
        return jsonify({"error": "Shopify cache table not found. Run the migration first, then sync."}), 500

    # Build separate maps for normal and damaged variants
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
                "status": r["status"],
                "last_synced": r["last_synced"].isoformat() if r["last_synced"] else None,
                "is_damaged": is_dmg,
            }
        target[tcg]["shopify_qty"] += (r["shopify_qty"] or 0)

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
            "in_store": sd is not None,
            "store_title": sd["title"] if sd else None, "store_price": sd["shopify_price"] if sd else None,
            "store_qty": sd["shopify_qty"] if sd else None, "store_handle": sd["handle"] if sd else None,
            "store_product_id": sd["shopify_product_id"] if sd else None,
            "damaged_variant_exists": damaged_variant_exists if is_damaged_item else None,
            "store_note": store_note,
        })

    hit = sum(1 for i in result_items if i["in_store"])
    return jsonify({"items": result_items, "total": len(result_items), "in_store": hit,
                    "not_in_store": len(result_items) - hit,
                    "cache_hit_rate": round(hit / len(result_items) * 100, 1) if result_items else 0})


# ==========================================
# MAIN
# ==========================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")
