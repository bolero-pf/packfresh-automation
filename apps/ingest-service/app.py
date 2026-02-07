"""
TCG Store - Intake Service
Flask API for collection intake (sealed via Collectr CSV, raw via manual entry).

Endpoints:
    Dashboard:
        GET  /                              - Intake dashboard UI

    Collectr CSV Flow:
        POST /api/intake/upload-collectr    - Upload & parse Collectr CSV
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
from decimal import Decimal, InvalidOperation

from flask import Flask, request, jsonify, render_template, send_file
from flask_cors import CORS

import db
from ppt_client import PPTClient, PPTError
from collectr_parser import parse_collectr_csv
from barcode_gen import generate_barcode_image
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


# ==========================================
# ITEM MAPPING
# ==========================================

@app.route("/api/intake/map-item", methods=["POST"])
def map_item():
    """Map an intake item to a tcgplayer_id."""
    data = request.json or {}
    item_id = data.get("item_id")
    tcgplayer_id = data.get("tcgplayer_id")

    if not item_id or not tcgplayer_id:
        return jsonify({"error": "item_id and tcgplayer_id required"}), 400

    try:
        tcgplayer_id = int(tcgplayer_id)
    except (ValueError, TypeError):
        return jsonify({"error": "tcgplayer_id must be an integer"}), 400

    # Optionally verify price via PPT
    new_price = None
    verify_price = data.get("verify_price", False)
    if verify_price and ppt:
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
        updated = intake.map_item(item_id, tcgplayer_id, new_price)
        return jsonify({
            "success": True,
            "item": _serialize(updated),
            "ppt_price_used": new_price is not None,
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
# FINALIZATION
# ==========================================

@app.route("/api/intake/finalize/<session_id>", methods=["POST"])
def finalize(session_id):
    """Finalize an intake session."""
    result = intake.finalize_session(session_id)
    if result["success"]:
        return jsonify(result)
    return jsonify(result), 400


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
        return jsonify({"product": product_data})
    except PPTError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/ppt/search-sealed", methods=["POST"])
def ppt_search_sealed():
    """Search sealed products by name via PPT /v2/sealed-products?search=..."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "query required"}), 400

    try:
        results = ppt.search_sealed_products(query, limit=10)
        return jsonify({"results": results})
    except PPTError as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/ppt/search-cards", methods=["POST"])
def ppt_search_cards():
    """Search cards by name via PPT /v2/cards?search=..."""
    if not ppt:
        return jsonify({"error": "PPT API not configured (set PPT_API_KEY env var)"}), 503

    data = request.json or {}
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "query required"}), 400

    try:
        results = ppt.search_cards(query, limit=10)
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
# MAIN
# ==========================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG", "0") == "1")
