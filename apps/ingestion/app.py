"""
Ingest Service — ingest.pack-fresh.com
Warehouse team dashboard for breaking down sealed products and pushing inventory to Shopify.

Separate from offers.pack-fresh.com — reads the same DB (intake_sessions, intake_items,
inventory_product_cache) but serves a different audience (warehouse vs buying team).
"""

import os
import json
import logging
import hashlib
import secrets
import threading
import uuid as _uuid
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, Blueprint, render_template, request, jsonify, redirect, make_response

import db
import ingest
from shopify_client import ShopifyClient, ShopifyError
from price_provider import PriceProvider, create_price_provider, PriceError
import product_enrichment as enrichment
from cache_manager import CacheManager
try:
    import psa_client
    from psa_client import PSAQuotaHit, PSANotFound, ShopifyCreateError
except ImportError:
    psa_client = None
    PSAQuotaHit = Exception
    PSANotFound = Exception
    ShopifyCreateError = Exception
try:
    from storage import assign_bins, release_bins, _canonical_card_type, assign_display, get_binder_capacity
except ImportError as e:
    logger.error(f"storage import failed: {e} — raw card push will not work")
    assign_bins = release_bins = _canonical_card_type = assign_display = get_binder_capacity = None
try:
    from barcode_gen import generate_barcode_id, generate_barcode_image
except ImportError as e:
    logger.error(f"barcode_gen import failed: {e} — raw card push will not work")
    generate_barcode_id = generate_barcode_image = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# Serve shared static assets (pf_theme.css, pf_ui.js) at /pf-static/
# In Docker: WORKDIR=/app, shared/ is at /app/shared/ (not ../shared/)
_pf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shared", "static")
if not os.path.isdir(_pf_dir):
    _pf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared", "static")
pf_static = Blueprint(
    "pf_static", __name__,
    static_folder=_pf_dir,
    static_url_path="/pf-static",
)
app.register_blueprint(pf_static)

# ─── Password gate ───────────────────────────────────────────────
INGEST_PASSWORD = os.getenv("INGEST_PASSWORD", "")

INGEST_API_KEY = os.getenv("INGEST_API_KEY", "")  # shared secret for server-to-server calls

def _check_auth():
    """Returns True if auth is disabled, valid API key header, or valid session cookie."""
    if not INGEST_PASSWORD:
        return True
    # Allow server-to-server calls with API key header
    if INGEST_API_KEY and request.headers.get("X-Ingest-Api-Key") == INGEST_API_KEY:
        return True
    token = request.cookies.get("ingest_auth")
    if not token:
        return False
    expected = hashlib.sha256(f"{INGEST_PASSWORD}:{app.secret_key}".encode()).hexdigest()
    return token == expected

def _make_auth_cookie(response):
    """Set the auth cookie on a response."""
    token = hashlib.sha256(f"{INGEST_PASSWORD}:{app.secret_key}".encode()).hexdigest()
    response.set_cookie("ingest_auth", token, max_age=60*60*24*30, httponly=True, samesite="Lax")
    return response

@app.after_request
def _add_admin_bar(response):
    try:
        from auth import inject_admin_bar, get_current_user
        if get_current_user():
            return inject_admin_bar(response)
    except Exception:
        pass
    return response

@app.before_request
def require_auth():
    """Gate all routes behind JWT cookie (admin portal) or legacy password."""
    if request.path in ("/login", "/health"):
        return None
    if request.path.startswith(("/static", "/pf-static")):
        return None
    # Try JWT auth first (from admin portal)
    try:
        from auth import require_auth as jwt_auth
        result = jwt_auth(roles=["manager", "owner"])
        if result is None:
            return None  # JWT valid — authenticated
    except Exception:
        pass
    # Fall through to legacy auth
    if not INGEST_PASSWORD:
        return None
    if request.path.startswith("/api/"):
        # API calls: check cookie but also check if referer is from our domain
        if _check_auth():
            return None
        # If cookie check fails, still allow if there's a valid referer from same origin
        referer = request.headers.get("Referer", "")
        if referer and ("ingest.pack-fresh.com" in referer or "ingest-inventory" in referer):
            return None
        return jsonify({"error": "Not authenticated"}), 401
    if not _check_auth():
        return redirect("/login")


# ─── JSON error handlers ────────────────────────────────────────────
@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def server_error(e):
    logger.exception("Internal server error")
    return jsonify({"error": "Internal server error", "detail": str(e)}), 500

# ─── Init services ──────────────────────────────────────────────────

db.init_pool()

shopify = None
if os.getenv("SHOPIFY_TOKEN") and os.getenv("SHOPIFY_STORE"):
    shopify = ShopifyClient(os.getenv("SHOPIFY_TOKEN"), os.getenv("SHOPIFY_STORE"))
    logger.info("Shopify client initialized")
else:
    logger.warning("SHOPIFY_TOKEN / SHOPIFY_STORE not set — push-live disabled")
cache_mgr = CacheManager(db, shopify, table_prefix="inventory_", cache_all_products=True)

ppt = create_price_provider(db=db)

# Register shared breakdown blueprint (replaces breakdown-cache, PPT search, store-prices routes)
from breakdown_routes import create_breakdown_blueprint
app.register_blueprint(create_breakdown_blueprint(db, ppt_getter=lambda: ppt))


def _serialize(obj):
    """JSON-safe serialization for DB rows."""
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


# ═══════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════

@app.route("/login", methods=["GET", "POST"])
def login():
    if not INGEST_PASSWORD:
        return redirect("/")
    if _check_auth():
        return redirect("/")

    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == INGEST_PASSWORD:
            resp = make_response(redirect("/"))
            return _make_auth_cookie(resp)
        error = "Wrong password"

    return f'''<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ingest — Login</title>
<style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{ background:#0f1117; color:#e2e8f0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
           display:flex; align-items:center; justify-content:center; min-height:100vh; }}
    .login-card {{ background:#1a1d2e; border:1px solid #2d3148; border-radius:12px; padding:40px;
                   width:100%; max-width:380px; text-align:center; }}
    h1 {{ font-size:1.4rem; margin-bottom:8px; }}
    p {{ color:#8892b0; font-size:0.9rem; margin-bottom:24px; }}
    input {{ width:100%; padding:12px 16px; background:#0f1117; border:1px solid #2d3148; border-radius:8px;
            color:#e2e8f0; font-size:1rem; margin-bottom:16px; outline:none; }}
    input:focus {{ border-color:#4f7df9; }}
    button {{ width:100%; padding:12px; background:#4f7df9; color:#fff; border:none; border-radius:8px;
             font-size:1rem; cursor:pointer; font-weight:600; }}
    button:hover {{ background:#3d6ae0; }}
    .error {{ color:#ff6b6b; font-size:0.85rem; margin-bottom:12px; }}
</style>
</head><body>
<div class="login-card">
    <h1>📦 Pack Fresh Ingest</h1>
    <p>Enter password to continue</p>
    {"<div class='error'>" + error + "</div>" if error else ""}
    <form method="POST">
        <input type="password" name="password" placeholder="Password" autofocus>
        <button type="submit">Log In</button>
    </form>
</div>
</body></html>'''


# ═══════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("ingest_dashboard.html")


# ═══════════════════════════════════════════════════════════════════
# SESSION LIST
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/sessions")
def list_sessions():
    status = request.args.get("status", "pending")  # 'pending' or 'completed'
    limit = int(request.args.get("limit", 50))
    days = request.args.get("days")  # for completed: filter by recency
    search = request.args.get("search", "").strip()
    if status == "completed":
        sessions = ingest.list_sessions_completed(limit=limit, days=int(days) if days else None, search=search or None)
    else:
        sessions = ingest.list_sessions_pending(limit=limit)
    return jsonify([_serialize(s) for s in sessions])


@app.route("/api/ingest/session/<session_id>")
def get_session(session_id):
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    # Include missing items so Verify tab can display them
    items = ingest.get_session_items(session_id, include_missing=True)

    # Enrich items with store prices from inventory_product_cache
    tcg_ids = list(set(int(i["tcgplayer_id"]) for i in items if i.get("tcgplayer_id")))
    store_map = {}
    if tcg_ids:
        try:
            ph = ",".join(["%s"] * len(tcg_ids))
            store_rows = db.query(
                f"SELECT tcgplayer_id, shopify_price, shopify_qty FROM inventory_product_cache WHERE tcgplayer_id IN ({ph}) AND is_damaged = FALSE",
                tuple(tcg_ids))
            store_map = {r["tcgplayer_id"]: r for r in store_rows}
        except Exception:
            pass

    # Enrich with velocity data (prefer non-damaged variant with most sales)
    velocity_map = {}
    if tcg_ids:
        try:
            vph = ",".join(["%s"] * len(tcg_ids))
            vel_rows = db.query(f"""
                SELECT a.tcgplayer_id, a.units_sold_90d, a.units_sold_30d, a.units_sold_7d,
                       a.total_sold_all_time, a.first_seen_date,
                       a.velocity_score, a.current_qty, a.avg_days_to_sell, a.out_of_stock_days
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
        d = _serialize(i)
        sp = store_map.get(i.get("tcgplayer_id"))
        d["store_price"] = float(sp["shopify_price"]) if sp and sp.get("shopify_price") else None
        d["store_qty"] = int(sp["shopify_qty"] or 0) if sp else None
        vel = velocity_map.get(i.get("tcgplayer_id"))
        d["velocity"] = _serialize(vel) if vel else None
        serialized.append(d)

    return jsonify({
        "session": _serialize(session),
        "items": serialized,
    })


# ═══════════════════════════════════════════════════════════════════
# BREAK DOWN
# ═══════════════════════════════════════════════════════════════════

# (break-down route moved to BREAKDOWN CACHE section below)


@app.route("/api/ingest/item/<item_id>/undo-breakdown", methods=["POST"])
def undo_breakdown(item_id):
    """Undo a break-down: delete children, restore parent."""
    try:
        result = ingest.undo_break_down(item_id)
        return jsonify({
            "success": True,
            "item": _serialize(result["item"]),
            "session": _serialize(result["session"]),
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"Undo breakdown failed for item {item_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ingest/item/<item_id>/damage", methods=["POST"])
def damage_item(item_id):
    """Mark item (or partial qty) as damaged."""
    data = request.get_json(silent=True) or {}
    damaged_qty = data.get("damaged_qty")  # None = damage all

    if damaged_qty is not None:
        # Partial damage — split the item
        try:
            result = ingest.split_damaged(item_id, int(damaged_qty))
            return jsonify({"success": True, "result": _serialize(result)})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
    else:
        item = ingest.mark_item_damaged(item_id)
        return jsonify({"success": True, "item": _serialize(item)})


@app.route("/api/ingest/item/<item_id>/mark-good", methods=["POST"])
def mark_good(item_id):
    item = ingest.mark_item_good(item_id)
    return jsonify({"success": True, "item": _serialize(item)})


@app.route("/api/ingest/search-cards", methods=["POST"])
def search_cards_for_relink():
    """
    Cache-first card search for the re-link flow (preview panel uses this when
    PSA and Scrydex set names disagree). Reads scrydex_price_cache directly so
    zero API credits are consumed.

    POST body: { "query": "alakazam", "set_name": "base set 2", "limit": 20 }
    """
    data    = request.get_json(silent=True) or {}
    query   = (data.get("query") or "").strip()
    set_nm  = (data.get("set_name") or "").strip()
    limit   = min(int(data.get("limit") or 20), 50)

    if not query and not set_nm:
        return jsonify({"error": "query or set_name required"}), 400

    where = ["product_type = 'card'"]
    params = []
    if query:
        where.append("product_name ILIKE %s")
        params.append(f"%{query}%")
    if set_nm:
        where.append("expansion_name ILIKE %s")
        params.append(f"%{set_nm}%")

    # One row per scrydex_id — NM raw variant first (most representative)
    sql = f"""
        SELECT DISTINCT ON (scrydex_id)
               scrydex_id, tcgplayer_id, product_name, expansion_name,
               card_number, rarity, variant, image_small, image_medium, market_price
        FROM scrydex_price_cache
        WHERE {' AND '.join(where)}
        ORDER BY scrydex_id, price_type ASC, condition ASC
        LIMIT %s
    """
    params.append(limit)
    rows = db.query(sql, tuple(params))

    results = [{
        "scrydex_id":    r.get("scrydex_id"),
        "tcgplayer_id":  r.get("tcgplayer_id"),
        "product_name":  r.get("product_name"),
        "set_name":      r.get("expansion_name"),
        "card_number":   r.get("card_number"),
        "rarity":        r.get("rarity"),
        "variant":       r.get("variant"),
        "image":         r.get("image_small") or r.get("image_medium"),
        "market_price":  float(r["market_price"]) if r.get("market_price") else None,
    } for r in rows if r.get("tcgplayer_id")]  # must have a TCG ID to be usable

    return jsonify({"results": results, "total": len(results)})


@app.route("/api/ingest/item/<item_id>/relink", methods=["POST"])
def relink_item(item_id):
    """Relink an item to a different PPT product (change name, tcgplayer_id, market price)."""
    data = request.get_json(silent=True) or {}
    try:
        result = ingest.relink_item(item_id, data)
        return jsonify({"success": True, "item": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"Relink failed for item {item_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ingest/item/<item_id>/update-qty", methods=["POST"])
def update_item_qty(item_id):
    """Update item quantity."""
    data = request.get_json(silent=True) or {}
    new_qty = data.get("quantity")
    if not new_qty or int(new_qty) < 1:
        return jsonify({"error": "Invalid quantity"}), 400
    try:
        result = ingest.update_item_quantity(item_id, int(new_qty))
        return jsonify({"success": True, "item": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ingest/item/<item_id>/delete", methods=["POST"])
def delete_item(item_id):
    """Remove an item from the session."""
    try:
        result = ingest.delete_item(item_id)
        return jsonify({"success": True, "result": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════
# OFFER ADJUSTMENT SUMMARY
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/session/<session_id>/offer-summary", methods=["GET"])
def offer_summary(session_id):
    """Get offer adjustment summary comparing current state to receive-time snapshot."""
    result = ingest.get_offer_adjustment_summary(session_id)
    if result is None:
        return jsonify({"available": False})
    return jsonify({"available": True, **result})


# ═══════════════════════════════════════════════════════════════════
# ADD ITEM TO SESSION
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/session/<session_id>/add-item", methods=["POST"])
def add_item(session_id):
    """Add a new item to an ingest session."""
    data = request.get_json(silent=True) or {}
    try:
        item = ingest.add_item_to_session(session_id, data)
        return jsonify({"success": True, "item": _serialize(item)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"Add item failed for session {session_id}")
        return jsonify({"error": str(e)}), 500


# PPT search + breakdown-cache routes now served by shared breakdown blueprint


# ═══════════════════════════════════════════════════════════════════
# VERIFY STAGE
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/item/<item_id>/verify", methods=["POST"])
def verify_item(item_id):
    """Verify an item during the verify stage: here, missing, or damaged."""
    data = request.get_json(silent=True) or {}
    status = data.get("status")  # "here", "missing", "damaged"
    if status not in ("here", "missing", "damaged"):
        return jsonify({"error": "status must be 'here', 'missing', or 'damaged'"}), 400

    try:
        if status == "here":
            qty_confirmed = data.get("qty_confirmed")
            if qty_confirmed is not None:
                qty_confirmed = int(qty_confirmed)
            result = ingest.verify_item_here(item_id, qty_confirmed=qty_confirmed)

            # Update condition (raw cards — recalculates offer) or grade (graded slabs)
            condition = data.get("condition")
            grade_company = data.get("grade_company")
            grade_value = data.get("grade_value")
            price_override = data.get("price_override")  # manual price override

            if condition:
                result = ingest.update_item_condition(
                    item_id, condition, ppt_client=ppt,
                    price_override=float(price_override) if price_override is not None else None)
            elif grade_company or grade_value:
                result = ingest.update_item_grade(
                    item_id,
                    grade_company=grade_company,
                    grade_value=grade_value,
                    ppt_client=ppt,
                    price_override=float(price_override) if price_override is not None else None,
                    db_module=db)
            elif price_override is not None:
                # Pure price override without condition/grade change
                result = ingest.override_item_price(item_id, float(price_override))
            return jsonify({"success": True, "result": _serialize(result)})

        elif status == "missing":
            missing_qty = data.get("missing_qty")
            if missing_qty is not None:
                missing_qty = int(missing_qty)
            result = ingest.verify_item_missing(item_id, missing_qty=missing_qty)
            return jsonify({"success": True, "result": _serialize(result)})

        else:  # damaged
            damaged_qty = data.get("damaged_qty")
            if damaged_qty is not None:
                result = ingest.split_damaged(item_id, int(damaged_qty))
                # Also stamp verified_at on both parts
                if isinstance(result, dict) and "good_item" in result:
                    db.execute("UPDATE intake_items SET verified_at = CURRENT_TIMESTAMP WHERE id IN (%s, %s)",
                               (result["good_item"]["id"], result["damaged_item"]["id"]))
                else:
                    db.execute("UPDATE intake_items SET verified_at = CURRENT_TIMESTAMP WHERE id = %s", (item_id,))
            else:
                ingest.mark_item_damaged(item_id)
                db.execute("UPDATE intake_items SET verified_at = CURRENT_TIMESTAMP WHERE id = %s", (item_id,))
                result = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
            return jsonify({"success": True, "result": _serialize(result)})

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"Verify failed for item {item_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ingest/item/<item_id>/convert-type", methods=["POST"])
def convert_item_type_route(item_id):
    """
    Flip a raw card ↔ graded slab during verify.

    POST body:
        { "to_graded": true, "grade_company": "PSA", "grade_value": "10", "price_override": null }
        { "to_graded": false, "condition": "NM", "price_override": null }
    """
    data = request.get_json(silent=True) or {}
    to_graded = bool(data.get("to_graded"))
    price_override = data.get("price_override")
    try:
        result = ingest.convert_item_type(
            item_id,
            to_graded=to_graded,
            condition=data.get("condition"),
            grade_company=data.get("grade_company"),
            grade_value=data.get("grade_value"),
            ppt_client=ppt,
            price_override=float(price_override) if price_override is not None else None,
            db_module=db,
        )
        return jsonify({"success": True, "item": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"convert_item_type failed for item {item_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ingest/item/<item_id>/undo-verify", methods=["POST"])
def undo_verify(item_id):
    """Reset an item back to unverified good status."""
    try:
        result = ingest.undo_verify(item_id)
        return jsonify({"success": True, "item": _serialize(result)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"Undo verify failed for item {item_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ingest/session/<session_id>/complete-verify", methods=["POST"])
def complete_verify(session_id):
    """Complete the verification stage — transitions received → verified."""
    try:
        session = ingest.complete_verification(session_id)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/ingest/session/<session_id>/complete-breakdown", methods=["POST"])
def complete_breakdown(session_id):
    """Complete the breakdown stage — transitions verified → breakdown_complete."""
    try:
        session = ingest.complete_breakdown(session_id)
        return jsonify({"success": True, "session": _serialize(session)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


# ═══════════════════════════════════════════════════════════════════
# PUSH LIVE TO SHOPIFY
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/session/<session_id>/push-dry-run", methods=["POST"])
def push_dry_run(session_id):
    """Dry run — shows exactly what push-live would do without calling Shopify."""
    if cache_mgr:
        cache_mgr.check_and_refresh_if_stale()
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    data = request.get_json(silent=True) or {}
    requested_item_ids = set(str(x) for x in (data.get("item_ids") or []))

    items = ingest.get_session_items(session_id)
    active = [i for i in items if i.get("item_status") in ("good", "damaged")
              and i.get("is_mapped") and not i.get("pushed_at")]

    if requested_item_ids:
        active = [i for i in active if str(i["id"]) in requested_item_ids]

    if not active:
        return jsonify({"error": "No active mapped items to preview"}), 400

    # Split by product_type
    raw_items    = [i for i in active if i.get("product_type") == "raw" and not i.get("is_graded")]
    graded_items = [i for i in active if i.get("is_graded")]
    sealed_items = [i for i in active if i.get("product_type") != "raw" and not i.get("is_graded")]

    results = []

    # Raw cards — show what would be inserted into raw_cards table
    for item in raw_items:
        dest = item.get("routing_destination") or "storage"
        dest_notes = {
            "storage": "Barcode + bin assignment",
            "display": "Barcode + binder assignment",
            "grade":   "Marked for grading — no barcode",
            "bulk":    "Bulk — no tracking",
        }
        results.append({
            "product_name": item.get("product_name"),
            "quantity": item.get("quantity", 1),
            "action": "would_ingest_raw",
            "new_title": item.get("product_name"),
            "listing_price": float(item.get("market_price", 0)),
            "note": f"→ {dest.upper()}: {dest_notes.get(dest, dest)}",
            "routing_destination": dest,
        })

    # Graded slabs — show cert entry required
    for item in graded_items:
        results.append({
            "product_name": item.get("product_name"),
            "quantity": item.get("quantity", 1),
            "action": "would_push_graded",
            "new_title": item.get("product_name"),
            "listing_price": float(item.get("market_price", 0)),
            "note": f"{item.get('grade_company','PSA')} {item.get('grade_value','?')} — cert number required at push",
        })

    tcg_ids = list(set(i["tcgplayer_id"] for i in sealed_items if i.get("tcgplayer_id")))
    normal_cache, damaged_cache = ingest.build_cache_maps(tcg_ids)

    # Consolidate by (tcg_id, is_damaged)
    consolidated = {}
    for item in sealed_items:
        tcg_id = item["tcgplayer_id"]
        is_damaged = item.get("item_status") == "damaged"
        key = (tcg_id, is_damaged)
        if key not in consolidated:
            consolidated[key] = {"tcg_id": tcg_id, "is_damaged": is_damaged, "total_qty": 0, "items": [], "product_name": item.get("product_name")}
        consolidated[key]["total_qty"] += item.get("quantity", 1)
        consolidated[key]["items"].append(item)

    for key, group in consolidated.items():
        tcg_id, is_damaged = key
        qty = group["total_qty"]
        entry = {
            "product_name": group["product_name"],
            "tcgplayer_id": tcg_id,
            "quantity": qty,
            "is_damaged": is_damaged,
            "consolidated_from": len(group["items"]),
        }

        if not is_damaged:
            cache_row = normal_cache.get(tcg_id)
            if cache_row and cache_row.get("shopify_variant_id"):
                entry["action"] = "would_increment"
                entry["shopify_variant_id"] = cache_row["shopify_variant_id"]
                entry["shopify_title"] = cache_row.get("title")
                entry["current_qty"] = cache_row.get("shopify_qty", 0)
                entry["new_qty"] = cache_row.get("shopify_qty", 0) + qty
            else:
                entry["action"] = "would_create_listing"
                entry["new_title"] = group["product_name"] or "Unknown"
                entry["listing_price"] = float(group["items"][0].get("market_price", 0))
        else:
            cache_row = damaged_cache.get(tcg_id)
            if cache_row and cache_row.get("shopify_variant_id"):
                entry["action"] = "would_increment"
                entry["shopify_variant_id"] = cache_row["shopify_variant_id"]
                entry["shopify_title"] = cache_row.get("title")
                entry["current_qty"] = cache_row.get("shopify_qty", 0)
                entry["new_qty"] = cache_row.get("shopify_qty", 0) + qty
            else:
                normal_row = normal_cache.get(tcg_id)
                if normal_row and normal_row.get("shopify_product_id"):
                    entry["action"] = "would_create_damaged"
                    entry["source_title"] = normal_row.get("title")
                    entry["damaged_title"] = f"{normal_row.get('title', '')} [DAMAGED]"
                    entry["store_price"] = float(normal_row.get("shopify_price", 0))
                    entry["note"] = "Price stays the same — 'damaged' tag triggers automatic discount on site"
                else:
                    entry["action"] = "would_create_listing"
                    damaged_title = f"{group['product_name'] or 'Unknown'} [DAMAGED]"
                    entry["new_title"] = damaged_title
                    entry["listing_price"] = float(group["items"][0].get("market_price", 0))

        results.append(entry)

    return jsonify({
        "dry_run": True,
        "results": [_serialize(r) for r in results],
        "total": len(active),
        "would_increment":      sum(1 for r in results if r.get("action") == "would_increment"),
        "would_create_damaged": sum(1 for r in results if r.get("action") == "would_create_damaged"),
        "would_create_listing": sum(1 for r in results if r.get("action") == "would_create_listing"),
        "would_ingest_raw":     sum(1 for r in results if r.get("action") == "would_ingest_raw"),
        "would_push_graded":    sum(1 for r in results if r.get("action") == "would_push_graded"),
    })



_push_jobs = {}  # {job_id: {status, progress, total, results, errors, ...}}

# ── Route enrichment (PPT graded prices + images for routing session) ──
_enrich_jobs = {}    # {job_id: {status, progress, total, session_id, errors}}
_enrich_cache = {}   # {session_id: {tcg_id_str: {image_url, graded_prices, grading_economics}}}

GRADING_COST = 30.0  # dollars per card to get graded


def _calc_grading_economics(graded_prices: dict, raw_price: float, condition: str) -> dict:
    """Calculate grade-worthiness metrics for a raw card."""
    result = {
        "eligible": False,
        "grade_worthy": False,
        "reason": None,
        "psa10_price": None,
        "psa9_price": None,
        "psa10_confidence": None,
        "psa9_confidence": None,
        "ev": None,
        "grading_cost": GRADING_COST,
        "total_cost": None,
        "expected_profit": None,
        "roi_pct": None,
    }

    # Suppress if condition is LP or worse (but still return data so frontend
    # can show the "LP vintage" hint)
    if condition and condition not in ("NM",):
        result["reason"] = f"Condition {condition} — skip for grading"
        # Still populate prices if available so the UI can show them dimmed
        psa = graded_prices.get("PSA", {})
        p10 = psa.get("10", {}).get("price")
        p9 = psa.get("9", {}).get("price")
        if p10:
            result["psa10_price"] = round(p10, 2)
            result["psa10_confidence"] = psa.get("10", {}).get("confidence")
        if p9:
            result["psa9_price"] = round(p9, 2)
            result["psa9_confidence"] = psa.get("9", {}).get("confidence")
        return result

    psa = graded_prices.get("PSA", {})
    psa10_data = psa.get("10", {})
    psa9_data = psa.get("9", {})
    psa10 = psa10_data.get("price")
    psa9 = psa9_data.get("price")

    if not psa10 or not psa9:
        result["reason"] = "Insufficient graded price data"
        if psa10:
            result["psa10_price"] = round(psa10, 2)
            result["psa10_confidence"] = psa10_data.get("confidence")
        if psa9:
            result["psa9_price"] = round(psa9, 2)
            result["psa9_confidence"] = psa9_data.get("confidence")
        return result

    result["eligible"] = True
    result["psa10_price"] = round(psa10, 2)
    result["psa9_price"] = round(psa9, 2)
    result["psa10_confidence"] = psa10_data.get("confidence")
    result["psa9_confidence"] = psa9_data.get("confidence")

    ev = (0.60 * psa10) + (0.40 * psa9)
    total_cost = raw_price + GRADING_COST
    expected_profit = ev - total_cost
    roi_pct = ((ev / total_cost) - 1) * 100 if total_cost > 0 else 0

    result["ev"] = round(ev, 2)
    result["total_cost"] = round(total_cost, 2)
    result["expected_profit"] = round(expected_profit, 2)
    result["roi_pct"] = round(roi_pct, 1)

    # Grade-worthy when BOTH thresholds met
    result["grade_worthy"] = expected_profit >= 40 and (ev / total_cost) >= 1.5

    return result


def _enrich_route_worker(job_id, session_id, items):
    """Background worker: fetch PPT data for each unique tcgplayer_id."""
    import time as _time
    job = _enrich_jobs[job_id]
    cache = {}

    # Deduplicate by tcgplayer_id — keep first item per tcg_id for condition/price
    unique = {}
    for item in items:
        tcg_id = item.get("tcgplayer_id")
        if tcg_id and str(tcg_id) not in unique:
            unique[str(tcg_id)] = item

    job["total"] = len(unique)
    errors = 0

    from graded_pricing import get_all_graded_comps

    for i, (tcg_id_str, item) in enumerate(unique.items()):
        try:
            card_data = ppt.get_card_by_tcgplayer_id(int(tcg_id_str))
            if card_data:
                image_url = (card_data.get("imageCdnUrl800")
                             or card_data.get("imageCdnUrl")
                             or card_data.get("imageCdnUrl400"))

                # Live eBay comps for graded prices — one API call per card
                # returns all grades. Falls back to cache aggregate if API unavailable.
                graded_prices = get_all_graded_comps(
                    int(tcg_id_str), db,
                    card_name=item.get("product_name"),
                    set_name=item.get("set_name"),
                    card_number=item.get("card_number"),
                )
                if not graded_prices:
                    graded_prices = PriceProvider.extract_graded_prices(card_data)

                raw_price = float(item.get("market_price") or 0)
                condition = item.get("condition") or "NM"
                economics = _calc_grading_economics(graded_prices, raw_price, condition)

                cache[tcg_id_str] = {
                    "image_url": image_url,
                    "graded_prices": graded_prices,
                    "grading_economics": economics,
                }
            else:
                cache[tcg_id_str] = {"error": "not_found"}
                errors += 1
        except Exception as e:
            logger.warning(f"PPT enrich failed for TCG#{tcg_id_str}: {e}")
            cache[tcg_id_str] = {"error": str(e)}
            errors += 1

        job["progress"] = i + 1
        job["errors"] = errors

    _enrich_cache[session_id] = cache
    job["status"] = "complete"


@app.route("/api/ingest/push-job/<job_id>", methods=["GET"])
def get_push_job(job_id):
    """Poll background push job status."""
    job = _push_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/ingest/session/<session_id>/push-live", methods=["POST"])
def push_session_live(session_id):
    """Push a received session to Shopify (runs in background thread)."""
    if cache_mgr:
        cache_mgr.check_and_refresh_if_stale()
    if not shopify:
        return jsonify({"error": "Shopify not configured"}), 503

    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] not in ("received", "verified", "breakdown_complete", "partially_ingested"):
        return jsonify({"error": f"Session cannot be pushed (currently: {session['status']})"}), 400

    data = request.get_json(silent=True) or {}
    requested_item_ids = set(str(x) for x in (data.get("item_ids") or []))

    items = ingest.get_session_items(session_id)
    active = [i for i in items if i.get("item_status") in ("good", "damaged")
              and i.get("is_mapped") and not i.get("pushed_at")]

    if requested_item_ids:
        active = [i for i in active if str(i["id"]) in requested_item_ids]

    if not active:
        already_pushed = [i for i in items if i.get("pushed_at")]
        if already_pushed:
            ingest.mark_session_ingested(session_id)
            return jsonify({"success": True, "results": [], "errors": [],
                            "total": 0, "ingested": True,
                            "message": "All items already pushed. Session marked ingested."})
        return jsonify({"error": "No active mapped items to push"}), 400

    # Serialize items for the background thread (avoid psycopg2 cursor issues)
    active_dicts = [dict(i) for i in active]

    job_id = str(_uuid.uuid4())
    _push_jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "total": len(active_dicts),
        "results": [],
        "errors": [],
        "session_id": session_id,
    }

    thread = threading.Thread(
        target=_push_session_worker,
        args=(job_id, session_id, active_dicts),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id, "status": "running", "total": len(active_dicts)})


def _push_session_worker(job_id, session_id, active):
    """Background worker that processes the actual push to Shopify."""
    job = _push_jobs[job_id]
    results = []
    errors = []

    try:
        # Split by product_type
        raw_items    = [i for i in active if i.get("product_type") == "raw" and not i.get("is_graded")]
        graded_items = [i for i in active if i.get("is_graded")]
        sealed_items = [i for i in active if i.get("product_type") != "raw" and not i.get("is_graded")]

        # Graded slabs
        for item in graded_items:
            results.append({
                "product_name": item.get("product_name"),
                "quantity":     item.get("quantity", 1),
                "action":       "graded_pending_cert",
                "note":         f"{item.get('grade_company','PSA')} {item.get('grade_value','?')} — use cert entry panel below",
            })
            job["progress"] += 1

        # Raw cards — route by destination
        for item in raw_items:
            item_dict = dict(item)
            item_dict["session_id"] = session_id
            dest = item.get("routing_destination") or "storage"
            try:
                if dest == "bulk":
                    r = _push_raw_to_bulk(item_dict)
                elif dest == "display":
                    r = _push_raw_to_display(item_dict)
                elif dest == "grade":
                    r = _push_raw_to_grade(item_dict)
                else:
                    r = _push_raw_item(item_dict)
                results.append(r)
                db.execute("UPDATE intake_items SET pushed_at = CURRENT_TIMESTAMP WHERE id = %s",
                           (item["id"],))
            except Exception as e:
                logger.exception(f"push_raw_item failed for {item['id']}: {e}")
                errors.append({"product_name": item.get("product_name"), "action": "error", "error": str(e)})
            job["progress"] += 1

        # Sealed items: consolidate
        tcg_ids = list(set(i["tcgplayer_id"] for i in sealed_items if i.get("tcgplayer_id")))
        normal_cache, damaged_cache = ingest.build_cache_maps(tcg_ids) if tcg_ids else ({}, {})

        consolidated = {}
        for item in sealed_items:
            tcg_id = item["tcgplayer_id"]
            is_damaged = item.get("item_status") == "damaged"
            key = (tcg_id, is_damaged)
            if key not in consolidated:
                consolidated[key] = {
                    "tcg_id": tcg_id,
                    "is_damaged": is_damaged,
                    "total_qty": 0,
                    "items": [],
                    "product_name": item.get("product_name"),
                }
            consolidated[key]["total_qty"] += item.get("quantity", 1)
            consolidated[key]["items"].append(item)

        # Process normal items before damaged — ensures normal listing exists
        # for damaged to duplicate (even if it has to be created fresh)
        sorted_keys = sorted(consolidated.keys(), key=lambda k: k[1])  # is_damaged=False first
        for key in sorted_keys:
            group = consolidated[key]
            tcg_id, is_damaged = key
            qty = group["total_qty"]
            entry = {
                "product_name": group["product_name"],
                "tcgplayer_id": tcg_id,
                "quantity": qty,
                "is_damaged": is_damaged,
                "consolidated_from": len(group["items"]),
            }

            try:
                if not is_damaged:
                    entry = _push_normal_item(entry, tcg_id, qty, group["items"][0], normal_cache)
                else:
                    entry = _push_damaged_item(entry, tcg_id, qty, group["items"][0], normal_cache, damaged_cache)
            except Exception as e:
                entry.update(action="error", error=str(e))
                errors.append(entry)
                job["progress"] += len(group["items"])
                continue

            if entry.get("action") == "error":
                errors.append(entry)
            else:
                results.append(entry)
                for pushed_item in group["items"]:
                    db.execute("UPDATE intake_items SET pushed_at = CURRENT_TIMESTAMP WHERE id = %s",
                               (pushed_item["id"],))

            job["progress"] += len(group["items"])

        # Suppress cache refresh from our own Shopify writes
        if cache_mgr:
            cache_mgr.record_tool_push()

        # Determine final session status
        # Transition even when there are errors — if some items pushed, reflect that
        all_items_after = ingest.get_session_items(session_id)
        remaining_unpushed = [i for i in all_items_after
                              if i.get("item_status") in ("good", "damaged")
                              and i.get("is_mapped") and not i.get("pushed_at")]
        any_pushed = any(i.get("pushed_at") for i in all_items_after
                         if i.get("item_status") in ("good", "damaged"))

        partially_ingested = False
        if not remaining_unpushed and not errors:
            ingest.mark_session_ingested(session_id)
        elif any_pushed:
            # Some items pushed (even with errors) — mark partial so it doesn't look stuck
            db.execute(
                "UPDATE intake_sessions SET status = 'partially_ingested' WHERE id = %s AND status != 'ingested'",
                (session_id,)
            )
            partially_ingested = True

        # Notify intake cache
        if not errors:
            try:
                import requests as _req
                intake_url = os.getenv("INTAKE_INTERNAL_URL", "")
                if intake_url:
                    _req.post(f"{intake_url}/api/cache/invalidate",
                              json={"reason": "ingest"},
                              timeout=3)
            except Exception:
                pass

        job.update({
            "status": "complete",
            "success": len(errors) == 0,
            "results": results,
            "errors": errors,
            "total": len(active),
            "incremented": sum(1 for r in results if r.get("action") == "inventory_incremented"),
            "created_damaged": sum(1 for r in results if r.get("action") == "created_damaged_listing"),
            "created_listing": sum(1 for r in results if r.get("action") == "created_listing"),
            "error_count": len(errors),
            "ingested": not errors and not partially_ingested,
            "partially_ingested": not errors and partially_ingested,
            "pushed_count": len(results),
            "remaining_count": len(remaining_unpushed),
            "can_retry": len(errors) > 0,
        })

    except Exception as e:
        logger.exception(f"Push worker crashed for session {session_id}: {e}")
        job.update({
            "status": "complete",
            "success": False,
            "results": results,
            "errors": errors + [{"action": "error", "error": f"Worker crashed: {str(e)}"}],
            "error_count": len(errors) + 1,
            "total": len(active),
            "pushed_count": len(results),
            "can_retry": True,
        })


def _compute_weighted_cost(current_cost, current_qty, our_unit_cost, adding_qty):
    """Weighted average COGS. If no current cost set, just use ours."""
    if not current_cost or current_qty <= 0:
        return our_unit_cost
    return (current_cost * current_qty + our_unit_cost * adding_qty) / (current_qty + adding_qty)


def _push_raw_item(item: dict) -> dict:
    """
    Push a raw (ungraded) card to internal inventory:
      1. Generate barcode
      2. Fetch PPT card data for image URL + clean name
      3. Assign bin location
      4. Insert into raw_cards table
      5. Return barcode PNG bytes (base64) for immediate printing

    Returns entry dict with action, barcode, bin assignments.
    """
    if not generate_barcode_id or not assign_bins:
        raise RuntimeError("barcode_gen or storage module not available")

    tcg_id    = item.get("tcgplayer_id")
    card_name = item.get("product_name", "Unknown")
    set_name  = item.get("set_name", "")
    condition = item.get("condition") or "NM"
    qty       = item.get("quantity", 1)
    cost      = float(item.get("offer_price", 0)) / max(qty, 1)
    card_type = "pokemon"  # default; could be inferred from tags later

    # Fetch PPT data for image URL + clean name + real card number
    image_url = None
    ppt_card_number = None
    if tcg_id and ppt:
        try:
            card_data = ppt.get_card_by_tcgplayer_id(int(tcg_id))
            if card_data:
                image_url = (card_data.get("imageCdnUrl800")
                             or card_data.get("imageCdnUrl")
                             or card_data.get("imageCdnUrl400"))
                card_name = card_data.get("name") or card_name
                set_name  = card_data.get("setName") or set_name
                # Real card number e.g. "004/125" — not the Collectr set code
                ppt_card_number = card_data.get("cardNumber") or card_data.get("number")
        except Exception as e:
            logger.warning(f"PPT fetch for raw card TCG#{tcg_id} failed: {e}")

    # Assign bin(s) — one card at a time for placement accuracy
    assignments = assign_bins(card_type, qty, db)

    results = []
    for assignment in assignments:
        bin_id    = assignment["bin_id"]
        bin_label = assignment["bin_label"]
        count     = assignment["count"]

        for _ in range(count):
            barcode_id = generate_barcode_id()

            db.execute("""
                INSERT INTO raw_cards (
                    barcode, tcgplayer_id, card_name, set_name,
                    card_number, condition, rarity,
                    state, cost_basis, current_price, last_price_update,
                    bin_id, image_url,
                    is_graded, grade_company, grade_value,
                    variant, language,
                    intake_session_id, stored_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    'STORED', %s, %s, CURRENT_TIMESTAMP,
                    %s, %s,
                    FALSE, NULL, NULL,
                    %s, 'EN',
                    %s, CURRENT_TIMESTAMP
                )
            """, (
                barcode_id, tcg_id, card_name, set_name,
                ppt_card_number or item.get("card_number"), condition, item.get("rarity"),
                cost, float(item.get("market_price", cost)),
                bin_id, image_url,
                item.get("variant"),
                item.get("session_id"),
            ))

            # Generate barcode PNG
            price_str = f"${float(item.get('market_price', 0)):.2f}"
            png_bytes = generate_barcode_image(
                barcode_id,
                card_name=card_name,
                set_name=set_name,
                condition=condition,
                card_number=ppt_card_number or item.get("card_number") or "",
            )

            results.append({
                "barcode":    barcode_id,
                "bin_label":  bin_label,
                "card_name":  card_name,
                "set_name":   set_name,
                "condition":  condition,
                "png_b64":    __import__("base64").b64encode(png_bytes).decode(),
            })

    return {
        "action":      "raw_card_ingested",
        "destination":  "storage",
        "product_name": card_name,
        "quantity":    qty,
        "barcodes":    results,
        "bins":        [a["bin_label"] for a in assignments],
    }


def _fetch_ppt_data(tcg_id, card_name, set_name):
    """Shared PPT lookup for raw card push functions."""
    image_url = None
    ppt_card_number = None
    if tcg_id and ppt:
        try:
            card_data = ppt.get_card_by_tcgplayer_id(int(tcg_id))
            if card_data:
                image_url = (card_data.get("imageCdnUrl800")
                             or card_data.get("imageCdnUrl")
                             or card_data.get("imageCdnUrl400"))
                card_name = card_data.get("name") or card_name
                set_name  = card_data.get("setName") or set_name
                ppt_card_number = card_data.get("cardNumber") or card_data.get("number")
        except Exception as e:
            logger.warning(f"PPT fetch for raw card TCG#{tcg_id} failed: {e}")
    return card_name, set_name, image_url, ppt_card_number


def _push_raw_to_display(item: dict) -> dict:
    """Push raw card to a binder display location. Barcode + label generated."""
    if not generate_barcode_id or not assign_display:
        raise RuntimeError("barcode_gen or storage module not available")

    tcg_id    = item.get("tcgplayer_id")
    card_name = item.get("product_name", "Unknown")
    set_name  = item.get("set_name", "")
    condition = item.get("condition") or "NM"
    qty       = item.get("quantity", 1)
    cost      = float(item.get("offer_price", 0)) / max(qty, 1)

    card_name, set_name, image_url, ppt_card_number = _fetch_ppt_data(tcg_id, card_name, set_name)

    assignments = assign_display(qty, db)
    if not assignments:
        # No binder capacity — fall back to storage
        logger.info(f"No binder capacity for {card_name} — falling back to storage")
        return _push_raw_item(item)

    assigned_qty = sum(a["count"] for a in assignments)

    results = []
    for assignment in assignments:
        bin_id    = assignment["bin_id"]
        bin_label = assignment["bin_label"]
        count     = assignment["count"]

        for _ in range(count):
            barcode_id = generate_barcode_id()
            db.execute("""
                INSERT INTO raw_cards (
                    barcode, tcgplayer_id, card_name, set_name,
                    card_number, condition, rarity,
                    state, cost_basis, current_price, last_price_update,
                    bin_id, image_url,
                    is_graded, grade_company, grade_value,
                    variant, language,
                    intake_session_id, stored_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    'DISPLAY', %s, %s, CURRENT_TIMESTAMP,
                    %s, %s,
                    FALSE, NULL, NULL,
                    %s, 'EN',
                    %s, CURRENT_TIMESTAMP
                )
            """, (
                barcode_id, tcg_id, card_name, set_name,
                ppt_card_number or item.get("card_number"), condition, item.get("rarity"),
                cost, float(item.get("market_price", cost)),
                bin_id, image_url,
                item.get("variant"),
                item.get("session_id"),
            ))

            png_bytes = generate_barcode_image(
                barcode_id,
                card_name=card_name,
                set_name=set_name,
                condition=condition,
                card_number=ppt_card_number or item.get("card_number") or "",
            )

            results.append({
                "barcode":    barcode_id,
                "bin_label":  bin_label,
                "card_name":  card_name,
                "set_name":   set_name,
                "condition":  condition,
                "png_b64":    __import__("base64").b64encode(png_bytes).decode(),
            })

    # If binders couldn't hold all cards, push remainder to storage
    if assigned_qty < qty:
        overflow_item = dict(item)
        overflow_item["quantity"] = qty - assigned_qty
        overflow_result = _push_raw_item(overflow_item)
        results.extend(overflow_result.get("barcodes", []))

    return {
        "action":      "raw_card_ingested",
        "destination":  "display",
        "product_name": card_name,
        "quantity":    qty,
        "barcodes":    results,
        "bins":        [a["bin_label"] for a in assignments],
    }


def _push_raw_to_grade(item: dict) -> dict:
    """Mark raw card as sent for grading. No barcode, no bin assignment."""
    tcg_id    = item.get("tcgplayer_id")
    card_name = item.get("product_name", "Unknown")
    set_name  = item.get("set_name", "")
    condition = item.get("condition") or "NM"
    qty       = item.get("quantity", 1)
    cost      = float(item.get("offer_price", 0)) / max(qty, 1)

    card_name, set_name, image_url, ppt_card_number = _fetch_ppt_data(tcg_id, card_name, set_name)

    for _ in range(qty):
        barcode_id = generate_barcode_id() if generate_barcode_id else str(_uuid.uuid4())[:20]
        db.execute("""
            INSERT INTO raw_cards (
                barcode, tcgplayer_id, card_name, set_name,
                card_number, condition, rarity,
                state, cost_basis, current_price, last_price_update,
                bin_id, image_url,
                is_graded, grade_company, grade_value,
                variant, language,
                intake_session_id, removal_reason, removal_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                'REMOVED', %s, %s, CURRENT_TIMESTAMP,
                NULL, %s,
                FALSE, NULL, NULL,
                %s, 'EN',
                %s, 'GRADING', CURRENT_TIMESTAMP
            )
        """, (
            barcode_id, tcg_id, card_name, set_name,
            ppt_card_number or item.get("card_number"), condition, item.get("rarity"),
            cost, float(item.get("market_price", cost)),
            image_url,
            item.get("variant"),
            item.get("session_id"),
        ))

    return {
        "action":      "raw_card_graded",
        "destination":  "grade",
        "product_name": card_name,
        "quantity":    qty,
        "barcodes":    [],
        "bins":        [],
    }


def _push_raw_to_bulk(item: dict) -> dict:
    """Bulk cards — no individual tracking, no barcode, no raw_cards row."""
    card_name = item.get("product_name", "Unknown")
    qty       = item.get("quantity", 1)

    # We don't insert into raw_cards — these are not individually tracked
    logger.info(f"Bulk routed: {qty}x {card_name}")

    return {
        "action":      "raw_card_bulk",
        "destination":  "bulk",
        "product_name": card_name,
        "quantity":    qty,
        "barcodes":    [],
        "bins":        [],
    }


def _push_normal_item(entry: dict, tcg_id: int, qty: int, item: dict, normal_cache: dict) -> dict:
    """Push a normal (non-damaged) item: find variant and increment, or create new listing."""
    cache_row = normal_cache.get(tcg_id)
    if cache_row and cache_row.get("shopify_variant_id"):
        inv_item_id = shopify.get_inventory_item_id(cache_row["shopify_variant_id"])
        if inv_item_id:
            # Weighted average COGS before adjusting inventory
            our_unit_cost = float(item.get("offer_price") or 0) / max(int(item.get("quantity") or 1), 1)
            try:
                current_cost, current_qty = shopify.get_inventory_item_cost_and_qty(inv_item_id)
                new_cost = _compute_weighted_cost(current_cost, current_qty, our_unit_cost, qty)
                shopify.set_unit_cost(inv_item_id, new_cost)
                entry["new_unit_cost"] = round(new_cost, 2)
            except Exception as e:
                logger.warning(f"Could not update COGS for {inv_item_id}: {e}")
            shopify.adjust_inventory(inv_item_id, qty, reason="received")
            entry["action"] = "inventory_incremented"
            entry["shopify_variant_id"] = cache_row["shopify_variant_id"]
        else:
            entry.update(action="error", error="Could not find inventory item ID")
    else:
        # No Shopify match — create fully enriched draft listing via enrichment pipeline
        product_name = item.get("product_name", "Unknown Product")
        our_unit_cost = float(item.get("offer_price") or 0) / max(int(item.get("quantity") or 1), 1)

        ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcg_id) if tcg_id else None
        if not ppt_item:
            # PPT lookup failed — build synthetic ppt_item from what we know
            # so enrichment still sets tags, vendor, weight, metafields, AI fields, etc.
            logger.warning(f"PPT lookup failed for {tcg_id} ({product_name}) — enriching from name only")
            ppt_item = {"name": product_name, "tcgPlayerId": tcg_id or "", "setName": ""}

        market_price = float(ppt_item.get("marketPrice") or ppt_item.get("unopenedPrice") or item.get("market_price") or 0)
        try:
            summary = enrichment.create_draft_listing(
                ppt_item,
                price=market_price,
                offer_price=our_unit_cost if our_unit_cost > 0 else None,
                quantity=qty,
            )
            entry["action"] = "created_listing"
            entry["new_product_id"] = summary.get("product_id")
            entry["new_title"] = product_name
            entry["listing_price"] = market_price
            entry["enriched"] = True
            entry["quantity_set"] = summary.get("quantity_set", 0)
        except Exception as e:
            logger.exception(f"Enriched listing creation failed for {tcg_id} — falling back to bare product")
            market_price = float(item.get("market_price", 0))
            new_product = shopify.create_product(
                title=product_name,
                price=market_price,
                tags=["auto-created", "ingest", "needs-enrichment"],
                tcgplayer_id=tcg_id if tcg_id else None,
                quantity=qty,
            )
            entry["action"] = "created_listing"
            entry["new_product_id"] = new_product["id"]
            entry["new_title"] = product_name
            entry["listing_price"] = market_price
            entry["enriched"] = False
            entry["enrich_error"] = str(e)
    return entry


def _push_damaged_item(entry: dict, tcg_id: int, qty: int, item: dict,
                       normal_cache: dict, damaged_cache: dict) -> dict:
    """Push a damaged item: increment existing damaged listing or create one."""
    cache_row = damaged_cache.get(tcg_id)

    if cache_row and cache_row.get("shopify_variant_id"):
        # Damaged listing exists — increment inventory
        inv_item_id = shopify.get_inventory_item_id(cache_row["shopify_variant_id"])
        if inv_item_id:
            our_unit_cost = float(item.get("offer_price") or 0) / max(int(item.get("quantity") or 1), 1)
            try:
                current_cost, current_qty = shopify.get_inventory_item_cost_and_qty(inv_item_id)
                new_cost = _compute_weighted_cost(current_cost, current_qty, our_unit_cost, qty)
                shopify.set_unit_cost(inv_item_id, new_cost)
                entry["new_unit_cost"] = round(new_cost, 2)
            except Exception as e:
                logger.warning(f"Could not update COGS for damaged {inv_item_id}: {e}")
            shopify.adjust_inventory(inv_item_id, qty, reason="received")
            entry["action"] = "inventory_incremented"
            entry["shopify_variant_id"] = cache_row["shopify_variant_id"]
        else:
            entry.update(action="error", error="No inventory item for damaged variant")
    else:
        # No damaged listing — try to duplicate the normal one
        normal_row = normal_cache.get(tcg_id)
        if normal_row and normal_row.get("shopify_product_id"):
            product_gid = f"gid://shopify/Product/{normal_row['shopify_product_id']}"
            original_title = normal_row.get("title", item.get("product_name", "Unknown"))
            damaged_title = f"{original_title} [DAMAGED]"

            new_product = shopify.duplicate_product_as_damaged(product_gid, damaged_title)
            shopify.add_tags(new_product["id"], ["damaged"])

            # Set inventory
            new_var = new_product["variants"]["edges"][0]["node"]
            new_inv_id = new_var.get("inventoryItem", {}).get("id", "").split("/")[-1]
            if new_inv_id:
                shopify.set_inventory_quantity(new_inv_id, qty)

            entry.update(
                action="created_damaged_listing",
                new_title=damaged_title,
                store_price=float(normal_row.get("shopify_price", 0)),
            )
        else:
            # No normal product to duplicate — create the normal listing first,
            # then duplicate it as damaged. This ensures both variants exist.
            product_name = item.get("product_name", "Unknown Product")
            damaged_title = f"{product_name} [DAMAGED]"
            our_unit_cost = float(item.get("offer_price") or 0) / max(int(item.get("quantity") or 1), 1)

            # Step 1: Create normal enriched listing (qty=0, normal items will increment later)
            normal_product_id = None
            ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcg_id) if tcg_id and ppt else None
            if ppt_item and enrichment:
                market_price = float(ppt_item.get("marketPrice") or ppt_item.get("unopenedPrice") or item.get("market_price") or 0)
                try:
                    summary = enrichment.create_draft_listing(
                        ppt_item,
                        price=market_price,
                        offer_price=our_unit_cost if our_unit_cost > 0 else None,
                        quantity=0,  # normal items will increment when they process
                    )
                    normal_product_id = summary.get("product_id")
                    logger.info(f"Created normal listing {normal_product_id} for {product_name} (damaged needed it)")
                except Exception as e:
                    logger.warning(f"Enriched normal listing failed for {tcg_id}: {e}")
            if not normal_product_id:
                # Fallback: skeleton normal listing
                market_price = float(item.get("market_price", 0))
                new_product = shopify.create_product(
                    title=product_name,
                    price=market_price,
                    tags=["auto-created", "ingest", "needs-enrichment"],
                    tcgplayer_id=tcg_id if tcg_id else None,
                    quantity=0,
                )
                normal_product_id = str(new_product["id"]).replace("gid://shopify/Product/", "")
                logger.info(f"Created skeleton normal listing {normal_product_id} for {product_name} (damaged needed it)")

            # Seed normal_cache so normal items for this tcg_id just increment
            normal_cache[tcg_id] = {
                "shopify_product_id": normal_product_id,
                "shopify_variant_id": None,  # will be looked up by _push_normal_item if needed
                "title": product_name,
                "shopify_price": market_price,
            }
            # Look up the variant ID for the cache entry
            try:
                prod_data = shopify._rest("GET", f"/products/{normal_product_id}.json")
                variant = prod_data["product"]["variants"][0]
                normal_cache[tcg_id]["shopify_variant_id"] = str(variant["id"])
            except Exception as e:
                logger.warning(f"Could not fetch variant for new normal product {normal_product_id}: {e}")

            # Step 2: Duplicate the normal listing as damaged
            product_gid = f"gid://shopify/Product/{normal_product_id}"
            try:
                new_product = shopify.duplicate_product_as_damaged(product_gid, damaged_title)
                shopify.add_tags(new_product["id"], ["damaged"])
                new_var = new_product["variants"]["edges"][0]["node"]
                new_inv_id = new_var.get("inventoryItem", {}).get("id", "").split("/")[-1]
                if new_inv_id:
                    shopify.set_inventory_quantity(new_inv_id, qty)
                entry.update(
                    action="created_damaged_listing",
                    new_title=damaged_title,
                    also_created_normal=True,
                )
            except Exception as e:
                logger.exception(f"Duplicate as damaged failed for {normal_product_id}")
                entry.update(action="error", error=f"Created normal listing but duplication failed: {e}")

    return entry


# ═══════════════════════════════════════════════════════════════════
# STORE CHECK (read-only — what's in Shopify for these items?)
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/session/<session_id>/store-check")
def store_check(session_id):
    """Check which items have Shopify listings and inventory."""
    items = ingest.get_session_items(session_id)
    active = [i for i in items if i.get("item_status") in ("good", "damaged") and i.get("is_mapped")]
    tcg_ids = list(set(i["tcgplayer_id"] for i in active if i.get("tcgplayer_id")))
    normal_cache, damaged_cache = ingest.build_cache_maps(tcg_ids)

    results = []
    for item in active:
        tcg_id = item["tcgplayer_id"]
        is_damaged = item.get("item_status") == "damaged"
        cache = damaged_cache.get(tcg_id) if is_damaged else normal_cache.get(tcg_id)
        normal = normal_cache.get(tcg_id)

        r = {
            "item_id": item["id"],
            "product_name": item.get("product_name"),
            "tcgplayer_id": tcg_id,
            "quantity": item.get("quantity", 1),
            "is_damaged": is_damaged,
            "in_store": cache is not None,
            "shopify_price": float(cache["shopify_price"]) if cache else None,
            "shopify_qty": cache.get("shopify_qty", 0) if cache else 0,
            "shopify_title": cache.get("title") if cache else None,
        }

        if is_damaged and not cache and normal:
            r["store_note"] = f"No damaged variant — will duplicate listing, site applies auto-discount via 'damaged' tag"
            r["needs_listing"] = True
        elif not cache:
            r["store_note"] = "No Shopify listing found"
            r["needs_listing"] = True
        else:
            r["needs_listing"] = False

        results.append(r)

    return jsonify({
        "results": [_serialize(r) for r in results],
        "in_store": sum(1 for r in results if r["in_store"]),
        "needs_listing": sum(1 for r in results if r["needs_listing"]),
    })


# ═══════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════
# RAW CARD + GRADED SLAB ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/item/<item_id>/preview-graded", methods=["POST"])
def preview_graded_item(item_id):
    """
    Look up a cert and return all decision-making data WITHOUT creating a Shopify listing.

    Calls the grader's API (PSA for now; BGS/CGC to follow) for cert + images + pop.
    Reads Scrydex local cache for per-grade market/low/mid/high + trends. Reads item
    cost basis from intake_items.

    POST body: { "cert_number": "12345678" }
    """
    data = request.get_json(silent=True) or {}
    cert_number = (data.get("cert_number") or "").strip()
    if not cert_number:
        return jsonify({"error": "cert_number required"}), 400

    item = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        return jsonify({"error": "Item not found"}), 404

    company = (item.get("grade_company") or "PSA").upper()
    grade   = str(item.get("grade_value") or "").strip()
    tcg_id  = item.get("tcgplayer_id")
    qty     = max(1, int(item.get("quantity") or 1))
    unit_cost = float(item.get("offer_price") or 0) / qty

    result = {
        "company":     company,
        "grade":       grade,
        "cert_number": cert_number,
        "product_name": item.get("product_name"),
        "set_name":    item.get("set_name"),
        "cost_basis":  round(unit_cost, 2),
    }

    # ── Grader lookup (cert + images + pop) ─────────────────────────────────
    # PSA is wired. BGS/CGC/SGC: return placeholder, user fills price manually.
    if company == "PSA" and psa_client:
        try:
            psa_cert = psa_client.get_psa_data(cert_number)
            result["psa"] = {
                "year":                psa_cert.get("Year"),
                "subject":              psa_cert.get("Subject"),
                "brand":                psa_cert.get("Brand"),
                "variety":              psa_cert.get("Variety"),
                "card_number":          psa_cert.get("CardNumber"),
                "grade_description":    psa_cert.get("GradeDescription"),
                "total_population":     psa_cert.get("TotalPopulation"),
                "population_higher":    psa_cert.get("PopulationHigher"),
                "qualifier_population": psa_cert.get("TotalPopulationWithQualifier"),
            }
            result["images"] = psa_client.get_psa_images(cert_number)
        except PSANotFound:
            return jsonify({"error": f"PSA cert {cert_number} not found"}), 404
        except PSAQuotaHit as e:
            return jsonify({"error": f"PSA API quota hit — try again tomorrow: {e}"}), 429
        except Exception as e:
            logger.exception(f"PSA preview failed for cert {cert_number}: {e}")
            return jsonify({"error": f"PSA lookup failed: {e}"}), 500
    else:
        result["psa"] = None
        result["images"] = []
        if company != "PSA":
            result["note"] = f"{company} cert lookup not yet implemented — fill price manually"

    # ── Scrydex cache lookup: full card row for set-name comparison ────────
    scrydex_set = None
    scrydex_card_name = None
    if tcg_id:
        card_row = db.query_one("""
            SELECT expansion_name, product_name, card_number
            FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'card'
            LIMIT 1
        """, (int(tcg_id),))
        if card_row:
            scrydex_set = card_row.get("expansion_name")
            scrydex_card_name = card_row.get("product_name")
    result["scrydex_card"] = {
        "set_name": scrydex_set,
        "card_name": scrydex_card_name,
    }

    # ── Set-name mismatch check (PSA brand vs Scrydex set) ─────────────────
    # Catches TCG-ID misassignments at intake (e.g. Base Set 2 slab linked to
    # Legendary Collection). Normalization is crude on purpose — we just want
    # a heads-up, not a gate.
    result["set_mismatch"] = False
    psa_brand = (result.get("psa") or {}).get("brand") or ""
    if psa_brand and scrydex_set:
        def _norm(s):
            import re as _re
            s = _re.sub(r"(?i)\bpokemon\b", "", s or "")
            s = _re.sub(r"\b(EN|JP|ENG|JPN|FR|SVP|SWSH|SVI|XY|BW|SM|EX|PROMO)[-\s]?\w*\b", "", s, flags=_re.IGNORECASE)
            s = _re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
            return s
        a, b = _norm(psa_brand), _norm(scrydex_set)
        if a and b:
            # Match if either normalized string contains the other, or they share
            # at least half of the shorter string's tokens
            if a in b or b in a:
                result["set_mismatch"] = False
            else:
                ta, tb = set(a.split()), set(b.split())
                if ta and tb:
                    overlap = len(ta & tb) / min(len(ta), len(tb))
                    result["set_mismatch"] = overlap < 0.5

    # ── Scrydex graded pricing (always live for slabs) ──────────────────────
    # Pass card_name + set_name so JP cards without tcgplayer_id can still
    # be resolved by name search in the cache.
    from graded_pricing import get_live_graded_comps
    result["scrydex"] = None
    if grade:
        result["scrydex"] = get_live_graded_comps(
            int(tcg_id) if tcg_id else None, company, grade, db,
            card_name=item.get("product_name"),
            set_name=item.get("set_name"),
            card_number=item.get("card_number"),
        )

    return jsonify(_serialize(result))


@app.route("/api/ingest/item/<item_id>/push-graded", methods=["POST"])
def push_graded_item(item_id):
    """
    Push a single graded slab to Shopify.
    Called per-item from the frontend's cert-entry flow.

    POST body:
        { "cert_number": "12345678", "session_id": "..." }

    Returns:
        { action, shopify_product_id, shopify_variant_id, title, cert_number }
    """
    if not shopify:
        return jsonify({"error": "Shopify not configured"}), 503
    if not psa_client:
        return jsonify({"error": "psa_client module not available"}), 503

    data        = request.get_json(silent=True) or {}
    cert_number = (data.get("cert_number") or "").strip()
    session_id  = data.get("session_id")
    price_override = data.get("price")  # From preview panel — user's chosen listing price

    if not cert_number:
        return jsonify({"error": "cert_number is required"}), 400

    # Load item
    item = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        return jsonify({"error": "Item not found"}), 404
    if item.get("pushed_at"):
        return jsonify({"error": "Item already pushed"}), 400

    # If qty > 1, peel off one slab so each cert gets its own Shopify product
    if (item.get("quantity") or 1) > 1:
        try:
            item = ingest.split_one_slab(item_id)
            item_id = item["id"]
        except Exception as e:
            logger.exception(f"split_one_slab failed for {item_id}: {e}")
            return jsonify({"error": f"Could not split slab: {e}"}), 500

    grade_company = (item.get("grade_company") or "PSA").upper()
    grade_value   = item.get("grade_value") or "9"
    tcg_id        = item.get("tcgplayer_id")
    # User-chosen price from preview panel; fall back to market (what we valued it at)
    # then offer (cost). Previously defaulted to offer which listed at cost = 0 margin.
    if price_override is not None and price_override != "":
        try:
            price = float(price_override)
        except (TypeError, ValueError):
            return jsonify({"error": "price must be a number"}), 400
        if price <= 0:
            return jsonify({"error": "price must be greater than 0"}), 400
    else:
        price = float(item.get("market_price") or item.get("offer_price") or 0)

    # Fetch PPT card data for clean name
    ppt_card = None
    if tcg_id and ppt:
        try:
            ppt_card = ppt.get_card_by_tcgplayer_id(int(tcg_id))
        except Exception as e:
            logger.warning(f"PPT fetch for graded TCG#{tcg_id}: {e}")

    try:
        result = psa_client.push_graded_slab(
            tcgplayer_id=tcg_id,
            grade_company=grade_company,
            grade_value=grade_value,
            cert_number=cert_number,
            price=price,
            ppt_card=ppt_card,
            shopify_domain=shopify.store,
            shopify_token=shopify.token,
            db=db,
        )
    except PSAQuotaHit as e:
        return jsonify({"error": f"PSA API quota hit — try again tomorrow: {e}"}), 429
    except ShopifyCreateError as e:
        logger.exception(f"push_graded_item failed for item {item_id}: {e}")
        return jsonify({
            "error": f"Shopify rejected the listing ({e.status_code})",
            "shopify_body": e.body,
        }), 502
    except Exception as e:
        logger.exception(f"push_graded_item failed for item {item_id}: {e}")
        return jsonify({"error": str(e)}), 500

    # ── Post-creation enrichment (category, era, tcgplayer_id, COGS) ────────
    # Graded listings bypass product_enrichment so these need to be added
    # after the Shopify product exists. Non-fatal — log and continue.
    product_gid = result.get("shopify_product_id")
    if product_gid and result.get("action") == "created_listing":
        product_gid_str = f"gid://shopify/Product/{product_gid}"
        try:
            # Category: Gaming Cards
            enrichment.set_product_category(product_gid_str)
        except Exception as e:
            logger.warning(f"Slab enrichment: category failed: {e}")
        try:
            # Era inference + tcgplayer_id metafield (tcg namespace, list type)
            card_name = item.get("product_name") or ""
            set_name  = item.get("set_name") or ""
            era = enrichment.infer_era(card_name, set_name)
            enrichment.set_product_metafields(product_gid_str, str(tcg_id) if tcg_id else "", era)
        except Exception as e:
            logger.warning(f"Slab enrichment: metafields failed: {e}")
        try:
            # COGS — unit cost from intake offer
            unit_cost = float(item.get("offer_price") or 0)
            if unit_cost > 0:
                enrichment.set_variant_cost(product_gid_str, unit_cost)
        except Exception as e:
            logger.warning(f"Slab enrichment: variant cost failed: {e}")

    # Mark item pushed + store cert number
    db.execute("""
        UPDATE intake_items
        SET pushed_at = CURRENT_TIMESTAMP, cert_number = %s
        WHERE id = %s
    """, (cert_number, item_id))

    # Update session status
    if session_id:
        remaining = db.query_one("""
            SELECT COUNT(*) AS cnt FROM intake_items
            WHERE session_id = %s AND pushed_at IS NULL
              AND item_status IN ('good','damaged') AND is_mapped = TRUE
        """, (session_id,))
        if remaining and remaining["cnt"] == 0:
            db.execute(
                "UPDATE intake_sessions SET status='ingested' WHERE id=%s",
                (session_id,)
            )

    return jsonify({"success": True, **result})


# ═══════════════════════════════════════════════════════════════════
# RAW CARD ROUTING
# ═══════════════════════════════════════════════════════════════════

ROUTING_DESTINATIONS = {"storage", "display", "grade", "bulk"}


@app.route("/api/ingest/session/<session_id>/auto-route", methods=["POST"])
def auto_route_session(session_id):
    """
    Apply price-based routing rules to all raw items in a session.
    Rules: <$1 = bulk, $1-$5 = display (if binder capacity), else storage.
    Does NOT overwrite items that have already been manually routed.
    """
    # Only consider items NOT yet manually reviewed — auto-route should never
    # overwrite a user's decision.
    items = db.query("""
        SELECT id, market_price, quantity, routing_destination
        FROM intake_items
        WHERE session_id = %s
          AND product_type = 'raw'
          AND is_graded IS NOT TRUE
          AND item_status IN ('good', 'damaged')
          AND is_mapped = TRUE
          AND pushed_at IS NULL
          AND routing_reviewed_at IS NULL
        ORDER BY COALESCE(market_price, 0) ASC
    """, (session_id,))

    if not items:
        # Not an error — just means all items have been reviewed already
        return jsonify({"success": True, "routed": {"storage": 0, "display": 0, "grade": 0, "bulk": 0},
                        "skipped_reviewed": True})

    # Get total binder capacity, accounting for items already routed to display
    binder_remaining = 0
    if get_binder_capacity:
        binders = get_binder_capacity(db)
        binder_remaining = sum(b["available"] for b in binders)

    # Subtract qty already routed to display by reviewed items
    already_displayed = db.query_one("""
        SELECT COALESCE(SUM(quantity), 0) as qty FROM intake_items
        WHERE session_id = %s AND product_type = 'raw' AND routing_destination = 'display'
          AND routing_reviewed_at IS NOT NULL
    """, (session_id,))
    binder_remaining = max(0, binder_remaining - int(already_displayed["qty"] or 0))

    routed = {"storage": 0, "display": 0, "grade": 0, "bulk": 0}

    for item in items:
        price = float(item.get("market_price") or 0)
        qty = item.get("quantity", 1)

        if price < 1.00:
            dest = "bulk"
        elif price < 5.00 and binder_remaining >= qty:
            dest = "display"
            binder_remaining -= qty
        else:
            dest = "storage"

        # Do NOT set routing_reviewed_at — these are only defaults, user hasn't decided
        db.execute("""
            UPDATE intake_items SET routing_destination = %s
            WHERE id = %s AND routing_reviewed_at IS NULL
        """, (dest, item["id"]))
        routed[dest] += qty

    return jsonify(_serialize({"success": True, "routed": routed, "binder_remaining": binder_remaining}))


@app.route("/api/ingest/session/<session_id>/route-card", methods=["POST"])
def route_card(session_id):
    """Set routing destination for a single item — marks item as reviewed."""
    data = request.get_json(silent=True) or {}
    item_id = data.get("item_id")
    destination = data.get("destination")

    if not item_id or destination not in ROUTING_DESTINATIONS:
        return jsonify({"error": f"item_id required, destination must be one of {ROUTING_DESTINATIONS}"}), 400

    db.execute("""
        UPDATE intake_items
        SET routing_destination = %s, routing_reviewed_at = NOW()
        WHERE id = %s AND session_id = %s
    """, (destination, item_id, session_id))

    return jsonify({"success": True})


@app.route("/api/ingest/session/<session_id>/route-batch", methods=["POST"])
def route_batch(session_id):
    """Batch route multiple items to the same destination — marks all as reviewed."""
    data = request.get_json(silent=True) or {}
    item_ids = data.get("item_ids", [])
    destination = data.get("destination")

    if not item_ids or destination not in ROUTING_DESTINATIONS:
        return jsonify({"error": "item_ids and valid destination required"}), 400

    for item_id in item_ids:
        db.execute("""
            UPDATE intake_items
            SET routing_destination = %s, routing_reviewed_at = NOW()
            WHERE id = %s AND session_id = %s
        """, (destination, item_id, session_id))

    return jsonify({"success": True, "count": len(item_ids)})


@app.route("/api/ingest/session/<session_id>/route-progress")
def route_progress(session_id):
    """Return routing session progress (how many reviewed, total)."""
    row = db.query_one("""
        SELECT
            COUNT(*) FILTER (WHERE item_status IN ('good','damaged')
                             AND is_mapped = TRUE
                             AND pushed_at IS NULL
                             AND is_graded IS NOT TRUE
                             AND product_type = 'raw') AS total_routable,
            COUNT(*) FILTER (WHERE item_status IN ('good','damaged')
                             AND is_mapped = TRUE
                             AND pushed_at IS NULL
                             AND is_graded IS NOT TRUE
                             AND product_type = 'raw'
                             AND routing_reviewed_at IS NOT NULL) AS reviewed
        FROM intake_items
        WHERE session_id = %s
    """, (session_id,))
    total = row["total_routable"] or 0
    reviewed = row["reviewed"] or 0
    return jsonify({
        "total": total,
        "reviewed": reviewed,
        "has_progress": reviewed > 0 and reviewed < total,
        "complete": total > 0 and reviewed == total,
    })


@app.route("/api/ingest/session/<session_id>/split-route", methods=["POST"])
def split_route(session_id):
    """Split an item's quantity so a portion can be routed to a different destination.
    Body: { item_id, split_qty, destination }
    Reduces original item qty, creates new item with split_qty and the given destination.
    """
    data = request.get_json(silent=True) or {}
    item_id = data.get("item_id")
    split_qty = int(data.get("split_qty", 0))
    destination = data.get("destination")

    if not item_id or split_qty < 1 or destination not in ROUTING_DESTINATIONS:
        return jsonify({"error": "item_id, split_qty >= 1, and valid destination required"}), 400

    item = db.query_one("SELECT * FROM intake_items WHERE id = %s AND session_id = %s", (item_id, session_id))
    if not item:
        return jsonify({"error": "Item not found"}), 404

    total_qty = item.get("quantity", 1)
    if split_qty >= total_qty:
        return jsonify({"error": f"split_qty must be less than current quantity ({total_qty})"}), 400

    remaining_qty = total_qty - split_qty
    market_price = float(item.get("market_price") or 0)
    unit_cost = float(item.get("unit_cost_basis") or 0)

    # Reduce original item qty and recalculate offer
    session = db.query_one("SELECT offer_percentage FROM intake_sessions WHERE id = %s", (session_id,))
    offer_pct = float(session.get("offer_percentage", 65)) / 100 if session else 0.65
    remaining_offer = round(market_price * offer_pct * remaining_qty, 2)

    db.execute("""
        UPDATE intake_items SET quantity = %s, offer_price = %s WHERE id = %s
    """, (remaining_qty, remaining_offer, item_id))

    # Create split item — copies all card fields, new qty + destination
    import uuid as _uuid
    split_id = str(_uuid.uuid4())
    split_offer = round(market_price * offer_pct * split_qty, 2)

    db.execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, set_name, tcgplayer_id, card_number,
            condition, rarity, variant, language, variance,
            quantity, market_price, offer_price, unit_cost_basis,
            product_type, is_mapped, item_status, is_graded, grade_company, grade_value,
            routing_destination, verified_at, listing_condition
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
    """, (
        split_id, session_id, item.get("product_name"), item.get("set_name"),
        item.get("tcgplayer_id"), item.get("card_number"),
        item.get("condition"), item.get("rarity"), item.get("variant"),
        item.get("language"), item.get("variance"),
        split_qty, market_price, split_offer, unit_cost,
        item.get("product_type", "raw"), item.get("is_mapped", False),
        item.get("item_status", "good"), item.get("is_graded", False),
        item.get("grade_company"), item.get("grade_value"),
        destination, item.get("verified_at"), item.get("listing_condition"),
    ))

    return jsonify({
        "success": True,
        "original": {"id": item_id, "quantity": remaining_qty},
        "split": {"id": split_id, "quantity": split_qty, "destination": destination},
    })


@app.route("/api/ingest/session/<session_id>/split-singles", methods=["POST"])
def split_singles(session_id):
    """Explode an item with qty > 1 into individual qty=1 rows.
    Body: { item_id }
    """
    data = request.get_json(silent=True) or {}
    item_id = data.get("item_id")
    if not item_id:
        return jsonify({"error": "item_id required"}), 400

    item = db.query_one("SELECT * FROM intake_items WHERE id = %s AND session_id = %s", (item_id, session_id))
    if not item:
        return jsonify({"error": "Item not found"}), 404

    total_qty = item.get("quantity", 1)
    if total_qty <= 1:
        return jsonify({"error": "Item already qty=1"}), 400

    session = db.query_one("SELECT offer_percentage FROM intake_sessions WHERE id = %s", (session_id,))
    offer_pct = float(session.get("offer_percentage", 65)) / 100 if session else 0.65
    market_price = float(item.get("market_price") or 0)
    unit_offer = round(market_price * offer_pct, 2)
    unit_cost = float(item.get("unit_cost_basis") or 0)
    dest = item.get("routing_destination") or "storage"

    import uuid as _uuid

    # Keep original as qty=1
    db.execute("UPDATE intake_items SET quantity = 1, offer_price = %s WHERE id = %s",
               (unit_offer, item_id))

    # Create (total_qty - 1) new rows
    new_ids = []
    for _ in range(total_qty - 1):
        new_id = str(_uuid.uuid4())
        new_ids.append(new_id)
        db.execute("""
            INSERT INTO intake_items (
                id, session_id, product_name, set_name, tcgplayer_id, card_number,
                condition, rarity, variant, language, variance,
                quantity, market_price, offer_price, unit_cost_basis,
                product_type, is_mapped, item_status, is_graded, grade_company, grade_value,
                routing_destination, verified_at, listing_condition
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                1, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """, (
            new_id, session_id, item.get("product_name"), item.get("set_name"),
            item.get("tcgplayer_id"), item.get("card_number"),
            item.get("condition"), item.get("rarity"), item.get("variant"),
            item.get("language"), item.get("variance"),
            market_price, unit_offer, unit_cost,
            item.get("product_type", "raw"), item.get("is_mapped", False),
            item.get("item_status", "good"), item.get("is_graded", False),
            item.get("grade_company"), item.get("grade_value"),
            dest, item.get("verified_at"), item.get("listing_condition"),
        ))

    return jsonify({
        "success": True,
        "original_id": item_id,
        "created": len(new_ids),
        "total": total_qty,
    })


@app.route("/api/ingest/session/<session_id>/route-summary")
def route_summary(session_id):
    """Get routing status for a session's raw items."""
    items = db.query("""
        SELECT id, product_name, set_name, condition, market_price, quantity,
               routing_destination, routing_reviewed_at, tcgplayer_id, card_number, offer_price
        FROM intake_items
        WHERE session_id = %s
          AND product_type = 'raw'
          AND is_graded IS NOT TRUE
          AND item_status IN ('good', 'damaged')
          AND is_mapped = TRUE
          AND pushed_at IS NULL
        ORDER BY COALESCE(market_price, 0) ASC
    """, (session_id,))

    by_dest = {"storage": 0, "display": 0, "grade": 0, "bulk": 0}
    for item in items:
        dest = item.get("routing_destination") or "storage"
        qty = item.get("quantity", 1)
        by_dest[dest] = by_dest.get(dest, 0) + qty

    total_qty = sum(i.get("quantity", 1) for i in items)

    # Binder capacity
    binder_capacity = []
    if get_binder_capacity:
        binder_capacity = get_binder_capacity(db)

    return jsonify(_serialize({
        "items": [dict(i) for i in items],
        "total_items": len(items),
        "total_qty": total_qty,
        "by_destination": by_dest,
        "binder_capacity": binder_capacity,
    }))


@app.route("/api/ingest/binder-locations")
def binder_locations():
    """List binder locations with capacity."""
    if not get_binder_capacity:
        return jsonify({"error": "Storage module not available"}), 503
    return jsonify(_serialize({"binders": get_binder_capacity(db)}))


# ── Route enrichment endpoints (PPT graded prices + images) ──────────

@app.route("/api/ingest/session/<session_id>/enrich-route", methods=["POST"])
def enrich_route(session_id):
    """Kick off background PPT fetch for graded prices + images for all routable items."""
    if not ppt:
        return jsonify({"error": "PPT not configured"}), 503

    # If cache already exists for this session, skip re-fetch unless forced
    data = request.get_json(silent=True) or {}
    force = data.get("force", False)
    if session_id in _enrich_cache and not force:
        return jsonify({"job_id": None, "status": "complete", "total": len(_enrich_cache[session_id])})
    if force:
        _enrich_cache.pop(session_id, None)

    items = db.query("""
        SELECT id, product_name, set_name, condition, market_price, quantity,
               routing_destination, tcgplayer_id, card_number, offer_price, variant
        FROM intake_items
        WHERE session_id = %s
          AND product_type = 'raw'
          AND is_graded IS NOT TRUE
          AND item_status IN ('good', 'damaged')
          AND is_mapped = TRUE
          AND pushed_at IS NULL
        ORDER BY COALESCE(market_price, 0) ASC
    """, (session_id,))

    if not items:
        return jsonify({"error": "No raw items to enrich"}), 400

    active_dicts = [dict(i) for i in items]

    # Count unique tcgplayer_ids
    unique_tcg = set(str(i["tcgplayer_id"]) for i in active_dicts if i.get("tcgplayer_id"))

    job_id = str(_uuid.uuid4())
    _enrich_jobs[job_id] = {
        "status": "running",
        "progress": 0,
        "total": len(unique_tcg),
        "session_id": session_id,
        "errors": 0,
    }

    thread = threading.Thread(
        target=_enrich_route_worker,
        args=(job_id, session_id, active_dicts),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id, "status": "running", "total": len(unique_tcg)})


@app.route("/api/ingest/enrich-job/<job_id>", methods=["GET"])
def get_enrich_job(job_id):
    """Poll background enrichment job status."""
    job = _enrich_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/ingest/session/<session_id>/route-enriched")
def route_enriched(session_id):
    """Get routing data enriched with PPT graded prices, images, and grading economics."""
    items = db.query("""
        SELECT id, product_name, set_name, condition, market_price, quantity,
               routing_destination, tcgplayer_id, card_number, offer_price,
               variant, rarity, language
        FROM intake_items
        WHERE session_id = %s
          AND product_type = 'raw'
          AND is_graded IS NOT TRUE
          AND item_status IN ('good', 'damaged')
          AND is_mapped = TRUE
          AND pushed_at IS NULL
        ORDER BY COALESCE(market_price, 0) ASC
    """, (session_id,))

    cache = _enrich_cache.get(session_id, {})

    enriched = []
    for item in items:
        d = dict(item)
        tcg_id = str(d.get("tcgplayer_id") or "")
        cached = cache.get(tcg_id)  # None if not yet fetched
        if cached is not None and "error" not in cached:
            d["_enriched"] = True
            d["image_url"] = cached.get("image_url")
            d["graded_prices"] = cached.get("graded_prices", {})
            d["grading_economics"] = cached.get("grading_economics", {})
        elif cached is not None:
            # PPT errored for this card — still mark as enriched (done, just no data)
            d["_enriched"] = True
            d["image_url"] = None
            d["graded_prices"] = {}
            d["grading_economics"] = {}
        else:
            # Not yet fetched by background worker
            d["_enriched"] = False
            d["image_url"] = None
            d["graded_prices"] = {}
            d["grading_economics"] = {}
        enriched.append(d)

    by_dest = {"storage": 0, "display": 0, "grade": 0, "bulk": 0}
    for item in enriched:
        dest = item.get("routing_destination") or "storage"
        qty = item.get("quantity", 1)
        by_dest[dest] = by_dest.get(dest, 0) + qty

    return jsonify(_serialize({
        "items": enriched,
        "total_items": len(enriched),
        "total_qty": sum(i.get("quantity", 1) for i in enriched),
        "by_destination": by_dest,
    }))


@app.route("/api/ingest/session/<session_id>/push-raw", methods=["POST"])
def push_raw_items(session_id):
    """
    Push all unmapped raw (non-graded) items in a session to internal inventory.
    Routes cards based on routing_destination: storage, display, grade, or bulk.

    POST body (optional): { "item_ids": [...] }  — subset of items to push

    Returns list of results grouped by destination.
    """
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    data = request.get_json(silent=True) or {}
    requested_ids = set(str(x) for x in (data.get("item_ids") or []))

    items = db.query("""
        SELECT * FROM intake_items
        WHERE session_id = %s
          AND product_type = 'raw'
          AND is_graded IS NOT TRUE
          AND item_status IN ('good', 'damaged')
          AND is_mapped = TRUE
          AND pushed_at IS NULL
    """, (session_id,))

    if requested_ids:
        items = [i for i in items if str(i["id"]) in requested_ids]

    if not items:
        return jsonify({"error": "No unmapped raw items to push"}), 400

    results = []
    errors  = []

    for item in items:
        item_dict = dict(item)
        item_dict["session_id"] = session_id
        dest = item.get("routing_destination") or "storage"
        try:
            if dest == "bulk":
                r = _push_raw_to_bulk(item_dict)
            elif dest == "display":
                r = _push_raw_to_display(item_dict)
            elif dest == "grade":
                r = _push_raw_to_grade(item_dict)
            else:
                r = _push_raw_item(item_dict)  # storage (original)
            results.append(r)
            db.execute(
                "UPDATE intake_items SET pushed_at = CURRENT_TIMESTAMP WHERE id = %s",
                (item["id"],)
            )
        except Exception as e:
            logger.exception(f"push_raw_item failed for {item['id']}: {e}")
            errors.append({"item_id": str(item["id"]),
                           "product_name": item.get("product_name"), "error": str(e)})

    return jsonify({
        "success":  True,
        "pushed":   len(results),
        "errors":   errors,
        "results":  results,
    })


@app.route("/api/raw-cards/barcode/<barcode_id>.png")
def get_raw_barcode(barcode_id):
    """
    Generate + return barcode label PNG for a raw card.
    Reprintable at any time — looks up card data from raw_cards table.
    """
    if not generate_barcode_image:
        return jsonify({"error": "barcode_gen not available"}), 503

    card = db.query_one("""
        SELECT card_name, set_name, condition, card_number
        FROM raw_cards WHERE barcode = %s
    """, (barcode_id,))

    if not card:
        return jsonify({"error": "Barcode not found"}), 404

    png = generate_barcode_image(
        barcode_id,
        card_name=card["card_name"],
        set_name=card["set_name"],
        condition=card.get("condition", ""),
        card_number=card.get("card_number") or "",
    )

    from flask import Response
    return Response(png, mimetype="image/png",
                    headers={"Content-Disposition": f'inline; filename="{barcode_id}.png"'})


@app.route("/api/raw-cards/session/<session_id>")
def get_session_raw_cards(session_id):
    """List all raw cards ingested from a session, with bin assignments."""
    cards = db.query("""
        SELECT rc.barcode, rc.card_name, rc.set_name, rc.condition,
               rc.current_price, rc.state, rc.image_url,
               sl.bin_label, sl.card_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.intake_session_id = %s
        ORDER BY rc.created_at ASC
    """, (session_id,))
    return jsonify({"cards": [dict(c) for c in cards]})


# MANUAL OVERRIDES
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ingest/session/<session_id>/force-ingested", methods=["POST"])
def force_mark_ingested(session_id):
    """Manually mark a session as ingested (escape hatch for stuck sessions)."""
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] == "ingested":
        return jsonify({"error": "Already ingested"}), 400
    ingest.mark_session_ingested(session_id)
    return jsonify({"success": True, "message": "Session manually marked as ingested."})


# Breakdown-cache, store-prices routes now served by shared breakdown blueprint


@app.route("/api/ingest/item/<item_id>/break-down", methods=["POST"])
def break_down_item_endpoint(item_id):
    """
    Break down a sealed item (or a portion of it) into components.

    Body:
      components    list  — component objects (required)
      qty_to_break  int   — how many units to break (default: all)
      variant_name  str   — config name for cache (default: "Standard")
      variant_id    str   — update existing variant instead of creating new
      variant_notes str   — notes for this variant
      save_to_cache bool  — persist recipe (default: true)
    """
    data = request.get_json(silent=True) or {}
    components = data.get("components", [])
    qty_to_break = data.get("qty_to_break")
    variant_name = data.get("variant_name", "Standard")
    variant_id = data.get("variant_id")
    variant_notes = data.get("variant_notes")
    save_to_cache = data.get("save_to_cache", True)

    if not components:
        return jsonify({"error": "No components provided"}), 400
    try:
        if qty_to_break is not None:
            result = ingest.split_then_break_down(
                item_id, int(qty_to_break), components,
                variant_name=variant_name, variant_notes=variant_notes,
                variant_id=variant_id, save_to_cache=save_to_cache,
            )
        else:
            result = ingest.break_down_item_with_cache(
                item_id, components,
                variant_name=variant_name, variant_notes=variant_notes,
                variant_id=variant_id, save_to_cache=save_to_cache,
            )
        return jsonify({
            "success": True,
            "parent_item": _serialize(result["parent_item"]),
            "remainder_item": _serialize(result.get("remainder_item")),
            "child_items": [_serialize(c) for c in result["child_items"]],
            "session": _serialize(result["session"]),
            "cache_saved": result.get("cache_saved", False),
            "variant_name": result.get("variant_name", variant_name),
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.exception(f"Break down failed for item {item_id}")
        return jsonify({"error": f"Break down failed: {str(e)}\n\nHave you run migrate_breakdown_cache.py?"}), 500


# ═══════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════
# PRODUCT ENRICHMENT
# ═══════════════════════════════════════════════════════════════════

@app.route("/enrich")
def enrich_page():
    store = os.getenv("SHOPIFY_STORE", "")
    # Extract store handle from myshopify.com domain for admin URL construction
    # e.g. "d1m1a4-1i.myshopify.com" -> "d1m1a4-1i"
    store_handle = store.replace(".myshopify.com", "").split(".")[0] if store else ""
    return render_template("enrich_preview.html", shopify_store_handle=store_handle)


@app.route("/api/ppt/search-sealed", methods=["POST"])
def ppt_search_sealed():
    """Search for sealed products by name. Uses cache first; pass live=true to skip cache."""
    if not ppt:
        return jsonify({"error": "PPT API not configured"}), 503
    data = request.get_json(silent=True) or {}
    q = data.get("query", "").strip()
    if not q:
        return jsonify({"error": "No query"}), 400
    live_only = data.get("live", False)
    try:
        if live_only:
            # Bypass cache — go straight to PPT/Scrydex live API
            from price_provider import PriceProvider as _PP
            results = ppt.primary.search_sealed_products(q, limit=5)
            results = ppt._stamp(results, ppt._primary_source)
        else:
            results = ppt.search_sealed_products(q, limit=5)
        for r in results:
            if not r.get("tcgplayer_id"):
                tcg_id = r.get("tcgplayerId") or r.get("tcgPlayerId") or r.get("id")
                if tcg_id:
                    r["tcgplayer_id"] = int(tcg_id)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/store/search", methods=["POST"])
def store_search():
    """Search the Shopify product cache by title. Finds Japanese products, accessories, etc."""
    data = request.get_json(silent=True) or {}
    q = data.get("query", "").strip()
    if not q:
        return jsonify({"error": "No query"}), 400
    try:
        results = db.query("""
            SELECT title, shopify_price, tcgplayer_id, shopify_variant_id, handle
            FROM inventory_product_cache
            WHERE title ILIKE %s AND is_damaged = FALSE
            ORDER BY title ASC
            LIMIT 15
        """, (f"%{q}%",))
        return jsonify({"results": [dict(r) for r in results]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ppt/lookup-by-id/<int:tcgplayer_id>")
def ppt_lookup_by_id(tcgplayer_id):
    """Look up any product (card or sealed) by TCGPlayer ID. Tries card first, then sealed."""
    if not ppt:
        return jsonify({"error": "PPT API not configured"}), 503
    try:
        # Try card first
        card = ppt.get_card_by_tcgplayer_id(tcgplayer_id)
        if card:
            market_price = PriceProvider.extract_market_price(card)
            variants = PriceProvider.extract_variants(card)
            return jsonify({
                "found": True,
                "type": "card",
                "name": card.get("name", ""),
                "set_name": card.get("setName", ""),
                "card_number": card.get("cardNumber", ""),
                "rarity": card.get("rarity", ""),
                "tcgplayer_id": tcgplayer_id,
                "market_price": float(market_price) if market_price else 0,
                "variants": variants,
                "image_url": card.get("imageCdnUrl800") or card.get("imageCdnUrl") or card.get("imageCdnUrl400"),
                "price_source": card.get("_price_source"),
            })

        # Try sealed
        sealed = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
        if sealed:
            price = sealed.get("unopenedPrice") or sealed.get("marketPrice") or 0
            return jsonify({
                "found": True,
                "type": "sealed",
                "name": sealed.get("name") or sealed.get("productName", ""),
                "set_name": sealed.get("setName") or sealed.get("set_name", ""),
                "tcgplayer_id": tcgplayer_id,
                "market_price": float(price),
                "price_source": sealed.get("_price_source"),
            })

        return jsonify({"found": False, "tcgplayer_id": tcgplayer_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ppt/sealed/<int:tcgplayer_id>")
def ppt_sealed_lookup(tcgplayer_id):
    """Fetch a sealed product from PPT by TCGPlayer ID — used by the preview page."""
    item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
    if not item:
        return jsonify({"error": f"No PPT product found for TCGPlayer ID {tcgplayer_id}"}), 404
    return jsonify(item)


@app.route("/api/ppt/sealed/<int:tcgplayer_id>/raw")
def ppt_sealed_raw(tcgplayer_id):
    """Return the raw PPT response for debugging — shows all available fields."""
    item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
    if not item:
        return jsonify({"error": f"No PPT product found for TCGPlayer ID {tcgplayer_id}"}), 404
    return jsonify({"keys": list(item.keys()), "data": item})


@app.route("/api/price-compare/<int:tcgplayer_id>")
def price_compare(tcgplayer_id):
    """
    Side-by-side comparison of PPT vs Scrydex for a card.
    Hit this while routing to see both providers in action.
    """
    from ppt_client import PPTClient
    from scrydex_client import ScrydexClient
    import os, time

    result = {"tcgplayer_id": tcgplayer_id, "ppt": None, "scrydex": None}

    # PPT lookup
    ppt_key = os.getenv("PPT_API_KEY", "")
    if ppt_key:
        try:
            ppt_direct = PPTClient(ppt_key)
            t0 = time.time()
            card = ppt_direct.get_card_by_tcgplayer_id(tcgplayer_id)
            ppt_ms = int((time.time() - t0) * 1000)
            if card:
                result["ppt"] = {
                    "name": card.get("name"),
                    "set": card.get("setName"),
                    "market": float(PPTClient.extract_market_price(card) or 0),
                    "variants": PPTClient.extract_variants(card),
                    "graded": PPTClient.extract_graded_prices(card),
                    "image": card.get("imageCdnUrl800") or card.get("imageCdnUrl"),
                    "ms": ppt_ms,
                }
        except Exception as e:
            result["ppt"] = {"error": str(e)}

    # Scrydex lookup
    sx_key = os.getenv("SCRYDEX_API_KEY", "")
    sx_team = os.getenv("SCRYDEX_TEAM_ID", "")
    if sx_key and sx_team:
        try:
            sx = ScrydexClient(sx_key, sx_team, db=db)
            t0 = time.time()
            card = sx.get_card_by_tcgplayer_id(tcgplayer_id, include_history=True)
            sx_ms = int((time.time() - t0) * 1000)
            if card:
                result["scrydex"] = {
                    "name": card.get("name"),
                    "set": card.get("setName"),
                    "scrydex_id": card.get("scrydexId"),
                    "market": float(ScrydexClient.extract_market_price(card) or 0),
                    "variants": ScrydexClient.extract_variants(card),
                    "graded": ScrydexClient.extract_graded_prices(card),
                    "image": card.get("imageCdnUrl800") or card.get("imageCdnUrl"),
                    "ms": sx_ms,
                }
            else:
                result["scrydex"] = {"error": "No mapping found", "ms": sx_ms}
        except Exception as e:
            result["scrydex"] = {"error": str(e)}

    # Summary
    if result["ppt"] and result["scrydex"] and "error" not in result["ppt"] and "error" not in result["scrydex"]:
        ppt_m = result["ppt"]["market"]
        sx_m = result["scrydex"]["market"]
        if ppt_m > 0:
            result["market_delta_pct"] = round(abs(ppt_m - sx_m) / ppt_m * 100, 1)
        result["ppt_ms"] = result["ppt"]["ms"]
        result["scrydex_ms"] = result["scrydex"]["ms"]

    return jsonify(result)


@app.route("/price-dashboard")
def price_dashboard():
    """Side-by-side comparison of Store vs PPT vs Cache across inventory."""
    return render_template("price_dashboard.html")


@app.route("/api/price-compare/batch", methods=["POST"])
def price_compare_batch():
    """
    Batch price comparison for inventory items.
    Body: { "filter": "sealed"|"card"|"all", "limit": 100, "offset": 0, "search": "" }
    Returns array of items with store/ppt/cache prices.
    """
    from ppt_client import PPTClient
    from price_cache import PriceCache

    data = request.get_json(silent=True) or {}
    product_filter = data.get("filter", "all")
    limit = min(int(data.get("limit", 50)), 200)
    offset = int(data.get("offset", 0))
    search = data.get("search", "").strip()

    # Get inventory items that have tcgplayer IDs
    where = ["ipc.tcgplayer_id IS NOT NULL", "ipc.is_damaged = FALSE"]
    params = []

    if product_filter == "sealed":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%sealed%", "%booster%", "%etb%"])
    elif product_filter == "card":
        where.append("ipc.tags ILIKE %s")
        params.append("%slab%")

    if search:
        where.append("ipc.title ILIKE %s")
        params.append(f"%{search}%")

    sql = f"""
        SELECT ipc.title, ipc.tcgplayer_id, ipc.shopify_variant_id,
               ipc.shopify_price, ipc.shopify_qty, ipc.tags
        FROM inventory_product_cache ipc
        WHERE {' AND '.join(where)}
        ORDER BY ipc.title ASC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    items = db.query(sql, tuple(params))

    # Count total
    count_sql = f"""
        SELECT COUNT(*) as cnt FROM inventory_product_cache ipc
        WHERE {' AND '.join(where)}
    """
    total = db.query_one(count_sql, tuple(params[:-2]))["cnt"]

    # Initialize cache reader
    cache = PriceCache(db)

    results = []
    for item in items:
        tcg_id = item["tcgplayer_id"]
        row = {
            "title": item["title"],
            "tcgplayer_id": tcg_id,
            "variant_id": item["shopify_variant_id"],
            "store_price": float(item["shopify_price"]) if item["shopify_price"] else None,
            "qty": item["shopify_qty"],
            "cache": None,
            "ppt": None,
        }

        # Cache read (instant)
        try:
            # Try card first, then sealed
            cached = cache.get_card_by_tcgplayer_id(tcg_id)
            if not cached:
                cached = cache.get_sealed_product_by_tcgplayer_id(tcg_id)
            if cached:
                market = PriceProvider.extract_market_price(cached)
                row["cache"] = {
                    "market": float(market) if market else None,
                    "name": cached.get("name"),
                    "source": "scrydex_cache",
                }
        except Exception:
            pass

        results.append(row)

    return jsonify({
        "items": results,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


@app.route("/api/price-compare/batch/enrich", methods=["POST"])
def price_compare_enrich():
    """
    Enrich a single item with live PPT data. Called lazily from the UI
    to avoid hammering PPT for the whole list.
    Body: { "tcgplayer_id": 12345 }
    """
    from ppt_client import PPTClient

    data = request.get_json(silent=True) or {}
    tcg_id = data.get("tcgplayer_id")
    if not tcg_id:
        return jsonify({"error": "No tcgplayer_id"}), 400

    ppt_key = os.getenv("PPT_API_KEY", "")
    if not ppt_key:
        return jsonify({"error": "PPT not configured"}), 503

    try:
        ppt_direct = PPTClient(ppt_key)
        card = ppt_direct.get_card_by_tcgplayer_id(int(tcg_id))
        if card:
            market = PPTClient.extract_market_price(card)
            return jsonify({
                "market": float(market) if market else None,
                "name": card.get("name"),
                "source": "ppt_live",
            })
        sealed = ppt_direct.get_sealed_product_by_tcgplayer_id(int(tcg_id))
        if sealed:
            market = PPTClient.extract_market_price(sealed)
            return jsonify({
                "market": float(market) if market else None,
                "name": sealed.get("name"),
                "source": "ppt_live",
            })
        return jsonify({"market": None, "source": "ppt_live", "error": "not found"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _detect_game_from_tags(tags: str) -> str:
    """Detect game from tags string. Returns game identifier or None."""
    if not tags:
        return None
    t = tags.lower()
    if "lorcana" in t:
        return "lorcana"
    if "one piece" in t:
        return "onepiece"
    if "riftbound" in t:
        return "riftbound"
    if "magic: the gathering" in t or "magic the gathering" in t or ",mtg," in f",{t}," or t.startswith("mtg,") or t.endswith(",mtg"):
        return "mtg"
    if "pokemon" in t or "pokémon" in t:
        return "pokemon"
    return None


@app.route("/api/price-compare/unmatched", methods=["POST"])
def price_compare_unmatched():
    """
    Get store items that have no Scrydex cache match.
    Body: { "filter": "sealed"|"card"|"all", "game": "pokemon"|"mtg"|..., "limit": 50, "offset": 0, "search": "" }
    """
    data = request.get_json(silent=True) or {}
    product_filter = data.get("filter", "sealed")
    game_filter = (data.get("game") or "").strip().lower() or None
    limit = min(int(data.get("limit", 50)), 200)
    offset = int(data.get("offset", 0))
    search = data.get("search", "").strip()

    # Exclude slabs and accessories — they're not TCG products for linking purposes
    where = ["ipc.tcgplayer_id IS NOT NULL", "ipc.is_damaged = FALSE",
             "ipc.tags NOT ILIKE %s",
             "ipc.tags NOT ILIKE %s",
             "NOT EXISTS (SELECT 1 FROM scrydex_price_cache spc WHERE spc.tcgplayer_id = ipc.tcgplayer_id)"]
    params = ["%slab%", "%accessories%"]

    if product_filter == "sealed":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%sealed%", "%booster%", "%etb%", "%collection box%", "%tin%"])

    # Game filter via tag matching
    if game_filter == "pokemon":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%pokemon%", "%pokémon%"])
    elif game_filter == "mtg":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%magic: the gathering%", "%magic the gathering%"])
    elif game_filter == "lorcana":
        where.append("ipc.tags ILIKE %s")
        params.append("%lorcana%")
    elif game_filter == "onepiece":
        where.append("ipc.tags ILIKE %s")
        params.append("%one piece%")
    elif game_filter == "riftbound":
        where.append("ipc.tags ILIKE %s")
        params.append("%riftbound%")

    if search:
        where.append("ipc.title ILIKE %s")
        params.append(f"%{search}%")

    sql = f"""
        SELECT ipc.title, ipc.tcgplayer_id, ipc.shopify_price, ipc.shopify_qty, ipc.tags
        FROM inventory_product_cache ipc
        WHERE {' AND '.join(where)}
        ORDER BY ipc.title
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    items = db.query(sql, tuple(params))

    count_sql = f"""
        SELECT COUNT(*) as cnt FROM inventory_product_cache ipc
        WHERE {' AND '.join(where)}
    """
    total = db.query_one(count_sql, tuple(params[:-2]))["cnt"]

    return jsonify({
        "items": [{"title": i["title"], "tcgplayer_id": i["tcgplayer_id"],
                    "store_price": float(i["shopify_price"]) if i["shopify_price"] else None,
                    "qty": i["shopify_qty"],
                    "game": _detect_game_from_tags(i["tags"] or "")} for i in items],
        "total": total, "limit": limit, "offset": offset,
    })


_enrich_unmatched_jobs = {}  # {job_id: {status, progress, total, errors, started_at}}


def _enrich_unmatched_worker(job_id: str, tcg_ids: list[int]):
    """Background worker: enrich a batch of tcgplayer_ids via PPT, with rate-limit pauses."""
    import time as _time
    job = _enrich_unmatched_jobs[job_id]
    MCAP_KEYWORDS = ("miscellaneous cards & products", "miscellaneous")

    for i, tcg_id in enumerate(tcg_ids):
        if job.get("cancelled"):
            job["status"] = "cancelled"
            return

        # Wait out rate limits if we hit them
        wait_count = 0
        while ppt and ppt.should_throttle() and wait_count < 120:
            rate_info = ppt.get_rate_limit_info()
            retry_after = rate_info.get("retry_after") or 5
            job["status"] = f"rate_limited (waiting {retry_after}s)"
            _time.sleep(min(retry_after, 5))
            wait_count += 1

        job["status"] = "running"
        try:
            ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcg_id)
            if not ppt_item:
                ppt_item = ppt.get_card_by_tcgplayer_id(tcg_id)
            if ppt_item:
                set_name = ppt_item.get("setName", "") or ""
                ppt_name = ppt_item.get("name", "") or ""
                is_mcap = set_name.lower() in MCAP_KEYWORDS
                db.execute("""
                    INSERT INTO tcgplayer_set_lookup (tcgplayer_id, set_name, product_name, is_mcap, looked_up_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (tcgplayer_id) DO UPDATE SET
                        set_name = EXCLUDED.set_name,
                        product_name = EXCLUDED.product_name,
                        is_mcap = EXCLUDED.is_mcap,
                        looked_up_at = NOW()
                """, (tcg_id, set_name, ppt_name, is_mcap))
            else:
                db.execute("""
                    INSERT INTO tcgplayer_set_lookup (tcgplayer_id, set_name, product_name, is_mcap, looked_up_at)
                    VALUES (%s, NULL, NULL, FALSE, NOW())
                    ON CONFLICT (tcgplayer_id) DO UPDATE SET looked_up_at = NOW()
                """, (tcg_id,))
                job["not_found"] = job.get("not_found", 0) + 1
        except Exception as e:
            logger.warning(f"Unmatched enrich failed for tcg={tcg_id}: {e}")
            job["errors"] = job.get("errors", 0) + 1

        job["progress"] = i + 1

    job["status"] = "complete"


@app.route("/api/price-compare/enrich-unmatched-start", methods=["POST"])
def enrich_unmatched_start():
    """Start a background job to PPT-enrich all unmatched items.
    Body: { "filter": "sealed"|"all", "game": "pokemon"|... } — same filters as unmatched endpoint.
    """
    if not ppt:
        return jsonify({"error": "PPT not configured"}), 503

    data = request.get_json(silent=True) or {}
    product_filter = data.get("filter", "sealed")
    game_filter = (data.get("game") or "").strip().lower() or None

    where = ["ipc.tcgplayer_id IS NOT NULL", "ipc.is_damaged = FALSE",
             "ipc.tags NOT ILIKE %s", "ipc.tags NOT ILIKE %s",
             "NOT EXISTS (SELECT 1 FROM scrydex_price_cache spc WHERE spc.tcgplayer_id = ipc.tcgplayer_id)",
             "NOT EXISTS (SELECT 1 FROM tcgplayer_set_lookup tsl WHERE tsl.tcgplayer_id = ipc.tcgplayer_id)"]
    params = ["%slab%", "%accessories%"]

    if product_filter == "sealed":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%sealed%", "%booster%", "%etb%", "%collection box%", "%tin%"])

    if game_filter == "pokemon":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%pokemon%", "%pokémon%"])
    elif game_filter == "mtg":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%magic: the gathering%", "%magic the gathering%"])
    elif game_filter == "lorcana":
        where.append("ipc.tags ILIKE %s")
        params.append("%lorcana%")
    elif game_filter == "onepiece":
        where.append("ipc.tags ILIKE %s")
        params.append("%one piece%")
    elif game_filter == "riftbound":
        where.append("ipc.tags ILIKE %s")
        params.append("%riftbound%")

    rows = db.query(f"""
        SELECT DISTINCT ipc.tcgplayer_id FROM inventory_product_cache ipc
        WHERE {' AND '.join(where)}
    """, tuple(params))
    tcg_ids = [r["tcgplayer_id"] for r in rows]

    if not tcg_ids:
        return jsonify({"job_id": None, "status": "complete", "total": 0,
                        "message": "Nothing to enrich — all items already looked up"})

    import uuid as _uuid, threading as _threading
    job_id = str(_uuid.uuid4())
    _enrich_unmatched_jobs[job_id] = {
        "status": "running", "progress": 0, "total": len(tcg_ids),
        "errors": 0, "not_found": 0, "cancelled": False,
    }

    thread = _threading.Thread(target=_enrich_unmatched_worker, args=(job_id, tcg_ids), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id, "status": "running", "total": len(tcg_ids)})


@app.route("/api/price-compare/enrich-unmatched-job/<job_id>")
def enrich_unmatched_job(job_id):
    """Poll status of a background enrichment job."""
    job = _enrich_unmatched_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)


@app.route("/api/price-compare/enrich-unmatched-cancel/<job_id>", methods=["POST"])
def enrich_unmatched_cancel(job_id):
    """Cancel a running background enrichment job."""
    job = _enrich_unmatched_jobs.get(job_id)
    if job:
        job["cancelled"] = True
    return jsonify({"ok": True})


@app.route("/api/price-compare/export-unmatched", methods=["POST"])
def export_unmatched():
    """
    Export unmatched items as CSV, with PPT-enriched set info and MCAP flag.
    First run will hit PPT for items without cached set info (slow).
    Subsequent runs reuse cached data (fast).

    Body: { "game": "pokemon"|..., "filter": "sealed"|"all", "enrich_limit": 200 }
    """
    from flask import Response
    import csv
    from io import StringIO

    data = request.get_json(silent=True) or {}
    game_filter = (data.get("game") or "").strip().lower() or None
    product_filter = data.get("filter", "sealed")
    enrich_limit = min(int(data.get("enrich_limit", 200)), 500)

    # Get all unmatched items (same filtering as unmatched endpoint)
    where = ["ipc.tcgplayer_id IS NOT NULL", "ipc.is_damaged = FALSE",
             "ipc.tags NOT ILIKE %s", "ipc.tags NOT ILIKE %s",
             "NOT EXISTS (SELECT 1 FROM scrydex_price_cache spc WHERE spc.tcgplayer_id = ipc.tcgplayer_id)"]
    params = ["%slab%", "%accessories%"]

    if product_filter == "sealed":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%sealed%", "%booster%", "%etb%", "%collection box%", "%tin%"])

    if game_filter == "pokemon":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%pokemon%", "%pokémon%"])
    elif game_filter == "mtg":
        where.append("(ipc.tags ILIKE %s OR ipc.tags ILIKE %s)")
        params.extend(["%magic: the gathering%", "%magic the gathering%"])
    elif game_filter == "lorcana":
        where.append("ipc.tags ILIKE %s")
        params.append("%lorcana%")
    elif game_filter == "onepiece":
        where.append("ipc.tags ILIKE %s")
        params.append("%one piece%")
    elif game_filter == "riftbound":
        where.append("ipc.tags ILIKE %s")
        params.append("%riftbound%")

    items = db.query(f"""
        SELECT ipc.title, ipc.tcgplayer_id, ipc.shopify_price, ipc.shopify_qty, ipc.tags
        FROM inventory_product_cache ipc
        WHERE {' AND '.join(where)}
        ORDER BY ipc.title
    """, tuple(params))

    # Enrich with cached PPT set data
    tcg_ids = [i["tcgplayer_id"] for i in items]
    cached = {}
    if tcg_ids:
        rows = db.query(f"""
            SELECT tcgplayer_id, set_name, product_name, is_mcap
            FROM tcgplayer_set_lookup
            WHERE tcgplayer_id IN ({",".join(["%s"] * len(tcg_ids))})
        """, tuple(tcg_ids))
        cached = {r["tcgplayer_id"]: r for r in rows}

    # Find items without cached set info — enrich up to enrich_limit via PPT
    uncached = [i for i in items if i["tcgplayer_id"] not in cached]
    enriched_count = 0
    enrich_errors = 0
    if ppt and uncached:
        MCAP_KEYWORDS = ("miscellaneous cards & products", "miscellaneous")
        for item in uncached[:enrich_limit]:
            if ppt.should_throttle():
                break
            tcg_id = item["tcgplayer_id"]
            try:
                # Try sealed first (faster for our use case)
                ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcg_id)
                if not ppt_item:
                    ppt_item = ppt.get_card_by_tcgplayer_id(tcg_id)
                if ppt_item:
                    set_name = ppt_item.get("setName", "") or ""
                    ppt_name = ppt_item.get("name", "") or ""
                    is_mcap = set_name.lower() in MCAP_KEYWORDS
                    db.execute("""
                        INSERT INTO tcgplayer_set_lookup (tcgplayer_id, set_name, product_name, is_mcap, looked_up_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (tcgplayer_id) DO UPDATE SET
                            set_name = EXCLUDED.set_name,
                            product_name = EXCLUDED.product_name,
                            is_mcap = EXCLUDED.is_mcap,
                            looked_up_at = NOW()
                    """, (tcg_id, set_name, ppt_name, is_mcap))
                    cached[tcg_id] = {"tcgplayer_id": tcg_id, "set_name": set_name,
                                       "product_name": ppt_name, "is_mcap": is_mcap}
                    enriched_count += 1
                else:
                    # Not found on PPT — mark so we don't retry
                    db.execute("""
                        INSERT INTO tcgplayer_set_lookup (tcgplayer_id, set_name, product_name, is_mcap, looked_up_at)
                        VALUES (%s, NULL, NULL, FALSE, NOW())
                        ON CONFLICT (tcgplayer_id) DO UPDATE SET looked_up_at = NOW()
                    """, (tcg_id,))
                    cached[tcg_id] = {"tcgplayer_id": tcg_id, "set_name": None, "product_name": None, "is_mcap": False}
            except Exception as e:
                logger.warning(f"PPT enrich failed for {tcg_id}: {e}")
                enrich_errors += 1

    # Build CSV
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["tcgplayer_id", "game", "set_name", "is_mcap", "product_name_store",
                     "product_name_ppt", "store_price", "qty", "tags"])
    for item in items:
        c = cached.get(item["tcgplayer_id"], {})
        game = _detect_game_from_tags(item["tags"] or "") or ""
        writer.writerow([
            item["tcgplayer_id"],
            game,
            c.get("set_name") or "",
            "YES" if c.get("is_mcap") else "",
            item["title"] or "",
            c.get("product_name") or "",
            f"{float(item['shopify_price']):.2f}" if item["shopify_price"] else "",
            item["shopify_qty"] or 0,
            item["tags"] or "",
        ])

    csv_data = buf.getvalue()
    filename = f"unmatched-{game_filter or 'all'}-{product_filter}.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Total-Items": str(len(items)),
            "X-Enriched-This-Run": str(enriched_count),
            "X-Already-Cached": str(len(items) - len(uncached)),
            "X-Pending-Enrich": str(max(0, len(uncached) - enrich_limit)),
        }
    )


@app.route("/api/price-compare/scrydex-search", methods=["POST"])
def scrydex_search_for_link():
    """
    Search Scrydex cache for a product to link to.
    Body: { "query": "...", "type": "sealed"|"card"|"all", "game": "pokemon"|... }
    """
    data = request.get_json(silent=True) or {}
    query = data.get("query", "").strip()
    product_type = data.get("type", "all")
    game_filter = (data.get("game") or "").strip().lower() or None
    if not query:
        return jsonify({"results": []})

    where = ["product_name ILIKE %s"]
    params = [f"%{query}%"]

    if product_type != "all":
        where.append("product_type = %s")
        params.append(product_type)

    if game_filter:
        where.append("game = %s")
        params.append(game_filter)

    # Get each variant as a separate row so PC ETBs show both normal + pokemonCenter
    results = db.query(f"""
        SELECT DISTINCT ON (scrydex_id, variant)
            scrydex_id, product_name, expansion_name, product_type, variant,
            market_price, low_price, image_medium, tcgplayer_id, game
        FROM scrydex_price_cache
        WHERE {' AND '.join(where)}
        AND condition IN ('NM', 'U') AND price_type = 'raw'
        ORDER BY scrydex_id, variant
        LIMIT 30
    """, tuple(params))

    variant_labels = {
        "normal": "Normal", "pokemonCenter": "Pokemon Center",
        "holofoil": "Holofoil", "reverseHolofoil": "Reverse Holofoil",
    }

    return jsonify({
        "results": [{
            "scrydex_id": r["scrydex_id"],
            "variant": r["variant"],
            "variant_label": variant_labels.get(r["variant"], r["variant"]),
            "name": r["product_name"],
            "set": r["expansion_name"],
            "type": r["product_type"],
            "game": r["game"],
            "market": float(r["market_price"]) if r["market_price"] else None,
            "low": float(r["low_price"]) if r["low_price"] else None,
            "image": r["image_medium"],
            "already_linked": r["tcgplayer_id"] is not None,
        } for r in results]
    })


@app.route("/api/price-compare/link", methods=["POST"])
def scrydex_link():
    """
    Manually link a store item (tcgplayer_id) to a Scrydex product + variant.
    Body: { "tcgplayer_id": 12345, "scrydex_id": "sv8-s2", "variant": "pokemonCenter" }
    """
    data = request.get_json(silent=True) or {}
    tcg_id = data.get("tcgplayer_id")
    scrydex_id = data.get("scrydex_id")
    variant = data.get("variant", "normal")
    if not tcg_id or not scrydex_id:
        return jsonify({"error": "Need tcgplayer_id and scrydex_id"}), 400

    tcg_id = int(tcg_id)

    # Update cache rows for this specific variant — unconditional so re-linking works
    # (variant-specific: normal and pokemonCenter get different tcgplayer_ids)
    db.execute(
        "UPDATE scrydex_price_cache SET tcgplayer_id = %s WHERE scrydex_id = %s AND variant = %s",
        (tcg_id, scrydex_id, variant)
    )

    # Add to mapping table (scrydex_id -> tcgplayer_id)
    # Note: for variant-specific links (PC ETB vs normal ETB), both map to the
    # same scrydex_id but different tcgplayer_ids. The mapping table stores
    # the primary link; variant-specific pricing comes from the cache rows.
    try:
        db.execute("""
            INSERT INTO scrydex_tcg_map (scrydex_id, tcgplayer_id, product_type, updated_at)
            VALUES (%s, %s, 'sealed', NOW())
            ON CONFLICT (scrydex_id) DO NOTHING
        """, (scrydex_id, tcg_id))
    except Exception:
        pass

    # Get the linked price from the specific variant to confirm
    row = db.query_one("""
        SELECT product_name, market_price FROM scrydex_price_cache
        WHERE scrydex_id = %s AND variant = %s AND condition IN ('NM', 'U') AND price_type = 'raw'
        LIMIT 1
    """, (scrydex_id, variant))

    return jsonify({
        "ok": True,
        "scrydex_id": scrydex_id,
        "variant": variant,
        "tcgplayer_id": tcg_id,
        "name": row["product_name"] if row else None,
        "market": float(row["market_price"]) if row and row["market_price"] else None,
    })


@app.route("/api/enrich/preview", methods=["POST"])
def enrich_preview():
    """
    Preview what tags/era/weight would be inferred for a product.
    Does NOT call Shopify. Safe to call at any time.

    Body: { "product_name": "...", "set_name": "...", "tcgplayer_id": "..." }
    """
    data = request.get_json() or {}
    name = data.get("product_name", "")
    set_name = data.get("set_name", "")
    return jsonify({
        "product_name": name,
        "set_name": set_name,
        "tags": enrichment.infer_tags(name, set_name),
        "era": enrichment.infer_era(name, set_name),
        "weight_oz": enrichment.infer_weight_oz(name),
    })


@app.route("/api/enrich/product", methods=["POST"])
def enrich_existing_product():
    """
    Enrich an existing Shopify product using a PPT item.
    Useful for enriching products already in Shopify that are missing tags/images/etc.

    Body: {
        "product_gid": "gid://shopify/Product/...",
        "tcgplayer_id": 12345,         (used to fetch PPT item)
        "offer_price": 9.99            (optional, sets COGS)
    }
    """
    data = request.get_json() or {}
    product_gid = data.get("product_gid")
    tcgplayer_id = data.get("tcgplayer_id")
    offer_price = data.get("offer_price")

    if not product_gid or not tcgplayer_id:
        return jsonify({"error": "product_gid and tcgplayer_id required"}), 400

    ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
    if not ppt_item:
        return jsonify({"error": f"PPT item not found for tcgplayer_id {tcgplayer_id}"}), 404

    summary = enrichment.enrich_product(product_gid, ppt_item, offer_price=offer_price)
    return jsonify(summary)


@app.route("/api/enrich/create-listing", methods=["POST"])
def create_listing():
    """
    Create a new DRAFT Shopify listing from a PPT item and fully enrich it.

    Body: {
        "tcgplayer_id": 12345,
        "price": 29.99,           (Shopify listing price — defaults to PPT market price)
        "offer_price": 18.00,     (optional, sets COGS)
        "quantity": 0             (optional, default 0 — set >0 to stock the listing)
    }
    """
    data = request.get_json() or {}
    tcgplayer_id = data.get("tcgplayer_id")
    offer_price = data.get("offer_price")
    quantity = int(data.get("quantity", 0))

    if not tcgplayer_id:
        return jsonify({"error": "tcgplayer_id required"}), 400

    ppt_item = ppt.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
    if not ppt_item:
        return jsonify({"error": f"PPT item not found for tcgplayer_id {tcgplayer_id}"}), 404

    # Default price to PPT market price if not provided
    price = data.get("price") or ppt_item.get("marketPrice") or ppt_item.get("unopenedPrice") or 0
    if not price:
        return jsonify({"error": "No price available — provide price or ensure PPT has market data"}), 400

    summary = enrichment.create_draft_listing(ppt_item, price=float(price),
                                              offer_price=offer_price,
                                              quantity=quantity)
    return jsonify(summary)


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "ingest",
        "shopify": shopify is not None,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=True)
