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
from flask import Flask, render_template, request, jsonify, redirect, make_response

import db
import ingest
from shopify_client import ShopifyClient, ShopifyError
from ppt_client import PPTClient
import product_enrichment as enrichment
from cache_manager import CacheManager
try:
    import psa_client
    from psa_client import PSAQuotaHit
except ImportError:
    psa_client = None
    PSAQuotaHit = Exception
try:
    from storage import assign_bins, release_bins, _canonical_card_type
except ImportError as e:
    logger.error(f"storage import failed: {e} — raw card push will not work")
    assign_bins = release_bins = _canonical_card_type = None
try:
    from barcode_gen import generate_barcode_id, generate_barcode_image
except ImportError as e:
    logger.error(f"barcode_gen import failed: {e} — raw card push will not work")
    generate_barcode_id = generate_barcode_image = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

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
    if request.path.startswith("/static"):
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

ppt = PPTClient(os.getenv("PPT_API_KEY", ""))


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
    if status == "completed":
        sessions = ingest.list_sessions_completed(limit=limit, days=int(days) if days else None)
    else:
        sessions = ingest.list_sessions_pending(limit=limit)
    return jsonify([_serialize(s) for s in sessions])


@app.route("/api/ingest/session/<session_id>")
def get_session(session_id):
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    items = ingest.get_session_items(session_id)

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


# ═══════════════════════════════════════════════════════════════════
# PPT SEARCH (for break-down modal)
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/ppt/search-sealed", methods=["POST"])
def ppt_search_sealed():
    data = request.get_json(silent=True) or {}
    q = data.get("query", "").strip()
    if not q:
        return jsonify({"error": "No query"}), 400
    try:
        results = ppt.search_sealed_products(q, limit=10)
        # Normalize tcgplayer_id field — PPT may return it as tcgplayerId, tcgPlayerId, etc.
        for r in results:
            if not r.get("tcgplayer_id"):
                tcg_id = r.get("tcgplayerId") or r.get("tcgPlayerId") or r.get("tcgplayer_id") or r.get("id")
                if tcg_id:
                    r["tcgplayer_id"] = int(tcg_id)
        return jsonify({"results": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        results.append({
            "product_name": item.get("product_name"),
            "quantity": item.get("quantity", 1),
            "action": "would_ingest_raw",
            "new_title": item.get("product_name"),
            "listing_price": float(item.get("market_price", 0)),
            "note": "Barcode generated + assigned to bin — no Shopify listing",
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
    if session["status"] not in ("received", "partially_ingested"):
        return jsonify({"error": f"Session must be 'received' or 'partially_ingested' (currently: {session['status']})"}), 400

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

        # Raw cards
        for item in raw_items:
            item_dict = dict(item)
            item_dict["session_id"] = session_id
            try:
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

        # Determine final session status
        all_items_after = ingest.get_session_items(session_id)
        remaining_unpushed = [i for i in all_items_after
                              if i.get("item_status") in ("good", "damaged")
                              and i.get("is_mapped") and not i.get("pushed_at")]

        partially_ingested = False
        if not errors:
            if remaining_unpushed:
                db.execute(
                    "UPDATE intake_sessions SET status = 'partially_ingested' WHERE id = %s",
                    (session_id,)
                )
                partially_ingested = True
            else:
                ingest.mark_session_ingested(session_id)

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
    condition = item.get("condition", "NM")
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
        "product_name": card_name,
        "quantity":    qty,
        "barcodes":    results,
        "bins":        [a["bin_label"] for a in assignments],
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
            # Fallback: skeleton listing if PPT lookup fails
            logger.warning(f"PPT lookup failed for {tcg_id} ({product_name}) — creating skeleton")
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
            return entry

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
            logger.exception(f"Enriched listing creation failed for {tcg_id} — falling back to skeleton")
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
            # No normal product to duplicate — create fresh damaged listing
            product_name = item.get("product_name", "Unknown Product")
            damaged_title = f"{product_name} [DAMAGED]"
            market_price = float(item.get("market_price", 0))
            new_product = shopify.create_product(
                title=damaged_title,
                price=market_price,
                tags=["auto-created", "ingest", "damaged"],
                tcgplayer_id=tcg_id if tcg_id else None,
                quantity=qty,
            )
            entry.update(
                action="created_damaged_listing",
                new_title=damaged_title,
            )

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

    if not cert_number:
        return jsonify({"error": "cert_number is required"}), 400

    # Load item
    item = db.query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        return jsonify({"error": "Item not found"}), 404
    if item.get("pushed_at"):
        return jsonify({"error": "Item already pushed"}), 400

    grade_company = (item.get("grade_company") or "PSA").upper()
    grade_value   = item.get("grade_value") or "9"
    tcg_id        = item.get("tcgplayer_id")
    price         = float(item.get("offer_price") or item.get("market_price") or 0)

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
            shopify_domain=shopify.domain,
            shopify_token=shopify.token,
            db=db,
        )
    except PSAQuotaHit as e:
        return jsonify({"error": f"PSA API quota hit — try again tomorrow: {e}"}), 429
    except Exception as e:
        logger.exception(f"push_graded_item failed for item {item_id}: {e}")
        return jsonify({"error": str(e)}), 500

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


@app.route("/api/ingest/session/<session_id>/push-raw", methods=["POST"])
def push_raw_items(session_id):
    """
    Push all unmapped raw (non-graded) items in a session to internal inventory.
    Generates barcodes, assigns bins, inserts raw_cards rows.

    POST body (optional): { "item_ids": [...] }  — subset of items to push

    Returns list of barcode results with PNG data (base64) for printing.
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
        try:
            r = _push_raw_item(item_dict)
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


# ═══════════════════════════════════════════════════════════════════
# BREAKDOWN CACHE API  (multi-variant)
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/breakdown-cache")
def list_breakdown_caches():
    """List all cached products (summary only, no components)."""
    rows = ingest.list_breakdown_cache()
    return jsonify({"caches": [_serialize(r) for r in rows], "count": len(rows)})


@app.route("/api/breakdown-cache/<int:tcgplayer_id>")
def get_breakdown_cache(tcgplayer_id):
    """Get full breakdown record (all variants + components) for a product."""
    result = ingest.get_breakdown_cache(tcgplayer_id)
    if not result:
        return jsonify({"found": False, "cache": None})
    return jsonify({"found": True, "cache": _serialize(result)})


@app.route("/api/breakdown-cache/<int:tcgplayer_id>", methods=["DELETE"])
def delete_breakdown_cache(tcgplayer_id):
    """Delete entire breakdown record (all variants) for a product."""
    deleted = ingest.delete_breakdown_cache(tcgplayer_id)
    return jsonify({"success": deleted})


@app.route("/api/breakdown-cache/<int:tcgplayer_id>/variant", methods=["POST"])
def save_variant(tcgplayer_id):
    """
    Create or update a named variant.
    Body: {product_name, variant_name, components, notes?, variant_id?}
      variant_id: omit to create new; supply to update existing variant in-place.
    """
    data = request.get_json(silent=True) or {}
    product_name = data.get("product_name", "Unknown")
    variant_name = data.get("variant_name", "Standard")
    components = data.get("components", [])
    notes = data.get("notes")
    variant_id = data.get("variant_id")

    if not components:
        return jsonify({"error": "components required"}), 400
    try:
        result = ingest.save_variant(
            tcgplayer_id=tcgplayer_id,
            product_name=product_name,
            variant_name=variant_name,
            components=components,
            notes=notes,
            variant_id=variant_id,
        )
        return jsonify({"success": True, "cache": _serialize(result)})
    except Exception as e:
        logger.exception(f"Failed to save variant for {tcgplayer_id}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/breakdown-cache/variant/<variant_id>", methods=["DELETE"])
def delete_variant(variant_id):
    """Delete a single variant. Deletes parent too if it was the last one."""
    result = ingest.delete_variant(variant_id)
    return jsonify({"success": True, "cache": _serialize(result)})


@app.route("/api/breakdown-cache/batch", methods=["POST"])
def breakdown_cache_batch():
    """Batch-fetch breakdown summaries for multiple tcgplayer_ids (used by intake)."""
    data = request.get_json(silent=True) or {}
    tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
    if not tcg_ids:
        return jsonify({"summaries": {}})
    summaries = ingest.get_breakdown_summary_for_items(tcg_ids, ppt=ppt)
    return jsonify({"summaries": _serialize(summaries)})


@app.route("/api/store-prices", methods=["POST"])
def get_store_prices():
    """
    Look up inventory_product_cache prices for a list of tcgplayer_ids.
    Body: {tcgplayer_ids: [int, ...]}
    Returns: {tcgplayer_id: {shopify_price, shopify_qty, handle, title}}
    """
    data = request.get_json(silent=True) or {}
    tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
    if not tcg_ids:
        return jsonify({"prices": {}})
    ph = ",".join(["%s"] * len(tcg_ids))
    rows = db.query(
        f"SELECT tcgplayer_id, shopify_price, shopify_qty, handle, title FROM inventory_product_cache WHERE tcgplayer_id IN ({ph}) AND is_damaged = FALSE",
        tuple(tcg_ids)
    )
    prices = {r["tcgplayer_id"]: dict(r) for r in rows}
    return jsonify({"prices": _serialize(prices)})


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


# Pre-warm rembg model in background thread so it's ready before first listing creation
import threading
threading.Thread(target=enrichment._prewarm_rembg, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=True)
