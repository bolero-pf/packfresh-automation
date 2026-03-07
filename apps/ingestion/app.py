"""
Ingest Service — ingest.pack-fresh.com
Warehouse team dashboard for breaking down sealed products and pushing inventory to Shopify.

Separate from offers.pack-fresh.com — reads the same DB (intake_sessions, intake_items,
shopify_product_cache) but serves a different audience (warehouse vs buying team).
"""

import os
import json
import logging
import hashlib
import secrets
from datetime import datetime, date
from decimal import Decimal
from flask import Flask, render_template, request, jsonify, redirect, make_response

import db
import ingest
from shopify_client import ShopifyClient, ShopifyError
from ppt_client import PPTClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", secrets.token_hex(32))

# ─── Password gate ───────────────────────────────────────────────
INGEST_PASSWORD = os.getenv("INGEST_PASSWORD", "")

def _check_auth():
    """Returns True if auth is disabled or user has valid session cookie."""
    if not INGEST_PASSWORD:
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

@app.before_request
def require_auth():
    """Gate all routes behind password if INGEST_PASSWORD is set."""
    if not INGEST_PASSWORD:
        return None
    # Allow login page, health check, and API through
    if request.path in ("/login", "/health"):
        return None
    if request.path.startswith("/static"):
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
    sessions = ingest.list_sessions(limit=int(request.args.get("limit", 50)))
    return jsonify([_serialize(s) for s in sessions])


@app.route("/api/ingest/session/<session_id>")
def get_session(session_id):
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    items = ingest.get_session_items(session_id)
    return jsonify({
        "session": _serialize(session),
        "items": [_serialize(i) for i in items],
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
    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404

    items = ingest.get_session_items(session_id)
    active = [i for i in items if i.get("item_status") in ("good", "damaged") and i.get("is_mapped")]

    if not active:
        return jsonify({"error": "No active mapped items to push"}), 400

    tcg_ids = list(set(i["tcgplayer_id"] for i in active if i.get("tcgplayer_id")))
    normal_cache, damaged_cache = ingest.build_cache_maps(tcg_ids)

    results = []

    # Consolidate by (tcg_id, is_damaged)
    consolidated = {}
    for item in active:
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
        "would_increment": sum(1 for r in results if r.get("action") == "would_increment"),
        "would_create_damaged": sum(1 for r in results if r.get("action") == "would_create_damaged"),
        "would_create_listing": sum(1 for r in results if r.get("action") == "would_create_listing"),
    })



@app.route("/api/ingest/session/<session_id>/push-live", methods=["POST"])
def push_session_live(session_id):
    """Push a received session to Shopify."""
    if not shopify:
        return jsonify({"error": "Shopify not configured"}), 503

    session = ingest.get_session(session_id)
    if not session:
        return jsonify({"error": "Session not found"}), 404
    if session["status"] != "received":
        return jsonify({"error": f"Session must be 'received' (currently: {session['status']})"}), 400

    items = ingest.get_session_items(session_id)
    # Only push good/damaged mapped items (not broken_down, missing, rejected)
    # Skip items already pushed (have pushed_at timestamp) — allows retry of just failed items
    active = [i for i in items if i.get("item_status") in ("good", "damaged")
              and i.get("is_mapped") and not i.get("pushed_at")]

    if not active:
        # If nothing to push but there ARE pushed items, everything succeeded — mark ingested
        already_pushed = [i for i in items if i.get("pushed_at")]
        if already_pushed:
            ingest.mark_session_ingested(session_id)
            return jsonify({"success": True, "results": [], "errors": [],
                            "total": 0, "ingested": True,
                            "message": "All items already pushed. Session marked ingested."})
        return jsonify({"error": "No active mapped items to push"}), 400

    # Build cache maps
    tcg_ids = list(set(i["tcgplayer_id"] for i in active if i.get("tcgplayer_id")))
    normal_cache, damaged_cache = ingest.build_cache_maps(tcg_ids)

    results = []
    errors = []

    # ── Consolidate items by (tcg_id, is_damaged) to minimize Shopify API calls ──
    consolidated = {}  # (tcg_id, is_damaged) -> {total_qty, items[], ...}
    for item in active:
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
        item_names = ", ".join(set(i.get("product_name", "") for i in group["items"]))
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
            continue

        if entry.get("action") == "error":
            errors.append(entry)
        else:
            results.append(entry)
            # Mark successfully pushed items so retry skips them
            for pushed_item in group["items"]:
                db.execute("UPDATE intake_items SET pushed_at = CURRENT_TIMESTAMP WHERE id = %s",
                           (pushed_item["id"],))

    # Mark session as ingested if no errors
    if not errors:
        ingest.mark_session_ingested(session_id)

    return jsonify({
        "success": len(errors) == 0,
        "results": results,
        "errors": errors,
        "total": len(active),
        "incremented": sum(1 for r in results if r.get("action") == "inventory_incremented"),
        "created_damaged": sum(1 for r in results if r.get("action") == "created_damaged_listing"),
        "created_listing": sum(1 for r in results if r.get("action") == "created_listing"),
        "error_count": len(errors),
        "ingested": len(errors) == 0,
        "can_retry": len(errors) > 0,
    })


def _push_normal_item(entry: dict, tcg_id: int, qty: int, item: dict, normal_cache: dict) -> dict:
    """Push a normal (non-damaged) item: find variant and increment, or create new listing."""
    cache_row = normal_cache.get(tcg_id)
    if cache_row and cache_row.get("shopify_variant_id"):
        inv_item_id = shopify.get_inventory_item_id(cache_row["shopify_variant_id"])
        if inv_item_id:
            shopify.adjust_inventory(inv_item_id, qty, reason="received")
            entry["action"] = "inventory_incremented"
            entry["shopify_variant_id"] = cache_row["shopify_variant_id"]
        else:
            entry.update(action="error", error="Could not find inventory item ID")
    else:
        # No Shopify match — create a new product
        product_name = item.get("product_name", "Unknown Product")
        market_price = float(item.get("market_price", 0))
        new_product = shopify.create_product(
            title=product_name,
            price=market_price,
            tags=["auto-created", "ingest"],
            tcgplayer_id=tcg_id if tcg_id else None,
            quantity=qty,
        )
        entry["action"] = "created_listing"
        entry["new_product_id"] = new_product["id"]
        entry["new_title"] = product_name
        entry["listing_price"] = market_price
    return entry


def _push_damaged_item(entry: dict, tcg_id: int, qty: int, item: dict,
                       normal_cache: dict, damaged_cache: dict) -> dict:
    """Push a damaged item: increment existing damaged listing or create one."""
    cache_row = damaged_cache.get(tcg_id)

    if cache_row and cache_row.get("shopify_variant_id"):
        # Damaged listing exists — increment inventory
        inv_item_id = shopify.get_inventory_item_id(cache_row["shopify_variant_id"])
        if inv_item_id:
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
    summaries = ingest.get_breakdown_summary_for_items(tcg_ids)
    return jsonify({"summaries": _serialize(summaries)})


@app.route("/api/store-prices", methods=["POST"])
def get_store_prices():
    """
    Look up shopify_product_cache prices for a list of tcgplayer_ids.
    Body: {tcgplayer_ids: [int, ...]}
    Returns: {tcgplayer_id: {shopify_price, shopify_qty, handle, title}}
    """
    data = request.get_json(silent=True) or {}
    tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
    if not tcg_ids:
        return jsonify({"prices": {}})
    ph = ",".join(["%s"] * len(tcg_ids))
    rows = db.query(
        f"SELECT tcgplayer_id, shopify_price, shopify_qty, handle, title FROM shopify_product_cache WHERE tcgplayer_id IN ({ph}) AND is_damaged = FALSE",
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

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "ingest",
        "shopify": shopify is not None,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=True)
