"""
card-manager — admin.pack-fresh.com (or cards-admin.pack-fresh.com)
Staff panel for processing holds, pulling cards, accepting/rejecting,
creating Shopify draft listings at POS, and returning cards to storage.

Env vars needed:
  DATABASE_URL
  SHOPIFY_STORE   (e.g. pack-fresh.myshopify.com)
  SHOPIFY_TOKEN   (Admin API token)
  SHOPIFY_VERSION (optional, defaults to 2025-01)
"""

import os
import logging
import requests as _requests
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify

import db
from storage import assign_bins
from barcode_gen import generate_barcode_image
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()

SHOPIFY_STORE   = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN   = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = os.environ.get("SHOPIFY_VERSION", "2025-01")


def _shopify(method, path, **kwargs):
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    r = _requests.request(method, url, headers=headers, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json()


def _ser(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
            out[k] = float(v)
        else:
            out[k] = v
    return out


from auth import register_auth_hooks
register_auth_hooks(app)  # any authenticated user


# ═══════════════════════════════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ═══════════════════════════════════════════════════════════════════════════════
# Hold Queue API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/holds")
def list_holds():
    """All active holds ordered by created_at."""
    status_filter = request.args.get("status", "active")
    if status_filter == "active":
        rows = db.query("""
            SELECT h.*,
                   COUNT(hi.id) AS total_items,
                   COUNT(hi.id) FILTER (WHERE hi.status = 'PULLED') AS pulled_items,
                   COUNT(hi.id) FILTER (WHERE hi.status = 'REQUESTED') AS pending_items
            FROM holds h
            LEFT JOIN hold_items hi ON hi.hold_id = h.id
            WHERE h.status IN ('PENDING','PULLING','READY')
              AND NOT (h.cohort = 'champion' AND h.checkout_status = 'pending')
            GROUP BY h.id
            ORDER BY h.created_at DESC
        """)
    else:
        rows = db.query("""
            SELECT h.*,
                   COUNT(hi.id) AS total_items
            FROM holds h
            LEFT JOIN hold_items hi ON hi.hold_id = h.id
            WHERE h.status = %s
            GROUP BY h.id
            ORDER BY h.created_at DESC
            LIMIT 50
        """, (status_filter.upper(),))

    return jsonify({"holds": [_ser(dict(r)) for r in rows]})


@app.route("/api/holds/<hold_id>")
def get_hold(hold_id):
    """Hold detail with optimized pull list."""
    hold = db.query_one("SELECT * FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404

    items = db.query("""
        SELECT hi.id AS hold_item_id, hi.status AS item_status,
               hi.barcode, hi.pulled_at, hi.resolved_at,
               hi.shopify_product_id,
               rc.id AS card_id, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.tcgplayer_id,
               rc.image_url, rc.state AS card_state,
               sl.bin_label
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE hi.hold_id = %s
        ORDER BY sl.bin_label NULLS LAST, rc.card_name
    """, (hold_id,))

    # Build optimized pull list:
    # Group by (tcgplayer_id, condition), find bin with most copies, pull from there
    pull_groups = {}
    for item in items:
        key = (item["tcgplayer_id"], item["condition"])
        if key not in pull_groups:
            pull_groups[key] = {
                "card_name":    item["card_name"],
                "set_name":     item["set_name"],
                "card_number":  item["card_number"],
                "condition":    item["condition"],
                "tcgplayer_id": item["tcgplayer_id"],
                "image_url":    item["image_url"],
                "items":        [],
            }
        pull_groups[key]["items"].append(dict(item))

    # For each group, find the best bin (most copies available)
    pull_list = []
    for key, group in pull_groups.items():
        tcg_id, condition = key
        qty_needed = len(group["items"])

        # Find bins containing this card ordered by count DESC
        best_bins = db.query("""
            SELECT sl.bin_label, sl.id AS bin_id,
                   COUNT(*) AS available_here
            FROM raw_cards rc
            JOIN storage_locations sl ON rc.bin_id = sl.id
            WHERE rc.tcgplayer_id = %s
              AND rc.condition = %s
            GROUP BY sl.bin_label, sl.id
            ORDER BY available_here DESC
        """, (tcg_id, condition)) if tcg_id else []

        group["best_bins"]  = [{"bin_label": b["bin_label"], "count": b["available_here"]} for b in best_bins]
        group["qty_needed"] = qty_needed

        # Collect all valid barcodes for this (tcgplayer_id, condition) — any can be scanned
        valid = db.query("""
            SELECT barcode FROM raw_cards
            WHERE tcgplayer_id = %s AND condition = %s AND state = 'STORED'
        """, (tcg_id, condition)) if tcg_id else []
        group["valid_barcodes"] = [v["barcode"] for v in valid]

        pull_list.append(group)

    return jsonify({
        "hold":      _ser(dict(hold)),
        "items":     [_ser(dict(i)) for i in items],
        "pull_list": pull_list,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Hold Status Transitions
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/holds/<hold_id>/status", methods=["POST"])
def update_hold_status(hold_id):
    """Transition hold status: PENDING→PULLING, PULLING→READY."""
    new_status = (request.get_json() or {}).get("status", "").upper()
    valid = {"PULLING", "READY", "RETURNED", "ACCEPTED"}
    if new_status not in valid:
        return jsonify({"error": f"Invalid status. Must be one of: {valid}"}), 400

    extra = {}
    if new_status == "READY":
        extra["ready_at"] = datetime.utcnow()
        extra["expires_at"] = datetime.utcnow() + timedelta(hours=2)

    db.execute("""
        UPDATE holds SET status = %s,
            ready_at   = COALESCE(%s, ready_at),
            expires_at = COALESCE(%s, expires_at)
        WHERE id = %s
    """, (new_status,
          extra.get("ready_at"), extra.get("expires_at"),
          hold_id))

    return jsonify({"success": True, "status": new_status})


@app.route("/api/holds/<hold_id>/scan", methods=["POST"])
def scan_card(hold_id):
    """
    Staff scans a barcode during PULLING phase.
    Accepts the scan if the barcode matches any card sharing
    tcgplayer_id + condition with an item on this hold.

    Returns which hold_item was fulfilled (may differ from exact barcode
    if a sibling copy was scanned).
    """
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    # Find the scanned card
    scanned = db.query_one("""
        SELECT rc.*, sl.bin_label
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.barcode = %s
    """, (barcode,))

    if not scanned:
        return jsonify({"error": "Barcode not found", "barcode": barcode}), 404

    # Find a REQUESTED hold_item on this hold matching tcgplayer_id + condition
    match = db.query_one("""
        SELECT hi.id, hi.barcode, rc.card_name, rc.condition
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.hold_id = %s
          AND hi.status = 'REQUESTED'
          AND rc.tcgplayer_id = %s
          AND rc.condition = %s
        LIMIT 1
    """, (hold_id, scanned["tcgplayer_id"], scanned["condition"]))

    if not match:
        return jsonify({
            "error": "This card doesn't match any outstanding item on this hold",
            "card_name": scanned["card_name"],
            "barcode": barcode,
        }), 409

    # Mark this hold_item as PULLED, update the actual barcode used
    db.execute("""
        UPDATE hold_items
        SET status = 'PULLED', pulled_at = CURRENT_TIMESTAMP, barcode = %s
        WHERE id = %s
    """, (barcode, str(match["id"])))

    # Update the raw_card state
    db.execute("""
        UPDATE raw_cards SET state = 'PULLED' WHERE barcode = %s
    """, (barcode,))

    return jsonify({
        "success":   True,
        "card_name": scanned["card_name"],
        "condition": scanned["condition"],
        "bin_label": scanned.get("bin_label"),
        "hold_item_id": str(match["id"]),
    })


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/decision", methods=["POST"])
def item_decision(hold_id, hold_item_id):
    """Accept or reject a single hold item."""
    data       = request.get_json() or {}
    decision   = data.get("decision", "").upper()  # ACCEPTED or REJECTED
    if decision not in ("ACCEPTED", "REJECTED"):
        return jsonify({"error": "decision must be ACCEPTED or REJECTED"}), 400

    item = db.query_one("""
        SELECT hi.*, rc.id AS card_id FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.id = %s AND hi.hold_id = %s
    """, (hold_item_id, hold_id))

    if not item:
        return jsonify({"error": "Hold item not found"}), 404

    if decision == "REJECTED":
        db.execute("""
            UPDATE hold_items SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (hold_item_id,))
        db.execute("""
            UPDATE raw_cards SET state = 'PENDING_RETURN', current_hold_id = NULL
            WHERE id = %s
        """, (str(item["card_id"]),))
    else:
        # ACCEPTED — listing will be created in the finish step
        db.execute("""
            UPDATE hold_items SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (hold_item_id,))

    return jsonify({"success": True, "decision": decision})


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/missing", methods=["POST"])
def mark_item_missing(hold_id, hold_item_id):
    """Mark a hold item as MISSING — card can't be found during pulling."""
    item = db.query_one("""
        SELECT hi.*, rc.id AS card_id FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.id = %s AND hi.hold_id = %s
    """, (hold_item_id, hold_id))
    if not item:
        return jsonify({"error": "Hold item not found"}), 404

    db.execute("""
        UPDATE hold_items SET status = 'MISSING', resolved_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (hold_item_id,))
    db.execute("""
        UPDATE raw_cards SET state = 'MISSING', current_hold_id = NULL
        WHERE id = %s
    """, (str(item["card_id"]),))

    return jsonify({"success": True, "status": "MISSING"})


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/reverse", methods=["POST"])
def reverse_decision(hold_id, hold_item_id):
    """
    Reverse a hold item decision after finalization.
    - Re-accept a REJECTED card → create Shopify listing
    - Return an ACCEPTED card → delete Shopify listing, mark PENDING_RETURN
    """
    item = db.query_one("""
        SELECT hi.*, rc.id AS card_id, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.tcgplayer_id, rc.barcode
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.id = %s AND hi.hold_id = %s
    """, (hold_item_id, hold_id))
    if not item:
        return jsonify({"error": "Hold item not found"}), 404

    action = (request.get_json() or {}).get("action", "").lower()

    if action == "re-accept" and item["status"] == "REJECTED":
        # Create Shopify listing and mark accepted
        try:
            listing = _create_raw_listing(item)
            db.execute("""
                UPDATE hold_items
                SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP,
                    shopify_product_id = %s, shopify_variant_id = %s
                WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], hold_item_id))
            db.execute("""
                UPDATE raw_cards SET state = 'PENDING_SALE', current_hold_id = NULL
                WHERE id = %s
            """, (str(item["card_id"]),))
            return jsonify({"success": True, "action": "re-accepted", "product_id": listing["product_id"]})
        except Exception as e:
            return jsonify({"error": f"Failed to create listing: {e}"}), 500

    elif action == "return" and item["status"] == "ACCEPTED" and item.get("shopify_product_id"):
        # Delete Shopify listing and mark for return
        try:
            _shopify("DELETE", f"/products/{item['shopify_product_id']}.json")
        except Exception as e:
            logger.warning(f"Failed to delete Shopify product {item['shopify_product_id']}: {e}")
        db.execute("""
            UPDATE hold_items
            SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP,
                shopify_product_id = NULL, shopify_variant_id = NULL
            WHERE id = %s
        """, (hold_item_id,))
        db.execute("""
            UPDATE raw_cards SET state = 'PENDING_RETURN', current_hold_id = NULL
            WHERE id = %s
        """, (str(item["card_id"]),))
        return jsonify({"success": True, "action": "returned"})

    return jsonify({"error": "Invalid action or item status"}), 400


@app.route("/api/holds/<hold_id>/finish", methods=["POST"])
def finish_hold(hold_id):
    """
    Finish a hold:
    - For guest holds: Create Shopify draft listings for ACCEPTED items, reject undecided
    - For champion holds: Skip product creation (already exists), auto-accept all pulled items
    """
    # Check if this is a Champion (kiosk checkout) hold
    hold = db.query_one("SELECT cohort, checkout_status FROM holds WHERE id = %s", (hold_id,))
    is_champion = hold and hold.get("cohort") == "champion"

    if is_champion:
        return _finish_champion_hold(hold_id)

    if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
        return jsonify({"error": "Shopify not configured"}), 503

    accepted = db.query("""
        SELECT hi.id AS hold_item_id, hi.barcode,
               rc.id AS card_id, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.tcgplayer_id
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.hold_id = %s AND hi.status = 'ACCEPTED'
    """, (hold_id,))

    results  = []
    errors   = []

    for item in accepted:
        try:
            listing = _create_raw_listing(item)
            db.execute("""
                UPDATE hold_items
                SET shopify_product_id = %s, shopify_variant_id = %s
                WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], str(item["hold_item_id"])))
            db.execute("""
                UPDATE raw_cards SET state = 'PENDING_SALE', current_hold_id = NULL
                WHERE id = %s
            """, (str(item["card_id"]),))
            results.append({
                "barcode":     item["barcode"],
                "card_name":   item["card_name"],
                "product_id":  listing["product_id"],
                "action":      "listing_created",
            })
        except Exception as e:
            logger.exception(f"Failed to create listing for {item['barcode']}: {e}")
            errors.append({"barcode": item["barcode"], "error": str(e)})

    # Any items still REQUESTED or PULLED (not yet decided) → auto-reject
    # MISSING items are already resolved — leave them alone
    db.execute("""
        UPDATE raw_cards SET state = 'PENDING_RETURN', current_hold_id = NULL
        WHERE id IN (
            SELECT raw_card_id FROM hold_items
            WHERE hold_id = %s AND status IN ('REQUESTED','PULLED')
        )
    """, (hold_id,))
    db.execute("""
        UPDATE hold_items SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP
        WHERE hold_id = %s AND status IN ('REQUESTED','PULLED')
    """, (hold_id,))

    # Close the hold
    final_status = "ACCEPTED" if results else "RETURNED"
    db.execute("""
        UPDATE holds SET status = %s, resolved_at = CURRENT_TIMESTAMP WHERE id = %s
    """, (final_status, hold_id))

    return jsonify({
        "success":  True,
        "created":  len(results),
        "errors":   errors,
        "results":  results,
    })


def _create_raw_listing(item: dict) -> dict:
    """Create a Shopify DRAFT product for a raw card at POS."""
    condition_labels = {"NM": "Near Mint", "LP": "Lightly Played",
                        "MP": "Moderately Played", "HP": "Heavily Played", "DMG": "Damaged"}
    cond_label = condition_labels.get(item["condition"], item["condition"])
    card_num   = f" #{item['card_number']}" if item.get("card_number") else ""
    title      = f"{item['card_name']}{card_num} [{cond_label}]"
    body       = (f"<p>{item['card_name']}{card_num}</p>"
                  f"<p>Set: {item.get('set_name','')}</p>"
                  f"<p>Condition: {cond_label}</p>"
                  f"<p>Barcode: {item['barcode']}</p>")
    price      = float(item.get("current_price") or 0)

    payload = {
        "product": {
            "title":       title,
            "body_html":   body,
            "status":      "draft",
            "product_type": "Pokemon",
            "vendor":      "Pack Fresh",
            "images":      [{"src": item["image_url"]}] if item.get("image_url") else [],
            "variants": [{
                "price":                str(round(price, 2)),
                "sku":                  item["barcode"],
                "barcode":              item["barcode"],
                "inventory_management": "shopify",
                "inventory_quantity":   1,
                "requires_shipping":    True,
            }],
        }
    }

    result  = _shopify("POST", "/products.json", json=payload)
    product = result["product"]
    return {
        "product_id": product["id"],
        "variant_id": product["variants"][0]["id"],
        "title":      title,
    }


def _finish_champion_hold(hold_id):
    """
    Finish a Champion (kiosk checkout) hold.
    Products already exist on Shopify (created by kiosk at checkout).
    Auto-accept all PULLED items, mark MISSING items, close the hold.
    """
    # Auto-accept all PULLED/REQUESTED items (customer already paid)
    items = db.query("""
        SELECT hi.id AS hold_item_id, hi.status, hi.raw_card_id
        FROM hold_items hi
        WHERE hi.hold_id = %s AND hi.status IN ('PULLED', 'REQUESTED')
    """, (hold_id,))

    for item in items:
        db.execute("""
            UPDATE hold_items SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (str(item["hold_item_id"]),))
        db.execute("""
            UPDATE raw_cards SET state = 'PENDING_SALE', current_hold_id = NULL
            WHERE id = %s
        """, (str(item["raw_card_id"]),))

    # Handle MISSING items — release them
    db.execute("""
        UPDATE raw_cards SET current_hold_id = NULL
        WHERE id IN (
            SELECT raw_card_id FROM hold_items
            WHERE hold_id = %s AND status = 'MISSING'
        )
    """, (hold_id,))

    # Close the hold
    db.execute("""
        UPDATE holds SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP WHERE id = %s
    """, (hold_id,))

    accepted_count = len(items)
    logger.info(f"Finished Champion hold {hold_id}: {accepted_count} items accepted (pre-paid)")

    return jsonify({
        "success": True,
        "champion": True,
        "accepted": accepted_count,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Return Queue API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/returns")
def list_returns():
    """All PENDING_RETURN cards."""
    rows = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.condition,
               rc.card_number, rc.image_url, rc.current_price,
               sl.bin_label AS last_bin
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.state = 'PENDING_RETURN'
        ORDER BY rc.updated_at DESC NULLS LAST
    """)
    return jsonify({"cards": [_ser(dict(r)) for r in rows]})


@app.route("/api/missing")
def list_missing():
    """All cards in MISSING state."""
    rows = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.condition,
               rc.card_number, rc.image_url, rc.current_price, rc.updated_at,
               sl.bin_label AS last_bin
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.state = 'MISSING'
        ORDER BY rc.updated_at DESC NULLS LAST
    """)
    return jsonify({"cards": [_ser(dict(r)) for r in rows]})


@app.route("/api/missing/<card_id>/gone", methods=["POST"])
def mark_gone(card_id):
    """Permanently mark a missing card as GONE (lost/theft)."""
    card = db.query_one("SELECT id, card_name FROM raw_cards WHERE id::text = %s AND state = 'MISSING'", (card_id,))
    if not card:
        return jsonify({"error": "Card not found or not in MISSING state"}), 404
    db.execute("""
        UPDATE raw_cards SET state = 'GONE', updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (card_id,))
    return jsonify({"success": True, "card_name": card["card_name"]})


@app.route("/api/returns/scan", methods=["POST"])
def scan_return():
    """Verify a card being physically returned. Also handles re-found MISSING cards."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    card = db.query_one("""
        SELECT id, card_name, set_name, condition, state FROM raw_cards WHERE barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found"}), 404

    # Handle MISSING cards being re-found — treat as a return
    if card["state"] == "MISSING":
        return jsonify({
            "success":   True,
            "card_name": card["card_name"],
            "condition": card["condition"],
            "barcode":   barcode,
            "was_missing": True,
        })

    if card["state"] != "PENDING_RETURN":
        return jsonify({"error": f"Card is {card['state']}, not PENDING_RETURN", "card_name": card["card_name"]}), 409

    # Tag as scanned-back — use a temporary marker in card state
    # We keep PENDING_RETURN but mark updated_at so UI can show "scanned"
    # Scan confirmed client-side; no DB write needed until store_returns
    return jsonify({
        "success":   True,
        "card_name": card["card_name"],
        "condition": card["condition"],
        "barcode":   barcode,
    })


@app.route("/api/returns/store", methods=["POST"])
def store_returns():
    """
    Assign PENDING_RETURN cards to bins and mark them STORED.
    Optionally takes a list of barcodes (scanned batch); if omitted, stores all PENDING_RETURN.
    """
    data     = request.get_json() or {}
    barcodes = data.get("barcodes")  # optional list

    if not barcodes:
        return jsonify({"error": "No barcodes provided — scan cards before storing"}), 400

    cards = db.query("""
        SELECT id, barcode, card_name
        FROM raw_cards WHERE barcode = ANY(%s) AND state IN ('PENDING_RETURN', 'MISSING')
    """, (list(barcodes),))

    if not cards:
        return jsonify({"error": "No scanned cards found in PENDING_RETURN state"}), 400

    # Group by card_type (pokemon / magic / etc.) for bin assignment
    # Default to 'pokemon' since that's what we have; card_type_hint would help here
    card_type = "pokemon"
    count     = len(cards)
    card_ids  = [str(c["id"]) for c in cards]

    try:
        assignments = assign_bins(card_type, count, db)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # Assign cards to bins in order
    card_idx = 0
    bin_summary = []
    for assignment in assignments:
        bin_id    = assignment["bin_id"]
        bin_label = assignment["bin_label"]
        take      = assignment["count"]
        batch_ids = card_ids[card_idx:card_idx + take]
        card_idx += take

        db.execute("""
            UPDATE raw_cards
            SET state = 'STORED', bin_id = %s, current_hold_id = NULL,
                stored_at = CURRENT_TIMESTAMP
            WHERE id::text = ANY(%s)
        """, (bin_id, batch_ids))

        bin_summary.append({
            "bin_label": bin_label,
            "count":     take,
            "cards":     [c["card_name"] for c in cards
                         if str(c["id"]) in set(batch_ids)][:5],
        })

    return jsonify({
        "success":     True,
        "stored":      count,
        "assignments": bin_summary,
    })


@app.route("/api/returns/recent")
def recent_assignments():
    """
    Recent return-to-storage assignments — cards that moved from
    PENDING_RETURN to STORED, grouped by bin, ordered by stored_at DESC.
    Shows the last 24 hours so staff can review where things went.
    """
    rows = db.query("""
        SELECT rc.card_name, rc.condition, rc.barcode,
               sl.bin_label, rc.stored_at
        FROM raw_cards rc
        JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.state = 'STORED'
          AND rc.stored_at >= NOW() - INTERVAL '24 hours'
        ORDER BY rc.stored_at DESC
        LIMIT 100
    """)

    # Group cards stored within 30 seconds of each other into one batch
    # regardless of bin — a single store operation may span multiple bins
    batches = []
    current_batch = None

    for r in rows:
        stored_at = r["stored_at"]
        bin_label = r["bin_label"]

        # New batch if > 30s gap from last card
        if (current_batch is None or
            abs((stored_at - current_batch["_ref_at"]).total_seconds()) > 30):
            current_batch = {
                "stored_at": stored_at.isoformat(),
                "_ref_at":   stored_at,
                "bins": {},
            }
            batches.append(current_batch)

        if bin_label not in current_batch["bins"]:
            current_batch["bins"][bin_label] = []
        current_batch["bins"][bin_label].append({
            "card_name": r["card_name"],
            "condition": r["condition"],
            "barcode":   r["barcode"],
        })

    # Flatten bins dict to list for JSON
    result_batches = []
    for b in batches:
        for bin_label, cards in b["bins"].items():
            result_batches.append({
                "bin_label": bin_label,
                "stored_at": b["stored_at"],
                "cards":     cards,
            })

    return jsonify({"batches": result_batches})


# ═══════════════════════════════════════════════════════════════════════════════
# Card Editor — search/inspect/repair raw_cards rows
# ═══════════════════════════════════════════════════════════════════════════════

# JPY rate mirrors ingestion's relink path so cache prices land in USD on read.
_JPY_USD_RATE = float(os.getenv("SCRYDEX_JPY_USD_RATE", "0.0066"))
_USD_PRICE_EXPR = (
    "CASE WHEN currency = 'JPY' "
    f"THEN ROUND(market_price::numeric * {_JPY_USD_RATE}::numeric, 2) "
    "ELSE market_price END"
)


@app.route("/api/editor/search")
def editor_search():
    """Aggregated search over raw_cards. Mirrors kiosk's /api/browse so the
    grid layout/data shape can stay identical. Includes ALL states (not just
    STORED) — the editor needs to repair PULLED/MISSING rows too."""
    q          = (request.args.get("q") or "").strip()
    page       = max(1, int(request.args.get("page", 1)))
    page_size  = 24
    offset     = (page - 1) * page_size

    # Editor grid mirrors kiosk's view (state='STORED' AND current_hold_id IS NULL)
    # so prices and counts reflect what's actually saleable. Repairs to non-saleable
    # rows (PULLED/MISSING/PENDING_*) still happen via direct barcode search.
    filters = ["state = 'STORED'", "current_hold_id IS NULL"]
    params  = []
    if q:
        # Barcode is unique; if the query looks like one, do an exact match
        # so a barcode scan jumps straight to the right copy.
        filters.append("(card_name ILIKE %s OR set_name ILIKE %s OR card_number ILIKE %s OR barcode = %s)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%", q]

    where = " AND ".join(filters)

    group_key = ("card_name, set_name, tcgplayer_id, "
                 "CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil') "
                 "THEN '' ELSE variant END")

    count_row = db.query_one(f"""
        SELECT COUNT(DISTINCT ({group_key})) AS total
        FROM raw_cards
        WHERE {where}
    """, tuple(params))
    total = count_row["total"] if count_row else 0

    rows = db.query(f"""
        SELECT
            card_name,
            set_name,
            tcgplayer_id,
            MAX(scrydex_id) AS scrydex_id,
            MAX(variant_raw) AS variant_raw,
            variant_key,
            MAX(image_url) AS image_url,
            SUM(cond_qty) AS total_qty,
            MIN(min_price) AS min_price,
            MAX(max_price) AS max_price,
            jsonb_object_agg(condition, cond_qty) AS conditions
        FROM (
            SELECT card_name, set_name, tcgplayer_id, scrydex_id,
                   variant AS variant_raw,
                   CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil')
                        THEN '' ELSE variant END AS variant_key,
                   image_url, condition,
                   COUNT(*) AS cond_qty,
                   MIN(current_price) AS min_price,
                   MAX(current_price) AS max_price
            FROM raw_cards
            WHERE {where}
            GROUP BY card_name, set_name, tcgplayer_id, scrydex_id,
                     variant_raw, variant_key, image_url, condition
        ) sub
        GROUP BY card_name, set_name, tcgplayer_id, variant_key
        ORDER BY card_name ASC
        LIMIT %s OFFSET %s
    """, tuple(params) + (page_size, offset))

    cards = []
    for r in rows:
        # Image fallback to scrydex cache (same as kiosk's logic) so legacy
        # JP rows light up here even before they get backfilled.
        image_url = r["image_url"]
        sid = r["scrydex_id"]
        tcg = r["tcgplayer_id"]
        if not image_url and (sid or tcg):
            if sid:
                sx = db.query_one("""
                    SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m, MAX(image_small) AS img_s
                    FROM scrydex_price_cache WHERE scrydex_id = %s
                """, (sid,))
            else:
                sx = db.query_one("""
                    SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m, MAX(image_small) AS img_s
                    FROM scrydex_price_cache WHERE tcgplayer_id = %s
                """, (tcg,))
            if sx:
                image_url = sx.get("img_l") or sx.get("img_m") or sx.get("img_s")

        variant_raw = (r.get("variant_raw") or "").strip()
        variant_label = variant_raw if variant_raw and variant_raw.lower() not in ("normal", "holofoil") else None

        cards.append({
            "card_name":     r["card_name"],
            "set_name":      r["set_name"],
            "tcgplayer_id":  r["tcgplayer_id"],
            "scrydex_id":    r["scrydex_id"],
            "variant_key":   r["variant_key"] or "",
            "variant_label": variant_label,
            "image_url":     image_url,
            "total_qty":     r["total_qty"],
            "min_price":     float(r["min_price"]) if r["min_price"] else None,
            "max_price":     float(r["max_price"]) if r["max_price"] else None,
            "conditions":    r["conditions"] or {},
        })

    return jsonify({
        "cards": cards,
        "total": total,
        "page":  page,
        "pages": max(1, (total + page_size - 1) // page_size),
    })


@app.route("/api/editor/copies")
def editor_copies():
    """Individual raw_cards copies for a (name, set, variant_key) tile.
    Returns every state (STORED, PULLED, MISSING, etc.) so the editor can
    fix any row, not just available ones."""
    card_name  = request.args.get("name", "")
    set_name   = request.args.get("set", "")
    variant_k  = request.args.get("variant", "")
    tcg_id     = request.args.get("tcgplayer_id")
    sx_id      = request.args.get("scrydex_id")

    variant_filter = (
        "AND (variant IS NULL OR LOWER(variant) IN ('normal','holofoil'))"
        if variant_k == ""
        else "AND variant = %s"
    )
    extra = []
    if variant_k:
        extra.append(variant_k)

    id_filter = ""
    if sx_id:
        id_filter = "AND scrydex_id = %s"
        extra.append(sx_id)
    elif tcg_id:
        id_filter = "AND tcgplayer_id = %s"
        extra.append(tcg_id)

    copies = db.query(f"""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.tcgplayer_id, rc.scrydex_id, rc.state, rc.cost_basis,
               rc.last_price_update, sl.bin_label
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.card_name = %s AND COALESCE(rc.set_name,'') = COALESCE(%s,'')
          {variant_filter}
          {id_filter}
        ORDER BY
            CASE rc.condition
                WHEN 'NM'  THEN 1 WHEN 'LP'  THEN 2 WHEN 'MP' THEN 3
                WHEN 'HP'  THEN 4 WHEN 'DMG' THEN 5 ELSE 9
            END,
            rc.current_price DESC
    """, (card_name, set_name, *extra))

    # Available variants from Scrydex for this card identity. Same scrydex_id
    # can hold multiple printings (1st Ed vs Unlimited, reverseHolofoil, etc.)
    # — staff needs a dropdown to swap a copy to the right one without
    # memorizing Scrydex's variant strings.
    available_variants = []
    if copies:
        first = copies[0]
        sid = first.get("scrydex_id")
        tcg = first.get("tcgplayer_id")
        if sid:
            rows = db.query("""
                SELECT DISTINCT variant FROM scrydex_price_cache
                WHERE scrydex_id = %s AND product_type = 'card'
                  AND price_type = 'raw' AND variant IS NOT NULL
                ORDER BY variant
            """, (sid,))
        elif tcg:
            rows = db.query("""
                SELECT DISTINCT variant FROM scrydex_price_cache
                WHERE tcgplayer_id = %s AND product_type = 'card'
                  AND price_type = 'raw' AND variant IS NOT NULL
                ORDER BY variant
            """, (int(tcg),))
        else:
            rows = []
        available_variants = [r["variant"] for r in rows if r.get("variant")]

    return jsonify({
        "copies": [_ser(dict(c)) for c in copies],
        "available_variants": available_variants,
    })


@app.route("/api/editor/scrydex-search", methods=["POST"])
def editor_scrydex_search():
    """Cache-first search of scrydex_price_cache for the relink modal.
    Mirrors the ingest service's /api/ingest/search-cards but lives here
    so card_manager can stay self-contained."""
    data    = request.get_json(silent=True) or {}
    query   = (data.get("query") or "").strip()
    set_nm  = (data.get("set_name") or "").strip()
    tcg_id  = data.get("tcgplayer_id")
    limit   = min(int(data.get("limit") or 20), 50)

    if not query and not set_nm and not tcg_id:
        return jsonify({"error": "query, set_name, or tcgplayer_id required"}), 400

    where  = ["product_type = 'card'", "price_type = 'raw'", "condition = 'NM'"]
    params = []

    SEARCH_EXPR = (
        "(COALESCE(product_name, '') || ' ' || "
        "COALESCE(product_name_en, '') || ' ' || "
        "COALESCE(expansion_name, '') || ' ' || "
        "COALESCE(expansion_name_en, '') || ' ' || "
        "COALESCE(card_number, ''))"
    )

    if tcg_id:
        try:
            where.append("tcgplayer_id = %s")
            params.append(int(tcg_id))
        except (ValueError, TypeError):
            return jsonify({"error": "tcgplayer_id must be numeric"}), 400

    if query:
        for tok in [t.lstrip("#").strip() for t in query.split() if t.strip().lstrip("#")]:
            if tok.isdigit():
                where.append(f"(card_number = %s OR {SEARCH_EXPR} ILIKE %s)")
                params.extend([tok, f"%{tok}%"])
            else:
                where.append(f"{SEARCH_EXPR} ILIKE %s")
                params.append(f"%{tok}%")
    if set_nm:
        where.append("(expansion_name ILIKE %s OR expansion_name_en ILIKE %s)")
        params.extend([f"%{set_nm}%", f"%{set_nm}%"])

    sql = f"""
        SELECT DISTINCT ON (scrydex_id, variant)
               scrydex_id, tcgplayer_id, product_name, product_name_en,
               expansion_name, expansion_name_en, language_code,
               card_number, rarity, variant, image_small, image_medium,
               currency,
               {_USD_PRICE_EXPR} AS market_price_usd
        FROM scrydex_price_cache
        WHERE {' AND '.join(where)}
        ORDER BY scrydex_id, variant
        LIMIT %s
    """
    params.append(limit)
    rows = db.query(sql, tuple(params))

    return jsonify({"results": [{
        "scrydex_id":   r.get("scrydex_id"),
        "tcgplayer_id": r.get("tcgplayer_id"),
        "name":         r.get("product_name") or r.get("product_name_en"),
        "set_name":     r.get("expansion_name") or r.get("expansion_name_en"),
        "language_code": r.get("language_code"),
        "card_number":  r.get("card_number"),
        "rarity":       r.get("rarity"),
        "variant":      r.get("variant"),
        "image":        r.get("image_small") or r.get("image_medium"),
        "market_price": float(r["market_price_usd"]) if r.get("market_price_usd") else None,
    } for r in rows], "total": len(rows)})


def _lookup_cache_image(scrydex_id, tcgplayer_id):
    if scrydex_id:
        row = db.query_one("""
            SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m, MAX(image_small) AS img_s
            FROM scrydex_price_cache WHERE scrydex_id = %s
        """, (scrydex_id,))
    elif tcgplayer_id:
        row = db.query_one("""
            SELECT MAX(image_large) AS img_l, MAX(image_medium) AS img_m, MAX(image_small) AS img_s
            FROM scrydex_price_cache WHERE tcgplayer_id = %s
        """, (int(tcgplayer_id),))
    else:
        return None
    if not row:
        return None
    return row.get("img_l") or row.get("img_m") or row.get("img_s")


def _lookup_cache_price(scrydex_id, tcgplayer_id, condition, variant):
    """Pull USD raw price from scrydex_price_cache for (id, condition, variant).
    Tries: (cond, variant) → (NM, variant) → (cond, no variant) → (NM, no variant).
    Returns Decimal or None."""
    cond_map = {"DMG": "DM"}  # Scrydex stores Damaged as 'DM'
    cache_cond = cond_map.get((condition or "").upper(), (condition or "NM").upper())

    id_where = []
    id_params = []
    if scrydex_id:
        id_where.append("scrydex_id = %s")
        id_params.append(scrydex_id)
    elif tcgplayer_id:
        id_where.append("tcgplayer_id = %s")
        id_params.append(int(tcgplayer_id))
    else:
        return None

    cond_chain = [cache_cond] if cache_cond == "NM" else [cache_cond, "NM"]
    variant_chain = [variant, None] if variant else [None]

    for v in variant_chain:
        for c in cond_chain:
            where = list(id_where) + ["product_type = 'card'", "price_type = 'raw'",
                                      "market_price IS NOT NULL", "condition = %s"]
            params = list(id_params) + [c]
            if v:
                where.append("variant = %s")
                params.append(v)
            sql = f"""
                SELECT {_USD_PRICE_EXPR} AS price_usd
                FROM scrydex_price_cache
                WHERE {' AND '.join(where)}
                ORDER BY fetched_at DESC NULLS LAST
                LIMIT 1
            """
            row = db.query_one(sql, tuple(params))
            if row and row.get("price_usd") is not None:
                return Decimal(str(row["price_usd"]))
    return None


@app.route("/api/editor/copies/<copy_id>/relink", methods=["POST"])
def editor_relink(copy_id):
    """Change identity (tcgplayer_id, scrydex_id, variant, name, set, card_number)
    on a single raw_cards row. Refreshes image_url and current_price from the
    Scrydex cache using the new identity + the row's existing condition."""
    data = request.get_json() or {}
    tcg_id     = data.get("tcgplayer_id")
    sx_id      = data.get("scrydex_id")
    variant    = data.get("variant")
    card_name  = data.get("card_name")
    set_name   = data.get("set_name")
    card_num   = data.get("card_number")

    if not tcg_id and not sx_id:
        return jsonify({"error": "tcgplayer_id or scrydex_id required"}), 400

    card = db.query_one("SELECT condition FROM raw_cards WHERE id::text = %s", (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404

    new_image = _lookup_cache_image(sx_id, tcg_id)
    new_price = _lookup_cache_price(sx_id, tcg_id, card["condition"], variant)

    db.execute("""
        UPDATE raw_cards
        SET tcgplayer_id    = %s,
            scrydex_id      = %s,
            variant         = %s,
            card_name       = COALESCE(%s, card_name),
            set_name        = COALESCE(%s, set_name),
            card_number     = COALESCE(%s, card_number),
            image_url       = COALESCE(%s, image_url),
            current_price   = COALESCE(%s, current_price),
            last_price_update = CASE WHEN %s IS NOT NULL THEN CURRENT_TIMESTAMP ELSE last_price_update END,
            updated_at      = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (tcg_id, sx_id, variant, card_name, set_name, card_num,
          new_image, new_price, new_price, copy_id))

    return jsonify({
        "success":      True,
        "image_url":    new_image,
        "current_price": float(new_price) if new_price is not None else None,
    })


@app.route("/api/editor/copies/<copy_id>/condition", methods=["POST"])
def editor_change_condition(copy_id):
    """Change a copy's condition. Refreshes price from Scrydex cache for the
    new condition since worse conditions trade lower."""
    new_cond = (request.get_json() or {}).get("condition", "").upper()
    if new_cond not in {"NM", "LP", "MP", "HP", "DMG"}:
        return jsonify({"error": "condition must be NM/LP/MP/HP/DMG"}), 400

    card = db.query_one("""
        SELECT scrydex_id, tcgplayer_id, variant FROM raw_cards WHERE id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404

    new_price = _lookup_cache_price(card["scrydex_id"], card["tcgplayer_id"], new_cond, card.get("variant"))

    db.execute("""
        UPDATE raw_cards
        SET condition = %s,
            current_price = COALESCE(%s, current_price),
            last_price_update = CASE WHEN %s IS NOT NULL THEN CURRENT_TIMESTAMP ELSE last_price_update END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (new_cond, new_price, new_price, copy_id))

    return jsonify({
        "success":       True,
        "condition":     new_cond,
        "current_price": float(new_price) if new_price is not None else None,
    })


@app.route("/api/editor/copies/<copy_id>/variant", methods=["POST"])
def editor_change_variant(copy_id):
    """Change a copy's variant (e.g. 'reverseHolofoil', 'firstEditionHolofoil').
    Refreshes price for the new variant + current condition."""
    new_variant = (request.get_json() or {}).get("variant")
    new_variant = new_variant.strip() if isinstance(new_variant, str) else None

    card = db.query_one("""
        SELECT scrydex_id, tcgplayer_id, condition FROM raw_cards WHERE id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404

    new_price = _lookup_cache_price(card["scrydex_id"], card["tcgplayer_id"], card["condition"], new_variant)

    db.execute("""
        UPDATE raw_cards
        SET variant = %s,
            current_price = COALESCE(%s, current_price),
            last_price_update = CASE WHEN %s IS NOT NULL THEN CURRENT_TIMESTAMP ELSE last_price_update END,
            updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (new_variant or None, new_price, new_price, copy_id))

    return jsonify({
        "success":       True,
        "variant":       new_variant,
        "current_price": float(new_price) if new_price is not None else None,
    })


@app.route("/api/editor/copies/<copy_id>/refresh-price", methods=["POST"])
def editor_refresh_price(copy_id):
    """Re-pull current_price from scrydex_price_cache for this copy's
    (id, condition, variant)."""
    card = db.query_one("""
        SELECT scrydex_id, tcgplayer_id, condition, variant
        FROM raw_cards WHERE id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404

    new_price = _lookup_cache_price(card["scrydex_id"], card["tcgplayer_id"], card["condition"], card.get("variant"))
    if new_price is None:
        return jsonify({"error": "No cached price found for this card+condition+variant"}), 404

    db.execute("""
        UPDATE raw_cards
        SET current_price = %s, last_price_update = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (new_price, copy_id))

    return jsonify({"success": True, "current_price": float(new_price)})


@app.route("/api/editor/copies/<copy_id>/barcode-image")
def editor_barcode_image(copy_id):
    """Regenerate the barcode label PNG for an existing copy and return it
    as image/png. The frontend opens this URL directly so the browser can
    print without round-tripping a base64 blob over HTTP/2."""
    from flask import Response
    card = db.query_one("""
        SELECT barcode, card_name, set_name, condition, card_number
        FROM raw_cards WHERE id::text = %s
    """, (copy_id,))
    if not card or not card.get("barcode"):
        return jsonify({"error": "Copy not found or has no barcode"}), 404

    png_bytes = generate_barcode_image(
        card["barcode"],
        card_name=card["card_name"] or "",
        set_name=card.get("set_name") or "",
        condition=card.get("condition") or "",
        card_number=card.get("card_number") or "",
    )
    return Response(png_bytes, mimetype="image/png", headers={
        "Content-Disposition": f'inline; filename="{card["barcode"]}.png"',
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5006)), debug=False)
