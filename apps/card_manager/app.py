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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5006)), debug=False)
