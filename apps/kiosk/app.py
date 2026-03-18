"""
kiosk — kiosk.pack-fresh.com
Customer-facing card browse + hold request system.

Read-only on raw_cards. Writes to holds + hold_items.
Cards are aggregated by (card_name, set_name) with qty per condition.
Max 20 cards per hold.
"""

import os
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Response, render_template

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()

MAX_HOLD_ITEMS = 20
HOLD_EXPIRY_HOURS = 2

# Inline the HTML so no template file needed



@app.route("/")
def index():
    return render_template("index.html")


# ═══════════════════════════════════════════════════════════════════════════════
# Browse API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/browse")
def browse():
    """
    Aggregated card listings.
    Groups raw_cards by (card_name, set_name, tcgplayer_id)
    Returns available qty per condition, total count, price range.

    Query params: q, set, page (24 per page)
    """
    q        = (request.args.get("q") or "").strip()
    set_name = (request.args.get("set") or "").strip()
    page     = max(1, int(request.args.get("page", 1)))
    offset   = (page - 1) * 24

    filters = ["state = 'STORED'"]
    params  = []

    if q:
        filters.append("(card_name ILIKE %s OR set_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    if set_name:
        filters.append("set_name ILIKE %s")
        params.append(f"%{set_name}%")

    where = " AND ".join(filters)

    # Count distinct cards (not individual copies)
    count_row = db.query_one(f"""
        SELECT COUNT(DISTINCT (card_name, set_name, tcgplayer_id)) AS total
        FROM raw_cards
        WHERE {where}
    """, tuple(params))
    total = count_row["total"] if count_row else 0

    # Aggregated cards with per-condition breakdown
    rows = db.query(f"""
        SELECT
            card_name,
            set_name,
            tcgplayer_id,
            MAX(image_url) AS image_url,
            COUNT(*) AS total_qty,
            MIN(current_price) AS min_price,
            MAX(current_price) AS max_price,
            jsonb_object_agg(condition, cond_qty) AS conditions
        FROM (
            SELECT card_name, set_name, tcgplayer_id, image_url,
                   condition, COUNT(*) AS cond_qty, MIN(current_price) AS current_price
            FROM raw_cards
            WHERE {where}
              AND state = 'STORED'
            GROUP BY card_name, set_name, tcgplayer_id, image_url, condition
        ) sub
        GROUP BY card_name, set_name, tcgplayer_id
        ORDER BY card_name ASC
        LIMIT 24 OFFSET %s
    """, tuple(params) + (offset,))

    cards = []
    for r in rows:
        cards.append({
            "card_name":    r["card_name"],
            "set_name":     r["set_name"],
            "tcgplayer_id": r["tcgplayer_id"],
            "image_url":    r["image_url"],
            "total_qty":    r["total_qty"],
            "min_price":    float(r["min_price"]) if r["min_price"] else None,
            "max_price":    float(r["max_price"]) if r["max_price"] else None,
            "conditions":   r["conditions"] or {},
        })

    return jsonify({
        "cards":  cards,
        "total":  total,
        "page":   page,
        "pages":  max(1, (total + 23) // 24),
    })


@app.route("/api/sets")
def list_sets():
    rows = db.query("""
        SELECT DISTINCT set_name FROM raw_cards
        WHERE state = 'STORED' AND set_name IS NOT NULL
        ORDER BY set_name ASC LIMIT 200
    """)
    return jsonify({"sets": [r["set_name"] for r in rows]})


@app.route("/api/card")
def card_detail():
    """
    Individual copies of a specific card for the detail view.
    Returns each copy with condition, price, card_number.
    """
    card_name = request.args.get("name", "")
    set_name  = request.args.get("set", "")

    copies = db.query("""
        SELECT id, barcode, card_name, set_name, card_number,
               condition, current_price, image_url
        FROM raw_cards
        WHERE card_name = %s AND set_name = %s AND state = 'STORED'
        ORDER BY
            CASE condition
                WHEN 'NM'  THEN 1 WHEN 'LP'  THEN 2 WHEN 'MP' THEN 3
                WHEN 'HP'  THEN 4 WHEN 'DMG' THEN 5 ELSE 9
            END,
            current_price DESC
    """, (card_name, set_name))

    return jsonify({"copies": [dict(c) for c in copies]})


# ═══════════════════════════════════════════════════════════════════════════════
# Hold API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/hold", methods=["POST"])
def create_hold():
    """
    Submit a hold request.

    POST body:
    {
        "customer_name": "Mark",
        "customer_phone": "555-1234",
        "items": [
            {"card_name": "Charizard ex", "set_name": "...", "condition": "NM", "qty": 2},
            ...
        ]
    }

    Resolves which specific barcodes to hold, marks them PULLED state
    (actually PULLED happens when staff scans — here we just reserve them).
    Returns hold_id + summary.
    """
    data     = request.get_json() or {}
    name     = (data.get("customer_name") or "").strip()
    phone    = (data.get("customer_phone") or "").strip()
    items    = data.get("items") or []

    if not name:
        return jsonify({"error": "Customer name required"}), 400
    if not items:
        return jsonify({"error": "No items in hold request"}), 400

    # Validate total quantity
    total_qty = sum(int(i.get("qty", 1)) for i in items)
    if total_qty > MAX_HOLD_ITEMS:
        return jsonify({"error": f"Maximum {MAX_HOLD_ITEMS} cards per hold (requested {total_qty})"}), 400
    if total_qty < 1:
        return jsonify({"error": "Must request at least 1 card"}), 400

    # For each line item, find available STORED cards matching card+set+condition
    # Lock them by setting current_hold_id
    hold_row = db.query_one("""
        INSERT INTO holds (customer_name, customer_phone, status, item_count)
        VALUES (%s, %s, 'PENDING', %s)
        RETURNING id
    """, (name, phone or None, total_qty))
    hold_id = str(hold_row["id"])

    assigned = []
    errors   = []

    for line in items:
        card_name = line.get("card_name", "")
        set_name  = line.get("set_name", "")
        condition = line.get("condition", "NM")
        qty       = max(1, int(line.get("qty", 1)))

        # Find available cards — exclude any already on a hold
        available = db.query("""
            SELECT id, barcode FROM raw_cards
            WHERE card_name = %s AND set_name = %s
              AND condition = %s AND state = 'STORED'
              AND current_hold_id IS NULL
            ORDER BY created_at ASC
            LIMIT %s
        """, (card_name, set_name, condition, qty))

        if len(available) < qty:
            # Partial or none available — hold what we can, flag the rest
            if not available:
                errors.append(f"No {condition} copies available for {card_name}")
                continue
            errors.append(f"Only {len(available)} {condition} {card_name} available (requested {qty})")

        for card in available:
            card_id = str(card["id"])
            barcode = card["barcode"]

            # Reserve card
            db.execute("""
                UPDATE raw_cards SET current_hold_id = %s WHERE id = %s
            """, (hold_id, card_id))

            # Create hold_item
            db.execute("""
                INSERT INTO hold_items (hold_id, raw_card_id, barcode, status)
                VALUES (%s, %s, %s, 'REQUESTED')
            """, (hold_id, card_id, barcode))

            assigned.append({"card_name": card_name, "condition": condition, "barcode": barcode})

    if not assigned:
        # Nothing could be assigned — cancel the hold
        db.execute("DELETE FROM holds WHERE id = %s", (hold_id,))
        return jsonify({"error": "No cards available for any requested items", "details": errors}), 409

    # Update actual item count
    db.execute("UPDATE holds SET item_count = %s WHERE id = %s", (len(assigned), hold_id))

    return jsonify({
        "success":   True,
        "hold_id":   hold_id,
        "assigned":  len(assigned),
        "requested": total_qty,
        "warnings":  errors,
        "items":     assigned,
    })


@app.route("/api/hold/<hold_id>")
def get_hold(hold_id):
    hold = db.query_one("SELECT * FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Hold not found"}), 404
    items = db.query("""
        SELECT hi.*, rc.card_name, rc.set_name, rc.condition,
               rc.current_price, rc.card_number, rc.image_url
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.hold_id = %s
        ORDER BY hi.created_at
    """, (hold_id,))
    return jsonify({
        "hold":  _ser(dict(hold)),
        "items": [_ser(dict(i)) for i in items],
    })


def _ser(d: dict) -> dict:
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif hasattr(v, "__float__"):
            d[k] = float(v)
    return d


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5005)), debug=False)
