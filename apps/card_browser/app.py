"""
card-browser — cards.pack-fresh.com
Internal kiosk browser for raw card inventory.

Reads raw_cards + storage_locations. No writes day 1.
Images served directly from TCGPlayer CDN (stable URLs, no caching needed).
"""

import os
import logging
from flask import Flask, render_template, request, jsonify

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)

PAGE_SIZE = 48


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/cards")
def list_cards():
    """
    Search/filter raw cards.
    Query params: q, set, condition, bin, state, page
    """
    q         = (request.args.get("q") or "").strip()
    set_name  = (request.args.get("set") or "").strip()
    condition = (request.args.get("condition") or "").strip().upper()
    bin_label = (request.args.get("bin") or "").strip().upper()
    state     = (request.args.get("state") or "STORED").strip().upper()
    page      = max(1, int(request.args.get("page", 1)))
    offset    = (page - 1) * PAGE_SIZE

    filters = ["rc.state = %s"]
    params  = [state]

    if q:
        filters.append("(rc.card_name ILIKE %s OR rc.set_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    if set_name:
        filters.append("rc.set_name ILIKE %s")
        params.append(f"%{set_name}%")
    if condition:
        filters.append("rc.condition = %s")
        params.append(condition)
    if bin_label:
        filters.append("sl.bin_label ILIKE %s")
        params.append(f"%{bin_label}%")

    where = " AND ".join(filters)

    count_row = db.query_one(f"""
        SELECT COUNT(*) AS total
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE {where}
    """, tuple(params))
    total = count_row["total"] if count_row else 0

    cards = db.query(f"""
        SELECT
            rc.id, rc.barcode, rc.card_name, rc.set_name,
            rc.card_number, rc.condition, rc.rarity,
            rc.is_graded, rc.grade_company, rc.grade_value,
            rc.variant, rc.language,
            rc.state, rc.current_price, rc.cost_basis,
            rc.image_url,
            sl.bin_label, sl.card_type,
            rc.created_at
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE {where}
        ORDER BY rc.created_at DESC
        LIMIT %s OFFSET %s
    """, tuple(params) + (PAGE_SIZE, offset))

    return jsonify({
        "cards":    [_serialize(c) for c in cards],
        "total":    total,
        "page":     page,
        "pages":    max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
        "per_page": PAGE_SIZE,
    })


@app.route("/api/cards/<barcode>")
def get_card(barcode):
    card = db.query_one("""
        SELECT rc.*, sl.bin_label, sl.card_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Not found"}), 404
    return jsonify(_serialize(card))


@app.route("/api/bins")
def list_bins():
    """Bin occupancy summary — for filter UI."""
    card_type = request.args.get("card_type")
    if card_type:
        rows = db.query("""
            SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count
            FROM storage_locations sl
            WHERE sl.card_type = %s AND sl.current_count > 0
            ORDER BY sl.bin_label ASC
        """, (card_type,))
    else:
        rows = db.query("""
            SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count
            FROM storage_locations sl
            WHERE sl.current_count > 0
            ORDER BY sl.bin_label ASC
        """)
    return jsonify({"bins": [dict(r) for r in rows]})


@app.route("/api/sets")
def list_sets():
    """Distinct set names for filter UI."""
    rows = db.query("""
        SELECT DISTINCT set_name FROM raw_cards
        WHERE state = 'STORED' AND set_name IS NOT NULL
        ORDER BY set_name ASC
        LIMIT 200
    """)
    return jsonify({"sets": [r["set_name"] for r in rows]})


@app.route("/api/stats")
def stats():
    row = db.query_one("""
        SELECT
            COUNT(*) FILTER (WHERE state='STORED')        AS stored,
            COUNT(*) FILTER (WHERE state='PULLED')        AS pulled,
            COUNT(*) FILTER (WHERE state='PENDING_SALE')  AS pending_sale,
            COUNT(*) FILTER (WHERE state='REMOVED')       AS removed,
            SUM(current_price) FILTER (WHERE state='STORED') AS total_value
        FROM raw_cards
    """)
    return jsonify(dict(row) if row else {})


def _serialize(row) -> dict:
    d = dict(row)
    # Convert decimals/dates to JSON-safe types
    for k in ("current_price", "cost_basis"):
        if d.get(k) is not None:
            d[k] = float(d[k])
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat()
    return d


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5004)), debug=False)
