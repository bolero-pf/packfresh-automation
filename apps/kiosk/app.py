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

# Era mapping — groups set names by TCG era for browsing filters
# Sets are matched by prefix/keyword. If a set doesn't match any era, it goes to "Classic".
ERA_KEYWORDS = {
    "Scarlet & Violet": ["Scarlet & Violet", "Paldea", "Obsidian Flames", "151",
                         "Paradox Rift", "Temporal Forces", "Twilight Masque",
                         "Shrouded Fable", "Stellar Crown", "Surging Sparks",
                         "Prismatic Evolutions", "Journey Together", "Destined Rivals"],
    "Sword & Shield": ["Sword & Shield", "Rebel Clash", "Darkness Ablaze", "Vivid Voltage",
                        "Battle Styles", "Chilling Reign", "Evolving Skies", "Fusion Strike",
                        "Brilliant Stars", "Astral Radiance", "Lost Origin", "Silver Tempest",
                        "Crown Zenith", "Shining Fates", "Champion's Path", "Hidden Fates"],
    "Sun & Moon": ["Sun & Moon", "Guardians Rising", "Burning Shadows", "Crimson Invasion",
                   "Ultra Prism", "Forbidden Light", "Celestial Storm", "Lost Thunder",
                   "Team Up", "Unbroken Bonds", "Unified Minds", "Cosmic Eclipse",
                   "Detective Pikachu"],
    "XY": ["XY", "Flashfire", "Furious Fists", "Phantom Forces", "Primal Clash",
            "Roaring Skies", "Ancient Origins", "BREAKthrough", "BREAKpoint",
            "Fates Collide", "Steam Siege", "Evolutions", "Generations"],
}

def _classify_era(set_name: str) -> str:
    if not set_name:
        return "Classic"
    sn = set_name.strip()
    for era, keywords in ERA_KEYWORDS.items():
        for kw in keywords:
            if sn.lower().startswith(kw.lower()) or kw.lower() in sn.lower():
                return era
    return "Classic"



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
    q          = (request.args.get("q") or "").strip()
    set_name   = (request.args.get("set") or "").strip()
    conditions = [c.strip().upper() for c in (request.args.get("condition") or "").split(",") if c.strip()]
    min_price  = request.args.get("min_price", type=float)
    max_price  = request.args.get("max_price", type=float)
    era        = (request.args.get("era") or "").strip()
    sort       = (request.args.get("sort") or "name_asc").strip()
    page       = max(1, int(request.args.get("page", 1)))
    offset     = (page - 1) * 24

    filters = ["state = 'STORED'", "current_hold_id IS NULL"]
    params  = []

    if q:
        filters.append("(card_name ILIKE %s OR set_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    if set_name:
        filters.append("set_name ILIKE %s")
        params.append(f"%{set_name}%")
    if conditions:
        valid = [c for c in conditions if c in ("NM", "LP", "MP", "HP", "DMG")]
        if valid:
            cph = ",".join(["%s"] * len(valid))
            filters.append(f"condition IN ({cph})")
            params += valid
    if min_price is not None:
        filters.append("current_price >= %s")
        params.append(min_price)
    if max_price is not None:
        filters.append("current_price <= %s")
        params.append(max_price)
    if era:
        era_kws = ERA_KEYWORDS.get(era)
        if era_kws:
            era_clauses = " OR ".join(["set_name ILIKE %s"] * len(era_kws))
            filters.append(f"({era_clauses})")
            params += [f"%{kw}%" for kw in era_kws]

    where = " AND ".join(filters)

    # Sort mapping
    sort_map = {
        "name_asc": "card_name ASC",
        "price_asc": "min_price ASC NULLS LAST",
        "price_desc": "max_price DESC NULLS LAST",
        "newest": "MAX(created_at) DESC",
    }
    order_by = sort_map.get(sort, "card_name ASC")

    # Count distinct cards (not individual copies)
    count_row = db.query_one(f"""
        SELECT COUNT(DISTINCT (card_name, set_name, tcgplayer_id)) AS total
        FROM raw_cards
        WHERE {where}
          AND current_hold_id IS NULL
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
            MAX(created_at) AS created_at,
            jsonb_object_agg(condition, cond_qty) AS conditions
        FROM (
            SELECT card_name, set_name, tcgplayer_id, image_url,
                   condition, COUNT(*) AS cond_qty, MIN(current_price) AS current_price,
                   MAX(created_at) AS created_at
            FROM raw_cards
            WHERE {where}
              AND state = 'STORED'
            GROUP BY card_name, set_name, tcgplayer_id, image_url, condition
        ) sub
        GROUP BY card_name, set_name, tcgplayer_id
        ORDER BY {order_by}
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


@app.route("/api/eras")
def list_eras():
    """Return available eras based on sets currently in stock."""
    rows = db.query("""
        SELECT DISTINCT set_name FROM raw_cards
        WHERE state = 'STORED' AND set_name IS NOT NULL
    """)
    era_counts = {}
    for r in rows:
        era = _classify_era(r["set_name"])
        era_counts[era] = era_counts.get(era, 0) + 1
    # Return eras sorted by name, with set counts
    eras = [{"name": k, "set_count": v} for k, v in sorted(era_counts.items())]
    return jsonify({"eras": eras})


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
        WHERE card_name = %s AND set_name = %s AND state = 'STORED' AND current_hold_id IS NULL
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
    assigned = []
    errors   = []

    # Resolve which cards to hold before opening transaction
    lines_resolved = []
    for line in items:
        card_name = line.get("card_name", "")
        set_name  = line.get("set_name", "")
        condition = line.get("condition", "NM")
        qty       = max(1, int(line.get("qty", 1)))

        available = db.query("""
            SELECT id, barcode FROM raw_cards
            WHERE card_name = %s AND set_name = %s
              AND condition = %s AND state = 'STORED'
              AND current_hold_id IS NULL
            ORDER BY created_at ASC
            LIMIT %s
        """, (card_name, set_name, condition, qty))

        if not available:
            errors.append(f"No {condition} copies available for {card_name}")
            continue
        if len(available) < qty:
            errors.append(f"Only {len(available)} {condition} {card_name} available (requested {qty})")

        for card in available:
            lines_resolved.append({
                "card_name": card_name, "set_name": set_name,
                "condition": condition,
                "card_id": str(card["id"]), "barcode": card["barcode"],
            })

    if not lines_resolved:
        return jsonify({"error": "No cards available for any requested items", "details": errors}), 409

    # Single transaction: create hold + reserve cards + create hold_items
    with db.get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                from psycopg2.extras import RealDictCursor
                cur = conn.cursor(cursor_factory=RealDictCursor)

                cur.execute("""
                    INSERT INTO holds (customer_name, customer_phone, status, item_count)
                    VALUES (%s, %s, 'PENDING', %s) RETURNING id
                """, (name, phone or None, len(lines_resolved)))
                hold_id = str(cur.fetchone()["id"])

                for r in lines_resolved:
                    cur.execute("""
                        UPDATE raw_cards SET current_hold_id = %s WHERE id = %s
                    """, (hold_id, r["card_id"]))
                    cur.execute("""
                        INSERT INTO hold_items (hold_id, raw_card_id, barcode, status)
                        VALUES (%s, %s, %s, 'REQUESTED')
                    """, (hold_id, r["card_id"], r["barcode"]))
                    assigned.append({"card_name": r["card_name"], "condition": r["condition"], "barcode": r["barcode"]})

                conn.commit()
        except Exception:
            conn.rollback()
            raise

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
