"""
kiosk — kiosk.pack-fresh.com
Customer-facing card browse + hold request system.

Two cohorts:
  1. Guests (in-store) — browse + hold requests, staff pulls cards, pay at register
  2. Champions (VIP3, remote) — browse + checkout via Shopify Storefront API cart

Read-only on raw_cards. Writes to holds + hold_items.
Cards are aggregated by (card_name, set_name) with qty per condition.
Max 20 cards per hold.
"""

import os
import hmac
import hashlib
import base64
import logging
import time
import threading
import requests as _requests
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Response, render_template

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()

MAX_HOLD_ITEMS = 20
HOLD_EXPIRY_HOURS = 2
CHAMPION_HOLD_MINUTES = 30

# Shopify Admin API (for product creation + customer lookup)
SHOPIFY_STORE   = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN   = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = os.environ.get("SHOPIFY_VERSION", "2025-01")

# Shopify Storefront API (for cart creation)
SHOPIFY_STOREFRONT_TOKEN = os.environ.get("SHOPIFY_STOREFRONT_TOKEN", "")

# Webhook verification
SHOPIFY_WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")

# Shipping threshold for Champions — below this, they pay shipping
KIOSK_FREE_SHIP_THRESHOLD = float(os.environ.get("KIOSK_FREE_SHIP_THRESHOLD", "200"))
KIOSK_FREE_SHIP_CODE = os.environ.get("KIOSK_FREE_SHIP_CODE", "CHAMPION_RAW_FREESHIP")

# Cleanup endpoint auth
CLEANUP_SECRET = os.environ.get("CLEANUP_SECRET", "")

# Shopify publication ID for the Kiosk headless channel (set in Railway env)
KIOSK_PUBLICATION_ID = os.environ.get("KIOSK_PUBLICATION_ID", "")

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


# ═══════════════════════════════════════════════════════════════════════════════
# Shopify Admin API helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _shopify_rest(method, path, **kwargs):
    """Shopify Admin REST API call with retry."""
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    for attempt in range(4):
        try:
            r = _requests.request(method, url, headers=headers, timeout=30, **kwargs)
            r.raise_for_status()
            return r.json() if r.content else {}
        except (_requests.Timeout, _requests.ConnectionError, _requests.HTTPError) as e:
            if attempt >= 3:
                raise
            if hasattr(e, 'response') and e.response is not None and e.response.status_code < 500:
                raise
            time.sleep(1.0 * (1.5 ** attempt))


def _shopify_gql(query, variables=None):
    """Shopify Admin GraphQL call (for customer lookup + publication)."""
    from shopify_graphql import shopify_gql
    return shopify_gql(query, variables)


# ═══════════════════════════════════════════════════════════════════════════════
# Champion Identification
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/champion/identify", methods=["POST"])
def champion_identify():
    """
    Look up a Shopify customer by email and verify they have VIP3 tag.
    No auth layer — just identity check. They still must log in at Shopify checkout.
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "Email required"}), 400

    if not SHOPIFY_TOKEN or not SHOPIFY_STORE:
        return jsonify({"error": "Shopify not configured"}), 503

    result = _shopify_gql("""
        query($query: String!) {
          customers(first: 1, query: $query) {
            edges {
              node {
                id
                firstName
                lastName
                email
                tags
              }
            }
          }
        }
    """, {"query": f"email:{email}"})

    edges = result.get("data", {}).get("customers", {}).get("edges", [])
    if not edges:
        return jsonify({"verified": False, "reason": "No account found with that email"}), 200

    customer = edges[0]["node"]
    tags = [t.strip().upper() for t in (customer.get("tags") or [])]
    if "VIP3" not in tags:
        return jsonify({"verified": False, "reason": "Only Champions can check out online"}), 200

    return jsonify({
        "verified": True,
        "first_name": customer.get("firstName") or "",
        "customer_gid": customer["id"],
        "email": customer.get("email") or email,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Champion Checkout
# ═══════════════════════════════════════════════════════════════════════════════

def _create_kiosk_product(card, hold_id):
    """
    Create a real ACTIVE Shopify product for a raw card,
    published only to the Kiosk headless channel.
    Returns {"product_id": ..., "variant_id": ..., "variant_gid": ...}
    """
    condition_labels = {
        "NM": "Near Mint", "LP": "Lightly Played",
        "MP": "Moderately Played", "HP": "Heavily Played", "DMG": "Damaged"
    }
    cond_label = condition_labels.get(card["condition"], card["condition"])
    card_num = f" #{card['card_number']}" if card.get("card_number") else ""
    title = f"{card['card_name']}{card_num} [{cond_label}]"
    body = (f"<p>{card['card_name']}{card_num}</p>"
            f"<p>Set: {card.get('set_name', '')}</p>"
            f"<p>Condition: {cond_label}</p>")
    price = float(card.get("current_price") or 0)

    payload = {
        "product": {
            "title": title,
            "body_html": body,
            "status": "active",
            "product_type": "Raw Card",
            "vendor": "Pack Fresh",
            "tags": f"kiosk-raw,kiosk-hold-{hold_id}",
            "published": False,  # Don't auto-publish; we'll publish to Kiosk channel only
            "images": [{"src": card["image_url"]}] if card.get("image_url") else [],
            "variants": [{
                "price": str(round(price, 2)),
                "sku": card["barcode"],
                "barcode": card["barcode"],
                "inventory_management": "shopify",
                "inventory_quantity": 1,
                "requires_shipping": True,
            }],
        }
    }

    result = _shopify_rest("POST", "/products.json", json=payload)
    product = result["product"]
    product_id = product["id"]
    variant_id = product["variants"][0]["id"]

    # Publish ONLY to Kiosk headless channel (product was created unpublished)
    if KIOSK_PUBLICATION_ID:
        product_gid = f"gid://shopify/Product/{product_id}"
        _shopify_gql("""
            mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
              publishablePublish(id: $id, input: $input) {
                userErrors { field message }
              }
            }
        """, {
            "id": product_gid,
            "input": [{"publicationId": KIOSK_PUBLICATION_ID}],
        })

    return {
        "product_id": product_id,
        "variant_id": variant_id,
        "variant_gid": f"gid://shopify/ProductVariant/{variant_id}",
        "title": title,
    }


@app.route("/api/checkout", methods=["POST"])
def champion_checkout():
    """
    Champion checkout flow:
    1. Verify VIP3 status
    2. Create hold (lock cards)
    3. Create real Shopify products (active, Kiosk channel only)
    4. Create Storefront API cart → get checkout URL
    5. Return checkout URL to frontend
    """
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    customer_gid = (data.get("customer_gid") or "").strip()
    items = data.get("items") or []

    if not email or not customer_gid:
        return jsonify({"error": "Champion email and customer_gid required"}), 400
    if not items:
        return jsonify({"error": "No items in checkout"}), 400

    if not SHOPIFY_STOREFRONT_TOKEN:
        return jsonify({"error": "Storefront API not configured"}), 503

    total_qty = sum(int(i.get("qty", 1)) for i in items)
    if total_qty > MAX_HOLD_ITEMS:
        return jsonify({"error": f"Maximum {MAX_HOLD_ITEMS} cards per checkout"}), 400

    # ── Step 1: Re-verify Champion ──────────────────────────────────────────
    verify_result = _shopify_gql("""
        query($id: ID!) {
          customer(id: $id) { tags }
        }
    """, {"id": customer_gid})
    tags = [t.strip().upper() for t in (
        verify_result.get("data", {}).get("customer", {}).get("tags") or []
    )]
    if "VIP3" not in tags:
        return jsonify({"error": "Only Champions can check out online"}), 403

    # ── Step 2: Resolve available cards (same as create_hold) ───────────────
    lines_resolved = []
    errors = []
    for line in items:
        card_name = line.get("card_name", "")
        set_name = line.get("set_name", "")
        condition = line.get("condition", "NM")
        qty = max(1, int(line.get("qty", 1)))

        available = db.query("""
            SELECT id, barcode, card_name, set_name, card_number,
                   condition, current_price, image_url
            FROM raw_cards
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
            lines_resolved.append(dict(card))

    if not lines_resolved:
        return jsonify({"error": "No cards available", "details": errors}), 409

    # ── Step 3: Create hold + lock cards ────────────────────────────────────
    with db.get_conn() as conn:
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                from psycopg2.extras import RealDictCursor
                cur = conn.cursor(cursor_factory=RealDictCursor)

                cur.execute("""
                    INSERT INTO holds
                        (customer_name, customer_phone, status, item_count,
                         cohort, customer_email, shopify_customer_gid, checkout_status)
                    VALUES (%s, NULL, 'PENDING', %s, 'champion', %s, %s, 'pending')
                    RETURNING id
                """, (email, len(lines_resolved), email, customer_gid))
                hold_id = str(cur.fetchone()["id"])

                for card in lines_resolved:
                    cur.execute("""
                        UPDATE raw_cards SET current_hold_id = %s WHERE id = %s
                    """, (hold_id, card["id"]))
                    cur.execute("""
                        INSERT INTO hold_items (hold_id, raw_card_id, barcode, status)
                        VALUES (%s, %s, %s, 'REQUESTED')
                    """, (hold_id, card["id"], card["barcode"]))

                conn.commit()
        except Exception:
            conn.rollback()
            raise

    # ── Step 4: Create Shopify products ─────────────────────────────────────
    variant_gids = []
    cart_total = 0
    try:
        for card in lines_resolved:
            listing = _create_kiosk_product(card, hold_id)
            variant_gids.append(listing["variant_gid"])
            cart_total += float(card.get("current_price") or 0)

            # Store product/variant IDs on hold_items for cleanup
            db.execute("""
                UPDATE hold_items
                SET shopify_product_id = %s, shopify_variant_id = %s
                WHERE hold_id = %s AND raw_card_id = %s
            """, (str(listing["product_id"]), str(listing["variant_id"]),
                  hold_id, card["id"]))
    except Exception as e:
        logger.error(f"Failed to create Shopify products for hold {hold_id}: {e}")
        # Clean up: release the hold
        _cleanup_hold(hold_id)
        return jsonify({"error": "Failed to create checkout products"}), 500

    # ── Step 5: Create Storefront API cart ──────────────────────────────────
    discount_codes = []
    if cart_total >= KIOSK_FREE_SHIP_THRESHOLD and KIOSK_FREE_SHIP_CODE:
        discount_codes.append(KIOSK_FREE_SHIP_CODE)

    try:
        from shopify_storefront import create_cart
        checkout_url = create_cart(variant_gids, email, discount_codes or None)
    except Exception as e:
        logger.error(f"Failed to create Storefront cart for hold {hold_id}: {e}")
        _cleanup_hold(hold_id)
        return jsonify({"error": "Failed to create checkout"}), 500

    # Store checkout URL on hold
    db.execute("UPDATE holds SET checkout_url = %s WHERE id = %s", (checkout_url, hold_id))

    logger.info(f"Champion checkout: hold={hold_id} email={email} items={len(lines_resolved)} total=${cart_total:.2f}")

    return jsonify({
        "success": True,
        "hold_id": hold_id,
        "checkout_url": checkout_url,
        "item_count": len(lines_resolved),
        "cart_total": round(cart_total, 2),
        "free_shipping": cart_total >= KIOSK_FREE_SHIP_THRESHOLD,
        "warnings": errors,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Order Webhook — close the loop when Champion pays
# ═══════════════════════════════════════════════════════════════════════════════

def _verify_shopify_webhook(data: bytes, hmac_header: str) -> bool:
    """Verify Shopify webhook HMAC-SHA256 signature."""
    if not SHOPIFY_WEBHOOK_SECRET:
        return False
    digest = hmac.new(SHOPIFY_WEBHOOK_SECRET.encode(), data, hashlib.sha256).digest()
    computed = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed, hmac_header)


@app.route("/api/webhooks/order-paid", methods=["POST"])
def webhook_order_paid():
    """
    Shopify orders/create webhook.
    When a Champion completes checkout, find the kiosk hold and mark it PAID.
    """
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not _verify_shopify_webhook(request.get_data(), hmac_header):
        return jsonify({"error": "Invalid signature"}), 401

    order = request.get_json(silent=True) or {}
    line_items = order.get("line_items", [])

    # Find kiosk-raw items by checking product tags
    hold_ids = set()
    for item in line_items:
        # Shopify includes tags as a comma-separated string on the product
        tags = (item.get("properties") or [])
        # Also check via product_id against our hold_items
        variant_id = item.get("variant_id")
        if variant_id:
            hold_item = db.query_one("""
                SELECT hi.hold_id FROM hold_items hi
                JOIN holds h ON hi.hold_id = h.id
                WHERE hi.shopify_variant_id = %s AND h.cohort = 'champion'
            """, (str(variant_id),))
            if hold_item:
                hold_ids.add(str(hold_item["hold_id"]))

    if not hold_ids:
        return jsonify({"ok": True, "kiosk": False}), 200

    # Extract order info for staff fulfillment
    shopify_order_id = order.get("id")
    order_number = order.get("name") or f"#{order.get('order_number', '')}"
    shipping = order.get("shipping_address") or {}
    shipping_name = f"{shipping.get('first_name', '')} {shipping.get('last_name', '')}".strip()
    if not shipping_name:
        shipping_name = f"{order.get('customer', {}).get('first_name', '')} {order.get('customer', {}).get('last_name', '')}".strip()
    shipping_addr = ", ".join(filter(None, [
        shipping.get("address1"), shipping.get("address2"),
        shipping.get("city"), shipping.get("province_code"),
        shipping.get("zip"), shipping.get("country"),
    ]))

    for hold_id in hold_ids:
        db.execute("""
            UPDATE holds
            SET checkout_status = 'completed', status = 'PENDING',
                customer_name = %s, shopify_order_number = %s,
                shipping_name = %s, shipping_address = %s
            WHERE id = %s AND cohort = 'champion' AND checkout_status = 'pending'
        """, (shipping_name or order_number, order_number,
              shipping_name, shipping_addr, hold_id))
        logger.info(f"Champion order paid: hold={hold_id} order={order_number} ship_to={shipping_name}")

    return jsonify({"ok": True, "kiosk": True, "holds": list(hold_ids)}), 200


# ═══════════════════════════════════════════════════════════════════════════════
# Cleanup — expire abandoned Champion holds
# ═══════════════════════════════════════════════════════════════════════════════

def _cleanup_hold(hold_id):
    """Release cards and delete Shopify products for an abandoned hold."""
    # Get hold items with Shopify product IDs
    items = db.query("""
        SELECT raw_card_id, shopify_product_id FROM hold_items WHERE hold_id = %s
    """, (hold_id,))

    # Delete Shopify products
    for item in items:
        pid = item.get("shopify_product_id")
        if pid:
            try:
                _shopify_rest("DELETE", f"/products/{pid}.json")
            except Exception as e:
                logger.warning(f"Failed to delete Shopify product {pid}: {e}")

    # Release cards back to STORED
    db.execute("""
        UPDATE raw_cards SET current_hold_id = NULL, state = 'STORED'
        WHERE current_hold_id = %s
    """, (hold_id,))

    # Mark hold abandoned
    db.execute("""
        UPDATE holds SET checkout_status = 'abandoned', status = 'ABANDONED'
        WHERE id = %s
    """, (hold_id,))

    # Clean up hold_items
    db.execute("DELETE FROM hold_items WHERE hold_id = %s", (hold_id,))

    logger.info(f"Cleaned up abandoned Champion hold {hold_id}")


@app.route("/api/cleanup/abandoned", methods=["POST"])
def cleanup_abandoned():
    """
    Expire Champion holds that haven't been paid within CHAMPION_HOLD_MINUTES.
    Called by Railway cron every 10 minutes.
    """
    auth = request.headers.get("Authorization", "")
    if CLEANUP_SECRET and auth != f"Bearer {CLEANUP_SECRET}":
        return jsonify({"error": "Unauthorized"}), 401

    cutoff = datetime.utcnow() - timedelta(minutes=CHAMPION_HOLD_MINUTES)
    expired = db.query("""
        SELECT id FROM holds
        WHERE cohort = 'champion'
          AND checkout_status = 'pending'
          AND created_at < %s
    """, (cutoff,))

    cleaned = 0
    for hold in expired:
        _cleanup_hold(str(hold["id"]))
        cleaned += 1

    if cleaned:
        logger.info(f"Cleanup: expired {cleaned} abandoned Champion hold(s)")

    return jsonify({"cleaned": cleaned})


@app.route("/health")
def health():
    return "ok"


# ── Background cleanup: expire abandoned Champion holds every 10 min ─────────
def _cleanup_loop():
    while True:
        time.sleep(600)
        try:
            cutoff = datetime.utcnow() - timedelta(minutes=CHAMPION_HOLD_MINUTES)
            expired = db.query("""
                SELECT id FROM holds
                WHERE cohort = 'champion'
                  AND checkout_status = 'pending'
                  AND created_at < %s
            """, (cutoff,))
            for hold in expired:
                _cleanup_hold(str(hold["id"]))
            if expired:
                logger.info(f"Background cleanup: expired {len(expired)} abandoned Champion hold(s)")
        except Exception as e:
            logger.warning(f"Background cleanup error: {e}")

_cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True)
_cleanup_thread.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5005)), debug=False)
