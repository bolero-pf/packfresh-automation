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
from storage import assign_bins, assign_display_case, get_display_case_capacity, get_binder_capacity
from barcode_gen import generate_barcode_image
from price_rounding import charm_ceil_raw
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
# Price-check is a public, read-only kiosk page (cards.pack-fresh.com/price-check/).
# It exposes only name/set/condition/price/image for whatever a customer scans —
# no holds, no PII, no edit. Whitelisted from JWT auth via public_paths.
register_auth_hooks(
    app,
    public_paths=('/health', '/ping', '/favicon.ico',
                  '/price-check', '/price-check/', '/api/price-check'),
)  # any authenticated user otherwise


# ═══════════════════════════════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ═══════════════════════════════════════════════════════════════════════════════
# Public price-check kiosk (no auth, read-only)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/price-check")
@app.route("/price-check/")
def price_check_page():
    """Public, customer-facing barcode scanner. Renders only name/set/
    condition/price/image for the scanned item. No hold queue, no PII,
    no auth — whitelisted in register_auth_hooks above."""
    return render_template("price_check.html")


@app.route("/api/price-check")
def price_check_api():
    """Resolve a barcode against our owned inventory and return price/image.
    Order of resolution:
      1. raw_cards.barcode (a Pack Fresh-printed single)
      2. Shopify variant by barcode OR sku (sealed inventory we sell)
    Anything else → 404 'Not found'. We do NOT consult external pricing
    sources here — if it's not in our DB / Shopify, we don't sell it."""
    barcode = (request.args.get("barcode") or "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    # 1) Singles — raw_cards keyed by our printed barcode.
    raw = db.query_one("""
        SELECT rc.card_name, rc.set_name, rc.card_number, rc.condition,
               rc.current_price, rc.image_url, rc.scrydex_id, rc.tcgplayer_id,
               rc.variant
        FROM raw_cards rc
        WHERE rc.barcode = %s
        LIMIT 1
    """, (barcode,))

    if raw:
        # Image fallback to scrydex cache (matches editor behaviour).
        image_url = raw["image_url"]
        sid = raw["scrydex_id"]
        tcg = raw["tcgplayer_id"]
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

        # Price is charm-ceiled to .99 so the customer-facing number matches
        # what kiosks/POS would actually charge.
        price = charm_ceil_raw(raw.get("current_price") or 0)

        title_parts = [raw["card_name"]]
        if raw.get("card_number"): title_parts.append(f"#{raw['card_number']}")
        return jsonify({
            "kind":         "raw",
            "title":        " ".join(title_parts),
            "set_name":     raw.get("set_name") or "",
            "condition":    raw.get("condition") or "",
            "image_url":    image_url,
            "price":        float(price) if price is not None else None,
            "source_label": "Pack Fresh single",
        })

    # 2) Sealed — Shopify variant lookup by barcode OR sku.
    if SHOPIFY_STORE and SHOPIFY_TOKEN:
        try:
            from shopify_graphql import shopify_gql
            query = """
            query LookupVariant($q: String!) {
              productVariants(first: 1, query: $q) {
                edges { node {
                  id sku barcode title price
                  product {
                    title handle status
                    featuredImage { url }
                    images(first: 1) { edges { node { url } } }
                  }
                } }
              }
            }
            """
            # Shopify supports `barcode:` and `sku:` filters on productVariants;
            # OR-combine them so customers can scan either side of a sealed item.
            esc_bc = barcode.replace('"', '\\"')
            q = f'barcode:"{esc_bc}" OR sku:"{esc_bc}"'
            data = shopify_gql(query, {"q": q})
            edges = (data.get("data", {})
                         .get("productVariants", {})
                         .get("edges", []) or [])
            if edges:
                v = edges[0]["node"]
                p = v.get("product") or {}
                if p.get("status") == "ACTIVE":  # don't surface drafts/archives
                    img = (p.get("featuredImage") or {}).get("url")
                    if not img:
                        ie = ((p.get("images") or {}).get("edges") or [])
                        if ie:
                            img = ie[0]["node"]["url"]
                    title = p.get("title") or "Sealed product"
                    if v.get("title") and v["title"].lower() != "default title":
                        title = f"{title} — {v['title']}"
                    price_raw = v.get("price")
                    try:
                        price_val = float(price_raw) if price_raw is not None else None
                    except (TypeError, ValueError):
                        price_val = None
                    return jsonify({
                        "kind":         "sealed",
                        "title":        title,
                        "set_name":     "",
                        "condition":    "",
                        "image_url":    img,
                        "price":        price_val,
                        "source_label": "Sealed product",
                    })
        except Exception as e:
            logger.warning(f"price-check Shopify lookup failed for {barcode}: {e}")
            # fall through to 404 — don't expose the error to the public page

    return jsonify({"error": "Not found"}), 404


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


@app.route("/api/badges")
def sidebar_badges():
    """Cheap counts for the sidebar nav badges + a "newest hold timestamp"
    so the client can detect new arrivals and play a notify sound without
    re-rendering the whole queue. Polled from every view."""
    row = db.query_one("""
        SELECT
          (SELECT COUNT(*) FROM holds
             WHERE status = 'PENDING'
               AND NOT (cohort = 'champion' AND checkout_status = 'pending')
          ) AS holds,
          (SELECT COUNT(*) FROM raw_cards WHERE state = 'PENDING_RETURN') AS returns,
          (SELECT COUNT(*) FROM raw_cards WHERE state = 'MISSING')        AS missing,
          (SELECT COUNT(*) FROM raw_cards WHERE state = 'PENDING_SALE')   AS active_listings,
          (SELECT MAX(created_at) FROM holds
             WHERE status IN ('PENDING','PULLING','READY')
               AND NOT (cohort = 'champion' AND checkout_status = 'pending')
          ) AS latest_hold_at
    """)
    latest = row.get("latest_hold_at") if row else None
    return jsonify({
        "holds":           int(row["holds"] or 0),
        "returns":         int(row["returns"] or 0),
        "missing":         int(row["missing"] or 0),
        "active_listings": int(row["active_listings"] or 0),
        "latest_hold_at":  latest.isoformat() if latest else None,
    })


@app.route("/api/holds/<hold_id>")
def get_hold(hold_id):
    """Hold detail with optimized pull list."""
    hold = db.query_one("SELECT * FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404

    # LEFT JOIN raw_cards so sealed/slab items (raw_card_id IS NULL) survive.
    # Sealed/slab items carry their own title/image/sku/unit_price on hold_items;
    # raw items pull those fields from raw_cards.
    items = db.query("""
        SELECT hi.id AS hold_item_id, hi.status AS item_status,
               hi.item_kind, hi.barcode, hi.pulled_at, hi.resolved_at,
               hi.shopify_product_id, hi.shopify_variant_id,
               hi.sku AS hi_sku, hi.title AS hi_title,
               hi.image_url AS hi_image_url, hi.unit_price AS hi_unit_price,
               rc.id AS card_id, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.tcgplayer_id,
               rc.image_url AS rc_image_url, rc.state AS card_state,
               sl.bin_label
        FROM hold_items hi
        LEFT JOIN raw_cards rc ON hi.raw_card_id = rc.id
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE hi.hold_id = %s
        ORDER BY hi.item_kind NULLS FIRST,
                 sl.bin_label NULLS LAST,
                 COALESCE(rc.card_name, hi.title)
    """, (hold_id,))

    # Normalize the row shape so the frontend can render raw and sealed/slab
    # the same way — kind-aware where it matters, but with consistent keys.
    norm_items = []
    for raw in items:
        d = dict(raw)
        kind = (d.get("item_kind") or "raw").lower()
        if kind in ("sealed", "slab"):
            d["card_name"]   = d.get("hi_title") or "(Untitled product)"
            d["image_url"]   = d.get("hi_image_url")
            d["current_price"] = d.get("hi_unit_price")
            d["sku"]         = d.get("hi_sku")
            d["set_name"]    = None
            d["card_number"] = None
            d["condition"]   = None
            d["tcgplayer_id"] = None
        else:
            d["image_url"] = d.get("rc_image_url")
            d["sku"]       = None
        norm_items.append(d)

    # Build optimized pull list FROM RAW ITEMS ONLY — the bin-grouping logic
    # is meaningless for sealed/slab (no bins, no condition matching).
    pull_groups = {}
    for item in norm_items:
        if (item.get("item_kind") or "raw") != "raw":
            continue
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

    # Separate sealed/slab items so the frontend can render them as a
    # discrete "Sealed / Slabs" section in the hold detail.
    product_items = [i for i in norm_items if (i.get("item_kind") or "raw") != "raw"]

    return jsonify({
        "hold":          _ser(dict(hold)),
        "items":         [_ser(dict(i)) for i in norm_items],
        "pull_list":     pull_list,
        "product_items": [_ser(dict(i)) for i in product_items],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Hold Status Transitions
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/holds/<hold_id>/cancel", methods=["POST"])
def cancel_hold(hold_id):
    """
    Cancel a hold outright — "leave the cards there, just close this out".

    Releases any raw_cards lock held by REQUESTED/PULLED items (state goes
    back to STORED so they're immediately available again) and marks those
    items + the hold as CANCELLED.

    Refuses if any item is ACCEPTED — those have Shopify draft listings,
    so the user must reverse-decision each one first to delete the listing.
    Items already in terminal states (REJECTED, MISSING, EXPIRED_UNCLAIMED,
    SOLD, RETURNED, UNRESOLVED, CANCELLED) are left untouched; only the hold
    itself flips to CANCELLED so it drops off the active queue.

    Idempotent on already-cancelled holds.
    """
    hold = db.query_one("SELECT id, status FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404
    if hold["status"] == "CANCELLED":
        return jsonify({"success": True, "status": "CANCELLED", "noop": True})

    accepted = db.query_one("""
        SELECT COUNT(*)::int AS n FROM hold_items
        WHERE hold_id = %s AND status = 'ACCEPTED'
    """, (hold_id,))
    if (accepted or {}).get("n"):
        return jsonify({
            "error": "Hold has accepted items with active listings. "
                     "Use 'Return' on each accepted card before cancelling."
        }), 409

    # Release raw_card locks for any non-terminal raw items
    db.execute("""
        UPDATE raw_cards
           SET state = 'STORED', current_hold_id = NULL
         WHERE id IN (
             SELECT raw_card_id FROM hold_items
              WHERE hold_id = %s
                AND raw_card_id IS NOT NULL
                AND status IN ('REQUESTED','PULLED')
         )
    """, (hold_id,))

    # Mark the cancellable items as CANCELLED — both raw and sealed/slab.
    db.execute("""
        UPDATE hold_items
           SET status = 'CANCELLED', resolved_at = CURRENT_TIMESTAMP
         WHERE hold_id = %s
           AND status IN ('REQUESTED','PULLED')
    """, (hold_id,))

    # Mark the hold itself
    db.execute("UPDATE holds SET status = 'CANCELLED' WHERE id = %s", (hold_id,))

    return jsonify({"success": True, "status": "CANCELLED"})


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
        # Not a raw card. Try sealed/slab — match by SKU on this hold's
        # REQUESTED **or EXPIRED_UNCLAIMED** product items. Including expired
        # recovers the case where the customer came back after the 15-min
        # cleanup cron flipped a sitting REQUESTED item to EXPIRED_UNCLAIMED;
        # sealed/slab inventory was never decremented anyway, so it's safe to
        # re-pull. Status goes straight to PULLED.
        prod_match = db.query_one("""
            SELECT hi.id, hi.title, hi.item_kind, hi.shopify_variant_id, hi.status
            FROM hold_items hi
            WHERE hi.hold_id = %s
              AND hi.status IN ('REQUESTED','EXPIRED_UNCLAIMED')
              AND hi.item_kind IN ('sealed','slab')
              AND hi.sku = %s
            LIMIT 1
        """, (hold_id, barcode))
        if prod_match:
            db.execute("""
                UPDATE hold_items
                SET status = 'PULLED',
                    pulled_at = CURRENT_TIMESTAMP,
                    barcode = %s
                WHERE id = %s
            """, (barcode, str(prod_match["id"])))
            return jsonify({
                "success":      True,
                "kind":         prod_match["item_kind"],
                "card_name":    prod_match["title"],
                "hold_item_id": str(prod_match["id"]),
            })
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

    # Mark this hold_item as PULLED, lock in the EXACT physical copy that was
    # scanned. We update both `barcode` (display) AND `raw_card_id` (FK) so
    # every downstream action — accept, reject, return, finish — operates on
    # the specific copy the user pulled, not the original allocation. Without
    # this, a hold for 4× NM Pikachu would always resolve actions back through
    # the originally-allocated raw_card_ids, even after staff scanned 4
    # different physical copies — causing "Return on row 2" to delete the
    # Shopify listing for a different copy than the one shown in the UI.
    db.execute("""
        UPDATE hold_items
        SET status = 'PULLED',
            pulled_at = CURRENT_TIMESTAMP,
            barcode = %s,
            raw_card_id = %s
        WHERE id = %s
    """, (barcode, str(scanned["id"]), str(match["id"])))

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
    Reverse a hold item decision. Works on both open and closed holds —
    keys off the raw_card's current state, not hold_items.status, so
    "I changed my mind" is always reachable from the hold detail.
    - re-accept: PENDING_RETURN → create listing → PENDING_SALE
    - return:    PENDING_SALE   → delete listing → PENDING_RETURN
    """
    item = db.query_one("""
        SELECT hi.id AS hold_item_id, hi.status AS hi_status,
               hi.shopify_product_id AS hi_product_id,
               rc.id AS card_id, rc.state AS card_state,
               rc.shopify_product_id AS rc_product_id,
               rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url,
               rc.tcgplayer_id, rc.barcode
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.id = %s AND hi.hold_id = %s
    """, (hold_item_id, hold_id))
    if not item:
        return jsonify({"error": "Hold item not found"}), 404

    action = (request.get_json() or {}).get("action", "").lower()

    if action == "re-accept":
        # Allowed from REJECTED/PENDING_RETURN, or from PULLED if the item is
        # already flagged ACCEPTED but the listing never got created (orphaned
        # by a Shopify failure during finish_hold) — we just retry the listing.
        if item["card_state"] not in ("PENDING_RETURN", "STORED", "DISPLAY", "PULLED"):
            return jsonify({"error": f"Can't re-accept — card is {item['card_state']}"}), 409
        try:
            listing = _create_raw_listing(item)
        except Exception as e:
            return jsonify({"error": f"Failed to create listing: {e}"}), 500
        db.execute("""
            UPDATE hold_items
            SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP,
                shopify_product_id = %s, shopify_variant_id = %s
            WHERE id = %s
        """, (listing["product_id"], listing["variant_id"], hold_item_id))
        db.execute("""
            UPDATE raw_cards
            SET state = 'PENDING_SALE',
                shopify_product_id = %s,
                shopify_variant_id = %s,
                current_hold_id = NULL
            WHERE id = %s
        """, (listing["product_id"], listing["variant_id"], str(item["card_id"])))
        return jsonify({"success": True, "action": "re-accepted", "product_id": listing["product_id"]})

    if action == "return":
        # PENDING_SALE: the normal undo — listing exists, delete it.
        # PULLED: orphaned ACCEPTED (finish_hold failed to create the listing).
        #   No Shopify cleanup needed; just route the card to the Return Queue.
        if item["card_state"] not in ("PENDING_SALE", "PULLED"):
            return jsonify({"error": f"Can't return — card is {item['card_state']}"}), 409
        product_id = item.get("rc_product_id") or item.get("hi_product_id")
        if product_id:
            try:
                _shopify("DELETE", f"/products/{product_id}.json")
            except Exception as e:
                logger.warning(f"Failed to delete Shopify product {product_id}: {e}")
        db.execute("""
            UPDATE hold_items
            SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP,
                shopify_product_id = NULL, shopify_variant_id = NULL
            WHERE id = %s
        """, (hold_item_id,))
        db.execute("""
            UPDATE raw_cards
            SET state = 'PENDING_RETURN',
                shopify_product_id = NULL,
                shopify_variant_id = NULL,
                current_hold_id = NULL
            WHERE id = %s
        """, (str(item["card_id"]),))
        return jsonify({"success": True, "action": "returned"})

    return jsonify({"error": f"Unknown action '{action}' (expected 're-accept' or 'return')"}), 400


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
               hi.shopify_product_id AS hi_product_id,
               hi.shopify_variant_id AS hi_variant_id,
               rc.id AS card_id, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.tcgplayer_id,
               rc.state AS card_state,
               rc.shopify_product_id AS rc_product_id
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        WHERE hi.hold_id = %s AND hi.status = 'ACCEPTED'
    """, (hold_id,))

    results  = []
    errors   = []
    skipped_already_listed = []

    # Defense-in-depth dedupe by barcode within this push: if the same physical
    # copy somehow appears on two hold_items rows (e.g. legacy pre-fix data),
    # only push it once.
    seen_barcodes = set()

    for item in accepted:
        bc = item["barcode"]

        # Skip if this hold_item already has a Shopify listing — happens when
        # the customer toggled accept→return→accept; reverseDecision created
        # the listing during the toggle, so finish_hold must not create a
        # second one. Just confirm raw_card state is PENDING_SALE and move on.
        existing_pid = item.get("hi_product_id") or item.get("rc_product_id")
        if existing_pid and item.get("card_state") == "PENDING_SALE":
            skipped_already_listed.append({
                "barcode": bc,
                "product_id": existing_pid,
                "card_name": item["card_name"],
            })
            seen_barcodes.add(bc)
            continue

        # Defense-in-depth: skip duplicate barcodes within this push pass.
        if bc in seen_barcodes:
            logger.warning(
                f"finish_hold {hold_id}: duplicate barcode {bc} "
                f"({item['card_name']}) appeared on multiple ACCEPTED hold_items — "
                f"skipping second listing. Manual cleanup may be needed."
            )
            errors.append({
                "barcode": bc,
                "error": "duplicate barcode in this hold — only listed once (manual cleanup may be needed)",
            })
            continue
        seen_barcodes.add(bc)

        try:
            listing = _create_raw_listing(item)
            db.execute("""
                UPDATE hold_items
                SET shopify_product_id = %s, shopify_variant_id = %s
                WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], str(item["hold_item_id"])))
            db.execute("""
                UPDATE raw_cards
                SET state = 'PENDING_SALE',
                    shopify_product_id = %s,
                    shopify_variant_id = %s,
                    current_hold_id = NULL
                WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], str(item["card_id"])))
            results.append({
                "barcode":     item["barcode"],
                "card_name":   item["card_name"],
                "product_id":  listing["product_id"],
                "action":      "listing_created",
            })
        except Exception as e:
            logger.exception(f"Failed to create listing for {item['barcode']}: {e}")
            errors.append({"barcode": item["barcode"], "error": str(e)})

    if skipped_already_listed:
        logger.info(
            f"finish_hold {hold_id}: skipped {len(skipped_already_listed)} item(s) "
            f"that already had Shopify listings (created via reverseDecision toggle): "
            f"{[s['barcode'] for s in skipped_already_listed]}"
        )

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

    # Close the hold. ACCEPTED if anything was listed (newly OR via toggle),
    # otherwise RETURNED.
    has_any_listing = bool(results) or bool(skipped_already_listed)
    final_status = "ACCEPTED" if has_any_listing else "RETURNED"
    db.execute("""
        UPDATE holds SET status = %s, resolved_at = CURRENT_TIMESTAMP WHERE id = %s
    """, (final_status, hold_id))

    return jsonify({
        "success":               True,
        "created":               len(results),
        "already_listed":        len(skipped_already_listed),
        "errors":                errors,
        "results":               results,
        "skipped_already_listed": skipped_already_listed,
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
    # Always charm-ceil to a .99 price so POS listings match what the kiosk
    # would have shown — current_price for DISPLAY/recently-edited cards can
    # be raw market until the nightly raw_card_updater rounds it.
    price      = charm_ceil_raw(item.get("current_price") or 0)

    payload = {
        "product": {
            "title":       title,
            "body_html":   body,
            "status":      "draft",
            "product_type": "Pokemon",
            "vendor":      "Pack Fresh",
            "images":      [{"src": item["image_url"]}] if item.get("image_url") else [],
            "variants": [{
                "price":                str(price),
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
        SELECT hi.id AS hold_item_id, hi.status, hi.raw_card_id,
               hi.shopify_product_id, hi.shopify_variant_id
        FROM hold_items hi
        WHERE hi.hold_id = %s AND hi.status IN ('PULLED', 'REQUESTED')
    """, (hold_id,))

    for item in items:
        db.execute("""
            UPDATE hold_items SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (str(item["hold_item_id"]),))
        db.execute("""
            UPDATE raw_cards
            SET state = 'PENDING_SALE',
                shopify_product_id = COALESCE(%s, shopify_product_id),
                shopify_variant_id = COALESCE(%s, shopify_variant_id),
                current_hold_id = NULL
            WHERE id = %s
        """, (item.get("shopify_product_id"), item.get("shopify_variant_id"),
              str(item["raw_card_id"])))

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
        SELECT id, barcode, card_name, COALESCE(game, 'pokemon') AS game
        FROM raw_cards WHERE barcode = ANY(%s) AND state IN ('PENDING_RETURN', 'MISSING')
    """, (list(barcodes),))

    if not cards:
        return jsonify({"error": "No scanned cards found in PENDING_RETURN state"}), 400

    # Group by game (pokemon / magic / etc.) and run one assign_bins pass per
    # type so MTG cards land in MTG rows, not Pokemon bins.
    from collections import defaultdict
    by_game = defaultdict(list)
    for c in cards:
        by_game[c["game"]].append(c)

    bin_summary = []
    errors      = []
    for game, gcards in by_game.items():
        try:
            assignments = assign_bins(game, len(gcards), db)
        except ValueError as e:
            errors.append({"game": game, "error": str(e)})
            continue

        idx = 0
        for a in assignments:
            take      = a["count"]
            batch     = gcards[idx:idx + take]
            batch_ids = [str(c["id"]) for c in batch]
            idx += take

            db.execute("""
                UPDATE raw_cards
                SET state = 'STORED', bin_id = %s, current_hold_id = NULL,
                    stored_at = CURRENT_TIMESTAMP
                WHERE id::text = ANY(%s)
            """, (a["bin_id"], batch_ids))

            bin_summary.append({
                "bin_label": a["bin_label"],
                "count":     take,
                "game":      game,
                "cards":     [c["card_name"] for c in batch][:5],
            })

    return jsonify({
        "success":     not errors,
        "stored":      sum(b["count"] for b in bin_summary),
        "assignments": bin_summary,
        "errors":      errors,
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
# Display Case — Front Glass + capacity management
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/display/cases")
def display_cases():
    """List every display_case location + the cards currently in each."""
    cases = get_display_case_capacity(db)
    out = []
    for c in cases:
        cards = db.query("""
            SELECT id, barcode, card_name, set_name, card_number, condition,
                   current_price, image_url, variant, tcgplayer_id, scrydex_id,
                   stored_at
            FROM raw_cards
            WHERE state = 'DISPLAY' AND bin_id = %s
            ORDER BY current_price DESC NULLS LAST, card_name ASC
        """, (c["id"],))
        out.append({
            "id":            str(c["id"]),
            "bin_label":     c["bin_label"],
            "capacity":      c["capacity"],
            "current_count": c["current_count"],
            "available":     c["available"],
            "cards":         [_ser(dict(r)) for r in cards],
        })
    return jsonify({"cases": out})


@app.route("/api/display/cases/<case_id>/capacity", methods=["POST"])
def display_case_capacity(case_id):
    """Edit a display case's capacity (Front Glass starts at 50; ops resize as
    physical layout changes)."""
    new_cap = (request.get_json() or {}).get("capacity")
    try:
        new_cap = int(new_cap)
    except (TypeError, ValueError):
        return jsonify({"error": "capacity must be an integer"}), 400
    if new_cap < 1 or new_cap > 1000:
        return jsonify({"error": "capacity must be between 1 and 1000"}), 400

    row = db.query_one("""
        SELECT sl.id, sl.current_count, sr.location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (case_id,))
    if not row or row["location_type"] != "display_case":
        return jsonify({"error": "Display case not found"}), 404
    if new_cap < row["current_count"]:
        return jsonify({"error": f"Capacity {new_cap} is below current count {row['current_count']} — return cards first"}), 400

    db.execute("UPDATE storage_locations SET capacity = %s WHERE id::text = %s", (new_cap, case_id))
    return jsonify({"success": True, "capacity": new_cap})


# ═══════════════════════════════════════════════════════════════════════════════
# Display Case — Set Out (suggest + scan + finalize) and Return All
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/display/set-out/suggest")
def display_suggest():
    """Score cards in storage and return the top-N as a shopping list with bin
    labels. Staff uses this as guidance, not gospel — the actual transition is
    driven by what they scan, not by this list."""
    try:
        count = max(1, min(int(request.args.get("count", 50)), 200))
    except (TypeError, ValueError):
        return jsonify({"error": "count must be an integer"}), 400

    # Pull 5x candidates so the diversity-cap pass below has room to work with.
    candidates = db.query("""
        WITH base AS (
            SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
                   rc.condition, rc.current_price, rc.image_url, rc.variant,
                   rc.tcgplayer_id, rc.scrydex_id, rc.game, sl.bin_label,
                   (SELECT MAX(rarity) FROM scrydex_price_cache spc
                    WHERE (rc.scrydex_id IS NOT NULL AND spc.scrydex_id = rc.scrydex_id)
                       OR (rc.tcgplayer_id IS NOT NULL AND spc.tcgplayer_id = rc.tcgplayer_id)
                   ) AS rarity,
                   (SELECT MAX(weight) FROM featured_cards fc
                    WHERE rc.card_name ILIKE '%%' || fc.name_pattern || '%%'
                      AND (fc.game = '*' OR fc.game = COALESCE(rc.game, 'pokemon'))
                   ) AS featured_boost
            FROM raw_cards rc
            LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
            WHERE rc.state = 'STORED'
              AND rc.current_hold_id IS NULL
              AND rc.current_price IS NOT NULL
              AND rc.current_price >= 1.0
        )
        SELECT *,
               LN(GREATEST(1, current_price)) * 5 AS price_score,
               CASE
                   WHEN rarity IS NULL THEN 0
                   WHEN rarity ILIKE '%%secret%%' OR rarity ILIKE '%%special%%' THEN 12
                   WHEN rarity ILIKE '%%hyper%%'  OR rarity ILIKE '%%illustration%%' THEN 12
                   WHEN rarity ILIKE '%%ultra%%'  OR rarity ILIKE '%%mythic%%' THEN 10
                   WHEN rarity ILIKE '%%holo%%'   OR rarity ILIKE '%%rare%%'   THEN 6
                   WHEN rarity ILIKE '%%uncommon%%' THEN 2
                   ELSE 0
               END AS rarity_score,
               COALESCE(featured_boost, 0) AS featured_score
        FROM base
        ORDER BY (LN(GREATEST(1, current_price)) * 5
                  + COALESCE(featured_boost, 0)
                  + CASE
                      WHEN rarity IS NULL THEN 0
                      WHEN rarity ILIKE '%%secret%%' OR rarity ILIKE '%%special%%' THEN 12
                      WHEN rarity ILIKE '%%hyper%%'  OR rarity ILIKE '%%illustration%%' THEN 12
                      WHEN rarity ILIKE '%%ultra%%'  OR rarity ILIKE '%%mythic%%' THEN 10
                      WHEN rarity ILIKE '%%holo%%'   OR rarity ILIKE '%%rare%%'   THEN 6
                      WHEN rarity ILIKE '%%uncommon%%' THEN 2
                      ELSE 0
                  END
                 ) DESC
        LIMIT %s
    """, (count * 5,))

    # Greedy diversity pass: ≤2 per (name, variant), ≤10 per set, condition NM/LP only.
    # Strong condition filter keeps damaged stuff out of the front glass — staff
    # can manually scan an off-list MP card if they really want one.
    chosen = []
    by_card = {}
    by_set  = {}
    for c in candidates:
        if len(chosen) >= count:
            break
        if c.get("condition") not in ("NM", "LP"):
            continue
        cv = (c["card_name"], (c.get("variant") or "").lower())
        if by_card.get(cv, 0) >= 2:
            continue
        if by_set.get(c.get("set_name") or "", 0) >= 10:
            continue
        chosen.append(c)
        by_card[cv] = by_card.get(cv, 0) + 1
        by_set[c.get("set_name") or ""] = by_set.get(c.get("set_name") or "", 0) + 1

    return jsonify({"suggestions": [_ser(dict(c)) for c in chosen], "total": len(chosen)})


@app.route("/api/display/set-out/scan", methods=["POST"])
def display_scan_set_out():
    """Validate a barcode for the Set Out flow. Card must be in storage and
    not held. Returns its current row so the UI can render it in the scan list."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.state, rc.current_hold_id, sl.bin_label
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found", "barcode": barcode}), 404
    if card["state"] != "STORED":
        return jsonify({"error": f"Card is {card['state']}, not in storage", "barcode": barcode, "card_name": card["card_name"]}), 409
    if card.get("current_hold_id"):
        return jsonify({"error": "Card is on hold for a customer", "barcode": barcode, "card_name": card["card_name"]}), 409

    return jsonify({"success": True, "card": _ser(dict(card))})


@app.route("/api/display/set-out/finalize", methods=["POST"])
def display_finalize_set_out():
    """Move every scanned card from STORED to DISPLAY at the chosen case.
    Only commits the move for cards that are still in a valid pre-state when
    the request arrives — anything that drifted (e.g. someone else placed a
    hold mid-scan) is reported back so staff can re-shelve."""
    data    = request.get_json() or {}
    barcodes = [b for b in (data.get("barcodes") or []) if b]
    case_id  = data.get("case_id")
    if not barcodes or not case_id:
        return jsonify({"error": "barcodes and case_id required"}), 400

    case = db.query_one("""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count, sr.location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (case_id,))
    if not case or case["location_type"] != "display_case":
        return jsonify({"error": "Display case not found"}), 404

    available = case["capacity"] - case["current_count"]
    if len(barcodes) > available:
        return jsonify({"error": f"Case has only {available} slots free; you scanned {len(barcodes)} cards. Increase capacity or set out fewer."}), 409

    # Atomic-ish: only update rows still STORED & unhel
    moved = db.execute("""
        UPDATE raw_cards
        SET state = 'DISPLAY', bin_id = %s, updated_at = CURRENT_TIMESTAMP
        WHERE barcode = ANY(%s) AND state = 'STORED' AND current_hold_id IS NULL
    """, (case_id, barcodes))

    return jsonify({"success": True, "moved": moved, "scanned": len(barcodes)})


@app.route("/api/display/return/scan", methods=["POST"])
def display_scan_return():
    """Validate a barcode for the Return-to-Storage flow. Card must be in DISPLAY."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.state, rc.game, sl.bin_label
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found", "barcode": barcode}), 404
    if card["state"] != "DISPLAY":
        return jsonify({"error": f"Card is {card['state']}, not on display", "barcode": barcode, "card_name": card["card_name"]}), 409

    return jsonify({"success": True, "card": _ser(dict(card))})


@app.route("/api/display/return/finalize", methods=["POST"])
def display_finalize_return():
    """Batch-assign scanned display cards back to storage bins.
    Groups by card_type so MTG cards land in MTG rows, Pokemon in Pokemon rows.
    The whole pull goes in one assignment pass per type — no A1/A2/A1 ping-pong."""
    from collections import defaultdict
    barcodes = [b for b in ((request.get_json() or {}).get("barcodes") or []) if b]
    if not barcodes:
        return jsonify({"error": "No barcodes provided"}), 400

    cards = db.query("""
        SELECT id, barcode, card_name, COALESCE(game, 'pokemon') AS game
        FROM raw_cards
        WHERE barcode = ANY(%s) AND state = 'DISPLAY'
    """, (barcodes,))
    if not cards:
        return jsonify({"error": "None of the scanned cards are currently on display"}), 400

    by_game = defaultdict(list)
    for c in cards:
        by_game[c["game"]].append(c)

    bin_summary = []
    errors = []
    for game, gcards in by_game.items():
        try:
            assignments = assign_bins(game, len(gcards), db)
        except ValueError as e:
            errors.append({"game": game, "error": str(e)})
            continue

        idx = 0
        for a in assignments:
            slice_ids = [str(gcards[idx + i]["id"]) for i in range(a["count"])]
            idx += a["count"]
            db.execute("""
                UPDATE raw_cards
                SET state = 'STORED', bin_id = %s, current_hold_id = NULL,
                    stored_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id::text = ANY(%s)
            """, (a["bin_id"], slice_ids))
            bin_summary.append({
                "bin_label": a["bin_label"],
                "count":     a["count"],
                "game":      game,
                "cards":     [c["card_name"] for c in gcards if str(c["id"]) in set(slice_ids)][:5],
            })

    return jsonify({
        "success":     not errors,
        "stored":      sum(b["count"] for b in bin_summary),
        "assignments": bin_summary,
        "errors":      errors,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Sell (POS) — scan stream → batch Shopify draft listings → mark PENDING_SALE
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/sell/scan", methods=["POST"])
def sell_scan():
    """Validate a barcode for the Sell flow. Card must be physically present
    and not already committed elsewhere (no holds, not already mid-sale)."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.tcgplayer_id, rc.state, rc.current_hold_id, sl.bin_label,
               sr.location_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        LEFT JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found", "barcode": barcode}), 404
    if card.get("current_hold_id"):
        return jsonify({"error": "Card is on hold for a customer", "barcode": barcode, "card_name": card["card_name"]}), 409
    # Sellable from anywhere physically in the store. PENDING_SALE means
    # someone else already started ringing it up; PENDING_RETURN/MISSING/GONE
    # are not currently holdable.
    if card["state"] not in ("STORED", "DISPLAY"):
        return jsonify({"error": f"Card is {card['state']}, can't ring up", "barcode": barcode, "card_name": card["card_name"]}), 409

    return jsonify({"success": True, "card": _ser(dict(card))})


@app.route("/api/sell/finalize", methods=["POST"])
def sell_finalize():
    """Finalize an in-store sale: for every scanned barcode, create a Shopify
    draft listing (SKU = barcode) and mark the card PENDING_SALE. The Shopify
    POS device then rings each card up by scanning the same SKU; the existing
    orders/create webhook flips PENDING_SALE → SOLD on payment."""
    if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
        return jsonify({"error": "Shopify not configured"}), 503

    barcodes = [b for b in ((request.get_json() or {}).get("barcodes") or []) if b]
    if not barcodes:
        return jsonify({"error": "No barcodes provided"}), 400

    cards = db.query("""
        SELECT id, barcode, card_name, set_name, card_number, condition,
               current_price, image_url, tcgplayer_id, state, current_hold_id
        FROM raw_cards
        WHERE barcode = ANY(%s)
    """, (barcodes,))

    found_by_barcode = {c["barcode"]: c for c in cards}
    listings = []
    skipped  = []

    for bc in barcodes:
        card = found_by_barcode.get(bc)
        if not card:
            skipped.append({"barcode": bc, "reason": "not found"})
            continue
        if card.get("current_hold_id") or card["state"] not in ("STORED", "DISPLAY"):
            skipped.append({"barcode": bc, "reason": f"state={card['state']}"})
            continue

        try:
            listing = _create_raw_listing(dict(card))
            db.execute("""
                UPDATE raw_cards
                SET state = 'PENDING_SALE',
                    shopify_product_id = %s,
                    shopify_variant_id = %s,
                    current_hold_id = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], str(card["id"])))
            listings.append({
                "barcode":    bc,
                "card_name":  card["card_name"],
                "condition":  card["condition"],
                "price":      charm_ceil_raw(card.get("current_price") or 0),
                "product_id": listing["product_id"],
                "variant_id": listing["variant_id"],
                "title":      listing["title"],
            })
        except Exception as e:
            logger.exception(f"Sell finalize failed for {bc}: {e}")
            skipped.append({"barcode": bc, "reason": str(e)})

    return jsonify({
        "success":  bool(listings),
        "listings": listings,
        "skipped":  skipped,
        "total":    sum(l["price"] for l in listings),
    })


@app.route("/api/sell/active")
def sell_active_listings():
    """Every PENDING_SALE card with an active Shopify draft listing.
    Surfaced in the Sell tab so the front-of-house person can see what's
    waiting on the register and pull a listing if a customer changes
    their mind — no need to dig back into a closed hold."""
    rows = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.shopify_product_id, rc.shopify_variant_id, rc.updated_at,
               h.id AS hold_id, h.customer_name AS hold_customer,
               hi.id AS hold_item_id
        FROM raw_cards rc
        LEFT JOIN hold_items hi ON hi.raw_card_id = rc.id AND hi.status = 'ACCEPTED'
        LEFT JOIN holds h ON hi.hold_id = h.id
        WHERE rc.state = 'PENDING_SALE'
        ORDER BY rc.updated_at DESC NULLS LAST
    """)
    return jsonify({"cards": [_ser(dict(r)) for r in rows]})


@app.route("/api/sell/pull-listing", methods=["POST"])
def sell_pull_listing():
    """Customer changed their mind — yank the active Shopify listing and
    send the card to the Return Queue so it gets re-shelved on next pass.
    Works whether the card was listed via finish_hold or sell/finalize."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT id, state, shopify_product_id, card_name
        FROM raw_cards WHERE barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found"}), 404
    if card["state"] != "PENDING_SALE":
        return jsonify({"error": f"Card is {card['state']}, not PENDING_SALE"}), 409

    product_id = card.get("shopify_product_id")
    if not product_id:
        # Fall back to the hold_items linkage if the column wasn't backfilled
        hi = db.query_one("""
            SELECT shopify_product_id FROM hold_items
            WHERE raw_card_id = %s AND status = 'ACCEPTED'
              AND shopify_product_id IS NOT NULL
            ORDER BY resolved_at DESC NULLS LAST LIMIT 1
        """, (str(card["id"]),))
        product_id = hi and hi.get("shopify_product_id")

    if product_id:
        try:
            _shopify("DELETE", f"/products/{product_id}.json")
        except Exception as e:
            logger.warning(f"Failed to delete Shopify product {product_id}: {e}")

    db.execute("""
        UPDATE hold_items
        SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP,
            shopify_product_id = NULL, shopify_variant_id = NULL
        WHERE raw_card_id = %s AND status = 'ACCEPTED'
    """, (str(card["id"]),))
    db.execute("""
        UPDATE raw_cards
        SET state = 'PENDING_RETURN',
            shopify_product_id = NULL,
            shopify_variant_id = NULL,
            current_hold_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (str(card["id"]),))
    return jsonify({"success": True, "card_name": card["card_name"]})


@app.route("/api/sell/relist", methods=["POST"])
def sell_relist():
    """Customer rejected, then changed their mind — pull the card out of
    the Return Queue and create a fresh Shopify listing so it's ready to
    ring up. Works on any PENDING_RETURN card; no hold required."""
    if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
        return jsonify({"error": "Shopify not configured"}), 503

    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT id, barcode, card_name, set_name, card_number, condition,
               current_price, image_url, tcgplayer_id, state
        FROM raw_cards WHERE barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found"}), 404
    if card["state"] != "PENDING_RETURN":
        return jsonify({"error": f"Card is {card['state']}, not PENDING_RETURN"}), 409

    try:
        listing = _create_raw_listing(dict(card))
    except Exception as e:
        return jsonify({"error": f"Failed to create listing: {e}"}), 500

    db.execute("""
        UPDATE raw_cards
        SET state = 'PENDING_SALE',
            shopify_product_id = %s,
            shopify_variant_id = %s,
            current_hold_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (listing["product_id"], listing["variant_id"], str(card["id"])))
    # Reattach to the most recent hold_items row if there is one, so the
    # closed hold's history reflects the flip.
    db.execute("""
        UPDATE hold_items
        SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP,
            shopify_product_id = %s, shopify_variant_id = %s
        WHERE id = (
            SELECT id FROM hold_items
            WHERE raw_card_id = %s
            ORDER BY resolved_at DESC NULLS LAST LIMIT 1
        )
    """, (listing["product_id"], listing["variant_id"], str(card["id"])))
    return jsonify({"success": True, "card_name": card["card_name"],
                    "product_id": listing["product_id"], "title": listing.get("title")})


# ═══════════════════════════════════════════════════════════════════════════════
# Binders — list, fill (suggest+scan+finalize), pull (scan+batch-bin)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/binders")
def list_binders():
    """List every binder with its current contents and capacity meter."""
    binders = get_binder_capacity(db)
    out = []
    for b in binders:
        cards = db.query("""
            SELECT id, barcode, card_name, set_name, card_number, condition,
                   current_price, image_url, variant, tcgplayer_id, scrydex_id,
                   game, stored_at
            FROM raw_cards
            WHERE state = 'DISPLAY' AND bin_id = %s
            ORDER BY card_name ASC
        """, (b["id"],))
        out.append({
            "id":            str(b["id"]),
            "bin_label":     b["bin_label"],
            "capacity":      b["capacity"],
            "current_count": b["current_count"],
            "available":     b["available"],
            "cards":         [_ser(dict(r)) for r in cards],
        })
    return jsonify({"binders": out})


@app.route("/api/binders/<binder_id>/fill-suggest")
def binder_fill_suggest(binder_id):
    """Suggest STORED cards to add to a specific binder. Same scoring as
    set-out but with two binder-specific rules:
      - Cap ≤2 per (name, variant) counting what's *already in this binder*
        plus the suggestions, so a binder with 2 Charizards never gets a
        third recommendation.
      - Honors the 'how many' input but never exceeds the binder's slot count."""
    try:
        count = max(1, min(int(request.args.get("count", 50)), 480))
    except (TypeError, ValueError):
        return jsonify({"error": "count must be an integer"}), 400

    binder = db.query_one("""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available, sr.location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (binder_id,))
    if not binder or binder["location_type"] != "binder":
        return jsonify({"error": "Binder not found"}), 404

    count = min(count, binder["available"])
    if count <= 0:
        return jsonify({"suggestions": [], "total": 0, "binder": _ser(dict(binder))})

    # Existing contents — used to seed the by_card cap and avoid re-suggesting
    # cards that are already at quota in this binder.
    existing = db.query("""
        SELECT card_name, COALESCE(LOWER(variant), '') AS variant
        FROM raw_cards WHERE state = 'DISPLAY' AND bin_id = %s
    """, (binder_id,))

    candidates = db.query("""
        WITH base AS (
            SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
                   rc.condition, rc.current_price, rc.image_url, rc.variant,
                   rc.tcgplayer_id, rc.scrydex_id, rc.game, sl.bin_label,
                   (SELECT MAX(rarity) FROM scrydex_price_cache spc
                    WHERE (rc.scrydex_id IS NOT NULL AND spc.scrydex_id = rc.scrydex_id)
                       OR (rc.tcgplayer_id IS NOT NULL AND spc.tcgplayer_id = rc.tcgplayer_id)
                   ) AS rarity,
                   (SELECT MAX(weight) FROM featured_cards fc
                    WHERE rc.card_name ILIKE '%%' || fc.name_pattern || '%%'
                      AND (fc.game = '*' OR fc.game = COALESCE(rc.game, 'pokemon'))
                   ) AS featured_boost
            FROM raw_cards rc
            LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
            WHERE rc.state = 'STORED'
              AND rc.current_hold_id IS NULL
              AND rc.current_price IS NOT NULL
              AND rc.current_price >= 1.0
        )
        SELECT *,
               LN(GREATEST(1, current_price)) * 5 AS price_score,
               CASE
                   WHEN rarity IS NULL THEN 0
                   WHEN rarity ILIKE '%%secret%%' OR rarity ILIKE '%%special%%' THEN 12
                   WHEN rarity ILIKE '%%hyper%%'  OR rarity ILIKE '%%illustration%%' THEN 12
                   WHEN rarity ILIKE '%%ultra%%'  OR rarity ILIKE '%%mythic%%' THEN 10
                   WHEN rarity ILIKE '%%holo%%'   OR rarity ILIKE '%%rare%%'   THEN 6
                   WHEN rarity ILIKE '%%uncommon%%' THEN 2
                   ELSE 0
               END AS rarity_score,
               COALESCE(featured_boost, 0) AS featured_score
        FROM base
        ORDER BY (LN(GREATEST(1, current_price)) * 5
                  + COALESCE(featured_boost, 0)
                  + CASE
                      WHEN rarity IS NULL THEN 0
                      WHEN rarity ILIKE '%%secret%%' OR rarity ILIKE '%%special%%' THEN 12
                      WHEN rarity ILIKE '%%hyper%%'  OR rarity ILIKE '%%illustration%%' THEN 12
                      WHEN rarity ILIKE '%%ultra%%'  OR rarity ILIKE '%%mythic%%' THEN 10
                      WHEN rarity ILIKE '%%holo%%'   OR rarity ILIKE '%%rare%%'   THEN 6
                      WHEN rarity ILIKE '%%uncommon%%' THEN 2
                      ELSE 0
                  END
                 ) DESC
        LIMIT %s
    """, (count * 5,))

    by_card = {}
    for e in existing:
        cv = (e["card_name"], e["variant"] or "")
        by_card[cv] = by_card.get(cv, 0) + 1

    chosen = []
    by_set = {}
    for c in candidates:
        if len(chosen) >= count:
            break
        if c.get("condition") not in ("NM", "LP", "MP"):
            continue  # binders are customer-touch; keep DMG/HP out
        cv = (c["card_name"], (c.get("variant") or "").lower())
        if by_card.get(cv, 0) >= 2:
            continue
        if by_set.get(c.get("set_name") or "", 0) >= 30:
            continue  # softer set cap than display case (binders hold more)
        chosen.append(c)
        by_card[cv] = by_card.get(cv, 0) + 1
        by_set[c.get("set_name") or ""] = by_set.get(c.get("set_name") or "", 0) + 1

    return jsonify({
        "suggestions": [_ser(dict(c)) for c in chosen],
        "total":       len(chosen),
        "binder":      _ser(dict(binder)),
    })


@app.route("/api/binders/<binder_id>/fill/scan", methods=["POST"])
def binder_fill_scan(binder_id):
    """Same as display set-out/scan — validate the card is in storage and
    available, return its row for the scan list."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.state, rc.current_hold_id, sl.bin_label
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found", "barcode": barcode}), 404
    if card["state"] != "STORED":
        return jsonify({"error": f"Card is {card['state']}, not in storage", "barcode": barcode, "card_name": card["card_name"]}), 409
    if card.get("current_hold_id"):
        return jsonify({"error": "Card is on hold for a customer", "barcode": barcode, "card_name": card["card_name"]}), 409

    return jsonify({"success": True, "card": _ser(dict(card))})


@app.route("/api/binders/<binder_id>/fill/finalize", methods=["POST"])
def binder_fill_finalize(binder_id):
    """Move every scanned card into this binder. Capacity-guarded."""
    barcodes = [b for b in ((request.get_json() or {}).get("barcodes") or []) if b]
    if not barcodes:
        return jsonify({"error": "No barcodes provided"}), 400

    binder = db.query_one("""
        SELECT sl.id, sl.capacity, sl.current_count, sr.location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (binder_id,))
    if not binder or binder["location_type"] != "binder":
        return jsonify({"error": "Binder not found"}), 404

    available = binder["capacity"] - binder["current_count"]
    if len(barcodes) > available:
        return jsonify({"error": f"Binder has only {available} slots free; you scanned {len(barcodes)}. Pull cards out first or pick a different binder."}), 409

    moved = db.execute("""
        UPDATE raw_cards
        SET state = 'DISPLAY', bin_id = %s, updated_at = CURRENT_TIMESTAMP
        WHERE barcode = ANY(%s) AND state = 'STORED' AND current_hold_id IS NULL
    """, (binder_id, barcodes))

    return jsonify({"success": True, "moved": moved, "scanned": len(barcodes)})


@app.route("/api/binders/pull/scan", methods=["POST"])
def binder_pull_scan():
    """Validate a barcode for the Pull-from-Binder flow. Card must currently
    live in any binder (state=DISPLAY, location_type=binder)."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    card = db.query_one("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url, rc.variant,
               rc.state, rc.game, sl.bin_label, sr.location_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        LEFT JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE rc.barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found", "barcode": barcode}), 404
    if card["state"] != "DISPLAY" or card.get("location_type") != "binder":
        return jsonify({"error": f"Card is not currently in a binder (state={card['state']})", "barcode": barcode, "card_name": card["card_name"]}), 409

    return jsonify({"success": True, "card": _ser(dict(card))})


@app.route("/api/binders/pull/finalize", methods=["POST"])
def binder_pull_finalize():
    """Batch-move scanned binder cards back to storage. Same group-by-game +
    one-assign_bins-pass-per-type pattern as the display Return All flow."""
    from collections import defaultdict
    barcodes = [b for b in ((request.get_json() or {}).get("barcodes") or []) if b]
    if not barcodes:
        return jsonify({"error": "No barcodes provided"}), 400

    cards = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, COALESCE(rc.game, 'pokemon') AS game
        FROM raw_cards rc
        JOIN storage_locations sl ON rc.bin_id = sl.id
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE rc.barcode = ANY(%s) AND rc.state = 'DISPLAY' AND sr.location_type = 'binder'
    """, (barcodes,))
    if not cards:
        return jsonify({"error": "None of the scanned cards are currently in a binder"}), 400

    by_game = defaultdict(list)
    for c in cards:
        by_game[c["game"]].append(c)

    bin_summary = []
    errors = []
    for game, gcards in by_game.items():
        try:
            assignments = assign_bins(game, len(gcards), db)
        except ValueError as e:
            errors.append({"game": game, "error": str(e)})
            continue

        idx = 0
        for a in assignments:
            slice_ids = [str(gcards[idx + i]["id"]) for i in range(a["count"])]
            idx += a["count"]
            db.execute("""
                UPDATE raw_cards
                SET state = 'STORED', bin_id = %s, current_hold_id = NULL,
                    stored_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id::text = ANY(%s)
            """, (a["bin_id"], slice_ids))
            bin_summary.append({
                "bin_label": a["bin_label"],
                "count":     a["count"],
                "game":      game,
                "cards":     [c["card_name"] for c in gcards if str(c["id"]) in set(slice_ids)][:5],
            })

    return jsonify({
        "success":     not errors,
        "stored":      sum(b["count"] for b in bin_summary),
        "assignments": bin_summary,
        "errors":      errors,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Featured Cards — Set Out / Fill Binder scoring boost (multi-IP)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/featured-cards")
def list_featured():
    rows = db.query("""
        SELECT id, name_pattern, game, weight, notes, created_at
        FROM featured_cards
        ORDER BY game, name_pattern
    """)
    return jsonify({"cards": [_ser(dict(r)) for r in rows]})


@app.route("/api/featured-cards", methods=["POST"])
def create_featured():
    data = request.get_json() or {}
    pattern = (data.get("name_pattern") or "").strip()
    game    = (data.get("game") or "*").strip().lower()
    weight  = data.get("weight", 50)
    notes   = (data.get("notes") or "").strip() or None

    if not pattern:
        return jsonify({"error": "name_pattern is required"}), 400
    try:
        weight = int(weight)
    except (TypeError, ValueError):
        return jsonify({"error": "weight must be an integer"}), 400

    row = db.execute_returning("""
        INSERT INTO featured_cards (name_pattern, game, weight, notes)
        VALUES (%s, %s, %s, %s)
        RETURNING id, name_pattern, game, weight, notes, created_at
    """, (pattern, game, weight, notes))
    return jsonify(_ser(dict(row)))


@app.route("/api/featured-cards/<int:card_id>", methods=["DELETE"])
def delete_featured(card_id):
    db.execute("DELETE FROM featured_cards WHERE id = %s", (card_id,))
    return jsonify({"success": True})


@app.route("/api/featured-cards/<int:card_id>", methods=["POST"])
def update_featured(card_id):
    data = request.get_json() or {}
    fields = []
    params = []
    for col in ("name_pattern", "game", "weight", "notes"):
        if col in data:
            fields.append(f"{col} = %s")
            params.append(data[col])
    if not fields:
        return jsonify({"error": "No fields to update"}), 400
    params.append(card_id)
    db.execute(f"UPDATE featured_cards SET {', '.join(fields)} WHERE id = %s", tuple(params))
    return jsonify({"success": True})


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

    # Editor grid covers everything physically in the store (bins + binders +
    # display cases). PULLED/PENDING_*/MISSING are still reachable by direct
    # barcode search, but the grid stays focused on cards that are saleable.
    filters = ["state IN ('STORED', 'DISPLAY')", "current_hold_id IS NULL"]
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


@app.route("/api/editor/lookup-barcode")
def editor_lookup_barcode():
    """Resolve a barcode scan straight to the editor-card shape so the
    editor's scan-to-edit input can open the detail modal in one round
    trip. 404 if the barcode isn't in raw_cards at all."""
    barcode = (request.args.get("barcode") or "").strip()
    if not barcode:
        return jsonify({"error": "No barcode provided"}), 400

    row = db.query_one("""
        SELECT card_name, set_name, tcgplayer_id, scrydex_id,
               variant AS variant_raw,
               CASE WHEN variant IS NULL OR LOWER(variant) IN ('normal','holofoil')
                    THEN '' ELSE variant END AS variant_key,
               image_url
        FROM raw_cards
        WHERE barcode = %s
        LIMIT 1
    """, (barcode,))
    if not row:
        return jsonify({"error": "No card with that barcode"}), 404

    # Image fallback to scrydex cache (matches editor_search behaviour).
    image_url = row["image_url"]
    sid = row["scrydex_id"]
    tcg = row["tcgplayer_id"]
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

    variant_raw   = (row.get("variant_raw") or "").strip()
    variant_label = variant_raw if variant_raw and variant_raw.lower() not in ("normal", "holofoil") else None

    return jsonify({
        "card": {
            "card_name":     row["card_name"],
            "set_name":      row["set_name"],
            "tcgplayer_id":  row["tcgplayer_id"],
            "scrydex_id":    row["scrydex_id"],
            "variant_key":   row["variant_key"] or "",
            "variant_label": variant_label,
            "image_url":     image_url,
        },
        "barcode": barcode,
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
    raw_market = _lookup_cache_price(sx_id, tcg_id, card["condition"], variant)
    new_price  = charm_ceil_raw(raw_market) if raw_market is not None else None

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

    raw_market = _lookup_cache_price(card["scrydex_id"], card["tcgplayer_id"], new_cond, card.get("variant"))
    new_price  = charm_ceil_raw(raw_market) if raw_market is not None else None

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

    raw_market = _lookup_cache_price(card["scrydex_id"], card["tcgplayer_id"], card["condition"], new_variant)
    new_price  = charm_ceil_raw(raw_market) if raw_market is not None else None

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

    raw_market = _lookup_cache_price(card["scrydex_id"], card["tcgplayer_id"], card["condition"], card.get("variant"))
    if raw_market is None:
        return jsonify({"error": "No cached price found for this card+condition+variant"}), 404
    new_price = charm_ceil_raw(raw_market)

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
