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
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, jsonify, g

import db
from storage import (assign_bins, assign_display_case, get_display_case_capacity,
                     get_binder_capacity, infer_card_type_from_set,
                     _canonical_card_type)
from barcode_gen import generate_barcode_image, generate_barcode_id
from price_rounding import charm_ceil_raw
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
db.init_pool()

_ACTIVE_HOLD_STATUSES = ("PENDING", "PULLING", "READY")


def _ensure_columns():
    """Defensive idempotent column add for inventory_product_cache.barcode.
    The column is owned by shared/cache_manager.py, but the scan endpoint
    references it directly — if card_manager deploys before inventory's
    cache_manager runs the migration, the JOIN would 500. Cheap to retry."""
    try:
        db.execute(
            "ALTER TABLE inventory_product_cache "
            "ADD COLUMN IF NOT EXISTS barcode VARCHAR(200)"
        )
    except Exception as e:
        logger.debug(f"inventory_product_cache.barcode migration skipped ({e})")


def _ensure_price_check_tables():
    """Lookup log for the customer price-check kiosk. Powers the
    'Hot right now' (global, last hour, recency-weighted) and 'Recently
    checked' (per-device, last 24h) sections on /price-check."""
    try:
        db.execute("""
            CREATE TABLE IF NOT EXISTS price_check_lookups (
                id            BIGSERIAL PRIMARY KEY,
                device_id     VARCHAR(64),
                identifier    VARCHAR(300) NOT NULL,
                kind          VARCHAR(20),
                title         VARCHAR(500),
                set_name      VARCHAR(200),
                image_url     TEXT,
                price         NUMERIC(10,2),
                looked_up_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_pcl_device_time
            ON price_check_lookups (device_id, looked_up_at DESC)
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_pcl_time
            ON price_check_lookups (looked_up_at DESC)
        """)
    except Exception as e:
        logger.warning(f"price_check_lookups migration skipped ({e})")


def _heal_stale_hold_locks():
    """Clear current_hold_id on any raw_card whose referenced hold is in a
    terminal status. These accumulate when staff scan a sibling copy during
    pulling — the kiosk-allocated row keeps its lock forever, and the kiosk
    browse filter (`current_hold_id IS NULL`) hides it from customers. Cheap
    one-shot fix on boot; no-op once the table is clean."""
    try:
        n = db.execute("""
            UPDATE raw_cards rc
            SET current_hold_id = NULL
            FROM holds h
            WHERE rc.current_hold_id = h.id
              AND h.status NOT IN ('PENDING','PULLING','READY')
        """)
        if n:
            logger.info(f"_heal_stale_hold_locks: cleared {n} stale current_hold_id row(s)")
    except Exception as e:
        logger.warning(f"_heal_stale_hold_locks failed: {e}")


def _heal_grading_dupes():
    """Collapse the BARCODED + REMOVED+GRADING pair the ingest pipeline left
    behind when raw cards routed to grade had already been pre-barcoded. The
    REMOVED row carries a phantom never-printed barcode; the BARCODED row
    holds the physical label. Keep the BARCODED row (flipped to REMOVED), drop
    the orphan. Cheap one-shot on boot; no-op once the table is clean."""
    try:
        rows = db.query("""
            WITH bc AS (
              SELECT id, intake_item_id,
                     ROW_NUMBER() OVER (PARTITION BY intake_item_id ORDER BY created_at) AS rn
                FROM raw_cards
               WHERE intake_item_id IS NOT NULL
                 AND state IN ('BARCODED','BARCODED_STORAGE','BARCODED_DISPLAY',
                               'ROUTED_STORAGE','ROUTED_BINDER')
            ),
            rm AS (
              SELECT id, intake_item_id, removal_date,
                     ROW_NUMBER() OVER (PARTITION BY intake_item_id ORDER BY created_at) AS rn
                FROM raw_cards
               WHERE intake_item_id IS NOT NULL
                 AND state = 'REMOVED' AND removal_reason = 'GRADING'
            )
            SELECT bc.id AS barcoded_id, rm.id AS removed_id, rm.removal_date AS removal_date
              FROM bc JOIN rm ON rm.intake_item_id = bc.intake_item_id AND rm.rn = bc.rn
        """)
        if not rows:
            return
        for r in rows:
            db.execute("""
                UPDATE raw_cards
                   SET state = 'REMOVED',
                       removal_reason = 'GRADING',
                       removal_date = COALESCE(%s, CURRENT_TIMESTAMP),
                       bin_id = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id::text = %s
            """, (r.get("removal_date"), str(r["barcoded_id"])))
            db.execute("DELETE FROM raw_cards WHERE id::text = %s", (str(r["removed_id"]),))
        logger.info(f"_heal_grading_dupes: collapsed {len(rows)} BARCODED/REMOVED grading pair(s)")
    except Exception as e:
        logger.warning(f"_heal_grading_dupes failed: {e}")


_ensure_columns()
_ensure_price_check_tables()
_heal_stale_hold_locks()
_heal_grading_dupes()

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


def _resolve_hold_lock(card: dict) -> bool:
    """Return True if the card is genuinely held by an active hold.
    If `current_hold_id` references a hold in a terminal state (the lock is
    stale — usually from sibling-substitution during pulling), clear it inline
    and return False. Used by every "is this card available?" scan endpoint."""
    hid = card.get("current_hold_id")
    if not hid:
        return False
    hold = db.query_one("SELECT status FROM holds WHERE id = %s", (hid,))
    if hold and hold["status"] in ("PENDING", "PULLING", "READY"):
        return True
    db.execute(
        "UPDATE raw_cards SET current_hold_id = NULL WHERE id = %s",
        (str(card["id"]),),
    )
    card["current_hold_id"] = None
    return False


def _ser(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if isinstance(v, datetime):
            # Postgres TIMESTAMP (without time zone) columns come back as
            # naive datetimes even though the values are UTC. Bare isoformat
            # then drops the zone marker, and the JS frontend parses naive
            # ISO as *local* time — turning a 10:14 AM hold into a 3:14 PM
            # display. Tag naive as UTC so clients render in their own zone.
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            out[k] = v.isoformat()
        elif hasattr(v, "isoformat"):
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
                  '/price-check', '/price-check/',
                  '/api/price-check', '/api/price-check/search',
                  '/api/price-check/log', '/api/price-check/recent'),
)  # any authenticated user otherwise


# ═══════════════════════════════════════════════════════════════════════════════
# Pages
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html", standalone=None)


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


@app.route("/api/price-check/search")
def price_check_search():
    """Free-text fallback for the price-check page when the input doesn't match
    a barcode. Returns up to ~12 hits combining: (a) raw_cards grouped by
    identity (so all conditions of the same card collapse into one row with a
    price range), and (b) ACTIVE Shopify products by title. Same response
    shape as /api/price-check so the frontend can reuse the result renderer."""
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify({"results": []})

    like = f"%{q}%"
    raw_rows = db.query("""
        SELECT card_name,
               set_name,
               card_number,
               MAX(image_url) AS image_url,
               MAX(scrydex_id) AS scrydex_id,
               MAX(tcgplayer_id) AS tcgplayer_id,
               MIN(current_price) AS min_price,
               MAX(current_price) AS max_price,
               COUNT(*) AS qty,
               array_agg(DISTINCT condition) AS conditions
        FROM raw_cards
        WHERE state IN ('STORED','DISPLAY')
          AND current_hold_id IS NULL
          AND current_price IS NOT NULL
          AND (card_name ILIKE %s
               OR set_name ILIKE %s
               OR card_number ILIKE %s)
        GROUP BY card_name, set_name, card_number
        ORDER BY MIN(current_price) DESC NULLS LAST
        LIMIT 8
    """, (like, like, like))

    results = []
    for r in raw_rows:
        # Image fallback to scrydex cache (matches the barcode lookup path).
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

        title_parts = [r["card_name"]]
        if r.get("card_number"):
            title_parts.append(f"#{r['card_number']}")
        min_p = charm_ceil_raw(r["min_price"] or 0)
        max_p = charm_ceil_raw(r["max_price"] or 0)
        results.append({
            "kind":         "raw",
            "title":        " ".join(title_parts),
            "set_name":     r.get("set_name") or "",
            "condition":    "",
            "conditions":   list(r.get("conditions") or []),
            "image_url":    image_url,
            "price":        float(min_p) if min_p is not None else None,
            "price_max":    float(max_p) if max_p is not None else None,
            "qty":          int(r["qty"] or 0),
            "source_label": "Pack Fresh single",
        })

    # Shopify active products by title — best-effort. Any failure here just
    # means we return raw-only results; the page still works.
    if SHOPIFY_STORE and SHOPIFY_TOKEN:
        try:
            from shopify_graphql import shopify_gql
            query = """
            query SearchProducts($q: String!) {
              products(first: 8, query: $q) {
                edges { node {
                  title handle status
                  featuredImage { url }
                  images(first: 1) { edges { node { url } } }
                  variants(first: 1) { edges { node { price } } }
                } }
              }
            }
            """
            esc_q = q.replace('"', '\\"')
            data = shopify_gql(query, {"q": f'title:*{esc_q}* AND status:active'})
            edges = (data.get("data", {})
                         .get("products", {})
                         .get("edges", []) or [])
            for edge in edges:
                p = edge["node"]
                if (p.get("status") or "").upper() != "ACTIVE":
                    continue
                img = (p.get("featuredImage") or {}).get("url")
                if not img:
                    ie = ((p.get("images") or {}).get("edges") or [])
                    if ie:
                        img = ie[0]["node"]["url"]
                ve = ((p.get("variants") or {}).get("edges") or [])
                price_val = None
                if ve:
                    try:
                        price_val = float(ve[0]["node"].get("price"))
                    except (TypeError, ValueError):
                        price_val = None
                results.append({
                    "kind":         "sealed",
                    "title":        p.get("title") or "Sealed product",
                    "set_name":     "",
                    "condition":    "",
                    "image_url":    img,
                    "price":        price_val,
                    "source_label": "Sealed product",
                })
        except Exception as e:
            logger.warning(f"price-check search Shopify lookup failed for {q!r}: {e}")

    return jsonify({"results": results[:12]})


# ═══════════════════════════════════════════════════════════════════════════════
# Price-check lookup log → Hot Right Now + Recently Checked
# ═══════════════════════════════════════════════════════════════════════════════

# How many rows each section returns.
PRICE_CHECK_HOT_LIMIT    = 6
PRICE_CHECK_RECENT_LIMIT = 10


@app.route("/api/price-check/log", methods=["POST"])
def price_check_log():
    """Append a single lookup row. Called by the kiosk after every
    successful resolve (barcode or click-from-list). No-op on invalid
    payloads — the kiosk page shouldn't fail because logging hiccupped."""
    data = request.get_json(silent=True) or {}
    identifier = (data.get("identifier") or "").strip()
    if not identifier or len(identifier) > 300:
        return jsonify({"ok": False, "error": "identifier required"}), 400

    device_id = (data.get("device_id") or "").strip()[:64] or None
    kind = (data.get("kind") or "").strip()[:20] or None
    title = (data.get("title") or "").strip()[:500] or None
    set_name = (data.get("set_name") or "").strip()[:200] or None
    image_url = (data.get("image_url") or "").strip() or None
    price_raw = data.get("price")
    try:
        price_val = float(price_raw) if price_raw is not None else None
    except (TypeError, ValueError):
        price_val = None

    try:
        db.execute("""
            INSERT INTO price_check_lookups
                   (device_id, identifier, kind, title, set_name, image_url, price)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (device_id, identifier, kind, title, set_name, image_url, price_val))
    except Exception as e:
        logger.warning(f"price_check_log insert failed: {e}")
        return jsonify({"ok": False}), 500
    return jsonify({"ok": True})


@app.route("/api/price-check/recent")
def price_check_recent():
    """Hot Right Now + Recently Checked. Both rails are global — we
    don't get enough price-check volume to justify per-device scoping,
    and a single shared rail is more useful as "what's been getting
    looked up around the store" than as one-station history."""

    # "Hot" — last hour, recency-weighted (half-life ≈ 30 min via
    # EXP(-Δseconds/1800)). HAVING COUNT > 1 filters one-off scans so the
    # rail reflects real traffic, not whoever just walked up.
    hot = db.query("""
        SELECT identifier,
               (array_agg(title       ORDER BY looked_up_at DESC))[1] AS title,
               (array_agg(set_name    ORDER BY looked_up_at DESC))[1] AS set_name,
               (array_agg(image_url   ORDER BY looked_up_at DESC))[1] AS image_url,
               (array_agg(price       ORDER BY looked_up_at DESC))[1] AS price,
               (array_agg(kind        ORDER BY looked_up_at DESC))[1] AS kind,
               COUNT(*)                                                AS hits,
               MAX(looked_up_at)                                       AS last_seen
        FROM price_check_lookups
        WHERE looked_up_at > NOW() - INTERVAL '1 hour'
        GROUP BY identifier
        HAVING COUNT(*) > 1
        ORDER BY SUM(EXP(-EXTRACT(EPOCH FROM (NOW() - looked_up_at))/1800.0)) DESC
        LIMIT %s
    """, (PRICE_CHECK_HOT_LIMIT,))

    # "Recently checked" — last 24h across all devices, most-recently-
    # touched row per identifier.
    recent = db.query("""
        SELECT identifier,
               (array_agg(title     ORDER BY looked_up_at DESC))[1] AS title,
               (array_agg(set_name  ORDER BY looked_up_at DESC))[1] AS set_name,
               (array_agg(image_url ORDER BY looked_up_at DESC))[1] AS image_url,
               (array_agg(price     ORDER BY looked_up_at DESC))[1] AS price,
               (array_agg(kind      ORDER BY looked_up_at DESC))[1] AS kind,
               MAX(looked_up_at)                                    AS last_seen
        FROM price_check_lookups
        WHERE looked_up_at > NOW() - INTERVAL '24 hours'
        GROUP BY identifier
        ORDER BY MAX(looked_up_at) DESC
        LIMIT %s
    """, (PRICE_CHECK_RECENT_LIMIT,))

    def fmt(row):
        return {
            "identifier": row["identifier"],
            "title":      row["title"],
            "set_name":   row.get("set_name"),
            "image_url":  row.get("image_url"),
            "price":      float(row["price"]) if row.get("price") is not None else None,
            "kind":       row.get("kind"),
            "hits":       int(row["hits"]) if row.get("hits") is not None else None,
            "last_seen":  row["last_seen"].isoformat() if row.get("last_seen") else None,
        }

    return jsonify({
        "hot":    [fmt(r) for r in hot],
        "recent": [fmt(r) for r in recent],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# Hold Queue API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/holds")
def list_holds():
    """Active in-store guest holds ordered by created_at.

    Champion holds live in the Shipping Queue (paid, awaiting ship) and never
    appear here — they're a fundamentally different workflow (shipped, not
    handed across a counter)."""
    status_filter = request.args.get("status", "active")
    if status_filter == "active":
        rows = db.query("""
            SELECT h.*,
                   COUNT(hi.id) FILTER (WHERE hi.item_kind = 'raw') AS total_items,
                   COUNT(hi.id) FILTER (
                       WHERE hi.item_kind = 'raw'
                         AND hi.status IN ('REJECTED','MISSING')
                   ) AS resolved_items,
                   COUNT(hi.id) FILTER (
                       WHERE hi.item_kind = 'raw' AND hi.status = 'REQUESTED'
                   ) AS pending_items,
                   COUNT(hi.id) FILTER (WHERE hi.item_kind IN ('sealed','slab')) AS sealed_items
            FROM holds h
            LEFT JOIN hold_items hi ON hi.hold_id = h.id
            WHERE h.status IN ('PENDING','PULLING','READY')
              AND h.cohort IS DISTINCT FROM 'champion'
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
             WHERE status IN ('PENDING','PULLING','READY')
               AND cohort IS DISTINCT FROM 'champion'
          ) AS holds,
          (SELECT COUNT(*) FROM raw_cards WHERE state = 'PENDING_RETURN') AS returns,
          (SELECT COUNT(*) FROM raw_cards WHERE state = 'MISSING')        AS missing,
          (SELECT COUNT(*) FROM raw_cards WHERE state = 'PENDING_SALE')   AS active_listings,
          (SELECT COUNT(*) FROM raw_cards
             WHERE state = 'REMOVED' AND removal_reason = 'GRADING'
          ) AS grading,
          (SELECT MAX(created_at) FROM holds
             WHERE status IN ('PENDING','PULLING','READY')
               AND cohort IS DISTINCT FROM 'champion'
          ) AS latest_hold_at
    """)
    latest = row.get("latest_hold_at") if row else None
    return jsonify({
        "holds":           int(row["holds"] or 0),
        "returns":         int(row["returns"] or 0),
        "missing":         int(row["missing"] or 0),
        "active_listings": int(row["active_listings"] or 0),
        "grading":         int(row["grading"] or 0),
        "latest_hold_at":  latest.isoformat() if latest else None,
    })


def _zone_center_hold(hold_id):
    """Re-allocate hold_items so the puller visits the fewest physical zones.

    Rule: for each REQUESTED raw hold_item, if a same-identity sibling lives in
    the hold's dominant zone but the currently-allocated copy doesn't, swap
    raw_card_id (and current_hold_id) to the sibling. Preferences inside ties:
    STORED over DISPLAY, then oldest created_at first (so old stock cycles out
    before CardTrader picks it up).

    Idempotent — running twice is a no-op."""
    # Heal any pre-existing drift: an older revision of this function only
    # updated raw_card_id on swap, leaving hi.barcode pointing at the original
    # copy while the bin label came from the substituted copy. Realign before
    # zone-centering so the puller (and send-to-pos) sees a consistent row.
    db.execute("""
        UPDATE hold_items hi
           SET barcode = rc.barcode
          FROM raw_cards rc
         WHERE rc.id = hi.raw_card_id
           AND hi.hold_id = %s
           AND hi.item_kind = 'raw'
           AND hi.raw_card_id IS NOT NULL
           AND hi.barcode IS DISTINCT FROM rc.barcode
    """, (hold_id,))

    items = db.query("""
        SELECT hi.id AS hold_item_id, hi.raw_card_id,
               rc.tcgplayer_id, rc.scrydex_id, rc.condition,
               COALESCE(sr.location_type, 'bin') AS bin_type
        FROM hold_items hi
        JOIN raw_cards rc ON rc.id = hi.raw_card_id
        LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
        LEFT JOIN storage_rows sr ON sr.id = sl.row_id
        WHERE hi.hold_id = %s AND hi.item_kind = 'raw' AND hi.status = 'REQUESTED'
    """, (hold_id,))
    if not items:
        return

    # Bucket each storage location into a coarse "zone" — bins vs display
    # surfaces (binders + display cases). Pullers walk these as one trip.
    def zone_of(bin_type):
        return 'display' if bin_type in ('binder', 'display_case') else 'bin'

    zone_counts = {}
    for it in items:
        z = zone_of(it["bin_type"])
        zone_counts[z] = zone_counts.get(z, 0) + 1
    if not zone_counts:
        return
    # Dominant zone — most items. Tie breaks toward 'bin' (storage).
    dominant = max(zone_counts.items(), key=lambda kv: (kv[1], kv[0] == 'bin'))[0]

    for it in items:
        if zone_of(it["bin_type"]) == dominant:
            continue  # already in the right zone
        # Look for an unlocked sibling in the dominant zone.
        id_clauses, id_params = [], []
        if it.get("tcgplayer_id"):
            id_clauses.append("rc.tcgplayer_id = %s")
            id_params.append(it["tcgplayer_id"])
        if it.get("scrydex_id"):
            id_clauses.append("rc.scrydex_id = %s")
            id_params.append(it["scrydex_id"])
        if not id_clauses:
            continue
        id_where = "(" + " OR ".join(id_clauses) + ")"

        zone_filter = (
            "COALESCE(sr.location_type,'bin') NOT IN ('binder','display_case')"
            if dominant == 'bin'
            else "COALESCE(sr.location_type,'bin') IN ('binder','display_case')"
        )
        sub = db.query_one(f"""
            SELECT rc.id, rc.barcode
            FROM raw_cards rc
            LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
            LEFT JOIN storage_rows sr ON sr.id = sl.row_id
            LEFT JOIN holds h ON h.id = rc.current_hold_id
            WHERE {id_where}
              AND rc.condition = %s
              AND rc.state IN ('STORED','DISPLAY')
              AND rc.id <> %s
              AND {zone_filter}
              AND (
                rc.current_hold_id IS NULL
                OR rc.current_hold_id = %s
                OR h.status NOT IN ('PENDING','PULLING','READY')
              )
            ORDER BY (rc.state = 'STORED') DESC, rc.created_at ASC
            LIMIT 1
        """, (*id_params, it["condition"], str(it["raw_card_id"]), hold_id))
        if not sub:
            continue
        # Transfer the lock + the hold_item allocation atomically enough.
        # hi.barcode MUST be updated alongside raw_card_id — the pull-list UI
        # shows hi.barcode while the bin label comes from raw_card_id→bin_id,
        # and send-to-pos lists rc.barcode (the substituted copy). Leaving
        # hi.barcode stale makes the operator pull the wrong physical card.
        db.execute("""
            UPDATE raw_cards
               SET current_hold_id = NULL, updated_at = CURRENT_TIMESTAMP
             WHERE id = %s AND current_hold_id = %s
        """, (str(it["raw_card_id"]), hold_id))
        db.execute("""
            UPDATE raw_cards
               SET current_hold_id = %s, updated_at = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (hold_id, str(sub["id"])))
        db.execute("""
            UPDATE hold_items SET raw_card_id = %s, barcode = %s WHERE id = %s
        """, (str(sub["id"]), sub["barcode"], str(it["hold_item_id"])))


@app.route("/api/holds/<hold_id>")
def get_hold(hold_id):
    """Hold detail with optimized pull list.

    Runs the zone-centering allocator on every open. Idempotent — if the
    allocation is already optimal nothing changes."""
    hold = db.query_one("SELECT * FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404

    # Only re-allocate while the hold is still open. Closed holds are
    # historical and shouldn't have their lineup rewritten.
    if hold["status"] in ('PENDING', 'PULLING', 'READY'):
        try:
            _zone_center_hold(hold_id)
        except Exception as e:
            logger.warning(f"Zone-center failed for hold {hold_id}: {e}")

    # LEFT JOIN raw_cards so sealed/slab items (raw_card_id IS NULL) survive.
    # Sealed/slab items carry their own title/image/sku/unit_price on hold_items;
    # raw items pull those fields from raw_cards.
    items = db.query("""
        SELECT hi.id AS hold_item_id, hi.status AS item_status,
               hi.item_kind, hi.barcode, hi.pulled_at, hi.resolved_at,
               hi.shopify_product_id, hi.shopify_variant_id,
               hi.sku AS hi_sku, hi.title AS hi_title,
               hi.image_url AS hi_image_url, hi.unit_price AS hi_unit_price,
               ipc.barcode AS prod_barcode,
               rc.id AS card_id, rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.tcgplayer_id, rc.scrydex_id,
               rc.image_url AS rc_image_url, rc.state AS card_state,
               sl.bin_label, COALESCE(sr.location_type, 'bin') AS bin_type
        FROM hold_items hi
        LEFT JOIN raw_cards rc ON hi.raw_card_id = rc.id
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        LEFT JOIN storage_rows sr ON sl.row_id = sr.id
        LEFT JOIN inventory_product_cache ipc
          ON ipc.shopify_variant_id = hi.shopify_variant_id
         AND hi.item_kind IN ('sealed','slab')
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
            d["scrydex_id"]  = None
        else:
            d["image_url"] = d.get("rc_image_url")
            d["sku"]       = None
        norm_items.append(d)

    # Build optimized pull list FROM RAW ITEMS ONLY — the bin-grouping logic
    # is meaningless for sealed/slab (no bins, no condition matching).
    # Identity key is (tcgplayer_id, scrydex_id, condition): JP / Scrydex-only
    # cards have no tcgplayer_id, so grouping by tcg alone collapses every
    # JP card into one bucket. Carrying scrydex_id keeps them distinct.
    pull_groups = {}
    for item in norm_items:
        if (item.get("item_kind") or "raw") != "raw":
            continue
        key = (item["tcgplayer_id"], item.get("scrydex_id") or "", item["condition"])
        if key not in pull_groups:
            pull_groups[key] = {
                "card_name":    item["card_name"],
                "set_name":     item["set_name"],
                "card_number":  item["card_number"],
                "condition":    item["condition"],
                "tcgplayer_id": item["tcgplayer_id"],
                "scrydex_id":   item.get("scrydex_id"),
                "image_url":    item["image_url"],
                "items":        [],
            }
        pull_groups[key]["items"].append(dict(item))

    # For each group, find the best bin (most copies available)
    pull_list = []
    for key, group in pull_groups.items():
        tcg_id, sx_id, condition = key
        qty_needed = len(group["items"])
        # Match on whichever ID the card has — JP cards are scrydex-only,
        # most US cards have tcgplayer_id (and increasingly also scrydex_id).
        # OR'd so a single query handles both kinds without splitting.
        if tcg_id or sx_id:
            id_clause = []
            id_params = []
            if tcg_id:
                id_clause.append("rc.tcgplayer_id = %s")
                id_params.append(tcg_id)
            if sx_id:
                id_clause.append("rc.scrydex_id = %s")
                id_params.append(sx_id)
            id_where = "(" + " OR ".join(id_clause) + ")"

            best_bins = db.query(f"""
                SELECT sl.bin_label, sl.id AS bin_id,
                       COALESCE(sr.location_type, 'bin') AS bin_type,
                       COUNT(*) AS available_here
                FROM raw_cards rc
                JOIN storage_locations sl ON rc.bin_id = sl.id
                JOIN storage_rows sr ON sl.row_id = sr.id
                WHERE {id_where}
                  AND rc.condition = %s
                  AND rc.state IN ('STORED','DISPLAY')
                GROUP BY sl.bin_label, sl.id, sr.location_type
                ORDER BY available_here DESC
            """, tuple(id_params) + (condition,))

            valid = db.query(f"""
                SELECT barcode FROM raw_cards rc
                WHERE {id_where}
                  AND rc.condition = %s
                  AND rc.state IN ('STORED','DISPLAY')
            """, tuple(id_params) + (condition,))
        else:
            best_bins = []
            valid = []

        group["best_bins"] = [
            {"bin_label": b["bin_label"], "count": b["available_here"],
             "bin_type": b["bin_type"]}
            for b in best_bins
        ]
        group["qty_needed"] = qty_needed
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
    Cancel a hold outright. The puller is assumed to be physically holding
    every requested/rejected card (no scan-to-PULLED in the new flow, so we
    can't tell whether they actually walked the bins yet — and the cost of
    asking is high). All raw items route to the Return Queue, where the
    goback scan flow will re-shelve them.

    Idempotent on already-cancelled holds.
    """
    hold = db.query_one("SELECT id, status FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404
    if hold["status"] == "CANCELLED":
        return jsonify({"success": True, "status": "CANCELLED", "noop": True})

    # Route every raw item to the Return Queue regardless of original state.
    # No Shopify listings exist at this point (Send-to-POS is the only path
    # that creates them) so there's nothing to delete.
    db.execute("""
        UPDATE raw_cards
           SET state = 'PENDING_RETURN',
               current_hold_id = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE id IN (
             SELECT raw_card_id FROM hold_items
              WHERE hold_id = %s
                AND raw_card_id IS NOT NULL
                AND status IN ('REQUESTED','PULLED','REJECTED')
         )
    """, (hold_id,))

    db.execute("""
        UPDATE hold_items
           SET status = 'CANCELLED', resolved_at = CURRENT_TIMESTAMP
         WHERE hold_id = %s
           AND status IN ('REQUESTED','PULLED','REJECTED')
    """, (hold_id,))

    db.execute("UPDATE holds SET status = 'CANCELLED' WHERE id = %s", (hold_id,))

    return jsonify({"success": True, "status": "CANCELLED"})


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/decision", methods=["POST"])
def item_decision(hold_id, hold_item_id):
    """Mark or unmark a hold item as customer-rejected.

    Acceptance is implicit in the new flow — nothing here creates listings or
    flips raw_cards state. Send to POS commits the final ACCEPTED/REJECTED
    routing. Until then, REJECTED is fully reversible to REQUESTED with no
    Shopify side effects.

    Body: {"decision": "REJECTED" | "REQUESTED"}
    """
    decision = (request.get_json() or {}).get("decision", "").upper()
    if decision not in ("REJECTED", "REQUESTED"):
        return jsonify({"error": "decision must be REJECTED or REQUESTED"}), 400

    item = db.query_one("""
        SELECT id, status, item_kind FROM hold_items
        WHERE id = %s AND hold_id = %s
    """, (hold_item_id, hold_id))
    if not item:
        return jsonify({"error": "Hold item not found"}), 404
    if (item.get("item_kind") or "raw") != "raw":
        return jsonify({"error": "Only raw items can be rejected"}), 409
    if item["status"] not in ("REQUESTED", "REJECTED"):
        return jsonify({"error": f"Item is {item['status']}, can't toggle reject"}), 409

    if decision == "REJECTED":
        db.execute("""
            UPDATE hold_items
               SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (hold_item_id,))
    else:
        db.execute("""
            UPDATE hold_items
               SET status = 'REQUESTED', resolved_at = NULL
             WHERE id = %s
        """, (hold_item_id,))

    return jsonify({"success": True, "status": decision})


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/missing", methods=["POST"])
def mark_item_missing(hold_id, hold_item_id):
    """Mark the originally-allocated copy MISSING and auto-substitute.

    No picker: the system chooses the best sibling using:
      1) zone-of-bulk-pull (whichever zone has most other items on this hold)
      2) STORED before DISPLAY
      3) oldest created_at first (so old stock cycles out)

    Returns the new physical location so the puller can keep walking without
    another round-trip to the UI.
    """
    item = db.query_one("""
        SELECT hi.id, hi.raw_card_id,
               rc.tcgplayer_id, rc.scrydex_id, rc.condition, rc.card_name
        FROM hold_items hi
        JOIN raw_cards rc ON rc.id = hi.raw_card_id
        WHERE hi.id = %s AND hi.hold_id = %s
    """, (hold_item_id, hold_id))
    if not item:
        return jsonify({"error": "Hold item not found"}), 404

    # 1. Flip the originally-allocated copy to MISSING and clear its lock.
    db.execute("""
        UPDATE raw_cards
           SET state = 'MISSING',
               current_hold_id = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (str(item["raw_card_id"]),))

    # 2. Determine dominant zone of the rest of the hold.
    zones = db.query("""
        SELECT COALESCE(sr.location_type, 'bin') AS bin_type,
               COUNT(*)::int AS n
        FROM hold_items hi
        JOIN raw_cards rc ON rc.id = hi.raw_card_id
        LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
        LEFT JOIN storage_rows sr ON sr.id = sl.row_id
        WHERE hi.hold_id = %s
          AND hi.id <> %s
          AND hi.item_kind = 'raw'
          AND hi.status = 'REQUESTED'
        GROUP BY 1
    """, (hold_id, hold_item_id))
    bin_count = sum(z["n"] for z in zones if z["bin_type"] not in ("binder","display_case"))
    disp_count = sum(z["n"] for z in zones if z["bin_type"] in ("binder","display_case"))
    prefer_bin = bin_count >= disp_count  # tie breaks toward storage

    # 3. Find the best substitute.
    id_clauses, id_params = [], []
    if item.get("tcgplayer_id"):
        id_clauses.append("rc.tcgplayer_id = %s")
        id_params.append(item["tcgplayer_id"])
    if item.get("scrydex_id"):
        id_clauses.append("rc.scrydex_id = %s")
        id_params.append(item["scrydex_id"])

    substitute = None
    if id_clauses:
        id_where = "(" + " OR ".join(id_clauses) + ")"
        zone_pref = (
            "CASE WHEN COALESCE(sr.location_type,'bin') NOT IN ('binder','display_case') THEN 0 ELSE 1 END ASC"
            if prefer_bin
            else "CASE WHEN COALESCE(sr.location_type,'bin') IN ('binder','display_case') THEN 0 ELSE 1 END ASC"
        )
        substitute = db.query_one(f"""
            SELECT rc.id, rc.barcode, rc.state,
                   sl.bin_label,
                   COALESCE(sr.location_type, 'bin') AS bin_type
            FROM raw_cards rc
            LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
            LEFT JOIN storage_rows sr ON sr.id = sl.row_id
            LEFT JOIN holds h ON h.id = rc.current_hold_id
            WHERE {id_where}
              AND rc.condition = %s
              AND rc.state IN ('STORED','DISPLAY')
              AND rc.id <> %s
              AND (
                rc.current_hold_id IS NULL
                OR h.status NOT IN ('PENDING','PULLING','READY')
              )
            ORDER BY
              {zone_pref},
              (rc.state = 'STORED') DESC,
              rc.created_at ASC
            LIMIT 1
        """, (*id_params, item["condition"], str(item["raw_card_id"])))

    if substitute:
        # Update BOTH raw_card_id and barcode on the hold_item. The UI shows
        # hi.barcode in the slip and the scan-row — if we only flip
        # raw_card_id, the row displays the new bin (via joined rc.*) next
        # to the OLD barcode, leaving the puller no idea which copy to grab.
        db.execute("""
            UPDATE hold_items
               SET raw_card_id = %s, barcode = %s
             WHERE id = %s
        """, (str(substitute["id"]), substitute["barcode"], hold_item_id))
        db.execute("""
            UPDATE raw_cards
               SET current_hold_id = %s, updated_at = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (hold_id, str(substitute["id"])))
        return jsonify({
            "success":   True,
            "swapped":   True,
            "new_card_id": str(substitute["id"]),
            "new_barcode": substitute["barcode"],
            "new_bin":     substitute["bin_label"],
            "new_bin_type": substitute["bin_type"],
            "card_name": item["card_name"],
        })

    # No substitute — flip the hold_item itself to MISSING.
    db.execute("""
        UPDATE hold_items
           SET status = 'MISSING', resolved_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (hold_item_id,))
    return jsonify({
        "success":   True,
        "swapped":   False,
        "card_name": item["card_name"],
    })


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
               COALESCE(hi.shopify_product_id, rc.shopify_product_id) AS shopify_product_id,
               rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.current_price, rc.image_url,
               rc.tcgplayer_id, rc.barcode,
               h.cohort AS hold_cohort, h.checkout_status AS hold_checkout_status
        FROM hold_items hi
        JOIN raw_cards rc ON hi.raw_card_id = rc.id
        JOIN holds h ON hi.hold_id = h.id
        WHERE hi.id = %s AND hi.hold_id = %s
    """, (hold_item_id, hold_id))
    if not item:
        return jsonify({"error": "Hold item not found"}), 404

    # Paid Champion holds are fulfilled from the Shipping Queue — staff
    # can't unwind individual items here without breaking the outbound ship.
    if item.get("hold_cohort") == "champion" and item.get("hold_checkout_status") == "completed":
        return jsonify({
            "error": "Paid Champion hold — manage this from the Shipping tab in Screening, not the hold detail."
        }), 409

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


@app.route("/api/holds/<hold_id>/send-to-pos", methods=["POST"])
@app.route("/api/holds/<hold_id>/finish",      methods=["POST"])  # legacy alias for stale tabs
def send_to_pos(hold_id):
    """Commit a guest hold to the register.

    For every REQUESTED raw item: create a Shopify draft listing, flip raw_card
    state to PENDING_SALE, mark hold_item ACCEPTED. For every REJECTED raw
    item: route to PENDING_RETURN. For every REQUESTED sealed/slab item:
    mark ACCEPTED (no listing — already on Shopify). MISSING items are left
    alone (already terminal).

    Closes the hold ACCEPTED if anything goes to the register, RETURNED if
    everything was rejected (functionally identical to Cancel Hold).

    Champion holds are handled by /api/ship/<id>/mark-shipped — they don't
    use this path.
    """
    hold = db.query_one(
        "SELECT cohort, status FROM holds WHERE id = %s", (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404
    if hold.get("cohort") == "champion":
        return jsonify({
            "error": "Champion holds ship from the Shipping tab in Screening, not Send to POS",
        }), 409
    if hold["status"] in ('ACCEPTED', 'RETURNED', 'CANCELLED', 'AUTO_EXPIRED'):
        return jsonify({"error": f"Hold is already {hold['status']}"}), 409

    raw_to_list = db.query("""
        SELECT hi.id AS hold_item_id, hi.barcode AS hi_barcode,
               rc.id AS card_id, rc.barcode, rc.card_name, rc.set_name,
               rc.card_number, rc.condition, rc.current_price, rc.image_url,
               rc.tcgplayer_id
        FROM hold_items hi
        JOIN raw_cards rc ON rc.id = hi.raw_card_id
        WHERE hi.hold_id = %s
          AND hi.item_kind = 'raw'
          AND hi.status = 'REQUESTED'
    """, (hold_id,))

    results, errors = [], []
    if raw_to_list:
        if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
            return jsonify({"error": "Shopify not configured"}), 503
        seen_barcodes = set()
        for item in raw_to_list:
            bc = item["barcode"]
            if bc in seen_barcodes:
                errors.append({"barcode": bc, "error": "duplicate within this hold"})
                continue
            seen_barcodes.add(bc)
            try:
                listing = _create_raw_listing(dict(item))
            except Exception as e:
                logger.exception(f"Send-to-POS listing failed for {bc}: {e}")
                errors.append({"barcode": bc, "error": str(e)})
                continue
            db.execute("""
                UPDATE hold_items
                   SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP,
                       shopify_product_id = %s, shopify_variant_id = %s
                 WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], str(item["hold_item_id"])))
            db.execute("""
                UPDATE raw_cards
                   SET state = 'PENDING_SALE',
                       shopify_product_id = %s,
                       shopify_variant_id = %s,
                       current_hold_id = NULL,
                       updated_at = CURRENT_TIMESTAMP
                 WHERE id = %s
            """, (listing["product_id"], listing["variant_id"], str(item["card_id"])))
            results.append({
                "barcode":   bc,
                "card_name": item["card_name"],
                "product_id": listing["product_id"],
            })

    # REJECTED raw items → Return Queue (puller is holding them).
    db.execute("""
        UPDATE raw_cards
           SET state = 'PENDING_RETURN', current_hold_id = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE id IN (
             SELECT raw_card_id FROM hold_items
              WHERE hold_id = %s
                AND item_kind = 'raw'
                AND status = 'REJECTED'
                AND raw_card_id IS NOT NULL
         )
    """, (hold_id,))

    # Sealed/slab REQUESTED → ACCEPTED. They live on Shopify, customer pays
    # at POS, Shopify decrements its own inventory.
    db.execute("""
        UPDATE hold_items
           SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP
         WHERE hold_id = %s
           AND item_kind IN ('sealed','slab')
           AND status = 'REQUESTED'
    """, (hold_id,))

    # Close the hold. ACCEPTED if anything's going to the register; RETURNED
    # if every line was rejected (functionally identical to Cancel Hold).
    has_sealed_accepted = db.query_one("""
        SELECT 1 FROM hold_items
        WHERE hold_id = %s AND item_kind IN ('sealed','slab') AND status = 'ACCEPTED'
        LIMIT 1
    """, (hold_id,))
    final_status = "ACCEPTED" if (results or has_sealed_accepted) else "RETURNED"
    db.execute("""
        UPDATE holds SET status = %s, resolved_at = CURRENT_TIMESTAMP WHERE id = %s
    """, (final_status, hold_id))

    return jsonify({
        "success":    True,
        "status":     final_status,
        "created":    len(results),
        "results":    results,
        "errors":     errors,
    })


@app.route("/api/holds/<hold_id>/packing-slip")
def hold_packing_slip(hold_id):
    """Returns the slip data + HTML so the frontend can window.print() it.

    Big images, big letters, bin location per line — same style as
    screening's combined-shipping packing slip."""
    hold = db.query_one("""
        SELECT id, customer_name, customer_phone, created_at
        FROM holds WHERE id = %s
    """, (hold_id,))
    if not hold:
        return jsonify({"error": "Not found"}), 404

    items = db.query("""
        SELECT hi.id, hi.item_kind, hi.title AS hi_title, hi.image_url AS hi_image_url,
               hi.unit_price AS hi_unit_price, hi.sku AS hi_sku,
               hi.shopify_variant_id,
               rc.card_name, rc.set_name, rc.card_number, rc.condition,
               rc.current_price, rc.image_url AS rc_image_url, rc.barcode,
               sl.bin_label, COALESCE(sr.location_type, 'bin') AS bin_type,
               ipc.image_url AS ipc_image_url
        FROM hold_items hi
        LEFT JOIN raw_cards rc ON rc.id = hi.raw_card_id
        LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
        LEFT JOIN storage_rows sr ON sr.id = sl.row_id
        LEFT JOIN inventory_product_cache ipc
          ON ipc.shopify_variant_id = hi.shopify_variant_id
         AND hi.item_kind IN ('sealed','slab')
        WHERE hi.hold_id = %s
          AND hi.status IN ('REQUESTED','REJECTED')
        ORDER BY sl.bin_label NULLS LAST, COALESCE(rc.card_name, hi.title)
    """, (hold_id,))

    return jsonify({
        "hold":  _ser(dict(hold)),
        "items": [_ser(dict(i)) for i in items],
    })


def _create_raw_listing(item: dict) -> dict:
    """Create a Shopify DRAFT product for a raw card at POS.

    Idempotent on item['shopify_product_id']: if that product is still in
    Shopify (active or archived), reuse it instead of creating a new one.
    Prevents duplicate listings on the same SKU when accept/return/accept
    is toggled — Shopify doesn't enforce SKU uniqueness, and stamping a
    fresh product_id over the old one used to leave orphans behind.
    """
    existing_pid = item.get("shopify_product_id")
    if existing_pid:
        try:
            prod = _shopify("GET", f"/products/{existing_pid}.json").get("product")
            if prod and prod.get("variants"):
                variant_id = prod["variants"][0]["id"]
                if prod.get("status") == "archived":
                    # Un-archive so POS can find it again on the next sync.
                    _shopify("PUT", f"/products/{existing_pid}.json",
                             json={"product": {"id": existing_pid, "status": "active"}})
                return {
                    "product_id": existing_pid,
                    "variant_id": variant_id,
                    "title":      prod.get("title", ""),
                }
        except Exception as e:
            logger.info(f"Existing product {existing_pid} unreachable ({e}); creating new")

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

    # No image: POS matches by SKU/barcode and the cashier never looks at the
    # product card art at the register. `images: [{src: url}]` blocks while
    # Shopify fetches the URL server-side, which was the bulk of the per-card
    # latency on multi-card holds.
    payload = {
        "product": {
            "title":       title,
            "body_html":   body,
            "status":      "active",
            "published":   False,
            "product_type": "Pokemon",
            "vendor":      "Pack Fresh",
            "variants": [{
                "price":                str(price),
                "sku":                  item["barcode"],
                "barcode":              item["barcode"],
                "inventory_management": "shopify",
                "inventory_quantity":   1,
                "requires_shipping":    True,
                # Raw single in a sleeve/top-loader — a stack of these is still a
                # small box. Without an explicit weight Shopify falls back to a
                # default that over-charges shipping (e.g. 49 cards rated as lbs).
                "weight":               1,
                "weight_unit":          "oz",
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


# ─── Legacy stubs (stale-tab compat) ─────────────────────────────────────────
# Old card_manager UI tabs that haven't been refreshed since the redesign call
# these. Return JSON (not Flask's HTML 404) so the frontend's r.json() doesn't
# explode with a SyntaxError that surfaces as a useless "Unexpected token <"
# toast. The error messages tell the user the page is stale — refresh.

@app.route("/api/holds/<hold_id>/status",                          methods=["POST"])
def _legacy_status(hold_id):
    return jsonify({
        "success": True,
        "status": (request.get_json() or {}).get("status", "PENDING"),
    })


@app.route("/api/holds/<hold_id>/scan",                            methods=["POST"])
def _legacy_scan(hold_id):
    return jsonify({
        "error": "Holds no longer scan from the back. Refresh this page (Ctrl+F5) and use the packing slip.",
    }), 410


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/tap-pull",   methods=["POST"])
def _legacy_tap_pull(hold_id, hold_item_id):
    return jsonify({
        "error": "Tap-to-pull is gone. Refresh this page (Ctrl+F5) and follow the packing slip.",
    }), 410


@app.route("/api/holds/<hold_id>/items/<hold_item_id>/missing-candidates")
def _legacy_missing_candidates(hold_id, hold_item_id):
    # Empty list so the old UI falls through to its single-candidate path,
    # which calls /missing with no body — the new endpoint accepts that.
    return jsonify({"candidates": [], "assigned_card_id": None})


# ═══════════════════════════════════════════════════════════════════════════════
# Return Queue API
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/returns")
def list_returns():
    """All PENDING_RETURN cards. `bin_type` lets the UI render display-family
    rows with a one-tap restore button (no scan needed) — the card is going
    back to the same case/binder slot it came from. Storage rows still go
    through scan + store_returns to get a fresh bin assignment."""
    rows = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.condition,
               rc.card_number, rc.image_url, rc.current_price,
               sl.bin_label AS last_bin,
               COALESCE(sr.location_type, 'bin') AS bin_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        LEFT JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE rc.state = 'PENDING_RETURN'
        ORDER BY rc.updated_at DESC NULLS LAST
    """)
    return jsonify({"cards": [_ser(dict(r)) for r in rows]})


@app.route("/api/returns/<card_id>/tap-restore", methods=["POST"])
def tap_restore_display(card_id):
    """Removed — every Return Queue card must be scanned. Stub kept so
    stale tabs get a JSON error instead of Flask's HTML 404."""
    return jsonify({
        "error": "Tap-restore is gone — scan every return through the queue.",
    }), 410


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


_BARCODED_STATES = ('BARCODED', 'BARCODED_STORAGE', 'BARCODED_DISPLAY',
                    'ROUTED_STORAGE', 'ROUTED_BINDER')


@app.route("/api/grading")
def list_grading():
    """All raw cards sent out for grading. Prefers the pre-barcoded sibling's
    barcode when the ingest pipeline double-wrote the row (REMOVED row with a
    phantom barcode + BARCODED row carrying the physical label) so the barcode
    shown here matches what's actually on the card."""
    rows = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.condition,
               rc.card_number, rc.image_url, rc.current_price,
               rc.removal_date AS sent_at,
               sib.barcode AS sibling_barcode
        FROM raw_cards rc
        LEFT JOIN LATERAL (
            SELECT barcode FROM raw_cards
             WHERE intake_item_id = rc.intake_item_id
               AND intake_item_id IS NOT NULL
               AND state = ANY(%s)
             ORDER BY created_at ASC
             LIMIT 1
        ) sib ON TRUE
        WHERE rc.state = 'REMOVED' AND rc.removal_reason = 'GRADING'
        ORDER BY rc.removal_date DESC NULLS LAST
    """, (list(_BARCODED_STATES),))
    out = []
    for r in rows:
        d = dict(r)
        if d.get("sibling_barcode"):
            d["barcode"] = d["sibling_barcode"]
        d.pop("sibling_barcode", None)
        out.append(_ser(d))
    return jsonify({"cards": out})


def _resolve_grading_target(card):
    """Given a raw_cards row matched by scan, return (target_id, orphan_id_or_None)
    where target_id is the row to flip to PENDING_RETURN and orphan_id is a
    dupe row to delete. Handles the BARCODED + REMOVED+GRADING duplicate pair
    left behind by the pre-barcoded → grade ingest bug.

    Returns (None, None) if the card isn't at grading (directly or via sibling)."""
    state = card["state"]
    removal = card.get("removal_reason")
    iid = card.get("intake_item_id")

    if state == "REMOVED" and removal == "GRADING":
        if iid:
            sib = db.query_one("""
                SELECT id FROM raw_cards
                 WHERE intake_item_id = %s AND state = ANY(%s)
                 ORDER BY created_at ASC LIMIT 1
            """, (str(iid), list(_BARCODED_STATES)))
            if sib:
                return str(sib["id"]), str(card["id"])
        return str(card["id"]), None

    if state in _BARCODED_STATES and iid:
        sib = db.query_one("""
            SELECT id FROM raw_cards
             WHERE intake_item_id = %s
               AND state = 'REMOVED' AND removal_reason = 'GRADING'
             ORDER BY removal_date DESC LIMIT 1
        """, (str(iid),))
        if sib:
            return str(card["id"]), str(sib["id"])

    return None, None


def _flip_to_pending_return(card_id, orphan_id):
    db.execute("""
        UPDATE raw_cards
           SET state = 'PENDING_RETURN',
               removal_reason = NULL,
               removal_date = NULL,
               current_hold_id = NULL,
               bin_id = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE id::text = %s
    """, (card_id,))
    if orphan_id:
        db.execute("DELETE FROM raw_cards WHERE id::text = %s", (orphan_id,))


@app.route("/api/grading/<card_id>/reverse", methods=["POST"])
def reverse_grading(card_id):
    """Undo a 'sent to grading' decision: route the card to the Return Queue so
    the operator can re-shelve it through the normal store-returns flow.

    If the row has a pre-barcoded sibling (ingest dupe), flip the sibling and
    delete this orphan so the printed barcode keeps working downstream."""
    card = db.query_one("""
        SELECT id, card_name, state, removal_reason, intake_item_id FROM raw_cards
         WHERE id::text = %s
    """, (card_id,))
    if not card:
        return jsonify({"error": "Card not found"}), 404
    target, orphan = _resolve_grading_target(card)
    if not target:
        return jsonify({"error": "Card not in grading state"}), 404
    _flip_to_pending_return(target, orphan)
    return jsonify({"success": True, "card_name": card["card_name"]})


@app.route("/api/grading/scan", methods=["POST"])
def scan_grading():
    """Scan a barcode from the At Grading view: flip the matching card →
    PENDING_RETURN so the next /api/returns/store call assigns a storage bin.
    Accepts either the REMOVED+GRADING row directly OR a BARCODED row whose
    intake_item has a REMOVED+GRADING sibling (the ingest-dupe path); collapses
    the duplicate so future scans go through cleanly."""
    barcode = (request.get_json() or {}).get("barcode", "").strip()
    card = db.query_one("""
        SELECT id, card_name, condition, state, removal_reason, intake_item_id
        FROM raw_cards WHERE barcode = %s
    """, (barcode,))
    if not card:
        return jsonify({"error": "Barcode not found"}), 404
    target, orphan = _resolve_grading_target(card)
    if not target:
        return jsonify({
            "error": f"Card is {card['state']}/{card['removal_reason'] or '-'}, not at grading",
            "card_name": card["card_name"],
        }), 409
    _flip_to_pending_return(target, orphan)
    return jsonify({
        "success":   True,
        "card_name": card["card_name"],
        "condition": card["condition"],
        "barcode":   barcode,
    })


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

    # Handle MISSING cards being re-found — flip to PENDING_RETURN so the card
    # actually surfaces in the Return Queue and gets a bin assigned on the
    # next store_returns pass. Without this, scanning a found-missing barcode
    # only returned a soft-success and the card stayed stuck in MISSING.
    if card["state"] == "MISSING":
        db.execute("""
            UPDATE raw_cards
            SET state = 'PENDING_RETURN', current_hold_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (str(card["id"]),))
        return jsonify({
            "success":   True,
            "card_name": card["card_name"],
            "condition": card["condition"],
            "barcode":   barcode,
            "was_missing": True,
        })

    # DISPLAY-state cards: operator has the card in hand and needs to put it
    # back somewhere. Surface in Return Queue (preserves bin_id so the
    # display_cards branch in store_returns re-shelves to the original
    # case/binder slot). STORED is included for the same reason — relocate-
    # in-hand cards shouldn't get stuck because their state didn't match.
    if card["state"] in ("DISPLAY", "STORED"):
        db.execute("""
            UPDATE raw_cards
            SET state = 'PENDING_RETURN', current_hold_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (str(card["id"]),))
        return jsonify({
            "success":   True,
            "card_name": card["card_name"],
            "condition": card["condition"],
            "barcode":   barcode,
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
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name,
               COALESCE(rc.game, 'pokemon') AS game,
               rc.bin_id, sl.bin_label,
               COALESCE(sr.location_type, 'bin') AS bin_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        LEFT JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE rc.barcode = ANY(%s) AND rc.state IN ('PENDING_RETURN', 'MISSING')
    """, (list(barcodes),))

    if not cards:
        return jsonify({"error": "No scanned cards found in PENDING_RETURN state"}), 400

    bin_summary = []
    errors      = []

    # Display-family cards (FG / Binder) go back to their original bin —
    # the puller is physically restoring the card to its case/binder slot,
    # so no new assignment is needed. Just flip state DISPLAY and preserve
    # bin_id. The Return Queue surfaces these for the explicit "remember
    # to put it back" task without funnelling them into storage bins.
    display_cards = [c for c in cards if c["bin_type"] in ("display_case", "binder")]
    if display_cards:
        ids = [str(c["id"]) for c in display_cards]
        db.execute("""
            UPDATE raw_cards
            SET state = 'DISPLAY', current_hold_id = NULL,
                stored_at = CURRENT_TIMESTAMP
            WHERE id::text = ANY(%s)
        """, (ids,))
        # Group by original bin for the operator-facing summary.
        by_bin: dict = {}
        for c in display_cards:
            label = c["bin_label"] or "(unknown)"
            by_bin.setdefault(label, []).append(c["card_name"])
        for label, names in by_bin.items():
            bin_summary.append({
                "bin_label": label,
                "count":     len(names),
                "game":      "display",
                "cards":     names[:5],
            })

    # Storage-bound cards (real bins or no bin yet) go through assign_bins
    # as today, grouped by game so MTG/Pokemon land in their own rows.
    storage_cards = [c for c in cards if c["bin_type"] not in ("display_case", "binder")]
    if storage_cards:
        from collections import defaultdict
        by_game = defaultdict(list)
        for c in storage_cards:
            game = c["game"]
            # Safety net: a card sitting at the Pokemon default (real game NULL,
            # or mis-tagged 'pokemon' at intake) whose set name resolves to a
            # single non-Pokemon game in the Scrydex cache was mislabeled — route
            # by the real game so a Magic "Final Fantasy" single doesn't get
            # dumped into a Pokemon bin. Only rescues the Pokemon case; a genuine
            # Pokemon card's set name resolves to pokemon (or not at all).
            if game == "pokemon":
                inferred = infer_card_type_from_set(c.get("set_name"), db)
                if inferred and inferred != "pokemon":
                    game = inferred
            by_game[game].append(c)
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
# Display Case + Binder fill — shared scoring/quota helpers
# ═══════════════════════════════════════════════════════════════════════════════

# Each filled location targets a balanced spread across these tiers so set-out
# isn't just the top-N by price. Quotas are recomputed per request from the
# location's *current* tier breakdown — refilling a case where all the
# expensive cards sold pulls more premium cards back in.
PRICE_TIERS    = ("premium", "high", "mid", "low")
TIER_TARGETS   = {"premium": 0.25, "high": 0.25, "mid": 0.25, "low": 0.25}
TIER_THRESHOLDS = (("premium", 100.0), ("high", 25.0), ("mid", 5.0), ("low", 0.0))


def _classify_tier(price):
    if price is None:
        return "low"
    p = float(price)
    for label, lo in TIER_THRESHOLDS:
        if p >= lo:
            return label
    return "low"


def _compute_tier_quotas(capacity, count_to_fill, current_counts):
    """Allocate `count_to_fill` slots across price tiers based on the *deficit*
    from each tier's target at full capacity.

    If every tier is already at or above target, distribute new picks evenly
    so we keep refreshing variety. Returns {tier: quota}."""
    if count_to_fill <= 0:
        return {t: 0 for t in PRICE_TIERS}

    target_at_full = {t: int(capacity * TIER_TARGETS[t]) for t in PRICE_TIERS}
    deficits = {t: max(0, target_at_full[t] - current_counts.get(t, 0)) for t in PRICE_TIERS}
    total_deficit = sum(deficits.values())

    quotas = {t: 0 for t in PRICE_TIERS}
    remaining = count_to_fill

    if total_deficit > 0:
        # Allocate proportionally to deficits, capped by per-tier deficit.
        for t in PRICE_TIERS:
            q = min(deficits[t], remaining, int(round(count_to_fill * deficits[t] / total_deficit)))
            quotas[t] = q
            remaining -= q

        # Hand remainder (rounding losses) to tiers with the largest leftover deficit.
        if remaining > 0:
            for t in sorted(PRICE_TIERS, key=lambda x: deficits[x] - quotas[x], reverse=True):
                if remaining <= 0:
                    break
                extra = min(remaining, deficits[t] - quotas[t])
                if extra > 0:
                    quotas[t] += extra
                    remaining -= extra

    # Anything still unallocated (all deficits met or capacity=0) — spread evenly.
    if remaining > 0:
        order = sorted(PRICE_TIERS, key=lambda x: quotas[x])
        i = 0
        while remaining > 0:
            quotas[order[i % len(PRICE_TIERS)]] += 1
            remaining -= 1
            i += 1

    return quotas


# Canonical game IDs that can be assigned to a typed location. Two pseudo-types
# layer on top: 'other' (every non-Pokemon TCG) and 'mixed' (every TCG including
# Pokemon). Add new games here as we onboard them — the filter + UI dropdown
# both consume this list.
SINGLE_GAME_TYPES = ("pokemon", "magic", "onepiece", "lorcana", "riftbound", "yugioh")
MULTI_GAME_TYPES  = ("other", "mixed")
ALL_CARD_TYPES    = SINGLE_GAME_TYPES + MULTI_GAME_TYPES


def _game_filter_sql(card_type):
    """Return a SQL fragment filtering rc.game for a typed location.

    Single-game types (pokemon, magic, …) -> only that game.
    'other' -> every non-Pokémon TCG (legacy FG-2 case, still useful).
    'mixed' -> every TCG including Pokémon.
    Anything else -> no filter (legacy fallback).
    """
    if card_type in SINGLE_GAME_TYPES:
        return f"rc.game = '{card_type}'"
    if card_type == "other":
        return "rc.game IS NOT NULL AND rc.game <> 'pokemon'"
    if card_type == "mixed":
        return "rc.game IS NOT NULL"
    return "TRUE"


# Shared scoring SQL — one source of truth for set-out and binder fill so they
# rank candidates identically. Caller appends the game filter and limit.
_SCORE_SELECT_SQL = """
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
          AND {game_filter}
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
"""


def _greedy_pick_with_quotas(candidates, count, allowed_conditions,
                             tier_quotas, name_cap, set_cap, by_card_seed=None,
                             game_cap=None):
    """Greedy pass over scored candidates with diversity caps.

    `tier_quotas` enforces the price-tier mix. `by_card_seed` lets the caller
    pre-load existing binder/case contents into the (name,variant) cap so we
    never recommend a third copy of something already over-represented.
    `game_cap` enforces a per-game ceiling for mixed locations (FG-2)."""
    chosen = []
    by_card = dict(by_card_seed or {})
    by_set  = {}
    by_game = {}
    tier_used = {t: 0 for t in PRICE_TIERS}

    for c in candidates:
        if len(chosen) >= count:
            break
        if c.get("condition") not in allowed_conditions:
            continue

        tier = _classify_tier(c.get("current_price"))
        if tier_used[tier] >= tier_quotas.get(tier, 0):
            continue

        cv = (c["card_name"], (c.get("variant") or "").lower())
        if by_card.get(cv, 0) >= name_cap:
            continue
        if by_set.get(c.get("set_name") or "", 0) >= set_cap:
            continue
        if game_cap is not None:
            g = c.get("game") or "_"
            if by_game.get(g, 0) >= game_cap:
                continue

        chosen.append(c)
        by_card[cv] = by_card.get(cv, 0) + 1
        by_set[c.get("set_name") or ""] = by_set.get(c.get("set_name") or "", 0) + 1
        tier_used[tier] += 1
        if game_cap is not None:
            g = c.get("game") or "_"
            by_game[g] = by_game.get(g, 0) + 1

    # Backfill pass: if quotas left a tier short (not enough storage stock at
    # that price), use the freed slots to take the next-best cards from any
    # tier so staff still gets a full shopping list.
    if len(chosen) < count:
        chosen_ids = {c["id"] for c in chosen}
        for c in candidates:
            if len(chosen) >= count:
                break
            if c["id"] in chosen_ids:
                continue
            if c.get("condition") not in allowed_conditions:
                continue
            cv = (c["card_name"], (c.get("variant") or "").lower())
            if by_card.get(cv, 0) >= name_cap:
                continue
            if by_set.get(c.get("set_name") or "", 0) >= set_cap:
                continue
            if game_cap is not None:
                g = c.get("game") or "_"
                if by_game.get(g, 0) >= game_cap:
                    continue
            chosen.append(c)
            by_card[cv] = by_card.get(cv, 0) + 1
            by_set[c.get("set_name") or ""] = by_set.get(c.get("set_name") or "", 0) + 1
            if game_cap is not None:
                g = c.get("game") or "_"
                by_game[g] = by_game.get(g, 0) + 1

    return chosen


def _current_tier_counts(bin_uuid):
    """Per-tier breakdown of cards currently sitting in this case/binder."""
    rows = db.query("""
        SELECT current_price FROM raw_cards
        WHERE state = 'DISPLAY' AND bin_id = %s
    """, (bin_uuid,))
    counts = {t: 0 for t in PRICE_TIERS}
    for r in rows:
        counts[_classify_tier(r["current_price"])] += 1
    return counts


# ═══════════════════════════════════════════════════════════════════════════════
# Display Case — Front Glass + capacity management
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/display/cases")
def display_cases():
    """List every display_case location with its capacity meter (headers only).

    Cards are NOT included here — a full glass can hold hundreds of rows and
    each needs an image-resolving subquery, so loading every case's cards up
    front made the Display tab crawl. The UI fetches a case's cards lazily via
    /api/display/cases/<id>/cards when the operator expands that case."""
    cases = get_display_case_capacity(db)
    out = [{
        "id":            str(c["id"]),
        "bin_label":     c["bin_label"],
        "card_type":     c["card_type"],
        "capacity":      c["capacity"],
        "current_count": c["current_count"],
        "available":     c["available"],
    } for c in cases]
    return jsonify({"cases": out, "card_type_options": list(ALL_CARD_TYPES)})


@app.route("/api/display/cases/<case_id>/cards")
def display_case_cards(case_id):
    """Cards currently in one display case — fetched lazily when the case is
    expanded in the UI. COALESCEs image_url through scrydex_price_cache so the
    render matches the editor's barcode-lookup endpoint."""
    cards = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.card_number, rc.condition,
               rc.current_price,
               COALESCE(
                 rc.image_url,
                 (SELECT MAX(image_large)  FROM scrydex_price_cache WHERE scrydex_id   = rc.scrydex_id),
                 (SELECT MAX(image_medium) FROM scrydex_price_cache WHERE scrydex_id   = rc.scrydex_id),
                 (SELECT MAX(image_small)  FROM scrydex_price_cache WHERE scrydex_id   = rc.scrydex_id),
                 (SELECT MAX(image_large)  FROM scrydex_price_cache WHERE tcgplayer_id = rc.tcgplayer_id),
                 (SELECT MAX(image_medium) FROM scrydex_price_cache WHERE tcgplayer_id = rc.tcgplayer_id),
                 (SELECT MAX(image_small)  FROM scrydex_price_cache WHERE tcgplayer_id = rc.tcgplayer_id)
               ) AS image_url,
               rc.variant, rc.tcgplayer_id, rc.scrydex_id, rc.stored_at
        FROM raw_cards rc
        WHERE rc.state = 'DISPLAY' AND rc.bin_id = %s
        ORDER BY rc.current_price DESC NULLS LAST, rc.card_name ASC
    """, (str(case_id),))
    return jsonify({"cards": [_ser(dict(r)) for r in cards]})


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


@app.route("/api/display/cases/<case_id>/card-type", methods=["POST"])
def display_case_card_type(case_id):
    """Change which game(s) a display case holds. Set Out's shopping list
    will start filtering by the new type immediately, but cards already in
    the case stay where they are — staff can return them manually if they no
    longer fit. The pseudo-types 'other' (every non-Pokemon TCG) and 'mixed'
    (every TCG) are supported alongside single-game IDs."""
    ct = ((request.get_json() or {}).get("card_type") or "").strip().lower()
    if ct not in ALL_CARD_TYPES:
        return jsonify({"error": f"card_type must be one of: {', '.join(ALL_CARD_TYPES)}"}), 400

    row = db.query_one("""
        SELECT sl.id, sr.location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (case_id,))
    if not row or row["location_type"] != "display_case":
        return jsonify({"error": "Display case not found"}), 404

    db.execute("UPDATE storage_locations SET card_type = %s WHERE id::text = %s", (ct, case_id))
    return jsonify({"success": True, "card_type": ct})


@app.route("/api/display/cases", methods=["POST"])
def display_case_create():
    """Add a new display case. Auto-picks the next FG-N partition number so
    bin_labels stay sequential (FG-3, FG-4, …)."""
    data = request.get_json() or {}
    try:
        capacity = int(data.get("capacity", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "capacity must be an integer"}), 400
    if capacity < 1 or capacity > 1000:
        return jsonify({"error": "capacity must be between 1 and 1000"}), 400
    ct = (data.get("card_type") or "").strip().lower()
    if ct not in ALL_CARD_TYPES:
        return jsonify({"error": f"card_type must be one of: {', '.join(ALL_CARD_TYPES)}"}), 400

    fg_row = db.query_one("""
        SELECT sl.row_id
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sr.location_type = 'display_case'
        LIMIT 1
    """)
    if not fg_row:
        return jsonify({"error": "No display_case row exists yet — run migrate_display_phase1 first"}), 500
    row_id = fg_row["row_id"]

    next_part_row = db.query_one("""
        SELECT COALESCE(MAX(partition_num), 0) + 1 AS next_part
        FROM storage_locations WHERE row_id = %s
    """, (row_id,))
    next_part = next_part_row["next_part"]
    bin_label = f"FG-{next_part}"

    db.execute("""
        INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type, capacity)
        VALUES (%s, %s, %s, %s, %s)
    """, (bin_label, row_id, next_part, ct, capacity))

    return jsonify({"success": True, "bin_label": bin_label, "card_type": ct, "capacity": capacity})


@app.route("/api/display/cases/<case_id>", methods=["DELETE"])
def display_case_delete(case_id):
    """Delete an empty display case. Refuses if any cards are still in it —
    return them to storage first via the Return All flow."""
    row = db.query_one("""
        SELECT sl.id, sl.bin_label, sl.current_count, sr.location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (case_id,))
    if not row or row["location_type"] != "display_case":
        return jsonify({"error": "Display case not found"}), 404
    if row["current_count"] > 0:
        return jsonify({
            "error": f"{row['bin_label']} still holds {row['current_count']} card(s). "
                     "Return them to storage before deleting."
        }), 409

    db.execute("DELETE FROM storage_locations WHERE id::text = %s", (case_id,))
    return jsonify({"success": True})


# ═══════════════════════════════════════════════════════════════════════════════
# Display Case — Set Out (suggest + scan + finalize) and Return All
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/display/set-out/suggest")
def display_suggest():
    """Score cards in storage and return a shopping list scoped to the chosen
    case's game type with a balanced price-tier mix. Caps at the case's free
    slots — if the case is at 89/100, suggesting more than 11 is meaningless."""
    try:
        count = max(1, min(int(request.args.get("count", 50)), 200))
    except (TypeError, ValueError):
        return jsonify({"error": "count must be an integer"}), 400

    case_id = request.args.get("case_id", "").strip()
    case = None
    if case_id:
        case = db.query_one("""
            SELECT sl.id, sl.bin_label, sl.card_type, sl.capacity, sl.current_count,
                   (sl.capacity - sl.current_count) AS available, sr.location_type
            FROM storage_locations sl
            JOIN storage_rows sr ON sl.row_id = sr.id
            WHERE sl.id::text = %s
        """, (case_id,))
        if not case or case["location_type"] != "display_case":
            return jsonify({"error": "Display case not found"}), 404

    # No case_id given (legacy callers) → score broadly without game scoping.
    card_type = case["card_type"] if case else "mixed"
    capacity  = case["capacity"]  if case else max(count, 1)
    available = case["available"] if case else count

    if case and available <= 0:
        return jsonify({"suggestions": [], "total": 0, "case": _ser(dict(case))})

    count = min(count, available)

    # Pull a generous candidate pool — the price score skews premium, so a
    # tight LIMIT clips out the low/mid-tier cards the quota pass needs.
    candidates = db.query(
        _SCORE_SELECT_SQL.format(game_filter=_game_filter_sql(card_type)),
        (max(count * 5, 500),),
    )

    # Seed the (name, variant) cap with what's already in the case so we never
    # suggest a copy of a card the case already has — display cases hold one of
    # each, no duplicates.
    by_card_seed = {}
    if case:
        existing = db.query("""
            SELECT card_name, COALESCE(LOWER(variant), '') AS variant
            FROM raw_cards WHERE state = 'DISPLAY' AND bin_id = %s
        """, (case["id"],))
        for e in existing:
            cv = (e["card_name"], e["variant"] or "")
            by_card_seed[cv] = by_card_seed.get(cv, 0) + 1

    # Refill weighs against what's already in the case — if all the premium
    # cards sold, the deficit pass pulls premium back in instead of just
    # piling on more of whichever tier is already over-represented.
    current_counts = _current_tier_counts(case["id"]) if case else {t: 0 for t in PRICE_TIERS}
    quotas = _compute_tier_quotas(capacity, count, current_counts)

    # Multi-game cases ('other', 'mixed') split as evenly as possible across
    # whichever games actually have storage stock so one TCG can't dominate
    # when its stock dwarfs the others (e.g. lots of MTG, only a handful of
    # OP). Backfill kicks in for games with low inventory — they just get
    # fewer picks instead of blocking the case from filling.
    game_cap = None
    if card_type in MULTI_GAME_TYPES:
        distinct_games = db.query(f"""
            SELECT COUNT(DISTINCT rc.game) AS n
            FROM raw_cards rc
            WHERE rc.state = 'STORED'
              AND rc.current_hold_id IS NULL
              AND rc.current_price IS NOT NULL
              AND rc.current_price >= 1.0
              AND {_game_filter_sql(card_type)}
        """)
        n_games = max(1, (distinct_games[0]["n"] if distinct_games else 1))
        game_cap = max(1, -(-count // n_games))  # ceil(count / n_games)

    chosen = _greedy_pick_with_quotas(
        candidates, count,
        allowed_conditions=("NM", "LP"),
        tier_quotas=quotas,
        name_cap=1,   # one copy of each (name, variant) — no duplicates in the case
        set_cap=10,
        by_card_seed=by_card_seed,
        game_cap=game_cap,
    )

    return jsonify({
        "suggestions": [_ser(dict(c)) for c in chosen],
        "total":       len(chosen),
        "case":        _ser(dict(case)) if case else None,
    })


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
    if _resolve_hold_lock(card):
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
    """Validate a barcode for the Return-to-Storage flow. Accepts DISPLAY or STORED cards."""
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
    if card["state"] not in ("DISPLAY", "STORED"):
        return jsonify({"error": f"Card is {card['state']}, can't return to storage", "barcode": barcode, "card_name": card["card_name"]}), 409

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
        WHERE barcode = ANY(%s) AND state IN ('DISPLAY', 'STORED')
    """, (barcodes,))
    if not cards:
        return jsonify({"error": "None of the scanned cards can be returned to storage"}), 400

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
    if _resolve_hold_lock(card):
        return jsonify({"error": "Card is on hold for a customer", "barcode": barcode, "card_name": card["card_name"]}), 409
    # Sellable from anywhere physically in the store — bin, binder, glass.
    # Walk-up customer says "got Charizard 196?" → puller grabs it from the
    # bin and rings it up. No synthetic hold required.
    # PENDING_SALE = already on Sell screen; MISSING/GONE/PENDING_RETURN are
    # mid-recovery and shouldn't be re-sold without going through their flow.
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
               current_price, image_url, tcgplayer_id, state, current_hold_id,
               shopify_product_id
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
        # Scan-to-sell accepts anything not locked to an active hold. Walk-up
        # customers ask "got X?", staff finds it in bin/binder/glass, scans
        # at the laptop. The hold-lock check (current_hold_id IS NULL) is
        # the only thing that protects against double-allocation; zone is
        # not security, it was theater.
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
    # Paid Champion cards belong to the Shipping Queue, not the register —
    # exclude them so a barcode scan on the Sell tab can't yank a listing
    # that's already committed to an online order.
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
          AND NOT EXISTS (
            SELECT 1 FROM hold_items hi2
            JOIN holds h2 ON hi2.hold_id = h2.id
            WHERE hi2.raw_card_id = rc.id
              AND h2.cohort = 'champion'
              AND h2.checkout_status = 'completed'
              AND h2.status NOT IN ('ACCEPTED','RETURNED','CANCELLED','AUTO_EXPIRED')
          )
        ORDER BY rc.updated_at DESC NULLS LAST
    """)

    # Auto-heal: if the Shopify product is already archived or gone,
    # the card was sold but the DB update didn't commit (e.g. webhook
    # used db.query instead of db.execute). Mark it SOLD now.
    clean = []
    live = []
    for r in rows:
        pid = r.get("shopify_product_id")
        if pid:
            try:
                prod = _shopify("GET", f"/products/{pid}.json").get("product", {})
                if prod.get("status") == "archived":
                    db.execute("""
                        UPDATE raw_cards
                        SET state = 'SOLD', current_hold_id = NULL,
                            removal_reason = 'SOLD',
                            removal_date = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s AND state = 'PENDING_SALE'
                    """, (str(r["id"]),))
                    logger.info(f"Auto-healed PENDING_SALE → SOLD: {r['barcode']} (product archived)")
                    clean.append(r["barcode"])
                    continue
            except Exception:
                # Product deleted or API error — also means it's gone
                db.execute("""
                    UPDATE raw_cards
                    SET state = 'SOLD', current_hold_id = NULL,
                        removal_reason = 'SOLD',
                        removal_date = CURRENT_TIMESTAMP,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND state = 'PENDING_SALE'
                """, (str(r["id"]),))
                logger.info(f"Auto-healed PENDING_SALE → SOLD: {r['barcode']} (product gone)")
                clean.append(r["barcode"])
                continue
        live.append(_ser(dict(r)))

    return jsonify({"cards": live})


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

    # Paid Champion cards live in the Shipping Queue — never pull at the register.
    champ = db.query_one("""
        SELECT h.id FROM hold_items hi
        JOIN holds h ON hi.hold_id = h.id
        WHERE hi.raw_card_id = %s
          AND h.cohort = 'champion'
          AND h.checkout_status = 'completed'
          AND h.status NOT IN ('ACCEPTED','RETURNED','CANCELLED','AUTO_EXPIRED')
        LIMIT 1
    """, (str(card["id"]),))
    if champ:
        return jsonify({
            "error": "This card belongs to a paid Champion order — handle it from the Shipping tab in Screening, not the register."
        }), 409

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
               current_price, image_url, tcgplayer_id, state,
               shopify_product_id
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
    """List every binder with its capacity meter (headers only).

    Cards are NOT included — binders hold hundreds of cards each, so loading
    them all up front was the main reason the Binders tab was slow. The UI
    fetches a binder's cards lazily via /api/binders/<id>/cards on expand."""
    binders = get_binder_capacity(db)
    out = [{
        "id":            str(b["id"]),
        "bin_label":     b["bin_label"],
        "capacity":      b["capacity"],
        "current_count": b["current_count"],
        "available":     b["available"],
    } for b in binders]
    return jsonify({"binders": out})


@app.route("/api/binders/<binder_id>/cards")
def binder_cards(binder_id):
    """Cards currently in one binder — fetched lazily when the binder is
    expanded in the UI."""
    cards = db.query("""
        SELECT id, barcode, card_name, set_name, card_number, condition,
               current_price, image_url, variant, tcgplayer_id, scrydex_id,
               game, stored_at
        FROM raw_cards
        WHERE state = 'DISPLAY' AND bin_id = %s
        ORDER BY card_name ASC
    """, (str(binder_id),))
    return jsonify({"cards": [_ser(dict(r)) for r in cards]})


@app.route("/api/binders/<binder_id>/fill-suggest")
def binder_fill_suggest(binder_id):
    """Shopping list for a specific binder: same scoring + price-tier
    distribution as set-out, plus a name-cap seeded by what's *already in this
    binder* so a binder with 2 Charizards never gets a third recommendation.
    Filtered by the binder's card_type (pokemon-only, magic-only, etc.)."""
    try:
        count = max(1, min(int(request.args.get("count", 50)), 480))
    except (TypeError, ValueError):
        return jsonify({"error": "count must be an integer"}), 400

    binder = db.query_one("""
        SELECT sl.id, sl.bin_label, sl.card_type, sl.capacity, sl.current_count,
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

    # Existing contents — seed by_card cap so over-represented copies aren't
    # suggested again.
    existing = db.query("""
        SELECT card_name, COALESCE(LOWER(variant), '') AS variant
        FROM raw_cards WHERE state = 'DISPLAY' AND bin_id = %s
    """, (binder_id,))
    by_card_seed = {}
    for e in existing:
        cv = (e["card_name"], e["variant"] or "")
        by_card_seed[cv] = by_card_seed.get(cv, 0) + 1

    candidates = db.query(
        _SCORE_SELECT_SQL.format(game_filter=_game_filter_sql(binder["card_type"])),
        (max(count * 5, 500),),
    )

    current_counts = _current_tier_counts(binder["id"])
    quotas = _compute_tier_quotas(binder["capacity"], count, current_counts)

    chosen = _greedy_pick_with_quotas(
        candidates, count,
        # Binders are customer-touch; MP allowed (softer than display case),
        # DMG/HP still excluded.
        allowed_conditions=("NM", "LP", "MP"),
        tier_quotas=quotas,
        name_cap=2,
        set_cap=30,  # softer set cap than display case (binders hold more)
        by_card_seed=by_card_seed,
    )

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
    if _resolve_hold_lock(card):
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

    # Apply BOTH tcg_id and scrydex_id when present — one Scrydex card_id
    # can hold multiple printings (Mew ex 205 has separate tcg ids for the
    # holofoil and the 151 metal card), so filtering only by scrydex_id
    # would mix them. tcg_id alone is also fine on its own when scrydex
    # isn't mapped yet.
    id_filter_parts = []
    if tcg_id:
        id_filter_parts.append("tcgplayer_id = %s")
        extra.append(tcg_id)
    if sx_id:
        id_filter_parts.append("scrydex_id = %s")
        extra.append(sx_id)
    id_filter = ("AND " + " AND ".join(id_filter_parts)) if id_filter_parts else ""

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

    # `name` / `set_name` are English-first because they're written straight
    # to raw_cards.card_name → Shopify listing title via _create_raw_listing.
    # Customers can't search the store with JP characters, so JP printings
    # need an English title. `name_native` / `set_name_native` carry the
    # original (e.g. カツラのウインディ / 闇からの挑戦) so the relink picker
    # can show both for operator confirmation.
    return jsonify({"results": [{
        "scrydex_id":   r.get("scrydex_id"),
        "tcgplayer_id": r.get("tcgplayer_id"),
        "name":         r.get("product_name_en") or r.get("product_name"),
        "set_name":     r.get("expansion_name_en") or r.get("expansion_name"),
        "name_native":     r.get("product_name"),
        "set_name_native": r.get("expansion_name"),
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


@app.route("/api/editor/copies/<copy_id>/mark-missing", methods=["POST"])
def editor_mark_missing(copy_id):
    """Flag a copy as MISSING from the editor — for cases where you know
    a card isn't where it should be without an active hold to drive it.
    Refuses to clobber a copy locked by an active hold (use the hold's
    Can't Find flow instead, so the hold_item gets the right attribution).
    """
    card = db.query_one("""
        SELECT rc.id, rc.state, rc.current_hold_id, rc.card_name,
               h.status AS hold_status, h.id AS hold_short_id
        FROM raw_cards rc
        LEFT JOIN holds h ON h.id = rc.current_hold_id
        WHERE rc.id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404
    if card["state"] not in ("STORED", "DISPLAY"):
        return jsonify({"error": f"Can only mark STORED/DISPLAY copies missing — this one is {card['state']}"}), 409
    if card.get("hold_status") in _ACTIVE_HOLD_STATUSES:
        return jsonify({
            "error": "This copy is assigned to an active hold. "
                     "Use the hold's Can't Find flow so the hold gets attributed correctly."
        }), 409

    db.execute("""
        UPDATE raw_cards
        SET state = 'MISSING',
            current_hold_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (copy_id,))
    return jsonify({"success": True, "state": "MISSING", "card_name": card["card_name"]})


@app.route("/api/editor/copies/<copy_id>/unmark-missing", methods=["POST"])
def editor_unmark_missing(copy_id):
    """Restore a wrongly-flagged MISSING copy. Owner/manager only — letting
    associates clear MISSING is a great way for a stolen card to get
    persistently un-flagged by the thief.

    If the row still has its last bin, restore to STORED there. Otherwise
    route to PENDING_RETURN so the next store_returns pass re-bins it.
    """
    user = getattr(g, "user", None) or {}
    role = (user.get("role") or "").lower()
    if role not in ("owner", "manager"):
        return jsonify({"error": "Owner or manager only."}), 403

    card = db.query_one("""
        SELECT id, state, bin_id, card_name FROM raw_cards WHERE id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404
    if card["state"] != "MISSING":
        return jsonify({"error": f"Copy is {card['state']}, not MISSING"}), 409

    new_state = "STORED" if card.get("bin_id") else "PENDING_RETURN"
    db.execute("""
        UPDATE raw_cards
        SET state = %s,
            current_hold_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (new_state, copy_id))
    return jsonify({"success": True, "state": new_state, "card_name": card["card_name"]})


@app.route("/api/cards/<copy_id>/regenerate-barcode", methods=["POST"])
def regenerate_card_barcode(copy_id):
    """Replace the barcode on a raw_cards row with a freshly-generated ID
    when the existing label is damaged/won't scan. Caller is expected to
    print a new label (GET /api/editor/copies/<copy_id>/barcode-image)
    and apply it to the physical card.

    Cascades to hold_items.barcode for any open hold_item attached to this
    copy — kiosk's staff scan-out endpoint matches on hi.barcode, so a
    silent update would orphan in-flight holds.
    """
    card = db.query_one("""
        SELECT id, barcode, card_name FROM raw_cards WHERE id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Copy not found"}), 404

    # Loop briefly to avoid the (extremely unlikely) collision with an
    # existing barcode. 5 tries against ~37^6 = 2.5B suffix space is plenty.
    new_bc = None
    for _ in range(5):
        candidate = generate_barcode_id()
        existing = db.query_one("SELECT 1 FROM raw_cards WHERE barcode = %s", (candidate,))
        if not existing:
            new_bc = candidate
            break
    if not new_bc:
        return jsonify({"error": "Could not generate a unique barcode"}), 500

    old_bc = card["barcode"]
    db.execute("""
        UPDATE raw_cards SET barcode = %s, updated_at = CURRENT_TIMESTAMP
        WHERE id::text = %s
    """, (new_bc, copy_id))
    db.execute("""
        UPDATE hold_items SET barcode = %s
        WHERE raw_card_id::text = %s
    """, (new_bc, copy_id))

    return jsonify({
        "success":     True,
        "card_name":   card["card_name"],
        "old_barcode": old_bc,
        "new_barcode": new_bc,
    })


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


# ═══════════════════════════════════════════════════════════════════════════════
# Audit — per-bin/binder scanner-led inventory check
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/audit/locations")
def audit_locations():
    """List every bin + binder with the count of cards the DB thinks live there
    (state STORED for bins, DISPLAY for binders). The Audit view uses this to
    populate the location dropdown plus an at-a-glance expected count."""
    rows = db.query(r"""
        SELECT sl.id, sl.bin_label,
               COALESCE(sr.location_type, 'bin') AS location_type,
               COUNT(rc.id) FILTER (
                 WHERE rc.bin_id = sl.id AND rc.state IN ('STORED', 'DISPLAY')
               ) AS expected_count
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        LEFT JOIN raw_cards rc ON rc.bin_id = sl.id
        WHERE COALESCE(sr.location_type, 'bin') IN ('bin', 'binder')
          AND COALESCE(sr.active, TRUE) = TRUE
        GROUP BY sl.id, sl.bin_label, sr.location_type
        ORDER BY sr.location_type DESC,
                 regexp_replace(sl.bin_label, '\d+$', '') ASC,
                 (substring(sl.bin_label from '\d+$'))::int ASC NULLS FIRST,
                 sl.bin_label ASC
    """)
    return jsonify({"locations": [_ser(dict(r)) for r in rows]})


@app.route("/api/audit/expected/<location_id>")
def audit_expected(location_id):
    """The set of cards the DB expects in this location — the audit's
    'unscanned' list. Each card carries enough metadata to render a row
    and submit it to /api/audit/mark-missing later."""
    loc = db.query_one("""
        SELECT sl.id, sl.bin_label,
               COALESCE(sr.location_type, 'bin') AS location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (str(location_id),))
    if not loc:
        return jsonify({"error": "Location not found"}), 404
    cards = db.query("""
        SELECT rc.id::text AS id, rc.barcode, rc.card_name, rc.condition,
               rc.set_name, rc.card_number, rc.variant, rc.game, rc.state
        FROM raw_cards rc
        WHERE rc.bin_id::text = %s AND rc.state IN ('STORED', 'DISPLAY')
        ORDER BY rc.card_name, rc.condition, rc.barcode
    """, (str(location_id),))
    return jsonify({
        "location": _ser(dict(loc)),
        "cards":    [_ser(dict(c)) for c in cards],
    })


@app.route("/api/audit/scan", methods=["POST"])
def audit_scan():
    """Resolve a scanned barcode against the currently-audited location.
    Statuses:
      EXPECTED          — at this location with a "should be here" state
      MISSING_RECOVERED — was MISSING, auto-restored to this location (the
                          'we might find them later' path Sean described)
      WRONG_BIN         — should be at another bin/binder; UI says where
      WRONG_STATE       — PULLED/PENDING_SALE/PENDING_RETURN/GONE — unexpected
      NOT_FOUND         — barcode not in raw_cards
    """
    data        = request.get_json() or {}
    barcode     = (data.get("barcode") or "").strip()
    location_id = data.get("location_id")
    if not barcode or not location_id:
        return jsonify({"error": "barcode and location_id required"}), 400

    loc = db.query_one("""
        SELECT sl.id, sl.bin_label,
               COALESCE(sr.location_type, 'bin') AS location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (str(location_id),))
    if not loc:
        return jsonify({"error": "Location not found"}), 404

    # Binders/display cases keep cards in DISPLAY state; storage bins use STORED.
    target_state = "DISPLAY" if loc["location_type"] in ("binder", "display_case") else "STORED"

    card = db.query_one("""
        SELECT rc.id::text AS id, rc.barcode, rc.card_name, rc.condition,
               rc.set_name, rc.card_number, rc.variant, rc.state,
               rc.bin_id::text AS bin_id, rc.current_hold_id,
               sl.bin_label AS current_bin_label,
               COALESCE(sr.location_type, 'bin') AS current_bin_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        LEFT JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE rc.barcode = %s
    """, (barcode,))

    if not card:
        return jsonify({"status": "NOT_FOUND", "barcode": barcode})

    card_d = _ser(dict(card))

    # EXPECTED — already here in a "should be here" state. Nothing to do.
    if card["bin_id"] == str(loc["id"]) and card["state"] in ("STORED", "DISPLAY"):
        return jsonify({"status": "EXPECTED", "card": card_d})

    # MISSING_RECOVERED — card was MISSING (lost during a pull) or GONE
    # (written off as permanently lost) and just turned up in this bin.
    # Sean's spec: "later on we might find them, and then we'd scan them into
    # where they belong." No sale was ever attached to these states, so we
    # just re-bind the same row to this location's natural state.
    if card["state"] in ("MISSING", "GONE"):
        db.execute("""
            UPDATE raw_cards
            SET state = %s, bin_id = %s, current_hold_id = NULL,
                stored_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id::text = %s
        """, (target_state, loc["id"], card["id"]))
        was_state = card["state"]
        card_d["state"] = target_state
        card_d["bin_id"] = str(loc["id"])
        return jsonify({
            "status":      "MISSING_RECOVERED",
            "card":        card_d,
            "restored_to": loc["bin_label"],
            "was_state":   was_state,
        })

    # WRONG_BIN — DB says it lives in another bin/binder. Don't move it; staff
    # will get to that bin's audit and the card will resolve as EXPECTED there.
    if card["state"] in ("STORED", "DISPLAY") and card["bin_id"]:
        return jsonify({
            "status":       "WRONG_BIN",
            "card":         card_d,
            "should_be_at": card["current_bin_label"] or "(unknown bin)",
        })

    # WRONG_STATE — PULLED with an active hold, PENDING_SALE, PENDING_RETURN,
    # GONE, or STORED/DISPLAY with no bin. All unexpected to find here.
    return jsonify({
        "status":        "WRONG_STATE",
        "card":          card_d,
        "current_state": card["state"],
    })


@app.route("/api/audit/restore", methods=["POST"])
def audit_restore_swap():
    """Barcode-swap recovery during an audit.

    Scenario: at POS, staff scanned card A's barcode but the customer walked
    out with card B (same identity, different physical copy). Card A's
    raw_cards row is now PENDING_SALE attached to the Shopify draft — but card
    A is still physically here in this bin. We don't touch the original row
    (Shopify reconciliation depends on it). Instead, we clone the identity
    into a fresh STORED/DISPLAY row at this audit's location with a new
    barcode. The operator reprints the label and applies it to the physical
    card. Card B (the one that actually left) shows up as MISSING whenever
    its home location is audited next — that's correct.
    """
    data        = request.get_json() or {}
    original_id = (data.get("original_card_id") or "").strip()
    location_id = data.get("location_id")
    if not original_id or not location_id:
        return jsonify({"error": "original_card_id and location_id required"}), 400

    loc = db.query_one("""
        SELECT sl.id, sl.bin_label,
               COALESCE(sr.location_type, 'bin') AS location_type
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.id::text = %s
    """, (str(location_id),))
    if not loc:
        return jsonify({"error": "Location not found"}), 404

    original = db.query_one("""
        SELECT id, barcode, card_name, state
        FROM raw_cards WHERE id::text = %s
    """, (original_id,))
    if not original:
        return jsonify({"error": "Original card not found"}), 404
    # PENDING_SALE (active Shopify draft) and SOLD (order paid / product
    # archived) both carry a real sale transaction. Cloning preserves that
    # record while putting the physically-present card back in inventory.
    if original["state"] not in ("PENDING_SALE", "SOLD"):
        return jsonify({
            "error": f"Restore only works on sold cards (this one is {original['state']}).",
        }), 409

    target_state = "DISPLAY" if loc["location_type"] in ("binder", "display_case") else "STORED"

    # Tight retry loop for the (vanishingly unlikely) barcode collision; matches
    # the pattern in /api/cards/<id>/regenerate-barcode.
    new_bc = None
    for _ in range(5):
        candidate = generate_barcode_id()
        if not db.query_one("SELECT 1 FROM raw_cards WHERE barcode = %s", (candidate,)):
            new_bc = candidate
            break
    if not new_bc:
        return jsonify({"error": "Could not generate a unique barcode"}), 500

    # Clone identity columns from the PENDING_SALE row. Anything not named here
    # (current_hold_id, shopify_*, removal_*) defaults/NULLs out — exactly what
    # we want for a fresh stored copy.
    new_row = db.execute_returning("""
        INSERT INTO raw_cards (
            barcode, tcgplayer_id, scrydex_id, card_name, set_name,
            card_number, condition, rarity,
            state, cost_basis, current_price, last_price_update,
            bin_id, image_url,
            is_graded, grade_company, grade_value,
            variant, language,
            intake_session_id, intake_item_id,
            stored_at, created_at, updated_at
        )
        SELECT
            %s, tcgplayer_id, scrydex_id, card_name, set_name,
            card_number, condition, rarity,
            %s, cost_basis, current_price, last_price_update,
            %s, image_url,
            is_graded, grade_company, grade_value,
            variant, language,
            intake_session_id, intake_item_id,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM raw_cards
        WHERE id = %s
        RETURNING id::text AS id, barcode, card_name, set_name, card_number,
                  condition, variant, state, bin_id::text AS bin_id
    """, (new_bc, target_state, loc["id"], original["id"]))

    if not new_row:
        return jsonify({"error": "Failed to clone card"}), 500

    return jsonify({
        "success":        True,
        "card":           _ser(dict(new_row)),
        "old_barcode":    original["barcode"],
        "location_label": loc["bin_label"],
    })


@app.route("/api/audit/mark-missing", methods=["POST"])
def audit_mark_missing():
    """Bulk-flip the unscanned tail of an audit to MISSING. Only touches rows
    still in STORED/DISPLAY so a concurrent change (e.g. a card got pulled
    for a hold mid-audit) doesn't get clobbered."""
    data     = request.get_json() or {}
    card_ids = [str(i) for i in (data.get("card_ids") or []) if i]
    if not card_ids:
        return jsonify({"error": "No card_ids provided"}), 400
    n = db.execute("""
        UPDATE raw_cards
        SET state = 'MISSING', current_hold_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE id::text = ANY(%s) AND state IN ('STORED', 'DISPLAY')
    """, (card_ids,))
    return jsonify({"success": True, "marked": n or 0})


# ═══════════════════════════════════════════════════════════════════════════════
# Game-tag audit (owner-only) — the detector that replaces a per-card game toggle
# ═══════════════════════════════════════════════════════════════════════════════
#
# Cards carry a `game` field that drives bin routing. The intake hole that let a
# Magic "Final Fantasy" single get tagged 'pokemon' is closed going forward
# (explicit game required at manual entry + set-name routing backstop), but a
# card whose set isn't in the Scrydex cache can still drift in. Rather than put a
# free-form game toggle on every card (a new mislabel vector that every operator
# can fumble), this is a read-only detector: it cross-checks each on-shelf card's
# stored game against what its set name resolves to, and lets an owner one-click
# correct only the genuine conflicts.

def _require_owner():
    """Returns None if the caller is an owner, else a (json, status) 403 tuple."""
    user = getattr(g, "user", None) or {}
    if (user.get("role") or "").lower() != "owner":
        return jsonify({"error": "Owner only."}), 403
    return None


def _load_set_game_map():
    """expansion_name(lower) -> canonical card_type, for expansions that resolve
    to exactly one game in the cache. One query; mirrors infer_card_type_from_set
    but bulk so the audit doesn't fire a query per card."""
    rows = db.query("""
        SELECT lower(trim(expansion_name)) AS exp, array_agg(DISTINCT game) AS games
        FROM scrydex_price_cache
        WHERE expansion_name IS NOT NULL AND game IS NOT NULL
        GROUP BY lower(trim(expansion_name))
    """)
    out = {}
    for r in rows:
        games = [g for g in (r["games"] or []) if g]
        if len(games) == 1:
            out[r["exp"]] = _canonical_card_type(games[0])
    return out


@app.route("/api/audit/game-tags")
def audit_game_tags():
    """Owner-only. Every on-shelf card whose stored game disagrees with the game
    its set name resolves to in the Scrydex cache. These are the routing
    landmines — a card tagged for the wrong game lands in the wrong bin."""
    deny = _require_owner()
    if deny:
        return deny

    set_game = _load_set_game_map()
    cards = db.query("""
        SELECT rc.id, rc.barcode, rc.card_name, rc.set_name, rc.game,
               rc.scrydex_id, rc.tcgplayer_id, rc.state,
               sl.bin_label, sl.card_type AS bin_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.state IN ('STORED','DISPLAY','PENDING_RETURN','PULLED','MISSING')
    """)

    mismatches = []
    for rc in cards:
        stored = _canonical_card_type(rc["game"] or "pokemon")
        inferred = set_game.get((rc["set_name"] or "").strip().lower())
        if inferred and inferred != stored:
            row = _ser(dict(rc))
            row["stored_game"] = stored
            row["inferred_game"] = inferred
            # tag-only fix when the card already sits in a location matching the
            # real game; otherwise correcting it also has to re-route the card.
            row["location_ok"] = (rc.get("bin_type") == inferred)
            row["link"] = "manual" if not rc["scrydex_id"] else "linked"
            mismatches.append(row)

    mismatches.sort(key=lambda r: (r["stored_game"], r["inferred_game"], r.get("set_name") or ""))
    return jsonify({"mismatches": mismatches, "scanned": len(cards)})


@app.route("/api/audit/game-tags/<copy_id>/fix", methods=["POST"])
def audit_game_tags_fix(copy_id):
    """Owner-only. Correct one card's game to what its set name resolves to.
    If the card is STORED in a bin that doesn't match the real game, also route
    it to the Return Queue so the next store pass re-bins it correctly (mirrors
    how a mis-binned card is handled). Cards already in the right kind of
    location get a tag-only fix."""
    deny = _require_owner()
    if deny:
        return deny

    card = db.query_one("""
        SELECT rc.id, rc.card_name, rc.set_name, rc.game, rc.state,
               sl.card_type AS bin_type
        FROM raw_cards rc
        LEFT JOIN storage_locations sl ON rc.bin_id = sl.id
        WHERE rc.id::text = %s
    """, (copy_id,))
    if not card:
        return jsonify({"error": "Card not found"}), 404

    inferred = infer_card_type_from_set(card.get("set_name"), db)
    if not inferred:
        return jsonify({"error": "Set name no longer resolves to a single game — fix manually."}), 409

    # Re-route only when the card is physically stored in a bin of the wrong
    # game; display/binder cards and correctly-located storage just get the tag.
    needs_reroute = card["state"] == "STORED" and card.get("bin_type") not in (None, inferred)
    if needs_reroute:
        db.execute("""
            UPDATE raw_cards
            SET game = %s, state = 'PENDING_RETURN', updated_at = CURRENT_TIMESTAMP
            WHERE id::text = %s
        """, (inferred, copy_id))
    else:
        db.execute("""
            UPDATE raw_cards
            SET game = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id::text = %s
        """, (inferred, copy_id))

    return jsonify({
        "success": True,
        "game": inferred,
        "rerouted": needs_reroute,
        "card_name": card["card_name"],
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5006)), debug=False)
