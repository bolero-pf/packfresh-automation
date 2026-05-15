"""
screening — screening.pack-fresh.com
Order screening + review console: verification queue, combine shipping queue.
"""
# trigger redeploy 2

import os
import logging
from flask import Flask, request, jsonify, render_template_string, g

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = Flask(__name__)

from routes import bp as screening_bp
app.register_blueprint(screening_bp)

import db
db.init_pool()
db.execute("""
    CREATE TABLE IF NOT EXISTS customer_notes (
        id SERIAL PRIMARY KEY,
        customer_email TEXT NOT NULL,
        customer_name TEXT,
        note_type TEXT NOT NULL DEFAULT 'note',
        note_text TEXT NOT NULL,
        active BOOLEAN NOT NULL DEFAULT true,
        created_by TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
""")
db.execute("CREATE INDEX IF NOT EXISTS idx_customer_notes_email ON customer_notes(customer_email)")

# Easter egg promo tables
try:
    db.execute("""
        CREATE TABLE IF NOT EXISTS easter_egg_pool (
            id                SERIAL PRIMARY KEY,
            tier              TEXT NOT NULL,
            claimed_by_order  TEXT,
            claimed_by_email  TEXT,
            claimed_at        TIMESTAMPTZ
        )
    """)
    # Seed pool only if empty
    existing = db.query_one("SELECT COUNT(*) AS cnt FROM easter_egg_pool")
    if not existing or existing["cnt"] == 0:
        db.execute("""
            INSERT INTO easter_egg_pool (tier) VALUES
            ('stink'),('stink'),('bronze'),('silver'),('bronze'),('bronze'),('bronze'),('bronze'),('stink'),('silver'),
            ('stink'),('stink'),('silver'),('stink'),('stink'),('stink'),('stink'),('stink'),('gold'),('bronze'),
            ('silver'),('silver'),('gold'),('silver'),('stink'),('stink'),('bronze'),('stink'),('bronze'),('stink'),
            ('stink'),('stink'),('stink'),('stink'),('bronze'),('bronze'),('stink'),('stink'),('bronze'),('bronze'),
            ('stink'),('bronze'),('bronze'),('silver'),('silver'),('stink'),('bronze'),('gold'),('silver'),('stink'),
            ('stink'),('stink'),('stink'),('stink'),('stink'),('bronze'),('stink'),('bronze'),('stink'),('bronze'),
            ('bronze'),('stink'),('silver'),('stink'),('silver'),('silver'),('stink'),('stink'),('stink'),('bronze'),
            ('bronze'),('gold'),('stink'),('bronze'),('bronze'),('bronze'),('stink'),('stink'),('stink'),('stink'),
            ('silver'),('stink'),('stink'),('silver'),('stink'),('bronze'),('bronze'),('bronze'),('stink'),('stink'),
            ('stink'),('stink'),('bronze'),('silver'),('bronze'),('stink'),('bronze'),('gold'),('stink'),('stink')
        """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS easter_egg_log (
            id              SERIAL PRIMARY KEY,
            order_gid       TEXT NOT NULL,
            order_name      TEXT NOT NULL,
            customer_email  TEXT,
            order_total     NUMERIC(10,2),
            has_collection_box BOOLEAN,
            eligible        BOOLEAN NOT NULL,
            ineligible_reason TEXT,
            tier            TEXT,
            was_live        BOOLEAN NOT NULL DEFAULT false,
            logged_at       TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_egg_log_order_gid ON easter_egg_log(order_gid)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_egg_log_logged_at ON easter_egg_log(logged_at DESC)")
except Exception as e:
    print(f"[screening] Easter egg migration failed: {e}", flush=True)

# Screening event log — append-only, never updated/deleted
db.execute("""
    CREATE TABLE IF NOT EXISTS screening_log (
        id              SERIAL PRIMARY KEY,
        order_gid       TEXT NOT NULL,
        order_name      TEXT,
        customer_email  TEXT,
        event_type      TEXT NOT NULL,
        check_type      TEXT NOT NULL,
        details         JSONB DEFAULT '{}',
        created_at      TIMESTAMPTZ DEFAULT NOW()
    )
""")
db.execute("CREATE INDEX IF NOT EXISTS idx_screening_log_order ON screening_log(order_gid)")
db.execute("CREATE INDEX IF NOT EXISTS idx_screening_log_type ON screening_log(event_type, check_type)")
db.execute("CREATE INDEX IF NOT EXISTS idx_screening_log_created ON screening_log(created_at DESC)")

from auth import register_auth_hooks
register_auth_hooks(app, roles=["owner", "manager", "associate"], public_prefixes=('/screening/',))


# Associates only get the Combine tab. Owner + manager see everything.
# Routes not in this allow-list 403 for associates.
_ASSOCIATE_ALLOWED_PATHS = {
    "/",                          # console index — JS hides non-combine tabs
    "/api/held-orders",           # combine cards + verification empty for associate
    "/api/uncombine-order",       # combine action
    "/api/release-and-fulfill",   # combine "Release & Ship" with tracking
    "/api/raw-card-pulls",        # paid Champion holds awaiting pull
    "/health", "/ping", "/favicon.ico",
}
_ASSOCIATE_ALLOWED_PREFIX_PATHS = ("/api/raw-card-pulls/",)  # /<hold_id>/mark-shipped etc.
_ASSOCIATE_ALLOWED_PREFIXES = ("/screening/", "/static", "/pf-static")


@app.before_request
def _gate_associate_routes():
    user = getattr(g, "user", None)
    if not user or user.get("role") != "associate":
        return None
    path = request.path
    if path in _ASSOCIATE_ALLOWED_PATHS:
        return None
    for prefix in _ASSOCIATE_ALLOWED_PREFIXES:
        if path.startswith(prefix):
            return None
    for prefix in _ASSOCIATE_ALLOWED_PREFIX_PATHS:
        if path.startswith(prefix):
            return None
    return jsonify({"error": "Not authorized"}), 403


@app.route("/")
def index():
    store = os.environ.get("SHOPIFY_STORE", "").replace(".myshopify.com", "")
    return render_template_string(CONSOLE_HTML, shopify_store=store)


@app.route("/api/screening-history")
def api_screening_history():
    """Query screening_log with filters: event_type, check_type, date range, search, pagination."""
    from datetime import datetime, timedelta

    event_type = request.args.get("event_type", "").strip()
    check_type = request.args.get("check_type", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    search = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", "1")))
    per_page = 50

    conditions = []
    params = []

    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)
    if check_type:
        conditions.append("check_type = %s")
        params.append(check_type)
    if date_from:
        conditions.append("created_at >= %s")
        params.append(date_from)
    if date_to:
        # Include the full end day
        conditions.append("created_at < %s::date + interval '1 day'")
        params.append(date_to)
    if search:
        conditions.append("(order_name ILIKE %s OR customer_email ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    count_row = db.query_one(f"SELECT COUNT(*) AS cnt FROM screening_log {where}", tuple(params))
    total = count_row["cnt"] if count_row else 0

    rows = db.query(
        f"SELECT * FROM screening_log {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
        tuple(params) + (per_page, (page - 1) * per_page),
    )

    # Serialize
    import json
    events = []
    for r in rows:
        events.append({
            "id": r["id"],
            "order_gid": r["order_gid"],
            "order_name": r["order_name"],
            "customer_email": r["customer_email"],
            "event_type": r["event_type"],
            "check_type": r["check_type"],
            "details": r["details"] if isinstance(r["details"], dict) else json.loads(r["details"] or "{}"),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return jsonify({
        "events": events,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": max(1, -(-total // per_page)),
    })


@app.route("/api/held-orders")
def api_held_orders():
    """Fetch all orders with hold-for-review tag."""
    from shopify_graphql import shopify_gql, gid_numeric

    data = shopify_gql("""
        query($first:Int!, $q:String!) {
          orders(first:$first, query:$q, sortKey:CREATED_AT, reverse:true) {
            edges {
              node {
                id
                name
                createdAt
                tags
                note
                currentTotalPriceSet { shopMoney { amount } }
                displayFulfillmentStatus
                customer {
                  id email firstName lastName
                }
                shippingAddress { firstName lastName address1 city province zip }
                lineItems(first:20) {
                  edges { node { title quantity sku image { url } } }
                }
              }
            }
          }
        }
    """, {"first": 50, "q": 'tag:"hold-for-review"'})

    verification = []
    combine = []

    def _bin_lookup(orders_list):
        """Annotate each line item with bin_label/is_raw if its SKU matches a
        raw_cards.barcode. The Shopify product is deleted on Champion sale,
        but the line item's SKU is preserved on the order itself — so this
        join works even after deletion."""
        all_skus = set()
        for o in orders_list:
            for item in o.get("items", []):
                sku = (item.get("sku") or "").strip()
                if sku:
                    all_skus.add(sku)
        if not all_skus:
            return
        placeholders = ",".join(["%s"] * len(all_skus))
        rows = db.query(
            f"""
            SELECT rc.barcode, rc.condition, rc.variant,
                   sl.bin_label, COALESCE(sr.location_type, 'bin') AS bin_type
              FROM raw_cards rc
              LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
              LEFT JOIN storage_rows sr ON sr.id = sl.row_id
             WHERE rc.barcode IN ({placeholders})
            """,
            tuple(all_skus),
        )
        by_sku = {r["barcode"]: r for r in rows}
        for o in orders_list:
            for item in o.get("items", []):
                m = by_sku.get((item.get("sku") or "").strip())
                if m:
                    item["is_raw"] = True
                    item["bin_label"] = m.get("bin_label")
                    item["bin_type"] = m.get("bin_type")
                    item["condition"] = m.get("condition")
                    item["variant"] = m.get("variant")

    for edge in data.get("data", {}).get("orders", {}).get("edges", []):
        o = edge["node"]
        # Skip fulfilled orders — they've already been shipped
        if o.get("displayFulfillmentStatus") in ("FULFILLED", "PARTIALLY_FULFILLED"):
            continue
        tags = [t.lower() for t in (o.get("tags") or [])]
        customer = o.get("customer") or {}
        addr = o.get("shippingAddress") or {}
        items = [{"title": e["node"]["title"], "qty": e["node"]["quantity"],
                  "sku": (e["node"].get("sku") or "").strip(),
                  "image": (e["node"].get("image") or {}).get("url", "")}
                 for e in o.get("lineItems", {}).get("edges", [])]
        note = o.get("note") or ""

        order_data = {
            "id": o["id"],
            "numeric_id": gid_numeric(o["id"]),
            "name": o["name"],
            "created_at": o["createdAt"],
            "total": float(o.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0)),
            "fulfillment_status": o.get("displayFulfillmentStatus"),
            "customer_name": f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip(),
            "customer_email": customer.get("email", ""),
            "customer_id": customer.get("id"),
            "shipping_address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('province', '')} {addr.get('zip', '')}",
            "tags": o.get("tags", []),
            "note": note,
            "items": items,
        }

        # Determine type based on tags
        is_verification = any(t in tags for t in [
            "high-value-tier1", "high-value-tier2", "spend-spike-review",
            "fraud-medium", "firsttime5-review", "customer-hold"
        ])
        is_combine = "combine" in note.lower() if note else False

        if is_verification:
            # Determine specific type
            if "high-value-tier2" in tags:
                order_data["check_type"] = "ID + Selfie ($1000+)"
                order_data["is_cumulative"] = True
            elif "high-value-tier1" in tags:
                order_data["check_type"] = "ID Verification ($700+)"
                order_data["is_cumulative"] = True
            elif "spend-spike-review" in tags:
                order_data["check_type"] = "Spend Spike"
            elif "fraud-medium" in tags:
                order_data["check_type"] = "Medium Fraud"
            elif "firsttime5-review" in tags:
                order_data["check_type"] = "FIRSTTIME5 Abuse"
            elif "customer-hold" in tags:
                order_data["check_type"] = "Customer Hold"
            else:
                order_data["check_type"] = "Review"
            verification.append(order_data)
        elif is_combine:
            order_data["check_type"] = "Combine Shipping"
            combine.append(order_data)
        else:
            order_data["check_type"] = "Other Hold"
            verification.append(order_data)

    # Group cumulative verification orders by customer
    verification_groups = {}
    standalone_verification = []
    for o in verification:
        if o.get("is_cumulative"):
            key = o["customer_email"] or o["customer_name"]
            if key not in verification_groups:
                verification_groups[key] = {
                    "customer_name": o["customer_name"],
                    "customer_email": o["customer_email"],
                    "customer_id": o["customer_id"],
                    "check_type": o["check_type"],
                    "orders": [],
                    "total_value": 0,
                }
            verification_groups[key]["orders"].append(o)
            verification_groups[key]["total_value"] += o["total"]
            # Use highest tier check_type in the group
            if "Selfie" in o["check_type"]:
                verification_groups[key]["check_type"] = o["check_type"]
        else:
            standalone_verification.append(o)

    # Groups with 1 order go back to standalone list
    final_verification_groups = []
    for group in verification_groups.values():
        if len(group["orders"]) > 1:
            group["check_type"] = "Cumulative " + group["check_type"]
            final_verification_groups.append(group)
        else:
            standalone_verification.extend(group["orders"])

    # Enrich every order's items with bin labels for any SKU matching a
    # raw card barcode. Lets the combined packing list show bin locations
    # next to raw items (Champion or otherwise).
    _bin_lookup(verification + combine)

    # Group combine orders by customer
    combine_groups = {}
    for o in combine:
        key = o["customer_email"] or o["customer_name"]
        if key not in combine_groups:
            combine_groups[key] = {
                "customer_name": o["customer_name"],
                "customer_email": o["customer_email"],
                "shipping_address": o["shipping_address"],
                "orders": [],
                "total_value": 0,
                "all_items": [],
            }
        combine_groups[key]["orders"].append(o)
        combine_groups[key]["total_value"] += o["total"]
        # Consolidate duplicate SKUs in combined packing list. Raw cards are
        # each a unique physical copy with their own bin — never merge.
        for item in o["items"]:
            if item.get("is_raw"):
                combine_groups[key]["all_items"].append({**item})
                continue
            existing = next((a for a in combine_groups[key]["all_items"]
                           if not a.get("is_raw") and a["title"] == item["title"]), None)
            if existing:
                existing["qty"] += item["qty"]
            else:
                combine_groups[key]["all_items"].append({**item})

    # Associates only get combine — strip verification data so the JSON
    # can't leak PII even if an associate hits this URL directly.
    user = getattr(g, "user", None) or {}
    if user.get("role") == "associate":
        return jsonify({
            "verification": [],
            "verification_groups": [],
            "combine_groups": list(combine_groups.values()),
        })

    return jsonify({
        "verification": standalone_verification,
        "verification_groups": final_verification_groups,
        "combine_groups": list(combine_groups.values()),
    })


@app.route("/api/raw-card-pulls")
def api_raw_card_pulls():
    """Paid Champion holds awaiting outbound shipment — surfaced here so
    associates can do raw-card pulls without owning card_manager access.

    Cards are already state='SOLD' by the time they hit this view (kiosk
    webhook flips them at payment) and the Shopify listing is deleted in
    the same handler. The only handle we have is hold_items.raw_card_id,
    which the webhook leaves intact. Bin labels survive because raw_cards
    keeps its bin_id after the sale (puller needs to know where to look).

    Champion "checkout" merges raws into the customer's existing Shopify
    cart (kiosk creates listings and redirects to /pages/kiosk-add), so
    the paid order can also include sealed and any other storefront items
    the customer had in their cart. Those don't live in hold_items, so we
    pull the Shopify line items in a second batched GraphQL call and merge
    everything that isn't already in the hold as "other" items.

    Same call doubles as a cancellation probe: cancelledAt on the order
    marks the hold as CANCELLED in the response so the UI can gate Mark
    Pulled & Shipped and prompt the operator to restock instead of ship.
    """
    rows = db.query("""
        SELECT h.id::text AS hold_id,
               h.shopify_order_number,
               h.customer_name, h.customer_email,
               h.shipping_name, h.shipping_address,
               h.created_at,
               hi.id AS hold_item_id, hi.item_kind, hi.status AS hi_status,
               hi.title AS hi_title, hi.sku AS hi_sku, hi.unit_price AS hi_price,
               hi.barcode AS hi_barcode,
               rc.card_name, rc.set_name, rc.card_number,
               rc.condition, rc.variant,
               rc.current_price AS rc_price, rc.image_url AS rc_image,
               sl.bin_label, COALESCE(sr.location_type, 'bin') AS bin_type
          FROM holds h
          LEFT JOIN hold_items hi ON hi.hold_id = h.id
          LEFT JOIN raw_cards rc ON rc.id = hi.raw_card_id
          LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
          LEFT JOIN storage_rows sr ON sr.id = sl.row_id
         WHERE h.cohort = 'champion'
           AND h.checkout_status = 'completed'
           AND h.status NOT IN ('ACCEPTED','RETURNED','CANCELLED','AUTO_EXPIRED')
         ORDER BY h.created_at ASC, sl.bin_label NULLS LAST
    """)

    holds_by_id = {}
    for r in rows:
        hid = r["hold_id"]
        if hid not in holds_by_id:
            holds_by_id[hid] = {
                "hold_id": hid,
                "order_number": r.get("shopify_order_number"),
                "customer_name": r.get("customer_name"),
                "customer_email": r.get("customer_email"),
                "shipping_name": r.get("shipping_name"),
                "shipping_address": r.get("shipping_address"),
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                "items": [],
                "raw_count": 0,
                "sealed_count": 0,
                "cancelled": False,
                "cancelled_at": None,
                "financial_status": None,
            }
        if not r.get("hold_item_id"):
            continue
        kind = (r.get("item_kind") or "raw").lower()
        if kind == "raw":
            holds_by_id[hid]["items"].append({
                "kind": "raw",
                "hold_item_id": r.get("hold_item_id"),
                "hi_status": r.get("hi_status"),
                "barcode": r.get("hi_barcode"),
                "title": r.get("card_name"),
                "set_name": r.get("set_name"),
                "card_number": r.get("card_number"),
                "condition": r.get("condition"),
                "variant": r.get("variant"),
                "price": float(r["rc_price"]) if r.get("rc_price") is not None else None,
                "image_url": r.get("rc_image"),
                "bin_label": r.get("bin_label"),
                "bin_type": r.get("bin_type"),
            })
            holds_by_id[hid]["raw_count"] += 1
        else:
            holds_by_id[hid]["items"].append({
                "kind": kind,
                "title": r.get("hi_title"),
                "sku": r.get("hi_sku"),
                "price": float(r["hi_price"]) if r.get("hi_price") is not None else None,
                "bin_label": None,
            })
            holds_by_id[hid]["sealed_count"] += 1

    holds = list(holds_by_id.values())
    # ?fast=1 skips the Shopify GraphQL roundtrip so the badge count + initial
    # render are sub-second. The frontend does a second non-fast fetch to
    # progressively enrich the rendered holds with sealed items + cancellation.
    if request.args.get("fast") != "1":
        _merge_shopify_order_data(holds)
    return jsonify({"holds": holds})


def _merge_shopify_order_data(holds):
    """Fetch Shopify order data (line items + cancellation status) for every
    hold in `holds` in a single batched GraphQL call, then merge: (a) any
    line items not already in hold_items append as 'shopify' kind items so
    sealed and other storefront add-ons show up in the pull view, (b)
    cancelledAt flips the hold's cancelled flag for UI gating. Failures are
    swallowed and logged — the pull view still works without this enrichment,
    it just won't show sealed."""
    order_names = [h["order_number"] for h in holds if h.get("order_number")]
    if not order_names:
        return
    try:
        from shopify_graphql import shopify_gql
        # Shopify's name: filter is exact-match; OR'd to batch.
        q = " OR ".join(f'name:"{n}"' for n in order_names)
        data = shopify_gql("""
            query($first:Int!, $q:String!) {
              orders(first:$first, query:$q) {
                edges { node {
                  name
                  cancelledAt
                  displayFinancialStatus
                  displayFulfillmentStatus
                  lineItems(first:50) {
                    edges { node {
                      title
                      quantity
                      sku
                      image { url }
                      originalUnitPriceSet { shopMoney { amount } }
                    } }
                  }
                } }
              }
            }
        """, {"first": min(max(len(order_names), 1) * 2, 50), "q": q})
    except Exception as e:
        app.logger.warning(f"raw-card-pulls Shopify fetch failed: {e}")
        return

    order_map = {}
    for edge in (data.get("data", {}).get("orders", {}) or {}).get("edges", []):
        node = edge["node"]
        order_map[node.get("name")] = node

    for h in holds:
        node = order_map.get(h.get("order_number"))
        if not node:
            continue
        h["cancelled_at"] = node.get("cancelledAt")
        h["cancelled"] = bool(node.get("cancelledAt"))
        h["financial_status"] = node.get("displayFinancialStatus")
        h["fulfillment_status"] = node.get("displayFulfillmentStatus")

        # SKUs already represented by hold_items (raw barcodes + any sealed
        # tracked locally). Skip Shopify line items whose SKU matches one of
        # these so we don't double-list the raw cards.
        known_skus = set()
        for it in h["items"]:
            if it.get("barcode"):
                known_skus.add(str(it["barcode"]).strip())
            if it.get("sku"):
                known_skus.add(str(it["sku"]).strip())

        for li_edge in (node.get("lineItems", {}) or {}).get("edges", []):
            li = li_edge["node"]
            sku = (li.get("sku") or "").strip()
            if sku and sku in known_skus:
                continue
            price = 0.0
            try:
                price = float((li.get("originalUnitPriceSet") or {})
                              .get("shopMoney", {}).get("amount") or 0)
            except (TypeError, ValueError):
                price = 0.0
            h["items"].append({
                "kind": "shopify",
                "title": li.get("title"),
                "sku": sku or None,
                "qty": li.get("quantity", 1),
                "image_url": (li.get("image") or {}).get("url"),
                "price": price,
                "bin_label": None,
            })
            h["sealed_count"] += 1


@app.route("/api/raw-card-pulls/<hold_id>/close-cancelled", methods=["POST"])
def api_raw_card_pulls_close_cancelled(hold_id):
    """Close a paid Champion hold whose Shopify order was cancelled. Operator
    is expected to have already restocked every raw card via the per-item
    Restock action; this just flips the hold and any remaining unresolved
    items so the row drops off the queue. We don't auto-restock cards here —
    state changes for raw_cards must go through the per-item endpoint so the
    operator confirms the bin destination."""
    hold = db.query_one("""
        SELECT id, cohort, status, checkout_status FROM holds WHERE id = %s
    """, (hold_id,))
    if not hold:
        return jsonify({"error": "Hold not found"}), 404
    if hold.get("cohort") != "champion":
        return jsonify({"error": "Not a Champion hold"}), 400
    if hold.get("status") in ("ACCEPTED", "RETURNED", "CANCELLED", "AUTO_EXPIRED"):
        return jsonify({"error": f"Hold already closed (status={hold.get('status')})"}), 409

    # Refuse to close if any raw card is still SOLD on this hold — that means
    # the operator hasn't restocked it yet and would lose track.
    unresolved = db.query_one("""
        SELECT COUNT(*) AS n
          FROM hold_items hi
          JOIN raw_cards rc ON rc.id = hi.raw_card_id
         WHERE hi.hold_id = %s
           AND hi.item_kind = 'raw'
           AND rc.state = 'SOLD'
    """, (hold_id,))
    if unresolved and unresolved.get("n", 0) > 0:
        return jsonify({
            "error": f"{unresolved['n']} raw card(s) on this hold are still SOLD. "
                     "Restock each one first, then close."
        }), 409

    db.execute("""
        UPDATE holds SET status = 'CANCELLED', resolved_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (hold_id,))
    db.execute("""
        UPDATE hold_items SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP
        WHERE hold_id = %s
          AND status NOT IN ('ACCEPTED','REJECTED','SOLD','MISSING')
    """, (hold_id,))
    return jsonify({"success": True})


@app.route("/api/raw-card-pulls/<hold_id>/mark-shipped", methods=["POST"])
def api_raw_card_pulls_mark_shipped(hold_id):
    """Close a paid Champion hold after the physical pull + ship. Cards are
    already state='SOLD' (set by the kiosk webhook at payment), so this just
    flips the hold itself to ACCEPTED so it drops off the pull queue.
    Shopify-side fulfillment + tracking is done separately in Shopify admin."""
    hold = db.query_one("""
        SELECT id, cohort, status, checkout_status FROM holds WHERE id = %s
    """, (hold_id,))
    if not hold:
        return jsonify({"error": "Hold not found"}), 404
    if hold.get("cohort") != "champion":
        return jsonify({"error": "Not a Champion hold"}), 400
    if hold.get("checkout_status") != "completed":
        return jsonify({"error": f"Hold not paid (checkout_status={hold.get('checkout_status')})"}), 409
    if hold.get("status") in ("ACCEPTED","RETURNED","CANCELLED","AUTO_EXPIRED"):
        return jsonify({"error": f"Hold already closed (status={hold.get('status')})"}), 409

    db.execute("""
        UPDATE holds SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (hold_id,))
    db.execute("""
        UPDATE hold_items SET status = 'ACCEPTED', resolved_at = CURRENT_TIMESTAMP
        WHERE hold_id = %s
          AND status NOT IN ('ACCEPTED','REJECTED','SOLD','MISSING')
    """, (hold_id,))
    return jsonify({"success": True})


# ─────────────────────────────────────────────────────────────────────────
#  Per-item mutations on a paid Champion hold
#
#  After a Champion pays, raw_cards.state='SOLD' and the Shopify listing is
#  deleted. We still need three operator escape-hatches before "Mark Shipped":
#     • Mark Missing      — can't find the card; refund handled manually
#     • Change Condition  — we mis-graded our own inventory; data fix only
#     • Back to Storage   — won't ship this one; restore card to STORED
#
#  All three log to screening_log for the History tab.
# ─────────────────────────────────────────────────────────────────────────

_VALID_CONDITIONS = ("NM", "LP", "MP", "HP", "DMG")


def _load_pull_item(hold_item_id):
    """Resolve a hold_item from a paid Champion hold. Returns the row + hold,
    or (None, error_jsonify_response, status) on failure."""
    row = db.query_one("""
        SELECT hi.id AS hold_item_id, hi.hold_id, hi.raw_card_id,
               hi.status AS hi_status, hi.item_kind, hi.title AS hi_title,
               hi.barcode AS hi_barcode,
               h.cohort, h.checkout_status, h.status AS hold_status,
               h.shopify_order_number, h.customer_email,
               rc.state AS rc_state, rc.condition AS rc_condition,
               rc.card_name
          FROM hold_items hi
          JOIN holds h ON h.id = hi.hold_id
          LEFT JOIN raw_cards rc ON rc.id = hi.raw_card_id
         WHERE hi.id = %s
    """, (hold_item_id,))
    if not row:
        return None, (jsonify({"error": "Hold item not found"}), 404)
    if row.get("item_kind") != "raw" or not row.get("raw_card_id"):
        return None, (jsonify({"error": "Only raw card items can be mutated here"}), 400)
    if row.get("cohort") != "champion":
        return None, (jsonify({"error": "Not a Champion hold"}), 400)
    if row.get("checkout_status") != "completed":
        return None, (jsonify({"error": "Hold not paid"}), 409)
    if row.get("hi_status") in ("ACCEPTED", "REJECTED", "MISSING"):
        return None, (jsonify({"error": f"Hold item already resolved ({row.get('hi_status')})"}), 409)
    return row, None


def _log_pull_event(check_type, item_row, details):
    """Mirror screening_log writes for raw-card-pull mutations."""
    try:
        import json
        db.execute("""
            INSERT INTO screening_log (order_gid, order_name, customer_email, event_type, check_type, details)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            item_row.get("shopify_order_number") or item_row.get("hold_id"),
            item_row.get("shopify_order_number") or "",
            item_row.get("customer_email") or "",
            "raw_pull",
            check_type,
            json.dumps(details or {}),
        ))
    except Exception as e:
        print(f"[screening] pull log write failed: {e}", flush=True)


@app.route("/api/raw-card-pulls/item/<hold_item_id>/missing", methods=["POST"])
def api_raw_card_pull_missing(hold_item_id):
    """Flag a Champion-purchased raw card as MISSING because we can't find it
    in the bin. The card stays SOLD on the Shopify side (refund is manual);
    raw_cards flips to MISSING and the hold_item is closed out so the puller
    can keep moving."""
    row, err = _load_pull_item(hold_item_id)
    if err:
        return err
    db.execute("""
        UPDATE raw_cards
           SET state = 'MISSING',
               current_hold_id = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (row["raw_card_id"],))
    db.execute("""
        UPDATE hold_items
           SET status = 'MISSING', resolved_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (hold_item_id,))
    _log_pull_event("raw_pull_missing", row, {
        "hold_item_id": hold_item_id,
        "raw_card_id": str(row["raw_card_id"]),
        "barcode": row.get("hi_barcode"),
        "card_name": row.get("card_name") or row.get("hi_title"),
        "prior_condition": row.get("rc_condition"),
    })
    return jsonify({"success": True})


@app.route("/api/raw-card-pulls/item/<hold_item_id>/condition", methods=["POST"])
def api_raw_card_pull_condition(hold_item_id):
    """Correct the condition on a Champion-purchased raw card when our grading
    was wrong. Pure data fix — the card still ships, the customer still gets
    it; this just makes our records honest. Refund-or-not is the operator's
    call in Shopify Admin."""
    data = request.get_json(silent=True) or {}
    new_condition = (data.get("condition") or "").strip().upper()
    if new_condition not in _VALID_CONDITIONS:
        return jsonify({"error": f"Condition must be one of {_VALID_CONDITIONS}"}), 400

    row, err = _load_pull_item(hold_item_id)
    if err:
        return err

    prior = row.get("rc_condition")
    if prior == new_condition:
        return jsonify({"success": True, "noop": True})

    db.execute("""
        UPDATE raw_cards
           SET condition = %s,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (new_condition, row["raw_card_id"]))
    _log_pull_event("raw_pull_condition", row, {
        "hold_item_id": hold_item_id,
        "raw_card_id": str(row["raw_card_id"]),
        "barcode": row.get("hi_barcode"),
        "card_name": row.get("card_name") or row.get("hi_title"),
        "from": prior,
        "to": new_condition,
    })
    return jsonify({"success": True, "condition": new_condition})


@app.route("/api/raw-card-pulls/item/<hold_item_id>/restock", methods=["POST"])
def api_raw_card_pull_restock(hold_item_id):
    """Send a Champion-purchased raw card back to inventory (refund path).

    Two paths:
      • Has bin_id (the common case — bin survives the SOLD transition):
        card stays physically where it is. State flips to match the bin's
        location_type (DISPLAY for binder/display_case, STORED for bin).
      • No bin_id (rare; should not happen for Champion-eligible cards but
        we handle it anyway): auto-assign a storage bin via shared/storage
        the same way the Return Queue does, and tell the operator which
        bin to walk the card to.

    Shopify listing was deleted at sale, so we clear shopify_product_id/
    variant_id either way. Champion listings are created on the fly at
    checkout, so the card just becomes browsable on the kiosk again as
    soon as state flips back. Operator refunds in Shopify Admin manually.
    """
    row, err = _load_pull_item(hold_item_id)
    if err:
        return err

    rc = db.query_one("""
        SELECT rc.id, rc.bin_id, COALESCE(rc.game, 'pokemon') AS game,
               sl.bin_label,
               COALESCE(sr.location_type, 'bin') AS location_type
          FROM raw_cards rc
          LEFT JOIN storage_locations sl ON sl.id = rc.bin_id
          LEFT JOIN storage_rows sr ON sr.id = sl.row_id
         WHERE rc.id = %s
    """, (row["raw_card_id"],))

    auto_assigned = False
    if rc and rc.get("bin_id"):
        location_type = rc["location_type"]
        new_state = "DISPLAY" if location_type in ("binder", "display_case") else "STORED"
        bin_label = rc.get("bin_label")
        db.execute("""
            UPDATE raw_cards
               SET state = %s,
                   current_hold_id = NULL,
                   shopify_product_id = NULL,
                   shopify_variant_id = NULL,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (new_state, row["raw_card_id"]))
    else:
        # Edge case: no bin recorded. Auto-assign one via the same path
        # card_manager's Return Queue uses, so the operator gets a real
        # destination instead of an error.
        try:
            from storage import assign_bins
            assignments = assign_bins((rc or {}).get("game", "pokemon"), 1, db)
        except Exception as e:
            return jsonify({"error": f"No bin available to restock into: {e}"}), 409
        if not assignments:
            return jsonify({"error": "No bin available to restock into."}), 409
        a = assignments[0]
        new_state = "STORED"
        location_type = "bin"
        bin_label = a["bin_label"]
        auto_assigned = True
        db.execute("""
            UPDATE raw_cards
               SET state = 'STORED',
                   bin_id = %s,
                   current_hold_id = NULL,
                   shopify_product_id = NULL,
                   shopify_variant_id = NULL,
                   updated_at = CURRENT_TIMESTAMP
             WHERE id = %s
        """, (a["bin_id"], row["raw_card_id"]))

    db.execute("""
        UPDATE hold_items
           SET status = 'REJECTED', resolved_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (hold_item_id,))
    _log_pull_event("raw_pull_restock", row, {
        "hold_item_id": hold_item_id,
        "raw_card_id": str(row["raw_card_id"]),
        "barcode": row.get("hi_barcode"),
        "card_name": row.get("card_name") or row.get("hi_title"),
        "prior_condition": row.get("rc_condition"),
        "restored_state": new_state,
        "bin_label": bin_label,
        "location_type": location_type,
        "auto_assigned": auto_assigned,
    })
    return jsonify({
        "success": True,
        "state": new_state,
        "bin_label": bin_label,
        "location_type": location_type,
        "auto_assigned": auto_assigned,
    })


@app.route("/api/release-hold", methods=["POST"])
def api_release_hold():
    """Release a held order: remove tags, release fulfillment holds."""
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400

    from service import on_order_fulfilled
    result = on_order_fulfilled(order_gid)
    return jsonify({"ok": True, **result})


@app.route("/api/release-verification-group", methods=["POST"])
def api_release_verification_group():
    """Release verification holds from a cumulative group. Preserves combine state."""
    data = request.get_json(silent=True) or {}
    order_ids = data.get("order_ids")
    if not order_ids or not isinstance(order_ids, list):
        return jsonify({"error": "order_ids (list) required"}), 400

    from service import release_verification_group
    result = release_verification_group(order_ids)
    return jsonify({"ok": True, **result})


@app.route("/api/release-and-fulfill", methods=["POST"])
def api_release_and_fulfill():
    """Release holds and create fulfillment with tracking for an order."""
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    tracking = (data.get("tracking_number") or "").strip()
    company = (data.get("tracking_company") or "USPS").strip()
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400
    if not tracking:
        return jsonify({"error": "tracking_number required"}), 400

    from service import release_and_fulfill, on_order_fulfilled
    result = release_and_fulfill(order_gid, tracking, company)
    if result.get("fulfilled"):
        # Clean up tags
        on_order_fulfilled(order_gid)
    return jsonify(result)


def _uncombine_single_order(order_gid, shopify_gql, release_holds_fn, sig_threshold):
    """Release a single order from combine: remove tag, release hold, clean note, check signature."""
    result = {"order_gid": order_gid}

    # 1. Remove only hold-for-review tag (leaves other screening tags intact)
    try:
        shopify_gql("""
            mutation TagsRemove($id: ID!, $tags: [String!]!) {
              tagsRemove(id: $id, tags: $tags) {
                node { ... on Order { id tags } }
                userErrors { field message }
              }
            }
        """, {"id": order_gid, "tags": ["hold-for-review"]})
        result["tag_removed"] = "hold-for-review"
    except Exception as e:
        print(f"[screening] Failed to remove hold-for-review from {order_gid}: {e}", flush=True)

    # 2. Release fulfillment hold
    try:
        result["holds_released"] = release_holds_fn(order_gid)
    except Exception as e:
        print(f"[screening] Failed to release holds for {order_gid}: {e}", flush=True)

    # 3. Strip combine + signature notes from the order note
    try:
        note_data = shopify_gql("""
            query($id: ID!) { order(id: $id) { id note currentTotalPriceSet { shopMoney { amount } } } }
        """, {"id": order_gid})
        order_node = note_data.get("data", {}).get("order", {})
        existing_note = (order_node.get("note") or "")
        order_total = float(order_node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))

        cleaned_lines = [l for l in existing_note.split("\n")
                         if "combine order" not in l.lower()
                         and "combine shipping" not in l.lower()]
        # If this order is below the signature threshold on its own,
        # the combined-signature note no longer applies — strip it
        if order_total < sig_threshold:
            cleaned_lines = [l for l in cleaned_lines
                             if "signature required" not in l.lower()]
            result["signature_removed"] = True
        cleaned_note = "\n".join(cleaned_lines).strip()
        while "\n---\n\n---\n" in cleaned_note:
            cleaned_note = cleaned_note.replace("\n---\n\n---\n", "\n---\n")
        cleaned_note = cleaned_note.strip().strip("-").strip()
        shopify_gql("""
            mutation OrderUpdate($input: OrderInput!) {
              orderUpdate(input: $input) { order { id note } userErrors { field message } }
            }
        """, {"input": {"id": order_gid, "note": cleaned_note}})
    except Exception as e:
        print(f"[screening] Failed to clean note from {order_gid}: {e}", flush=True)

    return result


def _clean_combine_references(order_gid, removed_names, shopify_gql, strip_signature=False):
    """Remove references to uncombined order names from a sibling's combine notes.

    Notes look like: '📦 Combine Order (#1234, #1235)'
    After removing #1234: '📦 Combine Order (#1235)'
    If all names removed from a line, the whole line is dropped.
    If strip_signature=True, also removes 'Signature Required' notes (combined
    total dropped below threshold).
    """
    import re
    note_data = shopify_gql("""
        query($id: ID!) { order(id: $id) { id note } }
    """, {"id": order_gid})
    existing_note = (note_data.get("data", {}).get("order", {}).get("note") or "")
    if not existing_note:
        return

    updated_lines = []
    changed = False
    for line in existing_note.split("\n"):
        if "combine order" in line.lower():
            new_line = line
            for name in removed_names:
                new_line = re.sub(r",?\s*" + re.escape(name) + r"\s*,?", "", new_line)
            new_line = re.sub(r"\(\s*,\s*", "(", new_line)
            new_line = re.sub(r",\s*\)", ")", new_line)
            new_line = new_line.strip()
            if re.search(r"\(\s*\)", new_line):
                changed = True
                continue
            if new_line != line:
                changed = True
            updated_lines.append(new_line)
        elif strip_signature and "signature required" in line.lower():
            changed = True
            continue
        else:
            updated_lines.append(line)

    if changed:
        cleaned_note = "\n".join(updated_lines).strip()
        while "\n---\n\n---\n" in cleaned_note:
            cleaned_note = cleaned_note.replace("\n---\n\n---\n", "\n---\n")
        cleaned_note = cleaned_note.strip().strip("-").strip()
        shopify_gql("""
            mutation OrderUpdate($input: OrderInput!) {
              orderUpdate(input: $input) { order { id note } userErrors { field message } }
            }
        """, {"input": {"id": order_gid, "note": cleaned_note}})


@app.route("/api/uncombine-order", methods=["POST"])
def api_uncombine_order():
    """Remove a single order from a combine group.

    Only removes hold-for-review tag and releases the fulfillment hold.
    Does NOT touch other screening tags (tier verification, fraud, etc.) or Klaviyo.

    If this leaves only 1 order in the group, that order is also auto-uncombined.
    Re-evaluates signature required for each released order based on its individual total.
    """
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    order_name = data.get("order_name", "")
    # All orders in this combine group: [{id, name}, ...]
    group_orders = data.get("group_orders", [])
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400

    from service import _release_fulfillment_holds, _log_screening, SIGNATURE_THRESHOLD
    from shopify_graphql import shopify_gql

    # Figure out which orders to uncombine vs which stay combined
    orders_to_release = [order_gid]
    remaining = [o for o in group_orders if o["id"] != order_gid]
    # If only 1 order would remain, uncombine it too — don't leave a solo combine group
    if len(remaining) == 1:
        orders_to_release.append(remaining[0]["id"])
        remaining = []

    # 1. Release the uncombined orders
    released = []
    for oid in orders_to_release:
        r = _uncombine_single_order(oid, shopify_gql, _release_fulfillment_holds, SIGNATURE_THRESHOLD)
        released.append(r)
        # Log uncombine for each released order
        oname = next((o.get("name", "") for o in group_orders if o["id"] == oid), order_name)
        _log_screening(oid, oname, "", "uncombine", "combine", {
            "group_size": len(group_orders), "remaining": len(remaining),
        })

    # 2. Update remaining siblings' notes to remove references to uncombined orders
    released_names = {order_name}
    for o in group_orders:
        if o["id"] in orders_to_release:
            released_names.add(o.get("name", ""))
    released_names.discard("")

    if remaining and released_names:
        # Check if the remaining combined total drops below signature threshold
        remaining_combined_total = sum(o.get("total", 0) for o in remaining)
        strip_sig = remaining_combined_total < SIGNATURE_THRESHOLD

        for sib in remaining:
            try:
                _clean_combine_references(sib["id"], released_names, shopify_gql,
                                          strip_signature=strip_sig)
            except Exception as e:
                print(f"[screening] Failed to update sibling note {sib['id']}: {e}", flush=True)

    return jsonify({"ok": True, "released": released})


@app.route("/api/cancel-order", methods=["POST"])
def api_cancel_order():
    """Cancel + full refund a held order, then clean up tags."""
    data = request.get_json(silent=True) or {}
    order_gid = data.get("order_id")
    if not order_gid:
        return jsonify({"error": "order_id required"}), 400

    from shopify_graphql import shopify_gql
    from service import _log_screening

    # Log cancel events before we clear everything
    _otags = set()
    _oemail = ""
    try:
        odata = shopify_gql("query($id:ID!){order(id:$id){name tags note customer{email}}}", {"id": order_gid})
        o = odata.get("data", {}).get("order", {})
        _oname = o.get("name", "?")
        _oemail = ((o.get("customer") or {}).get("email") or "").strip()
        _otags = set(o.get("tags") or [])
        _TAG_MAP = {
            "high-value-tier1": "tier1", "high-value-tier2": "tier2",
            "spend-spike-review": "spend_spike", "fraud-medium": "fraud_medium",
            "FIRSTTIME5-review": "firsttime5", "customer-hold": "customer_hold",
        }
        _logged = False
        for _tag, _check in _TAG_MAP.items():
            if _tag in _otags:
                _log_screening(order_gid, _oname, _oemail, "cancel", _check)
                _logged = True
        if not _logged and "combine" in (o.get("note") or "").lower():
            _log_screening(order_gid, _oname, _oemail, "cancel", "combine")
    except Exception:
        pass  # don't block cancel if logging fails

    # Cancel with full refund
    try:
        shopify_gql("""
            mutation OrderCancel($orderId:ID!, $reason:OrderCancelReason!, $refund:Boolean!, $restock:Boolean!, $notifyCustomer:Boolean, $staffNote:String) {
              orderCancel(orderId:$orderId, reason:$reason, refund:$refund, restock:$restock, notifyCustomer:$notifyCustomer, staffNote:$staffNote) {
                orderCancelUserErrors { field message code }
              }
            }
        """, {
            "orderId": order_gid,
            "reason": "OTHER",
            "refund": True,
            "restock": True,
            "notifyCustomer": True,
            "staffNote": "Cancelled from screening console",
        })
    except Exception as e:
        return jsonify({"error": f"Cancel failed: {e}"}), 500

    # Handle FIRSTTIME5 abuse confirmation before tag cleanup strips the tag
    if "FIRSTTIME5-review" in _otags and _oemail:
        try:
            from service import on_order_cancelled
            on_order_cancelled(order_gid)
        except Exception as e:
            print(f"[screening] FIRSTTIME5 abuse confirmation failed: {e}", flush=True)

    # Clean up tags
    from service import on_order_fulfilled
    try:
        on_order_fulfilled(order_gid)
    except Exception:
        pass

    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Freshdesk integration — tickets, conversations, canned responses
# ---------------------------------------------------------------------------

def _fd_attachment(a):
    """Normalize a Freshdesk attachment dict for the UI. Keeps id + content_type
    so the frontend can rewrite non-image inline <img data-id="..."> tags
    (e.g. PDFs the customer attached) into clickable links instead of broken images."""
    return {
        "id": a.get("id"),
        "name": a.get("name", ""),
        "url": a.get("attachment_url") or a.get("url", ""),
        "content_type": a.get("content_type", ""),
        "inline": bool(a.get("inline", False)),
    }


@app.route("/api/freshdesk-tickets")
def api_freshdesk_tickets():
    """Fetch Freshdesk tickets + conversations for a customer email."""
    import freshdesk as fd
    if not fd.is_configured():
        return jsonify({"configured": False, "tickets": []})

    email = (request.args.get("email") or "").strip().lower()
    if not email:
        return jsonify({"configured": True, "tickets": []})

    try:
        raw_tickets = fd.search_tickets_by_email(email)
    except Exception as e:
        logging.warning("Freshdesk ticket search failed for %s: %s", email, e)
        return jsonify({"configured": True, "tickets": [], "error": str(e)})

    tickets = []
    for t in raw_tickets:
        tid = t.get("id")
        # Only include open/pending/resolved tickets (skip deleted/spam)
        status = t.get("status", 0)
        if status >= 5 and status != 4:  # 5=closed is ok to skip, but include 4=resolved
            continue
        convos = []

        # Freshdesk's list-by-email endpoint omits the `attachments` field.
        # Re-fetch each ticket individually so customer-uploaded files (PDFs of
        # IDs, etc.) on email-source tickets actually surface in the console.
        try:
            t_full = fd.get_ticket(tid)
            ticket_attachments = t_full.get("attachments") or []
        except Exception as e:
            logging.warning("Freshdesk single-ticket fetch failed for %s: %s", tid, e)
            ticket_attachments = t.get("attachments") or []

        # The ticket's own description is the FIRST message — for email-created tickets
        # (source=1), that's the customer's inbound text. Without this, customer replies
        # that arrive as new tickets (because the verification email was sent via Shopify
        # Flow, not Freshdesk, so there's no thread to attach to) look empty in the UI.
        desc_text = t.get("description_text") or ""
        desc_html = t.get("description") or ""
        if desc_text or desc_html:
            convos.append({
                "id": f"desc-{tid}",
                "body_text": desc_text,
                "body": desc_html,
                "from_email": (t.get("requester", {}) or {}).get("email", "") if isinstance(t.get("requester"), dict) else "",
                "incoming": t.get("source") == 1,  # email source → customer authored
                "created_at": t.get("created_at", ""),
                "attachments": [_fd_attachment(a) for a in ticket_attachments],
            })

        try:
            raw_convos = fd.get_ticket_conversations(tid)
        except Exception as e:
            logging.warning("Freshdesk conversations failed for ticket %s: %s", tid, e)
            raw_convos = []

        for c in raw_convos:
            convos.append({
                "id": c.get("id"),
                "body_text": c.get("body_text", ""),
                "body": c.get("body", ""),
                "from_email": c.get("from_email", ""),
                "incoming": c.get("incoming", False),
                "created_at": c.get("created_at", ""),
                "attachments": [_fd_attachment(a) for a in (c.get("attachments") or [])],
            })

        tickets.append({
            "id": tid,
            "subject": t.get("subject", ""),
            "status": status,
            "status_label": {2: "Open", 3: "Pending", 4: "Resolved", 5: "Closed"}.get(status, f"Status {status}"),
            "created_at": t.get("created_at", ""),
            "updated_at": t.get("updated_at", ""),
            "conversations": convos,
            "has_customer_reply": any(c["incoming"] for c in convos),
        })

    # Sort: tickets with customer replies first, newest-first within each group.
    # The list API already returns newest-first; stable sort preserves that order.
    tickets.sort(key=lambda t: not t["has_customer_reply"])

    return jsonify({"configured": True, "tickets": tickets})


@app.route("/api/freshdesk-canned-responses")
def api_freshdesk_canned_responses():
    """Fetch canned response folders + responses for the reply dropdown."""
    import freshdesk as fd
    if not fd.is_configured():
        return jsonify({"configured": False, "folders": []})

    try:
        folders = fd.list_canned_response_folders()
    except Exception as e:
        logging.warning("Freshdesk canned response folders failed: %s", e)
        return jsonify({"configured": True, "folders": [], "error": str(e)})

    result = []
    for f in folders:
        fid = f.get("id")
        try:
            responses = fd.list_canned_responses_in_folder(fid)
        except Exception as e:
            logging.warning("Freshdesk canned responses failed for folder %s: %s", fid, e)
            responses = []

        result.append({
            "id": fid,
            "name": f.get("name", ""),
            "responses": [{"id": r.get("id"), "title": r.get("title", "")} for r in responses],
        })

    return jsonify({"configured": True, "folders": result})


@app.route("/api/freshdesk-debug")
def api_freshdesk_debug():
    """Inspect what Freshdesk actually returns for a given email or ticket.
    Usage:
      /api/freshdesk-debug?email=customer@example.com  → all tickets, summary
      /api/freshdesk-debug?ticket_id=2252              → one ticket, full detail

    Returns a focused summary (just attachment-related fields) so the result
    fits in a paste, plus a list of all keys present so we can spot fields we
    aren't reading (e.g. cloud_files, support_attachments, etc.)."""
    import freshdesk as fd
    if not fd.is_configured():
        return jsonify({"error": "Freshdesk not configured"}), 400

    def summarize(t, *, full=False):
        out = {
            "id": t.get("id"),
            "subject": t.get("subject"),
            "status": t.get("status"),
            "source": t.get("source"),
            "all_keys": sorted(t.keys()),
            "attachments": t.get("attachments") or [],
            "cloud_files": t.get("cloud_files") or [],
        }
        if full:
            out["description_html_excerpt"] = (t.get("description") or "")[:500]
        return out

    ticket_id = request.args.get("ticket_id")
    if ticket_id:
        try:
            t_list = fd._request("GET", f"/tickets/{ticket_id}")
            t_full = fd.get_ticket(int(ticket_id))
            convos = fd.get_ticket_conversations(int(ticket_id))
            return jsonify({
                "ticket_id": ticket_id,
                "list_endpoint_keys": sorted(t_list.keys()),
                "list_endpoint_attachments": t_list.get("attachments") or [],
                "list_endpoint_cloud_files": t_list.get("cloud_files") or [],
                "single_ticket_with_include": summarize(t_full, full=True),
                "conversations_count": len(convos),
                "conversations": [
                    {
                        "id": c.get("id"),
                        "incoming": c.get("incoming"),
                        "all_keys": sorted(c.keys()),
                        "attachments": c.get("attachments") or [],
                        "cloud_files": c.get("cloud_files") or [],
                    }
                    for c in convos
                ],
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    email = (request.args.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "email or ticket_id param required"}), 400
    try:
        tickets = fd.search_tickets_by_email(email)
        summaries = []
        for t in tickets:
            tid = t.get("id")
            s = summarize(t)
            try:
                t_full = fd.get_ticket(tid)
                s["single_ticket_attachments"] = t_full.get("attachments") or []
                s["single_ticket_cloud_files"] = t_full.get("cloud_files") or []
                s["single_ticket_keys"] = sorted(t_full.keys())
            except Exception as e:
                s["single_ticket_error"] = str(e)
            summaries.append(s)
        return jsonify({"email": email, "tickets": summaries})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/freshdesk-canned-response/<int:response_id>")
def api_freshdesk_canned_response(response_id):
    """Fetch a single canned response body for preview."""
    import freshdesk as fd
    if not fd.is_configured():
        return jsonify({"error": "Freshdesk not configured"}), 400
    try:
        canned = fd.get_canned_response(response_id)
        # Freshdesk returns `content_html` (rich) and `content` (plain text).
        # The reply endpoint expects HTML, and so does the in-modal preview.
        return jsonify({
            "id": canned.get("id"),
            "title": canned.get("title", ""),
            "content": canned.get("content_html") or canned.get("content") or canned.get("body", ""),
        })
    except Exception as e:
        logging.warning("Freshdesk canned response %s fetch failed: %s", response_id, e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/freshdesk-reply", methods=["POST"])
def api_freshdesk_reply():
    """Send a canned response reply on a Freshdesk ticket and resolve it."""
    import freshdesk as fd
    if not fd.is_configured():
        return jsonify({"error": "Freshdesk not configured"}), 400

    data = request.get_json(silent=True) or {}
    ticket_id = data.get("ticket_id")
    canned_response_id = data.get("canned_response_id")

    if not ticket_id or not canned_response_id:
        return jsonify({"error": "ticket_id and canned_response_id required"}), 400

    try:
        canned = fd.get_canned_response(canned_response_id)
        body_html = canned.get("content_html") or canned.get("content") or canned.get("body", "")
        if not body_html:
            return jsonify({"error": "Canned response has no content"}), 400
        fd.reply_and_resolve(ticket_id, body_html)
        return jsonify({"ok": True})
    except Exception as e:
        logging.error("Freshdesk reply failed: %s", e)
        return jsonify({"error": f"Freshdesk reply failed: {e}"}), 500


@app.route("/api/customer-search")
def api_customer_search():
    """Search Shopify customers by name or email."""
    q = (request.args.get("q") or "").strip()
    if not q or len(q) < 2:
        return jsonify([])

    from shopify_graphql import shopify_gql
    data = shopify_gql("""
        query($q: String!) {
          customers(first: 10, query: $q) {
            edges {
              node {
                id
                firstName
                lastName
                email
                phone
                numberOfOrders
                defaultAddress {
                  address1
                  city
                  province
                  zip
                }
              }
            }
          }
        }
    """, {"q": q})

    results = []
    for edge in data.get("data", {}).get("customers", {}).get("edges", []):
        c = edge["node"]
        addr = c.get("defaultAddress") or {}
        results.append({
            "name": f"{c.get('firstName', '')} {c.get('lastName', '')}".strip(),
            "email": c.get("email", ""),
            "phone": c.get("phone", ""),
            "orders": c.get("numberOfOrders", 0),
            "address": f"{addr.get('address1', '')}, {addr.get('city', '')} {addr.get('province', '')} {addr.get('zip', '')}".strip(", ") if addr.get("address1") else "",
        })
    return jsonify(results)


@app.route("/api/customer-notes")
def api_customer_notes():
    """List active customer notes, optional ?q= search."""
    q = (request.args.get("q") or "").strip().lower()
    if q:
        rows = db.query(
            "SELECT * FROM customer_notes WHERE active = true AND (LOWER(customer_email) LIKE %s OR LOWER(customer_name) LIKE %s) ORDER BY created_at DESC",
            (f"%{q}%", f"%{q}%"),
        )
    else:
        rows = db.query("SELECT * FROM customer_notes WHERE active = true ORDER BY created_at DESC")
    for r in rows:
        r["created_at"] = r["created_at"].isoformat() if r.get("created_at") else None
        r["updated_at"] = r["updated_at"].isoformat() if r.get("updated_at") else None
    return jsonify(rows)


@app.route("/api/customer-notes", methods=["POST"])
def api_create_customer_note():
    """Create a customer note."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    name = (data.get("name") or "").strip()
    note_type = data.get("type", "note")
    text = (data.get("text") or "").strip()

    if not email or not text:
        return jsonify({"error": "email and text required"}), 400
    if note_type not in ("note", "hold"):
        return jsonify({"error": "type must be 'note' or 'hold'"}), 400

    row = db.execute_returning(
        "INSERT INTO customer_notes (customer_email, customer_name, note_type, note_text) VALUES (%s, %s, %s, %s) RETURNING *",
        (email, name or None, note_type, text),
    )
    if row:
        row["created_at"] = row["created_at"].isoformat() if row.get("created_at") else None
        row["updated_at"] = row["updated_at"].isoformat() if row.get("updated_at") else None
    return jsonify(row), 201


@app.route("/api/customer-notes/<int:note_id>", methods=["DELETE"])
def api_delete_customer_note(note_id):
    """Soft-delete a customer note."""
    affected = db.execute(
        "UPDATE customer_notes SET active = false, updated_at = NOW() WHERE id = %s AND active = true",
        (note_id,),
    )
    if not affected:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/egg-hunt")
def api_egg_hunt():
    """Egg hunt monitor — log of eligibility checks + pool status."""
    rows = db.query("""
        SELECT order_name, customer_email, order_total,
               has_collection_box, eligible, ineligible_reason,
               tier, was_live, logged_at
        FROM easter_egg_log
        ORDER BY logged_at DESC
        LIMIT 100
    """)

    pool_status = db.query("""
        SELECT tier,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE claimed_by_order IS NULL) AS remaining
        FROM easter_egg_pool
        GROUP BY tier
        ORDER BY tier
    """)

    active = os.environ.get("EASTER_EGG_ACTIVE", "false").lower() == "true"

    return jsonify({
        "active": active,
        "log": [dict(r) for r in rows],
        "pool": [dict(r) for r in pool_status],
    })


@app.route("/api/egg-manual-assign", methods=["POST"])
def api_egg_manual_assign():
    """Manually assign an easter egg to an order by order name (e.g. #1234)."""
    data = request.get_json(silent=True) or {}
    order_name = (data.get("order_name") or "").strip()
    if not order_name:
        return jsonify({"error": "order_name required"}), 400

    # Look up the order in Shopify by name
    from shopify_graphql import shopify_gql
    from service import assign_easter_egg

    search = shopify_gql("""
        query($q: String!) {
          orders(first: 1, query: $q) {
            edges { node {
              id name
              currentTotalPriceSet { shopMoney { amount } }
              customer { id email }
            } }
          }
        }
    """, {"q": f"name:{order_name}"})

    edges = search.get("data", {}).get("orders", {}).get("edges", [])
    if not edges:
        return jsonify({"error": f"Order {order_name} not found"}), 404

    order = edges[0]["node"]
    customer = order.get("customer") or {}
    cust_gid = customer.get("id")
    email = (customer.get("email") or "").strip()
    if not cust_gid or not email:
        return jsonify({"error": "Order has no customer/email"}), 400

    order_gid = order["id"]
    o_total = float(order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))

    # Force live assignment — skip spend/collection checks
    egg = assign_easter_egg(
        order_gid=order_gid,
        order_name=order.get("name", order_name),
        order_total=o_total,
        customer_gid=cust_gid,
        email=email,
        force_live=True,
    )
    return jsonify(egg)


@app.route("/api/egg-test-order", methods=["POST"])
def api_egg_test_order():
    """Test an order against egg eligibility without claiming a slot."""
    data = request.get_json(silent=True) or {}
    order_name = (data.get("order_name") or "").strip()
    if not order_name:
        return jsonify({"error": "order_name required"}), 400

    from shopify_graphql import shopify_gql
    from service import EASTER_EGG_MIN_SPEND, _order_has_collection_box

    search = shopify_gql("""
        query($q: String!) {
          orders(first: 1, query: $q) {
            edges { node {
              id name
              currentTotalPriceSet { shopMoney { amount } }
              customer { id email }
            } }
          }
        }
    """, {"q": f"name:{order_name}"})

    edges = search.get("data", {}).get("orders", {}).get("edges", [])
    if not edges:
        return jsonify({"error": f"Order {order_name} not found"}), 404

    order = edges[0]["node"]
    customer = order.get("customer") or {}
    o_total = float(order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
    email = (customer.get("email") or "").strip()
    order_gid = order["id"]

    checks = {
        "order_name": order.get("name", order_name),
        "email": email,
        "order_total": o_total,
        "min_spend": EASTER_EGG_MIN_SPEND,
        "meets_min_spend": o_total >= EASTER_EGG_MIN_SPEND,
        "has_collection_box": _order_has_collection_box(order_gid),
        "has_customer": bool(customer.get("id") and email),
    }
    checks["would_qualify"] = all([
        checks["meets_min_spend"],
        checks["has_collection_box"],
        checks["has_customer"],
    ])

    # Check if customer already has an egg
    if email:
        row = db.query_one(
            "SELECT tier FROM easter_egg_pool WHERE claimed_by_email = %s",
            (email,)
        )
        if row:
            checks["already_assigned"] = row["tier"]
            checks["would_qualify"] = False

    return jsonify(checks)


CONSOLE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Screening · Pack Fresh</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🛡</text></svg>">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=DM+Mono:wght@400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/pf-static/pf_theme.css">
<script src="/pf-static/pf_ui.js"></script>
<style>
.header { padding:20px 24px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:16px; }
.header h1 { font-size:1.3rem; }
.main { max-width:1000px; margin:0 auto; padding:20px; }
.section-title { font-size:0.75rem; color:var(--dim); text-transform:uppercase; letter-spacing:0.1em; margin:24px 0 12px; display:flex; align-items:center; gap:8px; }
.section-title:first-child { margin-top:0; }
.count-badge { background:var(--red); color:#fff; border-radius:10px; padding:1px 8px; font-size:0.7rem; }
.order-header { display:flex; align-items:center; gap:12px; flex-wrap:wrap; margin-bottom:8px; }
.order-name { font-weight:700; font-size:1rem; }
.order-meta { font-size:0.8rem; color:var(--dim); line-height:1.5; }
.items-list { font-size:0.8rem; color:var(--dim); margin-top:6px; padding:8px 12px; background:var(--s2); border-radius:6px; }
.btn-green { background:var(--green); color:#000; }
.combine-group { background:var(--surface); border:2px solid var(--accent); border-radius:12px; padding:18px; margin-bottom:14px; }
.combine-header { font-weight:700; font-size:1rem; margin-bottom:4px; }
.combine-orders { display:flex; flex-direction:column; gap:8px; margin:10px 0; }
.combine-order { background:var(--s2); border-radius:8px; padding:10px 14px; }
.tab { background:none; border:none; padding:10px 18px; color:var(--dim); cursor:pointer; font-size:0.88rem; font-weight:500; border-bottom:2px solid transparent; font-family:inherit; }
.tab:hover { color:var(--text); }
.tab.active { color:var(--accent); border-bottom-color:var(--accent); }
.pane { display:none; }
.pane.active { display:block; }
.spinner { width:24px; height:24px; border:3px solid var(--border); border-top-color:var(--accent); border-radius:50%; animation:spin 0.7s linear infinite; margin:30px auto; }
@keyframes spin { to { transform:rotate(360deg); } }
.empty { color:var(--dim); text-align:center; padding:30px; }
/* Egg Hunt tab */
.egg-status-bar { display:flex; gap:12px; margin-bottom:16px; flex-wrap:wrap; }
.egg-status-pill { padding:6px 14px; border-radius:20px; font-size:0.8rem; font-weight:600; }
.egg-pill-live { background:rgba(0,200,80,0.15); color:var(--green); border:1px solid var(--green); }
.egg-pill-dry { background:rgba(255,170,0,0.12); color:var(--amber); border:1px solid var(--amber); }
.egg-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
.egg-table th { text-align:left; color:var(--dim); font-weight:500; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.06em; padding:6px 10px; border-bottom:1px solid var(--border); }
.egg-table td { padding:8px 10px; border-bottom:1px solid rgba(255,255,255,0.04); vertical-align:middle; }
.egg-table tr:hover td { background:var(--s2); }
.egg-tier { font-weight:700; font-size:0.8rem; }
.egg-tier-stink      { color:#7a8450; }
.egg-tier-bronze     { color:#cd7f32; }
.egg-tier-silver     { color:#aaa; }
.egg-tier-gold       { color:#f0b800; }
.egg-ineligible      { color:var(--dim); font-style:italic; }
.egg-simulated       { opacity:0.6; }
.egg-pool-grid { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; }
.egg-pool-card { background:var(--s2); border-radius:8px; padding:10px 16px; min-width:110px; }
.egg-pool-card .remaining { font-size:1.4rem; font-weight:700; }
.egg-pool-card .label { font-size:0.72rem; color:var(--dim); margin-top:2px; }
/* Freshdesk conversation UI */
.fd-section { margin-top:8px; border-top:1px solid var(--border); padding-top:8px; }
.fd-toggle { font-size:0.78rem; color:var(--accent); cursor:pointer; display:inline-flex; align-items:center; gap:4px; }
.fd-toggle:hover { text-decoration:underline; }
.fd-convos { margin-top:6px; display:none; }
.fd-convos.open { display:block; }
.fd-convo { padding:8px 12px; margin-bottom:6px; border-radius:6px; font-size:0.82rem; line-height:1.5; }
.fd-convo.incoming { background:rgba(0,180,100,0.08); border-left:3px solid var(--green); }
.fd-convo.outgoing { background:var(--s2); border-left:3px solid var(--dim); }
.fd-convo-meta { font-size:0.72rem; color:var(--dim); margin-bottom:4px; }
.fd-convo-body { word-break:break-word; max-height:280px; overflow-y:auto; background:rgba(0,0,0,0.15); border-radius:4px; padding:8px 10px; }
/* Force readable text — emails ship with inline color:#000 / background:#fff that breaks dark theme */
.fd-convo-body, .fd-convo-body *:not(a) { color:var(--text) !important; background-color:transparent !important; }
.fd-convo-body a, .fd-convo-body a * { color:var(--accent) !important; }
.fd-convo-body img { max-width:160px; max-height:160px; border-radius:4px; border:1px solid var(--border); object-fit:contain; cursor:zoom-in; vertical-align:middle; margin:4px; display:inline-block; }
.fd-convo-body img[width="1"], .fd-convo-body img[height="1"] { display:none; } /* hide tracking pixels */
.fd-convo-body-text { white-space:pre-wrap; }
.fd-attach { font-size:0.72rem; margin-top:4px; }
.fd-attach a { color:var(--accent); }
.fd-badge { font-size:0.7rem; padding:1px 7px; border-radius:10px; font-weight:500; }
.fd-badge.reply { background:rgba(0,180,100,0.15); color:var(--green); }
.fd-badge.waiting { background:rgba(255,170,0,0.12); color:var(--amber); }
.fd-badge.none { background:var(--s2); color:var(--dim); }
/* Freshdesk reply modal */
.fd-modal-overlay { position:fixed; inset:0; background:rgba(0,0,0,0.55); z-index:100; display:flex; align-items:center; justify-content:center; }
.fd-modal { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px 24px; width:90%; max-width:520px; max-height:80vh; overflow-y:auto; }
.fd-modal h3 { margin:0 0 14px; font-size:1rem; }
.fd-modal select { width:100%; padding:8px 10px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); font-size:0.85rem; margin-bottom:12px; }
.fd-modal-preview { background:var(--s2); border-radius:6px; padding:12px; font-size:0.82rem; max-height:200px; overflow-y:auto; margin-bottom:14px; color:var(--dim); }
.fd-modal-actions { display:flex; gap:8px; justify-content:flex-end; }
/* History tab */
.history-filters { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:16px; align-items:flex-end; }
.history-filters label { font-size:0.72rem; color:var(--dim); display:block; margin-bottom:2px; }
.history-filters select, .history-filters input { padding:6px 10px; background:var(--s2); border:1px solid var(--border); border-radius:6px; color:var(--text); font-size:0.82rem; font-family:inherit; }
.history-filters input[type="date"] { width:130px; }
.history-filters input[type="text"] { width:160px; }
.history-table { width:100%; border-collapse:collapse; font-size:0.82rem; }
.history-table th { text-align:left; color:var(--dim); font-weight:500; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.06em; padding:6px 10px; border-bottom:1px solid var(--border); }
.history-table td { padding:8px 10px; border-bottom:1px solid rgba(255,255,255,0.04); vertical-align:middle; }
.history-table tr:hover td { background:var(--s2); }
.history-pagination { display:flex; gap:8px; align-items:center; justify-content:center; margin-top:16px; font-size:0.82rem; }
.history-pagination button { padding:4px 12px; }
.history-event { font-weight:600; font-size:0.78rem; padding:2px 8px; border-radius:10px; }
.history-event-hold { background:rgba(255,170,0,0.12); color:var(--amber); }
.history-event-release { background:rgba(0,200,80,0.12); color:var(--green); }
.history-event-cancel { background:rgba(255,60,60,0.12); color:var(--red); }
.history-event-auto_cancel { background:rgba(255,60,60,0.2); color:var(--red); }
.history-event-uncombine { background:rgba(130,130,255,0.12); color:#99f; }
.history-event-upgrade { background:rgba(0,180,255,0.12); color:#5cf; }
.history-details { font-size:0.75rem; color:var(--dim); max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
</style>
</head>
<body>
<div class="header">
  <h1>🛡️ Screening Console</h1>
  <button class="btn btn-secondary btn-sm" onclick="loadOrders()" style="margin-left:auto;">↻ Refresh</button>
</div>

<div class="main">
  <div id="screening-tabs" style="display:flex;gap:2px;margin-bottom:20px;border-bottom:1px solid var(--border);">
    <button class="tab active" id="tab-verify" onclick="switchTab('verify')">🔍 Verification Queue</button>
    <button class="tab" id="tab-combine" onclick="switchTab('combine')">📦 Shipping</button>
    <button class="tab" id="tab-pulls" onclick="switchTab('pulls')">🃏 Raw Card Pulls</button>
    <button class="tab" id="tab-notes" onclick="switchTab('notes')">👤 Customer Notes</button>
    <button class="tab" id="tab-egg" onclick="switchTab('egg')">🥚 Egg Hunt</button>
    <button class="tab" id="tab-history" onclick="switchTab('history')">📊 History</button>
  </div>
  <div id="pane-verify" class="pane active"><div class="spinner"></div></div>
  <div id="pane-combine" class="pane"><div class="spinner"></div></div>
  <div id="pane-pulls" class="pane"><div class="spinner"></div></div>
  <div id="pane-notes" class="pane"></div>
  <div id="pane-egg" class="pane"></div>
  <div id="pane-history" class="pane"></div>
</div>
<script>
// Associates see Combine + Raw Card Pulls. window._pfUser is set by the
// shared admin bar (shared/auth.py::ADMIN_BAR_HTML).
(function () {
  const role = (window._pfUser || {}).role;
  if (role !== 'associate') return;
  ['tab-verify', 'tab-notes', 'tab-egg', 'tab-history'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = 'none';
  });
  // Default to combine pane since verify is hidden.
  document.getElementById('tab-verify').classList.remove('active');
  document.getElementById('pane-verify').classList.remove('active');
  document.getElementById('tab-combine').classList.add('active');
  document.getElementById('pane-combine').classList.add('active');
})();
</script>

<script>
let _data = null;
let _fdTickets = {};  // email → { tickets: [...], configured }
let _fdCanned = null; // { configured, folders: [...] }

let _eggPollTimer = null;

function switchTab(id) {
  document.querySelectorAll('.pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('pane-' + id).classList.add('active');
  document.getElementById('tab-' + id).classList.add('active');
  if (id === 'notes') loadNotes();
  else if (id === 'egg') { loadEggHunt(); startEggPoll(); }
  else if (id === 'history') { stopEggPoll(); loadHistory(); }
  else if (id === 'pulls') { stopEggPoll(); loadRawPulls(); }
  else { stopEggPoll(); if (_data) renderAll(); }
}

function startEggPoll() {
  stopEggPoll();
  _eggPollTimer = setInterval(loadEggHunt, 30000);
}

function stopEggPoll() {
  if (_eggPollTimer) { clearInterval(_eggPollTimer); _eggPollTimer = null; }
}

async function loadEggHunt() {
  try {
    const r = await fetch('/api/egg-hunt');
    const d = await r.json();
    renderEggHunt(d);
  } catch(e) {
    document.getElementById('pane-egg').innerHTML =
      `<div class="empty">Failed to load: ${e.message}</div>`;
  }
}

function renderEggHunt(d) {
  const TIER_EMOJI = { stink:'🤢', bronze:'🥉', silver:'🥈', gold:'🥇' };
  const TIER_LABEL = { stink:'Stink Egg', bronze:'Bronze Egg', silver:'Silver Egg', gold:'Golden Egg' };

  const poolHtml = (d.pool || []).map(p => {
    const pct = Math.round((p.remaining / p.total) * 100);
    const tier = p.tier;
    return `
      <div class="egg-pool-card">
        <div class="remaining egg-tier egg-tier-${tier}">
          ${TIER_EMOJI[tier] || '🥚'} ${p.remaining}<span style="font-size:0.75rem;font-weight:400;color:var(--dim)">/${p.total}</span>
        </div>
        <div class="label">${TIER_LABEL[tier] || tier} · ${pct}% left</div>
      </div>`;
  }).join('');

  const rowsHtml = (d.log || []).map(row => {
    const time = new Date(row.logged_at).toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
    const total = row.order_total ? '$' + parseFloat(row.order_total).toFixed(2) : '—';
    const hasBox = row.has_collection_box === null ? '—'
                 : row.has_collection_box ? '✅' : '❌';

    let tierHtml = '—';
    if (row.tier) {
      const cls = `egg-tier egg-tier-${row.tier}`;
      const label = (TIER_EMOJI[row.tier] || '') + ' ' + (TIER_LABEL[row.tier] || row.tier);
      const simTag = !row.was_live ? ' <span style="font-size:0.68rem;color:var(--dim)">(sim)</span>' : '';
      tierHtml = `<span class="${cls}">${label}</span>${simTag}`;
    }

    const eligibleHtml = row.eligible
      ? '<span style="color:var(--green)">✅ Eligible</span>'
      : `<span class="egg-ineligible">❌ ${(row.ineligible_reason || '').replace(/_/g,' ')}</span>`;

    const rowCls = row.was_live ? '' : 'egg-simulated';

    return `
      <tr class="${rowCls}">
        <td style="font-weight:600">${row.order_name}</td>
        <td style="color:var(--dim)">${row.customer_email || '—'}</td>
        <td>${total}</td>
        <td style="text-align:center">${hasBox}</td>
        <td>${eligibleHtml}</td>
        <td>${tierHtml}</td>
        <td style="color:var(--dim);font-size:0.75rem">${time}</td>
      </tr>`;
  }).join('');

  const modeHtml = d.active
    ? '<span class="egg-status-pill egg-pill-live">🟢 LIVE — Assigning eggs</span>'
    : '<span class="egg-status-pill egg-pill-dry">🟡 DRY RUN — Simulating only</span>';

  document.getElementById('pane-egg').innerHTML = `
    <div class="egg-status-bar">
      ${modeHtml}
      <span style="font-size:0.8rem;color:var(--dim);align-self:center;">
        Auto-refreshes every 30s
      </span>
      <div style="margin-left:auto;display:flex;gap:6px;align-items:center;">
        <input id="egg-manual-order" type="text" placeholder="#1234" style="width:100px;padding:5px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.82rem;">
        <button class="btn btn-secondary btn-sm" onclick="testEggOrder()">🔍 Test</button>
        <button class="btn btn-green btn-sm" onclick="manualAssignEgg()">🥚 Assign</button>
      </div>
    </div>
    <div class="egg-pool-grid">${poolHtml || '<div style="color:var(--dim)">Pool not seeded yet</div>'}</div>
    ${d.log && d.log.length ? `
    <table class="egg-table">
      <thead><tr>
        <th>Order</th><th>Email</th><th>Total</th>
        <th style="text-align:center">Box?</th>
        <th>Eligibility</th><th>Tier</th><th>Time</th>
      </tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table>` : '<div class="empty">No orders screened yet during this promo window.</div>'}
  `;
}

async function testEggOrder() {
  const input = document.getElementById('egg-manual-order');
  const name = (input.value || '').trim();
  if (!name) { alert('Enter an order number'); return; }
  try {
    const r = await fetch('/api/egg-test-order', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_name: name }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    const lines = [
      d.order_name + ' (' + (d.email || 'no email') + ')',
      'Total: $' + (d.order_total || 0).toFixed(2) + (d.meets_min_spend ? ' ✅' : ' ❌ below $' + d.min_spend),
      'Collection Box: ' + (d.has_collection_box ? '✅ yes' : '❌ no'),
      'Customer: ' + (d.has_customer ? '✅' : '❌ missing'),
    ];
    if (d.already_assigned) lines.push('Already assigned: ' + d.already_assigned + ' egg');
    lines.push('');
    lines.push(d.would_qualify ? '✅ WOULD QUALIFY' : '❌ Would NOT qualify');
    alert(lines.join(String.fromCharCode(10)));
  } catch(e) { alert(e.message); }
}

async function manualAssignEgg() {
  const input = document.getElementById('egg-manual-order');
  const name = (input.value || '').trim();
  if (!name) { alert('Enter an order number'); return; }
  if (!confirm('Manually assign an egg to order ' + name + '? This bypasses spend/collection checks.')) return;
  try {
    const r = await fetch('/api/egg-manual-assign', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_name: name }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    if (d.skipped) {
      toast(name + ': ' + (d.reason || 'skipped'), 'amber');
    } else {
      toast(name + ' → ' + (d.tier || '?') + ' egg assigned!', 'green');
    }
    input.value = '';
    loadEggHunt();
  } catch(e) { alert(e.message); }
}

async function loadOrders() {
  document.getElementById('pane-verify').innerHTML = '<div class="spinner"></div>';
  document.getElementById('pane-combine').innerHTML = '<div class="spinner"></div>';
  try {
    const r = await fetch('/api/held-orders');
    _data = await r.json();
    renderAll();
    // Load Freshdesk data in background (non-blocking)
    loadFreshdeskData();
  } catch(e) {
    document.getElementById('pane-verify').innerHTML = `<div class="empty">${e.message}</div>`;
  }
}

async function loadFreshdeskData() {
  // Fetch canned responses once
  if (!_fdCanned) {
    try {
      const r = await fetch('/api/freshdesk-canned-responses');
      _fdCanned = await r.json();
    } catch(e) { _fdCanned = { configured: false, folders: [] }; }
  }
  if (!_fdCanned.configured) return;

  // Fetch tickets for each unique email in verification queue (including groups)
  const groupEmails = (_data.verification_groups || []).map(g => g.customer_email).filter(Boolean);
  const emails = [...new Set([...(_data.verification || []).map(o => o.customer_email).filter(Boolean), ...groupEmails])];
  const fetches = emails.filter(e => !_fdTickets[e]).map(async (email) => {
    try {
      const r = await fetch('/api/freshdesk-tickets?email=' + encodeURIComponent(email));
      _fdTickets[email] = await r.json();
    } catch(e) { _fdTickets[email] = { configured: true, tickets: [] }; }
  });
  if (fetches.length) {
    await Promise.all(fetches);
    // Re-render verification cards with Freshdesk data
    renderVerification(_data.verification || [], _data.verification_groups || []);
  }
}

function renderAll() {
  renderVerification(_data.verification || [], _data.verification_groups || []);
  renderCombine(_data.combine_groups || []);
  const vCount = (_data.verification||[]).length + (_data.verification_groups||[]).reduce((s,g)=>s+g.orders.length, 0);
  document.getElementById('tab-verify').textContent = '🔍 Verification (' + vCount + ')';
  document.getElementById('tab-combine').textContent = '📦 Shipping (' + (_data.combine_groups||[]).length + ')';
  // Eagerly load pull count so the badge reflects reality without a tab click.
  if (_pullsData) {
    document.getElementById('tab-pulls').textContent = '🃏 Raw Card Pulls (' + (_pullsData.holds||[]).length + ')';
  } else {
    loadRawPullsCount();
  }
}

let _pullsData = null;

async function loadRawPullsCount() {
  // Badge only — skip the Shopify enrichment so we don't pay a Shopify
  // roundtrip on every page load.
  try {
    const r = await fetch('/api/raw-card-pulls?fast=1');
    _pullsData = await r.json();
    document.getElementById('tab-pulls').textContent =
      '🃏 Raw Card Pulls (' + (_pullsData.holds||[]).length + ')';
  } catch (e) { /* leave default label */ }
}

async function loadRawPulls() {
  document.getElementById('pane-pulls').innerHTML = '<div class="spinner"></div>';

  // Phase 1: render DB-only data immediately so the operator sees the queue.
  try {
    const fastR = await fetch('/api/raw-card-pulls?fast=1');
    _pullsData = await fastR.json();
    renderRawPulls(_pullsData.holds || []);
    document.getElementById('tab-pulls').textContent =
      '🃏 Raw Card Pulls (' + (_pullsData.holds||[]).length + ')';
  } catch (e) {
    document.getElementById('pane-pulls').innerHTML =
      '<div class="empty">Failed to load: ' + e.message + '</div>';
    return;
  }

  // Phase 2: enrich with Shopify (sealed items, cancelled status) in the
  // background. Slow ~1-3s; re-renders silently when done. If Shopify is
  // unreachable the DB-only render stays put — no regression vs before.
  try {
    const r = await fetch('/api/raw-card-pulls');
    const d = await r.json();
    _pullsData = d;
    renderRawPulls(_pullsData.holds || []);
  } catch (e) { /* leave the fast render in place */ }
}

function renderRawPulls(holds) {
  const el = document.getElementById('pane-pulls');
  if (!holds.length) { el.innerHTML = '<div class="empty">✅ No raw card pulls pending</div>'; return; }

  // Cross-reference: does this customer also have orders in Combine?
  const combineByEmail = {};
  (_data && _data.combine_groups || []).forEach(g => {
    const k = (g.customer_email || '').toLowerCase();
    if (k) combineByEmail[k] = g;
  });

  el.innerHTML = holds.map(h => {
    const ce = (h.customer_email || '').toLowerCase();
    const comboMatch = combineByEmail[ce];
    const comboBadge = comboMatch
      ? '<button class="btn btn-secondary btn-sm" style="font-size:0.7rem;padding:2px 8px;margin-left:8px;" onclick="switchTab(&apos;combine&apos;)">📦 ' + comboMatch.orders.length + ' combinable</button>'
      : '';
    const raws = h.items.filter(i => i.kind === 'raw');
    const sealed = h.items.filter(i => i.kind !== 'raw');
    const cancelled = !!h.cancelled;
    const cancelBanner = cancelled
      ? '<div style="margin:8px 0;padding:10px 12px;background:rgba(255,80,80,0.18);border:1px solid rgba(255,80,80,0.45);color:#f88;border-radius:6px;font-weight:700;">⚠ ORDER CANCELLED IN SHOPIFY ' + (h.cancelled_at ? '— ' + new Date(h.cancelled_at).toLocaleString() : '') + '<div style="font-weight:400;font-size:0.78rem;margin-top:4px;color:#fcc;">Do not ship. Restock each raw card below; the cards are still marked SOLD until you do.</div></div>'
      : '';
    const rawHtml = raws.map(i => {
      const resolved = (i.hi_status === 'MISSING' || i.hi_status === 'REJECTED' || i.hi_status === 'ACCEPTED');
      const resolvedBadge = resolved
        ? '<span style="margin-left:6px;padding:2px 6px;border-radius:4px;font-size:0.66rem;font-weight:700;background:'
          + (i.hi_status === 'MISSING' ? 'rgba(255,80,80,0.18);color:#f88'
             : i.hi_status === 'REJECTED' ? 'rgba(255,170,0,0.18);color:#fb6'
             : 'rgba(80,200,120,0.18);color:#8e8') + ';">' + _esc(i.hi_status) + '</span>'
        : '';
      const actions = resolved
        ? '<span style="font-size:0.72rem;color:var(--dim);">—</span>'
        : (
          '<button class="btn btn-secondary btn-sm" style="font-size:0.7rem;padding:2px 8px;" title="Wrong condition" '
          + 'onclick="pullChangeCondition(&apos;' + i.hold_item_id + '&apos;, &apos;' + _esc(i.condition || '') + '&apos;, this)">✎ Cond</button>'
          + ' <button class="btn btn-secondary btn-sm" style="font-size:0.7rem;padding:2px 8px;color:#fb6;" title="Send back to storage (refund)" '
          + 'onclick="pullRestock(&apos;' + i.hold_item_id + '&apos;, &apos;' + _esc(i.bin_label || '') + '&apos;, &apos;' + _esc(i.bin_type || '') + '&apos;, this)">↩ Restock</button>'
          + ' <button class="btn btn-secondary btn-sm" style="font-size:0.7rem;padding:2px 8px;color:#f88;" title="Mark missing (can&apos;t find)" '
          + 'onclick="pullMarkMissing(&apos;' + i.hold_item_id + '&apos;, this)">⚠ Missing</button>'
        );
      return `
      <div style="display:flex;align-items:center;gap:8px;padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.04);${resolved ? 'opacity:0.55;' : ''}" data-hold-item-id="${i.hold_item_id}">
        ${i.image_url ? `<img src="${i.image_url}" style="width:42px;height:42px;object-fit:cover;border-radius:4px;">` : '<div style="width:42px;height:42px;background:var(--s2);border-radius:4px;"></div>'}
        <div style="flex:1;font-size:0.85rem;">
          <strong>${_esc(i.title || '—')}</strong>${resolvedBadge}
          <div style="font-size:0.72rem;color:var(--dim);"><span class="pull-cond">${_esc(i.set_name || '')} ${i.card_number ? '#' + _esc(i.card_number) : ''} · ${_esc(i.condition || '')}${i.variant ? ' · ' + _esc(i.variant) : ''}</span></div>
        </div>
        <div style="font-family:monospace;font-size:0.78rem;color:var(--dim);">${_esc(i.barcode || '')}</div>
        <div style="min-width:90px;text-align:right;">
          ${i.bin_label
            ? '<span style="padding:3px 10px;background:rgba(0,180,255,0.12);color:#5cf;border-radius:10px;font-weight:600;font-size:0.78rem;">' + (i.bin_type === 'display' ? '📍 ' : (i.bin_type === 'binder' ? '📒 ' : '📦 ')) + _esc(i.bin_label) + '</span>'
            : '<span style="color:var(--red);font-size:0.78rem;">⚠ no bin</span>'}
        </div>
        <div style="display:flex;gap:4px;align-items:center;white-space:nowrap;">${actions}</div>
      </div>
    `;
    }).join('');
    const sealedHtml = sealed.length ? `
      <div style="margin-top:10px;padding-top:8px;border-top:1px dashed var(--border);">
        <div style="font-size:0.72rem;font-weight:600;color:var(--dim);margin-bottom:4px;">Other items in this order (sealed / storefront add-ons):</div>
        ${sealed.map(i => {
          const qty = (i.qty && i.qty > 1) ? ' ×' + i.qty : '';
          return '<div style="font-size:0.82rem;padding:3px 0;display:flex;align-items:center;gap:6px;">'
            + (i.image_url ? '<img src="' + _esc(i.image_url) + '" style="width:28px;height:28px;object-fit:cover;border-radius:3px;">' : '')
            + '<span>• ' + _esc(i.title || '—') + qty
            + (i.sku ? ' <span style="color:var(--dim);font-family:monospace;font-size:0.72rem;">[' + _esc(i.sku) + ']</span>' : '')
            + '</span></div>';
        }).join('')}
      </div>
    ` : '';

    const shipBtn = cancelled
      ? '<button class="btn btn-secondary btn-sm" style="color:#fb6;" onclick="closeCancelledHold(&apos;' + h.hold_id + '&apos;, this)">✕ Close Cancelled Hold</button>'
      : '<button class="btn btn-green btn-sm" onclick="markPullShipped(&apos;' + h.hold_id + '&apos;, this)">✓ Mark Pulled &amp; Shipped</button>';

    return `
      <div class="combine-group" data-hold-id="${h.hold_id}">
        <div class="combine-header" style="display:flex;align-items:center;">
          ${_esc(h.customer_name || h.shipping_name || '—')}
          ${h.order_number ? '<span style="margin-left:8px;font-size:0.78rem;color:var(--dim);font-weight:400;">' + _esc(h.order_number) + '</span>' : ''}
          ${comboBadge}
        </div>
        <div style="font-size:0.78rem;color:var(--dim);">${_esc(h.customer_email || '')} · ${_esc(h.shipping_address || '')}</div>
        ${cancelBanner}
        <div style="margin-top:10px;background:var(--s2);border-radius:6px;overflow:hidden;">
          ${rawHtml || '<div style="padding:8px;color:var(--dim);font-size:0.82rem;">No raw items</div>'}
        </div>
        ${sealedHtml}
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
          <button class="btn btn-secondary btn-sm" onclick="printPullSlip('${h.hold_id}')">🖨 Print Pull Slip</button>
          ${shipBtn}
        </div>
      </div>
    `;
  }).join('');
}

function printPullSlip(holdId) {
  const hold = ((_pullsData && _pullsData.holds) || []).find(h => h.hold_id === holdId);
  if (!hold) return;

  const NO_BIN = '__NO_BIN__';
  const raws = (hold.items || []).filter(i => i.kind === 'raw' && i.hi_status !== 'MISSING' && i.hi_status !== 'REJECTED');
  const sealed = (hold.items || []).filter(i => i.kind !== 'raw');

  // Bucket raws by bin so the puller walks one location at a time.
  const buckets = {};
  raws.forEach(i => {
    const key = i.bin_label || NO_BIN;
    if (!buckets[key]) buckets[key] = { bin_label: i.bin_label, bin_type: i.bin_type, items: [] };
    buckets[key].items.push(i);
  });
  // Sort: real bins alphabetically, then no-bin last.
  const bucketKeys = Object.keys(buckets).sort((a, b) => {
    if (a === NO_BIN) return 1;
    if (b === NO_BIN) return -1;
    return a.localeCompare(b, undefined, { numeric: true });
  });

  const binIcon = (t) => t === 'binder' ? '📒' : ((t === 'display_case' || t === 'display') ? '📍' : '📦');
  const binKind = (t) => t === 'binder' ? 'binder' : ((t === 'display_case' || t === 'display') ? 'display case' : 'storage bin');

  const routeSummary = bucketKeys.map((k, idx) => {
    const b = buckets[k];
    const label = b.bin_label ? (binIcon(b.bin_type) + ' ' + _esc(b.bin_label)) : '⚠ NO BIN';
    return '<span style="display:inline-block;padding:3px 10px;margin:2px;border:1px solid #888;border-radius:4px;font-weight:600;">' + (idx + 1) + '. ' + label + ' <span style="font-weight:400;color:#666;">(' + b.items.length + ')</span></span>';
  }).join(' ');

  const cancelledNote = hold.cancelled
    ? '<div style="margin:10px 0;padding:10px;background:#fee;border:2px solid #c00;color:#900;font-weight:700;border-radius:6px;">⚠ ORDER CANCELLED IN SHOPIFY — DO NOT SHIP</div>'
    : '';

  let body = '';
  bucketKeys.forEach((k, idx) => {
    const b = buckets[k];
    const header = b.bin_label
      ? '<h2 style="margin:18px 0 6px;padding:6px 10px;background:#eef6ff;border-left:4px solid #06c;font-size:1.05rem;">' + (idx + 1) + '. ' + binIcon(b.bin_type) + ' ' + _esc(b.bin_label) + ' <span style="font-weight:400;color:#666;font-size:0.85rem;">(' + binKind(b.bin_type) + ', ' + b.items.length + ' card' + (b.items.length === 1 ? '' : 's') + ')</span></h2>'
      : '<h2 style="margin:18px 0 6px;padding:6px 10px;background:#fee;border-left:4px solid #c00;font-size:1.05rem;color:#900;">' + (idx + 1) + '. ⚠ NO BIN ASSIGNED <span style="font-weight:400;font-size:0.85rem;">(' + b.items.length + ')</span></h2>';
    body += header;
    body += '<table style="width:100%;border-collapse:collapse;margin-bottom:8px;">';
    b.items.forEach(i => {
      const meta = [i.set_name, i.card_number ? '#' + i.card_number : '', i.condition, i.variant].filter(Boolean).join(' · ');
      body += '<tr style="border-bottom:1px solid #eee;">'
        + '<td style="padding:6px 4px;width:60px;">' + (i.image_url ? '<img src="' + _esc(i.image_url) + '" style="width:50px;height:50px;object-fit:cover;border-radius:3px;">' : '') + '</td>'
        + '<td style="padding:6px 4px;"><strong>' + _esc(i.title || '—') + '</strong><div style="font-size:0.8rem;color:#666;">' + _esc(meta) + '</div></td>'
        + '<td style="padding:6px 4px;font-family:monospace;font-size:0.85rem;text-align:right;">' + _esc(i.barcode || '') + '</td>'
        + '</tr>';
    });
    body += '</table>';
  });

  if (sealed.length) {
    body += '<h2 style="margin:18px 0 6px;padding:6px 10px;background:#fffbe6;border-left:4px solid #c80;font-size:1.05rem;">📦 Other items (sealed / storefront)</h2>';
    body += '<table style="width:100%;border-collapse:collapse;margin-bottom:8px;">';
    sealed.forEach(i => {
      const qty = i.qty && i.qty > 1 ? ' ×' + i.qty : '';
      body += '<tr style="border-bottom:1px solid #eee;">'
        + '<td style="padding:6px 4px;width:60px;">' + (i.image_url ? '<img src="' + _esc(i.image_url) + '" style="width:50px;height:50px;object-fit:cover;border-radius:3px;">' : '') + '</td>'
        + '<td style="padding:6px 4px;"><strong>' + _esc(i.title || '—') + qty + '</strong></td>'
        + '<td style="padding:6px 4px;font-family:monospace;font-size:0.85rem;text-align:right;">' + _esc(i.sku || '') + '</td>'
        + '</tr>';
    });
    body += '</table>';
  }

  const headerHtml = '<div style="margin-bottom:14px;">'
    + '<h1 style="margin:0 0 4px;font-size:1.4rem;">' + _esc(hold.customer_name || hold.shipping_name || '—') + (hold.order_number ? ' <span style="font-weight:400;color:#666;font-size:1rem;">' + _esc(hold.order_number) + '</span>' : '') + '</h1>'
    + '<div style="font-size:0.9rem;color:#444;">' + _esc(hold.customer_email || '') + '</div>'
    + '<div style="font-size:0.9rem;color:#444;">' + _esc(hold.shipping_address || '') + '</div>'
    + '</div>';

  const routeHtml = bucketKeys.length
    ? '<div style="margin-bottom:14px;padding:8px 10px;background:#fafafa;border:1px solid #ddd;border-radius:6px;"><div style="font-size:0.85rem;font-weight:700;margin-bottom:4px;">PULL ROUTE</div>' + routeSummary + '</div>'
    : '';

  const w = window.open('', '_blank');
  w.document.write('<html><head><title>Pull Slip — ' + _esc(hold.order_number || hold.hold_id) + '</title>'
    + '<style>body{font-family:sans-serif;padding:20px;max-width:800px;}h1{font-size:1.4rem;}h2{font-size:1.05rem;}table{font-size:0.95rem;}@media print { h2 { break-inside: avoid; } table { break-inside: avoid; } }</style>'
    + '</head><body>');
  w.document.write(headerHtml + cancelledNote + routeHtml + (body || '<p>No items.</p>'));
  w.document.write('</body></html>');
  w.document.close();
  setTimeout(() => w.print(), 100);
}

async function markPullShipped(holdId, btn) {
  if (!confirm('Mark this hold as pulled and shipped? Cards are already marked SOLD; this just clears it from the queue.')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/raw-card-pulls/' + holdId + '/mark-shipped', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Failed'); btn.disabled = false; return; }
    toast('Hold closed', 'green');
    loadRawPulls();
  } catch (e) { alert(e.message); btn.disabled = false; }
}

async function closeCancelledHold(holdId, btn) {
  if (!confirm('Close this cancelled hold? Make sure every raw card has been Restocked first — this only closes the hold, it does NOT auto-restock any remaining cards.')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/raw-card-pulls/' + holdId + '/close-cancelled', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Failed'); btn.disabled = false; return; }
    toast('Cancelled hold closed', 'green');
    loadRawPulls();
  } catch (e) { alert(e.message); btn.disabled = false; }
}

// ── Per-item mutations (Mark Missing / Change Condition / Restock) ──────────

async function pullMarkMissing(holdItemId, btn) {
  if (!confirm('Mark this card MISSING? It stays SOLD on Shopify — refund the customer manually.')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/raw-card-pulls/item/' + holdItemId + '/missing', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Failed'); btn.disabled = false; return; }
    toast('Marked missing', 'red');
    loadRawPulls();
  } catch (e) { alert(e.message); btn.disabled = false; }
}

async function pullRestock(holdItemId, binLabel, binType, btn) {
  let where;
  if (binLabel) {
    let kind;
    if (binType === 'binder') kind = '📒 binder';
    else if (binType === 'display_case' || binType === 'display') kind = '📍 display case';
    else kind = '📦 storage bin';
    where = 'LEAVE THE CARD WHERE IT IS — it is already in ' + kind + ' ' + binLabel + '.';
  } else {
    where = 'No bin on file for this card — the system will auto-assign a storage bin and tell you where to put it after you confirm.';
  }
  if (!confirm(where + '\\n\\nState flips back so the card becomes browsable in the kiosk again (Champion listings are created on the fly at checkout, so there is nothing to re-list). Refund the customer manually in Shopify Admin.')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/raw-card-pulls/item/' + holdItemId + '/restock', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) { alert(d.error || 'Failed'); btn.disabled = false; return; }
    if (d.auto_assigned) {
      alert('Card has no original bin — put it in ' + (d.bin_label || '?') + '.');
    }
    toast('Restocked → ' + d.state + ' @ ' + (d.bin_label || '?'), 'green');
    loadRawPulls();
  } catch (e) { alert(e.message); btn.disabled = false; }
}

const _PULL_CONDITIONS = ['NM', 'LP', 'MP', 'HP', 'DMG'];

function pullChangeCondition(holdItemId, currentCondition, btn) {
  // Lightweight inline picker — no modal library, just buttons in a popover.
  document.querySelectorAll('.pull-cond-popover').forEach(el => el.remove());
  const pop = document.createElement('div');
  pop.className = 'pull-cond-popover';
  pop.style.cssText = 'position:absolute;background:var(--s2);border:1px solid var(--border);border-radius:6px;padding:6px;box-shadow:0 8px 24px rgba(0,0,0,0.5);z-index:200;display:flex;gap:4px;';
  pop.innerHTML = _PULL_CONDITIONS.map(c =>
    '<button class="btn btn-secondary btn-sm" style="font-size:0.72rem;padding:3px 8px;' +
    (c === currentCondition ? 'background:rgba(0,180,255,0.18);color:#5cf;' : '') +
    '" data-cond="' + c + '">' + c + '</button>'
  ).join('');
  const rect = btn.getBoundingClientRect();
  pop.style.top = (window.scrollY + rect.bottom + 4) + 'px';
  pop.style.left = (window.scrollX + rect.left) + 'px';
  document.body.appendChild(pop);

  const cleanup = () => { pop.remove(); document.removeEventListener('click', onDocClick, true); };
  const onDocClick = (ev) => { if (!pop.contains(ev.target) && ev.target !== btn) cleanup(); };
  setTimeout(() => document.addEventListener('click', onDocClick, true), 0);

  pop.querySelectorAll('button[data-cond]').forEach(b => {
    b.addEventListener('click', async (ev) => {
      ev.stopPropagation();
      const cond = b.dataset.cond;
      if (cond === currentCondition) { cleanup(); return; }
      b.disabled = true;
      try {
        const r = await fetch('/api/raw-card-pulls/item/' + holdItemId + '/condition', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ condition: cond }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed'); cleanup(); return; }
        toast('Condition → ' + cond, 'green');
        cleanup();
        loadRawPulls();
      } catch (e) { alert(e.message); cleanup(); }
    });
  });
}

function _fdBadgeHtml(email) {
  const fd = _fdTickets[email];
  if (!fd || !fd.configured) return '';
  const tickets = fd.tickets || [];
  if (!tickets.length) return '<span class="fd-badge none">No ticket</span>';
  const hasReply = tickets.some(t => t.has_customer_reply);
  if (hasReply) return '<span class="fd-badge reply">Customer replied</span>';
  return '<span class="fd-badge waiting">Awaiting response</span>';
}

function _fdSectionHtml(email, orderId) {
  const fd = _fdTickets[email];
  if (!fd || !fd.configured || !fd.tickets || !fd.tickets.length) return '';
  const safeId = orderId.replace(/[^a-zA-Z0-9]/g, '_');
  let html = '<div class="fd-section">';
  html += '<span class="fd-toggle" onclick="toggleFdConvos(&apos;' + safeId + '&apos;)">💬 Freshdesk (' + fd.tickets.length + ' ticket' + (fd.tickets.length === 1 ? '' : 's') + ') <span id="fd-arrow-' + safeId + '">▸</span></span>';
  html += '<div class="fd-convos" id="fd-convos-' + safeId + '">';
  for (const t of fd.tickets) {
    html += '<div style="font-size:0.75rem;font-weight:600;margin:8px 0 4px;color:var(--text);">' + _esc(t.subject) + ' <span style="font-weight:400;color:var(--dim);">' + t.status_label + '</span></div>';
    if (!t.conversations.length) {
      html += '<div style="font-size:0.78rem;color:var(--dim);padding:4px 12px;">No replies yet</div>';
    }
    for (const c of t.conversations) {
      const dir = c.incoming ? 'incoming' : 'outgoing';
      const who = c.incoming ? 'Customer' : 'Agent';
      const date = c.created_at ? new Date(c.created_at).toLocaleString() : '';
      html += '<div class="fd-convo ' + dir + '">';
      html += '<div class="fd-convo-meta">' + who + ' · ' + date + '</div>';
      // Render the actual HTML body. Inline images render in place; non-image
      // inline attachments (e.g. PDFs Freshdesk dropped in as <img> tags) get
      // rewritten to clickable links so they aren't broken thumbnails.
      const atts = c.attachments || [];
      if (c.body) {
        html += '<div class="fd-convo-body">' + _fdRewriteInlineNonImages(c.body, atts) + '</div>';
      } else {
        html += '<div class="fd-convo-body fd-convo-body-text">' + _esc(c.body_text || '') + '</div>';
      }
      if (atts.length) {
        html += '<div class="fd-attach">' + atts.map(a => {
          const isImg = (a.content_type || '').startsWith('image/');
          const icon = isImg ? '🖼️' : ((a.content_type || '').includes('pdf') ? '📄' : '📎');
          return '<a href="' + a.url + '" target="_blank">' + icon + ' ' + _esc(a.name) + '</a>';
        }).join(' · ') + '</div>';
      }
      html += '</div>';
    }
  }
  html += '</div></div>';
  return html;
}

function _esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

// Freshdesk delivers every email attachment as <img data-id="..."> in the body
// HTML. For PDFs (and anything non-image) the browser renders that as a broken
// thumbnail. Match each <img> against the conversation's attachment list by
// data-id and swap non-images for a clickable file link.
function _fdRewriteInlineNonImages(bodyHtml, attachments) {
  if (!bodyHtml || !attachments || !attachments.length) return bodyHtml;
  const tmp = document.createElement('div');
  tmp.innerHTML = bodyHtml;
  tmp.querySelectorAll('img[data-id]').forEach(img => {
    const id = img.getAttribute('data-id');
    const att = attachments.find(a => String(a.id) === String(id));
    if (!att) return;
    const ct = (att.content_type || '').toLowerCase();
    if (ct.startsWith('image/')) return;
    const link = document.createElement('a');
    link.href = att.url || img.getAttribute('src') || '#';
    link.target = '_blank';
    link.rel = 'noreferrer';
    link.textContent = (ct.includes('pdf') ? '📄 ' : '📎 ') + (att.name || 'attachment');
    link.style.cssText = 'display:inline-block;padding:6px 10px;margin:4px 0;background:rgba(255,255,255,0.06);border-radius:6px;color:var(--accent);text-decoration:none;font-weight:500;';
    img.replaceWith(link);
  });
  return tmp.innerHTML;
}

// Click any image inside a Freshdesk convo to open it full-size in a new tab.
document.addEventListener('click', (e) => {
  const img = e.target.closest('.fd-convo-body img');
  if (img && img.src) { e.preventDefault(); window.open(img.src, '_blank'); }
});

function toggleFdConvos(safeId) {
  const el = document.getElementById('fd-convos-' + safeId);
  const arrow = document.getElementById('fd-arrow-' + safeId);
  if (el) { el.classList.toggle('open'); arrow.textContent = el.classList.contains('open') ? '▾' : '▸'; }
}

function _getTicketForOrder(email) {
  const fd = _fdTickets[email];
  if (!fd || !fd.configured || !fd.tickets || !fd.tickets.length) return null;
  // Return the most recently updated open/pending ticket
  return fd.tickets.find(t => t.status <= 3) || fd.tickets[0];
}

function renderVerification(orders, groups) {
  const el = document.getElementById('pane-verify');
  if (!orders.length && !(groups && groups.length)) { el.innerHTML = '<div class="empty">✅ No orders awaiting verification</div>'; return; }

  // Render cumulative verification groups first
  const groupHtml = (groups || []).map(g => {
    const orderIds = JSON.stringify(g.orders.map(o => o.id)).replace(/"/g, '&quot;');
    const orderNames = g.orders.map(o => o.name).join(', ');
    return `
    <div class="card" style="border-left:3px solid var(--amber);">
      <div class="order-header">
        <span class="badge badge-amber">${g.check_type}</span>
        ${_fdBadgeHtml(g.customer_email)}
        <strong>${g.customer_name}</strong> · ${g.orders.length} orders · <span style="font-weight:700;">$${g.total_value.toFixed(2)}</span>
        <div style="margin-left:auto;display:flex;gap:6px;">
          <button class="btn btn-green btn-sm" onclick="releaseVerificationGroup(${orderIds},'${orderNames}','${g.customer_email}')">✓ Verify & Release All</button>
          <button class="btn btn-sm" style="background:var(--red);color:#fff;" onclick="cancelVerificationGroup(${orderIds},'${orderNames}','${g.customer_email}')">✕ Cancel & Refund All</button>
        </div>
      </div>
      <div class="order-meta" style="margin-bottom:4px;">
        ${g.customer_email}
      </div>
      <div style="display:flex;flex-direction:column;gap:8px;">
        ${g.orders.map(o => `
          <div style="padding:8px 12px;background:rgba(255,255,255,0.03);border-radius:6px;">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <a href="https://admin.shopify.com/store/{{ shopify_store }}/orders/${o.numeric_id}" target="_blank" style="color:var(--accent);text-decoration:none;font-weight:600;">${o.name}</a>
              <span style="font-weight:600;">$${o.total.toFixed(2)}</span>
            </div>
            ${o.shipping_address ? '<div style="font-size:0.78rem;color:var(--dim);margin-top:2px;">' + o.shipping_address + '</div>' : ''}
            <div class="items-list" style="margin-top:4px;">${o.items.map(i => i.title + ' ×' + i.qty).join(' · ')}</div>
            ${o.note ? '<em style="color:var(--amber);font-size:0.78rem;">' + o.note + '</em>' : ''}
          </div>
        `).join('')}
      </div>
      ${_fdSectionHtml(g.customer_email, g.orders[0].id)}
    </div>`;
  }).join('');

  // Render standalone verification orders
  const orderHtml = orders.map(o => `
    <div class="card">
      <div class="order-header">
        <a href="https://admin.shopify.com/store/{{ shopify_store }}/orders/${o.numeric_id}" target="_blank" class="order-name" style="color:var(--accent);text-decoration:none;">${o.name}</a>
        <span class="badge badge-amber">${o.check_type}</span>
        ${_fdBadgeHtml(o.customer_email)}
        <span style="font-weight:700;">$${o.total.toFixed(2)}</span>
        <div style="margin-left:auto;display:flex;gap:6px;">
          <button class="btn btn-green btn-sm" onclick="releaseHold('${o.id}','${o.name}','${o.customer_email}')">✓ Verify & Release</button>
          <button class="btn btn-sm" style="background:var(--red);color:#fff;" onclick="cancelOrder('${o.id}','${o.name}','${o.customer_email}')">✕ Cancel & Refund</button>
        </div>
      </div>
      <div class="order-meta">
        <strong>${o.customer_name}</strong> · ${o.customer_email}<br>
        ${o.shipping_address}<br>
        ${o.note ? '<em style="color:var(--amber);">' + o.note + '</em>' : ''}
      </div>
      <div class="items-list">${o.items.map(i => i.title + ' ×' + i.qty).join(' · ')}</div>
      ${_fdSectionHtml(o.customer_email, o.id)}
    </div>
  `).join('');

  el.innerHTML = groupHtml + orderHtml;
}

function renderCombine(groups) {
  const el = document.getElementById('pane-combine');
  if (!groups.length) { el.innerHTML = '<div class="empty">✅ No orders to combine</div>'; return; }
  const printAllBtn = groups.length > 1
    ? '<div style="margin-bottom:14px;"><button class="btn btn-secondary btn-sm" onclick="printAllPackingLists()">🖨 Print All Packing Lists (' + groups.length + ')</button></div>'
    : '';
  el.innerHTML = printAllBtn + groups.map(g => `
    <div class="combine-group">
      <div class="combine-header">${g.customer_name} · ${g.orders.length} orders · $${g.total_value.toFixed(2)}</div>
      <div style="font-size:0.8rem;color:var(--dim);">${g.customer_email} · ${g.shipping_address}</div>
      <div class="combine-orders">
        ${g.orders.map(o => `
          <div class="combine-order">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <strong>${o.name}</strong>
              <div style="display:flex;align-items:center;gap:8px;">
                <span>$${o.total.toFixed(2)}</span>
                <button class="btn btn-sm" style="background:var(--amber);color:#000;font-size:0.7rem;padding:2px 8px;" onclick="uncombineOrder('${o.id}','${o.name}',${JSON.stringify(g.orders.map(x=>({id:x.id,name:x.name,total:x.total}))).replace(/"/g,'&quot;')},this)">✂ Do Not Combine</button>
              </div>
            </div>
            <div style="font-size:0.78rem;color:var(--dim);margin-top:4px;">
              ${o.items.map(i => i.title + ' ×' + i.qty).join(' · ')}
            </div>
            ${((o.note || '').split(/\\n+/).filter(l => l.trim() && !l.includes('Combine Order')).length > 0) ? '<div style="font-size:0.75rem;margin-top:4px;padding:4px 8px;background:rgba(255,170,0,0.08);border-radius:4px;color:var(--amber);">' + (o.note || '').split(/\\n+/).filter(l => l.trim() && !l.includes('Combine Order')).map(l => '⚠ ' + l.trim()).join('<br>') + '</div>' : ''}
          </div>
        `).join('')}
      </div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin:8px 0 4px;">
        <span style="font-size:0.78rem;font-weight:600;">Combined Packing List:</span>
        <button class="btn btn-secondary btn-sm" style="font-size:0.72rem;padding:2px 8px;" onclick="printPackingList(this)">🖨 Print</button>
      </div>
      <div class="items-list packing-list-content">
        ${g.all_items.map(i => {
          const isRaw = i.is_raw;
          const qty = isRaw ? '1×' : (i.qty + '×');
          const meta = isRaw && (i.condition || i.variant)
            ? '<span style="font-size:0.72rem;color:var(--dim);margin-left:6px;">' + [i.condition, i.variant].filter(Boolean).join(' · ') + '</span>'
            : '';
          const binPill = isRaw && i.bin_label
            ? '<span style="margin-left:auto;padding:2px 8px;background:rgba(0,180,255,0.12);color:#5cf;border-radius:10px;font-weight:600;font-size:0.72rem;">' + (i.bin_type === 'display' ? '📍 ' : (i.bin_type === 'binder' ? '📒 ' : '📦 ')) + i.bin_label + '</span>'
            : (isRaw ? '<span style="margin-left:auto;color:var(--red);font-size:0.72rem;">⚠ no bin</span>' : '');
          return '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' + (i.image ? '<img src="' + i.image + '" style="width:40px;height:40px;object-fit:cover;border-radius:4px;">' : '') + '<span><strong>' + qty + '</strong> ' + i.title + meta + '</span>' + binPill + '</div>';
        }).join('')}
      </div>
      <div style="margin-top:10px;display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;">
        <div style="flex:1;min-width:200px;">
          <label style="font-size:0.72rem;color:var(--dim);">Tracking Number</label>
          <input type="text" class="tracking-input" placeholder="Paste tracking #" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
        </div>
        <div style="width:100px;">
          <label style="font-size:0.72rem;color:var(--dim);">Carrier</label>
          <select class="carrier-select" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
            <option value="USPS">USPS</option>
            <option value="UPS">UPS</option>
            <option value="FedEx">FedEx</option>
          </select>
        </div>
        <button class="btn btn-green btn-sm" onclick="releaseAndFulfillGroup(this, ${JSON.stringify(g.orders.map(o=>o.id)).replace(/"/g,'&quot;')})">🚀 Release & Ship</button>
      </div>
      <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
        ${g.orders.map(o => `
          <a href="https://admin.shopify.com/store/{{ shopify_store }}/orders/${o.numeric_id}" target="_blank" class="btn btn-secondary btn-sm">
            ${o.name} → Admin ↗
          </a>
        `).join('')}
      </div>
    </div>
  `).join('');
}

async function cancelOrder(orderId, orderName, email) {
  const ticket = _getTicketForOrder(email);
  if (ticket && _fdCanned && _fdCanned.configured) {
    showFdReplyModal(ticket, 'deny', async (ticketId, cannedId) => {
      if (!confirm('Cancel ' + orderName + ' and issue full refund?')) return;
      if (ticketId && cannedId) {
        try {
          await fetch('/api/freshdesk-reply', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ ticket_id: ticketId, canned_response_id: cannedId }),
          });
        } catch(e) { console.warn('Freshdesk reply failed:', e); }
      }
      await _doCancelOrder(orderId, orderName);
    });
  } else {
    if (!confirm('Cancel ' + orderName + ' and issue full refund?')) return;
    await _doCancelOrder(orderId, orderName);
  }
}

async function _doCancelOrder(orderId, orderName) {
  try {
    const r = await fetch('/api/cancel-order', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_id: orderId }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Cancelled + refunded: ' + orderName, 'green');
    _fdTickets = {};  // Clear ticket cache
    loadOrders();
  } catch(e) { alert(e.message); }
}

async function releaseHold(orderId, orderName, email) {
  const ticket = _getTicketForOrder(email);
  if (ticket && _fdCanned && _fdCanned.configured) {
    showFdReplyModal(ticket, 'approve', async (ticketId, cannedId) => {
      if (!confirm('Release hold on ' + orderName + '?')) return;
      if (ticketId && cannedId) {
        try {
          await fetch('/api/freshdesk-reply', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ ticket_id: ticketId, canned_response_id: cannedId }),
          });
        } catch(e) { console.warn('Freshdesk reply failed:', e); }
      }
      await _doReleaseHold(orderId, orderName);
    });
  } else {
    if (!confirm('Release hold on ' + orderName + '?')) return;
    await _doReleaseHold(orderId, orderName);
  }
}

async function _doReleaseHold(orderId, orderName) {
  try {
    const r = await fetch('/api/release-hold', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_id: orderId }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    toast('Released: ' + orderName, 'green');
    _fdTickets = {};
    loadOrders();
  } catch(e) { alert(e.message); }
}

async function releaseVerificationGroup(orderIds, orderNames, email) {
  const ticket = _getTicketForOrder(email);
  if (ticket && _fdCanned && _fdCanned.configured) {
    showFdReplyModal(ticket, 'approve', async (ticketId, cannedId) => {
      if (!confirm('Release verification hold on ' + orderNames + '?')) return;
      if (ticketId && cannedId) {
        try {
          await fetch('/api/freshdesk-reply', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ ticket_id: ticketId, canned_response_id: cannedId }),
          });
        } catch(e) { console.warn('Freshdesk reply failed:', e); }
      }
      await _doReleaseVerificationGroup(orderIds, orderNames);
    });
  } else {
    if (!confirm('Release verification hold on ' + orderNames + '?')) return;
    await _doReleaseVerificationGroup(orderIds, orderNames);
  }
}

async function _doReleaseVerificationGroup(orderIds, orderNames) {
  try {
    const r = await fetch('/api/release-verification-group', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_ids: orderIds }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); return; }
    const kept = (d.orders || []).filter(o => o.kept_for_combine).length;
    if (kept) {
      toast('Verified: ' + orderNames + ' — moved to Combine', 'green');
    } else {
      toast('Verified & released: ' + orderNames, 'green');
    }
    _fdTickets = {};
    loadOrders();
  } catch(e) { alert(e.message); }
}

async function cancelVerificationGroup(orderIds, orderNames, email) {
  const ticket = _getTicketForOrder(email);
  if (ticket && _fdCanned && _fdCanned.configured) {
    showFdReplyModal(ticket, 'deny', async (ticketId, cannedId) => {
      if (!confirm('Cancel ALL orders (' + orderNames + ') and issue full refunds?')) return;
      if (ticketId && cannedId) {
        try {
          await fetch('/api/freshdesk-reply', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ ticket_id: ticketId, canned_response_id: cannedId }),
          });
        } catch(e) { console.warn('Freshdesk reply failed:', e); }
      }
      await _doCancelVerificationGroup(orderIds, orderNames);
    });
  } else {
    if (!confirm('Cancel ALL orders (' + orderNames + ') and issue full refunds?')) return;
    await _doCancelVerificationGroup(orderIds, orderNames);
  }
}

async function _doCancelVerificationGroup(orderIds, orderNames) {
  try {
    let failed = [];
    for (const oid of orderIds) {
      const r = await fetch('/api/cancel-order', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ order_id: oid }),
      });
      if (!r.ok) {
        const d = await r.json();
        failed.push(d.error || oid);
      }
    }
    if (failed.length) {
      alert('Some cancels failed: ' + failed.join(', '));
    } else {
      toast('Cancelled & refunded all: ' + orderNames, 'green');
    }
    _fdTickets = {};
    loadOrders();
  } catch(e) { alert(e.message); }
}

function showFdReplyModal(ticket, action, onConfirm) {
  const actionLabel = action === 'approve' ? 'Verify & Release' : 'Cancel & Refund';
  const actionColor = action === 'approve' ? 'var(--green)' : 'var(--red)';
  const actionTextColor = action === 'approve' ? '#000' : '#fff';

  // Build canned response options
  let optionsHtml = '<option value="">— Select a response —</option>';
  for (const folder of (_fdCanned.folders || [])) {
    if (!folder.responses.length) continue;
    optionsHtml += '<optgroup label="' + _esc(folder.name) + '">';
    for (const r of folder.responses) {
      optionsHtml += '<option value="' + r.id + '">' + _esc(r.title) + '</option>';
    }
    optionsHtml += '</optgroup>';
  }

  const overlay = document.createElement('div');
  overlay.className = 'fd-modal-overlay';
  overlay.innerHTML = `
    <div class="fd-modal">
      <h3>Reply to Freshdesk Ticket</h3>
      <div style="font-size:0.82rem;color:var(--dim);margin-bottom:12px;">
        Ticket: <strong>${_esc(ticket.subject)}</strong> (#${ticket.id})
      </div>
      <label style="font-size:0.75rem;color:var(--dim);">Canned Response</label>
      <select id="fd-canned-select" onchange="previewFdCanned(this.value)">
        ${optionsHtml}
      </select>
      <div id="fd-canned-preview" class="fd-modal-preview" style="display:none;"></div>
      <div class="fd-modal-actions">
        <button class="btn btn-secondary btn-sm" onclick="this.closest('.fd-modal-overlay').remove()">Cancel</button>
        <button class="btn btn-sm" style="color:var(--dim);" onclick="closeFdModalAndProceed(null, null)">Skip (no reply)</button>
        <button id="fd-send-btn" class="btn btn-sm" style="background:${actionColor};color:${actionTextColor};" onclick="closeFdModalAndSend()">Send & ${actionLabel}</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  // Close on backdrop click
  overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });

  // Store callback
  window._fdModalCallback = onConfirm;
  window._fdModalTicketId = ticket.id;
  window._fdModalOverlay = overlay;
}

const _fdCannedBodyCache = {};
async function previewFdCanned(responseId) {
  const preview = document.getElementById('fd-canned-preview');
  if (!responseId) { preview.style.display = 'none'; return; }
  preview.style.display = 'block';
  if (_fdCannedBodyCache[responseId]) {
    preview.innerHTML = _fdCannedBodyCache[responseId];
    return;
  }
  preview.innerHTML = '<div class="spinner" style="margin:10px auto;"></div>';
  try {
    const r = await fetch('/api/freshdesk-canned-response/' + responseId);
    const d = await r.json();
    if (!r.ok) {
      preview.innerHTML = '<em style="color:var(--red);">' + _esc(d.error || 'Failed to load') + '</em>';
      return;
    }
    const html = d.content || '<em>(empty)</em>';
    _fdCannedBodyCache[responseId] = html;
    preview.innerHTML = html;
  } catch(e) {
    preview.innerHTML = '<em style="color:var(--red);">Failed to load preview</em>';
  }
}

function closeFdModalAndSend() {
  const select = document.getElementById('fd-canned-select');
  const cannedId = select ? parseInt(select.value) : null;
  if (!cannedId) { alert('Select a canned response or click "Skip"'); return; }
  const overlay = window._fdModalOverlay;
  const callback = window._fdModalCallback;
  const ticketId = window._fdModalTicketId;
  if (overlay) overlay.remove();
  if (callback) callback(ticketId, cannedId);
}

function closeFdModalAndProceed(ticketId, cannedId) {
  const overlay = window._fdModalOverlay;
  const callback = window._fdModalCallback;
  if (overlay) overlay.remove();
  if (callback) callback(null, null);
}

const _slipStyle = '<style>body{font-family:-apple-system,sans-serif;padding:24px;font-size:18px;}'
  + 'h2{margin:0 0 6px;font-size:22px;} .addr{color:#666;font-size:15px;margin-bottom:14px;}'
  + '.orders{color:#666;font-size:14px;margin-bottom:18px;}'
  + '.item-row{display:flex;align-items:center;gap:12px;margin-bottom:10px;}'
  + '.item-row img{width:60px;height:60px;object-fit:cover;border-radius:6px;border:1px solid #ddd;}'
  + '.item-row strong{font-size:20px;} .item-row span{font-size:17px;}'
  + '.slip{margin-bottom:24px;}'
  + '@media print{body{padding:12px;} .slip{page-break-after:always;} .slip:last-child{page-break-after:auto;}}</style>';

function _packingSlipHtml(group) {
  const header = group.querySelector('.combine-header').textContent;
  const addr = group.querySelector('.combine-header').nextElementSibling.textContent;
  const orders = [...group.querySelectorAll('.combine-order')].map(o => o.querySelector('strong').textContent).join(', ');
  const itemEls = group.querySelectorAll('.packing-list-content .item-row, .packing-list-content div[style]');
  let itemsHtml = '';
  itemEls.forEach(el => { itemsHtml += '<div class="item-row">' + el.innerHTML + '</div>'; });
  return '<div class="slip"><h2>' + header + '</h2><div class="addr">' + addr + '</div><div class="orders">Orders: ' + orders + '</div><hr>' + itemsHtml + '</div>';
}

function printPackingList(btn) {
  const group = btn.closest('.combine-group');
  const win = window.open('', '_blank', 'width=500,height=700');
  win.document.write('<html><head><title>Packing List</title>' + _slipStyle + '</head><body>' + _packingSlipHtml(group) + '</body></html>');
  win.document.close();
  win.print();
}

function printAllPackingLists() {
  const groups = document.querySelectorAll('.combine-group');
  const slips = [...groups].map(g => _packingSlipHtml(g)).join('');
  const win = window.open('', '_blank', 'width=500,height=700');
  win.document.write('<html><head><title>All Packing Lists</title>' + _slipStyle + '</head><body>' + slips + '</body></html>');
  win.document.close();
  win.print();
}

async function releaseAndFulfillGroup(btn, orderIds) {
  const container = btn.parentElement;
  const tracking = container.querySelector('.tracking-input').value.trim();
  const company = container.querySelector('.carrier-select').value;
  if (!tracking) { alert('Enter a tracking number'); return; }
  if (!confirm('Release holds and fulfill ' + orderIds.length + ' orders with tracking ' + tracking + '?')) return;
  btn.disabled = true;
  btn.textContent = '⏳ Fulfilling...';
  let ok = 0, errors = [];
  for (const id of orderIds) {
    try {
      const r = await fetch('/api/release-and-fulfill', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ order_id: id, tracking_number: tracking, tracking_company: company }),
      });
      const d = await r.json();
      if (d.fulfilled) ok++;
      else errors.push(d.error || 'Unknown error');
    } catch(e) { errors.push(e.message); }
  }
  btn.disabled = false;
  btn.textContent = '🚀 Release & Ship';
  if (errors.length) {
    toast(ok + ' fulfilled, ' + errors.length + ' failed: ' + errors[0], 'red');
  } else {
    toast('All ' + ok + ' orders fulfilled with tracking', 'green');
  }
  loadOrders();
}

async function uncombineOrder(orderId, orderName, groupOrders, btn) {
  const remaining = groupOrders.filter(o => o.id !== orderId);
  const msg = remaining.length === 1
    ? 'This group only has 2 orders — both will be released for normal fulfillment. Continue?'
    : 'Remove ' + orderName + ' from this combine group? It will be released for normal fulfillment.';
  if (!confirm(msg)) return;
  btn.disabled = true;
  btn.textContent = '⏳...';
  try {
    const r = await fetch('/api/uncombine-order', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ order_id: orderId, order_name: orderName, group_orders: groupOrders }),
    });
    const d = await r.json();
    if (!r.ok) { alert(d.error); btn.disabled = false; btn.textContent = '✂ Do Not Combine'; return; }
    const count = (d.released || []).length;
    toast(count > 1 ? count + ' orders released from combine group' : orderName + ' removed from combine group', 'green');
    loadOrders();
  } catch(e) { alert(e.message); btn.disabled = false; btn.textContent = '✂ Do Not Combine'; }
}

// ── Customer Notes Tab ──
let _selectedCustomer = null;

async function loadNotes(q) {
  const el = document.getElementById('pane-notes');
  const url = q ? '/api/customer-notes?q=' + encodeURIComponent(q) : '/api/customer-notes';
  try {
    const r = await fetch(url);
    const notes = await r.json();
    renderNotes(notes);
  } catch(e) {
    el.innerHTML = '<div class="empty">' + e.message + '</div>';
  }
}

function renderNotes(notes) {
  const el = document.getElementById('pane-notes');
  const form = `
    <div class="card" style="margin-bottom:16px;">
      <div style="font-weight:600;margin-bottom:10px;">Add Customer Note</div>
      <div style="position:relative;margin-bottom:10px;">
        <label style="font-size:0.72rem;color:var(--dim);">Find Customer</label>
        <input id="cn-customer-search" type="text" placeholder="Search by name or email..." autocomplete="off"
          oninput="debounceCustomerSearch()"
          style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
        <div id="cn-search-results" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:10;background:var(--surface);border:1px solid var(--border);border-radius:6px;margin-top:2px;max-height:250px;overflow-y:auto;box-shadow:0 4px 12px rgba(0,0,0,0.3);"></div>
      </div>
      <div id="cn-selected" style="display:none;margin-bottom:10px;padding:10px 14px;background:var(--s2);border-radius:8px;border:1px solid var(--accent);">
      </div>
      <div id="cn-note-form" style="display:none;">
        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;">
          <input id="cn-email" type="hidden">
          <input id="cn-name" type="hidden">
          <div style="width:120px;">
            <label style="font-size:0.72rem;color:var(--dim);">Type</label>
            <select id="cn-type" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
              <option value="note">Note</option>
              <option value="hold">Hold</option>
            </select>
          </div>
          <div style="flex:1;min-width:200px;">
            <label style="font-size:0.72rem;color:var(--dim);">Note Text *</label>
            <input id="cn-text" type="text" placeholder="e.g. KEEP ADDRESS AS 123 Main St" style="width:100%;margin-top:2px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
          </div>
          <button class="btn btn-green btn-sm" onclick="addNote()">+ Add</button>
        </div>
      </div>
    </div>
    <div class="section-title">Active Notes <span class="count-badge">${notes.length}</span></div>
    <div style="margin-bottom:12px;">
      <input id="cn-filter" type="text" placeholder="Filter existing notes..." oninput="debounceNoteFilter()" style="width:100%;max-width:350px;padding:6px 10px;background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:0.85rem;">
    </div>
  `;

  if (!notes.length) {
    el.innerHTML = form + '<div class="empty">No customer notes yet</div>';
    return;
  }

  const rows = notes.map(n => `
    <div class="card" style="display:flex;align-items:center;gap:12px;">
      <div style="flex:1;">
        <div style="font-weight:600;">${n.customer_name || '—'} <span style="font-weight:400;color:var(--dim);font-size:0.85rem;">${n.customer_email}</span></div>
        <div style="margin-top:4px;font-size:0.9rem;">${n.note_text}</div>
        <div style="margin-top:4px;font-size:0.72rem;color:var(--dim);">Added ${n.created_at ? new Date(n.created_at).toLocaleDateString() : '—'}</div>
      </div>
      <span class="badge ${n.note_type === 'hold' ? 'badge-red' : 'badge-blue'}" style="white-space:nowrap;">${n.note_type === 'hold' ? '⏸ Hold' : '📋 Note'}</span>
      <button class="btn btn-sm" style="background:var(--red);color:#fff;" onclick="deleteNote(${n.id})">✕</button>
    </div>
  `).join('');

  el.innerHTML = form + rows;
}

let _custSearchTimer;
function debounceCustomerSearch() {
  clearTimeout(_custSearchTimer);
  _custSearchTimer = setTimeout(searchCustomers, 350);
}

async function searchCustomers() {
  const q = document.getElementById('cn-customer-search').value.trim();
  const resultsEl = document.getElementById('cn-search-results');
  if (q.length < 2) { resultsEl.style.display = 'none'; return; }

  try {
    const r = await fetch('/api/customer-search?q=' + encodeURIComponent(q));
    const customers = await r.json();
    if (!customers.length) {
      resultsEl.innerHTML = '<div style="padding:10px;color:var(--dim);font-size:0.85rem;">No customers found</div>';
      resultsEl.style.display = 'block';
      return;
    }
    resultsEl.innerHTML = customers.map((c, i) => `
      <div onclick='selectCustomer(${JSON.stringify(c).replace(/'/g,"&#39;")})' style="padding:10px 14px;cursor:pointer;border-bottom:1px solid var(--border);${i === 0 ? '' : ''}" onmouseover="this.style.background='var(--s2)'" onmouseout="this.style.background='none'">
        <div style="font-weight:600;font-size:0.9rem;">${c.name} <span style="font-weight:400;color:var(--dim);font-size:0.8rem;">${c.orders} order${c.orders===1?'':'s'}</span></div>
        <div style="font-size:0.8rem;color:var(--dim);">${c.email}${c.phone ? ' · ' + c.phone : ''}</div>
        ${c.address ? '<div style="font-size:0.78rem;color:var(--dim);margin-top:2px;">' + c.address + '</div>' : ''}
      </div>
    `).join('');
    resultsEl.style.display = 'block';
  } catch(e) {
    resultsEl.innerHTML = '<div style="padding:10px;color:var(--red);">Search failed</div>';
    resultsEl.style.display = 'block';
  }
}

function selectCustomer(c) {
  _selectedCustomer = c;
  document.getElementById('cn-search-results').style.display = 'none';
  document.getElementById('cn-customer-search').value = '';

  const selEl = document.getElementById('cn-selected');
  selEl.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:space-between;">
      <div>
        <div style="font-weight:600;">${c.name}</div>
        <div style="font-size:0.85rem;color:var(--dim);">${c.email}${c.phone ? ' · ' + c.phone : ''}</div>
        ${c.address ? '<div style="font-size:0.82rem;color:var(--dim);margin-top:2px;">' + c.address + '</div>' : ''}
        <div style="font-size:0.75rem;color:var(--dim);margin-top:2px;">${c.orders} order${c.orders===1?'':'s'}</div>
      </div>
      <button class="btn btn-secondary btn-sm" onclick="clearCustomer()">✕</button>
    </div>
  `;
  selEl.style.display = 'block';

  document.getElementById('cn-email').value = c.email;
  document.getElementById('cn-name').value = c.name;
  document.getElementById('cn-note-form').style.display = 'block';
}

function clearCustomer() {
  _selectedCustomer = null;
  document.getElementById('cn-selected').style.display = 'none';
  document.getElementById('cn-note-form').style.display = 'none';
  document.getElementById('cn-email').value = '';
  document.getElementById('cn-name').value = '';
  document.getElementById('cn-text').value = '';
}

let _noteFilterTimer;
function debounceNoteFilter() {
  clearTimeout(_noteFilterTimer);
  _noteFilterTimer = setTimeout(() => {
    const q = document.getElementById('cn-filter').value.trim();
    loadNotes(q || undefined);
  }, 300);
}

async function addNote() {
  const email = document.getElementById('cn-email').value.trim();
  const name = document.getElementById('cn-name').value.trim();
  const type = document.getElementById('cn-type').value;
  const text = document.getElementById('cn-text').value.trim();
  if (!email || !text) { alert('Email and note text are required'); return; }
  try {
    const r = await fetch('/api/customer-notes', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ email, name, type, text }),
    });
    if (!r.ok) { const d = await r.json(); alert(d.error); return; }
    toast('Note added for ' + name, 'green');
    clearCustomer();
    loadNotes();
  } catch(e) { alert(e.message); }
}

async function deleteNote(id) {
  if (!confirm('Remove this customer note?')) return;
  try {
    const r = await fetch('/api/customer-notes/' + id, { method: 'DELETE' });
    if (!r.ok) { alert('Failed to delete'); return; }
    toast('Note removed', 'green');
    loadNotes();
  } catch(e) { alert(e.message); }
}

// Close search results when clicking outside
document.addEventListener('click', (e) => {
  const results = document.getElementById('cn-search-results');
  if (results && !e.target.closest('#cn-customer-search') && !e.target.closest('#cn-search-results')) {
    results.style.display = 'none';
  }
});

// ── History Tab ──
let _historyPage = 1;

function _historyFiltersHtml() {
  return `
    <div class="history-filters">
      <div>
        <label>Event Type</label>
        <select id="hist-event" onchange="loadHistory(1)">
          <option value="">All Events</option>
          <option value="hold">Hold</option>
          <option value="release">Release</option>
          <option value="cancel">Cancel</option>
          <option value="auto_cancel">Auto-Cancel</option>
          <option value="uncombine">Uncombine</option>
          <option value="upgrade">Upgrade</option>
        </select>
      </div>
      <div>
        <label>Check Type</label>
        <select id="hist-check" onchange="loadHistory(1)">
          <option value="">All Checks</option>
          <option value="tier1">Tier 1 ($700+)</option>
          <option value="tier2">Tier 2 ($1000+)</option>
          <option value="fraud_medium">Medium Fraud</option>
          <option value="fraud_high">High Fraud</option>
          <option value="spend_spike">Spend Spike</option>
          <option value="combine">Combine</option>
          <option value="firsttime5">FIRSTTIME5</option>
          <option value="customer_hold">Customer Hold</option>
        </select>
      </div>
      <div>
        <label>From</label>
        <input type="date" id="hist-from" onchange="loadHistory(1)">
      </div>
      <div>
        <label>To</label>
        <input type="date" id="hist-to" onchange="loadHistory(1)">
      </div>
      <div>
        <label>Search</label>
        <input type="text" id="hist-search" placeholder="Order # or email" onkeyup="if(event.key==='Enter')loadHistory(1)">
      </div>
      <div style="display:flex;align-items:flex-end;gap:6px;">
        <button class="btn btn-secondary btn-sm" onclick="loadHistory(1)">Search</button>
        <button class="btn btn-secondary btn-sm" onclick="clearHistoryFilters()">Clear</button>
      </div>
    </div>`;
}

function clearHistoryFilters() {
  document.getElementById('hist-event').value = '';
  document.getElementById('hist-check').value = '';
  document.getElementById('hist-from').value = '';
  document.getElementById('hist-to').value = '';
  document.getElementById('hist-search').value = '';
  loadHistory(1);
}

const CHECK_LABELS = {
  tier1: 'Tier 1 ($700+)', tier2: 'Tier 2 ($1000+)',
  fraud_medium: 'Medium Fraud', fraud_high: 'High Fraud',
  spend_spike: 'Spend Spike', combine: 'Combine',
  firsttime5: 'FIRSTTIME5', customer_hold: 'Customer Hold',
};

const EVENT_LABELS = {
  hold: 'Hold', release: 'Release', cancel: 'Cancel',
  auto_cancel: 'Auto-Cancel', uncombine: 'Uncombine', upgrade: 'Upgrade',
};

function _detailsSummary(details) {
  if (!details || !Object.keys(details).length) return '';
  const parts = [];
  if (details.cumulative_total != null) parts.push('cumulative $' + parseFloat(details.cumulative_total).toFixed(2));
  if (details.order_total != null) parts.push('order $' + parseFloat(details.order_total).toFixed(2));
  if (details.sibling_count != null) parts.push(details.sibling_count + ' sibling' + (details.sibling_count === 1 ? '' : 's'));
  if (details.siblings) parts.push(details.siblings.map(s => s.name).join(', '));
  if (details.from_tier != null) parts.push('from tier ' + details.from_tier);
  if (details.max_previous != null) parts.push('max prev $' + parseFloat(details.max_previous).toFixed(2));
  if (details.match_count != null) parts.push(details.match_count + ' match(es)');
  if (details.remaining != null) parts.push(details.remaining + ' remaining');
  if (details.group_size != null) parts.push('group of ' + details.group_size);
  if (details.note_text) parts.push(details.note_text);
  if (details.is_first_order) parts.push('first order');
  return parts.join(' · ');
}

async function loadHistory(page) {
  if (page != null) _historyPage = page;
  const el = document.getElementById('pane-history');

  // Preserve filters if already rendered, otherwise build fresh with defaults
  if (!document.getElementById('hist-event')) {
    el.innerHTML = _historyFiltersHtml() + '<div id="hist-results"><div class="spinner"></div></div>';
    // Set defaults: Combine + Hold + last 30 days
    document.getElementById('hist-event').value = 'hold';
    document.getElementById('hist-check').value = 'combine';
    const now = new Date();
    document.getElementById('hist-to').value = now.toISOString().slice(0, 10);
    const ago = new Date(now.getTime() - 30 * 86400000);
    document.getElementById('hist-from').value = ago.toISOString().slice(0, 10);
  } else {
    document.getElementById('hist-results').innerHTML = '<div class="spinner"></div>';
  }

  const params = new URLSearchParams();
  const ev = document.getElementById('hist-event').value;
  const ch = document.getElementById('hist-check').value;
  const df = document.getElementById('hist-from').value;
  const dt = document.getElementById('hist-to').value;
  const q = document.getElementById('hist-search').value.trim();
  if (ev) params.set('event_type', ev);
  if (ch) params.set('check_type', ch);
  if (df) params.set('date_from', df);
  if (dt) params.set('date_to', dt);
  if (q) params.set('q', q);
  params.set('page', _historyPage);

  try {
    const r = await fetch('/api/screening-history?' + params.toString());
    const d = await r.json();
    renderHistory(d);
  } catch(e) {
    document.getElementById('hist-results').innerHTML = '<div class="empty">' + e.message + '</div>';
  }
}

function renderHistory(d) {
  const el = document.getElementById('hist-results');
  if (!d.events.length) {
    el.innerHTML = '<div class="empty">No screening events match these filters.</div>';
    return;
  }

  const rows = d.events.map(e => {
    const time = new Date(e.created_at).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'});
    const eventCls = 'history-event history-event-' + e.event_type;
    const detailStr = _detailsSummary(e.details);
    return `<tr>
      <td style="white-space:nowrap;">${time}</td>
      <td><strong>${_esc(e.order_name || '—')}</strong></td>
      <td style="color:var(--dim);">${_esc(e.customer_email || '—')}</td>
      <td><span class="${eventCls}">${EVENT_LABELS[e.event_type] || e.event_type}</span></td>
      <td>${CHECK_LABELS[e.check_type] || e.check_type}</td>
      <td class="history-details" title="${_esc(detailStr)}">${_esc(detailStr)}</td>
    </tr>`;
  }).join('');

  let paginationHtml = '';
  if (d.pages > 1) {
    paginationHtml = '<div class="history-pagination">';
    if (d.page > 1) paginationHtml += '<button class="btn btn-secondary btn-sm" onclick="loadHistory(' + (d.page - 1) + ')">← Prev</button>';
    paginationHtml += '<span>Page ' + d.page + ' of ' + d.pages + ' (' + d.total + ' events)</span>';
    if (d.page < d.pages) paginationHtml += '<button class="btn btn-secondary btn-sm" onclick="loadHistory(' + (d.page + 1) + ')">Next →</button>';
    paginationHtml += '</div>';
  } else {
    paginationHtml = '<div class="history-pagination"><span>' + d.total + ' event' + (d.total === 1 ? '' : 's') + '</span></div>';
  }

  el.innerHTML = `
    <table class="history-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Order</th>
          <th>Customer</th>
          <th>Event</th>
          <th>Check</th>
          <th>Details</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    ${paginationHtml}`;
}

loadOrders();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)), debug=False)
