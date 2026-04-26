"""
screening — screening.pack-fresh.com
Order screening + review console: verification queue, combine shipping queue.
"""

import os
import logging
from flask import Flask, request, jsonify, render_template_string

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
register_auth_hooks(app, roles=["owner", "manager"], public_prefixes=('/screening/',))


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
                  edges { node { title quantity image { url } } }
                }
              }
            }
          }
        }
    """, {"first": 50, "q": 'tag:"hold-for-review"'})

    verification = []
    combine = []

    for edge in data.get("data", {}).get("orders", {}).get("edges", []):
        o = edge["node"]
        # Skip fulfilled orders — they've already been shipped
        if o.get("displayFulfillmentStatus") in ("FULFILLED", "PARTIALLY_FULFILLED"):
            continue
        tags = [t.lower() for t in (o.get("tags") or [])]
        customer = o.get("customer") or {}
        addr = o.get("shippingAddress") or {}
        items = [{"title": e["node"]["title"], "qty": e["node"]["quantity"],
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
        # Consolidate duplicate SKUs in combined packing list
        for item in o["items"]:
            existing = next((a for a in combine_groups[key]["all_items"]
                           if a["title"] == item["title"]), None)
            if existing:
                existing["qty"] += item["qty"]
            else:
                combine_groups[key]["all_items"].append({**item})

    return jsonify({
        "verification": standalone_verification,
        "verification_groups": final_verification_groups,
        "combine_groups": list(combine_groups.values()),
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
                "attachments": [
                    {"name": a.get("name", ""), "url": a.get("attachment_url", "")}
                    for a in (t.get("attachments") or [])
                ],
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
                "attachments": [
                    {"name": a.get("name", ""), "url": a.get("attachment_url", "")}
                    for a in (c.get("attachments") or [])
                ],
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

    # Sort: tickets with customer replies first, then by updated_at desc
    tickets.sort(key=lambda t: (not t["has_customer_reply"], t.get("updated_at", "")), reverse=False)

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


@app.route("/api/freshdesk-canned-response/<int:response_id>")
def api_freshdesk_canned_response(response_id):
    """Fetch a single canned response body for preview."""
    import freshdesk as fd
    if not fd.is_configured():
        return jsonify({"error": "Freshdesk not configured"}), 400
    try:
        canned = fd.get_canned_response(response_id)
        return jsonify({
            "id": canned.get("id"),
            "title": canned.get("title", ""),
            "content": canned.get("content", canned.get("body", "")),
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
        body_html = canned.get("content", canned.get("body", ""))
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
<title>Pack Fresh — Screening Console</title>
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
.fd-convo-body { white-space:pre-wrap; word-break:break-word; }
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
  <div style="display:flex;gap:2px;margin-bottom:20px;border-bottom:1px solid var(--border);">
    <button class="tab active" id="tab-verify" onclick="switchTab('verify')">🔍 Verification Queue</button>
    <button class="tab" id="tab-combine" onclick="switchTab('combine')">📦 Combine Shipping</button>
    <button class="tab" id="tab-notes" onclick="switchTab('notes')">👤 Customer Notes</button>
    <button class="tab" id="tab-egg" onclick="switchTab('egg')">🥚 Egg Hunt</button>
    <button class="tab" id="tab-history" onclick="switchTab('history')">📊 History</button>
  </div>
  <div id="pane-verify" class="pane active"><div class="spinner"></div></div>
  <div id="pane-combine" class="pane"><div class="spinner"></div></div>
  <div id="pane-notes" class="pane"></div>
  <div id="pane-egg" class="pane"></div>
  <div id="pane-history" class="pane"></div>
</div>

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
  document.getElementById('tab-combine').textContent = '📦 Combine (' + (_data.combine_groups||[]).length + ')';
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
      html += '<div class="fd-convo-body">' + _esc(c.body_text || '').substring(0, 500) + (c.body_text && c.body_text.length > 500 ? '...' : '') + '</div>';
      if (c.attachments && c.attachments.length) {
        html += '<div class="fd-attach">' + c.attachments.map(a => '<a href="' + a.url + '" target="_blank">' + _esc(a.name) + '</a>').join(' · ') + '</div>';
      }
      html += '</div>';
    }
  }
  html += '</div></div>';
  return html;
}

function _esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

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
        ${g.all_items.map(i => '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">' + (i.image ? '<img src="' + i.image + '" style="width:40px;height:40px;object-fit:cover;border-radius:4px;">' : '') + '<span><strong>' + i.qty + '×</strong> ' + i.title + '</span></div>').join('')}
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
      // Send Freshdesk reply first (non-blocking for the actual cancel)
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
