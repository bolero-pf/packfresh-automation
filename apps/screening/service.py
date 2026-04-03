# screening/service.py
"""
Order screening service:
  1) FIRSTTIME5 discount code abuse detection          (first-time orders only)
  2) Cumulative verification (no delivered orders yet)  (every order)
  3) Spend spike detection (has delivered orders)       (every order)
  4) Combine shipping (same customer unfulfilled)       (every order)
  5) Signature required ($500+)                         (every order)
  6) Shopify fraud risk (medium → verify, high → cancel)(risk analyzed trigger)
  7) Order cancelled → Klaviyo abuse notification
  8) Order fulfilled → cleanup tags/holds
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
import os, re, json, time
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ──────────────────────────────────────────────────────────────
TIER1_THRESHOLD     = float(os.environ.get("TIER1_THRESHOLD", "700.00"))
TIER2_THRESHOLD     = float(os.environ.get("TIER2_THRESHOLD", "1000.00"))
SPIKE_THRESHOLD     = float(os.environ.get("SPIKE_THRESHOLD", "1000.00"))
SPIKE_RATIO         = float(os.environ.get("SPIKE_RATIO", "0.20"))
SIGNATURE_THRESHOLD = float(os.environ.get("SIGNATURE_THRESHOLD", "500.00"))
FIRSTTIME5_CODE     = os.environ.get("FIRSTTIME5_CODE", "FIRSTTIME5")

# Easter egg promo config
EASTER_EGG_ACTIVE = os.environ.get("EASTER_EGG_ACTIVE", "false").lower() == "true"
EASTER_EGG_MIN_SPEND = float(os.environ.get("EASTER_EGG_MIN_SPEND", "75.00"))
EASTER_EGG_PRODUCT_TAG = os.environ.get("EASTER_EGG_PRODUCT_TAG", "collection box")

from shopify_graphql import shopify_gql, shopify_metafields_set, gid_numeric
from klaviyo import upsert_profile

# ═══════════════════════════════════════════════════════════════════════
#  GRAPHQL
# ═══════════════════════════════════════════════════════════════════════

ORDER_DETAIL_Q = """
query OrderDetail($id: ID!) {
  order(id: $id) {
    id
    name
    note
    createdAt
    tags
    displayFinancialStatus
    currentTotalPriceSet { shopMoney { amount currencyCode } }
    discountCodes
    risk { recommendation assessments { riskLevel } }
    customer {
      id
      email
      phone
      numberOfOrders
      firstName
      lastName
      tags
      defaultAddress { address1 address2 city province zip country phone }
    }
    shippingAddress { firstName lastName address1 address2 city province zip country phone }
    billingAddress  { firstName lastName address1 address2 city province zip country phone }
  }
}
"""

ORDERS_WITH_DISCOUNT_Q = """
query OrdersWithDiscount($first: Int!, $after: String, $query: String!) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
    edges {
      cursor
      node {
        id
        name
        createdAt
        discountCodes
        customer {
          id email phone firstName lastName
          defaultAddress { address1 address2 city province zip phone }
        }
        shippingAddress { firstName lastName address1 address2 city province zip phone }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# All customer orders with fulfillment + financial status (for cumulative/spike/combine)
CUSTOMER_ALL_ORDERS_Q = """
query CustomerAllOrders($customerId: ID!, $first: Int!) {
  customer(id: $customerId) {
    id
    tags
    orders(first: $first, sortKey: CREATED_AT, reverse: true) {
      edges {
        node {
          id
          name
          createdAt
          tags
          displayFulfillmentStatus
          displayFinancialStatus
          currentTotalPriceSet { shopMoney { amount } }
          shippingAddress { address1 address2 city province zip }
        }
      }
    }
  }
}
"""

ORDER_FULFILLMENT_ORDERS_Q = """
query OrderFulfillmentOrders($id: ID!) {
  order(id: $id) {
    id
    fulfillmentOrders(first: 10) {
      edges { node { id status } }
    }
  }
}
"""

ORDER_UPDATE_NOTE = """
mutation OrderUpdate($input: OrderInput!) {
  orderUpdate(input: $input) {
    order { id note }
    userErrors { field message }
  }
}
"""

ORDER_TAGS_ADD = """
mutation TagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node { ... on Order { id tags } }
    userErrors { field message }
  }
}
"""

ORDER_TAGS_REMOVE = """
mutation TagsRemove($id: ID!, $tags: [String!]!) {
  tagsRemove(id: $id, tags: $tags) {
    node { ... on Order { id tags } }
    userErrors { field message }
  }
}
"""

CUSTOMER_TAGS_ADD = """
mutation TagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node { ... on Customer { id tags } }
    userErrors { field message }
  }
}
"""

CUSTOMER_TAGS_REMOVE = """
mutation TagsRemove($id: ID!, $tags: [String!]!) {
  tagsRemove(id: $id, tags: $tags) {
    node { ... on Customer { id tags } }
    userErrors { field message }
  }
}
"""

FULFILLMENT_ORDER_HOLD = """
mutation FulfillmentOrderHold($fulfillmentHold: FulfillmentOrderHoldInput!, $id: ID!) {
  fulfillmentOrderHold(fulfillmentHold: $fulfillmentHold, id: $id) {
    fulfillmentOrder { id status }
    userErrors { field message }
  }
}
"""

FULFILLMENT_ORDER_RELEASE_HOLD = """
mutation FulfillmentOrderReleaseHold($id: ID!) {
  fulfillmentOrderReleaseHold(id: $id) {
    fulfillmentOrder { id status }
    userErrors { field message }
  }
}
"""

FULFILLMENT_CREATE = """
mutation FulfillmentCreate($fulfillment: FulfillmentV2Input!) {
  fulfillmentCreateV2(fulfillment: $fulfillment) {
    fulfillment { id status trackingInfo { number url company } }
    userErrors { field message }
  }
}
"""

ORDER_CANCEL = """
mutation OrderCancel($orderId: ID!, $reason: OrderCancelReason!, $notifyCustomer: Boolean, $refund: Boolean!, $restock: Boolean!, $staffNote: String) {
  orderCancel(orderId: $orderId, reason: $reason, notifyCustomer: $notifyCustomer, refund: $refund, restock: $restock, staffNote: $staffNote) {
    orderCancelUserErrors { field message code }
  }
}
"""

# ═══════════════════════════════════════════════════════════════════════
#  NORMALIZERS
# ═══════════════════════════════════════════════════════════════════════

def _normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    return digits[-10:] if len(digits) >= 10 else digits

def _normalize_address(addr: dict | None) -> str:
    if not addr:
        return ""
    street = (addr.get("address1") or "").strip().lower()
    street = re.sub(r"\s*(apt|suite|ste|unit|#)\s*\S*$", "", street, flags=re.I)
    zipcode = (addr.get("zip") or "").strip().replace("-", "").replace(" ", "")
    if not street or not zipcode:
        return ""
    return f"{street} {zipcode}"

def _normalize_name(first: str | None, last: str | None) -> str:
    f = (first or "").strip().lower()
    l = (last or "").strip().lower()
    if not f and not l:
        return ""
    return f"{f} {l}".strip()

def _extract_signals(order_node: dict) -> dict:
    cust = order_node.get("customer") or {}
    ship = order_node.get("shippingAddress") or {}
    bill = order_node.get("billingAddress") or cust.get("defaultAddress") or {}

    phones = set()
    for p in [cust.get("phone"), ship.get("phone"), bill.get("phone"),
              (cust.get("defaultAddress") or {}).get("phone")]:
        norm = _normalize_phone(p)
        if norm:
            phones.add(norm)

    shipping_addresses, billing_addresses = set(), set()
    ship_norm = _normalize_address(ship)
    if ship_norm:
        shipping_addresses.add(ship_norm)
    default_norm = _normalize_address(cust.get("defaultAddress"))
    if default_norm:
        shipping_addresses.add(default_norm)
    bill_norm = _normalize_address(bill)
    if bill_norm:
        billing_addresses.add(bill_norm)

    names = set()
    ship_name = _normalize_name(ship.get("firstName"), ship.get("lastName"))
    if ship_name:
        names.add(ship_name)
    cust_name = _normalize_name(cust.get("firstName"), cust.get("lastName"))
    if cust_name:
        names.add(cust_name)

    return {
        "email": (cust.get("email") or "").strip().lower(),
        "phones": phones,
        "shipping_addresses": shipping_addresses,
        "billing_addresses": billing_addresses,
        "names": names,
        "customer_gid": cust.get("id"),
    }

# ═══════════════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _add_order_note(order_gid: str, note_text: str):
    data = shopify_gql("""
        query($id: ID!) { order(id: $id) { id note } }
    """, {"id": order_gid})
    existing_note = (data.get("data", {}).get("order", {}).get("note") or "").strip()
    separator = "\n\n---\n\n" if existing_note else ""
    full_note = f"{existing_note}{separator}{note_text}"
    shopify_gql(ORDER_UPDATE_NOTE, {"input": {"id": order_gid, "note": full_note}})

def _hold_fulfillment(order_gid: str, reason_notes: str) -> int:
    data = shopify_gql(ORDER_FULFILLMENT_ORDERS_Q, {"id": order_gid})
    fo_edges = (data.get("data", {}).get("order", {})
                .get("fulfillmentOrders", {}).get("edges", []))
    held = 0
    for edge in fo_edges:
        fo = edge["node"]
        if fo["status"] in ("OPEN", "SCHEDULED"):
            try:
                shopify_gql(FULFILLMENT_ORDER_HOLD, {
                    "id": fo["id"],
                    "fulfillmentHold": {"reason": "OTHER", "reasonNotes": reason_notes},
                })
                held += 1
            except Exception as e:
                print(f"[screening] Failed to hold fulfillment {fo['id']}: {e}", flush=True)
    return held

def _release_fulfillment_holds(order_gid: str) -> int:
    data = shopify_gql(ORDER_FULFILLMENT_ORDERS_Q, {"id": order_gid})
    fo_edges = (data.get("data", {}).get("order", {})
                .get("fulfillmentOrders", {}).get("edges", []))
    released = 0
    for edge in fo_edges:
        fo = edge["node"]
        if fo["status"] == "ON_HOLD":
            try:
                shopify_gql(FULFILLMENT_ORDER_RELEASE_HOLD, {"id": fo["id"]})
                released += 1
            except Exception as e:
                print(f"[screening] Failed to release hold {fo['id']}: {e}", flush=True)
    return released

def release_and_fulfill(order_gid: str, tracking_number: str, tracking_company: str = "USPS") -> dict:
    """Release holds on an order and create a fulfillment with tracking info.

    Resilient to orders where the hold was already manually released in Shopify
    and/or the order was already fulfilled (label purchased manually).
    """
    data = shopify_gql(ORDER_FULFILLMENT_ORDERS_Q, {"id": order_gid})
    fo_edges = (data.get("data", {}).get("order", {})
                .get("fulfillmentOrders", {}).get("edges", []))

    # Release any remaining holds (ignore errors — hold may already be released)
    for edge in fo_edges:
        fo = edge["node"]
        if fo["status"] == "ON_HOLD":
            try:
                shopify_gql(FULFILLMENT_ORDER_RELEASE_HOLD, {"id": fo["id"]})
            except Exception as e:
                print(f"[screening] Failed to release hold {fo['id']}: {e}", flush=True)

    # Re-query to get current statuses after release attempts
    data = shopify_gql(ORDER_FULFILLMENT_ORDERS_Q, {"id": order_gid})
    fo_edges = (data.get("data", {}).get("order", {})
                .get("fulfillmentOrders", {}).get("edges", []))

    # Only collect FOs that are OPEN (ready for fulfillment)
    # Skip FULFILLED, CLOSED, CANCELLED — those are already done
    fulfillment_order_ids = []
    already_done = 0
    for edge in fo_edges:
        fo = edge["node"]
        if fo["status"] in ("OPEN", "SCHEDULED"):
            fulfillment_order_ids.append(fo["id"])
        elif fo["status"] in ("FULFILLED", "CLOSED"):
            already_done += 1

    # If everything is already fulfilled, that's a success — not an error
    if not fulfillment_order_ids:
        if already_done > 0:
            return {"fulfilled": True, "note": "Already fulfilled"}
        return {"fulfilled": False, "error": "No fulfillment orders found"}

    # Create fulfillment with tracking
    try:
        result = shopify_gql(FULFILLMENT_CREATE, {
            "fulfillment": {
                "lineItemsByFulfillmentOrder": [
                    {"fulfillmentOrderId": fo_id} for fo_id in fulfillment_order_ids
                ],
                "trackingInfo": {
                    "number": tracking_number,
                    "company": tracking_company,
                },
                "notifyCustomer": True,
            }
        })
        errors = result.get("data", {}).get("fulfillmentCreateV2", {}).get("userErrors", [])
        if errors:
            return {"fulfilled": False, "error": "; ".join(e["message"] for e in errors)}
        return {"fulfilled": True}
    except Exception as e:
        return {"fulfilled": False, "error": str(e)}


def _cancel_order(order_gid: str, staff_note: str, notify_customer: bool = False):
    shopify_gql(ORDER_CANCEL, {
        "orderId": order_gid,
        "reason": "FRAUD",
        "notifyCustomer": notify_customer,
        "refund": True,
        "restock": True,
        "staffNote": staff_note,
    })

def _apply_verification(order_gid: str, order_name: str, customer: dict,
                        tier: int, cumulative_total: float, note_text: str,
                        is_first_order: bool = False):
    """
    Shared logic for applying verification tags, holds, Klaviyo.
    First-time orders → id_verification_required / id_selfie_required
    Multi-order customers → cumulative_verification_required
    """
    customer_gid = customer.get("id")
    email = (customer.get("email") or "").strip()

    tag = "high-value-tier2" if tier >= 2 else "high-value-tier1"
    hold_reason = f"Cumulative ${cumulative_total:.2f} — verification required"

    try:
        shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": [tag, "hold-for-review"]})
    except Exception as e:
        print(f"[screening] Failed to tag order {order_gid}: {e}", flush=True)

    try:
        _add_order_note(order_gid, note_text)
    except Exception as e:
        print(f"[screening] Failed to add note to {order_gid}: {e}", flush=True)

    try:
        _hold_fulfillment(order_gid, hold_reason)
    except Exception as e:
        print(f"[screening] Failed to hold fulfillment for {order_gid}: {e}", flush=True)

    if customer_gid and email:
        try:
            external_id = customer_gid.split("/")[-1]
            if is_first_order:
                # First-time: use the original tier-specific properties
                klaviyo_prop = "id_selfie_required" if tier >= 2 else "id_verification_required"
                upsert_profile(email=email, external_id=external_id, properties={
                    klaviyo_prop: True,
                    f"{klaviyo_prop}_order": order_name,
                    f"{klaviyo_prop}_amount": cumulative_total,
                    f"{klaviyo_prop}_requested_at": datetime.now(timezone.utc).isoformat(),
                })
            else:
                # Multi-order: use cumulative property
                upsert_profile(email=email, external_id=external_id, properties={
                    "cumulative_verification_required": True,
                    "cumulative_verification_required_order": order_name,
                    "cumulative_verification_required_amount": cumulative_total,
                    "cumulative_verification_required_requested_at": datetime.now(timezone.utc).isoformat(),
                })
        except Exception as e:
            print(f"[screening] Klaviyo flag failed for {customer_gid}: {e}", flush=True)

    if customer_gid:
        try:
            shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": [tag]})
        except Exception as e:
            print(f"[screening] Failed to tag customer {customer_gid}: {e}", flush=True)

# ═══════════════════════════════════════════════════════════════════════
#  CHECK 1: FIRSTTIME5 ABUSE DETECTION
# ═══════════════════════════════════════════════════════════════════════

def _find_firsttime5_matches(current_order_gid, current_signals, max_pages=5):
    matches = []
    query_str = f'discount_code:"{FIRSTTIME5_CODE}"'
    after = None

    for _ in range(max_pages):
        data = shopify_gql(ORDERS_WITH_DISCOUNT_Q, {
            "first": 50, "after": after, "query": query_str,
        })
        orders = data.get("data", {}).get("orders", {})
        edges = orders.get("edges", [])

        for edge in edges:
            node = edge["node"]
            if node["id"] == current_order_gid:
                continue

            prev_signals = _extract_signals(node)
            reasons, confidence = [], "low"

            ship_overlap = current_signals["shipping_addresses"] & prev_signals["shipping_addresses"]
            if ship_overlap:
                reasons.append(f"shipping address: {list(ship_overlap)[0]}")
                confidence = "high"

            phone_overlap = current_signals["phones"] & prev_signals["phones"]
            if phone_overlap:
                reasons.append(f"phone: {list(phone_overlap)[0]}")
                confidence = "high"

            ship_to_bill = current_signals["shipping_addresses"] & prev_signals["billing_addresses"]
            bill_to_ship = current_signals["billing_addresses"] & prev_signals["shipping_addresses"]
            if ship_to_bill:
                reasons.append(f"shipping→prev billing: {list(ship_to_bill)[0]}")
                if confidence != "high": confidence = "medium"
            if bill_to_ship:
                reasons.append(f"billing→prev shipping: {list(bill_to_ship)[0]}")
                if confidence != "high": confidence = "medium"

            name_overlap = current_signals["names"] & prev_signals["names"]
            if name_overlap:
                matched_name = list(name_overlap)[0]
                if reasons:
                    reasons.append(f"name: {matched_name}")
                else:
                    reasons.append(f"name match: {matched_name}")
                    confidence = "medium"

            if reasons:
                matches.append({
                    "order_gid": node["id"], "order_name": node.get("name", "?"),
                    "customer_gid": prev_signals["customer_gid"], "email": prev_signals["email"],
                    "created_at": node.get("createdAt"), "match_reasons": reasons,
                    "confidence": confidence,
                })

        if not orders.get("pageInfo", {}).get("hasNextPage"):
            break
        after = edges[-1]["cursor"]
        time.sleep(0.1)

    return matches

def check_firsttime5_abuse(order_gid: str) -> dict:
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]

    codes = [c.upper() for c in (order.get("discountCodes") or [])]
    if FIRSTTIME5_CODE.upper() not in codes:
        return {"flagged": False, "reason": "no_firsttime5", "matches": []}

    signals = _extract_signals(order)
    matches = _find_firsttime5_matches(order_gid, signals)
    if not matches:
        return {"flagged": False, "reason": "no_matches", "matches": []}

    lines = [f"⚠️ FIRSTTIME5 REUSE DETECTED — {len(matches)} potential match(es):"]
    for m in matches[:5]:
        reasons_str = ", ".join(m["match_reasons"])
        lines.append(f"  • [{m['confidence'].upper()}] {m['order_name']} ({m['email'] or 'no email'}) — {reasons_str}")
    lines.append("")
    lines.append("ACTION: Verify before fulfilling. If confirmed abuse, cancel order.")
    note_text = "\n".join(lines)

    customer_gid = signals["customer_gid"]
    if customer_gid:
        try:
            shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["FIRSTTIME5-review"]})
        except Exception as e:
            print(f"[screening] Failed to tag customer {customer_gid}: {e}", flush=True)

    try:
        shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["FIRSTTIME5-review", "hold-for-review"]})
    except Exception as e:
        print(f"[screening] Failed to tag order {order_gid}: {e}", flush=True)

    try:
        _add_order_note(order_gid, note_text)
    except Exception as e:
        print(f"[screening] Failed to add note to {order_gid}: {e}", flush=True)

    try:
        _hold_fulfillment(order_gid, "FIRSTTIME5 reuse detected — verify before fulfilling")
    except Exception as e:
        print(f"[screening] Failed to hold fulfillment for {order_gid}: {e}", flush=True)

    return {"flagged": True, "reason": "firsttime5_reuse", "matches": matches[:5], "note": note_text}

# ═══════════════════════════════════════════════════════════════════════
#  EVERY-ORDER CHECKS (combine, cumulative verify, spike, signature)
#  All run from /screening/order_combine — one Flow, one API call
# ═══════════════════════════════════════════════════════════════════════

COMBINE_SKIP_TAGS = {"pre-order", "preorder", "pre_order"}
VERIFICATION_TAGS = {"high-value-tier1", "high-value-tier2"}
CANCELLED_STATUSES = {"VOIDED", "REFUNDED", "EXPIRED"}


def _apply_customer_notes(order_gid: str, order_name: str, customer_email: str) -> dict | None:
    """Check for customer notes/holds and apply them to the order."""
    try:
        from db import query
        notes = query(
            "SELECT id, note_type, note_text FROM customer_notes WHERE customer_email = %s AND active = true",
            (customer_email.lower(),),
        )
    except Exception as e:
        print(f"[screening] customer_notes lookup failed: {e}", flush=True)
        return None

    if not notes:
        return None

    applied = []
    held = False
    for note in notes:
        note_text = note["note_text"]
        note_type = note["note_type"]

        # Append note to order
        _add_order_note(order_gid, f"📋 {note_text}")

        if note_type == "hold":
            if not held:
                # Tag + hold the order
                try:
                    shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["hold-for-review", "customer-hold"]})
                    _hold_fulfillment(order_gid, f"Customer hold: {note_text}")
                    held = True
                except Exception as e:
                    print(f"[screening] customer hold failed for {order_name}: {e}", flush=True)

        applied.append({"type": note_type, "text": note_text})

    print(f"[screening] Applied {len(applied)} customer note(s) to {order_name} ({customer_email})", flush=True)
    return {"applied": applied, "held": held}


def screen_every_order(order_gid: str) -> dict:
    """
    Master function for every-order checks. Fetches order + customer history
    once, then runs: cumulative verification, spend spike, combine, signature.
    """
    # 1. Fetch the order
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    customer = order.get("customer") or {}
    customer_gid = customer.get("id")
    order_name = order.get("name", "?")
    order_total = float(order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))

    results = {
        "order_gid": order_gid,
        "order_name": order_name,
        "verification": None,
        "spend_spike": None,
        "combine": None,
        "signature": None,
        "customer_notes": None,
    }

    # Easter egg — remove tag after redemption (customer placed a new order)
    EGG_TAG_THRESHOLDS = {
        "STINK_EGG": 0, "BRONZE_EGG": 0,
        "SILVER_EGG": 50, "GOLDEN_EGG": 100,
    }
    if customer_gid:
        cust_tags = set(customer.get("tags") or [])
        for tag, min_total in EGG_TAG_THRESHOLDS.items():
            if tag in cust_tags and order_total >= min_total:
                try:
                    shopify_gql(CUSTOMER_TAGS_REMOVE, {"id": customer_gid, "tags": [tag]})
                    print(f"[easter_egg] Removed {tag} from {customer_gid} — order {order_name} ${order_total:.2f}", flush=True)
                except Exception as e:
                    print(f"[easter_egg] Failed to remove {tag}: {e}", flush=True)

    # 1b. Check for customer notes/holds
    customer_email = (customer.get("email") or "").strip().lower()
    if customer_email:
        results["customer_notes"] = _apply_customer_notes(order_gid, order_name, customer_email)

    if not customer_gid:
        # No customer (guest?) — just check signature on this order alone
        results["signature"] = _check_signature(order_gid, order_total)
        return results

    # 2. Fetch ALL customer orders in one call
    cust_data = shopify_gql(CUSTOMER_ALL_ORDERS_Q, {"customerId": customer_gid, "first": 50})
    cust = cust_data.get("data", {}).get("customer") or {}
    cust_tags = set(t.lower() for t in (cust.get("tags") or []))
    all_edges = cust.get("orders", {}).get("edges", [])

    # Normalize current order's shipping address for combine matching
    current_ship_addr = _normalize_address(order.get("shippingAddress"))

    # Check if the current order itself is a pre-order (skip combine if so)
    current_order_tags = set(t.lower() for t in (order.get("tags") or []))
    current_is_preorder = bool(current_order_tags & {"pre-order", "preorder", "pre_order"})

    # 3. Classify all orders
    has_delivered = False
    has_active_verification = False  # any non-cancelled order with a verification tag?
    non_cancelled_totals = []       # all orders that aren't cancelled (for cumulative)
    max_previous_total = 0.0        # for spend spike
    unfulfilled_siblings = []       # for combine

    for edge in all_edges:
        node = edge["node"]
        nid = node["id"]
        status = (node.get("displayFulfillmentStatus") or "").upper()
        financial = (node.get("displayFinancialStatus") or "").upper()
        total = float(node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
        tags = set(t.lower() for t in (node.get("tags") or []))

        # Check for any delivered order
        if status == "DELIVERED" or status == "FULFILLED":
            has_delivered = True

        # Skip cancelled for cumulative totals
        if financial not in CANCELLED_STATUSES:
            non_cancelled_totals.append(total)
            # Track max previous order (not current) for spike
            if nid != order_gid and total > max_previous_total:
                max_previous_total = total
            # Check if any active order already has a verification tag
            if nid != order_gid and (tags & {t.lower() for t in VERIFICATION_TAGS}):
                has_active_verification = True

        # Unfulfilled/on-hold siblings for combine — must match shipping address
        if nid != order_gid and status in ("UNFULFILLED", "ON_HOLD") and financial not in CANCELLED_STATUSES:
            if not (tags & COMBINE_SKIP_TAGS):
                sib_ship_addr = _normalize_address(node.get("shippingAddress"))
                if current_ship_addr and sib_ship_addr and current_ship_addr == sib_ship_addr:
                    unfulfilled_siblings.append({
                        "order_gid": nid,
                        "order_name": node.get("name", "?"),
                        "total": total,
                        "created_at": node.get("createdAt"),
                    })

    cumulative_total = sum(non_cancelled_totals)

    # ── CHECK: Cumulative verification (no delivered orders yet) ──
    if not has_delivered:
        # Skip if another active (non-cancelled) order already has a verification tag
        if not has_active_verification and cumulative_total >= TIER1_THRESHOLD:
            tier = 2 if cumulative_total >= TIER2_THRESHOLD else 1
            if tier >= 2:
                desc = "photo ID + selfie + shipping address confirmation"
            else:
                desc = "photo ID + shipping address confirmation"

            note_text = f"🪪 Waiting on ID Verification (${cumulative_total:.2f})"

            order_count = int(customer.get("numberOfOrders", 0) or 0)
            _apply_verification(order_gid, order_name, customer, tier, cumulative_total, note_text,
                                is_first_order=(order_count <= 1))
            results["verification"] = {
                "flagged": True, "reason": f"cumulative_tier{tier}",
                "tier": tier, "cumulative_total": cumulative_total,
            }
        else:
            results["verification"] = {
                "flagged": False,
                "reason": "active_verification_exists" if has_active_verification else "below_threshold",
                "cumulative_total": cumulative_total,
                "has_delivered": False,
            }
    else:
        results["verification"] = {
            "flagged": False, "reason": "has_delivered_order",
            "cumulative_total": cumulative_total,
        }

    # ── CHECK: Spend spike (only if they HAVE delivered orders) ──
    if has_delivered and order_total >= SPIKE_THRESHOLD:
        spike_ceiling = order_total * SPIKE_RATIO
        if max_previous_total < spike_ceiling:
            email = (customer.get("email") or "").strip()
            note_text = f"🪪 Waiting on ID Verification (${order_total:.2f})"

            try:
                shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["spend-spike-review", "hold-for-review"]})
            except Exception as e:
                print(f"[screening] Failed to tag order {order_gid}: {e}", flush=True)

            try:
                _add_order_note(order_gid, note_text)
            except Exception as e:
                print(f"[screening] Failed to add note to {order_gid}: {e}", flush=True)

            try:
                _hold_fulfillment(order_gid, f"Spend spike — ${order_total:.2f} vs max prev ${max_previous_total:.2f}")
            except Exception as e:
                print(f"[screening] Failed to hold fulfillment for {order_gid}: {e}", flush=True)

            if customer_gid and email:
                try:
                    external_id = customer_gid.split("/")[-1]
                    upsert_profile(email=email, external_id=external_id, properties={
                        "spend_spike_verification_required": True,
                        "spend_spike_verification_order": order_name,
                        "spend_spike_verification_amount": order_total,
                        "spend_spike_verification_max_previous": max_previous_total,
                        "spend_spike_verification_requested_at": datetime.now(timezone.utc).isoformat(),
                    })
                except Exception as e:
                    print(f"[screening] Klaviyo spend spike failed: {e}", flush=True)

            if customer_gid:
                try:
                    shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["spend-spike-review"]})
                except Exception as e:
                    print(f"[screening] Failed to tag customer: {e}", flush=True)

            results["spend_spike"] = {
                "flagged": True, "reason": "spend_spike",
                "total": order_total, "max_previous": max_previous_total,
            }
        else:
            results["spend_spike"] = {
                "flagged": False, "reason": "no_spike",
                "total": order_total, "max_previous": max_previous_total,
            }
    else:
        results["spend_spike"] = {"flagged": False, "reason": "not_applicable"}

    # ── CHECK: Combine shipping (skip if current order is a pre-order) ──
    if current_is_preorder:
        results["combine"] = {"flagged": False, "reason": "current_is_preorder"}
    elif unfulfilled_siblings:
        sibling_names = ", ".join(s["order_name"] for s in unfulfilled_siblings)
        sibling_details = "; ".join(f"{s['order_name']} (${s['total']:.2f})" for s in unfulfilled_siblings)

        note_text = f"📦 Combine Order ({sibling_names})"

        # Hold the current order + tag for screening console
        try:
            _hold_fulfillment(order_gid, f"Combine with {sibling_names} — same customer")
        except Exception as e:
            print(f"[screening] Failed to hold for combine {order_gid}: {e}", flush=True)
        try:
            shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["hold-for-review"]})
        except Exception as e:
            print(f"[screening] Failed to tag combine {order_gid}: {e}", flush=True)

        try:
            _add_order_note(order_gid, note_text)
        except Exception as e:
            print(f"[screening] Failed to add combine note to {order_gid}: {e}", flush=True)

        # Hold ALL siblings too — prevent fulfillment until combined
        for s in unfulfilled_siblings:
            try:
                _hold_fulfillment(s["order_gid"], f"Combine with {order_name} — same customer")
            except Exception as e:
                print(f"[screening] Failed to hold sibling {s['order_gid']}: {e}", flush=True)
            try:
                shopify_gql(ORDER_TAGS_ADD, {"id": s["order_gid"], "tags": ["hold-for-review"]})
            except Exception as e:
                print(f"[screening] Failed to tag sibling {s['order_gid']}: {e}", flush=True)
            try:
                _add_order_note(s["order_gid"],
                    f"📦 Combine Order ({order_name})")
            except Exception as e:
                print(f"[screening] Failed to note sibling {s['order_gid']}: {e}", flush=True)

        results["combine"] = {
            "flagged": True, "reason": "combine_orders",
            "siblings": unfulfilled_siblings,
        }
    else:
        results["combine"] = {"flagged": False, "reason": "no_siblings"}

    # ── CHECK: Signature ($500+ individual or combined with siblings) ──
    # Pre-orders don't need signature — they ship later when product arrives
    if current_is_preorder:
        results["signature"] = {"flagged": False, "reason": "current_is_preorder"}
    else:
        combined_ship_total = order_total + sum(s["total"] for s in unfulfilled_siblings)
        results["signature"] = _check_signature(
            order_gid, order_total,
            combined_total=combined_ship_total if unfulfilled_siblings else None,
            siblings=unfulfilled_siblings if unfulfilled_siblings else None,
        )

    results["any_flagged"] = any(
        (results[k] or {}).get("flagged", False)
        for k in ("verification", "spend_spike", "combine", "signature")
    )

    # Easter egg — assign if no holds, OR if the only flags are combine/signature
    # (combine is a shipping optimization, signature is a shipping instruction — neither is a trust issue)
    has_verification = (results["verification"] or {}).get("flagged", False)
    has_spike = (results["spend_spike"] or {}).get("flagged", False)
    only_benign_flags = results["any_flagged"] and not has_verification and not has_spike
    if (not results["any_flagged"] or only_benign_flags) and customer_gid and customer_email:
        egg = assign_easter_egg(
            order_gid=order_gid,
            order_name=order_name,
            order_total=order_total,
            customer_gid=customer_gid,
            email=customer_email,
        )
        results["easter_egg"] = egg

    return results


def _check_signature(order_gid, order_total, combined_total=None, siblings=None):
    needs_sig = order_total >= SIGNATURE_THRESHOLD
    combined_sig = False
    if not needs_sig and combined_total is not None:
        combined_sig = combined_total >= SIGNATURE_THRESHOLD

    if not needs_sig and not combined_sig:
        return {"flagged": False, "reason": "below_signature_threshold",
                "order_total": order_total, "combined_total": combined_total}

    note = "✍️ Signature Required"

    try:
        _add_order_note(order_gid, note)
    except Exception as e:
        print(f"[screening] Failed to add signature note to {order_gid}: {e}", flush=True)

    if combined_sig and siblings:
        for s in siblings:
            try:
                _add_order_note(s["order_gid"], "✍️ Signature Required")
            except Exception as e:
                print(f"[screening] Failed to note sibling signature {s['order_gid']}: {e}", flush=True)

    return {"flagged": True,
            "reason": "signature_combined" if combined_sig else "signature_required",
            "order_total": order_total, "combined_total": combined_total}


# ═══════════════════════════════════════════════════════════════════════
#  SHOPIFY FRAUD RISK
# ═══════════════════════════════════════════════════════════════════════

def check_fraud_risk(order_gid: str) -> dict:
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    customer = order.get("customer") or {}

    risk = order.get("risk") or {}
    risk_level = None
    recommendation = (risk.get("recommendation") or "").upper()
    if recommendation == "CANCEL":
        risk_level = "HIGH"
    elif recommendation == "INVESTIGATE":
        risk_level = "MEDIUM"
    else:
        for a in (risk.get("assessments") or []):
            level = (a.get("riskLevel") or "").upper()
            if level == "HIGH":
                risk_level = "HIGH"
                break
            elif level == "MEDIUM" and risk_level != "HIGH":
                risk_level = "MEDIUM"

    if risk_level not in ("MEDIUM", "HIGH"):
        return {"flagged": False, "reason": "low_or_no_risk", "risk_level": risk_level}

    customer_gid = customer.get("id")
    order_name = order.get("name", "?")
    email = (customer.get("email") or "").strip()
    total = float(order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))

    if risk_level == "HIGH":
        note_text = "🚨 High Fraud Risk — Auto-cancelled"
        try: shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["fraud-high", "auto-cancelled"]})
        except Exception as e: print(f"[screening] Tag failed: {e}", flush=True)
        if customer_gid:
            try: shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["fraud-high"]})
            except Exception as e: print(f"[screening] Tag failed: {e}", flush=True)
        try: _add_order_note(order_gid, note_text)
        except Exception as e: print(f"[screening] Note failed: {e}", flush=True)
        try: _cancel_order(order_gid, staff_note="Auto-cancelled: Shopify high fraud risk", notify_customer=False)
        except Exception as e: print(f"[screening] Cancel failed: {e}", flush=True)
        return {"flagged": True, "reason": "fraud_high_auto_cancelled",
                "risk_level": risk_level, "order_name": order_name, "note": note_text}

    # MEDIUM
    note_text = "🚨 Medium Fraud Verification"
    try: shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["fraud-medium", "hold-for-review"]})
    except Exception as e: print(f"[screening] Tag failed: {e}", flush=True)
    if customer_gid:
        try: shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["fraud-medium"]})
        except Exception as e: print(f"[screening] Tag failed: {e}", flush=True)
    try: _add_order_note(order_gid, note_text)
    except Exception as e: print(f"[screening] Note failed: {e}", flush=True)
    try: _hold_fulfillment(order_gid, "Medium fraud risk — verify before fulfilling")
    except Exception as e: print(f"[screening] Hold failed: {e}", flush=True)

    # Only set Klaviyo fraud props if no higher-priority verification already exists
    # ($1000/$700 verification emails take precedence over medium fraud)
    order_tags = set(t.lower() for t in (order.get("tags") or []))
    has_verification = bool(order_tags & {"high-value-tier1", "high-value-tier2", "spend-spike-review"})
    if customer_gid and email and not has_verification:
        try:
            external_id = customer_gid.split("/")[-1]
            upsert_profile(email=email, external_id=external_id, properties={
                "fraud_verification_required": True,
                "fraud_verification_order": order_name,
                "fraud_verification_amount": total,
                "fraud_verification_requested_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e: print(f"[screening] Klaviyo failed: {e}", flush=True)
    elif has_verification:
        print(f"[screening] Skipping fraud Klaviyo for {order_name} — verification already active", flush=True)
    return {"flagged": True, "reason": "fraud_medium_verification",
            "risk_level": risk_level, "order_name": order_name, "note": note_text}

# ═══════════════════════════════════════════════════════════════════════
#  EVENT: ORDER CANCELLED
# ═══════════════════════════════════════════════════════════════════════

def on_order_cancelled(order_gid: str) -> dict:
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    customer = order.get("customer") or {}
    order_tags = set(order.get("tags") or [])
    customer_gid = customer.get("id")
    order_name = order.get("name", "?")
    email = (customer.get("email") or "").strip()

    result = {"order_gid": order_gid, "order_name": order_name,
              "firsttime5_abuse": False, "klaviyo_set": False, "verification_cleared": False}

    # If this was a FIRSTTIME5 abuse cancellation, set the abuse properties
    if "FIRSTTIME5-review" in order_tags:
        result["firsttime5_abuse"] = True
        if customer_gid and email:
            try:
                external_id = customer_gid.split("/")[-1]
                upsert_profile(email=email, external_id=external_id, properties={
                    "firsttime5_abuse_confirmed": True,
                    "firsttime5_abuse_order": order_name,
                    "firsttime5_abuse_confirmed_at": datetime.now(timezone.utc).isoformat(),
                })
                result["klaviyo_set"] = True
            except Exception as e:
                print(f"[screening] Klaviyo abuse flag failed: {e}", flush=True)
                result["klaviyo_error"] = str(e)

    # If this order had any verification tags, clear Klaviyo so next order re-triggers
    verification_tags = {"high-value-tier1", "high-value-tier2", "spend-spike-review", "fraud-medium"}
    if order_tags & verification_tags:
        if customer_gid and email:
            try:
                external_id = customer_gid.split("/")[-1]
                upsert_profile(email=email, external_id=external_id, properties={
                    "id_verification_required": False,
                    "id_selfie_required": False,
                    "cumulative_verification_required": False,
                    "spend_spike_verification_required": False,
                    "fraud_verification_required": False,
                    "verification_cleared_at": datetime.now(timezone.utc).isoformat(),
                })
                result["verification_cleared"] = True
            except Exception as e:
                print(f"[screening] Klaviyo verification clear failed: {e}", flush=True)

    if not result["firsttime5_abuse"] and not result["verification_cleared"]:
        result["reason"] = "no_screening_tags"

    # Easter egg — return slot if this order had one
    from db import query_one, execute
    egg_row = query_one(
        "SELECT tier FROM easter_egg_pool WHERE claimed_by_order = %s",
        (order_gid,)
    )
    if egg_row:
        tier = egg_row["tier"]
        tag = TIER_TO_TAG[tier]
        execute("""
            UPDATE easter_egg_pool
            SET claimed_by_order = NULL,
                claimed_by_email = NULL,
                claimed_at       = NULL
            WHERE claimed_by_order = %s
        """, (order_gid,))
        if customer_gid:
            try:
                shopify_gql("""
                    mutation TagsRemove($id: ID!, $tags: [String!]!) {
                      tagsRemove(id: $id, tags: $tags) {
                        node { ... on Customer { id tags } }
                        userErrors { field message }
                      }
                    }
                """, {"id": customer_gid, "tags": [tag]})
            except Exception as e:
                print(f"[easter_egg] Tag removal failed on cancel for {customer_gid}: {e}", flush=True)
        result["easter_egg_returned"] = tier
        print(f"[easter_egg] Slot returned for cancelled order={order_name} tier={tier}", flush=True)

    return result

# ═══════════════════════════════════════════════════════════════════════
#  EVENT: ORDER FULFILLED → cleanup
# ═══════════════════════════════════════════════════════════════════════

SCREENING_ORDER_TAGS = [
    "hold-for-review", "FIRSTTIME5-review",
    "high-value-tier1", "high-value-tier2",
    "spend-spike-review", "fraud-medium",
    "customer-hold",
]

def on_order_fulfilled(order_gid: str) -> dict:
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    order_tags = set(order.get("tags") or [])

    tags_to_remove = [t for t in SCREENING_ORDER_TAGS if t in order_tags]
    result = {"order_gid": order_gid, "tags_removed": [], "holds_released": 0}

    if not tags_to_remove:
        result["reason"] = "no_screening_tags"
        return result

    try:
        shopify_gql(ORDER_TAGS_REMOVE, {"id": order_gid, "tags": tags_to_remove})
        result["tags_removed"] = tags_to_remove
    except Exception as e:
        print(f"[screening] Failed to remove tags from {order_gid}: {e}", flush=True)

    try:
        result["holds_released"] = _release_fulfillment_holds(order_gid)
    except Exception as e:
        print(f"[screening] Failed to release holds for {order_gid}: {e}", flush=True)

    # Clear screening notes from the order
    try:
        existing_note = (order.get("note") or "")
        if existing_note:
            cleaned_lines = [
                l for l in existing_note.split("\n")
                if "waiting on id verification" not in l.lower()
            ]
            cleaned_note = "\n".join(cleaned_lines).strip()
            # Collapse leftover separators
            while "\n---\n\n---\n" in cleaned_note:
                cleaned_note = cleaned_note.replace("\n---\n\n---\n", "\n---\n")
            cleaned_note = cleaned_note.strip().strip("-").strip()
            if cleaned_note != existing_note.strip():
                shopify_gql(ORDER_UPDATE_NOTE, {"input": {"id": order_gid, "note": cleaned_note}})
                result["note_cleared"] = True
    except Exception as e:
        print(f"[screening] Failed to clear note from {order_gid}: {e}", flush=True)

    customer = order.get("customer") or {}
    customer_gid = customer.get("id")
    email = (customer.get("email") or "").strip()
    if customer_gid and email:
        try:
            external_id = customer_gid.split("/")[-1]
            upsert_profile(email=email, external_id=external_id, properties={
                "id_verification_required": False,
                "id_selfie_required": False,
                "cumulative_verification_required": False,
                "spend_spike_verification_required": False,
                "fraud_verification_required": False,
                "verification_cleared_at": datetime.now(timezone.utc).isoformat(),
            })
            result["klaviyo_cleared"] = True
        except Exception as e:
            print(f"[screening] Klaviyo clear failed: {e}", flush=True)

    # Easter egg — assign for verification/fraud orders now cleared
    # Skip if the order is cancelled (api_cancel_order calls on_order_fulfilled for tag cleanup)
    financial = (order.get("displayFinancialStatus") or "").upper()
    if financial not in ("REFUNDED", "VOIDED"):
        order_name = order.get("name", "?")
        cust_email = (customer.get("email") or "").strip()
        cust_gid = customer.get("id")
        o_total = float(order.get("currentTotalPriceSet", {})
                        .get("shopMoney", {}).get("amount", 0))
        if cust_email and cust_gid:
            egg = assign_easter_egg(
                order_gid=order_gid,
                order_name=order_name,
                order_total=o_total,
                customer_gid=cust_gid,
                email=cust_email,
            )
            result["easter_egg"] = egg

    return result

# ═══════════════════════════════════════════════════════════════════════
#  EASTER EGG PROMO
# ═══════════════════════════════════════════════════════════════════════

TIER_TO_TAG = {
    "stink":  "STINK_EGG",
    "bronze": "BRONZE_EGG",
    "silver": "SILVER_EGG",
    "gold":   "GOLDEN_EGG",
}


def _order_has_collection_box(order_gid: str) -> bool:
    """Returns True if any line item's product has the 'collection box' tag."""
    if not EASTER_EGG_PRODUCT_TAG:
        print("[easter_egg] EASTER_EGG_PRODUCT_TAG not set — skipping tag check", flush=True)
        return False

    target_tag = EASTER_EGG_PRODUCT_TAG.lower()

    q = """
    query OrderLineItems($id: ID!) {
      order(id: $id) {
        lineItems(first: 50) {
          edges {
            node {
              product { tags }
            }
          }
        }
      }
    }
    """
    data = shopify_gql(q, {"id": order_gid})
    edges = (data.get("data", {}).get("order", {})
                 .get("lineItems", {}).get("edges", []))
    for edge in edges:
        product = (edge.get("node") or {}).get("product") or {}
        tags = [t.lower() for t in (product.get("tags") or [])]
        if target_tag in tags:
            return True
    return False


def _log_egg_result(order_gid, order_name, email, order_total,
                    has_box, eligible, ineligible_reason, tier, was_live):
    try:
        from db import execute
        execute("""
            INSERT INTO easter_egg_log
                (order_gid, order_name, customer_email, order_total,
                 has_collection_box, eligible, ineligible_reason, tier, was_live)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (order_gid, order_name, email, order_total,
              has_box, eligible, ineligible_reason, tier, was_live))
    except Exception as e:
        print(f"[easter_egg] Log write failed: {e}", flush=True)


def assign_easter_egg(order_gid, order_name, order_total,
                      customer_gid, email, force_live=False) -> dict:
    """
    Attempt to assign an easter egg to a qualifying order.
    Safe to call multiple times — checks for prior assignment.
    force_live=True bypasses spend/collection/active checks (manual assign).
    Returns dict with keys: skipped (bool), reason (str), tier (str|None)
    """
    result = {"skipped": True, "reason": None, "tier": None}
    was_live = EASTER_EGG_ACTIVE or force_live

    if not force_live:
        # Check spend threshold
        if order_total < EASTER_EGG_MIN_SPEND:
            _log_egg_result(order_gid, order_name, email, order_total,
                            None, False, "below_min_spend", None, was_live)
            result["reason"] = f"below_min_spend ({order_total} < {EASTER_EGG_MIN_SPEND})"
            return result

        # Check collection box
        has_box = _order_has_collection_box(order_gid)
        if not has_box:
            _log_egg_result(order_gid, order_name, email, order_total,
                            False, False, "no_collection_box", None, was_live)
            result["reason"] = "no_collection_box"
            return result

    if not customer_gid or not email:
        _log_egg_result(order_gid, order_name, email, order_total,
                        True, False, "no_customer", None, was_live)
        result["reason"] = "no_customer"
        return result

    from db import query_one, execute_returning

    # One egg per customer
    existing = query_one(
        "SELECT tier FROM easter_egg_pool WHERE claimed_by_email = %s",
        (email,)
    )
    if existing:
        _log_egg_result(order_gid, order_name, email, order_total,
                        True, False, "already_assigned_this_customer",
                        existing["tier"], was_live)
        result["skipped"] = True
        result["reason"] = "already_assigned_this_customer"
        result["tier"] = existing["tier"]
        return result

    # ── DRY RUN (promo inactive) ──────────────────────────────────────
    if not EASTER_EGG_ACTIVE and not force_live:
        next_row = query_one(
            "SELECT tier FROM easter_egg_pool "
            "WHERE claimed_by_order IS NULL ORDER BY id ASC LIMIT 1"
        )
        simulated_tier = next_row["tier"] if next_row else "pool_empty"
        _log_egg_result(order_gid, order_name, email, order_total,
                        True, True, None, simulated_tier, False)
        result["reason"] = "promo_inactive_simulated"
        result["tier"] = simulated_tier
        result["simulated"] = True
        return result

    # ── LIVE ASSIGNMENT ───────────────────────────────────────────────
    row = execute_returning("""
        UPDATE easter_egg_pool
        SET claimed_by_order = %s,
            claimed_by_email = %s,
            claimed_at       = NOW()
        WHERE id = (
            SELECT id FROM easter_egg_pool
            WHERE claimed_by_order IS NULL
            ORDER BY id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING tier, id
    """, (order_gid, email))

    if not row:
        _log_egg_result(order_gid, order_name, email, order_total,
                        True, False, "pool_exhausted", None, True)
        result["reason"] = "pool_exhausted"
        return result

    tier = row["tier"]
    tag = TIER_TO_TAG[tier]
    external_id = customer_gid.split("/")[-1]

    _log_egg_result(order_gid, order_name, email, order_total,
                    True, True, None, tier, True)

    try:
        shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": [tag]})
    except Exception as e:
        print(f"[easter_egg] Shopify tag failed for {customer_gid}: {e}", flush=True)
        result["shopify_tag_error"] = str(e)

    try:
        upsert_profile(email=email, external_id=external_id, properties={
            "easter_egg_tier":       tier,
            "easter_egg_tag":        tag,
            "easter_egg_order":      order_name,
            "easter_egg_granted_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        print(f"[easter_egg] Klaviyo upsert failed for {email}: {e}", flush=True)
        result["klaviyo_error"] = str(e)

    result["skipped"] = False
    result["reason"] = "assigned"
    result["tier"] = tier
    print(f"[easter_egg] Assigned {tier} to order={order_name} email={email}", flush=True)
    return result


# ═══════════════════════════════════════════════════════════════════════
#  ENTRY POINTS (called by routes)
# ═══════════════════════════════════════════════════════════════════════

def screen_order(order_gid: str) -> dict:
    """First-time only: FIRSTTIME5 abuse check."""
    results = {"order_gid": order_gid, "firsttime5": None}
    try:
        results["firsttime5"] = check_firsttime5_abuse(order_gid)
    except Exception as e:
        print(f"[screening] FIRSTTIME5 check failed for {order_gid}: {e}", flush=True)
        results["firsttime5"] = {"flagged": False, "error": str(e)}
    results["any_flagged"] = (results["firsttime5"] or {}).get("flagged", False)
    return results
