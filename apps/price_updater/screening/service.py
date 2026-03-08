# screening/service.py
"""
Order screening service:
  1) FIRSTTIME5 discount code abuse detection
  2) Tiered high-value first-time order verification ($700/$1000)
  3) Spend spike detection ($1000+ with small prior history)
  4) Shopify fraud risk handling (medium → verify, high → auto-cancel)
  5) Order cancelled → Klaviyo abuse notification
  6) Order fulfilled → cleanup tags/holds
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
import os, re, json, time
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ──────────────────────────────────────────────────────────────
TIER1_THRESHOLD   = float(os.environ.get("TIER1_THRESHOLD", "700.00"))   # photo ID + confirm address
TIER2_THRESHOLD   = float(os.environ.get("TIER2_THRESHOLD", "1000.00"))  # photo ID + selfie + confirm address
SPIKE_THRESHOLD   = float(os.environ.get("SPIKE_THRESHOLD", "1000.00"))  # spend spike minimum order
SPIKE_RATIO       = float(os.environ.get("SPIKE_RATIO", "0.20"))        # prev max < 20% of current
FIRSTTIME5_CODE   = os.environ.get("FIRSTTIME5_CODE", "FIRSTTIME5")

import sys, os as _os
_ROOT = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from vip.service import (
    shopify_gql, shopify_metafields_set, upsert_customer_metafields,
    gid_numeric, _SHOPIFY_TOKEN, _SHOPIFY_STORE, _PER_CALL_TIMEOUT,
)
from integrations.klaviyo import upsert_profile

# ═══════════════════════════════════════════════════════════════════════
#  GRAPHQL
# ═══════════════════════════════════════════════════════════════════════

ORDER_DETAIL_Q = """
query OrderDetail($id: ID!) {
  order(id: $id) {
    id
    name
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

# Get a customer's recent orders with totals (for spend spike detection)
CUSTOMER_ORDERS_Q = """
query CustomerOrders($customerId: ID!, $first: Int!) {
  customer(id: $customerId) {
    id
    orders(first: $first, sortKey: CREATED_AT, reverse: true) {
      edges {
        node {
          id
          name
          createdAt
          currentTotalPriceSet { shopMoney { amount } }
        }
      }
    }
  }
}
"""

# Get a customer's unfulfilled orders (for combine check)
CUSTOMER_UNFULFILLED_ORDERS_Q = """
query CustomerUnfulfilledOrders($customerId: ID!, $first: Int!) {
  customer(id: $customerId) {
    id
    orders(first: $first, sortKey: CREATED_AT, reverse: true, query: "fulfillment_status:unfulfilled") {
      edges {
        node {
          id
          name
          createdAt
          tags
          displayFulfillmentStatus
          currentTotalPriceSet { shopMoney { amount } }
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

    shipping_addresses = set()
    billing_addresses = set()
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

    email = (cust.get("email") or "").strip().lower()
    return {
        "email": email,
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

def _cancel_order(order_gid: str, staff_note: str, notify_customer: bool = False):
    shopify_gql(ORDER_CANCEL, {
        "orderId": order_gid,
        "reason": "FRAUD",
        "notifyCustomer": notify_customer,
        "refund": True,
        "restock": True,
        "staffNote": staff_note,
    })

def _get_customer_max_previous_order(customer_gid: str, current_order_gid: str) -> float:
    """Fetch the customer's previous orders and return the max total, excluding current order."""
    data = shopify_gql(CUSTOMER_ORDERS_Q, {"customerId": customer_gid, "first": 50})
    customer = data.get("data", {}).get("customer") or {}
    edges = customer.get("orders", {}).get("edges", [])

    max_total = 0.0
    for edge in edges:
        node = edge["node"]
        if node["id"] == current_order_gid:
            continue
        total = float(node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
        if total > max_total:
            max_total = total
    return max_total

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
            reasons = []
            confidence = "low"

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
                if confidence != "high":
                    confidence = "medium"
            if bill_to_ship:
                reasons.append(f"billing→prev shipping: {list(bill_to_ship)[0]}")
                if confidence != "high":
                    confidence = "medium"

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
                    "order_gid": node["id"],
                    "order_name": node.get("name", "?"),
                    "customer_gid": prev_signals["customer_gid"],
                    "email": prev_signals["email"],
                    "created_at": node.get("createdAt"),
                    "match_reasons": reasons,
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
#  CHECK 2: HIGH-VALUE FIRST-TIME ORDER (tiered)
# ═══════════════════════════════════════════════════════════════════════

def check_high_value_first_order(order_gid: str) -> dict:
    """
    Tiered verification for first-time orders:
      $700–$999:  photo ID + confirm shipping address     → id_verification_required
      $1000+:     photo ID + selfie + confirm address      → id_selfie_required
    """
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    customer = order.get("customer") or {}

    order_count = int(customer.get("numberOfOrders", 0) or 0)
    if order_count > 1:
        return {"flagged": False, "reason": "repeat_customer", "order_count": order_count}

    total = float(order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
    if total < TIER1_THRESHOLD:
        return {"flagged": False, "reason": "below_threshold", "total": total}

    customer_gid = customer.get("id")
    order_name = order.get("name", "?")
    email = (customer.get("email") or "").strip()

    # Determine tier
    if total >= TIER2_THRESHOLD:
        tier = 2
        klaviyo_prop = "id_selfie_required"
        verification_desc = "photo ID + selfie + shipping address confirmation"
        tag = "high-value-tier2"
        hold_reason = f"First order ${total:.2f} — photo ID + selfie required before fulfilling"
    else:
        tier = 1
        klaviyo_prop = "id_verification_required"
        verification_desc = "photo ID + shipping address confirmation"
        tag = "high-value-tier1"
        hold_reason = f"First order ${total:.2f} — photo ID required before fulfilling"

    note_text = (
        f"⚠️ HIGH-VALUE FIRST ORDER (Tier {tier}) — ${total:.2f}\n"
        f"Order {order_name} requires {verification_desc} before fulfilling.\n"
        f"Customer: {customer.get('firstName', '')} {customer.get('lastName', '')} ({email})"
    )

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
            upsert_profile(email=email, external_id=external_id, properties={
                klaviyo_prop: True,
                f"{klaviyo_prop}_order": order_name,
                f"{klaviyo_prop}_amount": total,
                f"{klaviyo_prop}_requested_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            print(f"[screening] Klaviyo verification flag failed for {customer_gid}: {e}", flush=True)

    if customer_gid:
        try:
            shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": [tag]})
        except Exception as e:
            print(f"[screening] Failed to tag customer {customer_gid}: {e}", flush=True)

    return {"flagged": True, "reason": f"high_value_tier{tier}", "tier": tier,
            "total": total, "order_name": order_name, "customer_gid": customer_gid, "note": note_text}

# ═══════════════════════════════════════════════════════════════════════
#  CHECK 3: SPEND SPIKE ($1000+ with small prior history)
# ═══════════════════════════════════════════════════════════════════════

def check_spend_spike(order_gid: str) -> dict:
    """
    For returning customers (ordersCount > 1):
    Flag if order >= SPIKE_THRESHOLD and their max previous order < SPIKE_RATIO of current.
    e.g. $1200 order flags if biggest prior order was < $240 (20%).
    → spend_spike_verification_required on Klaviyo
    """
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    customer = order.get("customer") or {}

    order_count = int(customer.get("numberOfOrders", 0) or 0)
    if order_count <= 1:
        return {"flagged": False, "reason": "first_time_customer"}

    total = float(order.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
    if total < SPIKE_THRESHOLD:
        return {"flagged": False, "reason": "below_spike_threshold", "total": total}

    customer_gid = customer.get("id")
    if not customer_gid:
        return {"flagged": False, "reason": "no_customer_gid"}

    order_name = order.get("name", "?")
    email = (customer.get("email") or "").strip()

    # Fetch their previous orders
    max_prev = _get_customer_max_previous_order(customer_gid, order_gid)
    spike_ceiling = total * SPIKE_RATIO

    if max_prev >= spike_ceiling:
        return {"flagged": False, "reason": "no_spike", "total": total,
                "max_previous": max_prev, "spike_ceiling": spike_ceiling}

    note_text = (
        f"⚠️ SPEND SPIKE DETECTED — ${total:.2f}\n"
        f"Order {order_name} is significantly larger than this customer's history.\n"
        f"Largest previous order: ${max_prev:.2f} (threshold: ${spike_ceiling:.2f})\n"
        f"Customer: {customer.get('firstName', '')} {customer.get('lastName', '')} ({email})\n"
        f"Requires photo ID + selfie + shipping address confirmation before fulfilling."
    )

    try:
        shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["spend-spike-review", "hold-for-review"]})
    except Exception as e:
        print(f"[screening] Failed to tag order {order_gid}: {e}", flush=True)

    try:
        _add_order_note(order_gid, note_text)
    except Exception as e:
        print(f"[screening] Failed to add note to {order_gid}: {e}", flush=True)

    try:
        _hold_fulfillment(order_gid, f"Spend spike — ${total:.2f} vs max prev ${max_prev:.2f}. Verify before fulfilling.")
    except Exception as e:
        print(f"[screening] Failed to hold fulfillment for {order_gid}: {e}", flush=True)

    if customer_gid and email:
        try:
            external_id = customer_gid.split("/")[-1]
            upsert_profile(email=email, external_id=external_id, properties={
                "spend_spike_verification_required": True,
                "spend_spike_verification_order": order_name,
                "spend_spike_verification_amount": total,
                "spend_spike_verification_max_previous": max_prev,
                "spend_spike_verification_requested_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            print(f"[screening] Klaviyo spend spike flag failed for {customer_gid}: {e}", flush=True)

    if customer_gid:
        try:
            shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["spend-spike-review"]})
        except Exception as e:
            print(f"[screening] Failed to tag customer {customer_gid}: {e}", flush=True)

    return {"flagged": True, "reason": "spend_spike", "total": total,
            "max_previous": max_prev, "spike_ceiling": spike_ceiling,
            "order_name": order_name, "note": note_text}

# ═══════════════════════════════════════════════════════════════════════
#  CHECK 4: SHOPIFY FRAUD RISK
# ═══════════════════════════════════════════════════════════════════════

def check_fraud_risk(order_gid: str) -> dict:
    """
    Called from "Order risk analyzed" Flow trigger.
    MEDIUM → hold + verification email.  HIGH → auto-cancel.
    """
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

    # ── HIGH: auto-cancel ──
    if risk_level == "HIGH":
        note_text = (
            f"🚨 HIGH FRAUD RISK — Auto-cancelled\n"
            f"Order {order_name} (${total:.2f}) was automatically cancelled due to high fraud risk.\n"
            f"Shopify's fraud analysis flagged this order as high risk."
        )

        try:
            shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["fraud-high", "auto-cancelled"]})
        except Exception as e:
            print(f"[screening] Failed to tag order {order_gid}: {e}", flush=True)

        if customer_gid:
            try:
                shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["fraud-high"]})
            except Exception as e:
                print(f"[screening] Failed to tag customer {customer_gid}: {e}", flush=True)

        try:
            _add_order_note(order_gid, note_text)
        except Exception as e:
            print(f"[screening] Failed to add note to {order_gid}: {e}", flush=True)

        try:
            _cancel_order(order_gid, staff_note="Auto-cancelled: Shopify high fraud risk", notify_customer=False)
        except Exception as e:
            print(f"[screening] Failed to cancel order {order_gid}: {e}", flush=True)

        return {"flagged": True, "reason": "fraud_high_auto_cancelled",
                "risk_level": risk_level, "order_name": order_name, "note": note_text}

    # ── MEDIUM: hold + verify ──
    note_text = (
        f"⚠️ MEDIUM FRAUD RISK — Verification required\n"
        f"Order {order_name} (${total:.2f}) was flagged with medium fraud risk.\n"
        f"Hold placed. Verify customer identity before fulfilling."
    )

    try:
        shopify_gql(ORDER_TAGS_ADD, {"id": order_gid, "tags": ["fraud-medium", "hold-for-review"]})
    except Exception as e:
        print(f"[screening] Failed to tag order {order_gid}: {e}", flush=True)

    if customer_gid:
        try:
            shopify_gql(CUSTOMER_TAGS_ADD, {"id": customer_gid, "tags": ["fraud-medium"]})
        except Exception as e:
            print(f"[screening] Failed to tag customer {customer_gid}: {e}", flush=True)

    try:
        _add_order_note(order_gid, note_text)
    except Exception as e:
        print(f"[screening] Failed to add note to {order_gid}: {e}", flush=True)

    try:
        _hold_fulfillment(order_gid, "Medium fraud risk — verify customer identity before fulfilling")
    except Exception as e:
        print(f"[screening] Failed to hold fulfillment for {order_gid}: {e}", flush=True)

    if customer_gid and email:
        try:
            external_id = customer_gid.split("/")[-1]
            upsert_profile(email=email, external_id=external_id, properties={
                "fraud_verification_required": True,
                "fraud_verification_order": order_name,
                "fraud_verification_amount": total,
                "fraud_verification_requested_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            print(f"[screening] Klaviyo fraud verification failed for {customer_gid}: {e}", flush=True)

    return {"flagged": True, "reason": "fraud_medium_verification",
            "risk_level": risk_level, "order_name": order_name, "note": note_text}

# ═══════════════════════════════════════════════════════════════════════
#  EVENT: ORDER CANCELLED → Klaviyo abuse notification
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
              "firsttime5_abuse": False, "klaviyo_set": False}

    if "FIRSTTIME5-review" not in order_tags:
        result["reason"] = "no_firsttime5_tag"
        return result

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
            print(f"[screening] Klaviyo abuse flag failed for {customer_gid}: {e}", flush=True)
            result["klaviyo_error"] = str(e)

    return result

# ═══════════════════════════════════════════════════════════════════════
#  EVENT: ORDER FULFILLED → cleanup
# ═══════════════════════════════════════════════════════════════════════

SCREENING_ORDER_TAGS = [
    "hold-for-review", "FIRSTTIME5-review",
    "high-value-tier1", "high-value-tier2",
    "spend-spike-review", "fraud-medium",
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

    # Clear ALL Klaviyo verification flags
    customer = order.get("customer") or {}
    customer_gid = customer.get("id")
    email = (customer.get("email") or "").strip()
    if customer_gid and email:
        try:
            external_id = customer_gid.split("/")[-1]
            upsert_profile(email=email, external_id=external_id, properties={
                "id_verification_required": False,
                "id_selfie_required": False,
                "spend_spike_verification_required": False,
                "fraud_verification_required": False,
                "verification_cleared_at": datetime.now(timezone.utc).isoformat(),
            })
            result["klaviyo_cleared"] = True
        except Exception as e:
            print(f"[screening] Klaviyo clear failed for {customer_gid}: {e}", flush=True)

    return result

# ═══════════════════════════════════════════════════════════════════════
#  COMBINED SCREENERS
# ═══════════════════════════════════════════════════════════════════════

def screen_order(order_gid: str) -> dict:
    """
    First-time order screening (ordersCount == 1).
    Runs FIRSTTIME5 abuse + tiered high-value checks.
    """
    results = {"order_gid": order_gid, "firsttime5": None, "high_value": None}

    try:
        results["firsttime5"] = check_firsttime5_abuse(order_gid)
    except Exception as e:
        print(f"[screening] FIRSTTIME5 check failed for {order_gid}: {e}", flush=True)
        results["firsttime5"] = {"flagged": False, "error": str(e)}

    try:
        results["high_value"] = check_high_value_first_order(order_gid)
    except Exception as e:
        print(f"[screening] High-value check failed for {order_gid}: {e}", flush=True)
        results["high_value"] = {"flagged": False, "error": str(e)}

    results["any_flagged"] = (
        (results["firsttime5"] or {}).get("flagged", False)
        or (results["high_value"] or {}).get("flagged", False)
    )
    return results

def screen_order_spike(order_gid: str) -> dict:
    """
    Returning customer screening (ordersCount > 1).
    Runs spend spike check only.
    """
    results = {"order_gid": order_gid, "spend_spike": None}

    try:
        results["spend_spike"] = check_spend_spike(order_gid)
    except Exception as e:
        print(f"[screening] Spend spike check failed for {order_gid}: {e}", flush=True)
        results["spend_spike"] = {"flagged": False, "error": str(e)}

    results["any_flagged"] = (results["spend_spike"] or {}).get("flagged", False)
    return results

# ═══════════════════════════════════════════════════════════════════════
#  CHECK 5: COMBINE ORDERS (same customer, unfulfilled)
# ═══════════════════════════════════════════════════════════════════════

# Tags that indicate an order should be skipped in the combine check
COMBINE_SKIP_TAGS = {"pre-order", "preorder", "pre_order", "hold-for-review"}

def check_combine_orders(order_gid: str) -> dict:
    """
    When a new order comes in, check if the same customer has other
    unfulfilled orders that aren't pre-orders or already on hold.
    If so, hold the new order with a note to combine shipping.
    """
    data = shopify_gql(ORDER_DETAIL_Q, {"id": order_gid})
    order = data["data"]["order"]
    customer = order.get("customer") or {}
    customer_gid = customer.get("id")

    if not customer_gid:
        return {"flagged": False, "reason": "no_customer"}

    # Fetch this customer's unfulfilled orders
    cust_data = shopify_gql(CUSTOMER_UNFULFILLED_ORDERS_Q, {
        "customerId": customer_gid, "first": 20,
    })
    cust = cust_data.get("data", {}).get("customer") or {}
    edges = cust.get("orders", {}).get("edges", [])

    # Find sibling unfulfilled orders (not this order, not pre-orders, not on hold)
    siblings = []
    for edge in edges:
        node = edge["node"]
        # Skip the current order
        if node["id"] == order_gid:
            continue

        # Skip if not truly unfulfilled
        status = (node.get("displayFulfillmentStatus") or "").upper()
        if status not in ("UNFULFILLED",):
            continue

        # Skip pre-orders and already-held orders
        order_tags = set(t.lower() for t in (node.get("tags") or []))
        if order_tags & COMBINE_SKIP_TAGS:
            continue

        total = float(node.get("currentTotalPriceSet", {}).get("shopMoney", {}).get("amount", 0))
        siblings.append({
            "order_gid": node["id"],
            "order_name": node.get("name", "?"),
            "total": total,
            "created_at": node.get("createdAt"),
        })

    if not siblings:
        return {"flagged": False, "reason": "no_siblings"}

    # Build the note
    order_name = order.get("name", "?")
    sibling_names = ", ".join(s["order_name"] for s in siblings)
    sibling_details = "; ".join(
        f"{s['order_name']} (${s['total']:.2f})" for s in siblings
    )

    note_text = (
        f"📦 COMBINE SHIPPING — {len(siblings)} other unfulfilled order(s):\n"
        f"  {sibling_details}\n"
        f"Ship this order together with the above."
    )

    hold_reason = f"Combine with {sibling_names} — same customer has unfulfilled orders"

    # Hold the new order
    try:
        _hold_fulfillment(order_gid, hold_reason)
    except Exception as e:
        print(f"[screening] Failed to hold fulfillment for combine {order_gid}: {e}", flush=True)

    # Add note to the new order
    try:
        _add_order_note(order_gid, note_text)
    except Exception as e:
        print(f"[screening] Failed to add combine note to {order_gid}: {e}", flush=True)

    # Also add a note to each sibling so fulfillment staff see it from either side
    for s in siblings:
        try:
            sib_note = (
                f"📦 NEW ORDER FROM SAME CUSTOMER — {order_name}\n"
                f"Combine shipping with {order_name}."
            )
            _add_order_note(s["order_gid"], sib_note)
        except Exception as e:
            print(f"[screening] Failed to add combine note to sibling {s['order_gid']}: {e}", flush=True)

    return {
        "flagged": True,
        "reason": "combine_orders",
        "order_name": order_name,
        "siblings": siblings,
        "note": note_text,
    }

