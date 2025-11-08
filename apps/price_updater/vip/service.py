# vip/service.py
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, Iterable, Optional
import os, requests, time
from dotenv import load_dotenv

load_dotenv()
# ---- CONFIG ----
TIERS = [
    ("VIP3", 2500.0),
    ("VIP2", 1250.0),
    ("VIP1",  500.0),
    ("VIP0",    0.0),
]

# Metafield actual names as created in Admin (UI forces "custom." namespace)
MF_ROLLING   = ("custom", "loyalty_rolling_spend_90d")  # number_decimal
MF_TIER      = ("custom", "loyalty_vip_tier")           # single_line_text
MF_LOCK      = ("custom", "loyalty_lock_window")        # json
MF_PROV      = ("custom", "loyalty_lock_provenance")    # json
MF_LASTCALC  = ("custom", "loyalty_last_calc_at")       # date_time

# ---- YOU MUST WIRE THESE TWO FUNCTIONS TO YOUR EXISTING SHOPIFY CLIENT ----
# Replace the bodies to call your Admin GraphQL helper. Keep signatures the same.

_SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
_SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
_GRAPHQL_ENDPOINT = f"https://{_SHOPIFY_STORE}/admin/api/2025-10/graphql.json"
_PER_CALL_TIMEOUT = int(os.environ.get("SHOPIFY_HTTP_TIMEOUT", "60"))

def gid_numeric(gid: str) -> str:
    # "gid://shopify/Customer/7836399894748" -> "7836399894748"
    return gid.rsplit("/", 1)[-1]


def shopify_gql(query: str, variables=None):
    if not _SHOPIFY_TOKEN or not _SHOPIFY_STORE:
        raise RuntimeError("Missing SHOPIFY_TOKEN or SHOPIFY_STORE in environment.")
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": _SHOPIFY_TOKEN,
    }
    payload = {"query": query, "variables": variables or {}}

    for attempt in range(6):  # ~5 retries
        try:
            resp = requests.post(
                _GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=_PER_CALL_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errors"):
                messages = " ".join(e.get("message", "") for e in data["errors"])
                # Retry on throttling or remote hiccups
                if "Throttled" in messages or "throttle" in messages.lower():
                    raise requests.HTTPError("Throttled")
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError):
            if attempt >= 5:
                raise
            # jittered backoff: 1.0s, 1.8s, 3.2s, 5.7s, 10.0s
            sleep_s = min(10.0, 1.0 * (1.8 ** attempt))
            time.sleep(sleep_s)

def shopify_metafields_set(inputs: Iterable[Dict[str, Any]]) -> None:
    """
    Convenience wrapper around metafieldsSet mutation.
    inputs: list of MetafieldsSetInput
    """
    mutation = """
    mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { id namespace key }
        userErrors { field message }
      }
    }
    """
    resp = shopify_gql(mutation, {"metafields": list(inputs)})
    errs = resp.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")

# ---- QUARTER HELPERS ----

def quarter_bounds(d: date):
    q = (d.month - 1) // 3 + 1
    start = date(d.year, 3*(q-1)+1, 1)
    if q == 4:
        end = date(d.year, 12, 31)
    else:
        end = date(d.year, 3*q+1, 1) - timedelta(days=1)
    return start, end

def current_quarter_window(today: date):
    start, end = quarter_bounds(today)
    return {"start": start.isoformat(), "end": end.isoformat()}

# ---- TIERING ----

def tier_from_spend(amount: float) -> str:
    for tag, threshold in TIERS:
        if amount >= threshold:
            return tag
    return "VIP0"

def tag_for_tier(tier: str) -> str:
    return tier  # we’ll keep tags exactly VIP0/1/2/3

# ---- DATA FETCH ----

ORDERS_FOR_CUSTOMER = """
query OrdersForCustomer($first:Int!, $after:String, $query:String!) {
  orders(first:$first, after:$after, query:$query, sortKey:CREATED_AT, reverse:true) {
    edges {
      cursor
      node {
        createdAt
        currentTotalPriceSet { shopMoney { amount } }
        totalRefundedSet { shopMoney { amount } }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def compute_rolling_90d_spend(customer_gid: str, today: Optional[datetime]=None) -> float:
    """Sum paid orders minus refunds in last 90 days (shop currency)."""
    if today is None:
        today = datetime.now(timezone.utc)
    since = today - timedelta(days=90)
    cust_legacy_id = gid_numeric(customer_gid)
    q = f'customer_id:{cust_legacy_id} financial_status:paid created_at:>="{iso_utc(since)}"'
    first = 100
    after = None
    total = 0.0
    while True:
        vars = {"first": first, "after": after, "query": q}
        data = shopify_gql(ORDERS_FOR_CUSTOMER, vars)
        orders = data["data"]["orders"]
        for edge in orders["edges"]:
            node = edge["node"]
            subtotal = float(node["currentTotalPriceSet"]["shopMoney"]["amount"])
            refunded = float(node["totalRefundedSet"]["shopMoney"]["amount"])
            total += max(0.0, subtotal - refunded)
        if not orders["pageInfo"]["hasNextPage"]:
            break
        after = orders["edges"][-1]["cursor"]
    return round(total, 2)

# ---- TAGS ----

# in service.py -> set_vip_tag
def set_vip_tag(customer_gid: str, tier: str):
    """
    Keep tags in sync with tier with minimal churn:
    - If VIP0: remove VIP and any VIP* tags.
    - If VIP1/2/3: ensure 'VIP' + specific tier exist, remove other VIP* tiers.
    """
    # read current tags once
    state = get_customer_state(customer_gid)  # has "tags"
    cur = set(state.get("tags", []) or [])
    vip_tiers = {"VIP0","VIP1","VIP2","VIP3"}

    if tier == "VIP0":
        to_remove = list(cur & vip_tiers) + (["VIP"] if "VIP" in cur else [])
        if to_remove:
            shopify_gql("""mutation($id:ID!,$tags:[String!]!){ tagsRemove(id:$id,tags:$tags){ userErrors{message}} }""",
                        {"id": customer_gid, "tags": to_remove})
        return

    # desired final set
    desired = {"VIP", tier}
    # figure diffs
    wrong_tiers = list((cur & vip_tiers) - {tier})
    missing     = list(desired - cur)

    if wrong_tiers:
        shopify_gql("""mutation($id:ID!,$tags:[String!]!){ tagsRemove(id:$id,tags:$tags){ userErrors{message}} }""",
                    {"id": customer_gid, "tags": wrong_tiers})
    if missing:
        shopify_gql("""mutation($id:ID!,$tags:[String!]!){ tagsAdd(id:$id,tags:$tags){ userErrors{message}} }""",
                    {"id": customer_gid, "tags": missing})


# ---- KLAYVIO SYNC TOUCH (force Shopify to include tags in customers/update) ----
def klaviyo_touch_tags(customer_gid: str, touch_tag: str = "_kl_sync"):
    """
    Briefly add then remove a throwaway tag so the customers/update webhook
    includes the full, current tag set. This fixes stale Klaviyo 'Shopify Tags'.
    """
    add = """
    mutation TagsAdd($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) { userErrors { message } }
    }"""
    rem = """
    mutation TagsRemove($id: ID!, $tags: [String!]!) {
      tagsRemove(id: $id, tags: $tags) { userErrors { message } }
    }"""
    try:
        shopify_gql(add, {"id": customer_gid, "tags": [touch_tag]})
        time.sleep(0.8)  # small pause so Shopify emits two distinct writes
    finally:
        shopify_gql(rem, {"id": customer_gid, "tags": [touch_tag]})

# ---- METAFIELDS UPSERT ----

def upsert_customer_metafields(customer_gid: str, updates: Dict[str, Any]):
    """
    updates: dict of { (namespace,key,type): value }
    type one of: "number_decimal","single_line_text_field","json","date_time"
    """
    inputs = []
    for (ns, key, mftype), value in updates.items():
        if value is None:
            # optionally skip clears on backfill
            continue
        # values must be strings for metafieldsSet
        if mftype == "json":
            v = json_dumps(value)
        elif mftype == "date_time":
            # expect ISO 8601 string
            v = str(value)
        else:
            v = str(value)
        inputs.append({
            "ownerId": customer_gid,
            "namespace": ns,
            "key": key,
            "type": mftype,
            "value": v,
        })
    if inputs:
        shopify_metafields_set(inputs)

def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

# ---- BACKFILL UNIT ----

def backfill_customer(customer_gid: str, today: Optional[datetime]=None) -> Dict[str, Any]:
    """
    Compute 90d spend, set tier, seed a current-quarter lock (if tier >= VIP1).
    Returns a small summary for logs.
    """
    if today is None:
        today = datetime.now(timezone.utc)
    spend = compute_rolling_90d_spend(customer_gid, today=today)
    tier = tier_from_spend(spend)

    # Seed current-quarter lock only if VIP1+
    lock = None
    if tier in ("VIP1", "VIP2", "VIP3"):
        win = current_quarter_window(today.date())
        lock = {"start": win["start"], "end": win["end"], "tier": tier}

    # Write metafields
    write_state(customer_gid, rolling=spend, tier=tier, lock=lock or {}, prov={})

    # Set tag to match
    set_vip_tag(customer_gid, tier)

    return {"customer": customer_gid, "spend90d": spend, "tier": tier, "lock": lock}

# ---- BATCHING FOR QUICK POC ----

CUSTOMERS_QUERY = """
query($first:Int!, $after:String) {
  customers(first:$first, after:$after, sortKey:ID) {
    edges { cursor node { id } }
    pageInfo { hasNextPage endCursor }
  }
}
"""

def fetch_customer_ids_page(first: int = 250, after: str | None = None):
    """Return (ids, next_cursor_or_None)."""
    data = shopify_gql(CUSTOMERS_QUERY, {"first": first, "after": after})
    cs = data["data"]["customers"]
    ids = [e["node"]["id"] for e in cs["edges"]]
    next_cursor = cs["pageInfo"]["endCursor"] if cs["pageInfo"]["hasNextPage"] else None
    return ids, next_cursor
# ---------- STATE HELPERS ----------
CUSTOMER_STATE_Q = """
query($id:ID!){
  customer(id:$id){
    id
    tags
    metafields(first:20, namespace:"custom"){
      edges{ node{ key type value } }
    }
  }
}
"""

def _mf_to_dict(edges):
    out = {}
    import json
    for e in edges or []:
        n = e["node"]
        k = n["key"]
        t = n["type"]
        v = n["value"]
        try:
            if t == "json":
                out[k] = json.loads(v) if v else {}
            elif t == "number_decimal":
                out[k] = float(v) if v not in (None, "", "null") else 0.0
            else:
                out[k] = v
        except Exception:
            out[k] = v
    return out

TIER_RANK = {"VIP0":0, "VIP1":1, "VIP2":2, "VIP3":3}
THRESH = {"VIP1":500.0, "VIP2":1250.0, "VIP3":2500.0}


def build_public_from_state(state: dict) -> dict:
    """Derive a safe, storefront-visible status from existing metafields."""
    tier = state.get("tier", "VIP0") or "VIP0"
    lock = state.get("lock") or {}
    rolling = float(state.get("rolling") or 0.0)

    thresholds = {"VIP1": 500.0, "VIP2": 1250.0, "VIP3": 2500.0}
    order = ["VIP0", "VIP1", "VIP2", "VIP3"]

    idx = order.index(tier) if tier in order else 0
    next_tier = None if tier == "VIP3" else order[idx + 1]

    # Maintain (remain at current tier) numbers — present for ALL tiers
    maintain_threshold = thresholds.get(tier, 0.0)
    maintain_remaining = max(0.0, maintain_threshold - rolling)

    base = {
        "tier": tier if next_tier else "VIP3",  # normalize VIP3
        "lock": {"end": lock.get("end")} if lock.get("end") else {},
        "maintain": {
            "threshold": maintain_threshold,
            "remaining": round(maintain_remaining, 2),
        },
    }

    if next_tier:
        target = thresholds[next_tier]
        lower = thresholds.get(tier, 0.0)
        span = max(0.0, target - lower)
        progress = 0.0 if span <= 0 else max(0.0, min(1.0, (rolling - lower) / span))
        return {
            **base,
            "next": {
                "threshold": target,
                "remaining": round(max(0.0, target - rolling), 2),
            },
            "progress": round(progress, 3),
            "progress_kind": "next",
        }
    else:
        # VIP3: show progress to maintain threshold (2,500)
        denom = thresholds["VIP3"]
        progress = 1.0 if denom <= 0 else max(0.0, min(1.0, rolling / denom))
        return {
            **base,
            "next": None,
            "progress": round(progress, 3),
            "progress_kind": "maintain",
        }

def get_customer_state(customer_gid: str):
    data = shopify_gql(CUSTOMER_STATE_Q, {"id": customer_gid})
    c = data["data"]["customer"]
    m = _mf_to_dict(c["metafields"]["edges"])
    return {
        "tags": c["tags"],
        "tier": m.get("loyalty_vip_tier", "VIP0"),
        "lock": m.get("loyalty_lock_window", {}) or {},
        "prov": m.get("loyalty_lock_provenance", {}) or {},
        "rolling": m.get("loyalty_rolling_spend_90d", 0.0),
    }

def inside_lock(lock: dict, today_date=None) -> bool:
    if not lock or "end" not in lock:
        return False
    from datetime import date
    if today_date is None:
        today_date = date.today()
    try:
        return lock["start"] <= today_date.isoformat() <= lock["end"]
    except Exception:
        return False

def current_quarter_lock_for(tier: str, today_date=None):
    from datetime import date
    if today_date is None:
        today_date = date.today()
    win = current_quarter_window(today_date)
    return {"start": win["start"], "end": win["end"], "tier": tier}

def rolling_90_lock_for(tier: str, today_date=None):
    """Return a dynamic 90-day lock window from today for the given tier."""
    from datetime import date, timedelta
    if today_date is None:
        today_date = date.today()
    start = today_date
    end = today_date + timedelta(days=90)
    return {"start": start.isoformat(), "end": end.isoformat(), "tier": tier}


def next_quarter_lock_for(tier: str, today_date=None):
    from datetime import date, timedelta
    if today_date is None:
        today_date = date.today()
    # compute next quarter bounds
    _, cur_end = quarter_bounds(today_date)
    start_next = cur_end + timedelta(days=1)
    nstart, nend = quarter_bounds(start_next)
    return {"start": nstart.isoformat(), "end": nend.isoformat(), "tier": tier}

def threshold_for_tier(tier: str) -> float:
    return THRESH.get(tier, 9e9)

# ---------- WRITE STATE ----------
def write_state(customer_gid: str, *, rolling=None, tier=None, lock=None, prov=None):
    # pull current if needed (no order math)
    if rolling is None or tier is None or lock is None:
        state = get_customer_state(customer_gid)
        if rolling is None: rolling = state["rolling"]
        if tier is None: tier = state["tier"]
        if lock is None: lock = state["lock"]

    public = build_public_from_state({"tier": tier, "lock": lock or {}, "rolling": rolling or 0.0})

    updates = {}
    if rolling is not None:
        updates[(MF_ROLLING[0], MF_ROLLING[1], "number_decimal")] = rolling
    if tier is not None:
        updates[(MF_TIER[0], MF_TIER[1], "single_line_text_field")] = tier
    if lock is not None:
        updates[(MF_LOCK[0], MF_LOCK[1], "json")] = lock
    if prov is not None:
        updates[(MF_PROV[0], MF_PROV[1], "json")] = prov
    updates[( "custom", "vip_public", "json")] = public
    updates[(MF_LASTCALC[0], MF_LASTCALC[1], "date_time")] = datetime.now(timezone.utc).isoformat()

    upsert_customer_metafields(customer_gid, updates)
    if tier is not None:
        set_vip_tag(customer_gid, tier)


# ---------- HANDLERS ----------
def on_order_paid(customer_gid: str, order_gid: str, today: Optional[datetime]=None):
    if today is None:
        today = datetime.now(timezone.utc)
    state = get_customer_state(customer_gid)
    rolling = compute_rolling_90d_spend(customer_gid, today=today)
    tier_before = state["tier"]
    tier_after = tier_from_spend(rolling)
    lock = state["lock"]

    # If they qualify for any VIP tier (same or higher), extend/refresh 90-day lock
    if tier_after in ("VIP1","VIP2","VIP3"):
        new_lock = rolling_90_lock_for(tier_after, today.date())
        prov = {
            "created_by_order_id": order_gid,
            "tier_before": tier_before,
            "tier_after": tier_after,
            "prev_lock": lock if lock else {}
        }
        write_state(customer_gid, rolling=rolling, tier=tier_after, lock=new_lock, prov=prov)
        return {"upgraded": TIER_RANK[tier_after] > TIER_RANK[tier_before],
                "tier": tier_after, "lock": new_lock}
    else:
        # dropped below VIP1 → clear lock
        write_state(customer_gid, rolling=rolling, tier="VIP0", lock={}, prov={})
        return {"upgraded": False, "tier": "VIP0", "lock": {}}


def on_refund_created(customer_gid: str, order_gid: str, today: Optional[datetime]=None):
    if today is None:
        today = datetime.now(timezone.utc)
    state = get_customer_state(customer_gid)
    rolling = compute_rolling_90d_spend(customer_gid, today=today)
    tier = state["tier"]
    lock = state["lock"]
    prov = state["prov"]

    if inside_lock(lock, today.date()):
        # causal-refund exception?
        if prov and prov.get("created_by_order_id") == order_gid:
            needed = threshold_for_tier(lock.get("tier", tier))
            if rolling < needed:
                # revoke lock: restore previous if still valid; else set from rolling
                prev = prov.get("prev_lock") or {}
                if inside_lock(prev, today.date()):
                    new_tier = prev.get("tier", tier_from_spend(rolling))
                    write_state(customer_gid, rolling=rolling, tier=new_tier, lock=prev, prov={})
                    return {"revoked": True, "tier": new_tier, "lock": prev}
                else:
                    new_tier = tier_from_spend(rolling)
                    new_lock = current_quarter_lock_for(new_tier, today.date()) if new_tier in ("VIP1","VIP2","VIP3") else {}
                    write_state(customer_gid, rolling=rolling, tier=new_tier, lock=new_lock, prov={})
                    return {"revoked": True, "tier": new_tier, "lock": new_lock}
        # normal refund inside lock → no downgrade
        write_state(customer_gid, rolling=rolling)
        return {"revoked": False, "tier": tier, "lock": lock}

    # lock not active → recompute baseline
    # if refund drops them below threshold, recompute tier/lock normally
    new_tier = tier_from_spend(rolling)
    if new_tier in ("VIP1", "VIP2", "VIP3"):
        # still qualifies → refresh rolling 90d lock
        new_lock = rolling_90_lock_for(new_tier, today.date())
    else:
        new_lock = {}
    write_state(customer_gid, rolling=rolling, tier=new_tier, lock=new_lock, prov={})
    return {"revoked": False, "tier": new_tier, "lock": new_lock}



