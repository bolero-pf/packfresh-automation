# vip/service.py
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, Iterable, Optional
import os, requests
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
    resp = requests.post(_GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # Hard errors
    if data.get("errors"):
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data

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

def set_vip_tag(customer_gid: str, tier: str):
    """
    Maintain tags:
    - VIP1+/VIP2/VIP3: add both a generic 'VIP' and the specific 'VIPx'
    - VIP0: remove any VIP-related tags entirely
    """
    # fetch current tags
    q = """query($id: ID!) { customer(id:$id){ id tags } }"""
    cur = shopify_gql(q, {"id": customer_gid})
    tags = cur["data"]["customer"]["tags"] or []

    # compute removals: any VIP*, plus generic VIP
    to_remove = [t for t in tags if t == "VIP" or t.startswith("VIP")]
    if to_remove:
        rem = """
        mutation TagsRemove($id: ID!, $tags: [String!]!) {
          tagsRemove(id: $id, tags: $tags) { userErrors { message } }
        }"""
        shopify_gql(rem, {"id": customer_gid, "tags": to_remove})

    # add back as needed
    add = """
    mutation TagsAdd($id: ID!, $tags: [String!]!) {
      tagsAdd(id: $id, tags: $tags) { userErrors { message } }
    }"""

    if tier in ("VIP1","VIP2","VIP3"):
        # add generic VIP + specific tier tag
        shopify_gql(add, {"id": customer_gid, "tags": ["VIP", tier]})
    else:
        # VIP0 → add nothing
        pass


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
    upsert_customer_metafields(customer_gid, {
        (MF_ROLLING[0],  MF_ROLLING[1],  "number_decimal"): spend,
        (MF_TIER[0],     MF_TIER[1],     "single_line_text_field"): tier,
        (MF_LOCK[0],     MF_LOCK[1],     "json"): lock or {},
        (MF_PROV[0],     MF_PROV[1],     "json"): {},  # start clean
        (MF_LASTCALC[0], MF_LASTCALC[1], "date_time"): datetime.now(timezone.utc).isoformat(),
    })

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
    updates = {}
    if rolling is not None:
        updates[(MF_ROLLING[0], MF_ROLLING[1], "number_decimal")] = rolling
    if tier is not None:
        updates[(MF_TIER[0], MF_TIER[1], "single_line_text_field")] = tier
    if lock is not None:
        updates[(MF_LOCK[0], MF_LOCK[1], "json")] = lock
    if prov is not None:
        updates[(MF_PROV[0], MF_PROV[1], "json")] = prov
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

    # Upgrade only if rank increases
    if TIER_RANK[tier_after] > TIER_RANK[tier_before]:
        new_lock = next_quarter_lock_for(tier_after, today.date())
        prov = {
            "created_by_order_id": order_gid,
            "tier_before": tier_before,
            "tier_after": tier_after,
            "prev_lock": lock if lock else {}
        }
        write_state(customer_gid, rolling=rolling, tier=tier_after, lock=new_lock, prov=prov)
        return {"upgraded": True, "tier": tier_after, "lock": new_lock}
    else:
        # no change; just refresh rolling + timestamp
        write_state(customer_gid, rolling=rolling)
        return {"upgraded": False, "tier": tier_before, "lock": lock}

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
    new_tier = tier_from_spend(rolling)
    new_lock = current_quarter_lock_for(new_tier, today.date()) if new_tier in ("VIP1","VIP2","VIP3") else {}
    write_state(customer_gid, rolling=rolling, tier=new_tier, lock=new_lock, prov={})
    return {"revoked": False, "tier": new_tier, "lock": new_lock}

def on_quarter_roll(today: Optional[datetime]=None, limit: Optional[int]=None):
    if today is None:
        today = datetime.now(timezone.utc)
    cnt = 0
    for gid in iterate_customer_ids(limit=limit):
        spend = compute_rolling_90d_spend(gid, today=today)
        tier = tier_from_spend(spend)
        lock = current_quarter_lock_for(tier, today.date()) if tier in ("VIP1","VIP2","VIP3") else {}
        write_state(gid, rolling=spend, tier=tier, lock=lock, prov={})
        cnt += 1
    return {"processed": cnt}
