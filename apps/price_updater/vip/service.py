# vip/service.py
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Any, Iterable, Optional
import os, requests, time
from dotenv import load_dotenv
import json

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

VIP_DEBUG = os.getenv("VIP_DEBUG") == "1"
VIP_DRY_RUN = os.getenv("VIP_DRY_RUN") == "1"   # ← NEW
ORDER = ["VIP0","VIP1","VIP2","VIP3"]
TIER_MIN_CENTS = {"VIP0": 0, "VIP1": 50_000, "VIP2": 125_000, "VIP3": 250_000}

def _numeric_id_from_gid(gid: str) -> str:
    return gid.rsplit('/', 1)[-1]

def _pick_lock_until(lock: dict) -> str | None:
    if not lock:
        return None
    for k in ("end", "until", "expires", "expiry", "expiry_date"):
        v = lock.get(k)
        if v:
            return v.split("T")[0]
    return None

def _days_to_date(yyyymmdd: str | None, today: date) -> int:
    if not yyyymmdd:
        return 0
    try:
        d = date.fromisoformat(yyyymmdd)
        return max(0, (d - today).days)
    except Exception:
        return 0

def _gap_to_next_tier_cents(tier: str, rolling_cents: int) -> int:
    i = ORDER.index(tier)
    if i == len(ORDER) - 1:
        return 0
    nxt = ORDER[i + 1]
    return max(0, TIER_MIN_CENTS[nxt] - rolling_cents)

def _gap_to_requalify_cents(tier: str, rolling_cents: int) -> int:
    # spend needed to KEEP current tier during lock
    return max(0, TIER_MIN_CENTS.get(tier, 0) - rolling_cents)
def dlog(*args):
    if VIP_DEBUG:
        print(*args, flush=True)
def gid_numeric(gid: str) -> str:
    # "gid://shopify/Customer/7836399894748" -> "7836399894748"
    return gid.rsplit("/", 1)[-1]

def normalize_tier(t):
    """
    Coerce tier to one of 'VIP0','VIP1','VIP2','VIP3'.
    Accepts str ('vip3'/'VIP 3'), int/float (3, 3.0), or dict {'tier': 'VIP3'}.
    Fallback to 'VIP0' on anything else.
    """
    if t is None:
        return None
    if isinstance(t, dict):
        # common leak: passing the whole 'public' or state object
        if "tier" in t:
            return normalize_tier(t["tier"])
    if isinstance(t, (int, float)):
        i = int(t)
        return f"VIP{i}" if 0 <= i <= 3 else "VIP0"
    if isinstance(t, str):
        s = t.strip().upper().replace(" ", "")
        # allow 'VIP3' or 'VIP' + digits
        if s in {"VIP0","VIP1","VIP2","VIP3"}:
            return s
        if s.startswith("VIP") and s[3:].isdigit():
            i = int(s[3:])
            return f"VIP{i}" if 0 <= i <= 3 else "VIP0"
    return "VIP0"

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
_ORIG_SHOPIFY_GQL = shopify_gql  # keep a handle to the original

def shopify_gql(query: str, variables=None):
    qline = (query or "").strip().splitlines()[0]
    dlog(f"[GQL] {qline} vars={variables}")
    if VIP_DRY_RUN:
        dlog("[GQL] DRY_RUN → skipped")
        return {}
    t0 = time.time()
    resp = _ORIG_SHOPIFY_GQL(query, variables)
    dlog(f"[GQL] ok in {time.time()-t0:.3f}s")
    return resp
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
    tier = normalize_tier(tier) or "VIP0"

    # read current
    state = get_customer_state(customer_gid)
    cur = set(state.get("tags", []) or [])
    vip_tiers = {"VIP0","VIP1","VIP2","VIP3"}

    dlog(f"[VIP] {customer_gid} set_vip_tag tier={tier} cur={sorted(cur)}")

    if tier == "VIP0":
        to_remove = list((cur & vip_tiers) | ({"VIP"} if "VIP" in cur else set()))
        dlog(f"[VIP] {customer_gid} VIP0 → remove={to_remove}")
        if to_remove:
            shopify_gql("""mutation($id:ID!,$tags:[String!]!){
              tagsRemove(id:$id,tags:$tags){ userErrors{message} } }""",
              {"id": customer_gid, "tags": to_remove})
        after = set((get_customer_state(customer_gid).get("tags") or []))
        dlog(f"[VIP] {customer_gid} AFTER VIP0 cur={sorted(after)}")
        return

    # desired for VIP1..VIP3
    desired = {tier}
    wrong_tiers = list((cur & vip_tiers) - {tier})
    missing     = list(desired - cur)

    # AUDIT: never remove the target tier itself
    if tier in wrong_tiers:
        print(f"!!! ALERT: {customer_gid} WRONG REMOVAL planned: removing {tier} while tier={tier}", flush=True)

    dlog(f"[VIP] {customer_gid} desired={sorted(desired)} wrong={wrong_tiers} missing={missing}")

    if wrong_tiers:
        shopify_gql("""mutation($id:ID!,$tags:[String!]!){
          tagsRemove(id:$id,tags:$tags){ userErrors{message} } }""",
          {"id": customer_gid, "tags": wrong_tiers})

    if missing:
        shopify_gql("""mutation($id:ID!,$tags:[String!]!){
          tagsAdd(id:$id,tags:$tags){ userErrors{message} } }""",
          {"id": customer_gid, "tags": missing})

    # verify final
    after = set((get_customer_state(customer_gid).get("tags") or []))
    dlog(f"[VIP] {customer_gid} FINAL cur={sorted(after)} (expect ⊇ {sorted(desired)})")

    # AUDIT: if we ended without desired tags, scream
    if not desired.issubset(after):
        print(f"!!! ALERT: {customer_gid} FINAL missing {sorted(desired - after)}", flush=True)





# vip/service.py
import os, json, re, requests
from datetime import datetime, timezone



def _gid_to_numeric(gid: str) -> int:
    m = re.search(r"/Customer/(\d+)$", gid)
    if not m: raise ValueError(f"Bad gid: {gid}")
    return int(m.group(1))
def reassert_full_tags_ordered(customer_gid: str):
    state = get_customer_state(customer_gid)
    tags  = [t for t in (state.get("tags") or []) if t]

    # Priority: VIP3, VIP2, VIP1, VIP, then everything else alpha
    prio = {"VIP3": 0, "VIP2": 1, "VIP1": 2}
    def keyfunc(t: str):
        return (prio.get(t, 9), str(t))   # ALWAYS a tuple

    tags_sorted = sorted(tags, key=keyfunc)
    csv = ", ".join(tags_sorted)

    cid = gid_numeric(customer_gid)
    url = f"https://{_SHOPIFY_STORE}/admin/api/2025-10/customers/{cid}.json"
    headers = {
        "X-Shopify-Access-Token": _SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    print(f"[REASSERT-ORDER] {customer_gid} → {csv}", flush=True)
    r = requests.put(url, headers=headers, json={"customer": {"id": int(cid), "tags": csv}}, timeout=_PER_CALL_TIMEOUT)
    print(f"[REASSERT-ORDER] status={r.status_code}", flush=True)
    r.raise_for_status()

def retag_customer_tags_only(customer_gid: str) -> dict:
    """
    Tags-only normalization:
    - Read current tier from metafields
    - Remove VIP1/2/3 (and plain VIP if present)
    - Brief pause
    - Re-add only the normalized tier tag if VIP1..VIP3
    - (Optional) double-touch to push a full-tags webhook
    """
    state = get_customer_state(customer_gid)
    tier  = normalize_tier(state.get("tier") or "VIP0")

    # current tags
    cur = set(t for t in (state.get("tags") or []) if t)
    vip_family = {"VIP1","VIP2","VIP3","VIP"}

    to_remove = sorted(cur & vip_family)
    if to_remove:
        shopify_gql(
            """mutation($id:ID!,$tags:[String!]!){
               tagsRemove(id:$id,tags:$tags){ userErrors{message} } }""",
            {"id": customer_gid, "tags": to_remove}
        )

    # give Shopify → Klaviyo time to see the removal (break stale mapping)
    time.sleep(0.8)

    # add only the correct tier tag (no tier => VIP0 => add nothing)
    added = []
    if tier in {"VIP1","VIP2","VIP3"}:
        shopify_gql(
            """mutation($id:ID!,$tags:[String!]!){
               tagsAdd(id:$id,tags:$tags){ userErrors{message} } }""",
            {"id": customer_gid, "tags": [tier]}
        )
        added = [tier]

    # nudge: send a full-tags array twice (anchor add/remove) to force rebuild
    try:
        reassert_full_tags_two_step(customer_gid)
    except Exception:
        pass

    return {"customer": customer_gid, "tier": tier, "removed": to_remove, "added": added}
def reassert_full_tags_two_step(customer_gid: str):
    """
    Force two REST updates that *both* include the full tag CSV:
      1) add a one-time anchor (net change → webhook)
      2) remove the anchor (final state equals original)
    Klaviyo sees complete tag arrays both times.
    """
    state = get_customer_state(customer_gid)
    cur = [t for t in (state.get("tags") or []) if t]
    full = sorted(cur)
    base_csv = ", ".join(full)

    anchor = f"KL-TOUCH{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    csv_with_anchor = ", ".join(sorted(full + [anchor]))

    cid = gid_numeric(customer_gid)
    url = f"https://{_SHOPIFY_STORE}/admin/api/2025-10/customers/{cid}.json"
    headers = {
        "X-Shopify-Access-Token": _SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Step 1: add anchor (full CSV)
    print(f"[REASSERT1] gid={customer_gid} → {csv_with_anchor}", flush=True)
    r1 = requests.put(url, headers=headers, json={"customer": {"id": int(cid), "tags": csv_with_anchor}}, timeout=_PER_CALL_TIMEOUT)
    print(f"[REASSERT1] status={r1.status_code}", flush=True)
    if r1.status_code >= 400:
        print(f"[REASSERT1] ERR {r1.text[:300]}", flush=True)
        r1.raise_for_status()

    # brief pause so the first webhook can propagate
    time.sleep(1.2)

    # Step 2: remove anchor (full CSV back to original)
    print(f"[REASSERT2] gid={customer_gid} → {base_csv}", flush=True)
    r2 = requests.put(url, headers=headers, json={"customer": {"id": int(cid), "tags": base_csv}}, timeout=_PER_CALL_TIMEOUT)
    print(f"[REASSERT2] status={r2.status_code}", flush=True)
    if r2.status_code >= 400:
        print(f"[REASSERT2] ERR {r2.text[:300]}", flush=True)
        r2.raise_for_status()
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

def get_customer_lifetime_spend(customer_gid: str) -> float:
    """
    Return Shopify's lifetime total_spent for a customer (shop currency, dollars).
    """
    cid = gid_numeric(customer_gid)
    url = f"https://{_SHOPIFY_STORE}/admin/api/2025-10/customers/{cid}.json"
    headers = {
        "X-Shopify-Access-Token": _SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    r = requests.get(url, headers=headers, timeout=_PER_CALL_TIMEOUT)
    r.raise_for_status()
    data = r.json() or {}
    cust = data.get("customer") or {}
    raw = cust.get("total_spent") or "0"
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


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
    email
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
        "email": c.get("email"),
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
    if rolling is None or tier is None or lock is None:
        state = get_customer_state(customer_gid)
        if rolling is None: rolling = state["rolling"]
        if tier is None: tier = state["tier"]
        if lock is None: lock = state["lock"]

    norm_tier = normalize_tier(tier) if tier is not None else None
    public = build_public_from_state({"tier": norm_tier, "lock": lock or {}, "rolling": rolling or 0.0})

    if isinstance(prov, str):
        prov = {"source": prov}
    elif prov is not None and not isinstance(prov, dict):
        prov = {}

    updates = {}
    if rolling is not None:
        updates[(MF_ROLLING[0], MF_ROLLING[1], "number_decimal")] = float(rolling)
    if norm_tier is not None:
        updates[(MF_TIER[0], MF_TIER[1], "single_line_text_field")] = norm_tier
    if lock is not None:
        updates[(MF_LOCK[0], MF_LOCK[1], "json")] = lock
    if prov is not None:
        updates[(MF_PROV[0], MF_PROV[1], "json")] = prov
    updates[("custom", "vip_public", "json")] = public
    updates[(MF_LASTCALC[0], MF_LASTCALC[1], "date_time")] = datetime.now(timezone.utc).isoformat()

    upsert_customer_metafields(customer_gid, updates)

    # IMPORTANT: tag using the normalized tier
    print(f"[vip] {customer_gid} tier_in={tier!r} -> tier_norm={norm_tier}")
    if norm_tier is not None:
        set_vip_tag(customer_gid, norm_tier)



# ---------- HANDLERS ----------
def on_order_paid(customer_gid: str, order_gid: str, today: Optional[datetime] = None):
    if today is None:
        today = datetime.now(timezone.utc)

    state = get_customer_state(customer_gid)
    rolling = compute_rolling_90d_spend(customer_gid, today=today)

    tier_before = state.get("tier") or "VIP0"
    lock        = state.get("lock") or {}
    tier_after  = tier_from_spend(rolling)

    today_date = today.date()

    before_rank = TIER_RANK.get(tier_before, 0)
    after_rank  = TIER_RANK.get(tier_after, 0)

    # --- INSIDE A LOCK: only extend/upgrade, never downgrade ---
    if inside_lock(lock, today_date):
        if tier_after in ("VIP1", "VIP2", "VIP3") and after_rank >= before_rank:
            # Same or higher tier → extend/upgrade the rolling 90-day lock
            new_lock = rolling_90_lock_for(tier_after, today_date)
            prov = {
                "created_by_order_id": order_gid,
                "tier_before": tier_before,
                "tier_after": tier_after,
                "prev_lock": lock if lock else {},
                "reason": "extend_or_upgrade_inside_lock",
            }
            write_state(
                customer_gid,
                rolling=rolling,
                tier=tier_after,
                lock=new_lock,
                prov=prov,
            )
            return {
                "upgraded": after_rank > before_rank,
                "tier": tier_after,
                "lock": new_lock,
            }

        # Would be a downgrade (or VIP0) → keep tier + lock, just update rolling
        write_state(customer_gid, rolling=rolling)
        return {
            "upgraded": False,
            "tier": tier_before,
            "lock": lock,
        }

    # --- OUTSIDE A LOCK: keep your original behavior ---
    if tier_after in ("VIP1", "VIP2", "VIP3"):
        new_lock = rolling_90_lock_for(tier_after, today_date)
        prov = {
            "created_by_order_id": order_gid,
            "tier_before": tier_before,
            "tier_after": tier_after,
            "prev_lock": lock if lock else {},
        }
        write_state(
            customer_gid,
            rolling=rolling,
            tier=tier_after,
            lock=new_lock,
            prov=prov,
        )
        return {
            "upgraded": after_rank > before_rank,
            "tier": tier_after,
            "lock": new_lock,
        }
    else:
        # dropped below VIP1 with no active lock → clear lock and tier
        write_state(customer_gid, rolling=rolling, tier="VIP0", lock={}, prov={})
        return {
            "upgraded": False,
            "tier": "VIP0",
            "lock": {},
        }



def on_refund_created(customer_gid: str, order_gid: str, today: Optional[datetime]=None):
    if today is None:
        today = datetime.now(timezone.utc)
    state = get_customer_state(customer_gid)
    rolling = compute_rolling_90d_spend(customer_gid, today=today)
    tier = state["tier"]
    lock = state["lock"]
    prov = state["prov"]
    if not isinstance(prov, dict):
        prov = {}

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



