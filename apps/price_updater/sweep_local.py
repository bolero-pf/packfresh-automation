# run_sweep_one.py
import sys
from datetime import datetime, timezone, date

# your existing helpers
from vip.service import (
    get_customer_state, write_state,
    reassert_full_tags_ordered, reassert_full_tags_two_step,
)
from vip.routes import _push_vip_to_klaviyo


# Klaviyo helper you just added
from integrations.klaviyo import upsert_profile


# --- config for "gap to next tier" (in cents) — adjust to your real thresholds
TIER_MIN_CENTS = {"VIP0": 0, "VIP1": 50_000, "VIP2": 125_000, "VIP3": 250_000}
from datetime import date

ORDER = ["VIP0","VIP1","VIP2","VIP3"]

def _pick_lock_until(lock: dict) -> str | None:
    if not lock:
        return None
    for k in ("end","until","expires","expiry","expiry_date"):
        v = lock.get(k)
        if v:
            return v.split("T")[0]
    return None
def as_customer_gid(val: str | int) -> str:
    s = str(val)
    return s if s.startswith("gid://") else f"gid://shopify/Customer/{s}"
def _days_to_date(yyyymmdd: str | None, today: date) -> int:
    if not yyyymmdd:
        return 0
    try:
        d = date.fromisoformat(yyyymmdd)
        return max(0, (d - today).days)
    except Exception:
        return 0

def _gap_to_requalify_cents(tier: str, rolling_cents: int) -> int:
    # spend needed to KEEP current tier during a lock
    return max(0, TIER_MIN_CENTS.get(tier, 0) - rolling_cents)
def as_gid(val: str) -> str:
    return val if val.startswith("gid://shopify/Customer/") else f"gid://shopify/Customer/{val}"

def _numeric_id_from_gid(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]

def _gap_to_next_tier_cents(tier: str, rolling_cents: int) -> int:
    order = ["VIP0", "VIP1", "VIP2", "VIP3"]
    idx = order.index(tier)
    if idx == len(order) - 1:
        return 0
    next_tier = order[idx + 1]
    return max(0, TIER_MIN_CENTS[next_tier] - rolling_cents)

def _days_to_expiry(lock: dict, today: date) -> int:
    until = (lock or {}).get("until")
    if not until:
        return 0
    d = date.fromisoformat(until)
    return max(0, (d - today).days)


def run_one(customer_id: str):
    try:
        _push_vip_to_klaviyo(as_customer_gid(customer_id))
    except Exception as e:
        print(f"Klaviyo push failed: {e}")



if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "8619730731228"
    run_one(arg)
