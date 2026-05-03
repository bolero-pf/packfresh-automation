"""
slab_updater.py — Nightly graded slab price sync.

Pulls live eBay comps from Scrydex for every slab in our Shopify store,
compares to current listing price, and decides per-slab:

  delta = (current - charm_ceil(market)) / charm_ceil(market)

  |delta| <= 10%        -> ok (no-op)
  delta   <  -10%        -> auto-RAISE in Shopify (always — never miss
                            a fast mover going up)
  delta   >   10%        -> flag_overpriced for human review (we never
                            auto-drop slab prices on live inventory)

Slabs in the price_auto_block list ('slab' domain, key=variant_gid) are
skipped — escape hatch for runaway suggestions on a single listing.

Usage:
    python slab_updater.py                # nightly mode (auto-raise + flag drops)
    python slab_updater.py --dry-run      # no Shopify writes, log only
    python slab_updater.py --csv out.csv  # also write results to CSV

Can also be triggered via HTTP POST from review_dashboard.py or APScheduler.
"""

import os
import re
import sys
import csv
import math
import uuid
import logging
import argparse
from datetime import datetime, timezone
from decimal import Decimal


def charm_ceil(price) -> float:
    """Round price UP to a 'charm' price ending in .99.

    Tier scheme (Sean's preference, always-ceil so we never undercut median):
      <  $100  -> nearest $5  ending in .99 ($24.99, $29.99, ...)
      <  $500  -> nearest $10 ending in .99 ($109.99, $119.99, ...)
      >= $500  -> nearest $25 ending in .99 ($524.99, $549.99, ...)

    Always rounds UP — for a $100 median we suggest $109.99 not $99.99.
    Sean's positioning: trusted seller, accepts MSRP drops + discounts,
    so we stay >= market median.
    """
    try:
        p = float(price or 0)
    except (TypeError, ValueError):
        return 0.0
    if p <= 0:
        return 0.0
    if p < 100:
        increment = 5
    elif p < 500:
        increment = 10
    else:
        increment = 25
    next_step = math.ceil(p / increment) * increment
    candidate = next_step - 0.01
    if candidate < p:
        # Boundary case (e.g. p=$100 with $10 increment lands exactly on $99.99
        # which is below p — bump to the next tier price up).
        candidate = next_step + increment - 0.01
    return round(candidate, 2)

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SHOPIFY_STORE    = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN    = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION  = "2025-10"
GRAPHQL_ENDPOINT = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
}

# ── Grade extraction from Shopify product titles ─────────────────────────────
# Patterns like "PSA 10", "BGS 9.5", "CGC 9" anywhere in the title
_GRADE_PATTERN = re.compile(
    r"\b(PSA|BGS|CGC|SGC|ACE|TAG)\s+(\d+(?:\.\d)?)\b",
    re.IGNORECASE,
)


def extract_grade_from_title(title: str) -> tuple[str, str] | None:
    """Extract (company, grade_value) from a Shopify product title."""
    m = _GRADE_PATTERN.search(title or "")
    if m:
        return m.group(1).upper(), m.group(2)
    return None


def fetch_slab_products() -> list[dict]:
    """Fetch all Shopify products tagged 'slab' via GraphQL."""
    query = """
    query getSlabs($first: Int!, $cursor: String, $query: String!) {
      products(first: $first, after: $cursor, query: $query) {
        pageInfo { hasNextPage }
        edges {
          cursor
          node {
            id
            title
            handle
            tags
            variants(first: 10) {
              edges {
                node {
                  id
                  price
                  sku
                  inventoryQuantity
                  inventoryItem { id unitCost { amount } }
                }
              }
            }
            metafields(first: 10) {
              edges {
                node { namespace key value }
              }
            }
          }
        }
      }
    }
    """
    products = []
    cursor = None
    has_next = True

    while has_next:
        variables = {"first": 50, "cursor": cursor, "query": "tag:slab"}
        r = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                          json={"query": query, "variables": variables}, timeout=30)
        r.raise_for_status()
        data = r.json()["data"]["products"]
        for edge in data["edges"]:
            node = edge["node"]
            cursor = edge["cursor"]

            # Extract metafields
            mf = {}
            for mfe in node.get("metafields", {}).get("edges", []):
                n = mfe["node"]
                mf[f"{n['namespace']}.{n['key']}"] = n["value"]

            tcg_id = mf.get("tcg.tcgplayer_id") or mf.get("pf_slab.tcgplayer_id")
            if tcg_id:
                try:
                    tcg_id = int(str(tcg_id).strip().strip("[]\"'"))
                except (ValueError, TypeError):
                    tcg_id = None

            for ve in node["variants"]["edges"]:
                v = ve["node"]
                # Slabs are unique — once sold, they're gone forever. Don't
                # waste Scrydex credits or DB rows pricing variants at qty=0.
                if (v.get("inventoryQuantity") or 0) <= 0:
                    continue
                cost_amount = None
                if v.get("inventoryItem", {}).get("unitCost"):
                    try:
                        cost_amount = float(v["inventoryItem"]["unitCost"]["amount"])
                    except (TypeError, ValueError):
                        pass

                products.append({
                    "product_gid": node["id"],
                    "variant_gid": v["id"],
                    "title":       node["title"],
                    "tags":        node.get("tags", []),
                    "price":       float(v["price"]),
                    "sku":         v.get("sku", ""),
                    "qty":         v.get("inventoryQuantity", 0),
                    "cost_basis":  cost_amount,
                    "tcg_id":      tcg_id,
                })

        has_next = data["pageInfo"]["hasNextPage"]

    return products


_INSERT_RUN_SQL = """
    INSERT INTO slab_price_runs (
        run_id, started_at,
        product_gid, variant_gid, sku, title, qty, cost_basis,
        tcgplayer_id, company, grade,
        old_price, new_price, suggested_price,
        median, low_comp, high_comp, comps_count, delta_pct, trend_7d,
        action, reason
    ) VALUES (
        %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s
    )
"""


def _record_run_row(db_module, run_id: str, started_at: datetime, entry: dict):
    """Persist one slab_updater result row to slab_price_runs.

    Best-effort — DB failures here must never crash the run, since the
    Shopify mutation may have already gone through.
    """
    try:
        db_module.execute(_INSERT_RUN_SQL, (
            run_id, started_at,
            entry.get("product_gid"), entry.get("variant_gid"),
            entry.get("sku"), entry.get("title"),
            entry.get("qty"), entry.get("cost_basis"),
            entry.get("tcg_id"), entry.get("company"), entry.get("grade"),
            entry.get("price"), entry.get("new_price"), entry.get("suggested_price"),
            entry.get("median"), entry.get("low"), entry.get("high"),
            entry.get("comps_count"), entry.get("delta_pct"), entry.get("trend_7d"),
            entry.get("action"), entry.get("reason"),
        ))
    except Exception as e:
        logger.warning(f"Failed to persist slab_price_runs row for {entry.get('sku')}: {e}")


def update_variant_price(product_gid: str, variant_gid: str, new_price: float):
    """Update a single variant's price via GraphQL."""
    mutation = """
    mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants { id price }
        userErrors { field message }
      }
    }
    """
    variables = {
        "productId": product_gid,
        "variants": [{"id": variant_gid, "price": str(new_price)}],
    }
    r = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                      json={"query": mutation, "variables": variables}, timeout=30)
    r.raise_for_status()
    result = r.json()
    errs = result.get("data", {}).get("productVariantsBulkUpdate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"Shopify price update failed: {errs}")
    return result


def run(*, apply: bool = True, csv_path: str = None) -> list[dict]:
    """
    Main slab update loop.

    1. Fetch all slab products from Shopify
    2. Skip any in the price_auto_block list (domain='slab')
    3. For each, extract grade from title + resolve TCG ID
    4. Fetch live eBay comps via shared/graded_pricing.py
    5. Compare current price to charm-ceiled market target
    6. Auto-RAISE undervalued listings, flag overpriced ones
    7. Persist every row to slab_price_runs for the dashboard audit trail

    apply=True (default): auto-raise undervalued slabs in Shopify.
    apply=False         : log + persist only, no Shopify writes (dry-run).

    Returns list of result dicts.
    """
    import db as db_module
    db_module.init_pool()

    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))
    from price_auto_block import load_blocks
    from graded_pricing import get_live_graded_comps

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    logger.info(f"Slab updater run_id={run_id} started_at={started_at.isoformat()} apply={apply}")

    blocked = load_blocks(db_module, "slab")
    if blocked:
        logger.info(f"  {len(blocked)} slabs on price-auto-block list")

    logger.info("Fetching slab products from Shopify...")
    slabs = fetch_slab_products()
    logger.info(f"Found {len(slabs)} slab variants")

    results = []
    updated = 0
    flagged = 0

    for slab in slabs:
        title = slab["title"]
        if slab.get("variant_gid") in blocked:
            entry = {**slab, "action": "skip", "reason": "auto-block"}
            results.append(entry)
            _record_run_row(db_module, run_id, started_at, entry)
            continue

        grade_info = extract_grade_from_title(title)
        if not grade_info:
            entry = {**slab, "action": "skip", "reason": "no grade in title"}
            results.append(entry)
            _record_run_row(db_module, run_id, started_at, entry)
            continue

        company, grade_val = grade_info
        tcg_id = slab.get("tcg_id")
        if not tcg_id:
            entry = {**slab, "company": company, "grade": grade_val,
                     "action": "skip", "reason": "no tcg_id"}
            results.append(entry)
            _record_run_row(db_module, run_id, started_at, entry)
            continue

        # Fetch live comps. We use comps["market"] (the smart market price)
        # not comps["mid"] (raw median): smart market does IQR outlier removal,
        # protects the most recent 10% of sales from being dropped, and
        # exponentially decays older sales (14-day half-life). Raw median
        # treats a sale from 90 days ago the same as today's — bad signal
        # for fast-moving cards.
        comps = get_live_graded_comps(tcg_id, company, grade_val, db_module)
        if not comps or not comps.get("market"):
            entry = {**slab, "action": "skip", "reason": "no comp data",
                     "company": company, "grade": grade_val}
            results.append(entry)
            _record_run_row(db_module, run_id, started_at, entry)
            continue

        current = slab["price"]
        market  = float(comps["market"])
        cost    = slab.get("cost_basis") or 0
        comps_n = comps.get("comps_count", 0)
        comps_kept = comps.get("comps_kept", comps_n)
        outliers = comps.get("outliers_dropped", 0)

        # Decision target is charm_ceil(market), not raw market. Charm ceiling
        # intentionally lifts price to the next .99 tier — a $12 market
        # becomes a $14.99 target. Computing delta vs raw market means a card
        # priced correctly at $14.99 re-flags every run as "+25% overpriced"
        # and ping-pongs forever. Delta is the gap vs the actual target price.
        safe_price  = max(market, cost) if cost else market
        charm_price = charm_ceil(safe_price)
        target      = charm_price or market
        delta_pct   = ((current - target) / target * 100) if target > 0 else 0

        entry = {
            **slab,
            "company":     company,
            "grade":       grade_val,
            # NB: column is named 'median' in slab_price_runs for legacy reasons
            # but we store the smart market price (recency-weighted, IQR-cleaned)
            "median":      market,
            "low":         comps.get("low"),
            "high":        comps.get("high"),
            "comps_count": comps_n,
            "trend_7d":    comps.get("trend_7d_pct"),
            "delta_pct":   round(delta_pct, 1),
        }

        # Auto-raise UP, flag DROPS for review. Cost basis is still the floor
        # below which we never go (max(market, cost) above).
        if abs(delta_pct) <= 10:
            entry["action"] = "ok"
            entry["reason"] = f"within 10% of target ${target:.2f} (delta {delta_pct:+.1f}%)"
        elif delta_pct > 10:
            # Currently priced above target — flag, don't auto-drop
            entry["action"] = "flag_overpriced"
            entry["reason"] = f"{delta_pct:+.1f}% over target ${target:.2f} — review"
            entry["suggested_price"] = charm_price
            flagged += 1
        else:
            # Currently priced below target — auto-raise to chase the market
            entry["suggested_price"] = charm_price
            if apply and slab["qty"] > 0:
                try:
                    update_variant_price(slab["product_gid"], slab["variant_gid"], charm_price)
                    entry["action"] = "adjusted"
                    entry["new_price"] = charm_price
                    entry["reason"] = (f"auto-raised {abs(delta_pct):.1f}% to follow market; "
                                       f"${current:.2f} -> ${charm_price:.2f}")
                    updated += 1
                except Exception as e:
                    entry["action"] = "error"
                    entry["reason"] = f"price update failed: {e}"
            else:
                entry["action"] = "flag_underpriced"
                entry["reason"] = (f"[DRY-RUN] would auto-raise {abs(delta_pct):.1f}% "
                                   f"to ${charm_price:.2f}")
                flagged += 1

        results.append(entry)
        _record_run_row(db_module, run_id, started_at, entry)
        logger.info(f"  {title}: ${current:.2f} vs market ${market:.2f} ({comps_n} comps) → {entry['action']}")

    logger.info(f"\nDone. run_id={run_id}  {len(slabs)} slabs, {updated} adjusted, {flagged} flagged")

    # Write CSV if requested
    if csv_path and results:
        _write_csv(csv_path, results)
        logger.info(f"Results written to {csv_path}")

    return results


def _write_csv(path: str, results: list[dict]):
    fields = ["title", "sku", "company", "grade", "price", "median", "low", "high",
              "comps_count", "delta_pct", "trend_7d", "action", "reason",
              "suggested_price", "new_price", "cost_basis", "qty"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow(r)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

    parser = argparse.ArgumentParser(description="Nightly slab price updater")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't push price changes to Shopify (default: apply auto-raises)")
    parser.add_argument("--csv", default=None, help="Write results to CSV file")
    args = parser.parse_args()

    run(apply=not args.dry_run, csv_path=args.csv)
