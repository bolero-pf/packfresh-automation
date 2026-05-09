"""
One-time sweep to archive orphan Shopify draft listings.

Background: accept→return→accept toggling on hold items used to create a new
Shopify product on every re-accept without verifying whether one already
existed for that SKU. Shopify doesn't enforce SKU uniqueness, so toggle-thrash
left duplicate active products sharing one barcode. raw_cards only stamps the
latest one, so the order-paid webhook only archived that. Older duplicates
remained live in Shopify and confused POS.

This script walks every raw_cards row that has a barcode, queries Shopify for
all active products with that SKU, and archives every one that isn't the
legitimate stamped listing for a PENDING_SALE card. Cards in any other state
shouldn't have a live listing at all — those are archived in full.

Default mode is dry-run. Pass --apply to actually archive.

Usage:
    python sweep_orphan_listings.py            # dry-run
    python sweep_orphan_listings.py --apply    # archive for real
    python sweep_orphan_listings.py --apply --limit 50    # debug
"""
import argparse
import logging
import os
import sys
import time

import requests
from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "shared"))
load_dotenv(os.path.join(_HERE, ".env"))
load_dotenv(os.path.join(_HERE, "..", "admin", ".env"))

import db  # noqa: E402  (shared/db.py)
from shopify_graphql import shopify_gql  # noqa: E402  (shared/shopify_graphql.py)

SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = os.environ.get("SHOPIFY_VERSION", "2025-01")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _shopify_rest(method: str, path: str, **kwargs) -> dict:
    url = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}{path}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    r = requests.request(method, url, headers=headers, timeout=30, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}


def find_active_products_for_sku(sku: str) -> list[dict]:
    """Return [{pid, gid, title, status}] for every active Shopify product
    carrying this SKU. Filters server-side via GraphQL `sku:X` and re-checks
    exact-match locally (the search is loose)."""
    if not sku:
        return []
    try:
        result = shopify_gql("""
            query($q: String!) {
              productVariants(first: 50, query: $q) {
                edges {
                  node {
                    sku
                    product { id title status legacyResourceId }
                  }
                }
              }
            }
        """, {"q": f"sku:{sku}"})
    except Exception as e:
        logger.warning(f"GraphQL lookup failed for sku={sku}: {e}")
        return []

    edges = (result.get("data", {}).get("productVariants", {}).get("edges") or [])
    products: dict[str, dict] = {}
    for edge in edges:
        node = edge.get("node") or {}
        if (node.get("sku") or "") != sku:
            continue
        prod = node.get("product") or {}
        gid = prod.get("id")
        if not gid or gid in products:
            continue
        products[gid] = {
            "pid":    str(prod.get("legacyResourceId") or ""),
            "gid":    gid,
            "title":  prod.get("title") or "",
            "status": (prod.get("status") or "").upper(),
        }
    return [p for p in products.values() if p["status"] == "ACTIVE" and p["pid"]]


def archive(pid: str, dry_run: bool) -> bool:
    if dry_run:
        return True
    try:
        _shopify_rest("PUT", f"/products/{pid}.json",
                      json={"product": {"id": pid, "status": "archived"}})
        return True
    except Exception as e:
        logger.error(f"Archive failed for product {pid}: {e}")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually archive (default is dry-run)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Process at most N raw_cards rows (debug)")
    args = ap.parse_args()
    dry_run = not args.apply

    if not SHOPIFY_STORE or not SHOPIFY_TOKEN:
        logger.error("SHOPIFY_STORE / SHOPIFY_TOKEN not set in env")
        sys.exit(1)

    db.init_pool()

    sql = """
        SELECT id, barcode, state, shopify_product_id, card_name
        FROM raw_cards
        WHERE barcode IS NOT NULL AND barcode != ''
        ORDER BY updated_at DESC NULLS LAST
    """
    if args.limit:
        sql += f" LIMIT {args.limit}"
    cards = db.query(sql)

    total = len(cards)
    mode = "DRY-RUN" if dry_run else "LIVE"
    logger.info(f"[{mode}] scanning {total} raw_cards rows")

    archived = 0
    kept = 0
    no_listings = 0

    for i, card in enumerate(cards, start=1):
        if i % 50 == 0:
            logger.info(f"  progress {i}/{total} (archived={archived}, kept={kept})")

        sku = card["barcode"]
        state = card["state"]
        keep_pid = str(card["shopify_product_id"]) if card.get("shopify_product_id") else None

        active = find_active_products_for_sku(sku)
        if not active:
            no_listings += 1
            time.sleep(0.05)
            continue

        for prod in active:
            pid = prod["pid"]
            keep_this = (state == "PENDING_SALE" and pid == keep_pid)
            if keep_this:
                kept += 1
                continue
            logger.info(
                f"  archive sku={sku} pid={pid} card_state={state} "
                f"title={prod['title']!r} card={card.get('card_name')!r}"
            )
            if archive(pid, dry_run):
                archived += 1

        time.sleep(0.1)  # gentle pacing for Shopify rate limits

    logger.info(
        f"[{mode}] done. archived={archived}, kept={kept}, "
        f"no-listings={no_listings}, total={total}"
    )
    if dry_run and archived:
        logger.info("Re-run with --apply to actually archive.")


if __name__ == "__main__":
    main()
