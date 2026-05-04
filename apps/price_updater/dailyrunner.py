import requests
import time
import re
import sys
import uuid
import concurrent
import os
import random
import threading
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException
import traceback

load_dotenv()

CHROME_BINARY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "chrome", "chrome.exe"))
CHROME_BINARY_PATH = ".venv/Scripts/chrome/chrome.exe"
MAX_WORKERS = 3
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent

sys.path.insert(0, str(BASE_DIR.parent / "shared"))
import db as shared_db
from price_auto_block import load_blocks
from price_rounding import charm_drop_auto_threshold as _drop_threshold_for


_INSERT_RUN_SQL = """
    INSERT INTO sealed_price_runs (
        run_id, started_at,
        product_gid, variant_id, sku, title, handle, tcgplayer_id, qty,
        old_price, tcg_price, suggested_price, new_price, delta_pct,
        action, reason, apply_status, applied_at, applied_price
    ) VALUES (
        %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
"""


def _coerce_money(val):
    if val is None or val == "":
        return None
    try:
        return round(float(val), 2)
    except (TypeError, ValueError):
        return None


def _persist_rows(run_id: str, started_at: datetime, rows: list[dict]) -> int:
    """Insert a batch of result rows into sealed_price_runs. Best-effort —
    DB failures are logged and the run continues (the next batch flush
    will retry the whole batch since we hold rows in memory)."""
    if not rows:
        return 0
    payload = []
    for r in rows:
        payload.append((
            run_id, started_at,
            r.get("product_gid"), str(r.get("variant_id") or "") or None,
            r.get("sku"), r.get("title"), r.get("handle"),
            str(r.get("tcgplayer_id") or "") or None,
            r.get("shopify_qty") if r.get("shopify_qty") is not None
                else r.get("inventory_quantity"),
            _coerce_money(r.get("shopify_price")),
            _coerce_money(r.get("tcg_price")),
            _coerce_money(r.get("suggested_price")),
            _coerce_money(r.get("new_price") or r.get("uploaded_price")),
            r.get("percent_diff") if isinstance(r.get("percent_diff"), (int, float)) else None,
            r.get("action"),
            r.get("reason") or r.get("note"),
            r.get("apply_status", "pending"),
            r.get("applied_at"),
            _coerce_money(r.get("applied_price")),
        ))
    try:
        return shared_db.execute_many_batch(_INSERT_RUN_SQL, payload, page_size=200)
    except Exception as e:
        print(f"[ERROR] sealed_price_runs batch insert failed ({len(rows)} rows): {e}")
        return 0


def _flush_to_db(run_id: str, started_at: datetime, buffered: list[dict]) -> None:
    """Drain the buffer into sealed_price_runs and clear it on success."""
    if not buffered:
        return
    inserted = _persist_rows(run_id, started_at, buffered)
    if inserted:
        buffered.clear()
        print(f"[WRITE] sealed_price_runs +{inserted} rows (run={run_id[:8]})")
# === CONFIG ===
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE")
GRAPHQL_ENDPOINT = f"https://{SHOPIFY_STORE}/admin/api/2025-10/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
}
TCGPLAYER_PRICE_ADJUSTMENT = 0.98
MAX_PERCENT_DROP = 5  # % drop below current price to flag
ignored_skus = set()
if os.path.exists(".venv/Scripts/ignore_skus.txt"):
    with open(".venv/Scripts/ignore_skus.txt") as f:
        ignored_skus = set(line.strip() for line in f if line.strip())
# === FUNCTIONS ===

def round_nice_price(price):
    """
    Rounds price to the nearest 'pretty' value:
    .00, .25, .49, .75, or .99
    """
    if price < 1.00:
        return round(price, 2)  # Just round it regularly for sub-$1 items

    # Find the whole dollar and the decimal portion
    whole = int(price)
    decimal = price - whole

    if decimal < 0.125:
        pretty_decimal = 0.00
    elif decimal < 0.37:
        pretty_decimal = 0.25
    elif decimal < 0.62:
        pretty_decimal = 0.49
    elif decimal < 0.87:
        pretty_decimal = 0.75
    else:
        pretty_decimal = 0.99

    return round(whole + pretty_decimal, 2)

def get_shopify_products(first=100):
    query = """
    query getProducts($first: Int!, $cursor: String) {
      products(first: $first, after: $cursor) {
        pageInfo {
          hasNextPage
        }
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
                  inventoryItem {
                    id
                  }
                }
              }
            }
            metafields(namespace: "tcg", first: 5) {
              edges {
                node {
                  key
                  value
                }
              }
            }
          }
        }
      }
    }
    """
    products = []
    has_next_page = True
    cursor = None

    print("🛒 Fetching products from Shopify...")

    while has_next_page:
        variables = {"first": first, "cursor": cursor}
        response = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={"query": query, "variables": variables})
        response.raise_for_status()
        data = response.json()
        product_edges = data["data"]["products"]["edges"]

        for edge in product_edges:
            node = edge["node"]
            tcg_id = None
            for mf in node["metafields"]["edges"]:
                if mf["node"]["key"] == "tcgplayer_id":
                    val = mf["node"]["value"]
                    if isinstance(val, str) and val.startswith("["):
                        val = val.strip("[]").replace('"', '').replace("'", '')
                    tcg_id = val
                    break

            for variant_edge in node["variants"]["edges"]:
                variant = variant_edge["node"]
                inventory_item_id = None
                if "inventoryItem" in variant and variant["inventoryItem"]:
                    inventory_item_id = variant["inventoryItem"]["id"].split("/")[-1]
                products.append({
                    "product_gid": node["id"],
                    "title": node["title"],
                    "handle": node["handle"],
                    "variant_id": variant["id"].split("/")[-1],
                    "shopify_price": float(variant["price"]),
                    "shopify_inventory_item_id": inventory_item_id,
                    "shopify_qty": variant["inventoryQuantity"],
                    "sku": variant["sku"],
                    "tcgplayer_id": tcg_id,
                    "tags": node.get("tags", [])
                })

        has_next_page = data["data"]["products"]["pageInfo"]["hasNextPage"]
        if has_next_page:
            cursor = product_edges[-1]["cursor"]
            time.sleep(0.25)

    print(f"🔄 Processed {len(products)} items...")
    return products
def get_shopify_products_for_feed(first=100):
    """
    Returns product-level structures with nested variants and minimal fields
    needed for the Reddit catalog feed.

    Shape:
    {
      "id": <product_gid>,
      "title": "...",
      "handle": "...",
      "body_html": "...",
      "image": {"src": "..."} or None,
      "variants": [
          {
            "id": <numeric_variant_id_str>,
            "price": "12.34",
            "barcode": "...",
            "sku": "...",
            "inventory_quantity": 5,
            "image": {"src": "..."} or None,
          },
          ...
      ]
    }
    """
    query = """
    query getProductsForFeed($first: Int!, $cursor: String) {
      products(first: $first, after: $cursor) {
        pageInfo {
          hasNextPage
        }
        edges {
          cursor
          node {
            id
            title
            handle
            bodyHtml
            tags
            featuredImage { url }
            images(first: 1) {
              edges {
                node { url }
              }
            }
            variants(first: 50) {
              edges {
                node {
                  id
                  title
                  price
                  barcode
                  sku
                  inventoryQuantity
                  image { url }
                }
              }
            }
          }
        }
      }
    }
    """

    products = []
    has_next_page = True
    cursor = None

    while has_next_page:
        variables = {"first": first, "cursor": cursor}
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=HEADERS,
            json={"query": query, "variables": variables},
        )
        resp.raise_for_status()
        data = resp.json()
        product_edges = data["data"]["products"]["edges"]

        for edge in product_edges:
            node = edge["node"]

            # pick a product-level image
            feat = (node.get("featuredImage") or {}).get("url")
            img_edge = (node.get("images") or {}).get("edges") or []
            fallback = img_edge[0]["node"]["url"] if img_edge else None
            product_image_url = feat or fallback

            product_dict = {
                "id": node["id"],
                "title": node["title"],
                "handle": node["handle"],
                "body_html": node.get("bodyHtml") or "",
                "tags": node.get("tags") or [],
                "image": {"src": product_image_url} if product_image_url else None,
                "variants": [],
            }

            for var_edge in node["variants"]["edges"]:
                v = var_edge["node"]
                var_img_url = (v.get("image") or {}).get("url")
                variant_dict = {
                    # numeric ID for the storefront link
                    "id": v["id"].split("/")[-1],
                    "price": v["price"],
                    "barcode": v.get("barcode"),
                    "sku": v.get("sku"),
                    "inventory_quantity": v.get("inventoryQuantity") or 0,
                    "image": {"src": var_img_url} if var_img_url else None,
                }
                product_dict["variants"].append(variant_dict)

            products.append(product_dict)

        has_next_page = data["data"]["products"]["pageInfo"]["hasNextPage"]
        if has_next_page:
            cursor = product_edges[-1]["cursor"]
            time.sleep(0.25)

    return products
def get_featured_price_tcgplayer_internal(tcgplayer_id: str, chrome_path: str) -> float or None:
    #print(f"✅ ENTERED get_featured_price_tcgplayer_internal for {tcgplayer_id}")
    #print(f"✅ Chrome path: {chrome_path}")
    url = f"https://www.tcgplayer.com/product/{tcgplayer_id}?Language=English"

    # Rotate user-agent strings
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.70 Safari/537.36"
    ]
    #print(f"Options check")
    options = Options()
    options.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/chromium")

    options.add_argument("--headless=new")  # or just "--headless"
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument(f"--user-agent={random.choice(user_agents)}")

    service = Service(os.environ.get("CHROMEDRIVER", "/usr/bin/chromedriver"))
    try:
        driver = webdriver.Chrome(service=service, options=options)
        #print("✅ Browser launched. Navigating to:", url)
    except Exception as e:
        print(f"❌ Failed to launch ChromeDriver: {e}")
        return None

    #print("Browser launched. Navigating to:", url)

    try:
        driver.get(url)

        if "/uhoh" in driver.current_url or "Uh-oh!" in driver.page_source:
            print(f"🚫 Bot detection triggered right after navigation to {url} — sleeping 5 minutes...")
            driver.quit()
            time.sleep(300)
            return None

        try:
            WebDriverWait(driver, 12).until(
                lambda d: d.find_element(By.CSS_SELECTOR,
                                         "section.spotlight__listing .spotlight__price").text.strip() != ""
            )
        except TimeoutException:
            if "/uhoh" in driver.current_url or "Uh-oh!" in driver.page_source:
                print(f"🚫 Bot detection during wait at {url} — sleeping 5 minutes...")
                driver.quit()
                time.sleep(300)
                return None
            else:
                print(f"⚠️ Timeout or failure while loading page for {tcgplayer_id}")
                return None

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # === ABOVE THE FOLD PRICE ===
        spotlight = soup.select_one("section.spotlight__listing")
        if spotlight:
            price_text = spotlight.select_one(".spotlight__price")
            shipping_text = spotlight.select_one(".spotlight__shipping")
            try:
                price_str = price_text.text.replace("$", "").replace(",", "").strip()
                price = float(price_str) if price_str else 0.0
            except ValueError:
                print(f"⚠️ Could not parse price for {tcgplayer_id} → raw: '{price_text.text}'")
                return None
            shipping = 0.0
            if shipping_text and "Included" not in shipping_text.text:
                match = re.search(r"\$([\d,]+\.\d{2})", shipping_text.text)
                if match:
                    shipping = float(match.group(1).replace(",", ""))

            total = price + shipping
            #print(f"✅ Featured price found: ${price:.2f} + ${shipping:.2f} shipping = ${total:.2f}")
            return round(total, 2)

        print(f"❌ No featured price found above the fold.")
        return None

    except Exception as e:
        print(f"❌ Exception during fetch for {tcgplayer_id}: {e}")
        return None

    finally:
        try:
            driver.quit()
        except Exception as e:
            print(f"⚠️ Failed to quit driver cleanly: {e}")


import multiprocessing

def _price_scraper_worker(tcgplayer_id, return_dict):
    try:
        print(f"[child] Starting scrape for ID {tcgplayer_id}")
        price = get_featured_price_tcgplayer_internal(tcgplayer_id)
        return_dict["price"] = price
    except Exception as e:
        return_dict["error"] = str(e)

from concurrent.futures import ThreadPoolExecutor, TimeoutError as ThreadTimeout

def get_featured_price_tcgplayer(tcgplayer_id: str, timeout=30):
    result = {"value": None}

    def worker():
        result["value"] = get_featured_price_tcgplayer_internal(tcgplayer_id, None)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        print(f"⏱️ Timeout: skipping Selenium scrape for {tcgplayer_id}")
        return None
    return result["value"]

def update_variant_price(product_gid: str, variant_id: str, new_price: float):
    mutation = """
    mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants { id price }
        userErrors { field message }
      }
    }
    """
    variables = {
        "productId": product_gid,  # full gid, e.g., "gid://shopify/Product/1234567890"
        "variants": [{
            "id": f"gid://shopify/ProductVariant/{variant_id}",
            "price": str(new_price)
        }]
    }
    r = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={"query": mutation, "variables": variables})
    r.raise_for_status()
    return r.json()
import math
def round_competitive_price(tcg_price: float) -> float:
    """
    Always return a clean .99, .75, .50, or .25 ending that stays *below* TCGPlayer market.
      • Prefer the closest ending below tcg_price within 0.5% or $0.25 (whichever smaller).
      • Never round up or exceed TCG price.
      • If no valid tier undercut fits within caps, fall back to maximum allowed drop.
    """
    if tcg_price < 1.0:
        return round(max(0.25, tcg_price * 0.97), 2)

    cap_pct = 0.005     # 0.5 %
    cap_abs = 0.25      # $0.25

    endings = [0.99, 0.75, 0.50, 0.25]
    floor_val = math.floor(tcg_price)

    candidates = []
    for ending in endings:
        price = floor_val + ending
        # if the chosen .ending is above tcg_price, step down one dollar
        if price >= tcg_price:
            price = (floor_val - 1) + ending
        drop_abs = tcg_price - price
        drop_pct = drop_abs / tcg_price if tcg_price else 0.0
        if 0 < drop_abs <= cap_abs and drop_pct <= cap_pct:
            candidates.append((drop_abs, price))

    if candidates:
        # pick the *closest* undercut (smallest drop)
        _, best = min(candidates, key=lambda x: x[0])
        return round(best, 2)

    # if all four are beyond cap, take the max allowed undercut
    limit = round(tcg_price - min(cap_abs, tcg_price * cap_pct), 2)
    # snap limit down to nearest allowed ending
    dec = limit % 1
    for ending in endings:
        if dec >= ending:
            return round(math.floor(limit) + ending, 2)
    return round(math.floor(limit) - 1 + endings[0], 2)

def safe_percent_diff(old_price: float, new_price: float):
    if not old_price or old_price <= 0:
        return ""   # or "n/a"
    return round(100 * (new_price - old_price) / old_price, 2)

def process_product(product, blocked: set | None = None):
    """Decide what to do with one Shopify variant. Returns the original
    product dict mutated with `action`, `reason`, and pricing fields the
    DB writer expects.

    Decision tree:
      block-listed       -> skip
      ignore-skus file   -> untouched (legacy local mute)
      tagged ignore_*    -> untouched
      no tcg_id          -> missing
      tcg fetch failed   -> missing
      new > current      -> updated (always raise)
      new < current OOS  -> updated (auto-lower OOS only)
      new < current in-stock -> review (bulk-approve in dashboard)
      new == current     -> untouched
    """
    blocked = blocked or set()
    tcg_id = product["tcgplayer_id"]
    current_price = float(product.get("shopify_price") or product.get("price") or 0)

    variant_key = str(product.get("variant_id") or "")
    if variant_key and variant_key in blocked:
        product.update({"action": "skip", "reason": "auto-block"})
        return "skip", product

    if not tcg_id:
        product.update({"action": "missing", "reason": "no tcgplayer_id metafield"})
        return "missing", product

    if product["sku"] in ignored_skus:
        product.update({"action": "untouched", "reason": "ignored sku (local file)"})
        return "untouched", product

    raw_tags = product.get("tags") or []
    tags = [t.strip().lower() for t in raw_tags] if isinstance(raw_tags, list) else [t.strip().lower() for t in
                                                                                     raw_tags.split(",")]

    if any(tag in tags for tag in ["weekly deals", "ignore_update", "slab"]):
        product.update({"action": "untouched", "reason": "tagged for skip"})
        return "untouched", product

    print(f"[{tcg_id}] Checking {product['title']}...")
    tcg_price = get_featured_price_tcgplayer(tcg_id)
    if tcg_price is None:
        product.update({"action": "missing", "reason": "tcgplayer scrape failed",
                        "tcg_price": None})
        return "missing", product

    new_price = round_competitive_price(tcg_price)
    percent_diff = safe_percent_diff(current_price, new_price)

    if new_price < current_price:
        shopify_qty = int(product.get("shopify_qty") or product.get("inventory_quantity") or 0)
        drop_dollars = current_price - new_price
        # Sealed competitive endings (.99/.75/.50/.25) hop by ≤ ~$1; under
        # $10 by ≤ ~$0.50. Use the same shared threshold as raw so a $1.49
        # → $0.99 drop (50%) auto-applies because it's only 50¢.
        drop_threshold = _drop_threshold_for(new_price)
        small_dollar_drop = drop_dollars <= drop_threshold
        if shopify_qty <= 0:
            print(f"  ⬇ OOS auto-update {product['title']}: ${current_price:.2f} → ${new_price:.2f} (qty=0)")
            update_variant_price(product["product_gid"], product["variant_id"], new_price)
            product.update({
                "action": "updated",
                "tcg_price": tcg_price,
                "suggested_price": new_price,
                "uploaded_price": new_price, "new_price": new_price,
                "percent_diff": percent_diff,
                "reason": "OOS auto-lower to market",
                "apply_status": "applied",
                "applied_at": datetime.now(timezone.utc),
                "applied_price": new_price,
            })
            return "updated", product
        elif small_dollar_drop:
            # Charm-rounding noise — apply silently. Anything bigger still
            # waits for a human in the review queue.
            print(f"  ⬇ small-drop auto-update {product['title']}: ${current_price:.2f} → ${new_price:.2f} (${drop_dollars:.2f} ≤ ${drop_threshold:.2f})")
            update_variant_price(product["product_gid"], product["variant_id"], new_price)
            product.update({
                "action": "updated",
                "tcg_price": tcg_price,
                "suggested_price": new_price,
                "uploaded_price": new_price, "new_price": new_price,
                "percent_diff": percent_diff,
                "reason": f"auto-dropped ${drop_dollars:.2f} (within ${drop_threshold:.2f} charm tier)",
                "apply_status": "applied",
                "applied_at": datetime.now(timezone.utc),
                "applied_price": new_price,
            })
            return "updated", product
        else:
            product.update({
                "action": "review",
                "tcg_price": tcg_price,
                "suggested_price": new_price,
                "percent_diff": percent_diff,
                "reason": (f"Lower ${drop_dollars:.2f} (> ${drop_threshold:.2f} charm tier) "
                           "— in stock, needs review"),
            })
            return "review", product

    elif new_price > current_price:
        print(f" Updating {product['title']} : {tcg_price} : {new_price}")
        update_variant_price(product["product_gid"], product["variant_id"], new_price)
        product.update({
            "action": "updated",
            "tcg_price": tcg_price,
            "suggested_price": new_price,
            "uploaded_price": new_price, "new_price": new_price,
            "percent_diff": percent_diff,
            "reason": "Raise to stay near market",
            "apply_status": "applied",
            "applied_at": datetime.now(timezone.utc),
            "applied_price": new_price,
        })
        return "updated", product

    product.update({
        "action": "untouched", "tcg_price": tcg_price,
        "percent_diff": percent_diff, "reason": "At target",
    })
    return "untouched", product

def _invalidate_inventory_cache():
    """Notify inventory + intake services that Shopify prices changed."""
    for name, url_var in [("inventory", "INVENTORY_INTERNAL_URL"), ("intake", "INTAKE_INTERNAL_URL")]:
        url = os.environ.get(url_var, "")
        if not url:
            continue
        try:
            requests.post(f"{url}/api/cache/invalidate",
                          json={"reason": "price_updater"}, timeout=5)
            print(f"📡 Notified {name} service to refresh cache")
        except Exception as e:
            print(f"⚠️ Failed to notify {name} cache: {e}")


def process_product_with_delay(product_and_index, blocked):
    product, index = product_and_index

    # Scale delay as batch progresses to dodge TCGplayer's bot detection
    delay = random.uniform(2.5, 5.0) + (index / 700.0) * 2.5  # starts ~3s, ends ~5.5s
    time.sleep(delay)

    return process_product(product, blocked=blocked)


def _classify_pre_scrape(product, blocked, ignored_skus):
    """Decide what we know about a variant without ever hitting TCGplayer.
    Returns (action, reason) for variants we can short-circuit, or None
    when a real scrape is required.

    Mirrors the head of process_product — any change there must be
    reflected here or vice-versa, otherwise the pre-filter and the loop
    will disagree.
    """
    variant_key = str(product.get("variant_id") or "")
    if variant_key and variant_key in blocked:
        return ("skip", "auto-block")
    if not product.get("tcgplayer_id"):
        return ("missing", "no tcgplayer_id metafield")
    if product.get("sku") in ignored_skus:
        return ("untouched", "ignored sku (local file)")
    raw_tags = product.get("tags") or []
    tags = ([t.strip().lower() for t in raw_tags] if isinstance(raw_tags, list)
            else [t.strip().lower() for t in raw_tags.split(",")])
    if any(tag in tags for tag in ("weekly deals", "ignore_update", "slab")):
        return ("untouched", "tagged for skip")
    return None


def run_price_sync():
    """Scan every Shopify variant, call TCGplayer for the featured price,
    apply auto-raises immediately and queue drops for review. Every row
    persists to sealed_price_runs (the dashboard reads from there).

    Performance: variants without a tcgplayer_id (or that are blocked,
    sku-ignored, or tag-skipped) get classified up front and persisted in
    one batch — the threaded scrape loop only runs over real candidates.
    On a ~2200-SKU store with ~800 priceable variants this trims hours of
    per-product sleeps + several inter-batch cooldowns."""
    shared_db.init_pool()
    blocked = load_blocks(shared_db, "sealed")
    if blocked:
        print(f"  {len(blocked)} variants on sealed price-auto-block list")

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    print(f"=== sealed run_id={run_id} started_at={started_at.isoformat()} ===")

    all_products = get_shopify_products()
    counts = {"updated": 0, "review": 0, "missing": 0, "untouched": 0, "skip": 0}
    pending_db: list[dict] = []

    # Partition: anything decidable without a network call gets persisted
    # straight to sealed_price_runs; the loop below only does real work.
    to_scrape: list[dict] = []
    pre_classified: list[dict] = []
    for p in all_products:
        pre = _classify_pre_scrape(p, blocked, ignored_skus)
        if pre is None:
            to_scrape.append(p)
        else:
            action, reason = pre
            p["action"] = action
            p["reason"] = reason
            pre_classified.append(p)
            counts[action] = counts.get(action, 0) + 1

    print(f"🛒 {len(all_products)} variants total — {len(to_scrape)} need a "
          f"TCGplayer scrape, {len(pre_classified)} pre-classified (skipped, "
          f"missing, ignored, or blocked)")
    if pre_classified:
        _persist_rows(run_id, started_at, pre_classified)
        print(f"[WRITE] sealed_price_runs +{len(pre_classified)} pre-classified "
              f"rows (run={run_id[:8]})")

    products = to_scrape

    try:
        print(f"🔄 Scrape candidates: {len(products)}")

        for batch_start in range(0, len(products), 200):
            batch = products[batch_start:batch_start + 200]
            print(f"\n📦 Starting batch {batch_start + 1} to {batch_start + len(batch)}...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                indexed_batch = [(product, batch_start + i) for i, product in enumerate(batch)]
                futures = {executor.submit(process_product_with_delay, p, blocked): p[0] for p in indexed_batch}
                completed = 0
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result_type, data = future.result(timeout=60)
                    except concurrent.futures.TimeoutError:
                        product = futures[future]
                        print(f"⏰ Timeout in thread for product: {product.get('title', 'Unknown')}")
                        result_type = "missing"
                        data = {**product, "action": "missing", "reason": "Thread timeout"}

                    completed += 1
                    if completed % 10 == 0 or completed == len(batch):
                        print(f"🔁 Processed {completed}/{len(batch)} items in this batch...")

                    counts[result_type] = counts.get(result_type, 0) + 1
                    pending_db.append(data)

            # Persist this batch (and any unflushed prior rows) before sleeping.
            _flush_to_db(run_id, started_at, pending_db)

            if batch_start + len(batch) < len(products):
                print(f"🟡 Cooling down to avoid IP rate limit", flush=True)
                for i in range(10, 0, -1):
                    print(f"💤 Still alive… sleeping {i} more minute(s)", flush=True)
                    time.sleep(60)

        _flush_to_db(run_id, started_at, pending_db)

        print(f"\n✅ Updates pushed:        {counts.get('updated', 0)}")
        print(f"⚠️  Flagged for review:    {counts.get('review', 0)}")
        print(f"❓ Missing listings:       {counts.get('missing', 0)}")
        print(f"🚫 Auto-blocked / skipped: {counts.get('skip', 0)}")
        print(f"👌 Untouched (no change):  {counts.get('untouched', 0)}")
    except Exception as e:
        print("FATAL error in run_price_sync: ", e)
        traceback.print_exc()
        raise
    finally:
        print("🧹 Final flush (crash-safe)!")
        _flush_to_db(run_id, started_at, pending_db)
        _invalidate_inventory_cache()




# === RUN ===
if __name__ == "__main__":
    print("=== ENTER price sync ===")
    run_price_sync()
    print("=== EXIT price sync ===")

