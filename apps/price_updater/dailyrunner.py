import requests
import time
import re
import csv
import concurrent
import argparse
import os
import random
import pandas as pd
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
MAX_WORKERS = 5
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent

# Write outputs next to your app, not the ephemeral CWD
REVIEW_CSV     = BASE_DIR / "price_updates_needs_review.csv"
PUSHED_CSV     = BASE_DIR / "price_updates_pushed.csv"
UNTOUCHED_CSV  = BASE_DIR / "price_updates_untouched.csv"
MISSING_CSV    = BASE_DIR / "price_updates_missing_listing.csv"  # keep your current name

# One place to define the columns your dashboard can tolerate
CSV_COLS = [
    "title","sku","shopify_price","suggested_price","price_to_upload",
    "shopify_qty","variant_id","shopify_inventory_item_id","pending_shopify_update",
    "price_last_updated","notes","product_gid","tcg_price","percent_diff","reason",
    "tcgplayer_id","handle","tags","uploaded_price","new_price","note"
]

def _write_csv_safe(rows, path, cols=CSV_COLS):
    import pandas as pd
    # Always emit headers so read_csv never fails
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=cols)
    # Re-order columns where possible (missing columns will be created)
    df = df.reindex(columns=cols, fill_value="")
    df.to_csv(path, index=False)
    print(f"[WRITE] {path} rows={len(df)}")
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
    #print("Resolved chrome path:", chrome_path)
    #print("Exists?", os.path.exists(chrome_path))
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--headless=new")  # Optional: comment this out to see browser
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
    chrome_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "chrome", "chrome.exe"))

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(get_featured_price_tcgplayer_internal, tcgplayer_id, chrome_path)
        try:
            return future.result(timeout=timeout)
        except ThreadTimeout:
            print(f"⏱️ Timeout: skipping Selenium scrape for {tcgplayer_id}")
            return None

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

def process_product(product):
    tcg_id = product["tcgplayer_id"]
    current_price = float(product.get("shopify_price") or product.get("price") or 0)
    if not tcg_id:
        return "missing", product

    if product["sku"] in ignored_skus:
        print(f"🚫 Ignoring {product['title']} (SKU: {product['sku']})")
        return "untouched", product

    raw_tags = product.get("tags") or []
    tags = [t.strip().lower() for t in raw_tags] if isinstance(raw_tags, list) else [t.strip().lower() for t in
                                                                                     raw_tags.split(",")]

    if any(tag in tags for tag in ["weekly deals", "ignore_update", "slab"]):
        #print(f"🛑 Skipping {product['title']} (tagged as Weekly Deals or Ignore Update or a graded card)")
        return "untouched", product

    print(f"[{tcg_id}] Checking {product['title']}...")
    tcg_price = get_featured_price_tcgplayer(tcg_id)
    if tcg_price is None:
        return "missing", product

    #new_price = round_nice_price(tcg_price * TCGPLAYER_PRICE_ADJUSTMENT)
    new_price = round_competitive_price(tcg_price)

    if new_price < current_price:
        # price cut → send to review
        percent_diff = safe_percent_diff(current_price, new_price)
        return ("review",
                {**product,
                 "shopify_price": current_price,
                 "price_to_upload": "",  # empty until you approve it
                 "current_price": current_price,  # optional alias for clarity
                 "product_gid": product["product_gid"],
                 "tcg_price": tcg_price,
                 "suggested_price": new_price,
                 "percent_diff": percent_diff,
                 "reason": "Lower to stay near market"})

    elif new_price > current_price:
        # price increase → auto-update
        print(f" Updating {product['title']} : {tcg_price} : {new_price}")
        update_variant_price(product["product_gid"], product["variant_id"], new_price)
        percent_diff = safe_percent_diff(current_price, new_price)
        return ("updated",
                {**product,
                 "shopify_price": current_price,
                 "current_price": current_price,
                 "tcg_price": tcg_price,
                 "uploaded_price": new_price,
                 "new_price": new_price,
                 "percent_diff": percent_diff,
                 "reason": "Raise to stay near market"})

    # equal → no-op
    return ("untouched",
            {**product, "tcg_price": tcg_price, "note": "At target"})

def _variant_product_gid_from_graphql(variant_id: str) -> str:
    # Fallback resolver if CSV lacks product_gid (should be rare once we write it)
    q = """
    query($id: ID!) { productVariant(id: $id) { product { id } } }
    """
    vid = f"gid://shopify/ProductVariant/{variant_id}"
    r = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={"query": q, "variables": {"id": vid}})
    r.raise_for_status()
    data = r.json()
    return data["data"]["productVariant"]["product"]["id"]

def upload_reviewed_csv():
    import math
    try:
        # Keep empty strings as "" instead of NaN
        df = pd.read_csv(REVIEW_CSV, keep_default_na=False)
    except Exception as e:
        print(f"❌ Failed to read review CSV: {e}")
        return

    print(f"🚀 Uploading reviewed prices from {REVIEW_CSV}...")

    for _, row in df.iterrows():
        variant_id = str(row.get("variant_id") or "").strip()
        raw = row.get("price_to_upload", "")

        # Normalize to a clean string
        s = ("" if raw is None else str(raw)).strip()
        if s == "" or s.lower() == "nan":
            print(f"⚠️ Skipping row with missing/NaN price: {row.get('title')}")
            continue

        # Validate numeric + finite
        try:
            new_price = float(s)
            if not math.isfinite(new_price) or new_price <= 0:
                print(f"⚠️ Skipping non-finite/invalid price '{s}' for {row.get('title')}")
                continue
        except Exception:
            print(f"⚠️ Skipping non-numeric price '{s}' for {row.get('title')}")
            continue

        if not variant_id:
            print(f"⚠️ Skipping row without variant_id: {row.get('title')}")
            continue

        product_gid = row.get("product_gid")
        if not isinstance(product_gid, str) or not product_gid.startswith("gid://shopify/Product/"):
            product_gid = _variant_product_gid_from_graphql(variant_id)

        try:
            update_variant_price(product_gid, variant_id, new_price)
            print(f"✅ Updated {row.get('title')} to ${new_price:.2f}")
        except Exception as e:
            print(f"❌ Failed to update variant {variant_id}: {e}")

def upload_missing_csv():
    import math
    try:
        df = pd.read_csv(MISSING_CSV, keep_default_na=False)
    except Exception as e:
        print(f"❌ Failed to read missing CSV: {e}")
        return

    print(f"🚀 Uploading reviewed prices from {MISSING_CSV}...")

    for _, row in df.iterrows():
        variant_id = str(row.get("variant_id") or "").strip()
        raw = row.get("price_to_upload", "")

        s = ("" if raw is None else str(raw)).strip()
        if s == "" or s.lower() == "nan":
            print(f"⚠️ Skipping row with missing/NaN price: {row.get('title')}")
            continue

        try:
            new_price = float(s)
            if not math.isfinite(new_price) or new_price <= 0:
                print(f"⚠️ Skipping non-finite/invalid price '{s}' for {row.get('title')}")
                continue
        except Exception:
            print(f"⚠️ Skipping non-numeric price '{s}' for {row.get('title')}")
            continue

        if not variant_id:
            print(f"⚠️ Skipping row without variant_id: {row.get('title')}")
            continue

        # ensure we have product_gid for this variant
        product_gid = row.get("product_gid")
        if not isinstance(product_gid, str) or not product_gid.startswith("gid://shopify/Product/"):
            product_gid = _variant_product_gid_from_graphql(variant_id)

        try:
            update_variant_price(product_gid, variant_id, new_price)
            print(f"✅ Updated {row.get('title')} to ${new_price:.2f}")
        except Exception as e:
            print(f"❌ Failed to update variant {variant_id}: {e}")


def process_product_with_delay(product_and_index):
    product, index = product_and_index

    # Scale delay as batch progresses
    delay = random.uniform(2.5, 5.0) + (index / 700.0) * 2.5  # starts ~3s, ends ~5.5s
    time.sleep(delay)

    return process_product(product)

def run_price_sync():

    parser = argparse.ArgumentParser()
    parser.add_argument("--upload-reviewed", action="store_true", help="Upload reviewed prices")
    parser.add_argument("--upload-missing", action="store_true", help="Upload missing listings")
    args = parser.parse_args()

    if args.upload_reviewed:
        upload_reviewed_csv()
        return
    elif args.upload_missing:
        upload_missing_csv()
        return

    products = get_shopify_products()
    updated_rows, flagged_rows, missing_rows, untouched_rows  = [], [], [], []

    try:

        print(f"🔄 Total products to process: {len(products)}")

        for batch_start in range(0, len(products), 200):
            batch = products[batch_start:batch_start + 200]
            print(f"\n📦 Starting batch {batch_start + 1} to {batch_start + len(batch)}...")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                indexed_batch = [(product, batch_start + i) for i, product in enumerate(batch)]
                futures = {executor.submit(process_product_with_delay, p): p[0] for p in indexed_batch}
                completed = 0
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result_type, data = future.result(timeout=60)
                    except concurrent.futures.TimeoutError:
                        print(f"⏰ Timeout in thread for product: {futures[future].get('title', 'Unknown')}")
                        result_type, data = "missing", {**futures[future], "reason": "Thread timeout"}

                    completed += 1
                    if completed % 10 == 0 or completed == len(batch):
                        print(f"🔁 Processed {completed}/{len(batch)} items in this batch...")

                    if result_type == "updated":
                        updated_rows.append(data)
                    elif result_type == "review":
                        flagged_rows.append(data)
                    elif result_type == "missing":
                        missing_rows.append(data)
                    elif result_type == "untouched":
                        untouched_rows.append(data)
            # Incrementally write each time.
            _write_csv_safe(updated_rows, PUSHED_CSV)
            _write_csv_safe(flagged_rows, REVIEW_CSV)
            _write_csv_safe(untouched_rows, UNTOUCHED_CSV)
            _write_csv_safe([{**r, "price_to_upload": ""} for r in missing_rows], MISSING_CSV)
            # ✅ Sleep after each batch, except the last one
            if batch_start + len(batch) < len(products):
                print(f"🟡 Cooling down to avoid IP rate limit", flush=True)
                for i in range(10, 0, -1):
                    print(f"💤 Still alive… sleeping {i} more minute(s)", flush=True)
                    time.sleep(60)

        _write_csv_safe(updated_rows, PUSHED_CSV)
        _write_csv_safe(flagged_rows, REVIEW_CSV)
        _write_csv_safe(untouched_rows, UNTOUCHED_CSV)
        _write_csv_safe([{**r, "price_to_upload": ""} for r in missing_rows], MISSING_CSV)

        print(f"\n✅ Updates pushed: {len(updated_rows)}")
        print(f"⚠️  Flagged for review: {len(flagged_rows)}")
        print(f"❓ Missing listings: {len(missing_rows)}")
        print(f"👌 Untouched (no change needed): {len(untouched_rows)}")
    except Exception as e:
        print("FATAL error in run_price_sync: ", e)
        traceback.print_exc()
        raise
    finally:
        print("🧹 Final flush (crash-safe)!")
        _write_csv_safe(updated_rows, PUSHED_CSV)
        _write_csv_safe(flagged_rows, REVIEW_CSV)
        _write_csv_safe(untouched_rows, UNTOUCHED_CSV)
        _write_csv_safe([{**r, "price_to_upload": ""} for r in missing_rows],
                        MISSING_CSV)




# === RUN ===
if __name__ == "__main__":
    print("=== ENTER price sync ===")
    run_price_sync()
    print("=== EXIT price sync ===")

