
def scrape_store(driver):
    print("[1] Scraping /store...")
    driver.get("https://platform.rarecandy.com/store")
    time.sleep(2)
    return scroll_store(driver, "div.group\\/ListingCard")



import time
import pandas as pd
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
def fetch_shopify_inventory():
    import requests

    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": "***REMOVED******REMOVED***"
    }

    url = "https://***REMOVED***.myshopify.com/admin/api/2023-10/graphql.json"

    all_items = []
    has_next_page = True
    cursor = None

    while has_next_page:
        query = """
        query ($cursor: String) {
          products(first: 250, after: $cursor) {
            pageInfo {
              hasNextPage
            }
            edges {
              cursor
              node {
                title
                variants(first: 10) {
                  edges {
                    node {
                      title
                      price
                      inventoryQuantity
                    }
                  }
                }
              }
            }
          }
        }
        """

        variables = {"cursor": cursor} if cursor else {}

        response = requests.post(url, headers=headers, json={"query": query, "variables": variables})
        data = response.json()

        products = data["data"]["products"]["edges"]
        for edge in products:
            cursor = edge["cursor"]
            product = edge["node"]
            product_name = product["title"]

            for variant_edge in product["variants"]["edges"]:
                variant = variant_edge["node"]
                variant_name = variant["title"]
                full_name = f"{product_name} - {variant_name}" if variant_name != "Default Title" else product_name
                all_items.append({
                    "name": full_name.strip(),
                    "shopify_price": variant["price"],
                    "shopify_qty": variant["inventoryQuantity"]
                })

        has_next_page = data["data"]["products"]["pageInfo"]["hasNextPage"]

    return all_items

def init_driver(headless=False):
    chrome_options = Options()
    chrome_options.add_argument("--user-data-dir=C:/Users/Sean/.rare_candy_chrome_profile")
    chrome_options.add_argument("--profile-directory=Default")
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(options=chrome_options)
def scroll_inventory_items_and_parse(driver, pause_time=0.6, max_tries=6):
    from collections import OrderedDict
    from selenium.common.exceptions import StaleElementReferenceException
    import time

    try:
        safe_click_target = driver.find_element(By.CSS_SELECTOR, "div.css-1rynq56.text-xl")
        ActionChains(driver).move_to_element(safe_click_target).click().perform()
        driver.execute_script("arguments[0].focus();", safe_click_target)
    except Exception as e:
        print(f"[!] Couldn't click scroll activator for inventory: {e}")

    seen = set()
    parsed_inventory = OrderedDict()
    tries = 0
    scrolls = 0
    start_time = time.time()
    TIMEOUT_SECONDS = 300

    while tries < max_tries:
        if time.time() - start_time > TIMEOUT_SECONDS:
            print("[!] Scroll timeout hit — exiting.")
            break

        ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(pause_time)

        cards = driver.find_elements(By.CSS_SELECTOR, "div.group\\/inventoryItem")
        new_found = 0

        for card in cards[-36:]:
            try:
                field_texts = [f.text.strip() for f in card.find_elements(By.CSS_SELECTOR, ".css-1rynq56")]
                name = field_texts[0] if field_texts else None

                card_key = (name, card.location['y'])

                if not name or card_key in seen:
                    continue
                seen.add(card_key)

                listed = None
                for i, text in enumerate(field_texts):
                    if text.upper() == "LISTED" and i + 1 < len(field_texts):
                        try:
                            listed = int(field_texts[i + 1])
                        except:
                            listed = 0

                if name and listed is not None:
                    parsed_inventory[name] = listed
                    new_found += 1

            except StaleElementReferenceException:
                continue
            except Exception as e:
                print(f"[!] Error parsing scrolling inventory card: {e}")

        print(f"[scroll {scrolls}] +{new_found} new cards ({len(parsed_inventory)} total)")
        scrolls += 1

        if new_found == 0:
            tries += 1
        else:
            tries = 0

    print(f"[✓] Finished scrolling & parsing. Found {len(parsed_inventory)} inventory entries.")
    return parsed_inventory

def scroll_store(driver, selector, pause_time=1.25, max_tries=8):
    body = driver.find_element(By.TAG_NAME, "body")
    ActionChains(driver).move_to_element(body).click().perform()
    driver.execute_script("arguments[0].focus();", body)

    seen = set()
    scrolls = 0
    tries = 0
    last_seen_count = 0
    start_time = time.time()
    TIMEOUT_SECONDS = 300

    while tries < max_tries:
        if time.time() - start_time > TIMEOUT_SECONDS:
            print("[!] Store scroll timeout hit — exiting.")
            break

        cards = driver.find_elements(By.CSS_SELECTOR, selector)
        current_seen_count = len(cards)

        if current_seen_count == last_seen_count:
            tries += 1
        else:
            tries = 0
            last_seen_count = current_seen_count

        ActionChains(driver).send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(pause_time)
        scrolls += 1

    print(f"[✓] Finished store scroll. Found {last_seen_count} items.")
    return cards
def parse_store_cards(card_elements):
    parsed = []

    for el in card_elements:
        try:
            name = el.find_element(By.CSS_SELECTOR, "div.css-1rynq56").text.strip()

            # Try to find quantity text (may not exist if discount is shown instead)
            try:
                qty_text = el.find_element(By.XPATH, ".//div[contains(text(), 'AVAILABLE') or contains(text(), 'ONLY') or contains(text(), 'SOLD OUT')]").text.strip()
                if "SOLD OUT" in qty_text:
                    qty = 0
                elif "ONLY" in qty_text:
                    qty = int("".join(filter(str.isdigit, qty_text)))
                else:
                    qty = int(qty_text.split()[0])
            except:
                qty = None  # will patch from inventory_map later

            # Find price if it's visible
            try:
                price = el.find_element(By.XPATH, ".//div[contains(text(), '$')]").text.strip()
            except:
                price = None

            parsed.append({
                "name": name,
                "rc_qty": qty,
                "price": price
            })

        except Exception as e:
            print(f"[!] Error parsing card: {e}")

    return parsed


def scrape_inventory(driver):
    print("[2] Scraping /inventory for quantity reconciliation...")
    driver.get("https://platform.rarecandy.com/inventory")
    time.sleep(2)
    return scroll_inventory_items_and_parse(driver)

def reconcile_quantities(items, inventory_map):
    print("[3] Reconciling missing quantities...")
    patched = 0

    for item in items:
        if item["rc_qty"] is None and item["name"] in inventory_map:
            item["rc_qty"] = inventory_map[item["name"]]
            patched += 1

    print(f"[✓] Patched {patched} items from /inventory.")
    return items

def run_full_scrape():
    driver = init_driver(headless=False)

    try:
       # store_items = parse_store_cards(scrape_store(driver))
        #inventory_map = scrape_inventory(driver)
        #full_items = reconcile_quantities(store_items, inventory_map)

#        df_rc = pd.DataFrame(full_items)
#        df_rc.to_csv("rare_candy_inventory.csv", index=False)
 #       print(f"[💾] Saved {len(df)} items to rare_candy_inventory.csv")


        df_rc = pd.read_csv(".venv/Scripts/rare_candy_inventory.csv")
        shopify_items = fetch_shopify_inventory()
        df_shopify = pd.DataFrame(shopify_items)
        df_shopify.to_csv("shopify_inventory.csv", index=False)
        print(f"[💾] Saved {len(df_shopify)} Shopify items to shopify_inventory.csv")

        merged = pd.merge(df_rc, df_shopify, on="name", how="outer")
        merged.to_csv("merged_inventory.csv", index=False)
        print(f"[🔁] Merged inventory saved to merged_inventory.csv")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_full_scrape()
