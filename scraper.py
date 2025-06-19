import re
import time
import random
import logging
from typing import Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.webdriver import WebDriver
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )
}

def init_driver() -> WebDriver:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument(f'user-agent={HEADERS["User-Agent"]}')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def extract_price(text: str) -> Optional[float]:
    match = re.search(r"\$([\d,]+\.\d{2})", text)
    if match:
        return float(match.group(1).replace(",", ""))
    return None

def get_featured_price_tcgplayer(tcg_id: str) -> Optional[float]:
    url = f"https://www.tcgplayer.com/product/{tcg_id}/"
    driver = None
    try:
        driver = init_driver()
        driver.get(url)
        time.sleep(random.uniform(3, 5))  # Let JS render

        # Try featured listing first
        featured = driver.find_elements(By.CSS_SELECTOR, ".buybox__price")
        if featured:
            price_text = featured[0].text.strip()
            if "Shipping" in price_text:
                price_parts = price_text.split("Shipping")
                item_price = extract_price(price_parts[0])
                shipping_price = extract_price(price_parts[1]) if len(price_parts) > 1 else 0
                total_price = item_price + shipping_price if item_price else None
            else:
                total_price = extract_price(price_text)
            if total_price:
                return total_price

        # Fallback to lowest listing in standard results
        listings = driver.find_elements(By.CSS_SELECTOR, ".search-result__market-price")
        for listing in listings:
            price = extract_price(listing.text.strip())
            if price:
                return price

        return "NO_LISTING"

    except Exception as e:
        logger.error(f"[{tcg_id}] ❌ Failed to locate featured or fallback price: {e}")
        return "NO_LISTING"
    finally:
        if driver:
            driver.quit()
