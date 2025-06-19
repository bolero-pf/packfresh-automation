import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


def get_tcg_low_price(tcgplayer_id):
    url = f"https://www.tcgplayer.com/product/{tcgplayer_id}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        raise Exception(f"Failed to load page for ID {tcgplayer_id}, status {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    # Try to extract the "starting at" price (usually for NM)
    try:
        price_container = soup.select_one("span[class*='product-listing__price']")

        if not price_container:
            # Sometimes it's inside "product-detail__price" if the other fails
            price_container = soup.select_one("span[class*='product-detail__price']")

        price_text = price_container.get_text(strip=True).replace("$", "")
        price = float(price_text)
        return price
    except Exception as e:
        print(f"Error parsing price for ID {tcgplayer_id}: {e}")
        return None