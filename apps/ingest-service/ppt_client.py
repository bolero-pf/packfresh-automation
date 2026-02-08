"""
PokemonPriceTracker API Client v3

PPT Response shape for cards:
    {"data": [{"name": "Charizard", "setName": "Base Set", "cardNumber": "4",
               "rarity": "Holo Rare", "tcgPlayerId": 1234,
               "prices": {
                   "market": 103.85, "low": 87.05, "sellers": 28, "listings": 0,
                   "primaryPrinting": "Holofoil",
                   "conditions": {
                       "Damaged": {"price": 49.99, ...},
                       "Heavily Played": {"price": ..., ...},
                       "Lightly Played": {"price": 87.80, ...},
                       "Moderately Played": {"price": 64.95, ...},
                       "Near Mint": {"price": 103.85, ...},
                   },
                   "variants": {
                       "Holofoil": {
                           "Damaged": {"price": 49.99, ...},
                           "Lightly Played": {"price": 87.80, ...},
                           "Near Mint": {"price": 103.85, ...},
                           "Moderately Played": {"price": 64.95, ...},
                           "Heavily Played": {"price": ..., ...},
                       },
                       "Reverse Holofoil": { ... },
                       "Normal": { ... },
                   }
               }}]}

Condition name mapping (PPT -> our codes):
    "Near Mint"         -> NM
    "Lightly Played"    -> LP
    "Moderately Played" -> MP
    "Heavily Played"    -> HP
    "Damaged"           -> DMG
"""

import time
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import requests

logger = logging.getLogger(__name__)

UA = "pack-fresh-intake/1.0"
DEFAULT_HEADERS = {"Accept": "application/json", "User-Agent": UA}

# Mapping between our short codes and PPT's full condition names
CONDITION_TO_PPT = {
    "NM":  "Near Mint",
    "LP":  "Lightly Played",
    "MP":  "Moderately Played",
    "HP":  "Heavily Played",
    "DMG": "Damaged",
}
PPT_TO_CONDITION = {v: k for k, v in CONDITION_TO_PPT.items()}

# Fallback multipliers only used when PPT doesn't return condition data
FALLBACK_MULTIPLIERS = {
    "NM": Decimal("1.00"), "LP": Decimal("0.80"), "MP": Decimal("0.65"),
    "HP": Decimal("0.45"), "DMG": Decimal("0.25"),
}


class PPTError(Exception):
    def __init__(self, message: str, status_code: int = None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class PPTClient:

    def __init__(self, api_key: str, base_url: str = "https://www.pokemonpricetracker.com/api"):
        self.base_url = base_url.rstrip("/")
        self.headers = {**DEFAULT_HEADERS, "Authorization": f"Bearer {api_key}"}

    # ── request engine ───────────────────────────────────────────────

    def _request(self, method, url, *, params=None, json_body=None, max_tries=3):
        last_err = None
        for attempt in range(1, max_tries + 1):
            try:
                r = (requests.get(url, headers=self.headers, params=params, timeout=15)
                     if method == "GET" else
                     requests.post(url, headers=self.headers, json=json_body, timeout=15))
            except requests.exceptions.RequestException as e:
                logger.warning(f"PPT request failed (attempt {attempt}): {e}")
                time.sleep(min(1.0 * attempt, 3.0))
                continue

            if r.status_code < 400:
                return r.json()

            if r.status_code == 429:
                # NEVER sleep long — this blocks gunicorn sync workers.
                # On first 429, do a short retry. On second, fail immediately.
                if attempt < max_tries:
                    wait = min(2.0 * attempt, 5.0)  # max 5s wait
                    logger.warning(f"PPT 429, short retry in {wait:.1f}s (attempt {attempt})")
                    time.sleep(wait)
                    continue
                else:
                    # Extract retry-after info for the caller
                    reset = r.headers.get("X-Ratelimit-Minute-Reset")
                    retry_after = None
                    if reset:
                        try:
                            retry_after = max(0, int(float(reset) - time.time()))
                        except (ValueError, TypeError):
                            pass
                    daily_remaining = r.headers.get("X-RateLimit-Daily-Remaining")
                    msg = "PPT rate limit exceeded"
                    if daily_remaining and int(daily_remaining) <= 0:
                        msg = "PPT daily credit limit reached (100/day on free tier)"
                    elif retry_after:
                        msg = f"PPT rate limited — try again in {retry_after}s"
                    raise PPTError(msg, 429, {"retry_after": retry_after})

            last_err = r
            time.sleep(1.0 * attempt)

        status = last_err.status_code if last_err else "UNKNOWN"
        body = None
        try:
            body = last_err.json() if last_err else None
        except Exception:
            body = last_err.text if last_err else None
        raise PPTError(f"PPT failed after {max_tries} tries: {status}", status, body)

    def _get(self, url, params=None, **kw):
        return self._request("GET", url, params=params, **kw)

    def _post(self, url, json_body=None, **kw):
        return self._request("POST", url, json_body=json_body, **kw)

    @staticmethod
    def _extract_data(resp):
        data = resp.get("data", resp)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and ("name" in data or "productName" in data):
            return [data]
        return []

    # ── card endpoints ───────────────────────────────────────────────

    def get_card_by_tcgplayer_id(self, tcgplayer_id, *, include_history=False):
        params = {"tcgPlayerId": str(int(tcgplayer_id))}
        if include_history:
            params["includeHistory"] = "true"
        items = self._extract_data(self._get(f"{self.base_url}/v2/cards", params))
        return items[0] if items else None

    def search_cards(self, query, *, set_name=None, limit=10):
        """Search cards by name, optionally filtered by set."""
        params = {"search": query, "limit": limit}
        if set_name:
            params["set"] = set_name
        return self._extract_data(self._get(f"{self.base_url}/v2/cards", params))

    # ── sealed product endpoints ─────────────────────────────────────

    def get_sealed_product_by_tcgplayer_id(self, tcgplayer_id, *, include_history=False):
        params = {"tcgPlayerId": str(int(tcgplayer_id))}
        if include_history:
            params["includeHistory"] = "true"
        items = self._extract_data(self._get(f"{self.base_url}/v2/sealed-products", params))
        return items[0] if items else None

    def search_sealed_products(self, query, *, set_name=None, limit=10):
        """Search sealed products by name, optionally filtered by set."""
        params = {"search": query, "limit": limit, "sortBy": "name", "sortOrder": "asc"}
        if set_name:
            params["set"] = set_name
        return self._extract_data(self._get(f"{self.base_url}/v2/sealed-products", params))

    # ── parse-title ──────────────────────────────────────────────────

    def parse_title(self, title, *, fuzzy=True, max_suggestions=5):
        body = {
            "title": title,
            "options": {"fuzzyMatching": fuzzy, "maxSuggestions": max_suggestions, "includeConfidence": True},
        }
        try:
            resp = self._post(f"{self.base_url}/v2/parse-title", body)
            matches = resp.get("matches") or resp.get("data", {}).get("matches", [])
            return matches if isinstance(matches, list) else []
        except PPTError as e:
            logger.warning(f"parse_title failed for '{title}': {e}")
            return []

    # ── price extraction ─────────────────────────────────────────────

    @staticmethod
    def extract_market_price(card_data):
        """Extract NM market price."""
        if not card_data:
            return None
        prices = card_data.get("prices", {})
        if isinstance(prices, dict):
            market = prices.get("market") or prices.get("mid")
            if market is not None:
                return Decimal(str(market))
        for key in ("market_price", "marketPrice", "price"):
            val = card_data.get(key)
            if val is not None:
                return Decimal(str(val))
        return None

    @staticmethod
    def extract_variants(card_data) -> dict:
        """
        Extract all variant → condition → price data from the PPT response.
        
        Returns:
            {
                "Holofoil": {"NM": 103.85, "LP": 87.80, "MP": 64.95, "HP": None, "DMG": 49.99},
                "Reverse Holofoil": {"NM": ..., ...},
                ...
            }
        Also includes the "primaryPrinting" field if available.
        """
        if not card_data:
            return {}

        prices = card_data.get("prices", {})
        if not isinstance(prices, dict):
            return {}

        result = {}
        variants = prices.get("variants", {})

        if variants and isinstance(variants, dict):
            for variant_name, conditions in variants.items():
                if not isinstance(conditions, dict):
                    continue
                variant_prices = {}
                for ppt_cond, cond_data in conditions.items():
                    short_code = PPT_TO_CONDITION.get(ppt_cond)
                    if short_code and isinstance(cond_data, dict):
                        price = cond_data.get("price")
                        variant_prices[short_code] = float(price) if price is not None else None
                if variant_prices:
                    result[variant_name] = variant_prices

        # If no variants found, try the flat "conditions" object
        if not result:
            conditions = prices.get("conditions", {})
            if conditions and isinstance(conditions, dict):
                flat = {}
                for ppt_cond, cond_data in conditions.items():
                    short_code = PPT_TO_CONDITION.get(ppt_cond)
                    if short_code and isinstance(cond_data, dict):
                        price = cond_data.get("price")
                        flat[short_code] = float(price) if price is not None else None
                if flat:
                    variant_label = prices.get("primaryPrinting", "Default")
                    result[variant_label] = flat

        # Last resort: use market price with fallback multipliers
        if not result:
            nm = PPTClient.extract_market_price(card_data)
            if nm is not None:
                result["Default"] = {
                    code: float((nm * mult).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                    for code, mult in FALLBACK_MULTIPLIERS.items()
                }

        return result

    @staticmethod
    def get_primary_printing(card_data) -> str:
        """Get the primary printing variant name (e.g., 'Holofoil')."""
        if not card_data:
            return "Default"
        prices = card_data.get("prices", {})
        return prices.get("primaryPrinting", "Default") if isinstance(prices, dict) else "Default"

    @staticmethod
    def extract_condition_price(card_data, condition, variant=None):
        """
        Get price for a specific condition + variant.
        If variant is None, uses primaryPrinting.
        Falls back to market price × multiplier if structured data unavailable.
        """
        variants = PPTClient.extract_variants(card_data)
        if not variants:
            nm = PPTClient.extract_market_price(card_data)
            if nm is None:
                return None
            mult = FALLBACK_MULTIPLIERS.get(condition, Decimal("1.00"))
            return (nm * mult).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Pick variant
        if variant and variant in variants:
            v = variants[variant]
        else:
            # Use primary printing or first available
            primary = PPTClient.get_primary_printing(card_data)
            v = variants.get(primary) or next(iter(variants.values()))

        price = v.get(condition)
        if price is not None:
            return Decimal(str(price))

        # Condition not available in this variant; fall back to NM × multiplier
        nm = v.get("NM")
        if nm is not None:
            mult = FALLBACK_MULTIPLIERS.get(condition, Decimal("1.00"))
            return (Decimal(str(nm)) * mult).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        return PPTClient.extract_market_price(card_data)
