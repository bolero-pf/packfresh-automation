"""
PokemonPriceTracker API Client
Adapted for intake service needs: card lookup, sealed product lookup, price verification.
Based on existing PPTClient but streamlined for this service's use cases.
"""

import time
import logging
from decimal import Decimal
from typing import Optional

import requests

logger = logging.getLogger(__name__)

UA = "pack-fresh-intake/1.0"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": UA,
}


class PPTError(Exception):
    """PPT API error with status code and body"""
    def __init__(self, message: str, status_code: int = None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class PPTClient:
    """
    Client for PokemonPriceTracker API v2.
    
    Endpoints used:
        GET  /api/v2/cards              - Raw card lookup by tcgPlayerId
        GET  /api/v2/sealed-products    - Sealed product lookup by tcgPlayerId
        POST /api/v2/parse-title        - Fuzzy match product names to find tcgPlayerId
    """

    def __init__(self, api_key: str, base_url: str = "https://www.pokemonpricetracker.com/api"):
        self.base_url = base_url.rstrip("/")
        self.headers = {**DEFAULT_HEADERS, "Authorization": f"Bearer {api_key}"}

    # ------------------------------------------------------------------
    # Internal: request with backoff + rate-limit respect
    # ------------------------------------------------------------------

    def _request(self, method: str, url: str, *, params: dict = None,
                 json_body: dict = None, max_tries: int = 4):
        """HTTP request with retry, backoff, and rate-limit handling."""
        last_err = None

        for attempt in range(1, max_tries + 1):
            try:
                if method == "GET":
                    r = requests.get(url, headers=self.headers, params=params, timeout=15)
                elif method == "POST":
                    r = requests.post(url, headers=self.headers, json=json_body, timeout=15)
                else:
                    raise ValueError(f"Unsupported method: {method}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"PPT request failed (attempt {attempt}): {e}")
                time.sleep(1.0 * attempt)
                continue

            # Happy path
            if r.status_code < 400:
                # Respect rate limit headers if close to exhaustion
                mr = r.headers.get("X-Ratelimit-Minute-Remaining")
                reset = r.headers.get("X-Ratelimit-Minute-Reset")
                if mr is not None and reset is not None:
                    try:
                        if int(mr) <= 1:
                            wait = max(0.0, float(reset) - time.time() + 0.5)
                            logger.info(f"PPT rate limit near exhaustion, sleeping {wait:.1f}s")
                            time.sleep(wait)
                    except (ValueError, TypeError):
                        pass
                return r.json()

            # 429: rate limited
            if r.status_code == 429:
                reset = r.headers.get("X-Ratelimit-Minute-Reset")
                wait = 2.0 + attempt
                if reset:
                    try:
                        wait = max(0.0, float(reset) - time.time() + 0.5)
                    except (ValueError, TypeError):
                        pass
                logger.warning(f"PPT 429 rate limited, sleeping {wait:.1f}s (attempt {attempt})")
                time.sleep(wait)
                continue

            # Other errors
            last_err = r
            time.sleep(1.0 * attempt)

        # Exhausted retries
        status = last_err.status_code if last_err else "UNKNOWN"
        body = None
        try:
            body = last_err.json() if last_err else None
        except Exception:
            body = last_err.text if last_err else None
        raise PPTError(f"PPT API failed after {max_tries} attempts: {status}", status, body)

    def _get(self, url: str, params: dict = None, **kwargs):
        return self._request("GET", url, params=params, **kwargs)

    def _post(self, url: str, json_body: dict = None, **kwargs):
        return self._request("POST", url, json_body=json_body, **kwargs)

    # ------------------------------------------------------------------
    # Public: Raw card lookup
    # ------------------------------------------------------------------

    def get_card_by_tcgplayer_id(self, tcgplayer_id: int, *,
                                  days: int = 7,
                                  include_history: bool = False) -> Optional[dict]:
        """
        Look up a raw card by tcgPlayerId.
        Returns card data including prices.market and condition-based pricing.
        
        Response shape (relevant fields):
        {
            "data": [{
                "name": "Charizard ex",
                "setName": "Obsidian Flames",
                "cardNumber": "125",
                "rarity": "...",
                "prices": {"market": 125.50, "low": ..., "mid": ..., "high": ...},
                "tcgPlayerId": 490294,
                ...
            }]
        }
        """
        url = f"{self.base_url}/v2/cards"
        params = {
            "tcgPlayerId": str(int(tcgplayer_id)),
            "days": int(days),
        }
        if include_history:
            params["includeHistory"] = "true"

        resp = self._get(url, params)

        # Extract from response
        data = resp.get("data", resp)
        if isinstance(data, list):
            return data[0] if data else None
        elif isinstance(data, dict) and "name" in data:
            return data
        return None

    # ------------------------------------------------------------------
    # Public: Sealed product lookup
    # ------------------------------------------------------------------

    def get_sealed_product_by_tcgplayer_id(self, tcgplayer_id: int, *,
                                            days: int = 7,
                                            include_history: bool = False) -> Optional[dict]:
        """
        Look up a sealed product by tcgPlayerId.
        Uses the dedicated /v2/sealed-products endpoint.
        
        Response includes market price and recent sales data.
        """
        url = f"{self.base_url}/v2/sealed-products"
        params = {
            "tcgPlayerId": str(int(tcgplayer_id)),
            "days": int(days),
        }
        if include_history:
            params["includeHistory"] = "true"

        resp = self._get(url, params)

        data = resp.get("data", resp)
        if isinstance(data, list):
            return data[0] if data else None
        elif isinstance(data, dict) and "name" in data:
            return data
        return None

    # ------------------------------------------------------------------
    # Public: Fuzzy title matching (for auto-suggesting tcgplayer IDs)
    # ------------------------------------------------------------------

    def parse_title(self, title: str, *, fuzzy: bool = True,
                    max_suggestions: int = 5) -> list[dict]:
        """
        Use the parse-title endpoint to fuzzy-match a product name.
        Useful for suggesting tcgplayer_id when staff is mapping Collectr items.
        
        Returns list of matches with confidence scores:
        [{"card": {"name": ..., "tcgPlayerId": ..., "prices": {...}}, "confidence": 0.95}, ...]
        """
        url = f"{self.base_url}/v2/parse-title"
        body = {
            "title": title,
            "options": {
                "fuzzyMatching": fuzzy,
                "maxSuggestions": max_suggestions,
                "includeConfidence": True,
            }
        }

        try:
            resp = self._post(url, body)
            matches = resp.get("matches") or resp.get("data", {}).get("matches", [])
            return matches if isinstance(matches, list) else []
        except PPTError as e:
            logger.warning(f"parse_title failed for '{title}': {e}")
            return []

    # ------------------------------------------------------------------
    # Helpers: Extract standardized pricing from PPT responses
    # ------------------------------------------------------------------

    @staticmethod
    def extract_market_price(card_data: dict) -> Optional[Decimal]:
        """Extract market price from a PPT card or sealed-product response."""
        if not card_data:
            return None

        prices = card_data.get("prices", {})
        if isinstance(prices, dict):
            market = prices.get("market") or prices.get("mid")
            if market is not None:
                return Decimal(str(market))

        # Fallback: check top-level fields
        for key in ("market_price", "marketPrice", "price"):
            val = card_data.get(key)
            if val is not None:
                return Decimal(str(val))

        return None

    @staticmethod
    def extract_condition_price(card_data: dict, condition: str) -> Optional[Decimal]:
        """
        Extract condition-specific price from a PPT card response.
        PPT includes TCGPlayer condition pricing in the response.
        
        Condition mapping:
            NM  -> Near Mint
            LP  -> Lightly Played  
            MP  -> Moderately Played
            HP  -> Heavily Played
            DMG -> Damaged
        """
        CONDITION_MAP = {
            "NM": "nearMint",
            "LP": "lightlyPlayed",
            "MP": "moderatelyPlayed",
            "HP": "heavilyPlayed",
            "DMG": "damaged",
            # Full name variants
            "Near Mint": "nearMint",
            "Lightly Played": "lightlyPlayed",
            "Moderately Played": "moderatelyPlayed",
            "Heavily Played": "heavilyPlayed",
            "Damaged": "damaged",
        }

        if not card_data:
            return None

        mapped = CONDITION_MAP.get(condition, "nearMint")
        prices = card_data.get("prices", {})

        # Try direct condition key
        if isinstance(prices, dict):
            val = prices.get(mapped)
            if val is not None:
                return Decimal(str(val))

        # Fallback to market price
        return PPTClient.extract_market_price(card_data)
