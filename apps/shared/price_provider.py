"""
price_provider.py — Unified pricing interface that routes to PPT or Scrydex.

Env var: PRICE_PROVIDER = "ppt" | "scrydex" | "both" (default: "ppt")
    - ppt:     PPTClient only (current behavior)
    - scrydex: ScrydexClient only
    - both:    Scrydex primary, PPT shadow with discrepancy logging

Every service replaces `PPTClient(api_key)` with `create_price_provider()`.
The returned object has the exact same method signatures as PPTClient.
"""

import os
import time
import logging
import threading
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

logger = logging.getLogger(__name__)


class PriceError(Exception):
    """Base exception for pricing provider errors."""
    def __init__(self, message: str, status_code: int = None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


# Re-export constants so callsites can do `from price_provider import FALLBACK_MULTIPLIERS`
from ppt_client import (
    CONDITION_TO_PPT,
    PPT_TO_CONDITION,
    FALLBACK_MULTIPLIERS,
)

# Make PPTError and ScrydexError catchable as PriceError
from ppt_client import PPTError
from scrydex_client import ScrydexError


class PriceProvider:
    """
    Unified pricing interface. Delegates to PPT, Scrydex, or both
    depending on PRICE_PROVIDER env var.
    """

    def __init__(self, primary, shadow=None, mode="ppt", cache=None):
        """
        Args:
            primary: PPTClient or ScrydexClient instance
            shadow: Optional secondary client for comparison logging
            mode: "ppt", "scrydex", or "both"
            cache: Optional PriceCache instance — checked first for ID lookups
        """
        self.primary = primary
        self.shadow = shadow
        self.mode = mode
        self.cache = cache
        self._client_class = type(primary)
        # Determine the live source label from the primary client type
        self._primary_source = "scrydex" if "Scrydex" in type(primary).__name__ else "ppt"

    # ── source attribution ─────────────────────────────────────

    def _stamp(self, result, source: str):
        """Tag a result dict (or list of dicts) with _price_source."""
        if isinstance(result, dict):
            result["_price_source"] = source
        elif isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    item["_price_source"] = source
        return result

    # ── shadow comparison ─────────────────────────────────────────

    def _compare_in_background(self, method_name: str, primary_result, shadow_fn):
        """Run shadow call in background thread and log discrepancies."""
        if not self.shadow:
            return

        def _run():
            try:
                shadow_result = shadow_fn()
                self._log_discrepancy(method_name, primary_result, shadow_result)
            except Exception as e:
                logger.debug(f"Shadow {method_name} failed: {e}")

        t = threading.Thread(target=_run, daemon=True)
        t.start()

    def _log_discrepancy(self, method_name, primary_result, shadow_result):
        """Compare prices from primary vs shadow and log differences > 5%."""
        if primary_result is None or shadow_result is None:
            if primary_result is not None or shadow_result is not None:
                logger.info(
                    f"[price_compare] {method_name}: "
                    f"primary={'found' if primary_result else 'None'} "
                    f"shadow={'found' if shadow_result else 'None'}"
                )
            return

        # Compare market prices
        p_price = self._client_class.extract_market_price(primary_result)
        s_price = type(self.shadow).extract_market_price(shadow_result)

        if p_price is None or s_price is None:
            return
        if p_price == 0 or s_price == 0:
            return

        delta_pct = abs(float(p_price - s_price) / float(p_price)) * 100
        if delta_pct > 5:
            name = primary_result.get("name", "?")
            logger.warning(
                f"[price_compare] {method_name} '{name}': "
                f"primary=${p_price} shadow=${s_price} delta={delta_pct:.1f}%"
            )

    # ── PPT-compatible methods ────────────────────────────────────

    def get_card_by_tcgplayer_id(self, tcgplayer_id, *, include_history=False):
        # Cache-first: local DB read, zero API calls
        if self.cache:
            try:
                cached = self.cache.get_card_by_tcgplayer_id(tcgplayer_id)
                if cached:
                    logger.info(f"Cache HIT: card tcg={tcgplayer_id}")
                    return self._stamp(cached, "cache")
                else:
                    logger.info(f"Cache MISS: card tcg={tcgplayer_id}")
            except Exception as e:
                logger.warning(f"Cache read FAILED for card {tcgplayer_id}: {e}")

        try:
            result = self.primary.get_card_by_tcgplayer_id(
                tcgplayer_id, include_history=include_history
            )
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

        if self.shadow:
            self._compare_in_background(
                "get_card_by_tcgplayer_id",
                result,
                lambda: self.shadow.get_card_by_tcgplayer_id(
                    tcgplayer_id, include_history=include_history
                ),
            )
        return self._stamp(result, self._primary_source)

    def search_cards(self, query, *, set_name=None, limit=5):
        if self.cache:
            try:
                results = self.cache.search_cards(query, set_name=set_name, limit=limit)
                if results:
                    return self._stamp(results, "cache")
            except Exception as e:
                logger.warning(f"Cache search failed: {e}")
        try:
            return self._stamp(
                self.primary.search_cards(query, set_name=set_name, limit=limit),
                self._primary_source,
            )
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

    def get_sealed_product_by_tcgplayer_id(self, tcgplayer_id, *, include_history=False):
        # Cache-first
        if self.cache:
            try:
                cached = self.cache.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
                if cached:
                    logger.info(f"Cache HIT: sealed tcg={tcgplayer_id}")
                    return self._stamp(cached, "cache")
                else:
                    logger.info(f"Cache MISS: sealed tcg={tcgplayer_id}")
            except Exception as e:
                logger.warning(f"Cache read FAILED for sealed {tcgplayer_id}: {e}")

        try:
            result = self.primary.get_sealed_product_by_tcgplayer_id(
                tcgplayer_id, include_history=include_history
            )
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

        if self.shadow:
            self._compare_in_background(
                "get_sealed_product_by_tcgplayer_id",
                result,
                lambda: self.shadow.get_sealed_product_by_tcgplayer_id(
                    tcgplayer_id, include_history=include_history
                ),
            )
        return self._stamp(result, self._primary_source)

    def search_sealed_products(self, query, *, set_name=None, limit=5):
        if self.cache:
            try:
                results = self.cache.search_sealed_products(query, set_name=set_name, limit=limit)
                if results:
                    return self._stamp(results, "cache")
            except Exception as e:
                logger.warning(f"Cache sealed search failed: {e}")
        try:
            return self._stamp(
                self.primary.search_sealed_products(query, set_name=set_name, limit=limit),
                self._primary_source,
            )
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

    @staticmethod
    def _dedup_search(results: list, limit: int) -> list:
        """Deduplicate merged search results. Cache wins over live for same product."""
        if not results:
            return results
        seen_names = {}
        seen_tcg = set()
        deduped = []
        # Cache items come first in the merged list, so they win ties
        for item in results:
            name = (item.get("name") or "").lower().strip()
            tcg_id = item.get("tcgPlayerId") or item.get("tcgplayer_id")
            # Skip if we already have this tcgplayer_id
            if tcg_id and tcg_id in seen_tcg:
                continue
            # Skip if exact same name from a different source
            if name and name in seen_names:
                continue
            if tcg_id:
                seen_tcg.add(tcg_id)
            if name:
                seen_names[name] = True
            deduped.append(item)
        return deduped[:limit]

    def parse_title(self, title, *, fuzzy=True, max_suggestions=5):
        try:
            result = self.primary.parse_title(
                title, fuzzy=fuzzy, max_suggestions=max_suggestions
            )
            return self._stamp(result, self._primary_source)
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

    # ── rate limiting (delegates to primary) ──────────────────────

    def get_rate_limit_info(self) -> dict:
        return self.primary.get_rate_limit_info()

    def should_throttle(self) -> bool:
        return self.primary.should_throttle()

    # ── static price extraction ───────────────────────────────────
    # These are identical between PPTClient and ScrydexClient since
    # ScrydexClient normalizes data to PPT shape. Delegate to primary's class.

    @staticmethod
    def extract_market_price(item_data):
        """Extract market price — works on normalized data from either backend."""
        if not item_data:
            return None
        unopened = item_data.get("unopenedPrice")
        if unopened is not None:
            return Decimal(str(unopened))
        prices = item_data.get("prices", {})
        if isinstance(prices, dict):
            market = prices.get("market") or prices.get("mid")
            if market is not None:
                return Decimal(str(market))
        for key in ("market_price", "marketPrice", "price"):
            val = item_data.get(key)
            if val is not None:
                return Decimal(str(val))
        return None

    @staticmethod
    def extract_variants(card_data) -> dict:
        from ppt_client import PPTClient
        return PPTClient.extract_variants(card_data)

    @staticmethod
    def get_primary_printing(card_data) -> str:
        from ppt_client import PPTClient
        return PPTClient.get_primary_printing(card_data)

    @staticmethod
    def extract_graded_prices(card_data) -> dict:
        from ppt_client import PPTClient
        return PPTClient.extract_graded_prices(card_data)

    @staticmethod
    def get_graded_price(card_data, grade_company: str, grade_value: str):
        from ppt_client import PPTClient
        return PPTClient.get_graded_price(card_data, grade_company, grade_value)

    @staticmethod
    def extract_condition_price(card_data, condition, variant=None):
        from ppt_client import PPTClient
        return PPTClient.extract_condition_price(card_data, condition, variant)


def create_price_provider(db=None) -> PriceProvider:
    """
    Factory: reads env vars and returns a configured PriceProvider.

    Env vars:
        PRICE_PROVIDER:   "ppt" | "scrydex" | "both" (default: "ppt")
        PPT_API_KEY:      PPT API key
        SCRYDEX_API_KEY:  Scrydex API key
        SCRYDEX_TEAM_ID:  Scrydex team ID
    """
    from ppt_client import PPTClient
    from scrydex_client import ScrydexClient

    mode = os.getenv("PRICE_PROVIDER", "ppt").lower().strip()
    ppt_key = os.getenv("PPT_API_KEY", "")
    scrydex_key = os.getenv("SCRYDEX_API_KEY", "")
    scrydex_team = os.getenv("SCRYDEX_TEAM_ID", "")

    # Initialize local price cache if PRICE_CACHE=true (or any mode with db)
    cache = None
    use_cache = os.getenv("PRICE_CACHE", "").lower().strip() in ("true", "1", "yes")
    if use_cache and db:
        try:
            from price_cache import PriceCache
            cache = PriceCache(db)
            logger.info("Price cache enabled — ID lookups will read from local DB first")
        except Exception as e:
            logger.warning(f"Failed to init price cache: {e}")

    if mode == "scrydex":
        if not scrydex_key or not scrydex_team:
            logger.error("PRICE_PROVIDER=scrydex but SCRYDEX_API_KEY/SCRYDEX_TEAM_ID not set")
            raise RuntimeError("Scrydex credentials not configured")
        primary = ScrydexClient(scrydex_key, scrydex_team, db=db)
        return PriceProvider(primary, mode="scrydex", cache=cache)

    elif mode == "both":
        if not scrydex_key or not scrydex_team:
            logger.error("PRICE_PROVIDER=both but Scrydex credentials missing — falling back to PPT")
            primary = PPTClient(ppt_key)
            return PriceProvider(primary, mode="ppt", cache=cache)
        # PPT is primary (safe — known working), Scrydex is shadow (comparison logging)
        primary = PPTClient(ppt_key)
        shadow = ScrydexClient(scrydex_key, scrydex_team, db=db)
        return PriceProvider(primary, shadow=shadow, mode="both", cache=cache)

    else:  # "ppt" (default)
        primary = PPTClient(ppt_key)
        return PriceProvider(primary, mode="ppt", cache=cache)
