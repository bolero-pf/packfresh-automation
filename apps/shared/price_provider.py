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
                    logger.debug(f"Cache HIT: card tcg={tcgplayer_id}")
                    return self._stamp(cached, "cache")
                else:
                    logger.debug(f"Cache MISS: card tcg={tcgplayer_id}")
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

    def _get_ppt_client(self):
        """Return whichever client is the PPT one (primary or shadow), or None."""
        for client in (self.shadow, self.primary):
            if client and "Scrydex" not in type(client).__name__:
                return client
        return None

    def search_cards(self, query, *, set_name=None, limit=8, all_games=True,
                     cache_only=False, prefer=None):
        """Browse-style card search for any UI that lets an operator pick a
        printing+variant. Cache first (multi-token across name / card_number /
        printed_number / expansion_id / expansion_name — JP sets work because
        ``all_games=True`` drops the game filter), live primary fallback when
        cache is empty. Each result row carries a ``market_price`` field
        (NM-condition price, or ``prices.market`` fallback, or 0) so chip
        pickers can render a price without a second lookup.

        ``cache_only=True`` skips the live fallback — used by price_updater
        raw-rebind, which can only bind to printings that are already in the
        Scrydex cache (the rebind writes scrydex_id; PPT can't supply one,
        and PPT has no JP cards anyway). Also avoids surfacing PPT 401s on
        services that don't have PPT_API_KEY set.

        ``prefer="ppt"`` skips cache + Scrydex entirely and queries PPT
        directly. Operator escape hatch for when Scrydex returns wrong
        matches (e.g. fuzzy hits a different card with the same partial
        name). Raises PriceError if no PPT client is configured.

        Single source of truth — intake (/api/search/cards), ingestion, and
        price_updater (raw-rebind) all go through this. Don't reimplement the
        cache→live orchestration at the call site."""
        if prefer == "ppt":
            ppt_client = self._get_ppt_client()
            if not ppt_client:
                raise PriceError(
                    "PPT not configured — set PPT_API_KEY to enable PPT fallback search",
                    status_code=503,
                )
            try:
                live = ppt_client.search_cards(query, set_name=set_name, limit=limit) or []
                results = self._stamp(live, "ppt")
            except (PPTError, ScrydexError) as e:
                raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e
            for r in results or []:
                if not r.get("market_price"):
                    conds = (r.get("prices") or {}).get("conditions") or {}
                    nm = conds.get("Near Mint") or conds.get("NM") or {}
                    r["market_price"] = nm.get("price") or (r.get("prices") or {}).get("market") or 0
            return results

        results = []
        if self.cache:
            try:
                results = self.cache.search_cards(
                    query, set_name=set_name, limit=limit, all_games=all_games,
                )
                results = self._stamp(results, "cache")
            except Exception as e:
                logger.warning(f"Cache card search failed: {e}")
                results = []
        if not results and not cache_only:
            try:
                live = self.primary.search_cards(
                    query, set_name=set_name, limit=limit,
                ) or []
                results = self._stamp(live, self._primary_source)
            except (PPTError, ScrydexError) as e:
                raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e
        for r in results or []:
            if not r.get("market_price"):
                conds = (r.get("prices") or {}).get("conditions") or {}
                nm = conds.get("Near Mint") or conds.get("NM") or {}
                r["market_price"] = nm.get("price") or (r.get("prices") or {}).get("market") or 0
        return results

    def get_sealed_product_by_tcgplayer_id(self, tcgplayer_id, *, include_history=False):
        # Cache-first
        if self.cache:
            try:
                cached = self.cache.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
                if cached:
                    logger.debug(f"Cache HIT: sealed tcg={tcgplayer_id}")
                    return self._stamp(cached, "cache")
                else:
                    logger.debug(f"Cache MISS: sealed tcg={tcgplayer_id}")
            except Exception as e:
                logger.warning(f"Cache read FAILED for sealed {tcgplayer_id}: {e}")

        try:
            result = self.primary.get_sealed_product_by_tcgplayer_id(
                tcgplayer_id, include_history=include_history
            )
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

        # ScrydexClient._resolve_tcgplayer_id reads from the local
        # scrydex_tcg_map table — it doesn't ask Scrydex's API to resolve a
        # TCG ID at lookup time. So a TCG ID we've never seeded (brand-new
        # sealed product, never searched/synced) misses here even when
        # Scrydex itself knows the mapping. PPT can resolve sealed by TCG ID
        # directly, so fall back synchronously when primary is empty.
        if result is None and self.shadow and self._primary_source == "scrydex":
            try:
                shadow_result = self.shadow.get_sealed_product_by_tcgplayer_id(
                    tcgplayer_id, include_history=include_history
                )
                if shadow_result:
                    logger.info(f"Sealed TCG#{tcgplayer_id}: scrydex miss, ppt sealed-endpoint hit")
                    return self._stamp(shadow_result, "ppt")
                # Some sealed-shaped products (Ultra Premium Collections, single-
                # card promo boxes, etc.) get categorized under PPT's card
                # endpoint rather than sealed-products. Try that before giving
                # up — the response shape is compatible enough for the
                # comparison panel (name + market price).
                card_result = self.shadow.get_card_by_tcgplayer_id(
                    tcgplayer_id, include_history=include_history
                )
                if card_result:
                    logger.info(f"Sealed TCG#{tcgplayer_id}: scrydex miss, ppt card-endpoint hit")
                    return self._stamp(card_result, "ppt")
                logger.info(f"Sealed TCG#{tcgplayer_id}: scrydex AND ppt both miss")
            except (PPTError, ScrydexError) as e:
                logger.warning(f"PPT fallback for sealed TCG#{tcgplayer_id} failed: {e}")

        if result is not None and self.shadow:
            self._compare_in_background(
                "get_sealed_product_by_tcgplayer_id",
                result,
                lambda: self.shadow.get_sealed_product_by_tcgplayer_id(
                    tcgplayer_id, include_history=include_history
                ),
            )
        return self._stamp(result, self._primary_source) if result is not None else None

    def search_sealed_products(self, query, *, set_name=None, limit=5, prefer=None):
        if prefer == "ppt":
            ppt_client = self._get_ppt_client()
            if not ppt_client:
                raise PriceError(
                    "PPT not configured — set PPT_API_KEY to enable PPT fallback search",
                    status_code=503,
                )
            try:
                return self._stamp(
                    ppt_client.search_sealed_products(query, set_name=set_name, limit=limit) or [],
                    "ppt",
                )
            except (PPTError, ScrydexError) as e:
                raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e
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

    # ════════════════════════════════════════════════════════════════
    # Scalar API — the forward-going interface. Cache first, primary
    # fallback. Always USD. Scrydex-native variant names.
    # ════════════════════════════════════════════════════════════════

    def get_raw_condition_price(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        condition: str = "NM", variant: Optional[str] = None,
    ) -> Optional[Decimal]:
        """Return USD raw market price. Cache first, then live primary."""
        if self.cache and (scrydex_id or tcgplayer_id):
            try:
                p = self.cache.get_raw_condition_price(
                    scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                    condition=condition, variant=variant,
                )
                if p is not None:
                    return p
            except Exception as e:
                logger.warning(f"Cache raw-condition lookup failed: {e}")

        if not tcgplayer_id:
            return None
        try:
            return self.primary.get_raw_condition_price(
                tcgplayer_id, condition=condition, variant=variant,
            )
        except (PPTError, ScrydexError) as e:
            raise PriceError(str(e), e.status_code, getattr(e, 'body', None)) from e

    def get_condition_prices(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        variant: Optional[str] = None,
    ) -> dict:
        """{our_code: USD Decimal} for every condition available."""
        if self.cache and (scrydex_id or tcgplayer_id):
            try:
                out = self.cache.get_condition_prices(
                    scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                    variant=variant,
                )
                if out:
                    return out
            except Exception as e:
                logger.warning(f"Cache condition-prices lookup failed: {e}")

        if not tcgplayer_id:
            return {}
        try:
            return self.primary.get_condition_prices(tcgplayer_id, variant=variant)
        except (PPTError, ScrydexError):
            return {}

    def get_graded_price(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        company: str, grade: str, variant: Optional[str] = None,
    ) -> Optional[Decimal]:
        """USD graded market price (PSA 10, etc.). Cache first."""
        if self.cache and (scrydex_id or tcgplayer_id):
            try:
                p = self.cache.get_graded_price(
                    scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                    company=company, grade=grade, variant=variant,
                )
                if p is not None:
                    return p
            except Exception as e:
                logger.warning(f"Cache graded lookup failed: {e}")

        if not tcgplayer_id:
            return None
        try:
            return self.primary.get_graded_price_for(
                tcgplayer_id, company=company, grade=grade,
            )
        except (PPTError, ScrydexError):
            return None

    def get_card_metadata(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
    ) -> Optional[dict]:
        """Scrydex-native card metadata. Cache first, then primary."""
        if self.cache and (scrydex_id or tcgplayer_id):
            try:
                meta = self.cache.get_card_metadata(
                    scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                )
                if meta:
                    return meta
            except Exception as e:
                logger.warning(f"Cache metadata lookup failed: {e}")

        if not tcgplayer_id:
            return None
        try:
            return self.primary.get_card_metadata(tcgplayer_id)
        except (PPTError, ScrydexError):
            return None

    def get_sealed_market_price(self, tcgplayer_id) -> Optional[Decimal]:
        """USD unopened price for a sealed product."""
        if self.cache:
            try:
                p = self.cache.get_sealed_market_price(tcgplayer_id)
                if p is not None:
                    return p
            except Exception as e:
                logger.warning(f"Cache sealed lookup failed: {e}")
        try:
            return self.primary.get_sealed_market_price(tcgplayer_id)
        except (PPTError, ScrydexError):
            return None

    def get_card_view(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
    ) -> Optional[dict]:
        """Everything a condition-picker UI needs in one call. Scrydex-native
        shape, USD throughout.

        Returns None if the card isn't in cache or primary.

        Shape (variant keys are display-cased — "Holofoil", "Normal", "Foil"
        — so they match what /api/search/cards emits and the intake chip
        passes back as preselectedVariant):
            {
                "scrydex_id", "tcgplayer_id", "name", "set_name",
                "card_number", "printed_number", "rarity", "game",
                "image_small" / "image_medium" / "image_large",
                "variants": {
                    "Holofoil": {"NM": Decimal, "LP": Decimal, ...},
                    "Normal":   {...},
                },
                "primary_variant": "Holofoil",
                "graded": {
                    "PSA": {"10": Decimal, "9": Decimal, ...},
                },
            }
        """
        # Single-query cache path — avoids N+1 fan-out across variants.
        if self.cache and (scrydex_id or tcgplayer_id):
            try:
                view = self.cache.get_card_view(
                    scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                )
                if view:
                    return self._stamp(view, "cache")
            except Exception as e:
                logger.warning(f"Cache card-view lookup failed: {e}")

        # Primary fallback: assemble a view from the live client's scalar API.
        # Cache misses for JP / niche cards should be rare; only hit here.
        if not tcgplayer_id:
            return None

        meta = None
        try:
            meta = self.primary.get_card_metadata(tcgplayer_id)
        except (PPTError, ScrydexError):
            pass
        if not meta:
            return None

        # Match the cache path's display-cased variant keys so callers can
        # treat both code paths the same.
        from price_cache import PriceCache
        variants_map: dict = {}
        for v in meta.get("variants") or []:
            try:
                cond_prices = self.primary.get_condition_prices(tcgplayer_id, variant=v)
            except (PPTError, ScrydexError):
                cond_prices = {}
            if cond_prices:
                variants_map[PriceCache._display_variant(v)] = cond_prices

        primary = None
        for candidate in ("Holofoil", "Normal"):
            if candidate in variants_map:
                primary = candidate
                break
        if primary is None and variants_map:
            primary = next(iter(variants_map))

        return self._stamp({
            **{k: meta.get(k) for k in (
                "scrydex_id", "tcgplayer_id", "name",
                "card_number", "printed_number", "rarity", "game",
                "image_small", "image_medium", "image_large",
            )},
            "set_name": meta.get("expansion_name"),
            "expansion_name": meta.get("expansion_name"),
            "variants": variants_map,
            "primary_variant": primary,
            "graded": {},
        }, self._primary_source)


def create_price_provider(db=None, game: str = "pokemon") -> PriceProvider:
    """
    Factory: reads env vars and returns a configured PriceProvider.

    Args:
        db: Database module for cache reads
        game: Game identifier — pokemon, magicthegathering, lorcana, onepiece, riftbound

    Env vars:
        PRICE_PROVIDER:   "ppt" | "scrydex" | "both" (default: "ppt")
        PPT_API_KEY:      PPT API key (Pokemon only)
        SCRYDEX_API_KEY:  Scrydex API key
        SCRYDEX_TEAM_ID:  Scrydex team ID
    """
    from ppt_client import PPTClient
    from scrydex_client import ScrydexClient

    mode = os.getenv("PRICE_PROVIDER", "ppt").lower().strip()
    ppt_key = os.getenv("PPT_API_KEY", "")
    scrydex_key = os.getenv("SCRYDEX_API_KEY", "")
    scrydex_team = os.getenv("SCRYDEX_TEAM_ID", "")

    # Non-Pokemon games: PPT doesn't exist, force scrydex/cache-only
    if game != "pokemon" and mode == "ppt":
        mode = "scrydex"
    if game != "pokemon" and mode == "both":
        mode = "scrydex"

    # Initialize local price cache if PRICE_CACHE=true (or any mode with db)
    cache = None
    use_cache = os.getenv("PRICE_CACHE", "").lower().strip() in ("true", "1", "yes")
    if use_cache and db:
        try:
            from price_cache import PriceCache
            cache = PriceCache(db, game=game)
            logger.info(f"Price cache enabled for {game} — ID lookups will read from local DB first")
        except Exception as e:
            logger.warning(f"Failed to init price cache: {e}")

    if mode == "scrydex":
        if not scrydex_key or not scrydex_team:
            logger.error("PRICE_PROVIDER=scrydex but SCRYDEX_API_KEY/SCRYDEX_TEAM_ID not set")
            raise RuntimeError("Scrydex credentials not configured")
        primary = ScrydexClient(scrydex_key, scrydex_team, db=db, game=game)
        return PriceProvider(primary, mode="scrydex", cache=cache)

    elif mode == "both":
        if not scrydex_key or not scrydex_team:
            logger.error("PRICE_PROVIDER=both but Scrydex credentials missing — falling back to PPT")
            primary = PPTClient(ppt_key)
            return PriceProvider(primary, mode="ppt", cache=cache)
        primary = ScrydexClient(scrydex_key, scrydex_team, db=db, game=game)
        shadow = PPTClient(ppt_key) if ppt_key else None
        return PriceProvider(primary, shadow=shadow, mode="both", cache=cache)

    else:  # "ppt" (default)
        primary = PPTClient(ppt_key)
        return PriceProvider(primary, mode="ppt", cache=cache)
