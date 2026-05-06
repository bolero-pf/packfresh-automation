"""
Scrydex API Client — primary data source for card prices.

Scrydex uses its own IDs (e.g. "base1-4"); TCGPlayer IDs are exposed inside
variants[].marketplaces[] (one per variant) and a mapping table
(scrydex_tcg_map) bridges the two ID systems for reverse lookups.

There are two APIs here:

1. **Scalar instance API** — `get_raw_condition_price`, `get_condition_prices`,
   `get_graded_price_for`, `get_card_metadata`, `get_sealed_market_price`.
   These read native Scrydex fields (variants[].prices[]) and return Decimal
   prices in USD. JP-marketplace JPY rows are converted inline via
   SCRYDEX_JPY_USD_RATE. This is the forward-going interface — callers should
   use these (directly, or through PriceProvider which is cache-first).

2. **Legacy PPT-shaped normalization** — `get_card_by_tcgplayer_id`,
   `get_card_by_id`, `_normalize_card`, `_build_prices_object`. These emit a
   dict matching PPTClient's shape so bulk comparison flows (intake session
   compare) can share code. Prices are now USD-converted at build time
   (historical bug fix — see _to_usd usage in _build_prices_object), so any
   caller touching these dicts gets USD. New callers should use the scalar
   API above instead.

Scrydex response shape for cards:
    {"data": [{"id": "base1-4", "name": "Charizard", "number": "4",
               "expansion": {"id": "base1", "name": "Base", ...},
               "variants": [{"name": "holofoil", "prices": [
                   {"condition": "NM", "type": "raw", "market": 103.85, "low": 87.05},
                   {"condition": "NM", "type": "graded", "company": "PSA", "grade": "10", "market": 1500}
               ]}]}]}

Condition mapping (Scrydex -> our codes):
    "NM"  -> NM
    "LP"  -> LP
    "MP"  -> MP
    "HP"  -> HP
    "DM"  -> DMG
    "U"   -> UNOPENED (sealed only)

Variant name mapping (Scrydex -> PPT display names):
    "normal"            -> "Normal"
    "holofoil"          -> "Holofoil"
    "reverseHolofoil"   -> "Reverse Holofoil"
"""

import os
import time
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Same env var / default as shared/price_cache.py and ingestion/app.py. Scrydex
# sends JP-marketplace raw rows in JPY; convert inline so the scalar API always
# returns USD. eBay-scraped graded rows already come back in USD.
_JPY_USD_RATE = Decimal(os.getenv("SCRYDEX_JPY_USD_RATE", "0.0066"))


def _to_usd(price, currency: Optional[str]) -> Optional[Decimal]:
    """Convert a Scrydex price to USD based on its currency field."""
    if price is None:
        return None
    dec = Decimal(str(price))
    if (currency or "USD").upper() == "JPY":
        dec = dec * _JPY_USD_RATE
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

UA = "pack-fresh/1.0"
DEFAULT_HEADERS = {"Accept": "application/json", "User-Agent": UA}

# Match PPT's condition mapping exactly
CONDITION_TO_PPT = {
    "NM":  "Near Mint",
    "LP":  "Lightly Played",
    "MP":  "Moderately Played",
    "HP":  "Heavily Played",
    "DMG": "Damaged",
}
PPT_TO_CONDITION = {v: k for k, v in CONDITION_TO_PPT.items()}

# Scrydex uses short codes; map to PPT's full names for normalization
SCRYDEX_CONDITION_TO_PPT = {
    "NM": "Near Mint",
    "LP": "Lightly Played",
    "MP": "Moderately Played",
    "HP": "Heavily Played",
    "DM": "Damaged",  # Scrydex uses DM, not DMG
}
SCRYDEX_CONDITION_TO_SHORT = {
    "NM": "NM", "LP": "LP", "MP": "MP", "HP": "HP", "DM": "DMG",
}

# Scrydex variant names -> PPT display names
VARIANT_DISPLAY = {
    "normal": "Normal",
    "holofoil": "Holofoil",
    "reverseHolofoil": "Reverse Holofoil",
    "unlimitedHolofoil": "Holofoil",
    "firstEditionHolofoil": "1st Edition Holofoil",
    "firstEditionShadowlessHolofoil": "1st Edition Shadowless Holofoil",
    "unlimitedShadowlessHolofoil": "Shadowless Holofoil",
}

# Reverse map: PPT grade keys -> (company, grade) for graded price normalization
GRADE_KEY_MAP = {
    "psa10":  ("PSA", "10"),
    "psa9":   ("PSA", "9"),
    "psa8":   ("PSA", "8"),
    "psa7":   ("PSA", "7"),
    "bgs10":  ("BGS", "10"),
    "bgs9.5": ("BGS", "9.5"),
    "bgs9":   ("BGS", "9"),
    "bgs8":   ("BGS", "8"),
    "cgc10":  ("CGC", "10"),
    "cgc9":   ("CGC", "9"),
    "cgc8":   ("CGC", "8"),
    "sgc10":  ("SGC", "10"),
    "sgc9":   ("SGC", "9"),
    "tag10":  ("TAG", "10"),
    "tag9":   ("TAG", "9"),
    "tag8":   ("TAG", "8"),
}

# Reverse: (company, grade) -> PPT key
GRADE_TO_KEY = {v: k for k, v in GRADE_KEY_MAP.items()}


def _match_condition(ppt_cond: str) -> str | None:
    """Match a PPT condition string to a short code (same as ppt_client)."""
    short = PPT_TO_CONDITION.get(ppt_cond)
    if short:
        return short
    for full_name, code in PPT_TO_CONDITION.items():
        if ppt_cond.startswith(full_name):
            return code
    return None


def _scrub_query(q: str) -> str:
    """Sanitize a free-text query for Scrydex's q= param.

    Scrydex uses Lucene-flavored search syntax. Apostrophes truncate the
    query at the first occurrence (everything after gets dropped), and
    parentheses + colons are operators that produce 400s on free text.
    Replace those characters with spaces so a query like
    "Team Rocket's Moltres ex Ultra Premium" becomes
    "Team Rocket s Moltres ex Ultra Premium" — Scrydex's tokenizer then
    matches the meaningful terms instead of bailing on the first apostrophe.
    """
    if not q:
        return ""
    out = []
    for ch in q:
        if ch in "'’()[]{}:\"":
            out.append(" ")
        else:
            out.append(ch)
    return " ".join("".join(out).split())


class ScrydexError(Exception):
    def __init__(self, message: str, status_code: int = None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class ScrydexClient:

    # Supported game path prefixes for Scrydex API
    GAMES = ("pokemon", "magicthegathering", "lorcana", "onepiece", "riftbound")

    # Process-wide negative cache for 5xx responses. Shared across instances
    # because every PriceProvider creates its own ScrydexClient — instance-
    # local would let us re-hit the same broken URL from another request.
    _failed_urls: dict = {}
    _NEGATIVE_CACHE_TTL = 300.0  # 5 minutes

    def __init__(self, api_key: str, team_id: str,
                 base_url: str = "https://api.scrydex.com",
                 db=None, game: str = "pokemon"):
        """
        Args:
            api_key: Scrydex API key
            team_id: Scrydex team ID
            base_url: API base URL
            db: Optional database connection for TCGPlayer ID mapping table
            game: Game identifier — pokemon, magicthegathering, lorcana, onepiece, riftbound
        """
        self.base_url = base_url.rstrip("/")
        self.game = game
        self.headers = {
            **DEFAULT_HEADERS,
            "X-Api-Key": api_key,
            "X-Team-ID": team_id,
        }
        self.db = db
        # Rate limiting — 100 req/sec hard limit
        self._request_times: list[float] = []  # sliding window
        self._credits_remaining = None
        # Negative cache for known-failing URLs (5xx after retries exhausted).
        # Some Scrydex sealed/card pages return 500 persistently (broken on
        # their side); without this we'd burn ~3-4s of retries on every call.

    # ── rate limiting ─────────────────────────────────────────────

    def get_rate_limit_info(self) -> dict:
        return {
            "minute_remaining": None,  # Scrydex doesn't have per-minute limits
            "daily_remaining": self._credits_remaining,
            "retry_after": None,
        }

    def should_throttle(self) -> bool:
        """Check if we're near the 100 req/sec hard limit."""
        now = time.time()
        # Prune requests older than 1 second
        self._request_times = [t for t in self._request_times if now - t < 1.0]
        return len(self._request_times) >= 95  # leave 5 req/sec headroom

    # ── request engine ────────────────────────────────────────────

    def _request(self, method, url, *, params=None, max_tries=3):
        if self.should_throttle():
            time.sleep(0.1)  # brief pause to stay under rate limit

        # Negative cache: short-circuit known-failing URLs so a single broken
        # sealed product (e.g. /sealed/mep-23 returning 500) doesn't burn 3-4s
        # of retries on every page load. URL alone is the key — Scrydex sealed
        # responses don't vary by params we send.
        skip_until = self._failed_urls.get(url)
        if skip_until and time.time() < skip_until:
            raise ScrydexError(
                f"Scrydex {url} in negative cache (recent 5xx, retry after {int(skip_until - time.time())}s)",
                503,
            )

        last_err = None
        for attempt in range(1, max_tries + 1):
            try:
                self._request_times.append(time.time())
                logger.info(f"Scrydex {method} {url} params={params}")
                r = requests.request(method, url, headers=self.headers,
                                     params=params, timeout=15)
            except requests.exceptions.RequestException as e:
                logger.warning(f"Scrydex request failed (attempt {attempt}): {e}")
                time.sleep(min(1.0 * attempt, 3.0))
                continue

            logger.info(f"Scrydex response: status={r.status_code}")

            if r.status_code < 400:
                return r.json()

            if r.status_code == 429:
                if attempt < max_tries:
                    wait = min(2 ** attempt, 10)
                    logger.warning(f"Scrydex 429 — sleeping {wait}s (attempt {attempt}/{max_tries})")
                    time.sleep(wait)
                    continue
                raise ScrydexError("Scrydex rate limited", 429)

            if 400 <= r.status_code < 500:
                try:
                    body = r.json()
                except Exception:
                    body = r.text
                raise ScrydexError(f"Scrydex {r.status_code}", r.status_code, body)

            # 5xx — retry
            last_err = r
            time.sleep(1.0 * attempt)

        # All retries exhausted on 5xx — mark URL as known-failing for TTL.
        self._failed_urls[url] = time.time() + self._NEGATIVE_CACHE_TTL
        status = last_err.status_code if last_err else "UNKNOWN"
        raise ScrydexError(f"Scrydex failed after {max_tries} tries: {status}", status)

    def _get(self, url, params=None, **kw):
        return self._request("GET", url, params=params, **kw)

    # ── TCGPlayer ID mapping ──────────────────────────────────────

    def _resolve_tcgplayer_id(self, tcgplayer_id: int) -> Optional[str]:
        """Look up Scrydex card ID from TCGPlayer ID via mapping table."""
        if not self.db:
            return None
        row = self.db.query_one(
            "SELECT scrydex_id FROM scrydex_tcg_map WHERE tcgplayer_id = %s",
            (int(tcgplayer_id),)
        )
        return row["scrydex_id"] if row else None

    def _save_tcg_mapping(self, scrydex_id: str, tcgplayer_id: int):
        """Save a Scrydex ID -> TCGPlayer ID mapping. Multiple rows per
        scrydex_id are allowed (one per variant: normal/altArt/foil/etc.)."""
        if not self.db or not tcgplayer_id:
            return
        try:
            self.db.execute("""
                INSERT INTO scrydex_tcg_map (scrydex_id, tcgplayer_id)
                VALUES (%s, %s)
                ON CONFLICT (scrydex_id, tcgplayer_id) DO UPDATE SET updated_at = NOW()
            """, (scrydex_id, int(tcgplayer_id)))
        except Exception as e:
            logger.warning(f"Failed to save TCG mapping {scrydex_id} -> {tcgplayer_id}: {e}")

    # ── normalization (Scrydex -> PPT shape) ──────────────────────

    @staticmethod
    def _get_image_urls(raw: dict) -> tuple[str, str, str]:
        """Extract image URLs from Scrydex images array."""
        images = raw.get("images") or []
        for img in images:
            if img.get("type") == "front":
                return (
                    img.get("large", ""),
                    img.get("medium", ""),
                    img.get("small", ""),
                )
        # Fallback: first image
        if images:
            img = images[0]
            return img.get("large", ""), img.get("medium", ""), img.get("small", "")
        return ("", "", "")

    @staticmethod
    def _variant_display_name(scrydex_name: str) -> str:
        """Convert Scrydex camelCase variant name to PPT display name."""
        return VARIANT_DISPLAY.get(scrydex_name, scrydex_name.replace("_", " ").title())

    @staticmethod
    def _build_prices_object(variants: list[dict]) -> dict:
        """
        Convert Scrydex variants array to PPT's prices object:
        {market, primaryPrinting, conditions: {...}, variants: {...}}
        """
        result = {
            "market": None,
            "low": None,
            "primaryPrinting": "Default",
            "conditions": {},
            "variants": {},
        }

        # Track NM prices per variant to determine primary
        variant_nm: dict[str, float] = {}

        for v in (variants or []):
            v_name = v.get("name", "normal")
            display_name = ScrydexClient._variant_display_name(v_name)
            v_conditions = {}

            for p in (v.get("prices") or []):
                if p.get("type") != "raw":
                    continue  # graded prices handled separately
                cond_short = SCRYDEX_CONDITION_TO_SHORT.get(p.get("condition", ""), None)
                if not cond_short:
                    continue
                ppt_cond_name = CONDITION_TO_PPT.get(cond_short, p.get("condition"))
                # Convert JPY → USD at build time so the normalized dict is
                # always USD (eBay-sourced graded rows are already USD; this
                # converter is a no-op for them).
                currency = p.get("currency")
                market_usd = _to_usd(p.get("market"), currency)
                low_usd = _to_usd(p.get("low"), currency)
                market = float(market_usd) if market_usd is not None else None
                low = float(low_usd) if low_usd is not None else None
                v_conditions[ppt_cond_name] = {"price": market, "low": low}

                if cond_short == "NM" and market is not None:
                    variant_nm[display_name] = market

            # Per-variant image (OP altArt has its own /OP14-041A/large URL).
            # Underscore-prefixed keys live alongside condition names so they
            # don't collide with NM/LP/etc. lookups.
            v_images = v.get("images") or []
            for img in v_images:
                if img.get("type") == "front":
                    v_conditions["_image_small"] = img.get("small")
                    v_conditions["_image_medium"] = img.get("medium")
                    v_conditions["_image_large"] = img.get("large")
                    break

            # Per-variant TCGPlayer ID (OP14-041 normal=668333, altArt=668335)
            for mp in (v.get("marketplaces") or []):
                if mp.get("name") == "tcgplayer" and mp.get("product_id"):
                    try:
                        v_conditions["_tcgplayer_id"] = int(mp["product_id"])
                    except (ValueError, TypeError):
                        pass
                    break

            if v_conditions:
                result["variants"][display_name] = v_conditions

        # Primary variant: prefer "Normal" if it exists (most common printing),
        # otherwise pick the variant with the LOWEST NM price (the common one,
        # not the chase variant). This matches PPT's primaryPrinting behavior.
        if "Normal" in variant_nm:
            primary_variant = "Normal"
        elif variant_nm:
            primary_variant = min(variant_nm, key=variant_nm.get)
        elif result["variants"]:
            primary_variant = next(iter(result["variants"]))
        else:
            primary_variant = None

        if primary_variant:
            result["primaryPrinting"] = primary_variant

        result["market"] = variant_nm.get(primary_variant) if primary_variant else None

        # Build flat conditions from primary variant
        primary = result["primaryPrinting"]
        if primary in result["variants"]:
            result["conditions"] = dict(result["variants"][primary])

        return result

    @staticmethod
    def _build_graded_object(variants: list[dict], listings: list[dict] = None) -> dict:
        """
        Build PPT-compatible ebay.salesByGrade from Scrydex inline graded prices
        and/or listings data.

        Returns: {"salesByGrade": {"psa10": {...}, ...}}
        """
        sales_by_grade: dict = {}

        # Source 1: Inline graded prices from variants (aggregated by Scrydex)
        for v in (variants or []):
            for p in (v.get("prices") or []):
                if p.get("type") != "graded":
                    continue
                company = (p.get("company") or "").upper()
                grade = str(p.get("grade") or "")
                key = GRADE_TO_KEY.get((company, grade))
                if not key:
                    continue

                currency = p.get("currency")
                market_usd = _to_usd(p.get("market"), currency)
                low_usd = _to_usd(p.get("low"), currency)
                mid_usd = _to_usd(p.get("mid"), currency)
                high_usd = _to_usd(p.get("high"), currency)
                market = float(market_usd) if market_usd is not None else None
                low = float(low_usd) if low_usd is not None else None
                mid = float(mid_usd) if mid_usd is not None else None
                high = float(high_usd) if high_usd is not None else None

                if market is None and mid is None:
                    continue

                sales_by_grade[key] = {
                    "smartMarketPrice": {
                        "price": market or mid,
                        "confidence": "medium",
                        "method": "scrydex_aggregated",
                        "daysUsed": None,
                    },
                    "marketPrice7Day": market,
                    "medianPrice": mid,
                    "minPrice": low,
                    "maxPrice": high,
                    "count": None,
                    "dailyVolume7Day": None,
                    "marketTrend": None,
                }

                # Enrich with trend data if available
                trends = p.get("trends", {})
                d7 = trends.get("days_7", {})
                if d7.get("percent_change") is not None:
                    pct = d7["percent_change"]
                    if pct > 5:
                        sales_by_grade[key]["marketTrend"] = "up"
                    elif pct < -5:
                        sales_by_grade[key]["marketTrend"] = "down"
                    else:
                        sales_by_grade[key]["marketTrend"] = "stable"

        # Source 2: Raw listings (individual eBay sold comps)
        if listings:
            from collections import defaultdict
            from statistics import median as calc_median

            by_grade: dict[str, list[float]] = defaultdict(list)
            for listing in listings:
                company = (listing.get("company") or "").upper()
                grade = str(listing.get("grade") or "")
                key = GRADE_TO_KEY.get((company, grade))
                if not key:
                    continue
                price = listing.get("price")
                if price is not None:
                    by_grade[key].append(float(price))

            for key, prices in by_grade.items():
                if not prices:
                    continue
                count = len(prices)
                med = calc_median(prices)
                avg = sum(prices) / count

                entry = sales_by_grade.get(key, {})
                # Listings-derived data is more granular — enrich existing entry
                entry["count"] = count
                entry["minPrice"] = min(prices)
                entry["maxPrice"] = max(prices)
                entry["medianPrice"] = med

                if "smartMarketPrice" not in entry:
                    entry["smartMarketPrice"] = {
                        "price": avg,
                        "confidence": "high" if count >= 10 else "medium" if count >= 4 else "low",
                        "method": "scrydex_listings",
                        "daysUsed": 30,
                    }
                else:
                    # Update confidence based on listing count
                    smp = entry["smartMarketPrice"]
                    smp["confidence"] = "high" if count >= 10 else "medium" if count >= 4 else "low"

                if "marketPrice7Day" not in entry or entry["marketPrice7Day"] is None:
                    entry["marketPrice7Day"] = avg

                sales_by_grade[key] = entry

        return {"salesByGrade": sales_by_grade}

    @staticmethod
    def _extract_tcgplayer_id(raw: dict) -> Optional[int]:
        """
        Extract TCGPlayer ID from Scrydex response.
        Lives inside variants[].marketplaces[] where name == "tcgplayer".
        The product_id there is the TCGPlayer product ID.
        Returns the first one found (cards typically share the same TCG product).
        """
        for variant in (raw.get("variants") or []):
            for mp in (variant.get("marketplaces") or []):
                if mp.get("name") == "tcgplayer":
                    val = mp.get("product_id")
                    if val is not None:
                        try:
                            return int(val)
                        except (ValueError, TypeError):
                            pass
        return None

    def _normalize_card(self, raw: dict, tcgplayer_id: int = None,
                        listings: list[dict] = None) -> dict:
        """Convert Scrydex card response to PPT-compatible shape."""
        large, medium, small = self._get_image_urls(raw)
        expansion = raw.get("expansion") or {}
        variants = raw.get("variants") or []

        prices_obj = self._build_prices_object(variants)
        graded_obj = self._build_graded_object(variants, listings)

        # TCGPlayer ID: use caller-provided, else extract from response
        tcg_id = tcgplayer_id or self._extract_tcgplayer_id(raw)

        # Auto-save mapping if we found both IDs
        scrydex_id = raw.get("id")
        if scrydex_id and tcg_id:
            self._save_tcg_mapping(scrydex_id, tcg_id)

        return {
            # Identity
            "name": raw.get("name"),
            "setName": expansion.get("name", ""),
            "expansionId": expansion.get("id", ""),
            "game": self.game,
            "cardNumber": raw.get("number") or raw.get("printed_number"),
            "printedNumber": raw.get("printed_number"),
            "tcgPlayerId": tcg_id,
            "scrydexId": scrydex_id,
            "rarity": raw.get("rarity"),
            # Images
            "imageCdnUrl800": large,
            "imageCdnUrl": medium,
            "imageCdnUrl400": small,
            # Prices (PPT shape)
            "prices": prices_obj,
            # Graded (PPT shape)
            "ebay": graded_obj,
            # Scrydex extras
            "_scrydex_raw": raw,  # keep raw for debugging
        }

    def _normalize_sealed(self, raw: dict, tcgplayer_id: int = None) -> dict:
        """Convert Scrydex sealed product response to PPT-compatible shape."""
        large, medium, small = self._get_image_urls(raw)
        expansion = raw.get("expansion") or {}
        variants = raw.get("variants") or []

        tcg_id = tcgplayer_id or self._extract_tcgplayer_id(raw)
        scrydex_id = raw.get("id")
        if scrydex_id and tcg_id:
            self._save_tcg_mapping(scrydex_id, tcg_id)

        # Extract unopened price from variants. Convert JPY → USD inline so
        # the normalized dict is always in USD regardless of the source row.
        market_price = None
        for v in variants:
            for p in (v.get("prices") or []):
                if p.get("condition") == "U" and p.get("type") == "raw":
                    usd = _to_usd(p.get("market"), p.get("currency"))
                    market_price = float(usd) if usd is not None else None
                    break
            if market_price is not None:
                break

        # Fallback: any NM raw price
        if market_price is None:
            for v in variants:
                for p in (v.get("prices") or []):
                    if p.get("type") == "raw" and p.get("market") is not None:
                        usd = _to_usd(p.get("market"), p.get("currency"))
                        market_price = float(usd) if usd is not None else None
                        break
                if market_price is not None:
                    break

        return {
            "name": raw.get("name"),
            "setName": expansion.get("name", ""),
            "tcgPlayerId": tcg_id,
            "scrydexId": scrydex_id,
            "unopenedPrice": market_price,
            "marketPrice": market_price,
            "imageCdnUrl800": large,
            "imageCdnUrl": medium,
            "imageCdnUrl400": small,
            "productType": raw.get("type"),  # "Booster Box", "ETB", etc.
            "_scrydex_raw": raw,
        }

    # ── card endpoints ────────────────────────────────────────────

    def get_card_by_id(self, scrydex_id: str, *, include_prices=True,
                       include_listings=False) -> Optional[dict]:
        """Fetch a card by its Scrydex ID. Returns normalized PPT-shape dict."""
        params = {}
        if include_prices:
            params["include"] = "prices"
        raw = self._get(f"{self.base_url}/{self.game}/v1/cards/{scrydex_id}", params)
        if not raw or raw.get("status") == "error":
            return None
        card_data = raw.get("data", raw)
        if not card_data or not isinstance(card_data, dict) or "name" not in card_data:
            return None

        listings = None
        if include_listings:
            try:
                listings = self._get_card_listings_raw(scrydex_id)
            except Exception as e:
                logger.warning(f"Failed to fetch listings for {scrydex_id}: {e}")

        return self._normalize_card(card_data, listings=listings)

    def get_card_by_tcgplayer_id(self, tcgplayer_id, *, include_history=False) -> Optional[dict]:
        """
        PPT-compatible: fetch card by TCGPlayer ID.

        Scrydex has NO search-by-TCGPlayer-ID capability.
        TCGPlayer IDs live inside variants[].marketplaces[].product_id and are
        not queryable. The only path is the local mapping table (scrydex_tcg_map),
        populated by nightly set-based pulls or the seed script.

        Returns None if no mapping exists — caller should fall back gracefully.
        """
        tcg_id = int(tcgplayer_id)

        # Only path: mapping table (free — no API call)
        scrydex_id = self._resolve_tcgplayer_id(tcg_id)
        if scrydex_id:
            card = self.get_card_by_id(scrydex_id, include_listings=include_history)
            if card:
                card["tcgPlayerId"] = tcg_id
                return card

        logger.debug(f"No Scrydex mapping for TCGPlayer ID {tcg_id} — "
                     f"run seed script or wait for nightly set pull")
        return None

    def search_cards(self, query, *, set_name=None, limit=5) -> list[dict]:
        """Search cards by name. Returns list of normalized PPT-shape dicts."""
        q_parts = [_scrub_query(query)]
        if set_name:
            q_parts.append(f'expansion.name:"{set_name}"')
        params = {
            "q": " ".join(q_parts),
            "page_size": min(limit, 100),
            "include": "prices",
        }
        resp = self._get(f"{self.base_url}/{self.game}/v1/cards", params)
        items = resp.get("data", []) if isinstance(resp, dict) else []
        return [self._normalize_card(item) for item in items[:limit]]

    # ── sealed product endpoints ──────────────────────────────────

    def get_sealed_by_id(self, scrydex_id: str) -> Optional[dict]:
        """Fetch sealed product by Scrydex ID."""
        params = {"include": "prices"}
        raw = self._get(f"{self.base_url}/{self.game}/v1/sealed/{scrydex_id}", params)
        if not raw or raw.get("status") == "error":
            return None
        data = raw.get("data", raw)
        if not data or not isinstance(data, dict) or "name" not in data:
            return None
        return self._normalize_sealed(data)

    def get_sealed_product_by_tcgplayer_id(self, tcgplayer_id, *,
                                           include_history=False) -> Optional[dict]:
        """
        PPT-compatible: fetch sealed product by TCGPlayer ID.

        Sealed products DO have TCGPlayer IDs (one per variant, under
        variants[].marketplaces[].product_id), but Scrydex has no
        search-by-TCGPlayer-ID endpoint. The mapping table is the only path;
        it gets populated by nightly set-based sealed pulls (get_set_sealed
        -> _normalize_sealed -> _save_tcg_mapping).
        """
        tcg_id = int(tcgplayer_id)

        scrydex_id = self._resolve_tcgplayer_id(tcg_id)
        if scrydex_id:
            product = self.get_sealed_by_id(scrydex_id)
            if product:
                product["tcgPlayerId"] = tcg_id
                return product

        logger.debug(f"No Scrydex mapping for sealed TCGPlayer ID {tcg_id}")
        return None

    def search_sealed_products(self, query, *, set_name=None, limit=5) -> list[dict]:
        """Search sealed products by name."""
        q_parts = [_scrub_query(query)]
        if set_name:
            q_parts.append(f'expansion.name:"{set_name}"')
        params = {
            "q": " ".join(q_parts),
            "page_size": min(limit, 100),
            "include": "prices",
        }
        resp = self._get(f"{self.base_url}/{self.game}/v1/sealed", params)
        items = resp.get("data", []) if isinstance(resp, dict) else []
        return [self._normalize_sealed(item) for item in items[:limit]]

    # ── parse title (no Scrydex equivalent — use search) ──────────

    def parse_title(self, title, *, fuzzy=True, max_suggestions=5) -> list[dict]:
        """PPT-compatible: parse title into card matches using search."""
        try:
            results = self.search_cards(title, limit=max_suggestions)
            return [
                {
                    "match": r,
                    "confidence": 0.8 if r.get("name", "").lower() in title.lower() else 0.5,
                    "tcgPlayerId": r.get("tcgPlayerId"),
                    "name": r.get("name"),
                    "setName": r.get("setName"),
                }
                for r in results
            ]
        except ScrydexError as e:
            logger.warning(f"parse_title search failed for '{title}': {e}")
            return []

    # ── Scrydex-native endpoints ──────────────────────────────────

    def get_set_cards(self, expansion_id: str, *, include_prices=True,
                      page_size=100) -> list[dict]:
        """
        Pull ALL cards in a set (paginated). Returns normalized card dicts.
        This is Scrydex's killer feature: ~3-4 credits for an entire set.
        """
        all_cards = []
        page = 1
        params = {"page_size": page_size}
        if include_prices:
            params["include"] = "prices"

        while True:
            params["page"] = page
            resp = self._get(
                f"{self.base_url}/{self.game}/v1/expansions/{expansion_id}/cards",
                params
            )
            items = resp.get("data", []) if isinstance(resp, dict) else []
            if not items:
                break

            for item in items:
                all_cards.append(self._normalize_card(item))

            if len(items) < page_size:
                break
            page += 1

        logger.info(f"Scrydex set pull: {expansion_id} -> {len(all_cards)} cards ({page} pages)")
        return all_cards

    def get_set_sealed(self, expansion_id: str) -> list[dict]:
        """Pull all sealed products for a set."""
        params = {"page_size": 100, "include": "prices"}
        resp = self._get(
            f"{self.base_url}/{self.game}/v1/expansions/{expansion_id}/sealed",
            params
        )
        items = resp.get("data", []) if isinstance(resp, dict) else []
        return [self._normalize_sealed(item) for item in items]

    # The Scrydex listings endpoint has no grade filter, so we fetch every
    # listing for the card and filter to the requested grade in Python. Cap
    # pagination so a single hot card (1000+ listings) can't burn through
    # Railway's 30s gateway timeout — 5 pages = 500 listings is plenty for a
    # median.
    LISTINGS_MAX_PAGES = 5

    def _get_card_listings_raw(self, scrydex_card_id: str, *,
                               days: int = 30, source: str = "ebay") -> list[dict]:
        """Fetch raw eBay listings for a card (for graded pricing)."""
        all_listings = []
        page = 1
        while page <= self.LISTINGS_MAX_PAGES:
            params = {
                "days": days,
                "source": source,
                "page": page,
                "page_size": 100,
            }
            resp = self._get(
                f"{self.base_url}/{self.game}/v1/cards/{scrydex_card_id}/listings",
                params
            )
            items = resp.get("data", []) if isinstance(resp, dict) else []
            if not items:
                break
            all_listings.extend(items)
            total = resp.get("total_count", 0)
            if page * 100 >= total:
                break
            page += 1
        return all_listings

    def get_card_listings(self, scrydex_card_id: str, *,
                          days: int = 30) -> list[dict]:
        """Fetch eBay sold listings for graded pricing data."""
        return self._get_card_listings_raw(scrydex_card_id, days=days)

    def get_card_price_history(self, scrydex_card_id: str, *,
                               days: int = 90, variant: str = None,
                               condition: str = None) -> list[dict]:
        """Fetch price history for a card."""
        params = {"days": days, "page_size": 100}
        if variant:
            params["variant"] = variant
        if condition:
            params["condition"] = condition
        resp = self._get(
            f"{self.base_url}/{self.game}/v1/cards/{scrydex_card_id}/price_history",
            params
        )
        return resp.get("data", []) if isinstance(resp, dict) else []

    def get_expansions(self, *, language_code: str = None) -> list[dict]:
        """List all available expansions. Pass language_code to filter (e.g., 'EN', 'JA')."""
        all_expansions = []
        page = 1
        while True:
            params = {"page": page, "page_size": 100}
            if language_code:
                params["q"] = f"language_code:{language_code}"
            resp = self._get(f"{self.base_url}/{self.game}/v1/expansions", params)
            items = resp.get("data", []) if isinstance(resp, dict) else []
            if not items:
                break
            all_expansions.extend(items)
            if len(items) < 100:
                break
            page += 1
        return all_expansions

    def get_usage(self) -> dict:
        """Check credit balance."""
        return self._get(f"{self.base_url}/account/v1/usage")

    # ════════════════════════════════════════════════════════════════
    # Scalar instance API — reads directly from the raw Scrydex response
    # (preserved on normalized dicts as _scrydex_raw). Currency-aware at
    # the row level so JP-marketplace JPY prices convert to USD natively.
    # ════════════════════════════════════════════════════════════════

    _CONDITION_TO_SCRYDEX = {
        "NM": "NM", "LP": "LP", "MP": "MP", "HP": "HP",
        "DMG": "DM",  # Scrydex uses DM for damaged
    }

    @staticmethod
    def _get_raw(card_data):
        """Return the raw Scrydex response stashed on normalized card dicts
        by `_normalize_card`. Needed because `_build_prices_object` drops the
        per-row currency field."""
        if not card_data:
            return None
        return card_data.get("_scrydex_raw")

    def get_raw_condition_price(
        self, tcgplayer_id, condition: str = "NM",
        variant: Optional[str] = None,
    ) -> Optional[Decimal]:
        """Live Scrydex lookup → USD raw market price or None."""
        try:
            data = self.get_card_by_tcgplayer_id(tcgplayer_id)
        except ScrydexError:
            return None
        if not data:
            return None

        raw = self._get_raw(data)
        target_cond = self._CONDITION_TO_SCRYDEX.get(
            (condition or "").upper(), (condition or "NM").upper())

        if raw:
            # Read native Scrydex shape: variants[].prices[] with per-row currency
            for v in raw.get("variants") or []:
                if variant is not None and v.get("name") != variant:
                    continue
                for p in v.get("prices") or []:
                    if (p.get("type") == "raw"
                            and (p.get("condition") or "").upper() == target_cond):
                        return _to_usd(p.get("market"), p.get("currency"))
            # Fallback: pick first variant that has this condition
            if variant is None:
                for v in raw.get("variants") or []:
                    for p in v.get("prices") or []:
                        if (p.get("type") == "raw"
                                and (p.get("condition") or "").upper() == target_cond):
                            return _to_usd(p.get("market"), p.get("currency"))
            return None

        # No raw response (shouldn't happen — _normalize_card always stamps
        # _scrydex_raw). Fall through to the normalized dict's prices map.
        prices = (data.get("prices") or {}).get("variants", {})
        ppt_cond = CONDITION_TO_PPT.get(condition.upper(), condition)
        for vname, conds in prices.items():
            if variant is not None and vname.lower() != variant.lower():
                continue
            cd = conds.get(ppt_cond) if isinstance(conds, dict) else None
            p = cd.get("price") if isinstance(cd, dict) else None
            if p is not None:
                return Decimal(str(p))
        return None

    def get_condition_prices(
        self, tcgplayer_id, variant: Optional[str] = None,
    ) -> dict:
        """Live Scrydex lookup → {our_code: Decimal USD} for the variant."""
        try:
            data = self.get_card_by_tcgplayer_id(tcgplayer_id)
        except ScrydexError:
            return {}
        if not data:
            return {}

        raw = self._get_raw(data)
        if not raw:
            return {}

        scrydex_to_ours = {v: k for k, v in self._CONDITION_TO_SCRYDEX.items()}
        out: dict = {}
        for v in raw.get("variants") or []:
            if variant is not None and v.get("name") != variant:
                continue
            for p in v.get("prices") or []:
                if p.get("type") != "raw":
                    continue
                sx_cond = (p.get("condition") or "").upper()
                our_code = scrydex_to_ours.get(sx_cond, sx_cond)
                usd = _to_usd(p.get("market"), p.get("currency"))
                if usd is not None and our_code not in out:
                    out[our_code] = usd
            if variant is not None:
                break  # only look at the matching variant
        return out

    def get_graded_price_for(
        self, tcgplayer_id, company: str, grade: str,
    ) -> Optional[Decimal]:
        """Live Scrydex lookup → single graded price in USD."""
        try:
            data = self.get_card_by_tcgplayer_id(tcgplayer_id)
        except ScrydexError:
            return None
        if not data:
            return None

        raw = self._get_raw(data)
        if raw:
            target_company = company.upper()
            target_grade = str(grade)
            for v in raw.get("variants") or []:
                for p in v.get("prices") or []:
                    if (p.get("type") == "graded"
                            and (p.get("company") or "").upper() == target_company
                            and str(p.get("grade") or "") == target_grade):
                        return _to_usd(p.get("market"), p.get("currency"))
        return None

    def get_card_metadata(self, tcgplayer_id) -> Optional[dict]:
        """Live Scrydex lookup → Scrydex-native metadata dict."""
        try:
            data = self.get_card_by_tcgplayer_id(tcgplayer_id)
        except ScrydexError:
            return None
        if not data:
            return None

        raw = self._get_raw(data)
        variants_seen = []
        if raw:
            for v in raw.get("variants") or []:
                name = v.get("name") or "normal"
                if name not in variants_seen:
                    variants_seen.append(name)

        return {
            "scrydex_id":     data.get("scrydexId"),
            "tcgplayer_id":   data.get("tcgPlayerId") or tcgplayer_id,
            "name":           data.get("name"),
            "expansion_id":   data.get("expansionId"),
            "expansion_name": data.get("setName"),
            "card_number":    data.get("cardNumber"),
            "printed_number": data.get("printedNumber"),
            "rarity":         data.get("rarity"),
            "game":           data.get("game"),
            "image_small":    data.get("imageCdnUrl400"),
            "image_medium":   data.get("imageCdnUrl"),
            "image_large":    data.get("imageCdnUrl800"),
            "variants":       variants_seen,
        }

    def get_sealed_market_price(self, tcgplayer_id) -> Optional[Decimal]:
        """Live Scrydex lookup → unopened price in USD."""
        try:
            data = self.get_sealed_product_by_tcgplayer_id(tcgplayer_id)
        except ScrydexError:
            return None
        if not data:
            return None
        raw = data.get("_scrydex_raw")
        if raw:
            for v in raw.get("variants") or []:
                for p in v.get("prices") or []:
                    if p.get("type") == "raw" and p.get("condition") in ("U", "NM"):
                        return _to_usd(p.get("market"), p.get("currency"))
        # Fallback — normalized dict already has unopenedPrice as-is (assume USD)
        up = data.get("unopenedPrice")
        return Decimal(str(up)) if up is not None else None
