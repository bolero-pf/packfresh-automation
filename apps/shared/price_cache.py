"""
price_cache.py — Read pricing data from scrydex_price_cache (local DB).

Returns data in the same PPT-normalized shape that all downstream code expects,
so it's a drop-in replacement for live API calls. Zero credits, zero latency.

Usage:
    from price_cache import PriceCache
    cache = PriceCache(db)
    card = cache.get_card_by_tcgplayer_id(83472)
    # Returns same dict shape as PPTClient.get_card_by_tcgplayer_id()
"""

import logging
import re
from decimal import Decimal
from typing import Optional

_TOKEN_SPLIT = re.compile(r"[\s\-_/]+")

logger = logging.getLogger(__name__)

# Map Scrydex condition codes to PPT full names
CONDITION_DISPLAY = {
    "NM": "Near Mint",
    "LP": "Lightly Played",
    "MP": "Moderately Played",
    "HP": "Heavily Played",
    "DM": "Damaged",
    "U": "Unopened",
}

# Scrydex variant names -> PPT display names
VARIANT_DISPLAY = {
    # Pokemon
    "normal": "Normal",
    "holofoil": "Holofoil",
    "reverseHolofoil": "Reverse Holofoil",
    "unlimitedHolofoil": "Holofoil",
    "firstEditionHolofoil": "1st Edition Holofoil",
    "firstEditionShadowlessHolofoil": "1st Edition Shadowless Holofoil",
    "unlimitedShadowlessHolofoil": "Shadowless Holofoil",
    "pokemonCenter": "Pokemon Center",
    # One Piece
    "altArt": "Alt Art",
    "specialAltArt": "Special Alt Art",
    "mangaAltArt": "Manga Alt Art",
    "premiumAltArt": "Premium Alt Art",
    "foil": "Foil",
    "jollyRogerFoil": "Jolly Roger Foil",
    "fullArt": "Full Art",
    "parallel": "Parallel",
    # Lorcana
    "cold_foil": "Cold Foil",
    "enchanted": "Enchanted",
    # MTG
    "etched": "Etched Foil",
}

GRADE_TO_KEY = {
    ("PSA", "10"): "psa10", ("PSA", "9"): "psa9", ("PSA", "8"): "psa8", ("PSA", "7"): "psa7",
    ("BGS", "10"): "bgs10", ("BGS", "9.5"): "bgs9.5", ("BGS", "9"): "bgs9", ("BGS", "8"): "bgs8",
    ("CGC", "10"): "cgc10", ("CGC", "9"): "cgc9", ("CGC", "8"): "cgc8",
    ("SGC", "10"): "sgc10", ("SGC", "9"): "sgc9",
}


class PriceCache:

    def __init__(self, db, game: str = "pokemon"):
        self.db = db
        self.game = game

    @staticmethod
    def _tokenize(query: str) -> list[list[str]]:
        """Split a search query into tokens on whitespace/hyphen/underscore/slash.

        Returns a list of token-equivalence-groups: each group is a list of
        strings that should OR-match (any one matching counts as the token
        matching). A digit-only token like "041" expands to ["041", "41"]
        because Scrydex stores card numbers without leading zeros (OP14-041
        is stored as card_number="41").
        """
        raw = (query or "").strip()
        if not raw:
            return []
        toks = [t for t in _TOKEN_SPLIT.split(raw) if t]
        if not toks:
            toks = [raw]
        groups: list[list[str]] = []
        for t in toks:
            forms = [t]
            stripped = t.lstrip("0")
            if t.isdigit() and stripped and stripped != t:
                forms.append(stripped)
            groups.append(forms)
        return groups

    def get_card_by_tcgplayer_id(self, tcgplayer_id, **kwargs) -> Optional[dict]:
        """
        Read card data from scrydex_price_cache.
        Returns PPT-shaped dict or None if not cached.
        """
        tcg_id = int(tcgplayer_id)
        rows = self.db.query("""
            SELECT * FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'card' AND game = %s
            ORDER BY variant, condition, price_type
        """, (tcg_id, self.game))

        if not rows:
            return None

        return self._build_card_dict(rows, tcg_id)

    def get_card_by_scrydex_id(self, scrydex_id: str) -> Optional[dict]:
        """Read card data by Scrydex ID."""
        rows = self.db.query("""
            SELECT * FROM scrydex_price_cache
            WHERE scrydex_id = %s AND product_type = 'card' AND game = %s
            ORDER BY variant, condition, price_type
        """, (scrydex_id, self.game))

        if not rows:
            return None

        tcg_id = rows[0].get("tcgplayer_id")
        return self._build_card_dict(rows, tcg_id)

    # Bundle/set/case keywords that indicate a multi-pack product, not the base item
    _BUNDLE_KEYWORDS = ('art bundle', 'set of', 'bundle (', 'pack of', '(set of',
                        'case', 'plus -', 'plus case', 'international version')

    def get_sealed_product_by_tcgplayer_id(self, tcgplayer_id, **kwargs) -> Optional[dict]:
        """Read sealed product from cache. Returns PPT-shaped dict."""
        tcg_id = int(tcgplayer_id)
        rows = self.db.query("""
            SELECT * FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'sealed' AND game = %s
            ORDER BY variant, condition
        """, (tcg_id, self.game))

        if not rows:
            return None

        # If multiple scrydex products share this tcgplayer_id, prefer the base
        # product over bundles/art sets/cases
        scrydex_ids = set(r["scrydex_id"] for r in rows)
        if len(scrydex_ids) > 1:
            rows = self._prefer_base_product(rows)

        return self._build_sealed_dict(rows, tcg_id)

    def _prefer_base_product(self, rows: list[dict]) -> list[dict]:
        """When multiple scrydex products share a tcgplayer_id, keep only the base product."""
        by_sid: dict[str, list[dict]] = {}
        for r in rows:
            by_sid.setdefault(r["scrydex_id"], []).append(r)

        if len(by_sid) <= 1:
            return rows

        # Score each group: base products score 0, bundles score 1
        def is_bundle(group_rows):
            name = (group_rows[0].get("product_name") or "").lower()
            return any(kw in name for kw in self._BUNDLE_KEYWORDS)

        base_groups = [(sid, grp) for sid, grp in by_sid.items() if not is_bundle(grp)]

        if base_groups:
            # Among base products, pick the one with the shortest name (most specific)
            base_groups.sort(key=lambda x: len(x[1][0].get("product_name", "")))
            return base_groups[0][1]

        # All are bundles — pick shortest name as least wrong
        all_groups = sorted(by_sid.items(), key=lambda x: len(x[1][0].get("product_name", "")))
        return all_groups[0][1]

    def search_cards(self, query: str, *, set_name: str = None, limit: int = 5,
                     all_games: bool = False) -> list[dict]:
        """Search cards by name in the local cache.

        Multi-token search: each whitespace/hyphen-separated token must match
        product_name OR card_number OR expansion_id OR expansion_name. So
        "boa hancock OP14" finds the One Piece Boa Hancock from the OP14 set,
        and "OP14-041" finds card #041 in OP14.

        all_games=True drops the game filter so multi-TCG manual entry can find
        non-Pokemon cards (One Piece, Lorcana, MTG, etc.) in one search.
        """
        params: list = []
        sql = """
            SELECT DISTINCT ON (scrydex_id) *
            FROM scrydex_price_cache
            WHERE product_type = 'card'
        """
        if not all_games:
            sql += " AND game = %s"
            params.append(self.game)

        for forms in self._tokenize(query):
            # Each token group: any form matching any of the 4 columns counts as a match
            ors = []
            for f in forms:
                ors.append("(product_name ILIKE %s OR card_number ILIKE %s "
                           "OR expansion_id ILIKE %s OR expansion_name ILIKE %s)")
                p = f"%{f}%"
                params.extend([p, p, p, p])
            sql += " AND (" + " OR ".join(ors) + ")"

        if set_name:
            sql += " AND expansion_name ILIKE %s"
            params.append(f"%{set_name}%")
        sql += " ORDER BY scrydex_id, condition LIMIT %s"
        params.append(limit)

        hits = self.db.query(sql, tuple(params))
        results = []
        for row in hits:
            # Get all rows for this card to build full dict
            card_rows = self.db.query("""
                SELECT * FROM scrydex_price_cache
                WHERE scrydex_id = %s ORDER BY variant, condition, price_type
            """, (row["scrydex_id"],))
            if card_rows:
                results.append(self._build_card_dict(card_rows, row.get("tcgplayer_id")))
        return results

    def search_sealed_products(self, query: str, *, set_name: str = None, limit: int = 5,
                                all_games: bool = False) -> list[dict]:
        """Search sealed products by name in the local cache.
        Results are sorted so base products appear before bundles/art sets.

        Tokenized search across product_name, expansion_id, expansion_name.
        all_games=True drops the game filter (multi-TCG manual entry).
        """
        params: list = []
        sql = """
            SELECT DISTINCT ON (scrydex_id) *
            FROM scrydex_price_cache
            WHERE product_type = 'sealed'
        """
        if not all_games:
            sql += " AND game = %s"
            params.append(self.game)

        for forms in self._tokenize(query):
            ors = []
            for f in forms:
                ors.append("(product_name ILIKE %s OR expansion_id ILIKE %s "
                           "OR expansion_name ILIKE %s)")
                p = f"%{f}%"
                params.extend([p, p, p])
            sql += " AND (" + " OR ".join(ors) + ")"

        if set_name:
            sql += " AND expansion_name ILIKE %s"
            params.append(f"%{set_name}%")
        # Fetch extra to allow re-sorting after bundle deprioritization
        sql += " ORDER BY scrydex_id, condition LIMIT %s"
        params.append(limit * 3)

        hits = self.db.query(sql, tuple(params))
        results = []
        for row in hits:
            card_rows = self.db.query("""
                SELECT * FROM scrydex_price_cache
                WHERE scrydex_id = %s ORDER BY variant, condition
            """, (row["scrydex_id"],))
            if card_rows:
                results.append(self._build_sealed_dict(card_rows, row.get("tcgplayer_id")))

        # Sort: base products first (shorter names, no bundle keywords), then bundles
        def _bundle_sort_key(item):
            name = (item.get("name") or "").lower()
            is_bundle = any(kw in name for kw in self._BUNDLE_KEYWORDS)
            return (1 if is_bundle else 0, len(name))
        results.sort(key=_bundle_sort_key)
        return results[:limit]

    # ── Build PPT-shaped dicts from cache rows ────────────

    def _build_card_dict(self, rows: list[dict], tcg_id: int = None) -> dict:
        """Assemble a PPT-shaped card dict from scrydex_price_cache rows."""
        first = rows[0]

        # Group raw prices by variant -> condition
        variants_data: dict[str, dict] = {}
        graded_data: dict[str, dict] = {}

        for r in rows:
            variant = self._display_variant(r.get("variant", "normal"))
            condition = r.get("condition", "NM")
            price_type = r.get("price_type", "raw")
            market = r.get("market_price")
            low = r.get("low_price")

            if price_type == "raw":
                ppt_cond = CONDITION_DISPLAY.get(condition, condition)
                if variant not in variants_data:
                    variants_data[variant] = {}
                variants_data[variant][ppt_cond] = {
                    "price": float(market) if market is not None else None,
                    "low": float(low) if low is not None else None,
                }
            elif price_type == "graded":
                company = (r.get("grade_company") or "").upper()
                grade = r.get("grade_value") or ""
                key = GRADE_TO_KEY.get((company, grade))
                if key and market is not None:
                    mid_p = float(r.get("mid_price")) if r.get("mid_price") else float(market)
                    low_p = float(low) if low else None
                    high_p = float(r.get("high_price")) if r.get("high_price") else None
                    confidence = self._graded_confidence(float(market), low_p, mid_p, high_p)
                    graded_data[key] = {
                        "smartMarketPrice": {"price": float(market), "method": "scrydex_cache",
                                             "confidence": confidence},
                        "marketPrice7Day": float(market),
                        "medianPrice": mid_p,
                        "minPrice": low_p,
                        "maxPrice": high_p,
                        "count": None,
                        "dailyVolume7Day": None,
                        "marketTrend": self._trend_from_pct(r.get("trend_7d_pct")),
                    }

        # Determine primary variant and market price
        primary = "Normal" if "Normal" in variants_data else (
            next(iter(variants_data)) if variants_data else "Default"
        )

        # Market = primary variant's NM price
        market_price = None
        if primary in variants_data:
            nm = variants_data[primary].get("Near Mint", {})
            market_price = nm.get("price")

        # Build conditions from primary variant
        conditions = variants_data.get(primary, {})

        return {
            "name": first.get("product_name"),
            "setName": first.get("expansion_name", ""),
            "expansionId": first.get("expansion_id", ""),
            "game": first.get("game", ""),
            "cardNumber": first.get("card_number"),
            "tcgPlayerId": tcg_id,
            "scrydexId": first.get("scrydex_id"),
            "rarity": first.get("rarity"),
            "imageCdnUrl800": first.get("image_large", ""),
            "imageCdnUrl": first.get("image_medium", ""),
            "imageCdnUrl400": first.get("image_small", ""),
            "prices": {
                "market": market_price,
                "primaryPrinting": primary,
                "conditions": conditions,
                "variants": variants_data,
            },
            "ebay": {"salesByGrade": graded_data},
            "_from_cache": True,
        }

    def _build_sealed_dict(self, rows: list[dict], tcg_id: int = None) -> dict:
        """Assemble a PPT-shaped sealed product dict from cache rows."""
        first = rows[0]

        # Find the unopened/NM market price
        market_price = None
        for r in rows:
            if r.get("condition") in ("U", "NM") and r.get("price_type") == "raw":
                market_price = float(r["market_price"]) if r.get("market_price") else None
                break

        return {
            "name": first.get("product_name"),
            "setName": first.get("expansion_name", ""),
            "expansionId": first.get("expansion_id", ""),
            "game": first.get("game", ""),
            "tcgPlayerId": tcg_id,
            "scrydexId": first.get("scrydex_id"),
            "unopenedPrice": market_price,
            "marketPrice": market_price,
            "imageCdnUrl800": first.get("image_large", ""),
            "imageCdnUrl": first.get("image_medium", ""),
            "imageCdnUrl400": first.get("image_small", ""),
            "productType": None,
            "_from_cache": True,
        }

    @staticmethod
    def _display_variant(name: str) -> str:
        return VARIANT_DISPLAY.get(name, name.replace("_", " ").title())

    @staticmethod
    def _graded_confidence(market: float, low: float | None, mid: float | None,
                           high: float | None) -> str:
        """Derive confidence from price spread. When low/mid/high are all the same,
        it means very few sales (low confidence). Real spread = more data."""
        if market <= 0:
            return "low"
        # If we have low + high, check spread relative to market
        if low is not None and high is not None and low > 0:
            spread_pct = (high - low) / market * 100 if market > 0 else 999
            # Tight spread (<30%) with distinct low/mid/high = good data
            if low != high and spread_pct < 50:
                return "high"
            # Some spread but not wild
            if low != high:
                return "medium"
        # Mid differs from market = at least some price diversity
        if mid is not None and mid != market:
            return "medium"
        # All values identical or missing = single data point
        return "low"

    @staticmethod
    def _trend_from_pct(pct) -> Optional[str]:
        if pct is None:
            return None
        try:
            p = float(pct)
            if p > 5:
                return "up"
            elif p < -5:
                return "down"
            return "stable"
        except (ValueError, TypeError):
            return None
