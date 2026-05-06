"""
price_cache.py — Read pricing data from scrydex_price_cache (local DB).

Two APIs live here:

1. Scalar API (current) — returns Decimal / dict of scalars in USD. Scrydex-
   native naming (variants are 'holofoil', 'normal', 'altArt', etc.). JPY rows
   are converted to USD via SCRYDEX_JPY_USD_RATE inside the accessor so callers
   stay currency-blind.

2. Legacy PPT-shaped API (`get_card_by_*`, `search_cards`, `_build_card_dict`)
   — kept during migration so existing callers don't break. Do not add new
   callers. Being ripped out once the last caller migrates.
"""

import logging
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

# Scrydex sends JP-marketplace rows in JPY and eBay graded rows in USD. Convert
# inside every accessor so the scalar API always returns USD. Rate matches the
# ingestion service's SCRYDEX_JPY_USD_RATE (same env var, same default).
_JPY_USD_RATE = Decimal(os.getenv("SCRYDEX_JPY_USD_RATE", "0.0066"))

# Split on whitespace, hyphen/underscore/slash, AND apostrophes (straight and
# Unicode curly right-single-quote). Without splitting on apostrophes, a query
# like "Team Rocket's Moltres ex" tokenizes to ["Team", "Rocket's", "Moltres",
# "ex"] and ILIKE '%Rocket's%' only matches rows whose product_name contains
# that exact contraction with the same apostrophe character — DB rows from
# Scrydex sometimes use the curly U+2019 form, so the straight ASCII typed
# query never hits.
_TOKEN_SPLIT = re.compile(r"[\s\-_/'’]+")

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


def _to_usd(price, currency: Optional[str]) -> Optional[Decimal]:
    """Convert a cached price to USD based on its row currency.
    NULL/USD pass through; JPY multiplied by the configured rate.
    Quantized to 2 decimals — all downstream code expects cents precision."""
    if price is None:
        return None
    dec = Decimal(str(price))
    if (currency or "USD").upper() == "JPY":
        dec = dec * _JPY_USD_RATE
    return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _normalize_condition(cond: str) -> str:
    """Our canonical condition codes (NM/LP/MP/HP/DMG) → Scrydex cache form
    (the cache stores Damaged as 'DM' because that's what Scrydex sends).
    Case-insensitive; trimmed; unknown values pass through upper-cased."""
    c = (cond or "").upper().strip()
    if c == "DMG":
        return "DM"
    return c


def _variant_ranking_case() -> str:
    """ORDER BY fragment: prefer holofoil, then normal, then anything else.
    Used when the caller hasn't specified a variant and we have to pick one."""
    return ("CASE variant "
            "WHEN 'holofoil' THEN 0 "
            "WHEN 'normal' THEN 1 "
            "ELSE 2 END")


class PriceCache:

    def __init__(self, db, game: str = "pokemon"):
        self.db = db
        self.game = game

    # ════════════════════════════════════════════════════════════════
    # Scalar API — Scrydex-native, USD-converted, no PPT dict shape
    # ════════════════════════════════════════════════════════════════

    def get_raw_condition_price(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        condition: str = "NM", variant: Optional[str] = None,
    ) -> Optional[Decimal]:
        """Return USD raw market price for a card at a given condition.

        Args:
            scrydex_id: Preferred key (more specific — tcg IDs can collide
                across variants). Provide this when available.
            tcgplayer_id: Alternative key.
            condition: NM, LP, MP, HP, DMG. Case-insensitive. Cache stores
                DMG as 'DM' (Scrydex-native); translation is internal.
            variant: Scrydex-native variant name ('holofoil', 'normal',
                'altArt', 'firstEditionHolofoil', etc.). None → whichever
                variant has this condition, preferring holofoil > normal.

        Returns Decimal USD price, or None if no matching row exists.
        JPY rows converted to USD via SCRYDEX_JPY_USD_RATE.
        """
        if not scrydex_id and not tcgplayer_id:
            return None
        cache_cond = _normalize_condition(condition)

        params: list = []
        where_parts = ["product_type = 'card'", "price_type = 'raw'",
                       "condition = %s", "market_price IS NOT NULL"]
        params.append(cache_cond)

        if scrydex_id:
            where_parts.append("scrydex_id = %s")
            params.append(scrydex_id)
        else:
            where_parts.append("tcgplayer_id = %s")
            params.append(int(tcgplayer_id))

        if variant is not None:
            where_parts.append("variant = %s")
            params.append(variant)

        sql = f"""
            SELECT market_price, currency
            FROM scrydex_price_cache
            WHERE {' AND '.join(where_parts)}
            ORDER BY {_variant_ranking_case()}, fetched_at DESC NULLS LAST
            LIMIT 1
        """
        rows = self.db.query(sql, tuple(params))
        if not rows:
            return None
        return _to_usd(rows[0]["market_price"], rows[0].get("currency"))

    def get_condition_prices(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        variant: Optional[str] = None,
    ) -> dict:
        """Return {condition_code: USD Decimal} for every condition the cache
        has for this card+variant. Condition codes are our canonical form
        (NM/LP/MP/HP/DMG). Only populated conditions appear — callers that
        need synthesized fallbacks should use `price_synthesis.synthesize_from_nm`.

        If variant is None, picks the primary variant (holofoil > normal > other)
        that has the most rows.
        """
        if not scrydex_id and not tcgplayer_id:
            return {}

        params: list = []
        where_parts = ["product_type = 'card'", "price_type = 'raw'",
                       "market_price IS NOT NULL"]
        if scrydex_id:
            where_parts.append("scrydex_id = %s")
            params.append(scrydex_id)
        else:
            where_parts.append("tcgplayer_id = %s")
            params.append(int(tcgplayer_id))

        if variant is None:
            # Resolve the primary variant first so the condition map is
            # internally consistent (one variant, not a mix).
            variant = self._primary_variant(scrydex_id=scrydex_id,
                                            tcgplayer_id=tcgplayer_id)
            if variant is None:
                return {}
        where_parts.append("variant = %s")
        params.append(variant)

        sql = f"""
            SELECT condition, market_price, currency
            FROM scrydex_price_cache
            WHERE {' AND '.join(where_parts)}
        """
        rows = self.db.query(sql, tuple(params))
        out: dict = {}
        for r in rows:
            cond = r.get("condition") or "NM"
            canonical = "DMG" if cond == "DM" else cond
            price = _to_usd(r["market_price"], r.get("currency"))
            if price is not None:
                out[canonical] = price
        return out

    def get_graded_price(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        company: str, grade: str, variant: Optional[str] = None,
    ) -> Optional[Decimal]:
        """Return USD graded market price for (card, company, grade).
        Graded rows come from eBay comps — they're already USD regardless of
        the card's language, but _to_usd still honors the currency column
        defensively."""
        if not scrydex_id and not tcgplayer_id:
            return None

        params: list = []
        where_parts = ["product_type = 'card'", "price_type = 'graded'",
                       "market_price IS NOT NULL",
                       "grade_company = %s", "grade_value = %s"]
        params.extend([company.upper(), str(grade)])

        if scrydex_id:
            where_parts.append("scrydex_id = %s")
            params.append(scrydex_id)
        else:
            where_parts.append("tcgplayer_id = %s")
            params.append(int(tcgplayer_id))

        if variant is not None:
            where_parts.append("variant = %s")
            params.append(variant)

        sql = f"""
            SELECT market_price, currency
            FROM scrydex_price_cache
            WHERE {' AND '.join(where_parts)}
            ORDER BY {_variant_ranking_case()}, fetched_at DESC NULLS LAST
            LIMIT 1
        """
        rows = self.db.query(sql, tuple(params))
        if not rows:
            return None
        return _to_usd(rows[0]["market_price"], rows[0].get("currency"))

    def get_graded_prices(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
        variant: Optional[str] = None,
    ) -> dict:
        """Return {(company, grade): {market, low, mid, high, trend}} for every
        graded row the cache has for this card+variant. Prices are USD."""
        if not scrydex_id and not tcgplayer_id:
            return {}

        params: list = []
        where_parts = ["product_type = 'card'", "price_type = 'graded'",
                       "market_price IS NOT NULL"]
        if scrydex_id:
            where_parts.append("scrydex_id = %s")
            params.append(scrydex_id)
        else:
            where_parts.append("tcgplayer_id = %s")
            params.append(int(tcgplayer_id))

        if variant is None:
            variant = self._primary_variant(scrydex_id=scrydex_id,
                                            tcgplayer_id=tcgplayer_id)
        if variant is not None:
            where_parts.append("variant = %s")
            params.append(variant)

        sql = f"""
            SELECT grade_company, grade_value,
                   market_price, low_price, mid_price, high_price,
                   trend_7d_pct, currency
            FROM scrydex_price_cache
            WHERE {' AND '.join(where_parts)}
        """
        rows = self.db.query(sql, tuple(params))
        out: dict = {}
        for r in rows:
            company = (r.get("grade_company") or "").upper()
            grade = r.get("grade_value") or ""
            if not company or not grade:
                continue
            currency = r.get("currency")
            out[(company, grade)] = {
                "market": _to_usd(r.get("market_price"), currency),
                "low":    _to_usd(r.get("low_price"), currency),
                "mid":    _to_usd(r.get("mid_price"), currency),
                "high":   _to_usd(r.get("high_price"), currency),
                "trend_7d_pct": r.get("trend_7d_pct"),
            }
        return out

    def get_card_metadata(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
    ) -> Optional[dict]:
        """Scrydex-native card metadata. No prices.

        Returns {
            scrydex_id, tcgplayer_id, name, expansion_id, expansion_name,
            card_number, printed_number, rarity, game,
            image_small, image_medium, image_large,
            variants: [list of Scrydex-native variant names available],
        } or None if the card isn't cached.
        """
        if not scrydex_id and not tcgplayer_id:
            return None

        if scrydex_id:
            rows = self.db.query("""
                SELECT * FROM scrydex_price_cache
                WHERE scrydex_id = %s AND product_type = 'card'
                ORDER BY variant, condition, price_type
            """, (scrydex_id,))
        else:
            rows = self.db.query("""
                SELECT * FROM scrydex_price_cache
                WHERE tcgplayer_id = %s AND product_type = 'card'
                ORDER BY variant, condition, price_type
            """, (int(tcgplayer_id),))

        if not rows:
            return None

        first = rows[0]
        variants_seen = []
        for r in rows:
            v = r.get("variant") or "normal"
            if v not in variants_seen:
                variants_seen.append(v)

        # Prefer the holofoil row for the top-level image (most JP/older cards
        # have a holo printing). Falls back to first row.
        img_row = next((r for r in rows if r.get("variant") == "holofoil"), first)
        return {
            "scrydex_id":     first.get("scrydex_id"),
            "tcgplayer_id":   first.get("tcgplayer_id"),
            "name":           first.get("product_name"),
            "expansion_id":   first.get("expansion_id"),
            "expansion_name": first.get("expansion_name"),
            "card_number":    first.get("card_number"),
            "printed_number": first.get("printed_number"),
            "rarity":         first.get("rarity"),
            "game":           first.get("game"),
            "image_small":    img_row.get("image_small"),
            "image_medium":   img_row.get("image_medium"),
            "image_large":    img_row.get("image_large"),
            "variants":       variants_seen,
        }

    def get_sealed_market_price(self, tcgplayer_id) -> Optional[Decimal]:
        """Unopened (U) or NM market price for a sealed product. USD."""
        rows = self.db.query("""
            SELECT market_price, currency
            FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'sealed'
              AND price_type = 'raw'
              AND condition IN ('U', 'NM')
              AND market_price IS NOT NULL
            ORDER BY fetched_at DESC NULLS LAST
            LIMIT 1
        """, (int(tcgplayer_id),))
        if not rows:
            return None
        return _to_usd(rows[0]["market_price"], rows[0].get("currency"))

    def get_sealed_metadata(self, tcgplayer_id) -> Optional[dict]:
        """Scrydex-native sealed metadata. No price."""
        rows = self.db.query("""
            SELECT * FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'sealed'
            ORDER BY condition
        """, (int(tcgplayer_id),))
        if not rows:
            return None
        scrydex_ids = set(r["scrydex_id"] for r in rows)
        if len(scrydex_ids) > 1:
            rows = self._prefer_base_product(rows)
        first = rows[0]
        return {
            "scrydex_id":     first.get("scrydex_id"),
            "tcgplayer_id":   first.get("tcgplayer_id"),
            "name":           first.get("product_name"),
            "expansion_id":   first.get("expansion_id"),
            "expansion_name": first.get("expansion_name"),
            "game":           first.get("game"),
            "image_small":    first.get("image_small"),
            "image_medium":   first.get("image_medium"),
            "image_large":    first.get("image_large"),
        }

    def _primary_variant(self, *, scrydex_id=None, tcgplayer_id=None) -> Optional[str]:
        """Pick the primary variant for a card: holofoil > normal > whichever
        row comes first. Used when a caller doesn't specify a variant and we
        need to anchor a multi-condition or multi-grade query on one."""
        if scrydex_id:
            rows = self.db.query(f"""
                SELECT variant FROM scrydex_price_cache
                WHERE scrydex_id = %s AND product_type = 'card' AND price_type = 'raw'
                ORDER BY {_variant_ranking_case()}, fetched_at DESC NULLS LAST
                LIMIT 1
            """, (scrydex_id,))
        else:
            rows = self.db.query(f"""
                SELECT variant FROM scrydex_price_cache
                WHERE tcgplayer_id = %s AND product_type = 'card' AND price_type = 'raw'
                ORDER BY {_variant_ranking_case()}, fetched_at DESC NULLS LAST
                LIMIT 1
            """, (int(tcgplayer_id),))
        return rows[0]["variant"] if rows else None

    def get_card_view(
        self, *, scrydex_id: str = None, tcgplayer_id=None,
    ) -> Optional[dict]:
        """Single-query card view: all variants × conditions + all graded rows
        + metadata, in one SELECT. USD throughout.

        Shape matches PriceProvider.get_card_view. Use this directly from a
        PriceCache instance; PriceProvider delegates to this and falls back to
        the live primary only when the cache misses.
        """
        if not scrydex_id and not tcgplayer_id:
            return None

        if scrydex_id:
            rows = self.db.query("""
                SELECT * FROM scrydex_price_cache
                WHERE scrydex_id = %s AND product_type = 'card'
                ORDER BY variant, condition, price_type
            """, (scrydex_id,))
        else:
            rows = self.db.query("""
                SELECT * FROM scrydex_price_cache
                WHERE tcgplayer_id = %s AND product_type = 'card'
                ORDER BY variant, condition, price_type
            """, (int(tcgplayer_id),))

        if not rows:
            return None

        first = rows[0]
        variants_map: dict = {}
        graded_nested: dict = {}

        for r in rows:
            # Normalize to display names ("Foil", "Holofoil", …) so this view's
            # variant keys match what /api/search/cards emits — the intake
            # condition picker passes the chip's display name back through
            # /api/lookup/card and looks it up directly in this map.
            variant = self._display_variant(r.get("variant") or "normal")
            currency = r.get("currency")
            price = _to_usd(r.get("market_price"), currency)
            if price is None:
                continue
            price_type = r.get("price_type", "raw")

            if price_type == "raw":
                cond = r.get("condition") or "NM"
                canonical = "DMG" if cond == "DM" else cond
                variants_map.setdefault(variant, {})[canonical] = price
            elif price_type == "graded":
                company = (r.get("grade_company") or "").upper()
                grade = r.get("grade_value") or ""
                if company and grade:
                    graded_nested.setdefault(company, {})[grade] = price

        # Primary variant = Holofoil > Normal > first available.
        primary = None
        for candidate in ("Holofoil", "Normal"):
            if candidate in variants_map:
                primary = candidate
                break
        if primary is None and variants_map:
            primary = next(iter(variants_map))

        # Prefer the holofoil row's image if present, else the first row's.
        img_row = next((r for r in rows if r.get("variant") == "holofoil"), first)

        return {
            "scrydex_id":     first.get("scrydex_id"),
            "tcgplayer_id":   first.get("tcgplayer_id"),
            "name":           first.get("product_name"),
            "set_name":       first.get("expansion_name"),
            "expansion_name": first.get("expansion_name"),
            "card_number":    first.get("card_number"),
            "printed_number": first.get("printed_number"),
            "rarity":         first.get("rarity"),
            "game":           first.get("game"),
            "image_small":    img_row.get("image_small"),
            "image_medium":   img_row.get("image_medium"),
            "image_large":    img_row.get("image_large"),
            "variants":       variants_map,
            "primary_variant": primary,
            "graded":         graded_nested,
        }

    def search_cards_native(
        self, query: str, *, set_name: str = None, limit: int = 5,
        all_games: bool = False,
    ) -> list:
        """Same search as `search_cards`, but returns Scrydex-native metadata
        dicts (shape: same as `get_card_metadata`). No prices — callers who
        need a price for a search hit should call `get_raw_condition_price`
        / `get_condition_prices` on the returned scrydex_id."""
        hits = self._search_card_ids(query, set_name=set_name, limit=limit,
                                     all_games=all_games)
        out = []
        for h in hits:
            meta = self.get_card_metadata(scrydex_id=h["scrydex_id"])
            if meta:
                out.append(meta)
        return out

    def search_sealed_native(
        self, query: str, *, set_name: str = None, limit: int = 5,
        all_games: bool = False,
    ) -> list:
        """Scrydex-native version of `search_sealed_products`."""
        params: list = []
        sql = """
            SELECT DISTINCT ON (scrydex_id) scrydex_id, tcgplayer_id
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
        sql += " ORDER BY scrydex_id, condition LIMIT %s"
        params.append(limit * 3)

        hits = self.db.query(sql, tuple(params))
        out = []
        for h in hits:
            meta = self.get_sealed_metadata(h["tcgplayer_id"]) if h.get("tcgplayer_id") else None
            if meta:
                out.append(meta)
        # Base products first, bundles last
        def _bundle_sort_key(item):
            name = (item.get("name") or "").lower()
            is_bundle = any(kw in name for kw in self._BUNDLE_KEYWORDS)
            return (1 if is_bundle else 0, len(name))
        out.sort(key=_bundle_sort_key)
        return out[:limit]

    def _search_card_ids(self, query: str, *, set_name=None, limit=5,
                          all_games=False) -> list:
        """Internal: run the card search query, return [{scrydex_id, tcgplayer_id}].

        Tokens match across product_name / product_name_en / card_number /
        printed_number / expansion_id / expansion_name / expansion_name_en.
        The *_en columns are critical for non-English cards — JP rows store
        the JP name in product_name and the English name in product_name_en,
        so without them an operator typing "blaine arcanine" never finds
        gym2_ja-34. Mirrors the public `search_cards()` clause."""
        full_query = (query or "").strip()
        where_parts: list = ["product_type = 'card'"]
        params: list = []
        if not all_games:
            where_parts.append("game = %s")
            params.append(self.game)

        for forms in self._tokenize(query):
            ors = []
            for f in forms:
                ors.append("(product_name ILIKE %s OR product_name_en ILIKE %s "
                           "OR card_number ILIKE %s OR printed_number ILIKE %s "
                           "OR expansion_id ILIKE %s "
                           "OR expansion_name ILIKE %s OR expansion_name_en ILIKE %s)")
                p = f"%{f}%"
                params.extend([p, p, p, p, p, p, p])
            where_parts.append("(" + " OR ".join(ors) + ")")

        if set_name:
            where_parts.append("(expansion_name ILIKE %s OR expansion_name_en ILIKE %s)")
            params.extend([f"%{set_name}%", f"%{set_name}%"])

        where_clause = " AND ".join(where_parts)

        score_sql = "0"
        score_params: list = []
        if full_query:
            score_sql = (
                "(CASE WHEN LOWER(printed_number) = LOWER(%s) THEN 100 ELSE 0 END) + "
                "(CASE WHEN printed_number ILIKE %s THEN 30 ELSE 0 END) + "
                "(CASE WHEN printed_number ILIKE %s THEN 10 ELSE 0 END)"
            )
            score_params = [full_query, f"{full_query}%", f"%{full_query}%"]

        sql = f"""
            SELECT scrydex_id, tcgplayer_id, score FROM (
                SELECT DISTINCT ON (scrydex_id) scrydex_id, tcgplayer_id,
                       ({score_sql}) AS score
                FROM scrydex_price_cache
                WHERE {where_clause}
                ORDER BY scrydex_id, condition, price_type
            ) sub
            ORDER BY score DESC, scrydex_id
            LIMIT %s
        """
        return self.db.query(sql, tuple(score_params + params + [limit]))

    # ════════════════════════════════════════════════════════════════
    # Legacy PPT-shaped API — deprecated, removed once no callers remain
    # ════════════════════════════════════════════════════════════════

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

        TCGPlayer IDs are globally unique across all games (TCGplayer assigns
        them sequentially), so we deliberately do NOT filter by self.game here.
        That used to mean an intake-service initialized for game='pokemon' would
        return "not found" for any One Piece / Lorcana / MTG TCG ID even though
        the row was sitting in cache.
        """
        tcg_id = int(tcgplayer_id)
        rows = self.db.query("""
            SELECT * FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'card'
            ORDER BY variant, condition, price_type
        """, (tcg_id,))

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
        """Read sealed product from cache. Returns PPT-shaped dict.

        TCGPlayer IDs are globally unique — no game filter needed (and
        applying one breaks cross-game lookups).
        """
        tcg_id = int(tcgplayer_id)
        rows = self.db.query("""
            SELECT * FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'sealed'
            ORDER BY variant, condition
        """, (tcg_id,))

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
        product_name / product_name_en / card_number / printed_number /
        expansion_id / expansion_name / expansion_name_en. The *_en columns
        are critical for non-English cards — the JP "Blaine's Arcanine" is
        stored as `カツラのウインディ` in product_name with `Blaine's Arcanine`
        in product_name_en, so without the EN columns the operator-typed
        English name never hits. So "blaine arcanine" finds the JP gym2_ja-34
        printing, and "boa hancock OP14" still finds the One Piece card the
        same way it always did.

        all_games=True drops the game filter so multi-TCG manual entry can find
        non-Pokemon cards (One Piece, Lorcana, MTG, etc.) in one search.
        """
        # Whole-query forms used to boost card-code matches: "P-075" should
        # promote rows whose printed_number = 'P-075' over rows that just
        # happen to contain "P" + "75" as separate tokens (otherwise MTG sets
        # like 10E with printed_number '275' bury the actual hit).
        full_query = (query or "").strip()
        full_query_forms = [full_query] if full_query else []

        where_parts: list[str] = ["product_type = 'card'"]
        params: list = []
        if not all_games:
            where_parts.append("game = %s")
            params.append(self.game)

        for forms in self._tokenize(query):
            ors = []
            for f in forms:
                ors.append("(product_name ILIKE %s OR product_name_en ILIKE %s "
                           "OR card_number ILIKE %s OR printed_number ILIKE %s "
                           "OR expansion_id ILIKE %s "
                           "OR expansion_name ILIKE %s OR expansion_name_en ILIKE %s)")
                p = f"%{f}%"
                params.extend([p, p, p, p, p, p, p])
            where_parts.append("(" + " OR ".join(ors) + ")")

        # set_name used to be a hard WHERE filter, but Collectr's set names
        # don't always match Scrydex's (Collectr says "Mega Evolution" for ME01
        # but Scrydex calls it something else; "Pokemon 151" vs "SV: 151"; etc.)
        # so a hard filter killed real matches. It's a soft score boost now —
        # printed_number scoring still pins the right printing.
        where_clause = " AND ".join(where_parts)

        # Relevance score: exact printed_number > printed_number prefix >
        # printed_number contains > set_name token match > anything else.
        # Wrap in a subquery so we can DISTINCT ON (scrydex_id) for de-dup,
        # then ORDER BY score. NOTE: parameter order matters — score
        # placeholders appear FIRST in the SQL (in the SELECT list), so they
        # must come first in the params tuple too.
        score_parts = ["0"]
        score_params: list = []
        if full_query:
            score_parts.append(
                "(CASE WHEN LOWER(printed_number) = LOWER(%s) THEN 100 ELSE 0 END)"
            )
            score_params.append(full_query)
            score_parts.append(
                "(CASE WHEN printed_number ILIKE %s THEN 30 ELSE 0 END)"
            )
            score_params.append(f"{full_query}%")
            score_parts.append(
                "(CASE WHEN printed_number ILIKE %s THEN 10 ELSE 0 END)"
            )
            score_params.append(f"%{full_query}%")
        if set_name:
            # Whole-string match on expansion_name (or its EN twin) is the
            # strongest set signal — beats per-token. Per-token boosts catch
            # partial matches like Collectr "Mega Evolution" against Scrydex
            # "Mega Evolution Base" / "Mega Evolution: Promo".
            score_parts.append(
                "(CASE WHEN expansion_name ILIKE %s OR expansion_name_en ILIKE %s "
                "OR expansion_id ILIKE %s THEN 50 ELSE 0 END)"
            )
            score_params.extend([f"%{set_name}%", f"%{set_name}%", f"%{set_name}%"])
            for tok in (set_name or "").split():
                if len(tok) < 2:
                    continue
                score_parts.append(
                    "(CASE WHEN expansion_name ILIKE %s OR expansion_name_en ILIKE %s "
                    "THEN 8 ELSE 0 END)"
                )
                score_params.extend([f"%{tok}%", f"%{tok}%"])
        score_sql = " + ".join(score_parts)

        sql = f"""
            SELECT scrydex_id, tcgplayer_id, score FROM (
                SELECT DISTINCT ON (scrydex_id) scrydex_id, tcgplayer_id,
                       ({score_sql}) AS score
                FROM scrydex_price_cache
                WHERE {where_clause}
                ORDER BY scrydex_id, condition, price_type
            ) sub
            ORDER BY score DESC, scrydex_id
            LIMIT %s
        """
        sub_params = score_params + params + [limit]

        hits = self.db.query(sql, tuple(sub_params))
        results = []
        for row in hits:
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
        # Track which variants showed up in graded rows so the rebind
        # chip-picker can surface "graded-only" variants (no raw price in
        # cache, but the printing exists). ~14% of cached scrydex_ids are
        # graded-only — almost all shinies / hyper rares / promos that
        # mostly sell PSA-slabbed. Operator still needs to bind raw stock
        # to them; the picker offers a "bind + auto-block" path so nightly
        # leaves them alone and operator manages price manually.
        graded_variants_seen: dict[str, dict] = {}  # {variantName: {top_grade, top_price}}

        # Grade preference order: PSA 10 > PSA 9 > BGS 10 > ... — used to
        # pick the top graded comp to display on the graded-only chip.
        _GRADE_PREF = ['psa10','psa9','psa8','bgs10','bgs9.5','bgs9','cgc10','cgc9','sgc10','sgc9']

        for r in rows:
            variant = self._display_variant(r.get("variant", "normal"))
            condition = r.get("condition", "NM")
            price_type = r.get("price_type", "raw")
            currency = r.get("currency")
            market = _to_usd(r.get("market_price"), currency)
            low = _to_usd(r.get("low_price"), currency)

            if price_type == "raw":
                ppt_cond = CONDITION_DISPLAY.get(condition, condition)
                if variant not in variants_data:
                    variants_data[variant] = {}
                variants_data[variant][ppt_cond] = {
                    "price": float(market) if market is not None else None,
                    "low": float(low) if low is not None else None,
                }
                # Per-variant image + tcg_id (each variant row carries its own
                # image_* + tcgplayer_id after the per-variant sync fix).
                # Set once per variant — first row wins, all rows for one
                # variant carry the same metadata.
                if "_image_small" not in variants_data[variant]:
                    if r.get("image_small"):
                        variants_data[variant]["_image_small"] = r.get("image_small")
                    if r.get("image_medium"):
                        variants_data[variant]["_image_medium"] = r.get("image_medium")
                    if r.get("image_large"):
                        variants_data[variant]["_image_large"] = r.get("image_large")
                if "_tcgplayer_id" not in variants_data[variant] and r.get("tcgplayer_id"):
                    variants_data[variant]["_tcgplayer_id"] = r.get("tcgplayer_id")
            elif price_type == "graded":
                company = (r.get("grade_company") or "").upper()
                grade = r.get("grade_value") or ""
                key = GRADE_TO_KEY.get((company, grade))
                if key and market is not None:
                    mid_p = _to_usd(r.get("mid_price"), currency) or market
                    high_p = _to_usd(r.get("high_price"), currency)
                    mid_p = float(mid_p) if mid_p is not None else float(market)
                    low_p = float(low) if low else None
                    high_p = float(high_p) if high_p is not None else None
                    market_f = float(market)
                    confidence = self._graded_confidence(market_f, low_p, mid_p, high_p)
                    graded_data[key] = {
                        "smartMarketPrice": {"price": market_f, "method": "scrydex_cache",
                                             "confidence": confidence},
                        "marketPrice7Day": market_f,
                        "medianPrice": mid_p,
                        "minPrice": low_p,
                        "maxPrice": high_p,
                        "count": None,
                        "dailyVolume7Day": None,
                        "marketTrend": self._trend_from_pct(r.get("trend_7d_pct")),
                    }
                    # Track for the graded-only chip — keep the highest-pref
                    # grade that has a price for this variant.
                    existing = graded_variants_seen.get(variant)
                    cur_pref = _GRADE_PREF.index(key) if key in _GRADE_PREF else 99
                    if existing is None or cur_pref < existing.get("_pref", 99):
                        graded_variants_seen[variant] = {
                            "top_grade": key,
                            "top_price": market_f,
                            "_pref": cur_pref,
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

        # Variants that exist only in graded rows (no raw price). Surfaced
        # as a separate map so the rebind chip-picker can render them with
        # a "graded only — bind + auto-block" affordance, distinct from
        # bindable raw chips.
        graded_only = {
            v: {"top_grade": info["top_grade"], "top_price": info["top_price"]}
            for v, info in graded_variants_seen.items()
            if v not in variants_data
        }

        return {
            "name": first.get("product_name"),
            # English fallback + language code so non-EN cards (JP especially)
            # are recognizable in chip-picker UIs — operator searched "Blaine's
            # Arcanine" and gets a result row whose product_name is カツラのウインディ;
            # surfacing nameEn + languageCode lets the UI show both.
            "nameEn": first.get("product_name_en"),
            "setNameEn": first.get("expansion_name_en"),
            "languageCode": first.get("language_code"),
            "setName": first.get("expansion_name", ""),
            "expansionId": first.get("expansion_id", ""),
            "game": first.get("game", ""),
            "cardNumber": first.get("card_number"),
            "printedNumber": first.get("printed_number"),
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
                "gradedOnlyVariants": graded_only,
            },
            "ebay": {"salesByGrade": graded_data},
            "_from_cache": True,
        }

    def _build_sealed_dict(self, rows: list[dict], tcg_id: int = None) -> dict:
        """Assemble a PPT-shaped sealed product dict from cache rows."""
        first = rows[0]

        # Find the unopened/NM market price (USD — convert from JPY if needed)
        market_price = None
        for r in rows:
            if r.get("condition") in ("U", "NM") and r.get("price_type") == "raw":
                usd = _to_usd(r.get("market_price"), r.get("currency"))
                market_price = float(usd) if usd is not None else None
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
