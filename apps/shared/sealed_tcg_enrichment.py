"""
Backfill missing tcgplayer_id on sealed search results.

Scrydex's /sealed endpoint does not include marketplaces / product_id (cards
do, sealed doesn't), so sealed hits from the Scrydex cache land with
tcgPlayerId=None. We try, in order:

  1. Store cache (inventory_product_cache) — strict token match against
     every product title (>=3 chars, word boundary).
  2. Store cache, relaxed — drop tokens that come from the expansion name,
     since Shopify often names sealed titles without the set prefix
     (Scrydex: "Shrouded Fable Kingdra ex Special Illustration Collection"
     vs. store: "Kingdra ex Special Illustration Collection").
  3. PPT search by name — for products that are on TCGplayer but never
     listed in our Shopify store (Costco / Sam's Club bundles, generic
     retailer-exclusive packs). PPT carries the tcgplayer_id for any
     product TCGplayer tracks. Exact case-insensitive name match required.

Disambiguate store-cache hits by shortest title; skip on equal-length ties
with differing TCG IDs. On any successful match, persist the
scrydex_id -> tcgplayer_id link via the price provider's primary client so
future lookups resolve natively without re-running this enrichment.

Shared across the intake, ingest, and inventory services — all three query
the same inventory_product_cache table.
"""
import logging
import os
import re
import unicodedata
from typing import Optional

logger = logging.getLogger(__name__)


def _strip_accents(s: str) -> str:
    """Pokémon -> Pokemon. Scrydex preserves accents, Shopify titles usually
    don't — if we tokenize 'Pokémon' as-is the regex splits it at 'é' and
    neither half matches 'Pokemon' in the store cache."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def enrich_sealed_with_shopify_tcg(results: list, db_module, price_provider=None) -> None:
    """Mutates `results` in place. No-ops for entries that already carry a
    tcgplayer_id, and silently skips on any unexpected DB/provider error."""
    if not results:
        return

    scrydex_primary = getattr(price_provider, "primary", None)
    saver = getattr(scrydex_primary, "_save_tcg_mapping", None)

    def _lookup(tokens: list[str]) -> list[dict]:
        if not tokens:
            return []
        sql = ("SELECT tcgplayer_id, title FROM inventory_product_cache "
               "WHERE tcgplayer_id IS NOT NULL")
        params: list = []
        for t in tokens:
            sql += " AND title ~* %s"
            params.append(r"\m" + re.escape(t) + r"\M")
        sql += " ORDER BY LENGTH(title) ASC LIMIT 5"
        try:
            return db_module.query(sql, tuple(params))
        except Exception as e:
            logger.debug(f"Sealed-TCG enrichment query failed: {e}")
            return []

    # Lazy-init a PPT client for the fallback phase — only built if any
    # result actually misses the store cache.
    _ppt_client = [None]  # boxed so the closure can mutate it
    _ppt_init_attempted = [False]

    def _get_ppt():
        if _ppt_init_attempted[0]:
            return _ppt_client[0]
        _ppt_init_attempted[0] = True
        ppt_key = os.getenv("PPT_API_KEY", "")
        if not ppt_key:
            return None
        try:
            from ppt_client import PPTClient
            _ppt_client[0] = PPTClient(ppt_key)
        except Exception as e:
            logger.debug(f"PPT client init for sealed-TCG fallback failed: {e}")
        return _ppt_client[0]

    def _persist(r: dict, tcg_id: int) -> None:
        r["tcgplayer_id"] = tcg_id
        r["tcgPlayerId"] = tcg_id
        scrydex_id = r.get("scrydexId") or r.get("scrydex_id")
        if scrydex_id and callable(saver):
            try:
                saver(scrydex_id, tcg_id)
            except Exception:
                pass

    for r in results:
        if r.get("tcgplayer_id") or r.get("tcgPlayerId") or r.get("tcgplayerId"):
            continue
        name = (r.get("name") or r.get("product_name") or "").strip()
        tokens = [t.lower() for t in re.findall(r"[A-Za-z0-9]+", _strip_accents(name)) if len(t) >= 3]
        if not tokens or not name:
            continue

        # Phase 1: strict store-cache match.
        rows = _lookup(tokens)
        # Phase 2: relaxed store-cache match (drop set-name tokens). Requires
        # at least 3 surviving tokens so generic remainders like
        # ["costco", "bundle"] can't grab the wrong store SKU — they'll fall
        # through to the PPT phase instead.
        if not rows:
            set_name = (r.get("setName") or r.get("set_name") or "")
            set_tokens = {t.lower() for t in re.findall(r"[A-Za-z0-9]+", _strip_accents(set_name)) if len(t) >= 3}
            relaxed = [t for t in tokens if t not in set_tokens]
            if relaxed and len(relaxed) < len(tokens) and len(relaxed) >= 3:
                rows = _lookup(relaxed)

        if rows:
            top: Optional[dict] = rows[0]
            for other in rows[1:]:
                if len(other["title"]) == len(top["title"]) and other["tcgplayer_id"] != top["tcgplayer_id"]:
                    top = None
                    break
            if top:
                try:
                    _persist(r, int(top["tcgplayer_id"]))
                    continue
                except (TypeError, ValueError):
                    pass

        # Phase 3: PPT fallback — covers products on TCGplayer but never
        # listed in our store (Costco / Sam's Club bundles).
        ppt = _get_ppt()
        if not ppt:
            continue
        try:
            ppt_results = ppt.search_sealed_products(name, limit=3) or []
        except Exception as e:
            logger.debug(f"PPT sealed search fallback failed for {name!r}: {e}")
            continue
        norm_name = name.lower()
        for pr in ppt_results:
            pr_name = (pr.get("name") or "").strip().lower()
            if pr_name != norm_name:
                continue
            pr_tcg = pr.get("tcgPlayerId") or pr.get("tcgplayer_id") or pr.get("id")
            if not pr_tcg:
                continue
            try:
                _persist(r, int(pr_tcg))
            except (TypeError, ValueError):
                pass
            break
