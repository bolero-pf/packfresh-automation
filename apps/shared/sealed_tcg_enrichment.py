"""
Backfill missing tcgplayer_id on sealed search results from the store cache.

Scrydex's /sealed endpoint does not include marketplaces / product_id (cards
do, sealed doesn't), so sealed hits from the Scrydex cache land with
tcgPlayerId=None. The store has been selling sealed forever — if we carry
the product in Shopify, the TCG ID is already on the product metafield and
mirrored into inventory_product_cache. Look it up there by title and backfill.

Match is two-phase:
  1. Strict: every name-token (>=3 chars) must hit as a word-boundary match.
  2. Relaxed (only if strict found nothing): drop the tokens that come from
     the expansion name, since Shopify often names sealed titles without the
     set prefix — e.g. Scrydex's "Shrouded Fable Kingdra ex Special
     Illustration Collection" vs. store's "Kingdra ex Special Illustration
     Collection".

Disambiguate by shortest title and skip on equal-length ties with differing
TCG IDs. On a hit, persist the scrydex_id -> tcgplayer_id link via the
price provider's primary client so future lookups resolve natively.

Shared across the intake, ingest, and inventory services — all three query
the same inventory_product_cache table.
"""
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


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

    for r in results:
        if r.get("tcgplayer_id") or r.get("tcgPlayerId") or r.get("tcgplayerId"):
            continue
        name = (r.get("name") or r.get("product_name") or "").strip()
        tokens = [t.lower() for t in re.findall(r"[A-Za-z0-9]+", name) if len(t) >= 3]
        if not tokens:
            continue

        rows = _lookup(tokens)
        if not rows:
            set_name = (r.get("setName") or r.get("set_name") or "")
            set_tokens = {t.lower() for t in re.findall(r"[A-Za-z0-9]+", set_name) if len(t) >= 3}
            relaxed = [t for t in tokens if t not in set_tokens]
            if relaxed and len(relaxed) < len(tokens) and len(relaxed) >= 2:
                rows = _lookup(relaxed)
        if not rows:
            continue

        top: Optional[dict] = rows[0]
        for other in rows[1:]:
            if len(other["title"]) == len(top["title"]) and other["tcgplayer_id"] != top["tcgplayer_id"]:
                top = None
                break
        if not top:
            continue
        try:
            tcg_id = int(top["tcgplayer_id"])
        except (TypeError, ValueError):
            continue

        r["tcgplayer_id"] = tcg_id
        r["tcgPlayerId"] = tcg_id
        scrydex_id = r.get("scrydexId") or r.get("scrydex_id")
        if scrydex_id and callable(saver):
            try:
                saver(scrydex_id, tcg_id)
            except Exception:
                pass
