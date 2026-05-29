"""English-name localization for Japanese-language Scrydex cards.

Sean's rule: no Japanese characters in any UI. When a card's scrydex_id is
from a JP set, display the English equivalent with a " (JP)" suffix instead
of the Japanese product/expansion name. JP scrydex ids contain '_ja-'.

Used by ingestion (raw_card INSERT) and ingest-service (intake_items
backfill). Both sides write the localized form to their own table so kiosk,
card_manager, and ingest dashboards all read clean English names.
"""

JP_SUFFIX = " (JP)"


def is_japanese_scrydex_id(scrydex_id) -> bool:
    return bool(scrydex_id) and "_ja-" in scrydex_id


def _with_jp_suffix(name: str) -> str:
    if not name:
        return name
    if name.endswith(JP_SUFFIX) or name.endswith("(JP)"):
        return name
    return f"{name}{JP_SUFFIX}"


def localize_card_and_set(db, scrydex_id, fallback_name, fallback_set_name):
    """Return (card_name, set_name) for display.

    - Non-JP scrydex ids: pass the fallbacks through unchanged.
    - JP scrydex ids: look up Scrydex's English product_name_en /
      expansion_name_en and return them with " (JP)" appended. Falls back
      to the JP form + " (JP)" if no English translation exists in the
      cache (e.g., Japan-only cards Scrydex has no EN mapping for) — Sean
      can hand-edit those, but they still get the language tag.
    """
    if not is_japanese_scrydex_id(scrydex_id):
        return fallback_name, fallback_set_name
    try:
        row = db.query_one(
            """SELECT product_name_en, expansion_name_en, product_name, expansion_name
                 FROM scrydex_price_cache
                WHERE scrydex_id = %s
                ORDER BY (product_name_en IS NULL), fetched_at DESC
                LIMIT 1""",
            (scrydex_id,),
        )
    except Exception:
        return fallback_name, fallback_set_name
    if not row:
        return _with_jp_suffix(fallback_name), _with_jp_suffix(fallback_set_name)
    en_name = (row.get("product_name_en") or "").strip() or fallback_name
    en_set = (row.get("expansion_name_en") or "").strip() or fallback_set_name
    return _with_jp_suffix(en_name), _with_jp_suffix(en_set)
