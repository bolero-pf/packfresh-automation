"""
Price history snapshot.

Copies the current scrydex_price_cache into scrydex_price_history
so we have a daily record of market prices over time.

Must run BEFORE the nightly scrydex sync overwrites the cache.
In practice: the analytics pipeline runs first, snapshots yesterday's
prices (which are still in the cache), then scrydex_nightly.py updates
the cache with today's prices.
"""

import logging
from datetime import date

import db

logger = logging.getLogger(__name__)


def snapshot_scrydex_prices():
    """
    Snapshot current scrydex_price_cache into scrydex_price_history.
    Uses today's date as the snapshot_date. Idempotent via ON CONFLICT DO NOTHING.
    Returns count of rows inserted.
    """
    today = date.today()

    # Check if we already have a snapshot for today
    existing = db.query_one(
        "SELECT 1 FROM scrydex_price_history WHERE snapshot_date = %s LIMIT 1",
        (today,)
    )
    if existing:
        logger.info(f"Price history snapshot for {today} already exists, skipping")
        return {"date": str(today), "skipped": True, "inserted": 0}

    # Bulk copy from cache to history
    result = db.execute("""
        INSERT INTO scrydex_price_history (
            snapshot_date, scrydex_id, tcgplayer_id, expansion_id, expansion_name,
            product_type, product_name, variant, condition, price_type,
            grade_company, grade_value, market_price, low_price
        )
        SELECT
            %s, scrydex_id, tcgplayer_id, expansion_id, expansion_name,
            product_type, product_name, variant, condition, price_type,
            grade_company, grade_value, market_price, low_price
        FROM scrydex_price_cache
        WHERE market_price IS NOT NULL
        ON CONFLICT DO NOTHING
    """, (today,))

    inserted = result if isinstance(result, int) else 0
    logger.info(f"Price history snapshot: {inserted} rows for {today}")
    return {"date": str(today), "inserted": inserted}
