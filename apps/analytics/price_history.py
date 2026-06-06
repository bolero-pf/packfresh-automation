"""
Price history snapshot.

Copies the current scrydex_price_cache into scrydex_price_history so we have a
daily record of market prices over time.

Scope: only condition='NM', price_type='raw' rows are snapshotted — that is the
only slice analytics/margins.py ever reads (market price at time of sale by
tcgplayer_id). Storing every condition + graded row was ~5x the data for no
consumer.

Storage: scrydex_price_history is RANGE-partitioned by snapshot_date (one
partition per month, created on demand here). Partitions older than
RETENTION_DAYS are dropped each run, which keeps the nightly INSERT fast (it
only ever touches the small current-month partition) and bounds table growth.

Must run BEFORE the nightly scrydex sync overwrites the cache. In practice the
analytics pipeline runs first, snapshots yesterday's prices (still in the
cache), then scrydex_nightly.py updates the cache with today's prices.
"""

import logging
from datetime import date, timedelta

import db

logger = logging.getLogger(__name__)

RETENTION_DAYS = 90


def _month_floor(d):
    return d.replace(day=1)


def _next_month(d):
    d = _month_floor(d)
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _ensure_month_partition(d):
    """Create the monthly partition that holds date `d`, if it doesn't exist.
    Date values are derived from `d` (not user input), so formatting them into
    the DDL is safe."""
    start = _month_floor(d)
    nxt = _next_month(start)
    name = f"scrydex_price_history_{start:%Y%m}"
    db.execute(
        f"CREATE TABLE IF NOT EXISTS {name} PARTITION OF scrydex_price_history "
        f"FOR VALUES FROM ('{start:%Y-%m-%d}') TO ('{nxt:%Y-%m-%d}')"
    )
    return name


def _drop_old_partitions(keep_days=RETENTION_DAYS):
    """Drop month partitions whose entire range is older than the retention
    cutoff. Dropping a partition is instant and bloat-free vs a giant DELETE."""
    cutoff = date.today() - timedelta(days=keep_days)
    parts = db.query("""
        SELECT c.relname AS name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'scrydex_price_history'
    """)
    dropped = []
    for row in parts:
        name = row["name"]
        suffix = name.rsplit("_", 1)[-1]  # YYYYMM
        if len(suffix) != 6 or not suffix.isdigit():
            continue
        yr, mo = int(suffix[:4]), int(suffix[4:])
        part_end = date(yr + 1, 1, 1) if mo == 12 else date(yr, mo + 1, 1)
        if part_end <= cutoff:  # every day in this partition is past retention
            db.execute(f"DROP TABLE IF EXISTS {name}")
            dropped.append(name)
    if dropped:
        logger.info(f"Dropped {len(dropped)} expired price-history partitions: {dropped}")
    return dropped


def snapshot_scrydex_prices():
    """
    Snapshot current scrydex_price_cache (NM/raw only) into scrydex_price_history.
    Uses today's date as the snapshot_date. Idempotent via the per-date guard
    plus ON CONFLICT DO NOTHING. Returns count of rows inserted.
    """
    today = date.today()

    # Already snapshotted today? Bail before doing any work.
    existing = db.query_one(
        "SELECT 1 FROM scrydex_price_history WHERE snapshot_date = %s LIMIT 1",
        (today,)
    )
    if existing:
        logger.info(f"Price history snapshot for {today} already exists, skipping")
        return {"date": str(today), "skipped": True, "inserted": 0}

    # Make sure this month's partition exists, then bulk copy NM/raw rows into it.
    _ensure_month_partition(today)

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
          AND condition = 'NM'
          AND price_type = 'raw'
        ON CONFLICT DO NOTHING
    """, (today,))

    inserted = result if isinstance(result, int) else 0
    logger.info(f"Price history snapshot: {inserted} rows for {today}")

    # Retention: drop partitions past the window.
    _drop_old_partitions()

    return {"date": str(today), "inserted": inserted}
