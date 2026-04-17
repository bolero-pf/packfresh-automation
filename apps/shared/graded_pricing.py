"""
graded_pricing.py — Live eBay comp data for graded slabs via Scrydex listings API.

The nightly-synced inline graded prices (scrydex_price_cache) are unreliable single-
value aggregates — often 3x off from real market (e.g. $229 cached vs $76 real on a
Celebi V PSA 9 with 67 actual comps). This module calls Scrydex's per-card listings
endpoint to get REAL eBay sold data, then computes stats + trends from individual sales.

Usage:
    from graded_pricing import get_live_graded_comps
    result = get_live_graded_comps(253138, "PSA", "9", db)
    # result["mid"] = 75.00 (median of 67 comps)
    # result["sales"] = [{price, date}, ...] for outlier inspection

Cost: 1 Scrydex credit per call. Acceptable for per-slab interactive actions (preview,
intake) and nightly batch updates (~100 slabs = ~100 credits).

Falls back to scrydex_price_cache if no SCRYDEX_API_KEY or API call fails.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from statistics import median as _median
from typing import Optional

logger = logging.getLogger(__name__)


def get_live_graded_comps(
    tcgplayer_id: int,
    grade_company: str,
    grade_value: str,
    db,
    *,
    days: int = 90,
) -> Optional[dict]:
    """
    Fetch real eBay sold comps for a specific graded card from Scrydex listings API.

    Returns dict with market/low/mid/high, 7d/30d trends computed from sale dates,
    comp counts per window, and raw sales list for outlier visibility. Returns None
    if the card isn't in our Scrydex mapping or no comps found at all.

    Falls back to scrydex_price_cache (unreliable) if API call fails.
    """
    company = grade_company.upper().strip()
    grade   = str(grade_value).strip()

    # Resolve scrydex_id from cache (free DB read, no API call)
    id_row = db.query_one("""
        SELECT scrydex_id FROM scrydex_price_cache
        WHERE tcgplayer_id = %s AND product_type = 'card'
        LIMIT 1
    """, (int(tcgplayer_id),))
    scrydex_id = id_row.get("scrydex_id") if id_row else None

    if not scrydex_id:
        logger.debug(f"No scrydex_id for TCG#{tcgplayer_id} — falling back to cache")
        return _fallback_from_cache(tcgplayer_id, company, grade, db)

    # Try live listings
    result = _fetch_live(scrydex_id, company, grade, db, days=days)
    if result:
        return result

    # Live failed — fall back to cache
    return _fallback_from_cache(tcgplayer_id, company, grade, db)


def _fetch_live(scrydex_id: str, company: str, grade: str, db, *, days: int = 90) -> Optional[dict]:
    """Call Scrydex listings API, filter to grade, compute stats + trends."""
    sx_key  = os.getenv("SCRYDEX_API_KEY", "")
    sx_team = os.getenv("SCRYDEX_TEAM_ID", "")
    if not sx_key or not sx_team:
        logger.debug("No SCRYDEX_API_KEY/TEAM_ID — skipping live listings")
        return None

    try:
        from scrydex_client import ScrydexClient
        sx = ScrydexClient(sx_key, sx_team, db=db)
        raw_listings = sx.get_card_listings(scrydex_id, days=days)
    except Exception as e:
        logger.warning(f"Scrydex listings call failed for {scrydex_id}: {e}")
        return None

    now = datetime.now(timezone.utc)

    # Filter to exact company + grade, parse dates
    sales = []
    for l in raw_listings:
        if ((l.get("company") or "").upper() != company
                or str(l.get("grade", "")) != grade
                or l.get("price") is None):
            continue
        price_val = float(l["price"])
        sale_date = _parse_date(l)
        sales.append({"price": price_val, "date": sale_date})

    if not sales:
        logger.debug(f"No {company} {grade} comps in {len(raw_listings)} listings for {scrydex_id}")
        return None

    prices_all = sorted(s["price"] for s in sales)
    med_all = round(_median(prices_all), 2)

    # Bucket by recency
    dated_sales_objs = [s for s in sales if s["date"]]
    undated_count = len(sales) - len(dated_sales_objs)
    prices_7d    = [s["price"] for s in sales if s["date"] and (now - s["date"]).days <= 7]
    prices_30d   = [s["price"] for s in sales if s["date"] and (now - s["date"]).days <= 30]
    prices_older = [s["price"] for s in sales if s["date"] and 30 < (now - s["date"]).days <= days]

    if undated_count > 0:
        logger.warning(f"  {undated_count}/{len(sales)} listings have no parseable date — "
                       f"trends may be inaccurate")

    # Market price: use 30-day average (current market), not 90-day average
    # which gets dragged down by older sales on appreciating cards. Fall back
    # to all-time average only if no dated 30d window.
    if prices_30d:
        market = round(sum(prices_30d) / len(prices_30d), 2)
    else:
        market = round(sum(prices_all) / len(prices_all), 2)

    # Trend: compare recent average to older average
    trend_7d = _compute_trend(prices_7d, prices_older)
    trend_30d = _compute_trend(prices_30d, prices_older)

    # Sort sales by date descending for UI (most recent first)
    dated_sales = [{"price": s["price"], "date": s["date"].isoformat() if s["date"] else None}
                   for s in sorted(sales, key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)]

    logger.info(f"Live comps for {scrydex_id} {company} {grade}: "
                f"{len(prices_all)} total (7d:{len(prices_7d)}, 30d:{len(prices_30d)}, "
                f"undated:{undated_count}), "
                f"${prices_all[0]:.2f}-${prices_all[-1]:.2f}, "
                f"30d avg ${market:.2f}, all-time median ${med_all:.2f}")

    return {
        "market":        market,
        "low":           prices_all[0],
        "mid":           med_all,
        "high":          prices_all[-1],
        "trend_7d_pct":  trend_7d,
        "trend_30d_pct": trend_30d,
        "trend_1d_pct":  None,
        "fetched_at":    now.isoformat(),
        "suggested_price": market,  # 30d avg as the anchor, not all-time median
        "comps_count":   len(prices_all),
        "comps_7d":      len(prices_7d),
        "comps_30d":     len(prices_30d),
        "undated_count": undated_count,
        "source":        "live_listings",
        "sales":         dated_sales,
    }


def _fallback_from_cache(tcgplayer_id: int, company: str, grade: str, db) -> Optional[dict]:
    """Read from scrydex_price_cache — unreliable for graded but better than nothing."""
    row = db.query_one("""
        SELECT market_price, low_price, mid_price, high_price,
               trend_1d_pct, trend_7d_pct, trend_30d_pct, fetched_at
        FROM scrydex_price_cache
        WHERE tcgplayer_id = %s AND price_type = 'graded'
          AND grade_company = %s AND grade_value = %s
        ORDER BY fetched_at DESC LIMIT 1
    """, (int(tcgplayer_id), company, grade))
    if not row:
        return None

    def _f(v):
        return float(v) if v is not None else None

    market = _f(row.get("market_price"))
    mid    = _f(row.get("mid_price"))
    return {
        "market":        market,
        "low":           _f(row.get("low_price")),
        "mid":           mid,
        "high":          _f(row.get("high_price")),
        "trend_1d_pct":  _f(row.get("trend_1d_pct")),
        "trend_7d_pct":  _f(row.get("trend_7d_pct")),
        "trend_30d_pct": _f(row.get("trend_30d_pct")),
        "fetched_at":    row["fetched_at"].isoformat() if row.get("fetched_at") else None,
        "suggested_price": market or mid,
        "comps_count":   None,
        "comps_7d":      None,
        "comps_30d":     None,
        "source":        "cache",
        "sales":         [],
    }


_DATE_FIELDS_LOGGED = False

def _parse_date(listing: dict) -> Optional[datetime]:
    """Try all plausible date field names from Scrydex listing data."""
    global _DATE_FIELDS_LOGGED
    if not _DATE_FIELDS_LOGGED:
        # Log the actual keys from the first listing so we know what Scrydex sends
        logger.info(f"Scrydex listing keys: {sorted(listing.keys())}")
        _DATE_FIELDS_LOGGED = True

    # Try every plausible field name
    for field in ("date", "sold_date", "sold_at", "listed_at", "created_at",
                  "end_time", "endTime", "sale_date", "timestamp", "time",
                  "endedAt", "ended_at", "closedAt", "closed_at"):
        ds = listing.get(field)
        if not ds:
            continue
        try:
            s = str(ds).strip()
            # ISO format (most common)
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
        # Try epoch timestamp (seconds or milliseconds)
        try:
            ts = float(ds)
            if ts > 1e12:  # milliseconds
                ts /= 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            if dt.year >= 2020:  # sanity check
                return dt
        except Exception:
            pass
    return None


def _compute_trend(recent_prices: list[float], older_prices: list[float]) -> Optional[float]:
    """Percent change: recent avg vs older avg. Returns None if insufficient data."""
    if not recent_prices or not older_prices:
        return None
    avg_recent = sum(recent_prices) / len(recent_prices)
    avg_older  = sum(older_prices) / len(older_prices)
    if avg_older <= 0:
        return None
    return round((avg_recent - avg_older) / avg_older * 100, 1)
