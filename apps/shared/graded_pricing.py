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

import math
import os
import logging
from datetime import datetime, timedelta, timezone
from statistics import median as _median
from typing import Optional

logger = logging.getLogger(__name__)


# ── Market price computation ─────────────────────────────────────────────────
# IQR outlier removal → protect recent tail → exponential recency weighting.
# Half-life of 14 days: a sale today weighs ~16× more than one 8 weeks ago.

_HALF_LIFE_DAYS = 14.0
_DECAY = math.log(2) / _HALF_LIFE_DAYS


def _compute_smart_market(sales: list[dict], now: datetime) -> tuple[float, int, int]:
    """
    Compute market price from raw sales using:
      1. IQR-based outlier removal (1.5× IQR)
      2. Protect the most recent N sales from being dropped as outliers —
         they represent the current market, not noise
      3. Exponential recency-weighted average (14-day half-life)

    Returns (market_price, kept_count, dropped_count).
    """
    if not sales:
        return 0.0, 0, 0

    # Sort by date descending (most recent first); undated go to the end
    min_dt = datetime.min.replace(tzinfo=timezone.utc)
    ordered = sorted(sales, key=lambda s: s["date"] or min_dt, reverse=True)
    n = len(ordered)

    if n < 4:
        avg = round(sum(s["price"] for s in ordered) / n, 2)
        return avg, n, 0

    # IQR bounds
    sorted_prices = sorted(s["price"] for s in ordered)
    q1 = sorted_prices[n // 4]
    q3 = sorted_prices[(3 * n) // 4]
    iqr = q3 - q1
    low_bound  = q1 - 1.5 * iqr
    high_bound = q3 + 1.5 * iqr

    # Protect the tail: most recent N sales can't be outliers.
    # Scale with dataset size — at least 5, up to ~10% of total, capped at 20.
    protect_n = min(max(n // 10, 5), 20, n)

    kept = []
    dropped = 0
    for i, s in enumerate(ordered):
        p = s["price"]
        in_bounds = low_bound <= p <= high_bound
        in_tail   = i < protect_n
        if in_bounds or in_tail:
            kept.append(s)
        else:
            dropped += 1

    if not kept:
        kept = ordered  # safety: never drop everything

    # Recency-weighted average
    total_weight = 0.0
    weighted_sum = 0.0
    for s in kept:
        if s["date"]:
            days_ago = max(0, (now - s["date"]).total_seconds() / 86400)
        else:
            days_ago = 45.0  # undated gets mid-range weight
        w = math.exp(-_DECAY * days_ago)
        weighted_sum += s["price"] * w
        total_weight += w

    if total_weight > 0:
        market = round(weighted_sum / total_weight, 2)
    else:
        market = round(sum(s["price"] for s in kept) / len(kept), 2)

    return market, len(kept), dropped


def _resolve_scrydex_id(tcgplayer_id: int | None, db, *,
                        card_name: str = None, set_name: str = None,
                        card_number: str = None) -> str | None:
    """
    Resolve a scrydex_id via multiple strategies:
      1. tcgplayer_id direct lookup (fastest, most reliable)
      2. card_number + set_name (handles JP cards where name is in Japanese)
      3. card_name + set_name (English name search)
      4. card_name alone (broadest, least specific)
    """
    if tcgplayer_id:
        row = db.query_one("""
            SELECT scrydex_id FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND product_type = 'card'
            LIMIT 1
        """, (int(tcgplayer_id),))
        if row:
            return row["scrydex_id"]

    # Normalize card number: "052/173" → "52", "004" → "4"
    clean_num = None
    if card_number:
        clean_num = card_number.split("/")[0].lstrip("0") or "0"

    # Build partial set patterns: "Tag Team GX All Stars" → ['%tag%team%']
    # JP sets have mixed language names (TAG TEAM GX タッグオールスターズ) so
    # full-string ILIKE fails. Use just the first 2 significant words.
    set_patterns = []
    if set_name:
        words = [w for w in set_name.strip().split() if len(w) > 2]
        if len(words) >= 2:
            set_patterns.append(f"%{words[0]}%{words[1]}%")
        if words:
            set_patterns.append(f"%{words[0]}%")

    # Strategy 2: card_number + set (JP cards have Japanese names but same numbers)
    if clean_num:
        for pat in (set_patterns or [None]):
            where = "product_type = 'card' AND card_number = %s"
            params = [clean_num]
            if pat:
                where += " AND expansion_name ILIKE %s"
                params.append(pat)
            row = db.query_one(f"""
                SELECT scrydex_id FROM scrydex_price_cache
                WHERE {where}
                ORDER BY fetched_at DESC LIMIT 1
            """, tuple(params))
            if row:
                logger.info(f"Resolved scrydex_id by card# + set: #{clean_num} / pattern '{pat}' -> {row['scrydex_id']}")
                return row["scrydex_id"]

    # Strategy 3: name + set (English names)
    if card_name:
        # Strip parenthetical notes like "(JP)" from card name
        import re
        clean_name = re.sub(r'\s*\([^)]*\)\s*', ' ', card_name).strip()
        for pat in (set_patterns or [None]):
            where = "product_type = 'card' AND product_name ILIKE %s"
            params = [f"%{clean_name}%"]
            if pat:
                where += " AND expansion_name ILIKE %s"
                params.append(pat)
            row = db.query_one(f"""
                SELECT scrydex_id FROM scrydex_price_cache
                WHERE {where}
                ORDER BY fetched_at DESC LIMIT 1
            """, tuple(params))
            if row:
                logger.info(f"Resolved scrydex_id by name + set: '{clean_name}' / '{pat}' -> {row['scrydex_id']}")
                return row["scrydex_id"]

    logger.warning(f"Could not resolve scrydex_id for TCG#{tcgplayer_id} / "
                   f"'{card_name}' #{card_number} / set '{set_name}'")
    return None


def get_live_graded_comps(
    tcgplayer_id: int | None,
    grade_company: str,
    grade_value: str,
    db,
    *,
    days: int = 90,
    card_name: str = None,
    set_name: str = None,
    card_number: str = None,
    scrydex_id: str = None,
) -> Optional[dict]:
    """
    Fetch real eBay sold comps for a specific graded card from Scrydex listings API.

    Returns dict with market/low/mid/high, 7d/30d trends computed from sale dates,
    comp counts per window, and raw sales list for outlier visibility. Returns None
    if the card isn't in our Scrydex mapping or no comps found at all.

    Identification priority: scrydex_id (if supplied) → tcgplayer_id →
    name+set+number lookup. Pass scrydex_id directly when the item is
    Scrydex-only (e.g. JP cards with no TCGplayer marketplace mapping).

    Falls back to scrydex_price_cache (unreliable) if the live API call fails.
    """
    company = grade_company.upper().strip()
    grade   = str(grade_value).strip()

    if not scrydex_id:
        scrydex_id = _resolve_scrydex_id(tcgplayer_id, db, card_name=card_name,
                                         set_name=set_name, card_number=card_number)

    if not scrydex_id:
        logger.debug(f"No scrydex_id for TCG#{tcgplayer_id} / '{card_name}' #{card_number} — falling back to cache")
        if tcgplayer_id:
            return _fallback_from_cache(tcgplayer_id, company, grade, db)
        return None

    # Try live listings
    result = _fetch_live(scrydex_id, company, grade, db, days=days)
    if result:
        return result

    # Live failed — fall back to cache. Use scrydex_id directly so JP cards
    # (no tcgplayer_id) still resolve via the cache.
    return _fallback_from_cache(tcgplayer_id, company, grade, db, scrydex_id=scrydex_id)


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
    undated_count = sum(1 for s in sales if not s["date"])
    prices_7d    = [s["price"] for s in sales if s["date"] and (now - s["date"]).days <= 7]
    prices_30d   = [s["price"] for s in sales if s["date"] and (now - s["date"]).days <= 30]
    prices_older = [s["price"] for s in sales if s["date"] and 30 < (now - s["date"]).days <= days]

    if undated_count > 0:
        logger.warning(f"  {undated_count}/{len(sales)} listings have no parseable date")

    # Velocity-adaptive window: use just enough recent sales for a reliable
    # signal, not so many that old prices dilute a moving market.
    #
    # Target ~15 non-outlier sales — enough for confidence, few enough to
    # track momentum. Lookback = target / velocity (sales per day).
    # Floor 3 days, cap 30 days.
    dated_sorted = sorted(
        [s for s in sales if s["date"]],
        key=lambda s: s["date"], reverse=True,
    )
    if len(dated_sorted) >= 2:
        span_days = max(1, (dated_sorted[0]["date"] - dated_sorted[-1]["date"]).days)
        velocity = len(dated_sorted) / span_days  # sales per day
    else:
        velocity = 0

    target_n = 15
    if velocity > 0:
        lookback_days = max(3, min(30, round(target_n / velocity)))
    else:
        lookback_days = 30

    recent_sales = [s for s in sales if s["date"] and (now - s["date"]).days <= lookback_days]
    if len(recent_sales) < 5:
        # Not enough in adaptive window — widen to 30d
        recent_sales = [s for s in sales if s["date"] and (now - s["date"]).days <= 30]
    if not recent_sales:
        recent_sales = sales

    market, kept, dropped = _compute_smart_market(recent_sales, now)
    logger.info(f"  Velocity: {velocity:.1f}/day, lookback: {lookback_days}d, "
                f"fed {len(recent_sales)} sales into market calc")

    # Simple window averages for context (no outlier removal on these — raw signal)
    avg_7d  = round(sum(prices_7d) / len(prices_7d), 2) if prices_7d else None
    avg_30d = round(sum(prices_30d) / len(prices_30d), 2) if prices_30d else None

    # Trend: compare recent average to older average
    trend_7d = _compute_trend(prices_7d, prices_older)
    trend_30d = _compute_trend(prices_30d, prices_older)

    # Sort sales by date descending for UI (most recent first)
    dated_sales = [{"price": s["price"], "date": s["date"].isoformat() if s["date"] else None}
                   for s in sorted(sales, key=lambda x: x["date"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)]

    logger.info(f"Live comps for {scrydex_id} {company} {grade}: "
                f"{len(sales)} total, {kept} kept, {dropped} outliers dropped, "
                f"market ${market:.2f} (7d avg ${avg_7d}, 30d avg ${avg_30d}), "
                f"range ${prices_all[0]:.2f}-${prices_all[-1]:.2f}")
    return {
        "market":        market,      # recency-weighted, outlier-cleaned
        "avg_7d":        avg_7d,      # raw 7d average (no outlier removal)
        "avg_30d":       avg_30d,     # raw 30d average
        "low":           prices_all[0],
        "mid":           med_all,
        "high":          prices_all[-1],
        "trend_7d_pct":  trend_7d,
        "trend_30d_pct": trend_30d,
        "trend_1d_pct":  None,
        "fetched_at":    now.isoformat(),
        "suggested_price": market,
        "comps_count":   len(sales),
        "comps_kept":    kept,
        "outliers_dropped": dropped,
        "comps_7d":      len(prices_7d),
        "comps_30d":     len(prices_30d),
        "undated_count": undated_count,
        "source":        "live_listings",
        "sales":         dated_sales,
    }


def get_all_graded_comps(tcgplayer_id: int | None, db, *, days: int = 90,
                         card_name: str = None, set_name: str = None,
                         card_number: str = None,
                         scrydex_id: str = None,
                         variant: str = None) -> dict:
    """
    Fetch live eBay comps for ALL grades of a card in a single API call.

    Returns dict matching the shape of PriceProvider.extract_graded_prices():
        {"PSA": {"10": {"price": 450.0, "confidence": "high", "count": 67, ...}, "9": {...}}, ...}

    One Scrydex credit. Used by the grading economics calculator so the 60/40
    EV computation uses real market data instead of unreliable cache aggregates.

    scrydex_id: pass directly to skip the tcgplayer_id → scrydex_id resolution
    step. Required for Scrydex-only cards that have no TCG marketplace mapping.
    variant: when set (e.g. 'firstEditionHolofoil' vs 'unlimitedHolofoil'),
    drop listings that don't match. Critical for cards where the 1st edition
    and unlimited printings share a TCG product ID but trade at wildly
    different prices (Sabrina's Alakazam 1st Ed PSA 10 is ~$4k, unlimited
    is a fraction of that).
    """
    company_map = {}

    if not scrydex_id:
        scrydex_id = _resolve_scrydex_id(tcgplayer_id, db, card_name=card_name,
                                         set_name=set_name, card_number=card_number)
    if not scrydex_id:
        return {}

    sx_key  = os.getenv("SCRYDEX_API_KEY", "")
    sx_team = os.getenv("SCRYDEX_TEAM_ID", "")
    if not sx_key or not sx_team:
        return {}

    try:
        from scrydex_client import ScrydexClient
        sx = ScrydexClient(sx_key, sx_team, db=db)
        raw_listings = sx.get_card_listings(scrydex_id, days=days)
    except Exception as e:
        logger.warning(f"Scrydex listings call failed for {scrydex_id}: {e}")
        return {}

    if not raw_listings:
        return {}

    now = datetime.now(timezone.utc)

    # Filter by variant if requested — listings on Scrydex carry a 'variant'
    # field (firstEditionHolofoil, unlimitedHolofoil, reverseHolofoil, etc.)
    # and 1st Ed vs Unlimited for the same TCG product can trade at 10x apart.
    if variant:
        before = len(raw_listings)
        raw_listings = [l for l in raw_listings if (l.get("variant") or "") == variant]
        logger.info(f"Filtered {scrydex_id} listings to variant='{variant}': "
                    f"{len(raw_listings)}/{before} remain")
        if not raw_listings:
            return {}

    # Group all listings by company + grade
    from collections import defaultdict
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for l in raw_listings:
        co = (l.get("company") or "").upper()
        gr = str(l.get("grade", "")).strip()
        if not co or not gr or l.get("price") is None:
            continue
        sale_date = _parse_date(l)
        buckets[(co, gr)].append({"price": float(l["price"]), "date": sale_date})

    # Compute smart market for each company+grade bucket
    for (co, gr), sales in buckets.items():
        if not sales:
            continue

        # Velocity-adaptive window (same logic as single-grade)
        dated = sorted([s for s in sales if s["date"]], key=lambda s: s["date"], reverse=True)
        if len(dated) >= 2:
            span = max(1, (dated[0]["date"] - dated[-1]["date"]).days)
            vel = len(dated) / span
        else:
            vel = 0
        lookback = max(3, min(30, round(15 / vel))) if vel > 0 else 30
        recent = [s for s in sales if s["date"] and (now - s["date"]).days <= lookback]
        if len(recent) < 5:
            recent = [s for s in sales if s["date"] and (now - s["date"]).days <= 30]
        if not recent:
            recent = sales

        market, kept, dropped = _compute_smart_market(recent, now)
        count = len(sales)

        # Confidence from count
        confidence = "high" if count >= 10 else "medium" if count >= 4 else "low"

        company_map.setdefault(co, {})[gr] = {
            "price":      market,
            "confidence": confidence,
            "count":      count,
            "method":     "live_listings",
        }

    logger.info(f"All graded comps for {scrydex_id}: "
                f"{sum(len(g) for g in company_map.values())} grade buckets from "
                f"{len(raw_listings)} listings")
    return company_map


def _fallback_from_cache(tcgplayer_id, company: str, grade: str, db,
                         *, scrydex_id: str = None) -> Optional[dict]:
    """Read from scrydex_price_cache — unreliable for graded but better than nothing.
    Looks up by scrydex_id when supplied (preferred for JP cards that have no
    TCGplayer mapping), else by tcgplayer_id."""
    if scrydex_id:
        row = db.query_one("""
            SELECT market_price, low_price, mid_price, high_price,
                   trend_1d_pct, trend_7d_pct, trend_30d_pct, fetched_at
            FROM scrydex_price_cache
            WHERE scrydex_id = %s AND price_type = 'graded'
              AND grade_company = %s AND grade_value = %s
            ORDER BY fetched_at DESC LIMIT 1
        """, (scrydex_id, company, grade))
    elif tcgplayer_id:
        row = db.query_one("""
            SELECT market_price, low_price, mid_price, high_price,
                   trend_1d_pct, trend_7d_pct, trend_30d_pct, fetched_at
            FROM scrydex_price_cache
            WHERE tcgplayer_id = %s AND price_type = 'graded'
              AND grade_company = %s AND grade_value = %s
            ORDER BY fetched_at DESC LIMIT 1
        """, (int(tcgplayer_id), company, grade))
    else:
        return None
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


def _parse_date(listing: dict) -> Optional[datetime]:
    """Parse sale date from a Scrydex listing. Scrydex uses sold_at with slash format (2026/04/15)."""
    for field in ("sold_at", "date", "sold_date"):
        ds = listing.get(field)
        if not ds:
            continue
        try:
            s = str(ds).strip().replace("/", "-")
            # ISO format (most common) — also handles 2026/04/15 slash dates
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
