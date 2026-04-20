"""
raw_card_updater — nightly raw-card price refresh.

For every raw card in stock (state='STORED', not on hold):
  1. Look up market price in scrydex_price_cache by
     (tcgplayer_id, variant, condition, price_type='raw').
  2. Floor at cost_basis (never sell below cost).
  3. Charm-ceil round to a customer-friendly .99 price.
  4. Compare to raw_cards.current_price:
       |delta| <= AUTO_DELTA_PCT  -> auto-apply (small drift, rebalance silently)
       |delta| >  AUTO_DELTA_PCT  -> flag for review in /dashboard/raw-runs
  5. Persist every row to raw_card_price_runs for audit + per-row apply.

No Shopify mutations — raw card listings are created on-demand at Champion
checkout from the live raw_cards.current_price. Updating the DB is the
source of truth.

Triggered by Shopify Flow at e.g. 5 AM UTC (after Scrydex sync at 1 AM and
slab updater at 3:30 AM).
"""
import math
import os
import sys
import uuid
import logging
from datetime import datetime, timezone
from decimal import Decimal

logger = logging.getLogger(__name__)

# Decision thresholds
AUTO_DELTA_PCT = 10.0   # within +/-10%, auto-apply silently
SKIP_DELTA_PCT = 0.5    # within +/-0.5%, treat as no-op (don't burn a write)


def charm_ceil_raw(price) -> float:
    """Round price UP to a 'charm' .99 price tuned for raw cards.

    Tier scheme (denser at low prices since raw cards span $0.50-$500+):
      <  $20   -> nearest $1   ending in .99 ($1.99, $2.99, ..., $19.99)
      <  $100  -> nearest $5   ending in .99 ($24.99, $29.99, ..., $99.99)
      <  $500  -> nearest $10  ending in .99 ($109.99, $119.99, ...)
      >= $500  -> nearest $25  ending in .99 ($524.99, $549.99, ...)

    Always ceils so we never undercut the market.
    """
    try:
        p = float(price or 0)
    except (TypeError, ValueError):
        return 0.0
    if p <= 0:
        return 0.0
    if p < 20:
        increment = 1
    elif p < 100:
        increment = 5
    elif p < 500:
        increment = 10
    else:
        increment = 25
    next_step = math.ceil(p / increment) * increment
    candidate = next_step - 0.01
    if candidate < p:
        candidate = next_step + increment - 0.01
    return round(candidate, 2)


_INSERT_RUN_SQL = """
    INSERT INTO raw_card_price_runs (
        run_id, started_at,
        raw_card_id, barcode, tcgplayer_id, scrydex_id, card_name, set_name,
        card_number, condition, variant, cost_basis,
        old_price, new_price, suggested_price, cache_market, cache_low,
        delta_pct, action, reason, apply_status, applied_at, applied_price
    ) VALUES (
        %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
"""


def _record(db_module, run_id: str, started_at: datetime, entry: dict):
    try:
        db_module.execute(_INSERT_RUN_SQL, (
            run_id, started_at,
            entry.get("raw_card_id"), entry.get("barcode"),
            entry.get("tcgplayer_id"), entry.get("scrydex_id"),
            entry.get("card_name"), entry.get("set_name"),
            entry.get("card_number"), entry.get("condition"),
            entry.get("variant"), entry.get("cost_basis"),
            entry.get("old_price"), entry.get("new_price"),
            entry.get("suggested_price"), entry.get("cache_market"),
            entry.get("cache_low"), entry.get("delta_pct"),
            entry.get("action"), entry.get("reason"),
            entry.get("apply_status", "pending"),
            entry.get("applied_at"), entry.get("applied_price"),
        ))
    except Exception as e:
        logger.warning(f"Failed to persist raw_card_price_runs row "
                       f"for {entry.get('barcode')}: {e}")


# Look up market_price for a (tcgplayer_id, variant, condition) tuple.
# Variant normalization: strip non-alphanumerics and lowercase so the three
# conventions in raw_cards.variant ("Alt Art", "altArt", "alt art") all
# collapse to the same key ("altart") and match the camelCase convention
# Scrydex uses in its cache. NULL/"normal"/"holofoil" all fold to the
# default bucket so single-printing cards still match.
_CACHE_LOOKUP_SQL = """
    SELECT market_price, low_price, scrydex_id, variant
    FROM scrydex_price_cache
    WHERE tcgplayer_id = %s
      AND product_type = 'card'
      AND price_type   = 'raw'
      AND UPPER(condition) = UPPER(%s)
      AND CASE WHEN variant IS NULL
                 OR regexp_replace(LOWER(variant), '[^a-z0-9]', '', 'g') IN ('normal','holofoil')
               THEN ''
               ELSE regexp_replace(LOWER(variant), '[^a-z0-9]', '', 'g')
          END
        = CASE WHEN %s IS NULL
                 OR regexp_replace(LOWER(%s), '[^a-z0-9]', '', 'g') IN ('normal','holofoil')
               THEN ''
               ELSE regexp_replace(LOWER(%s), '[^a-z0-9]', '', 'g')
          END
      AND market_price IS NOT NULL
    ORDER BY fetched_at DESC NULLS LAST
    LIMIT 1
"""


def _lookup_cache_price(db_module, tcgplayer_id, condition, variant) -> dict | None:
    if not tcgplayer_id or not condition:
        return None
    rows = db_module.query(
        _CACHE_LOOKUP_SQL,
        (int(tcgplayer_id), condition, variant, variant, variant),
    )
    return rows[0] if rows else None


def _apply_db_price(db_module, raw_card_id: str, new_price: float) -> None:
    """Update raw_cards.current_price for one card."""
    db_module.execute(
        """UPDATE raw_cards
              SET current_price = %s, last_price_update = NOW()
            WHERE id = %s""",
        (round(float(new_price), 2), raw_card_id),
    )


def run(*, apply_auto: bool = True, db_module=None) -> dict:
    """Scan in-stock raw cards and apply / flag price updates.

    apply_auto=True  : auto-apply small (<= AUTO_DELTA_PCT) deltas immediately;
                       larger deltas get logged as flag_* for human review.
    apply_auto=False : pure dry-run, no DB writes to raw_cards (still records
                       to raw_card_price_runs for visibility).

    Returns stats dict with counts per action and a run_id.
    """
    if db_module is None:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
        import db as db_module
        db_module.init_pool()

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    logger.info(f"raw_card_updater run_id={run_id} apply_auto={apply_auto}")

    cards = db_module.query("""
        SELECT id, barcode, tcgplayer_id, scrydex_id, card_name, set_name,
               card_number, condition, variant, current_price, cost_basis
        FROM raw_cards
        WHERE state = 'STORED' AND current_hold_id IS NULL
          AND is_graded = FALSE
        ORDER BY card_name, set_name
    """)
    logger.info(f"  scanning {len(cards)} in-stock raw cards")

    stats = {
        "run_id": run_id, "scanned": len(cards),
        "auto_applied": 0, "flag_overpriced": 0, "flag_underpriced": 0,
        "ok": 0, "skip": 0, "error": 0,
    }

    for card in cards:
        entry = {
            "raw_card_id": str(card["id"]),
            "barcode":     card["barcode"],
            "tcgplayer_id": card["tcgplayer_id"],
            "scrydex_id":  card["scrydex_id"],
            "card_name":   card["card_name"],
            "set_name":    card["set_name"],
            "card_number": card["card_number"],
            "condition":   card["condition"],
            "variant":     card["variant"],
            "cost_basis":  float(card["cost_basis"]) if card["cost_basis"] is not None else None,
            "old_price":   float(card["current_price"]) if card["current_price"] is not None else None,
        }

        if not card["tcgplayer_id"]:
            entry.update({"action": "skip", "reason": "no tcgplayer_id"})
            stats["skip"] += 1
            _record(db_module, run_id, started_at, entry)
            continue

        cache = _lookup_cache_price(
            db_module, card["tcgplayer_id"], card["condition"], card["variant"])
        if not cache or cache.get("market_price") is None:
            entry.update({
                "action": "skip",
                "reason": (f"no cache price for tcg={card['tcgplayer_id']} "
                           f"variant={card['variant']!r} cond={card['condition']!r}"),
            })
            stats["skip"] += 1
            _record(db_module, run_id, started_at, entry)
            continue

        market = float(cache["market_price"])
        floor = float(card["cost_basis"]) if card["cost_basis"] is not None else 0.0
        target_raw = max(market, floor)
        suggested = charm_ceil_raw(target_raw)
        old = entry["old_price"] or 0.0
        delta_pct = ((old - suggested) / suggested * 100.0) if suggested > 0 else 0.0

        entry.update({
            "cache_market":    market,
            "cache_low":       float(cache["low_price"]) if cache.get("low_price") is not None else None,
            "suggested_price": suggested,
            "delta_pct":       round(delta_pct, 2),
            "scrydex_id":      entry["scrydex_id"] or cache.get("scrydex_id"),
        })

        if abs(delta_pct) <= SKIP_DELTA_PCT:
            entry.update({
                "action": "ok",
                "reason": f"already at suggested ({delta_pct:+.2f}%)",
            })
            stats["ok"] += 1

        elif abs(delta_pct) <= AUTO_DELTA_PCT:
            # Small drift — auto-apply unless we'd dip below cost basis
            if floor and suggested < floor:
                entry.update({
                    "action": "skip",
                    "reason": f"suggested ${suggested:.2f} below cost ${floor:.2f}",
                })
                stats["skip"] += 1
            elif apply_auto:
                try:
                    _apply_db_price(db_module, entry["raw_card_id"], suggested)
                    entry.update({
                        "action": "auto_applied",
                        "new_price": suggested,
                        "applied_at": datetime.now(timezone.utc),
                        "applied_price": suggested,
                        "apply_status": "applied",
                        "reason": (f"auto-applied {delta_pct:+.1f}% drift; "
                                   f"${old:.2f} -> ${suggested:.2f}"),
                    })
                    stats["auto_applied"] += 1
                except Exception as e:
                    entry.update({"action": "error", "reason": f"DB update failed: {e}"})
                    stats["error"] += 1
            else:
                # Dry-run mode: would auto-apply but don't actually write
                entry.update({
                    "action": "auto_applied",
                    "reason": f"[DRY-RUN] would auto-apply {delta_pct:+.1f}%",
                    "apply_status": "pending",
                })
                stats["auto_applied"] += 1

        elif delta_pct > AUTO_DELTA_PCT:
            entry.update({
                "action": "flag_overpriced",
                "reason": f"overpriced {delta_pct:+.1f}% — review",
            })
            stats["flag_overpriced"] += 1
        else:  # delta_pct < -AUTO_DELTA_PCT
            entry.update({
                "action": "flag_underpriced",
                "reason": f"underpriced {delta_pct:+.1f}% vs suggested — review",
            })
            stats["flag_underpriced"] += 1

        _record(db_module, run_id, started_at, entry)

    logger.info(
        f"raw_card_updater done run_id={run_id} "
        f"auto={stats['auto_applied']} flag_over={stats['flag_overpriced']} "
        f"flag_under={stats['flag_underpriced']} ok={stats['ok']} "
        f"skip={stats['skip']} error={stats['error']}"
    )
    return stats
