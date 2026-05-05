"""
raw_card_updater — nightly raw-card price refresh.

For every raw card in stock (state IN ('STORED','DISPLAY'), not on hold):
DISPLAY cards (binders) need nightly repricing too — they're customer-
facing via kiosk and POS rings them up at current_price just like STORED.
  1. Skip cards in the price_auto_block list ('raw' domain) — escape
     hatch for runaway suggestions (e.g. a corrupted catalog mapping
     pricing a $10 card at $13k).
  2. Look up market price in scrydex_price_cache by
     (scrydex_id, variant, condition, price_type='raw'), falling back to
     tcgplayer_id when no scrydex_id is bound. Scrydex IDs are more
     specific (TCG IDs can collide across variants or be orphaned).
  3. Floor at cost_basis (never sell below cost).
  4. Charm-ceil round to a customer-friendly .99 price.
  5. Compare to raw_cards.current_price (delta = (old - suggested)/suggested):
       |delta| <= AUTO_DELTA_PCT  -> auto-apply (small drift, rebalance silently)
       delta < -AUTO_DELTA_PCT    -> auto-apply (raise — never miss a fast mover)
       delta >  AUTO_DELTA_PCT    -> flag_overpriced (review needed to drop)
  6. Persist every row to raw_card_price_runs for audit + per-row apply.

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
    """Round price UP to a 'charm' price tuned for raw cards.

    Tier scheme (denser at low prices since raw cards span $0.50-$2000+):
      <  $10    -> nearest .49 or .99 ($0.49, $0.99, $1.49, ..., $9.99)
      <  $100   -> nearest $1   ending in .99 ($10.99, $19.99, $99.99)
      <  $500   -> nearest $5   ending in .99 ($104.99, $124.99, $499.99)
      <  $2000  -> nearest $25  ending in .99 ($524.99, $1224.99)
      >= $2000  -> nearest $50  ending in .99 ($2049.99, $2099.99)

    Always ceils so we never undercut the market.

    Keep this in sync with shared/price_rounding.py::charm_ceil_raw.
    """
    try:
        p = float(price or 0)
    except (TypeError, ValueError):
        return 0.0
    if p <= 0:
        return 0.0
    if p < 10:
        # Tighter charm increments under $10 — a $1.03 card jumping to
        # $1.99 burned too much margin. Now $1.03 → $1.49, $1.50 → $1.99.
        floor = math.floor(p)
        for c in (floor + 0.49, floor + 0.99, floor + 1.49):
            if c >= p:
                return round(c, 2)
        return round(floor + 1.49, 2)
    if p < 100:
        increment = 1
    elif p < 500:
        increment = 5
    elif p < 2000:
        increment = 25
    else:
        increment = 50
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


# Look up market_price for a (scrydex_id|tcgplayer_id, variant, condition) tuple.
# Variant normalization: strip non-alphanumerics and lowercase so the three
# conventions in raw_cards.variant ("Alt Art", "altArt", "alt art") all
# collapse to the same key ("altart") and match the camelCase convention
# Scrydex uses in its cache. NULL/"normal"/"holofoil" all fold to the
# default bucket so single-printing cards still match.
#
# `currency` column: Scrydex sends JP-marketplace raw prices in JPY; the
# CASE expression converts to USD inline so the returned market_price is
# always USD regardless of the source row's currency.
#
# JP price-point selection: For JPY rows, prefer mid_price over market_price.
# Scrydex's JP "market" oscillates between {low, mid, high} per sync — for
# Blaine's Arcanine the cache moved from low (12000) to high (24000) overnight,
# producing 2× swings in nightly output. mid is the median observed price and
# is far more stable. Falls back to market when mid isn't populated (Scrydex
# only fills mid for ~32% of JP raw rows). USD rows use market unchanged —
# Scrydex's EN market is a properly-smoothed value, not a corner-pick.
_CACHE_LOOKUP_SQL_TEMPLATE = """
    SELECT
        CASE WHEN currency = 'JPY' AND mid_price IS NOT NULL
             THEN ROUND(mid_price::numeric * %s::numeric, 2)
             WHEN currency = 'JPY'
             THEN ROUND(market_price::numeric * %s::numeric, 2)
             ELSE market_price END AS market_price,
        CASE WHEN currency = 'JPY' AND mid_price IS NOT NULL THEN 'mid_jpy'
             WHEN currency = 'JPY' THEN 'market_jpy'
             ELSE 'market' END AS price_basis,
        CASE WHEN currency = 'JPY'
             THEN ROUND(low_price::numeric * %s::numeric, 2)
             ELSE low_price END AS low_price,
        currency,
        scrydex_id, tcgplayer_id, variant
    FROM scrydex_price_cache
    WHERE {key_clause}
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

_CACHE_LOOKUP_BY_SCRYDEX = _CACHE_LOOKUP_SQL_TEMPLATE.format(key_clause="scrydex_id = %s")
_CACHE_LOOKUP_BY_TCG     = _CACHE_LOOKUP_SQL_TEMPLATE.format(key_clause="tcgplayer_id = %s")

# Matches shared/price_cache.py and ingestion/app.py. Override via env var
# when the yen moves materially.
_JPY_USD_RATE = float(os.getenv("SCRYDEX_JPY_USD_RATE", "0.0066"))


def _lookup_one(db_module, scrydex_id, tcgplayer_id, condition, variant) -> dict | None:
    """Single-condition cache lookup. Scrydex-first, TCG fallback."""
    if not condition:
        return None
    # Three JPY-rate placeholders now: mid, market, low (mid was added to
    # stabilize JP pricing — see SQL comment above).
    if scrydex_id:
        rows = db_module.query(
            _CACHE_LOOKUP_BY_SCRYDEX,
            (_JPY_USD_RATE, _JPY_USD_RATE, _JPY_USD_RATE, scrydex_id,
             condition, variant, variant, variant),
        )
        if rows:
            return rows[0]
    if tcgplayer_id:
        rows = db_module.query(
            _CACHE_LOOKUP_BY_TCG,
            (_JPY_USD_RATE, _JPY_USD_RATE, _JPY_USD_RATE, int(tcgplayer_id),
             condition, variant, variant, variant),
        )
        if rows:
            return rows[0]
    return None


def _lookup_cache_price(db_module, scrydex_id, tcgplayer_id, condition, variant) -> dict | None:
    """Scrydex-first lookup with TCG fallback AND condition fallback.

    Tries exact (cond, variant) first. If miss and condition != NM, falls
    back to NM × FALLBACK_MULTIPLIERS[condition] so a damaged JP card in
    an NM-only-cached printing still gets a derived price (instead of
    being skipped on every nightly run forever). The returned dict carries
    a `source` field — 'exact' or 'nm_fallback' — so the caller can record
    an honest reason in raw_card_price_runs.

    JP cards (currency='JPY' on the NM source row) use JP_FALLBACK_MULTIPLIERS
    (DMG=0.10, HP=0.20, MP=0.40, LP=0.65) — the JP collector market is much
    more condition-strict than EN and the EN curve overprices off-NM JP cards
    by 2-4×. Multiplier table choice is recorded in the source dict so the
    audit string in raw_card_price_runs.reason names which one ran.
    """
    if not condition:
        return None

    # 1. Exact-condition match — preferred.
    hit = _lookup_one(db_module, scrydex_id, tcgplayer_id, condition, variant)
    if hit and hit.get("market_price") is not None:
        hit = dict(hit)
        hit["source"] = "exact"
        return hit

    # 2. NM fallback × multiplier — never overrides a real per-condition row,
    # only fills the gap when the cache has no row for this condition. NM
    # itself never falls back (it IS the base).
    cond_upper = (condition or "").upper()
    if cond_upper == "NM":
        return None
    nm_hit = _lookup_one(db_module, scrydex_id, tcgplayer_id, "NM", variant)
    if not nm_hit or nm_hit.get("market_price") is None:
        return None

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
    is_jp = (nm_hit.get("currency") or "").upper() == "JPY"
    if is_jp:
        from ppt_client import JP_FALLBACK_MULTIPLIERS as _MULTS
        mult_table = "jp"
    else:
        from ppt_client import FALLBACK_MULTIPLIERS as _MULTS
        mult_table = "en"
    mult = _MULTS.get(cond_upper)
    if mult is None:
        return None

    nm_market = Decimal(str(nm_hit["market_price"]))
    derived = (nm_market * mult).quantize(Decimal("0.01"))
    nm_low = nm_hit.get("low_price")
    derived_low = (Decimal(str(nm_low)) * mult).quantize(Decimal("0.01")) if nm_low is not None else None
    return {
        "market_price": derived,
        "low_price":    derived_low,
        "scrydex_id":   nm_hit.get("scrydex_id"),
        "tcgplayer_id": nm_hit.get("tcgplayer_id"),
        "variant":      nm_hit.get("variant"),
        "source":       "nm_fallback",
        "nm_market":    nm_market,
        "multiplier":   mult,
        "mult_table":   mult_table,
        "nm_basis":     nm_hit.get("price_basis"),
    }


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

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
    from price_auto_block import load_blocks, raw_key
    from price_rounding import charm_drop_auto_threshold

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    logger.info(f"raw_card_updater run_id={run_id} apply_auto={apply_auto}")

    blocked = load_blocks(db_module, "raw")
    if blocked:
        logger.info(f"  {len(blocked)} cards on raw price-auto-block list")

    cards = db_module.query("""
        SELECT id, barcode, tcgplayer_id, scrydex_id, card_name, set_name,
               card_number, condition, variant, current_price, cost_basis,
               state
        FROM raw_cards
        WHERE state IN ('STORED', 'DISPLAY') AND current_hold_id IS NULL
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

        block_key = raw_key(card["scrydex_id"], card["tcgplayer_id"])
        if block_key and block_key in blocked:
            entry.update({"action": "skip", "reason": f"auto-block ({block_key})"})
            stats["skip"] += 1
            _record(db_module, run_id, started_at, entry)
            continue

        if not card["tcgplayer_id"] and not card["scrydex_id"]:
            entry.update({"action": "skip", "reason": "no scrydex_id or tcgplayer_id"})
            stats["skip"] += 1
            _record(db_module, run_id, started_at, entry)
            continue

        cache = _lookup_cache_price(
            db_module, card["scrydex_id"], card["tcgplayer_id"],
            card["condition"], card["variant"])
        if not cache or cache.get("market_price") is None:
            entry.update({
                "action": "skip",
                "reason": (f"no cache price for scrydex={card['scrydex_id']!r} "
                           f"tcg={card['tcgplayer_id']} "
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

        # Annotate the audit trail when we're pricing off NM × multiplier
        # rather than a real per-condition row — operator should be able to
        # see at a glance why a DMG card got a price despite the cache only
        # having NM. Names the multiplier table (jp vs en — JP uses tighter
        # multipliers because JP collectors only buy NM-MT) and the source
        # price basis (mid_jpy means we used the median of Scrydex's
        # low/mid/high range to dodge their day-over-day market swings).
        source_note = ""
        if cache.get("source") == "nm_fallback":
            mult = cache.get("multiplier")
            nm_m = cache.get("nm_market")
            tbl = cache.get("mult_table") or ""
            basis = cache.get("nm_basis") or ""
            tag = f"{tbl}/{basis}" if (tbl or basis) else ""
            source_note = (f"NM-fallback[{tag}] ${float(nm_m):.2f} × {float(mult):.2f} "
                           f"({card['condition']}) → ${market:.2f}; ")

        entry.update({
            "cache_market":    market,
            "cache_low":       float(cache["low_price"]) if cache.get("low_price") is not None else None,
            "suggested_price": suggested,
            "delta_pct":       round(delta_pct, 2),
            "scrydex_id":      entry["scrydex_id"] or cache.get("scrydex_id"),
        })

        # Decision tree:
        #   |delta| <= SKIP                            : ok (no-op)
        #   delta > AUTO  AND drop_$ > tier_threshold  : flag_overpriced (review)
        #   otherwise (small drift, raise, charm-tier drop) : auto-apply
        # Raw cards live on the in-store hub, not advertised online — no
        # Shopify price-drop notifications fire here, so drops can flow
        # through silently as long as they're within the charm tier (e.g.
        # $1.49 -> $0.99 = 50¢ on sub-$10). Bigger drops still flag as a
        # defense against data corruption (a corrupted comp pulling a $50
        # card to $5 should land in review). Sealed has its own review-
        # gate because it has an online notification audience.
        drop_dollars = abs(old - suggested) if delta_pct > 0 else 0.0
        drop_threshold = charm_drop_auto_threshold(suggested)
        small_dollar_drop = (delta_pct > AUTO_DELTA_PCT
                             and drop_dollars <= drop_threshold)

        if abs(delta_pct) <= SKIP_DELTA_PCT:
            entry.update({
                "action": "ok",
                "reason": source_note + f"already at suggested ({delta_pct:+.2f}%)",
            })
            stats["ok"] += 1

        elif delta_pct > AUTO_DELTA_PCT and not small_dollar_drop:
            entry.update({
                "action": "flag_overpriced",
                "reason": source_note + (f"overpriced {delta_pct:+.1f}% (${drop_dollars:.2f} drop "
                           f"> ${drop_threshold:.2f} tier) — review"),
            })
            stats["flag_overpriced"] += 1

        else:
            if floor and suggested < floor:
                entry.update({
                    "action": "skip",
                    "reason": source_note + f"suggested ${suggested:.2f} below cost ${floor:.2f}",
                })
                stats["skip"] += 1
            elif apply_auto:
                try:
                    _apply_db_price(db_module, entry["raw_card_id"], suggested)
                    if delta_pct < -AUTO_DELTA_PCT:
                        why = (f"auto-raised {abs(delta_pct):.1f}% to follow market; "
                               f"${old:.2f} -> ${suggested:.2f}")
                    elif small_dollar_drop:
                        why = (f"auto-dropped ${drop_dollars:.2f} (within "
                               f"${drop_threshold:.2f} charm tier); "
                               f"${old:.2f} -> ${suggested:.2f}")
                    else:
                        why = (f"auto-applied {delta_pct:+.1f}% drift; "
                               f"${old:.2f} -> ${suggested:.2f}")
                    entry.update({
                        "action": "auto_applied",
                        "new_price": suggested,
                        "applied_at": datetime.now(timezone.utc),
                        "applied_price": suggested,
                        "apply_status": "applied",
                        "reason": source_note + why,
                    })
                    stats["auto_applied"] += 1
                except Exception as e:
                    entry.update({"action": "error", "reason": f"DB update failed: {e}"})
                    stats["error"] += 1
            else:
                entry.update({
                    "action": "auto_applied",
                    "reason": source_note + f"[DRY-RUN] would auto-apply {delta_pct:+.1f}%",
                    "apply_status": "pending",
                })
                stats["auto_applied"] += 1

        _record(db_module, run_id, started_at, entry)

    logger.info(
        f"raw_card_updater done run_id={run_id} "
        f"auto={stats['auto_applied']} flag_over={stats['flag_overpriced']} "
        f"flag_under={stats['flag_underpriced']} ok={stats['ok']} "
        f"skip={stats['skip']} error={stats['error']}"
    )
    return stats


if __name__ == "__main__":
    # Subprocess entrypoint so review_dashboard.py can launch this as a
    # separate process (output goes to stdout, parent tees into RUN_LOG).
    import argparse as _argparse
    from dotenv import load_dotenv as _load_dotenv

    _load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    p = _argparse.ArgumentParser(description="Nightly raw card price updater")
    p.add_argument("--dry-run", action="store_true",
                   help="Don't write raw_cards.current_price (default: apply auto-raises)")
    args = p.parse_args()

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
    import db as _db
    _db.init_pool()
    run(apply_auto=not args.dry_run, db_module=_db)
