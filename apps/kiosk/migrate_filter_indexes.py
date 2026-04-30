"""
Add GIN indexes on scrydex_card_meta JSONB columns used by kiosk filter queries.

The kiosk public-facing browser supports game-aware advanced filters (color,
type, rarity). Those filters compile to `jsonb ?` / `@>` lookups against
columns like `types`, `colors`, and `raw->'color_identity'`. These are
already fast at current cache size (~100k MTG cards, ~13k Pokemon, ~3k OP)
because the planner can join from raw_cards (small) outward, but a GIN index
keeps the latency stable as the catalog grows.

CONCURRENTLY: required so the kiosk doesn't lock up while these are built.
IF NOT EXISTS: idempotent — safe to re-run.

This is a kiosk-side migration because filters are a kiosk-only feature
right now; if other services start filtering on the same columns we'll
move it to shared/.

Run once:
    python apps/kiosk/migrate_filter_indexes.py
"""
import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

INDEXES = [
    # Pokemon types ('Fire', 'Water', etc.) — JSONB array of strings
    ("idx_scrydex_card_meta_types_gin",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_card_meta_types_gin "
     "ON scrydex_card_meta USING GIN (types jsonb_path_ops)"),

    # OP colors ('Red', 'Blue', etc.) — JSONB array of strings
    ("idx_scrydex_card_meta_colors_gin",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_card_meta_colors_gin "
     "ON scrydex_card_meta USING GIN (colors jsonb_path_ops)"),

    # OP card_type ('Leader', 'Character', 'Event', 'Stage') — plain text equality
    ("idx_scrydex_card_meta_card_type",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_card_meta_card_type "
     "ON scrydex_card_meta(game, card_type) WHERE card_type IS NOT NULL"),

    # MTG color_identity + types live in raw->jsonb; index those expressions
    # so `m.raw->'color_identity' ? 'B'` and `m.raw->'types' ? 'Sorcery'`
    # don't sequential-scan when the catalog grows.
    ("idx_scrydex_card_meta_raw_color_identity_gin",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_card_meta_raw_color_identity_gin "
     "ON scrydex_card_meta USING GIN ((raw->'color_identity') jsonb_path_ops)"),

    ("idx_scrydex_card_meta_raw_types_gin",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_card_meta_raw_types_gin "
     "ON scrydex_card_meta USING GIN ((raw->'types') jsonb_path_ops)"),

    # game filter is the first thing in every filter query — small nudge for
    # plans that prefer a partial scan to an exact-match
    ("idx_scrydex_card_meta_game",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrydex_card_meta_game "
     "ON scrydex_card_meta(game)"),

    # raw_cards.rarity is filterable from the kiosk; tiny index but keeps
    # rarity + game queries plan well at scale
    ("idx_raw_cards_game_rarity",
     "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_cards_game_rarity "
     "ON raw_cards(game, rarity) "
     "WHERE state='STORED' AND current_hold_id IS NULL"),
]


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        # Fall back to admin/.env for local dev
        try:
            from dotenv import load_dotenv  # type: ignore
            for cand in ("admin/.env", "kiosk/.env", "../admin/.env"):
                if os.path.exists(cand):
                    load_dotenv(cand)
                    break
        except ImportError:
            pass
        db_url = os.environ.get("DATABASE_URL")

    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    # CONCURRENTLY needs autocommit
    conn = psycopg2.connect(db_url)
    conn.autocommit = True

    with conn.cursor() as cur:
        for name, sql in INDEXES:
            logger.info(f"Creating {name}...")
            try:
                cur.execute(sql)
                logger.info(f"  -> done")
            except Exception as e:
                logger.error(f"  -> failed: {e}")

    conn.close()
    logger.info("All indexes created.")


if __name__ == "__main__":
    main()
