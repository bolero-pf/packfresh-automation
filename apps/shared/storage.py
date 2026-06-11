"""
storage.py — Raw card bin placement logic.

Rules:
  - Bins hold 100 cards each, organized by card_type (pokemon / magic / etc.)
  - Best-fit single-bin: if a single bin can hold the WHOLE batch, pick the
    bin with the least free space that still fits — consolidates partial bins
    rather than fragmenting.
  - Otherwise distribute across bins, taking the bin with the most free space
    first (worst-fit), then the next-most, until placed.
  - Same algorithm for storage bins, binders, and display cases — anywhere
    cards land.
  - Returning cards (count=1) degenerate to "place into the most-full bin
    with at least one slot," which is the consolidating behavior we want.

Admin UI manages storage_rows (add new row, assign card_type).
This module only handles assignment + release.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Map from various input strings to canonical card_type values.
# Accepts both human labels ("magic") and Scrydex game ids
# ("magicthegathering") so callers can pass card_data.get("game") directly.
CARD_TYPE_MAP = {
    "pokemon":            "pokemon",
    "magic":              "magic",
    "mtg":                "magic",
    "magicthegathering":  "magic",
    "onepiece":           "onepiece",
    "one_piece":          "onepiece",
    "op":                 "onepiece",
    "lorcana":            "lorcana",
    "riftbound":          "riftbound",
    "yugioh":             "yugioh",
    "yu-gi-oh":           "yugioh",
    "other":              "other",
}


# A bin row auto-grows its partitions on demand up to this many (A-1 .. A-100).
# Each partition holds DEFAULT_BIN_CAPACITY cards, so a full row = 100 × 50 = 5000.
MAX_PARTITIONS_PER_ROW = 100
DEFAULT_BIN_CAPACITY = 50


def _canonical_card_type(card_type: str) -> str:
    return CARD_TYPE_MAP.get((card_type or "pokemon").lower().strip(), "other")


def infer_card_type_from_set(set_name: str, db) -> Optional[str]:
    """Routing safety net: infer a card's game from its set/expansion name.

    Used ONLY to rescue a card that fell back to the Pokemon default (or has no
    game) before it lands in a bin — e.g. a Magic 'Final Fantasy' single that
    was tagged 'pokemon' at manual intake because the operator left the game
    selector on its old value and the card isn't in Scrydex/PPT to self-correct.

    Looks the set name up in scrydex_price_cache. Returns the canonical
    card_type only when EVERY cached row for that expansion shares one game;
    returns None when the set is unknown or spans multiple games, so the caller
    keeps its existing behavior. Data-driven — no hardcoded franchise list.
    Never call this to override a confident non-Pokemon tag; it's a backstop for
    the Pokemon dumping-ground case only.
    """
    if not set_name or not set_name.strip():
        return None
    try:
        rows = db.query("""
            SELECT DISTINCT game
            FROM scrydex_price_cache
            WHERE expansion_name ILIKE %s AND game IS NOT NULL
            LIMIT 3
        """, (set_name.strip(),))
    except Exception as e:
        logger.debug(f"set-name game inference for '{set_name}' failed: {e}")
        return None
    games = {(r.get("game") or "").strip() for r in rows}
    games.discard("")
    if len(games) != 1:
        return None
    return _canonical_card_type(next(iter(games)))


def _best_fit_assign(bins: list[dict], count: int) -> list[dict]:
    """Place `count` cards across `bins`, preferring a single-bin best fit.

    Algorithm:
      1. If any single bin has enough room for the whole batch, pick the bin
         with the LEAST free space that still holds it (best-fit). This tops
         off partial bins instead of opening more empty space.
      2. Otherwise distribute, taking the bin with the MOST free space first,
         then the next-most, until the batch is placed (or bins exhausted —
         caller should pre-check capacity).

    `bins` rows must include id, bin_label, and an `available` field. Returns
    a list of {bin_id, bin_label, count} dicts. May return less than `count`
    in total if combined capacity is insufficient — caller handles that case.
    """
    if count <= 0 or not bins:
        return []

    fits = [b for b in bins if b["available"] >= count]
    if fits:
        chosen = min(fits, key=lambda b: (b["available"], b.get("partition_num", 0)))
        return [{
            "bin_id":    str(chosen["id"]),
            "bin_label": chosen["bin_label"],
            "count":     count,
        }]

    # Distribute: largest free-space first.
    sorted_bins = sorted(bins, key=lambda b: (-b["available"], b.get("partition_num", 0)))
    assignments = []
    remaining = count
    for b in sorted_bins:
        if remaining <= 0:
            break
        if b["available"] <= 0:
            continue
        take = min(remaining, b["available"])
        assignments.append({
            "bin_id":    str(b["id"]),
            "bin_label": b["bin_label"],
            "count":     take,
        })
        remaining -= take
    return assignments


def _auto_expand_bins(ctype: str, needed: int, db) -> int:
    """Seed new partitions on the active 'bin' row for `ctype` so storage grows
    on demand instead of erroring out when it fills up.

    Adds 50-card partitions (e.g. A-51, A-52, ...) until the new free capacity
    covers `needed` or the row hits MAX_PARTITIONS_PER_ROW. Returns the number
    of partitions created (0 if there is no expandable row or it is already at
    the cap).

    Targets the single active location_type='bin' row for the card_type — the
    physical aisle (A=pokemon, B=magic, C=onepiece). Bulk/binder/display rows
    and inactive (seeded-but-not-built) rows are never auto-expanded.
    """
    row = db.query_one("""
        SELECT id, row_label
        FROM storage_rows
        WHERE card_type = %s
          AND COALESCE(active, TRUE) = TRUE
          AND COALESCE(location_type, 'bin') = 'bin'
        ORDER BY row_label ASC
        LIMIT 1
    """, (ctype,))
    if not row:
        return 0

    last = db.query_one("""
        SELECT MAX(partition_num) AS max_part
        FROM storage_locations WHERE row_id = %s
    """, (row["id"],))
    next_part = (last["max_part"] or 0) + 1

    added = 0
    gained = 0
    while gained < needed and next_part <= MAX_PARTITIONS_PER_ROW:
        bin_label = f"{row['row_label']}-{next_part}"
        db.execute("""
            INSERT INTO storage_locations
                (bin_label, row_id, partition_num, card_type, capacity)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (bin_label) DO NOTHING
        """, (bin_label, str(row["id"]), next_part, ctype, DEFAULT_BIN_CAPACITY))
        added += 1
        gained += DEFAULT_BIN_CAPACITY
        next_part += 1

    if added:
        logger.info(f"Auto-expanded row {row['row_label']} ({ctype}): "
                    f"+{added} bin(s), +{gained} card capacity.")
    return added


def assign_bins(card_type: str, count: int, db) -> list[dict]:
    """
    Assign bins for `count` cards of `card_type`. Best-fit single-bin if
    possible, otherwise distribute starting from the bin with the most free
    space.

    Returns [{"bin_id": UUID, "bin_label": "A-1", "count": 47}, ...].

    Raises ValueError if no bins available with sufficient combined capacity.
    """
    if count <= 0:
        return []

    ctype = _canonical_card_type(card_type)

    # storage_rows.active = FALSE excludes seeded-but-physically-absent rows
    # (e.g. C/D were seeded by the original migration but Sean only built A
    # and B in the warehouse).
    bin_query = """
        SELECT sl.id, sl.bin_label, sl.partition_num, sl.capacity,
               sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.card_type = %s
          AND sl.current_count < sl.capacity
          AND COALESCE(sr.active, TRUE) = TRUE
          AND COALESCE(sr.location_type, 'bin') = 'bin'
    """
    bins = db.query(bin_query, (ctype,))
    total_available = sum(b["available"] for b in bins)

    # Grow the row on demand when it can't hold the batch, rather than erroring
    # out and making someone add a row by hand mid-intake.
    if total_available < count:
        if _auto_expand_bins(ctype, count - total_available, db):
            bins = db.query(bin_query, (ctype,))
            total_available = sum(b["available"] for b in bins)

    if not bins:
        raise ValueError(f"No available bins for card_type='{ctype}'. "
                         f"Add a new storage row via the admin UI.")

    if total_available < count:
        raise ValueError(
            f"Not enough bin capacity for {count} cards of type '{ctype}'. "
            f"Available: {total_available} (row at max {MAX_PARTITIONS_PER_ROW} "
            f"bins). Add more storage rows."
        )

    assignments = _best_fit_assign(bins, count)

    # NOTE: current_count is maintained by the update_bin_count trigger on raw_cards.
    # Do NOT increment here — that would double-count.
    logger.info(f"Assigned {count} '{ctype}' cards across "
                f"{len(assignments)} bin(s): "
                f"{[a['bin_label'] for a in assignments]}")
    return assignments


def assign_display(count: int, db, card_type: str | None = None) -> list[dict]:
    """
    Assign cards to binder display locations (location_type='binder').
    Same best-fit-then-most-free algorithm as assign_bins().

    When `card_type` is supplied (e.g. 'pokemon', 'magic'), only binders
    tagged for that game type — or untagged / 'mixed' — are eligible.
    This stops Magic singles from drifting into Pokemon binders.

    Returns [] if no binder capacity available (caller should fall back to
    storage). Returns a partial assignment if combined binder capacity is
    less than `count` — caller handles overflow.
    """
    if count <= 0:
        return []

    where_extra = ""
    params: tuple = ()
    if card_type:
        where_extra = "AND (sl.card_type = %s OR sl.card_type IS NULL OR sl.card_type = 'mixed')"
        params = (card_type,)

    binders = db.query(f"""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sr.location_type = 'binder'
          AND sl.current_count < sl.capacity
          {where_extra}
    """, params)

    if not binders:
        return []

    assignments = _best_fit_assign(binders, count)
    placed = sum(a["count"] for a in assignments)
    logger.info(f"Assigned {placed} cards to {card_type or 'any'} binder(s): "
                f"{[a['bin_label'] for a in assignments]}")
    return assignments


def get_binder_capacity(db) -> list[dict]:
    """Return binder locations with current capacity info."""
    return [dict(r) for r in db.query("""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sr.location_type = 'binder'
        ORDER BY sl.bin_label ASC
    """)]


def assign_display_case(count: int, db) -> list[dict]:
    """Assign cards to display-case locations (location_type='display_case').
    Mirrors assign_display, but for the customer-facing glass cases out front.
    Same best-fit-then-most-free algorithm. Returns [] if no display capacity."""
    if count <= 0:
        return []

    cases = db.query("""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sr.location_type = 'display_case'
          AND sl.current_count < sl.capacity
    """)

    if not cases:
        return []

    assignments = _best_fit_assign(cases, count)
    placed = sum(a["count"] for a in assignments)
    logger.info(f"Assigned {placed} cards to display case(s): "
                f"{[a['bin_label'] for a in assignments]}")
    return assignments


def get_display_case_capacity(db) -> list[dict]:
    """Return display-case locations with current capacity info."""
    return [dict(r) for r in db.query("""
        SELECT sl.id, sl.bin_label, sl.card_type, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sr.location_type = 'display_case'
        ORDER BY sl.bin_label ASC
    """)]


def release_bins(assignments: list[dict], db) -> None:
    """
    Return cards to bins (undo an assignment — e.g. on push-live rollback).
    """
    for a in assignments:
        db.execute("""
            UPDATE storage_locations
            SET current_count = GREATEST(0, current_count - %s)
            WHERE id = %s
        """, (a["count"], a["bin_id"]))
    logger.info(f"Released bins: {[a['bin_label'] for a in assignments]}")


def get_bin_summary(card_type: Optional[str], db) -> list[dict]:
    """
    Return bin occupancy summary, optionally filtered by card_type.
    Used by admin UI and card browser.
    """
    if card_type:
        ctype = _canonical_card_type(card_type)
        rows = db.query("""
            SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count,
                   sr.row_label, sr.description
            FROM storage_locations sl
            JOIN storage_rows sr ON sl.row_id = sr.id
            WHERE sl.card_type = %s
            ORDER BY sr.row_label ASC, sl.partition_num ASC
        """, (ctype,))
    else:
        rows = db.query("""
            SELECT sl.bin_label, sl.card_type, sl.capacity, sl.current_count,
                   sr.row_label, sr.description
            FROM storage_locations sl
            JOIN storage_rows sr ON sl.row_id = sr.id
            ORDER BY sr.row_label ASC, sl.partition_num ASC
        """)
    return [dict(r) for r in rows]


def get_or_add_bin_for_row(row_label: str, db) -> Optional[dict]:
    """
    Find the next available partition in a given row, or return None if full.
    Used when admin adds a new row — auto-seeds the next partition.
    """
    row = db.query_one(
        "SELECT id, card_type FROM storage_rows WHERE row_label = %s",
        (row_label,)
    )
    if not row:
        return None

    # Find the highest partition in this row
    last = db.query_one("""
        SELECT MAX(partition_num) AS max_part
        FROM storage_locations WHERE row_id = %s
    """, (row["id"],))

    next_part = (last["max_part"] or 0) + 1
    if next_part > MAX_PARTITIONS_PER_ROW:
        return None  # row is full (100 bins × 50 = 5000 cards)

    bin_label = f"{row_label}-{next_part}"
    db.execute("""
        INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
        VALUES (%s, %s, %s, %s)
    """, (bin_label, str(row["id"]), next_part, row["card_type"]))

    return {"bin_label": bin_label, "partition_num": next_part}
