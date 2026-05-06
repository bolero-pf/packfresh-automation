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


def _canonical_card_type(card_type: str) -> str:
    return CARD_TYPE_MAP.get((card_type or "pokemon").lower().strip(), "other")


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
    bins = db.query("""
        SELECT sl.id, sl.bin_label, sl.partition_num, sl.capacity,
               sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.card_type = %s
          AND sl.current_count < sl.capacity
          AND COALESCE(sr.active, TRUE) = TRUE
          AND COALESCE(sr.location_type, 'bin') = 'bin'
    """, (ctype,))

    if not bins:
        raise ValueError(f"No available bins for card_type='{ctype}'. "
                         f"Add a new storage row via the admin UI.")

    total_available = sum(b["available"] for b in bins)
    if total_available < count:
        raise ValueError(
            f"Not enough bin capacity for {count} cards of type '{ctype}'. "
            f"Available: {total_available}. Add more storage rows."
        )

    assignments = _best_fit_assign(bins, count)

    # NOTE: current_count is maintained by the update_bin_count trigger on raw_cards.
    # Do NOT increment here — that would double-count.
    logger.info(f"Assigned {count} '{ctype}' cards across "
                f"{len(assignments)} bin(s): "
                f"{[a['bin_label'] for a in assignments]}")
    return assignments


def assign_display(count: int, db) -> list[dict]:
    """
    Assign cards to binder display locations (location_type='binder').
    Same best-fit-then-most-free algorithm as assign_bins().

    Returns [] if no binder capacity available (caller should fall back to
    storage). Returns a partial assignment if combined binder capacity is
    less than `count` — caller handles overflow.
    """
    if count <= 0:
        return []

    binders = db.query("""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sr.location_type = 'binder'
          AND sl.current_count < sl.capacity
    """)

    if not binders:
        return []

    assignments = _best_fit_assign(binders, count)
    placed = sum(a["count"] for a in assignments)
    logger.info(f"Assigned {placed} cards to binder(s): "
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
    if next_part > 50:
        return None  # row is full (50 bins × 50 = 2500 cards)

    bin_label = f"{row_label}-{next_part}"
    db.execute("""
        INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
        VALUES (%s, %s, %s, %s)
    """, (bin_label, str(row["id"]), next_part, row["card_type"]))

    return {"bin_label": bin_label, "partition_num": next_part}
