"""
storage.py — Raw card bin placement logic.

Rules:
  - Bins hold 100 cards each, organized by card_type (pokemon / magic / etc.)
  - Always fill the earliest available bin first (lowest partition_num)
  - If a whole collection fits in one bin with room, put it there
  - If it needs to be split, fill sequentially — no gaps, no trying to restore original location
  - Returning cards go to the earliest bin with room (same algorithm, no home tracking)
  - Placement is easy: find earliest bin(s) with room, fill, done.

Admin UI manages storage_rows (add new row, assign card_type).
This module only handles assignment + release.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Map from various input strings to canonical card_type values
CARD_TYPE_MAP = {
    "pokemon":  "pokemon",
    "magic":    "magic",
    "mtg":      "magic",
    "yugioh":   "yugioh",
    "yu-gi-oh": "yugioh",
    "other":    "other",
}


def _canonical_card_type(card_type: str) -> str:
    return CARD_TYPE_MAP.get((card_type or "pokemon").lower().strip(), "pokemon")


def assign_bins(card_type: str, count: int, db) -> list[dict]:
    """
    Assign bins for `count` cards of `card_type`.

    Returns a list of assignments:
        [{"bin_id": UUID, "bin_label": "A-1", "count": 47}, ...]

    Raises ValueError if no bins available with sufficient combined capacity.
    """
    if count <= 0:
        return []

    ctype = _canonical_card_type(card_type)

    # Fetch all bins for this card_type ordered (row_label, partition_num).
    # Ordering by partition_num alone ties A-1, B-1, C-1, D-1 (all
    # partition_num=1) and fills them in arbitrary order, which is what
    # caused a single batch to land in A-1 + C-1 instead of A-1 → A-2.
    # The join + composite sort guarantees we exhaust A-1..A-50 before
    # any of row B's bins get touched.
    bins = db.query("""
        SELECT sl.id, sl.bin_label, sl.capacity, sl.current_count,
               (sl.capacity - sl.current_count) AS available
        FROM storage_locations sl
        JOIN storage_rows sr ON sl.row_id = sr.id
        WHERE sl.card_type = %s
          AND sl.current_count < sl.capacity
        ORDER BY sr.row_label ASC, sl.partition_num ASC
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

    assignments = []
    remaining   = count

    for b in bins:
        if remaining <= 0:
            break
        take = min(remaining, b["available"])
        assignments.append({
            "bin_id":    str(b["id"]),
            "bin_label": b["bin_label"],
            "count":     take,
        })
        remaining -= take

    # NOTE: current_count is maintained by the update_bin_count trigger on raw_cards.
    # Do NOT increment here — that would double-count.
    logger.info(f"Assigned {count} '{ctype}' cards across "
                f"{len(assignments)} bin(s): "
                f"{[a['bin_label'] for a in assignments]}")
    return assignments


def assign_display(count: int, db) -> list[dict]:
    """
    Assign cards to binder display locations (location_type='binder').

    Fills earliest binder with room first, same sequential pattern as assign_bins().
    Returns [] if no binder capacity available (caller should fall back to storage).
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
        ORDER BY sr.row_label ASC, sl.partition_num ASC
    """)

    if not binders:
        return []

    total_available = sum(b["available"] for b in binders)
    if total_available < count:
        # Partial assignment: fill what we can, caller handles the rest
        if total_available == 0:
            return []

    assignments = []
    remaining = count

    for b in binders:
        if remaining <= 0:
            break
        take = min(remaining, b["available"])
        assignments.append({
            "bin_id":    str(b["id"]),
            "bin_label": b["bin_label"],
            "count":     take,
        })
        remaining -= take

    logger.info(f"Assigned {count - remaining} cards to binder(s): "
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
        return None  # row is full (50 bins × 100 = 5000 cards)

    bin_label = f"{row_label}-{next_part}"
    db.execute("""
        INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
        VALUES (%s, %s, %s, %s)
    """, (bin_label, str(row["id"]), next_part, row["card_type"]))

    return {"bin_label": bin_label, "partition_num": next_part}
