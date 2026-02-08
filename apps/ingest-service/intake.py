"""
Intake business logic.

Handles:
    - Session creation and management
    - Product mapping (collectr_name <-> tcgplayer_id)
    - Offer calculation
    - Finalization: sealed → COGS entries, raw → raw_cards with barcodes
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from db import query, query_one, execute, execute_returning, execute_many_batch
from barcode_gen import generate_barcode_id

logger = logging.getLogger(__name__)


# ==========================================
# PRODUCT MAPPING (collectr_name <-> tcgplayer_id)
# ==========================================

def get_cached_mapping(collectr_name: str, product_type: str) -> Optional[int]:
    """Check if we have a saved mapping for this Collectr product name."""
    row = query_one("""
        SELECT tcgplayer_id FROM product_mappings
        WHERE collectr_name = %s AND product_type = %s
    """, (collectr_name, product_type))

    if row:
        # Bump usage stats
        execute("""
            UPDATE product_mappings
            SET use_count = use_count + 1, last_used = CURRENT_TIMESTAMP
            WHERE collectr_name = %s AND product_type = %s
        """, (collectr_name, product_type))
        return row["tcgplayer_id"]
    return None


def save_mapping(collectr_name: str, tcgplayer_id: int, product_type: str,
                 set_name: str = None, card_number: str = None):
    """Save or update a product mapping for future imports."""
    execute("""
        INSERT INTO product_mappings (collectr_name, tcgplayer_id, product_type, set_name, card_number)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (collectr_name, product_type)
        DO UPDATE SET
            tcgplayer_id = EXCLUDED.tcgplayer_id,
            last_used = CURRENT_TIMESTAMP,
            use_count = product_mappings.use_count + 1
    """, (collectr_name, tcgplayer_id, product_type, set_name, card_number))


def get_all_mappings(product_type: str = None) -> list[dict]:
    """Get all cached mappings, optionally filtered by type."""
    if product_type:
        return query("""
            SELECT collectr_name, tcgplayer_id, product_type, set_name, card_number, use_count
            FROM product_mappings WHERE product_type = %s
            ORDER BY use_count DESC
        """, (product_type,))
    return query("""
        SELECT collectr_name, tcgplayer_id, product_type, set_name, card_number, use_count
        FROM product_mappings ORDER BY use_count DESC
    """)


# ==========================================
# INTAKE SESSION MANAGEMENT
# ==========================================

def create_session(customer_name: str, session_type: str,
                   offer_percentage: Decimal,
                   file_name: str = None, file_hash: str = None,
                   employee_id: str = None, notes: str = None) -> dict:
    """Create a new intake session. Returns the full session row."""
    session_id = str(uuid4())
    return execute_returning("""
        INSERT INTO intake_sessions
            (id, customer_name, session_type, offer_percentage,
             source_file_name, source_file_hash, employee_id, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (session_id, customer_name, session_type, offer_percentage,
          file_name, file_hash, employee_id, notes))


def get_session(session_id: str) -> Optional[dict]:
    """Get session by ID using the summary view."""
    return query_one("SELECT * FROM intake_session_summary WHERE id = %s", (session_id,))


def get_session_items(session_id: str) -> list[dict]:
    """Get all items in a session, unmapped items first."""
    return query("""
        SELECT * FROM intake_items
        WHERE session_id = %s
        ORDER BY is_mapped ASC, product_name
    """, (session_id,))


def list_sessions(status: str = "in_progress", limit: int = 50) -> list[dict]:
    """List intake sessions by status."""
    return query("""
        SELECT * FROM intake_session_summary
        WHERE status = %s
        ORDER BY created_at DESC
        LIMIT %s
    """, (status, limit))


def check_duplicate_import(file_hash: str) -> Optional[str]:
    """Check if this file has already been imported. Returns session_id if duplicate."""
    row = query_one(
        "SELECT id FROM intake_sessions WHERE source_file_hash = %s",
        (file_hash,)
    )
    return row["id"] if row else None


# ==========================================
# ADDING ITEMS TO SESSION
# ==========================================

def add_items_to_session(session_id: str, items: list[dict]) -> int:
    """
    Batch-add items to an intake session.
    
    Each item dict should have:
        product_name, product_type, quantity, market_price, offer_price, unit_cost_basis
        Optional: tcgplayer_id, set_name, card_number, condition, rarity
    
    Returns number of items added.
    """
    sql = """
        INSERT INTO intake_items
            (session_id, product_name, tcgplayer_id, product_type,
             set_name, card_number, condition, rarity,
             quantity, market_price, offer_price, unit_cost_basis, is_mapped)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params_list = [
        (
            session_id,
            item["product_name"],
            item.get("tcgplayer_id"),
            item["product_type"],
            item.get("set_name"),
            item.get("card_number"),
            item.get("condition"),
            item.get("rarity"),
            item["quantity"],
            item["market_price"],
            item["offer_price"],
            item["unit_cost_basis"],
            item.get("tcgplayer_id") is not None,
        )
        for item in items
    ]
    return execute_many_batch(sql, params_list)


def add_single_raw_item(session_id: str, product_name: str, tcgplayer_id: int,
                         set_name: str, card_number: str, condition: str,
                         rarity: str, quantity: int, market_price: Decimal,
                         offer_percentage: Decimal) -> dict:
    """
    Add a single raw card item to a session (manual entry flow).
    Calculates offer_price and unit_cost_basis from the given offer_percentage.
    Returns the created intake_item row.
    """
    offer_price = market_price * quantity * (offer_percentage / Decimal("100"))
    unit_cost_basis = offer_price / quantity if quantity > 0 else Decimal("0")

    return execute_returning("""
        INSERT INTO intake_items
            (session_id, product_name, tcgplayer_id, product_type,
             set_name, card_number, condition, rarity,
             quantity, market_price, offer_price, unit_cost_basis, is_mapped)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
        RETURNING *
    """, (session_id, product_name, tcgplayer_id, "raw",
          set_name, card_number, condition, rarity,
          quantity, market_price, offer_price, unit_cost_basis))


# ==========================================
# MAPPING ITEMS TO TCGPLAYER IDS
# ==========================================

def map_item(item_id: str, tcgplayer_id: int,
             new_market_price: Decimal = None) -> dict:
    """
    Map an intake item to a tcgplayer_id.
    Optionally update the market price (e.g., from PPT verification).
    Recalculates offer_price based on session's offer_percentage.
    
    Returns updated item row.
    """
    # Get item and session
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError(f"Item {item_id} not found")

    session = query_one(
        "SELECT offer_percentage FROM intake_sessions WHERE id = %s",
        (item["session_id"],)
    )
    if not session:
        raise ValueError(f"Session for item {item_id} not found")

    # Use new price if provided, otherwise keep existing
    market_price = new_market_price if new_market_price is not None else item["market_price"]
    offer_pct = session["offer_percentage"]
    offer_price = market_price * item["quantity"] * (offer_pct / Decimal("100"))
    unit_cost_basis = offer_price / item["quantity"] if item["quantity"] > 0 else Decimal("0")

    # Update item
    updated = execute_returning("""
        UPDATE intake_items
        SET tcgplayer_id = %s, market_price = %s, offer_price = %s,
            unit_cost_basis = %s, is_mapped = TRUE
        WHERE id = %s
        RETURNING *
    """, (tcgplayer_id, market_price, offer_price, unit_cost_basis, item_id))

    # Cache the mapping for future imports
    save_mapping(
        item["product_name"], tcgplayer_id, item["product_type"],
        item.get("set_name"), item.get("card_number")
    )

    # Recalculate session totals
    _recalculate_session_totals(item["session_id"])

    return updated


def _recalculate_session_totals(session_id: str):
    """Recalculate total_market_value and total_offer_amount for a session."""
    execute("""
        UPDATE intake_sessions SET
            total_market_value = (
                SELECT COALESCE(SUM(market_price * quantity), 0)
                FROM intake_items WHERE session_id = %s
            ),
            total_offer_amount = (
                SELECT COALESCE(SUM(offer_price), 0)
                FROM intake_items WHERE session_id = %s
            )
        WHERE id = %s
    """, (session_id, session_id, session_id))


def update_offer_percentage(session_id: str, new_percentage: Decimal) -> dict:
    """
    Change the offer percentage for a session and recalculate all item offer prices.
    """
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")
    if session["status"] != "in_progress":
        raise ValueError("Cannot change offer % on a finalized session")

    # Update session offer_percentage
    execute("""
        UPDATE intake_sessions SET offer_percentage = %s WHERE id = %s
    """, (new_percentage, session_id))

    # Recalculate every item's offer_price
    execute("""
        UPDATE intake_items
        SET offer_price = market_price * quantity * (%s / 100.0)
        WHERE session_id = %s
    """, (new_percentage, session_id))

    _recalculate_session_totals(session_id)
    return get_session(session_id)


def update_item_price(item_id: str, new_market_price: Decimal, session_id: str) -> dict:
    """Update an item's market price and recalculate its offer price."""
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")

    offer_pct = session["offer_percentage"]

    item = query_one("SELECT quantity FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    offer_price = new_market_price * item["quantity"] * (offer_pct / Decimal("100"))

    execute("""
        UPDATE intake_items
        SET market_price = %s, offer_price = %s
        WHERE id = %s
    """, (new_market_price, offer_price, item_id))

    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


# ==========================================
# FINALIZATION
# ==========================================

def finalize_session(session_id: str) -> dict:
    """
    Finalize an intake session.
    
    - Checks all items are mapped
    - For sealed items: creates/updates COGS entries
    - For raw items: creates raw_cards entries with barcodes
    - Marks session as finalized
    
    Returns dict with success status, created card barcodes, etc.
    """
    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))
    if not session:
        return {"success": False, "error": "Session not found"}

    if session["status"] == "finalized":
        return {"success": False, "error": "Session already finalized"}

    items = query("SELECT * FROM intake_items WHERE session_id = %s", (session_id,))
    if not items:
        return {"success": False, "error": "Session has no items"}

    # Check all items are mapped
    unmapped = [i for i in items if not i["is_mapped"]]
    if unmapped:
        names = [i["product_name"] for i in unmapped[:5]]
        return {
            "success": False,
            "error": f"{len(unmapped)} items still need tcgplayer_id mapping",
            "unmapped_names": names,
        }

    result = {
        "success": True,
        "session_id": session_id,
        "sealed_processed": 0,
        "raw_cards_created": 0,
        "barcodes": [],
    }

    # Process sealed items
    sealed_items = [i for i in items if i["product_type"] == "sealed"]
    if sealed_items:
        result["sealed_processed"] = _finalize_sealed(sealed_items, session_id)

    # Process raw items
    raw_items = [i for i in items if i["product_type"] == "raw"]
    if raw_items:
        cards = _finalize_raw(raw_items, session_id)
        result["raw_cards_created"] = len(cards)
        result["barcodes"] = [c["barcode"] for c in cards]

    # Mark session as finalized
    execute("""
        UPDATE intake_sessions
        SET status = 'finalized', finalized_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (session_id,))

    # Recalculate totals one final time
    _recalculate_session_totals(session_id)

    return result


def _finalize_sealed(items: list[dict], session_id: str) -> int:
    """
    Process sealed items: update weighted-average COGS.
    
    Note: shopify_product_id is nullable — it gets linked later when 
    the product is matched in Shopify by tcgplayer_id metafield.
    """
    count = 0
    for item in items:
        tcgplayer_id = item["tcgplayer_id"]
        quantity_delta = item["quantity"]
        cost_added = item["offer_price"]  # total cost for this line item

        existing = query_one(
            "SELECT * FROM sealed_cogs WHERE tcgplayer_id = %s",
            (tcgplayer_id,)
        )

        if existing:
            old_qty = existing["current_quantity"]
            old_total = existing["total_cost"]
            new_qty = old_qty + quantity_delta
            new_total = old_total + cost_added
            new_avg = new_total / new_qty if new_qty > 0 else Decimal("0")

            execute("""
                UPDATE sealed_cogs
                SET current_quantity = %s, total_cost = %s, avg_cogs = %s,
                    last_updated = CURRENT_TIMESTAMP, last_intake_session_id = %s
                WHERE tcgplayer_id = %s
            """, (new_qty, new_total, new_avg, session_id, tcgplayer_id))

            # Log COGS history
            execute("""
                INSERT INTO cogs_history
                    (sealed_cogs_id, old_quantity, new_quantity,
                     old_avg_cogs, new_avg_cogs,
                     quantity_delta, cost_added, intake_session_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (existing["id"], old_qty, new_qty,
                  existing["avg_cogs"], new_avg,
                  quantity_delta, cost_added, session_id))
        else:
            avg_cogs = cost_added / quantity_delta if quantity_delta > 0 else Decimal("0")

            # Note: shopify_product_id is NULL here — linked separately
            execute("""
                INSERT INTO sealed_cogs
                    (tcgplayer_id, product_name, current_quantity,
                     total_cost, avg_cogs, last_intake_session_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (tcgplayer_id, item["product_name"], quantity_delta,
                  cost_added, avg_cogs, session_id))

        count += 1
    return count


def _finalize_raw(items: list[dict], session_id: str) -> list[dict]:
    """
    Process raw card items: create individual raw_cards entries with barcodes.
    Expands quantity (e.g., 3x Charizard → 3 separate raw_card rows).
    """
    cards_to_insert = []
    for item in items:
        for _ in range(item["quantity"]):
            barcode_id = generate_barcode_id()
            cards_to_insert.append({
                "barcode": barcode_id,
                "tcgplayer_id": item["tcgplayer_id"],
                "card_name": item["product_name"],
                "set_name": item.get("set_name", ""),
                "card_number": item.get("card_number", ""),
                "condition": item.get("condition", "NM"),
                "rarity": item.get("rarity", ""),
                "cost_basis": item["unit_cost_basis"],
                "current_price": item["market_price"],
                "intake_session_id": session_id,
            })

    if not cards_to_insert:
        return []

    sql = """
        INSERT INTO raw_cards
            (barcode, tcgplayer_id, card_name, set_name, card_number,
             condition, rarity, cost_basis, current_price,
             intake_session_id, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'PURCHASED')
    """
    params_list = [
        (c["barcode"], c["tcgplayer_id"], c["card_name"], c["set_name"],
         c["card_number"], c["condition"], c["rarity"], c["cost_basis"],
         c["current_price"], c["intake_session_id"])
        for c in cards_to_insert
    ]
    execute_many_batch(sql, params_list)

    return cards_to_insert
