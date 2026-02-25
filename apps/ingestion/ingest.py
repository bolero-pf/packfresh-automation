"""
Ingest business logic.

Handles:
    - Listing received sessions for warehouse processing
    - Breaking down sealed products into components
    - Pushing inventory live to Shopify (increment, create damaged, new listings)
    - Marking items as damaged during ingest
"""

import logging
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from db import query, query_one, execute, execute_returning

logger = logging.getLogger(__name__)


# ==========================================
# SESSION QUERIES
# ==========================================

def list_sessions(limit: int = 50) -> list[dict]:
    """List sessions in received + ingested status for the ingest queue."""
    return query("""
        SELECT s.*,
               COUNT(i.id) AS item_count,
               COALESCE(SUM(i.quantity), 0) AS total_qty
        FROM intake_sessions s
        LEFT JOIN intake_items i ON i.session_id = s.id
            AND i.item_status IN ('good', 'damaged')
        WHERE s.status IN ('received', 'ingested')
        GROUP BY s.id
        ORDER BY
            CASE s.status WHEN 'received' THEN 0 ELSE 1 END,
            s.created_at DESC
        LIMIT %s
    """, (limit,))


def get_session(session_id: str) -> Optional[dict]:
    return query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))


def get_session_items(session_id: str) -> list[dict]:
    return query(
        "SELECT * FROM intake_items WHERE session_id = %s ORDER BY created_at",
        (session_id,)
    )


# ==========================================
# BREAK DOWN
# ==========================================

def break_down_item(item_id: str, components: list[dict]) -> dict:
    """
    Break down a sealed product into its component items.

    The parent item gets status 'broken_down' (excluded from push).
    Child items are created with the parent's session, each with their own
    tcgplayer_id, name, quantity (multiplied by parent qty), and market price.

    COGS allocation: The parent's offer_price (what we paid) is distributed
    proportionally across children based on their relative market values.
    e.g. parent paid $100, child A market $90, child B market $60:
         total market = $150, A COGS = $100 * 90/150 = $60, B COGS = $100 * 60/150 = $40

    components: list of {product_name, tcgplayer_id, quantity, market_price, set_name?}
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    if item.get("item_status") == "broken_down":
        raise ValueError("Item is already broken down. Undo the breakdown first.")

    session_id = item["session_id"]
    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))
    if not session:
        raise ValueError("Session not found")

    parent_qty = item.get("quantity", 1)
    parent_offer = Decimal(str(item.get("offer_price", 0)))  # Total we paid for the parent

    # Mark parent as broken down
    execute("UPDATE intake_items SET item_status = 'broken_down' WHERE id = %s", (item_id,))

    # Calculate total market value of all components (per parent unit)
    # Each component has a market_price (per unit) and a quantity (per parent unit)
    total_component_market = Decimal("0")
    for comp in components:
        comp_market = Decimal(str(comp.get("market_price", 0)))
        comp_qty_per_parent = int(comp["quantity"])
        total_component_market += comp_market * comp_qty_per_parent

    child_items = []
    allocated_offer = Decimal("0")
    for idx, comp in enumerate(components):
        child_qty = int(comp["quantity"]) * parent_qty
        market_price = Decimal(str(comp.get("market_price", 0)))
        comp_qty_per_parent = int(comp["quantity"])

        # Proportional COGS: this component's share of the parent's cost
        if total_component_market > 0:
            # Market value of this component (per parent unit)
            comp_value = market_price * comp_qty_per_parent
            # Its share of total market value
            share = comp_value / total_component_market
            # COGS for all units of this component
            comp_offer = (parent_offer * share).quantize(Decimal("0.01"))
        else:
            # Fallback: split evenly
            comp_offer = (parent_offer / len(components)).quantize(Decimal("0.01"))

        # For the last component, assign whatever's left to avoid rounding drift
        if idx == len(components) - 1:
            comp_offer = parent_offer - allocated_offer
        allocated_offer += comp_offer

        child_id = str(uuid4())
        execute("""
            INSERT INTO intake_items (
                id, session_id, product_name, set_name, tcgplayer_id,
                quantity, market_price, offer_price, product_type,
                is_mapped, item_status, parent_item_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            child_id, session_id, comp["product_name"], comp.get("set_name"),
            comp.get("tcgplayer_id"), child_qty, market_price,
            comp_offer, "sealed",
            comp.get("tcgplayer_id") is not None, "good", item_id,
        ))

        # Save product mapping if we have a tcgplayer_id
        if comp.get("tcgplayer_id"):
            _save_mapping(comp["product_name"], int(comp["tcgplayer_id"]),
                         "sealed", market_price, comp.get("set_name"))

        child = query_one("SELECT * FROM intake_items WHERE id = %s", (child_id,))
        child_items.append(child)

    _recalculate_session_totals(session_id)

    return {
        "parent_item": query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,)),
        "child_items": child_items,
        "session": query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,)),
    }


def undo_break_down(item_id: str) -> dict:
    """
    Undo a break-down: delete all children and restore parent to 'good' status.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")
    if item.get("item_status") != "broken_down":
        raise ValueError("Item is not broken down")

    session_id = item["session_id"]

    # Delete all children of this item
    execute("DELETE FROM intake_items WHERE parent_item_id = %s", (item_id,))

    # Restore parent
    execute("UPDATE intake_items SET item_status = 'good' WHERE id = %s", (item_id,))

    _recalculate_session_totals(session_id)

    return {
        "item": query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,)),
        "session": query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,)),
    }


def mark_item_damaged(item_id: str) -> dict:
    """Mark an entire item as damaged."""
    execute(
        "UPDATE intake_items SET item_status = 'damaged' WHERE id = %s",
        (item_id,)
    )
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def split_damaged(item_id: str, damaged_qty: int) -> dict:
    """
    Split an item into good + damaged portions.
    If damaged_qty == total qty, just marks the whole thing damaged.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    total_qty = item.get("quantity", 1)
    if damaged_qty < 1 or damaged_qty > total_qty:
        raise ValueError(f"damaged_qty must be 1-{total_qty}")

    if damaged_qty == total_qty:
        # Damage the whole thing
        return mark_item_damaged(item_id)

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    market_price = Decimal(str(item.get("market_price", 0)))
    DAMAGE_DISCOUNT = Decimal("0.85")

    # Reduce original item qty
    good_qty = total_qty - damaged_qty
    good_offer = (market_price * offer_pct * good_qty).quantize(Decimal("0.01"))
    execute("""
        UPDATE intake_items SET quantity = %s, offer_price = %s WHERE id = %s
    """, (good_qty, good_offer, item_id))

    # Create damaged split
    damaged_id = str(uuid4())
    damaged_offer = (market_price * DAMAGE_DISCOUNT * offer_pct * damaged_qty).quantize(Decimal("0.01"))
    execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, set_name, tcgplayer_id,
            quantity, market_price, offer_price, product_type,
            is_mapped, item_status, parent_item_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        damaged_id, item["session_id"], item.get("product_name"), item.get("set_name"),
        item.get("tcgplayer_id"), damaged_qty, market_price,
        damaged_offer, item.get("product_type", "sealed"),
        item.get("is_mapped", False), "damaged", item_id,
    ))

    _recalculate_session_totals(item["session_id"])

    return {
        "good_item": query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,)),
        "damaged_item": query_one("SELECT * FROM intake_items WHERE id = %s", (damaged_id,)),
    }


def mark_item_good(item_id: str) -> dict:
    """Restore an item to good status."""
    execute(
        "UPDATE intake_items SET item_status = 'good' WHERE id = %s",
        (item_id,)
    )
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


# ==========================================
# PUSH LIVE
# ==========================================

def build_cache_maps(tcg_ids: list[int]) -> tuple[dict, dict]:
    """
    Build normal_cache and damaged_cache maps from shopify_product_cache.
    Returns (normal_cache, damaged_cache) keyed by tcgplayer_id.
    """
    if not tcg_ids:
        return {}, {}

    placeholders = ",".join(["%s"] * len(tcg_ids))
    rows = query(
        f"SELECT * FROM shopify_product_cache WHERE tcgplayer_id IN ({placeholders})",
        tuple(tcg_ids)
    )

    normal_cache = {}
    damaged_cache = {}
    for r in rows:
        tcg = r["tcgplayer_id"]
        is_dmg = r.get("is_damaged") or False
        target = damaged_cache if is_dmg else normal_cache
        if tcg not in target:
            target[tcg] = r

    return normal_cache, damaged_cache


def mark_session_ingested(session_id: str):
    """Transition session to ingested status."""
    execute(
        "UPDATE intake_sessions SET status = 'ingested', ingested_at = CURRENT_TIMESTAMP WHERE id = %s",
        (session_id,)
    )


# ==========================================
# HELPERS
# ==========================================

def _recalculate_session_totals(session_id: str):
    """Recalculate market value and offer total for a session."""
    totals = query_one("""
        SELECT
            COALESCE(SUM(market_price * quantity), 0) AS market_total,
            COALESCE(SUM(offer_price), 0) AS offer_total
        FROM intake_items
        WHERE session_id = %s AND item_status IN ('good', 'damaged')
    """, (session_id,))

    if totals:
        execute("""
            UPDATE intake_sessions
            SET total_market_value = %s, total_offer_amount = %s
            WHERE id = %s
        """, (totals["market_total"], totals["offer_total"], session_id))


def _save_mapping(product_name: str, tcgplayer_id: int, product_type: str,
                  market_price: Decimal = None, set_name: str = None):
    """Save a product name -> tcgplayer_id mapping."""
    existing = query_one(
        "SELECT id FROM product_mappings WHERE collectr_name = %s AND product_type = %s",
        (product_name, product_type)
    )
    if existing:
        execute("""
            UPDATE product_mappings
            SET tcgplayer_id = %s, market_price = COALESCE(%s, market_price),
                set_name = COALESCE(%s, set_name), updated_at = CURRENT_TIMESTAMP
            WHERE collectr_name = %s AND product_type = %s
        """, (tcgplayer_id, market_price, set_name, product_name, product_type))
    else:
        execute("""
            INSERT INTO product_mappings (collectr_name, tcgplayer_id, product_type, market_price, set_name)
            VALUES (%s, %s, %s, %s, %s)
        """, (product_name, tcgplayer_id, product_type, market_price, set_name))
