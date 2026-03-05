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


def relink_item(item_id: str, data: dict) -> dict:
    """
    Relink an item to a different product.
    data: {product_name, tcgplayer_id, market_price, set_name?}
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    product_name = data.get("product_name", item.get("product_name"))
    tcgplayer_id = data.get("tcgplayer_id")
    market_price = Decimal(str(data.get("market_price", item.get("market_price", 0))))
    set_name = data.get("set_name", item.get("set_name"))

    # Recalculate offer proportionally — keep the same COGS ratio
    old_market = Decimal(str(item.get("market_price", 0)))
    old_offer = Decimal(str(item.get("offer_price", 0)))
    qty = item.get("quantity", 1)

    if old_market > 0 and qty > 0:
        # Preserve the offer ratio (COGS per unit / market per unit)
        ratio = old_offer / (old_market * qty)
        new_offer = (market_price * qty * ratio).quantize(Decimal("0.01"))
    else:
        # Fallback: use session offer percentage
        session = query_one("SELECT offer_percentage FROM intake_sessions WHERE id = %s", (item["session_id"],))
        pct = Decimal(str(session.get("offer_percentage", 65))) / 100
        new_offer = (market_price * qty * pct).quantize(Decimal("0.01"))

    execute("""
        UPDATE intake_items
        SET product_name = %s, tcgplayer_id = %s, market_price = %s,
            set_name = %s, offer_price = %s, is_mapped = %s
        WHERE id = %s
    """, (product_name, tcgplayer_id, market_price, set_name, new_offer,
          tcgplayer_id is not None, item_id))

    # Save mapping
    if tcgplayer_id:
        _save_mapping(product_name, int(tcgplayer_id), "sealed", set_name=set_name)

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def update_item_quantity(item_id: str, new_qty: int) -> dict:
    """Update an item's quantity and recalculate offer price proportionally."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    old_qty = item.get("quantity", 1)
    old_offer = Decimal(str(item.get("offer_price", 0)))
    # Scale offer proportionally
    if old_qty > 0:
        per_unit_offer = old_offer / old_qty
        new_offer = (per_unit_offer * new_qty).quantize(Decimal("0.01"))
    else:
        new_offer = Decimal("0")

    execute("UPDATE intake_items SET quantity = %s, offer_price = %s WHERE id = %s",
            (new_qty, new_offer, item_id))
    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def delete_item(item_id: str) -> dict:
    """Delete an item from a session."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")
    session_id = item["session_id"]
    # Also delete any children
    execute("DELETE FROM intake_items WHERE parent_item_id = %s", (item_id,))
    execute("DELETE FROM intake_items WHERE id = %s", (item_id,))
    _recalculate_session_totals(session_id)
    return {"deleted": item_id, "session_id": session_id}


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
    execute("""
        INSERT INTO product_mappings (collectr_name, tcgplayer_id, product_type, set_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (collectr_name, product_type)
        DO UPDATE SET
            tcgplayer_id = EXCLUDED.tcgplayer_id,
            last_used = CURRENT_TIMESTAMP,
            use_count = product_mappings.use_count + 1
    """, (product_name, tcgplayer_id, product_type, set_name))


# ==========================================
# BREAKDOWN CACHE
# ==========================================

def get_breakdown_cache(tcgplayer_id: int) -> Optional[dict]:
    """
    Fetch the cached breakdown for a sealed product (with components).
    Returns None if no cache exists.
    """
    cache = query_one(
        "SELECT * FROM sealed_breakdown_cache WHERE tcgplayer_id = %s",
        (tcgplayer_id,)
    )
    if not cache:
        return None

    components = query("""
        SELECT * FROM sealed_breakdown_components
        WHERE breakdown_id = %s
        ORDER BY display_order, created_at
    """, (str(cache["id"]),))

    return {
        **cache,
        "components": list(components),
    }


def save_breakdown_cache(tcgplayer_id: int, product_name: str,
                          components: list[dict],
                          promo_notes: str = None,
                          updated_by: str = None) -> dict:
    """
    Save or update the breakdown cache for a sealed product.

    components: list of {product_name, tcgplayer_id?, set_name?, quantity_per_parent, market_price, notes?}
    Completely replaces the existing component list.
    """
    existing = query_one(
        "SELECT id FROM sealed_breakdown_cache WHERE tcgplayer_id = %s",
        (tcgplayer_id,)
    )

    # Calculate totals
    total_market = sum(
        Decimal(str(c.get("market_price", 0))) * int(c.get("quantity_per_parent", 1))
        for c in components
    )
    component_count = len(components)

    if existing:
        cache_id = str(existing["id"])
        execute("""
            UPDATE sealed_breakdown_cache
            SET product_name = %s, total_component_market = %s,
                component_count = %s, promo_notes = %s,
                last_updated = CURRENT_TIMESTAMP, last_updated_by = %s
            WHERE id = %s
        """, (product_name, total_market, component_count, promo_notes, updated_by, cache_id))
        # Delete old components
        execute("DELETE FROM sealed_breakdown_components WHERE breakdown_id = %s", (cache_id,))
    else:
        row = execute_returning("""
            INSERT INTO sealed_breakdown_cache
                (tcgplayer_id, product_name, total_component_market,
                 component_count, promo_notes, last_updated_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (tcgplayer_id, product_name, total_market, component_count, promo_notes, updated_by))
        cache_id = str(row["id"])

    # Insert fresh components
    for order, comp in enumerate(components):
        execute("""
            INSERT INTO sealed_breakdown_components
                (breakdown_id, tcgplayer_id, product_name, set_name,
                 quantity_per_parent, market_price, notes, display_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            cache_id,
            comp.get("tcgplayer_id"),
            comp["product_name"],
            comp.get("set_name"),
            int(comp.get("quantity_per_parent", comp.get("quantity", 1))),
            Decimal(str(comp.get("market_price", 0))),
            comp.get("notes"),
            order,
        ))

    # Bump use count
    execute("""
        UPDATE sealed_breakdown_cache
        SET use_count = use_count + 1
        WHERE tcgplayer_id = %s
    """, (tcgplayer_id,))

    return get_breakdown_cache(tcgplayer_id)


def list_breakdown_cache(limit: int = 100) -> list[dict]:
    """List all cached breakdowns ordered by most-used."""
    rows = query("""
        SELECT sbc.*, COUNT(sbco.id) AS component_count_live
        FROM sealed_breakdown_cache sbc
        LEFT JOIN sealed_breakdown_components sbco ON sbco.breakdown_id = sbc.id
        GROUP BY sbc.id
        ORDER BY sbc.use_count DESC, sbc.last_updated DESC
        LIMIT %s
    """, (limit,))
    return list(rows)


def delete_breakdown_cache(tcgplayer_id: int) -> bool:
    """Delete a breakdown cache entry (and its components via CASCADE)."""
    rows = execute(
        "DELETE FROM sealed_breakdown_cache WHERE tcgplayer_id = %s",
        (tcgplayer_id,)
    )
    return rows > 0


def break_down_item_with_cache(item_id: str, components: list[dict],
                                 save_to_cache: bool = True) -> dict:
    """
    Break down a sealed product and optionally save the recipe to cache.
    Wraps break_down_item(), then persists cache if requested.
    """
    # First do the actual breakdown
    result = break_down_item(item_id, components)

    # Optionally save to cache
    if save_to_cache:
        parent = result["parent_item"]
        tcgplayer_id = parent.get("tcgplayer_id")
        product_name = parent.get("product_name", "Unknown")
        if tcgplayer_id:
            # Normalize components to cache format
            cache_components = []
            for comp in components:
                cache_components.append({
                    "product_name": comp["product_name"],
                    "tcgplayer_id": comp.get("tcgplayer_id"),
                    "set_name": comp.get("set_name"),
                    "quantity_per_parent": int(comp.get("quantity", 1)),
                    "market_price": comp.get("market_price", 0),
                    "notes": comp.get("notes"),
                })
            save_breakdown_cache(tcgplayer_id, product_name, cache_components)
            result["cache_saved"] = True
        else:
            result["cache_saved"] = False

    return result


def get_breakdown_value_for_items(tcg_ids: list[int]) -> dict[int, dict]:
    """
    For a list of tcgplayer_ids, return a map of tcg_id -> breakdown info
    (total component market value, component count) where cache exists.
    Used by intake store check to show breakdown value.
    """
    if not tcg_ids:
        return {}

    placeholders = ",".join(["%s"] * len(tcg_ids))
    rows = query(f"""
        SELECT tcgplayer_id, product_name, total_component_market,
               component_count, promo_notes
        FROM sealed_breakdown_cache
        WHERE tcgplayer_id IN ({placeholders})
    """, tuple(tcg_ids))

    return {r["tcgplayer_id"]: dict(r) for r in rows}


def get_breakdown_store_check(tcg_ids: list[int]) -> dict[int, dict]:
    """
    For store check: for each tcg_id that has a breakdown cache,
    return a map with component-level Shopify presence.
    Used to determine if 'broken down' counts as in-store.
    """
    if not tcg_ids:
        return {}

    placeholders = ",".join(["%s"] * len(tcg_ids))
    # Get all caches for these products
    caches = query(f"""
        SELECT sbc.id AS cache_id, sbc.tcgplayer_id AS parent_tcg_id,
               sbc.total_component_market, sbc.component_count,
               sbco.tcgplayer_id AS comp_tcg_id, sbco.product_name,
               sbco.quantity_per_parent, sbco.market_price
        FROM sealed_breakdown_cache sbc
        JOIN sealed_breakdown_components sbco ON sbco.breakdown_id = sbc.id
        WHERE sbc.tcgplayer_id IN ({placeholders})
    """, tuple(tcg_ids))

    if not caches:
        return {}

    # Get all component tcg_ids to check store presence
    comp_tcg_ids = list(set(r["comp_tcg_id"] for r in caches if r["comp_tcg_id"]))
    store_map = {}
    if comp_tcg_ids:
        comp_placeholders = ",".join(["%s"] * len(comp_tcg_ids))
        store_rows = query(f"""
            SELECT tcgplayer_id, shopify_qty, shopify_price, title
            FROM shopify_product_cache
            WHERE tcgplayer_id IN ({comp_placeholders}) AND is_damaged = FALSE
        """, tuple(comp_tcg_ids))
        for r in store_rows:
            store_map[r["tcgplayer_id"]] = r

    # Build result per parent
    result = {}
    for row in caches:
        parent_id = row["parent_tcg_id"]
        if parent_id not in result:
            result[parent_id] = {
                "total_component_market": float(row["total_component_market"] or 0),
                "component_count": row["component_count"],
                "components": [],
                "all_components_in_store": True,
                "components_in_store": 0,
            }
        comp_store = store_map.get(row["comp_tcg_id"])
        in_store = comp_store is not None and (comp_store.get("shopify_qty") or 0) > 0
        result[parent_id]["components"].append({
            "tcgplayer_id": row["comp_tcg_id"],
            "product_name": row["product_name"],
            "quantity_per_parent": row["quantity_per_parent"],
            "market_price": float(row["market_price"] or 0),
            "in_store": in_store,
            "store_qty": comp_store.get("shopify_qty", 0) if comp_store else 0,
            "store_price": float(comp_store["shopify_price"]) if comp_store and comp_store.get("shopify_price") else None,
        })
        if in_store:
            result[parent_id]["components_in_store"] += 1
        else:
            result[parent_id]["all_components_in_store"] = False

    return result
