"""
Ingest business logic.

Handles:
    - Listing received sessions for warehouse processing
    - Breaking down sealed products into components
    - Pushing inventory live to Shopify (increment, create damaged, new listings)
    - Marking items as damaged during ingest
"""

import json
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
        WHERE s.status IN ('received', 'ingested', 'partially_ingested')
        GROUP BY s.id
        ORDER BY
            CASE s.status WHEN 'received' THEN 0 ELSE 1 END,
            s.created_at DESC
        LIMIT %s
    """, (limit,))


def list_sessions_pending(limit: int = 50) -> list[dict]:
    """List sessions that still need ingesting (received + partially_ingested)."""
    return query("""
        SELECT s.*,
               COUNT(i.id) AS item_count,
               COALESCE(SUM(i.quantity), 0) AS total_qty
        FROM intake_sessions s
        LEFT JOIN intake_items i ON i.session_id = s.id
            AND i.item_status IN ('good', 'damaged')
        WHERE s.status IN ('received', 'partially_ingested')
        GROUP BY s.id
        ORDER BY
            CASE s.status WHEN 'received' THEN 0 ELSE 1 END,
            s.created_at DESC
        LIMIT %s
    """, (limit,))


def list_sessions_completed(limit: int = 50, days: int = None) -> list[dict]:
    """List fully ingested sessions, optionally filtered by recency."""
    if days:
        return query("""
            SELECT s.*,
                   COUNT(i.id) AS item_count,
                   COALESCE(SUM(i.quantity), 0) AS total_qty
            FROM intake_sessions s
            LEFT JOIN intake_items i ON i.session_id = s.id
                AND i.item_status IN ('good', 'damaged')
            WHERE s.status = 'ingested'
              AND s.ingested_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            GROUP BY s.id
            ORDER BY s.ingested_at DESC
            LIMIT %s
        """, (days, limit))
    return query("""
        SELECT s.*,
               COUNT(i.id) AS item_count,
               COALESCE(SUM(i.quantity), 0) AS total_qty
        FROM intake_sessions s
        LEFT JOIN intake_items i ON i.session_id = s.id
            AND i.item_status IN ('good', 'damaged')
        WHERE s.status = 'ingested'
        GROUP BY s.id
        ORDER BY s.ingested_at DESC
        LIMIT %s
    """, (limit,))


def get_session(session_id: str) -> Optional[dict]:
    return query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))


def get_session_items(session_id: str) -> list[dict]:
    return query(
        "SELECT * FROM intake_items WHERE session_id = %s AND item_status NOT IN ('rejected', 'missing') ORDER BY created_at",
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


DAMAGE_DISCOUNT = Decimal("0.88")  # 88% of offer price for damaged items


def mark_item_damaged(item_id: str) -> dict:
    """Mark an entire item as damaged and apply damage discount to offer."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    market_price = Decimal(str(item.get("market_price", 0)))
    qty = item.get("quantity", 1)
    damaged_offer = (market_price * DAMAGE_DISCOUNT * offer_pct * qty).quantize(Decimal("0.01"))

    execute(
        "UPDATE intake_items SET item_status = 'damaged', offer_price = %s WHERE id = %s",
        (damaged_offer, item_id)
    )
    _recalculate_session_totals(item["session_id"])
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
    """Restore an item to good status and restore full offer price."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    market_price = Decimal(str(item.get("market_price", 0)))
    qty = item.get("quantity", 1)
    full_offer = (market_price * offer_pct * qty).quantize(Decimal("0.01"))

    execute(
        "UPDATE intake_items SET item_status = 'good', offer_price = %s WHERE id = %s",
        (full_offer, item_id)
    )
    _recalculate_session_totals(item["session_id"])
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
    Build normal_cache and damaged_cache maps from inventory_product_cache.
    Returns (normal_cache, damaged_cache) keyed by tcgplayer_id.
    """
    if not tcg_ids:
        return {}, {}

    placeholders = ",".join(["%s"] * len(tcg_ids))
    rows = query(
        f"SELECT * FROM inventory_product_cache WHERE tcgplayer_id IN ({placeholders})",
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
# OFFER ADJUSTMENT TRACKING
# ==========================================

def _ensure_offer_snapshot(session_id: str):
    """Backfill original_offer_amount and snapshot if missing (pre-migration sessions)."""
    try:
        session = query_one("SELECT original_offer_amount, received_items_snapshot FROM intake_sessions WHERE id = %s", (session_id,))
    except Exception:
        return  # columns don't exist yet — migration not run
    if session and (session.get("original_offer_amount") is None or session.get("received_items_snapshot") is None):
        current = query_one("SELECT total_offer_amount FROM intake_sessions WHERE id = %s", (session_id,))
        items = get_session_items(session_id)
        snapshot = json.dumps([{
            "id": str(i["id"]),
            "product_name": i.get("product_name"),
            "tcgplayer_id": i.get("tcgplayer_id"),
            "quantity": i.get("quantity", 1),
            "market_price": float(i.get("market_price") or 0),
            "offer_price": float(i.get("offer_price") or 0),
            "item_status": i.get("item_status", "good"),
        } for i in items if i.get("item_status") in ("good", "damaged")])
        execute("""
            UPDATE intake_sessions
            SET original_offer_amount = COALESCE(original_offer_amount, total_offer_amount),
                received_items_snapshot = COALESCE(received_items_snapshot, %s)
            WHERE id = %s
        """, (snapshot, session_id))


def get_offer_adjustment_summary(session_id: str) -> Optional[dict]:
    """
    Compare current session items against the receive-time snapshot.
    Returns {original_offer, adjusted_offer, delta, adjustments: [{type, description, amount}]}
    or None if no snapshot is available.
    """
    _ensure_offer_snapshot(session_id)
    try:
        session = query_one(
            "SELECT original_offer_amount, total_offer_amount, received_items_snapshot FROM intake_sessions WHERE id = %s",
            (session_id,)
        )
    except Exception:
        return None  # columns don't exist yet — migration not run
    if not session or not session.get("received_items_snapshot"):
        return None

    original_offer = float(session.get("original_offer_amount") or 0)
    adjusted_offer = float(session.get("total_offer_amount") or 0)
    snapshot_items = session["received_items_snapshot"]
    if isinstance(snapshot_items, str):
        snapshot_items = json.loads(snapshot_items)

    # Build lookup maps
    snap_by_id = {s["id"]: s for s in snapshot_items}
    snap_id_set = set(snap_by_id.keys())

    # Get ALL current items (including broken_down, etc.)
    all_current_items = query(
        "SELECT * FROM intake_items WHERE session_id = %s ORDER BY created_at",
        (session_id,)
    )
    all_curr_by_id = {str(i["id"]): i for i in all_current_items}
    current_active = [i for i in all_current_items if i.get("item_status") in ("good", "damaged")]
    curr_by_id = {str(i["id"]): i for i in current_active}

    # ── Build family tree: trace every current item back to its snapshot ancestor ──
    # Each current item contributes its offer to exactly one snapshot item's "family".
    # Items with no snapshot ancestor are truly "added".

    def _find_snap_ancestor(item_id):
        """Walk up parent_item_id chain to find the snapshot item this descends from."""
        visited = set()
        cur = item_id
        while cur and cur not in visited:
            if cur in snap_id_set:
                return cur
            visited.add(cur)
            parent = all_curr_by_id.get(cur)
            if not parent:
                # Item deleted — check if cur itself was a snapshot item
                return cur if cur in snap_id_set else None
            cur = str(parent.get("parent_item_id") or "")
        return None

    # For each snapshot item, sum the current active offer from its family
    family_offer = {sid: 0.0 for sid in snap_by_id}
    orphan_items = []  # active items with no snapshot ancestor (truly added)

    for i in current_active:
        iid = str(i["id"])
        offer = float(i.get("offer_price") or 0)
        if iid in snap_id_set:
            # This item IS a snapshot item — its offer contributes to itself
            family_offer[iid] += offer
        elif i.get("parent_item_id"):
            ancestor = _find_snap_ancestor(str(i["parent_item_id"]))
            if ancestor and ancestor in family_offer:
                family_offer[ancestor] += offer
            else:
                orphan_items.append(i)  # can't trace back — treat as added
        else:
            orphan_items.append(i)  # no parent, not in snapshot — added

    # ── Classify each snapshot item's change ──
    adjustments = []

    for sid, snap in snap_by_id.items():
        snap_offer = snap["offer_price"]
        curr_total = family_offer[sid]
        amount = round(curr_total - snap_offer, 2)

        # No change — skip
        if abs(amount) < 0.01:
            continue

        curr = curr_by_id.get(sid)
        full_item = all_curr_by_id.get(sid)

        # ── Item fully broken down (status=broken_down, family includes children) ──
        if full_item and full_item.get("item_status") == "broken_down":
            curr_qty = full_item.get("quantity", 1)
            snap_qty = snap.get("quantity", 1)
            if curr_qty < snap_qty:
                # Qty was reduced before breakdown (e.g. relinked some to different product)
                missing_qty = snap_qty - curr_qty
                adjustments.append({
                    "type": "qty_changed",
                    "description": f"Qty: {snap['product_name']} ({snap_qty} → {curr_qty})",
                    "amount": amount,
                })
            elif abs(amount) > 0.01:
                adjustments.append({
                    "type": "price_changed",
                    "description": f"Breakdown rounding: {snap['product_name']}",
                    "amount": amount,
                })
            continue

        # ── Item deleted from DB ──
        if not full_item:
            adjustments.append({
                "type": "missing",
                "description": f"Missing: {snap['product_name']} (×{snap.get('quantity', 1)})",
                "amount": amount,
            })
            continue

        # ── Item still active ──
        if curr:
            # Check for new damage-split children (not in snapshot)
            new_damaged = [i for i in all_current_items
                           if str(i.get("parent_item_id") or "") == sid
                           and i.get("item_status") == "damaged"
                           and str(i["id"]) not in snap_id_set]

            if new_damaged:
                total_damaged_qty = sum(k.get("quantity", 1) for k in new_damaged)
                adjustments.append({
                    "type": "damaged",
                    "description": f"Damaged: {snap['product_name']} (×{total_damaged_qty})",
                    "amount": amount,
                })
                continue

            # Whole item changed good → damaged
            if curr.get("item_status") == "damaged" and snap.get("item_status") != "damaged":
                adjustments.append({
                    "type": "damaged",
                    "description": f"Damaged: {snap['product_name']} (×{curr.get('quantity', 1)})",
                    "amount": amount,
                })
                continue

            # Has breakdown children — figure out if there's also missing qty
            bd_children = [i for i in all_current_items
                           if str(i.get("parent_item_id") or "") == sid
                           and i.get("item_status") == "broken_down"]
            if bd_children:
                bd_qty = sum(c.get("quantity", 1) for c in bd_children)
                expected_qty = snap.get("quantity", 1) - bd_qty
                actual_qty = curr.get("quantity", 1)
                if actual_qty < expected_qty:
                    missing_qty = expected_qty - actual_qty
                    adjustments.append({
                        "type": "missing",
                        "description": f"Missing: {snap['product_name']} (×{missing_qty})",
                        "amount": amount,
                    })
                elif abs(amount) > 0.01:
                    adjustments.append({
                        "type": "price_changed",
                        "description": f"Breakdown rounding: {snap['product_name']}",
                        "amount": amount,
                    })
                continue

            # Relinked (different product)
            curr_tcg = curr.get("tcgplayer_id")
            snap_tcg = snap.get("tcgplayer_id")
            if curr_tcg and snap_tcg and int(curr_tcg) != int(snap_tcg):
                adjustments.append({
                    "type": "relinked",
                    "description": f"Changed: {snap['product_name']} → {curr.get('product_name')}",
                    "amount": amount,
                })
                continue

            # Quantity changed
            if curr.get("quantity", 1) != snap.get("quantity", 1):
                adjustments.append({
                    "type": "qty_changed",
                    "description": f"Qty: {snap['product_name']} ({snap['quantity']} → {curr.get('quantity', 1)})",
                    "amount": amount,
                })
                continue

            # Price changed
            adjustments.append({
                "type": "price_changed",
                "description": f"Price adjusted: {snap['product_name']}",
                "amount": amount,
            })
            continue

        # Item exists but non-active status (e.g. rejected) — and family has some value
        adjustments.append({
            "type": "removed",
            "description": f"Removed: {snap['product_name']} (×{snap.get('quantity', 1)})",
            "amount": amount,
        })

    # ── Truly new items (no snapshot ancestor) ──
    for curr in orphan_items:
        amount = round(float(curr.get("offer_price") or 0), 2)
        if abs(amount) < 0.01:
            continue
        adjustments.append({
            "type": "added",
            "description": f"Added: {curr.get('product_name')} (×{curr.get('quantity', 1)})",
            "amount": amount,
        })

    delta = round(adjusted_offer - original_offer, 2)

    return {
        "original_offer": round(original_offer, 2),
        "adjusted_offer": round(adjusted_offer, 2),
        "delta": delta,
        "adjustments": adjustments,
    }


# ==========================================
# ADD ITEM TO SESSION
# ==========================================

def add_item_to_session(session_id: str, data: dict) -> dict:
    """Add a new item to an ingest session."""
    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))
    if not session:
        raise ValueError("Session not found")
    if session["status"] not in ("received", "partially_ingested"):
        raise ValueError(f"Cannot add items — session is '{session['status']}'")

    product_name = data.get("product_name")
    tcgplayer_id = data.get("tcgplayer_id")
    market_price = Decimal(str(data.get("market_price", 0)))
    quantity = int(data.get("quantity", 1))
    set_name = data.get("set_name")
    product_type = data.get("product_type", "sealed")

    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    offer_price = (market_price * offer_pct * quantity).quantize(Decimal("0.01"))

    item_id = str(uuid4())
    execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, set_name, tcgplayer_id,
            quantity, market_price, offer_price, product_type,
            is_mapped, item_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        item_id, session_id, product_name, set_name,
        int(tcgplayer_id) if tcgplayer_id else None,
        quantity, market_price, offer_price, product_type,
        tcgplayer_id is not None, "good",
    ))

    if tcgplayer_id and product_name:
        _save_mapping(product_name, int(tcgplayer_id), product_type, set_name=set_name)

    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


# ==========================================
# BREAKDOWN CACHE  (multi-variant)
# ==========================================

def get_breakdown_cache(tcgplayer_id: int) -> Optional[dict]:
    """
    Fetch full breakdown record for a product: all variants + their components.
    Returns None if not cached.
    Shape: {id, tcgplayer_id, product_name, variant_count, best_variant_market, use_count,
            variants: [{id, variant_name, notes, total_component_market, component_count,
                        components: [{tcgplayer_id, product_name, set_name, quantity_per_parent, market_price}]}]}
    """
    cache = query_one("SELECT * FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcgplayer_id,))
    if not cache:
        return None
    variants = query(
        "SELECT * FROM sealed_breakdown_variants WHERE breakdown_id=%s ORDER BY display_order, created_at",
        (str(cache["id"]),)
    )
    result = dict(cache)
    result["variants"] = []
    for v in variants:
        comps = query(
            "SELECT * FROM sealed_breakdown_components WHERE variant_id=%s ORDER BY display_order",
            (str(v["id"]),)
        )
        result["variants"].append({**v, "components": list(comps)})
    return result


def save_variant(tcgplayer_id: int, product_name: str,
                 variant_name: str, components: list[dict],
                 notes: str = None, variant_id: str = None) -> dict:
    """
    Create or replace a named variant for a product.
    - variant_id=None  → create new variant
    - variant_id=<id>  → replace components of that existing variant in-place

    components: [{product_name, tcgplayer_id?, set_name?, quantity_per_parent (or quantity), market_price}]
    Returns full cache record.
    """
    # Ensure parent cache row exists
    existing = query_one("SELECT id FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcgplayer_id,))
    if existing:
        cache_id = str(existing["id"])
        execute("UPDATE sealed_breakdown_cache SET product_name=%s, last_updated=CURRENT_TIMESTAMP WHERE id=%s",
                (product_name, cache_id))
    else:
        row = execute_returning(
            "INSERT INTO sealed_breakdown_cache (tcgplayer_id, product_name) VALUES (%s,%s) RETURNING id",
            (tcgplayer_id, product_name)
        )
        cache_id = str(row["id"])

    # Compute totals
    total_market = sum(
        Decimal(str(c.get("market_price", 0))) * int(c.get("quantity_per_parent", c.get("quantity", 1)))
        for c in components
    )
    comp_count = len(components)

    if variant_id:
        # Update existing variant
        execute("""
            UPDATE sealed_breakdown_variants
            SET variant_name=%s, notes=%s, total_component_market=%s, component_count=%s, last_updated=CURRENT_TIMESTAMP
            WHERE id=%s
        """, (variant_name, notes, total_market, comp_count, variant_id))
        execute("DELETE FROM sealed_breakdown_components WHERE variant_id=%s", (variant_id,))
        vid = variant_id
    else:
        # Count existing for display order
        order_row = query_one(
            "SELECT COUNT(*) AS cnt FROM sealed_breakdown_variants WHERE breakdown_id=%s", (cache_id,)
        )
        disp = int(order_row["cnt"]) if order_row else 0
        v_row = execute_returning("""
            INSERT INTO sealed_breakdown_variants
                (breakdown_id, variant_name, notes, total_component_market, component_count, display_order)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
        """, (cache_id, variant_name, notes, total_market, comp_count, disp))
        vid = str(v_row["id"])

    # Insert components
    for order, comp in enumerate(components):
        execute("""
            INSERT INTO sealed_breakdown_components
                (variant_id, tcgplayer_id, product_name, set_name, quantity_per_parent, market_price, notes, display_order, component_type, market_price_updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        """, (
            vid,
            comp.get("tcgplayer_id"),
            comp["product_name"],
            comp.get("set_name"),
            int(comp.get("quantity_per_parent", comp.get("quantity", 1))),
            Decimal(str(comp.get("market_price", 0))),
            comp.get("notes"),
            order,
            comp.get("component_type", "sealed"),
        ))

    _refresh_cache_totals(cache_id)
    # Bump use count
    execute("UPDATE sealed_breakdown_cache SET use_count=use_count+1 WHERE id=%s", (cache_id,))
    return get_breakdown_cache(tcgplayer_id)


def delete_variant(variant_id: str) -> Optional[dict]:
    """
    Delete a single variant. If the parent has no remaining variants, deletes the parent too.
    Returns updated cache dict or None if parent was also deleted.
    """
    v = query_one("SELECT breakdown_id FROM sealed_breakdown_variants WHERE id=%s", (variant_id,))
    if not v:
        return None
    cache_id = str(v["breakdown_id"])
    execute("DELETE FROM sealed_breakdown_variants WHERE id=%s", (variant_id,))

    remaining = query_one(
        "SELECT COUNT(*) AS cnt FROM sealed_breakdown_variants WHERE breakdown_id=%s", (cache_id,)
    )
    if not remaining or int(remaining["cnt"]) == 0:
        execute("DELETE FROM sealed_breakdown_cache WHERE id=%s", (cache_id,))
        return None

    _refresh_cache_totals(cache_id)
    parent = query_one("SELECT tcgplayer_id FROM sealed_breakdown_cache WHERE id=%s", (cache_id,))
    return get_breakdown_cache(int(parent["tcgplayer_id"])) if parent else None


def delete_breakdown_cache(tcgplayer_id: int) -> bool:
    """Delete the entire breakdown record (all variants) for a product."""
    rows = execute("DELETE FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcgplayer_id,))
    return rows > 0


def list_breakdown_cache(limit: int = 200) -> list[dict]:
    """List all cached products with variant names, ordered by most used."""
    return list(query("""
        SELECT sbc.id, sbc.tcgplayer_id, sbc.product_name,
               sbc.variant_count, sbc.best_variant_market,
               sbc.use_count, sbc.last_updated,
               COALESCE(
                   (SELECT STRING_AGG(variant_name, ' / ' ORDER BY display_order)
                    FROM sealed_breakdown_variants WHERE breakdown_id=sbc.id),
                   ''
               ) AS variant_names
        FROM sealed_breakdown_cache sbc
        ORDER BY sbc.use_count DESC, sbc.last_updated DESC
        LIMIT %s
    """, (limit,)))


def _refresh_cache_totals(cache_id: str):
    """Recompute variant_count + best_variant_market on the parent cache row."""
    execute("""
        UPDATE sealed_breakdown_cache SET
            variant_count=(SELECT COUNT(*) FROM sealed_breakdown_variants WHERE breakdown_id=%s),
            best_variant_market=COALESCE(
                (SELECT MAX(total_component_market) FROM sealed_breakdown_variants WHERE breakdown_id=%s), 0
            ),
            last_updated=CURRENT_TIMESTAMP
        WHERE id=%s
    """, (cache_id, cache_id, cache_id))


def break_down_item_with_cache(item_id: str, components: list[dict],
                                variant_name: str = "Standard",
                                variant_notes: str = None,
                                variant_id: str = None,
                                save_to_cache: bool = True) -> dict:
    """
    Break down a sealed item and optionally save/update the variant recipe in cache.
    qty_to_break defaults to the item's full quantity.
    """
    result = break_down_item(item_id, components)

    if save_to_cache:
        parent = result["parent_item"]
        tcgplayer_id = parent.get("tcgplayer_id")
        product_name = parent.get("product_name", "Unknown")
        if tcgplayer_id:
            cache_comps = [{
                "product_name": c["product_name"],
                "tcgplayer_id": c.get("tcgplayer_id"),
                "set_name": c.get("set_name"),
                "quantity_per_parent": int(c.get("quantity", 1)),
                "market_price": c.get("market_price", 0),
            } for c in components]
            save_variant(tcgplayer_id, product_name, variant_name, cache_comps,
                         notes=variant_notes, variant_id=variant_id)
            result["cache_saved"] = True
            result["variant_name"] = variant_name
        else:
            result["cache_saved"] = False

    return result


def split_then_break_down(item_id: str, qty_to_break: int,
                           components: list[dict],
                           variant_name: str = "Standard",
                           variant_notes: str = None,
                           variant_id: str = None,
                           save_to_cache: bool = True) -> dict:
    """
    For items where qty > 1 and you only want to break down some:
    splits off qty_to_break into a new row, then breaks that down.
    Returns result + remainder_item.
    """
    item = query_one("SELECT * FROM intake_items WHERE id=%s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    total_qty = int(item.get("quantity", 1))
    if qty_to_break < 1 or qty_to_break > total_qty:
        raise ValueError(f"qty_to_break must be 1–{total_qty}")

    remainder_item = None

    if qty_to_break < total_qty:
        remainder_qty = total_qty - qty_to_break
        old_offer = Decimal(str(item.get("offer_price", 0)))
        per_unit = old_offer / total_qty if total_qty else Decimal("0")
        remainder_offer = (per_unit * remainder_qty).quantize(Decimal("0.01"))
        break_offer = (per_unit * qty_to_break).quantize(Decimal("0.01"))

        execute("UPDATE intake_items SET quantity=%s, offer_price=%s WHERE id=%s",
                (remainder_qty, remainder_offer, item_id))

        from uuid import uuid4
        new_id = str(uuid4())
        execute("""
            INSERT INTO intake_items
                (id, session_id, product_name, tcgplayer_id, product_type, set_name,
                 quantity, market_price, offer_price, unit_cost_basis, is_mapped, item_status, parent_item_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (new_id, item["session_id"], item["product_name"], item.get("tcgplayer_id"),
              item.get("product_type", "sealed"), item.get("set_name"),
              qty_to_break, item["market_price"], break_offer,
              item.get("unit_cost_basis"), item.get("is_mapped"), "good", item_id))

        target_item_id = new_id
        remainder_item = query_one("SELECT * FROM intake_items WHERE id=%s", (item_id,))
        _recalculate_session_totals(item["session_id"])
    else:
        target_item_id = item_id

    result = break_down_item_with_cache(
        target_item_id, components,
        variant_name=variant_name, variant_notes=variant_notes,
        variant_id=variant_id, save_to_cache=save_to_cache,
    )
    result["remainder_item"] = remainder_item
    return result


def get_breakdown_summary_for_items(tcg_ids: list[int], ppt=None) -> dict:
    """
    Batch lookup: tcg_id -> {variant_count, best_variant_market, variant_names,
                              best_variant_store, parent_store_price,
                              components_in_store, total_components}.
    Joins inventory_product_cache to compute store-aware totals for the best variant.
    Four cases handled by frontend:
      parent+children in store  -> compare children store total vs parent store
      parent in store, no child store -> compare children market vs parent store
      children in store, no parent -> compare children store vs parent market
      neither in store -> compare market totals
    """
    if not tcg_ids:
        return {}
    ph = ",".join(["%s"] * len(tcg_ids))

    # Step 1: get best variant per parent (highest total_component_market)
    variant_rows = query(f"""
        SELECT sbc.tcgplayer_id AS parent_id,
               sbc.variant_count, sbc.best_variant_market,
               COALESCE(
                   (SELECT STRING_AGG(sbv2.variant_name, ' / ' ORDER BY sbv2.display_order)
                    FROM sealed_breakdown_variants sbv2 WHERE sbv2.breakdown_id=sbc.id), ''
               ) AS variant_names,
               sbv.id AS variant_id
        FROM sealed_breakdown_cache sbc
        JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
            AND sbv.total_component_market = sbc.best_variant_market
        WHERE sbc.tcgplayer_id IN ({ph})
    """, tuple(tcg_ids))

    if not variant_rows:
        return {}

    # Step 2: get components for those variants
    variant_ids = [r["variant_id"] for r in variant_rows]

    # JIT refresh stale component market prices
    if ppt and variant_ids:
        try:
            import db as _db
            from breakdown_helpers import refresh_stale_component_prices
            refresh_stale_component_prices(variant_ids, _db, ppt)
        except Exception as e:
            logger.warning(f"Component price refresh skipped: {e}")
    parent_ids  = [r["parent_id"]  for r in variant_rows]
    vph = ",".join(["%s"] * len(variant_ids))

    comp_rows = query(f"""
        SELECT sbco.variant_id, sbco.tcgplayer_id AS comp_tcg_id,
               sbco.quantity_per_parent, sbco.market_price AS comp_market,
               COALESCE(sbco.component_type, 'sealed') AS component_type
        FROM sealed_breakdown_components sbco
        WHERE sbco.variant_id IN ({vph})
    """, tuple(variant_ids))

    # Step 3: batch store lookup for parents + all component tcg_ids
    comp_tcg_ids = list(set(r["comp_tcg_id"] for r in comp_rows if r.get("comp_tcg_id")))
    all_store_ids = list(set(parent_ids + comp_tcg_ids))
    if all_store_ids:
        sph = ",".join(["%s"] * len(all_store_ids))
        store_rows = query(
            f"SELECT tcgplayer_id, shopify_price, shopify_qty FROM inventory_product_cache "
            f"WHERE tcgplayer_id IN ({sph}) AND is_damaged = FALSE",
            tuple(all_store_ids)
        )
        store_map = {r["tcgplayer_id"]: r for r in store_rows}
    else:
        store_map = {}

    # Load ALL variants' components for deep value (not just the best variant)
    all_variant_comps = []
    if tcg_ids:
        avph = ",".join(["%s"] * len(tcg_ids))
        all_variant_comps = query(f"""
            SELECT sbco.tcgplayer_id AS comp_tcg_id, sbco.quantity_per_parent,
                   sbco.market_price AS comp_market, sbv.id AS variant_id,
                   sbc.tcgplayer_id AS parent_id
            FROM sealed_breakdown_components sbco
            JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
            WHERE sbc.tcgplayer_id IN ({avph}) AND sbco.tcgplayer_id IS NOT NULL
        """, tuple(tcg_ids))

    all_comp_tcg_ids = list(set(
        comp_tcg_ids + [int(c["comp_tcg_id"]) for c in all_variant_comps if c["comp_tcg_id"]]
    ))

    # Nested breakdown lookup: which components have their own recipes?
    child_bd_map = {}       # market-based (kept for has_breakdown flag)
    child_bd_store_map = {} # store-based (used for deep value)
    if all_comp_tcg_ids:
        cbp = ",".join(["%s"] * len(all_comp_tcg_ids))
        child_bd_rows = query(
            f"SELECT tcgplayer_id, best_variant_market FROM sealed_breakdown_cache WHERE tcgplayer_id IN ({cbp})",
            tuple(all_comp_tcg_ids)
        )
        child_bd_map = {int(r["tcgplayer_id"]): float(r["best_variant_market"] or 0) for r in child_bd_rows}

        # Compute store-based BD value for children with recipes (grandchild store prices)
        if child_bd_map:
            try:
                child_tcg_list = list(child_bd_map.keys())
                gcph = ",".join(["%s"] * len(child_tcg_list))
                gc_rows = query(f"""
                    SELECT sbc.tcgplayer_id AS child_tcg_id,
                           sbco.tcgplayer_id AS gc_tcg_id,
                           sbco.quantity_per_parent
                    FROM sealed_breakdown_cache sbc
                    JOIN sealed_breakdown_variants sbv ON sbv.breakdown_id = sbc.id
                        AND sbv.total_component_market = sbc.best_variant_market
                    LEFT JOIN sealed_breakdown_components sbco ON sbco.variant_id = sbv.id
                    WHERE sbc.tcgplayer_id IN ({gcph}) AND sbco.tcgplayer_id IS NOT NULL
                """, tuple(child_tcg_list))
                gc_ids = list(set(r["gc_tcg_id"] for r in gc_rows if r["gc_tcg_id"]))
                gc_store = {}
                if gc_ids:
                    gcp = ",".join(["%s"] * len(gc_ids))
                    gc_sp = query(
                        f"SELECT tcgplayer_id, shopify_price FROM inventory_product_cache WHERE tcgplayer_id IN ({gcp}) AND is_damaged = FALSE",
                        tuple(gc_ids))
                    gc_store = {r["tcgplayer_id"]: float(r["shopify_price"] or 0) for r in gc_sp}
                _gc_by_child = {}
                for r in gc_rows:
                    _gc_by_child.setdefault(r["child_tcg_id"], []).append(r)
                for ctid, gcs in _gc_by_child.items():
                    sv = 0.0
                    all_have = True
                    for gc in gcs:
                        sp = gc_store.get(gc["gc_tcg_id"], 0)
                        if sp > 0:
                            sv += sp * (gc["quantity_per_parent"] or 1)
                        else:
                            all_have = False
                    if all_have and sv > 0:
                        child_bd_store_map[ctid] = sv
            except Exception:
                pass

    # Step 4: assemble results
    # Index comp_rows by variant_id
    comps_by_variant = {}
    for c in comp_rows:
        comps_by_variant.setdefault(c["variant_id"], []).append(c)

    result = {}
    for vrow in variant_rows:
        pid = vrow["parent_id"]
        vid = vrow["variant_id"]
        comps = comps_by_variant.get(vid, [])

        parent_store = store_map.get(pid)
        parent_store_price = float(parent_store["shopify_price"]) if parent_store and parent_store.get("shopify_price") else None

        total_comp_market = 0.0
        total_comp_store  = 0.0
        comps_with_store  = 0

        for c in comps:
            qty = c["quantity_per_parent"] or 1
            mkt = float(c["comp_market"] or 0)
            total_comp_market += mkt * qty
            is_promo = c.get("component_type") == "promo"
            if is_promo:
                # Promos are never in the store — use market price for apples-to-apples comparison
                total_comp_store += mkt * qty
                comps_with_store += 1
            else:
                cs = store_map.get(c["comp_tcg_id"])
                if cs and cs.get("shopify_price"):
                    total_comp_store += float(cs["shopify_price"]) * qty
                    comps_with_store += 1

        total_components = len(comps)
        all_comps_in_store = (comps_with_store == total_components and total_components > 0)
        any_comps_in_store = comps_with_store > 0

        # Compute store-based deep value across ALL variants
        best_deep_value = 0.0
        _pvar_comps = {}
        for avc in all_variant_comps:
            if avc["parent_id"] == pid:
                _pvar_comps.setdefault(str(avc["variant_id"]), []).append(avc)
        for _pvid, _pvcomps in _pvar_comps.items():
            dv = 0.0
            dv_has = False
            for vc in _pvcomps:
                cid = int(vc["comp_tcg_id"])
                qty = vc["quantity_per_parent"] or 1
                # Prefer store-based child BD value, fallback to store price, then market
                cbd_store = child_bd_store_map.get(cid, 0)
                if cbd_store > 0:
                    dv += cbd_store * qty
                    dv_has = True  # this child has its own recipe
                else:
                    cs = store_map.get(cid)
                    sp = float(cs["shopify_price"]) if cs and cs.get("shopify_price") else 0
                    if sp > 0:
                        dv += sp * qty
                    else:
                        dv += float(vc["comp_market"] or 0) * qty
            if dv_has and dv > best_deep_value:
                best_deep_value = dv
        has_deep = best_deep_value > 0

        result[pid] = {
            "variant_count":        vrow["variant_count"],
            "best_variant_market":  float(vrow["best_variant_market"] or 0),
            "variant_names":        vrow["variant_names"],
            # Store data
            "parent_store_price":   parent_store_price,
            "best_variant_store":   round(total_comp_store, 2) if any_comps_in_store else None,
            "best_variant_store_partial": any_comps_in_store and not all_comps_in_store,
            "components_in_store":  comps_with_store,
            "total_components":     total_components,
            "deep_bd_value":        round(best_deep_value, 2) if has_deep else None,
        }

    return result
