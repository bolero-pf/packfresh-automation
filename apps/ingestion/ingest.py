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
        WHERE s.status IN ('received', 'verified', 'breakdown_complete', 'ingested', 'partially_ingested')
        GROUP BY s.id
        ORDER BY
            CASE s.status
                WHEN 'received' THEN 0
                WHEN 'verified' THEN 1
                WHEN 'breakdown_complete' THEN 2
                WHEN 'partially_ingested' THEN 3
                ELSE 4
            END,
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
        WHERE s.status IN ('received', 'verified', 'breakdown_complete', 'partially_ingested')
        GROUP BY s.id
        ORDER BY
            CASE s.status
                WHEN 'received' THEN 0
                WHEN 'verified' THEN 1
                WHEN 'breakdown_complete' THEN 2
                WHEN 'partially_ingested' THEN 3
                ELSE 4
            END,
            s.created_at DESC
        LIMIT %s
    """, (limit,))


def list_sessions_completed(limit: int = 50, days: int = None, search: str = None) -> list[dict]:
    """List fully ingested sessions, optionally filtered by recency and/or product search."""
    # When searching, find sessions that contain items matching the search term
    if search:
        search_pattern = f"%{search}%"
        params = [search_pattern]
        days_clause = ""
        if days:
            days_clause = "AND s.ingested_at >= CURRENT_TIMESTAMP - INTERVAL '%s days'"
            params.append(days)
        params.append(limit)
        return query(f"""
            SELECT s.*,
                   COUNT(DISTINCT i.id) AS item_count,
                   COALESCE(SUM(i.quantity), 0) AS total_qty
            FROM intake_sessions s
            JOIN intake_items i ON i.session_id = s.id
            WHERE s.status = 'ingested'
              AND i.product_name ILIKE %s
              {days_clause}
            GROUP BY s.id
            ORDER BY s.ingested_at DESC
            LIMIT %s
        """, tuple(params))

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


def get_session_items(session_id: str, include_missing: bool = False) -> list[dict]:
    if include_missing:
        return query(
            "SELECT * FROM intake_items WHERE session_id = %s AND item_status NOT IN ('rejected') ORDER BY created_at",
            (session_id,)
        )
    return query(
        "SELECT * FROM intake_items WHERE session_id = %s AND item_status NOT IN ('rejected', 'missing') ORDER BY created_at",
        (session_id,)
    )


# ==========================================
# BREAK DOWN
# ==========================================

import re

_CARD_NUMBER_RE = re.compile(
    r' - \d{2,3}(/\d{2,3})?$'   # "- 073" or "- 153/214"
    r'|SWSH\d{2,3}$'            # "SWSH136"
    r'|TG\d{2,3}$'              # "TG15"
    r'| - [A-Z]{2,4}\d{2,4}$'   # "- SV046"
)
_SEALED_KEYWORDS = ['booster pack', 'booster box', 'etb', 'elite trainer box',
                    'tin', 'bundle', 'blister', 'collection box', 'build & battle',
                    'mini tins', 'premium collection']

def _looks_like_single_card(name: str) -> bool:
    """
    Detect if a breakdown component name is a single card vs sealed product.
    Logic: if the name contains a sealed keyword, it's sealed. Otherwise it's a card.
    Breakdown children are either packs/boxes/tins (sealed) or individual cards (raw).
    """
    if not name:
        return False
    n = name.lower()
    if any(kw in n for kw in _SEALED_KEYWORDS):
        return False
    # No sealed keywords → it's a single card
    return True


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
        # Determine if child is a raw card or sealed product.
        # Promos are always raw. For "sealed" components, detect single cards
        # by card number patterns (e.g. "- 073", "- 153/214", "SWSH136").
        comp_type = comp.get("component_type")  # None if not set in recipe
        comp_name = comp.get("product_name", "")
        if comp_type in ("promo", "raw"):
            child_product_type = "raw"
        elif comp_type == "sealed":
            child_product_type = "sealed"
        else:
            # component_type not set — detect from name
            child_product_type = "raw" if _looks_like_single_card(comp_name) else "sealed"
        execute("""
            INSERT INTO intake_items (
                id, session_id, product_name, set_name, tcgplayer_id,
                quantity, market_price, offer_price, product_type,
                is_mapped, item_status, parent_item_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            child_id, session_id, comp["product_name"], comp.get("set_name"),
            comp.get("tcgplayer_id"), child_qty, market_price,
            comp_offer, child_product_type,
            comp.get("tcgplayer_id") is not None, "good", item_id,
        ))

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

# Condition multipliers for raw cards (applied to market price before offer %)
def update_item_condition(item_id: str, condition: str, price_provider=None, price_override: float = None) -> dict:
    """
    Update a raw card's condition and recalculate its offer price.
    Only adjusts price if condition actually changed or price_override is set.

    Price hierarchy (per root CLAUDE.md — always prefer Scrydex):
        price_override > Scrydex cache per-condition > PPT per-condition
        > condition-multiplier fallback from the existing deal market price.
    """
    from price_provider import PriceProvider, FALLBACK_MULTIPLIERS
    from price_cache import PriceCache
    from scrydex_client import ScrydexClient
    import db as db_module

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    condition = condition.upper().strip()
    valid = {"NM", "LP", "MP", "HP", "DMG"}
    if condition not in valid:
        raise ValueError(f"Invalid condition: {condition}. Must be NM, LP, MP, HP, or DMG")

    old_condition = (item.get("condition") or "NM").upper().strip()
    condition_changed = condition != old_condition

    # If condition didn't change and no price override, just update the column (no-op on price)
    if not condition_changed and price_override is None:
        execute("UPDATE intake_items SET condition = %s WHERE id = %s", (condition, item_id))
        return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    qty = item.get("quantity", 1)
    tcg_id = item.get("tcgplayer_id")
    sid = item.get("scrydex_id")  # preferred when set (Scrydex-only cards, incl. old JP)
    variant = item.get("variant")

    if price_override is not None:
        new_market = Decimal(str(price_override)).quantize(Decimal("0.01"))
    elif condition_changed:
        condition_market = None

        # 1. Scrydex cache — lookup by scrydex_id first if present (truly
        # primary), else fall through to tcgplayer_id resolution.
        try:
            cache = PriceCache(db_module)
            card_data = None
            if sid:
                card_data = cache.get_card_by_scrydex_id(sid)
            elif tcg_id:
                card_data = cache.get_card_by_tcgplayer_id(int(tcg_id))
            if card_data:
                condition_market = ScrydexClient.extract_condition_price(
                    card_data, condition, variant=variant)
                if condition_market is not None:
                    logger.info(f"Condition update price from Scrydex cache: "
                                f"${condition_market} for {condition} "
                                f"sid={sid} tcg={tcg_id}")
        except Exception as e:
            logger.warning(f"Scrydex cache condition lookup failed for sid={sid} tcg={tcg_id}: {e}")

        # 2. PPT fallback — only makes sense when we have a TCG ID, since PPT
        # is keyed on TCGplayer product IDs. Scrydex-only cards skip this.
        if condition_market is None and tcg_id and price_provider:
            try:
                card_data = price_provider.get_card_by_tcgplayer_id(int(tcg_id))
                if card_data:
                    condition_market = PriceProvider.extract_condition_price(
                        card_data, condition, variant=variant)
                    if condition_market is not None:
                        logger.info(f"Condition update price from PPT fallback: "
                                    f"${condition_market} for {condition} TCG#{tcg_id}")
            except Exception as e:
                logger.warning(f"PPT condition lookup failed for TCG#{tcg_id}: {e}")

        if condition_market is not None:
            new_market = condition_market
        else:
            # 3. Multiplier fallback from deal-time price
            deal_market = Decimal(str(item.get("market_price", 0)))
            old_mult = FALLBACK_MULTIPLIERS.get(old_condition, Decimal("1.00"))
            new_mult = FALLBACK_MULTIPLIERS.get(condition, Decimal("1.00"))
            if old_mult > 0:
                nm_price = deal_market / old_mult
            else:
                nm_price = deal_market
            new_market = (nm_price * new_mult).quantize(Decimal("0.01"))
    else:
        # No change needed
        return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))

    # Once the item is stamped verified_at, COGS is locked — the offer was
    # already finalized during verify and payment logically flowed on that.
    # Later stages (breakdown, routing) may spot damage we missed, but we've
    # already paid, so the cost of acquisition shouldn't move. Only condition
    # and market_price change; offer_price / unit_cost_basis stay put. The
    # per-item margin will look worse, and that's the correct signal — it
    # flags the missed damage without hiding the evidence.
    cogs_locked = bool(item.get("verified_at"))

    if cogs_locked:
        execute("""
            UPDATE intake_items SET condition = %s, market_price = %s WHERE id = %s
        """, (condition, new_market, item_id))
        # Don't recalc session totals — offer_price didn't change.
    else:
        # Pre-verify: we're still in the offer-adjustment window, so a condition
        # change moves offer_price too.
        new_offer = (new_market * offer_pct * qty).quantize(Decimal("0.01"))
        execute("""
            UPDATE intake_items SET condition = %s, market_price = %s, offer_price = %s WHERE id = %s
        """, (condition, new_market, new_offer, item_id))
        _recalculate_session_totals(item["session_id"])

    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def update_item_grade(item_id: str, grade_company: str = None, grade_value: str = None,
                      price_provider=None, price_override: float = None,
                      db_module=None) -> dict:
    """
    Update a graded slab's company/grade and recalculate its offer price.
    Uses price_override if provided, then Scrydex live → cache → PPT fallback.
    """
    from price_provider import PriceProvider

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    company = grade_company or item.get("grade_company", "PSA")
    grade = grade_value or item.get("grade_value", "")

    old_company = (item.get("grade_company") or "PSA").upper()
    old_grade = str(item.get("grade_value") or "")
    grade_changed = company.upper() != old_company or str(grade) != old_grade

    # If grade didn't change and no price override, just update columns
    if not grade_changed and price_override is None:
        execute("UPDATE intake_items SET grade_company = %s, grade_value = %s WHERE id = %s",
                (company, grade, item_id))
        return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    qty = item.get("quantity", 1)
    tcg_id = item.get("tcgplayer_id")

    sid = item.get("scrydex_id")  # preferred when set (Scrydex-only / JP cards)

    new_market = None
    if price_override is not None:
        new_market = Decimal(str(price_override)).quantize(Decimal("0.01"))
    elif grade_changed and (tcg_id or sid):
        # Scrydex first (live eBay comps → cache). scrydex_id takes priority
        # so JP cards without a TCGplayer mapping still resolve.
        if db_module:
            try:
                from graded_pricing import get_live_graded_comps
                comps = get_live_graded_comps(
                    int(tcg_id) if tcg_id else None, company, grade, db_module,
                    card_name=item.get("product_name"),
                    set_name=item.get("set_name"),
                    card_number=item.get("card_number"),
                    scrydex_id=sid,
                )
                if comps and comps.get("market"):
                    new_market = Decimal(str(comps["market"])).quantize(Decimal("0.01"))
                    logger.info(f"Grade update price from Scrydex ({comps.get('source', '?')}): "
                                f"${new_market} for {company} {grade} sid={sid} tcg={tcg_id}")
            except Exception as e:
                logger.warning(f"Scrydex graded lookup failed for sid={sid} tcg={tcg_id}: {e}")

        # PPT fallback
        if new_market is None and price_provider:
            try:
                card_data = price_provider.get_card_by_tcgplayer_id(int(tcg_id))
                if card_data:
                    graded_price = PriceProvider.get_graded_price(card_data, company, grade)
                    if graded_price is not None:
                        new_market = graded_price
                        logger.info(f"Grade update price from PPT fallback: ${new_market} "
                                    f"for {company} {grade} TCG#{tcg_id}")
            except Exception as e:
                logger.warning(f"PPT graded lookup failed for TCG#{tcg_id}: {e}")

    updates = ["grade_company = %s", "grade_value = %s"]
    params = [company, grade]

    if new_market is not None:
        new_offer = (new_market * offer_pct * qty).quantize(Decimal("0.01"))
        updates.extend(["market_price = %s", "offer_price = %s"])
        params.extend([new_market, new_offer])

    params.append(item_id)
    execute(f"UPDATE intake_items SET {', '.join(updates)} WHERE id = %s", tuple(params))

    if new_market is not None:
        _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def convert_item_type(item_id: str, to_graded: bool,
                      condition: str = None,
                      grade_company: str = None, grade_value: str = None,
                      price_provider=None, price_override: float = None,
                      db_module=None) -> dict:
    """
    Convert an intake item between raw and graded.

    Use cases:
      - Item was entered as raw NM but is actually PSA 10 (common for JP cards
        that aren't in TCGplayer's graded database)
      - Item was entered as graded but the slab turned out to be a reholder or
        the grader's not recognized — drop back to raw

    Flipping is_graded:
      → graded: sets grade_company/grade_value, clears condition, looks up
                Scrydex live → Scrydex cache → PPT graded price, or uses price_override
      → raw:    clears grade_company/grade_value/cert_number, sets condition,
                looks up condition price or uses price_override
    """
    from price_provider import PriceProvider, FALLBACK_MULTIPLIERS

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    qty = item.get("quantity", 1)
    tcg_id = item.get("tcgplayer_id")
    sid = item.get("scrydex_id")

    new_market = None
    if price_override is not None:
        new_market = Decimal(str(price_override)).quantize(Decimal("0.01"))

    if to_graded:
        company = (grade_company or "PSA").upper()
        grade = str(grade_value or "").strip()
        if not grade:
            raise ValueError("grade_value is required when converting to graded")

        # Price lookup: Scrydex live listings → Scrydex cache → PPT fallback
        if new_market is None and (tcg_id or sid):
            # Try Scrydex first (live eBay comps)
            if db_module:
                try:
                    from graded_pricing import get_live_graded_comps
                    comps = get_live_graded_comps(
                        int(tcg_id) if tcg_id else None, company, grade, db_module,
                        card_name=item.get("product_name"),
                        set_name=item.get("set_name"),
                        card_number=item.get("card_number"),
                        scrydex_id=sid,
                    )
                    if comps and comps.get("market"):
                        new_market = Decimal(str(comps["market"])).quantize(Decimal("0.01"))
                        logger.info(f"Graded price from Scrydex ({comps.get('source', '?')}): "
                                    f"${new_market} for {company} {grade} TCG#{tcg_id}")
                except Exception as e:
                    logger.warning(f"Scrydex graded lookup failed for TCG#{tcg_id}: {e}")

            # PPT fallback
            if new_market is None and price_provider:
                try:
                    card_data = price_provider.get_card_by_tcgplayer_id(int(tcg_id))
                    if card_data:
                        graded_price = PriceProvider.get_graded_price(card_data, company, grade)
                        if graded_price is not None:
                            new_market = graded_price
                            logger.info(f"Graded price from PPT fallback: ${new_market} "
                                        f"for {company} {grade} TCG#{tcg_id}")
                except Exception as e:
                    logger.warning(f"PPT graded lookup failed for TCG#{tcg_id}: {e}")

        updates = [
            "is_graded = TRUE",
            "grade_company = %s",
            "grade_value = %s",
            "condition = NULL",
        ]
        params = [company, grade]
    else:
        cond = (condition or "NM").upper().strip()
        valid = {"NM", "LP", "MP", "HP", "DMG"}
        if cond not in valid:
            raise ValueError(f"Invalid condition: {cond}. Must be NM, LP, MP, HP, or DMG")

        if new_market is None and tcg_id and price_provider:
            try:
                card_data = price_provider.get_card_by_tcgplayer_id(int(tcg_id))
                if card_data:
                    cond_price = PriceProvider.extract_condition_price(
                        card_data, cond, variant=item.get("variant"))
                    if cond_price is not None:
                        new_market = cond_price
            except Exception as e:
                logger.warning(f"PPT condition lookup failed for TCG#{tcg_id}: {e}")

        updates = [
            "is_graded = FALSE",
            "grade_company = NULL",
            "grade_value = NULL",
            "cert_number = NULL",
            "condition = %s",
        ]
        params = [cond]

    if new_market is not None:
        new_offer = (new_market * offer_pct * qty).quantize(Decimal("0.01"))
        updates.extend(["market_price = %s", "offer_price = %s"])
        params.extend([new_market, new_offer])

    params.append(item_id)
    execute(f"UPDATE intake_items SET {', '.join(updates)} WHERE id = %s", tuple(params))

    if new_market is not None:
        _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def override_item_price(item_id: str, price: float) -> dict:
    """Override an item's market price and recalculate offer."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    qty = item.get("quantity", 1)
    new_market = Decimal(str(price)).quantize(Decimal("0.01"))
    new_offer = (new_market * offer_pct * qty).quantize(Decimal("0.01"))

    execute("""
        UPDATE intake_items SET market_price = %s, offer_price = %s WHERE id = %s
    """, (new_market, new_offer, item_id))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


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


def split_one_slab(item_id: str) -> dict:
    """
    Split a single slab off a graded item (qty > 1) so each slab can carry
    its own cert number and become its own Shopify product.

    If qty == 1, returns the original item (no split needed).
    Otherwise, decrements the parent qty by 1, creates a child row with qty=1
    inheriting all graded fields, and returns the child for pushing.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    qty = item.get("quantity", 1)
    if qty <= 1:
        return item

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    market_price = Decimal(str(item.get("market_price", 0)))

    # Decrement parent qty + its offer
    new_parent_qty = qty - 1
    new_parent_offer = (market_price * offer_pct * new_parent_qty).quantize(Decimal("0.01"))
    execute(
        "UPDATE intake_items SET quantity = %s, offer_price = %s WHERE id = %s",
        (new_parent_qty, new_parent_offer, item_id),
    )

    # Create a 1-qty child carrying all the graded fields + verified state
    child_id = str(uuid4())
    child_offer = (market_price * offer_pct * 1).quantize(Decimal("0.01"))
    execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, set_name, tcgplayer_id,
            quantity, market_price, offer_price, product_type,
            is_mapped, item_status, verified_at,
            is_graded, grade_company, grade_value,
            variant, language, parent_item_id, condition
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, CURRENT_TIMESTAMP,
            %s, %s, %s,
            %s, %s, %s, %s
        )
    """, (
        child_id, item["session_id"], item.get("product_name"), item.get("set_name"),
        item.get("tcgplayer_id"),
        1, market_price, child_offer, item.get("product_type", "raw"),
        item.get("is_mapped", False), item.get("item_status", "good"),
        item.get("is_graded", True), item.get("grade_company"), item.get("grade_value"),
        item.get("variant"), item.get("language", "EN"), item_id, item.get("condition"),
    ))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (child_id,))


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



# ==========================================
# VERIFY STAGE
# ==========================================

def verify_item_here(item_id: str, qty_confirmed: int = None) -> dict:
    """
    Mark an item as verified (present).
    If qty_confirmed < item.quantity, splits off the missing portion.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    total_qty = item.get("quantity", 1)

    if qty_confirmed is not None and qty_confirmed < total_qty:
        if qty_confirmed < 0:
            raise ValueError("qty_confirmed cannot be negative")
        if qty_confirmed == 0:
            # All missing
            return verify_item_missing(item_id)

        # Split: keep confirmed portion as good+verified, create missing remainder
        session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
        offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
        market_price = Decimal(str(item.get("market_price", 0)))

        # Update original to confirmed qty
        good_offer = (market_price * offer_pct * qty_confirmed).quantize(Decimal("0.01"))
        execute("""
            UPDATE intake_items SET quantity = %s, offer_price = %s, verified_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (qty_confirmed, good_offer, item_id))

        # Create missing split for remainder
        missing_qty = total_qty - qty_confirmed
        missing_id = str(uuid4())
        execute("""
            INSERT INTO intake_items (
                id, session_id, product_name, set_name, tcgplayer_id,
                quantity, market_price, offer_price, product_type,
                is_mapped, item_status, verified_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'missing', CURRENT_TIMESTAMP)
        """, (
            missing_id, item["session_id"], item.get("product_name"), item.get("set_name"),
            item.get("tcgplayer_id"), missing_qty, market_price,
            Decimal("0"), item.get("product_type", "sealed"),
            item.get("is_mapped", False),
        ))

        _recalculate_session_totals(item["session_id"])
        return {
            "good_item": query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,)),
            "missing_item": query_one("SELECT * FROM intake_items WHERE id = %s", (missing_id,)),
        }

    # Full qty confirmed — just stamp verified_at
    execute("UPDATE intake_items SET verified_at = CURRENT_TIMESTAMP WHERE id = %s", (item_id,))
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def verify_item_missing(item_id: str, missing_qty: int = None) -> dict:
    """
    Mark an item (or partial qty) as missing.
    If missing_qty < total, splits: good portion stays, missing portion split off.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    total_qty = item.get("quantity", 1)

    if missing_qty is not None and missing_qty < total_qty:
        if missing_qty < 1:
            raise ValueError("missing_qty must be at least 1")
        # Partial missing — keep the good portion, split off missing
        confirmed_qty = total_qty - missing_qty
        return verify_item_here(item_id, qty_confirmed=confirmed_qty)

    # All missing
    execute("""
        UPDATE intake_items SET item_status = 'missing', offer_price = 0, verified_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (item_id,))
    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def undo_verify(item_id: str) -> dict:
    """
    Reset an item back to unverified good status.
    Restores damaged items to good. Does NOT rejoin split items.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")
    if item.get("pushed_at"):
        raise ValueError("Cannot undo verification on a pushed item")

    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (item["session_id"],))
    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    market_price = Decimal(str(item.get("market_price", 0)))
    qty = item.get("quantity", 1)

    if item["item_status"] == "missing":
        # Restore to good with offer recalculated
        full_offer = (market_price * offer_pct * qty).quantize(Decimal("0.01"))
        execute("""
            UPDATE intake_items SET item_status = 'good', offer_price = %s, verified_at = NULL
            WHERE id = %s
        """, (full_offer, item_id))
    elif item["item_status"] == "damaged":
        # Restore to good
        full_offer = (market_price * offer_pct * qty).quantize(Decimal("0.01"))
        execute("""
            UPDATE intake_items SET item_status = 'good', offer_price = %s, verified_at = NULL
            WHERE id = %s
        """, (full_offer, item_id))
    else:
        # Good item — just clear verified_at
        execute("UPDATE intake_items SET verified_at = NULL WHERE id = %s", (item_id,))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def complete_verification(session_id: str) -> dict:
    """
    Transition session from received → verified.
    Validates all non-missing items have verified_at set.
    """
    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))
    if not session:
        raise ValueError("Session not found")
    if session["status"] not in ("received", "verified", "breakdown_complete"):
        raise ValueError(f"Session must be pre-ingested to complete verification (currently: {session['status']})")

    # Check for unverified items (good/damaged items without verified_at)
    unverified = query("""
        SELECT id, product_name, quantity FROM intake_items
        WHERE session_id = %s AND item_status IN ('good', 'damaged')
          AND verified_at IS NULL
    """, (session_id,))

    if unverified:
        names = [f"{u['product_name']} (×{u['quantity']})" for u in unverified[:5]]
        remaining = len(unverified) - 5
        msg = "Unverified items remain: " + ", ".join(names)
        if remaining > 0:
            msg += f" and {remaining} more"
        raise ValueError(msg)

    # Only advance status, never regress
    if session["status"] == "received":
        execute("UPDATE intake_sessions SET status = 'verified' WHERE id = %s", (session_id,))
    return query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))


def complete_breakdown(session_id: str) -> dict:
    """
    Transition session from verified → breakdown_complete.
    No validation — choosing not to break anything down is valid.
    """
    session = query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))
    if not session:
        raise ValueError("Session not found")
    if session["status"] not in ("verified", "breakdown_complete"):
        raise ValueError(f"Session must be at least 'verified' to complete breakdown (currently: {session['status']})")

    # Only advance status, never regress
    if session["status"] == "verified":
        execute("UPDATE intake_sessions SET status = 'breakdown_complete' WHERE id = %s", (session_id,))
    return query_one("SELECT * FROM intake_sessions WHERE id = %s", (session_id,))


def relink_item(item_id: str, data: dict) -> dict:
    """
    Relink an item to a different product.
    data: {product_name, tcgplayer_id?, market_price, set_name?, scrydex_id?}

    scrydex_id is the true primary key for price lookups — tcgplayer_id is
    just one of several marketplace mappings Scrydex may or may not have.
    If only scrydex_id is supplied (no TCG mapping in Scrydex's data), the
    item is still considered mapped; the condition-change and route-enrich
    paths resolve prices via scrydex_id when tcgplayer_id is absent.

    When both IDs are supplied, the scrydex_tcg_map gets upserted and the
    cache rows for that scrydex_id are backfilled with the TCG ID, so the
    next relink search shows the card as directly linkable.
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    product_name = data.get("product_name", item.get("product_name"))
    tcgplayer_id = data.get("tcgplayer_id")
    set_name = data.get("set_name", item.get("set_name"))
    scrydex_id = data.get("scrydex_id")
    # variant distinguishes printings that share a TCG product (1st Ed vs
    # Unlimited, etc.) — load-bearing for correct graded price lookups.
    variant = data.get("variant", item.get("variant"))

    if not tcgplayer_id and not scrydex_id:
        raise ValueError("Relink requires tcgplayer_id or scrydex_id")

    # Always re-derive market_price from the Scrydex cache using the item's
    # actual condition (and grade if graded). The client passes a price taken
    # from the search listing, which can be for any condition/variant — saving
    # it blindly caused "crazy price" writebacks (e.g. a PSA-10 value landing
    # on a raw LP card). Fall back to the client value if the cache has
    # nothing for this combo.
    condition = item.get("condition") or "NM"
    is_graded = bool(item.get("is_graded"))
    grade_company = item.get("grade_company")
    grade_value = item.get("grade_value")
    derived = None
    # Scrydex sends JP-marketplace prices in JPY; convert on read so derived
    # market_price is always USD before it's written back to intake_items.
    # Rate lives in env (SCRYDEX_JPY_USD_RATE) so ops can tweak without a
    # redeploy when the yen moves.
    import os as _os
    jpy_rate = float(_os.getenv("SCRYDEX_JPY_USD_RATE", "0.0066"))
    price_expr = (
        "CASE WHEN currency = 'JPY' "
        f"THEN ROUND(market_price::numeric * {jpy_rate}::numeric, 2) "
        "ELSE market_price END AS market_price_usd"
    )
    try:
        if is_graded and grade_company and grade_value:
            cache_row = query_one(f"""
                SELECT {price_expr}
                FROM scrydex_price_cache
                WHERE ((%s IS NOT NULL AND tcgplayer_id = %s)
                       OR (%s IS NOT NULL AND scrydex_id = %s))
                  AND price_type = 'graded'
                  AND grade_company = %s
                  AND grade_value = %s
                  AND COALESCE(variant, '') = COALESCE(%s, '')
                ORDER BY fetched_at DESC
                LIMIT 1
            """, (tcgplayer_id, tcgplayer_id, scrydex_id, scrydex_id,
                  grade_company, grade_value, variant))
            if cache_row and cache_row.get("market_price_usd") is not None:
                derived = Decimal(str(cache_row["market_price_usd"]))
        else:
            cache_row = query_one(f"""
                SELECT {price_expr}
                FROM scrydex_price_cache
                WHERE ((%s IS NOT NULL AND tcgplayer_id = %s)
                       OR (%s IS NOT NULL AND scrydex_id = %s))
                  AND price_type = 'raw'
                  AND condition = %s
                  AND COALESCE(variant, '') = COALESCE(%s, '')
                ORDER BY fetched_at DESC
                LIMIT 1
            """, (tcgplayer_id, tcgplayer_id, scrydex_id, scrydex_id,
                  condition, variant))
            if cache_row and cache_row.get("market_price_usd") is not None:
                derived = Decimal(str(cache_row["market_price_usd"]))
            # Condition fallback chain — if NM/LP/MP/HP missing, try NM.
            if derived is None and condition != "NM":
                nm_row = query_one(f"""
                    SELECT {price_expr}
                    FROM scrydex_price_cache
                    WHERE ((%s IS NOT NULL AND tcgplayer_id = %s)
                           OR (%s IS NOT NULL AND scrydex_id = %s))
                      AND price_type = 'raw'
                      AND condition = 'NM'
                      AND COALESCE(variant, '') = COALESCE(%s, '')
                    ORDER BY fetched_at DESC
                    LIMIT 1
                """, (tcgplayer_id, tcgplayer_id, scrydex_id, scrydex_id, variant))
                if nm_row and nm_row.get("market_price_usd") is not None:
                    derived = Decimal(str(nm_row["market_price_usd"]))
    except Exception as e:
        logger.warning(f"Cache-backed price derivation failed for relink on {item_id}: {e}")

    if derived is not None:
        market_price = derived
    else:
        market_price = Decimal(str(data.get("market_price", item.get("market_price", 0))))

    # Sanity check — a raw card's market price should never exceed the highest
    # graded price for the same card. If it does, the cache row is almost
    # certainly in the wrong currency (Scrydex sends JP prices in JPY for some
    # Japanese marketplaces) or the data is otherwise bad. Reject the write so
    # staff see the issue instead of silently stamping an absurd price onto
    # the item. Raise ValueError so the API returns 400 with the message.
    if not is_graded and derived is not None and derived > 0:
        best_graded = query_one("""
            SELECT MAX(market_price) AS max_graded
            FROM scrydex_price_cache
            WHERE ((%s IS NOT NULL AND tcgplayer_id = %s)
                   OR (%s IS NOT NULL AND scrydex_id = %s))
              AND price_type = 'graded'
              AND market_price IS NOT NULL
        """, (tcgplayer_id, tcgplayer_id, scrydex_id, scrydex_id))
        if best_graded and best_graded.get("max_graded"):
            max_g = Decimal(str(best_graded["max_graded"]))
            if max_g > 0 and derived > max_g:
                raise ValueError(
                    f"Refusing relink: raw price ${derived:.2f} exceeds best graded "
                    f"price ${max_g:.2f} — cache data for this card is likely in a "
                    f"non-USD currency or otherwise wrong. Check the card in Scrydex."
                )

    # Recalculate offer proportionally — keep the same COGS ratio
    old_market = Decimal(str(item.get("market_price", 0)))
    old_offer = Decimal(str(item.get("offer_price", 0)))
    qty = item.get("quantity", 1)

    if old_market > 0 and qty > 0:
        ratio = old_offer / (old_market * qty)
        new_offer = (market_price * qty * ratio).quantize(Decimal("0.01"))
    else:
        session = query_one("SELECT offer_percentage FROM intake_sessions WHERE id = %s", (item["session_id"],))
        pct = Decimal(str(session.get("offer_percentage", 65))) / 100
        new_offer = (market_price * qty * pct).quantize(Decimal("0.01"))

    # is_mapped is true when we have either identifier — both resolve to
    # price data via shared/price_cache.
    is_mapped = tcgplayer_id is not None or scrydex_id is not None

    execute("""
        UPDATE intake_items
        SET product_name = %s, tcgplayer_id = %s, scrydex_id = %s, variant = %s,
            market_price = %s, set_name = %s, offer_price = %s,
            is_mapped = %s
        WHERE id = %s
    """, (product_name, tcgplayer_id, scrydex_id, variant,
          market_price, set_name, new_offer,
          is_mapped, item_id))

    if tcgplayer_id:
        _save_mapping(product_name, int(tcgplayer_id), "sealed", set_name=set_name)

        # If the user supplied a TCG ID for a Scrydex-only card, persist the
        # mapping + backfill cache rows so next search shows it as mapped.
        if scrydex_id:
            try:
                execute("""
                    INSERT INTO scrydex_tcg_map (scrydex_id, tcgplayer_id, product_type, game, updated_at)
                    VALUES (%s, %s, 'card', 'pokemon', NOW())
                    ON CONFLICT (scrydex_id) DO UPDATE SET
                        tcgplayer_id = EXCLUDED.tcgplayer_id,
                        updated_at   = NOW()
                """, (scrydex_id, int(tcgplayer_id)))
                execute("""
                    UPDATE scrydex_price_cache
                    SET tcgplayer_id = %s
                    WHERE scrydex_id = %s AND tcgplayer_id IS NULL
                """, (int(tcgplayer_id), scrydex_id))
            except Exception as e:
                logger.warning(f"scrydex_tcg_map write failed for {scrydex_id}={tcgplayer_id}: {e}")

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
    """Save a product name -> tcgplayer_id mapping.
    Uses the same 5-column functional index as intake's save_mapping.
    """
    execute("""
        INSERT INTO product_mappings
            (collectr_name, tcgplayer_id, product_type, set_name, card_number, variance)
        VALUES (%s, %s, %s, %s, '', '')
        ON CONFLICT (collectr_name, product_type, COALESCE(set_name, ''), COALESCE(card_number, ''), COALESCE(variance, ''))
        DO UPDATE SET
            tcgplayer_id = EXCLUDED.tcgplayer_id,
            last_used = CURRENT_TIMESTAMP,
            use_count = product_mappings.use_count + 1
    """, (product_name, tcgplayer_id, product_type, set_name or ''))



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
    # Group orphan breakdown children by their non-snapshot parent so we show
    # "Added: Skeledirge Tin (×5)" instead of each individual pack/promo.
    orphan_by_root = {}  # root_id -> {name, qty, total_offer}
    direct_orphans = []  # items with no parent (genuinely added top-level items)

    for curr in orphan_items:
        offer = float(curr.get("offer_price") or 0)
        pid = str(curr.get("parent_item_id") or "")

        if not pid:
            direct_orphans.append(curr)
            continue

        # Walk up to the top-level non-snapshot ancestor
        root_id = pid
        root_item = all_curr_by_id.get(root_id)
        visited = set()
        while root_item and str(root_item.get("parent_item_id") or "") and root_id not in visited:
            visited.add(root_id)
            next_pid = str(root_item["parent_item_id"])
            if next_pid in snap_id_set:
                break  # stop before entering snapshot territory
            root_id = next_pid
            root_item = all_curr_by_id.get(root_id)

        if root_id not in orphan_by_root:
            root = all_curr_by_id.get(root_id)
            orphan_by_root[root_id] = {
                "name": root.get("product_name") if root else curr.get("product_name"),
                "qty": root.get("quantity", 1) if root else curr.get("quantity", 1),
                "total_offer": 0.0,
            }
        orphan_by_root[root_id]["total_offer"] += offer

    # Emit grouped "Added" lines for breakdown families
    for root_id, info in orphan_by_root.items():
        amount = round(info["total_offer"], 2)
        if abs(amount) < 0.01:
            continue
        adjustments.append({
            "type": "added",
            "description": f"Added: {info['name']} (×{info['qty']})",
            "amount": amount,
        })

    # Emit direct orphans (top-level items added during ingest, not broken down)
    for curr in direct_orphans:
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
    if session["status"] not in ("received", "verified", "breakdown_complete", "partially_ingested"):
        raise ValueError(f"Cannot add items — session is '{session['status']}'")

    product_name = data.get("product_name")
    tcgplayer_id = data.get("tcgplayer_id")
    market_price = Decimal(str(data.get("market_price", 0)))
    quantity = int(data.get("quantity", 1))
    set_name = data.get("set_name")
    product_type = data.get("product_type", "sealed")
    condition = data.get("condition")
    card_number = data.get("card_number")
    rarity = data.get("rarity")
    variant = data.get("variant")
    is_graded = bool(data.get("is_graded", False))
    grade_company = (data.get("grade_company") or "").strip() or None
    grade_value = (data.get("grade_value") or "").strip() or None

    offer_pct = Decimal(str(session.get("offer_percentage", 65))) / 100
    offer_price = (market_price * offer_pct * quantity).quantize(Decimal("0.01"))

    item_id = str(uuid4())
    execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, set_name, tcgplayer_id,
            quantity, market_price, offer_price, product_type,
            is_mapped, item_status, condition, card_number, rarity, variant,
            is_graded, grade_company, grade_value
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        item_id, session_id, product_name, set_name,
        int(tcgplayer_id) if tcgplayer_id else None,
        quantity, market_price, offer_price, product_type,
        tcgplayer_id is not None, "good",
        condition, card_number, rarity, variant,
        is_graded, grade_company, grade_value,
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
