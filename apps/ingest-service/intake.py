"""
Intake business logic.
# v2.2 — added update_item_condition, search-based card entry

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

logger = logging.getLogger(__name__)


# ==========================================
# PRODUCT MAPPING (collectr_name <-> tcgplayer_id)
# ==========================================

def _norm_num(card_number: str) -> str:
    """Canonical card-number form for set-insensitive matching: upper, no spaces.
    '079/73' stays '079/73'; 'SWSH 260' -> 'SWSH260'; '92a/145' -> '92A/145'."""
    return (card_number or "").strip().upper().replace(" ", "")


def _norm_var(variance: str) -> str:
    """Canonical variance: case-folded, empty -> 'normal' (HTML imports leave it
    blank where CSV writes 'Normal'; both must compare equal)."""
    v = (variance or "").strip().lower()
    return v or "normal"


def _single_card(rows) -> Optional[dict]:
    """Given candidate mapping rows, return the one card they all point at, or
    None if they point at two or more distinct cards (genuine same-number
    reprints — e.g. Charizard 4 in Base vs Base Set 2). A card is identified by
    tcgplayer_id when present, else by scrydex_id."""
    cards: dict = {}
    for r in rows:
        tcg = r.get("tcgplayer_id")
        sx = r.get("scrydex_id")
        if tcg is not None:
            key = ("T", tcg)
        elif sx:
            key = ("S", sx)
        else:
            continue
        cur = cards.get(key)
        # Keep the richest row for the card (one carrying both ids).
        if cur is None or (sx and not cur.get("scrydex_id")):
            cards[key] = {"tcgplayer_id": tcg, "scrydex_id": sx}
    if len(cards) == 1:
        return next(iter(cards.values()))
    return None


def _is_japanese_text(s: str) -> bool:
    """True if a Collectr name/set reads as Japanese — an explicit (JP)/(Japanese)
    marker or any non-ASCII (kana/kanji) character."""
    s = s or ""
    low = s.lower()
    if "(jp)" in low or "(japanese)" in low or "japanese" in low:
        return True
    return any(ord(ch) > 127 for ch in s)


def _sid_is_japanese(scrydex_id: Optional[str]) -> bool:
    """True if a scrydex_id is a Japanese printing (ids look like 'sv2a_ja-197')."""
    s = scrydex_id or ""
    return "_ja-" in s or s.endswith("_ja")


def _filter_same_language(cands, item_is_jp: bool):
    """Drop candidates whose language disagrees with the item. The set-insensitive
    tiers below match on name+number and would otherwise cross English↔Japanese —
    'SV: 151' (EN, sv3pt5-*) and 'Pokémon Card 151' (JP, sv2a_ja-*) share names and
    numbers but are distinct cards with distinct prices. Tier 1 (exact set) is
    language-safe already. A tcg-only / scrydex-less row counts as English."""
    return [c for c in cands if _sid_is_japanese(c.get("scrydex_id")) == item_is_jp]


def _bump_use(collectr_name: str, product_type: str, sn: str, cn: str, vr: str):
    execute("""
        UPDATE product_mappings
        SET use_count = use_count + 1, last_used = CURRENT_TIMESTAMP
        WHERE collectr_name = %s AND product_type = %s
          AND COALESCE(set_name, '') = %s
          AND COALESCE(card_number, '') = %s
          AND COALESCE(variance, '') = %s
    """, (collectr_name, product_type, sn, cn, vr))


def get_cached_link(collectr_name: str, product_type: str,
                    set_name: str = None, card_number: str = None,
                    variance: str = None) -> Optional[dict]:
    """Return {'tcgplayer_id': ..., 'scrydex_id': ...} for a previously-linked
    Collectr product, or None.

    Raw matching is two-tier:
      1. Exact (name, set, number, variance). The cache is keyed on the exact
         parser output (see map_item), so re-importing the same file round-trips
         100% — every card you just linked comes back linked.
      2. Set-insensitive fallback on name + number + variance, for cards whose
         Collectr set *spelling* drifted between exports within one language
         ('Crown Zenith: Galarian Gallery' vs no colon, 'SWSH09: Brilliant Stars'
         vs 'Brilliant Stars'). It resolves only when every candidate points at a
         single card; if the same name+number maps to two different cards (~1% —
         true same-number reprints), it abstains so the operator picks. It is
         language-aware: it never crosses English↔Japanese ('SV: 151' EN vs
         'Pokémon Card 151' JP are different cards), via _filter_same_language.

    Sealed matches on name+type only (sealed lacks set/number identity)."""
    sn = set_name or ""
    cn = card_number or ""
    vr = variance or ""

    if product_type == "raw" and (sn or cn or vr):
        # Tier 1 — exact key.
        row = query_one("""
            SELECT tcgplayer_id, scrydex_id FROM product_mappings
            WHERE collectr_name = %s AND product_type = 'raw'
              AND COALESCE(set_name, '') = %s
              AND COALESCE(card_number, '') = %s
              AND COALESCE(variance, '') = %s
        """, (collectr_name, sn, cn, vr))
        if row:
            _bump_use(collectr_name, "raw", sn, cn, vr)
            return {"tcgplayer_id": row["tcgplayer_id"], "scrydex_id": row.get("scrydex_id")}

        # Tiers 2 & 3 are number-anchored — only run them when we actually have
        # a card number to anchor on (without one, name/set alone is too loose).
        nnum = _norm_num(cn)
        nvar = _norm_var(vr)
        # Language of the item being matched, so the set-insensitive tiers below
        # never cross English↔Japanese (the bug that mapped EN 'SV: 151' cards
        # onto JP 'Pokémon Card 151' scrydex_ids).
        item_jp = _is_japanese_text(collectr_name) or _is_japanese_text(sn)
        if nnum:
            # Tier 2 — set-insensitive, but NOT language-insensitive (Collectr
            # renames the same SET across exports — colon/spacing drift — within
            # one language). Anchored on name + number + variance; abstains if
            # name+number is two different cards (same-number reprints).
            cands = query("""
                SELECT DISTINCT tcgplayer_id, scrydex_id FROM product_mappings
                WHERE collectr_name = %s AND product_type = 'raw'
                  AND upper(replace(COALESCE(card_number, ''), ' ', '')) = %s
                  AND lower(COALESCE(NULLIF(variance, ''), 'normal')) = %s
            """, (collectr_name, nnum, nvar))
            chosen = _single_card(_filter_same_language(cands, item_jp))
            if chosen:
                return chosen

            # Tier 3 — name-insensitive (Collectr renames the same CARD: e.g.
            # 'Charizard V' vs 'Charizard V (Full Art)' at 153/172, both one
            # card). Anchored on set + number + variance, so the regular and
            # Full Art printings stay distinct by NUMBER, and same-number chip
            # variants (Master Ball vs Poké Ball — same number, different cards)
            # trip the single-card guard and abstain to the operator.
            if sn:
                cands = query("""
                    SELECT DISTINCT tcgplayer_id, scrydex_id FROM product_mappings
                    WHERE product_type = 'raw'
                      AND COALESCE(set_name, '') = %s
                      AND upper(replace(COALESCE(card_number, ''), ' ', '')) = %s
                      AND lower(COALESCE(NULLIF(variance, ''), 'normal')) = %s
                """, (sn, nnum, nvar))
                chosen = _single_card(_filter_same_language(cands, item_jp))
                if chosen:
                    return chosen
        return None

    # Sealed, or raw without identifying info — match on name+type only.
    row = query_one("""
        SELECT tcgplayer_id, scrydex_id FROM product_mappings
        WHERE collectr_name = %s AND product_type = %s
    """, (collectr_name, product_type))
    if row:
        return {"tcgplayer_id": row["tcgplayer_id"], "scrydex_id": row.get("scrydex_id")}
    return None


def get_cached_mapping(collectr_name: str, product_type: str,
                       set_name: str = None, card_number: str = None,
                       variance: str = None) -> Optional[int]:
    """Back-compat shim — returns just the tcgplayer_id. New callers should use
    get_cached_link() so they also recover the scrydex_id (JP / Scrydex-only)."""
    link = get_cached_link(collectr_name, product_type,
                           set_name=set_name, card_number=card_number,
                           variance=variance)
    return link["tcgplayer_id"] if link else None


def save_mapping(collectr_name: str, tcgplayer_id: Optional[int], product_type: str,
                 set_name: str = None, card_number: str = None, variance: str = None,
                 scrydex_id: str = None,
                 shopify_product_id: int = None, shopify_product_name: str = None):
    """Save or update a product mapping for future imports.
    tcgplayer_id may be None when linking a Scrydex-only product (JP, no TCG
    marketplace mapping) — scrydex_id carries it instead.

    Uses (collectr_name, product_type, set_name, card_number, variance) as the
    composite key. For raw cards this MUST be keyed on the Collectr-parsed
    identity (what the parser emits), never the Scrydex display values — see
    map_item — or the next import of the same file won't find it.
    """
    sn = set_name or ""
    cn = card_number or ""
    vr = variance or ""
    sx = (scrydex_id or "").strip() or None
    execute("""
        INSERT INTO product_mappings
            (collectr_name, tcgplayer_id, scrydex_id, product_type, set_name, card_number, variance,
             shopify_product_id, shopify_product_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (collectr_name, product_type, COALESCE(set_name, ''), COALESCE(card_number, ''), COALESCE(variance, ''))
        DO UPDATE SET
            tcgplayer_id = COALESCE(EXCLUDED.tcgplayer_id, product_mappings.tcgplayer_id),
            scrydex_id = COALESCE(EXCLUDED.scrydex_id, product_mappings.scrydex_id),
            shopify_product_id = COALESCE(EXCLUDED.shopify_product_id, product_mappings.shopify_product_id),
            shopify_product_name = COALESCE(EXCLUDED.shopify_product_name, product_mappings.shopify_product_name),
            last_used = CURRENT_TIMESTAMP,
            use_count = product_mappings.use_count + 1
    """, (collectr_name, tcgplayer_id, sx, product_type, sn, cn, vr,
          shopify_product_id, shopify_product_name))


def get_cached_shopify_link(collectr_name: str, product_type: str) -> Optional[dict]:
    """Return cached Shopify product link (id + name) for a Collectr product name, if any."""
    row = query_one("""
        SELECT shopify_product_id, shopify_product_name, tcgplayer_id
        FROM product_mappings
        WHERE collectr_name = %s AND product_type = %s
          AND shopify_product_id IS NOT NULL
    """, (collectr_name, product_type))
    if row:
        return {
            "shopify_product_id": row["shopify_product_id"],
            "shopify_product_name": row["shopify_product_name"],
            "tcgplayer_id": row["tcgplayer_id"],
        }
    return None


# ==========================================
# INTAKE SESSION MANAGEMENT
# ==========================================

def create_session(customer_name: str, session_type: str,
                   offer_percentage: Decimal = None,
                   cash_percentage: Decimal = None,
                   credit_percentage: Decimal = None,
                   is_walk_in: bool = False,
                   file_name: str = None, file_hash: str = None,
                   employee_id: str = None, notes: str = None,
                   bulk_tiers: list[dict] = None) -> dict:
    """Create a new intake session. Returns the full session row.

    Cash/credit split (Phase 2):
      - Pass `cash_percentage` and/or `credit_percentage` to set the split
        directly. If neither is supplied, fall back to `offer_percentage`
        as cash (legacy single-offer behavior).
      - `offer_percentage` is mirrored from cash_percentage (or whichever
        legacy value was passed) so old readers still see something sane
        until they migrate to the split columns.
      - `is_walk_in` flags counter sessions; on accept the writer skips
        the offered → accepted → received pickup/mail interstitial and
        jumps straight to received.
      - `bulk_tiers` overrides the column DEFAULT (legacy <$2 → 25%); pass
        None to inherit the DB default.
    """
    import json as _json
    # Defense in depth: ensure we always have a cash percentage even when a
    # caller still passes only the legacy offer_percentage.
    cash_pct = cash_percentage if cash_percentage is not None else offer_percentage
    credit_pct = credit_percentage  # may legitimately be None on legacy callers
    legacy_pct = offer_percentage if offer_percentage is not None else cash_pct

    tiers_json = None
    if bulk_tiers is not None:
        normalized = _normalize_bulk_tiers(bulk_tiers)
        tiers_json = _json.dumps([
            {"max": float(t["max"]), "pct": float(t["pct"])} for t in normalized
        ])

    session_id = str(uuid4())
    if tiers_json is not None:
        return execute_returning("""
            INSERT INTO intake_sessions
                (id, customer_name, session_type, offer_percentage,
                 cash_percentage, credit_percentage, is_walk_in,
                 source_file_name, source_file_hash, employee_id, notes,
                 bulk_tiers)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING *
        """, (session_id, customer_name, session_type, legacy_pct,
              cash_pct, credit_pct, bool(is_walk_in),
              file_name, file_hash, employee_id, notes, tiers_json))
    # No tiers supplied → let the column DEFAULT supply the legacy bucket.
    return execute_returning("""
        INSERT INTO intake_sessions
            (id, customer_name, session_type, offer_percentage,
             cash_percentage, credit_percentage, is_walk_in,
             source_file_name, source_file_hash, employee_id, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (session_id, customer_name, session_type, legacy_pct,
          cash_pct, credit_pct, bool(is_walk_in),
          file_name, file_hash, employee_id, notes))


def get_session(session_id: str) -> Optional[dict]:
    """Get session by ID using the summary view."""
    return query_one("SELECT * FROM intake_session_summary WHERE id = %s", (session_id,))


def get_session_items(session_id: str) -> list[dict]:
    """Get all items in a session, unmapped items first, then in entry order.
    Preserving entry order matters for manual stack intake — the physical
    stack order needs to survive intake → ingest → routing."""
    return query("""
        SELECT * FROM intake_items
        WHERE session_id = %s
        ORDER BY is_mapped ASC, created_at ASC
    """, (session_id,))


def list_sessions(status: str = "in_progress", limit: int = 50) -> list[dict]:
    """List intake sessions by status. Accepts comma-separated statuses."""
    statuses = [s.strip() for s in status.split(",") if s.strip()]
    if len(statuses) == 1:
        return query("""
            SELECT * FROM intake_session_summary
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (statuses[0], limit))
    else:
        placeholders = ",".join(["%s"] * len(statuses))
        return query(f"""
            SELECT * FROM intake_session_summary
            WHERE status IN ({placeholders})
            ORDER BY created_at DESC
            LIMIT %s
        """, tuple(statuses) + (limit,))


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

def _backfill_scrydex_ids(session_id: str) -> int:
    """Fill in scrydex_id for any rows in this session that have a tcgplayer_id
    but no scrydex_id, by looking up scrydex_price_cache.

    All 5 INSERT INTO intake_items statements below omit scrydex_id from the
    column list (the column was added later but the INSERTs were never updated).
    Without this backfill, scrydex_id propagates as NULL into raw_cards, and
    anything that reads raw_cards.scrydex_id directly (kiosk detail lookups,
    dupe normalization, dependency-free pricing paths) breaks.

    Per-tcg DISTINCT ON because scrydex_price_cache has one row per condition;
    they all carry the same scrydex_id for a given tcgplayer_id."""
    return execute("""
        UPDATE intake_items i
           SET scrydex_id = sub.scrydex_id
          FROM (
            SELECT DISTINCT ON (tcgplayer_id) tcgplayer_id, scrydex_id
              FROM scrydex_price_cache
             ORDER BY tcgplayer_id, scrydex_id
          ) sub
         WHERE i.session_id = %s
           AND i.scrydex_id IS NULL
           AND i.tcgplayer_id IS NOT NULL
           AND sub.tcgplayer_id = i.tcgplayer_id
    """, (session_id,))


def add_items_to_session(session_id: str, items: list[dict]) -> int:
    """
    Batch-add items to an intake session.

    Each item dict should have:
        product_name, product_type, quantity, market_price, offer_price, unit_cost_basis
        Optional: tcgplayer_id, set_name, card_number, condition, rarity,
                  is_graded, grade_company, grade_value

    Returns number of items added.

    Imports preserve input order: `execute_batch` runs in one transaction, so
    PostgreSQL's CURRENT_TIMESTAMP default resolves to the same value for every
    row and the `get_session_items` ORDER BY created_at falls back to storage
    order (effectively random). Stagger created_at by index here so the
    operator's spreadsheet order is what staff see in the dashboard / verify /
    routing — exactly what the manual-entry path already gets for free.
    """
    from datetime import datetime, timedelta, timezone
    base_ts = datetime.now(timezone.utc)

    sql = """
        INSERT INTO intake_items
            (session_id, product_name, tcgplayer_id, scrydex_id, product_type,
             set_name, card_number, condition, rarity, variance,
             quantity, market_price, offer_price, unit_cost_basis, is_mapped,
             is_graded, grade_company, grade_value, slab_uuid,
             shopify_product_id, shopify_product_name, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params_list = [
        (
            session_id,
            item["product_name"],
            item.get("tcgplayer_id"),
            (item.get("scrydex_id") or None),
            item["product_type"],
            item.get("set_name"),
            item.get("card_number"),
            item.get("condition"),
            item.get("rarity"),
            item.get("variance") or "",
            item["quantity"],
            item["market_price"],
            item["offer_price"],
            item["unit_cost_basis"],
            # is_mapped: true if we have a TCGplayer ID, a Scrydex-only link, or a shopify link
            (item.get("tcgplayer_id") is not None
             or bool(item.get("scrydex_id"))
             or item.get("shopify_product_id") is not None),
            item.get("is_graded", False),
            item.get("grade_company") or None,
            item.get("grade_value") or None,
            item.get("slab_uuid") or None,
            item.get("shopify_product_id") or None,
            item.get("shopify_product_name") or None,
            base_ts + timedelta(microseconds=idx),
        )
        for idx, item in enumerate(items)
    ]
    n = execute_many_batch(sql, params_list)
    _backfill_scrydex_ids(session_id)
    return n


def add_single_raw_item(session_id: str, product_name: str, tcgplayer_id,
                         set_name: str, card_number: str, condition: str,
                         rarity: str, quantity: int, market_price: Decimal,
                         offer_percentage: Decimal,
                         is_graded: bool = False, grade_company: str = "",
                         grade_value: str = "",
                         variance: str = "",
                         game: str = None,
                         scrydex_id: str = None,
                         bulk_tiers: list[dict] = None) -> dict:
    """
    Add a single raw card item to a session (manual entry flow).

    tcgplayer_id may be None for cards Scrydex doesn't track in TCGplayer's
    marketplace (Scrydex-only JP, MTG PEOE promos). Such picks still carry a
    scrydex_id from search — persist it so the row is mapped without a TCG
    link. is_mapped is true when either identifier is present.

    `game` is the operator-selected game for manual entries — required so bin
    routing in ingestion doesn't silently default unmapped cards to Pokemon.
    For mapped (tcgplayer_id-bearing) entries it's still respected but
    ingestion will prefer the looked-up game from Scrydex/PPT when available.

    Inserts one row with quantity=N — matches the CSV/Collectr/HTML import
    paths. Identity (per-copy barcode) is minted at finalize in ingestion's
    push-to-raw_cards loop. Verify/breakdown stages already split qty>1 rows
    on demand (split_damaged, breakdown split), so a single row is sufficient.

    Calculates offer_price and unit_cost_basis from the given offer_percentage.
    Returns the created intake_item row.
    """
    qty = max(1, int(quantity))
    if bulk_tiers is None:
        sess = query_one(
            "SELECT bulk_tiers FROM intake_sessions WHERE id = %s",
            (session_id,),
        )
        bulk_tiers = _session_bulk_tiers(sess)
    offer_total, unit_cost = calc_offer_price(
        market_price, qty, offer_percentage, product_type="raw",
        bulk_tiers=bulk_tiers)

    sx = (scrydex_id or "").strip() or None
    is_mapped = (tcgplayer_id is not None) or (sx is not None)
    row = execute_returning("""
        INSERT INTO intake_items
            (session_id, product_name, tcgplayer_id, scrydex_id, product_type,
             set_name, card_number, condition, rarity, variance,
             quantity, market_price, offer_price, unit_cost_basis, is_mapped,
             is_graded, grade_company, grade_value, game)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (session_id, product_name, tcgplayer_id, sx, "raw",
          set_name, card_number, condition, rarity, variance or "",
          qty, market_price, offer_total, unit_cost, is_mapped,
          is_graded, grade_company or None, grade_value or None,
          (game or "").strip().lower() or None))
    _backfill_scrydex_ids(session_id)
    return row


# ==========================================
# MAPPING ITEMS TO TCGPLAYER IDS
# ==========================================

def map_item(item_id: str, tcgplayer_id: int = None,
             new_market_price: Decimal = None,
             product_name: str = None, set_name: str = None,
             card_number: str = None, rarity: str = None,
             variance: str = None,
             scrydex_id: str = None) -> dict:
    """
    Map an intake item to a Scrydex product (preferred) and/or a TCGplayer ID.

    scrydex_id is the canonical primary identifier — sealed products and
    most JP cards have no TCGplayer marketplace mapping in Scrydex's data,
    but they all have a scrydex_id. Pass it directly to link those.

    tcgplayer_id is a property used by the price_updater to crawl TCGplayer
    for current low prices; it isn't required for linking. When both are
    supplied, the scrydex↔tcg map is also persisted so future searches
    show this product as having a TCG mapping.

    Optionally updates market price + identification fields. Recalculates
    offer_price based on session's offer_percentage. Returns updated row.
    """
    if not tcgplayer_id and not scrydex_id:
        raise ValueError("map_item requires tcgplayer_id or scrydex_id")

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError(f"Item {item_id} not found")

    session = query_one(
        "SELECT offer_percentage, bulk_tiers FROM intake_sessions WHERE id = %s",
        (item["session_id"],)
    )
    if not session:
        raise ValueError(f"Session for item {item_id} not found")

    market_price = new_market_price if new_market_price is not None else item["market_price"]
    offer_pct = session["offer_percentage"]
    offer_price, unit_cost_basis = calc_offer_price(
        market_price, item["quantity"], offer_pct,
        product_type=item.get("product_type", "raw"),
        bulk_tiers=_session_bulk_tiers(session))

    name = product_name if product_name else item["product_name"]
    sname = set_name if set_name else item.get("set_name")
    cnum = card_number if card_number else item.get("card_number")
    rar = rarity if rarity else item.get("rarity")
    var = variance if variance else item.get("variance") or ""

    updated = execute_returning("""
        UPDATE intake_items
        SET tcgplayer_id = COALESCE(%s, tcgplayer_id),
            scrydex_id   = COALESCE(%s, scrydex_id),
            market_price = %s, offer_price = %s,
            unit_cost_basis = %s, is_mapped = TRUE,
            product_name = %s, set_name = %s, card_number = %s, rarity = %s,
            variance = %s
        WHERE id = %s
        RETURNING *
    """, (tcgplayer_id, scrydex_id, market_price, offer_price, unit_cost_basis,
          name, sname, cnum, rar, var, item_id))

    # Cache the mapping for future imports. tcgplayer_id may be None for
    # Scrydex-only links (sealed JP, older JP cards) — save_mapping accepts
    # NULL and the row still helps next time the same Collectr name appears.
    #
    # CRITICAL: for raw cards the cache key must be the ORIGINAL Collectr-parsed
    # identity (`item[...]`, the pre-update row = exactly what the parser emits),
    # NOT the Scrydex display values the operator just picked (`name`/`sname`/
    # `cnum`/`var`). Re-importing the same file reproduces the parser identity,
    # so keying on it is what makes a re-import come back 100% linked. Keying on
    # the Scrydex set name ('SWSH09: Brilliant Stars') instead of Collectr's
    # ('Brilliant Stars') was the bug that stranded every relinked raw card.
    # Sealed keeps the existing behavior (name-only match, already reliable).
    link_sx = scrydex_id or item.get("scrydex_id")
    if item["product_type"] == "raw":
        save_mapping(
            item["product_name"], tcgplayer_id, "raw",
            item.get("set_name"), item.get("card_number"),
            variance=item.get("variance") or "",
            scrydex_id=link_sx,
        )
    else:
        save_mapping(
            name, tcgplayer_id, item["product_type"],
            sname, cnum, variance=var, scrydex_id=link_sx,
        )

    # Recalculate session totals
    _recalculate_session_totals(item["session_id"])

    return updated


def _recalculate_session_totals(session_id: str):
    """Recalculate total_market_value and total_offer_amount for a session.
    Only includes items with item_status 'good' or 'damaged'."""
    execute("""
        UPDATE intake_sessions SET
            total_market_value = (
                SELECT COALESCE(SUM(market_price * quantity), 0)
                FROM intake_items 
                WHERE session_id = %s AND item_status IN ('good', 'damaged')
            ),
            total_offer_amount = (
                SELECT COALESCE(SUM(offer_price), 0)
                FROM intake_items 
                WHERE session_id = %s AND item_status IN ('good', 'damaged')
            )
        WHERE id = %s
    """, (session_id, session_id, session_id))


def update_offer_percentage(session_id: str, new_percentage: Decimal) -> dict:
    """Legacy single-percentage update — still used by older flows that
    haven't been split yet. Internally this just updates `cash_percentage`
    (and the mirrored `offer_percentage`); credit is left untouched so a
    pre-existing credit number doesn't get clobbered.
    """
    return update_session_percentages(session_id, cash_pct=new_percentage)


def update_session_percentages(session_id: str,
                                cash_pct: Decimal = None,
                                credit_pct: Decimal = None) -> dict:
    """Update one or both percentages on a session and recalculate item
    offer prices using the cash percentage (since that's what
    `intake_items.offer_price` currently represents until a customer picks
    an offer type). Per-item bulk rule (raw < $2 → 25%) still applies.

    Either argument may be None to leave that side alone — useful when the
    UI saves the two inputs independently.
    """
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")
    if session["status"] in ("cancelled", "rejected", "received", "partially_ingested", "ingested", "finalized"):
        raise ValueError("Cannot change offer % on this session")

    # Build a dynamic UPDATE so unspecified columns aren't overwritten
    sets = []
    params = []
    if cash_pct is not None:
        sets.append("cash_percentage = %s")
        params.append(cash_pct)
        # Mirror to legacy column so any reader still on offer_percentage
        # sees the active cash number rather than a stale value.
        sets.append("offer_percentage = %s")
        params.append(cash_pct)
    if credit_pct is not None:
        sets.append("credit_percentage = %s")
        params.append(credit_pct)
    if sets:
        params.append(session_id)
        execute(f"UPDATE intake_sessions SET {', '.join(sets)} WHERE id = %s", tuple(params))

    # Recalculate item-level offer_price using cash (the operational baseline).
    # Until the customer commits to an offer type, item rows stay denominated
    # in cash; total_offer_amount also stays cash-based. The frontend
    # additionally renders a credit projection from session.credit_percentage.
    effective_cash = cash_pct if cash_pct is not None else session.get("cash_percentage")
    if effective_cash is None:
        effective_cash = session.get("offer_percentage")  # last-resort legacy fallback
    if effective_cash is not None:
        _recalc_session_item_prices(session_id, effective_cash)

    _recalculate_session_totals(session_id)
    return get_session(session_id)


def _recalc_session_item_prices(session_id: str, base_pct: Decimal):
    """Re-price every active item in the session using `base_pct` and the
    session's bulk_tiers. Replaces the legacy inline CASE WHEN SQL — with
    variable tiers a single SQL statement gets gnarly fast, so the loop
    here mirrors `calc_offer_price` and updates in batch. Honors the
    damaged 0.88 multiplier; unit_cost_basis tracks per-unit COGS."""
    session = query_one(
        "SELECT bulk_tiers FROM intake_sessions WHERE id = %s",
        (session_id,),
    )
    tiers = _session_bulk_tiers(session)
    rows = query("""
        SELECT id, product_type, market_price, quantity, item_status
        FROM intake_items
        WHERE session_id = %s
    """, (session_id,))
    updates = []
    for r in rows:
        is_damaged = (r.get("item_status") == "damaged")
        offer_price, unit_cost = calc_offer_price(
            r["market_price"], r["quantity"], base_pct,
            product_type=r.get("product_type", "raw"),
            is_damaged=is_damaged,
            bulk_tiers=tiers,
        )
        updates.append((offer_price, unit_cost, r["id"]))
    if updates:
        execute_many_batch(
            "UPDATE intake_items SET offer_price = %s, unit_cost_basis = %s WHERE id = %s",
            updates,
        )


def update_session_bulk_tiers(session_id: str, tiers: list[dict]) -> dict:
    """Update the per-session pricing tiers and re-price every item.
    Tiers are normalized (sorted ascending, bad rows dropped, capped at 3).
    Empty/None resets to the legacy default `[{max:2, pct:25}]`."""
    import json as _json
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")
    if session["status"] in ("cancelled", "rejected", "received",
                              "partially_ingested", "ingested", "finalized"):
        raise ValueError("Cannot change bulk tiers on this session")

    normalized = _normalize_bulk_tiers(tiers)
    tiers_json = _json.dumps([
        {"max": float(t["max"]), "pct": float(t["pct"])} for t in normalized
    ])
    execute(
        "UPDATE intake_sessions SET bulk_tiers = %s::jsonb WHERE id = %s",
        (tiers_json, session_id),
    )

    effective_cash = session.get("cash_percentage") or session.get("offer_percentage")
    if effective_cash is not None:
        _recalc_session_item_prices(session_id, effective_cash)

    _recalculate_session_totals(session_id)
    return get_session(session_id)


def set_walk_in(session_id: str, is_walk_in: bool) -> dict:
    """Flip the walk-in flag on a session. Walk-in sessions short-circuit
    the offered → accepted → received flow on accept (customer is already
    at the counter with the cards)."""
    execute(
        "UPDATE intake_sessions SET is_walk_in = %s WHERE id = %s",
        (bool(is_walk_in), session_id),
    )
    return get_session(session_id)


def compute_offer_totals(session_id: str) -> dict:
    """Compute live cash and credit totals for the session's active items
    (good + damaged), honoring the bulk-raw $2 floor on cash. Returns
    `{"cash": Decimal, "credit": Decimal}`. Used for the dual-projection
    UI before a customer commits to an offer type.

    Note: credit also respects the bulk-raw rule — bulk cards still pay
    25% in credit, same as cash. That mirrors how the original
    single-offer flow treated bulk regardless of percentage.
    """
    session = query_one(
        "SELECT cash_percentage, credit_percentage, offer_percentage, bulk_tiers "
        "FROM intake_sessions WHERE id = %s",
        (session_id,),
    )
    if not session:
        return {"cash": Decimal("0"), "credit": Decimal("0")}

    cash_pct = session["cash_percentage"] or session["offer_percentage"] or Decimal("0")
    credit_pct = session["credit_percentage"] or Decimal("0")
    tiers = _session_bulk_tiers(session)

    rows = query("""
        SELECT product_type, quantity, market_price, item_status
        FROM intake_items
        WHERE session_id = %s
    """, (session_id,))

    cash_total = Decimal("0")
    credit_total = Decimal("0")
    for r in rows:
        if r.get("item_status", "good") not in ("good", "damaged"):
            continue
        is_damaged = r.get("item_status") == "damaged"
        cash_offer, _ = calc_offer_price(
            r["market_price"], r["quantity"], cash_pct,
            product_type=r.get("product_type", "raw"),
            is_damaged=is_damaged, bulk_tiers=tiers)
        cash_total += cash_offer
        if credit_pct > 0:
            credit_offer, _ = calc_offer_price(
                r["market_price"], r["quantity"], credit_pct,
                product_type=r.get("product_type", "raw"),
                is_damaged=is_damaged, bulk_tiers=tiers)
            credit_total += credit_offer
    return {"cash": cash_total, "credit": credit_total}


def accept_offer(session_id: str, offer_type: str,
                 fulfillment: str = "pickup",
                 tracking_number: str = None,
                 pickup_date: str = None) -> dict:
    """Customer accepted one of the two offers.

    - `offer_type` must be 'cash' or 'credit'
    - Recomputes item offer_price using the chosen percentage
    - Stamps `accepted_offer_type` and `total_offer_amount`
    - For walk-in sessions, jumps status straight to 'received' (no
      pickup/mail interstitial — the customer is at the counter)
    - For mail/pickup sessions, behaves like the legacy accept (offered →
      accepted, awaits a separate /receive call when product arrives)

    Returns the updated session row.
    """
    if offer_type not in ("cash", "credit"):
        raise ValueError("offer_type must be 'cash' or 'credit'")

    session = query_one(
        "SELECT * FROM intake_sessions WHERE id = %s",
        (session_id,),
    )
    if not session:
        raise ValueError("Session not found")
    if session["status"] not in ("offered",):
        raise ValueError(f"Cannot accept — session is '{session['status']}'")

    if offer_type == "cash":
        chosen_pct = session["cash_percentage"] or session["offer_percentage"]
    else:
        chosen_pct = session["credit_percentage"]
    if chosen_pct is None:
        raise ValueError(f"Session has no {offer_type}_percentage set")

    # Re-price every active item at the accepted percentage so the rest of
    # the pipeline (received_items_snapshot, _finalize_*) sees the right
    # numbers. Per-session bulk tiers still apply. unit_cost_basis tracks
    # the per-unit cost the rest of the pipeline reads as COGS.
    _recalc_session_item_prices(session_id, chosen_pct)

    is_walk_in = bool(session.get("is_walk_in"))
    new_status = "received" if is_walk_in else "accepted"

    if is_walk_in:
        # Snapshot items at receive-time exactly like the regular receive
        # endpoint does, so partial-ingest reconciliation downstream sees
        # consistent shape regardless of fulfillment path.
        items = query(
            "SELECT * FROM intake_items WHERE session_id = %s",
            (session_id,),
        )
        import json as _json
        snapshot = _json.dumps([{
            "id": str(i["id"]),
            "product_name": i.get("product_name"),
            "tcgplayer_id": i.get("tcgplayer_id"),
            "quantity": i.get("quantity", 1),
            "market_price": float(i.get("market_price", 0)),
            "offer_price": float(i.get("offer_price", 0)),
            "item_status": i.get("item_status", "good"),
        } for i in items if i.get("item_status") in ("good", "damaged")])

        execute("""
            UPDATE intake_sessions
            SET status = 'received',
                accepted_offer_type = %s,
                accepted_at = CURRENT_TIMESTAMP,
                received_at = CURRENT_TIMESTAMP,
                fulfillment_method = COALESCE(%s, fulfillment_method, 'pickup'),
                tracking_number = COALESCE(%s, tracking_number),
                pickup_date = COALESCE(%s, pickup_date),
                received_items_snapshot = %s,
                original_offer_amount = total_offer_amount
            WHERE id = %s
        """, (offer_type, fulfillment, tracking_number, pickup_date,
              snapshot, session_id))
    else:
        execute("""
            UPDATE intake_sessions
            SET status = 'accepted',
                accepted_offer_type = %s,
                accepted_at = CURRENT_TIMESTAMP,
                fulfillment_method = %s,
                tracking_number = %s,
                pickup_date = %s
            WHERE id = %s
        """, (offer_type, fulfillment, tracking_number, pickup_date, session_id))

    _recalculate_session_totals(session_id)
    return get_session(session_id)


def update_item_price(item_id: str, new_market_price: Decimal, session_id: str) -> dict:
    """Update an item's market price and recalculate its offer price."""
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")

    offer_pct = session["offer_percentage"]
    tiers = _session_bulk_tiers(session)

    item = query_one("SELECT quantity, product_type FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    offer_price, _ = calc_offer_price(
        new_market_price, item["quantity"], offer_pct,
        product_type=item.get("product_type", "raw"),
        bulk_tiers=tiers)

    execute("""
        UPDATE intake_items
        SET market_price = %s, offer_price = %s
        WHERE id = %s
    """, (new_market_price, offer_price, item_id))

    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


# ==========================================
# SESSION CANCELLATION
# ==========================================

def cancel_session(session_id: str, reason: str = None) -> dict:
    """Cancel an intake session. No inventory is created."""
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")
    if session["status"] == "finalized":
        raise ValueError("Cannot cancel a finalized session")

    execute("""
        UPDATE intake_sessions 
        SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP, cancel_reason = %s
        WHERE id = %s
    """, (reason, session_id))

    return get_session(session_id)


def rejuvenate_session(session_id: str) -> dict:
    """Restore a cancelled/rejected session back to 'in_progress' status."""
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")
    if session["status"] not in ("cancelled", "rejected"):
        raise ValueError(f"Only cancelled or rejected sessions can be rejuvenated (current: {session['status']})")

    execute("""
        UPDATE intake_sessions
        SET status = 'in_progress',
            cancelled_at = NULL,
            cancel_reason = NULL
        WHERE id = %s
    """, (session_id,))

    return get_session(session_id)


def merge_sessions(target_session_id: str, source_session_id: str) -> dict:
    """Merge source session into target. Combines duplicate items, moves the rest, cancels source."""
    target = get_session(target_session_id)
    source = get_session(source_session_id)
    if not target:
        raise ValueError("Target session not found")
    if not source:
        raise ValueError("Source session not found")
    if target["status"] != "in_progress":
        raise ValueError(f"Target session must be in_progress (currently: {target['status']})")
    if source["status"] != "in_progress":
        raise ValueError(f"Source session must be in_progress (currently: {source['status']})")

    # Get items from both sessions
    target_items = query(
        "SELECT id, tcgplayer_id, condition, product_type, quantity, market_price, offer_price "
        "FROM intake_items WHERE session_id = %s", (target_session_id,))
    source_items = query(
        "SELECT id, tcgplayer_id, condition, product_type, quantity, market_price, offer_price "
        "FROM intake_items WHERE session_id = %s", (source_session_id,))

    # Build lookup of target items by (tcgplayer_id, condition, product_type)
    target_lookup = {}
    for item in target_items:
        if item["tcgplayer_id"]:
            key = (item["tcgplayer_id"], item["condition"], item["product_type"])
            target_lookup[key] = item

    merged_count = 0
    moved_count = 0

    for src_item in source_items:
        key = (src_item["tcgplayer_id"], src_item["condition"], src_item["product_type"]) if src_item["tcgplayer_id"] else None
        match = target_lookup.get(key) if key else None

        if match:
            # Duplicate — combine quantities and sum offer prices
            new_qty = match["quantity"] + src_item["quantity"]
            new_offer = match["offer_price"] + src_item["offer_price"]
            execute(
                "UPDATE intake_items SET quantity = %s, offer_price = %s WHERE id = %s",
                (new_qty, new_offer, match["id"]))
            execute("DELETE FROM intake_items WHERE id = %s", (src_item["id"],))
            match["quantity"] = new_qty
            match["offer_price"] = new_offer
            merged_count += 1
        else:
            # No match — move to target session
            execute(
                "UPDATE intake_items SET session_id = %s WHERE id = %s",
                (target_session_id, src_item["id"]))
            moved_count += 1

    _recalculate_session_totals(target_session_id)

    # Cancel the now-empty source session
    execute(
        "UPDATE intake_sessions SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP, "
        "cancel_reason = 'Merged into session ' || %s WHERE id = %s",
        (target_session_id[:8], source_session_id))

    result = get_session(target_session_id)
    result["merge_stats"] = {"merged": merged_count, "moved": moved_count}
    return result


# ==========================================
# ITEM STATUS CHANGES (damage, missing, rejected)
# ==========================================

DAMAGE_DISCOUNT = Decimal("0.88")  # 88% of offer price for damaged items
DEFAULT_BULK_TIERS = [{"max": Decimal("2"), "pct": Decimal("25")}]
# Per-session bulk pricing tiers: an ascending-by-max list of dollar buckets
# that override the session's per-item percentage for raw cards. The default
# preserves the legacy "raw < $2 → 25%" rule; sessions can extend it to up to
# 3 tiers (e.g. 0% under $1, 25% under $2, 50% under $5).
MAX_BULK_TIERS = 3


def _normalize_bulk_tiers(tiers) -> list[dict]:
    """Coerce a raw tiers payload (from JSON, the DB, or a request) into a
    sorted list of {"max": Decimal, "pct": Decimal} entries. Drops bad
    rows silently; caps at MAX_BULK_TIERS; returns the default when input
    is empty/missing so callers can rely on at least the legacy bucket.
    """
    if not tiers:
        return [dict(t) for t in DEFAULT_BULK_TIERS]
    out = []
    for t in tiers:
        try:
            mx = Decimal(str(t.get("max")))
            pc = Decimal(str(t.get("pct")))
        except (TypeError, ValueError, KeyError, AttributeError):
            continue
        if mx <= 0 or pc < 0 or pc > 100:
            continue
        out.append({"max": mx, "pct": pc})
    if not out:
        return [dict(t) for t in DEFAULT_BULK_TIERS]
    out.sort(key=lambda x: x["max"])
    return out[:MAX_BULK_TIERS]


def _session_bulk_tiers(session) -> list[dict]:
    """Pull bulk_tiers off a session dict and normalize. Tolerates rows
    that predate the column (NULL → default)."""
    if not session:
        return [dict(t) for t in DEFAULT_BULK_TIERS]
    return _normalize_bulk_tiers(session.get("bulk_tiers"))


def calc_offer_price(market_price: Decimal, quantity: int, offer_pct: Decimal,
                     product_type: str = "raw", is_damaged: bool = False,
                     bulk_tiers: list[dict] = None) -> tuple:
    """
    Calculate offer_price and unit_cost_basis for an item.

    For raw cards, walks `bulk_tiers` (ascending by max) and uses the
    first tier's pct whose `max` exceeds market_price. Above the top
    tier, falls back to the session's `offer_pct`. When tiers is None,
    falls back to DEFAULT_BULK_TIERS (the legacy "<$2 → 25%" rule).

    Returns (offer_price, unit_cost_basis).
    """
    if product_type == "raw":
        tiers = _normalize_bulk_tiers(bulk_tiers)
        effective_pct = offer_pct
        for t in tiers:
            if market_price < t["max"]:
                effective_pct = t["pct"]
                break
    else:
        effective_pct = offer_pct

    discount = DAMAGE_DISCOUNT if is_damaged else Decimal("1")
    offer_price = market_price * quantity * (effective_pct / Decimal("100")) * discount
    unit_cost_basis = offer_price / quantity if quantity > 0 else Decimal("0")
    return offer_price, unit_cost_basis


def split_damaged(item_id: str, damaged_qty: int) -> dict:
    """
    Split a line item into good + damaged.
    
    - Original item: qty decremented by damaged_qty
    - New item: qty = damaged_qty, item_status = 'damaged', 
      offer = 85% of original per-unit offer
    
    If damaged_qty == original qty, just flips the item to damaged (no split).
    """
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session = get_session(item["session_id"])
    if not session:
        raise ValueError("Session not found")

    original_qty = item["quantity"]
    if damaged_qty < 1 or damaged_qty > original_qty:
        raise ValueError(f"damaged_qty must be between 1 and {original_qty}")

    offer_pct = session["offer_percentage"]
    per_unit_market = item["market_price"]
    _, per_unit_offer = calc_offer_price(
        per_unit_market, 1, offer_pct,
        product_type=item.get("product_type", "raw"),
        bulk_tiers=_session_bulk_tiers(session))
    damaged_per_unit_offer = per_unit_offer * DAMAGE_DISCOUNT

    if damaged_qty == original_qty:
        # Flip entire item to damaged — no split needed
        damaged_offer_total = damaged_per_unit_offer * original_qty
        execute("""
            UPDATE intake_items 
            SET item_status = 'damaged',
                listing_condition = 'Damaged',
                offer_price = %s,
                unit_cost_basis = %s
            WHERE id = %s
        """, (damaged_offer_total, damaged_per_unit_offer, item_id))
    else:
        # Reduce original item quantity
        good_qty = original_qty - damaged_qty
        good_offer_total = per_unit_offer * good_qty
        execute("""
            UPDATE intake_items 
            SET quantity = %s, offer_price = %s
            WHERE id = %s
        """, (good_qty, good_offer_total, item_id))

        # Create new damaged line item
        damaged_offer_total = damaged_per_unit_offer * damaged_qty
        new_id = str(uuid4())
        execute("""
            INSERT INTO intake_items (
                id, session_id, product_name, tcgplayer_id, product_type,
                set_name, card_number, condition, rarity,
                quantity, market_price, offer_price, unit_cost_basis,
                is_mapped, item_status, listing_condition, parent_item_id
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, 'damaged', 'Damaged', %s
            )
        """, (
            new_id, item["session_id"], item["product_name"], item["tcgplayer_id"],
            item["product_type"], item["set_name"], item["card_number"],
            item["condition"], item["rarity"],
            damaged_qty, per_unit_market, damaged_offer_total, damaged_per_unit_offer,
            item["is_mapped"], item_id,
        ))

    _backfill_scrydex_ids(item["session_id"])
    _recalculate_session_totals(item["session_id"])
    return {
        "original_item": query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,)),
        "session": get_session(item["session_id"]),
    }


def mark_item_missing(item_id: str) -> dict:
    """Mark an item as missing — excluded from totals and payment."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    execute("""
        UPDATE intake_items SET item_status = 'missing' WHERE id = %s
    """, (item_id,))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def mark_item_damaged(item_id: str) -> dict:
    """Mark an item as damaged — stays in totals but flagged."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    execute("""
        UPDATE intake_items SET item_status = 'damaged' WHERE id = %s
    """, (item_id,))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def mark_item_rejected(item_id: str) -> dict:
    """Mark an item as rejected — seller kept it, excluded from totals."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    execute("""
        UPDATE intake_items SET item_status = 'rejected' WHERE id = %s
    """, (item_id,))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def restore_item(item_id: str) -> dict:
    """Restore a missing/rejected/damaged item back to 'good'."""
    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session = get_session(item["session_id"])
    offer_pct = session["offer_percentage"]

    # Recalculate offer at normal rate
    offer_price, _ = calc_offer_price(
        item["market_price"], item["quantity"], offer_pct,
        product_type=item.get("product_type", "raw"),
        bulk_tiers=_session_bulk_tiers(session))

    execute("""
        UPDATE intake_items 
        SET item_status = 'good', listing_condition = 'NM', offer_price = %s
        WHERE id = %s
    """, (offer_price, item_id))

    _recalculate_session_totals(item["session_id"])
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def get_item(item_id: str) -> dict | None:
    """Fetch a single intake item by ID."""
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def clone_item_with_overrides(source_item_id: str, session_id: str,
                               quantity: int, market_price: Decimal, notes: str) -> dict:
    """
    Clone an intake item with overridden quantity and market price.
    Used when splitting a partial breakdown from a multi-unit item.
    """
    import uuid
    source = query_one("SELECT * FROM intake_items WHERE id = %s", (source_item_id,))
    if not source:
        raise ValueError("Source item not found")

    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")

    offer_pct = session["offer_percentage"]
    is_damaged = source.get("item_status") == "damaged"
    offer_price, unit_cost_basis = calc_offer_price(
        market_price, quantity, offer_pct,
        product_type=source.get("product_type", "raw"),
        is_damaged=is_damaged,
        bulk_tiers=_session_bulk_tiers(session))

    new_id = str(uuid.uuid4())
    execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, tcgplayer_id, product_type,
            set_name, quantity, market_price, offer_price, unit_cost_basis,
            is_mapped, item_status, listing_condition, price_override_note
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        new_id, session_id,
        source["product_name"], source.get("tcgplayer_id"), source.get("product_type"),
        source.get("set_name"), quantity, market_price, offer_price,
        unit_cost_basis,
        source.get("is_mapped", False), source.get("item_status", "good"),
        source.get("listing_condition", "NM"), notes
    ))

    _backfill_scrydex_ids(session_id)
    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (new_id,))


def override_item_price(item_id: str, new_price: Decimal, note: str, session_id: str) -> dict:
    """Override an item's market price with a reason note."""
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    offer_pct = session["offer_percentage"]
    offer_price, _ = calc_offer_price(
        new_price, item["quantity"], offer_pct,
        product_type=item.get("product_type", "raw"),
        is_damaged=item.get("item_status") == "damaged",
        bulk_tiers=_session_bulk_tiers(session))

    execute("""
        UPDATE intake_items
        SET market_price = %s, offer_price = %s, price_override_note = %s
        WHERE id = %s
    """, (new_price, offer_price, note, item_id))

    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def delete_item(item_id: str) -> dict:
    """Permanently remove an item from a session."""
    item = query_one("SELECT session_id FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    session_id = item["session_id"]
    
    # Also delete any child items (damage splits)
    execute("DELETE FROM intake_items WHERE parent_item_id = %s", (item_id,))
    execute("DELETE FROM intake_items WHERE id = %s", (item_id,))

    _recalculate_session_totals(session_id)
    return get_session(session_id)


def update_item_quantity(item_id: str, new_qty: int, session_id: str) -> dict:
    """Update an item's quantity and recalculate offer price."""
    if new_qty < 1:
        raise ValueError("Quantity must be at least 1")

    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    offer_pct = session["offer_percentage"]
    offer_price, _ = calc_offer_price(
        item["market_price"], new_qty, offer_pct,
        product_type=item.get("product_type", "raw"),
        is_damaged=item.get("item_status") == "damaged",
        bulk_tiers=_session_bulk_tiers(session))

    execute("""
        UPDATE intake_items
        SET quantity = %s, offer_price = %s
        WHERE id = %s
    """, (new_qty, offer_price, item_id))

    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def update_item_condition(item_id: str, new_condition: str, session_id: str) -> dict:
    """Update an item's condition."""
    valid = ('NM', 'LP', 'MP', 'HP', 'DMG')
    new_condition = new_condition.upper().strip()
    if new_condition not in valid:
        raise ValueError(f"Invalid condition: {new_condition}. Must be one of {valid}")

    item = query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))
    if not item:
        raise ValueError("Item not found")

    execute("""
        UPDATE intake_items
        SET condition = %s
        WHERE id = %s
    """, (new_condition, item_id))

    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


def add_sealed_item(session_id: str, product_name: str, tcgplayer_id: int = None,
                    market_price: Decimal = Decimal("0"), quantity: int = 1,
                    set_name: str = None) -> dict:
    """Add a sealed item to an existing session (manual add during a buy)."""
    session = get_session(session_id)
    if not session:
        raise ValueError("Session not found")
    if session["status"] in ("cancelled", "rejected", "received", "partially_ingested", "ingested", "finalized"):
        raise ValueError("Cannot add items to this session")

    offer_pct = session["offer_percentage"]
    offer_price, unit_cost = calc_offer_price(
        market_price, quantity, offer_pct, product_type="sealed")
    is_mapped = tcgplayer_id is not None

    item_id = str(uuid4())
    execute("""
        INSERT INTO intake_items (
            id, session_id, product_name, tcgplayer_id, product_type,
            set_name, quantity, market_price, offer_price, unit_cost_basis,
            is_mapped, item_status, listing_condition
        ) VALUES (%s, %s, %s, %s, 'sealed', %s, %s, %s, %s, %s, %s, 'good', 'NM')
    """, (item_id, session_id, product_name, tcgplayer_id,
          set_name, quantity, market_price, offer_price, unit_cost, is_mapped))

    # Also save the mapping for future imports
    if tcgplayer_id and product_name:
        save_mapping(product_name, tcgplayer_id, 'sealed')

    _backfill_scrydex_ids(session_id)
    _recalculate_session_totals(session_id)
    return query_one("SELECT * FROM intake_items WHERE id = %s", (item_id,))


