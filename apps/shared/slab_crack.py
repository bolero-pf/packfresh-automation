"""
slab_crack.py — "Crack the slab" economics + execution.

A graded slab (PSA/CGC/etc.) sometimes sells for less than the equivalent
raw card at the grade-mapped condition. This module:

  1. Computes the comparison (slab listing vs raw at mapped condition)
  2. Executes the crack (mark slab REMOVED, create new raw_card row,
     delete Shopify listing) — leaving the raw_card in PENDING_RETURN
     so it lands in card_manager's Return Queue for label-print + scan-in.

Mapping policy (grade -> raw condition) was agreed offline:
    8-10 -> NM, 6-7 -> LP, 4-5 -> MP, 2-3 -> HP, 1 -> DMG

Operator can override the mapped condition at execute time since they
physically see the card when it pops out.
"""

from __future__ import annotations

import logging
import uuid as _uuid
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)


# Same multipliers as shared/price_synthesis.py so the slab-crack value
# is on the same scale as raw card prices the rest of the app shows.
GRADE_TO_CONDITION_DEFAULT = {
    "10":  "NM",
    "9.5": "NM",
    "9":   "NM",
    "8.5": "NM",
    "8":   "NM",
    "7.5": "LP",
    "7":   "LP",
    "6.5": "LP",
    "6":   "LP",
    "5.5": "MP",
    "5":   "MP",
    "4.5": "MP",
    "4":   "MP",
    "3.5": "HP",
    "3":   "HP",
    "2.5": "HP",
    "2":   "HP",
    "1.5": "DMG",
    "1":   "DMG",
}


def grade_to_condition(grade_value: Optional[str]) -> Optional[str]:
    """Map a grade string like '10' / '9.5' to a raw condition (NM/LP/MP/HP/DMG)."""
    if grade_value is None:
        return None
    key = str(grade_value).strip()
    if key in GRADE_TO_CONDITION_DEFAULT:
        return GRADE_TO_CONDITION_DEFAULT[key]
    # Tolerate '10.0' / '9.50' floats
    try:
        f = float(key)
    except (TypeError, ValueError):
        return None
    if f >= 8:    return "NM"
    if f >= 6:    return "LP"
    if f >= 4:    return "MP"
    if f >= 2:    return "HP"
    return "DMG"


def _fetch_slab(raw_card_id: str, db) -> Optional[dict]:
    row = db.query_one("""
        SELECT id, barcode, tcgplayer_id, scrydex_id, card_name, set_name,
               card_number, condition, rarity,
               state, cost_basis, current_price, last_price_update,
               bin_id, image_url,
               is_graded, grade_company, grade_value,
               variant, language, game,
               shopify_product_id, shopify_variant_id,
               intake_session_id, intake_item_id
        FROM raw_cards
        WHERE id = %s
    """, (str(raw_card_id),))
    if not row:
        return None
    return dict(row)


def compute_slab_crack(raw_card_id: str, db, pricing=None) -> Optional[dict]:
    """Compute the slab-vs-raw economics for a single slab.

    Returns a dict suitable for direct JSON rendering, or None if the row
    doesn't exist / isn't a slab.

    Output shape:
      {
        "raw_card_id": "...",
        "slab": {barcode, card_name, set_name, grade_company, grade_value,
                 current_price, image_url, ...},
        "mapped_condition": "NM",
        "raw_price_mapped": 42.50,   # raw market at the mapped condition
        "raw_price_nm":     58.00,   # raw market at NM (reference / ceiling)
        "slab_listing":     30.00,   # raw_cards.current_price
        "delta_mapped":     12.50,   # raw_mapped - slab_listing
        "delta_nm":         28.00,   # raw_nm     - slab_listing
        "recommend_crack":  true,    # delta_mapped > 0
        "reason":           "...",   # short explainer string
      }
    """
    slab = _fetch_slab(raw_card_id, db)
    if not slab:
        return None
    if not slab.get("is_graded"):
        return {"error": "not a slab", "raw_card_id": str(raw_card_id)}

    grade_value = slab.get("grade_value")
    mapped = grade_to_condition(grade_value)

    # Raw price lookup: try mapped condition + NM via the price provider.
    # cache_only equivalent: we never want this endpoint to time out on PPT.
    raw_mapped = None
    raw_nm = None
    if pricing is not None:
        try:
            raw_mapped = pricing.get_raw_condition_price(
                scrydex_id=slab.get("scrydex_id"),
                tcgplayer_id=slab.get("tcgplayer_id"),
                condition=mapped or "NM",
                variant=slab.get("variant"),
            )
        except Exception as e:
            logger.debug(f"slab_crack: mapped lookup failed for {raw_card_id}: {e}")
        try:
            raw_nm = pricing.get_raw_condition_price(
                scrydex_id=slab.get("scrydex_id"),
                tcgplayer_id=slab.get("tcgplayer_id"),
                condition="NM",
                variant=slab.get("variant"),
            )
        except Exception as e:
            logger.debug(f"slab_crack: NM lookup failed for {raw_card_id}: {e}")

    def _f(v):
        return float(v) if v is not None else None

    slab_listing = _f(slab.get("current_price"))
    raw_mapped_f = _f(raw_mapped)
    raw_nm_f = _f(raw_nm)

    delta_mapped = (
        round(raw_mapped_f - slab_listing, 2)
        if raw_mapped_f is not None and slab_listing is not None
        else None
    )
    delta_nm = (
        round(raw_nm_f - slab_listing, 2)
        if raw_nm_f is not None and slab_listing is not None
        else None
    )

    if delta_mapped is None:
        recommend = False
        reason = "No raw-market data at the mapped condition — can't compare."
    elif delta_mapped > 0:
        recommend = True
        reason = (
            f"Raw {mapped} (${raw_mapped_f:.2f}) is worth "
            f"${delta_mapped:.2f} more than the slab listing (${slab_listing:.2f})."
        )
    else:
        recommend = False
        reason = (
            f"Slab listing (${slab_listing:.2f}) still beats raw {mapped} "
            f"(${raw_mapped_f:.2f}) — don't crack."
        )

    return {
        "raw_card_id": str(slab["id"]),
        "slab": {
            "barcode":       slab.get("barcode"),
            "tcgplayer_id":  slab.get("tcgplayer_id"),
            "scrydex_id":    slab.get("scrydex_id"),
            "card_name":     slab.get("card_name"),
            "set_name":      slab.get("set_name"),
            "card_number":   slab.get("card_number"),
            "grade_company": slab.get("grade_company"),
            "grade_value":   slab.get("grade_value"),
            "image_url":     slab.get("image_url"),
            "state":         slab.get("state"),
            "variant":       slab.get("variant"),
            "current_price": slab_listing,
            "cost_basis":    _f(slab.get("cost_basis")),
            "shopify_product_id": slab.get("shopify_product_id"),
        },
        "mapped_condition":  mapped,
        "raw_price_mapped":  raw_mapped_f,
        "raw_price_nm":      raw_nm_f,
        "slab_listing":      slab_listing,
        "delta_mapped":      delta_mapped,
        "delta_nm":          delta_nm,
        "recommend_crack":   recommend,
        "reason":            reason,
    }


def compute_crack_from_inputs(
    *,
    db,
    pricing=None,
    scrydex_id: Optional[str] = None,
    tcgplayer_id: Optional[int] = None,
    grade_company: Optional[str] = None,
    grade_value: Optional[str] = None,
    slab_price: Optional[float] = None,
    variant: Optional[str] = None,
) -> dict:
    """Same comparison as compute_slab_crack(), but operates on raw inputs
    instead of a stored raw_cards row. Used by intake / ingest where the
    slab lives in intake_items (pre-push) and the listing price hasn't
    been written to raw_cards.current_price yet.

    slab_price is the operator's current listing target (intake_items.market_price
    for a fresh ingest, or the actual listing price if pulled from a draft).
    """
    mapped = grade_to_condition(grade_value)
    raw_mapped = raw_nm = None
    if pricing is not None and (scrydex_id or tcgplayer_id):
        try:
            raw_mapped = pricing.get_raw_condition_price(
                scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                condition=mapped or "NM", variant=variant,
            )
        except Exception as e:
            logger.debug(f"slab_crack inputs: mapped lookup failed: {e}")
        try:
            raw_nm = pricing.get_raw_condition_price(
                scrydex_id=scrydex_id, tcgplayer_id=tcgplayer_id,
                condition="NM", variant=variant,
            )
        except Exception as e:
            logger.debug(f"slab_crack inputs: NM lookup failed: {e}")

    def _f(v): return float(v) if v is not None else None
    slab_f = _f(slab_price)
    rm = _f(raw_mapped); rn = _f(raw_nm)

    delta_mapped = (round(rm - slab_f, 2)
                    if rm is not None and slab_f is not None else None)
    delta_nm = (round(rn - slab_f, 2)
                if rn is not None and slab_f is not None else None)

    return {
        "grade_company":     grade_company,
        "grade_value":       grade_value,
        "mapped_condition":  mapped,
        "raw_price_mapped":  rm,
        "raw_price_nm":      rn,
        "slab_listing":      slab_f,
        "delta_mapped":      delta_mapped,
        "delta_nm":          delta_nm,
        "recommend_crack":   delta_mapped is not None and delta_mapped > 0,
    }


def list_crack_candidates(db, pricing=None, *, min_delta: float = 1.0,
                          limit: int = 500) -> list[dict]:
    """Scan all live (state='STORED' or 'DISPLAY') slabs and return those
    where the raw price at the mapped condition beats the slab listing
    by at least `min_delta` dollars.

    Reads condition-mapped prices from scrydex_price_cache directly so
    this scales to thousands of slabs without firing one Scrydex call
    per row — same pattern breakdown summaries use.
    """
    slabs = db.query("""
        SELECT id, barcode, tcgplayer_id, scrydex_id, card_name, set_name,
               card_number, grade_company, grade_value, current_price,
               cost_basis, image_url, variant, state
        FROM raw_cards
        WHERE is_graded = TRUE
          AND state IN ('STORED', 'DISPLAY')
          AND current_price IS NOT NULL
        ORDER BY current_price DESC
    """)
    if not slabs:
        return []

    # Bulk price lookup: one query for all scrydex_ids the slabs reference.
    sx_ids = sorted({s["scrydex_id"] for s in slabs if s.get("scrydex_id")})
    raw_price_map: dict[tuple, dict] = {}  # (scrydex_id, condition) -> {market_price}
    if sx_ids:
        ph = ",".join(["%s"] * len(sx_ids))
        rows = db.query(f"""
            SELECT scrydex_id, condition, variant, market_price
              FROM scrydex_price_cache
             WHERE scrydex_id IN ({ph})
               AND price_type = 'raw'
               AND condition IN ('NM','LP','MP','HP','DMG')
        """, tuple(sx_ids))
        for r in rows:
            key = (r["scrydex_id"], r["condition"], r.get("variant") or "normal")
            existing = raw_price_map.get(key)
            if existing is None or (
                (existing.get("market_price") or 0)
                < (r.get("market_price") or 0)
            ):
                raw_price_map[key] = r

    out = []
    for s in slabs:
        sx = s.get("scrydex_id")
        if not sx:
            continue
        mapped = grade_to_condition(s.get("grade_value"))
        if not mapped:
            continue
        variant = s.get("variant") or "normal"
        # Prefer exact variant match, fall back to normal
        row = (raw_price_map.get((sx, mapped, variant))
               or raw_price_map.get((sx, mapped, "normal")))
        if not row or row.get("market_price") is None:
            continue
        raw_mapped = float(row["market_price"])
        slab_listing = float(s["current_price"])
        delta = round(raw_mapped - slab_listing, 2)
        if delta < min_delta:
            continue

        nm_row = (raw_price_map.get((sx, "NM", variant))
                  or raw_price_map.get((sx, "NM", "normal")))
        raw_nm = float(nm_row["market_price"]) if nm_row and nm_row.get("market_price") is not None else None

        out.append({
            "raw_card_id":      str(s["id"]),
            "barcode":          s["barcode"],
            "card_name":        s["card_name"],
            "set_name":         s["set_name"],
            "card_number":      s["card_number"],
            "grade_company":    s["grade_company"],
            "grade_value":      s["grade_value"],
            "image_url":        s.get("image_url"),
            "mapped_condition": mapped,
            "slab_listing":     slab_listing,
            "raw_price_mapped": raw_mapped,
            "raw_price_nm":     raw_nm,
            "delta_mapped":     delta,
            "delta_nm":         (round(raw_nm - slab_listing, 2)
                                 if raw_nm is not None else None),
            "state":            s["state"],
            "cost_basis":       float(s["cost_basis"]) if s.get("cost_basis") is not None else None,
            "tcgplayer_id":     s["tcgplayer_id"],
        })
        if len(out) >= limit:
            break

    # Hottest crack candidates first
    out.sort(key=lambda x: x["delta_mapped"], reverse=True)
    return out


def execute_slab_crack(
    raw_card_id: str, db,
    *,
    target_condition: Optional[str] = None,
    delete_shopify=None,
    operator: Optional[str] = None,
    generate_barcode=None,
) -> dict:
    """Execute the crack:
      1. Validate the slab is live (state in STORED/DISPLAY) and is_graded.
      2. Delete the Shopify listing (best-effort; logged if it fails).
      3. Flip slab row -> REMOVED with removal_reason='CRACKED'.
      4. Insert a new raw_card row with is_graded=FALSE, the chosen
         condition, state='PENDING_RETURN', no bin.
         (Lands in card_manager Return Queue for label-print + scan-in.)

    `delete_shopify` is an optional callable(product_id) -> None. If supplied,
    it's invoked before the DB transition so a Shopify failure aborts the
    crack instead of orphaning a listing. If None, the Shopify side is
    skipped and the operator must clean up manually (e.g. dev / test mode).

    `generate_barcode` is a callable() -> str. If None, uses uuid4()[:20].

    Returns:
      {"success": True, "new_raw_card_id": "...", "new_barcode": "..."}
    """
    slab = _fetch_slab(raw_card_id, db)
    if not slab:
        raise ValueError(f"raw_card {raw_card_id} not found")
    if not slab.get("is_graded"):
        raise ValueError(f"raw_card {raw_card_id} is not a slab")
    if slab.get("state") not in ("STORED", "DISPLAY"):
        raise ValueError(
            f"slab is in state {slab.get('state')!r} — only STORED/DISPLAY can be cracked"
        )

    condition = (target_condition
                 or grade_to_condition(slab.get("grade_value"))
                 or "NM").upper()
    if condition not in ("NM", "LP", "MP", "HP", "DMG"):
        raise ValueError(f"invalid target_condition {target_condition!r}")

    shopify_id = slab.get("shopify_product_id")
    if shopify_id and delete_shopify is not None:
        try:
            delete_shopify(int(shopify_id))
        except Exception as e:
            raise RuntimeError(
                f"Shopify delete failed for product {shopify_id}: {e}"
            ) from e

    new_barcode = (generate_barcode() if generate_barcode
                   else str(_uuid.uuid4())[:20])

    # Mark slab REMOVED. removal_reason='CRACKED' is new — parallel to GRADING.
    db.execute("""
        UPDATE raw_cards
           SET state = 'REMOVED',
               removal_reason = 'CRACKED',
               removal_date = CURRENT_TIMESTAMP,
               bin_id = NULL,
               shopify_product_id = NULL,
               shopify_variant_id = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE id = %s
    """, (str(slab["id"]),))

    # Insert the cracked raw equivalent. PENDING_RETURN so it shows up
    # in card_manager's Return Queue (scan-in + bin assignment).
    new_row = db.execute_returning("""
        INSERT INTO raw_cards (
            barcode, tcgplayer_id, scrydex_id, card_name, set_name,
            card_number, condition, rarity,
            state, cost_basis, current_price, last_price_update,
            bin_id, image_url,
            is_graded, grade_company, grade_value,
            variant, language, game,
            intake_session_id, intake_item_id
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            'PENDING_RETURN', %s, NULL, NULL,
            NULL, %s,
            FALSE, NULL, NULL,
            %s, %s, %s,
            %s, %s
        ) RETURNING id, barcode
    """, (
        new_barcode,
        slab.get("tcgplayer_id"),
        slab.get("scrydex_id"),
        slab.get("card_name"),
        slab.get("set_name"),
        slab.get("card_number"),
        condition,
        slab.get("rarity"),
        slab.get("cost_basis"),
        slab.get("image_url"),
        slab.get("variant"),
        slab.get("language") or "EN",
        slab.get("game"),
        slab.get("intake_session_id"),
        slab.get("intake_item_id"),
    ))

    logger.info(
        f"Slab crack: {slab['barcode']} ({slab.get('grade_company')} "
        f"{slab.get('grade_value')}) -> raw {condition} barcode={new_barcode} "
        f"operator={operator}"
    )

    return {
        "success":         True,
        "old_slab_id":     str(slab["id"]),
        "old_barcode":     slab["barcode"],
        "new_raw_card_id": str(new_row["id"]),
        "new_barcode":     new_row["barcode"],
        "condition":       condition,
    }
