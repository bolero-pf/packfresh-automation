"""
Shared breakdown business logic — recipe CRUD + batch summary computation.

All functions take a `db` module parameter (must have query, query_one, execute,
execute_returning) so any service can use them with its own DB connection pool.
"""

import logging
import time
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Recipe CRUD ────────────────────────────────────────────────────


def get_breakdown_cache(tcgplayer_id: int, db) -> Optional[dict]:
    """
    Fetch full breakdown record for a product: all variants + their components.
    Returns None if not cached.
    """
    cache = db.query_one("SELECT * FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcgplayer_id,))
    if not cache:
        return None
    variants = db.query(
        "SELECT * FROM sealed_breakdown_variants WHERE breakdown_id=%s ORDER BY display_order, created_at",
        (str(cache["id"]),)
    )
    result = dict(cache)
    result["variants"] = []
    for v in variants:
        comps = db.query(
            "SELECT * FROM sealed_breakdown_components WHERE variant_id=%s ORDER BY display_order",
            (str(v["id"]),)
        )
        result["variants"].append({**v, "components": list(comps)})
    return result


def save_variant(tcgplayer_id: int, product_name: str,
                 variant_name: str, components: list,
                 db, notes: str = None, variant_id: str = None) -> dict:
    """
    Create or replace a named variant for a product.
    - variant_id=None  -> create new variant
    - variant_id=<id>  -> replace components of that existing variant in-place
    Returns full cache record.
    """
    existing = db.query_one("SELECT id FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcgplayer_id,))
    if existing:
        cache_id = str(existing["id"])
        db.execute("UPDATE sealed_breakdown_cache SET product_name=%s, last_updated=CURRENT_TIMESTAMP WHERE id=%s",
                   (product_name, cache_id))
    else:
        row = db.execute_returning(
            "INSERT INTO sealed_breakdown_cache (tcgplayer_id, product_name) VALUES (%s,%s) RETURNING id",
            (tcgplayer_id, product_name)
        )
        cache_id = str(row["id"])

    total_market = sum(
        Decimal(str(c.get("market_price", 0))) * int(c.get("quantity_per_parent", c.get("quantity", 1)))
        for c in components
    )
    comp_count = len(components)

    if variant_id:
        db.execute("""
            UPDATE sealed_breakdown_variants
            SET variant_name=%s, notes=%s, total_component_market=%s, component_count=%s, last_updated=CURRENT_TIMESTAMP
            WHERE id=%s
        """, (variant_name, notes, total_market, comp_count, variant_id))
        db.execute("DELETE FROM sealed_breakdown_components WHERE variant_id=%s", (variant_id,))
        vid = variant_id
    else:
        order_row = db.query_one(
            "SELECT COUNT(*) AS cnt FROM sealed_breakdown_variants WHERE breakdown_id=%s", (cache_id,)
        )
        disp = int(order_row["cnt"]) if order_row else 0
        v_row = db.execute_returning("""
            INSERT INTO sealed_breakdown_variants
                (breakdown_id, variant_name, notes, total_component_market, component_count, display_order)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
        """, (cache_id, variant_name, notes, total_market, comp_count, disp))
        vid = str(v_row["id"])

    for order, comp in enumerate(components):
        db.execute("""
            INSERT INTO sealed_breakdown_components
                (variant_id, tcgplayer_id, product_name, set_name, quantity_per_parent,
                 market_price, notes, display_order, component_type, market_price_updated_at)
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

    _refresh_cache_totals(cache_id, db)
    db.execute("UPDATE sealed_breakdown_cache SET use_count=use_count+1 WHERE id=%s", (cache_id,))
    return get_breakdown_cache(tcgplayer_id, db)


def delete_variant(variant_id: str, db) -> Optional[dict]:
    """
    Delete a single variant. If the parent has no remaining variants, deletes the parent too.
    Returns updated cache dict or None if parent was also deleted.
    """
    v = db.query_one("SELECT breakdown_id FROM sealed_breakdown_variants WHERE id=%s", (variant_id,))
    if not v:
        return None
    cache_id = str(v["breakdown_id"])
    db.execute("DELETE FROM sealed_breakdown_variants WHERE id=%s", (variant_id,))

    remaining = db.query_one(
        "SELECT COUNT(*) AS cnt FROM sealed_breakdown_variants WHERE breakdown_id=%s", (cache_id,)
    )
    if not remaining or int(remaining["cnt"]) == 0:
        db.execute("DELETE FROM sealed_breakdown_cache WHERE id=%s", (cache_id,))
        return None

    _refresh_cache_totals(cache_id, db)
    parent = db.query_one("SELECT tcgplayer_id FROM sealed_breakdown_cache WHERE id=%s", (cache_id,))
    return get_breakdown_cache(int(parent["tcgplayer_id"]), db) if parent else None


def delete_breakdown_cache(tcgplayer_id: int, db) -> bool:
    """Delete the entire breakdown record (all variants) for a product."""
    rows = db.execute("DELETE FROM sealed_breakdown_cache WHERE tcgplayer_id=%s", (tcgplayer_id,))
    return rows > 0


def list_breakdown_cache(db, limit: int = 200) -> list:
    """List all cached products with variant names, ordered by most used."""
    return list(db.query("""
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


def _refresh_cache_totals(cache_id: str, db):
    """Recompute variant_count + best_variant_market on the parent cache row."""
    db.execute("""
        UPDATE sealed_breakdown_cache SET
            variant_count=(SELECT COUNT(*) FROM sealed_breakdown_variants WHERE breakdown_id=%s),
            best_variant_market=COALESCE(
                (SELECT MAX(total_component_market) FROM sealed_breakdown_variants WHERE breakdown_id=%s), 0
            ),
            last_updated=CURRENT_TIMESTAMP
        WHERE id=%s
    """, (cache_id, cache_id, cache_id))


# ─── Batch Summary (with store prices + deep values) ───────────────


def get_breakdown_summary_for_items(tcg_ids: list, db, ppt=None, max_age_hours=4) -> dict:
    """
    Batch lookup: tcg_id -> breakdown summary with market + store values.
    Includes deep value (nested breakdown) computation.

    Four cases for frontend display:
      parent+children in store  -> compare children store total vs parent store
      parent in store, no child -> compare children market vs parent store
      children in store, no parent -> compare children store vs parent market
      neither in store -> compare market totals
    """
    if not tcg_ids:
        return {}
    ph = ",".join(["%s"] * len(tcg_ids))
    _t0 = time.perf_counter()
    _t_variants = _t_refresh = _t_components = _t_store = _t_all_variant_comps = 0.0
    _t_nested = _t_recipes_full = 0.0

    # Step 1: get best variant per parent (highest total_component_market).
    # Any recipe with >1 variant is implicitly probabilistic at intake — the
    # operator only knows the actual variant when cracking it open in ingest.
    _ts = time.perf_counter()
    variant_rows = db.query(f"""
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

    _t_variants = time.perf_counter() - _ts
    if not variant_rows:
        if time.perf_counter() - _t0 > 0.5:
            logger.warning("bd_summary fast-exit: tcg_ids=%d variants=0 t=%.2fs",
                           len(tcg_ids), time.perf_counter() - _t0)
        return {}

    # Step 2: get components for those variants
    variant_ids = [r["variant_id"] for r in variant_rows]

    # JIT refresh stale component market prices
    if ppt and variant_ids:
        try:
            from breakdown_helpers import refresh_stale_component_prices
            _ts = time.perf_counter()
            refresh_stale_component_prices(variant_ids, db, ppt, max_age_hours=max_age_hours)
            _t_refresh = time.perf_counter() - _ts
        except Exception as e:
            logger.warning(f"Component price refresh skipped: {e}")

    parent_ids = [r["parent_id"] for r in variant_rows]
    vph = ",".join(["%s"] * len(variant_ids))

    _ts = time.perf_counter()
    comp_rows = db.query(f"""
        SELECT sbco.variant_id, sbco.tcgplayer_id AS comp_tcg_id,
               sbco.quantity_per_parent, sbco.market_price AS comp_market,
               COALESCE(sbco.component_type, 'sealed') AS component_type
        FROM sealed_breakdown_components sbco
        WHERE sbco.variant_id IN ({vph})
    """, tuple(variant_ids))
    _t_components = time.perf_counter() - _ts

    # Step 3: batch store lookup for parents + all component tcg_ids
    comp_tcg_ids = list(set(r["comp_tcg_id"] for r in comp_rows if r.get("comp_tcg_id")))
    all_store_ids = list(set(parent_ids + comp_tcg_ids))
    _ts = time.perf_counter()
    if all_store_ids:
        sph = ",".join(["%s"] * len(all_store_ids))
        store_rows = db.query(
            f"SELECT tcgplayer_id, shopify_price, shopify_qty FROM inventory_product_cache "
            f"WHERE tcgplayer_id IN ({sph}) AND is_damaged = FALSE",
            tuple(all_store_ids)
        )
        store_map = {r["tcgplayer_id"]: r for r in store_rows}
    else:
        store_map = {}
    _t_store = time.perf_counter() - _ts

    # Load ALL variants' components for deep value (not just the best variant)
    all_variant_comps = []
    _ts = time.perf_counter()
    if tcg_ids:
        avph = ",".join(["%s"] * len(tcg_ids))
        all_variant_comps = db.query(f"""
            SELECT sbco.tcgplayer_id AS comp_tcg_id, sbco.quantity_per_parent,
                   sbco.market_price AS comp_market, sbv.id AS variant_id,
                   sbc.tcgplayer_id AS parent_id
            FROM sealed_breakdown_components sbco
            JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
            WHERE sbc.tcgplayer_id IN ({avph}) AND sbco.tcgplayer_id IS NOT NULL
        """, tuple(tcg_ids))
    _t_all_variant_comps = time.perf_counter() - _ts

    all_comp_tcg_ids = list(set(
        comp_tcg_ids + [int(c["comp_tcg_id"]) for c in all_variant_comps if c["comp_tcg_id"]]
    ))

    # Nested breakdown lookup: which components have their own recipes?
    child_bd_map = {}
    child_bd_store_map = {}
    _ts = time.perf_counter()
    if all_comp_tcg_ids:
        cbp = ",".join(["%s"] * len(all_comp_tcg_ids))
        child_bd_rows = db.query(
            f"SELECT tcgplayer_id, best_variant_market FROM sealed_breakdown_cache WHERE tcgplayer_id IN ({cbp})",
            tuple(all_comp_tcg_ids)
        )
        child_bd_map = {int(r["tcgplayer_id"]): float(r["best_variant_market"] or 0) for r in child_bd_rows}

        # Compute store-based BD value for children with recipes (grandchild store prices)
        if child_bd_map:
            try:
                child_tcg_list = list(child_bd_map.keys())
                gcph = ",".join(["%s"] * len(child_tcg_list))
                gc_rows = db.query(f"""
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
                    gc_sp = db.query(
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
    _t_nested = time.perf_counter() - _ts

    # Step 3.5: per-variant totals across ALL variants of these parents (for
    # avg/min and the variants[] list used by the override picker).
    parent_id_to_variants = {}  # parent_tcg_id -> [{id, name, market, store, ...}]
    _ts = time.perf_counter()
    if tcg_ids:
        all_var_rows = db.query(f"""
            SELECT sbv.id AS variant_id, sbv.variant_name, sbv.display_order,
                   sbv.total_component_market AS variant_market,
                   sbc.tcgplayer_id AS parent_id
            FROM sealed_breakdown_variants sbv
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
            WHERE sbc.tcgplayer_id IN ({ph})
            ORDER BY sbv.display_order, sbv.created_at
        """, tuple(tcg_ids))

        # Pull components for ALL variants (with component_type for promo handling).
        all_var_ids = [str(r["variant_id"]) for r in all_var_rows]
        all_var_comp_rows = []
        if all_var_ids:
            avp = ",".join(["%s"] * len(all_var_ids))
            all_var_comp_rows = db.query(f"""
                SELECT variant_id, tcgplayer_id AS comp_tcg_id, quantity_per_parent,
                       market_price AS comp_market,
                       COALESCE(component_type, 'sealed') AS component_type
                FROM sealed_breakdown_components
                WHERE variant_id IN ({avp})
            """, tuple(all_var_ids))

        comps_by_var = {}
        for c in all_var_comp_rows:
            comps_by_var.setdefault(str(c["variant_id"]), []).append(c)

        for vr in all_var_rows:
            vkey = str(vr["variant_id"])
            vc = comps_by_var.get(vkey, [])
            v_total_market = 0.0
            v_total_store = 0.0
            v_in_store = 0
            for c in vc:
                qty = c["quantity_per_parent"] or 1
                mkt = float(c["comp_market"] or 0)
                v_total_market += mkt * qty
                if c.get("component_type") == "promo":
                    # Promos: treat market as store (no separate listing).
                    v_total_store += mkt * qty
                    v_in_store += 1
                else:
                    cs = store_map.get(c["comp_tcg_id"])
                    if cs and cs.get("shopify_price"):
                        v_total_store += float(cs["shopify_price"]) * qty
                        v_in_store += 1
            v_total_components = len(vc)
            v_all_in_store = (v_in_store == v_total_components and v_total_components > 0)
            v_any_in_store = v_in_store > 0
            parent_id_to_variants.setdefault(vr["parent_id"], []).append({
                "id": str(vr["variant_id"]),
                "name": vr["variant_name"],
                "display_order": vr["display_order"],
                "market": round(v_total_market, 2),
                "store": round(v_total_store, 2) if v_any_in_store else None,
                "store_partial": v_any_in_store and not v_all_in_store,
                "components_in_store": v_in_store,
                "total_components": v_total_components,
            })
    _t_recipes_full = time.perf_counter() - _ts

    # Step 4: assemble results
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
        total_comp_store = 0.0
        comps_with_store = 0

        for c in comps:
            qty = c["quantity_per_parent"] or 1
            mkt = float(c["comp_market"] or 0)
            total_comp_market += mkt * qty
            is_promo = c.get("component_type") == "promo"
            if is_promo:
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

        # Compute deep values across ALL variants — separate market vs store
        best_deep_market = 0.0
        best_deep_store = 0.0
        _pvar_comps = {}
        for avc in all_variant_comps:
            if avc["parent_id"] == pid:
                _pvar_comps.setdefault(str(avc["variant_id"]), []).append(avc)
        for _pvid, _pvcomps in _pvar_comps.items():
            dv_market = 0.0
            dv_store = 0.0
            has_child_bd = False
            all_store = True
            for vc in _pvcomps:
                cid = int(vc["comp_tcg_id"])
                qty = vc["quantity_per_parent"] or 1
                comp_market = float(vc["comp_market"] or 0)

                # Market deep: use child's BD market value if it has a recipe, else component market
                child_bd_mkt = child_bd_map.get(cid, 0)
                if child_bd_mkt > 0:
                    dv_market += child_bd_mkt * qty
                    has_child_bd = True
                else:
                    dv_market += comp_market * qty

                # Store deep: use child's BD store value if it has a recipe, else store price, else skip
                cbd_store = child_bd_store_map.get(cid, 0)
                if cbd_store > 0:
                    dv_store += cbd_store * qty
                    has_child_bd = True
                else:
                    cs = store_map.get(cid)
                    sp = float(cs["shopify_price"]) if cs and cs.get("shopify_price") else 0
                    if sp > 0:
                        dv_store += sp * qty
                    else:
                        all_store = False
                        dv_store += comp_market * qty

            if has_child_bd and dv_market > best_deep_market:
                best_deep_market = dv_market
            if has_child_bd and all_store and dv_store > best_deep_store:
                best_deep_store = dv_store

        # Aggregate market/store across ALL variants for this parent.
        pvariants = parent_id_to_variants.get(pid, [])
        v_markets = [v["market"] for v in pvariants if v["market"] is not None]
        v_stores  = [v["store"]  for v in pvariants if v["store"]  is not None]

        expected_market = round(sum(v_markets) / len(v_markets), 2) if v_markets else None
        worst_market    = round(min(v_markets), 2) if v_markets else None
        # Store aggregates only meaningful when EVERY variant has full store data.
        all_variants_have_store = (len(v_stores) == len(pvariants) and len(pvariants) > 0
                                    and all(not v.get("store_partial") for v in pvariants))
        if all_variants_have_store:
            best_store_agg     = round(max(v_stores), 2)
            expected_store_agg = round(sum(v_stores) / len(v_stores), 2)
            worst_store_agg    = round(min(v_stores), 2)
        else:
            best_store_agg = expected_store_agg = worst_store_agg = None

        result[pid] = {
            "variant_count":        vrow["variant_count"],
            "best_variant_market":  float(vrow["best_variant_market"] or 0),
            "expected_variant_market": expected_market,
            "worst_variant_market":    worst_market,
            "variant_names":        vrow["variant_names"],
            "parent_store_price":   parent_store_price,
            "best_variant_store":   round(total_comp_store, 2) if any_comps_in_store else None,
            "best_variant_store_partial": any_comps_in_store and not all_comps_in_store,
            "expected_variant_store":  expected_store_agg,
            "worst_variant_store":     worst_store_agg,
            "components_in_store":  comps_with_store,
            "total_components":     total_components,
            "deep_bd_market":       round(best_deep_market, 2) if best_deep_market > 0 else None,
            "deep_bd_store":        round(best_deep_store, 2) if best_deep_store > 0 else None,
            "variants":             pvariants,
        }

    _t_total = time.perf_counter() - _t0
    if _t_total > 1.0:
        logger.warning(
            "bd_summary slow: parents=%d variants=%d comps=%d total=%.2fs "
            "[variants_q=%.2fs refresh=%.2fs comps_q=%.2fs store_q=%.2fs "
            "all_var_comps_q=%.2fs nested=%.2fs recipes_full=%.2fs]",
            len(tcg_ids), len(variant_rows), len(comp_rows),
            _t_total, _t_variants, _t_refresh, _t_components, _t_store,
            _t_all_variant_comps, _t_nested, _t_recipes_full,
        )

    return result


def pick_offer_value(summary: dict, claimed_variant_id: Optional[str] = None,
                     prefer: str = "store") -> Optional[float]:
    """
    Single source of truth: which number drives intake offer math?

    - claimed_variant_id set     → that variant's value (operator locked it)
    - >1 variant (no claim)      → expected (avg) across variants
    - 1 variant                  → best (= only) variant value

    `prefer="store"` returns store-priced value if available, falls back to market.
    `prefer="market"` always returns the market-priced value.

    Returns None if `summary` is empty or carries no usable values.
    """
    if not summary:
        return None

    def _pick(store_val, market_val):
        if prefer == "store" and store_val is not None and store_val > 0:
            return float(store_val)
        return float(market_val) if market_val is not None else None

    if claimed_variant_id:
        for v in summary.get("variants") or []:
            if str(v.get("id")) == str(claimed_variant_id):
                return _pick(v.get("store"), v.get("market"))
        # Claim no longer points to a real variant — fall through to default.

    variant_count = summary.get("variant_count") or 0
    if variant_count > 1:
        return _pick(summary.get("expected_variant_store"),
                     summary.get("expected_variant_market"))

    return _pick(summary.get("best_variant_store"),
                 summary.get("best_variant_market"))


def get_store_prices(tcg_ids: list, db) -> dict:
    """Look up inventory_product_cache prices for a list of tcgplayer_ids."""
    if not tcg_ids:
        return {}
    ph = ",".join(["%s"] * len(tcg_ids))
    rows = db.query(
        f"SELECT tcgplayer_id, shopify_product_id, shopify_variant_id, shopify_price, shopify_qty, handle, title "
        f"FROM inventory_product_cache WHERE tcgplayer_id IN ({ph}) AND is_damaged = FALSE",
        tuple(tcg_ids)
    )
    return {r["tcgplayer_id"]: dict(r) for r in rows}
