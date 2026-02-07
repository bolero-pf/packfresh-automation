from datetime import date, timedelta
from .iterators import iter_slab_variants_with_meta
from ..pricing.strategies import smart_price, decide_update
from ..integrations.pokemon_price_tracker.adapter import normalize_bulk_response
from ..shopify.variant import update_variant_price

BATCH_SIZE = 50

def _group_batches_by_company(items: list[dict]) -> dict[str, list[dict]]:
    groups = {}
    for it in items:
        comp = (it["lookup"]["company"] or "psa").lower()
        groups.setdefault(comp, []).append(it)
    return groups

def _collect_ready_batches(shopify_client, tag="slab", size=BATCH_SIZE):
    batch = []
    not_ready = []
    for it in iter_slab_variants_with_meta(shopify_client, tag=tag):
        if it["ready"]:
            batch.append(it)
            if len(batch) >= size:
                yield batch
                batch = []
        else:
            not_ready.append(it)
    if batch:
        yield batch
    return not_ready  # note: only returned if caller exhausts generator immediately

def run_slabs_sync(shopify_client, ppt_client, *, dry_run=True, half_life_days=7.0):
    today = date.today()
    start = (today - timedelta(days=30)).isoformat()
    end   = today.isoformat()

    report = {"updated": [], "flag_down": [], "no_data": [], "parse_missing": [], "api_fail": []}

    # Gather & process batches
    # NOTE: _collect_ready_batches is a generator; to also receive not_ready list,
    # you can do a first pass to collect all ready items.
    ready_items = []
    not_ready = []
    for it in iter_slab_variants_with_meta(shopify_client, tag="slab"):
        if it["ready"]:
            ready_items.append(it)
        else:
            not_ready.append(it)

    missing_sets = {}
    for it in not_ready:
        if "set" in it["missing"] and it["meta"].set_name:
            missing_sets[it["meta"].set_name] = 1

    if missing_sets:
        report["parse_missing"].extend([{"set_name": s, "reason": "unmapped"} for s in missing_sets])
        # optional: early return if you want to enforce mapping completeness
        # return report
    for it in not_ready:
        report["parse_missing"].append({"variant_id": it["variant_id"], "missing": it["missing"]})

    # Group by company so PPT 'type' aligns (psa/cgc/bgs). (You can also group by set to improve cache locality.)
    by_company = _group_batches_by_company(ready_items)

    for comp, items in by_company.items():
        # walk in sub-batches of 50 (PPT bulk limit)
        for i in range(0, len(items), BATCH_SIZE):
            chunk = items[i:i+BATCH_SIZE]
            card_ids = [it["lookup"]["cardId"] for it in chunk]

            try:
                raw = ppt_client.bulk_history(card_ids=card_ids, type_=comp, start=start, end=end)
                by_card = normalize_bulk_response(raw)
            except Exception as e:
                for it in chunk:
                    report["api_fail"].append({"variant_id": it["variant_id"], "err": str(e)})
                continue

            # compute prices and decide actions
            for it in chunk:
                cid = it["lookup"]["cardId"]
                sales = by_card.get(cid) or []
                if not sales:
                    report["no_data"].append({"variant_id": it["variant_id"], "cardId": cid})
                    continue

                target = smart_price(sales, half_life_days=half_life_days, rounding=True)
                decision, newp = decide_update(it["current_price"], target, max_auto_down_pct=2.0)

                if decision == "update":
                    if dry_run:
                        report["updated"].append({"variant_id": it["variant_id"], "from": it["current_price"], "to": newp, "dry_run": True})
                    else:
                        update_variant_price(shopify_client, it["variant_id"], newp)
                        report["updated"].append({"variant_id": it["variant_id"], "from": it["current_price"], "to": newp})
                elif decision == "flag_down":
                    report["flag_down"].append({"variant_id": it["variant_id"], "from": it["current_price"], "to": newp})

    return report
