"""One-shot: classify unmatched sealed items as MCAP vs non-MCAP via PPT."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db; db.init_pool()
from ppt_client import PPTClient, PPTError
from collections import defaultdict

ppt = PPTClient(os.getenv("PPT_API_KEY"))

# Sanity
data = ppt.get_sealed_product_by_tcgplayer_id(282250)
print(f"Sanity: {data.get('name')} set={data.get('setName')}")
rl = ppt.get_rate_limit_info()
print(f"Rate: min={rl.get('minute_remaining')} daily={rl.get('daily_remaining')}")

unmatched = db.query("""
    SELECT DISTINCT ipc.title, ipc.tcgplayer_id
    FROM inventory_product_cache ipc
    LEFT JOIN scrydex_price_cache spc ON spc.tcgplayer_id = ipc.tcgplayer_id AND spc.product_type = 'sealed'
    WHERE ipc.tcgplayer_id IS NOT NULL AND ipc.is_damaged = FALSE
    AND spc.tcgplayer_id IS NULL
    AND (ipc.tags ILIKE '%%sealed%%' OR ipc.tags ILIKE '%%booster%%' OR ipc.tags ILIKE '%%etb%%')
    ORDER BY ipc.title
""")
print(f"Unmatched: {len(unmatched)}\n")

mcap, non_mcap, failed = [], [], []

for i, item in enumerate(unmatched):
    if ppt.should_throttle():
        rl = ppt.get_rate_limit_info()
        wait = (rl.get("retry_after") or 30) + 2
        print(f"  Throttled at {i} — waiting {wait}s", flush=True)
        time.sleep(wait)

    try:
        data = ppt.get_sealed_product_by_tcgplayer_id(item["tcgplayer_id"])
        if data:
            set_name = data.get("setName") or "UNKNOWN"
            name = data.get("name") or item["title"]
            if "miscellaneous" in set_name.lower():
                mcap.append((name, set_name, item["tcgplayer_id"]))
            else:
                non_mcap.append((name, set_name, item["tcgplayer_id"]))
        else:
            failed.append((item["title"], item["tcgplayer_id"], "not found"))
    except PPTError as e:
        if e.status_code == 429:
            rl = ppt.get_rate_limit_info()
            wait = (rl.get("retry_after") or 30) + 2
            print(f"  429 at {i} — sleeping {wait}s", flush=True)
            time.sleep(wait)
        failed.append((item["title"], item["tcgplayer_id"], str(e)[:50]))

    if (i + 1) % 50 == 0:
        rl = ppt.get_rate_limit_info()
        print(f"  [{i+1}/{len(unmatched)}] mcap={len(mcap)} non={len(non_mcap)} fail={len(failed)} min={rl.get('minute_remaining')} daily={rl.get('daily_remaining')}", flush=True)

print(f"\n{'='*60}")
print(f"MCAP: {len(mcap)}")
print(f"Non-MCAP: {len(non_mcap)}")
print(f"Failed: {len(failed)}")

print(f"\n--- NON-MCAP by set ---")
by_set = defaultdict(list)
for name, set_name, tcg_id in non_mcap:
    by_set[set_name].append((name, tcg_id))
for s in sorted(by_set.keys()):
    items = by_set[s]
    print(f"\n  {s} ({len(items)}):")
    for name, tcg_id in items[:5]:
        print(f"    {name} (tcg={tcg_id})")
    if len(items) > 5:
        print(f"    +{len(items)-5} more")

print(f"\n--- MCAP ({len(mcap)}) ---")
for name, sn, tcg_id in mcap[:15]:
    print(f"  {name} (tcg={tcg_id})")
if len(mcap) > 15:
    print(f"  +{len(mcap)-15} more")
