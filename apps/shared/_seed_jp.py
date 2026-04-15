"""Seed Japanese sealed products into scrydex_price_cache."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import db; db.init_pool()
from scrydex_client import ScrydexClient
from psycopg2.extras import execute_batch

client = ScrydexClient(os.getenv('SCRYDEX_API_KEY'), os.getenv('SCRYDEX_TEAM_ID'), db=db)

# Get JP expansions
all_exp = []
page = 1
while True:
    resp = client._get(f'{client.base_url}/pokemon/v1/ja/expansions', {'page': page, 'page_size': 100})
    data = resp.get('data', [])
    if not data:
        break
    all_exp.extend(data)
    if len(data) < 100:
        break
    page += 1

print(f"Japanese expansions: {len(all_exp)}", flush=True)

total_sealed = 0
total_credits = 0

for i, exp in enumerate(all_exp):
    eid = exp['id']
    try:
        resp = client._get(f'{client.base_url}/pokemon/v1/ja/expansions/{eid}/sealed',
                          {'page_size': 100, 'include': 'prices'})
        total_credits += 1
        items = resp.get('data', [])
        if not items:
            continue

        rows = []
        for item in items:
            sid = item.get('id')
            if not sid:
                continue
            name = item.get('name', '')
            expansion = item.get('expansion', {})
            exp_name = expansion.get('name', '')
            img_s = img_m = img_l = ''
            for img in (item.get('images') or []):
                if img.get('type') == 'front':
                    img_s, img_m, img_l = img.get('small',''), img.get('medium',''), img.get('large','')
                    break
            for v in (item.get('variants') or []):
                vname = v.get('name', 'normal')
                for p in (v.get('prices') or []):
                    condition = p.get('condition', 'U')
                    market = p.get('market')
                    low = p.get('low')
                    trends = p.get('trends') or {}
                    t1 = (trends.get('days_1') or {}).get('percent_change')
                    t7 = (trends.get('days_7') or {}).get('percent_change')
                    t30 = (trends.get('days_30') or {}).get('percent_change')
                    rows.append((sid, None, eid, exp_name, 'sealed', name, None, None,
                                vname, condition, 'raw', None, None,
                                market, low, None, None, t1, t7, t30,
                                img_s, img_m, img_l))

        if rows:
            with db.get_conn() as conn:
                with conn.cursor() as cur:
                    execute_batch(cur, """
                        INSERT INTO scrydex_price_cache (
                            scrydex_id, tcgplayer_id, expansion_id, expansion_name,
                            product_type, product_name, card_number, rarity,
                            variant, condition, price_type, grade_company, grade_value,
                            market_price, low_price, mid_price, high_price,
                            trend_1d_pct, trend_7d_pct, trend_30d_pct,
                            image_small, image_medium, image_large, fetched_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (scrydex_id, variant, condition, price_type, grade_company_key, grade_value_key)
                        DO UPDATE SET market_price=EXCLUDED.market_price, low_price=EXCLUDED.low_price, fetched_at=NOW()
                    """, rows, page_size=500)
                conn.commit()
            total_sealed += len(items)

    except Exception as e:
        print(f"  {eid}: {e}", flush=True)

    if (i+1) % 50 == 0:
        print(f"  [{i+1}/{len(all_exp)}] {total_sealed} sealed, {total_credits} credits", flush=True)

print(f"\nDone! {total_sealed} JP sealed products, {total_credits} credits", flush=True)
