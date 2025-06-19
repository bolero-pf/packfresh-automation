import re
import hashlib
import sys
import csv
from urllib.parse import urlparse
from app_with_render_table_updated import fetch_rc_inventory_via_graphql, get_bearer_token, update_rc_listing
import pandas as pd


def generate_clean_slug(name: str) -> str:
    base = name.lower()
    base = re.sub(r"[^\w\s-]", "", base)
    base = re.sub(r"\s+", "-", base.strip())
    return base

def extract_slug_from_share_url(url: str) -> str:
    path = urlparse(url).path
    return path.strip('/').split('/')[-1]

def looks_like_sku_slug(slug: str) -> bool:
    return bool(re.match(r"^PF\d{4}-\d{6}-\d{4}$", slug))

def build_slug_recovery_report():
    bearer_token = get_bearer_token()
    df = fetch_rc_inventory_via_graphql(bearer_token)

    results = []
    for _, row in df.iterrows():
        share_url = row.get("share_url")
        name = row.get("name")
        rare_find_id = row.get("rare_find_id")
        inventory_item_id = row.get("inventory_item_id")
        price = row.get("current price")
        quantity = row.get("quantity listed")

        if pd.isna(share_url):
            print(f"Skipping row with missing share_url: {row}")
            continue

        current_slug = extract_slug_from_share_url(share_url)

        if looks_like_sku_slug(current_slug):
            clean_slug = generate_clean_slug(name)
            hash_suffix = hashlib.md5(str(current_slug).encode()).hexdigest()[:8]
            new_slug = f"{clean_slug}-{hash_suffix}"
            url_changed = True
        else:
            new_slug = current_slug
            url_changed = False

        results.append({
            "rare_find_id": rare_find_id,
            "inventory_item_id": inventory_item_id,
            "name": name,
            "current_slug": current_slug,
            "new_slug": new_slug,
            "url_changed": url_changed,
            "price": price,
            "quantity": quantity
        })

    if not results:
        print("❌ No valid rows found. CSV not written.")
        return

    output_file = ".venv/Scripts/rc_slug_recovery.csv"
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"✅ Slug recovery report written to {output_file}")

def apply_slug_fixes_from_csv():
    bearer_token = get_bearer_token()
    df = pd.read_csv(".venv/Scripts/rc_slug_recovery.csv")

    for _, row in df.iterrows():
        if row.get("url_changed"):
            print(f"🔁 Updating slug for {row['name']}: {row['current_slug']} → {row['new_slug']}")
            update_rc_listing(
                bearer_token=bearer_token,
                inventory_item_id=row["inventory_item_id"],
                rare_find_id=row["rare_find_id"],
                name=row["name"],
                rc_slug=row["new_slug"],
                rc_price=row.get("price"),
                rc_qty=row.get("quantity")
            )


if __name__ == "__main__":
    if "--apply" in sys.argv:
        apply_slug_fixes_from_csv()
    else:
        build_slug_recovery_report()

