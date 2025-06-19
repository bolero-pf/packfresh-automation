import csv
import os
from datetime import datetime
import pandas as pd

INVENTORY_FILE = ".venv/Scripts/inventory_data.csv"

HEADERS = [
    "id",
    "name",
    "shopify_qty",
    "shopify_price",
    "rc_qty",
    "rc_price",
    "last_synced",
    "notes"
]

def init_csv():
    if not os.path.exists(INVENTORY_FILE):
        with open(INVENTORY_FILE, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(HEADERS)
        print(f"[+] Created new inventory CSV: {INVENTORY_FILE}")
    else:
        print(f"[=] Inventory CSV already exists: {INVENTORY_FILE}")

def add_or_update_item(item):
    df = pd.read_csv(INVENTORY_FILE)

    if item["id"] in df["id"].values:
        df.loc[df["id"] == item["id"], list(item.keys())] = list(item.values())
        print(f"[~] Updated item: {item['name']}")
    else:
        df = pd.concat([df, pd.DataFrame([item])], ignore_index=True)
        print(f"[+] Added item: {item['name']}")

    df.to_csv(INVENTORY_FILE, index=False)

def get_inventory():
    return pd.read_csv(INVENTORY_FILE)

def pretty_print():
    df = pd.read_csv(INVENTORY_FILE)
    print(df.to_string(index=False))

if __name__ == "__main__":
    init_csv()

    # 🔁 Sample add/update
    sample = {
        "id": "charizard-vmax",
        "name": "Charizard VMAX - Darkness Ablaze",
        "shopify_qty": 2,
        "shopify_price": 199.99,
        "rc_qty": 1,
        "rc_price": 219.99,
        "last_synced": datetime.utcnow().isoformat(),
        "notes": ""
    }

    add_or_update_item(sample)
    pretty_print()