
# meta_tracker.py

import json
from datetime import datetime

META_PATH = "meta.json"

def load_meta():
    try:
        with open(META_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_meta(meta):
    with open(META_PATH, "w") as f:
        json.dump(meta, f, indent=2)

def get_last_sync(platform):
    meta = load_meta()
    return meta.get(f"last_sales_sync_{platform}", "2000-01-01T00:00:00Z")

def update_last_sync(platform, timestamp):
    meta = load_meta()
    meta[f"last_sales_sync_{platform}"] = timestamp
    save_meta(meta)
