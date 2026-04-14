"""
Quick probe: hit Scrydex API with a known card and dump the full response
to find the TCGPlayer ID field name and verify the response shape.
"""
import os, json, requests

API_KEY = os.getenv("SCRYDEX_API_KEY", "")
TEAM_ID = os.getenv("SCRYDEX_TEAM_ID", "")
BASE = "https://api.scrydex.com"

headers = {
    "X-Api-Key": API_KEY,
    "X-Team-ID": TEAM_ID,
    "Accept": "application/json",
}

if not API_KEY or not TEAM_ID:
    print("Set SCRYDEX_API_KEY and SCRYDEX_TEAM_ID env vars first")
    exit(1)

# 1. Search for a well-known card (Charizard from base set)
print("=" * 60)
print("TEST 1: Search for 'Charizard' (cards)")
print("=" * 60)
r = requests.get(f"{BASE}/pokemon/v1/cards", headers=headers, params={
    "q": "name:Charizard expansion.name:Base",
    "page_size": 1,
    "include": "prices",
})
print(f"Status: {r.status_code}")
data = r.json()
print(json.dumps(data, indent=2, default=str)[:5000])

# 2. Get a specific card by Scrydex ID to see full shape
if data.get("data"):
    card_id = data["data"][0].get("id")
    print(f"\n{'=' * 60}")
    print(f"TEST 2: Get card by ID: {card_id}")
    print("=" * 60)
    r2 = requests.get(f"{BASE}/pokemon/v1/cards/{card_id}", headers=headers, params={
        "include": "prices",
    })
    print(f"Status: {r2.status_code}")
    card = r2.json()
    # Dump ALL top-level keys to find tcgplayer_id
    if isinstance(card.get("data"), dict):
        card_data = card["data"]
    else:
        card_data = card
    print(f"\nTop-level keys: {list(card_data.keys())}")
    print(json.dumps(card_data, indent=2, default=str)[:5000])

    # Search specifically for any key containing 'tcg'
    def find_tcg_keys(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if "tcg" in k.lower() or "external" in k.lower() or "player" in k.lower():
                    print(f"  FOUND: {path}.{k} = {v}")
                find_tcg_keys(v, f"{path}.{k}")
        elif isinstance(obj, list) and len(obj) < 20:
            for i, item in enumerate(obj):
                find_tcg_keys(item, f"{path}[{i}]")

    print(f"\nSearching for TCGPlayer ID fields...")
    find_tcg_keys(card_data)

# 3. Search for a sealed product
print(f"\n{'=' * 60}")
print("TEST 3: Search sealed products")
print("=" * 60)
r3 = requests.get(f"{BASE}/pokemon/v1/sealed", headers=headers, params={
    "q": "name:\"Surging Sparks Booster Box\"",
    "page_size": 1,
    "include": "prices",
})
print(f"Status: {r3.status_code}")
sealed = r3.json()
print(json.dumps(sealed, indent=2, default=str)[:3000])

if sealed.get("data"):
    sealed_data = sealed["data"][0]
    print(f"\nSealed top-level keys: {list(sealed_data.keys())}")
    print(f"\nSearching sealed for TCGPlayer ID fields...")
    find_tcg_keys(sealed_data)

# 4. Check credit usage
print(f"\n{'=' * 60}")
print("TEST 4: Credit usage")
print("=" * 60)
r4 = requests.get(f"{BASE}/account/v1/usage", headers=headers)
print(f"Status: {r4.status_code}")
print(json.dumps(r4.json(), indent=2, default=str))
