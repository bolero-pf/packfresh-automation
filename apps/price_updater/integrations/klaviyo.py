# integrations/klaviyo.py
import os, time, requests
from dotenv import load_dotenv
load_dotenv()
BASE = "https://a.klaviyo.com/api"
KEY  = os.environ["KLAVIYO_SECRET"]
REV  = "2024-10-15"  # API revision header

class KlaviyoError(RuntimeError): pass

def _headers():
    return {
        "Authorization": f"Klaviyo-API-Key {KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "revision": REV,
    }

def _patch_profile(profile_id: str, properties: dict, timeout=10):
    payload = {"data": {"type": "profile", "id": profile_id, "attributes": {"properties": properties}}}
    r = requests.patch(f"{BASE}/profiles/{profile_id}/", json=payload, headers=_headers(), timeout=timeout)
    if r.status_code not in (200,):
        raise KlaviyoError(f"PATCH failed {r.status_code}: {r.text}")
    return r.json()



def upsert_profile(*, email=None, external_id=None, properties: dict, timeout=10):
    # Strategy:
    # 1) POST using EMAIL ONLY (preferred unique key).
    # 2) If 409 with duplicate_profile_id -> PATCH that profile with properties.
    # 3) (Optional later) try to set external_id via PATCH if you want, but skip for now to avoid conflicts.
    if not email and not external_id:
        raise ValueError("Need email or external_id")

    attrs = {"properties": properties}
    if email:
        attrs["email"] = email
    # IMPORTANT: do NOT send external_id on initial POST to avoid cross-profile conflicts
    payload = {"data": {"type": "profile", "attributes": attrs}}

    for attempt in range(4):
        r = requests.post(f"{BASE}/profiles/", json=payload, headers=_headers(), timeout=timeout)
        if r.status_code in (200, 201):
            return r.json()

        if r.status_code == 409:
            try:
                dup_id = r.json()["errors"][0]["meta"]["duplicate_profile_id"]
            except Exception:
                raise KlaviyoError(f"409 without duplicate_profile_id: {r.text}")
            # Resolve by PATCHing the duplicate with properties
            return _patch_profile(dup_id, properties, timeout=timeout)

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5 * (2 ** attempt))
            continue

        raise KlaviyoError(f"POST failed {r.status_code}: {r.text}")

    raise KlaviyoError(f"POST retry exhaustion: {r.status_code}: {r.text}")
