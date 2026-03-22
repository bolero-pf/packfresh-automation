"""
Shared Klaviyo integration — profile upsert with duplicate resolution.

Used by vip/ (tier transitions) and screening/ (verification flags).
"""

import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

BASE = "https://a.klaviyo.com/api"
KEY = os.environ.get("KLAVIYO_SECRET", "")
REV = "2024-10-15"


class KlaviyoError(RuntimeError):
    pass


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
    """
    Upsert a Klaviyo profile by email with properties.
    Handles 409 duplicates by patching the existing profile.
    """
    if not email and not external_id:
        raise ValueError("Need email or external_id")

    attrs = {"properties": properties}
    if email:
        attrs["email"] = email
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
            return _patch_profile(dup_id, properties, timeout=timeout)

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.5 * (2 ** attempt))
            continue

        raise KlaviyoError(f"POST failed {r.status_code}: {r.text}")

    raise KlaviyoError(f"POST retry exhaustion: {r.status_code}: {r.text}")
