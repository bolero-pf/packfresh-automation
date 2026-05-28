"""
events/events_shopify.py — Event-specific Shopify wrappers.

Thin wrapper over the generic metaobject + file helpers in shared/shopify_client.py.
Binds the event-specific metaobject type strings and re-exports rich-text +
file-upload helpers so app.py only needs one import.

Metaobject type strings (CANNOT be renamed; preserved from initial setup):
    SERIES_TYPE     = 'event'             (the "Event Series" metaobject)
    OCCURRENCE_TYPE = 'event_occurence'   (single 'r' — original spelling)

This file is named events_shopify (NOT shopify_client) to avoid colliding with
shared/shopify_client.py on PYTHONPATH, which would shadow it.
"""

import logging

from shopify_client import (
    shopify_client_from_env,
    plain_text_to_rich_text,
    rich_text_to_plain,
)

logger = logging.getLogger(__name__)

SERIES_TYPE = "event"
OCCURRENCE_TYPE = "event_occurence"

_client = None


def _client_lazy():
    global _client
    if _client is None:
        _client = shopify_client_from_env()
    return _client


# ─── Series ──────────────────────────────────────────────────────────────────

def list_series(first: int = 100) -> list[dict]:
    return _client_lazy().metaobjects_list(SERIES_TYPE, first=first)


def get_series(gid: str) -> dict | None:
    return _client_lazy().metaobject_get(gid)


def create_series(fields: dict) -> dict:
    return _client_lazy().metaobject_create(SERIES_TYPE, fields)


def update_series(gid: str, fields: dict) -> dict:
    return _client_lazy().metaobject_update(gid, fields)


# ─── Occurrence ──────────────────────────────────────────────────────────────

def list_occurrences(first: int = 250) -> list[dict]:
    items = _client_lazy().metaobjects_list(OCCURRENCE_TYPE, first=first)
    # Flatten series ref into series_id/series_handle for client convenience.
    for o in items:
        sref = o.get("series")
        if isinstance(sref, dict):
            o["series_id"] = sref.get("id")
            o["series_handle"] = sref.get("handle")
    return items


def create_occurrence(fields: dict) -> dict:
    return _client_lazy().metaobject_create(OCCURRENCE_TYPE, fields)


def update_occurrence(gid: str, fields: dict) -> dict:
    return _client_lazy().metaobject_update(gid, fields)


# ─── Shared passthroughs (kept here so app.py uses one import) ───────────────

def delete_metaobject(gid: str) -> None:
    _client_lazy().metaobject_delete(gid)


def upload_file_to_shopify(file_bytes: bytes, filename: str, mime_type: str) -> str:
    return _client_lazy().upload_image_to_files(file_bytes, filename, mime_type)


# Re-export rich text helpers under the names app.py already uses.
plain_text_to_rich = plain_text_to_rich_text
rich_to_plain_text = rich_text_to_plain
