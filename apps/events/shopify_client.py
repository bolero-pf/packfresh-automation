"""
events/shopify_client.py — Shopify Admin GraphQL helpers for the events service.

Wraps metaobject CRUD + file upload calls used by the events console.

Metaobject type strings (CANNOT be renamed; preserved from initial setup):
    SERIES_TYPE     = 'event'             (the "Event Series" metaobject)
    OCCURRENCE_TYPE = 'event_occurence'   (single 'r' — original spelling)

All calls go through shared/shopify_graphql.py which handles retries, auth,
and throttling. We only build the query strings + parse responses here.
"""

import json
import logging
import requests
from typing import Optional

from shopify_graphql import shopify_gql

logger = logging.getLogger(__name__)

SERIES_TYPE = "event"
OCCURRENCE_TYPE = "event_occurence"


# -------- Field parsing --------

def _fields_to_dict(node: dict) -> dict:
    """Flatten a Shopify metaobject node's `fields` list into {key: value}.
    Resolves rich text JSON, parses booleans, and includes the resolved reference
    for File and Metaobject references when present.
    """
    out = {
        "id": node.get("id"),
        "handle": node.get("handle"),
        "displayName": node.get("displayName"),
        "type": node.get("type"),
    }
    for f in node.get("fields") or []:
        k = f.get("key")
        v = f.get("value")
        ref = f.get("reference")
        if ref:
            if "image" in ref:
                img = ref.get("image") or {}
                out[k] = {
                    "id": ref.get("id"),
                    "url": img.get("url"),
                    "alt": img.get("altText"),
                    "width": img.get("width"),
                    "height": img.get("height"),
                }
                continue
            if "handle" in ref and "type" in ref:
                out[k] = {
                    "id": ref.get("id"),
                    "handle": ref.get("handle"),
                    "type": ref.get("type"),
                }
                continue
        if v in ("true", "false"):
            out[k] = v == "true"
        else:
            out[k] = v
    return out


# -------- Series (event) --------

_SERIES_NODE_FRAGMENT = """
  id
  handle
  displayName
  type
  fields {
    key
    value
    reference {
      ... on MediaImage {
        id
        image { url altText width height }
      }
    }
  }
"""


def list_series(first: int = 100) -> list[dict]:
    """List all EventSeries metaobjects."""
    query = f"""
    query listSeries($first: Int!) {{
      metaobjects(type: "{SERIES_TYPE}", first: $first) {{
        edges {{ node {{ {_SERIES_NODE_FRAGMENT} }} }}
      }}
    }}
    """
    resp = shopify_gql(query, {"first": first})
    edges = (resp.get("data") or {}).get("metaobjects", {}).get("edges", [])
    return [_fields_to_dict(e["node"]) for e in edges]


def get_series(gid: str) -> Optional[dict]:
    query = f"""
    query getSeries($id: ID!) {{
      metaobject(id: $id) {{ {_SERIES_NODE_FRAGMENT} }}
    }}
    """
    resp = shopify_gql(query, {"id": gid})
    node = (resp.get("data") or {}).get("metaobject")
    return _fields_to_dict(node) if node else None


def create_series(fields: dict) -> dict:
    """Create a new EventSeries metaobject. `fields` keys map to metaobject field handles."""
    field_inputs = _build_field_inputs(fields)
    mutation = """
    mutation createSeries($metaobject: MetaobjectCreateInput!) {
      metaobjectCreate(metaobject: $metaobject) {
        metaobject { id handle displayName }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "metaobject": {
            "type": SERIES_TYPE,
            "fields": field_inputs,
        }
    }
    resp = shopify_gql(mutation, variables)
    result = (resp.get("data") or {}).get("metaobjectCreate") or {}
    errs = result.get("userErrors") or []
    if errs:
        raise RuntimeError(f"metaobjectCreate(series) errors: {errs}")
    return result.get("metaobject") or {}


def update_series(gid: str, fields: dict) -> dict:
    field_inputs = _build_field_inputs(fields)
    mutation = """
    mutation updateSeries($id: ID!, $metaobject: MetaobjectUpdateInput!) {
      metaobjectUpdate(id: $id, metaobject: $metaobject) {
        metaobject { id handle displayName }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "id": gid,
        "metaobject": {"fields": field_inputs},
    }
    resp = shopify_gql(mutation, variables)
    result = (resp.get("data") or {}).get("metaobjectUpdate") or {}
    errs = result.get("userErrors") or []
    if errs:
        raise RuntimeError(f"metaobjectUpdate(series) errors: {errs}")
    return result.get("metaobject") or {}


def delete_metaobject(gid: str) -> None:
    mutation = """
    mutation deleteMO($id: ID!) {
      metaobjectDelete(id: $id) {
        deletedId
        userErrors { field message code }
      }
    }
    """
    resp = shopify_gql(mutation, {"id": gid})
    errs = (resp.get("data") or {}).get("metaobjectDelete", {}).get("userErrors") or []
    if errs:
        raise RuntimeError(f"metaobjectDelete errors: {errs}")


# -------- Occurrence (event_occurence) --------

_OCC_NODE_FRAGMENT = """
  id
  handle
  displayName
  type
  fields {
    key
    value
    reference {
      ... on Metaobject {
        id
        handle
        type
        fields { key value }
      }
    }
  }
"""


def list_occurrences(first: int = 250) -> list[dict]:
    query = f"""
    query listOccurrences($first: Int!) {{
      metaobjects(type: "{OCCURRENCE_TYPE}", first: $first) {{
        edges {{ node {{ {_OCC_NODE_FRAGMENT} }} }}
      }}
    }}
    """
    resp = shopify_gql(query, {"first": first})
    edges = (resp.get("data") or {}).get("metaobjects", {}).get("edges", [])
    out = []
    for e in edges:
        node = _fields_to_dict(e["node"])
        # Flatten series ref into series_id/series_handle for client convenience
        series_ref = node.get("series")
        if isinstance(series_ref, dict):
            node["series_id"] = series_ref.get("id")
            node["series_handle"] = series_ref.get("handle")
            # series_ref already includes nested series fields if needed
        out.append(node)
    return out


def create_occurrence(fields: dict) -> dict:
    field_inputs = _build_field_inputs(fields)
    mutation = """
    mutation createOcc($metaobject: MetaobjectCreateInput!) {
      metaobjectCreate(metaobject: $metaobject) {
        metaobject { id handle displayName }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "metaobject": {
            "type": OCCURRENCE_TYPE,
            "fields": field_inputs,
        }
    }
    resp = shopify_gql(mutation, variables)
    result = (resp.get("data") or {}).get("metaobjectCreate") or {}
    errs = result.get("userErrors") or []
    if errs:
        raise RuntimeError(f"metaobjectCreate(occurrence) errors: {errs}")
    return result.get("metaobject") or {}


def update_occurrence(gid: str, fields: dict) -> dict:
    field_inputs = _build_field_inputs(fields)
    mutation = """
    mutation updateOcc($id: ID!, $metaobject: MetaobjectUpdateInput!) {
      metaobjectUpdate(id: $id, metaobject: $metaobject) {
        metaobject { id handle displayName }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "id": gid,
        "metaobject": {"fields": field_inputs},
    }
    resp = shopify_gql(mutation, variables)
    result = (resp.get("data") or {}).get("metaobjectUpdate") or {}
    errs = result.get("userErrors") or []
    if errs:
        raise RuntimeError(f"metaobjectUpdate(occurrence) errors: {errs}")
    return result.get("metaobject") or {}


# -------- Field inputs --------

def _build_field_inputs(fields: dict) -> list[dict]:
    """Convert a {key: value} dict into Shopify MetaobjectFieldInput list.

    Handles:
      - rich_text values as dict/list → JSON-encoded string
      - booleans → 'true'/'false'
      - money values: pass through as JSON string {"amount":"5.00","currency_code":"USD"}
      - skip None values (won't overwrite existing field with null)
    """
    out = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, bool):
            out.append({"key": k, "value": "true" if v else "false"})
        elif isinstance(v, (dict, list)):
            out.append({"key": k, "value": json.dumps(v)})
        else:
            out.append({"key": k, "value": str(v)})
    return out


def plain_text_to_rich(text: str) -> dict:
    """Convert plain text (with blank-line paragraph breaks) into Shopify rich text JSON."""
    if not text:
        return {"type": "root", "children": []}
    paragraphs = [p.strip() for p in text.replace("\r\n", "\n").split("\n\n") if p.strip()]
    children = []
    for p in paragraphs:
        # Within a paragraph, single \n becomes a literal space (no <br> support in our v1)
        flat = " ".join(line.strip() for line in p.split("\n") if line.strip())
        children.append({
            "type": "paragraph",
            "children": [{"type": "text", "value": flat}],
        })
    return {"type": "root", "children": children}


def rich_to_plain_text(rich_value: str) -> str:
    """Best-effort extract plain text from a Shopify rich text JSON value (round-trip for editing)."""
    if not rich_value:
        return ""
    try:
        tree = json.loads(rich_value) if isinstance(rich_value, str) else rich_value
    except Exception:
        return rich_value if isinstance(rich_value, str) else ""
    paragraphs = []

    def _walk(node):
        if not isinstance(node, dict):
            return ""
        t = node.get("type")
        if t == "text":
            return node.get("value", "")
        children = node.get("children") or []
        return "".join(_walk(c) for c in children)

    for child in (tree.get("children") or []):
        if child.get("type") in ("paragraph", "heading"):
            paragraphs.append(_walk(child))
        else:
            paragraphs.append(_walk(child))
    return "\n\n".join(p for p in paragraphs if p)


# -------- File upload (hero_image) --------

def upload_file_to_shopify(file_bytes: bytes, filename: str, mime_type: str) -> str:
    """Upload an image to Shopify Files and return the MediaImage GID.

    Two-step: stagedUploadsCreate → PUT bytes → fileCreate.
    """
    # Step 1: get staged upload target
    staged_mutation = """
    mutation staged($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets { url resourceUrl parameters { name value } }
        userErrors { field message }
      }
    }
    """
    staged_vars = {
        "input": [{
            "filename": filename,
            "mimeType": mime_type,
            "httpMethod": "POST",
            "resource": "IMAGE",
        }]
    }
    resp = shopify_gql(staged_mutation, staged_vars)
    staged = (resp.get("data") or {}).get("stagedUploadsCreate") or {}
    errs = staged.get("userErrors") or []
    if errs:
        raise RuntimeError(f"stagedUploadsCreate errors: {errs}")
    targets = staged.get("stagedTargets") or []
    if not targets:
        raise RuntimeError("stagedUploadsCreate returned no targets")
    target = targets[0]
    upload_url = target["url"]
    resource_url = target["resourceUrl"]
    params = {p["name"]: p["value"] for p in target.get("parameters", [])}

    # Step 2: POST file bytes to the staged target (Shopify uses S3-style form upload)
    files = {"file": (filename, file_bytes, mime_type)}
    r = requests.post(upload_url, data=params, files=files, timeout=60)
    if not r.ok:
        raise RuntimeError(f"staged upload failed: {r.status_code} {r.text[:200]}")

    # Step 3: register the uploaded file as a Shopify MediaImage
    create_mutation = """
    mutation registerFile($files: [FileCreateInput!]!) {
      fileCreate(files: $files) {
        files { ... on MediaImage { id image { url } } }
        userErrors { field message code }
      }
    }
    """
    create_vars = {
        "files": [{
            "alt": filename,
            "contentType": "IMAGE",
            "originalSource": resource_url,
        }]
    }
    resp = shopify_gql(create_mutation, create_vars)
    created = (resp.get("data") or {}).get("fileCreate") or {}
    errs = created.get("userErrors") or []
    if errs:
        raise RuntimeError(f"fileCreate errors: {errs}")
    out_files = created.get("files") or []
    if not out_files:
        raise RuntimeError("fileCreate returned no files")
    return out_files[0]["id"]
