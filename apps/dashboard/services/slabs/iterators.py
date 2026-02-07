from __future__ import annotations
from typing import Dict, Any, Iterator
from collections.abc import Mapping

from .parsing import parse_slab_meta, slab_lookup_key
from .ids import card_id_from_meta
from ..shopify.products import iter_products_by_tag
from .parse import parse_slab_fields  # tolerant parser
def _norm_tcg_id(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, list):
        for v in val:
            n = _norm_tcg_id(v)
            if n is not None:
                return n
        return None
    s = str(val).strip()
    # stringified list: ["12345"]
    if s.startswith("[") and s.endswith("]"):
        s = s.strip("[]").replace('"', "").replace("'", "").strip()
    # allow commas/spaces
    s = s.replace(",", "").strip()
    return int(s) if s.isdigit() else None

_V2_REQUIRED = ("set", "number")

def _meta_to_dict(m) -> Dict[str, Any]:
    if m is None:
        return {}
    if isinstance(m, Mapping):
        return dict(m)
    out = {}
    for attr in ("set_name", "year", "ip", "card_name", "population"):
        if hasattr(m, attr):
            out[attr] = getattr(m, attr)
    return out

def iter_slab_variants_with_meta(shopify_client, *, tag: str = "slab") -> Iterator[Dict[str, Any]]:
    """Yields one dict per variant (generator)."""
    for p in iter_products_by_tag(shopify_client, tag=tag) or []:
        body_html = p.get("bodyHtml") or ""
        product_title = p.get("title") or ""

        # NEW: product-level tcg metafield (singular)
        p_tcg = ((p.get("tcg") or {}) or {}).get("value")
        p_tcg_norm = _norm_tcg_id(p_tcg)

        base_meta_obj = parse_slab_meta(body_html) or None
        base_meta = _meta_to_dict(base_meta_obj)
        base_lookup = slab_lookup_key(base_meta_obj) or {}

        # v1 cardId (kept but not required for v2 flow)
        cid = card_id_from_meta(base_meta_obj)
        if cid:
            base_lookup["cardId"] = cid

        # If tcgplayer_id is on the product, seed it into base_lookup
        if p_tcg_norm:
            base_lookup["tcgplayer_id"] = p_tcg_norm

        for v_edge in (p.get("variants") or {}).get("edges", []) or []:
            v = v_edge["node"]
            variant_title = v.get("title") or ""
            sku = v.get("sku") or ""

            # NEW: variant-level tcg metafield (singular)
            v_tcg = ((v.get("tcg") or {}) or {}).get("value")
            v_tcg_norm = _norm_tcg_id(v_tcg)

            fields = parse_slab_fields(
                body_html=body_html,
                title=product_title,
                variant_title=variant_title,
                sku=sku,
            )

            meta = {
                "set_name":   fields.get("set")         or base_meta.get("set_name"),
                "year":       fields.get("year")        or base_meta.get("year"),
                "ip":         fields.get("ip")          or base_meta.get("ip"),
                "card_name":  fields.get("card_name")   or base_meta.get("card_name"),
                "population": fields.get("population")  or base_meta.get("population"),
            }

            lookup = dict(base_lookup)
            if meta.get("set_name"):
                lookup["set"] = meta["set_name"]
            if fields.get("card_number"):
                lookup["number"] = fields["card_number"]
            if fields.get("company"):
                lookup["company"] = fields["company"]
            if fields.get("grade"):
                lookup["grade"] = fields["grade"]

            # Prefer variant tcgplayer_id over product-level if present
            if v_tcg_norm:
                lookup["tcgplayer_id"] = v_tcg_norm

            missing = tuple(k for k in _V2_REQUIRED if not lookup.get(k))
            ready = len(missing) == 0

            price_val = v.get("price")
            try:
                current_price = float(price_val) if price_val is not None else None
            except Exception:
                current_price = None

            # IMPORTANT: expose metafields for the reader
            # - variant singular metafield at top-level key "metafield"
            # - product wrapper with singular metafield at "product.metafield"
            yield {
                "product_id": p["id"],
                "product_title": product_title,
                "product": {  # minimal stub for _read_tcgplayer_id_from_variant
                    "id": p["id"],
                    "metafield": {"key": "tcgplayer_id", "value": str(p_tcg_norm)} if p_tcg_norm else None,
                },
                "variant_id": v["id"],
                "variant_title": variant_title,
                "sku": sku,
                "current_price": current_price,
                "body_html": body_html,
                "meta": meta,
                "lookup": lookup,
                "ready": ready,
                "missing": missing,
                "metafield": {"key": "tcgplayer_id", "value": str(v_tcg_norm)} if v_tcg_norm else None,
            }

