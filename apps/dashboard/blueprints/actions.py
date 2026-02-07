from flask import Blueprint, request, jsonify
from ..adapters.shopify_client import ShopifyClient
from ..services.shopify.variant import update_variant_price
bp = Blueprint("actions", __name__)

bp = Blueprint("actions", __name__)

@bp.post("/update_variants")
def update_variants():
    payload = request.get_json(force=True)  # [{id, price?, inventoryQuantity?}, ...]
    results, errors = [], []

    for row in payload:
        vid = row["id"]

        # PRICE: works now
        if "price" in row and row["price"] != "":
            resp = update_variant_price(vid, row["price"])
            ue = resp.get("userErrors") or []
            if ue:
                errors.append({"id": vid, "errors": ue})
            else:
                pv = resp["productVariant"]
                results.append({"id": pv["id"], "price": pv["price"]})

        # QTY: not wired yet (needs inventory APIs)
        if "inventoryQuantity" in row and row["inventoryQuantity"] is not None:
            errors.append({"id": vid, "errors": [{"message": "Quantity updates require inventory APIs (inventoryItemId + locationId)."}]})

    return jsonify({"ok": len(errors) == 0, "results": results, "errors": errors})