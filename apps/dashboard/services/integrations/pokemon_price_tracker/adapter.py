def build_request_batch(items, date_from, date_to):
    """
    items: list of dicts from iter_slab_variants_with_meta()
    date_from, date_to: ISO strings
    Returns payload list for PPT bulk call.
    """
    out = []
    for it in items:
        lu = it["lookup"]
        out.append({
            "year": lu["year"],
            "set": lu["set"],
            "name": lu["name"],
            "card_number": lu["number"],
            "grading_company": lu["company"],   # PSA/CGC/BGS
            "grade": lu["grade"],
            "date_from": date_from,
            "date_to": date_to,
            "client_ref": it["variant_id"],     # round-trip identifier
        })
    return out


def normalize_response(raw_rows):
    """
    raw_rows: list[dict] from PPT
    Return mapping: {client_ref: [ {date, price}, ... ]}
    """
    out = {}
    for row in raw_rows:
        ref = row.get("client_ref")
        sales = row.get("sales") or []   # adapt field names if docs show differently
        norm = []
        for s in sales:
            # adjust keys per actual docs; here assume {date, price}
            norm.append({
                "date": s["date"],
                "price": float(s["price"]),
            })
        out[ref] = norm
    return out

def normalize_bulk_response(rows: list[dict]) -> dict[str, list[dict]]:
    """
    -> { "sv95-26": [ {"date": "...", "price": float}, ... ], ... }
    """
    out = {}
    for card in rows:
        cid = card.get("id")
        hist = card.get("history") or []
        out[cid] = [{"date": h["date"], "price": float(h["price"])} for h in hist if "price" in h]
    return out