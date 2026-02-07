def build_card_map_for_set(ppt_client, set_slug: str) -> dict[tuple[str, str], str]:
    """
    returns { (set_slug, number_str) : card_id } for all cards in a set
    """
    data = ppt_client.list_cards(setId=set_slug, fetchAllInSet=True)
    rows = data.get("data", data)
    out = {}
    for c in rows:
        num = str(c.get("number") or "").lstrip("#").lstrip("0") or None
        cid = c.get("id")
        if num and cid:
            out[(set_slug, num)] = cid
    return out