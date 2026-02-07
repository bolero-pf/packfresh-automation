from .setmap import to_ppt_set_id

def card_id_from_meta(meta) -> str | None:
    """
    meta: SlabMeta
    Return 'ppt_set_id-{number}' or None if unknown mapping.
    PPT expects the number without leading zeros.
    """
    ppt_set = to_ppt_set_id(meta.set_name)
    if not ppt_set or not meta.card_number:
        return None
    try:
        # strip leading zeros for PPT
        num = str(int(str(meta.card_number).lstrip("#")))
    except Exception:
        # if it isn't pure digits (rare), fall back to raw
        num = str(meta.card_number).lstrip("#")
    return f"{ppt_set}-{num}"
