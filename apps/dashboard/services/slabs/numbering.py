def _candidates_from_raw(raw: str):
    if not raw:
        return set()
    r = str(raw).strip()
    out = {r}

    compact = r.replace(" ", "").replace("-", "").lower()
    out.add(compact)

    # digits-only views
    digits = "".join(ch for ch in compact if ch.isdigit())
    if digits:
        out.add(digits.lstrip("0") or "0")
        out.add(digits.zfill(3))

    # letter suffix like 143a
    import re
    m = re.fullmatch(r"(\d+)([a-z])", compact)
    if m:
        d, tail = m.groups()
        out.add(f"{d}{tail}")
        out.add(f"{d.zfill(3)}{tail}")

    # subset prefixes (trainer galleries etc.)
    for pfx in ("tg","gg","sv","rc"):
        if compact.startswith(pfx):
            suf = compact[len(pfx):]
            out.add(f"{pfx}{suf}")
            if suf.isdigit():
                out.add(suf.lstrip("0") or "0")
                out.add(suf.zfill(2))
    return out

def index_set_by_number(rows):
    idx = {}
    for c in rows or []:
        raw = c.get("cardNumber") or c.get("number") or c.get("collectorNumber") or ""
        for k in _candidates_from_raw(raw):
            idx.setdefault(k, c)
    return idx

def canon_num(n: str):
    n = (n or "").strip()
    if not n:
        return n
    nn = (
        n.upper()
        .replace(" ", "")
        .replace("-", "")
        .replace("/", "")  # <— add this
        .replace("#", "")  # <— and this
    )

    # keep TG/GG/SV/RC prefix as-is; those can be keys in the index
    if nn[:2] in ("tg", "gg", "sv", "rc"):
        return nn
    import re
    m = re.fullmatch(r"(\d+)([a-z])", nn)
    if m:
        d, tail = m.groups()
        return f"{int(d)}{tail}"  # normalize leading zeros away
    # plain number
    digits = "".join(ch for ch in nn if ch.isdigit())
    return str(int(digits)) if digits else nn
