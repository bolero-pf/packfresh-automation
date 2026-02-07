import re

_PREFIX = re.compile(r"^(?:tg|gg|svp|swshp|smp|xyp|bwp)\s*[-/]?\s*", re.IGNORECASE)
def normalize_number(raw: str|int|None) -> str|None:
    if raw is None: return None
    s = str(raw).strip()
    s = _PREFIX.sub("", s)
    s = s.lstrip("#").split("/",1)[0]
    s = s.lstrip("0") or "0"
    return s

# New: emergency guesser from titles if description didn’t have Card Number:
_TITLE_PATTERNS = [
    re.compile(r"\b(?:#|No\.?|Num\.?)\s*([0-9]{1,3}[a-z]?)\b", re.I),
    re.compile(r"\b(?:TG|GG)\s*[-/]?\s*([0-9]{1,3})\b", re.I),
    re.compile(r"\bSVP\s*[-/]?\s*([0-9]{1,3})\b", re.I),
    re.compile(r"\b([0-9]{1,3}[a-z]?)\b")  # last resort, keep short
]

def fallback_number_from_titles(*titles: str) -> str|None:
    blob = " ".join(t for t in titles if t)
    for pat in _TITLE_PATTERNS:
        m = pat.search(blob)
        if m:
            return normalize_number(m.group(1))
    return None

def canon_card_number(n: str | int) -> str | None:
    if n is None:
        return None
    s = str(n).strip().upper()
    if not s:
        return None
    # "4/102" -> "4", "004" -> "4"
    m = re.match(r"^([A-Z]*\d+)\s*/\s*\d+$", s)
    if m:
        s = m.group(1)
    if re.fullmatch(r"\d+", s):
        s = str(int(s))
    return s

def variants_for_match(n: str | int, denom: str | int | None = None):
    """
    Given input number (e.g., 4), return alternative string forms we might see in API.
    """
    c = canon_card_number(n)
    if c is None:
        return []
    out = {c}
    # zero-padded 3-digit form (004)
    if c.isdigit():
        out.add(f"{int(c):03d}")
        if denom:
            out.add(f"{int(c)}/{int(denom)}")
    return list(out)
