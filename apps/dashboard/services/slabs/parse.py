# apps/dashboard/services/slabs/parse.py
import html, re, unicodedata

# normalize text
def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = s.replace("&", "and")
    s = re.sub(r"[’`]", "'", s)
    return re.sub(r"\s+", " ", s).strip()

_TAGS = re.compile(r"<[^>]+>")
_BR = re.compile(r"(?i)<\s*br\s*/?>")
_PARENS_SERIES = re.compile(r"\s*\([^)]*series\)\s*$", re.I)

# main entry
def parse_slab_fields(body_html: str, title: str = "", variant_title: str = "", sku: str = "") -> dict:
    # 1) strip HTML → plain text
    txt = body_html or ""
    txt = _BR.sub("\n", txt)
    txt = html.unescape(txt)
    txt = _TAGS.sub(" ", txt)
    txt = _norm(txt)

    # 2) line-wise search
    lines = [l.strip() for l in txt.splitlines() if l.strip()]

    # patterns (tolerant)
    rx = {
        "cert": [
            r"\bcert(?:ificate)?\s*(?:number|no\.?|#)?\s*:\s*([0-9]{6,})",
            r"\bcert(?:ificate)?\s*(?:number|no\.?|#)?\s*([0-9]{6,})",
        ],
        "year": [r"\byear\s*:\s*(\d{4})"],
        "ip": [r"\bip\s*:\s*([A-Za-z0-9 '&\-]+)"],
        "set": [
            # Stop BEFORE the next label (card name/number/grade/year/ip/population) or EOL
            r"\bset\s*:\s*([A-Za-z0-9 &'–\-\/]+?)(?=\s+(?:card\s*name|card\s*(?:number|no\.?|#)|grade|population|ip|year)\b|$)",
            r"\bset0\s*:\s*([A-Za-z0-9 &'–\-\/]+?)(?=\s+(?:card\s*name|card\s*(?:number|no\.?|#)|grade|population|ip|year)\b|$)",
            r"\bset\s*0\s*:\s*([A-Za-z0-9 &'–\-\/]+?)(?=\s+(?:card\s*name|card\s*(?:number|no\.?|#)|grade|population|ip|year)\b|$)",
        ],

        "card_number": [
            r"\bcard\s*(?:number|no\.?|#)\s*:\s*([A-Za-z]*\s*\d{1,3}[A-Za-z]?)",
            r"\bcollector\s*number\s*:\s*([A-Za-z]*\s*\d{1,3}[A-Za-z]?)",
            r"\b(?:#|no\.?)\s*([0-9]{1,3}[A-Za-z]?)\b",  # e.g. "#026"
        ],
        "grade": [
            r"\bgrade\s*:\s*([0-9]+(?:\.[0-9])?)",
            r"\bpsa\s*([0-9]+(?:\.[0-9])?)\b",
            r"\bcgc\s*([0-9]+(?:\.[0-9])?)\b",
            r"\bbgs\s*([0-9]+(?:\.[0-9])?)\b",
        ],
        "company": [r"\b(company|grader|grading)\s*:\s*(psa|cgc|bgs)\b", r"\b(psa|cgc|bgs)\s*[0-9]"],
        "population": [r"\bpop(?:ulation)?\s*:\s*([0-9]+)"],
        "card_name": [r"\bcard\s*name\s*:\s*(.+)$"],
    }

    out = {
        "cert": None, "year": None, "ip": None, "set": None, "card_number": None,
        "grade": None, "company": None, "population": None, "card_name": None,
    }

    def _find(patterns):
        for p in patterns:
            rxp = re.compile(p, re.I)
            for l in lines:
                m = rxp.search(l)
                if m:
                    return m.group(1).strip()
        return None

    for k, pats in rx.items():
        out[k] = _find(pats)

    # cleanup set: drop "(Sword & Shield series)" suffix
    if out["set"]:
        out["set"] = _PARENS_SERIES.sub("", out["set"]).strip()
        out["set"] = re.sub(r"\bcard\s*name\b.*$", "", out["set"], flags=re.I).strip()

    # normalize company to psa/cgc/bgs
    if out["company"]:
        m = re.search(r"(psa|cgc|bgs)", out["company"], re.I)
        if m:
            out["company"] = m.group(1).upper()
        else:
            out["company"] = None

    # fallback: try to pull number from title/variant/sku
    from .cardnum import normalize_number, fallback_number_from_titles
    if not out["card_number"]:
        out["card_number"] = fallback_number_from_titles(title, variant_title, sku)

    if out["card_number"]:
        out["card_number"] = normalize_number(out["card_number"])

    return out
