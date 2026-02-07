# apps/dashboard/services/slabs/parsing.py
from dataclasses import dataclass
import re, html

# Map the left-hand label in your description to a canonical field
FIELD_MAP = {
    "cert number": "cert_number",
    "certification number": "cert_number",
    "year": "year",
    "ip": "ip",
    "set": "set_name",
    "card name": "card_name",
    "card number": "card_number",
    "no.": "card_number",
    "grade": "grade",
    "population": "population",
    "grading company": "grading_company",
    "company": "grading_company",
}

LINE_RE = re.compile(r"^\s*([A-Za-z\. ]+):\s*(.+?)\s*$", re.MULTILINE)

CARD_NO_FIRST_NUM_RE = re.compile(r"(\d{1,4})")             # grabs 026 from "026/140" or "#026"
POP_DIGITS_RE = re.compile(r"[^\d]")                        # strip commas etc.
GRADE_WITH_COMPANY_RE = re.compile(r"^(PSA|BGS|CGC)\s*[-:]?\s*(.+)$", re.IGNORECASE)  # "PSA 8" → comp=PSA, grade=8

def _strip_html(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    # normalize <br> to newlines; drop remaining tags
    s = re.sub(r"<\s*br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    return s

@dataclass
class SlabMeta:
    cert_number: str|None = None
    year: str|None = None
    ip: str|None = None
    set_name: str|None = None
    card_name: str|None = None
    card_number: str|None = None
    grade: str|None = None
    population: int|None = None
    grading_company: str|None = None  # if missing, we may infer from grade line

def parse_slab_meta(body_html: str) -> SlabMeta:
    text = _strip_html(body_html or "")
    meta = SlabMeta()

    # First pass: direct "Key: Value" lines
    for raw_key, raw_val in LINE_RE.findall(text):
        k = raw_key.strip().lower()
        v = raw_val.strip()
        field = FIELD_MAP.get(k)
        if not field:
            continue

        if field == "population":
            digits = POP_DIGITS_RE.sub("", v)
            setattr(meta, field, int(digits) if digits else None)
            continue

        if field == "card_number":
            # Extract first numeric token, zero-pad to 3 (your canon)
            m = CARD_NO_FIRST_NUM_RE.search(v.lstrip("#"))
            meta.card_number = m.group(1).zfill(3) if m else None
            continue

        if field == "grade":
            # Accept raw grade; company may be embedded ("PSA 8" or "CGC 9.5")
            m = GRADE_WITH_COMPANY_RE.match(v)
            if m:
                comp, g = m.group(1).upper(), m.group(2).strip()
                meta.grading_company = meta.grading_company or comp
                meta.grade = g
            else:
                meta.grade = v
            continue

        if field == "grading_company":
            setattr(meta, field, v.upper())
            continue

        setattr(meta, field, v)

    # Second pass: inference/fallbacks
    if not meta.grading_company and meta.grade:
        m = GRADE_WITH_COMPANY_RE.match(meta.grade)
        if m:
            meta.grading_company = m.group(1).upper()
            meta.grade = m.group(2).strip()

    # Normalize known fields
    if meta.card_number:
        meta.card_number = meta.card_number.lstrip("#").zfill(3)
    if meta.grading_company:
        meta.grading_company = meta.grading_company.upper()

    return meta

# Optional helper: build a normalized lookup key for downstream adapters
def slab_lookup_key(meta: SlabMeta) -> dict:
    """
    Minimal, normalized identity for vendor lookups.
    """
    return {
        "year": (meta.year or "").strip(),
        "set": (meta.set_name or "").strip(),
        "name": (meta.card_name or "").strip(),
        "number": (meta.card_number or "").strip(),
        "company": (meta.grading_company or "PSA").upper(),  # default PSA if unknown
        "grade": (meta.grade or "").strip(),
    }
