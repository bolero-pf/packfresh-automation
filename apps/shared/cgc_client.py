"""
cgc_client.py — CGC cert data via the open ccg-ops population API.

CGC has no developer API for *cert* lookups, and the cert page
(https://www.cgccards.com/certlookup/<cert>/) sits behind a Cloudflare
*managed* challenge that detects headless browsers and stalls forever. We
spent a lot of effort trying to beat that from Railway's datacenter IP and
it isn't reliable, so we don't try.

Instead we lean on a fact discovered by inspecting the live page: the cert
page server-renders the cert→collectible mapping into an Angular bootstrap,
e.g.  ng-init="$popctrl.init('00519309', '9', 'G')"  — and everything else
(name, set, number, year, variant, full population) comes from a completely
OPEN, un-gated JSON API keyed by that collectibleID:

    https://production.api.aws.ccg-ops.com
        /api/cards/research/trading-cards/population/collectible/<collectibleID>

So the operator (whose real browser clears Cloudflare trivially) grabs the
collectibleID once per slab, pastes it in, and the server does the rest with
a plain HTTP call — no browser, no Cloudflare, no Selenium. The grade comes
from intake (`grade_value`), so the collectibleID is all we need.

Return shape mirrors PSA's PSACert dict so shared/psa_client.py's title /
description / tags builders consume CGC data with no per-grader branches:
    {
      "CertNumber":         str,
      "Year":               str,
      "Subject":            str,    # card name
      "Brand":              str,    # set / group
      "Variety":            str,    # variant text
      "CardNumber":         str,
      "CardGrade":          str,    # e.g. "9"
      "GradeDescription":   str,    # e.g. "CGC 9.0"
      "TotalPopulation":    int|None,
      "PopulationHigher":   int|None,
      "PopulationAtGrade":  int|None,
    }
"""

import re
import time
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Open, un-gated JSON population API (no Cloudflare). Keyed by collectibleID.
CGC_POP_API_TPL = (
    "https://production.api.aws.ccg-ops.com"
    "/api/cards/research/trading-cards/population/collectible/{cid}"
)

# In-process cache — preview + push hit this for the same slab; don't double
# call the API. Keyed by cert_number when known, else "cid:<collectibleID>".
_cgc_cert_cache:   dict[str, dict] = {}
_cgc_image_cache:  dict[str, list] = {}
_cgc_cache_times:  dict[str, float] = {}
_CGC_CACHE_TTL = 7200  # 2 hours

# CGC grade ladder, lowest → highest, mapping a grade to its population_* JSON
# field. Lets us report "population at this grade" and sum everything above it
# for "population higher".
_GRADE_LADDER = [
    ("Authentic Altered", "population_AA"),
    ("Authentic",         "population_AU"),
    ("1.0", "population_1_0"), ("1.5", "population_1_5"),
    ("2.0", "population_2_0"), ("2.5", "population_2_5"),
    ("3.0", "population_3_0"), ("3.5", "population_3_5"),
    ("4.0", "population_4_0"), ("4.5", "population_4_5"),
    ("5.0", "population_5_0"), ("5.5", "population_5_5"),
    ("6.0", "population_6_0"), ("6.5", "population_6_5"),
    ("7.0", "population_7_0"), ("7.5", "population_7_5"),
    ("8.0", "population_8_0"), ("8.5", "population_8_5"),
    ("9.0", "population_9_0"), ("9.5", "population_9_5"),
    ("Gem Mint 10", "population_GemMint10"),
    ("Pristine 10", "population_Pristine10"),
    ("Perfect 10",  "population_Perfect10"),
]


class CGCNotFound(Exception):
    pass


class CGCScrapeFailed(Exception):
    """Kept for name compatibility with callers; raised on any CGC failure."""
    pass


def _cgc_cache_valid(key: str) -> bool:
    return (key in _cgc_cache_times
            and (time.time() - _cgc_cache_times[key]) < _CGC_CACHE_TTL)


# ══════════════════════════════════════════════════════════════════════════════
# collectibleID parsing
# ══════════════════════════════════════════════════════════════════════════════

def normalize_collectible_id(raw: str) -> str:
    """Pull the collectibleID out of whatever the operator pasted.

    Forgiving by design — accepts the bare id, the Angular init() call, or the
    population API URL:
        00519309
        $popctrl.init('00519309', '9', 'G')
        https://production.api.aws.ccg-ops.com/.../collectible/00519309
    Returns "" if nothing id-like is found.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    # init('00519309', ...) — first quoted token is the collectibleID
    m = re.search(r"init\(\s*'([^']+)'", s)
    if m:
        return m.group(1).strip()
    # .../collectible/00519309
    m = re.search(r"/collectible/(\d+)", s)
    if m:
        return m.group(1)
    # bare id (digits, leading zeros preserved)
    if re.fullmatch(r"\d+", s):
        return s
    # last resort: longest digit run
    m = re.search(r"\d{4,}", s)
    return m.group(0) if m else ""


# ══════════════════════════════════════════════════════════════════════════════
# Population API + data build
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_population(collectible_id: str) -> Optional[dict]:
    """Call the open ccg-ops population API for a collectibleID. None on error."""
    if not collectible_id:
        return None
    url = CGC_POP_API_TPL.format(cid=collectible_id)
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.cgccards.com",
            "Referer": "https://www.cgccards.com/",
        })
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"CGC population API failed cid={collectible_id}: {e}")
        return None


def _grade_ladder_index(grade: str) -> Optional[int]:
    """Map a CGC grade (+ optional designation) to its `_GRADE_LADDER` index."""
    g = (grade or "").strip().lower()
    if not g:
        return None
    m = re.search(r"\d+(?:\.\d)?", g)
    num = m.group(0) if m else ""
    is_ten = num in ("10", "10.0") or "pristine" in g or "perfect" in g or "gem mint" in g
    if is_ten:
        # 10s split into Gem Mint / Pristine / Perfect. Designation word wins;
        # default to the common Gem Mint 10 (also the plain "10" case).
        if "perfect" in g:
            field = "population_Perfect10"
        elif "pristine" in g:
            field = "population_Pristine10"
        else:
            field = "population_GemMint10"
    elif g.startswith("auth"):
        field = "population_AU"
    elif num:
        if "." not in num:
            num += ".0"
        field = "population_" + num.replace(".", "_")
    else:
        return None
    for i, (_lbl, f) in enumerate(_GRADE_LADDER):
        if f == field:
            return i
    return None


def _build_cert_data(pop: dict, grade: str, cert_number: str) -> dict:
    """Build a PSACert-shaped dict from a population API response + the grade."""
    fields: dict[str, str] = {
        "CertNumber": cert_number or "",
        "Subject":    (pop.get("name") or "").strip(),
        "Brand":      ((pop.get("group") or {}).get("name") or "").strip(),
        "Year":       str(pop.get("cardYear") or "").strip(),
        "CardNumber": (pop.get("cardNumber") or "").strip(),
        "Variety":    (pop.get("variant") or "").strip(),
        "CardGrade":  (grade or "").strip(),
        "GradeDescription": "",
    }
    total = pop.get("population_Total")
    populations: dict[str, Optional[int]] = {
        "TotalPopulation":  total if isinstance(total, int) else None,
        "PopulationHigher": None,
        "PopulationAtGrade": None,
    }

    idx = _grade_ladder_index(grade)
    if idx is not None:
        label, field = _GRADE_LADDER[idx]
        at = pop.get(field)
        populations["PopulationAtGrade"] = at if isinstance(at, int) else None
        higher = 0
        for j in range(idx + 1, len(_GRADE_LADDER)):
            v = pop.get(_GRADE_LADDER[j][1])
            if isinstance(v, int):
                higher += v
        populations["PopulationHigher"] = higher
        fields["GradeDescription"] = f"CGC {label}"
    if not fields["GradeDescription"]:
        fields["GradeDescription"] = f"CGC {grade}".strip()

    out = {**fields, **populations}
    logger.info(
        f"CGC cert={cert_number!r} grade={grade!r} "
        f"subject={fields['Subject']!r} set={fields['Brand']!r} "
        f"year={fields['Year']!r} card_no={fields['CardNumber']!r} "
        f"total_pop={populations['TotalPopulation']!r} "
        f"at_grade={populations['PopulationAtGrade']!r} "
        f"higher={populations['PopulationHigher']!r}"
    )
    return out


# ══════════════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════════════

def get_cgc_data_by_collectible(collectible_id: str, grade: str,
                                cert_number: str = "") -> dict:
    """Fetch + build CGC cert data from an operator-supplied collectibleID.

    `grade` comes from intake (the operator graded the slab on entry). Caches
    under both the collectibleID and cert_number (when given) so push's
    `get_cgc_data(cert_number)` lookup hits without re-calling the API.
    Raises CGCScrapeFailed if the id is unusable or the API returns nothing.
    """
    cid = normalize_collectible_id(collectible_id)
    if not cid:
        raise CGCScrapeFailed("CGC card ID is empty or unrecognizable")

    cid_key = f"cid:{cid}"
    if cid_key in _cgc_cert_cache and _cgc_cache_valid(cid_key):
        data = _cgc_cert_cache[cid_key]
    else:
        pop = _fetch_population(cid)
        if not pop:
            raise CGCScrapeFailed(
                f"CGC population API returned nothing for card ID {cid}"
            )
        data = _build_cert_data(pop, grade, cert_number)
        _store(cid_key, data)

    # Re-key under the cert so the push path (which only knows the cert) finds it.
    if cert_number:
        _store(cert_number, {**data, "CertNumber": cert_number})
    return _cgc_cert_cache.get(cert_number) if cert_number else data


def _store(key: str, data: dict) -> None:
    _cgc_cert_cache[key]  = data
    _cgc_image_cache[key] = []
    _cgc_cache_times[key] = time.time()


def get_cgc_data(cert_number: str) -> dict:
    """Cache-only cert lookup (no browser). Populated by a prior
    `get_cgc_data_by_collectible(..., cert_number=...)` in this worker.

    Raises CGCScrapeFailed on a miss — callers (push) treat CGC enrichment as
    best-effort and fall back to the TCGplayer card name.
    """
    cert_number = (cert_number or "").strip()
    if cert_number in _cgc_cert_cache and _cgc_cache_valid(cert_number):
        return _cgc_cert_cache[cert_number]
    raise CGCScrapeFailed(
        f"CGC cert {cert_number}: no CGC card ID looked up yet (provide it in "
        f"the cert-entry panel)"
    )


def get_cgc_images(cert_number: str) -> list[str]:
    """CGC slab images aren't available via the open API — return cached/empty.

    (The slab scans live only on the CF-gated cert page; we don't fetch it.)
    """
    cert_number = (cert_number or "").strip()
    return _cgc_image_cache.get(cert_number, [])


# ══════════════════════════════════════════════════════════════════════════════
# PSACert-shape dispatcher (used by shared/psa_client.push_graded_slab)
# ══════════════════════════════════════════════════════════════════════════════

def get_grader_data(grade_company: str, cert_number: str) -> Optional[dict]:
    """Dispatch by grade_company. CGC is cache-only; returns None on miss."""
    if (grade_company or "").upper() == "CGC":
        try:
            return get_cgc_data(cert_number)
        except Exception as e:
            logger.warning(f"CGC data unavailable for {cert_number}: {e}")
            return None
    # PSA + others live in psa_client and are called directly from there
    return None


def get_grader_images(grade_company: str, cert_number: str) -> list[str]:
    if (grade_company or "").upper() == "CGC":
        return get_cgc_images(cert_number)
    return []
