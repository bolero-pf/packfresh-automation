import json, pathlib, re, unicodedata

_MAP = pathlib.Path(__file__).resolve().parents[2] / "data" / "ppt_set_slug_map.final.json"

def _norm(s: str) -> str:
    # normalize '&'→'and', smart quotes, accents, collapse spaces
    s = unicodedata.normalize("NFKD", (s or "")).lower()
    s = s.replace("&", "and")
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s
def apply_subset_slug(set_slug: str, set_name: str, number: str) -> str:
    n = (number or "").strip().replace(" ", "").replace("-", "").upper()
    if n.startswith("TG"):
        # Brilliant Stars, Silver Tempest, Lost Origin, etc. Trainer Gallery
        return f"{set_slug}-trainer-gallery"
    if n.startswith("GG"):
        # Crown Zenith only: Galarian Gallery
        name = (set_name or "").lower()
        if "crown zenith" in name or "swsh12pt5-crown-zenith" in set_slug:
            return f"{set_slug}-galarian-gallery"
    return set_slug

def _load_map() -> dict[str, str]:
    if not _MAP.exists():
        return {}
    raw = json.loads(_MAP.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return {_norm(k): v.strip() for k, v in raw.items() if isinstance(k, str) and isinstance(v, str) and v.strip()}
    if isinstance(raw, list):
        out = {}
        for row in raw:
            if not isinstance(row, dict): continue
            slug = (row.get("suggested_slug") or row.get("tcgPlayerId") or row.get("slug") or row.get("ppt_set_id") or "").strip()
            name = (row.get("set_name") or row.get("name") or row.get("psa") or row.get("psa_set") or "").strip()
            if slug and name:
                out[_norm(name)] = slug
            for alias in (row.get("aliases") or []):
                if isinstance(alias, str) and alias.strip():
                    out[_norm(alias)] = slug
        return out
    return {}

# PSA names often include extra noise like “(Sword & Shield series)”, or leading series
_DROP_PARENS_SERIES = re.compile(r"\s*\([^)]*series\)\s*$", re.I)
_LEAD_SERIES = re.compile(r"^(pokemon|pok mon|sword and shield|sun and moon|scarlet and violet)\s+", re.I)

# add near top
_ALIAS_CANON = {
    "game": "base set",                 # PSA "Game" == WotC Base Set
    "go": "pokemon go",                 # plain "Go" -> "Pokemon GO"
    "sv black star": "scarlet and violet promos",
    "black star": "wotc black star promos",
}

# update _expand_aliases()
def _expand_aliases(key: str) -> list[str]:
    key = key.strip()
    cands = [key]

    # canonical one-word aliases
    if key in _ALIAS_CANON:
        cands.append(_ALIAS_CANON[key])

    # handle leading shorthand codes
    m = re.match(r"^(ssp|obf|tef|jtg|scr|pal|paf)\b\s*", key)
    if m:
        repl = {
            "ssp": "surging sparks",
            "obf": "obsidian flames",
            "tef": "temporal forces",
            "jtg": "journey together",
            "scr": "stellar crown",
            "pal": "paldea evolved",
            "paf": "paldean fates",
        }[m.group(1)]
        # replace just the code with full name
        cands.append(re.sub(r"^(?:ssp|obf|tef|jtg|scr|pal|paf)\b\s*", repl + " ", key, count=1))

    # drop leading series names
    no_lead = _norm(_LEAD_SERIES.sub("", key))
    if no_lead and no_lead != key:
        cands.append(no_lead)

    # drop parenthetical series suffix
    no_paren = _norm(_DROP_PARENS_SERIES.sub("", key))
    if no_paren and no_paren not in cands:
        cands.append(no_paren)

    # de-dupe
    seen, uniq = set(), []
    for k in cands:
        k = _norm(k)
        if k not in seen:
            uniq.append(k); seen.add(k)
    return uniq

def to_set_slug(psa_set_name: str | None, year: str | int | None = None) -> str | None:
    if not psa_set_name:
        return None
    raw = psa_set_name
    key = _norm(raw)

    # dynamic McDonald's
    if "mcdonald" in key and year:
        y = str(year).strip()
        if re.fullmatch(r"\d{4}", y):
            return f"mcdonalds-promos-{y}"

    m = _load_map()

    # direct
    if key in m:
        return m[key]

    # try alias candidates
    for cand in _expand_aliases(key):
        if cand in m:
            return m[cand]

    return None



def map_path() -> str:
    return str(_MAP)
