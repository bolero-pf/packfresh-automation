import json, re, pathlib
from typing import Dict, Iterable

_MAP_PATH = pathlib.Path(__file__).resolve().parents[2] / "data" / "ppt_set_map.json"
_norm_re = re.compile(r"[^a-z0-9]+")

def _norm(s: str) -> str:
    s = s.lower().strip()
    s = _norm_re.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()

def load_set_map() -> Dict[str, list[str]]:
    with open(_MAP_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_set_map(map_: Dict[str, list[str]]):
    with open(_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(map_, f, indent=2, ensure_ascii=False)

def to_ppt_set_id(set_name: str | None) -> str | None:
    if not set_name:
        return None
    canon = load_set_map()
    rev = {}
    for ppt_id, aliases in canon.items():
        for a in aliases:
            rev[_norm(a)] = ppt_id
    key = _norm(set_name)
    if key in rev:
        return rev[key]
    # small fallback: strip leading words
    key2 = re.sub(r"^(pokemon|pokémon|set|sv)\s+", "", key)
    return rev.get(key2)

def extend_aliases(ppt_set_id: str, aliases: Iterable[str]):
    m = load_set_map()
    cur = {a.lower(): 1 for a in m.get(ppt_set_id, [])}
    for a in aliases:
        a = a.strip()
        if a and a.lower() not in cur:
            m.setdefault(ppt_set_id, []).append(a)
    save_set_map(m)
