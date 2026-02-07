from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple, Optional
import re

PRICE_KEYS = (
    "soldPrice", "salePrice", "finalPrice",
    "price", "market", "avg", "median", "value", "amount"
)
TIME_KEYS = (
    "soldDate", "dateSold", "sold_at", "sold_at_date",
    "date", "soldAt", "timestamp", "ts", "t", "time"
)

def _to_float(x):
    try:
        if x is None or (isinstance(x, str) and not x.strip()):
            return None
        return float(x)
    except Exception:
        return None

def _to_time(x):
    if x is None: return None
    if isinstance(x, (int, float)):
        return int(x/1000) if x > 10**12 else int(x)
    if isinstance(x, str):
        s = x.strip()
        if not s: return None
        if s.isdigit(): return int(s)
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return s
    return x

def _looks_like_point(d: dict) -> bool:
    if not isinstance(d, dict): return False
    has_time = any(k in d for k in TIME_KEYS)
    has_price = any(k in d for k in PRICE_KEYS)
    if has_time and has_price:
        return True
    tp = d.get("tcgplayer") or d.get("ebay") or d.get("psa")
    if isinstance(tp, dict):
        if any(k in tp for k in PRICE_KEYS) and has_time:
            return True
    return False

def _collect_arrays(x):
    out, visited = [], set()
    def walk(v):
        vid = id(v)
        if vid in visited: return
        visited.add(vid)
        if isinstance(v, list):
            if any(isinstance(it, dict) and _looks_like_point(it) for it in v):
                out.append(v)
            for it in v:
                if isinstance(it, (list, dict)): walk(it)
        elif isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, (list, dict)): walk(vv)
    walk(x)
    uniq, seen = [], set()
    for arr in out:
        if id(arr) not in seen:
            uniq.append(arr); seen.add(id(arr))
    return uniq
def _grade_num(g):
    """Parse '9', '9.0', 'PSA 9', 'Mint 9' → 9.0 ; return None if unknown."""
    if g is None: return None
    s = str(g).upper()
    m = re.search(r'(\d+(?:\.\d)?)', s)
    return float(m.group(1)) if m else None

def pick_series(card: Dict[str, Any], *, company: str = "PSA",
                grade: Optional[str] = None, strict_grade: bool = True) -> List[Dict[str, float]]:
    pts = flatten_all_points_with_paths(card)

    def match_exact(r):
        return (r.get("vendor") == "ebay" and
                r.get("company","").upper() == (company or "").upper() and
                (grade is None or str(r.get("grade")) == str(grade)))

    graded = [{"t": r["t"], "p": r["p"]} for r in pts if match_exact(r)]
    if graded:
        return graded

    if not strict_grade:
        graded_any = [{"t": r["t"], "p": r["p"]}
                      for r in pts
                      if r.get("vendor") == "ebay" and r.get("company","").upper() == (company or "").upper()]
        if graded_any:
            return graded_any

        nm = [{"t": r["t"], "p": r["p"]}
              for r in pts
              if r.get("vendor") == "" and "Near Mint.history" in r.get("path","")]
        if nm:
            return nm

    return []

# ---------- Introspection helpers (paths, flatten, arrays inventory) ----------

PRICE_KEYS = (
    # ebay/graded common first:
    "soldPrice", "salePrice", "finalPrice", "sold", "winningBid", "amount",
    # aggregates / tcgplayer market:
    "price", "market", "avg", "median", "value"
)

TIME_KEYS = (
    # ebay/graded variants:
    "soldDate", "dateSold", "sold_at", "soldAt", "endedAt", "endTime",
    # generic:
    "date", "timestamp", "ts", "t", "time", "dateListed"
)

def _walk_collect_with_paths(x, path="$"):
    """
    Recursively collect arrays and dicts with their JSON path.
    Yields tuples: (path, value)
    """
    yield (path, x)
    if isinstance(x, dict):
        for k, v in x.items():
            yield from _walk_collect_with_paths(v, f"{path}.{k}")
    elif isinstance(x, list):
        for i, v in enumerate(x):
            yield from _walk_collect_with_paths(v, f"{path}[{i}]")

def collect_arrays_inventory(card: dict):
    """
    Return a list of (path, list_len, sample_type) for every list we find.
    """
    inv = []
    seen = set()
    for path, v in _walk_collect_with_paths(card):
        if isinstance(v, list) and id(v) not in seen:
            seen.add(id(v))
            inv.append((path, len(v), type(v[0]).__name__ if v else "empty"))
    return inv
def _ci_get(d: Dict[str, Any], key: str) -> Any:
    if not isinstance(d, dict):
        return None
    lk = {str(k).lower(): k for k in d.keys()}
    k = lk.get(key.lower())
    return d.get(k) if k is not None else None

def _to_epoch(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts / 1000) if ts > 2_000_000_000 else int(ts)
    s = str(ts).strip()
    if s.isdigit():
        val = int(s)
        return int(val / 1000) if val > 2_000_000_000 else val
    try:
        if s.endswith("Z"):
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        if len(s) == 10 and s.count("-") == 2:  # YYYY-MM-DD
            return int(datetime.fromisoformat(s + "T00:00:00+00:00").timestamp())
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None
def _grade_key(company: str | None, grade: str | int | None) -> str | None:
    if not company or not grade:
        return None
    if str(company).upper() == "PSA":
        return f"psa{str(grade).lower()}"
    return None

def get_graded_aggregate(card: dict, *, company: str = "PSA", grade: str | int | None = None):
    """
    Return best aggregate graded price for the card, if available, in priority:
      1) salesByGrade[grade].smartMarketPrice.price
      2) salesByGrade[grade].marketPrice7Day
      3) salesByGrade[grade].marketPriceMedian7Day
      4) salesByGrade[grade].averagePrice
    Returns dict like {"price": float, "method": "smartMarketPrice|marketPrice7Day|..."} or None.
    """
    ebay = card.get("ebay") or {}
    key = _grade_key(company, grade)
    if not key:
        return None
    sbg = (ebay.get("salesByGrade") or {}).get(key) or {}
    # 1) smartMarketPrice.price
    smp = sbg.get("smartMarketPrice") or {}
    if "price" in smp and smp.get("price") is not None:
        try:
            return {"price": float(smp["price"]), "method": "smartMarketPrice"}
        except Exception:
            pass
    # 2) marketPrice7Day
    for k in ("marketPrice7Day", "marketPriceMedian7Day", "averagePrice"):
        v = sbg.get(k)
        if v is not None:
            try:
                return {"price": float(v), "method": k}
            except Exception:
                continue
    return None

def _row_price(row: Dict[str, Any]) -> Optional[float]:
    for k in ("price", "p", "salePrice", "value", "market", "average", "sevenDayAverage"):
        v = row.get(k)
        if v is None:
            continue
        try:
            return float(v)
        except Exception:
            continue
    return None

def _row_time(row: Dict[str, Any]) -> Optional[int]:
    for k in ("timestamp", "t", "soldAt", "date"):
        if k in row:
            return _to_epoch(row[k])
    return None

def _split_grade_key(k: str) -> (Optional[str], Optional[str]):
    """
    'psa10' -> ('PSA', '10')
    'bgs9.5' or 'bgs9_5' -> ('BGS', '9.5')
    'cgc10' -> ('CGC', '10')
    """
    s = str(k).strip().lower().replace(" ", "")
    s = s.replace("_", ".")
    m = re.match(r"^([a-z]+)(\d+(?:\.\d+)?)$", s)
    if not m:
        return None, None
    co = {"psa": "PSA", "bgs": "BGS", "cgc": "CGC", "sgc": "SGC"}.get(m.group(1), m.group(1).upper())
    gr = m.group(2)
    return co, gr

def flatten_all_points_with_paths(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # 1) Ungraded TCGplayer priceHistory (conditions.*.history)
    ph = _ci_get(card, "priceHistory")
    if isinstance(ph, dict):
        conds = _ci_get(ph, "conditions")
        if isinstance(conds, dict):
            for cond_name, cond_block in conds.items():
                if not isinstance(cond_block, dict):
                    continue
                hist = _ci_get(cond_block, "history")
                if isinstance(hist, list):
                    for row in hist:
                        if not isinstance(row, dict):
                            continue
                        p = _row_price(row); t = _row_time(row)
                        if p is None or t is None:
                            continue
                        out.append({
                            "p": p, "t": t,
                            "company": "", "grade": "",
                            "vendor": "",
                            "path": f"$.priceHistory.conditions.{cond_name}.history",
                            "raw_time_key": next((k for k in ("timestamp","t","soldAt","date") if k in row), ""),
                            "raw_price_key": next((k for k in ("price","p","salePrice","value","market","average","sevenDayAverage") if k in row), ""),
                        })

    # 2) Graded eBay (v2 shape: ebay.priceHistory.{gradeKey}.{YYYY-MM-DD} -> {average,...})
    eb = _ci_get(card, "ebay")
    if isinstance(eb, dict):
        ph2 = _ci_get(eb, "priceHistory")
        if isinstance(ph2, dict):
            for grade_key, series in ph2.items():
                co, gr = _split_grade_key(grade_key)
                if not (co and gr) or not isinstance(series, dict):
                    continue
                for date_key, row in series.items():
                    if not isinstance(row, dict):
                        continue
                    t = _to_epoch(date_key)
                    if t is None:
                        continue
                    # prefer day's average; fallback to 7d average if provided
                    p = row.get("average")
                    if p is None:
                        p = row.get("sevenDayAverage")
                    try:
                        p = float(p) if p is not None else None
                    except Exception:
                        p = None
                    if p is None:
                        continue
                    out.append({
                        "p": p, "t": t,
                        "company": co, "grade": gr,
                        "vendor": "ebay",
                        "path": f"$.ebay.priceHistory.{grade_key}.{date_key}",
                        "raw_time_key": "date",
                        "raw_price_key": "average" if "average" in row else "sevenDayAverage",
                    })

        # (optional) legacy/alternate shapes: ebay.PSA.10.history etc.
        # If you still want those, keep your prior handler here.

    out.sort(key=lambda r: r["t"])
    return out
