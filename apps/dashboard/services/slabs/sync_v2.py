# --- keep these existing imports at top of your file ---
from ..shopify.variant import update_variant_price
from .iterators import iter_slab_variants_with_meta
from .slugmap import to_set_slug
from ..integrations.pokemon_price_tracker.client import PPTClient, PPTError
from ..pricing.extract_v2 import pick_series
from ..pricing.strategies import smart_price, decide_update
from flask import current_app
from .iterators import _norm_tcg_id


from datetime import datetime, timezone
import time, requests
# ---- numbering helpers (import or local fallback) ----
try:
    from .numbering import index_set_by_number, canon_num  # use your existing utils if present
except Exception:
    import re

    def canon_num(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        s_up = (
            s.upper()
            .replace(" ", "")
            .replace("-", "")
            .replace("/", "")  # <— add this
            .replace("#", "")  # <— and this
        )

        # Pure digits -> strip leading zeros
        if s_up.isdigit():
            return str(int(s_up))
        # TG/GG/RC + digits (with optional leading zeros) -> normalize to e.g. TG3
        m = re.match(r'^(TG|GG|RC)(0*)(\d+)$', s_up)
        if m:
            pref = m.group(1)
            digits = str(int(m.group(3)))
            return f"{pref}{digits}"
        return s_up

    def index_set_by_number(rows):
        """
        Build a number -> card index with useful aliases:
        - 4, 04, 004
        - TG3, TG03, TG003 (also GG/RC)
        - raw/compact forms
        """
        idx = {}
        for c in rows or []:
            raw = c.get("cardNumber") or c.get("number") or c.get("collectorNumber") or ""
            compact = (
                (raw or "")
                .upper()
                .replace(" ", "")
                .replace("-", "")
                .replace("/", "")  # <— add
                .replace("#", "")  # <— add
            )
            if not compact:
                continue

            # Always map the compact raw
            idx[compact] = c

            # Digits: add unpadded + padded variants
            if compact.isdigit():
                d = str(int(compact))
                idx[d] = c
                idx[compact.zfill(2)] = c
                idx[compact.zfill(3)] = c
                continue

            # TG/GG/RC + digits: add unpadded + 2/3-digit padded variants
            m = re.match(r'^(TG|GG|RC)(0*)(\d+)$', compact)
            if m:
                pref = m.group(1)
                d = str(int(m.group(3)))
                idx[f"{pref}{d}"] = c
                idx[f"{pref}{d.zfill(2)}"] = c
                idx[f"{pref}{d.zfill(3)}"] = c
                continue

            # Fallback: also index normalized canon form
            idx[canon_num(raw)] = c

        return idx




# ---------- TCGplayer ID (PRODUCT-level metafield) ----------
import statistics as stats
_MAX_UP_REL = 0.5   # 50%
_MAX_UP_ABS = 300.0 # $300
_MIN_RECENT_FOR_MED = 3     # need at least 3 recent sales to trust median14
_MIN_RECENT_FOR_Q75 = 6     # need at least 6 to trust 75th percentile
_TRIM_FRACTION_60D = 0.10   # 10% trim on 60d window
_SURGE_RATIO = 1.40         # recent median must be ≥ 1.4x baseline to treat as surge
_UPWARD_BIAS_CAP = 1.35     # hard cap over baseline unless recent volume strong
_STRONG_RECENT_N = 8        # "strong recent volume" threshold
def _get(card: dict, *keys, default=""):
    for k in keys:
        v = (card or {}).get(k)
        if v not in (None, "", []):
            return v
    return default

def _card_basics(card: dict) -> dict:
    return {
        "card_name": _get(card, "name", "cardName"),
        "set":       _get(card, "setName", "set_name", "set"),
        "num":       _get(card, "cardNumber", "number", "collectorNumber"),
    }

def _guess_from_item(it: dict) -> dict:
    meta = it.get("meta") or {}
    lookup = it.get("lookup") or {}
    return {
        "card_name": (meta.get("card_name") or it.get("variant_title") or it.get("product_title") or "").strip(),
        "set":       (meta.get("set_name") or "").strip(),
        "num":       (lookup.get("number") or "").strip(),
    }
def _huge_upward(current: float | None, target: float) -> bool:
    if current is None: return False
    inc = target - current
    if inc <= 0: return False
    rel = inc / max(current, 0.01)
    return (inc >= _MAX_UP_ABS) or (rel >= _MAX_UP_REL)
def _recent_window(pts: list[dict], days: int) -> list[dict]:
    if not pts: return []
    cutoff = int(time.time()) - days*86400
    return [p for p in pts if p["timestamp"] >= cutoff]

def _median(values: list[float]) -> float | None:
    try:
        return float(stats.median(values)) if values else None
    except Exception:
        return None

def _quantile(values: list[float], q: float) -> float | None:
    if not values: return None
    try:
        idx = max(0, min(len(values)-1, int(round(q*(len(values)-1)))))
        return float(sorted(values)[idx])
    except Exception:
        return None
def _trim(values, frac):
    if not values or frac <= 0: return list(values)
    s = sorted(values)
    k = int(len(s) * frac)
    if k == 0 or 2*k >= len(s): return s
    return s[k:len(s)-k]
def _robust_ebay_target(card: dict, company: str | None, grade: str | None, *, half_life: float, base_days:int=60):
    pts_all = _points_from_ebay_daily(card, company, grade)
    if len(pts_all) < 2:
        return None, None, {"why": "no_series"}

    # 60d baseline
    pts60 = _recent_window(pts_all, base_days)
    if len(pts60) < 2:
        return None, None, {"why": f"sparse_series_{base_days}d"}

    ema60 = smart_price(pts60, half_life_days=half_life, rounding=False)
    vals60 = [p["price"] for p in pts60]
    med60 = _median(_trim(vals60, _TRIM_FRACTION_60D))  # trimmed 60d median

    # Baseline = max(EMA60, trimmed median60)
    baseline = max(ema60, med60 if med60 is not None else 0.0)
    base_src = "ebay_ema" if baseline == ema60 else "ebay_trimmed_median60"

    # Recent 14d stats (only if enough points)
    pts14 = _recent_window(pts60, 14)
    vals14 = [p["price"] for p in pts14]
    n14 = len(vals14)

    med14 = _median(vals14) if n14 >= _MIN_RECENT_FOR_MED else None
    q75_14 = _quantile(vals14, 0.75) if n14 >= _MIN_RECENT_FOR_Q75 else None

    chosen = baseline
    chosen_src = base_src
    surge = False
    cap_used = None

    # Consider using recent median (surge) only with enough recent sales
    if med14 is not None and med14 >= _SURGE_RATIO * baseline:
        surge = True
        chosen = med14
        chosen_src = "ebay_recent_median14_surge"

    # Otherwise, consider a gentle nudge upward to q75 only with decent volume
    elif q75_14 is not None and q75_14 > baseline:
        chosen = q75_14
        chosen_src = "ebay_recent_q75_14"

    # Cap aggressive upward jumps unless recent volume is strong
    if chosen > baseline and n14 < _STRONG_RECENT_N:
        limit = baseline * _UPWARD_BIAS_CAP
        if chosen > limit:
            cap_used = limit
            chosen = limit
            chosen_src += "_capped"

    dbg = {
        "ema": ema60,
        "med60_trim": med60,
        "baseline": baseline,
        "med14": med14,
        "q75_14": q75_14,
        "len_all": len(pts_all),
        "len_60d": len(pts60),
        "len_14d": n14,
        "surge": surge,
        "cap_used": cap_used,
    }
    return float(chosen), chosen_src, dbg


def _best_ebay_point_estimate(card: dict, company: str | None, grade: str | None) -> tuple[float | None, str | None]:
    bucket = _find_ebay_grade_bucket(card, company, grade)
    if not isinstance(bucket, dict): return (None, None)
    smp = (bucket.get("smartMarketPrice") or {}).get("price")
    if smp is not None:  return float(smp), "ebay_smart_market"
    mp7 = bucket.get("marketPrice7Day")
    if mp7 is not None:  return float(mp7), "ebay_mp7"
    med7 = bucket.get("marketPriceMedian7Day")
    if med7 is not None: return float(med7), "ebay_mp7_median"
    return (None, None)

import re
def _grade_key_variants(company: str | None, grade: str | int | float | None) -> list[str]:
    """
    Build likely salesByGrade/priceHistory keys, e.g. ['psa10', 'psa10_0'] or ['psa95','psa9_5'].
    We’ll match case-insensitive against whatever’s in the payload.
    """
    if not company or grade is None:
        return []
    pref = str(company).strip().lower()
    g = str(grade).strip().lower()  # '10', '9', '9.5'
    g_no_dot = g.replace(".", "")
    g_us = g.replace(".", "_")
    out = [f"{pref}{g}", f"{pref}{g_no_dot}", f"{pref}{g_us}"]
    # de-dup while preserving order
    seen, rv = set(), []
    for k in out:
        if k not in seen:
            seen.add(k); rv.append(k)
    return rv

def _find_ebay_grade_bucket(card: dict, company: str | None, grade: str | None) -> dict | None:
    ebay = (card or {}).get("ebay") or {}
    sbg = ebay.get("salesByGrade") or {}
    if not isinstance(sbg, dict):
        return None
    keys_lc = {str(k).lower(): k for k in sbg.keys()}
    for k in _grade_key_variants(company, grade):
        hit = keys_lc.get(k)
        if hit is not None:
            return sbg.get(hit) or None
    return None

def _grade_key_variants(company: str | None, grade: str | int | float | None) -> list[str]:
    if not company or grade is None:
        return []
    pref = str(company).strip().lower()
    g = str(grade).strip().lower()           # '10', '9', '9.5'
    g_no_dot = g.replace(".", "")            # '95'
    g_us = g.replace(".", "_")               # '9_5'
    out = [f"{pref}{g}", f"{pref}{g_no_dot}", f"{pref}{g_us}"]
    seen, rv = set(), []
    for k in out:
        if k not in seen:
            seen.add(k); rv.append(k)
    return rv

def _points_from_ebay_daily(card: dict, company: str | None, grade: str | None) -> list[dict]:
    """
    Build daily points from ebay.priceHistory[<company><grade>] where each day
    blob has {'average': ...}. Returns [{'timestamp': int, 'price': float}, ...].
    """
    ebay = (card or {}).get("ebay") or {}
    ph = ebay.get("priceHistory") or {}
    if not isinstance(ph, dict):
        return []

    # case-insensitive key match, e.g. 'psa9', 'psa9_5', 'psa95'
    keys_lc = {str(k).lower(): k for k in ph.keys()}
    hit_key = None
    for k in _grade_key_variants(company, grade):
        if k in keys_lc:
            hit_key = keys_lc[k]
            break
    if not hit_key:
        return []

    daily = ph.get(hit_key) or {}
    pts = []
    for day, v in daily.items():
        if not isinstance(v, dict):
            continue
        avg = v.get("average")
        if avg is None:
            continue
        try:
            dt = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        except Exception:
            continue
        pts.append({"timestamp": int(dt.timestamp()), "price": float(avg)})

    return sorted(pts, key=lambda x: x["timestamp"])

def _best_ebay_point_estimate(card: dict, company: str | None, grade: str | None) -> tuple[float | None, str | None]:
    """
    Choose a single-point estimate from ebay.salesByGrade (preferred) if present.
    Order: smartMarketPrice.price → marketPrice7Day → marketPriceMedian7Day.
    """
    bucket = _find_ebay_grade_bucket(card, company, grade)
    if not isinstance(bucket, dict):
        return None, None

    # smartMarketPrice.price (with optional confidence)
    smp = bucket.get("smartMarketPrice") or {}
    price = smp.get("price")
    if price is not None:
        try: return float(price), "ebay_smart_market"
        except Exception: pass

    # marketPrice7Day
    mp7 = bucket.get("marketPrice7Day")
    if mp7 is not None:
        try: return float(mp7), "ebay_mp7"
        except Exception: pass

    # marketPriceMedian7Day
    mp7m = bucket.get("marketPriceMedian7Day")
    if mp7m is not None:
        try: return float(mp7m), "ebay_mp7_median"
        except Exception: pass

    return None, None

def _fetch_wider_ebay(card_id: int, days: int) -> dict | None:
    """
    Force a wider eBay+history fetch to try to populate priceHistory when 7-day is sparse.
    """
    try:
        r = requests.get(
            "https://www.pokemonpricetracker.com/api/v2/cards",
            headers={"Authorization": f"Bearer {current_app.config['PPT_API_KEY']}"},
            params={"tcgPlayerId": int(card_id), "includeBoth": "true", "days": int(days)},
            timeout=15,
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        return rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else None)
    except Exception as e:
        print(f"[DBG] wider ebay/history fetch failed id={card_id}: {e}")
        return None

# down-change guard
_MIN_DOWN_REL = 0.03  # 3%
_MIN_DOWN_ABS = 3.00  # $3
def _tiny_downward(current: float | None, target: float | None) -> bool:
    if current is None or target is None or target >= current:
        return False
    diff = current - target
    rel = diff / max(current, 0.01)
    return (diff < _MIN_DOWN_ABS) or (rel < _MIN_DOWN_REL)
# --- NEW: variant flavor detection from your titles ---
_REV_PAT = re.compile(r'\b(reverse|rev[\s\-]*holo(?:foil)?|rh)\b', re.I)
_FIRST_PAT = re.compile(r'\b(1st|first\s*edition|1ED)\b', re.I)
_HOLO_PAT = re.compile(r'\b(holo(?:foil)?|foil)\b', re.I)

def _detect_variant_flavor(it: dict) -> dict:
    """
    Infer variant flavor from your product/variant titles only.
    Same tcgplayerId is used; we just pick the right price key.
    """
    txt = f"{it.get('product_title','')} {it.get('variant_title','')}"
    return {
        "is_reverse": bool(_REV_PAT.search(txt)),
        "is_first":   bool(_FIRST_PAT.search(txt)),
        # 'holo' is a hint for choosing holo vs normal when neither reverse/first applies
        "is_holoish": bool(_HOLO_PAT.search(txt)),
    }

# --- NEW: choose the right TCGplayer market key from the card payload ---
# we don't know exactly which keys a given set will expose, so we try a robust key list.
_PRICE_KEY_ORDER = {
    # first edition
    ("first", "reverse"): ["firstEditionReverseHolofoil", "firstEditionRevHolo", "firstEditionReverse", "reverseHolofoil", "reverse", "market"],
    ("first", "holo"):    ["firstEditionHolofoil", "1stEditionHolofoil", "firstEditionFoil", "firstEdition", "market"],
    ("first", "normal"):  ["firstEdition", "1stEditionNormal", "market"],
    # non-first
    ("std", "reverse"):   ["reverseHolofoil", "reverseHolo", "reverseFoil", "reverse", "market"],
    ("std", "holo"):      ["holofoil", "foil", "unlimitedHolofoil", "market"],
    ("std", "normal"):    ["normal", "unlimited", "market"],
}

def _price_from_tcg_market(card: dict, flavor: dict) -> float | None:
    prices = (card or {}).get("prices") or {}
    if not isinstance(prices, dict):
        return None
    first = "first" if flavor.get("is_first") else "std"
    # choose lane
    if flavor.get("is_reverse"):
        lane = "reverse"
    elif flavor.get("is_holoish"):
        lane = "holo"
    else:
        lane = "normal"
    for key in _PRICE_KEY_ORDER[(first, lane)]:
        v = prices.get(key)
        try:
            if v is None:
                continue
            # some APIs return nested dicts like {"market": 12.34}
            if isinstance(v, dict):
                if "market" in v and v["market"] is not None:
                    return float(v["market"])
                # otherwise try any numeric-looking leaf
                for k2 in ("avg", "average", "price", "low", "mid", "marketPrice"):
                    if k2 in v and v[k2] is not None:
                        return float(v[k2])
                continue
            return float(v)
        except Exception:
            continue
    # last resort: global 'market'
    try:
        m = prices.get("market")
        return float(m) if m is not None else None
    except Exception:
        return None

# --- NEW: small downward change guard ---
_MIN_DOWN_REL = 0.03  # 3%
_MIN_DOWN_ABS = 3.00  # $3
def _guard_small_downward_change(current: float | None, target: float | None) -> bool:
    """
    Returns True if we should NOOP due to a tiny downward change; else False.
    """
    if current is None or target is None:
        return False
    if target >= current:
        return False
    diff = current - target
    rel = diff / max(current, 0.01)
    return (diff < _MIN_DOWN_ABS) or (rel < _MIN_DOWN_REL)
def _ppt_fetch_by_id(ppt, tpid: int, *, days: int,
                     include_ebay: bool, include_history: bool, use_both: bool):
    """
    1) Call PPTClient with snake_case kwargs it accepts.
    2) If 'ebay' missing but requested, do a raw GET /api/v2/cards using the
       exact camelCase params from the public API (force includeBoth on retry).
    """
    def _coerce(resp):
        rows = resp.get("data", resp) if isinstance(resp, dict) else resp
        return rows[0] if isinstance(rows, list) and rows else (rows if isinstance(rows, dict) else None)

    # --- Attempt 1: use the wrapper exactly how it wants (snake_case) ---
    card = None
    last_err = None
    try:
        resp = ppt.get_card_by_id_v2(
            tcgplayer_id=int(tpid),
            days=int(days),
            include_ebay=bool(include_ebay),
            include_history=bool(include_history),
            include_both=bool(use_both),
        )
        card = _coerce(resp)
    except Exception as e:
        last_err = e

    # If we asked for eBay but didn't get it, fall back to raw HTTP with camelCase
    need_ebay = bool(include_ebay or use_both)
    # ... keep need_ebay calculation above ...

    if need_ebay and (not isinstance(card, dict) or not card.get("ebay")):
        try:
            params = {"tcgPlayerId": int(tpid), "days": int(days), "includeBoth": "true"}
            # use the client's backoff-aware getter (it already handles 429 + reset headers)
            j = ppt._get_with_backoff("https://www.pokemonpricetracker.com/api/v2/cards",
                                      params, max_tries=6)
            cand = _coerce(j)  # your existing helper
            if isinstance(cand, dict) and cand.get("ebay"):
                card = cand
            # else: keep whatever we had from the wrapper
        except requests.HTTPError as e:
            # bucket explicit 429s so the caller can mark 'rate_limited' not 'no_data'
            resp = getattr(e, "response", None)
            if resp is not None and getattr(resp, "status_code", None) == 429:
                raise PPTError("PPT rate-limited (429) during includeBoth fetch") from e
            last_err = e
        except Exception as e:
            last_err = e

    if not isinstance(card, dict) and last_err:
        raise last_err
    return card


def _extract_id_from_edges(edges) -> int | None:
    for e in edges or []:
        node = (e.get("node") if isinstance(e, dict) else None) or {}
        if node.get("key") == "tcgplayer_id":
            n = _norm_tcg_id(node.get("value"))
            if n:
                return n
    return None

def _gid_to_numeric(gid: str | None) -> int | None:
    if not gid:
        return None
    m = re.search(r"/(\d+)$", str(gid))
    return int(m.group(1)) if m else None

def _read_tcgplayer_id_from_variant(it: dict, shopify_client=None, product_meta_cache=None) -> int | None:
    """
    Order of precedence:
      0) lookup/meta and singular VARIANT metafield (tcg/metafield)
      1) variant metafields connection/list
      2) product singular metafield (product.tcg or product.metafield)
      3) product metafields connection
      4) lazy-load product metafields via client (edges or list)
    """
    product_meta_cache = product_meta_cache if product_meta_cache is not None else {}

    def _from_singular_mf(obj: dict | None) -> int | None:
        if not isinstance(obj, dict):
            return None
        # support alias "tcg" or "metafield" holding { value: ... }
        for k in ("tcg", "metafield"):
            node = obj.get(k)
            if isinstance(node, dict):
                n = _norm_tcg_id(node.get("value"))
                if n:
                    return n
            elif node is not None:
                n = _norm_tcg_id(node)
                if n:
                    return n
        # also allow direct stash
        for k in ("tcgplayer_id", "tcgPlayerId"):
            n = _norm_tcg_id(obj.get(k))
            if n:
                return n
        return None

    # 0) fast locals: lookup/meta
    meta   = it.get("meta") or {}
    lookup = it.get("lookup") or {}
    for k in ("tcgplayer_id", "tcgPlayerId"):
        n = _norm_tcg_id(lookup.get(k) or meta.get(k))
        if n:
            return n

    # 0b) singular VARIANT metafield on the item
    n = _from_singular_mf(it)
    if n:
        return n

    # 1) variant metafields edges/list
    vmf_edges = (((it.get("metafields") or {}).get("edges")) or [])
    n = _extract_id_from_edges(vmf_edges)
    if n:
        return n

    vmf_list = it.get("metafields") if isinstance(it.get("metafields"), list) else []
    for mf in vmf_list:
        if mf.get("key") == "tcgplayer_id":
            n = _norm_tcg_id(mf.get("value"))
            if n:
                return n

    # 2) product singular metafield
    prod = it.get("product") or {}
    n = _from_singular_mf(prod)
    if n:
        return n

    # 3) product metafields edges
    pmf_edges = (((prod.get("metafields") or {}).get("edges"))
                 or it.get("product_metafields_edges")
                 or [])
    n = _extract_id_from_edges(pmf_edges)
    if n:
        return n

    # 4) lazy-load via client
    pid = (it.get("product_id") or it.get("productId") or _gid_to_numeric(it.get("product_gid"))
           or _gid_to_numeric(prod.get("id")))
    if pid and pid not in product_meta_cache and shopify_client:
        try:
            if hasattr(shopify_client, "get_product_metafields_edges"):
                edges = shopify_client.get_product_metafields_edges(pid)
                product_meta_cache[pid] = {"edges": edges}
            elif hasattr(shopify_client, "get_product_metafields"):
                mfs = shopify_client.get_product_metafields(pid)
                product_meta_cache[pid] = {"list": mfs}
            elif hasattr(shopify_client, "get_product_metafield"):
                # singular fetch if your client exposes it
                val = shopify_client.get_product_metafield(pid, "tcg", "tcgplayer_id")
                product_meta_cache[pid] = {"single": val}
            else:
                product_meta_cache[pid] = {}
        except Exception as e:
            print(f"[DBG] fetch product metafields failed product_id={pid}: {e}")
            product_meta_cache[pid] = {}

    if pid and product_meta_cache.get(pid):
        cache_entry = product_meta_cache[pid]
        if "edges" in cache_entry:
            n = _extract_id_from_edges(cache_entry["edges"])
            if n:
                return n
        if "list" in cache_entry:
            for mf in cache_entry["list"]:
                if mf.get("key") == "tcgplayer_id":
                    n = _norm_tcg_id(mf.get("value"))
                    if n:
                        return n
        if "single" in cache_entry:
            n = _norm_tcg_id(cache_entry["single"])
            if n:
                return n

    return None






# ----------------- tiny helpers: number + subset handling -----------------
def _subset_looks_plain_digits(rows: list[dict]) -> bool:
    """
    Heuristic: if we see many non-empty numbers in the subset and none have TG/GG/RC,
    assume the subset uses plain digits (e.g., '3' instead of 'TG03').
    """
    seen = 0
    tgish = 0
    for c in rows[:50]:
        raw = (c.get("cardNumber") or c.get("number") or c.get("collectorNumber") or "")
        s = (raw or "").upper()
        if not s:
            continue
        seen += 1
        if re.search(r'\b(TG|GG|RC)\s*0*\d+\b', s):
            tgish += 1
    # if we saw numbers and none looked TG/GG/RC → plain digits
    return seen > 0 and tgish == 0

def _norm_grade(g):
    if g is None:
        return None
    s = str(g).strip()
    return s[:-2] if s.endswith(".0") else s

def _mget(meta, key, default=None):
    if meta is None:
        return default
    if isinstance(meta, dict):
        return meta.get(key, default)
    return getattr(meta, key, default)

def _subset_hint_from_titles(it: dict) -> str | None:
    txt = f"{it.get('product_title','')} {it.get('variant_title','')}".upper()
    if re.search(r'\bTG\s*0*\d+\b', txt):
        return "trainer-gallery"
    if re.search(r'\bGG\s*0*\d+\b', txt):
        return "galarian-gallery"
    return None

def _maybe_infer_tg_gg_number(it: dict, num: str) -> str:
    """If title says TG/GG## and num is plain digits, convert to TG#/GG#."""
    if not num or not num.strip().isdigit():
        return (num or "").strip()
    txt = f"{it.get('product_title','')} {it.get('variant_title','')}".upper()
    m = re.search(r'\b(TG|GG)\s*0*(\d+)\b', txt)
    return f"{m.group(1)}{int(m.group(2))}" if m else num.strip()

def _maybe_match_by_name_in_subset(item: dict, subset_rows: list[dict]) -> dict | None:
    """
    Try to find a TG/GG card in subset_rows by name when numbers are empty.
    We use the product/variant title text as the search surface.
    """
    # Build a cleaned title to search within
    title = f"{item.get('product_title', '')} {item.get('variant_title', '')}".lower()
    # strip grade tokens and TG/GG tokens
    title = re.sub(r'\b(psa|cgc|bgs)\s*\d+(\.\d+)?\b', '', title)
    title = re.sub(r'\b(tg|gg)\s*0*\d+\b', '', title)
    title = re.sub(r'\s+', ' ', title).strip()

    # quick passes: exact name, or name without punctuation
    def _clean(s: str) -> str:
        return re.sub(r'[^a-z0-9 ]+', '', s.lower())

    cleaned_title = _clean(title)

    # 1) exact substring
    for c in subset_rows:
        nm = (c.get("name") or "").lower()
        if nm and nm in title:
            return c

    # 2) punctuation-stripped substring
    for c in subset_rows:
        nm = _clean(c.get("name") or "")
        if nm and nm in cleaned_title:
            return c

    return None

def _all_num_keys(raw: str) -> set[str]:
    """
    Build every alias a set might use for a card number:
      - digits: '18', '018'
      - TG/GG/RC: 'TG3', 'TG03', 'TG003'
      - plus the raw uppercase, no spaces/hyphens
    """
    s = (raw or "").strip().upper().replace(" ", "").replace("-", "")
    keys = set()
    if not s:
        return keys
    keys.add(s)
    if s.isdigit():
        n = str(int(s))
        keys.add(n)
        keys.add(n.zfill(2))
        keys.add(n.zfill(3))
        return keys
    m = re.fullmatch(r'(TG|GG|RC)(\d+)', s)
    if m:
        p, d = m.groups()
        n = str(int(d))
        keys.add(f"{p}{n}")
        keys.add(f"{p}{d}")
        keys.add(f"{p}{n.zfill(2)}")
        keys.add(f"{p}{n.zfill(3)}")
    return keys

def _best_lookup_keys(num: str) -> list[str]:
    if not num:
        return []
    s = (num or "").strip().upper().replace(" ", "").replace("-", "").replace("/", "").replace("#", "")
    keys = []

    # plain digits
    if s.isdigit():
        n = str(int(s))
        keys += [n, n.zfill(2), n.zfill(3)]

    # TG/GG/RC
    m = re.fullmatch(r'(TG|GG|RC)0*(\d+)', s)
    if m:
        p, d = m.groups()
        n = str(int(d))
        # try TG/GG/RC (unpadded + padded)
        keys += [f"{p}{n}", f"{p}{n.zfill(2)}", f"{p}{n.zfill(3)}"]
        # **also try plain digits** because some subsets store "003" not "TG03"
        keys += [n, n.zfill(2), n.zfill(3)]

    # raw last
    keys.append(s)

    # dedupe, preserve order
    out, seen = [], set()
    for k in keys:
        if k not in seen:
            seen.add(k); out.append(k)
    return out



# ----------------- tiny helpers: eBay → points/aggregate -----------------

def _points_from_ebay_daily_v2(card: dict, grade: str | None):
    ebay = (card or {}).get("ebay") or {}
    ph = ebay.get("priceHistory") or {}
    g = (f"psa{str(grade).strip().lower()}" if grade else "psa10")
    daily = ph.get(g) or {}
    pts = []
    for day, v in daily.items():
        avg = (v or {}).get("average")
        if avg is None:
            continue
        dt = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        pts.append({"timestamp": int(dt.timestamp()), "price": float(avg)})
    return sorted(pts, key=lambda x: x["timestamp"])

def _aggregate_ebay_7d(card: dict, grade: str | None):
    ebay = (card or {}).get("ebay") or {}
    sbg = ebay.get("salesByGrade") or {}
    key = f"psa{str(grade).strip().lower()}" if grade else "psa10"
    g = sbg.get(key) or {}
    # prefer marketPrice7Day; fallback to smartMarketPrice.price
    return g.get("marketPrice7Day") or (g.get("smartMarketPrice") or {}).get("price")

def _enrich_card_by_id(ppt, card, *, days, include_ebay, include_history, include_both):
    tpid = card.get("tcgplayerId") or card.get("tcgPlayerId")
    if not (tpid and str(tpid).strip().isdigit()):
        url = (card.get("tcgPlayerUrl") or card.get("tcgplayerUrl") or "") or ""
        m = re.search(r"/product/(\d+)", url)
        if m: tpid = m.group(1)
    if not (tpid and str(tpid).strip().isdigit()):
        print(f"[DBG] enrich_by_id: skip (no numeric tcgplayerId) name={card.get('name')}")
        return card

    full = _ppt_fetch_by_id(
        ppt, int(tpid),
        days=int(days),
        include_ebay=bool(include_ebay),
        include_history=bool(include_history),
        use_both=bool(include_both or include_ebay)  # prefer both to guarantee ebay
    )
    if isinstance(full, dict) and full.get("ebay"):
        card["ebay"] = full["ebay"]
    else:
        print(f"[DBG] enrich_by_id: no ebay in response id={tpid}")
    return card





# ----------------- minimal fetch+index for a set (with TG/GG) -----------------

import re

def _canon_num_local(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Uppercase, strip spaces/hyphens/#, unify weird slashes to '/'
    up = re.sub(r'[ \-#]', '', s.upper())
    up = (up
          .replace('／', '/')
          .replace('⁄', '/')
          .replace('∕', '/'))
    base = up.split('/', 1)[0]  # take token before slash

    # pure digits → strip leading zeros
    if base.isdigit():
        return str(int(base))

    # TG/GG/RC + digits (allow leading zeros)
    m = re.match(r'^(TG|GG|RC)0*(\d+)$', base)
    if m:
        return f"{m.group(1)}{int(m.group(2))}"

    return base

def _index_set_by_number_local(rows):
    idx = {}
    for c in rows or []:
        raw = c.get("cardNumber") or c.get("number") or c.get("collectorNumber") or ""
        sraw = (raw or "").upper()
        compact = sraw.replace(" ", "").replace("-", "").replace("_", "")
        if not compact:
            continue

        # If this row is clearly a TG/GG/RC card and its number is digits,
        # make sure digit keys resolve to THIS subset row (not base).
        if compact.isdigit():
            if re.search(r'\bTG\s*0*\d+\b', sraw):
                d = str(int(compact))
                idx[d] = c
                idx[d.zfill(2)] = c
                idx[d.zfill(3)] = c
                idx[f"TG{d}"] = c
                idx[f"TG{d.zfill(2)}"] = c
                idx[f"TG{d.zfill(3)}"] = c
            elif re.search(r'\bGG\s*0*\d+\b', sraw):
                d = str(int(compact))
                idx[d] = c
                idx[d.zfill(2)] = c
                idx[d.zfill(3)] = c
                idx[f"GG{d}"] = c
                idx[f"GG{d.zfill(2)}"] = c
                idx[f"GG{d.zfill(3)}"] = c

        # 1) Always map compact and canon
        idx[compact] = c
        cn = _canon_num_local(raw)
        if cn:
            idx[cn] = c

        # 2) Digits: add padding aliases
        if compact.isdigit():
            d = str(int(compact))
            idx[d] = c
            idx[d.zfill(2)] = c
            idx[d.zfill(3)] = c

        # 3) TG/GG/RC tokens anywhere in the raw string → add canonical + padded keys
        #    This catches "TG 03", "TG-03", "… TG03/TG30 …", etc.
        for pref in ("TG", "GG", "RC"):
            m = re.search(rf"{pref}\s*0*(\d+)", sraw)
            if m:
                d = str(int(m.group(1)))
                idx[f"{pref}{d}"] = c
                idx[f"{pref}{d.zfill(2)}"] = c
                idx[f"{pref}{d.zfill(3)}"] = c
    return idx





def _apply_subset_slug(base_slug: str, subset: str) -> str:
    if subset == "trainer-gallery":
        return f"{base_slug}-trainer-gallery"
    if subset == "galarian-gallery":
        return f"{base_slug}-galarian-gallery"
    return base_slug

def _fetch_v2_set(ppt, slug: str, *, days, include_ebay, include_history, include_both):
    try:
        data = ppt.list_set_cards_v2_fetch_all(
            set_slug=slug,
            days=days,
            include_history=include_history,
            include_ebay=include_ebay,
            include_both=include_both,
            set_key="setId",
        )
    except Exception as e:
        print(f"[DBG] fetch_set FAILED slug={slug}: {e}")
        return []  # <— don’t crash the whole job

    rows = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(rows, list):
        return rows
    return [rows] if rows else []



def _fetch_set_cards_merged(
    ppt,
    base_slug: str,
    *,
    days,
    include_ebay,
    include_history,
    include_both,
    subsets_required: set[str] | None = None,
    dbg_label: str = "",
):
    subsets_required = subsets_required or set()

    # base
    base_rows = _fetch_v2_set(
        ppt, base_slug,
        days=days,
        include_ebay=include_ebay,
        include_history=include_history,
        include_both=include_both,
    )

    # subsets (TG/GG)
    subset_rows = []
    for sub in sorted(subsets_required):
        sub_slug = _apply_subset_slug(base_slug, sub)
        sr = _fetch_v2_set(
            ppt, sub_slug,
            days=days,
            include_ebay=include_ebay,
            include_history=include_history,
            include_both=include_both,
        )
        subset_rows.extend(sr)

    print(f"[DBG] {dbg_label or base_slug} → base:{base_slug} subset:{'+'.join(sorted(subsets_required)) if subsets_required else '-'} fetched:{len(base_rows) + len(subset_rows)}")

    base_index   = _index_set_by_number_local(base_rows)
    subset_index = _index_set_by_number_local(subset_rows)

    # (optional TG/GG inspect — keep if useful)
    if subset_rows:
        from pprint import pprint as pp
        def _num_probe(c: dict) -> dict:
            probe_keys = ["cardNumber","number","collectorNumber","collectorNo","collector_no","no","CardNumber","card_no","collectorNum"]
            out = {k: c.get(k) for k in probe_keys if c.get(k) not in (None, "", [])}
            for k, v in c.items():
                if "num" in k.lower() and k not in out and v not in (None, "", []):
                    out[k] = v
            out["name"] = c.get("name")
            out["tcgplayerId"] = c.get("tcgplayerId") or c.get("tcgPlayerId")
            out["tcgPlayerUrl"] = c.get("tcgPlayerUrl") or c.get("tcgplayerUrl")
            return out
        print(f"[DBG] subset_rows count: {len(subset_rows)}")
        for i, c in enumerate(subset_rows[:3]):
            print(f"[DBG] subset_rows[{i}] keys: {sorted(c.keys())[:30]} ...")
            print(f"[DBG] subset_rows[{i}] num_probe:"); pp(_num_probe(c))
            for k, v in c.items():
                if isinstance(v, dict):
                    print(f"[DBG] subset_rows[{i}].{k} keys: {list(v.keys())[:30]} ...")

    return base_rows, subset_rows, base_index, subset_index







# ----------------- THE stripped run() -----------------

def run_slabs_sync_v2(
    shopify_client,
    *,
    days=30,
    half_life=7.0,
    include_ebay=True,
    include_history=False,
    use_both=False,
    dry_run=True,
    ppt_client=None,
    sample: int = 0,
    only_sets: str = "",
    prefer_ebay_aggregate: bool = True,
    per_card: bool = False,  # ignored
    per_set_limit: int = 0,  # ignored
    limit: int = 0,          # ignored
    fetch_all: bool = True,  # ignored
    **_
):
    ppt = ppt_client or PPTClient(api_key=current_app.config["PPT_API_KEY"])

    report = {
        "updated": [],
        "flag_down": [],
        "no_data": [],
        "parse_missing": [],
        "skipped": [],  # <— NEW
    }

    variants = list(iter_slab_variants_with_meta(shopify_client, tag="slab"))

    # optional whitelist (kept, in case you’re filtering runs)
    only = {s.strip().lower() for s in only_sets.split(",") if s.strip()} if only_sets else set()
    if only:
        def _allow(set_name, slug):
            base = (slug or "").lower().replace("-trainer-gallery", "").replace("-galarian-gallery", "")
            return (set_name and set_name.lower() in only) or (slug and slug.lower() in only) or (base in only)
        variants = [v for v in variants if _allow((_mget(v.get("meta"), "set_name", "") or "").strip(),
                                                  to_set_slug((_mget(v.get("meta"), "set_name", "") or "").strip(),
                                                              year=_mget(v.get("meta"), "year")))]

    if sample and sample > 0:
        variants = variants[:sample]

    # tiny response cache if multiple variants share the same id
    id_card_cache: dict[int, dict] = {}
    product_meta_cache: dict[int, dict] = {}  # productId   -> {"edges": [...]} or {"list":[...]}

    for it in variants:
        lookup = it.get("lookup") or {}
        company = (lookup.get("company") or "PSA").upper()
        grade   = _norm_grade(lookup.get("grade"))
        tpid = _read_tcgplayer_id_from_variant(
            it, shopify_client=shopify_client, product_meta_cache=product_meta_cache
        )

        if not tpid:
            # hard requirement: skip if missing
            report["skipped"].append({
                "variant_id": it["variant_id"],
                "reason": "missing_tcgplayer_id",
                "product_title": it.get("product_title",""),
                "variant_title": it.get("variant_title",""),
            })
            print(f"[DBG] skip variant {it['variant_id']} → missing tcgplayerId")
            continue

        # -------- Fetch by ID (single call path) --------
        print(f"[DBG] by-id fetch tcgplayerId={tpid}")
        try:
            card = id_card_cache.get(tpid)
            if card is None:
                card = _ppt_fetch_by_id(
                    ppt, tpid,
                    days=int(days),
                    include_ebay=bool(include_ebay),
                    include_history=bool(include_history),
                    use_both=bool(use_both),
                )
                id_card_cache[tpid] = card or {}
        except Exception as e:
            report["no_data"].append({
                "variant_id": it["variant_id"],
                "tcgplayer_id": tpid,
                "reason": f"fetch_by_id_failed: {e}",
            })
            print(f"[DBG] ✖ by-id fetch failed id={tpid}: {e}")
            continue

        if not isinstance(card, dict):
            report["no_data"].append({
                "variant_id": it["variant_id"],
                "tcgplayer_id": tpid,
                "reason": "no_card_payload",
            })
            print(f"[DBG] ✖ no card for id={tpid}")
            continue

        cname = card.get("name") or "(unknown)"
        print(f"[DBG]  ✓ matched by id: {cname} (id={tpid})")

        # -------- Pricing (eBay-only; robust EMA/median preferred) --------
        raw_target, source = None, None

        # 1) Try robust target from 60d series (EMA vs recent median/quantile)
        raw_target, source, dbg = _robust_ebay_target(card, company, grade, half_life=half_life, base_days=60)

        # 2) If series sparse, fetch a wider window once and retry
        if raw_target is None:
            wider_days = 90 if int(days) < 90 else int(days)
            card_wide = _fetch_wider_ebay(tpid, wider_days) or card
            raw_target, source, dbg = _robust_ebay_target(card_wide, company, grade, half_life=half_life, base_days=60)

        # 3) Still nothing → single-point fallback (smartMarketPrice → mp7 → median7)
        if raw_target is None:
            v, src = _best_ebay_point_estimate(card, company, grade)
            if v is not None:
                raw_target, source = float(v), src

        if raw_target is None:
            fallback = _guess_from_item(it)
            report["no_data"].append({
                "variant_id": it["variant_id"],
                "product_title": it.get("product_title", ""),
                "variant_title": it.get("variant_title", ""),
                "sku": it.get("sku", ""),
                "card_name": fallback["card_name"],
                "set": fallback["set"],
                "num": fallback["num"],
                "tcgplayer_id": tpid,
                "reason": "ebay_insufficient",
            })
            print("[DBG] ✖ ebay insufficient")
            continue

        print(f"[DBG] source:{source} raw:{raw_target:.2f} dbg:{dbg}")

        # D) Decide + guard tiny downward changes
        decision, new_price = decide_update(it.get("current_price"), raw_target)
        if decision == "update" and _tiny_downward(it.get("current_price"), new_price):
            print("[DBG]  → noop (tiny downward)")
            decision = "noop"

        # flag very large upward moves
        if decision == "update" and _huge_upward(it.get("current_price"), new_price):
            decision = "flag_huge_up"

        basics = _card_basics(card)
        entry = {
            "variant_id": it["variant_id"],
            "product_title": it.get("product_title", ""),
            "variant_title": it.get("variant_title", ""),
            "sku": it.get("sku", ""),
            "card_name": basics["card_name"] or it.get("variant_title") or it.get("product_title", ""),
            "set": basics["set"],
            "num": basics["num"],
            "old": it.get("current_price"),
            "new": new_price,
            "tcgplayer_id": tpid,
            "source": source,
            "raw": raw_target,
        }

        if decision == "update":
            if not dry_run:
                update_variant_price(shopify_client, it["variant_id"], new_price)
            entry["dry_run"] = True if dry_run else False
            report["updated"].append(entry)
        elif decision.startswith("flag"):
            entry["reason"] = decision
            report["flag_down"].append(entry)
        else:
            print("[DBG]  → noop")

    return report


