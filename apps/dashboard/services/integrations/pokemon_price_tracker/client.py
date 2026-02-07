import httpx
import time, random, requests

UA = "slabs-sync/0.1 (+contact@yourdomain)"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": UA,
}


def _tflag(v: bool) -> str | None:
    return "true" if v else None


def _clean_params(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


class PPTError(Exception): ...

class PPTClient:
    def __init__(self, api_key: str, base_url="https://www.pokemonpricetracker.com/api"):
        self.base_url = base_url.rstrip("/")
        self.headers = {**DEFAULT_HEADERS, "Authorization": f"Bearer {api_key}"}
        self._spacing = 0.0
        self._last = 0.0

        # optional throttle

    import time, math, json

    def _sleep_until(reset_epoch: float, fudge: float = 0.5):
        now = time.time()
        delay = max(0.0, float(reset_epoch) - now + fudge)
        if delay > 0:
            time.sleep(delay)

    def _get_with_backoff(self, url, params, *, max_tries: int = 4):
        last_err = None
        for attempt in range(1, max_tries + 1):
            r = requests.get(url, headers=self.headers, params=params)
            # Handle happy path
            if r.status_code < 400:
                # Optional: be polite if header says we’re out next call
                mr = r.headers.get("X-Ratelimit-Minute-Remaining")
                reset = r.headers.get("X-Ratelimit-Minute-Reset")
                if mr is not None and reset is not None:
                    try:
                        if int(mr) <= 1:
                            _sleep_until(float(reset))
                    except Exception:
                        pass
                return r.json()

            # Handle 429 with server hints
            if r.status_code == 429:
                reset = r.headers.get("X-Ratelimit-Minute-Reset")
                wait_s = None
                try:
                    body = r.json()
                    # API returns {'required': N, 'available': M, 'minuteCost': C, ...}
                    minute_cost = body.get("minuteCost")
                    remaining = body.get("available")
                    if minute_cost is not None and remaining is not None:
                        # If we can’t afford this call, wait until reset
                        if reset is not None:
                            wait_s = max(0.0, float(reset) - time.time() + 0.5)
                except Exception:
                    pass
                if wait_s is None and reset is not None:
                    try:
                        wait_s = max(0.0, float(reset) - time.time() + 0.5)
                    except Exception:
                        wait_s = 2.0 + attempt  # tiny fallback
                time.sleep(wait_s or (2.0 + attempt))
                continue

            # Other errors: brief retry
            last_err = r
            time.sleep(1.0 * attempt)

        # Exhausted
        body = None
        try:
            body = last_err.json()
        except Exception:
            body = last_err.text if last_err is not None else "<no response>"
        msg = f"{last_err.status_code if last_err else 'ERR'} after retries; headers={dict(last_err.headers) if last_err else {} } body={body}"
        raise requests.HTTPError(msg, response=last_err)

    def bulk_history(self, card_ids: list[str], type_: str, start: str, end: str) -> list[dict]:
        """
        card_ids: ['sv95-26', ...]
        type_: 'all' | 'raw' | 'psa' | 'cgc' | 'bgs'
        start/end: 'YYYY-MM-DD'
        returns: list[{"id": "sv95-26", "history": [{"date":"2025-01-10","price":123.45}, ...]}]
        """
        url = f"{self.base_url}/cards/bulk-history"
        body = {"cardIds": card_ids, "type": type_.lower(), "startDate": start, "endDate": end}
        with httpx.Client(timeout=30) as s:
            r = s.post(url, headers=self.headers, json=body)
            if r.status_code >= 400:
                raise PPTError(f"{r.status_code} {r.text}")
            return r.json()

    def list_sets(self, **params) -> dict:
        """
        Returns JSON from /api/v2/sets. Pass query params like search=..., sorWWtBy=..., limit=...
        """
        url = f"{self.base_url}/v2/sets"
        with httpx.Client(timeout=30) as s:
            r = s.get(url, headers=self.headers, params=params)
            r.raise_for_status()
            return r.json()

    def list_cards(self, **params):
        url = f"{self.base_url}/v2/cards"
        with httpx.Client(timeout=30) as s:
            r = s.get(url, headers=self.headers, params=params)
            r.raise_for_status()
            return r.json()

    def list_sets_v1(self, **params) -> dict:
        """
        GET /api/sets → [{ id: 'sv8', name: 'Surging Sparks', ...}, ...]
        """
        url = f"{self.base_url}/sets"  # base_url = https://www.pokemonpricetracker.com/api
        with httpx.Client(timeout=30) as s:
            r = s.get(url, headers=self.headers, params=params)
            r.raise_for_status()
            return r.json()

    def get_card_v2(self, *, set_slug: str, number: str | int, days: int = 30, include_ebay: bool = False):
        """
        Kept for compatibility, but v2 doesn't support filtering by number directly.
        Prefer list_set_cards_v2() + local match instead of this method.
        """
        url = f"{self.base_url}/v2/cards"
        params = {"setId": set_slug, "includeHistory": True, "days": int(days)}
        if include_ebay:
            params["includeEbay"] = True
        # NOTE: No number param here in v2. We'll filter client-side.
        return self._get_with_backoff(url, params)


    def list_set_cards_v2(self, *, set_slug: str, limit: int = 50, offset: int = 0,
                          sort_by: str = "cardNumber", sort_order: str = "asc"):
        """
        v2 bulk listing (NO history). Keep limit small to avoid minute-rate 429s.
        """
        url = f"{self.base_url}/v2/cards"
        params = {
            "setId": set_slug,
            "fetchAllInSet": True,
            "includeHistory": False,
            "limit": int(limit),  # <= 50 plays nicely with 60/min
            "offset": int(offset),
            "sortBy": sort_by,
            "sortOrder": sort_order,  # 'asc' => 001..102 for Base
        }
        return self._get_with_backoff(url, params)

    def get_card_by_id_v2(
            self, *, tcgplayer_id, days=30,
            include_history=False, include_ebay=False, include_both=False
    ):
        url = f"{self.base_url}/v2/cards"
        params = {
            "tcgPlayerId": str(int(str(tcgplayer_id).strip())),  # <-- IMPORTANT
            "days": int(days),
        }
        if include_both:
            params["includeBoth"] = True
        else:
            if include_history:
                params["includeHistory"] = True
            if include_ebay:
                params["includeEbay"] = True

        # TEMP DEBUG: see exactly what we send
        print("[PPTClient.get_card_by_id_v2] params=", params)

        return self._get_with_backoff(url, params)

    def list_set_cards_v2_fetch_all(
            self, *, set_slug: str, days: int = 30,
            include_history: bool = False, include_ebay: bool = False, include_both: bool = False,
            set_key: str = "set",  # <-- default to 'set' per docs
    ):
        url = f"{self.base_url}/v2/cards"
        params = {
            set_key: set_slug,
            "fetchAllInSet": "true",  # MUST be string "true"
            "days": int(days),
        }
        if include_both:
            params["includeBoth"] = "true"
        else:
            ih = _tflag(include_history and not include_both)
            ie = _tflag(include_ebay)
            if ih: params["includeHistory"] = ih
            if ie: params["includeEbay"] = ie

        params = _clean_params(params)
        return self._get_with_backoff(url, params)

    def get_card_by_set_and_number_v2(
            self, *, set_slug: str, number: str, days: int = 30,
            include_history: bool = True, include_ebay: bool = False, include_both: bool = False,
            set_key: str = "setId",
    ):
        """
        V2: fetch a single card by set + number.
        v2 currently ignores `number` in many cases, so we:
          1) try with several likely param names (cardNumber/collectorNumber/number)
          2) FILTER client-side by comparing the returned rows' numbers
          3) fall back to a small paged query and filter there
        This keeps minute-cost low and prevents mismatches (e.g., Ultra Ball #186).
        """
        url = f"{self.base_url}/v2/cards"

        def _incl_params():
            p = {"days": int(days)}
            if include_both:
                p["includeBoth"] = True
            else:
                if include_history: p["includeHistory"] = True
                if include_ebay:    p["includeEbay"] = True
            return p

        # candidates to try (raw and zero-padded)
        n_raw = str(number).strip()
        candidates = [n_raw]
        if n_raw.isdigit():
            candidates.append(n_raw.zfill(3))  # "4" -> "004"
            candidates.append(str(int(n_raw)))  # canonical int -> "4"
        # dedupe but preserve order
        candidates = list(dict.fromkeys(candidates))

        # helper: extract a card's displayed number
        def _row_num(card):
            return (card.get("cardNumber")
                    or card.get("number")
                    or card.get("collectorNumber")
                    or "")

        # very light normalizer for comparison (keeps non-digit ids like "SV72")
        def _canon(s: str) -> str:
            s = (s or "").strip()
            if s.isdigit():
                # strip leading zeros for pure digits
                return str(int(s))
            return s.upper().replace(" ", "").replace("-", "")

        # 1) Try with various param names the API might honor
        for cand in candidates:
            base = {set_key: set_slug, "limit": 50, "sortBy": "cardNumber", "sortOrder": "asc"}
            base.update(_incl_params())

            # Try several likely filter keys; some may be ignored server-side
            attempts = [
                {**base, "cardNumber": cand},
                {**base, "collectorNumber": cand},
                {**base, "number": cand},
            ]

            for params in attempts:
                resp = self._get_with_backoff(url, params)
                rows = resp.get("data", resp)
                cards = rows if isinstance(rows, list) else ([rows] if rows else [])
                if not cards:
                    continue

                # FILTER client-side to the exact number we want
                want = _canon(cand)
                for c in cards:
                    rn = _row_num(c)
                    if rn == cand or _canon(rn) == want:
                        return c  # ✅ exact match

        # 2) Fallback: one SMALL page and filter locally (cheap minute-cost)
        params = {
            set_key: set_slug, "limit": 25, "offset": 0,
            "sortBy": "cardNumber", "sortOrder": "asc",
            **_incl_params(),
        }
        resp = self._get_with_backoff(url, params)
        rows = resp.get("data", resp)
        cards = rows if isinstance(rows, list) else ([rows] if rows else [])
        want = _canon(n_raw)
        for c in cards:
            rn = _row_num(c)
            if rn == n_raw or _canon(rn) == want or _canon(rn) in (_canon(x) for x in candidates):
                return c

        return None
