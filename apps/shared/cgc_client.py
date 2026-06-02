"""
cgc_client.py — CGC cert lookup via headless Chromium scrape.

CGC does not publish a developer API for cert lookups. The official cert
verification page (https://www.cgctradingcards.com/certlookup/<cert>/) is
an Angular SPA gated behind Cloudflare's "Just a moment..." JS challenge,
so a plain HTTP fetch returns the CF challenge page, not cert data. We
mirror the proven approach in apps/price_updater/dailyrunner.py: spawn a
headless Chromium instance, let it solve the CF challenge automatically,
then read the rendered DOM.

Return shape mirrors PSA's PSACert dict so shared/psa_client.py's title /
description / tags builders can consume CGC data without changes:
    {
      "CertNumber":         str,
      "Year":               str,
      "Subject":            str,    # card name on the label
      "Brand":              str,    # set / series
      "Variety":            str,    # variant text (e.g. "Holo")
      "CardNumber":         str,
      "CardGrade":          str,    # e.g. "9.5"
      "GradeDescription":   str,    # e.g. "Mint+ 9.5"
      "TotalPopulation":    int|None,
      "PopulationHigher":   int|None,
    }

Selectors are best-guess until a real cert page is observed in production —
the parser logs everything it finds so we can tighten the selectors after
the first live run instead of blocking on a perfect scrape today.
"""

import io
import os
import re
import time
import logging
import threading
import unicodedata
from typing import Optional

import requests
from PIL import Image

logger = logging.getLogger(__name__)

# Cert URL — trading-cards subdomain renders the same SPA as cgccards.com
# but is reliably reachable from US-Railway egress IPs (verified 2026-05-31).
CGC_CERT_URL_TPL = "https://www.cgctradingcards.com/certlookup/{cert}/"

# Timeouts are split so the happy path stays fast (we poll and return the
# moment the DOM is ready) while failures get a generous budget. On a
# memory-constrained Railway container the Cloudflare JS challenge alone can
# eat 10-20s before the Angular SPA even boots and fires its cert XHR, so the
# old single 25s budget was almost certainly too short.
CGC_PAGE_LOAD_TIMEOUT = 45   # driver.get() hard cap (CF can hang the load event)
CGC_CF_TIMEOUT        = 35   # max wait for the Cloudflare challenge to clear
CGC_DOM_TIMEOUT       = 25   # max wait for cert result DOM *after* CF clears

# Cloudflare interstitial markers — while any of these are present the real
# page hasn't rendered yet.
_CF_MARKERS = (
    "just a moment",
    "verify you are human",
    "checking your browser",
    "challenge-platform",
    "cf-chl",
    "/cdn-cgi/challenge",
    "enable javascript and cookies",
)

# Reuse cache shape from psa_client — preview + push share a single scrape
_cgc_cert_cache:   dict[str, dict] = {}
_cgc_image_cache:  dict[str, list] = {}
_cgc_cache_times:  dict[str, float] = {}
_CGC_CACHE_TTL = 7200  # 2 hours

# Serialize Selenium calls — Chromium consumes a lot of memory under
# concurrent load and the Push tab fans out cert previews in parallel.
_driver_lock = threading.Lock()


class CGCNotFound(Exception):
    pass


class CGCScrapeFailed(Exception):
    pass


def _cgc_cache_valid(cert_number: str) -> bool:
    return (cert_number in _cgc_cache_times
            and (time.time() - _cgc_cache_times[cert_number]) < _CGC_CACHE_TTL)


# ══════════════════════════════════════════════════════════════════════════════
# Selenium driver
# ══════════════════════════════════════════════════════════════════════════════

def _build_driver():
    """Return a configured headless Chromium driver.

    Mirrors the options stack proven in apps/price_updater/dailyrunner.py —
    same Cloudflare-defeating UA + automation flags. Caller is responsible
    for `driver.quit()`.
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    # Headless-rendering stability flags — mirror price_updater/dailyrunner.py,
    # which reliably clears the same Cloudflare challenge on this egress.
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--window-size=1280,1024")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )

    # Capture network traffic so a failed scrape can log the request URLs the
    # SPA fired — that reveals CGC's internal cert JSON endpoint, which is a
    # far more robust data source than scraping rendered Angular markup.
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    service = Service(os.environ.get("CHROMEDRIVER", "/usr/bin/chromedriver"))
    return webdriver.Chrome(service=service, options=options)


# ══════════════════════════════════════════════════════════════════════════════
# Cert scrape
# ══════════════════════════════════════════════════════════════════════════════

# Label patterns observed on PSA-style cert pages. For each conceptual field
# we try several human-readable labels — CGC uses different capitalization
# and word choice across product lines (sports vs TCG vs comics).
_LABEL_PATTERNS = {
    "Subject":         [r"item", r"description", r"subject", r"card name", r"title"],
    "Brand":           [r"set name", r"\bset\b", r"series", r"game", r"brand"],
    "Year":            [r"year", r"date"],
    "CardNumber":      [r"card\s*number", r"card\s*#", r"card no\.?", r"number"],
    "CardGrade":       [r"grade", r"final grade"],
    "Variety":         [r"variety", r"variant", r"feature"],
    "TotalPopulation": [r"population at this grade", r"pop at this grade", r"population\s*$"],
    "PopulationHigher": [r"higher", r"population higher"],
}


def get_cgc_data(cert_number: str) -> dict:
    """Scrape CGC cert details + return a PSACert-shaped dict.

    Cached 2h in-process — preview + push hit this for the same cert and
    we don't want to spend two Selenium launches per slab. Raises
    CGCNotFound if the page shows a "no such cert" error, CGCScrapeFailed
    if the DOM never settles (CF challenge stuck, network down, etc).
    """
    cert_number = (cert_number or "").strip()
    if not cert_number:
        raise CGCScrapeFailed("cert_number is empty")

    if cert_number in _cgc_cert_cache and _cgc_cache_valid(cert_number):
        logger.debug(f"CGC cert cache HIT for {cert_number}")
        return _cgc_cert_cache[cert_number]

    cert_data, image_urls = _scrape_cert(cert_number)

    _cgc_cert_cache[cert_number]  = cert_data
    _cgc_image_cache[cert_number] = image_urls
    _cgc_cache_times[cert_number] = time.time()
    return cert_data


def get_cgc_images(cert_number: str) -> list[str]:
    """Return cached image URLs (front, back) for a CGC cert.

    `get_cgc_data` already scrapes images alongside the text fields and
    caches both — no second Chromium launch.
    """
    cert_number = (cert_number or "").strip()
    if not cert_number:
        return []
    if cert_number in _cgc_image_cache and _cgc_cache_valid(cert_number):
        return _cgc_image_cache[cert_number]
    try:
        get_cgc_data(cert_number)
    except Exception as e:
        logger.warning(f"CGC image fetch (via data) failed for {cert_number}: {e}")
        return []
    return _cgc_image_cache.get(cert_number, [])


def _scrape_cert(cert_number: str) -> tuple[dict, list[str]]:
    """Launch Chromium, load the cert URL, parse DOM. Best-effort."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.common.exceptions import TimeoutException

    url = CGC_CERT_URL_TPL.format(cert=cert_number)

    with _driver_lock:
        driver = None
        try:
            logger.info(f"CGC scrape start cert={cert_number} url={url}")
            driver = _build_driver()
            try:
                driver.set_page_load_timeout(CGC_PAGE_LOAD_TIMEOUT)
            except Exception:
                pass
            try:
                driver.get(url)
            except TimeoutException:
                # CF sometimes blocks the load event indefinitely; keep going
                # and work with whatever has rendered so far.
                logger.warning(
                    f"CGC page load timed out (continuing anyway) cert={cert_number}"
                )

            # Phase 1 — wait for the Cloudflare JS challenge to clear. Until it
            # does, page_source is just the "Just a moment..." interstitial.
            cf_cleared = _wait_past_cloudflare(driver, cert_number)

            # Phase 2 — wait for the Angular cert result panel (or a real
            # "not found") to render after the cert XHR resolves.
            try:
                WebDriverWait(driver, CGC_DOM_TIMEOUT).until(
                    lambda d: _page_ready(d.page_source, cert_number)
                )
            except TimeoutException:
                _log_scrape_diagnostics(
                    driver, cert_number,
                    reason="cloudflare-stuck" if not cf_cleared else "dom-timeout",
                )
                src = (driver.page_source or "").lower()
                if "cannot be found" in src or "not found" in src:
                    raise CGCNotFound(f"CGC cert {cert_number} not found")
                if not cf_cleared:
                    raise CGCScrapeFailed(
                        f"CGC blocked by Cloudflare for cert {cert_number} "
                        f"(challenge never cleared)"
                    )
                raise CGCScrapeFailed(
                    f"CGC page never populated for cert {cert_number} (timeout)"
                )

            html = driver.page_source
            data, image_urls = _parse_cert_html(html, cert_number)

            # The page rendered but our selectors found nothing useful — this
            # is the "markup differs from our guesses" case. Dump diagnostics
            # so we can correct the parser without another blind round-trip.
            if not data.get("Subject") and not data.get("CardGrade"):
                _log_scrape_diagnostics(driver, cert_number, reason="empty-parse")

            return data, image_urls
        finally:
            if driver is not None:
                try: driver.quit()
                except Exception: pass


def _wait_past_cloudflare(driver, cert_number: str) -> bool:
    """Poll until the Cloudflare interstitial is gone. Returns True if it
    cleared, False if we hit the budget while still challenged.

    Returns immediately once the challenge markers disappear so a fast clear
    doesn't pay the full budget.
    """
    deadline = CGC_CF_TIMEOUT
    waited = 0.0
    step = 0.5
    while waited < deadline:
        try:
            src = (driver.page_source or "").lower()
        except Exception:
            src = ""
        if src and not any(m in src for m in _CF_MARKERS):
            if waited:
                logger.info(
                    f"CGC Cloudflare cleared after ~{waited:.0f}s cert={cert_number}"
                )
            return True
        time.sleep(step)
        waited += step
    logger.warning(
        f"CGC Cloudflare still challenging after {deadline}s cert={cert_number}"
    )
    return False


def _page_ready(html: str, cert_number: str = "") -> bool:
    """Heuristic: page is ready when any cert-result label is present.

    We're not picky about the exact selector because CGC's SPA varies its
    markup per category — if any of our recognized label words appears we
    treat the result panel as rendered and let the parser take a swing.
    """
    if not html:
        return False
    h = html.lower()
    # Don't declare ready while Cloudflare is still challenging.
    if any(m in h for m in _CF_MARKERS):
        return False
    if "cannot be found" in h or "not found" in h:
        return True
    # Look for labels / signals that only appear once the result panel renders.
    return any(needle in h for needle in (
        "population at this grade",
        "population",
        "cgc grade",
        "final grade",
        "card grade",
        "grade date",
        "card name",
        "set name",
        "certification number",
        "view label",
        "label image",
    ))


def _collect_request_urls(driver) -> list[str]:
    """Pull the SPA's network request URLs out of Chrome's performance log.

    We filter to CGC hosts and drop static assets so the cert data XHR — the
    endpoint we actually want to call directly — stands out.
    """
    import json as _json
    urls: list[str] = []
    try:
        for entry in driver.get_log("performance"):
            try:
                msg = _json.loads(entry.get("message", "{}")).get("message", {})
            except Exception:
                continue
            if msg.get("method") not in ("Network.requestWillBeSent",
                                         "Network.responseReceived"):
                continue
            params = msg.get("params", {})
            req = params.get("request") or params.get("response") or {}
            u = req.get("url", "")
            if not u or u.startswith("data:"):
                continue
            ul = u.lower()
            if "cgc" not in ul:
                continue
            if any(ul.endswith(ext) for ext in
                   (".js", ".css", ".png", ".jpg", ".jpeg", ".gif",
                    ".svg", ".woff", ".woff2", ".ico", ".map")):
                continue
            if u not in urls:
                urls.append(u)
    except Exception as e:
        logger.debug(f"CGC perf-log collection failed: {e}")
    return urls[:25]


def _log_scrape_diagnostics(driver, cert_number: str, reason: str) -> None:
    """Emit everything we know about a failed/empty scrape to the logs.

    This is the difference between 'nothing in the backend logs' and being
    able to fix the selectors (or switch to the JSON endpoint) on the next
    real lookup. Best-effort — never raises.
    """
    try:
        title = driver.title
    except Exception:
        title = "<unavailable>"
    try:
        cur_url = driver.current_url
    except Exception:
        cur_url = "<unavailable>"
    try:
        html = driver.page_source or ""
    except Exception:
        html = ""

    hl = html.lower()
    cf_present = any(m in hl for m in _CF_MARKERS)

    # Visible-text snippet (tags stripped) so we can read what actually rendered.
    snippet = ""
    try:
        from bs4 import BeautifulSoup
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        snippet = text[:1000]
    except Exception:
        snippet = html[:1000]

    req_urls = _collect_request_urls(driver)

    logger.warning(
        "CGC scrape FAILED cert=%s reason=%s\n"
        "  title=%r\n  url=%r\n  html_len=%d  cloudflare_present=%s\n"
        "  request_urls=%s\n  visible_text_snippet=%r",
        cert_number, reason, title, cur_url, len(html), cf_present,
        req_urls or "<none captured>", snippet,
    )


def _parse_cert_html(html: str, cert_number: str) -> tuple[dict, list[str]]:
    """Pull PSA-shaped fields out of a rendered CGC cert page.

    CGC presents most fields as <dt>label</dt><dd>value</dd> pairs or as
    <td>label</td><td>value</td>. We grep the rendered HTML for known
    labels (case-insensitive) and capture the next adjacent text node.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    if "cannot be found" in html.lower() or "not found" in html.lower():
        # Distinguish empty SPA shell from real "not found" — only treat
        # as not-found if no result section rendered at all.
        if not soup.find(string=re.compile(r"(?i)grade|population|card name")):
            raise CGCNotFound(f"CGC cert {cert_number} not found")

    fields: dict[str, str] = {
        "CertNumber": cert_number,
        "Subject":    "",
        "Brand":      "",
        "Year":       "",
        "CardNumber": "",
        "CardGrade":  "",
        "GradeDescription": "",
        "Variety":    "",
    }
    populations: dict[str, Optional[int]] = {
        "TotalPopulation":  None,
        "PopulationHigher": None,
    }

    # Walk every text-bearing element looking for label words and grab the
    # next sibling / cell as the value.
    for el in soup.find_all(string=True):
        label_text = (el or "").strip()
        if not label_text or len(label_text) > 60:
            continue
        lt = label_text.lower().rstrip(":").strip()

        for key, patterns in _LABEL_PATTERNS.items():
            if fields.get(key) or populations.get(key) is not None:
                continue
            for pat in patterns:
                if re.fullmatch(pat, lt):
                    value = _value_near(el)
                    if not value:
                        break
                    if key in populations:
                        populations[key] = _parse_int(value)
                    else:
                        fields[key] = value.strip()
                    break

    # CGC's grade label is "9.5" / "10 Pristine" / "9.0 Mint" — extract the
    # numeric grade for CardGrade and keep the full string in GradeDescription.
    if fields["CardGrade"]:
        full = fields["CardGrade"]
        fields["GradeDescription"] = full
        m = re.search(r"\d+(\.\d+)?", full)
        if m:
            fields["CardGrade"] = m.group(0)

    # Images — the cert page embeds front + back slab scans. CGC hosts them
    # under cdn.cgccards.com or cgctradingcards.com /resources/cert-images/.
    image_urls = _extract_images(soup, cert_number)

    out = {**fields, **populations}
    logger.info(
        f"CGC scrape parsed cert={cert_number}: "
        f"subject={fields['Subject']!r} brand={fields['Brand']!r} "
        f"year={fields['Year']!r} card_no={fields['CardNumber']!r} "
        f"grade={fields['CardGrade']!r} pop={populations['TotalPopulation']!r} "
        f"images={len(image_urls)}"
    )
    return out, image_urls


def _value_near(label_el) -> str:
    """Find the value text adjacent to a label element.

    Tries (in order) the next sibling, the next <dd> after a <dt>, the
    next <td> after a label <td>, and the parent's next sibling. Returns
    a stripped string or "".
    """
    parent = label_el.parent if hasattr(label_el, "parent") else None
    if parent is None:
        return ""

    # <dt>Label</dt><dd>Value</dd>
    if parent.name == "dt":
        dd = parent.find_next_sibling("dd")
        if dd:
            return dd.get_text(" ", strip=True)

    # <td>Label</td><td>Value</td>
    if parent.name == "td":
        nxt = parent.find_next_sibling("td")
        if nxt:
            return nxt.get_text(" ", strip=True)

    # <span class="label">Label</span><span class="value">Value</span>
    nxt = parent.find_next_sibling()
    if nxt:
        txt = nxt.get_text(" ", strip=True)
        if txt:
            return txt

    return ""


def _parse_int(s: str) -> Optional[int]:
    """Pull the first integer-with-commas out of a string."""
    if not s:
        return None
    m = re.search(r"(\d[\d,]*)", s)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _extract_images(soup, cert_number: str) -> list[str]:
    """Pull slab front/back image URLs from the cert page.

    CGC's cert images live under cdn.cgctradingcards.com or
    cgccards.com/resources — we accept any URL whose path includes
    'cert' / 'image' AND references the cert number, then fall back to
    the largest non-navigation images on the page.
    """
    urls: list[str] = []
    cert_in_url = re.compile(r"(?i)\b" + re.escape(cert_number) + r"\b")
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src or src.startswith("data:"):
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = "https://www.cgctradingcards.com" + src

        sl = src.lower()
        if cert_in_url.search(src) or (
            ("cert" in sl or "slab" in sl or "/scan" in sl or "/labels/" in sl)
            and ("cdn" in sl or "cgc" in sl or "amazonaws" in sl)
        ):
            if src not in urls:
                urls.append(src)

    # Front first — CGC usually labels image filenames with -F / -B or
    # /front/ /back/. If we can detect, prefer front first.
    def _front_rank(u: str) -> int:
        ul = u.lower()
        if "front" in ul or "-f." in ul or "_f." in ul:
            return 0
        if "back" in ul or "-b." in ul or "_b." in ul:
            return 1
        return 2
    urls.sort(key=_front_rank)

    return urls[:4]  # cap — front/back/maybe-detail; avoid trailing thumbs


# ══════════════════════════════════════════════════════════════════════════════
# Convenience: PSACert-shape dispatcher
# ══════════════════════════════════════════════════════════════════════════════
# Lets shared/psa_client.push_graded_slab call ONE function and get back
# the right grader's data, without needing per-company branches everywhere.

def get_grader_data(grade_company: str, cert_number: str) -> Optional[dict]:
    """Dispatch to PSA or CGC based on grade_company. Returns None on miss."""
    company = (grade_company or "").upper()
    if company == "CGC":
        try:
            return get_cgc_data(cert_number)
        except CGCNotFound:
            logger.warning(f"CGC cert {cert_number} not found")
            return None
        except Exception as e:
            logger.warning(f"CGC scrape failed for {cert_number}: {e}")
            return None
    # PSA + others live in psa_client and are called directly from there
    return None


def get_grader_images(grade_company: str, cert_number: str) -> list[str]:
    company = (grade_company or "").upper()
    if company == "CGC":
        return get_cgc_images(cert_number)
    return []
