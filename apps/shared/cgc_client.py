"""
cgc_client.py — CGC cert lookup via headless Chromium scrape.

CGC does not publish a developer API for cert lookups. The official cert
verification page (https://www.cgccards.com/certlookup/<cert>/) is
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

# Cert URL — MUST be cgccards.com. The cgctradingcards.com/certlookup/<cert>/
# path 302-redirects cross-host to cgccomics.com/grading/trading-cards/ (a
# marketing page) and drops the cert entirely, so we'd only ever scrape site
# chrome. cgccards.com/certlookup/<cert>/ is the real cert page (CF-gated).
CGC_CERT_URL_TPL = "https://www.cgccards.com/certlookup/{cert}/"

# Homepage warm-up target. The cert deep-link triggers a *managed* CF
# challenge that the post-verification recheck stalls on in headless; the
# homepage clears CF normally and the resulting cf_clearance cookie (scoped
# to www.cgccards.com) lets the cert page skip the managed challenge.
CGC_HOME_URL = "https://www.cgccards.com/"

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
    """Return a configured Chromium driver that can clear CGC's Cloudflare.

    Preferred path: undetected-chromedriver running **headful** under a virtual
    display (Xvfb). CGC's /certlookup/ path serves a *managed* CF challenge
    that solves the JS test then re-fingerprints the browser; plain headless
    Chrome (even with --disable-blink-features + JS stealth) is detected at
    that recheck and stalls forever on "Verification successful. Waiting for
    www.cgccards.com to respond". uc patches the chromedriver binary tells
    ($cdc_ props) that JS can't reach, and headful-under-Xvfb passes the
    recheck where headless does not.

    Falls back to plain headless Selenium if uc / pyvirtualdisplay aren't
    installed, so a deploy that lands before the image rebuild degrades to the
    old behavior instead of hard-failing the whole service.

    Returns a driver with a `_pf_display` attribute (the Xvfb handle or None);
    the caller must `driver.quit()` and then stop `_pf_display` if present.
    Caller is responsible for cleanup.
    """
    display = None
    try:
        import undetected_chromedriver as uc
        from pyvirtualdisplay import Display

        display = Display(visible=False, size=(1280, 1024))
        display.start()

        opts = uc.ChromeOptions()
        opts.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
        # NB: no --headless — that's the whole point. Keep only the sandbox /
        # shared-memory flags needed to run Chromium as root in a container.
        for arg in (
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--window-size=1280,1024",
            "--lang=en-US",
        ):
            opts.add_argument(arg)
        # NB: deliberately NO goog:loggingPrefs here. Performance logging keeps
        # the CDP Network domain attached, which CF's managed-challenge recheck
        # treats as automation and stalls on. uc also installs its own stealth,
        # so we don't call _apply_stealth (another CDP command) either.

        driver = uc.Chrome(
            options=opts,
            browser_executable_path=os.environ.get("CHROME_BIN", "/usr/bin/chromium"),
            driver_executable_path=os.environ.get("CHROMEDRIVER", "/usr/bin/chromedriver"),
            headless=False,
            use_subprocess=True,
        )
        driver._pf_display = display
        logger.info("CGC: using undetected-chromedriver (headful under Xvfb)")
        return driver
    except Exception as e:
        logger.warning(
            f"CGC: undetected-chromedriver/Xvfb unavailable, falling back to "
            f"headless Selenium (CF may block): {e}"
        )
        if display is not None:
            try: display.stop()
            except Exception: pass

    return _build_headless_driver()


def _build_headless_driver():
    """Fallback: plain headless Chromium. Mirrors price_updater/dailyrunner.py.

    Known to be detected by CGC's managed CF challenge — kept only so the
    service still runs (for PSA + non-CGC paths) if the uc/Xvfb deps are
    missing from the image.
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
    options.add_argument("--lang=en-US")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )

    # Cloudflare's *post-challenge* recheck re-fingerprints the browser and
    # silently refuses to hand off to the origin if it still smells automation
    # (observed: page sits on "Verification successful. Waiting for ... to
    # respond" forever). Strip the obvious tells.
    try:
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
    except Exception:
        pass

    # Capture network traffic so a failed scrape can log the request URLs the
    # SPA fired — that reveals CGC's internal cert JSON endpoint, which is a
    # far more robust data source than scraping rendered Angular markup.
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    service = Service(os.environ.get("CHROMEDRIVER", "/usr/bin/chromedriver"))
    driver = webdriver.Chrome(service=service, options=options)
    driver._pf_display = None
    _apply_stealth(driver)
    return driver


# JS injected before every document loads — hides the headless/automation
# fingerprints Cloudflare's post-challenge recheck looks for.
_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
window.chrome = window.chrome || { runtime: {} };
const _origQuery = window.navigator.permissions && window.navigator.permissions.query;
if (_origQuery) {
  window.navigator.permissions.query = (p) => (
    p && p.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : _origQuery(p)
  );
}
"""


def _apply_stealth(driver) -> None:
    """Install the stealth script via CDP so it runs before page scripts."""
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument", {"source": _STEALTH_JS}
        )
    except Exception as e:
        logger.debug(f"CGC stealth injection failed (continuing): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Cert scrape
# ══════════════════════════════════════════════════════════════════════════════

# The cert page server-renders the cert→collectible mapping into the Angular
# bootstrap, e.g.  ng-init="$popctrl.init('00519309', '9', 'G')"
#   → (collectibleID, grade, gradeType)
# This is the ONLY place the collectibleID exists (CGC resolves cert→card
# server-side; there's no public cert endpoint), so we must fetch this one
# CF-gated page — but everything else then comes from the open JSON API below.
_INIT_RE = re.compile(
    r"\$popctrl\.init\(\s*'([^']*)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*\)"
)

# Open, un-gated JSON population API (no Cloudflare). Keyed by collectibleID.
CGC_POP_API_TPL = (
    "https://production.api.aws.ccg-ops.com"
    "/api/cards/research/trading-cards/population/collectible/{cid}"
)

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

            # Only the headless fallback warms up on the homepage. uc is built
            # to hit the challenged page directly; an extra navigation just adds
            # CDP chatter that the managed-challenge recheck can flag.
            if getattr(driver, "_pf_display", None) is None:
                try:
                    driver.get(CGC_HOME_URL)
                    _wait_past_cloudflare(driver, f"{cert_number}/warmup")
                except Exception as e:
                    logger.debug(f"CGC homepage warm-up failed (continuing): {e}")

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
            try:
                return _build_cert_data(html, cert_number)
            except CGCScrapeFailed:
                # We got past CF but couldn't find $popctrl.init() — dump the
                # page so we can see what rendered instead.
                _log_scrape_diagnostics(driver, cert_number, reason="no-init")
                raise
        finally:
            if driver is not None:
                disp = getattr(driver, "_pf_display", None)
                try: driver.quit()
                except Exception: pass
                if disp is not None:
                    try: disp.stop()
                    except Exception: pass


def _wait_past_cloudflare(driver, cert_number: str) -> bool:
    """Poll until the Cloudflare interstitial is gone. Returns True if it
    cleared, False if we hit the budget while still challenged.

    Returns immediately once the challenge markers disappear so a fast clear
    doesn't pay the full budget.
    """
    # Poll gently. Reading page_source is a CDP round-trip; doing it in a tight
    # 0.5s loop during the challenge is itself an automation signal, so we let
    # the challenge breathe (3s steps) and give it an initial head start.
    deadline = CGC_CF_TIMEOUT
    step = 3.0
    time.sleep(step)
    waited = step
    while waited < deadline:
        try:
            src = (driver.page_source or "").lower()
        except Exception:
            src = ""
        if src and not any(m in src for m in _CF_MARKERS):
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
    """Page is ready once the cert result has server-rendered.

    The precise, reliable signal is the Angular bootstrap that carries the
    collectibleID — `$popctrl.init(...)` / the `certlookup-stats` container.
    A "cannot be found" message is also terminal (invalid cert).
    """
    if not html:
        return False
    h = html.lower()
    # Don't declare ready while Cloudflare is still challenging.
    if any(m in h for m in _CF_MARKERS):
        return False
    if "$popctrl.init(" in h or "certlookup-stats" in h:
        return True
    if "cannot be found" in h or "not found" in h or "is not valid" in h:
        return True
    return False


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


def _extract_init(html: str) -> Optional[tuple[str, str, str]]:
    """Pull (collectibleID, grade, gradeType) from the cert page's
    `$popctrl.init('00519309', '9', 'G')` Angular bootstrap. None if absent."""
    if not html:
        return None
    m = _INIT_RE.search(html)
    if not m:
        return None
    return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()


def _grade_ladder_index(grade: str, grade_type: str) -> Optional[int]:
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


def _build_cert_data(html: str, cert_number: str) -> tuple[dict, list[str]]:
    """Build a PSACert-shaped dict from the cert page + open population API.

    1. Regex the collectibleID/grade out of the page's `$popctrl.init(...)`.
    2. Fetch the un-gated JSON population API for the rich card data.
    3. Map the grade to "population at this grade" / "higher".
    """
    from bs4 import BeautifulSoup

    init = _extract_init(html)
    if not init:
        hl = (html or "").lower()
        if "cannot be found" in hl or "not found" in hl or "is not valid" in hl:
            raise CGCNotFound(f"CGC cert {cert_number} not found")
        raise CGCScrapeFailed(
            f"CGC cert page for {cert_number} had no $popctrl.init() "
            f"(layout changed or result didn't render)"
        )

    collectible_id, grade, grade_type = init

    fields: dict[str, str] = {
        "CertNumber": cert_number,
        "Subject":    "",
        "Brand":      "",
        "Year":       "",
        "CardNumber": "",
        "CardGrade":  grade,
        "GradeDescription": "",
        "Variety":    "",
    }
    populations: dict[str, Optional[int]] = {
        "TotalPopulation":  None,
        "PopulationHigher": None,
        "PopulationAtGrade": None,
    }

    pop = _fetch_population(collectible_id)
    if pop:
        fields["Subject"]    = (pop.get("name") or "").strip()
        fields["Variety"]    = (pop.get("variant") or "").strip()
        fields["CardNumber"] = (pop.get("cardNumber") or "").strip()
        fields["Year"]       = str(pop.get("cardYear") or "").strip()
        fields["Brand"]      = ((pop.get("group") or {}).get("name") or "").strip()
        total = pop.get("population_Total")
        populations["TotalPopulation"] = total if isinstance(total, int) else None

        idx = _grade_ladder_index(grade, grade_type)
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

    # Slab front/back images still come from the rendered page, if present.
    image_urls = _extract_images(BeautifulSoup(html, "html.parser"), cert_number)

    out = {**fields, **populations}
    logger.info(
        f"CGC cert={cert_number} cid={collectible_id} grade={grade!r} "
        f"subject={fields['Subject']!r} set={fields['Brand']!r} "
        f"year={fields['Year']!r} card_no={fields['CardNumber']!r} "
        f"total_pop={populations['TotalPopulation']!r} "
        f"at_grade={populations['PopulationAtGrade']!r} "
        f"higher={populations['PopulationHigher']!r} images={len(image_urls)}"
    )
    return out, image_urls


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
            src = "https://www.cgccards.com" + src

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
