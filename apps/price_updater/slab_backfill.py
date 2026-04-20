"""
slab_backfill — auto-fill missing tcgplayer_id metafields on Shopify slab
products by matching the title against scrydex_price_cache.

Scope: in-stock slabs (qty > 0) tagged 'slab' with no tcg metafield.
Strategy:
  1. Parse title -> game, card_number, grade, variant hints.
  2. Pull cache candidates by (game, card_number).
  3. Score each by % overlap of expansion_name + product_name words against
     the title; +0.5 bonus if a variant hint matches.
  4. Decide:
       - confident:  top score >= 1.4 AND lead over runner-up >= 0.3
       - same-card-multiple-variants: all top-tier candidates share one
         scrydex_id; pick the variant most likely to carry graded comps
         (priority list below).
       - else: ambiguous (skipped, surfaced for manual pick).
  5. For confident + same-id picks, write Shopify metafield
     pf_slab.tcgplayer_id and record the result.

Used by /backfill-slab-tcg-ids endpoint in review_dashboard.py.
"""
import os
import re
import sys
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

SHOPIFY_STORE   = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_TOKEN   = os.environ.get("SHOPIFY_TOKEN", "")
SHOPIFY_VERSION = "2025-10"
GRAPHQL_ENDPOINT = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_VERSION}/graphql.json"
HEADERS = {"Content-Type": "application/json", "X-Shopify-Access-Token": SHOPIFY_TOKEN}

GAME_HINTS = {
    "pokemon":  ["pokemon", "pokémon", "jpn pokemon", "japanese pokemon"],
    "magic":    ["magic ", "mtg ", "magic the gathering"],
    "onepiece": ["one piece", "onepiece"],
    "lorcana":  ["lorcana"],
}

# Variant prioritization for same-scrydex-id ambiguity. Earlier = preferred.
# These are the variants most likely to carry graded comps in Scrydex's data.
_VARIANT_PRIORITY = [
    "holofoil", "normal", "reverseHolofoil",
    "firstEditionHolofoil", "unlimitedHolofoil",
    "unlimitedShadowlessHolofoil",
]

_YEAR_RE = re.compile(r"^\s*\d{4}\s*", re.I)
_NUM_RE  = re.compile(r"#(\S+)")
_VARIANT_HINTS = {
    "1st edition":   "firstEditionHolofoil",
    "1st ed":        "firstEditionHolofoil",
    "shadowless":    "unlimitedShadowlessHolofoil",
    "reverse holo":  "reverseHolofoil",
    "rev holo":      "reverseHolofoil",
}
_GRADE_RE = re.compile(r"\b(PSA|BGS|CGC|SGC|ACE|TAG)\s+\d+(?:\.\d)?\b", re.I)


def _guess_game(title: str) -> str | None:
    t = (title or "").lower()
    for g, hints in GAME_HINTS.items():
        if any(h in t for h in hints):
            return g
    return None


def parse_title(title: str) -> dict:
    """Extract structured fields from a slab product title for matching."""
    t = (title or "").strip()
    g = _GRADE_RE.search(t)
    if g:
        t = t[:g.start()].strip()
    t = _YEAR_RE.sub("", t).strip()
    game = _guess_game(title)
    if game == "pokemon":
        t = re.sub(r"^(?:jpn\s+|japanese\s+)?pok[eé]mon\s+", "", t, flags=re.I).strip()
    cnum = None
    m = _NUM_RE.search(t)
    if m:
        cnum = m.group(1).strip().rstrip(".,")
        t = (t[:m.start()] + t[m.end():]).strip()
    variant_hint = None
    tl = t.lower()
    for needle, v in _VARIANT_HINTS.items():
        if needle in tl:
            variant_hint = v
    return {"text": t, "card_number": cnum, "game": game, "variant_hint": variant_hint}


def _score_candidates(parsed: dict, candidates: list[dict]) -> list[tuple[float, dict]]:
    text_lower = parsed["text"].lower()
    text_words = set(w for w in re.findall(r"[a-z0-9]+", text_lower) if len(w) > 2)
    out = []
    for r in candidates:
        en = (r["expansion_name"] or "").lower()
        pn = (r["product_name"]   or "").lower()
        en_words = [w for w in re.findall(r"[a-z0-9]+", en) if len(w) > 2]
        pn_words = [w for w in re.findall(r"[a-z0-9]+", pn) if len(w) > 2]
        en_hits = sum(1 for w in en_words if w in text_words)
        pn_hits = sum(1 for w in pn_words if w in text_words)
        score = (en_hits / max(1, len(en_words))) + (pn_hits / max(1, len(pn_words)))
        if parsed["variant_hint"] and r["variant"] == parsed["variant_hint"]:
            score += 0.5
        out.append((round(score, 2), r))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


def _candidates_from_cache(parsed: dict, db_module) -> list[dict]:
    if not parsed["game"] or not parsed["card_number"]:
        return []
    cnum = parsed["card_number"]
    cnum_alts = sorted({cnum, cnum.lstrip("0") or "0"})
    placeholders = ",".join(["%s"] * len(cnum_alts))
    return db_module.query(f"""
        SELECT DISTINCT scrydex_id, tcgplayer_id, variant, expansion_name, product_name, printed_number
        FROM scrydex_price_cache
        WHERE game = %s AND product_type = 'card' AND tcgplayer_id IS NOT NULL
          AND (card_number IN ({placeholders}) OR printed_number IN ({placeholders}))
    """, (parsed["game"], *cnum_alts, *cnum_alts))


def _pick_match(scored: list[tuple[float, dict]]) -> tuple[str, dict | None]:
    """Decide what to do with the scored candidate list.

    Returns (decision, picked_row | None) where decision is one of:
      'confident'           — top is a clear winner
      'collapsed_variant'   — top-tier candidates all share scrydex_id;
                              picked the most-comp-likely variant
      'no_match'            — no candidate scored above 0.5 (essentially
                              random hits on card_number alone)
      'ambiguous'           — multiple distinct scrydex_ids tied — needs human
    """
    if not scored:
        return "no_match", None
    top_score, top_row = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0

    # No real signal — card_number matched but name/expansion didn't
    if top_score < 0.5:
        return "no_match", None

    # Clean winner
    if top_score >= 1.4 and (top_score - second_score) >= 0.3:
        return "confident", top_row

    # All top-tier candidates within 0.3 of leader — bucket them
    top_band = [(s, r) for s, r in scored if (top_score - s) < 0.3]
    distinct_sids = {r["scrydex_id"] for _, r in top_band}

    if len(distinct_sids) == 1:
        # Same card, just multiple TCGPlayer printings (normal/foil/stamp)
        # — graded comps share scrydex_id so any variant works for lookup
        # purposes. Prefer the variant most likely to actually carry comps.
        candidates = [r for _, r in top_band]
        for preferred in _VARIANT_PRIORITY:
            for r in candidates:
                if r["variant"] == preferred:
                    return "collapsed_variant", r
        return "collapsed_variant", candidates[0]

    return "ambiguous", None


def _set_tcg_metafield(product_gid: str, tcgplayer_id: int) -> None:
    """Write pf_slab.tcgplayer_id metafield on a Shopify product."""
    mutation = """
    mutation Set($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        userErrors { field message }
      }
    }
    """
    variables = {"metafields": [{
        "ownerId": product_gid,
        "namespace": "pf_slab",
        "key": "tcgplayer_id",
        "type": "single_line_text_field",
        "value": str(int(tcgplayer_id)),
    }]}
    r = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                      json={"query": mutation, "variables": variables}, timeout=30)
    r.raise_for_status()
    errs = r.json().get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"metafieldsSet errors: {errs}")


def run(*, apply: bool = False, db_module=None) -> dict:
    """Match in-stock slabs to scrydex tcgplayer_ids and (optionally) write
    the metafield. apply=False is dry-run — returns proposed matches without
    touching Shopify.

    Returns:
      {
        scanned: int,
        confident: [{title, tcgplayer_id, scrydex_id, expansion_name, score, ...}],
        collapsed: [...],
        ambiguous: [{title, candidates: [...]}],
        no_match: [{title, reason}],
        applied: int,
        errors: [{title, error}],
      }
    """
    if db_module is None:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
        import db as db_module
        db_module.init_pool()

    # Local import so this module can be loaded without the rest of the
    # price_updater package being importable in unusual contexts.
    from slab_updater import fetch_slab_products

    slabs = fetch_slab_products()
    in_scope = [s for s in slabs if (s.get("qty") or 0) > 0 and not s.get("tcg_id")]
    logger.info(f"slab_backfill: {len(in_scope)} in-stock slabs missing tcg_id "
                f"(of {len(slabs)} total)")

    out = {
        "scanned": len(in_scope),
        "confident": [], "collapsed": [], "ambiguous": [], "no_match": [],
        "applied": 0, "errors": [],
    }

    for slab in in_scope:
        parsed = parse_title(slab["title"])
        cands = _candidates_from_cache(parsed, db_module)
        scored = _score_candidates(parsed, cands)
        decision, picked = _pick_match(scored)

        record = {
            "title":        slab["title"],
            "product_gid":  slab["product_gid"],
            "sku":          slab["sku"],
            "qty":          slab["qty"],
            "card_number":  parsed["card_number"],
            "game":         parsed["game"],
        }

        if decision in ("confident", "collapsed_variant"):
            record.update({
                "tcgplayer_id":   picked["tcgplayer_id"],
                "scrydex_id":     picked["scrydex_id"],
                "variant":        picked["variant"],
                "expansion_name": picked["expansion_name"],
                "product_name":   picked["product_name"],
                "score":          scored[0][0],
            })
            if apply:
                try:
                    _set_tcg_metafield(slab["product_gid"], picked["tcgplayer_id"])
                    out["applied"] += 1
                    record["status"] = "applied"
                except Exception as e:
                    out["errors"].append({"title": slab["title"], "error": str(e)})
                    record["status"] = "error"
                    record["error"] = str(e)
            else:
                record["status"] = "proposed"
            (out["confident"] if decision == "confident" else out["collapsed"]).append(record)

        elif decision == "ambiguous":
            record["candidates"] = [{
                "score": s, "tcgplayer_id": r["tcgplayer_id"], "scrydex_id": r["scrydex_id"],
                "variant": r["variant"], "expansion_name": r["expansion_name"],
                "product_name": r["product_name"],
            } for s, r in scored[:5]]
            out["ambiguous"].append(record)

        else:  # no_match
            reason = []
            if not parsed["game"]:        reason.append("no game guess")
            if not parsed["card_number"]: reason.append("no #card_number in title")
            if not reason: reason.append(f"no confident hit for {parsed['game']} #{parsed['card_number']}")
            record["reason"] = "; ".join(reason)
            out["no_match"].append(record)

    logger.info(
        f"slab_backfill done: confident={len(out['confident'])} "
        f"collapsed_variant={len(out['collapsed'])} "
        f"ambiguous={len(out['ambiguous'])} "
        f"no_match={len(out['no_match'])} "
        f"applied={out['applied']} errors={len(out['errors'])}"
    )
    return out
