"""
Shared Flask Blueprint for breakdown-cache API endpoints.

Usage in any service:
    from breakdown_routes import create_breakdown_blueprint
    app.register_blueprint(create_breakdown_blueprint(db, ppt_getter=lambda: ppt))
"""

import os
import logging
import threading
from datetime import datetime, date
from decimal import Decimal
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)


def _serialize(obj):
    """JSON-safe serialization for DB rows (Decimal, datetime, UUID)."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if hasattr(obj, '__str__') and type(obj).__name__ in ('UUID', 'uuid'):
        return str(obj)
    return obj


# ──────────────────────────────────────────────────────────────────────
# Audit viewer HTML — self-contained dark-theme page that fetches
# /audit-missing JSON and renders two tables (auto-fixable vs. cache gap).
# Inline so this blueprint stays drop-in for any service. __PREFIX__ is
# replaced with the blueprint's url_prefix at request time.
# ──────────────────────────────────────────────────────────────────────
AUDIT_HTML = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Breakdown cache audit</title>
<style>
  :root {
    --bg:#0b0f17; --surface:#121826; --surface-2:#1a2233; --border:#2a3447;
    --text:#e6edf7; --text-dim:#8b97ad; --accent:#4f7df9; --green:#34d399;
    --amber:#fbbf24; --red:#f87171; --purple:#c084fc;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
       font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:20px;}
  h1{margin:0 0 4px;font-size:1.4rem}
  .sub{color:var(--text-dim);font-size:.85rem;margin-bottom:18px}
  .stats{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:18px}
  .stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;
        padding:10px 14px;min-width:140px}
  .stat .v{font-size:1.5rem;font-weight:700}
  .stat .l{color:var(--text-dim);font-size:.75rem;text-transform:uppercase;letter-spacing:.05em}
  .stat.warn .v{color:var(--amber)} .stat.bad .v{color:var(--red)} .stat.ok .v{color:var(--green)}
  button{background:var(--accent);color:#fff;border:0;border-radius:6px;
         padding:8px 14px;font-weight:600;cursor:pointer;font-size:.9rem}
  button:hover{filter:brightness(1.1)}
  button:disabled{opacity:.5;cursor:not-allowed}
  button.secondary{background:var(--surface-2);color:var(--text)}
  button.danger{background:var(--red)}
  .actions{margin-bottom:18px;display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  section{margin-top:26px}
  h2{margin:0 0 6px;font-size:1.1rem}
  h2 .count{color:var(--text-dim);font-weight:400;font-size:.8em;margin-left:6px}
  .hint{color:var(--text-dim);font-size:.85rem;margin-bottom:10px}
  table{width:100%;border-collapse:collapse;background:var(--surface);
        border:1px solid var(--border);border-radius:8px;overflow:hidden}
  th,td{padding:8px 10px;border-bottom:1px solid var(--border);text-align:left;font-size:.85rem}
  th{background:var(--surface-2);color:var(--text-dim);font-weight:600;
     text-transform:uppercase;font-size:.7rem;letter-spacing:.05em}
  tr:last-child td{border-bottom:0}
  td a{color:var(--accent);text-decoration:none}
  td a:hover{text-decoration:underline}
  .pill{display:inline-block;padding:1px 6px;border-radius:4px;font-size:.7rem;
        background:var(--surface-2);color:var(--text-dim)}
  .pill.sealed{background:rgba(79,125,249,.18);color:#7aadff}
  .pill.promo{background:rgba(192,132,252,.18);color:var(--purple)}
  .pill.warn{background:rgba(251,191,36,.18);color:var(--amber)}
  .pill.bad{background:rgba(248,113,113,.18);color:var(--red)}
  .parents{font-size:.78rem;color:var(--text-dim)}
  .parents a{color:var(--text);}
  .parents .more{font-style:italic}
  .loading{padding:40px;text-align:center;color:var(--text-dim)}
  .toast{position:fixed;bottom:24px;right:24px;padding:12px 18px;border-radius:8px;
         font-weight:600;z-index:9999;background:var(--green);color:#000}
  .toast.err{background:var(--red);color:#fff}
</style>
</head><body>
<h1>Breakdown cache audit</h1>
<div class="sub">Components in <code>sealed_breakdown_components</code> whose <code>tcgplayer_id</code> doesn't resolve in <code>scrydex_price_cache</code> for the expected lookup type.</div>

<div class="stats">
  <div class="stat bad"><div class="v" id="s-total">—</div><div class="l">total broken</div></div>
  <div class="stat warn"><div class="v" id="s-wrong">—</div><div class="l">wrong type (auto-fixable)</div></div>
  <div class="stat"><div class="v" id="s-norow">—</div><div class="l">no cache row at all</div></div>
</div>

<div class="actions">
  <button id="btn-fix" disabled>Fix all wrong-type</button>
  <button class="secondary" id="btn-reload">Reload</button>
</div>

<section>
  <h2>Wrong type<span class="count" id="wrong-count"></span></h2>
  <div class="hint">Recipe author tagged the card as <code>sealed</code>, but Scrydex only has it as <code>card</code>. The lookup queries the wrong column and misses. <strong>Fix:</strong> flip <code>component_type</code> to <code>promo</code>. Reversible single-column update.</div>
  <table id="t-wrong"><thead><tr>
    <th>tcgplayer_id</th><th>tagged as</th><th>cached as</th>
    <th>recipes</th><th>parents</th><th>last priced</th>
  </tr></thead><tbody><tr><td colspan="6" class="loading">Loading…</td></tr></tbody></table>
</section>

<section>
  <h2>No cache row<span class="count" id="norow-count"></span></h2>
  <div class="hint">Scrydex has no row for this <code>tcgplayer_id</code> at all. Either the card isn't synced yet (newest sets / sealed-bundle promos), or the recipe references the wrong ID. Auto-fix isn't safe — needs a Scrydex sync or a manual relink in the recipe editor.</div>
  <table id="t-norow"><thead><tr>
    <th>tcgplayer_id</th><th>type</th><th>recipes</th><th>parents</th><th>last priced</th>
  </tr></thead><tbody><tr><td colspan="5" class="loading">Loading…</td></tr></tbody></table>
</section>

<script>
const PREFIX = "__PREFIX__";

function toast(msg, ok=true) {
  const t = document.createElement('div');
  t.className = 'toast' + (ok?'':' err'); t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(()=>t.remove(), 3500);
}
function fmtDate(s){ if(!s) return '—'; try{ return new Date(s).toLocaleDateString(); }catch{return s;} }
function tcgLink(id){ return `<a target="_blank" href="https://www.tcgplayer.com/product/${id}">${id}</a>`; }
function parentsCell(parents){
  if(!parents || !parents.length) return '<span class="parents">—</span>';
  const head = parents.slice(0, 3).map(p =>
    `<a target="_blank" href="https://www.tcgplayer.com/product/${p.tcgplayer_id}">`
    + (p.title ? p.title : ('#'+p.tcgplayer_id)) + `</a>`).join('<br>');
  const more = parents.length > 3 ? `<div class="more">+${parents.length - 3} more</div>` : '';
  return `<div class="parents">${head}${more}</div>`;
}

async function load() {
  const r = await fetch(PREFIX + '/audit-missing');
  const d = await r.json();
  document.getElementById('s-total').textContent = d.missing_count;
  document.getElementById('s-wrong').textContent = d.wrong_type_count;
  document.getElementById('s-norow').textContent = d.no_row_count;

  const wrong = d.missing.filter(m => m.wrong_type_in_cache);
  const norow = d.missing.filter(m => !m.in_cache_any_type);

  document.getElementById('wrong-count').textContent = ' (' + wrong.length + ')';
  document.getElementById('norow-count').textContent = ' (' + norow.length + ')';

  const tbW = document.querySelector('#t-wrong tbody');
  tbW.innerHTML = wrong.length ? wrong.map(m => `
    <tr>
      <td>${tcgLink(m.tcgplayer_id)}</td>
      <td><span class="pill ${m.component_type}">${m.component_type}</span></td>
      <td><span class="pill warn">${m.cached_product_type}</span></td>
      <td>${m.recipe_count}</td>
      <td>${parentsCell(m.parents)}</td>
      <td>${fmtDate(m.last_priced_at)}</td>
    </tr>`).join('') : '<tr><td colspan="6" class="loading">None — all clean.</td></tr>';

  const tbN = document.querySelector('#t-norow tbody');
  tbN.innerHTML = norow.length ? norow.map(m => `
    <tr>
      <td>${tcgLink(m.tcgplayer_id)}</td>
      <td><span class="pill ${m.component_type}">${m.component_type}</span></td>
      <td>${m.recipe_count}</td>
      <td>${parentsCell(m.parents)}</td>
      <td>${fmtDate(m.last_priced_at)}</td>
    </tr>`).join('') : '<tr><td colspan="5" class="loading">None — all clean.</td></tr>';

  document.getElementById('btn-fix').disabled = wrong.length === 0;
  document.getElementById('btn-fix').textContent =
      wrong.length ? `Fix all ${wrong.length} wrong-type` : 'Fix all wrong-type';
}

document.getElementById('btn-reload').onclick = load;
document.getElementById('btn-fix').onclick = async () => {
  const btn = document.getElementById('btn-fix');
  if (!confirm('Flip component_type from sealed to promo on every wrong-type row? This is reversible.')) return;
  btn.disabled = true; const oldText = btn.textContent; btn.textContent = 'Working…';
  try {
    const r = await fetch(PREFIX + '/audit-missing/fix-wrong-type', {
      method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'
    });
    const d = await r.json();
    if (!r.ok) { toast(d.error || 'Failed', false); btn.textContent = oldText; btn.disabled = false; return; }
    toast(`Updated ${d.updated_count} components (${d.unique_tcg_ids} unique IDs across ${d.unique_parents} recipes); refreshed ${d.refreshed_components} prices`);
    await load();
  } catch (e) { toast(e.message, false); btn.textContent = oldText; btn.disabled = false; }
};

load();
</script>
</body></html>"""


def create_breakdown_blueprint(db_module, ppt_getter=None, url_prefix="/api/breakdown-cache", name="breakdown_cache"):
    """
    Factory: returns a Flask Blueprint with all breakdown-cache endpoints.

    Args:
        db_module: the service's db module (must have query, query_one, execute, execute_returning)
        ppt_getter: callable returning a PriceProvider instance (or None to disable provider features)
        url_prefix: where to mount the blueprint (default /api/breakdown-cache)
        name: blueprint name (must be unique per app, default "breakdown_cache")
    """
    bp = Blueprint(
        name, __name__,
        url_prefix=url_prefix,
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
        static_url_path="/bd-static",
    )

    # Import shared logic (will be on PYTHONPATH via shared/)
    import breakdown_logic as logic

    def _get_ppt():
        if ppt_getter:
            return ppt_getter()
        return None

    # ─── List all recipes ───────────────────────────────────────────

    @bp.route("/")
    def list_cache():
        limit = request.args.get("limit", 200, type=int)
        rows = logic.list_breakdown_cache(db_module, limit=limit)
        return jsonify({"caches": _serialize(rows)})

    # ─── Get full breakdown record ──────────────────────────────────

    @bp.route("/<int:tcgplayer_id>")
    def get_cache(tcgplayer_id):
        result = logic.get_breakdown_cache(tcgplayer_id, db_module)
        if not result:
            return jsonify({"found": False, "cache": None})

        # JIT refresh stale component market prices in background. Backgrounded
        # so the modal opens instantly even when there are PPT calls in the mix
        # for cache-miss components Scrydex doesn't cover (sealed-in-sealed +
        # jumbo cards mostly). cache_only stays False (the default) so PPT IS
        # called for the no-row class — bounded by max_age_hours so each unique
        # missing component costs at most 1 PPT credit per refresh window.
        ppt = _get_ppt()
        if ppt:
            try:
                from breakdown_helpers import refresh_stale_component_prices
                variant_ids = [str(v["id"]) for v in result.get("variants", [])]
                if variant_ids:
                    threading.Thread(
                        target=refresh_stale_component_prices,
                        args=(variant_ids, db_module, ppt),
                        daemon=True
                    ).start()
            except Exception as e:
                logger.warning(f"Component price refresh skipped: {e}")

        return jsonify({"found": True, "cache": _serialize(result)})

    # ─── Delete entire record ───────────────────────────────────────

    @bp.route("/<int:tcgplayer_id>", methods=["DELETE"])
    def delete_cache(tcgplayer_id):
        deleted = logic.delete_breakdown_cache(tcgplayer_id, db_module)
        return jsonify({"success": deleted})

    # ─── Create/update variant ──────────────────────────────────────

    @bp.route("/<int:tcgplayer_id>/variant", methods=["POST"])
    def save_variant_route(tcgplayer_id):
        data = request.get_json(silent=True) or {}
        product_name = data.get("product_name", "Unknown")
        variant_name = data.get("variant_name", "Standard")
        components = data.get("components", [])
        notes = data.get("notes")
        variant_id = data.get("variant_id")

        if not components:
            return jsonify({"error": "components required"}), 400

        try:
            result = logic.save_variant(
                tcgplayer_id, product_name, variant_name, components,
                db_module, notes=notes, variant_id=variant_id
            )
            return jsonify({"success": True, "cache": _serialize(result)})
        except Exception as e:
            logger.exception(f"Failed to save variant for {tcgplayer_id}")
            return jsonify({"error": str(e)}), 500

    # ─── Delete variant ─────────────────────────────────────────────

    @bp.route("/variant/<variant_id>", methods=["DELETE"])
    def delete_variant_route(variant_id):
        result = logic.delete_variant(variant_id, db_module)
        return jsonify({"success": True, "cache": _serialize(result)})

    # ─── Batch summaries ────────────────────────────────────────────

    @bp.route("/batch", methods=["POST"])
    def batch_summaries():
        data = request.get_json(silent=True) or {}
        tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
        if not tcg_ids:
            return jsonify({"summaries": {}})
        ppt = _get_ppt()
        # Read endpoint — don't fall through to PPT on cache miss (avoids 12s
        # network stalls on promos Scrydex doesn't cover yet).
        summaries = logic.get_breakdown_summary_for_items(
            tcg_ids, db_module, ppt=ppt, max_age_hours=24, cache_only=True,
        )
        return jsonify({"summaries": _serialize(summaries)})

    # ─── Cache audit: which recipe components are missing from Scrydex ─
    # Sean's class of bug: recipe editor stores tcgplayer_id; cache lookup
    # is by tcgplayer_id; if Scrydex hasn't synced that ID (common for new
    # promo cards inside sealed bundles), every Collection Summary on a
    # session containing the parent box would burn 12s+ on PPT timeouts.
    # cache_only flag now suppresses the timeout, but the recipes are still
    # silently missing prices. This endpoint surfaces the broken set so
    # operators can fix at the source instead of grepping logs.
    @bp.route("/audit-missing")
    def audit_missing():
        """Components whose tcgplayer_id has no matching scrydex_price_cache row
        of the expected product_type. Sealed components expect product_type='sealed';
        promo components expect product_type='card'.

        Returns one row per (tcgplayer_id, component_type), with the parent
        recipes that wire it in so operators know what to fix.

        Optional query param ?include_any_row=1 also returns rows where the
        tcgplayer_id IS in the cache but under a different product_type
        (recipe author may have picked the wrong component_type).
        """
        include_any = request.args.get("include_any_row") == "1"

        rows = db_module.query("""
            WITH comp_recipes AS (
                SELECT sbco.tcgplayer_id,
                       COALESCE(sbco.component_type, 'sealed') AS component_type,
                       sbc.tcgplayer_id AS parent_tcg_id,
                       MAX(sbco.market_price) AS last_market_price,
                       MAX(sbco.market_price_updated_at) AS last_priced_at
                FROM sealed_breakdown_components sbco
                JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
                JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
                WHERE sbco.tcgplayer_id IS NOT NULL
                GROUP BY sbco.tcgplayer_id, sbco.component_type, sbc.tcgplayer_id
            ),
            agg AS (
                SELECT tcgplayer_id,
                       component_type,
                       array_agg(DISTINCT parent_tcg_id) AS parent_tcg_ids,
                       COUNT(DISTINCT parent_tcg_id) AS recipe_count,
                       MAX(last_market_price) AS last_market_price,
                       MAX(last_priced_at) AS last_priced_at
                FROM comp_recipes
                GROUP BY tcgplayer_id, component_type
            )
            SELECT a.tcgplayer_id,
                   a.component_type,
                   a.parent_tcg_ids,
                   a.recipe_count,
                   a.last_market_price,
                   a.last_priced_at,
                   EXISTS (
                       SELECT 1 FROM scrydex_price_cache spc
                       WHERE spc.tcgplayer_id = a.tcgplayer_id
                         AND spc.product_type = (CASE WHEN a.component_type = 'promo'
                                                       THEN 'card' ELSE 'sealed' END)
                   ) AS in_cache_correct_type,
                   EXISTS (
                       SELECT 1 FROM scrydex_price_cache spc
                       WHERE spc.tcgplayer_id = a.tcgplayer_id
                   ) AS in_cache_any_type,
                   (SELECT product_type FROM scrydex_price_cache spc
                     WHERE spc.tcgplayer_id = a.tcgplayer_id LIMIT 1) AS cached_product_type
            FROM agg a
            ORDER BY a.recipe_count DESC, a.tcgplayer_id
        """)

        # Default: only rows missing the expected type. With include_any_row=1,
        # also surface rows where the cache has a row of a *different* type —
        # those usually mean the recipe author tagged a card as sealed (or
        # vice versa) and the lookup goes to the wrong column.
        broken = []
        for r in rows:
            in_correct = r["in_cache_correct_type"]
            if include_any:
                if in_correct:
                    continue  # this one's fine
            else:
                if in_correct:
                    continue
                if r["in_cache_any_type"]:
                    # Tagged with wrong component_type — surface separately
                    pass
            broken.append({
                "tcgplayer_id": int(r["tcgplayer_id"]),
                "component_type": r["component_type"],
                "expected_cache_type": "card" if r["component_type"] == "promo" else "sealed",
                "in_cache_any_type": bool(r["in_cache_any_type"]),
                "cached_product_type": r["cached_product_type"],
                "wrong_type_in_cache": (
                    bool(r["in_cache_any_type"]) and not r["in_cache_correct_type"]
                ),
                "recipe_count": int(r["recipe_count"]),
                "parent_tcg_ids": [int(p) for p in (r["parent_tcg_ids"] or [])],
                "last_market_price": float(r["last_market_price"]) if r["last_market_price"] is not None else None,
                "last_priced_at": r["last_priced_at"].isoformat() if r["last_priced_at"] else None,
            })

        # Enrich parent TCG IDs with their Shopify titles so the UI/JSON
        # reader can identify the recipe without a second lookup.
        all_parents = sorted({p for row in broken for p in row["parent_tcg_ids"]})
        parent_titles: dict[int, str] = {}
        if all_parents:
            try:
                ph = ",".join(["%s"] * len(all_parents))
                for r in db_module.query(
                    f"SELECT tcgplayer_id, title FROM inventory_product_cache "
                    f"WHERE tcgplayer_id IN ({ph})",
                    tuple(all_parents),
                ):
                    parent_titles[int(r["tcgplayer_id"])] = r["title"] or ""
            except Exception as e:
                logger.warning(f"audit-missing: parent title enrichment failed: {e}")

        for row in broken:
            row["parents"] = [
                {"tcgplayer_id": p, "title": parent_titles.get(p)}
                for p in row["parent_tcg_ids"]
            ]
            del row["parent_tcg_ids"]

        return jsonify({
            "missing_count": len(broken),
            "wrong_type_count": sum(1 for b in broken if b["wrong_type_in_cache"]),
            "no_row_count": sum(1 for b in broken if not b["in_cache_any_type"]),
            "missing": broken,
        })

    @bp.route("/audit-missing/fix-wrong-type", methods=["POST"])
    def fix_wrong_type():
        """Bulk-flip component_type from 'sealed' to 'promo' on every breakdown
        component whose tcgplayer_id only exists in scrydex_price_cache as
        product_type='card'. Recipe author tagged the card as sealed; the
        lookup misses because get_sealed_market_price queries product_type='sealed'.

        Body: {"dry_run": true} to preview without changing anything.
        Body: {"component_id": "..."} to fix only one row (used by per-row UI button).
        """
        data = request.get_json(silent=True) or {}
        dry_run = bool(data.get("dry_run"))
        single_id = data.get("component_id")

        if single_id:
            # Single-row mode: validate it actually qualifies before flipping.
            row = db_module.query_one("""
                SELECT sbco.id, sbco.tcgplayer_id, sbc.tcgplayer_id AS parent_tcg_id,
                       EXISTS (SELECT 1 FROM scrydex_price_cache spc
                                WHERE spc.tcgplayer_id = sbco.tcgplayer_id
                                  AND spc.product_type = 'card') AS in_card,
                       EXISTS (SELECT 1 FROM scrydex_price_cache spc
                                WHERE spc.tcgplayer_id = sbco.tcgplayer_id
                                  AND spc.product_type = 'sealed') AS in_sealed,
                       COALESCE(sbco.component_type, 'sealed') AS current_type
                FROM sealed_breakdown_components sbco
                JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
                JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
                WHERE sbco.id = %s
            """, (single_id,))
            if not row:
                return jsonify({"error": f"component {single_id} not found"}), 404
            if row["current_type"] != "sealed":
                return jsonify({"error": f"component is type={row['current_type']}, not sealed — refusing to flip"}), 400
            if not row["in_card"] or row["in_sealed"]:
                return jsonify({"error": "cache state doesn't match wrong-type pattern (card row missing or sealed row exists)"}), 400
            if dry_run:
                return jsonify({"dry_run": True, "would_update": 1})
            db_module.execute(
                "UPDATE sealed_breakdown_components SET component_type='promo' WHERE id=%s",
                (single_id,),
            )
            return jsonify({"updated_count": 1, "tcgplayer_id": int(row["tcgplayer_id"])})

        # Bulk mode: find every component matching the wrong-type pattern.
        candidates = db_module.query("""
            SELECT sbco.id, sbco.variant_id, sbco.tcgplayer_id,
                   sbc.tcgplayer_id AS parent_tcg_id
            FROM sealed_breakdown_components sbco
            JOIN sealed_breakdown_variants sbv ON sbv.id = sbco.variant_id
            JOIN sealed_breakdown_cache sbc ON sbc.id = sbv.breakdown_id
            WHERE sbco.tcgplayer_id IS NOT NULL
              AND COALESCE(sbco.component_type, 'sealed') = 'sealed'
              AND EXISTS (SELECT 1 FROM scrydex_price_cache spc
                           WHERE spc.tcgplayer_id = sbco.tcgplayer_id
                             AND spc.product_type = 'card')
              AND NOT EXISTS (SELECT 1 FROM scrydex_price_cache spc
                               WHERE spc.tcgplayer_id = sbco.tcgplayer_id
                                 AND spc.product_type = 'sealed')
        """)

        if not candidates:
            return jsonify({"updated_count": 0, "would_update": 0, "dry_run": dry_run})

        if dry_run:
            return jsonify({
                "dry_run": True,
                "would_update": len(candidates),
                "unique_tcg_ids": len({int(c["tcgplayer_id"]) for c in candidates}),
                "unique_parents": len({int(c["parent_tcg_id"]) for c in candidates}),
            })

        ids = [c["id"] for c in candidates]
        affected_variant_ids = list({str(c["variant_id"]) for c in candidates})
        ph = ",".join(["%s"] * len(ids))
        updated = db_module.execute(
            f"UPDATE sealed_breakdown_components SET component_type='promo' WHERE id IN ({ph})",
            tuple(ids),
        )

        # Force-refresh the flipped components so market_price reflects the
        # new (now-resolvable) card lookup. cache_only=True is safe — if PPT
        # had data we wouldn't be in this code path. max_age_hours=0 makes
        # every just-flipped row count as stale.
        refreshed = 0
        ppt = _get_ppt()
        if ppt and affected_variant_ids:
            try:
                from breakdown_helpers import refresh_stale_component_prices
                refreshed = refresh_stale_component_prices(
                    affected_variant_ids, db_module, ppt,
                    max_age_hours=0, cache_only=True,
                )
            except Exception as e:
                logger.warning(f"Post-flip refresh failed: {e}")

        return jsonify({
            "updated_count": updated,
            "unique_tcg_ids": len({int(c["tcgplayer_id"]) for c in candidates}),
            "unique_parents": len({int(c["parent_tcg_id"]) for c in candidates}),
            "affected_variants": len(affected_variant_ids),
            "refreshed_components": refreshed,
        })

    @bp.route("/audit-missing.html")
    def audit_missing_html():
        """Operator-facing viewer for the audit. Loads JSON via fetch and
        renders two tables: wrong-type (auto-fixable) and no-row (cache gap).
        Bulk-fix button hits /fix-wrong-type."""
        from flask import Response
        import json as _json
        prefix = url_prefix.rstrip("/")
        html = AUDIT_HTML.replace("__PREFIX__", _json.dumps(prefix)[1:-1])
        return Response(html, mimetype="text/html")

    # ─── PPT search (sealed products) ──────────────────────────────

    @bp.route("/search")
    def search_sealed():
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"results": []})
        pricing = _get_ppt()
        if not pricing:
            return jsonify({"results": [], "error": "Price provider not configured"}), 503
        try:
            results = pricing.search_sealed_products(q, limit=10) or []
            # Normalize fields — provider returns varying field names
            for r in results:
                if not r.get("tcgplayer_id"):
                    tcg_id = r.get("tcgplayerId") or r.get("tcgPlayerId") or r.get("id")
                    if tcg_id:
                        try:
                            r["tcgplayer_id"] = int(tcg_id)
                        except (TypeError, ValueError):
                            pass
                # Sealed products: price is in unopenedPrice, not market_price
                if not r.get("market_price"):
                    r["market_price"] = r.get("unopenedPrice") or r.get("marketPrice") or r.get("midPrice") or 0
            # Scrydex sealed hits don't carry tcgplayer_id — backfill from
            # inventory_product_cache when the store already carries the product.
            try:
                from sealed_tcg_enrichment import enrich_sealed_with_shopify_tcg
                enrich_sealed_with_shopify_tcg(results, db_module, price_provider=pricing)
            except Exception as e:
                logger.debug(f"Sealed-TCG enrichment skipped: {e}")
            return jsonify({"results": results})
        except Exception as e:
            details = e.args[2] if len(e.args) > 2 else {}
            retry = details.get("retry_after", 60) if isinstance(details, dict) else 60
            return jsonify({"results": [], "error": str(e.args[0]) if e.args else str(e), "retry_after": retry}), 429

    # ─── PPT search (cards/promos) ──────────────────────────────────

    @bp.route("/search-cards")
    def search_cards():
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"results": []})
        ppt = _get_ppt()
        if not ppt:
            return jsonify({"results": [], "error": "PPT not configured"}), 503
        try:
            set_name = request.args.get("set_name", "").strip() or None
            limit = request.args.get("limit", 8, type=int)
            results = ppt.search_cards(q, set_name=set_name, limit=limit)
            # Extract NM price for each card
            for r in (results or []):
                if not r.get("market_price"):
                    conds = (r.get("prices") or {}).get("conditions") or {}
                    nm = conds.get("Near Mint") or conds.get("NM") or {}
                    r["market_price"] = nm.get("price") or (r.get("prices") or {}).get("market") or 0
            return jsonify({"results": results})
        except Exception as e:
            return jsonify({"results": [], "error": str(e)}), 500

    # ─── Store prices lookup ────────────────────────────────────────

    @bp.route("/store-prices", methods=["POST"])
    def store_prices():
        data = request.get_json(silent=True) or {}
        tcg_ids = [int(x) for x in data.get("tcgplayer_ids", []) if x]
        if not tcg_ids:
            return jsonify({"prices": {}})
        prices = logic.get_store_prices(tcg_ids, db_module)

        # Enrich with velocity data from sku_analytics
        try:
            from sku_analytics import get_analytics_for_tcgplayer_ids
            analytics = get_analytics_for_tcgplayer_ids(tcg_ids, db_module)
            for tcg_id, a in analytics.items():
                if tcg_id in prices:
                    prices[tcg_id]["velocity_score"] = a.get("velocity_score")
                    prices[tcg_id]["units_sold_90d"] = a.get("units_sold_90d")
                elif tcg_id not in prices:
                    # Component not in store but has analytics — still useful
                    prices[tcg_id] = {
                        "tcgplayer_id": tcg_id,
                        "shopify_price": None, "shopify_qty": None,
                        "velocity_score": a.get("velocity_score"),
                        "units_sold_90d": a.get("units_sold_90d"),
                    }
        except Exception as e:
            logger.warning(f"Velocity enrichment skipped: {e}")

        return jsonify({"prices": _serialize(prices)})

    return bp
