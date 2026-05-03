/**
 * Unified Breakdown Inline Badge
 *
 * Renders a compact breakdown summary badge into any container element.
 * Replaces 5 separate implementations across ingestion, intake, and inventory.
 *
 * Color philosophy: most sealed products are worth MORE sealed — that's normal.
 * Only draw attention when breakdown is profitable (green) or near-parity (accent).
 * Expected losses are shown in dim gray — information is there but doesn't shout.
 */

/* global renderBreakdownBadge, computeBreakdownComparison */
/* exported renderBreakdownBadge, computeBreakdownComparison */

/**
 * Pick which variant aggregate to use:
 *   - claimed_variant_id set  → that variant's market/store
 *   - >1 variant (no claim)   → expected (avg) — operator can't pre-select
 *   - 1 variant               → that variant's value (best == only)
 *
 * @param {Object} bdData - breakdown summary from batch API
 * @param {string|null} claimedVariantId - locked variant id (or null)
 * @returns {{market:number, store:?number, label:?string, mode:string}}
 *   `mode` is one of "claimed" | "expected" | "best".
 *   `label` carries the chosen variant's name when mode==="claimed".
 */
function resolveBreakdownAggregate(bdData, claimedVariantId) {
    if (claimedVariantId && Array.isArray(bdData.variants)) {
        for (var i = 0; i < bdData.variants.length; i++) {
            var v = bdData.variants[i];
            if (String(v.id) === String(claimedVariantId)) {
                return {
                    market: parseFloat(v.market || 0),
                    store:  v.store != null ? parseFloat(v.store) : null,
                    label:  v.name || null,
                    mode:   "claimed",
                };
            }
        }
        // Stale claim — fall through to default.
    }
    var multiVariant = (bdData.variant_count || 0) > 1;
    if (multiVariant && bdData.expected_variant_market != null) {
        return {
            market: parseFloat(bdData.expected_variant_market || 0),
            store:  bdData.expected_variant_store != null ? parseFloat(bdData.expected_variant_store) : null,
            label:  null,
            mode:   "expected",
        };
    }
    return {
        market: parseFloat(bdData.best_variant_market || 0),
        store:  bdData.best_variant_store != null ? parseFloat(bdData.best_variant_store) : null,
        label:  null,
        mode:   "best",
    };
}


/**
 * Compute the comparison values for breakdown vs sealed.
 * Implements the unified 4-case price logic.
 *
 * @param {Object} bdData - Breakdown summary from batch API
 * @param {Object} parentData - Parent item data
 * @param {string} priceMode - "market"|"store"|"best"
 * @param {Object} [opts] - { claimedVariantId }
 * @returns {Object} {sellSealed, sellBroken, caseLabel, pct, delta, color, arrow,
 *                    aggMode, aggLabel}
 */
function computeBreakdownComparison(bdData, parentData, priceMode, opts) {
    opts = opts || {};
    var parentMarket = parseFloat(parentData.market_price || parentData.parentMarket || 0);
    var parentStore  = parseFloat(parentData.store_price || parentData.parentStore || bdData.parent_store_price || 0) || null;
    var agg = resolveBreakdownAggregate(bdData, opts.claimedVariantId || parentData.claimed_variant_id || null);
    var childMarket  = agg.market || 0;
    var childStore   = agg.store != null ? agg.store : null;

    var sellSealed, sellBroken, caseLabel;

    if (priceMode === 'market') {
        sellSealed = parentMarket;
        sellBroken = childMarket;
        caseLabel = 'mkt $' + childMarket.toFixed(2) + ' vs sealed mkt $' + parentMarket.toFixed(2);
    } else if (priceMode === 'store') {
        if (parentStore && childStore) {
            sellSealed = parentStore;
            sellBroken = childStore;
            caseLabel = 'store $' + childStore.toFixed(2) + ' vs sealed store $' + parentStore.toFixed(2);
        } else {
            // Can't compare store-to-store — no data
            return null;
        }
    } else {
        // "best" mode: prefer store when both available, fall back to market
        if (parentStore && childStore) {
            sellSealed = parentStore;
            sellBroken = childStore;
            caseLabel = 'store $' + childStore.toFixed(2) + ' vs sealed store $' + parentStore.toFixed(2);
        } else if (parentStore && !childStore) {
            sellSealed = parentStore;
            sellBroken = childMarket;
            caseLabel = 'mkt $' + childMarket.toFixed(2) + ' vs sealed store $' + parentStore.toFixed(2) + ' \u26a0 children not listed';
        } else if (!parentStore && childStore) {
            sellSealed = parentMarket || childMarket;
            sellBroken = childStore;
            caseLabel = 'store $' + childStore.toFixed(2) + ' vs sealed mkt $' + (parentMarket || childMarket).toFixed(2) + ' \u26a0 parent not listed';
        } else {
            sellSealed = parentMarket;
            sellBroken = childMarket;
            caseLabel = 'mkt $' + childMarket.toFixed(2) + ' vs sealed mkt $' + parentMarket.toFixed(2) + ' (no store data)';
        }
    }

    var delta = sellBroken - sellSealed;
    var pct = sellSealed > 0 ? (delta / sellSealed * 100) : 0;

    // Color: green = profitable, accent = near-parity, dim = expected loss
    var color, arrow;
    if (pct > 0) {
        color = 'var(--green, #2dd4a0)';
        arrow = '\u25b2';  // ▲
    } else if (pct >= -15) {
        color = 'var(--accent, #7c93f5)';
        arrow = '\u2248';  // ≈
    } else {
        color = 'var(--text-dim, #666)';
        arrow = '';
    }

    return {
        sellSealed: sellSealed,
        sellBroken: sellBroken,
        caseLabel: caseLabel,
        pct: pct,
        delta: delta,
        color: color,
        arrow: arrow,
        aggMode: agg.mode,
        aggLabel: agg.label,
    };
}


/**
 * Render a breakdown badge into a container element.
 *
 * @param {HTMLElement} container - Where to append the badge
 * @param {Object} bdData - Breakdown summary from batch API:
 *   {best_variant_market, best_variant_store?, parent_store_price?, variant_count,
 *    variant_names?, deep_bd_value?, components_in_store?, total_components?}
 * @param {Object} parentData - Parent item data:
 *   {market_price?, store_price?, parentMarket?, parentStore?}
 * @param {Object} [options]
 * @param {string} [options.priceMode="best"] - "market"|"store"|"best"
 * @param {boolean} [options.showDeep=true] - Show deep breakdown value
 * @param {boolean} [options.compact=false] - Compact mode for tight table cells
 */
function renderBreakdownBadge(container, bdData, parentData, options) {
    options = options || {};
    var priceMode = options.priceMode || 'best';
    var compact   = options.compact || false;

    if (!bdData || !parseFloat(bdData.best_variant_market || 0)) {
        return; // No breakdown data
    }

    var claimedId = options.claimedVariantId || parentData.claimed_variant_id || null;
    var comp = computeBreakdownComparison(bdData, parentData, priceMode, { claimedVariantId: claimedId });
    if (!comp || comp.sellBroken <= 0) {
        return; // Can't compute comparison
    }

    // Per-variant value list for "avg ($40, $120)" formatting (probabilistic only).
    var variantList = Array.isArray(bdData.variants) ? bdData.variants : [];
    var variantValues = variantList.map(function (v) {
        var val = (v.store != null && v.store > 0) ? v.store : v.market;
        return parseFloat(val || 0);
    }).filter(function (n) { return n > 0; });

    // Config / aggregation label
    var configLabel = '';
    if (comp.aggMode === 'claimed' || comp.aggMode === 'expected') {
        configLabel = 'BD';
    } else if (bdData.variant_count > 1) {
        configLabel = bdData.variant_count + ' configs';
    } else if (bdData.variant_names) {
        configLabel = bdData.variant_names;
    }

    // Build badge HTML
    var parts = [];

    // Main value
    if (compact) {
        parts.push('<span style="color:' + comp.color + ';font-weight:600;" title="' + _esc(comp.caseLabel) + '">');
        parts.push('$' + comp.sellBroken.toFixed(2));
        parts.push('</span>');
    } else {
        // Build the badge — single line, no deep value (deep lives in the modal)
        var labelText = '';
        if (comp.arrow) labelText += comp.arrow + ' ';
        if (configLabel) labelText += configLabel;
        else labelText += 'BD';

        var valueText = '$' + comp.sellBroken.toFixed(2);
        if (comp.aggMode === 'claimed' && comp.aggLabel) {
            valueText += ' (claimed: ' + _esc(comp.aggLabel) + ')';
        } else if (comp.aggMode === 'expected' && variantValues.length > 1) {
            // "$80 avg ($40, $120)"
            var listed = variantValues.map(function (n) { return '$' + n.toFixed(2); }).join(', ');
            valueText += ' avg (' + listed + ')';
        } else if (comp.arrow) {
            var sign = comp.delta >= 0 ? '+' : '';
            valueText += ' (' + sign + '$' + Math.abs(comp.delta).toFixed(2) + ', ' + sign + comp.pct.toFixed(1) + '%)';
        }

        parts.push('<small class="bd-badge" style="display:block;color:' + comp.color + ';font-size:0.75rem;margin-top:2px;cursor:default;" title="' + _esc(comp.caseLabel) + '">');
        parts.push('\ud83d\udce6 ' + labelText + ': ' + valueText);

        parts.push('</small>');
    }

    // Append to container
    var span = document.createElement('span');
    span.innerHTML = parts.join('');
    container.appendChild(span.firstChild || span);
}


function _esc(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}


/**
 * Render a "Variant: [Unknown / Kanto Starters / ...]" picker for probabilistic
 * recipes. Only renders when the recipe has variant_resolution=probabilistic
 * and at least 2 variants.
 *
 * @param {HTMLElement} container - where to append the picker
 * @param {Object} bdData - breakdown summary
 * @param {string|null} claimedVariantId - currently locked variant (or null)
 * @param {Function} onChange - async (newVariantIdOrNull) => any
 *   Called when operator selects a new variant. Receives null when "Unknown" picked.
 */
function renderVariantClaimPicker(container, bdData, claimedVariantId, onChange) {
    if (!bdData || !Array.isArray(bdData.variants) || bdData.variants.length < 2) return;

    var wrap = document.createElement('span');
    wrap.className = 'bd-variant-picker';
    wrap.style.cssText = 'display:inline-flex;align-items:center;gap:4px;margin-left:8px;font-size:0.7rem;color:var(--text-dim,#888);';

    var label = document.createElement('span');
    label.textContent = 'Variant:';
    wrap.appendChild(label);

    var sel = document.createElement('select');
    sel.style.cssText = 'font-size:0.7rem;padding:1px 4px;background:var(--surface-2,#1a1a2e);color:var(--text,#ddd);border:1px solid var(--border,#333);border-radius:3px;';

    var unknownOpt = document.createElement('option');
    unknownOpt.value = '';
    var avgPart = (bdData.expected_variant_store != null && bdData.expected_variant_store > 0)
        ? bdData.expected_variant_store
        : bdData.expected_variant_market;
    unknownOpt.textContent = 'Unknown' + (avgPart ? ' — avg $' + parseFloat(avgPart).toFixed(2) : '');
    sel.appendChild(unknownOpt);

    bdData.variants.forEach(function (v) {
        var opt = document.createElement('option');
        opt.value = v.id;
        var val = (v.store != null && v.store > 0) ? v.store : v.market;
        opt.textContent = (v.name || 'Variant') + ' — $' + parseFloat(val || 0).toFixed(2);
        if (claimedVariantId && String(v.id) === String(claimedVariantId)) opt.selected = true;
        sel.appendChild(opt);
    });

    sel.addEventListener('change', function () {
        var newId = sel.value || null;
        if (typeof onChange === 'function') onChange(newId);
    });

    wrap.appendChild(sel);
    container.appendChild(wrap);
}
