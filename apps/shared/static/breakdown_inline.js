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
 * Compute the comparison values for breakdown vs sealed.
 * Implements the unified 4-case price logic.
 *
 * @param {Object} bdData - Breakdown summary from batch API
 * @param {Object} parentData - Parent item data
 * @param {string} priceMode - "market"|"store"|"best"
 * @returns {Object} {sellSealed, sellBroken, caseLabel, pct, delta, color, arrow}
 */
function computeBreakdownComparison(bdData, parentData, priceMode) {
    var parentMarket = parseFloat(parentData.market_price || parentData.parentMarket || 0);
    var parentStore  = parseFloat(parentData.store_price || parentData.parentStore || bdData.parent_store_price || 0) || null;
    var childMarket  = parseFloat(bdData.best_variant_market || 0);
    var childStore   = parseFloat(bdData.best_variant_store || 0) || null;

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
        arrow: arrow
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

    var comp = computeBreakdownComparison(bdData, parentData, priceMode);
    if (!comp || comp.sellBroken <= 0) {
        return; // Can't compute comparison
    }

    // Config label
    var configLabel = '';
    if (bdData.variant_count > 1) {
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
        if (comp.arrow) {
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
