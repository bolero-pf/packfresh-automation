// Shared card-variant visual helpers used by intake + ingest dashboards.
// Keeps printing-badge heuristics (PROMO / PRERELEASE / ETCHED / etc.) and
// foil/variant labeling in one place so both services stay consistent.
//
// Scrydex does not expose structured promo/frame/finish metadata for MTG,
// so these checks are pattern-based on set name, expansion code, and variant
// name. False positives are fine — badges are informational, not blocking.

(function (global) {
    'use strict';

    function mkBadge(text, bg, fg) {
        return '<span style="background:' + bg + '; color:' + fg +
               '; padding:2px 7px; border-radius:4px; font-size:0.65rem;' +
               ' font-weight:700; letter-spacing:0.4px; white-space:nowrap;">' +
               text + '</span>';
    }

    function pfCardPrintingBadges(setName, expCode, variantOrList) {
        var badges = [];
        var sn = (setName || '').toLowerCase();
        var ec = (expCode || '').toUpperCase();
        var vj;
        if (Array.isArray(variantOrList)) {
            vj = variantOrList.map(function (v) {
                return (v && (v.name || v) || '').toString().toLowerCase();
            }).join(' ');
        } else {
            vj = (variantOrList || '').toString().toLowerCase();
        }

        // PROMO — set name contains "promo", or expansion code has the P-prefix
        // used by MTG promo sets (PMKM, PDMU, PWAR). PLST ("The List") is a
        // known false-positive but rare enough to ignore.
        var isPromoSet = /promos?\b|promo pack|promo series/i.test(sn) ||
                         (ec && /^P[A-Z0-9]{2,4}$/.test(ec));
        if (isPromoSet) badges.push(mkBadge('PROMO', '#dc2626', '#fff'));

        if (/prerelease/i.test(vj) || /prerelease/i.test(sn)) badges.push(mkBadge('PRERELEASE', '#7c3aed', '#fff'));
        if (/etched/i.test(vj)) badges.push(mkBadge('ETCHED', '#f59e0b', '#000'));
        if (/extended art/i.test(sn)) badges.push(mkBadge('EXT ART', '#0891b2', '#fff'));
        if (/showcase/i.test(sn)) badges.push(mkBadge('SHOWCASE', '#0891b2', '#fff'));
        if (/borderless/i.test(sn)) badges.push(mkBadge('BORDERLESS', '#0891b2', '#fff'));
        if (/retro frame/i.test(sn)) badges.push(mkBadge('RETRO', '#0891b2', '#fff'));

        return badges.join(' ');
    }

    // Compact variant chip for table rows. Foil-like variants get the amber
    // treatment so they jump out next to the card name.
    function pfVariantLabel(variant) {
        if (!variant) return '';
        var v = String(variant);
        var isFoil = /foil|holo|etched/i.test(v);
        var isChase = /alt|manga|premium|special|enchanted|fullArt|jollyRoger/i.test(v);
        var bg, color, icon;
        if (isChase) { bg = 'var(--amber)'; color = '#000'; icon = '✦ '; }
        else if (isFoil) { bg = 'rgba(245,158,11,0.18)'; color = '#fbbf24'; icon = '✦ '; }
        else { bg = 'var(--surface-2)'; color = 'var(--text-dim)'; icon = ''; }
        return '<span style="background:' + bg + '; color:' + color +
               '; padding:1px 6px; border-radius:4px; font-size:0.65rem;' +
               ' font-weight:700; white-space:nowrap;">' + icon + v + '</span>';
    }

    // Small card thumbnail for table rows. Falls back to a dim placeholder
    // when no image is available so the row layout stays consistent.
    function pfCardThumb(imageUrl, opts) {
        opts = opts || {};
        var w = opts.width || 48;
        var h = opts.height || 67;
        if (imageUrl) {
            return '<img src="' + imageUrl + '" loading="lazy" style="width:' + w +
                   'px; height:' + h + 'px; object-fit:contain; border-radius:4px;' +
                   ' background:var(--surface-2); flex-shrink:0;" alt="">';
        }
        return '<div style="width:' + w + 'px; height:' + h +
               'px; background:var(--surface-2); border-radius:4px; flex-shrink:0;' +
               ' display:flex; align-items:center; justify-content:center;' +
               ' color:var(--text-dim); font-size:0.6rem;">no img</div>';
    }

    global.pfCardPrintingBadges = pfCardPrintingBadges;
    global.pfVariantLabel = pfVariantLabel;
    global.pfCardThumb = pfCardThumb;
})(window);
