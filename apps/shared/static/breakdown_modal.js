/**
 * Unified Breakdown Modal
 *
 * Single modal implementation for recipe editing + optional execution.
 * Replaces 3 separate modal implementations across ingestion, intake, and inventory.
 *
 * Usage:
 *   openBreakdownModal({
 *     tcgplayerId: 12345,
 *     productName: "Pokemon ETB",
 *     parentMarket: 49.99,
 *     parentStore: 54.99,
 *     parentQty: 5,
 *     apiBase: "/api/breakdown-cache",
 *     priceMode: "best",            // "market"|"store"|"best"
 *     onExecute: null,              // or async (variantId, qty, components) => result
 *     onSave: (cache) => {},        // called after save for service-specific refresh
 *     showQtySelector: false,
 *   });
 */

/* global openBreakdownModal */
/* exported openBreakdownModal */

(function() {
    'use strict';

    // ─── State ──────────────────────────────────────────────────────

    var _opts = {};
    var _components = [];    // Current editor components
    var _configName = 'Standard';
    var _editingVariantId = null;
    var _notes = '';
    var _showNotes = false;
    var _storePrices = {};   // tcg_id -> {shopify_price, shopify_qty, ...}
    var _cacheData = null;   // Full cache record from API
    var _searchResults = {}; // Keyed by index for safe onclick
    var _promoResults = {};
    var _overlayEl = null;

    // ─── Public API ─────────────────────────────────────────────────

    window.openBreakdownModal = function(options) {
        _opts = Object.assign({
            tcgplayerId: null,
            productName: '',
            parentMarket: 0,
            parentStore: null,
            parentQty: 1,
            apiBase: '/api/breakdown-cache',
            priceMode: 'best',
            onExecute: null,
            onSave: null,
            showQtySelector: false,
            showDeep: true,
        }, options);

        // Reset state
        _components = [];
        _configName = 'Standard';
        _editingVariantId = null;
        _notes = '';
        _showNotes = false;
        _storePrices = {};
        _cacheData = null;
        _searchResults = {};
        _promoResults = {};

        _createOverlay();
        _renderLoading();

        if (_opts.tcgplayerId) {
            _loadCache(_opts.tcgplayerId);
        } else {
            _renderEditor();
        }
    };

    // ─── Overlay / Lifecycle ────────────────────────────────────────

    function _createOverlay() {
        _closeModal();
        var el = document.createElement('div');
        el.className = 'bd-overlay';
        // Only close if BOTH mousedown and mouseup are on the overlay itself.
        // Prevents accidental dismiss when selecting text and releasing outside the modal.
        var _mouseDownOnOverlay = false;
        el.addEventListener('mousedown', function(e) {
            _mouseDownOnOverlay = (e.target === el);
        });
        el.addEventListener('mouseup', function(e) {
            if (_mouseDownOnOverlay && e.target === el) _closeModal();
            _mouseDownOnOverlay = false;
        });
        document.body.appendChild(el);
        _overlayEl = el;
        // ESC to close
        document.addEventListener('keydown', _escHandler);
    }

    function _escHandler(e) {
        if (e.key === 'Escape') _closeModal();
    }

    function _closeModal() {
        if (_overlayEl) {
            _overlayEl.remove();
            _overlayEl = null;
        }
        document.removeEventListener('keydown', _escHandler);
    }

    function _renderLoading() {
        if (!_overlayEl) return;
        _overlayEl.innerHTML = '<div class="bd-modal"><div class="bd-loading"><span class="bd-spinner"></span> Loading...</div></div>';
    }

    function _toast(msg) {
        var t = document.createElement('div');
        t.className = 'bd-toast show';
        t.textContent = msg;
        document.body.appendChild(t);
        setTimeout(function() { t.classList.remove('show'); }, 2500);
        setTimeout(function() { t.remove(); }, 3000);
    }

    // ─── Data Loading ───────────────────────────────────────────────

    function _loadCache(tcgId) {
        fetch(_opts.apiBase + '/' + tcgId)
            .then(function(r) { return r.json(); })
            .then(function(data) {
                _cacheData = data.found ? data.cache : null;
                _renderEditor();
                // Fetch parent store price even if editor is collapsed
                if (_opts.tcgplayerId && !_opts.parentStore) {
                    _fetchParentStorePrice();
                }
            })
            .catch(function() {
                _cacheData = null;
                _renderEditor();
            });
    }

    function _fetchParentStorePrice() {
        if (!_opts.tcgplayerId) return;
        fetch(_opts.apiBase + '/store-prices', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tcgplayer_ids: [_opts.tcgplayerId] })
        })
        .then(function(r) { return r.json(); })
        .then(function(data) {
            var prices = data.prices || {};
            var ps = prices[_opts.tcgplayerId];
            if (ps && ps.shopify_price) {
                _opts.parentStore = parseFloat(ps.shopify_price);
                _renderParentInfo();
            }
        })
        .catch(function() {});
    }

    function _fetchStorePrices() {
        var ids = _components.filter(function(c) { return c.tcgplayer_id; })
                             .map(function(c) { return c.tcgplayer_id; });
        // Also include parent
        if (_opts.tcgplayerId) ids.push(_opts.tcgplayerId);
        if (!ids.length) return;

        fetch(_opts.apiBase + '/store-prices', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tcgplayer_ids: ids })
        })
        .then(function(r) { return r.json(); })
        .then(function(data) {
            _storePrices = data.prices || {};
            // Update parent store price if we got it
            if (_opts.tcgplayerId && _storePrices[_opts.tcgplayerId]) {
                var ps = _storePrices[_opts.tcgplayerId];
                if (ps.shopify_price) _opts.parentStore = parseFloat(ps.shopify_price);
            }
            _renderComponentTable();
            _renderSummary();
            _renderParentInfo();
        })
        .catch(function() {});
    }

    // ─── Main Render ────────────────────────────────────────────────

    function _renderEditor() {
        if (!_overlayEl) return;

        var html = '<div class="bd-modal">';

        // Header
        html += '<div class="bd-header">';
        html += '<h3>' + _esc(_opts.productName);
        if (_opts.tcgplayerId) html += ' <span class="bd-pill" style="font-size:11px;background:var(--bg-highlight,#2a2a4a);padding:2px 6px;border-radius:4px;color:var(--text-dim,#888);">#' + _opts.tcgplayerId + '</span>';
        html += '</h3>';
        html += '<button class="bd-close" id="bd-close-btn">&times;</button>';
        html += '</div>';

        // Parent info bar
        html += '<div class="bd-parent-info" id="bd-parent-info">';
        html += _renderParentInfoHTML();
        html += '</div>';

        // Body
        html += '<div class="bd-body">';

        // Saved configs
        if (_cacheData && _cacheData.variants && _cacheData.variants.length > 0) {
            html += _renderConfigCards();
        }

        // Editor — collapsed behind "Add Config" when saved configs exist and nothing loaded
        var hasConfigs = _cacheData && _cacheData.variants && _cacheData.variants.length > 0;
        var editorOpen = _components.length > 0 || !hasConfigs;

        if (hasConfigs && !editorOpen) {
            html += '<div style="margin:12px 0;">';
            html += '<button class="bd-btn bd-btn-secondary" id="bd-add-config-btn" style="width:100%;">+ Add New Config</button>';
            html += '</div>';
        }

        html += '<div class="bd-editor" id="bd-editor-section" style="display:' + (editorOpen ? 'block' : 'none') + ';">';

        // Config name row
        html += '<div class="bd-editor-row">';
        html += '<label>Config name:</label>';
        html += '<input type="text" id="bd-config-name" value="' + _esc(_configName) + '">';
        html += '<span id="bd-editing-badge" style="display:none;" class="bd-editing-badge">editing</span>';
        html += '</div>';

        // Search panels
        html += '<div class="bd-search-grid">';
        html += _renderSearchPanel('sealed', 'Sealed', 'bd-search-sealed', 'bd-sealed-results');
        html += _renderSearchPanel('promos', 'Promos (NM price)', 'bd-search-promo', 'bd-promo-results');
        html += '</div>';

        // Component table
        html += '<div id="bd-comp-table-container">';
        html += _renderComponentTableHTML();
        html += '</div>';

        // Notes
        html += '<div class="bd-notes-row" id="bd-notes-row" style="display:' + (_showNotes ? 'block' : 'none') + ';">';
        html += '<textarea id="bd-notes" placeholder="Notes (optional)">' + _esc(_notes) + '</textarea>';
        html += '</div>';

        // Value summary
        html += '<div id="bd-summary-container">';
        html += _renderSummaryHTML();
        html += '</div>';

        // Qty selector
        if (_opts.showQtySelector && _opts.parentQty > 1) {
            html += _renderQtySelectorHTML();
        }

        html += '</div>'; // .bd-editor
        html += '</div>'; // .bd-body

        // Action bar
        html += '<div class="bd-actions">';
        html += '<button class="bd-btn-notes" id="bd-toggle-notes" style="display:' + (editorOpen ? '' : 'none') + ';">+ Notes</button>';
        html += '<button class="bd-btn bd-btn-secondary" id="bd-cancel-btn">' + (editorOpen ? 'Cancel' : 'Close') + '</button>';
        html += '<button class="bd-btn bd-btn-primary" id="bd-save-btn" style="display:' + (editorOpen ? '' : 'none') + ';">Save Recipe</button>';
        if (_opts.onExecute) {
            html += '<button class="bd-btn bd-btn-execute" id="bd-execute-btn" style="display:' + (editorOpen ? '' : 'none') + ';">Execute Breakdown</button>';
        }
        html += '</div>';

        html += '</div>'; // .bd-modal

        _overlayEl.innerHTML = html;
        _bindEvents();

        // Auto-fetch store prices
        if (_components.length > 0) {
            _fetchStorePrices();
        }
    }

    function _renderParentInfoHTML() {
        var parts = [];
        // Always show Store first, then Market
        var storeVal = parseFloat(_opts.parentStore || 0);
        var mktVal = parseFloat(_opts.parentMarket || 0);
        parts.push('<span><span class="bd-price-label">Store:</span> <span class="bd-price-tag" style="color:' + (storeVal > 0 ? 'var(--green,#2dd4a0)' : 'var(--text-dim,#888)') + ';">' + (storeVal > 0 ? '$' + storeVal.toFixed(2) : 'not listed') + '</span></span>');
        parts.push('<span><span class="bd-price-label">Market:</span> <span class="bd-price-tag">' + (mktVal > 0 ? '$' + mktVal.toFixed(2) : '\u2014') + '</span></span>');
        if (_opts.parentQty > 0) {
            parts.push('<span><span class="bd-price-label">Qty:</span> <span class="bd-price-tag">' + _opts.parentQty + '</span></span>');
        }
        return parts.join('');
    }

    function _renderParentInfo() {
        var el = document.getElementById('bd-parent-info');
        if (el) el.innerHTML = _renderParentInfoHTML();
    }

    // ─── Config Cards ───────────────────────────────────────────────

    function _renderConfigCards() {
        var variants = _cacheData.variants;
        var html = '<div class="bd-configs-section">';
        html += '<div class="bd-configs-header">Saved configs (' + variants.length + ')</div>';
        html += '<div class="bd-config-cards">';
        for (var i = 0; i < variants.length; i++) {
            var v = variants[i];
            var mkt = parseFloat(v.total_component_market || 0);
            var compCount = v.components ? v.components.length : (v.component_count || 0);

            // Compute delta vs parent
            var parentRef = parseFloat(_opts.parentStore || _opts.parentMarket || 0);
            var delta = parentRef > 0 ? ((mkt - parentRef) / parentRef * 100) : 0;
            var deltaColor, deltaStr;
            if (delta > 0) {
                deltaColor = 'var(--green,#2dd4a0)';
                deltaStr = '+' + delta.toFixed(1) + '%';
            } else if (delta >= -15) {
                deltaColor = 'var(--accent,#7c93f5)';
                deltaStr = delta.toFixed(1) + '%';
            } else {
                deltaColor = 'var(--text-dim,#888)';
                deltaStr = delta.toFixed(1) + '%';
            }

            html += '<div class="bd-config-card" data-variant-index="' + i + '">';
            html += '<div class="bd-config-name">' + _esc(v.variant_name || 'Standard') + '</div>';
            html += '<div class="bd-config-meta">' + compCount + ' components &middot; $' + mkt.toFixed(2) + ' market</div>';
            if (parentRef > 0) {
                html += '<div class="bd-config-value" style="color:' + deltaColor + ';">' + deltaStr + ' vs ' + (_opts.parentStore ? 'store' : 'market') + '</div>';
            }
            if (v.notes) {
                html += '<div class="bd-config-meta" style="margin-top:2px;font-style:italic;">' + _esc(v.notes) + '</div>';
            }
            html += '<div class="bd-config-actions">';
            html += '<button class="bd-btn-load" data-variant-index="' + i + '">Load</button>';
            html += '<button class="bd-btn-delete" data-variant-id="' + v.id + '">Delete</button>';
            html += '</div>';
            html += '</div>';
        }
        html += '</div></div>';
        return html;
    }

    // ─── Search Panel ───────────────────────────────────────────────

    function _renderSearchPanel(type, title, inputId, resultsId) {
        var prefix = type === 'sealed' ? '\ud83d\udce6' : '\u2728';
        var html = '<div class="bd-search-panel ' + type + '">';
        html += '<div class="bd-search-title">' + prefix + ' ' + title + '</div>';
        html += '<div class="bd-search-row">';
        html += '<input type="text" id="' + inputId + '" placeholder="Search...">';
        html += '<button data-search-type="' + type + '">Search</button>';
        html += '</div>';
        html += '<div class="bd-search-results" id="' + resultsId + '"></div>';
        html += '</div>';
        return html;
    }

    // ─── Component Table ────────────────────────────────────────────

    function _renderComponentTableHTML() {
        if (_components.length === 0) {
            return '<div style="color:var(--text-dim,#888);font-size:12px;padding:8px 0;">No components added yet. Use search above to add.</div>';
        }

        var hasStore = Object.keys(_storePrices).length > 0;
        var sealedComps = _components.filter(function(c) { return c.component_type !== 'promo'; });
        var promoComps = _components.filter(function(c) { return c.component_type === 'promo'; });

        var html = '<table class="bd-comp-table">';
        html += '<thead><tr>';
        html += '<th>Component</th><th style="width:55px;">Qty</th><th style="width:75px;">Market</th>';
        if (hasStore) html += '<th style="width:160px;">Store</th>';
        html += '<th style="width:30px;"></th>';
        html += '</tr></thead><tbody>';

        if (sealedComps.length > 0 && promoComps.length > 0) {
            html += '<tr class="bd-section-header"><td colspan="' + (hasStore ? 5 : 4) + '">\ud83d\udce6 Sealed</td></tr>';
        }
        for (var i = 0; i < _components.length; i++) {
            var c = _components[i];
            if (promoComps.length > 0 && sealedComps.length > 0 && c === promoComps[0]) {
                html += '<tr class="bd-section-header"><td colspan="' + (hasStore ? 5 : 4) + '">\u2728 Promos</td></tr>';
            }
            html += _renderComponentRow(c, i, hasStore);
        }

        html += '</tbody></table>';
        return html;
    }

    function _renderComponentRow(c, idx, hasStore) {
        var html = '<tr>';

        // Name
        html += '<td>';
        html += '<div class="bd-comp-name">' + _esc(c.product_name);
        if (c.component_type === 'promo') html += ' <span class="bd-promo-badge">PROMO</span>';
        html += '</div>';
        if (c.tcgplayer_id) {
            html += '<div class="bd-comp-tcg">#' + c.tcgplayer_id + '</div>';
        } else {
            html += '<div class="bd-comp-tcg" style="color:var(--red,#f05252);">\u26a0 no TCG ID</div>';
        }
        if (c.set_name) html += '<div class="bd-comp-set">' + _esc(c.set_name) + '</div>';
        html += '</td>';

        // Qty
        html += '<td><input type="number" min="1" value="' + (c.quantity_per_parent || 1) + '" data-comp-idx="' + idx + '" data-field="qty"></td>';

        // Market
        html += '<td><input type="number" min="0" step="0.01" value="' + parseFloat(c.market_price || 0).toFixed(2) + '" data-comp-idx="' + idx + '" data-field="market"></td>';

        // Store + Qty + Velocity
        if (hasStore) {
            var sp = c.tcgplayer_id ? _storePrices[c.tcgplayer_id] : null;
            html += '<td style="white-space:nowrap;">';
            if (sp && sp.shopify_price) {
                var storePrice = parseFloat(sp.shopify_price);
                var storeQty = parseInt(sp.shopify_qty) || 0;
                var qtyColor = storeQty === 0 ? 'var(--red,#f05252)' : storeQty <= 3 ? 'var(--amber,#f5a623)' : 'var(--text-dim,#888)';
                html += '<span class="bd-store-price">$' + storePrice.toFixed(2) + '</span>';
                // Qty + OOS purely from store data
                if (storeQty === 0) {
                    html += ' <span style="color:var(--red,#f05252);font-size:10px;">OOS</span>';
                } else {
                    html += ' <span style="color:' + qtyColor + ';font-size:10px;">qty:' + storeQty + '</span>';
                }
                // Velocity: days of stock (only when qty > 0 and we have data)
                if (storeQty > 0 && sp.velocity_score != null) {
                    var days = parseFloat(sp.velocity_score);
                    var velColor = days <= 14 ? 'var(--green,#2dd4a0)' : days <= 60 ? 'var(--text-dim,#888)' : 'var(--red,#f05252)';
                    html += ' <span style="color:' + velColor + ';font-size:10px;">' + Math.round(days) + 'd stock</span>';
                }
            } else if (c.component_type === 'promo') {
                html += '<span class="bd-no-store">&mdash;</span>';
            } else {
                html += '<span class="bd-no-store">not listed</span>';
            }
            html += '</td>';
        }

        // Delete
        html += '<td><button class="bd-delete-btn" data-comp-idx="' + idx + '">&times;</button></td>';

        html += '</tr>';
        return html;
    }

    function _renderComponentTable() {
        var el = document.getElementById('bd-comp-table-container');
        if (el) el.innerHTML = _renderComponentTableHTML();
        _bindComponentEvents();
    }

    // ─── Value Summary ──────────────────────────────────────────────

    function _renderSummaryHTML() {
        if (_components.length === 0) return '';

        var totalMarket = 0;
        var totalStore = 0;
        var storeCount = 0;
        var hasStoreData = Object.keys(_storePrices).length > 0;

        for (var i = 0; i < _components.length; i++) {
            var c = _components[i];
            var qty = c.quantity_per_parent || 1;
            var mkt = parseFloat(c.market_price || 0);
            totalMarket += mkt * qty;

            if (hasStoreData && c.tcgplayer_id) {
                var sp = _storePrices[c.tcgplayer_id];
                if (c.component_type === 'promo') {
                    totalStore += mkt * qty;
                    storeCount++;
                } else if (sp && sp.shopify_price) {
                    totalStore += parseFloat(sp.shopify_price) * qty;
                    storeCount++;
                }
            }
        }

        var html = '<div class="bd-summary">';

        // Market row
        html += '<div class="bd-summary-row">';
        html += '<span class="bd-summary-label">Market total:</span>';
        html += '<span class="bd-summary-value">$' + totalMarket.toFixed(2) + '</span>';

        // Market delta vs parent
        var parentMarket = parseFloat(_opts.parentMarket || 0);
        if (parentMarket > 0) {
            var mktDelta = totalMarket - parentMarket;
            var mktPct = (mktDelta / parentMarket * 100);
            var mktClass = mktPct > 0 ? 'positive' : mktPct >= -15 ? 'neutral' : 'negative';
            var mktSign = mktDelta >= 0 ? '+' : '';
            html += '<span class="bd-summary-delta ' + mktClass + '">' + mktSign + '$' + Math.abs(mktDelta).toFixed(2) + ' (' + mktSign + mktPct.toFixed(1) + '%) vs parent market</span>';
        }
        html += '</div>';

        // Store row
        if (hasStoreData && storeCount > 0) {
            html += '<div class="bd-summary-row">';
            html += '<span class="bd-summary-label">Store total:</span>';
            html += '<span class="bd-summary-value" style="color:var(--green,#2dd4a0);">$' + totalStore.toFixed(2) + '</span>';

            var parentStore = parseFloat(_opts.parentStore || 0);
            if (parentStore > 0) {
                var sDelta = totalStore - parentStore;
                var sPct = (sDelta / parentStore * 100);
                var sClass = sPct > 0 ? 'positive' : sPct >= -15 ? 'neutral' : 'negative';
                var sSign = sDelta >= 0 ? '+' : '';
                html += '<span class="bd-summary-delta ' + sClass + '">' + sSign + '$' + Math.abs(sDelta).toFixed(2) + ' (' + sSign + sPct.toFixed(1) + '%) vs parent store</span>';
            }

            // Coverage info
            html += '<span style="color:var(--text-dim,#888);font-size:11px;">(' + storeCount + '/' + _components.length + ' in store)</span>';
            html += '</div>';
        }

        html += '</div>';
        return html;
    }

    function _renderSummary() {
        var el = document.getElementById('bd-summary-container');
        if (el) el.innerHTML = _renderSummaryHTML();
    }

    // ─── Qty Selector ───────────────────────────────────────────────

    function _renderQtySelectorHTML() {
        var max = _opts.parentQty;
        var html = '<div class="bd-qty-section">';
        html += '<label>Break down:</label>';
        html += '<input type="number" id="bd-qty-input" value="1" min="1" max="' + max + '">';
        html += '<span style="color:var(--text-dim,#888);font-size:12px;">of ' + max + ' unit(s)</span>';
        if (max > 1) {
            html += '<div class="bd-qty-buttons">';
            [1, 2, 5, 10].forEach(function(n) {
                if (n <= max) html += '<button data-qty="' + n + '">' + n + '</button>';
            });
            html += '<button data-qty="' + max + '">All</button>';
            html += '</div>';
        }
        html += '</div>';
        return html;
    }

    // ─── Event Binding ──────────────────────────────────────────────

    function _showEditor() {
        var editor = document.getElementById('bd-editor-section');
        var addBtn = document.getElementById('bd-add-config-btn');
        if (editor) editor.style.display = 'block';
        if (addBtn) addBtn.parentElement.style.display = 'none';
        // Show action buttons
        var saveBtn = document.getElementById('bd-save-btn');
        var execBtn = document.getElementById('bd-execute-btn');
        var notesBtn = document.getElementById('bd-toggle-notes');
        var cancelBtn = document.getElementById('bd-cancel-btn');
        if (saveBtn) saveBtn.style.display = '';
        if (execBtn) execBtn.style.display = '';
        if (notesBtn) notesBtn.style.display = '';
        if (cancelBtn) cancelBtn.textContent = 'Cancel';
    }

    function _bindEvents() {
        // Close
        _on('bd-close-btn', 'click', _closeModal);
        _on('bd-cancel-btn', 'click', _closeModal);

        // Add Config button (expands editor)
        _on('bd-add-config-btn', 'click', function() {
            _components = [];
            _configName = 'Standard';
            _editingVariantId = null;
            _notes = '';
            _showNotes = false;
            _showEditor();
        });

        // Notes toggle
        _on('bd-toggle-notes', 'click', function() {
            _showNotes = !_showNotes;
            var nr = document.getElementById('bd-notes-row');
            if (nr) nr.style.display = _showNotes ? 'block' : 'none';
        });

        // Save
        _on('bd-save-btn', 'click', _saveRecipe);

        // Execute
        _on('bd-execute-btn', 'click', _executeBreakdown);

        // Config name
        var nameInput = document.getElementById('bd-config-name');
        if (nameInput) {
            nameInput.addEventListener('input', function() { _configName = this.value; });
        }

        // Search buttons
        var searchBtns = _overlayEl.querySelectorAll('[data-search-type]');
        for (var i = 0; i < searchBtns.length; i++) {
            searchBtns[i].addEventListener('click', function() {
                var type = this.getAttribute('data-search-type');
                if (type === 'sealed') _searchSealed();
                else _searchPromos();
            });
        }

        // Enter key in search inputs
        _onEnter('bd-search-sealed', _searchSealed);
        _onEnter('bd-search-promo', _searchPromos);

        // Load/delete config buttons
        // Config cards: click Load button OR double-click card to load
        var loadBtns = _overlayEl.querySelectorAll('.bd-btn-load');
        for (var j = 0; j < loadBtns.length; j++) {
            loadBtns[j].addEventListener('click', function(e) {
                e.stopPropagation();
                var idx = parseInt(this.getAttribute('data-variant-index'));
                _loadVariant(idx);
            });
        }
        var configCards = _overlayEl.querySelectorAll('.bd-config-card');
        for (var cc = 0; cc < configCards.length; cc++) {
            configCards[cc].addEventListener('dblclick', function(e) {
                var idx = parseInt(this.getAttribute('data-variant-index'));
                _loadVariant(idx);
            });
        }
        var delBtns = _overlayEl.querySelectorAll('.bd-btn-delete');
        for (var k = 0; k < delBtns.length; k++) {
            delBtns[k].addEventListener('click', function(e) {
                e.stopPropagation();
                var vid = this.getAttribute('data-variant-id');
                _deleteVariant(vid);
            });
        }

        // Qty buttons
        var qtyBtns = _overlayEl.querySelectorAll('[data-qty]');
        for (var q = 0; q < qtyBtns.length; q++) {
            qtyBtns[q].addEventListener('click', function() {
                var val = parseInt(this.getAttribute('data-qty'));
                var input = document.getElementById('bd-qty-input');
                if (input) input.value = val;
            });
        }

        _bindComponentEvents();
    }

    function _bindComponentEvents() {
        if (!_overlayEl) return;

        // Component field changes
        var inputs = _overlayEl.querySelectorAll('[data-comp-idx][data-field]');
        for (var i = 0; i < inputs.length; i++) {
            inputs[i].addEventListener('change', function() {
                var idx = parseInt(this.getAttribute('data-comp-idx'));
                var field = this.getAttribute('data-field');
                if (field === 'qty') {
                    _components[idx].quantity_per_parent = parseInt(this.value) || 1;
                } else if (field === 'market') {
                    _components[idx].market_price = parseFloat(this.value) || 0;
                }
                _renderSummary();
            });
        }

        // Delete component
        var delBtns = _overlayEl.querySelectorAll('.bd-delete-btn');
        for (var j = 0; j < delBtns.length; j++) {
            delBtns[j].addEventListener('click', function() {
                var idx = parseInt(this.getAttribute('data-comp-idx'));
                _components.splice(idx, 1);
                _renderComponentTable();
                _renderSummary();
            });
        }
    }

    function _on(id, event, handler) {
        var el = document.getElementById(id);
        if (el) el.addEventListener(event, handler);
    }

    function _onEnter(id, handler) {
        var el = document.getElementById(id);
        if (el) el.addEventListener('keydown', function(e) { if (e.key === 'Enter') handler(); });
    }

    // ─── Search ─────────────────────────────────────────────────────

    function _searchSealed() {
        var input = document.getElementById('bd-search-sealed');
        var q = input ? input.value.trim() : '';
        if (!q) return;
        var panel = document.getElementById('bd-sealed-results');
        if (panel) panel.innerHTML = '<div class="bd-loading"><span class="bd-spinner"></span></div>';

        fetch(_opts.apiBase + '/search?q=' + encodeURIComponent(q))
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    panel.innerHTML = '<div style="color:var(--red,#f05252);font-size:11px;">' + _esc(data.error) + '</div>';
                    return;
                }
                _searchResults = {};
                var results = (data.results || []).slice(0, 15);
                var html = '';
                for (var i = 0; i < results.length; i++) {
                    _searchResults[i] = results[i];
                    var r = results[i];
                    var price = parseFloat(r.market_price || r.unopenedPrice || r.midPrice || r.marketPrice || 0);
                    var setName = r.set_name || r.setName || r.set || '';
                    html += '<div class="bd-search-result" data-sr-idx="' + i + '" data-sr-type="sealed">';
                    html += '<span class="bd-sr-name">' + _esc(r.name || r.product_name || '');
                    if (setName) html += '<br><span class="bd-sr-detail">' + _esc(setName) + '</span>';
                    html += '</span>';
                    html += '<span class="bd-sr-price">' + (price > 0 ? '$' + price.toFixed(2) : '') + '</span>';
                    html += '</div>';
                }
                panel.innerHTML = html || '<div style="color:var(--text-dim,#888);font-size:11px;">No results</div>';
                _bindSearchResultClicks();
            })
            .catch(function(e) {
                panel.innerHTML = '<div style="color:var(--red,#f05252);font-size:11px;">Search failed</div>';
            });
    }

    function _searchPromos() {
        var input = document.getElementById('bd-search-promo');
        var q = input ? input.value.trim() : '';
        if (!q) return;
        var panel = document.getElementById('bd-promo-results');
        if (panel) panel.innerHTML = '<div class="bd-loading"><span class="bd-spinner"></span></div>';

        fetch(_opts.apiBase + '/search-cards?q=' + encodeURIComponent(q))
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.error) {
                    panel.innerHTML = '<div style="color:var(--red,#f05252);font-size:11px;">' + _esc(data.error) + '</div>';
                    return;
                }
                _promoResults = {};
                var results = (data.results || []).slice(0, 15);
                var html = '';
                for (var i = 0; i < results.length; i++) {
                    _promoResults[i] = results[i];
                    var r = results[i];
                    var price = parseFloat(r.market_price || 0);
                    var setName = r.set_name || r.setName || r.set || '';
                    var cardNum = r.number || r.cardNumber || '';
                    var rarity = r.rarity || '';
                    var detail = [setName, cardNum ? '#' + cardNum : '', rarity].filter(Boolean).join(' \u00b7 ');
                    html += '<div class="bd-search-result" data-sr-idx="' + i + '" data-sr-type="promo">';
                    html += '<span class="bd-sr-name">' + _esc(r.name || r.product_name || '');
                    if (detail) html += '<br><span class="bd-sr-detail">' + _esc(detail) + '</span>';
                    html += '</span>';
                    html += '<span class="bd-sr-price">' + (price > 0 ? '$' + price.toFixed(2) : '') + '</span>';
                    html += '</div>';
                }
                panel.innerHTML = html || '<div style="color:var(--text-dim,#888);font-size:11px;">No results</div>';
                _bindSearchResultClicks();
            })
            .catch(function() {
                panel.innerHTML = '<div style="color:var(--red,#f05252);font-size:11px;">Search failed</div>';
            });
    }

    function _bindSearchResultClicks() {
        if (!_overlayEl) return;
        var items = _overlayEl.querySelectorAll('.bd-search-result');
        for (var i = 0; i < items.length; i++) {
            items[i].addEventListener('click', function() {
                var idx = parseInt(this.getAttribute('data-sr-idx'));
                var type = this.getAttribute('data-sr-type');
                var results = type === 'promo' ? _promoResults : _searchResults;
                var r = results[idx];
                if (!r) return;

                var tcgId = r.tcgplayer_id || r.tcgplayerId || r.tcgPlayerId || r.id;

                var newComp = {
                    product_name: r.name || r.product_name || '',
                    tcgplayer_id: tcgId ? parseInt(tcgId) : null,
                    set_name: r.set_name || r.setName || '',
                    quantity_per_parent: 1,
                    market_price: parseFloat(r.market_price || r.unopenedPrice || r.midPrice || r.marketPrice || 0),
                    component_type: type === 'promo' ? 'promo' : 'sealed',
                };

                // Insert sealed items before the first promo to keep sections grouped
                if (newComp.component_type !== 'promo') {
                    var firstPromoIdx = -1;
                    for (var j = 0; j < _components.length; j++) {
                        if (_components[j].component_type === 'promo') { firstPromoIdx = j; break; }
                    }
                    if (firstPromoIdx >= 0) {
                        _components.splice(firstPromoIdx, 0, newComp);
                    } else {
                        _components.push(newComp);
                    }
                } else {
                    _components.push(newComp);
                }

                _renderComponentTable();
                _renderSummary();
                _fetchStorePrices();
            });
        }
    }

    // ─── Load Variant ───────────────────────────────────────────────

    function _loadVariant(idx) {
        if (!_cacheData || !_cacheData.variants[idx]) return;
        var v = _cacheData.variants[idx];

        _configName = v.variant_name || 'Standard';
        _editingVariantId = v.id;
        _notes = v.notes || '';
        _showNotes = !!_notes;
        _components = (v.components || []).map(function(c) {
            return {
                product_name: c.product_name || '',
                tcgplayer_id: c.tcgplayer_id ? parseInt(c.tcgplayer_id) : null,
                set_name: c.set_name || '',
                quantity_per_parent: c.quantity_per_parent || 1,
                market_price: parseFloat(c.market_price || 0),
                component_type: c.component_type || 'sealed',
            };
        });

        // Expand editor if collapsed
        _showEditor();

        // Update UI
        var nameInput = document.getElementById('bd-config-name');
        if (nameInput) nameInput.value = _configName;
        var badge = document.getElementById('bd-editing-badge');
        if (badge) badge.style.display = 'inline';
        var nr = document.getElementById('bd-notes-row');
        if (nr) nr.style.display = _showNotes ? 'block' : 'none';
        var ta = document.getElementById('bd-notes');
        if (ta) ta.value = _notes;

        _renderComponentTable();
        _renderSummary();
        _fetchStorePrices();
        _toast('Loaded: ' + _configName);
    }

    // ─── Delete Variant ─────────────────────────────────────────────

    function _deleteVariant(variantId) {
        if (!confirm('Delete this config?')) return;
        fetch(_opts.apiBase + '/variant/' + variantId, { method: 'DELETE' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
                _cacheData = data.cache || null;
                _renderEditor();
                _toast('Config deleted');
            })
            .catch(function() { _toast('Delete failed'); });
    }

    // ─── Save Recipe ────────────────────────────────────────────────

    function _saveRecipe() {
        if (_components.length === 0) {
            _toast('Add components first');
            return;
        }

        var nameInput = document.getElementById('bd-config-name');
        var notesEl = document.getElementById('bd-notes');
        var name = nameInput ? nameInput.value.trim() : _configName;
        var notes = notesEl ? notesEl.value.trim() : _notes;

        var payload = {
            product_name: _opts.productName,
            variant_name: name || 'Standard',
            notes: notes || null,
            variant_id: _editingVariantId || null,
            components: _components.map(function(c) {
                return {
                    product_name: c.product_name,
                    tcgplayer_id: c.tcgplayer_id,
                    set_name: c.set_name,
                    quantity_per_parent: c.quantity_per_parent || 1,
                    market_price: c.market_price || 0,
                    component_type: c.component_type || 'sealed',
                };
            }),
        };

        var saveBtn = document.getElementById('bd-save-btn');
        if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = 'Saving...'; }

        fetch(_opts.apiBase + '/' + _opts.tcgplayerId + '/variant', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        })
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.error) {
                _toast('Save failed: ' + data.error);
                if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = 'Save Recipe'; }
                return;
            }
            _cacheData = data.cache || null;
            _editingVariantId = null;
            _toast('Recipe saved');
            _renderEditor();

            if (_opts.onSave) {
                _opts.onSave(_cacheData);
            }
        })
        .catch(function(e) {
            _toast('Save failed');
            if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = 'Save Recipe'; }
        });
    }

    // ─── Execute Breakdown ──────────────────────────────────────────

    function _executeBreakdown() {
        if (!_opts.onExecute) return;
        if (_components.length === 0) {
            _toast('Add components first');
            return;
        }

        var qtyInput = document.getElementById('bd-qty-input');
        var qty = qtyInput ? parseInt(qtyInput.value) || 1 : 1;

        // Determine which variant to use
        var variantId = _editingVariantId;
        if (!variantId && _cacheData && _cacheData.variants && _cacheData.variants.length === 1) {
            variantId = _cacheData.variants[0].id;
        }

        var execBtn = document.getElementById('bd-execute-btn');
        if (execBtn) { execBtn.disabled = true; execBtn.textContent = 'Executing...'; }

        Promise.resolve(_opts.onExecute(variantId, qty, _components))
            .then(function(result) {
                _toast('Breakdown executed');
                _closeModal();
            })
            .catch(function(e) {
                _toast('Execute failed: ' + (e.message || e));
                if (execBtn) { execBtn.disabled = false; execBtn.textContent = 'Execute Breakdown'; }
            });
    }

    // ─── Util ───────────────────────────────────────────────────────

    function _esc(s) {
        if (!s) return '';
        var d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

})();
