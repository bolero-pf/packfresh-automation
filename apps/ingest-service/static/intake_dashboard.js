// ═══════════════════════════════ THEMED PROMPT/CONFIRM SYSTEM ═══════════════════════════════
function themedPrompt({ title, message, inputs = [], confirmText = 'Confirm', confirmClass = 'btn-primary', dangerous = false }) {
    return new Promise((resolve) => {
        const overlay = document.getElementById('themed-prompt-overlay');
        document.getElementById('tp-title').textContent = title;
        document.getElementById('tp-message').textContent = message || '';
        document.getElementById('tp-message').style.display = message ? 'block' : 'none';
        const confirmBtn = document.getElementById('tp-confirm');
        confirmBtn.textContent = confirmText;
        confirmBtn.className = `btn ${dangerous ? 'btn-danger' : confirmClass}`;

        const inputsDiv = document.getElementById('tp-inputs');
        inputsDiv.innerHTML = inputs.map((inp, idx) => {
            if (inp.type === 'select') {
                const opts = (inp.options || []).map(o => {
                    const val = typeof o === 'string' ? o : o.value;
                    const label = typeof o === 'string' ? o : o.label;
                    const sel = val === (inp.default || '') ? 'selected' : '';
                    return `<option value="${val}" ${sel}>${label}</option>`;
                }).join('');
                return `<div class="form-group"><label>${inp.label}</label>
                    <select id="tp-input-${idx}">${opts}</select></div>`;
            } else if (inp.type === 'number') {
                return `<div class="form-group"><label>${inp.label}</label>
                    <input type="number" id="tp-input-${idx}" value="${inp.default || ''}" 
                           min="${inp.min || ''}" max="${inp.max || ''}" step="${inp.step || 'any'}"
                           placeholder="${inp.placeholder || ''}"></div>`;
            } else if (inp.type === 'textarea') {
                return `<div class="form-group"><label>${inp.label}</label>
                    <textarea id="tp-input-${idx}" rows="2" placeholder="${inp.placeholder || ''}">${inp.default || ''}</textarea></div>`;
            } else {
                return `<div class="form-group"><label>${inp.label}</label>
                    <input type="text" id="tp-input-${idx}" value="${inp.default || ''}" 
                           placeholder="${inp.placeholder || ''}"></div>`;
            }
        }).join('');

        overlay.classList.add('active');
        if (inputs.length > 0) {
            setTimeout(() => document.getElementById('tp-input-0')?.focus(), 50);
        }

        function cleanup() {
            overlay.classList.remove('active');
            document.getElementById('tp-cancel').removeEventListener('click', onCancel);
            document.getElementById('tp-confirm').removeEventListener('click', onConfirm);
        }
        function onCancel() { cleanup(); resolve(null); }
        function onConfirm() {
            const values = inputs.map((_, idx) => document.getElementById(`tp-input-${idx}`)?.value ?? '');
            cleanup();
            resolve(inputs.length === 0 ? true : inputs.length === 1 ? values[0] : values);
        }
        document.getElementById('tp-cancel').addEventListener('click', onCancel);
        document.getElementById('tp-confirm').addEventListener('click', onConfirm);
        // Enter key confirms
        inputsDiv.addEventListener('keydown', (e) => { if (e.key === 'Enter') onConfirm(); });
    });
}

function themedConfirm(title, message, { confirmText = 'Confirm', dangerous = false } = {}) {
    return themedPrompt({ title, message, inputs: [], confirmText, dangerous });
}

// ═══════════════════════════════ ESCAPE KEY & CLICK-OUTSIDE ═══════════════════════════════
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        // editCondition mounts its own overlay at z-index:9999 with its own keydown — skip global handler if it's present
        if (document.querySelector('[data-ec-overlay]')) return;
        // Close topmost modal — check in reverse z-order
        const tp = document.getElementById('themed-prompt-overlay');
        if (tp.classList.contains('active')) { tp.classList.remove('active'); return; }
        const relink = document.getElementById('relink-modal');
        if (relink && relink.classList.contains('active')) {
            closeModal('relink-modal');
            if (window._relinkLinkedCount > 0) viewSession(currentSessionId, true);
            window._relinkLinkedCount = 0;
            return;
        }
        const mapping = document.getElementById('mapping-modal');
        if (mapping.classList.contains('active')) { closeModal('mapping-modal'); return; }
        const session = document.getElementById('session-view');
        if (session && session.classList.contains('active')) { hideSessionView(); return; }
    }
});
// Show/hide the full-page session view. Replaces the legacy modal pattern —
// when a session is open, the tab nav and tab-content blocks above are
// hidden via body.session-active and the view fills the container.
function showSessionView() {
    const view = document.getElementById('session-view');
    if (!view) return;
    view.classList.add('active');
    document.body.classList.add('session-active');
    window.scrollTo(0, 0);
}
function hideSessionView() {
    const view = document.getElementById('session-view');
    if (!view) return;
    view.classList.remove('active');
    document.body.classList.remove('session-active');
    currentSessionId = null;
    // Refresh the visible session list so any in-place changes show up.
    try {
        if (typeof loadFilteredActive === 'function') loadFilteredActive();
    } catch(e) {}
}
function closeSessionView() { hideSessionView(); }
// Click outside modal to close — track mousedown origin so text-select drags don't dismiss
{
    let _mousedownTarget = null;
    document.addEventListener('mousedown', (e) => { _mousedownTarget = e.target; });
    document.querySelectorAll('.modal-overlay').forEach(overlay => {
        if (overlay.id === 'themed-prompt-overlay') return; // only via Cancel/Confirm/Escape
        overlay.addEventListener('mouseup', (e) => {
            // Only dismiss if BOTH mousedown and mouseup landed on the bare overlay (not a drag)
            if (e.target === overlay && _mousedownTarget === overlay) {
                overlay.classList.remove('active');
            }
        });
    });
}

// ═══════════════════════════════ STATE ═══════════════════════════════
let currentSessionId = null;
let currentMapItemId = null;
let currentMapProductType = 'sealed';
let rawSessionId = null;
let intakeSessionId = null;
let intakeType = 'sealed'; // 'sealed' or 'card'

// ═══════════════════════════════ TABS ═══════════════════════════════
function switchTab(name) {
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.getElementById(name).classList.add('active');
    document.querySelectorAll('.tab').forEach(t => {
        t.classList.toggle('active', t.getAttribute('data-tab') === name);
    });
    window.location.hash = name;
    if (name === 'active-sessions') loadFilteredActive();
    if (name === 'completed') loadFilteredCompleted();
    if (name === 'cancelled') loadFilteredCancelled();
    if (name === 'store') loadCacheStatus();
}
// Restore tab from hash on load
(function() {
    const hash = window.location.hash.replace('#', '');
    const validTabs = ['new-intake','active-sessions','completed','cancelled'];
    if (hash && validTabs.includes(hash)) { switchTab(hash); }
    else { loadSessions('in_progress,offered,accepted,partially_ingested', 'active-sessions-list'); }
})();

// ═══════════════════════════════ MODAL HELPERS ═══════════════════════════════
function openModal(id) { document.getElementById(id).classList.add('active'); }
function closeModal(id) { document.getElementById(id).classList.remove('active'); }

// ═══════════════════════════════ SUB-TAB SWITCHING ═══════════════════════════════
function switchSubTab(btn, subtabId) {
    // Deactivate all sibling tabs
    btn.parentElement.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    // Hide all subtab content in same parent card
    const card = btn.closest('.card');
    card.querySelectorAll('.subtab-content').forEach(c => c.style.display = 'none');
    document.getElementById('subtab-' + subtabId).style.display = '';
}

// ═══════════════════════════════ CSV UPLOAD ═══════════════════════════════
document.getElementById('csv-upload-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const resultDiv = document.getElementById('csv-upload-result');
    resultDiv.innerHTML = '<div class="card"><div class="loading"><span class="spinner"></span> Processing CSV...</div></div>';

    const form = new FormData();
    form.append('file', document.getElementById('csv-file').files[0]);
    form.append('customer_name', document.getElementById('csv-customer').value);
    form.append('cash_percentage', document.getElementById('csv-cash-pct').value);
    form.append('credit_percentage', document.getElementById('csv-credit-pct').value);
    form.append('is_distribution', document.getElementById('csv-distribution').checked ? '1' : '0');
    if (_csvImportOverride && _csvImportOverride.token) form.append('override_token', _csvImportOverride.token);

    try {
        let r = await fetch('/api/intake/upload-collectr', { method: 'POST', body: form });
        let d = await r.json();
        if (r.status === 409 && d.existing_session_id) {
            if (!confirm(`This file was already imported (Session: ${d.existing_session_id.slice(0,8)}...). Re-import anyway?`)) {
                resultDiv.innerHTML = '';
                return;
            }
            form.append('force', '1');
            r = await fetch('/api/intake/upload-collectr', { method: 'POST', body: form });
            d = await r.json();
        }
        if (!r.ok) {
            resultDiv.innerHTML = `<div class="alert alert-error">${d.error}</div>`;
            return;
        }

        resultDiv.innerHTML = `
            <div class="alert alert-success">Uploaded! Session ${d.session_id.slice(0,8)}...</div>
            <div class="stats">
                <div class="stat"><div class="stat-label">Items</div><div class="stat-value">${d.item_count}</div></div>
                <div class="stat"><div class="stat-label">Market Value</div><div class="stat-value">$${d.total_market_value.toFixed(2)}</div></div>
                <div class="stat"><div class="stat-label">Your Offer</div><div class="stat-value">$${d.total_offer.toFixed(2)}</div></div>
                <div class="stat"><div class="stat-label">Auto-Mapped</div><div class="stat-value">${d.auto_mapped_count}/${d.item_count}</div></div>
            </div>
            ${d.unmapped_count > 0 ? `<div class="alert alert-warning">${d.unmapped_count} items need TCGPlayer ID linking</div>` : '<div class="alert alert-success">All items auto-mapped from cache!</div>'}
            <button class="btn btn-primary" onclick="viewSession('${d.session_id}')">View &amp; Map Items</button>
        `;
        document.getElementById('csv-upload-form').reset();
    } catch(err) {
        resultDiv.innerHTML = `<div class="alert alert-error">Upload failed: ${err.message}</div>`;
    }
});

// ═══════════════════════════════ HTML PASTE UPLOAD ═══════════════════════════════
document.getElementById('html-paste-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const resultDiv = document.getElementById('csv-upload-result');
    const htmlContent = document.getElementById('html-paste-input').value.trim();
    if (!htmlContent) {
        resultDiv.innerHTML = '<div class="alert alert-error">Please paste Collectr HTML first.</div>';
        return;
    }
    resultDiv.innerHTML = '<div class="card"><div class="loading"><span class="spinner"></span> Parsing HTML...</div></div>';

    try {
        const payload = {
            html: htmlContent,
            customer_name: document.getElementById('html-customer').value,
            cash_percentage: document.getElementById('html-cash-pct').value,
            credit_percentage: document.getElementById('html-credit-pct').value,
            is_distribution: document.getElementById('html-distribution').checked,
        };
        if (_htmlImportOverride && _htmlImportOverride.token) payload.override_token = _htmlImportOverride.token;
        let r = await fetch('/api/intake/upload-collectr-html', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
        });
        let d = await r.json();
        if (r.status === 409 && d.existing_session_id) {
            if (!confirm(`This content was already imported (Session: ${d.existing_session_id.slice(0,8)}...). Re-import anyway?`)) {
                resultDiv.innerHTML = '';
                return;
            }
            payload.force = true;
            r = await fetch('/api/intake/upload-collectr-html', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            d = await r.json();
        }
        if (!r.ok) {
            resultDiv.innerHTML = `<div class="alert alert-error">${d.error}</div>`;
            return;
        }

        resultDiv.innerHTML = `
            <div class="alert alert-success">Imported from HTML! Session ${d.session_id.slice(0,8)}...</div>
            <div class="stats">
                <div class="stat"><div class="stat-label">Items</div><div class="stat-value">${d.item_count}</div></div>
                <div class="stat"><div class="stat-label">Market Value</div><div class="stat-value">$${d.total_market_value.toFixed(2)}</div></div>
                <div class="stat"><div class="stat-label">Your Offer</div><div class="stat-value">$${d.total_offer.toFixed(2)}</div></div>
                <div class="stat"><div class="stat-label">Auto-Mapped</div><div class="stat-value">${d.auto_mapped_count}/${d.item_count}</div></div>
            </div>
            ${d.parse_errors.length ? `<div class="alert alert-warning">Parse warnings: ${d.parse_errors.join('; ')}</div>` : ''}
            ${d.unmapped_count > 0 ? `<div class="alert alert-warning">${d.unmapped_count} items need TCGPlayer ID linking</div>` : '<div class="alert alert-success">All items auto-mapped from cache!</div>'}
            <button class="btn btn-primary" onclick="viewSession('${d.session_id}')">View &amp; Map Items</button>
        `;
        document.getElementById('html-paste-form').reset();
    } catch(err) {
        resultDiv.innerHTML = `<div class="alert alert-error">Parse failed: ${err.message}</div>`;
    }
});

// ═══════════════════════════════ GENERIC CSV UPLOAD ═══════════════════════════════
let _genericCsvFile = null;
let _genericCsvMapping = {};

document.getElementById('generic-csv-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const previewDiv = document.getElementById('generic-csv-preview');
    const resultDiv = document.getElementById('generic-csv-result');
    resultDiv.innerHTML = '';

    const fileInput = document.getElementById('generic-csv-file');
    if (!fileInput.files.length) { previewDiv.innerHTML = '<div class="alert alert-error">Select a file.</div>'; return; }

    _genericCsvFile = fileInput.files[0];
    previewDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Detecting columns...</div>';

    try {
        const form = new FormData();
        form.append('file', _genericCsvFile);
        const r = await fetch('/api/intake/preview-csv', { method: 'POST', body: form });
        const d = await r.json();
        if (!r.ok) { previewDiv.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }

        _genericCsvMapping = d.mapping || {};
        const headers = d.headers || [];
        const required = ['name', 'quantity', 'price'];
        const optional = ['set_name', 'card_number', 'rarity', 'condition', 'tcgplayer_id', 'product_type', 'photo_url'];
        const allFields = [...required, ...optional];
        const fieldLabels = {
            name: 'Product Name *', quantity: 'Quantity *', price: 'Market Price *',
            set_name: 'Set Name', card_number: 'Card Number', rarity: 'Rarity',
            condition: 'Condition', tcgplayer_id: 'TCGPlayer ID', product_type: 'Type',
            photo_url: 'Photo URL (extracts real TCG ID)',
        };

        let html = '<div class="card" style="padding:16px;">';
        html += '<h3 style="margin-bottom:12px;">Column Mapping</h3>';
        html += '<p style="color:var(--text-dim); font-size:0.85rem; margin-bottom:12px;">We auto-detected these mappings. Adjust if needed. Fields marked * are required.</p>';
        html += '<div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(220px, 1fr)); gap:8px; margin-bottom:16px;">';

        for (const field of allFields) {
            const current = _genericCsvMapping[field] || '';
            const isRequired = required.includes(field);
            html += `<div class="form-group" style="margin-bottom:0;">
                <label style="font-size:0.8rem;${isRequired ? 'color:var(--accent);font-weight:700;' : ''}">${fieldLabels[field]}</label>
                <select id="gcm-${field}" style="font-size:0.85rem; padding:4px 8px;" onchange="_genericCsvMapping['${field}']=this.value||undefined; if(!this.value) delete _genericCsvMapping['${field}'];">
                    <option value="">— skip —</option>
                    ${headers.map(h => `<option value="${h}" ${h === current ? 'selected' : ''}>${h}</option>`).join('')}
                </select>
            </div>`;
        }
        html += '</div>';

        // Preview table
        if (d.preview_rows && d.preview_rows.length) {
            html += '<h4 style="margin-bottom:8px;">Preview (first 5 rows):</h4>';
            html += '<div style="overflow-x:auto;"><table style="font-size:0.8rem;"><thead><tr>';
            for (const h of headers) { html += `<th style="padding:4px 8px;">${h}</th>`; }
            html += '</tr></thead><tbody>';
            for (const row of d.preview_rows) {
                html += '<tr>';
                for (const h of headers) { html += `<td style="padding:4px 8px;">${row[h] || ''}</td>`; }
                html += '</tr>';
            }
            html += '</tbody></table></div>';
        }

        html += '<button class="btn btn-success" style="margin-top:12px;" onclick="submitGenericCsv()">Import with This Mapping</button>';
        html += '</div>';
        previewDiv.innerHTML = html;

    } catch(err) {
        previewDiv.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
});

async function submitGenericCsv() {
    const resultDiv = document.getElementById('generic-csv-result');
    const previewDiv = document.getElementById('generic-csv-preview');
    resultDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Importing...</div>';

    try {
        const form = new FormData();
        form.append('file', _genericCsvFile);
        form.append('customer_name', document.getElementById('generic-customer').value);
        form.append('cash_percentage', document.getElementById('generic-cash-pct').value);
        form.append('credit_percentage', document.getElementById('generic-credit-pct').value);
        form.append('is_distribution', document.getElementById('generic-distribution').checked ? '1' : '0');
        form.append('column_mapping', JSON.stringify(_genericCsvMapping));
        if (_genericImportOverride && _genericImportOverride.token) form.append('override_token', _genericImportOverride.token);

        let r = await fetch('/api/intake/upload-generic-csv', { method: 'POST', body: form });
        let d = await r.json();
        if (r.status === 409 && d.existing_session_id) {
            if (!confirm(`This file was already imported (Session: ${d.existing_session_id.slice(0,8)}...). Re-import anyway?`)) {
                resultDiv.innerHTML = '';
                return;
            }
            form.append('force', '1');
            r = await fetch('/api/intake/upload-generic-csv', { method: 'POST', body: form });
            d = await r.json();
        }
        if (!r.ok) {
            resultDiv.innerHTML = `<div class="alert alert-error">${d.error}${d.details ? '<br><small>' + d.details.join('; ') + '</small>' : ''}</div>`;
            return;
        }

        previewDiv.innerHTML = '';
        resultDiv.innerHTML = `
            <div class="alert alert-success">CSV imported! Session ${d.session_id.slice(0,8)}...</div>
            <div class="stats">
                <div class="stat"><div class="stat-label">Items</div><div class="stat-value">${d.item_count}</div></div>
                <div class="stat"><div class="stat-label">Market Value</div><div class="stat-value">$${d.total_market_value.toFixed(2)}</div></div>
                <div class="stat"><div class="stat-label">Your Offer</div><div class="stat-value">$${d.total_offer.toFixed(2)}</div></div>
                <div class="stat"><div class="stat-label">Auto-Mapped</div><div class="stat-value">${d.auto_mapped_count}/${d.item_count}</div></div>
            </div>
            <p style="color:var(--text-dim); font-size:0.85rem;">Columns used: ${Object.entries(d.column_mapping || {}).map(([k,v]) => k + '→' + v).join(', ')}</p>
            ${d.parse_errors && d.parse_errors.length ? `<div class="alert alert-warning">Warnings: ${d.parse_errors.join('; ')}</div>` : ''}
            ${d.unmapped_count > 0 ? `<div class="alert alert-warning">${d.unmapped_count} items need TCGPlayer ID linking</div>` : '<div class="alert alert-success">All items auto-mapped!</div>'}
            <button class="btn btn-primary" onclick="viewSession('${d.session_id}')">View &amp; Map Items</button>
        `;
        document.getElementById('generic-csv-form').reset();
    } catch(err) {
        resultDiv.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

// ═══════════════════════════════ UNIFIED INTAKE (MANUAL ENTRY) ═══════════════════════════════

// ═══════════════════════════════ ROLE / OVERRIDE STATE ═══════════════════════════════
// Per-session: when a manager types their PIN to authorize an
// out-of-policy percentage, we stash the resulting JWT here. New
// session → new override (the spec is explicit about no rolling
// unlock per browser).
let _intakeSetupOverride = null;        // override token + approver for the New Intake form
let _sessionOverrides = {};             // sessionId → { token, approver }
const ASSOCIATE_DEFAULTS = { cash: 65, credit: 75 };

function _pfRole() {
    // window._pfUser is server-rendered into the admin bar's <script> tag
    // by shared/auth.py inject_admin_bar — pf_auth is HttpOnly so JS can't
    // decode the JWT directly. Default to 'associate' only if the bar
    // didn't render (no g.user → unauthenticated, which shouldn't happen
    // since blanket auth runs first).
    try {
        if (window._pfUser && window._pfUser.role) {
            return String(window._pfUser.role).toLowerCase();
        }
    } catch (e) {}
    return 'associate';
}

function _outsideAssociate(cashPct, creditPct) {
    return Number(cashPct) !== ASSOCIATE_DEFAULTS.cash
        || Number(creditPct) !== ASSOCIATE_DEFAULTS.credit;
}

// Promise<{token, approver}> — null if cancelled, throws on PIN failure.
async function _promptManagerPin(reason) {
    return new Promise((resolve) => {
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay active';
        overlay.style.cssText = 'z-index:1300; align-items:center;';
        overlay.innerHTML = `
            <div class="prompt-modal" role="dialog" aria-modal="true" aria-label="Manager Override">
                <h3>🔒 Manager Override</h3>
                <p>${reason || 'A manager or owner must approve this offer percentage.'}</p>
                <div class="form-group">
                    <label for="pf-pin-input">PIN</label>
                    <input id="pf-pin-input" type="password" inputmode="numeric"
                           pattern="[0-9]*" maxlength="8" autocomplete="off"
                           style="font-size:1.4rem; letter-spacing:0.4em; text-align:center;">
                </div>
                <div class="prompt-buttons">
                    <button id="pf-pin-cancel" class="btn btn-secondary">Cancel</button>
                    <button id="pf-pin-submit" class="btn btn-primary">Approve</button>
                </div>
            </div>`;
        document.body.appendChild(overlay);
        const input = overlay.querySelector('#pf-pin-input');
        setTimeout(() => input.focus(), 50);

        function cleanup() {
            overlay.remove();
            document.removeEventListener('keydown', onKey);
        }
        function onKey(e) {
            if (e.key === 'Escape') { cleanup(); resolve(null); }
            if (e.key === 'Enter') { e.preventDefault(); submit(); }
        }
        document.addEventListener('keydown', onKey);
        overlay.querySelector('#pf-pin-cancel').addEventListener('click', () => { cleanup(); resolve(null); });

        async function submit() {
            const pin = (input.value || '').trim();
            if (pin.length < 4) {
                input.focus(); input.select();
                if (window.pfSound) try { pfSound.error(); } catch(_) {}
                return;
            }
            const submitBtn = overlay.querySelector('#pf-pin-submit');
            submitBtn.disabled = true; submitBtn.textContent = 'Verifying…';
            try {
                const r = await fetch('https://admin.pack-fresh.com/api/verify-pin', {
                    method: 'POST',
                    headers: {'Content-Type':'application/json'},
                    credentials: 'include',
                    body: JSON.stringify({ pin: pin, action: 'offer_percentage' }),
                });
                const d = await r.json().catch(() => ({}));
                if (!r.ok || !d.ok) {
                    cleanup();
                    if (window.pfBlock) {
                        pfBlock('Override Denied', 'Invalid PIN', { error: true, sound: 'error' });
                    } else {
                        alert('Invalid PIN');
                    }
                    resolve(null);
                    return;
                }
                cleanup();
                if (window.pfSound) try { pfSound.success(); } catch(_) {}
                resolve({ token: d.override_token, approver: d.manager });
            } catch (e) {
                cleanup();
                if (window.pfBlock) pfBlock('Override Denied', e.message || 'Network error', { error: true, sound: 'error' });
                else alert(e.message || 'Network error');
                resolve(null);
            }
        }
        overlay.querySelector('#pf-pin-submit').addEventListener('click', submit);
    });
}

// For a given role + percentages, decide whether we need an override
// token before submitting. Returns 'manager' (associate needs a manager
// PIN) or 'ok' (everyone else, or associate inside defaults).
//
// Per the role-friction policy: managers and owners ("admin" users)
// have ZERO friction — no caps, no PIN prompts. Only associates get
// gated, and only when their values diverge from the canonical defaults.
function _overrideNeeded(role, cashPct, creditPct) {
    role = (role || 'associate').toLowerCase();
    if (role !== 'associate') return 'ok';
    if (_outsideAssociate(cashPct, creditPct)) return 'manager';
    return 'ok';
}

function _renderOverrideBar(barId, override, role) {
    const bar = document.getElementById(barId);
    if (!bar) return;
    if (override && override.approver) {
        const ap = override.approver;
        bar.style.display = '';
        bar.innerHTML = `<span style="color:var(--green); font-weight:600;">✓ Override:</span> ${ap.name || 'Manager'} (${ap.role || 'manager'})`;
    } else if (role === 'associate') {
        bar.style.display = '';
        bar.innerHTML = `<span style="color:var(--text-dim);">🔒 Locked at defaults — manager override required to change.</span>`;
    } else {
        bar.style.display = 'none';
    }
}

// Per-form override state for the four entry surfaces. New Intake
// uses _intakeSetupOverride (declared earlier with a different scope);
// the three import forms get their own slots.
let _csvImportOverride = null;
let _genericImportOverride = null;
let _htmlImportOverride = null;

// Apply role-based read-only state + manager-override button to one
// entry form. Generic so the same code locks New Intake and the three
// import forms (Collectr CSV / Generic CSV / HTML Paste) — earlier I
// only locked the Manual Entry inputs and the import forms were left
// editable, which Sean (rightly) flagged as an unprotected bypass.
function _applyFormRoleLock(cfg) {
    const role = _pfRole();
    const cash = document.getElementById(cfg.cashId);
    const credit = document.getElementById(cfg.creditId);
    if (!cash || !credit) return;
    const ov = cfg.getOverride();
    // Remove any prior override button before re-rendering
    const existing = document.getElementById(cfg.btnId);
    if (existing) existing.remove();

    if (role === 'associate' && !(ov && ov.token)) {
        cash.value = ASSOCIATE_DEFAULTS.cash;
        credit.value = ASSOCIATE_DEFAULTS.credit;
        cash.readOnly = true; credit.readOnly = true;
        cash.style.opacity = credit.style.opacity = '0.6';
        const btn = document.createElement('button');
        btn.id = cfg.btnId;
        btn.type = 'button';  // critical for non-form-element-wrapped buttons; without this they submit forms
        btn.className = 'btn btn-sm btn-secondary';
        btn.style.cssText = 'margin-left:8px; align-self:flex-end;';
        btn.textContent = '🔒 Manager Override';
        btn.onclick = async (e) => {
            e.preventDefault();
            const newOv = await _promptManagerPin('A manager must approve a custom offer percentage.');
            if (!newOv) return;
            cfg.setOverride(newOv);
            cash.readOnly = false; credit.readOnly = false;
            cash.style.opacity = credit.style.opacity = '1';
            btn.remove();
            _renderOverrideBar(cfg.barId, newOv, role);
        };
        // Place the button next to the credit input so it's visually
        // associated with the percentage controls.
        credit.parentElement.appendChild(btn);
    } else {
        cash.readOnly = false; credit.readOnly = false;
        cash.style.opacity = credit.style.opacity = '1';
    }
    _renderOverrideBar(cfg.barId, ov, role);
}

const _ENTRY_FORM_LOCKS = [
    { cashId: 'intake-cash-pct',  creditId: 'intake-credit-pct',  btnId: 'intake-pct-override-btn',  barId: 'intake-pct-override-bar',
      getOverride: () => _intakeSetupOverride, setOverride: (ov) => { _intakeSetupOverride = ov; } },
    { cashId: 'csv-cash-pct',     creditId: 'csv-credit-pct',     btnId: 'csv-pct-override-btn',     barId: 'csv-pct-override-bar',
      getOverride: () => _csvImportOverride,    setOverride: (ov) => { _csvImportOverride = ov; } },
    { cashId: 'generic-cash-pct', creditId: 'generic-credit-pct', btnId: 'generic-pct-override-btn', barId: 'generic-pct-override-bar',
      getOverride: () => _genericImportOverride, setOverride: (ov) => { _genericImportOverride = ov; } },
    { cashId: 'html-cash-pct',    creditId: 'html-credit-pct',    btnId: 'html-pct-override-btn',    barId: 'html-pct-override-bar',
      getOverride: () => _htmlImportOverride,    setOverride: (ov) => { _htmlImportOverride = ov; } },
];

function _applyAllEntryRoleLocks() {
    _ENTRY_FORM_LOCKS.forEach(cfg => { try { _applyFormRoleLock(cfg); } catch(e) {} });
}

// Back-compat alias — older call sites still reference _applyIntakeRoleLock.
function _applyIntakeRoleLock() { _applyAllEntryRoleLocks(); }

(function _initEntryRoleLocks() {
    function _go() { _applyAllEntryRoleLocks(); }
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _go);
    } else { _go(); }
    // _pfUser is server-rendered now (shared/auth.py admin bar) so it's
    // present on the first synchronous run; the retries are kept as
    // belt-and-suspenders against re-renders or late style-loads.
    setTimeout(_go, 250);
    setTimeout(_go, 750);
})();

async function createIntakeSession() {
    const customer = document.getElementById('intake-customer').value || 'Walk-in';
    const cashPct = parseFloat(document.getElementById('intake-cash-pct').value) || 65;
    const creditPct = parseFloat(document.getElementById('intake-credit-pct').value) || 75;
    const isDist = document.getElementById('intake-distribution').checked;
    const isWalkIn = document.getElementById('intake-walkin').checked;

    // Frontend cap check — server is the authority but we want fast UX
    // feedback before kicking off the request.
    const role = _pfRole();
    const need = _overrideNeeded(role, cashPct, creditPct);
    if (need !== 'ok' && !(_intakeSetupOverride && _intakeSetupOverride.token)) {
        const ov = await _promptManagerPin(`A ${need} must approve this offer percentage.`);
        if (!ov) return;
        _intakeSetupOverride = ov;
    }

    try {
        const body = {
            customer_name: customer,
            session_type: 'mixed',
            cash_percentage: cashPct,
            credit_percentage: creditPct,
            is_walk_in: isWalkIn,
            is_distribution: isDist,
        };
        if (_intakeSetupOverride && _intakeSetupOverride.token) {
            body.override_token = _intakeSetupOverride.token;
        }
        const r = await fetch('/api/intake/create-session', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) {
            if (window.pfBlock) pfBlock('Cannot Start Session', d.error || 'Unknown error', { error: true, sound: 'error' });
            else alert(d.error);
            return;
        }
        intakeSessionId = d.session.id;
        document.getElementById('intake-setup').style.display = 'none';
        document.getElementById('intake-entry').style.display = 'block';
        const walkBadge = isWalkIn ? ' \u2022 <span style="background:#7c3aed;color:#fff;padding:1px 6px;border-radius:4px;font-size:0.75rem;">WALK-IN</span>' : '';
        document.getElementById('intake-session-info').innerHTML =
            'Session started for <strong>' + customer + '</strong> \u2022 Credit ' + creditPct + '% / Cash ' + cashPct + '%' + walkBadge + ' \u2022 ID: ' + intakeSessionId.slice(0,8) + '...';
        // Carry the override token into the session so subsequent edits
        // in the same session don't re-prompt unless the user changes
        // values again outside policy.
        if (_intakeSetupOverride && _intakeSetupOverride.token) {
            _sessionOverrides[intakeSessionId] = _intakeSetupOverride;
        }
        _intakeSetupOverride = null;
        _applyIntakeRoleLock();
        document.getElementById('intake-sealed-search').focus();
    } catch(err) { alert(err.message); }
}

function switchIntakeType(type) {
    intakeType = type;
    document.getElementById('intake-type-btn-sealed').className = 'btn btn-sm ' + (type === 'sealed' ? 'btn-primary' : 'btn-secondary');
    document.getElementById('intake-type-btn-card').className = 'btn btn-sm ' + (type === 'card' ? 'btn-primary' : 'btn-secondary');
    document.getElementById('intake-sealed-search-area').style.display = type === 'sealed' ? '' : 'none';
    document.getElementById('intake-card-search-area').style.display = type === 'card' ? '' : 'none';
    document.getElementById('intake-card-fields').style.display = type === 'card' ? '' : 'none';
    document.getElementById('intake-search-results').innerHTML = '';
}

function toggleIntakeGraded() {
    const isGraded = document.getElementById('intake-is-graded').checked;
    document.getElementById('intake-graded-fields').style.display = isGraded ? 'flex' : 'none';
    const condGroup = document.getElementById('intake-cond-group');
    condGroup.style.opacity = isGraded ? '0.4' : '1';
    condGroup.title = isGraded ? 'Condition not used for graded cards' : '';
    document.getElementById('intake-graded-hint').style.display = isGraded ? 'none' : '';
    const badge = document.getElementById('intake-graded-badge');
    if (badge) { badge.style.display = 'none'; badge.textContent = ''; }
}

function onIntakeGradeChange() {
    const badge = document.getElementById('intake-graded-badge');
    if (badge) { badge.style.display = 'none'; badge.textContent = ''; }
    document.getElementById('intake-search-results').innerHTML = '';
}

function toggleIntakeManualDirect() {
    const el = document.getElementById('intake-manual-direct');
    el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

async function intakeSealedSearch(live) {
    const searchTerm = document.getElementById('intake-sealed-search').value.trim();
    const resultsDiv = document.getElementById('intake-search-results');
    if (!searchTerm) { resultsDiv.innerHTML = '<div class="alert alert-warning">Enter a search term</div>'; return; }

    resultsDiv.innerHTML = `<div class="loading"><span class="spinner"></span> Searching${live ? ' PPT live' : ''}...</div>`;
    try {
        const body = { query: searchTerm };
        if (live) body.live = true;
        const r = await fetch('/api/search/sealed', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const contentType = r.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            const text = await r.text();
            resultsDiv.innerHTML = '<div class="alert alert-error">Server error (' + r.status + '): ' + text.substring(0, 100) + '</div>';
            return;
        }
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = '<div class="alert alert-error">' + d.error + '</div>'; return; }

        const products = d.results || [];
        if (!products.length) { resultsDiv.innerHTML = '<div class="alert alert-warning">No results found</div>'; return; }

        const qty = parseInt(document.getElementById('intake-sealed-qty').value) || 1;
        const offerPct = parseFloat(document.getElementById('intake-cash-pct').value) || 65;
        resultsDiv.innerHTML = products.map(p => {
            const price = p.unopenedPrice || (p.prices ? p.prices.market : null) || 0;
            const offer = (price * qty * offerPct / 100).toFixed(2);
            const tcgId = p.tcgPlayerId || p.tcgplayer_id || p.id || '';
            const setName = (p.setName || p.set_name || '').replace(/'/g, "\\'");
            const safeName = (p.name || '').replace(/'/g, "\\'");
            const img = p.imageCdnUrl400 || p.imageCdnUrl || p.imageCdnUrl800 || '';
            const src = p._price_source ? ({cache:'cache',ppt:'PPT',scrydex:'SDX'}[p._price_source]||p._price_source) : '';
            const expCode = (p.expansionId || p.expansion_id || '').toUpperCase();
            const game = (p.game || '').toLowerCase();
            const setCodeBadge = expCode ? `<span style="background:var(--surface); border:1px solid var(--border); color:var(--accent); padding:1px 6px; border-radius:4px; font-size:0.7rem; font-weight:700; font-family:ui-monospace,monospace; margin-right:4px;">${expCode}</span>` : '';
            const gameBadge = (game && game !== 'pokemon') ? `<span style="background:var(--accent-alt,#7c3aed); color:#fff; padding:1px 6px; border-radius:4px; font-size:0.65rem; font-weight:600; margin-right:4px;">${game.toUpperCase()}</span>` : '';
            const nameLower = (p.name||'').toLowerCase();
            const isBundle = ['art bundle','set of','bundle (','pack of','case','display'].some(kw => nameLower.includes(kw));
            return `<div style="display:flex; gap:10px; align-items:center; padding:8px 12px; border:1px solid var(--border); border-radius:6px; margin-bottom:4px; cursor:pointer;${isBundle ? ' opacity:0.6; border-left:3px solid var(--amber);' : ''}" onclick="addIntakeSealed(${tcgId || 'null'}, '${safeName}', '${setName}', ${price}, ${qty})" onmouseover="this.style.background='var(--surface-2)'" onmouseout="this.style.background='transparent'">
                ${img ? `<img src="${img}" loading="lazy" style="width:48px; height:48px; object-fit:contain; border-radius:4px; flex-shrink:0;">` : '<div style="width:48px;height:48px;background:var(--surface-2);border-radius:4px;flex-shrink:0;"></div>'}
                <div style="flex:1; min-width:0;">
                    <div style="font-weight:600; font-size:0.9rem;">${gameBadge}${p.name}${isBundle ? ' <span style="color:var(--amber);font-size:0.7rem;font-weight:700;">⚠ BUNDLE</span>' : ''}</div>
                    <div style="font-size:0.8rem; color:var(--text-dim);">${setCodeBadge}${p.setName || p.set_name || ''}${tcgId ? ' · TCG#' + tcgId : ''}</div>
                </div>
                <div style="text-align:right; flex-shrink:0;">
                    <div style="font-weight:700;">$${Number(price).toFixed(2)}</div>
                    <div style="font-size:0.7rem; color:var(--text-dim);">${src} · Offer: $${offer}</div>
                </div>
            </div>`;
        }).join('');

        // If results came from cache, offer PPT live fallback
        const anyCache = products.some(p => p._price_source === 'cache');
        if (anyCache) {
            resultsDiv.innerHTML += `<div style="margin-top:8px; text-align:center;">
                <button class="btn btn-secondary btn-sm" onclick="intakeSealedSearch(true)" style="font-size:0.78rem; opacity:0.8;">
                    Don't see it? Search live →
                </button>
            </div>`;
        }
    } catch(err) {
        resultsDiv.innerHTML = '<div class="alert alert-error">' + err.message + '</div>';
    }
}

async function addIntakeSealed(tcgplayerId, name, setName, price, qty) {
    const resultsDiv = document.getElementById('intake-search-results');
    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Adding...</div>';
    try {
        const r = await fetch('/api/intake/add-sealed-item', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: intakeSessionId,
                product_name: name,
                set_name: setName,
                tcgplayer_id: tcgplayerId,
                market_price: price,
                quantity: qty,
            }),
        });
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = '<div class="alert alert-error">' + d.error + '</div>'; return; }

        document.getElementById('intake-sealed-search').value = '';
        document.getElementById('intake-sealed-qty').value = '1';
        resultsDiv.innerHTML = '<div class="alert alert-success">Added! \u2713</div>';
        setTimeout(() => { resultsDiv.innerHTML = ''; }, 1500);
        loadIntakeItems();
        document.getElementById('intake-sealed-search').focus();
    } catch(err) {
        resultsDiv.innerHTML = '<div class="alert alert-error">' + err.message + '</div>';
    }
}

async function submitIntakeManualDirect() {
    const name = document.getElementById('intake-direct-name').value.trim();
    const setName = document.getElementById('intake-direct-set').value.trim();
    const tcgId = document.getElementById('intake-direct-tcgid').value.trim();
    const price = parseFloat(document.getElementById('intake-direct-price').value) || 0;
    const qty = parseInt(document.getElementById('intake-direct-qty').value) || 1;

    if (!name) { alert('Product name is required'); return; }
    if (price <= 0) { alert('Price is required'); return; }
    if (!intakeSessionId) { alert('Create a session first'); return; }

    try {
        const r = await fetch('/api/intake/add-sealed-item', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: intakeSessionId,
                product_name: name,
                set_name: setName,
                tcgplayer_id: tcgId ? parseInt(tcgId) : null,
                market_price: price,
                quantity: qty,
            }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed to add'); return; }

        document.getElementById('intake-direct-name').value = '';
        document.getElementById('intake-direct-set').value = '';
        document.getElementById('intake-direct-tcgid').value = '';
        document.getElementById('intake-direct-price').value = '';
        document.getElementById('intake-direct-qty').value = '1';
        document.getElementById('intake-manual-direct').style.display = 'none';
        loadIntakeItems();
    } catch(err) { alert(err.message); }
}

function intakeCardSearch() {
    const offerPct = parseFloat(document.getElementById('intake-cash-pct').value) || 65;
    smartCardSearch(intakeSessionId, offerPct, 'intake');
}

function toggleIntakeCardManual() {
    const el = document.getElementById('intake-card-manual');
    el.style.display = el.style.display === 'none' ? 'block' : 'none';
    if (el.style.display === 'block') {
        // Pre-fill from search inputs so staff don't retype the name
        const nm = document.getElementById('intake-card-manual-name');
        if (!nm.value) nm.value = (document.getElementById('intake-card-search')?.value || '').trim();
        const st = document.getElementById('intake-card-manual-set');
        if (!st.value) st.value = (document.getElementById('intake-card-set')?.value || '').trim();
        const tcg = document.getElementById('intake-card-manual-tcgid');
        if (!tcg.value) tcg.value = (document.getElementById('intake-card-tcgid')?.value || '').trim();
        document.getElementById('intake-card-manual-cond').value =
            document.getElementById('intake-card-condition')?.value || 'NM';
        document.getElementById('intake-card-manual-qty').value =
            document.getElementById('intake-card-qty')?.value || '1';
        setTimeout(() => nm.focus(), 50);
    }
}

async function submitIntakeCardManual() {
    if (!intakeSessionId) { alert('Start an intake session first'); return; }
    const name = document.getElementById('intake-card-manual-name').value.trim();
    const price = parseFloat(document.getElementById('intake-card-manual-price').value);
    if (!name) { alert('Name is required'); return; }
    if (isNaN(price) || price < 0) { alert('Enter a valid price'); return; }

    const tcgRaw = document.getElementById('intake-card-manual-tcgid').value.trim();
    const body = {
        session_id: intakeSessionId,
        card_name: name,
        tcgplayer_id: tcgRaw ? parseInt(tcgRaw) : null,
        market_price: price,
        set_name: document.getElementById('intake-card-manual-set').value.trim() || null,
        card_number: document.getElementById('intake-card-manual-cardnum').value.trim() || null,
        variance: document.getElementById('intake-card-manual-variant').value.trim() || null,
        quantity: parseInt(document.getElementById('intake-card-manual-qty').value) || 1,
        condition: document.getElementById('intake-card-manual-cond').value,
    };
    try {
        const r = await fetch('/api/intake/add-raw-card', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Add failed'); return; }
        // Reset form, keep drawer open for next entry
        ['intake-card-manual-name','intake-card-manual-set','intake-card-manual-cardnum',
         'intake-card-manual-tcgid','intake-card-manual-variant','intake-card-manual-price']
            .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
        document.getElementById('intake-card-manual-qty').value = '1';
        document.getElementById('intake-card-manual-name').focus();
        loadIntakeItems();
    } catch(err) { alert(err.message); }
}

async function loadIntakeItems() {
    if (!intakeSessionId) return;
    try {
        const r = await fetch('/api/intake/session/' + intakeSessionId);
        const d = await r.json();
        const items = d.items;
        const s = d.session;
        const tableDiv = document.getElementById('intake-items-table');
        document.getElementById('intake-finalize-btn').style.display = items.length ? '' : 'none';
        document.getElementById('intake-view-btn').style.display = items.length ? '' : 'none';
        if (!items.length) { tableDiv.innerHTML = '<p style="color:var(--text-dim);">No items added yet.</p>'; return; }

        const sealedCount = items.filter(i => i.product_type === 'sealed').length;
        const cardCount = items.filter(i => i.product_type === 'raw').length;
        let summary = items.length + ' items';
        if (sealedCount && cardCount) summary += ' (' + sealedCount + ' sealed, ' + cardCount + ' cards)';
        summary += ' \u2022 Market: $' + (s.total_market_value || 0).toFixed(2) + ' \u2022 Offer: $' + (s.total_offer_amount || 0).toFixed(2);

        tableDiv.innerHTML = '<div style="margin-bottom:8px; font-weight:600;">' + summary +
            '</div><table class="responsive-cards"><thead><tr><th>Item</th><th>Type</th><th>Qty</th><th>Market</th><th>Offer</th><th></th></tr></thead><tbody>' +
            items.map(i => {
                const isCard = i.product_type === 'raw';
                let typeBadge;
                if (i.is_graded) {
                    typeBadge = '<span class="badge" style="background:var(--accent-alt,#7c3aed);color:#fff;">' + (i.grade_company||'') + ' ' + (i.grade_value||'') + '</span>';
                } else if (isCard) {
                    const _c = i.condition || '';
                    const _s = _c==='NM' ? 'background:#14532d;color:#4ade80;' : _c==='LP' ? 'background:rgba(79,125,249,0.18);color:#7aadff;' : _c==='MP' ? 'background:#422006;color:#fbbf24;' : _c==='HP' ? 'background:#431407;color:#fb923c;' : _c==='DMG' ? 'background:#450a0a;color:#f87171;' : 'background:var(--surface-2);color:var(--text-dim);';
                    typeBadge = '<span class="badge" style="' + _s + '">' + _c + '</span>';
                } else {
                    typeBadge = '<span class="badge" style="background:rgba(79,125,249,0.18);color:#7aadff;">Sealed</span>';
                }
                const varLabel = (i.variance && i.variance !== 'Normal') ? ' · ' + i.variance : '';
                const nameHtml = isCard
                    ? i.product_name + '<br><small style="color:var(--text-dim);">' + (i.set_name || '') + ' ' + (i.card_number ? '#'+i.card_number : '') + varLabel + '</small>'
                    : i.product_name + (i.set_name ? '<br><small style="color:var(--text-dim);">' + i.set_name + '</small>' : '');
                return '<tr>' +
                    '<td data-label="">' + nameHtml + '</td>' +
                    '<td data-label="Type">' + typeBadge + '</td>' +
                    '<td data-label="Qty">' + i.quantity + '</td>' +
                    '<td data-label="Market">$' + (i.market_price || 0).toFixed(2) + '</td>' +
                    '<td data-label="Offer">$' + (i.offer_price || 0).toFixed(2) + '</td>' +
                    '<td data-label=""><button class="btn btn-sm" style="color:var(--red); font-size:0.7rem; padding:2px 6px;" onclick="deleteIntakeItem(\'' + i.id + '\')">✕</button></td>' +
                '</tr>';
            }).join('') + '</tbody></table>';
    } catch(err) { console.error(err); }
}

async function deleteIntakeItem(itemId) {
    try {
        await fetch('/api/intake/item/' + itemId + '/delete', { method: 'POST' });
        loadIntakeItems();
    } catch(err) { alert(err.message); }
}

async function finalizeIntakeSession() {
    if (!intakeSessionId) return;
    await finalizeSession(intakeSessionId);
    intakeSessionId = null;
    document.getElementById('intake-entry').style.display = 'none';
    document.getElementById('intake-setup').style.display = '';
    document.getElementById('sealed-manual-items-table').innerHTML = '';
}

// ═══════════════════════════════ SESSION LIST ═══════════════════════════════
function loadFilteredActive() {
    const status = document.getElementById('active-filter-status').value;
    const fulfillment = document.getElementById('active-filter-fulfillment').value;
    const search = document.getElementById('active-filter-search').value.trim();
    let url = `/api/intake/sessions?status=${status}`;
    if (fulfillment) url += `&fulfillment=${fulfillment}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;
    loadSessions(url, 'active-sessions-list');
}

function loadFilteredCompleted() {
    const days = document.getElementById('completed-filter-days').value;
    const search = document.getElementById('completed-filter-search').value.trim();
    let url = `/api/intake/sessions?status=received,ingested,finalized`;
    if (days) url += `&days=${days}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;
    loadSessions(url, 'completed-sessions-list');
}

function loadFilteredCancelled() {
    const days = document.getElementById('cancelled-filter-days').value;
    const search = document.getElementById('cancelled-filter-search').value.trim();
    let url = `/api/intake/sessions?status=cancelled,rejected`;
    if (days) url += `&days=${days}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;
    loadSessions(url, 'cancelled-sessions-list');
}

async function loadSessions(urlOrStatus, containerId) {
    const c = document.getElementById(containerId);
    c.innerHTML = '<div class="loading"><span class="spinner"></span> Loading...</div>';
    try {
        // Support both old-style (status, containerId) and new-style (full url, containerId)
        const url = urlOrStatus.startsWith('/') ? urlOrStatus : `/api/intake/sessions?status=${urlOrStatus}`;
        const r = await fetch(url);
        const d = await r.json();
        if (!d.sessions.length) { c.innerHTML = '<p style="color:var(--text-dim);">No sessions found.</p>'; return; }

        const isActive = containerId === 'active-sessions-list';
        c.innerHTML = `<table class="responsive-cards">
            <thead><tr>
                ${isActive ? '<th style="width:30px;"></th>' : ''}
                <th>Customer</th><th>Type</th><th>Status</th><th>Items</th><th>Offer</th>
                <th>Accepted Details</th><th>Created</th><th></th>
            </tr></thead>
            <tbody>${d.sessions.map(s => {
                const statusColors = {
                    'in_progress': 'background:var(--surface-2);color:var(--text);',
                    'offered': 'background:#2563eb;color:#fff;',
                    'accepted': 'background:#059669;color:#fff;',
                    'received': 'background:#7c3aed;color:#fff;',
                    'partially_ingested': 'background:#d97706;color:#fff;',
                    'ingested': 'background:#16a34a;color:#fff;',
                    'finalized': 'background:#16a34a;color:#fff;',
                    'rejected': 'background:#dc2626;color:#fff;',
                    'cancelled': 'background:#666;color:#fff;',
                };
                const statusStyle = statusColors[s.status] || '';
                // Show accepted-pickup or accepted-mail
                let statusLabel = s.status.replace('_', ' ');
                if (s.status === 'accepted' && s.fulfillment_method) {
                    statusLabel = s.fulfillment_method === 'mail' ? '📬 accepted' : '🚗 accepted';
                }
                // Accepted details: pickup date or tracking
                let details = '—';
                if (s.fulfillment_method === 'pickup' && s.pickup_date) {
                    details = '🚗 ' + s.pickup_date;
                } else if (s.fulfillment_method === 'mail' && s.tracking_number) {
                    const isUrl = (s.tracking_number || '').startsWith('http');
                    details = isUrl
                        ? '📬 <a href="' + s.tracking_number + '" target="_blank" style="color:var(--accent);font-size:0.8rem;">tracking</a>'
                        : '📬 ' + s.tracking_number;
                } else if (s.fulfillment_method === 'pickup') {
                    details = '🚗 no date set';
                } else if (s.fulfillment_method === 'mail') {
                    details = '📬 no tracking';
                }
                const walkBadge = s.is_walk_in === true ? ' <span class="badge" style="background:#7c3aed;color:#fff;font-size:0.65rem;">WALK-IN</span>' : '';
                const accBadge = s.accepted_offer_type ? ` <span class="badge" style="background:var(--green);color:#fff;font-size:0.65rem;">${s.accepted_offer_type.toUpperCase()}</span>` : '';
                return `<tr>
                ${isActive ? `<td><input type="checkbox" class="merge-check" data-session-id="${s.id}" data-customer="${s.customer_name || 'Unknown'}" onchange="updateMergeBtn()"></td>` : ''}
                <td data-label="Customer">${s.customer_name || 'Unknown'}${s.is_distribution === true ? ' <span class="badge" style="background:#7c3aed;color:#fff;font-size:0.65rem;">DIST</span>' : ''}${walkBadge}</td>
                <td data-label="Type"><span class="badge badge-blue">${s.session_type}</span></td>
                <td data-label="Status"><span class="badge" style="${statusStyle}">${statusLabel}</span>${accBadge}</td>
                <td data-label="Items">${s.item_count || 0}</td>
                <td data-label="Offer">$${(s.total_offer_amount || 0).toFixed(2)}</td>
                <td data-label="Details" style="font-size:0.8rem; color:var(--text-dim);">${details}</td>
                <td data-label="Created">${new Date(s.created_at).toLocaleDateString()}</td>
                <td data-label="" style="display:flex;gap:6px;align-items:center;">
                    <button class="btn btn-secondary btn-sm" onclick="viewSession('${s.id}')">View</button>
                    ${['cancelled','rejected'].includes(s.status) ? `
                    <button class="btn btn-sm" style="background:#059669;color:#fff;border:none;"
                        onclick="rejuvenateSession('${s.id}', event)">↺ Rejuvenate</button>` : ''}
                </td>
            </tr>`;
            }).join('')}</tbody>
        </table>`;
    } catch(err) {
        c.innerHTML = `<div class="alert alert-error">Failed to load: ${err.message}</div>`;
    }
}

// ═══════════════════════════════ MERGE SESSIONS ═══════════════════════════════

function updateMergeBtn() {
    const checked = document.querySelectorAll('.merge-check:checked');
    const btn = document.getElementById('merge-sessions-btn');
    btn.style.display = checked.length === 2 ? 'inline-block' : 'none';
}

async function mergeSessions() {
    const checked = [...document.querySelectorAll('.merge-check:checked')];
    if (checked.length !== 2) return;

    const s1 = { id: checked[0].dataset.sessionId, name: checked[0].dataset.customer };
    const s2 = { id: checked[1].dataset.sessionId, name: checked[1].dataset.customer };

    const pick = prompt(
        `Merge sessions:\n  1) ${s1.name} (${s1.id.slice(0,8)}...)\n  2) ${s2.name} (${s2.id.slice(0,8)}...)\n\n` +
        `Which session should KEEP? Enter 1 or 2:`, '1');
    if (!pick || !['1','2'].includes(pick.trim())) return;

    const target = pick.trim() === '1' ? s1 : s2;
    const source = pick.trim() === '1' ? s2 : s1;

    if (!confirm(`Merge "${source.name}" into "${target.name}"?\n\nDuplicate items will be combined. The source session will be cancelled.`)) return;

    try {
        const r = await fetch('/api/intake/merge-sessions', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ target_session_id: target.id, source_session_id: source.id }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        const stats = d.merge_stats || {};
        alert(`Merged! ${stats.merged || 0} items combined, ${stats.moved || 0} items moved.`);
        loadFilteredActive();
    } catch(err) {
        alert('Merge failed: ' + err.message);
    }
}

// ═══════════════════════════════ VIEW SESSION ═══════════════════════════════

function switchSessionTab(btn, tabId) {
    const parent = btn.closest('#session-modal-body');
    if (!parent) return;
    parent.querySelectorAll('[data-stab]').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    parent.querySelectorAll('.session-tab-content').forEach(c => c.style.display = 'none');
    const target = document.getElementById('stab-' + tabId);
    if (target) target.style.display = '';
}

async function viewSession(sessionId, _preserveScroll) {
    currentSessionId = sessionId;
    if (intakeSessionId && sessionId === intakeSessionId) {
        loadIntakeItems();
    }
    const body = document.getElementById('session-modal-body');
    // Capture window scroll BEFORE any DOM mutation — spinner reflow will zero it out
    const scrollTop = _preserveScroll ? window.scrollY : 0;
    if (!_preserveScroll) {
        body.innerHTML = '<div class="loading"><span class="spinner"></span></div>';
    }
    showSessionView();

    try {
        const r = await fetch(`/api/intake/session/${sessionId}`);
        const d = await r.json();
        const s = d.session;
        const items = d.items;
        window._sessionItems = items;
        window._sessionMeta = s;
        const editable = !['received','ingested','cancelled','rejected'].includes(s.status);
        const rejuvenatable = ['cancelled','rejected'].includes(s.status);

        // Resolve cash/credit numbers, falling back to legacy
        // offer_percentage on rows that predate the split.
        const _cashPct = (s.cash_percentage != null) ? Number(s.cash_percentage)
                        : (s.offer_percentage != null ? Number(s.offer_percentage) : 65);
        const _creditPct = (s.credit_percentage != null) ? Number(s.credit_percentage) : 75;
        const _cashTotal = (s.total_offer_cash != null) ? Number(s.total_offer_cash)
                          : Number(s.total_offer_amount || 0);
        const _creditTotal = (s.total_offer_credit != null) ? Number(s.total_offer_credit)
                            : (s.credit_percentage != null ? Number(s.total_market_value || 0) * _creditPct / 100 : 0);
        const _accepted = s.accepted_offer_type;
        const _isWalkIn = !!s.is_walk_in;

        body.innerHTML = `
            ${_renderStickyActions(sessionId, s, items)}
            <div class="stats">
                <div class="stat">
                    <div class="stat-label">Customer</div>
                    <div class="stat-value" style="font-size:1.1rem;">${s.customer_name}</div>
                    <div style="margin-top:4px; display:flex; gap:6px; flex-wrap:wrap; align-items:center;">
                        ${s.is_distribution === true ? '<span class="badge" style="background:#7c3aed;color:#fff;">📦 DISTRIBUTION</span>' : ''}
                        ${_isWalkIn ? '<span class="badge" style="background:#7c3aed;color:#fff;">🚪 WALK-IN</span>' : ''}
                        ${editable ? `<button class="btn btn-sm btn-secondary" style="font-size:0.7rem; padding:2px 6px;" onclick="toggleWalkIn('${sessionId}', ${!_isWalkIn})">${_isWalkIn ? 'Mark Mail/Pickup' : 'Mark Walk-In'}</button>` : ''}
                    </div>
                </div>
                <div class="stat"><div class="stat-label">Type</div><div class="stat-value" style="font-size:1.1rem;"><span class="badge badge-blue">${s.session_type}</span>
                    ${editable ? `<div style="margin-top:6px;"><button class="btn btn-sm" style="font-size:0.75rem;padding:2px 8px;background:${s.is_distribution === true ? '#7c3aed' : 'var(--surface-2)'};color:${s.is_distribution === true ? '#fff' : 'var(--text-dim)'};border:1px solid ${s.is_distribution === true ? '#7c3aed' : 'var(--border)'};" onclick="toggleDistribution('${sessionId}')">${s.is_distribution === true ? '📦 Distribution' : '🤝 Private Purchase'}</button></div>` : ''}
                </div></div>
                <div class="stat"><div class="stat-label">Market Value</div><div class="stat-value"><span id="market-value-display">$${(s.total_market_value || 0).toFixed(2)}</span></div></div>
                <div class="stat">
                    <div class="stat-label">Offer Percentages ${_accepted ? '<span class="badge" style="background:var(--green);color:#fff;">' + _accepted.toUpperCase() + ' ACCEPTED</span>' : ''}</div>
                    <div class="stat-value" style="display:flex; flex-direction:column; gap:4px;">
                        <div style="display:flex; align-items:center; gap:6px; font-size:0.95rem;">
                            <span style="color:var(--text-dim); width:60px;">Credit</span>
                            ${editable && !_accepted ? `<input id="pct-credit-input" type="number" min="0" max="100" step="0.5" value="${_creditPct}" data-orig="${_creditPct}" style="width:70px; padding:4px 6px; font-size:0.9rem;" oninput="onPctEdit('${sessionId}')">` : `<span>${_creditPct}%</span>`}
                            <span style="color:var(--text-dim); font-size:0.85rem;">→ <strong style="color:${_accepted==='credit'?'var(--green)':'var(--text)'};">$${_creditTotal.toFixed(2)}</strong></span>
                        </div>
                        <div style="display:flex; align-items:center; gap:6px; font-size:0.95rem;">
                            <span style="color:var(--text-dim); width:60px;">Cash</span>
                            ${editable && !_accepted ? `<input id="pct-cash-input" type="number" min="0" max="100" step="0.5" value="${_cashPct}" data-orig="${_cashPct}" style="width:70px; padding:4px 6px; font-size:0.9rem;" oninput="onPctEdit('${sessionId}')">` : `<span>${_cashPct}%</span>`}
                            <span style="color:var(--text-dim); font-size:0.85rem;">→ <strong style="color:${_accepted==='cash'?'var(--green)':'var(--text)'};">$${_cashTotal.toFixed(2)}</strong></span>
                        </div>
                        ${editable && !_accepted ? `<div id="pct-edit-controls" style="display:none; margin-top:4px; gap:6px; align-items:center; font-size:0.8rem;">
                            <button class="btn btn-sm btn-primary" style="font-size:0.75rem; padding:3px 8px;" onclick="savePcts('${sessionId}')">Save</button>
                            <button class="btn btn-sm btn-secondary" style="font-size:0.75rem; padding:3px 8px;" onclick="cancelPctEdit()">Cancel</button>
                            <span id="pct-override-status" style="color:var(--text-dim);"></span>
                        </div>` : ''}
                    </div>
                </div>
            </div>${_renderRoleLockBanner(sessionId, s)}

            <!-- Session Sub-tabs -->
            <div style="display:flex; gap:2px; margin-bottom:16px; border-bottom:1px solid var(--border);">
                <button class="tab active" data-stab="offer" onclick="switchSessionTab(this,'offer')">📋 Offer</button>
                <button class="tab" data-stab="summary" onclick="switchSessionTab(this,'summary'); if(!document.getElementById('stab-summary').dataset.loaded){ loadMetaStats('${sessionId}'); document.getElementById('stab-summary').dataset.loaded='1'; }">📊 Collection Summary</button>
                <button class="tab" data-stab="ppt" onclick="switchSessionTab(this,'ppt'); if(!document.getElementById('stab-ppt').dataset.loaded){ refreshPrices('${sessionId}'); document.getElementById('stab-ppt').dataset.loaded='1'; }">💰 Market Prices</button>
                ${s.session_type !== 'raw' ? `<button class="tab" data-stab="store" onclick="switchSessionTab(this,'store'); if(!document.getElementById('stab-store').dataset.loaded){ storeCheck('${sessionId}'); document.getElementById('stab-store').dataset.loaded='1'; }">🏪 Store</button>` : ''}
            </div>

            <!-- Collection Summary tab content — loaded lazily on first click -->
            <div id="stab-summary" class="session-tab-content" style="display:none;">
                <div id="meta-stats-panel"></div>
            </div>

            <!-- ═══ OFFER TAB ═══ -->
            <div id="stab-offer" class="session-tab-content">
                ${rejuvenatable ? `
                    <div style="margin-bottom:12px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
                        <button class="btn btn-sm" style="background:#059669;color:#fff;border:none;"
                            onclick="rejuvenateSession('${sessionId}')">↺ Rejuvenate Session</button>
                        <span style="font-size:0.8rem;color:var(--text-dim);">Restore this session to In Progress</span>
                    </div>
                ` : ''}

                ${(() => {
                    const activeUnmapped = items.filter(i => !i.is_mapped && ['good','damaged'].includes(i.item_status || 'good'));
                    return activeUnmapped.length > 0 ? `<div class="alert alert-warning" style="margin-bottom:12px;">${activeUnmapped.length} items need linking</div>` : '';
                })()}

                ${editable ? (() => {
                    // Always render both Add-Item subsections regardless of
                    // session_type — staff often need to drop a card into a
                    // sealed-imported session (and vice versa) without
                    // re-creating the session as 'mixed'.
                    let addHtml = '<div style="margin-bottom:16px; padding:12px; background:var(--surface-2); border-radius:8px; border:1px solid var(--border);">';
                    addHtml += '<h4 style="margin-bottom:10px; font-size:0.95rem;">➕ Add Item</h4>';
                    addHtml += '<div style="display:flex; gap:4px; margin-bottom:10px;">';
                    addHtml += '<button class="btn btn-sm btn-primary" id="session-add-type-sealed" onclick="switchSessionAddType(\'sealed\')">Sealed Product</button>';
                    addHtml += '<button class="btn btn-sm btn-secondary" id="session-add-type-card" onclick="switchSessionAddType(\'card\')">Individual Card</button>';
                    addHtml += '</div>';
                    const _sid = sessionId;
                    const _opct = s.offer_percentage;
                    {
                        addHtml += '<div id="session-add-sealed">';
                        addHtml += '<div style="display:flex; gap:8px; align-items:flex-end; flex-wrap:wrap;">';
                        addHtml += '<div class="form-group" style="flex:2; min-width:180px; margin:0;"><label>Search Prices</label>';
                        addHtml += '<input type="text" id="session-sealed-search" placeholder="e.g. Celebrations ETB" onkeydown="if(event.key===\'Enter\') searchSealedForSession(\'' + _sid + '\', ' + _opct + ')"></div>';
                        addHtml += '<div class="form-group" style="width:60px; margin:0;"><label>Qty</label><input type="number" id="session-sealed-qty" value="1" min="1"></div>';
                        addHtml += '<button class="btn btn-primary btn-sm" onclick="searchSealedForSession(\'' + _sid + '\', ' + _opct + ')">Search</button>';
                        addHtml += '<button class="btn btn-secondary btn-sm" onclick="toggleManualSealedAdd(\'' + _sid + '\', ' + _opct + ')">Manual</button>';
                        addHtml += '</div>';
                        addHtml += '<div id="session-sealed-results" style="margin-top:8px;"></div>';
                        addHtml += '<div id="session-sealed-manual" style="display:none; margin-top:12px; padding:12px; background:var(--surface-1); border-radius:6px; border:1px solid var(--border);">';
                        addHtml += '<h5 style="margin-bottom:8px; font-size:0.85rem; color:var(--text-dim);">Manual Add (for items not in PPT)</h5>';
                        addHtml += '<div style="display:flex; gap:8px; flex-wrap:wrap; align-items:flex-end;">';
                        addHtml += '<div class="form-group" style="flex:2; min-width:160px; margin:0;"><label style="font-size:0.8rem;">Product Name</label><input type="text" id="session-manual-name" placeholder="e.g. Terastal Festival ex Box"></div>';
                        addHtml += '<div class="form-group" style="flex:1; min-width:100px; margin:0;"><label style="font-size:0.8rem;">Set Name</label><input type="text" id="session-manual-set" placeholder="optional"></div>';
                        addHtml += '<div class="form-group" style="width:100px; margin:0;"><label style="font-size:0.8rem;">TCGPlayer ID</label><input type="number" id="session-manual-tcgid" placeholder="optional"></div>';
                        addHtml += '<div class="form-group" style="width:90px; margin:0;"><label style="font-size:0.8rem;">Price</label><input type="number" id="session-manual-price" step="0.01" min="0" placeholder="$0.00"></div>';
                        addHtml += '<div class="form-group" style="width:55px; margin:0;"><label style="font-size:0.8rem;">Qty</label><input type="number" id="session-manual-qty" value="1" min="1"></div>';
                        addHtml += '<button class="btn btn-success btn-sm" onclick="submitManualSealedAdd(\'' + _sid + '\', ' + _opct + ')">Add</button>';
                        addHtml += '</div></div></div>';
                    }
                    {
                        addHtml += '<div id="session-add-card" style="display:none;">';
                        // Graded toggle — mirrors the New Intake form so users can enter
                        // a PSA/BGS/CGC/SGC card directly without the "add raw → change to graded" detour.
                        addHtml += '<div style="display:flex; align-items:center; gap:12px; margin-bottom:10px; padding:8px 12px; background:var(--surface-2); border-radius:8px; border:1px solid var(--border);">';
                        addHtml += '<label style="display:flex; align-items:center; gap:8px; cursor:pointer; font-size:0.9rem;">';
                        addHtml += '<input type="checkbox" id="session-is-graded" onchange="toggleSessionGraded()" style="width:16px; height:16px;">';
                        addHtml += '<span style="font-weight:600;">Graded Card</span></label>';
                        addHtml += '<span id="session-graded-fields" style="display:none; align-items:center; gap:8px;">';
                        addHtml += '<select id="session-grade-company" style="width:80px; padding:4px 8px; background:var(--surface); border:1px solid var(--border); border-radius:4px; color:var(--text);">';
                        addHtml += '<option value="PSA">PSA</option><option value="BGS">BGS</option><option value="CGC">CGC</option><option value="SGC">SGC</option></select>';
                        addHtml += '<select id="session-grade-value" style="width:80px; padding:4px 8px; background:var(--surface); border:1px solid var(--border); border-radius:4px; color:var(--text);">';
                        addHtml += '<option value="10">10</option><option value="9.5">9.5</option><option value="9">9</option><option value="8.5">8.5</option><option value="8">8</option><option value="7">7</option></select></span>';
                        addHtml += '<span id="session-graded-hint" style="color:var(--text-dim); font-size:0.8rem;">Toggle for PSA/BGS/CGC graded cards — uses eBay market data</span>';
                        addHtml += '</div>';
                        addHtml += '<div style="display:flex; gap:8px; align-items:flex-end; flex-wrap:wrap;">';
                        addHtml += '<div class="form-group" style="flex:2; min-width:160px; margin:0;"><label>Card Search</label>';
                        addHtml += '<input type="text" id="session-raw-search" placeholder="e.g. Charizard ex" onkeydown="if(event.key===\'Enter\') smartCardSearch(\'' + _sid + '\', ' + _opct + ', \'session\')"></div>';
                        addHtml += '<div class="form-group" style="flex:1; min-width:120px; margin:0;"><label>Set</label><input type="text" id="session-raw-set" placeholder="optional"></div>';
                        addHtml += '<div class="form-group" style="width:90px; margin:0;"><label>TCG ID</label><input type="number" id="session-raw-tcgid" placeholder="optional" onkeydown="if(event.key===\'Enter\') smartCardSearch(\'' + _sid + '\', ' + _opct + ', \'session\')"></div>';
                        addHtml += '<div class="form-group" style="width:70px; margin:0;" id="session-raw-cond-group"><label>Cond</label><select id="session-raw-condition"><option value="NM">NM</option><option value="LP">LP</option><option value="MP">MP</option><option value="HP">HP</option><option value="DMG">DMG</option></select></div>';
                        addHtml += '<div class="form-group" style="width:50px; margin:0;"><label>Qty</label><input type="number" id="session-raw-qty" value="1" min="1"></div>';
                        addHtml += '<button class="btn btn-primary btn-sm" onclick="smartCardSearch(\'' + _sid + '\', ' + _opct + ', \'session\')">Search</button>';
                        addHtml += '<button class="btn btn-secondary btn-sm" onclick="toggleManualCardAdd(\'' + _sid + '\', ' + _opct + ')">Manual</button>';
                        addHtml += '</div>';
                        addHtml += '<div id="session-raw-results" style="margin-top:8px;"></div>';

                        // Manual card add — for cards Scrydex doesn\'t have (Tin Pack variants, DON!! cards, etc.)
                        addHtml += '<div id="session-card-manual" style="display:none; margin-top:12px; padding:12px; background:var(--surface-1); border-radius:6px; border:1px solid var(--border);">';
                        addHtml += '<h5 style="margin-bottom:8px; font-size:0.85rem; color:var(--text-dim);">Manual Card Add (for cards not in Scrydex)</h5>';
                        addHtml += '<div style="display:flex; gap:8px; flex-wrap:wrap; align-items:flex-end;">';
                        addHtml += '<div class="form-group" style="flex:2; min-width:160px; margin:0;"><label style="font-size:0.8rem;">Card Name</label><input type="text" id="session-card-manual-name" placeholder="e.g. Monkey.D.Luffy"></div>';
                        addHtml += '<div class="form-group" style="flex:1; min-width:120px; margin:0;"><label style="font-size:0.8rem;">Set</label><input type="text" id="session-card-manual-set" placeholder="e.g. Tin Pack Set Vol. 2"></div>';
                        addHtml += '<div class="form-group" style="width:90px; margin:0;"><label style="font-size:0.8rem;">Card #</label><input type="text" id="session-card-manual-num" placeholder="P-075"></div>';
                        addHtml += '<div class="form-group" style="width:100px; margin:0;"><label style="font-size:0.8rem;">TCGPlayer ID</label><input type="number" id="session-card-manual-tcgid" placeholder="optional"></div>';
                        addHtml += '<div class="form-group" style="width:70px; margin:0;"><label style="font-size:0.8rem;">Cond</label><select id="session-card-manual-cond"><option value="NM">NM</option><option value="LP">LP</option><option value="MP">MP</option><option value="HP">HP</option><option value="DMG">DMG</option></select></div>';
                        addHtml += '<div class="form-group" style="width:90px; margin:0;"><label style="font-size:0.8rem;">Price</label><input type="number" id="session-card-manual-price" step="0.01" min="0" placeholder="$0.00"></div>';
                        addHtml += '<div class="form-group" style="width:55px; margin:0;"><label style="font-size:0.8rem;">Qty</label><input type="number" id="session-card-manual-qty" value="1" min="1"></div>';
                        addHtml += '<button class="btn btn-success btn-sm" onclick="submitManualCardAdd(\'' + _sid + '\', ' + _opct + ')">Add</button>';
                        addHtml += '</div></div>';

                        addHtml += '</div>';
                    }
                    addHtml += '</div>';
                    return addHtml;
                })() : ''}

                <div style="overflow-x:auto;">
                <table class="responsive-cards">
                    <thead><tr>
                        <th>Product</th>${(s.session_type === 'raw' || s.session_type === 'mixed') ? '<th>Type</th>' : ''}<th>Qty</th><th>Market</th><th>Offer</th><th>Status</th><th>Actions</th>
                    </tr></thead>
                    <tbody>${items.map(i => {
                        const status = i.item_status || 'good';
                        const isDead = status === 'missing' || status === 'rejected';
                        const isBrokenDown = status === 'broken_down';
                        const isDamaged = status === 'damaged';
                        const isChild = !!i.parent_item_id;
                        const rowStyle = isDead ? 'opacity:0.45; text-decoration:line-through;' : isBrokenDown ? 'opacity:0.35; text-decoration:line-through;' : isDamaged ? 'background:rgba(255,170,0,0.08);' : '';
                        // Linked-status badge: differentiate TCG-linked from Store-only-linked
                        // so a Find-in-Store action that saves shopify_product_id without a
                        // tcgplayer_id stops rendering as "Linked TCG#null".
                        const _linkedBadge = (i) => {
                            if (i.tcgplayer_id) return `<span class="badge badge-green">Linked</span><br><small style="color:var(--text-dim);">TCG#${i.tcgplayer_id}</small>`;
                            if (i.shopify_product_id) {
                                const nm = (i.shopify_product_name || '').slice(0, 32);
                                return `<span class="badge" style="background:#7c3aed;color:#fff;">Store</span>${nm ? `<br><small style="color:var(--text-dim);">${nm}</small>` : ''}`;
                            }
                            return '';
                        };
                        const statusBadge = status === 'good'
                            ? (i.is_mapped ? _linkedBadge(i) || '<span class="badge badge-green">Linked</span>' : '<span class="badge badge-amber">Needs Link</span>')
                            : status === 'damaged' ? `<span class="badge" style="background:#b45309;color:#fff;">Damaged</span>${i.is_mapped ? '<br>' + _linkedBadge(i) : ''}`
                            : status === 'missing' ? '<span class="badge" style="background:#666;color:#fff;">Missing</span>'
                            : isBrokenDown ? '<span class="badge" style="background:var(--surface-2);color:var(--text-dim);">Broken Down</span>'
                            : '<span class="badge" style="background:#666;color:#fff;">Rejected</span>';
                        const overrideNote = i.price_override_note ? `<br><small style="color:var(--accent);" title="${(i.price_override_note||'').replace(/"/g,'&quot;')}">⚡ ${i.price_override_note}</small>` : '';
                        const parentNote = i.parent_item_id ? '<br><small style="color:var(--text-dim);">↳ split from above</small>' : '';
                        // Variant mismatch — seller claimed one variant at intake but the
                        // actual broken-down variant in ingest was different. Show signed
                        // dollar delta (actual - claimed): negative = we overpaid, positive = bonus.
                        let variantMismatch = '';
                        if (i.claimed_variant_id && i.actual_variant_id
                            && String(i.claimed_variant_id) !== String(i.actual_variant_id)
                            && bd && Array.isArray(bd.variants)) {
                            const _claimV = bd.variants.find(v => String(v.id) === String(i.claimed_variant_id));
                            const _actualV = bd.variants.find(v => String(v.id) === String(i.actual_variant_id));
                            const _claimVal = _claimV ? ((_claimV.store != null && _claimV.store > 0) ? _claimV.store : _claimV.market) : null;
                            const _actualVal = _actualV ? ((_actualV.store != null && _actualV.store > 0) ? _actualV.store : _actualV.market) : null;
                            if (_claimVal != null && _actualVal != null) {
                                const _delta = _actualVal - _claimVal;
                                const _sign = _delta >= 0 ? '+' : '-';
                                const _color = _delta < 0 ? 'var(--red,#f05252)' : 'var(--green,#2dd4a0)';
                                const _qty = i.quantity || 1;
                                const _totalDelta = _delta * _qty;
                                variantMismatch = `<br><span class="badge" style="background:${_color};color:#fff;font-size:0.65rem;" title="Claimed: ${_claimV.name} ($${_claimVal.toFixed(2)}) → Actual: ${_actualV.name} ($${_actualVal.toFixed(2)})${_qty > 1 ? ' × ' + _qty : ''}">Wrong variant: ${_sign}$${Math.abs(_totalDelta).toFixed(2)}</span>`;
                            } else {
                                variantMismatch = `<br><span class="badge" style="background:var(--red,#f05252);color:#fff;font-size:0.65rem;">Wrong variant</span>`;
                            }
                        }
                        const bd = s.session_type !== 'raw' ? i.breakdown_summary : null;
                        // Honor claimed_variant_id / probabilistic avg / selectable max
                        let bdVal = 0;
                        if (bd) {
                            if (typeof resolveBreakdownAggregate === 'function') {
                                const _agg = resolveBreakdownAggregate(bd, i.claimed_variant_id || null);
                                bdVal = (_agg.store != null && _agg.store > 0) ? _agg.store : (_agg.market || 0);
                            } else {
                                bdVal = parseFloat(bd.best_variant_market) || 0;
                            }
                        }
                        const bdNote = '';
                        const vel = i.velocity;
                        let velNote = '';
                        if (vel) {
                            const sold = vel.units_sold_90d || 0;
                            const allTime = vel.total_sold_all_time || 0;
                            const doi = vel.velocity_score || 9999;
                            const qty = vel.current_qty || 0;
                            const oos = vel.out_of_stock_days || 0;
                            const hasOosData = oos > 0;
                            const dailyRate = sold > 0 ? (sold / 90).toFixed(1) : 0;
                            let rateLabel, rateColor;
                            if (sold === 0 && allTime === 0) { rateLabel = 'No Sales'; rateColor = 'var(--text-dim)'; }
                            else if (sold === 0) { rateLabel = `${allTime} lifetime`; rateColor = 'var(--accent)'; }
                            else if (dailyRate >= 5) { rateLabel = 'Very Fast'; rateColor = 'var(--green)'; }
                            else if (dailyRate >= 1) { rateLabel = 'Fast'; rateColor = 'var(--green)'; }
                            else if (dailyRate >= 0.3) { rateLabel = 'Medium'; rateColor = 'var(--amber)'; }
                            else if (dailyRate >= 0.1) { rateLabel = 'Slow'; rateColor = 'var(--text-dim)'; }
                            else { rateLabel = 'Very Slow'; rateColor = 'var(--text-dim)'; }
                            const stockInfo = qty > 0 ? (doi < 9999 ? `${qty} stock (${Math.round(doi)}d)` : `${qty} stock`) : 'OOS';
                            const stockColor = qty === 0 ? 'var(--red)' : doi <= 14 ? 'var(--amber)' : 'var(--text-dim)';
                            const confidence = hasOosData ? '' : ' ⚠';
                            velNote = `<br><small>📊 <span style="color:${rateColor};">${sold} sold · ${rateLabel}</span> · <span style="color:${stockColor};">${stockInfo}</span>${confidence}</small>`;
                        }
                        return `<tr style="${rowStyle}" data-item-id="${i.id}">
                            <td class="name-cell" data-label="">${i.product_name}${i.set_name ? `<br><small style="color:var(--text-dim);">${i.set_name}${i.card_number ? ' #'+i.card_number : ''}</small>` : ''}${overrideNote}${parentNote}${variantMismatch}${bdNote}${velNote}</td>
                            ${(s.session_type === 'raw' || s.session_type === 'mixed') ? `<td data-label="Type">${
                                i.product_type === 'sealed'
                                ? '<span class="badge" style="background:rgba(79,125,249,0.18);color:#7aadff;">Sealed</span>'
                                : i.is_graded
                                ? `<span class="badge" style="background:linear-gradient(135deg,#7c3aed,#4f7df9);color:#fff;font-weight:700;">${i.grade_company || 'PSA'} ${i.grade_value || '?'}</span>`
                                : (() => {
                                    const _c = i.condition || i.listing_condition || '—';
                                    const _condStyle = _c==='NM' ? 'background:#14532d;color:#4ade80;' : _c==='LP' ? 'background:rgba(79,125,249,0.18);color:#7aadff;' : _c==='MP' ? 'background:#422006;color:#fbbf24;' : _c==='HP' ? 'background:#431407;color:#fb923c;' : _c==='DMG' ? 'background:#450a0a;color:#f87171;' : 'background:var(--surface-2);color:var(--text-dim);';
                                    return `<span class="badge" style="${_condStyle}">${_c}</span>`;
                                })()
                            }</td>` : ''}
                            <td data-label="Qty">${i.quantity}</td>
                            <td data-label="Market">$${(i.market_price || 0).toFixed(2)}</td>
                            <td data-label="Offer">$${(i.offer_price || 0).toFixed(2)}</td>
                            <td data-label="Status">${statusBadge}</td>
                            <td data-label="" style="white-space:nowrap;">
                                ${editable ? (() => {
                                    let btns = '';
                                    if (isDead) {
                                        btns += `<button class="btn btn-sm btn-secondary" style="font-size:0.7rem;padding:2px 6px;" onclick="restoreItem('${i.id}','${sessionId}')">Restore</button>`;
                                    } else {
                                        const linkLabel = i.is_mapped ? 'Relink' : 'Link';
                                        const linkClass = i.is_mapped ? 'btn-secondary' : 'btn-primary';
                                        if (i.product_type === 'raw') {
                                            btns += `<button class="btn ${linkClass} btn-sm" style="font-size:0.7rem;padding:2px 6px;" onclick="relinkRawCard('${i.id}','${sessionId}')">${linkLabel}</button> `;
                                        } else {
                                            btns += `<button class="btn ${linkClass} btn-sm" style="font-size:0.7rem;padding:2px 6px;" onclick="openMappingFromSession('${i.id}')">${linkLabel}</button> `;
                                        }
                                        btns += '<div class="action-dropdown-container" style="display:inline-block; position:relative;"><button class="btn btn-sm btn-secondary action-trigger" style="font-size:0.7rem;padding:2px 6px;" onclick="toggleActionMenu(event, this.nextElementSibling)">⋯</button><div class="action-dropdown">';
                                        if (!i.is_graded && i.product_type !== 'raw') {
                                            btns += status !== 'damaged' ? `<button onclick="damageItem('${i.id}','${sessionId}',${i.quantity})">&#9888; Damaged${i.quantity > 1 ? ' (partial?)' : ''}</button>` : `<button onclick="markItemStatus('${i.id}','${sessionId}','good')">Mark Good</button>`;
                                        }
                                        btns += `<button onclick="markItemStatus('${i.id}','${sessionId}','rejected')">🗑 Remove</button><hr>`;
                                        btns += `<button onclick="overridePrice('${i.id}','${sessionId}',${i.market_price||0})">Override Price</button>`;
                                        btns += `<button onclick="editQuantity('${i.id}','${sessionId}',${i.quantity})">Change Qty (${i.quantity})</button>`;
                                        if (i.product_type === 'raw') {
                                            if (i.is_graded) {
                                                btns += `<hr><button onclick="openMarkGraded('${i.id}','${sessionId}','${(i.product_name||'').replace(/'/g,"\\'").replace(/"/g,'&quot;')}',${i.tcgplayer_id ? parseInt(i.tcgplayer_id) : null},true,'${i.grade_company||''}','${i.grade_value||''}')">&#x1F3C5; Change Grade (${i.grade_company} ${i.grade_value})</button>`;
                                            } else {
                                                btns += `<hr><button onclick="openMarkGraded('${i.id}','${sessionId}','${(i.product_name||'').replace(/'/g,"\\'").replace(/"/g,'&quot;')}',${i.tcgplayer_id ? parseInt(i.tcgplayer_id) : null},false,'','')">✨ Mark as Graded</button>`;
                                                btns += `<button onclick="editCondition('${i.id}','${sessionId}','${i.condition || i.listing_condition || 'NM'}',${i.tcgplayer_id ? parseInt(i.tcgplayer_id) : null},'${(i.product_name||'').replace(/'/g,"\\'").replace(/"/g,'&quot;')}${i.set_name ? ' · '+i.set_name.replace(/'/g,"\\'").replace(/"/g,'&quot;') : ''}')">${condBadgeHtml(i.condition || i.listing_condition || 'NM')} Change Condition</button>`;
                                            }
                                        }

                                        if (i.product_type !== 'raw' && i.tcgplayer_id) {
                                            btns += `<hr><button onclick="openIntakeBreakdown(${i.tcgplayer_id}, '${(i.product_name||'').replace(/'/g,'').replace(/"/g,'')}', ${i.market_price||0})">📦 Breakdown Recipe</button>`;
                                            if (bd && bdVal > 0) {
                                                btns += `<button onclick="applyBreakdownPriceWithQty('${i.id}','${sessionId}',${bdVal},'${(bd.variant_names||'breakdown').replace(/'/g,'')}',${i.quantity})">💲 Price as Breakdown ($${bdVal.toFixed(2)})</button>`;
                                            }
                                        }
                                        btns += '</div></div>';
                                    }
                                    return btns;
                                })() : ''}
                            </td>
                        </tr>`;
                    }).join('')}</tbody>
                </table>
                </div>
            </div>

            <!-- ═══ PPT PRICES TAB ═══ -->
            <div id="stab-ppt" class="session-tab-content" style="display:none;">
                <div id="price-refresh-results">
                    <p style="color:var(--text-dim); text-align:center; padding:40px;">Loading PPT prices...</p>
                </div>
            </div>

            <!-- ═══ STORE CHECK TAB ═══ -->
            <div id="stab-store" class="session-tab-content" style="display:none;">
                <div id="store-check-results">
                    <p style="color:var(--text-dim); text-align:center; padding:40px;">Loading store data...</p>
                </div>
            </div>

            ${s.status === 'cancelled' || s.status === 'rejected' ? `
                <div class="alert alert-error" style="margin-top:16px;">
                    Session ${s.status}${s.cancel_reason ? ': ' + s.cancel_reason : ''}
                </div>
            ` : ''}

            ${(() => {
                const activeItems = items.filter(i => {
                    const st = i.item_status || 'good';
                    return st === 'good' || st === 'damaged';
                });
                const unmappedActive = activeItems.filter(i => !i.is_mapped);
                const excluded = items.filter(i => i.item_status === 'missing' || i.item_status === 'rejected');
                const brokenDown = items.filter(i => i.item_status === 'broken_down');
                let html = '';
                if (excluded.length > 0) {
                    html += '<div style="margin-top:12px;color:var(--text-dim);font-size:0.85rem;">' + excluded.length + ' item(s) excluded (missing/rejected)</div>';
                }
                if (brokenDown.length > 0) {
                    html += '<div style="margin-top:4px;color:var(--text-dim);font-size:0.85rem;">' + brokenDown.length + ' item(s) broken down into components</div>';
                }
                const timestamps = [];
                if (s.offered_at) timestamps.push('Offered: ' + new Date(s.offered_at).toLocaleString());
                if (s.accepted_at) timestamps.push('Accepted: ' + new Date(s.accepted_at).toLocaleString());
                if (s.received_at) timestamps.push('Received: ' + new Date(s.received_at).toLocaleString());
                if (s.rejected_at) timestamps.push('Rejected: ' + new Date(s.rejected_at).toLocaleString());
                if (s.ingested_at) timestamps.push('Ingested: ' + new Date(s.ingested_at).toLocaleString());
                if (timestamps.length > 0) {
                    html += '<div style="margin-top:12px;font-size:0.8rem;color:var(--text-dim);">' + timestamps.join(' · ') + '</div>';
                }
                html += '<div style="margin-top:12px; display:flex; gap:8px; flex-wrap:wrap; align-items:center;">';
                html += '<a href="/api/intake/session/' + sessionId + '/export-csv" class="btn btn-secondary btn-sm" style="font-size:0.8rem;">📋 Export CSV</a>';
                if (s.status === 'in_progress') {
                    if (unmappedActive.length === 0 && activeItems.length > 0) {
                        html += "<button class=\"btn btn-success\" onclick=\"transitionSession('" + sessionId + "', 'offer')\">📤 Lock &amp; Offer (" + activeItems.length + " items)</button>";
                    }
                } else if (s.status === 'offered') {
                    // Per spec: two clear actions — Accept Cash / Accept
                    // Credit. Walk-in sessions short-circuit to received
                    // on accept (the customer is at the counter); regular
                    // sessions still capture pickup vs mail in the
                    // accept-flow modal.
                    const _walkBadge = _isWalkIn ? ' <span style="font-size:0.7rem; opacity:0.85;">→ received</span>' : '';
                    const _credLabel = (s.credit_percentage != null) ? ` ($${_creditTotal.toFixed(2)})` : '';
                    const _cashLabel = ` ($${_cashTotal.toFixed(2)})`;
                    if (s.credit_percentage != null) {
                        html += `<button class="btn btn-success" onclick="acceptOffer('${sessionId}', 'credit')">💳 Accept Credit${_credLabel}${_walkBadge}</button>`;
                    }
                    html += `<button class="btn btn-success" onclick="acceptOffer('${sessionId}', 'cash')">💵 Accept Cash${_cashLabel}${_walkBadge}</button>`;
                    html += "<button class=\"btn btn-error\" onclick=\"transitionSession('" + sessionId + "', 'reject')\">❌ Customer Rejected</button>";
                    html += "<button class=\"btn btn-secondary btn-sm\" onclick=\"transitionSession('" + sessionId + "', 'reopen')\">↩ Reopen for Edits</button>";
                } else if (s.status === 'accepted') {
                    html += "<button class=\"btn btn-success\" onclick=\"transitionSession('" + sessionId + "', 'receive')\">📦 Product Received</button>";
                    html += "<button class=\"btn btn-secondary btn-sm\" onclick=\"transitionSession('" + sessionId + "', 'reopen')\">↩ Reopen for Edits</button>";
                    if (s.fulfillment_method === 'mail') {
                        html += '<div style="margin-top:8px; display:flex; align-items:center; gap:8px;">';
                        html += '<span class="badge badge-blue">📬 Mail</span>';
                        html += '<span style="font-size:0.85rem;color:var(--text-dim);">Tracking: </span>';
                        if (s.tracking_number) {
                            var isUrl = s.tracking_number.startsWith('http');
                            html += isUrl ? '<a href="' + s.tracking_number + '" target="_blank" style="color:var(--accent);font-size:0.85rem;">' + s.tracking_number + '</a>' : '<span style="font-size:0.85rem;">' + s.tracking_number + '</span>';
                        } else {
                            html += '<span style="font-size:0.85rem;color:var(--text-dim);">None</span>';
                        }
                        html += " <button class=\"btn btn-sm btn-secondary\" style=\"font-size:0.7rem;padding:2px 6px;\" onclick=\"editTracking('" + sessionId + "', '" + (s.tracking_number || '').replace(/'/g, "\\'") + "')\">Edit</button>";
                        html += '</div>';
                    } else {
                        html += '<div style="margin-top:8px; display:flex; align-items:center; gap:8px;">';
                        html += '<span class="badge badge-green">🚗 Pickup</span>';
                        if (s.pickup_date) {
                            html += '<span style="font-size:0.85rem;">' + s.pickup_date + '</span>';
                        } else {
                            html += '<span style="font-size:0.85rem;color:var(--text-dim);">No date set</span>';
                        }
                        html += " <button class=\"btn btn-sm btn-secondary\" style=\"font-size:0.7rem;padding:2px 6px;\" onclick=\"editPickupDate('" + sessionId + "', '" + (s.pickup_date || '') + "')\">Edit</button>";
                        html += '</div>';
                    }
                } else if (s.status === 'received') {
                    html += '<span style="color:var(--green); font-weight:600;">✅ Ready for Ingest Service</span>';
                    html += "<button class=\"btn btn-secondary btn-sm\" onclick=\"transitionSession('" + sessionId + "', 'reopen')\">↩ Reopen for Edits</button>";
                } else if (s.status === 'ingested') {
                    html += '<span style="color:var(--green); font-weight:600;">✅ Ingested</span>';
                }
                html += '</div>';
                return html;
            })()}
        `;
        // Restore scroll after render — use rAF + retries to survive repaint/reflow
        if (scrollTop) {
            const _restoreScroll = () => { window.scrollTo(0, scrollTop); };
            _restoreScroll();
            requestAnimationFrame(() => {
                _restoreScroll();
                requestAnimationFrame(_restoreScroll);
            });
            setTimeout(_restoreScroll, 100);
            setTimeout(_restoreScroll, 300);
        }
        // Enrich breakdown summaries inline without full reload
        _enrichIntakeBreakdowns(items, sessionId);
        // Collection Summary tab loads lazily on first click — see the
        // 📊 tab button above for the trigger.
        // Restore the user's "Sealed Product" vs "Individual Card" choice
        // so adding card after card doesn't keep flipping back to sealed
        _restoreSessionAddType();
    } catch(err) {
        body.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

function openMappingFromSession(itemId) {
    const items = window._sessionItems || [];
    const i = items.find(x => String(x.id) === String(itemId));
    if (!i) { alert('Item not found in session'); return; }
    openMapping(i.id, i.product_name || '', i.set_name || '', i.product_type || 'sealed', i.market_price || 0);
}

// ═══════════════════════════════ MAPPING ═══════════════════════════════
let currentMapCollectrPrice = 0;
// Set when the user picks a Scrydex-only search result (sealed JP, etc.).
// Cleared when the modal opens or when a TCG ID is pasted/picked.
let currentMapScrydexId = null;

function openMapping(itemId, productName, setName, productType, collectrPrice) {
    currentMapItemId = itemId;
    currentMapProductType = productType || 'sealed';
    currentMapCollectrPrice = collectrPrice || 0;
    currentMapScrydexId = null;
    document.getElementById('map-product-name').textContent = productName;
    document.getElementById('map-set-name').textContent = setName || 'N/A';
    document.getElementById('map-collectr-price').textContent = `$${currentMapCollectrPrice.toFixed(2)}`;
    document.getElementById('map-tcgplayer-id').value = '';
    document.getElementById('map-search-query').value = productName;
    document.getElementById('fuzzy-results').innerHTML = '';
    document.getElementById('price-comparison').style.display = 'none';
    document.getElementById('price-comparison').innerHTML = '';
    openModal('mapping-modal');
}

async function linkAndCompare() {
    const tcgId = document.getElementById('map-tcgplayer-id').value;
    if (!tcgId) { alert('Enter a TCGPlayer ID'); return; }

    const panel = document.getElementById('price-comparison');
    panel.style.display = 'block';
    panel.innerHTML = '<div class="loading"><span class="spinner"></span> Fetching PPT price...</div>';

    // Step 1: Look up PPT price
    let pptPrice = null;
    let pptProductName = '';
    // Primary live source is Scrydex (per project policy — PPT is fallback only).
    // Cache hits also reflect Scrydex's nightly snapshot. Default the label to
    // Scrydex so a missing _price_source field doesn't lie to staff with 'PPT'.
    let pptSourceLabel = 'Scrydex';
    try {
        const endpoint = currentMapProductType === 'raw' ? '/api/lookup/card' : '/api/lookup/sealed';
        const r = await fetch(endpoint, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tcgplayer_id: parseInt(tcgId) }),
        });
        const d = await r.json();
        if (r.ok) {
            const item = d.card || d.product || {};
            // Sealed products use 'unopenedPrice', cards use 'prices.market'
            pptPrice = item.unopenedPrice             // sealed products
                    || d.extracted_price               // backend pre-extracted
                    || item.prices?.market             // cards
                    || item.prices?.mid                // fallback
                    || item.market_price               // flat field
                    || null;
            pptProductName = item.name || item.productName || '';
            pptSourceLabel = {cache:'Cache',ppt:'PPT',scrydex:'Scrydex'}[item._price_source] || pptSourceLabel;
        } else {
            // PPT lookup failed — still allow linking, just no price comparison
            panel.innerHTML = `
                <div class="alert alert-warning" style="margin-top:12px;">
                    PPT lookup failed: ${d.error || 'Unknown error'}. You can still link with the Collectr price.
                </div>
                <button class="btn btn-primary" style="width:100%; margin-top:8px;" 
                        onclick="confirmLink(${currentMapCollectrPrice})">
                    Link with Collectr Price ($${currentMapCollectrPrice.toFixed(2)})
                </button>`;
            return;
        }
    } catch(err) {
        panel.innerHTML = `
            <div class="alert alert-error" style="margin-top:12px;">${err.message}</div>
            <button class="btn btn-primary" style="width:100%; margin-top:8px;" 
                    onclick="confirmLink(${currentMapCollectrPrice})">
                Link with Collectr Price ($${currentMapCollectrPrice.toFixed(2)})
            </button>`;
        return;
    }

    // Step 2: Show price comparison
    const collectr = currentMapCollectrPrice;
    const ppt = pptPrice || 0;
    const delta = collectr > 0 ? ((ppt - collectr) / collectr * 100) : 0;
    const absDelta = Math.abs(delta);
    const significant = absDelta > 10;
    const deltaColor = significant ? (delta > 0 ? 'var(--green)' : 'var(--red)') : 'var(--text-dim)';
    const deltaIcon = delta > 0 ? '▲' : delta < 0 ? '▼' : '—';

    panel.innerHTML = `
        ${pptProductName ? `<div style="margin-bottom:10px; font-size:0.85rem; color:var(--text-dim);">PPT match: <strong style="color:var(--text);">${pptProductName}</strong></div>` : ''}
        
        <div style="display:grid; grid-template-columns: 1fr auto 1fr; gap:12px; align-items:center; margin:12px 0;">
            <!-- Collectr price -->
            <div style="background:var(--surface-2); border:2px solid var(--border); border-radius:8px; padding:14px; text-align:center; cursor:pointer; transition:all 0.15s;"
                 onclick="selectPrice('collectr')" id="price-option-collectr">
                <div style="font-size:0.75rem; color:var(--text-dim); font-weight:600; text-transform:uppercase;">Collectr</div>
                <div style="font-size:1.4rem; font-weight:700; margin:4px 0;">$${collectr.toFixed(2)}</div>
                <div style="font-size:0.75rem; color:var(--text-dim);">From CSV import</div>
            </div>

            <!-- Delta -->
            <div style="text-align:center;">
                <div style="font-size:1.1rem; font-weight:700; color:${deltaColor};">
                    ${deltaIcon} ${absDelta.toFixed(1)}%
                </div>
                ${significant ? `<div style="font-size:0.7rem; color:${deltaColor}; font-weight:600;">⚠ SIGNIFICANT</div>` : ''}
            </div>

            <!-- PPT price -->
            <div style="background:var(--surface-2); border:2px solid var(--border); border-radius:8px; padding:14px; text-align:center; cursor:pointer; transition:all 0.15s;"
                 onclick="selectPrice('ppt')" id="price-option-ppt">
                <div style="font-size:0.75rem; color:var(--text-dim); font-weight:600; text-transform:uppercase;">${pptSourceLabel}</div>
                <div style="font-size:1.4rem; font-weight:700; margin:4px 0;">${ppt > 0 ? '$' + ppt.toFixed(2) : 'N/A'}</div>
                <div style="font-size:0.75rem; color:var(--text-dim);">${({Cache:'Scrydex nightly cache', Scrydex:'Scrydex live', PPT:'PPT (fallback)'})[pptSourceLabel] || 'Scrydex'}</div>
            </div>
        </div>

        <!-- Custom price option -->
        <div style="display:flex; gap:8px; align-items:center; margin-bottom:12px;">
            <span style="font-size:0.85rem; color:var(--text-dim);">Or custom:</span>
            <input type="number" id="custom-price" placeholder="Enter price" step="0.01" style="width:120px;" 
                   onfocus="selectPrice('custom')">
            <button class="btn btn-sm btn-secondary" onclick="selectPrice('custom')" id="price-option-custom-btn" 
                    style="opacity:0.5;">Custom</button>
        </div>

        <!-- Confirm button -->
        <button class="btn btn-primary" style="width:100%;" id="confirm-link-btn" data-ppt-price="${ppt}" data-price-source-label="${pptSourceLabel}" onclick="confirmSelectedLink(${ppt})">
            Select a price above, then confirm
        </button>
    `;

    // Default selection: PPT if available and significant diff, otherwise Collectr
    if (ppt > 0 && significant) {
        selectPrice('ppt');
    } else {
        selectPrice('collectr');
    }
}

let selectedPriceSource = null;

function selectPrice(source) {
    selectedPriceSource = source;
    // Update visual selection
    ['collectr', 'ppt'].forEach(s => {
        const el = document.getElementById('price-option-' + s);
        if (el) {
            el.style.borderColor = s === source ? 'var(--accent)' : 'var(--border)';
            el.style.background = s === source ? 'rgba(79,125,249,0.08)' : 'var(--surface-2)';
        }
    });
    const customBtn = document.getElementById('price-option-custom-btn');
    if (customBtn) customBtn.style.opacity = source === 'custom' ? '1' : '0.5';

    // Update confirm button text
    const btn = document.getElementById('confirm-link-btn');
    if (btn) {
        if (source === 'collectr') {
            btn.textContent = `Confirm Link — Use Collectr Price ($${currentMapCollectrPrice.toFixed(2)})`;
        } else if (source === 'ppt') {
            const pptVal = btn.dataset.pptPrice || '0';
            const lbl = btn.dataset.priceSourceLabel || 'Scrydex';
            btn.textContent = `Confirm Link — Use ${lbl} Price ($${parseFloat(pptVal).toFixed(2)})`;
        } else {
            btn.textContent = 'Confirm Link — Use Custom Price';
        }
    }
}

async function confirmSelectedLink(pptPrice) {
    let finalPrice;
    if (selectedPriceSource === 'collectr') {
        finalPrice = currentMapCollectrPrice;
    } else if (selectedPriceSource === 'ppt') {
        finalPrice = pptPrice;
    } else if (selectedPriceSource === 'custom') {
        finalPrice = parseFloat(document.getElementById('custom-price').value);
        if (isNaN(finalPrice) || finalPrice <= 0) { alert('Enter a valid custom price'); return; }
    } else {
        alert('Select a price first'); return;
    }
    await confirmLink(finalPrice);
}

async function confirmLink(price) {
    const tcgId = document.getElementById('map-tcgplayer-id').value;
    const body = {
        item_id: currentMapItemId,
        verify_price: false,
        override_price: price,
        session_id: currentSessionId,
    };
    // tcg ID takes precedence; scrydex_id is the fallback for Scrydex-only
    // products (sealed JP, etc.) where no TCG mapping exists.
    if (tcgId) {
        body.tcgplayer_id = parseInt(tcgId);
    } else if (currentMapScrydexId) {
        body.scrydex_id = currentMapScrydexId;
    } else {
        alert('Enter a TCGPlayer ID or pick a Scrydex result'); return;
    }
    try {
        const r = await fetch('/api/intake/map-item', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        closeModal('mapping-modal');
        if (d.siblings_linked > 0) {
            const toast = document.createElement('div');
            toast.style.cssText = 'position:fixed;bottom:24px;right:24px;background:var(--green);color:#000;padding:10px 18px;border-radius:8px;font-size:0.85rem;font-weight:600;z-index:9999;';
            toast.textContent = `✓ Also linked ${d.siblings_linked} duplicate${d.siblings_linked>1?'s':''} automatically`;
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 3500);
        }
        viewSession(currentSessionId, true);
    } catch(err) { alert(err.message); }
}

function pickScrydexResult(scrydexId, name, setName) {
    // Scrydex-only result selected. scrydex_id is the canonical linker —
    // tcgplayer_id stays NULL on this item until the price_updater (or a
    // future Scrydex sync that picks up a TCG mapping) populates it.
    // Skip the PPT comparison panel and link directly with the Collectr
    // price as a sensible default (user can still type a TCG ID above to
    // override and use the compare flow).
    currentMapScrydexId = scrydexId;
    const panel = document.getElementById('price-comparison');
    panel.style.display = 'block';
    panel.innerHTML = `
        <div style="background:rgba(192,132,252,0.08); border:1px solid var(--accent2,#c084fc); border-radius:8px; padding:14px; margin-top:12px;">
            <div style="font-size:0.85rem; font-weight:600; margin-bottom:6px;">
                Scrydex match: <span style="color:var(--accent2,#c084fc);">${name}</span>
            </div>
            <div style="font-size:0.78rem; color:var(--text-dim); margin-bottom:10px;">
                ${setName} · SDX <code>${scrydexId}</code> · No TCG mapping in Scrydex's data — linking by scrydex_id instead. price_updater can crawl TCGplayer once a TCG ID is added later.
            </div>
            <button class="btn btn-primary" style="width:100%;"
                    onclick="confirmLink(${currentMapCollectrPrice})">
                Link with Collectr Price ($${currentMapCollectrPrice.toFixed(2)})
            </button>
        </div>`;
}

async function fuzzySearch() {
    const searchQuery = (document.getElementById('map-search-query').value || '').trim();
    if (!searchQuery) { return; }
    const container = document.getElementById('fuzzy-results');
    container.innerHTML = '<div class="loading"><span class="spinner"></span> Searching prices...</div>';

    // Endpoint name says "ppt" for legacy reasons but it's cache-first
    // (Scrydex price_cache across multi-TCG) with PPT as live fallback.
    const endpoint = currentMapProductType === 'raw' ? '/api/search/cards' : '/api/search/sealed';
    const body = { query: searchQuery };

    try {
        const r = await fetch(endpoint, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) { container.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }

        let results = d.results || [];

        // Filter out "Code Card" results for sealed — those are almost never what stores want
        if (currentMapProductType !== 'raw') {
            const filtered = results.filter(p => {
                const name = (p.name || p.productName || '').toLowerCase();
                return !name.startsWith('code card');
            });
            if (filtered.length > 0) results = filtered;
        }

        if (!results.length) {
            container.innerHTML = `
                <div style="background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:14px;margin-top:8px;">
                    <div style="font-size:0.85rem;color:var(--amber);font-weight:600;margin-bottom:10px;">⚠ No match found for "${searchQuery}"</div>
                    <div style="font-size:0.8rem;color:var(--text-dim);margin-bottom:12px;">You can link to a store product instead, or accept the Collectr price with no TCGPlayer link.</div>
                    <div style="display:flex;gap:8px;flex-wrap:wrap;">
                        <button class="btn btn-secondary" onclick="mappingSearchStore()">🏪 Search Store Products</button>
                        <button class="btn btn-secondary" onclick="mappingAcceptPrice()">✓ Accept Collectr Price ($${currentMapCollectrPrice.toFixed(2)})</button>
                    </div>
                </div>`;
            return;
        }

        // Cards: hand off to the shared card-search renderer so the
        // Relink-via-mapping flow gets the same images, set/printing
        // badges, and variant chips that Add Card uses. onPick stuffs
        // the picked variant's tcgPlayer ID into the link input.
        if (currentMapProductType === 'raw') {
            _renderCardSearchResults(results, container, {
                condition: 'NM', qty: 1, offerPct: null,
                onPick: (pick) => {
                    document.getElementById('map-tcgplayer-id').value = pick.tcgId || '';
                    const inp = document.getElementById('map-tcgplayer-id');
                    if (inp) { inp.style.outline = '2px solid var(--accent)'; setTimeout(() => inp.style.outline = '', 600); }
                },
            });
            return;
        }

        // Sealed: enrich the existing rows with product images so the
        // mapping modal stops looking like a 2008 PPT search dump.
        container.innerHTML = results.map(p => {
            const tcgId = p.tcgPlayerId || p.tcgplayer_id || p.tcgPlayerID || '';
            const sid = p.scrydexId || p.scrydex_id || '';
            const price = p.unopenedPrice || p.prices?.market || p.market_price || p.price || '';
            const name = p.name || p.productName || '?';
            const pSetName = p.setName || p.set || '';
            const img = p.imageCdnUrl400 || p.imageCdnUrl || p.imageCdnUrl800 || p.image_url || '';
            const _esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
            const imgHtml = img
                ? `<img src="${_esc(img)}" loading="lazy" style="width:72px; height:96px; object-fit:contain; border-radius:6px; flex-shrink:0; background:var(--surface-2);">`
                : `<div style="width:72px; height:96px; background:var(--surface-2); border-radius:6px; flex-shrink:0;"></div>`;
            const priceText = price ? `<span style="font-weight:700; font-size:0.95rem;">$${Number(price).toFixed(2)}</span>` : '';

            if (!tcgId) {
                if (!sid) {
                    return `<div class="search-result" style="display:flex; gap:12px; align-items:center; opacity:0.55; cursor:not-allowed; border-style:dashed;">
                        ${imgHtml}
                        <div style="flex:1; min-width:0;">
                            <div style="font-weight:700;">${_esc(name)}</div>
                            <div style="font-size:0.8rem; color:var(--text-dim);">${_esc(pSetName)} ${priceText ? '· ' + priceText : ''}</div>
                            <div style="font-size:0.72rem; color:var(--text-dim);">No identifier available — not linkable</div>
                        </div>
                    </div>`;
                }
                return `<div class="search-result" style="display:flex; gap:12px; align-items:center; border:1px solid var(--accent2,#c084fc); background:rgba(192,132,252,0.04); cursor:pointer;"
                         onclick="pickScrydexResult('${sid}', '${name.replace(/'/g, "\\'")}', '${pSetName.replace(/'/g, "\\'")}')">
                    ${imgHtml}
                    <div style="flex:1; min-width:0;">
                        <div style="font-weight:700;">${_esc(name)}</div>
                        <div style="font-size:0.8rem; color:var(--text-dim);">${_esc(pSetName)} ${priceText ? '· ' + priceText : ''}</div>
                        <div style="font-size:0.72rem; color:var(--accent2,#c084fc);">Scrydex ID: ${sid} — click to link (no TCG mapping needed)</div>
                    </div>
                </div>`;
            }
            return `<div class="search-result" style="display:flex; gap:12px; align-items:center; cursor:pointer;"
                     onclick="document.getElementById('map-tcgplayer-id').value='${tcgId}'; this.style.outline='2px solid var(--accent)';">
                ${imgHtml}
                <div style="flex:1; min-width:0;">
                    <div style="font-weight:700;">${_esc(name)}</div>
                    <div style="font-size:0.8rem; color:var(--text-dim);">${_esc(pSetName)} ${priceText ? '· ' + priceText : ''}</div>
                    <div style="font-size:0.72rem; color:var(--accent);">TCGPlayer ID: ${tcgId} — click to use</div>
                </div>
            </div>`;
        }).join('');
    } catch(err) {
        container.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

// ═══════════════════════════════ MAPPING FALLBACKS ═══════════════════════════════

async function mappingSearchStore() {
    const productName = document.getElementById('map-product-name').textContent;
    const container = document.getElementById('fuzzy-results');
    container.innerHTML = '<div class="loading"><span class="spinner"></span> Searching store...</div>';

    try {
        const r = await fetch('/api/store/search?q=' + encodeURIComponent(productName));
        const d = await r.json();
        const results = d.results || [];

        if (!results.length) {
            container.innerHTML = `
                <div style="background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:14px;margin-top:8px;">
                    <div style="font-size:0.85rem;color:var(--amber);font-weight:600;margin-bottom:10px;">⚠ No store products found either</div>
                    <button class="btn btn-secondary" onclick="mappingAcceptPrice()">✓ Accept Collectr Price ($${currentMapCollectrPrice.toFixed(2)})</button>
                </div>`;
            return;
        }

        // Build HTML with data-index refs — never put product title/id in onclick attrs (breaks on quotes)
        window._storeMatchResults = results;
        const collectrPrice = currentMapCollectrPrice;
        container.innerHTML = `
            <div style="font-size:0.8rem;color:var(--text-dim);margin:8px 0 6px;">Select a store product to use its price:</div>
            ${results.map((p, idx) => {
                const price = p.shopify_price || 0;
                const title = p.title || '?';
                const qty = p.shopify_qty != null ? ` · qty ${p.shopify_qty}` : '';
                const priceStr = price ? '$' + parseFloat(price).toFixed(2) : '—';
                return `<div class="search-result" data-smi="${idx}">
                    <h4>${title}</h4>
                    <p>Store price: <strong>${priceStr}</strong>${qty}</p>
                    <div style="display:flex;gap:6px;margin-top:6px;">
                        <button class="btn btn-primary btn-sm" style="font-size:0.72rem;" data-smi="${idx}" data-use="store">Use Store Price (${priceStr})</button>
                        <button class="btn btn-secondary btn-sm" style="font-size:0.72rem;" data-smi="${idx}" data-use="collectr">Use Collectr Price ($${collectrPrice.toFixed(2)})</button>
                    </div>
                </div>`;
            }).join('')}
            <div style="margin-top:10px;padding-top:10px;border-top:1px solid var(--border);">
                <button class="btn btn-secondary btn-sm" id="store-match-none">✓ None of these — Accept Collectr Price ($${collectrPrice.toFixed(2)})</button>
            </div>`;
        // Attach event listeners — safe from quote-in-title issues
        container.querySelectorAll('[data-smi]').forEach(el => {
            el.addEventListener('click', (e) => {
                const idx = parseInt(el.dataset.smi);
                const p = window._storeMatchResults[idx];
                const pid = p.shopify_product_id || p.id || '';
                const title = p.title || '?';
                const storePrice = parseFloat(p.shopify_price) || 0;
                const tcgId = p.tcgplayer_id ? parseInt(p.tcgplayer_id) : null;
                const useStore = !el.dataset.use || el.dataset.use === 'store';
                const price = useStore ? storePrice : collectrPrice;
                e.stopPropagation();
                mappingLinkStore(pid, title, price, tcgId);
            });
        });
        document.getElementById('store-match-none')?.addEventListener('click', mappingAcceptPrice);
    } catch(err) {
        container.innerHTML = `
            <div style="background:var(--surface-2);border:1px solid var(--border);border-radius:8px;padding:14px;margin-top:8px;">
                <div style="font-size:0.85rem;color:var(--red);margin-bottom:10px;">Store search unavailable: ${err.message}</div>
                <button class="btn btn-secondary" onclick="mappingAcceptPrice()">✓ Accept Collectr Price ($${currentMapCollectrPrice.toFixed(2)})</button>
            </div>`;
    }
}

async function mappingLinkStore(storeProductId, storeProductName, storePrice, tcgplayerId) {
    // Use store price if available, otherwise fall back to collectr price
    const price = storePrice > 0 ? storePrice : currentMapCollectrPrice;
    const container = document.getElementById('fuzzy-results');
    container.innerHTML = '<div class="loading"><span class="spinner"></span> Saving...</div>';
    try {
        const body = {
            session_id: currentSessionId,
            override_price: price,
            store_product_id: storeProductId,
            store_product_name: storeProductName,
        };
        if (tcgplayerId) body.tcgplayer_id = tcgplayerId;
        const r = await fetch(`/api/intake/item/${currentMapItemId}/accept-price`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) { container.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }
        closeModal('mapping-modal');
        viewSession(currentSessionId, true);
    } catch(err) {
        container.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

async function mappingAcceptPrice() {
    // Accept the Collectr price as-is, mark as mapped.
    // If a TCGPlayer ID was manually typed, pass it along so it gets saved.
    const price = currentMapCollectrPrice;
    const manualTcgId = document.getElementById('map-tcgplayer-id').value.trim();
    const container = document.getElementById('fuzzy-results');
    container.innerHTML = '<div class="loading"><span class="spinner"></span> Saving...</div>';
    try {
        const body = {
            session_id: currentSessionId,
            override_price: price,
        };
        if (manualTcgId && parseInt(manualTcgId)) body.tcgplayer_id = parseInt(manualTcgId);
        const r = await fetch(`/api/intake/item/${currentMapItemId}/accept-price`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) { container.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }
        closeModal('mapping-modal');
        viewSession(currentSessionId, true);
    } catch(err) {
        container.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

// ═══════════════════════════════ EDIT OFFER % ═══════════════════════════════
async function editOfferPct(sessionId, currentPct) {
    const newPct = await themedPrompt({
        title: 'Edit Offer Percentage',
        message: `Current offer: ${currentPct}%`,
        inputs: [{ type: 'number', label: 'New offer percentage', default: String(currentPct), min: 1, max: 100, step: '0.5' }],
        confirmText: 'Update',
    });
    if (newPct === null || newPct === '') return;
    const val = parseFloat(newPct);
    if (isNaN(val) || val <= 0 || val > 100) { alert('Enter a valid percentage (1-100)'); return; }

    try {
        const r = await fetch(`/api/intake/session/${sessionId}/offer-percentage`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ offer_percentage: val }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function editOfferTotal(sessionId, currentTotal, marketValue) {
    const rounded = Math.round(currentTotal);
    const newTotal = await themedPrompt({
        title: 'Edit Offer Total',
        message: `Market value: $${marketValue.toFixed(2)}. Enter the round number you want to offer.`,
        inputs: [{ type: 'number', label: 'Offer total ($)', default: String(rounded), min: 1, step: '1' }],
        confirmText: 'Update',
    });
    if (newTotal === null || newTotal === '') return;
    const val = parseFloat(newTotal);
    if (isNaN(val) || val <= 0) { alert('Enter a valid dollar amount'); return; }
    if (marketValue <= 0) { alert('Market value is 0 — cannot calculate percentage'); return; }

    // Back-calculate percentage from desired total
    const newPct = Math.round((val / marketValue) * 10000) / 100; // 2 decimal places
    if (newPct > 100) {
        if (!confirm(`This offer ($${val}) is ${newPct.toFixed(1)}% of market value — more than 100%. Continue?`)) return;
    }

    try {
        const r = await fetch(`/api/intake/session/${sessionId}/offer-percentage`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ offer_percentage: newPct }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

// ═══════════════════════════════ REFRESH PRICES ═══════════════════════════════
// Accumulated PPT price results across batches
let _pptResults = {};  // keyed by item_id
let _pptAllComps = []; // ordered list from last response
let _pptMeta = {};     // total_unique, complete, etc
let _pptFilter = 'all';
let _pptSessionId = null;

async function refreshPrices(sessionId, offset) {
    offset = offset || 0;
    const panel = document.getElementById('price-refresh-results');
    if (!panel) return;

    // Reset accumulator on first batch
    if (offset === 0) {
        _pptResults = {};
        panel.innerHTML = '<div class="loading"><span class="spinner"></span> Fetching PPT prices...</div>';
    }

    try {
        const r = await fetch(`/api/intake/session/${sessionId}/refresh-prices`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ offset: offset }),
        });
        const d = await r.json();
        if (!r.ok) { panel.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }

        const comps = d.comparisons || [];
        if (!comps.length && offset === 0) {
            panel.innerHTML = '<div class="alert alert-warning">No linked items to refresh.</div>';
            return;
        }

        // Merge this batch's fetched results into accumulator
        for (const c of comps) {
            if (c.fetched) {
                _pptResults[c.item_id] = c;
            } else if (!_pptResults[c.item_id]) {
                _pptResults[c.item_id] = c;
            }
        }

        _pptAllComps = comps;
        _pptSessionId = sessionId;
        _pptMeta = {
            total_unique: d.total_unique || 0,
            complete: d.complete,
            rate_limited: d.rate_limited,
            retry_after: d.retry_after,
            next_offset: d.next_offset,
        };
        renderPptResults();

        // Auto-continue after rate limit delay
        if (d.rate_limited && d.retry_after && !d.complete) {
            let wait = d.retry_after + 5;
            const countdownEl = document.getElementById('ppt-wait');
            if (countdownEl) countdownEl.textContent = wait;
            const countdown = setInterval(() => {
                wait--;
                if (countdownEl) countdownEl.textContent = Math.max(0, wait);
                if (wait <= 0) {
                    clearInterval(countdown);
                    refreshPrices(sessionId, d.next_offset);
                }
            }, 1000);
        }
    } catch(err) {
        panel.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

function renderPptResults() {
    const panel = document.getElementById('price-refresh-results');
    if (!panel || !_pptAllComps.length) return;
    const sessionId = _pptSessionId;
    const m = _pptMeta;

    const allItems = _pptAllComps.map(c => _pptResults[c.item_id] || c);
    const fetchedItems = allItems.filter(c => c.fetched);
    const totalUnique = m.total_unique || 0;
    const fetchedCount = fetchedItems.length;
    const pct = totalUnique > 0 ? Math.round(fetchedCount / totalUnique * 100) : 0;

    // Stats
    const sigItems = fetchedItems.filter(c => c.significant);
    const failItems = fetchedItems.filter(c => c.ppt_market == null && c.error);
    const pptHigher = fetchedItems.filter(c => c.delta_pct != null && c.delta_pct > 10);
    const pptLower = fetchedItems.filter(c => c.delta_pct != null && c.delta_pct < -10);
    const totalCollectr = fetchedItems.reduce((s, c) => s + (c.collectr_price || 0) * (c.quantity || 1), 0);
    const totalPpt = fetchedItems.reduce((s, c) => s + (c.ppt_market != null ? c.ppt_market : (c.collectr_price || 0)) * (c.quantity || 1), 0);

    let statusHtml = '';
    if (m.rate_limited && m.retry_after && !m.complete) {
        statusHtml = `<div class="alert alert-warning" id="ppt-countdown">
            ⏳ Pausing for rate limit — auto-continuing in <strong id="ppt-wait">${m.retry_after}</strong>s
            (${fetchedCount}/${totalUnique} unique products fetched)
        </div>`;
    } else if (m.complete) {
        statusHtml = '';  // stats below replace this
    } else {
        statusHtml = `<div class="alert" style="background:var(--surface-2);">
            <div style="display:flex; align-items:center; gap:12px;">
                <span class="spinner"></span> Fetching... ${fetchedCount}/${totalUnique} (${pct}%)
            </div>
        </div>`;
    }

    const progressHtml = `<div style="height:4px;background:var(--surface-2);border-radius:2px;margin-bottom:12px;overflow:hidden;">
        <div style="height:100%;width:${pct}%;background:var(--accent);transition:width 0.3s;"></div>
    </div>`;

    // Stats row (only show when we have data)
    const statsHtml = fetchedCount > 0 ? `
        <div class="stats" style="margin-bottom:12px;">
            <div class="stat"><div class="stat-label">Fetched</div><div class="stat-value">${fetchedCount}/${totalUnique}</div></div>
            <div class="stat"><div class="stat-label">Current Total</div><div class="stat-value">$${totalCollectr.toFixed(0)}</div></div>
            <div class="stat"><div class="stat-label">PPT Total</div><div class="stat-value">$${totalPpt.toFixed(0)}</div></div>
            <div class="stat"><div class="stat-label">>10% Off</div><div class="stat-value" style="color:${sigItems.length > 0 ? 'var(--amber)' : 'var(--green)'};">${sigItems.length}</div></div>
            ${failItems.length > 0 ? `<div class="stat"><div class="stat-label">Errors</div><div class="stat-value" style="color:var(--red);">${failItems.length}</div></div>` : ''}
        </div>
    ` : '';

    // Filter buttons
    const pptFilterBtn = (id, label, count) => {
        const active = _pptFilter === id;
        return `<button class="btn btn-sm ${active ? 'btn-primary' : 'btn-secondary'}" style="font-size:0.75rem;padding:3px 8px;" onclick="_pptFilter='${id}'; renderPptResults();">${label} (${count})</button>`;
    };
    const filterHtml = fetchedCount > 0 ? `
        <div style="display:flex; gap:4px; flex-wrap:wrap; margin-bottom:12px;">
            ${pptFilterBtn('all', 'All', fetchedItems.length)}
            ${pptFilterBtn('sig', '>10% Off', sigItems.length)}
            ${pptFilterBtn('ppt_higher', 'PPT Higher', pptHigher.length)}
            ${pptFilterBtn('ppt_lower', 'PPT Lower', pptLower.length)}
            ${pptFilterBtn('errors', 'Errors', failItems.length)}
        </div>
    ` : '';

    // Apply filter
    let filtered = fetchedItems;
    if (_pptFilter === 'sig') filtered = sigItems;
    else if (_pptFilter === 'ppt_higher') filtered = pptHigher;
    else if (_pptFilter === 'ppt_lower') filtered = pptLower;
    else if (_pptFilter === 'errors') filtered = failItems;

    const tableHtml = filtered.length ? `
        <div style="overflow-x:auto; margin-bottom:16px;">
        <table>
            <thead><tr>
                <th>Product</th><th>Qty</th><th>Current</th><th>PPT Price</th><th>PPT Low</th><th>Delta</th><th></th>
            </tr></thead>
            <tbody>${filtered.map(c => {
                const delta = c.delta_pct;
                const sig = c.significant;
                const hasError = c.error && c.ppt_market == null;
                const deltaColor = sig ? (delta > 0 ? 'var(--green)' : 'var(--red)') : 'var(--text-dim)';
                const arrow = delta > 0 ? '▲' : delta < 0 ? '▼' : '—';
                return `<tr style="${hasError ? 'background:rgba(255,60,60,0.06);' : sig ? 'background:rgba(255,180,0,0.06);' : ''}">
                    <td>${c.product_name}
                        ${c.is_graded && c.grade_label ? `<br><small><span style="background:linear-gradient(135deg,#7c3aed,#4f7df9);color:#fff;border-radius:4px;padding:1px 5px;font-size:0.7rem;font-weight:700;">${c.grade_label}</span> <span style="color:var(--text-dim);font-size:0.72rem;">eBay market</span></small>` : (c.condition ? `<br><small style="color:var(--text-dim);">${c.condition}</small>` : '')}
                        <br><small style="color:var(--text-dim);">TCG#${c.tcgplayer_id}</small>
                        ${c.ppt_name && c.ppt_name !== c.product_name ? '<br><small style="color:var(--amber);">⚠ PPT: ' + c.ppt_name + '</small>' : ''}
                        ${hasError ? '<br><small style="color:var(--red);">✕ ' + c.error + '</small>' : ''}
                    </td>
                    <td>${c.quantity || 1}</td>
                    <td>$${c.collectr_price.toFixed(2)}</td>
                    <td style="${sig ? 'font-weight:700; color:var(--accent);' : ''}">
                        ${c.ppt_market != null ? '$' + c.ppt_market.toFixed(2) + (c.price_source ? ' <small style="color:var(--text-dim);font-size:0.65rem;opacity:0.7;" title="Price source: ' + c.price_source + '">' + ({cache:'⚡cache',ppt:'PPT',scrydex:'SDX'}[c.price_source]||c.price_source) + '</small>' : '') : hasError ? '<span style="color:var(--red);">Error</span>' : '—'}
                    </td>
                    <td>${c.ppt_low != null ? '$' + c.ppt_low.toFixed(2) : c.is_graded ? '<span style="color:var(--text-dim);font-size:0.75rem;">n/a</span>' : '—'}</td>
                    <td style="color:${deltaColor}; font-weight:${sig ? '700' : '400'};">
                        ${delta != null ? arrow + ' ' + Math.abs(delta).toFixed(1) + '%' : '—'}
                    </td>
                    <td>${c.ppt_market != null ? '<button class="btn btn-sm btn-secondary" style="font-size:0.7rem;padding:2px 6px;" onclick="applyPptPrice(\'' + c.item_id + '\', \'' + sessionId + '\', ' + c.ppt_market + ', this)">Apply</button>' : ''}</td>
                </tr>`;
            }).join('')}</tbody>
        </table>
        </div>
    ` : (fetchedCount > 0 ? '<p style="color:var(--text-dim); text-align:center; padding:20px;">No items match this filter.</p>' : '');

    panel.innerHTML = statusHtml + progressHtml + statsHtml + filterHtml + tableHtml;
}

async function applyPptPrice(itemId, sessionId, pptPrice, btn) {
    try {
        // Disable button immediately
        btn.disabled = true;
        btn.textContent = '...';

        const r = await fetch('/api/intake/update-item-price', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ item_id: itemId, session_id: sessionId, new_price: pptPrice }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); btn.disabled = false; btn.textContent = 'Apply'; return; }

        // Mark this row as applied visually
        const row = btn.closest('tr');
        if (row) {
            row.style.background = 'rgba(0,200,100,0.06)';
            const cells = row.querySelectorAll('td');
            // Update Current price column
            cells[1].innerHTML = `<span style="color:var(--green); font-weight:700;">$${pptPrice.toFixed(2)}</span>`;
            // Update Delta column
            cells[4].innerHTML = `<span style="color:var(--green);">✓ Applied</span>`;
            // Replace button
            cells[5].innerHTML = '<span class="badge badge-green">Applied</span>';
        }

        // Update session stats at top of modal (market value + offer total)
        try {
            const sr = await fetch(`/api/intake/session/${sessionId}`);
            const sd = await sr.json();
            const s = sd.session;
            const mvEl = document.getElementById('market-value-display');
            if (mvEl) mvEl.textContent = '$' + (s.total_market_value || 0).toFixed(2);
            const otEl = document.getElementById('offer-total-display');
            if (otEl) otEl.textContent = '$' + (s.total_offer_amount || 0).toFixed(2);
        } catch(e) { /* stats update is best-effort */ }
    } catch(err) { alert(err.message); }
}

// ═══════════════════════════════ SHOPIFY STORE CHECK ═══════════════════════════════

// Store check state for filtering
let _storeCheckData = null;
let _storeFilter = 'all';

async function storeCheck(sessionId) {
    const panel = document.getElementById('store-check-results');
    if (!panel) return;
    const _savedScroll = window.scrollY;
    panel.innerHTML = '<div class="loading"><span class="spinner"></span> Checking store inventory...</div>';

    try {
        const r = await fetch(`/api/shopify/session/${sessionId}/store-check`);
        const d = await r.json();
        if (!r.ok) {
            if (r.status === 503) {
                panel.innerHTML = `<div class="alert alert-warning">Shopify not configured. Set SHOPIFY_TOKEN and SHOPIFY_STORE env vars.</div>`;
            } else {
                panel.innerHTML = `<div class="alert alert-error">${d.error}</div>`;
            }
            return;
        }

        // Backend returns item_id; normalize to id for consistency with rest of frontend
        _storeCheckData = { items: (d.items || []).map(i => ({...i, id: i.id || i.item_id})), sessionId: sessionId };
        _storeFilter = 'all';
        renderStoreCheck();
        // Restore scroll after store check re-render
        if (_savedScroll) {
            window.scrollTo(0, _savedScroll);
            setTimeout(() => { window.scrollTo(0, _savedScroll); }, 50);
        }

        // If cache is currently refreshing in the background, poll and auto-reload when done
        _pollForCacheRefresh(sessionId);
    } catch(err) {
        panel.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

let _cacheRefreshPoller = null;
function _pollForCacheRefresh(sessionId) {
    if (_cacheRefreshPoller) clearInterval(_cacheRefreshPoller);
    _cacheRefreshPoller = setInterval(async () => {
        try {
            const r = await fetch('/api/cache/status');
            const d = await r.json();
            const label = document.getElementById('cache-age-label');
            if (d.refresh_in_progress) {
                if (label) label.innerHTML = '<span style="color:var(--accent);">⟳ refreshing...</span>';
            } else {
                // Refresh just completed — reload store check data
                clearInterval(_cacheRefreshPoller);
                _cacheRefreshPoller = null;
                if (label) label.innerHTML = '<span style="color:#22c55e;">✓ fresh</span>';
                // Re-run store check silently to get updated data
                const r2 = await fetch(`/api/shopify/session/${sessionId}/store-check`);
                const d2 = await r2.json();
                if (r2.ok) {
                    _storeCheckData = { items: (d2.items || []).map(i => ({...i, id: i.id || i.item_id})), sessionId };
                    renderStoreCheck();
                }
            }
        } catch(e) { clearInterval(_cacheRefreshPoller); _cacheRefreshPoller = null; }
    }, 3000);
    // Stop polling after 3 minutes regardless
    setTimeout(() => { if (_cacheRefreshPoller) { clearInterval(_cacheRefreshPoller); _cacheRefreshPoller = null; } }, 180000);
}

function renderStoreCheck() {
    const panel = document.getElementById('store-check-results');
    if (!panel || !_storeCheckData) return;
    // Preserve scroll position of the session modal
    const _savedScroll = window.scrollY;
    const items = _storeCheckData.items;
    const sessionId = _storeCheckData.sessionId;

    if (!items.length) {
        panel.innerHTML = '<div class="alert alert-warning">No mapped items to check.</div>';
        return;
    }

    // Compute per-item unit offer and margin
    const enriched = items.map(i => {
        const unitOffer = i.quantity > 0 ? (i.offer_price || 0) / i.quantity : 0;
        const hasStorePrice = i.in_store && i.store_price != null && i.store_price > 0;
        const margin = hasStorePrice && unitOffer > 0
            ? ((i.store_price - unitOffer) / i.store_price * 100)
            : null;
        const bd = i.breakdown;
        // Resolve breakdown values via shared helper — honors claimed_variant_id
        // (locked: that variant's value), probabilistic recipes (avg), or
        // selectable recipes (max). Falls back to best_* for old-style payloads.
        let bdMktVal = 0, bdStoreVal = 0;
        if (bd) {
            if (typeof resolveBreakdownAggregate === 'function') {
                const agg = resolveBreakdownAggregate(bd, i.claimed_variant_id || null);
                bdMktVal = agg.market || 0;
                bdStoreVal = agg.store != null ? agg.store : 0;
            } else {
                bdMktVal = parseFloat(bd.best_variant_market) || 0;
                bdStoreVal = parseFloat(bd.best_variant_store) || 0;
            }
        }
        const bdVal = bdStoreVal || bdMktVal;
        // effectiveSellPrice: best available sell price for this item
        // If no store price (null/0), treat same as not-in-store — use bd or estimate
        const sealedPrice = hasStorePrice ? i.store_price : 0;
        const effectiveSellPrice = Math.max(sealedPrice, bdStoreVal || bdMktVal);
        const maxMargin = effectiveSellPrice > 0 && unitOffer > 0
            ? ((effectiveSellPrice - unitOffer) / effectiveSellPrice * 100) : null;
        return { ...i, unitOffer, margin, bdVal, bdMktVal, bdStoreVal, effectiveSellPrice, hasStorePrice, maxMargin };
    });

    // Stats
    const inStore = enriched.filter(i => i.in_store);
    const inStoreWithPrice = enriched.filter(i => i.hasStorePrice);
    const notInStore = enriched.filter(i => !i.in_store);
    const noPrice = enriched.filter(i => i.in_store && !i.hasStorePrice);
    const zeroQty = inStore.filter(i => i.store_qty === 0);
    const withMargin = inStoreWithPrice.filter(i => i.margin !== null);
    const withMaxMargin = enriched.filter(i => i.maxMargin !== null);

    const totalOfferValue = enriched.reduce((s, i) => s + (i.offer_price || 0), 0);
    const totalCost = enriched.reduce((s, i) => s + (i.unitOffer || 0) * (i.quantity || 1), 0);
    // Store value: store_price * intake_qty for items listed in store, market_price * intake_qty for everything else
    const combinedStoreValue = enriched.reduce((s, i) => {
        const qty = i.quantity || 1;
        if (i.hasStorePrice) return s + i.store_price * qty;
        return s + (i.market_price || 0) * qty;
    }, 0);
    const totalStoreValue = inStoreWithPrice.reduce((s, i) => s + i.store_price * (i.quantity || 1), 0);
    const estimatedValue = [...notInStore, ...noPrice].reduce((s, i) => s + (i.market_price || 0) * (i.quantity || 1), 0);
    const grossMarginPct = combinedStoreValue > 0 ? ((combinedStoreValue - totalOfferValue) / combinedStoreValue * 100) : 0;
    const avgMargin = withMargin.length > 0 ? withMargin.reduce((s, i) => s + i.margin, 0) / withMargin.length : 0;

    // Max margin: best of (sell sealed, break down) per item
    const totalEffectiveSellValue = enriched.reduce((s, i) => {
        if (i.effectiveSellPrice > 0) return s + i.effectiveSellPrice * (i.quantity || 1);
        return s + (i.market_price || 0) * (i.quantity || 1);
    }, 0);
    const maxGrossMarginPct = totalEffectiveSellValue > 0 ? ((totalEffectiveSellValue - totalOfferValue) / totalEffectiveSellValue * 100) : 0;
    const itemsWithBdBoost = enriched.filter(i => i.bdVal > 0 && i.bdVal > (i.hasStorePrice ? i.store_price : 0)).length;

    // Filter
    let filtered = enriched;
    if (_storeFilter === 'not_in_store') filtered = notInStore;
    else if (_storeFilter === 'restock') filtered = zeroQty;
    else if (_storeFilter === 'negative_margin') filtered = enriched.filter(i => i.margin !== null && i.margin < 0);
    else if (_storeFilter === 'needs_listing') filtered = enriched.filter(i => i.item_status === 'damaged' && !i.damaged_variant_exists);
    else if (_storeFilter === 'has_breakdown') filtered = enriched.filter(i => i.breakdown);

    const filterBtn = (id, label, count) => {
        const active = _storeFilter === id;
        return `<button class="btn btn-sm ${active ? 'btn-primary' : 'btn-secondary'}" style="font-size:0.75rem;padding:3px 8px;" onclick="_storeFilter='${id}'; renderStoreCheck();">${label} (${count})</button>`;
    };

    panel.innerHTML = `
        <div class="stats" style="margin-bottom:12px;">
            <div class="stat"><div class="stat-label">In Store</div><div class="stat-value" style="color:var(--green);">${inStore.length}</div></div>
            <div class="stat"><div class="stat-label">Not Found</div><div class="stat-value" style="color:${notInStore.length > 0 ? 'var(--red)' : 'var(--text-dim)'};">${notInStore.length}</div></div>
            <div class="stat"><div class="stat-label">Restock (qty=0)</div><div class="stat-value">${zeroQty.length}</div></div>
            <div class="stat" title="$${totalStoreValue.toFixed(0)} listed in store + $${estimatedValue.toFixed(0)} market est. for ${[...notInStore,...noPrice].length} unlisted items"><div class="stat-label">Store Value</div><div class="stat-value" style="color:var(--green);">$${combinedStoreValue.toFixed(0)}<br><small style="font-size:0.65rem;font-weight:400;color:var(--text-dim);">${inStoreWithPrice.length} listed · ${[...notInStore,...noPrice].length} est.</small></div></div>
            <div class="stat"><div class="stat-label">Est. Gross Margin</div><div class="stat-value" style="color:${grossMarginPct > 20 ? 'var(--green)' : grossMarginPct > 10 ? 'var(--amber)' : 'var(--red)'};">${grossMarginPct.toFixed(1)}%<br><small style="font-size:0.65rem; font-weight:400; color:var(--text-dim);">$${(combinedStoreValue - totalOfferValue > 0 ? combinedStoreValue - totalOfferValue : 0).toFixed(0)} est. profit</small></div></div>
            ${itemsWithBdBoost > 0 ? `<div class="stat" title="Best-case margin if the ${itemsWithBdBoost} item${itemsWithBdBoost!==1?'s':''} where breakdown beats store price are broken down instead"><div class="stat-label">Max Margin ✦</div><div class="stat-value" style="color:var(--green);">${maxGrossMarginPct.toFixed(1)}%<br><small style="font-size:0.65rem; font-weight:400; color:var(--text-dim);">${itemsWithBdBoost} item${itemsWithBdBoost!==1?'s':''} better as bd</small></div></div>` : ''}
        </div>

        <div style="display:flex; gap:4px; flex-wrap:wrap; margin-bottom:12px;">
            ${filterBtn('all', 'All', enriched.length)}
            ${filterBtn('not_in_store', 'Not in Store', notInStore.length)}
            ${filterBtn('restock', 'Will Restock', zeroQty.length)}
            ${filterBtn('negative_margin', 'Negative Margin', enriched.filter(i => i.margin !== null && i.margin < 0).length)}
            ${filterBtn('needs_listing', 'Needs Listing', enriched.filter(i => i.item_status === 'damaged' && !i.damaged_variant_exists).length)}
            ${filterBtn('has_breakdown', 'Has Breakdown', enriched.filter(i => i.breakdown).length)}
            <span id="cache-freshness-badge" style="font-size:0.75rem; color:var(--text-dim); margin-left:auto; display:flex; align-items:center; gap:8px;">
                <span id="cache-age-label">checking cache...</span>
                <button class="btn btn-secondary btn-sm" onclick="shopifySync()" style="font-size:0.75rem;">↻ Refresh</button>
            </span>
        </div>

        <div style="overflow-x:auto;">
        <table>
            <thead><tr>
                <th>Product</th><th>Offer/Unit</th><th>Store Price</th><th>Store Qty</th><th>Store Margin</th><th>BD Margin</th><th></th>
            </tr></thead>
            <tbody>${filtered.map(i => {
                const found = i.in_store;
                const margin = i.margin;
                const marginColor = margin !== null
                    ? (margin > 30 ? 'var(--green)' : margin > 15 ? 'var(--amber)' : 'var(--red)')
                    : 'var(--text-dim)';
                const bd = i.breakdown;
                // BD Margin: store-children vs store-price (apples to apples)
                // Fall back to market-children vs store-price if no store bd, then market vs offer
                const bdStoreVal = i.bdStoreVal || 0;
                const bdMktVal = i.bdMktVal || 0;
                // BD Margin = (bd sell value - what we paid) / bd sell value
                // Mirrors Store Margin = (store_price - unitOffer) / store_price
                // Priority: store bd value > market bd value; always vs unitOffer
                let bdMargin = null;
                let bdMarginLabel = '';
                if (bd && i.unitOffer > 0) {
                    if (bdStoreVal > 0) {
                        bdMargin = (bdStoreVal - i.unitOffer) / bdStoreVal * 100;
                        bdMarginLabel = 'store bd margin (store components vs offer)';
                    } else if (bdMktVal > 0) {
                        bdMargin = (bdMktVal - i.unitOffer) / bdMktVal * 100;
                        bdMarginLabel = 'mkt bd margin (mkt components vs offer)';
                    }
                }
                const bdColor = bdMargin !== null ? (bdMargin > 30 ? 'var(--green)' : bdMargin > 15 ? 'var(--amber)' : 'var(--red)') : 'var(--text-dim)';
                const bdBetterThanStore = bd && found && (bdStoreVal || bdMktVal) > 0 && i.store_price > 0
                    ? (bdStoreVal || bdMktVal) > i.store_price : false;
                return `<tr style="">
                    <td>
                        ${i.product_name}
                        ${found && i.store_title !== i.product_name ? '<br><small style="color:var(--text-dim);">Shopify: ' + i.store_title + '</small>' : ''}
                        ${i.store_note ? '<br><small style="color:var(--amber);">&#x26A0; ' + i.store_note + '</small>' : ''}
                        ${i.item_status === 'damaged' ? '<br><span class="badge" style="background:#b45309;color:#fff;font-size:0.65rem;">DAMAGED</span>' + (!i.damaged_variant_exists ? ' <span class="badge" style="background:var(--red);color:#fff;font-size:0.65rem;">Needs Listing</span>' : '') : ''}
                        ${bd ? (() => {
                            // bdStoreVal/bdMktVal already honor claimed_variant_id and avg-for-multi-variant
                            // via resolveBreakdownAggregate in the per-item enrichment block.
                            const parentStore  = found && i.store_price != null ? i.store_price : null;
                            const childStore   = i.bdStoreVal > 0 ? i.bdStoreVal : null;
                            const childMkt     = i.bdMktVal || 0;
                            const itemMkt      = i.market_price || 0;
                            let sellSealed, sellBroken;
                            if (parentStore !== null && childStore !== null) {
                                sellSealed = parentStore; sellBroken = childStore;
                            } else if (parentStore !== null) {
                                sellSealed = parentStore; sellBroken = childMkt;
                            } else if (childStore !== null) {
                                sellSealed = itemMkt || childMkt; sellBroken = childStore;
                            } else {
                                sellSealed = itemMkt; sellBroken = childMkt;
                            }
                            const pct = sellSealed > 0 ? ((sellBroken - sellSealed) / sellSealed * 100) : 0;
                            const bdNoteColor = pct > 5 ? 'var(--green)' : pct >= -10 ? 'var(--amber)' : 'var(--red)';
                            const arrow = pct > 5 ? '▲' : pct >= -10 ? '≈' : '▼';
                            const displayVal = childStore || childMkt;
                            const valLabel = childStore ? 'store' : 'mkt';

                            // Suffix: claimed variant name OR avg + per-variant list (multi)
                            let suffix = '';
                            const _claimedV = i.claimed_variant_id && Array.isArray(bd.variants)
                                ? bd.variants.find(v => String(v.id) === String(i.claimed_variant_id)) : null;
                            if (_claimedV) {
                                suffix = ` (claimed: ${_claimedV.name || 'variant'})`;
                            } else if (bd.variant_count > 1 && Array.isArray(bd.variants)) {
                                const _vals = bd.variants
                                    .map(v => (v.store != null && v.store > 0) ? v.store : v.market)
                                    .filter(n => n > 0);
                                if (_vals.length > 1) {
                                    suffix = ` avg (${_vals.map(n => '$' + parseFloat(n).toFixed(2)).join(', ')})`;
                                }
                            }
                            const valStr = displayVal > 0 ? `${valLabel} $${displayVal.toFixed(2)}${suffix}` : '';
                            const name = (_claimedV || bd.variant_count <= 1)
                                ? (bd.variant_name || 'breakdown')
                                : `${bd.variant_count} configs`;
                            const deepVal = bd.deep_bd_store ? parseFloat(bd.deep_bd_store) : (bd.deep_bd_market ? parseFloat(bd.deep_bd_market) : 0);
                            const deepStr = deepVal > 0 ? ` &middot; <span style="color:var(--accent);" title="Store value if children are also broken down">Deep: $${deepVal.toFixed(2)}</span>` : '';
                            return `<br><small style="color:${bdNoteColor};">&#x1F4E6; ${arrow} ${name}${valStr ? ': ' + valStr : ''}${deepStr}${bd.variant_notes ? ' &middot; <em>' + bd.variant_notes + '</em>' : ''}</small>`;
                        })() : ''}
                    </td>
                    <td>$${(i.unitOffer||0).toFixed(2)}${i.quantity > 1 ? ' &times; ' + i.quantity : ''}</td>
                    <td style="${found ? 'font-weight:600;' : ''}">
                        ${found && i.store_price != null ? '$' + i.store_price.toFixed(2) : '<span style="color:var(--text-dim);">—</span>'}
                        ${!found && bd ? '<br><small style="color:var(--text-dim);">no listing</small>' : ''}
                    </td>
                    <td>
                        ${found
                            ? '<span style="font-weight:600; color:' + (i.store_qty > 0 ? 'var(--green)' : 'var(--red)') + ';">' + i.store_qty + '</span>'
                            : bd
                                ? '<span style="color:var(--amber); font-size:0.8rem;">bd</span>'
                                : '<span style="color:var(--text-dim);">—</span>'}
                    </td>
                    <td style="color:${marginColor}; font-weight:${margin !== null ? '600' : '400'};">
                        ${margin !== null ? margin.toFixed(1) + '%' : '—'}
                    </td>
                    <td style="color:${bdColor}; font-size:0.8rem;" title="${bdMarginLabel}">
                        ${bdMargin !== null ? bdMargin.toFixed(1) + '%' + (bdBetterThanStore ? ' <span style="color:var(--green); font-size:0.7rem;">&#x2191;</span>' : '') : '—'}
                    </td>
                    <td>
                        ${found && i.store_handle
                            ? '<a href="https://' + (window.__shopifyStore || '') + '/products/' + i.store_handle + '" target="_blank" class="btn btn-sm btn-secondary" style="font-size:0.7rem; padding:2px 6px;">View &#x2197;</a>'
                            : ''}
                        <button class="btn btn-sm btn-secondary" style="font-size:0.7rem; padding:2px 6px; margin-top:2px;"
                            onclick="openIntakeBreakdown(${i.tcgplayer_id||'null'}, '${(i.product_name||'').replace(/'/g,'').replace(/"/g,'')}', ${i.market_price||0})">
                            ${bd ? '&#x1F4E6; Recipe' : '+ Recipe'}
                        </button>
                        ${found && i.shopify_variant_id
                            ? '<a href="https://admin.shopify.com/store/' + (window.__shopifyStoreHandle||'') + '/products/' + i.shopify_product_id + '/variants/' + i.shopify_variant_id + '" target="_blank" class="btn btn-sm btn-secondary" style="font-size:0.7rem; padding:2px 6px; margin-top:2px;" title="Edit this variant in Shopify admin">Admin &#x2197;</a>'
                            : ''}
                        ${!i.tcgplayer_id
                            ? '<button class="btn btn-sm btn-primary" style="font-size:0.7rem; padding:2px 6px; margin-top:2px;" onclick="openMapping(\'' + i.id + '\',\'' + (i.product_name||'').replace(/'/g,"\\\\'").replace(/"/g,'&quot;') + '\',\'' + (i.set_name||'').replace(/'/g,"\\\\'").replace(/"/g,'&quot;') + '\',\'' + (i.product_type||'sealed') + '\',' + (i.market_price||0) + ')" title="Link this item to a TCGPlayer product">&#x1F517; Link</button>'
                            : ''}
                        ${i.tcgplayer_id
                            ? '<button class="btn btn-sm btn-secondary find-in-store-btn" style="font-size:0.7rem; padding:2px 6px; margin-top:2px;" data-tcg="' + i.tcgplayer_id + '" data-name="' + (i.product_name||'').replace(/"/g,'&amp;quot;') + '" data-item="' + i.id + '" data-offer="' + (i.offer_price||0) + '" data-market="' + (i.market_price||0) + '" title="Search your store cache to find this listing">&#x1F50D; Find</button>'
                            : ''}
                        ${!found && i.tcgplayer_id
                            ? '<button class="btn btn-sm btn-primary" id="cl-btn-' + i.tcgplayer_id + '" style="font-size:0.7rem; padding:2px 6px; margin-top:2px;" onclick="createListingFromStore(' + i.tcgplayer_id + ', this, \'' + i.id + '\', ' + (i.offer_price||0) + ')">+ List</button>'
                            : ''}
                    </td>
                </tr>`;
            }).join('')}</tbody>
        </table>
        </div>
    `;
    loadCacheStatus();
    // Restore scroll position after DOM reflow
    if (_savedScroll) {
        window.scrollTo(0, _savedScroll);
        setTimeout(() => { window.scrollTo(0, _savedScroll); }, 50);
    }
}

async function findInStore(btn, tcgplayerId, productName, itemId, offerPrice, marketPrice) {
    // Build a mini search modal to find an existing Shopify listing by name
    const overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:99999;display:flex;align-items:center;justify-content:center;';

    function renderFind(query, results, loading, error) {
        overlay.innerHTML = `
            <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;width:480px;max-width:95vw;">
                <div style="font-size:1rem;font-weight:700;margin-bottom:4px;">🔍 Find in Store Cache</div>
                <div style="font-size:0.8rem;color:var(--text-dim);margin-bottom:14px;">
                    Searching for: <em>${productName}</em>
                </div>
                <div style="display:flex;gap:8px;margin-bottom:12px;">
                    <input id="fis-input" type="text" value="${query}" placeholder="Search store by name…"
                        style="flex:1;font-size:0.9rem;"
                        onkeydown="if(event.key==='Enter') document.getElementById('fis-go').click()">
                    <button id="fis-go" class="btn btn-primary" style="padding:4px 12px;">Search</button>
                </div>
                <div id="fis-results" style="max-height:260px;overflow-y:auto;">
                    ${loading ? '<div class="loading"><span class="spinner"></span> Searching…</div>'
                      : error ? '<div class="alert alert-error">' + error + '</div>'
                      : results.length === 0 && query ? '<div style="color:var(--text-dim);font-size:0.85rem;">No results for "' + query + '"</div>'
                      : results.map((r, ri) => `
                        <div style="display:flex;align-items:center;gap:10px;padding:8px;border-radius:6px;border:1px solid var(--border);margin-bottom:6px;background:var(--surface-2);">
                            <div style="flex:1;min-width:0;">
                                <div style="font-size:0.85rem;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${r.title}</div>
                                <div style="font-size:0.75rem;color:var(--text-dim);">
                                    $${parseFloat(r.shopify_price||0).toFixed(2)} · qty ${r.shopify_qty??'?'}
                                    ${r.is_damaged ? ' · <span style="color:var(--red);">dmg</span>' : ''}
                                    ${r.tcgplayer_id ? ' · TCG#' + r.tcgplayer_id : ''}
                                </div>
                            </div>
                            <div style="display:flex;flex-direction:column;gap:4px;flex-shrink:0;">
                                ${itemId ? '<button class="btn btn-sm btn-primary" style="font-size:0.7rem;padding:2px 8px;" data-link-idx="' + ri + '">🔗 Link</button>' : ''}
                                ${r.shopify_variant_id
                                    ? '<a href="https://admin.shopify.com/store/' + (window.__shopifyStoreHandle||'') + '/products/' + r.shopify_product_id + '/variants/' + r.shopify_variant_id + '" target="_blank" class="btn btn-sm btn-secondary" style="font-size:0.7rem;padding:2px 8px;">Admin ↗</a>'
                                    : ''}
                                ${r.handle
                                    ? '<a href="https://' + (window.__shopifyStore||'') + '/products/' + r.handle + '" target="_blank" class="btn btn-sm btn-secondary" style="font-size:0.7rem;padding:2px 8px;">Store ↗</a>'
                                    : ''}
                            </div>
                        </div>`).join('')}
                </div>
                <div style="margin-top:12px;text-align:right;">
                    <button id="fis-cancel" class="btn btn-secondary">Close</button>
                </div>
            </div>`;

        overlay.querySelector('#fis-go').onclick = () => doSearch(overlay.querySelector('#fis-input').value.trim());
        overlay.querySelector('#fis-cancel').onclick = () => overlay.remove();
        overlay.querySelector('#fis-input')?.focus();
        if (itemId) {
            overlay.querySelectorAll('[data-link-idx]').forEach(linkBtn => {
                linkBtn.addEventListener('click', async () => {
                    const r = results[parseInt(linkBtn.dataset.linkIdx)];
                    const storePrice = parseFloat(r.shopify_price) || 0;
                    const usePrice = storePrice > 0 ? storePrice : (parseFloat(marketPrice) || parseFloat(offerPrice) || 0);
                    linkBtn.disabled = true; linkBtn.textContent = '⟳';
                    try {
                        const resp = await fetch(`/api/intake/item/${itemId}/accept-price`, {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                session_id: currentSessionId,
                                override_price: usePrice,
                                store_product_id: String(r.shopify_product_id || r.id || ''),
                                store_product_name: r.title || '',
                                tcgplayer_id: r.tcgplayer_id ? parseInt(r.tcgplayer_id) : undefined,
                            }),
                        });
                        const d = await resp.json();
                        if (!resp.ok) { alert(d.error || 'Link failed'); linkBtn.disabled = false; linkBtn.textContent = '🔗 Link'; return; }
                        document.body.removeChild(overlay);
                        storeCheck(currentSessionId);
                    } catch(e) { alert(e.message); linkBtn.disabled = false; linkBtn.textContent = '🔗 Link'; }
                });
            });
        }
    }

    async function doSearch(q) {
        if (!q) return;
        renderFind(q, [], true, null);
        try {
            const r = await fetch('/api/store/search?q=' + encodeURIComponent(q));
            const d = await r.json();
            renderFind(q, d.results || [], false, null);
        } catch(e) {
            renderFind(q, [], false, e.message);
        }
    }

    // Auto-search with the product name on open
    document.body.appendChild(overlay);
    renderFind(productName, [], true, null);
    doSearch(productName);
}

// Delegated click handler for Find buttons — survives innerHTML replacement
document.addEventListener('click', (e) => {
    const btn = e.target.closest('.find-in-store-btn');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    findInStore(btn, parseInt(btn.dataset.tcg), btn.dataset.name, btn.dataset.item, parseFloat(btn.dataset.offer), parseFloat(btn.dataset.market));
});

async function createListingFromStore(tcgplayerId, btn, itemId, offerPrice) {
    if (!tcgplayerId) { alert('No TCGPlayer ID — cannot create listing'); return; }
    const origText = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '⟳';
    btn.title = 'Creating listing... this takes ~30-60s for image processing';

    try {
        const body = { tcgplayer_id: tcgplayerId, quantity: 0 };
        if (itemId) body.item_id = itemId;
        if (offerPrice) body.offer_price = offerPrice;
        const r = await fetch('/api/create-listing', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) {
            btn.disabled = false;
            btn.innerHTML = origText;
            alert('Failed: ' + (d.error || 'Unknown error'));
            return;
        }
        // Success — replace button with confirmation + link
        const productId = d.product_id;
        const title = d.title || 'Draft listing';
        btn.outerHTML = `<span style="color:var(--green); font-size:0.7rem;">✓ Draft created</span>`;
    } catch(e) {
        btn.disabled = false;
        btn.innerHTML = origText;
        alert('Error: ' + e.message);
    }
}

    async function loadCacheStatus() {
    try {
        const r = await fetch('/api/cache/status');
        const d = await r.json();
        const label = document.getElementById('cache-age-label');
        if (!label) return;
        if (d.status === 'never_synced') {
            label.innerHTML = '<span style="color:var(--amber);">⚠ never synced</span>';
            return;
        }
        const age = d.age_minutes;
        const inProgress = d.refresh_in_progress;
        if (inProgress) {
            label.innerHTML = '<span style="color:var(--accent);">⟳ refreshing...</span>';
            setTimeout(loadCacheStatus, 3000); // poll until done
        } else if (age < 10) {
            label.innerHTML = `<span style="color:#22c55e;">✓ fresh</span> <span style="color:var(--text-dim);">${Math.round(age)}m ago</span>`;
        } else if (age < 60) {
            label.innerHTML = `<span style="color:var(--amber);">~ ${Math.round(age)}m ago</span>`;
        } else {
            const hrs = (age / 60).toFixed(1);
            label.innerHTML = `<span style="color:var(--red);">⚠ ${hrs}h ago</span>`;
        }
    } catch(e) { console.error('cache status error:', e); }
}

async function shopifySync() {
    document.getElementById('cache-age-label').innerHTML = '<span style="color:var(--accent);">⟳ refreshing...</span>';
    await fetch('/api/cache/refresh', { method: 'POST' });
    setTimeout(loadCacheStatus, 2000);
    const panel = document.getElementById('store-check-results');
    if (panel) panel.innerHTML = '<div class="loading"><span class="spinner"></span> Syncing from Shopify — fetching products...</div>';
    try {
        const r = await fetch('/api/shopify/sync', { method: 'POST' });
        if (!r.ok) {
            const d = await r.json();
            if (panel) panel.innerHTML = `<div class="alert alert-error">${d.error}</div>`;
            return;
        }
        const reader = r.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        let lastStatus = null;
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop(); // keep incomplete last line
            for (const line of lines) {
                if (!line.trim()) continue;
                try {
                    const msg = JSON.parse(line);
                    lastStatus = msg;
                    if (msg.status === 'progress' && panel) {
                        panel.innerHTML = `<div class="loading"><span class="spinner"></span> Syncing from Shopify — page ${msg.page}, saved ${msg.synced} products so far (${msg.total_variants} variants fetched)...</div>`;
                    }
                } catch(e) {}
            }
        }
        if (lastStatus?.status === 'done') {
            if (panel) panel.innerHTML = `<div class="alert alert-success">
                Synced <strong>${lastStatus.synced}</strong> products with TCGPlayer IDs from ${lastStatus.total_variants} total Shopify variants.
                Click <strong>🏪 Store</strong> tab again to refresh the table.
            </div>`;
        } else if (lastStatus?.status === 'error') {
            if (panel) panel.innerHTML = `<div class="alert alert-error">${lastStatus.error}</div>`;
        }
    } catch(err) {
        if (panel) panel.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

// Fetch shopify store domain for product links
(async function() {
    try {
        const r = await fetch('/api/shopify/status');
        const d = await r.json();
        if (d.store) {
            window.__shopifyStore = d.store;
            // Extract handle for admin URLs: "mystore.myshopify.com" -> "mystore"
            window.__shopifyStoreHandle = d.store.replace('.myshopify.com', '').replace(/\..*$/, '');
        }
    } catch(e) {}
})();

// ═══════════════════════════════ ADD SEALED ITEM TO EXISTING SESSION ═══════════════════════════════

// ═══════════════════════════════ ITEM STATUS MANAGEMENT ═══════════════════════════════

// Close action dropdowns when clicking outside
document.addEventListener('click', (e) => {
    if (!e.target.closest('.action-dropdown') && !e.target.closest('.action-trigger')) {
        document.querySelectorAll('.action-dropdown.show').forEach(d => d.classList.remove('show'));
    }
});

function toggleActionMenu(event, dd) {
    // Support both old call signature (btn) and new (event, dd)
    if (!dd) {
        // Old signature: toggleActionMenu(btn) — dd is next sibling
        const btn = event;
        dd = btn.nextElementSibling;
        event = null;
    }
    if (event) event.stopPropagation();

    // Close all others first
    document.querySelectorAll('.action-dropdown.show').forEach(d => { if (d !== dd) d.classList.remove('show'); });

    if (window.innerWidth > 768) {
        // Desktop: position near the trigger button
        const btn = dd.previousElementSibling;
        const rect = btn.getBoundingClientRect();
        const dropdownHeight = 220;
        if (rect.bottom + dropdownHeight > window.innerHeight) {
            dd.style.top = Math.max(4, rect.top - dropdownHeight) + 'px';
        } else {
            dd.style.top = (rect.bottom + 4) + 'px';
        }
        dd.style.left = Math.max(8, rect.right - 160) + 'px';
    } else {
        // Mobile: let CSS bottom-sheet handle positioning
        dd.style.left = ''; dd.style.top = '';
    }
    dd.classList.toggle('show');
}

window.addEventListener('resize', () => {
    document.querySelectorAll('.action-dropdown.show').forEach(d => d.classList.remove('show'));
});

async function cancelSession(sessionId) {
    const reason = await themedPrompt({
        title: '✕ Cancel Session',
        message: 'This session will be cancelled. No inventory will be created.',
        inputs: [{ type: 'text', label: 'Reason (optional)', placeholder: 'e.g. Deal fell through' }],
        confirmText: 'Cancel Session',
        dangerous: true,
    });
    if (reason === null) return;
    try {
        const r = await fetch(`/api/intake/cancel-session/${sessionId}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ reason: reason || 'No reason given' }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function rejuvenateSession(sessionId, evt) {
    if (evt) evt.stopPropagation();
    const confirmed = await themedPrompt({
        title: '↺ Rejuvenate Session',
        message: 'This will restore the session to In Progress. You can continue editing and making offers.',
        confirmText: 'Rejuvenate',
        dangerous: false,
    });
    if (confirmed === null) return;
    try {
        const r = await fetch(`/api/intake/rejuvenate-session/${sessionId}`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok || !d.success) { alert(d.error || 'Failed to rejuvenate'); return; }
        // Reload the session modal and refresh the cancelled list
        viewSession(sessionId, true);
        loadFilteredCancelled();
    } catch(err) { alert(err.message); }
}

async function damageItem(itemId, sessionId, currentQty) {
    let damagedQty = 1;
    if (currentQty > 1) {
        const input = await themedPrompt({
            title: '🔨 Mark as Damaged',
            message: `This item has ${currentQty} units. How many are damaged? Damaged items receive 85% of the original offer.`,
            inputs: [{ type: 'number', label: 'Damaged quantity', default: '1', min: 1, max: currentQty }],
            confirmText: 'Split & Mark Damaged',
            dangerous: true,
        });
        if (input === null) return;
        damagedQty = parseInt(input);
        if (isNaN(damagedQty) || damagedQty < 1 || damagedQty > currentQty) {
            alert(`Must be between 1 and ${currentQty}`);
            return;
        }
    } else {
        const ok = await themedConfirm(
            '🔨 Mark as Damaged',
            'Mark this item as damaged? Offer becomes 85% of original.',
            { confirmText: 'Mark Damaged', dangerous: true }
        );
        if (!ok) return;
    }
    try {
        const r = await fetch(`/api/intake/item/${itemId}/damage`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ damaged_qty: damagedQty }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function markItemStatus(itemId, sessionId, newStatus) {
    // Close dropdown
    document.querySelectorAll('.action-dropdown.show').forEach(d => d.classList.remove('show'));

    // Damaged has its own flow with 15% price reduction
    if (newStatus === 'damaged') {
        return damageItem(itemId, sessionId, 1);
    }

    const labels = { missing: 'Mark Missing', rejected: 'Reject', good: 'Mark Good' };
    const messages = {
        missing: 'This item will be excluded from payment and Shopify push.',
        rejected: 'Seller kept this item. It will be excluded from payment.',
        good: 'Restore this item to good status?',
    };

    const ok = await themedConfirm(
        labels[newStatus] || newStatus,
        messages[newStatus] || `Change status to ${newStatus}?`,
        { confirmText: labels[newStatus] || 'Confirm', dangerous: ['missing','rejected'].includes(newStatus) }
    );
    if (!ok) return;

    try {
        const r = await fetch(`/api/intake/item/${itemId}/status`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ status: newStatus }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function markMissing(itemId, sessionId) {
    const ok = await themedConfirm(
        '❌ Mark as Missing',
        'This item will be excluded from payment and Shopify push.',
        { confirmText: 'Mark Missing', dangerous: true }
    );
    if (!ok) return;
    try {
        const r = await fetch(`/api/intake/item/${itemId}/missing`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function markRejected(itemId, sessionId) {
    const ok = await themedConfirm(
        '🚫 Mark as Rejected',
        'Seller kept this item. It will be excluded from payment.',
        { confirmText: 'Mark Rejected', dangerous: true }
    );
    if (!ok) return;
    try {
        const r = await fetch(`/api/intake/item/${itemId}/rejected`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function restoreItem(itemId, sessionId) {
    const ok = await themedConfirm(
        'Restore Item',
        'Restore this item back to good status?',
        { confirmText: 'Restore' }
    );
    if (!ok) return;
    try {
        const r = await fetch(`/api/intake/item/${itemId}/restore`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function overridePrice(itemId, sessionId, currentPrice) {
    const values = await themedPrompt({
        title: '💰 Override Price',
        message: `Current market price: $${currentPrice.toFixed(2)}`,
        inputs: [
            { type: 'number', label: 'New market price', default: currentPrice.toFixed(2), min: 0, step: '0.01' },
            { type: 'text', label: 'Reason', placeholder: 'e.g. Better packs version, EV $45+' },
        ],
        confirmText: 'Override Price',
    });
    if (values === null) return;
    const [priceStr, note] = values;
    const parsed = parseFloat(priceStr);
    if (isNaN(parsed) || parsed < 0) { alert('Invalid price'); return; }

    try {
        const r = await fetch(`/api/intake/item/${itemId}/override-price`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ new_price: parsed, note: note, session_id: sessionId }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function applyBreakdownPriceWithQty(itemId, sessionId, bdValue, variantLabel, totalQty) {
    let breakdownQty = totalQty;
    if (totalQty > 1) {
        const ans = await themedPrompt({
            title: '📦 Price as Breakdown',
            message: `You have ${totalQty} units. How many should be priced as breakdown ($${parseFloat(bdValue).toFixed(2)} each)? The rest stay as-is.`,
            inputs: [{ label: `Qty to break down (1–${totalQty})`, type: 'number', default: '1', min: 1, max: totalQty, step: 1 }],
            confirmText: 'Price as Breakdown'
        });
        if (!ans) return;
        breakdownQty = Math.max(1, Math.min(parseInt(ans) || 1, totalQty));
    }
    await applyBreakdownPrice(itemId, sessionId, bdValue, variantLabel, breakdownQty);
}

async function applyBreakdownPrice(itemId, sessionId, bdValue, variantLabel, breakdownQty) {
    const ok = await themedConfirm(
        '📦 Price as Breakdown',
        `Set market price to $${parseFloat(bdValue).toFixed(2)} (breakdown value: ${variantLabel})?\n\nThe offer will be recalculated at the session's offer % based on this new price. Use this when you're buying to break open, not to sell as a sealed unit.`,
        { confirmText: 'Use Breakdown Price' }
    );
    if (!ok) return;
    try {
        const r = await fetch(`/api/intake/item/${itemId}/apply-breakdown-price`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ session_id: sessionId, breakdown_total: bdValue, variant_name: variantLabel, breakdown_qty: breakdownQty || 1 }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function editQuantity(itemId, sessionId, currentQty) {
    const input = await themedPrompt({
        title: '✏ Edit Quantity',
        message: `Current quantity: ${currentQty}`,
        inputs: [{ type: 'number', label: 'New quantity', default: String(currentQty), min: 1 }],
        confirmText: 'Update Quantity',
    });
    if (input === null) return;
    const newQty = parseInt(input);
    if (isNaN(newQty) || newQty < 1) { alert('Invalid quantity'); return; }
    if (newQty === currentQty) return;

    try {
        const r = await fetch(`/api/intake/item/${itemId}/update-quantity`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ quantity: newQty, session_id: sessionId }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

// Condition tile colors — shared by badge renderer and picker
const COND_STYLES = {
    NM:  { bg: '#14532d',              color: '#4ade80', label: 'Near Mint' },
    LP:  { bg: 'rgba(79,125,249,0.25)', color: '#7aadff', label: 'Lightly Played' },
    MP:  { bg: '#422006',              color: '#fbbf24', label: 'Mod. Played' },
    HP:  { bg: '#431407',              color: '#fb923c', label: 'Heavily Played' },
    DMG: { bg: '#450a0a',              color: '#f87171', label: 'Damaged' },
};

function condBadgeHtml(cond) {
    const s = COND_STYLES[cond];
    const style = s ? `background:${s.bg};color:${s.color};` : 'background:var(--surface-2);color:var(--text-dim);';
    return `<span class="badge" style="${style}">${cond || '—'}</span>`;
}

async function editCondition(itemId, sessionId, currentCond, tcgplayerId, cardName) {
    // Show overlay immediately with loading state — don't await fetch before showing UI
    const overlay = document.createElement('div');
    overlay.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:9999;display:flex;align-items:center;justify-content:center;padding:16px;';
    overlay.setAttribute('data-ec-overlay', '1');

    let variants = {};
    let selectedCond = currentCond;
    let _ecResolve = null;

    function _ecClose() {
        document.removeEventListener('keydown', _ecKeydown);
        if (document.body.contains(overlay)) document.body.removeChild(overlay);
    }
    function _ecKeydown(e) {
        if (e.key === 'Escape') { _ecClose(); if (_ecResolve) _ecResolve(null); }
    }
    document.addEventListener('keydown', _ecKeydown);

    function renderTiles() {
        const tiles = ['NM','LP','MP','HP','DMG'].map(cond => {
            const s = COND_STYLES[cond];
            const price = variants[cond];
            const isSel = cond === selectedCond;
            const border = isSel ? `2px solid ${s.color}` : '1px solid var(--border)';
            const bg = isSel ? s.bg : 'var(--surface-2)';
            const color = isSel ? s.color : 'var(--text-dim)';
            return `<div data-cond="${cond}" style="background:${bg};color:${color};border:${border};border-radius:6px;padding:6px 4px;text-align:center;cursor:pointer;transition:all 0.1s;min-width:0;">
                <div style="font-size:0.8rem;font-weight:700;">${cond}</div>
                ${price != null ? `<div style="font-size:0.75rem;font-weight:600;margin-top:2px;">$${price.toFixed(2)}</div>` : `<div style="font-size:0.7rem;opacity:0.4;margin-top:2px;">—</div>`}
            </div>`;
        }).join('');
        const selPrice = variants[selectedCond];
        const cardLabel = cardName ? `<div style="font-size:0.78rem;color:var(--text-dim);margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${(cardName||'').replace(/"/g,'&quot;')}">${cardName}</div>` : '';
        overlay.innerHTML = `
            <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;width:320px;max-width:100%;box-sizing:border-box;">
                <div style="font-size:0.9rem;font-weight:700;margin-bottom:6px;">🏷 Change Condition</div>
                ${cardLabel}
                <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:5px;margin-bottom:12px;">${tiles}</div>
                <div style="display:flex;gap:8px;">
                    <button id="ec-confirm" class="btn btn-primary" style="flex:1;font-size:0.85rem;padding:6px 8px;">
                        Set ${selectedCond}${selPrice != null ? ' — $'+selPrice.toFixed(2) : ''}
                    </button>
                    <button id="ec-cancel" class="btn btn-secondary" style="font-size:0.85rem;padding:6px 10px;">Cancel</button>
                </div>
            </div>`;
        overlay.querySelectorAll('[data-cond]').forEach(el => {
            el.addEventListener('click', () => { selectedCond = el.dataset.cond; renderTiles(); });
        });
        document.getElementById('ec-confirm').addEventListener('click', async () => {
            _ecClose();
            if (_ecResolve) _ecResolve(selectedCond);
            if (selectedCond === currentCond) return;
            try {
                const r = await fetch(`/api/intake/item/${itemId}/update-condition`, {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ condition: selectedCond, session_id: sessionId }),
                });
                const d = await r.json();
                if (!r.ok) { alert(d.error); return; }
                viewSession(sessionId, true);
            } catch(err) { alert(err.message); }
        });
        document.getElementById('ec-cancel').addEventListener('click', () => {
            _ecClose();
            if (_ecResolve) _ecResolve(null);
        });
    }

    // Show loading state immediately
    const cardLabel = cardName ? `<div style="font-size:0.78rem;color:var(--text-dim);margin-bottom:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${cardName}</div>` : '';
    overlay.innerHTML = `
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;width:320px;max-width:100%;box-sizing:border-box;">
            <div style="font-size:0.9rem;font-weight:700;margin-bottom:6px;">🏷 Change Condition</div>
            ${cardLabel}
            <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:5px;margin-bottom:12px;">
                ${['NM','LP','MP','HP','DMG'].map(cond => {
                    const s = COND_STYLES[cond];
                    const isSel = cond === selectedCond;
                    return `<div style="background:${isSel ? s.bg : 'var(--surface-2)'};color:${isSel ? s.color : 'var(--text-dim)'};border:${isSel ? '2px solid '+s.color : '1px solid var(--border)'};border-radius:6px;padding:6px 4px;text-align:center;">
                        <div style="font-size:0.8rem;font-weight:700;">${cond}</div>
                        <div style="font-size:0.7rem;opacity:0.4;margin-top:2px;">…</div>
                    </div>`;
                }).join('')}
            </div>
            <div style="font-size:0.78rem;color:var(--text-dim);text-align:center;">Loading prices…</div>
        </div>`;
    document.body.appendChild(overlay);

    // Now fetch prices in the background and re-render with them
    if (tcgplayerId) {
        try {
            const pr = await fetch('/api/lookup/card', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ tcgplayer_id: parseInt(tcgplayerId) }),
            });
            if (pr.ok) {
                const pd = await pr.json();
                const primary = pd.primary_printing || Object.keys(pd.variants || {})[0];
                variants = (pd.variants || {})[primary] || {};
            }
        } catch(e) { /* show tiles without prices */ }
    }
    renderTiles();
}

async function relinkRawCard(itemId, sessionId) {
    const items = window._sessionItems || [];
    const i = items.find(x => String(x.id) === String(itemId));
    if (!i) { alert('Item not found in session'); return; }
    // Initialize linked count on first entry into the flow
    if (!window._relinkLinkedCount) window._relinkLinkedCount = 0;
    window._relinkState = {
        itemId, sessionId,
        cardName: i.product_name || '', setName: i.set_name || '',
        cardNumber: i.card_number || '', isGraded: !!i.is_graded,
        gradeCompany: i.grade_company || '', gradeValue: i.grade_value || '',
        condition: i.condition || i.listing_condition || 'NM',
        quantity: parseInt(i.quantity) || 1, currentPrice: parseFloat(i.market_price) || 0,
    };
    _relinkShowSearch();
    openModal('relink-modal');
    _relinkDoSearch();
}

function _relinkShowSearch() {
    const rs = window._relinkState;
    const body = document.getElementById('relink-body');
    // Count remaining unmapped raw items for progress display
    const allItems = window._sessionItems || [];
    const unmappedRaw = allItems.filter(x => !x.is_mapped && x.product_type === 'raw' && ['good','damaged'].includes(x.item_status || 'good'));
    const remaining = unmappedRaw.length;
    const linked = window._relinkLinkedCount || 0;
    const hasQueue = remaining > 1 || (remaining === 1 && linked > 0);
    let titleText = '\u{1f517} Link: ' + rs.cardName + (rs.cardNumber ? ' #'+rs.cardNumber : '') + (rs.isGraded ? ' ['+rs.gradeCompany+' '+rs.gradeValue+']' : '') + (rs.quantity > 1 ? ' (\u00d7' + rs.quantity + ')' : '');
    if (hasQueue) titleText += ` (${remaining} remaining)`;
    document.getElementById('relink-title').textContent = titleText;
    body.innerHTML = `
        <div class="form-group"><label>Card Name</label>
            <input type="text" id="relink-search" value="${rs.cardName.replace(/"/g,'&quot;')}" placeholder="e.g. Charizard ex"></div>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
            <div class="form-group"><label>Set</label>
                <input type="text" id="relink-set" value="${(rs.setName||'').replace(/"/g,'&quot;')}" placeholder="e.g. Paldean Fates"></div>
            <div class="form-group"><label>TCGPlayer ID</label>
                <input type="number" id="relink-tcgid" placeholder="e.g. 535090"></div>
        </div>
        <div style="background:var(--surface-2);border-radius:6px;padding:8px 12px;margin-bottom:10px;font-size:0.85rem;">
            ${rs.cardNumber ? `<span style="color:var(--text-dim);">#${rs.cardNumber}</span> · ` : ''}${rs.isGraded ? `<span style="background:linear-gradient(135deg,#7c3aed,#4f7df9);color:#fff;font-weight:700;font-size:0.75rem;padding:1px 6px;border-radius:4px;">${rs.gradeCompany||'PSA'} ${rs.gradeValue||'?'}</span>` : `<span style="font-weight:600;">${rs.condition||'NM'}</span>`}${rs.currentPrice > 0 ? ` · Current: <strong style="color:var(--accent);">$${rs.currentPrice.toFixed(2)}</strong>` : ''}${rs.quantity > 1 ? ` · Qty ${rs.quantity}` : ''}
        </div>
        <button class="btn btn-primary" id="relink-search-btn" style="width:100%;">Search</button>
        <button class="btn btn-secondary" id="relink-manual-btn" style="width:100%;margin-top:6px;font-size:0.8rem;">💲 Manual Price (no TCGPlayer ID)</button>
        ${hasQueue ? `<div style="display:flex;gap:8px;margin-top:6px;">
            <button class="btn btn-secondary" id="relink-skip-btn" style="flex:1;font-size:0.8rem;">⏭ Skip</button>
            <button class="btn btn-secondary" id="relink-done-btn" style="flex:1;font-size:0.8rem;">✓ Done for now</button>
        </div>` : ''}
        <div id="relink-results" style="margin-top:12px;"></div>`;
    document.getElementById('relink-search-btn').addEventListener('click', _relinkDoSearch);
    document.getElementById('relink-manual-btn').addEventListener('click', _relinkManualPrice);
    document.getElementById('relink-search').addEventListener('keydown', function(e) { if (e.key === 'Enter') _relinkDoSearch(); });
    const skipBtn = document.getElementById('relink-skip-btn');
    if (skipBtn) skipBtn.addEventListener('click', () => _relinkAdvanceOrClose(rs.sessionId, true));
    const doneBtn = document.getElementById('relink-done-btn');
    if (doneBtn) doneBtn.addEventListener('click', () => {
        closeModal('relink-modal');
        const lc = window._relinkLinkedCount || 0;
        if (lc > 0) {
            const toast = document.createElement('div');
            toast.style.cssText = 'position:fixed;bottom:24px;right:24px;background:var(--green);color:#000;padding:10px 18px;border-radius:8px;font-size:0.85rem;font-weight:600;z-index:9999;';
            toast.textContent = '\u2713 Linked ' + lc + ' item' + (lc > 1 ? 's' : '');
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 3500);
        }
        window._relinkLinkedCount = 0;
        viewSession(currentSessionId, true);
    });
}

async function _relinkAdvanceOrClose(sessionId, isSkip) {
    // Re-fetch session to get fresh is_mapped state (handles sibling auto-linking)
    try {
        const r = await fetch('/api/intake/session/' + sessionId);
        const d = await r.json();
        window._sessionItems = d.items || [];
        window._sessionMeta = d.session || {};
    } catch(e) { /* proceed with stale data */ }

    const items = window._sessionItems || [];
    const unmapped = items.filter(x =>
        !x.is_mapped && x.product_type === 'raw' &&
        ['good','damaged'].includes(x.item_status || 'good')
    );

    if (unmapped.length > 0) {
        // Show brief toast for the just-linked item
        if (!isSkip) {
            const toast = document.createElement('div');
            toast.style.cssText = 'position:fixed;bottom:24px;right:24px;background:var(--green);color:#000;padding:10px 18px;border-radius:8px;font-size:0.85rem;font-weight:600;z-index:9999;transition:opacity 0.3s;';
            toast.textContent = '\u2713 Linked — ' + unmapped.length + ' remaining';
            document.body.appendChild(toast);
            setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 2000);
        }
        // Load next unmapped item
        const next = unmapped[0];
        window._relinkState = {
            itemId: next.id, sessionId,
            cardName: next.product_name || '', setName: next.set_name || '',
            cardNumber: next.card_number || '', isGraded: !!next.is_graded,
            gradeCompany: next.grade_company || '', gradeValue: next.grade_value || '',
            condition: next.condition || next.listing_condition || 'NM',
            quantity: parseInt(next.quantity) || 1, currentPrice: parseFloat(next.market_price) || 0,
        };
        _relinkShowSearch();
        _relinkDoSearch();
    } else {
        // All done
        closeModal('relink-modal');
        const lc = window._relinkLinkedCount || 0;
        if (lc > 0) {
            const toast = document.createElement('div');
            toast.style.cssText = 'position:fixed;bottom:24px;right:24px;background:var(--green);color:#000;padding:10px 18px;border-radius:8px;font-size:0.85rem;font-weight:600;z-index:9999;';
            toast.textContent = '\u2713 All items linked!' + (lc > 1 ? ' (' + lc + ' total)' : '');
            document.body.appendChild(toast);
            setTimeout(() => toast.remove(), 3500);
        }
        window._relinkLinkedCount = 0;
        viewSession(sessionId, true);
    }
}

async function _relinkManualPrice() {
    const rs = window._relinkState;
    const body = document.getElementById('relink-body');
    const isGraded = rs.isGraded;

    let condHtml = '';
    if (isGraded) {
        condHtml = `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;">
            <div class="form-group"><label>Grading Company</label>
            <select id="manual-grade-company">
                ${['PSA','BGS','CGC','SGC'].map(c => `<option value="${c}"${c===rs.gradeCompany?' selected':''}>${c}</option>`).join('')}
            </select></div>
            <div class="form-group"><label>Grade</label>
            <select id="manual-grade-value">
                ${['10','9.5','9','8.5','8','7','6','5','4','3','2','1'].map(g => `<option value="${g}"${g===rs.gradeValue?' selected':''}>${g}</option>`).join('')}
            </select></div>
        </div>`;
    } else {
        const cond = rs.condition || 'NM';
        condHtml = `<div class="form-group"><label>Condition</label>
            <select id="manual-condition">
                ${['NM','LP','MP','HP','DMG'].map(c => `<option value="${c}"${c===cond?' selected':''}>${c}</option>`).join('')}
            </select></div>`;
    }

    body.innerHTML = `
        <div style="margin-bottom:8px;font-size:0.85rem;"><strong>${rs.cardName}</strong>${rs.quantity > 1 ? ` <span style="color:var(--amber);font-weight:700;">× ${rs.quantity}</span>` : ''}
        ${rs.setName ? `<br><span style="color:var(--text-dim);">${rs.setName}</span>` : ''}</div>
        <div class="alert alert-warning" style="font-size:0.8rem;margin-bottom:12px;">No TCGPlayer ID — entering manual market price only.</div>
        ${condHtml}
        <div class="form-group"><label>Market Price ($)</label>
            <input type="number" id="manual-price-input" min="0" step="0.01" placeholder="e.g. 45.00" style="font-size:1.1rem;"></div>
        <div style="display:flex;gap:8px;">
            <button id="manual-link-ok" class="btn btn-primary" style="flex:1;">Link with Manual Price</button>
            <button id="manual-link-back" class="btn btn-secondary">← Back</button>
        </div>`;

    document.getElementById('manual-link-back').addEventListener('click', _relinkShowSearch);
    document.getElementById('manual-price-input').focus();
    document.getElementById('manual-link-ok').addEventListener('click', async function() {
        const price = parseFloat(document.getElementById('manual-price-input').value);
        if (isNaN(price) || price <= 0) { document.getElementById('manual-price-input').style.borderColor = 'var(--red)'; return; }

        body.innerHTML = '<div class="loading"><span class="spinner"></span> Linking...</div>';
        try {
            // Use accept-price endpoint (allows null tcgplayer_id)
            const payload = { session_id: rs.sessionId, override_price: price };
            const r = await fetch('/api/intake/item/' + rs.itemId + '/accept-price', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            if (!r.ok) { const d = await r.json(); body.innerHTML = '<div class="alert alert-error">' + (d.error||'Failed') + '</div>'; return; }

            // Update condition or grade
            if (isGraded) {
                const company = document.getElementById('manual-grade-company')?.value || rs.gradeCompany || 'PSA';
                const gradeVal = document.getElementById('manual-grade-value')?.value || rs.gradeValue || '9';
                await fetch('/api/intake/item/' + rs.itemId + '/mark-graded', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ session_id: rs.sessionId, grade_company: company, grade_value: gradeVal, market_price: price }),
                });
            } else {
                const cond = document.getElementById('manual-condition')?.value || 'NM';
                await fetch('/api/intake/item/' + rs.itemId + '/update-condition', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ condition: cond, session_id: rs.sessionId }),
                });
            }
            window._relinkLinkedCount = (window._relinkLinkedCount || 0) + 1;
            await _relinkAdvanceOrClose(rs.sessionId);
        } catch(err) { body.innerHTML = '<div class="alert alert-error">' + err.message + '</div>'; }
    });
}

function _relinkRenderResults(results, resultsDiv, append) {
    // Render via the shared card-search renderer so relink shows the
    // same images, set/printing/game badges, and variant chips that the
    // Add Card flow does. Click handler resolves to the picked variant's
    // tcgPlayer ID and opens the conditions panel for that card.
    const renderInto = (container) => {
        _renderCardSearchResults((results || []).slice(0, 12), container, {
            condition: 'NM',
            qty: 1,
            offerPct: null,  // no per-chip offer projection in relink
            onPick: (pick) => {
                if (pick && pick.tcgId) _relinkFetchAndShowConditions(parseInt(pick.tcgId));
            },
        });
    };
    if (append) {
        // Preserve any warning HTML already in resultsDiv (e.g. "set
        // filter dropped — showing all results") and append a wrapper
        // div that owns its own click listeners.
        const wrap = document.createElement('div');
        renderInto(wrap);
        resultsDiv.appendChild(wrap);
    } else {
        renderInto(resultsDiv);
    }
}

    async function _relinkDoSearch() {
    const rs = window._relinkState;
    const searchName = document.getElementById('relink-search').value.trim();
    const setFilter = document.getElementById('relink-set').value.trim();
    const tcgIdRaw = parseInt(document.getElementById('relink-tcgid').value);
    const resultsDiv = document.getElementById('relink-results');

    if (tcgIdRaw > 0) {
        resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Looking up TCG#' + tcgIdRaw + '...</div>';
        _relinkFetchAndShowConditions(tcgIdRaw);
        return;
    }
    if (!searchName) { resultsDiv.innerHTML = '<div class="alert alert-warning">Enter a card name or TCGPlayer ID</div>'; return; }

    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Searching...</div>';
    try {
        // Fold set into query string — PPT fuzzy-matches better than exact set filter
        // e.g. "Jolteon ex EX Delta Species" finds the right card even if set name differs slightly
        const combinedQuery = setFilter ? searchName + ' ' + setFilter : searchName;
        const body = { query: combinedQuery };
        const r = await fetch('/api/search/cards', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        const results = d.results || [];
        if (!r.ok || !results.length) {
            // Retry with just the card name if the combined query found nothing
            if (setFilter) {
                const r2 = await fetch('/api/search/cards', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ query: searchName }),
                });
                const d2 = await r2.json();
                if (d2.results && d2.results.length) {
                    // Show results but note the set filter was dropped
                    resultsDiv.innerHTML = '<div class="alert alert-warning" style="font-size:0.8rem; padding:6px 10px; margin-bottom:8px;">No exact match for set \"' + setFilter + '\" — showing all results for \"' + searchName + '\"</div>';
                    _relinkRenderResults(d2.results, resultsDiv, true);
                    return;
                }
            }
            resultsDiv.innerHTML = '<div class="alert alert-warning">No cards found</div>';
            return;
        }
        _relinkRenderResults(results, resultsDiv);
    } catch(err) { resultsDiv.innerHTML = '<div class="alert alert-error">' + err.message + '</div>'; }
}

async function _relinkFetchAndShowConditions(tcgId) {
    const rs = window._relinkState;
    const resultsDiv = document.getElementById('relink-results');
    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Loading pricing...</div>';
    try {
        const r = await fetch('/api/lookup/card', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tcgplayer_id: tcgId }),
        });
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = '<div class="alert alert-error">' + (d.error||'Failed') + '</div>'; return; }
        const card = d.card || {};
        const variants = d.variants || {};
        let primary = d.primary_printing || Object.keys(variants)[0] || 'Default';
        if (!variants[primary] && Object.keys(variants).length) primary = Object.keys(variants)[0];
        rs.tcgId = tcgId;
        rs.card = card;
        rs.variants = variants;
        rs.primary = primary;
        rs.newName = card.name || rs.cardName;
        rs.setName = card.setName || card.set_name || '';
        rs.cardNum = card.cardNumber || '';
        rs.rarity = card.rarity || '';
        rs.gradedPrices = d.graded_prices || null;
        // Seed mode and grade from the item — always set on first load
        if (!rs._relinkMode) rs._relinkMode = rs.isGraded ? 'graded' : 'raw';
        if (!rs._relinkSeeded) {
            rs._relinkGradeCompany = rs.gradeCompany || 'PSA';
            rs._relinkGradeValue = rs.gradeValue || '10';
            rs._relinkSeeded = true;
        }
        _relinkRenderConditions(rs.condition || 'NM');
    } catch(err) { resultsDiv.innerHTML = '<div class="alert alert-error">' + err.message + '</div>'; }
}

function _relinkRenderConditions(selectedCond) {
    const rs = window._relinkState;
    if (!rs._relinkMode) rs._relinkMode = rs.isGraded ? 'graded' : 'raw';
    const body = document.getElementById('relink-body');
    const variantNames = Object.keys(rs.variants || {});
    const selectedVariant = rs.primary;
    const variantData = (rs.variants || {})[selectedVariant] || {};

    const itemGradeBadge = rs.isGraded
        ? `<span style="background:linear-gradient(135deg,#7c3aed,#4f7df9);color:#fff;font-weight:700;font-size:0.72rem;padding:1px 6px;border-radius:4px;">${rs.gradeCompany||'PSA'} ${rs.gradeValue||'?'}</span>`
        : `<span style="font-weight:600;">${rs.condition||'NM'}</span>`;
    let html = `<div style="margin-bottom:6px; font-size:0.8rem; color:var(--text-dim);">Replacing: <span style="text-decoration:line-through;">${rs.cardName}</span>${rs.cardNumber ? ' #'+rs.cardNumber : ''} ${itemGradeBadge}${rs.quantity > 1 ? ` <span style="font-weight:700;color:var(--amber);">× ${rs.quantity}</span>` : ''}${rs.currentPrice > 0 ? ` — was <strong>$${rs.currentPrice.toFixed(2)}</strong>` : ''}</div>`;
    html += `<div style="margin-bottom:12px;"><strong style="color:var(--accent); font-size:1.1rem;">${rs.newName}</strong>`;
    html += `<span style="color:var(--text-dim);"> — ${rs.setName}${rs.cardNum ? ' #'+rs.cardNum : ''} · TCG#${rs.tcgId}</span>`;
    html += `${rs.quantity > 1 ? `<span style="background:var(--amber);color:#000;font-weight:700;font-size:0.75rem;padding:1px 6px;border-radius:4px;margin-left:6px;">Qty: ${rs.quantity}</span>` : ''}</div>`;

    // Raw / Graded toggle
    const modeRaw = rs._relinkMode === 'raw';
    html += `<div style="display:flex;gap:6px;margin-bottom:14px;">
        <button class="btn btn-sm ${modeRaw ? 'btn-primary' : 'btn-secondary'}" onclick="_relinkSetMode('raw')">🃏 Raw Card</button>
        <button class="btn btn-sm ${!modeRaw ? 'btn-primary' : 'btn-secondary'}" onclick="_relinkSetMode('graded')">🏅 Graded Slab</button>
    </div>`;

    if (modeRaw) {
        // Variant selector
        if (variantNames.length > 1) {
            html += `<div style="margin-bottom:10px;"><label style="font-size:0.8rem;color:var(--text-dim);display:block;margin-bottom:4px;">Variant</label><div style="display:flex;gap:6px;flex-wrap:wrap;">`;
            variantNames.forEach(v => {
                html += `<button class="btn btn-sm ${v === selectedVariant ? 'btn-primary' : 'btn-secondary'}" data-rv="${v.replace(/"/g,'&quot;')}">${v}</button>`;
            });
            html += `</div></div>`;
        } else if (variantNames.length === 1) {
            html += `<div style="margin-bottom:8px;font-size:0.85rem;color:var(--text-dim);">Printing: <strong style="color:var(--text);">${variantNames[0]}</strong></div>`;
        }

        html += `<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:12px;">`;
        ['NM','LP','MP','HP','DMG'].forEach(cond => {
            const p = variantData[cond];
            const isSel = cond === selectedCond;
            const hasP = p != null;
            const cs = COND_STYLES[cond] || {};
            const border = isSel ? `3px solid ${cs.color||'#fff'}` : '2px solid var(--border)';
            html += `<div data-rc="${cond}" style="background:${cs.bg||'var(--surface)'};color:${cs.color||'var(--text)'};padding:10px 6px;border-radius:6px;text-align:center;cursor:pointer;border:${border};opacity:${hasP?'1':'0.5'};">`;
            html += `<div style="font-size:0.75rem;font-weight:700;">${cond}</div>`;
            html += `<div style="font-size:0.65rem;opacity:0.8;margin-bottom:4px;">${(COND_STYLES[cond]||{}).label||''}</div>`;
            html += `<div style="font-size:1.05rem;font-weight:700;">${hasP ? '$'+p.toFixed(2) : '—'}</div>`;
            html += `</div>`;
        });
        html += `</div>`;

        const selPrice = variantData[selectedCond];
        const importedPrice = rs.currentPrice || 0;
        const pptLabel = selPrice ? `PPT: $${selPrice.toFixed(2)}` : 'PPT: —';
        const importLabel = importedPrice > 0 ? `Imported: $${importedPrice.toFixed(2)}` : '';
        html += `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
            <label style="font-size:0.8rem;color:var(--text-dim);white-space:nowrap;">Price $</label>
            <input type="number" id="relink-price-override" value="${selPrice ? selPrice.toFixed(2) : ''}" min="0" step="0.01" style="flex:1;font-size:1rem;font-weight:700;" placeholder="Enter price">
        </div>`;
        if (importedPrice > 0 && selPrice && Math.abs(importedPrice - selPrice) > 0.01) {
            html += `<div style="display:flex;gap:6px;margin-bottom:10px;font-size:0.82rem;">
                <button class="btn btn-sm btn-secondary" id="relink-use-ppt" style="font-size:0.78rem;">${pptLabel}</button>
                <button class="btn btn-sm btn-secondary" id="relink-use-imported" style="font-size:0.78rem;">${importLabel}</button>
            </div>`;
        }
        html += `<div style="display:flex;gap:8px;">`;
        html += `<button id="relink-ok" class="btn btn-primary" style="flex:1;">Relink ${selectedCond}</button>`;
        html += `<button id="relink-back" class="btn btn-secondary">← Back</button>`;
        html += `</div>`;

    } else {
        // Graded mode
        const graded = rs.gradedPrices || {};
        const selCompany = rs._relinkGradeCompany || 'PSA';
        const selGrade   = rs._relinkGradeValue || '9';
        const gradeData  = (graded[selCompany] || {})[selGrade] || {};
        const gradePrice = gradeData.price;

        html += `<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;">
            <div class="form-group"><label>Grading Company</label>
            <select id="relink-grade-company" onchange="_relinkGradeChanged()">
                ${['PSA','BGS','CGC','SGC'].map(c => `<option value="${c}"${c===selCompany?' selected':''}>${c}</option>`).join('')}
            </select></div>
            <div class="form-group"><label>Grade</label>
            <select id="relink-grade-value" onchange="_relinkGradeChanged()">
                ${['10','9.5','9','8.5','8','7','6','5','4','3','2','1'].map(g => `<option value="${g}"${g===selGrade?' selected':''}>${g}</option>`).join('')}
            </select></div>
        </div>`;

        // Price info box
        if (gradePrice != null) {
            const conf = gradeData.confidence;
            const confColor = conf === 'high' ? '#22c55e' : conf === 'medium' ? '#f59e0b' : '#ef4444';
            const method = (gradeData.method || '').replace(/_/g,' ');
            const signals = [];
            if (gradeData.count) signals.push(`${gradeData.count} sales`);
            if (gradeData.volume_7day) signals.push(`${gradeData.volume_7day.toFixed(1)}/day`);
            const breakdown = [];
            if (gradeData.price_7day != null) breakdown.push(`7d $${gradeData.price_7day.toFixed(2)}`);
            if (gradeData.median != null) breakdown.push(`median $${gradeData.median.toFixed(2)}`);
            if (gradeData.min != null && gradeData.max != null) breakdown.push(`range $${gradeData.min.toFixed(0)}–$${gradeData.max.toFixed(0)}`);
            html += `<div style="background:var(--surface-2);border-radius:6px;padding:10px;margin-bottom:12px;font-size:0.85rem;">
                <strong style="font-size:1.15rem;">$${gradePrice.toFixed(2)}</strong>
                <span style="color:${confColor};font-size:0.72rem;font-weight:600;text-transform:uppercase;margin-left:6px;">${conf||''}</span>
                <span style="color:var(--text-dim);font-size:0.7rem;margin-left:4px;">${method}</span>
                ${signals.length ? `<br><span style="color:var(--text-dim);font-size:0.72rem;">${signals.join(' · ')}</span>` : ''}
                ${breakdown.length ? `<br><span style="color:var(--text-dim);font-size:0.68rem;opacity:0.8;">${breakdown.join(' · ')}</span>` : ''}
            </div>`;
        } else {
            html += `<div style="background:var(--surface-2);border-radius:6px;padding:10px;margin-bottom:12px;font-size:0.85rem;color:var(--text-dim);">No eBay data for ${selCompany} ${selGrade}</div>`;
        }

        html += `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
            <label style="font-size:0.8rem;color:var(--text-dim);white-space:nowrap;">Price $</label>
            <input type="number" id="relink-price-override" value="${gradePrice ? gradePrice.toFixed(2) : ''}" min="0" step="0.01" style="flex:1;font-size:1rem;font-weight:700;" placeholder="Enter price">
        </div>`;
        html += `<div style="display:flex;gap:8px;">`;
        html += `<button id="relink-ok" class="btn btn-primary" style="flex:1;">Relink ${selCompany} ${selGrade}</button>`;
        html += `<button id="relink-back" class="btn btn-secondary">← Back</button>`;
        html += `</div>`;
    }

    body.innerHTML = html;

    // Attach listeners
    body.querySelectorAll('[data-rc]').forEach(el => {
        el.addEventListener('click', () => _relinkRenderConditions(el.getAttribute('data-rc')));
    });
    body.querySelectorAll('[data-rv]').forEach(el => {
        el.addEventListener('click', () => { rs.primary = el.getAttribute('data-rv'); _relinkRenderConditions(selectedCond); });
    });
    const _rlPptBtn = document.getElementById('relink-use-ppt');
    const _rlImpBtn = document.getElementById('relink-use-imported');
    const _rlPriceInput = document.getElementById('relink-price-override');
    if (_rlPptBtn) _rlPptBtn.addEventListener('click', () => {
        const vd = (rs.variants || {})[rs.primary] || {};
        if (vd[selectedCond] != null) _rlPriceInput.value = vd[selectedCond].toFixed(2);
    });
    if (_rlImpBtn) _rlImpBtn.addEventListener('click', () => {
        _rlPriceInput.value = (rs.currentPrice || 0).toFixed(2);
    });
    document.getElementById('relink-back').addEventListener('click', _relinkShowSearch);
    document.getElementById('relink-ok').addEventListener('click', async function() {
        const modeRaw = rs._relinkMode === 'raw';
        const overrideInput = document.getElementById('relink-price-override');
        let price = overrideInput ? parseFloat(overrideInput.value) : 0;

        if (!price || isNaN(price) || price <= 0) {
            if (overrideInput) { overrideInput.style.borderColor = 'var(--red)'; overrideInput.focus(); }
            return;
        }

        body.innerHTML = '<div class="loading"><span class="spinner"></span> Relinking...</div>';
        try {
            const mapPayload = {
                item_id: rs.itemId, tcgplayer_id: rs.tcgId,
                override_price: price,
                product_name: rs.newName, set_name: rs.setName,
                card_number: rs.cardNum, rarity: rs.rarity,
                variance: rs.primary || '',
                session_id: rs.sessionId,
            };
            const mapR = await fetch('/api/intake/map-item', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(mapPayload),
            });
            if (!mapR.ok) { const md = await mapR.json(); body.innerHTML = '<div class="alert alert-error">' + (md.error||'Failed') + '</div>'; return; }

            if (modeRaw) {
                await fetch('/api/intake/item/' + rs.itemId + '/update-condition', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ condition: selectedCond, session_id: rs.sessionId, skip_reprice: true }),
                });
            } else {
                const company = rs._relinkGradeCompany || 'PSA';
                const gradeVal = rs._relinkGradeValue || '9';
                await fetch(`/api/intake/item/${rs.itemId}/mark-graded`, {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ session_id: rs.sessionId, grade_company: company, grade_value: gradeVal, market_price: price }),
                });
            }
            window._relinkLinkedCount = (window._relinkLinkedCount || 0) + 1;
            await _relinkAdvanceOrClose(rs.sessionId);
        } catch(err) { body.innerHTML = '<div class="alert alert-error">' + err.message + '</div>'; }
    });
}

function _relinkSetMode(mode) {
    const rs = window._relinkState;
    if (!rs) return;
    rs._relinkMode = mode;
    if (mode === 'graded' && !rs.gradedPrices && rs.tcgId) {
        // Load graded prices if not already loaded
        const body = document.getElementById('relink-body');
        const spinner = document.createElement('div');
        spinner.className = 'loading';
        spinner.innerHTML = '<span class="spinner"></span> Loading graded prices...';
        // Keep existing HTML, just append loading indicator
        fetch('/api/lookup/card', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tcgplayer_id: rs.tcgId }),
        }).then(r => r.json()).then(d => {
            rs.gradedPrices = d.graded_prices || null;
            _relinkRenderConditions('NM');
        }).catch(() => _relinkRenderConditions('NM'));
        _relinkRenderConditions('NM');
    } else {
        _relinkRenderConditions('NM');
    }
}

function _relinkGradeChanged() {
    const rs = window._relinkState;
    if (!rs) return;
    rs._relinkGradeCompany = document.getElementById('relink-grade-company')?.value || 'PSA';
    rs._relinkGradeValue   = document.getElementById('relink-grade-value')?.value || '9';
    _relinkRenderConditions('NM');
}

// ═══════════════════════════════ MARK AS GRADED ═══════════════════════════════

async function openMarkGraded(itemId, sessionId, cardName, tcgplayerId, isGraded, gradeCompany, gradeValue) {
    window._mgState = { itemId, sessionId, cardName, tcgplayerId, cachedGraded: null };
    document.getElementById('mark-graded-title').textContent = isGraded ? '🏅 Change Grade' : '🏅 Mark as Graded';
    document.getElementById('mark-graded-card-info').textContent = cardName;
    document.getElementById('mg-company').value = gradeCompany || 'PSA';
    document.getElementById('mg-grade').value = gradeValue || '9';
    document.getElementById('mg-price-info').innerHTML = '<span style="color:var(--text-dim);">Loading prices...</span>';
    const _mgBtn = document.getElementById('mg-confirm-btn');
    if (_mgBtn) { _mgBtn.disabled = false; _mgBtn.textContent = 'Save Grade'; }
    openModal('mark-graded-modal');

    await onMarkGradedChange();
}

// Fetch live eBay comps + cached graded aggregates for the currently
// selected (company, grade) and render. Re-runs whenever the user changes
// either dropdown — the cache shape is just {company:{grade:price}}, so
// we hit the lookup endpoint with the grade params to get the rich
// live_graded payload (mid/low/high/comps_count/source).
async function onMarkGradedChange() {
    const st = window._mgState;
    if (!st) return;
    const company = document.getElementById('mg-company').value;
    const grade = document.getElementById('mg-grade').value;
    const priceInfo = document.getElementById('mg-price-info');

    if (!st.tcgplayerId) {
        priceInfo.innerHTML = '<span style="color:var(--text-dim);">No TCGPlayer ID linked — price will need manual override after saving.</span>';
        st._selectedPrice = null;
        return;
    }

    priceInfo.innerHTML = '<span style="color:var(--text-dim);">Loading prices...</span>';

    let cachedPrice = null;
    let live = null;
    try {
        const r = await fetch('/api/lookup/card', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                tcgplayer_id: st.tcgplayerId,
                grade_company: company,
                grade_value: grade,
            }),
        });
        const d = await r.json();
        const graded = d.graded_prices || {};
        cachedPrice = (graded[company] || {})[grade];
        const liveCo = d.live_graded || {};
        live = (liveCo[company] || {})[grade] || null;
    } catch(e) { /* fall through to no-data state */ }

    // Live (real eBay comps) wins over the cache aggregate.
    const price = (live && live.mid != null) ? live.mid : cachedPrice;
    st._selectedPrice = price != null ? Number(price) : null;

    if (st._selectedPrice == null) {
        priceInfo.innerHTML = `<span style="color:var(--text-dim);">No eBay data for ${company} ${grade} — backend will retry on save.</span>`;
        return;
    }

    const sourceLabel = live
        ? `<span style="color:#22c55e;font-size:0.72rem;font-weight:600;">live</span>`
        : `<span style="color:#f59e0b;font-size:0.72rem;font-weight:600;">cache</span>`;

    const stats = [];
    if (live && live.comps_count) stats.push(`${live.comps_count} comps`);
    if (live && live.low != null && live.high != null) {
        stats.push(`range $${Number(live.low).toFixed(0)}–$${Number(live.high).toFixed(0)}`);
    }
    const statsLine = stats.join(' · ');

    priceInfo.innerHTML =
        `<strong style="font-size:1.2rem;">$${Number(price).toFixed(2)}</strong> ${sourceLabel}` +
        (statsLine ? `<br><span style="color:var(--text-dim);font-size:0.72rem;">${statsLine}</span>` : '');
}

async function confirmMarkGraded() {
    const st = window._mgState;
    if (!st) return;
    const company = document.getElementById('mg-company').value;
    const grade = document.getElementById('mg-grade').value;
    const btn = document.getElementById('mg-confirm-btn');
    btn.disabled = true;
    btn.textContent = 'Saving...';

    try {
        const r = await fetch(`/api/intake/item/${st.itemId}/mark-graded`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: st.sessionId,
                grade_company: company,
                grade_value: grade,
                market_price: st._selectedPrice || null,
            }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed'); btn.disabled = false; btn.textContent = 'Save Grade'; return; }
        closeModal('mark-graded-modal');
        viewSession(st.sessionId, true);
    } catch(e) { alert(e.message); btn.disabled = false; btn.textContent = 'Save Grade'; }
}

    async function deleteItem(itemId, sessionId) {
    const ok = await themedConfirm(
        '🗑 Delete Item',
        'Permanently remove this item from the session? This cannot be undone.',
        { confirmText: 'Delete', dangerous: true }
    );
    if (!ok) return;
    try {
        const r = await fetch(`/api/intake/item/${itemId}/delete`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

// ═══════════════════════════════ ADD SEALED ITEM TO EXISTING SESSION ═══════════════════════════════

function switchSessionAddType(type) {
    // Remember choice across viewSession reloads — adding multiple cards
    // in a row shouldn't keep flipping back to "Sealed Product"
    window._lastSessionAddType = type;
    try { sessionStorage.setItem('pf_lastSessionAddType', type); } catch(e) {}
    const sealedDiv = document.getElementById('session-add-sealed');
    const cardDiv = document.getElementById('session-add-card');
    const sealedBtn = document.getElementById('session-add-type-sealed');
    const cardBtn = document.getElementById('session-add-type-card');
    if (sealedDiv) sealedDiv.style.display = type === 'sealed' ? '' : 'none';
    if (cardDiv) cardDiv.style.display = type === 'card' ? '' : 'none';
    if (sealedBtn) { sealedBtn.className = 'btn btn-sm ' + (type === 'sealed' ? 'btn-primary' : 'btn-secondary'); }
    if (cardBtn) { cardBtn.className = 'btn btn-sm ' + (type === 'card' ? 'btn-primary' : 'btn-secondary'); }
}

function _restoreSessionAddType() {
    // Called after viewSession finishes rendering — restores the user's
    // previously chosen add type (sealed vs card) so reloads don't reset it.
    let stored = window._lastSessionAddType;
    if (!stored) {
        try { stored = sessionStorage.getItem('pf_lastSessionAddType'); } catch(e) {}
    }
    if (stored === 'card' && document.getElementById('session-add-type-card')) {
        switchSessionAddType('card');
    }
    // Re-open manual card panel if it was open before reload
    if (window._lastSessionManualCardOpen) {
        const el = document.getElementById('session-card-manual');
        if (el) {
            el.style.display = 'block';
            const nameInput = document.getElementById('session-card-manual-name');
            if (nameInput) nameInput.focus();
        }
    }
}

function toggleManualSealedAdd(sessionId, offerPct) {
    const el = document.getElementById('session-sealed-manual');
    el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

function toggleManualCardAdd(sessionId, offerPct) {
    const el = document.getElementById('session-card-manual');
    const open = el.style.display === 'none';
    el.style.display = open ? 'block' : 'none';
    // Persist the open state across viewSession reloads so adding card
    // after card via manual flow doesn't make you re-open the panel
    window._lastSessionManualCardOpen = open;
    if (open) {
        const nameInput = document.getElementById('session-card-manual-name');
        if (nameInput) nameInput.focus();
    }
}

async function submitManualCardAdd(sessionId, offerPct) {
    const name = document.getElementById('session-card-manual-name').value.trim();
    const setName = document.getElementById('session-card-manual-set').value.trim();
    const cardNum = document.getElementById('session-card-manual-num').value.trim();
    const tcgId = document.getElementById('session-card-manual-tcgid').value.trim();
    const condition = document.getElementById('session-card-manual-cond').value;
    const price = parseFloat(document.getElementById('session-card-manual-price').value) || 0;
    const qty = parseInt(document.getElementById('session-card-manual-qty').value) || 1;

    if (!name) { alert('Card name is required'); return; }
    if (price <= 0) { alert('Price is required'); return; }

    try {
        const r = await fetch('/api/intake/add-raw-card', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: sessionId,
                tcgplayer_id: tcgId ? parseInt(tcgId) : null,
                card_name: name,
                set_name: setName,
                card_number: cardNum,
                condition: condition,
                quantity: qty,
                market_price: price,
            }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed to add'); return; }

        ['session-card-manual-name','session-card-manual-set','session-card-manual-num',
         'session-card-manual-tcgid','session-card-manual-price'].forEach(id => {
            const el = document.getElementById(id); if (el) el.value = '';
        });
        document.getElementById('session-card-manual-qty').value = '1';
        // Keep the manual panel open for the next card — adding multiple
        // off-catalog cards in a row is the whole point of this flow
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function submitManualSealedAdd(sessionId, offerPct) {
    const name = document.getElementById('session-manual-name').value.trim();
    const setName = document.getElementById('session-manual-set').value.trim();
    const tcgId = document.getElementById('session-manual-tcgid').value.trim();
    const price = parseFloat(document.getElementById('session-manual-price').value) || 0;
    const qty = parseInt(document.getElementById('session-manual-qty').value) || 1;

    if (!name) { alert('Product name is required'); return; }
    if (price <= 0) { alert('Price is required'); return; }

    try {
        const r = await fetch('/api/intake/add-sealed-item', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: sessionId,
                product_name: name,
                set_name: setName,
                tcgplayer_id: tcgId ? parseInt(tcgId) : null,
                market_price: price,
                quantity: qty,
            }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed to add'); return; }

        document.getElementById('session-manual-name').value = '';
        document.getElementById('session-manual-set').value = '';
        document.getElementById('session-manual-tcgid').value = '';
        document.getElementById('session-manual-price').value = '';
        document.getElementById('session-manual-qty').value = '1';
        document.getElementById('session-sealed-manual').style.display = 'none';
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function searchSealedForSession(sessionId, offerPct, live) {
    const searchTerm = document.getElementById('session-sealed-search').value.trim();
    const resultsDiv = document.getElementById('session-sealed-results');
    if (!searchTerm) { resultsDiv.innerHTML = '<div class="alert alert-warning">Enter a search term</div>'; return; }

    resultsDiv.innerHTML = `<div class="loading"><span class="spinner"></span> Searching${live ? ' PPT live' : ''}...</div>`;
    try {
        const body = { query: searchTerm };
        if (live) body.live = true;
        const r = await fetch('/api/search/sealed', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        
        // Handle non-JSON responses
        const contentType = r.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            const text = await r.text();
            resultsDiv.innerHTML = `<div class="alert alert-error">Server error (${r.status}): ${text.substring(0, 100)}</div>`;
            return;
        }
        
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }

        const products = d.results || [];
        if (!products.length) { resultsDiv.innerHTML = '<div class="alert alert-warning">No results found</div>'; return; }

        const qty = parseInt(document.getElementById('session-sealed-qty').value) || 1;
        resultsDiv.innerHTML = products.map(p => {
            const price = p.unopenedPrice || (p.prices ? p.prices.market : null) || 0;
            const offer = (price * qty * offerPct / 100).toFixed(2);
            const tcgId = p.tcgPlayerId || p.tcgplayer_id || p.id || '';
            const setName = (p.setName || p.set_name || '').replace(/'/g, "\\'");
            const img = p.imageCdnUrl400 || p.imageCdnUrl || p.imageCdnUrl800 || '';
            const src = p._price_source ? ({cache:'cache',ppt:'PPT',scrydex:'SDX'}[p._price_source]||p._price_source) : '';
            const nameLower = (p.name||'').toLowerCase();
            const isBundle = ['art bundle','set of','bundle (','pack of','case','display'].some(kw => nameLower.includes(kw));
            return `<div class="search-result" style="display:flex; gap:10px; align-items:center; padding:8px 12px; border:1px solid var(--border); border-radius:6px; margin-bottom:4px; cursor:pointer;${isBundle ? ' opacity:0.6; border-left:3px solid var(--amber);' : ''}" onclick="addSealedToSession('${sessionId}', ${tcgId || 'null'}, '${(p.name||'').replace(/'/g,"\\'")}', '${setName}', ${price}, ${qty}, ${offerPct})" onmouseover="this.style.background='var(--surface-2)'" onmouseout="this.style.background='transparent'">
                ${img ? `<img src="${img}" style="width:48px; height:48px; object-fit:contain; border-radius:4px; flex-shrink:0;">` : '<div style="width:48px;height:48px;background:var(--surface-2);border-radius:4px;flex-shrink:0;"></div>'}
                <div style="flex:1; min-width:0;">
                    <div style="font-weight:600; font-size:0.9rem;">${p.name}${isBundle ? ' <span style="color:var(--amber);font-size:0.7rem;font-weight:700;">⚠ BUNDLE</span>' : ''}</div>
                    <div style="font-size:0.8rem; color:var(--text-dim);">${p.setName || p.set_name || ''}${tcgId ? ' · TCG#' + tcgId : ''}</div>
                </div>
                <div style="text-align:right; flex-shrink:0;">
                    <div style="font-weight:700;">$${Number(price).toFixed(2)}</div>
                    <div style="font-size:0.7rem; color:var(--text-dim);">${src}${qty > 1 ? ' · x' + qty : ''}</div>
                </div>
            </div>`;
        }).join('');

        // If results came from cache, offer a PPT live search fallback
        const anyCache = products.some(p => p._price_source === 'cache');
        if (anyCache) {
            resultsDiv.innerHTML += `<div style="margin-top:8px; text-align:center;">
                <button class="btn btn-secondary btn-sm" onclick="searchSealedForSession('${sessionId}', ${offerPct}, true)" style="font-size:0.78rem; opacity:0.8;">
                    Don't see it? Search live →
                </button>
            </div>`;
        }
    } catch(err) {
        resultsDiv.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

async function addSealedToSession(sessionId, tcgplayerId, name, setName, price, qty, offerPct) {
    const resultsDiv = document.getElementById('session-sealed-results');
    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Adding...</div>';
    try {
        const r = await fetch('/api/intake/add-sealed-item', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: sessionId,
                product_name: name,
                set_name: setName,
                tcgplayer_id: tcgplayerId,
                market_price: price,
                quantity: qty,
            }),
        });
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }

        // Clear and reload
        document.getElementById('session-sealed-search').value = '';
        document.getElementById('session-sealed-qty').value = '1';
        resultsDiv.innerHTML = '<div class="alert alert-success">Added! ✓</div>';
        setTimeout(() => viewSession(sessionId, true), 500);
    } catch(err) {
        resultsDiv.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}
// ═══════════════════════════════ CARD SEARCH & ADD (shared by session modal + raw tab) ═══════════════════════════════

function _getCardFormIds(context) {
    if (context === 'session') return { search: 'session-raw-search', set: 'session-raw-set', tcgid: 'session-raw-tcgid', cond: 'session-raw-condition', qty: 'session-raw-qty', results: 'session-raw-results' };
    if (context === 'intake') return { search: 'intake-card-search', set: 'intake-card-set', tcgid: 'intake-card-tcgid', cond: 'intake-card-condition', qty: 'intake-card-qty', results: 'intake-search-results' };
    return { search: 'raw-card-search', set: 'raw-card-set', tcgid: 'raw-tcgid', cond: 'raw-condition', qty: 'raw-quantity', results: 'raw-search-results' };
}

function _isGradedContext(context) {
    if (context === 'intake') return document.getElementById('intake-is-graded')?.checked || false;
    if (context === 'raw') return document.getElementById('raw-is-graded')?.checked || false;
    if (context === 'session') return document.getElementById('session-is-graded')?.checked || false;
    return false;
}

function _getGradeFields(context) {
    if (context === 'intake') return { company: document.getElementById('intake-grade-company')?.value || 'PSA', value: document.getElementById('intake-grade-value')?.value || '10', badge: document.getElementById('intake-graded-badge') };
    if (context === 'session') return { company: document.getElementById('session-grade-company')?.value || 'PSA', value: document.getElementById('session-grade-value')?.value || '10', badge: null };
    return { company: document.getElementById('raw-grade-company')?.value || 'PSA', value: document.getElementById('raw-grade-value')?.value || '10', badge: document.getElementById('raw-graded-market-badge') };
}

function toggleSessionGraded() {
    const cb = document.getElementById('session-is-graded');
    const fields = document.getElementById('session-graded-fields');
    const hint = document.getElementById('session-graded-hint');
    const condGroup = document.getElementById('session-raw-cond-group');
    if (!cb || !fields) return;
    const on = cb.checked;
    fields.style.display = on ? 'inline-flex' : 'none';
    if (hint) hint.style.display = on ? 'none' : '';
    // Hide the raw condition picker when graded — graded slabs lock to NM
    if (condGroup) condGroup.style.display = on ? 'none' : '';
}

function smartCardSearch(sessionId, offerPct, context) {
    const ids = _getCardFormIds(context);
    const tcgId = (document.getElementById(ids.tcgid)?.value || '').trim();
    if (tcgId) {
        lookupByTcgId(sessionId, offerPct, context);
    } else {
        const searchTerm = document.getElementById(ids.search).value.trim();
        if (!searchTerm) { document.getElementById(ids.results).innerHTML = '<div class="alert alert-warning">Enter a card name or TCGPlayer ID</div>'; return; }
        document.getElementById(ids.results).innerHTML = '<div class="loading"><span class="spinner"></span> Searching...</div>';
        _doCardSearch(searchTerm, (document.getElementById(ids.set)?.value || '').trim(), document.getElementById(ids.results), sessionId, offerPct, context);
    }
}

async function lookupByTcgId(sessionId, offerPct, context) {
    const ids = _getCardFormIds(context);
    const tcgId = (document.getElementById(ids.tcgid)?.value || '').trim();
    const resultsDiv = document.getElementById(ids.results);
    if (!tcgId) { resultsDiv.innerHTML = '<div class="alert alert-warning">Enter a TCGPlayer ID</div>'; return; }

    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Looking up TCG#' + tcgId + '...</div>';
    const condition = document.getElementById(ids.cond).value;
    const qty = parseInt(document.getElementById(ids.qty).value) || 1;

    try {
        const lookupBody = { tcgplayer_id: parseInt(tcgId) };
        if (_isGradedContext(context)) {
            const _gf = _getGradeFields(context);
            lookupBody.grade_company = _gf.company;
            lookupBody.grade_value = _gf.value;
        }
        const r = await fetch('/api/lookup/card', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(lookupBody),
        });
        const contentType = r.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            resultsDiv.innerHTML = '<div class="alert alert-error">Server error (' + r.status + ')</div>';
            return;
        }
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = '<div class="alert alert-error">' + (d.error || 'Card not found') + '</div>'; return; }

        const card = d.card || {};
        const variants = d.variants || {};
        const gradedPrices = d.graded_prices || {};
        const liveGraded = d.live_graded || {};
        let primary = d.primary_printing || Object.keys(variants)[0] || 'Default';
        if (!variants[primary] && Object.keys(variants).length) primary = Object.keys(variants)[0];
        const variantData = variants[primary] || {};

        const setName = card.setName || card.set_name || '';
        const cardNum = card.cardNumber || card.number || '';
        const rarity = card.rarity || '';

        // Update graded badge if in graded mode
        if (gradedPrices && Object.keys(gradedPrices).length) {
            _updateGradedBadge(gradedPrices, context);
        }

        const inGradedMode = _isGradedContext(context);

        if (inGradedMode) {
            const _gf = _getGradeFields(context);
            const gradeCompany = _gf.company.toUpperCase();
            const gradeVal = _gf.value;
            const companyGrades = gradedPrices[gradeCompany] || {};
            const gradeEntry = companyGrades[gradeVal] || {};
            const gradedPrice = gradeEntry.price;
            const nmRaw = variantData['NM'];
            const safeName = (card.name||'').replace(/'/g, "\'");
            const safeSet = setName.replace(/'/g, "\'");

            // Merge live comps over cache aggregates — live is truth
            const liveCompany = liveGraded[gradeCompany] || {};

            let gradedHtml = '<div style="background:var(--surface-2); border-radius:8px; padding:14px; margin-top:8px;">';
            gradedHtml += '<div style="margin-bottom:10px;"><strong style="color:var(--accent);">' + (card.name||'') + '</strong>';
            gradedHtml += '<span style="color:var(--text-dim);"> — ' + setName + (cardNum ? ' #'+cardNum : '') + '</span></div>';

            // Collect all grades we know about (union of cache + live keys)
            const allGrades = new Set([...Object.keys(companyGrades), ...Object.keys(liveCompany)]);
            if (allGrades.size) {
                gradedHtml += '<div style="margin-bottom:12px;"><div style="font-size:0.8rem;color:var(--text-dim);margin-bottom:6px;">' + gradeCompany + ' eBay Comps</div>';
                gradedHtml += '<div style="display:flex; gap:8px; flex-wrap:wrap;">';
                [...allGrades].sort((a,b) => parseFloat(b)-parseFloat(a)).forEach(g => {
                    const live = liveCompany[g];
                    const cached = companyGrades[g] || {};
                    const isSel = g === gradeVal;
                    const price = live ? live.mid : cached.price;
                    const comps = live ? live.comps_count : cached.count;
                    const src = live ? (live.source === 'live_listings' ? 'live' : 'cache') : 'cache';
                    const srcColor = src === 'live' ? '#22c55e' : '#f59e0b';

                    gradedHtml += '<div style="background:' + (isSel ? 'var(--accent)' : 'var(--surface)') + ';color:' + (isSel ? '#fff' : 'var(--text)') + ';border:1px solid var(--border);border-radius:6px;padding:8px 12px;text-align:center;min-width:80px;">';
                    gradedHtml += '<div style="font-size:0.75rem;font-weight:600;">' + gradeCompany + ' ' + g + '</div>';
                    gradedHtml += '<div style="font-size:1rem;font-weight:700;">' + (price != null ? '$' + Number(price).toFixed(2) : '—') + '</div>';
                    if (live && live.low != null && live.high != null) {
                        gradedHtml += '<div style="font-size:0.6rem;opacity:0.7;">$' + live.low.toFixed(0) + '–$' + live.high.toFixed(0) + '</div>';
                    }
                    if (comps) gradedHtml += '<div style="font-size:0.6rem;color:' + srcColor + ';">' + comps + ' comps (' + src + ')</div>';
                    gradedHtml += '</div>';
                });
                gradedHtml += '</div></div>';
            }
            if (nmRaw) gradedHtml += '<div style="font-size:0.8rem;color:var(--text-dim);margin-bottom:10px;">Raw NM: $' + Number(nmRaw).toFixed(2) + '</div>';

            // Use live median for the selected grade if available, else cache price
            const liveSelected = liveCompany[gradeVal];
            const effectivePrice = liveSelected ? liveSelected.mid : gradedPrice;
            if (effectivePrice) {
                const offerAmt = (effectivePrice * qty * offerPct / 100).toFixed(2);
                const priceLabel = liveSelected
                    ? 'Median: $' + effectivePrice.toFixed(2) + ' (' + (liveSelected.comps_count||'?') + ' comps)'
                    : 'Market: $' + effectivePrice.toFixed(2) + ' (cache)';
                gradedHtml += '<div class="search-result" onclick="addCardFromSearch(\'' + sessionId + '\', ' + tcgId + ', \'' + safeName + '\', \'' + safeSet + '\', \'' + cardNum + '\', \'' + rarity + '\', \'' + condition + '\', ' + qty + ', ' + effectivePrice + ', \'' + context + '\')">' +
                    '<h4>Add ' + gradeCompany + ' ' + gradeVal + ' — ' + (card.name||'') + '</h4>' +
                    '<p>' + priceLabel + ' · Qty ' + qty + ' · Offer: $' + offerAmt + '</p></div>';
            } else {
                gradedHtml += '<div class="alert alert-warning">No ' + gradeCompany + ' ' + gradeVal + ' data.</div>';
                if (nmRaw) gradedHtml += '<div class="search-result" onclick="addCardFromSearch(\'' + sessionId + '\', ' + tcgId + ', \'' + safeName + '\', \'' + safeSet + '\', \'' + cardNum + '\', \'' + rarity + '\', \'NM\', ' + qty + ', ' + nmRaw + ', \'' + context + '\')">' +
                    '<h4>Add at Raw NM — ' + (card.name||'') + '</h4><p>$' + Number(nmRaw).toFixed(2) + ' · Qty ' + qty + '</p></div>';
            }
            gradedHtml += '</div>';
            resultsDiv.innerHTML = gradedHtml;
        } else {
            let price = variantData[condition] || null;
            if (!price && card.prices) price = card.prices.market || card.prices.mid || null;
            const displayPrice = price ? Number(price).toFixed(2) : 'no price';
            const offer = price ? (price * qty * offerPct / 100).toFixed(2) : '—';
            const variantNames = Object.keys(variants);
            let variantHtml = '';
            if (variantNames.length > 0) {
                variantHtml = '<div style="margin-top:8px;">' + variantNames.map(v => {
                    const vData = variants[v] || {};
                    const vPrice = vData[condition];
                    const safeName = (card.name||'').replace(/'/g, "\'");
                    const safeSet = setName.replace(/'/g, "\'");
                    return '<div class="search-result" onclick="addCardFromSearch(\'' + sessionId + '\', ' + tcgId + ', \'' + safeName + '\', \'' + safeSet + '\', \'' + cardNum + '\', \'' + rarity + '\', \'' + condition + '\', ' + qty + ', ' + (vPrice || 0) + ', \'' + context + '\')">' +
                        '<h4>' + (card.name||'') + ' — ' + v + '</h4>' +
                        '<p>' + setName + (cardNum ? ' #'+cardNum : '') + ' · ' + condition + ': ' + (vPrice ? '$'+vPrice.toFixed(2) : 'no price') + ' · Qty ' + qty + '</p></div>';
                }).join('') + '</div>';
            } else {
                const safeName = (card.name||'').replace(/'/g, "\'");
                const safeSet = setName.replace(/'/g, "\'");
                variantHtml = '<div class="search-result" onclick="addCardFromSearch(\'' + sessionId + '\', ' + tcgId + ', \'' + safeName + '\', \'' + safeSet + '\', \'' + cardNum + '\', \'' + rarity + '\', \'' + condition + '\', ' + qty + ', ' + (price || 0) + ', \'' + context + '\')">' +
                    '<h4>' + (card.name||'') + '</h4>' +
                    '<p>' + setName + ' · ' + condition + ': $' + displayPrice + ' · Qty ' + qty + '</p></div>';
            }
            resultsDiv.innerHTML = '<div class="alert alert-success"><strong>' + (card.name||'Unknown') + '</strong> — ' + setName + (cardNum ? ' #'+cardNum : '') + '</div>' + variantHtml;
        }
    } catch(err) {
        resultsDiv.innerHTML = '<div class="alert alert-error">' + err.message + '</div>';
    }
}

async function searchCardsForSession(sessionId, offerPct) {
    const searchTerm = document.getElementById('session-raw-search').value.trim();
    const setFilter = (document.getElementById('session-raw-set')?.value || '').trim();
    const resultsDiv = document.getElementById('session-raw-results');
    if (!searchTerm) { resultsDiv.innerHTML = '<div class="alert alert-warning">Enter a card name</div>'; return; }
    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Searching...</div>';
    await _doCardSearch(searchTerm, setFilter, resultsDiv, sessionId, offerPct, 'session');
}



// Shared renderer for /api/search/cards results. Both the in-session
// "Add Card" flow and the imported-item "Relink" flow consume the same
// payload — the only difference is what to do when the user clicks a
// card or one of its variant chips. Centralising rendering here keeps
// images, set/printing/game badges, and price chips identical across
// both flows so an imported item being relinked looks the same as a
// fresh card lookup.
//
// opts:
//   condition     — string used to look up per-variant prices ('NM' default)
//   qty           — qty for the offer-projection chip text (1 default)
//   offerPct      — number (0..100) shown as "offer $X.XX" alongside each
//                   variant chip; pass null to suppress (relink has no
//                   offer context)
//   inGraded      — when true, surfaces a small "PSA 10 — click variant
//                   for graded price" hint on each row
//   gradeCompany / gradeValue — only used for the graded hint label
//   onPick(pick)  — called when the row or a variant chip is clicked.
//                   `pick` is { tcgId, cardName, setName, cardNum, rarity,
//                   condition, qty, price, variant }
function _renderCardSearchResults(rawCards, container, opts) {
    opts = opts || {};
    const condition = opts.condition || 'NM';
    const qty = opts.qty || 1;
    const offerPct = (opts.offerPct == null) ? null : Number(opts.offerPct);
    const inGraded = !!opts.inGraded;
    const gradeCompany = opts.gradeCompany || 'PSA';
    const gradeValue = opts.gradeValue || '10';
    const onPick = typeof opts.onPick === 'function' ? opts.onPick : null;

    if (!rawCards || !rawCards.length) {
        container.innerHTML = '<div class="alert alert-warning">No cards found. Try a different name or set.</div>';
        container.onclick = null;
        return;
    }

    // Same cardRows shape as the original _doCardSearch render — one row
    // per printing, with variant chips inside each row.
    const cardRows = rawCards.map(c => {
        const cardTcg = c.tcgPlayerId || c.tcgplayer_id || c.id || '';
        const setName = c.setName || c.set_name || (c.set ? c.set.name : '') || '';
        const expCode = (c.expansionId || c.expansion_id || '').toUpperCase();
        const game = (c.game || '').toLowerCase();
        const cardNum = c.cardNumber || c.number || '';
        const rarity = c.rarity || '';
        const cardImg = c.imageCdnUrl400 || c.imageCdnUrl || c.imageCdnUrl800 || '';
        const prices = c.prices || {};
        const variantsObj = (prices && typeof prices === 'object' && prices.variants) || {};
        const variantNames = Object.keys(variantsObj);

        const variants = [];
        if (variantNames.length === 0) {
            let p = null;
            if (typeof prices === 'object') p = prices[condition.toLowerCase()] || prices.market || prices.mid || null;
            if (!p && c.price) p = c.price;
            variants.push({ name: '', tcgId: cardTcg, img: cardImg, price: p });
        } else {
            variantNames.forEach(vname => {
                const vData = variantsObj[vname] || {};
                const condEntry = vData[condition] || vData['Near Mint'] || vData['NM'] || {};
                const p = (typeof condEntry === 'object') ? (condEntry.price ?? null) : condEntry;
                const vImg = vData._image_small || vData._image_medium || vData._image_large || cardImg;
                const vTcg = vData._tcgplayer_id || cardTcg;
                variants.push({ name: vname, tcgId: vTcg, img: vImg, price: p });
            });
        }
        variants.sort((a, b) => (b.price || 0) - (a.price || 0));
        const maxPrice = variants.reduce((m, v) => Math.max(m, v.price || 0), 0);
        const displayImg = variants[0]?.img || cardImg;
        return { c, setName, expCode, game, cardNum, rarity, img: displayImg, variants, maxPrice };
    });
    cardRows.sort((a, b) => b.maxPrice - a.maxPrice);

    // Map of pickid -> pick data. Storing it here (not in HTML attrs)
    // keeps quoting safe and avoids re-parsing on every click.
    const picks = new Map();
    const _esc = (s) => String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

    container.innerHTML = cardRows.map((row, idx) => {
        const { c, setName, expCode, game, cardNum, rarity, img, variants } = row;
        const rowId = `_csrow_${idx}`;
        const cardName = c.name || '';

        variants.forEach((v, vi) => {
            picks.set(`${rowId}_v${vi}`, {
                tcgId: v.tcgId, cardName, setName, cardNum, rarity,
                condition, qty, price: v.price || 0, variant: v.name || '',
            });
        });
        // Row-level click defaults to the highest-priced variant (variants[0] post-sort)
        picks.set(rowId, picks.get(`${rowId}_v0`));

        const setCodeId = expCode + (cardNum ? '-' + cardNum : '');
        const setCodeBadge = expCode ? `<span style="background:var(--surface); border:1px solid var(--border); color:var(--accent); padding:2px 7px; border-radius:4px; font-size:0.72rem; font-weight:700; font-family:ui-monospace,monospace;">${_esc(setCodeId)}</span>` : '';
        const gameBadge = (game && game !== 'pokemon') ? `<span style="background:var(--accent-alt,#7c3aed); color:#fff; padding:2px 7px; border-radius:4px; font-size:0.65rem; font-weight:700;">${_esc(game.toUpperCase())}</span>` : '';
        const printingBadges = (typeof pfCardPrintingBadges === 'function') ? pfCardPrintingBadges(setName, expCode, variants) : '';

        const imgHtml = img
            ? `<img src="${_esc(img)}" loading="lazy" style="width:96px; height:134px; object-fit:contain; border-radius:6px; flex-shrink:0; background:var(--surface-2);">`
            : `<div style="width:96px; height:134px; background:var(--surface-2); border-radius:6px; flex-shrink:0;"></div>`;

        const chipsHtml = variants.map((v, vi) => {
            const pickId = `${rowId}_v${vi}`;
            const p = v.price;
            const hasPrice = p != null && p > 0;
            const isFoil = v.name && /foil|holo|etched/i.test(v.name);
            const isChase = v.name && /alt|manga|premium|special|enchanted|fullArt|jollyRoger/i.test(v.name);
            const baselineNonFoil = variants.find(x => !/foil|holo|etched/i.test(x.name||''));
            const highlight = isChase || (isFoil && variants.length > 1 && (v.price||0) > (baselineNonFoil?.price||0));
            const bg = highlight ? 'var(--amber)' : 'var(--surface-2)';
            const color = highlight ? '#000' : 'var(--text)';
            const label = v.name || 'Default';
            const icon = isFoil ? '✦ ' : '';
            const priceText = hasPrice ? '$' + Number(p).toFixed(2) : '—';
            const offerText = (hasPrice && offerPct != null) ? ' · offer $' + (p * qty * offerPct / 100).toFixed(2) : '';
            return `<span data-pickid="${pickId}" style="display:inline-flex; align-items:center; gap:6px; background:${bg}; color:${color}; padding:7px 11px; border-radius:6px; font-size:0.82rem; font-weight:600; cursor:pointer; border:1px solid var(--border); transition:filter 0.12s;" onmouseover="this.style.filter='brightness(1.15)'" onmouseout="this.style.filter=''">${icon}${_esc(label)} <span style="font-weight:700;">${priceText}</span><span style="opacity:0.7; font-size:0.72rem; font-weight:500;">${offerText}</span></span>`;
        }).join(' ');

        return `<div class="search-result" data-pickid="${rowId}" style="display:flex; gap:14px; align-items:stretch; padding:10px 12px; cursor:pointer;">
            ${imgHtml}
            <div style="flex:1; min-width:0; display:flex; flex-direction:column; justify-content:space-between; gap:8px;">
                <div>
                    <div style="display:flex; gap:6px; align-items:center; flex-wrap:wrap; margin-bottom:4px;">
                        ${gameBadge}
                        <span style="font-weight:700; font-size:0.95rem;">${_esc(cardName)}</span>
                        ${setCodeBadge}
                        ${printingBadges}
                    </div>
                    <div style="font-size:0.78rem; color:var(--text-dim);">${_esc(setName)}${rarity ? ' · '+_esc(rarity) : ''} · TCG#${_esc(picks.get(rowId).tcgId)}</div>
                    ${inGraded ? `<div style="font-size:0.75rem; color:var(--accent); margin-top:3px;">${_esc(gradeCompany)} ${_esc(gradeValue)} — click variant for graded price</div>` : ''}
                </div>
                <div style="display:flex; gap:6px; flex-wrap:wrap;">
                    ${chipsHtml}
                </div>
            </div>
        </div>`;
    }).join('');

    // Event delegation — click on a chip resolves to its pickid via
    // closest(); clicking the row outside any chip resolves to the row's
    // default pickid (highest-priced variant). Reassigning onclick (vs
    // addEventListener) means re-renders don't stack listeners.
    container.onclick = (e) => {
        const t = e.target.closest('[data-pickid]');
        if (!t || !container.contains(t)) return;
        const pick = picks.get(t.getAttribute('data-pickid'));
        if (pick && onPick) onPick(pick);
    };
}

async function _doCardSearch(searchTerm, setFilter, resultsDiv, sessionId, offerPct, context) {
    try {
        const r = await fetch('/api/search/cards', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ query: searchTerm, set_name: setFilter || undefined }),
        });
        const contentType = r.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            resultsDiv.innerHTML = `<div class="alert alert-error">Server error (${r.status})</div>`;
            return;
        }
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = `<div class="alert alert-error">${d.error}</div>`; return; }

        const cards = d.results || [];

        const ids = _getCardFormIds(context);
        const condition = document.getElementById(ids.cond)?.value || 'NM';
        const qty = parseInt(document.getElementById(ids.qty)?.value) || 1;
        const inGraded = _isGradedContext(context);
        const gf = inGraded ? _getGradeFields(context) : null;

        _renderCardSearchResults(cards, resultsDiv, {
            condition, qty, offerPct,
            inGraded,
            gradeCompany: gf?.company || 'PSA',
            gradeValue: gf?.value || '10',
            onPick: (pick) => {
                addCardFromSearch(sessionId, pick.tcgId, pick.cardName, pick.setName,
                    pick.cardNum, pick.rarity, pick.condition, pick.qty, pick.price,
                    context, pick.variant);
            },
        });
    } catch(err) {
        resultsDiv.innerHTML = `<div class="alert alert-error">${err.message}</div>`;
    }
}

async function addCardFromSearch(sessionId, tcgId, cardName, setName, cardNum, rarity, condition, qty, price, context, preselectedVariant) {
    const resultsDiv = document.getElementById(context === 'session' ? 'session-raw-results' : context === 'intake' ? 'intake-search-results' : 'raw-search-results');

    // Detail lookup to get all variant/condition prices
    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Loading pricing...</div>';

    let variants = {};
    let primaryVariant = 'Default';
    try {
        const lr = await fetch('/api/lookup/card', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tcgplayer_id: parseInt(tcgId) }),
        });
        if (lr.ok) {
            const ld = await lr.json();
            variants = ld.variants || {};
            primaryVariant = ld.primary_printing || Object.keys(variants)[0] || 'Default';
            // If primaryVariant isn't a key in variants, use first available key
            if (!variants[primaryVariant] && Object.keys(variants).length) {
                primaryVariant = Object.keys(variants)[0];
            }
            // If the user picked a specific variant in search results, honor it
            if (preselectedVariant && variants[preselectedVariant]) {
                primaryVariant = preselectedVariant;
            }
            const card = ld.card || {};
            if (card.setName && !setName) setName = card.setName;
            if (card.cardNumber && !cardNum) cardNum = card.cardNumber;
            if (card.rarity && !rarity) rarity = card.rarity;
        }
    } catch(e) { /* continue */ }

    const variantNames = Object.keys(variants);
    if (!variantNames.length) {
        _finalizeCardAdd(sessionId, tcgId, cardName, setName, cardNum, rarity, condition, qty, price, context, resultsDiv, preselectedVariant || '');
        return;
    }

    // Store pending card data and show condition picker
    window._pendingCard = { sessionId, tcgId, cardName, setName, cardNum, rarity, qty, context, variants, primaryVariant };
    _renderConditionPicker(resultsDiv, condition);
}

function _renderConditionPicker(resultsDiv, selectedCond) {
    const pc = window._pendingCard;
    if (!pc) return;
    const variants = pc.variants;
    const variantNames = Object.keys(variants);
    const selectedVariant = pc.primaryVariant;
    const variantData = variants[selectedVariant] || {};
    const qty = pc.qty;
    const divId = pc.context === 'session' ? 'session-raw-results' : pc.context === 'intake' ? 'intake-search-results' : 'raw-search-results';

    let html = '<div id="cond-picker" style="background:var(--surface-2); border-radius:8px; padding:16px; margin-top:8px;">';
    html += '<div style="margin-bottom:12px;"><strong style="color:var(--accent);">' + pc.cardName + '</strong>';
    html += '<span style="color:var(--text-dim);"> &mdash; ' + (pc.setName || '') + (pc.cardNum ? ' #' + pc.cardNum : '') + ' &bull; TCG#' + pc.tcgId + '</span></div>';

    // Variant selector
    if (variantNames.length > 1) {
        html += '<div style="margin-bottom:10px;"><label style="font-size:0.8rem; color:var(--text-dim); display:block; margin-bottom:4px;">Variant / Printing</label><div style="display:flex; gap:6px; flex-wrap:wrap;">';
        variantNames.forEach(function(v) {
            var cls = v === selectedVariant ? 'btn-primary' : 'btn-secondary';
            html += '<button class="btn btn-sm ' + cls + '" data-variant="' + v.replace(/"/g, '&quot;') + '">' + v + '</button>';
        });
        html += '</div></div>';
    } else if (variantNames.length === 1) {
        html += '<div style="margin-bottom:8px; font-size:0.85rem; color:var(--text-dim);">Printing: <strong style="color:var(--text);">' + variantNames[0] + '</strong></div>';
    }

    // Condition price grid
    html += '<div style="display:grid; grid-template-columns: repeat(5, 1fr); gap:8px; margin-bottom:12px;">';
    ['NM','LP','MP','HP','DMG'].forEach(function(cond) {
        var p = variantData[cond];
        var isSelected = cond === selectedCond;
        var hasPrice = p != null && p !== undefined;
        var bg = isSelected ? 'var(--accent)' : 'var(--surface)';
        var fg = isSelected ? '#fff' : 'var(--text)';
        html += '<div data-cond="' + cond + '" style="background:' + bg + '; color:' + fg + '; padding:10px 6px; border-radius:6px; text-align:center; cursor:pointer; border:1px solid var(--border); opacity:' + (hasPrice ? '1' : '0.4') + ';">';
        html += '<div style="font-size:0.75rem; font-weight:600;">' + cond + '</div>';
        html += '<div style="font-size:1.05rem; font-weight:700;">' + (hasPrice ? '$' + p.toFixed(2) : '&mdash;') + '</div>';
        html += '</div>';
    });
    html += '</div>';

    // Confirm + Cancel
    var selectedPrice = variantData[selectedCond];
    html += '<div style="display:flex; gap:8px; align-items:center;">';
    html += '<button id="cond-confirm-btn" class="btn btn-primary">Add ' + selectedCond + (selectedPrice ? ' &mdash; $' + selectedPrice.toFixed(2) : '') + '</button>';
    html += '<button id="cond-cancel-btn" class="btn btn-secondary">Cancel</button>';
    html += '</div></div>';

    resultsDiv.innerHTML = html;

    // Attach event listeners
    var picker = document.getElementById('cond-picker');
    // Condition clicks
    picker.querySelectorAll('[data-cond]').forEach(function(el) {
        el.addEventListener('click', function() {
            _renderConditionPicker(document.getElementById(divId), el.getAttribute('data-cond'));
        });
    });
    // Variant clicks
    picker.querySelectorAll('[data-variant]').forEach(function(el) {
        el.addEventListener('click', function() {
            window._pendingCard.primaryVariant = el.getAttribute('data-variant');
            _renderConditionPicker(document.getElementById(divId), selectedCond);
        });
    });
    // Confirm
    document.getElementById('cond-confirm-btn').addEventListener('click', function() {
        _finalizeCardAdd(pc.sessionId, pc.tcgId, pc.cardName, pc.setName, pc.cardNum, pc.rarity, selectedCond, qty, selectedPrice || 0, pc.context, null, pc.primaryVariant || '');
    });
    // Cancel
    document.getElementById('cond-cancel-btn').addEventListener('click', function() {
        document.getElementById(divId).innerHTML = '';
    });
}

async function _finalizeCardAdd(sessionId, tcgId, cardName, setName, cardNum, rarity, condition, qty, price, context, resultsDiv, variance) {
    if (!resultsDiv) resultsDiv = document.getElementById(context === 'session' ? 'session-raw-results' : context === 'intake' ? 'intake-search-results' : 'raw-search-results');

    if (!price || price <= 0) {
        const manual = await themedPrompt({
            title: 'No price found',
            message: 'No ' + condition + ' price for this card. Enter a market price:',
            inputs: [{ type: 'number', label: 'Market price ($)', min: 0, step: '0.01', placeholder: '0.00' }],
            confirmText: 'Add Card',
        });
        if (manual === null) return;
        price = parseFloat(manual);
        if (isNaN(price) || price <= 0) { alert('Invalid price'); return; }
    }

    resultsDiv.innerHTML = '<div class="loading"><span class="spinner"></span> Adding...</div>';
    try {
        // Pass graded info from the form (only relevant for manual entry in raw tab)
        const isGraded = _isGradedContext(context);
        const _gfAdd = isGraded ? _getGradeFields(context) : {};
        const gradeCompany = isGraded ? (_gfAdd.company || '') : '';
        const gradeValue = isGraded ? (_gfAdd.value || '') : '';
        const r = await fetch('/api/intake/add-raw-card', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                session_id: sessionId, tcgplayer_id: tcgId, card_name: cardName,
                condition: condition, set_name: setName, card_number: cardNum,
                rarity: rarity, quantity: qty, market_price: price,
                is_graded: isGraded, grade_company: gradeCompany, grade_value: gradeValue,
                variance: variance || '',
            }),
        });
        const d = await r.json();
        if (!r.ok) { resultsDiv.innerHTML = '<div class="alert alert-error">' + d.error + '</div>'; return; }

        var msg = 'Card added! ' + cardName + ' (' + condition + ') $' + Number(price).toFixed(2);
        if (context === 'session') {
            document.getElementById('session-raw-search').value = '';
            document.getElementById('session-raw-qty').value = '1';
            resultsDiv.innerHTML = '<div class="alert alert-success">' + msg + '</div>';
            setTimeout(function() { viewSession(sessionId, true); }, 500);
        } else if (context === 'intake') {
            document.getElementById('intake-card-search').value = '';
            document.getElementById('intake-card-set').value = '';
            document.getElementById('intake-card-qty').value = '1';
            resultsDiv.innerHTML = '<div class="alert alert-success">' + msg + '</div>';
            loadIntakeItems();
        } else {
            resultsDiv.innerHTML = '<div class="alert alert-success">' + msg + '</div>';
        }
    } catch(err) {
        resultsDiv.innerHTML = '<div class="alert alert-error">' + err.message + '</div>';
    }
}
// ═══════════════════════════════ SESSION STATUS TRANSITIONS ═══════════════════════════════
const transitionLabels = {
    offer: { title: 'Lock & Offer', msg: 'This will lock all prices and present the offer to the customer.', btn: 'Lock & Offer' },
    receive: { title: 'Product Received', msg: 'Mark the product as received. You can still make adjustments before ingest.', btn: 'Mark Received' },
    reject: { title: 'Customer Rejected', msg: 'Mark this offer as rejected. The session will be archived.', btn: 'Mark Rejected' },
    reopen: { title: 'Reopen Session', msg: 'Reopen this session for edits.', btn: 'Reopen' },
};

// ═══════════════════════════════ ACCEPT (PICKUP / MAIL) ═══════════════════════════════

// ═══════════════════════════════ ACCEPT (CASH / CREDIT) ═══════════════════════════════
//
// New shape (issue #7): customer picks one of the two offers. For
// walk-in sessions we POST without prompting for fulfillment — the
// customer's at the counter and the server short-circuits to
// 'received'. For mail/pickup sessions we still ask which mode.
async function acceptOffer(sessionId, offerType) {
    if (offerType !== 'cash' && offerType !== 'credit') return;
    // Look up walk-in via the cached session — fall back to fetching
    // current state if we don't have it (defensive for cross-tab edits).
    let isWalkIn = false;
    try {
        const r0 = await fetch('/api/intake/session/' + sessionId);
        const d0 = await r0.json();
        if (d0 && d0.session) isWalkIn = !!d0.session.is_walk_in;
    } catch (_) {}

    let body = { offer_type: offerType };
    if (!isWalkIn) {
        // Mail/pickup — same flow as the legacy accept, plus offer_type
        const choice = await themedPrompt({
            title: (offerType === 'cash' ? '💵' : '💳') + ' Accept ' + offerType.charAt(0).toUpperCase() + offerType.slice(1),
            message: 'How is the product getting to us?',
            inputs: [
                { type: 'select', label: 'Fulfillment', default: 'pickup', options: [
                    {value: 'pickup', label: 'Pickup (we go get it)'},
                    {value: 'mail',   label: 'Mail (customer ships it)'},
                ]},
                { type: 'text', label: 'Pickup date / tracking (optional)', placeholder: 'YYYY-MM-DD or tracking #' },
            ],
            confirmText: 'Confirm Accept',
        });
        if (choice === null) return;
        const [method, extra] = choice;
        body.fulfillment_method = method;
        if (method === 'mail') body.tracking_number = extra || '';
        else body.pickup_date = extra || '';
    }
    try {
        const r = await fetch('/api/intake/session/' + sessionId + '/accept', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) {
            if (window.pfBlock) pfBlock('Cannot Accept', d.error || 'Unknown error', { error: true, sound: 'error' });
            else alert(d.error);
            return;
        }
        if (window.pfSound) try { pfSound.success(); } catch(_) {}
        viewSession(sessionId, true);
        loadSessions();
    } catch (err) {
        if (window.pfBlock) pfBlock('Cannot Accept', err.message, { error: true, sound: 'error' });
        else alert(err.message);
    }
}

async function toggleWalkIn(sessionId, value) {
    try {
        const r = await fetch('/api/intake/session/' + sessionId + '/walk-in', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ is_walk_in: !!value }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed'); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

// Sticky action toolbar (issue #9). Renders the same primary action
// for the current session status at the top of the modal so staff
// doesn't have to scroll past hundreds of items to hit Accept /
// Reject. The bottom buttons stay (some staff prefer them there).
function _renderStickyActions(sessionId, s, items) {
    const status = s.status;
    const editable = !['received','ingested','cancelled','rejected'].includes(status);
    let inner = '';
    const activeItems = (items || []).filter(i => {
        const st = i.item_status || 'good';
        return st === 'good' || st === 'damaged';
    });
    const unmappedActive = activeItems.filter(i => !i.is_mapped);
    const isWalkIn = !!s.is_walk_in;
    const cashLabel = (s.total_offer_cash != null) ? '$' + Number(s.total_offer_cash).toFixed(2)
                   : '$' + Number(s.total_offer_amount || 0).toFixed(2);
    const credLabel = (s.total_offer_credit != null) ? '$' + Number(s.total_offer_credit).toFixed(2) : null;

    if (status === 'in_progress') {
        if (unmappedActive.length === 0 && activeItems.length > 0) {
            inner += `<button class="btn btn-success btn-sm" onclick="transitionSession('${sessionId}','offer')">📤 Lock & Offer (${activeItems.length})</button>`;
        } else if (unmappedActive.length > 0) {
            inner += `<span style="font-size:0.85rem;color:var(--text-dim);">${unmappedActive.length} item(s) need linking before you can offer</span>`;
        }
    } else if (status === 'offered') {
        if (credLabel) {
            inner += `<button class="btn btn-success btn-sm" onclick="acceptOffer('${sessionId}','credit')">💳 Accept Credit (${credLabel})</button>`;
        }
        inner += `<button class="btn btn-success btn-sm" onclick="acceptOffer('${sessionId}','cash')">💵 Accept Cash (${cashLabel})</button>`;
        inner += `<button class="btn btn-error btn-sm" onclick="transitionSession('${sessionId}','reject')">❌ Reject</button>`;
        if (isWalkIn) inner += `<span class="badge" style="background:#7c3aed;color:#fff;">🚪 walk-in → received</span>`;
    } else if (status === 'accepted') {
        inner += `<button class="btn btn-success btn-sm" onclick="transitionSession('${sessionId}','receive')">📦 Product Received</button>`;
        const acc = s.accepted_offer_type ? `<span class="badge" style="background:var(--green);color:#fff;">${s.accepted_offer_type.toUpperCase()}</span>` : '';
        if (acc) inner += acc;
    } else if (status === 'received') {
        inner += '<span style="color:var(--green); font-weight:600; font-size:0.85rem;">✅ Ready for Ingest</span>';
    }
    if (editable && status !== 'in_progress') {
        inner += `<button class="btn btn-secondary btn-sm" onclick="transitionSession('${sessionId}','reopen')">↩ Reopen</button>`;
    }
    // Top toolbar — Back button on the left, status-driven actions on the
    // right, Cancel folded in for editable sessions. Sticky to the page
    // top so actions stay visible while scrolling through hundreds of
    // items. The negative margins pull the bar flush with the session
    // view's outer padding.
    const cancelBtn = (editable && status === 'in_progress')
        ? `<button class="btn btn-sm" style="background:transparent;color:var(--red);border:1px solid var(--red);" onclick="cancelSession('${sessionId}')">✕ Cancel Session</button>`
        : '';
    return `
        <div class="session-actions" style="position: sticky; top: 0; z-index: 5;
             background: var(--surface);
             margin: -24px -28px 16px;
             padding: 12px 28px;
             border-bottom: 1px solid var(--border);
             display: flex; gap: 8px; flex-wrap: wrap; align-items: center;">
            <button class="btn btn-secondary btn-sm" onclick="closeSessionView()" style="margin-right:auto;">← Back to Sessions</button>
            ${inner}
            ${cancelBtn}
        </div>`;
}

// ═══════════════════════════════ ROLE LOCK / OVERRIDE — SESSION MODAL ═══════════════════════════════
function _renderRoleLockBanner(sessionId, s) {
    const role = _pfRole();
    const ov = _sessionOverrides[sessionId];
    // Managers and owners are admin-equivalent: no banners, no caps, no friction.
    if (role !== 'associate' || s.accepted_offer_type) return '';
    if (ov && ov.approver) {
        return `<div style="margin-bottom:12px; padding:8px 12px; background:var(--surface-2); border:1px solid var(--green); border-radius:6px; font-size:0.85rem;">
            <span style="color:var(--green); font-weight:600;">✓ Override:</span> ${ov.approver.name || 'Manager'} (${ov.approver.role || 'manager'}) authorized off-policy percentages on this session.
        </div>`;
    }
    return `<div style="margin-bottom:12px; padding:8px 12px; background:var(--surface-2); border:1px dashed var(--border); border-radius:6px; font-size:0.85rem; color:var(--text-dim);">
        🔒 Percentages locked at defaults (Credit ${ASSOCIATE_DEFAULTS.credit}% / Cash ${ASSOCIATE_DEFAULTS.cash}%) — manager override required to change.
    </div>`;
}

// Triggered by either percentage <input> in the session modal. Reveals
// the Save/Cancel controls when the user changes a value, and (for
// associates / out-of-cap managers) requests an override token before
// letting them save.
function onPctEdit(sessionId) {
    const cashEl = document.getElementById('pct-cash-input');
    const credEl = document.getElementById('pct-credit-input');
    const ctrls = document.getElementById('pct-edit-controls');
    if (!cashEl || !credEl || !ctrls) return;
    const dirty = (cashEl.value !== cashEl.dataset.orig) || (credEl.value !== credEl.dataset.orig);
    ctrls.style.display = dirty ? 'inline-flex' : 'none';
}

function cancelPctEdit() {
    const cashEl = document.getElementById('pct-cash-input');
    const credEl = document.getElementById('pct-credit-input');
    const ctrls = document.getElementById('pct-edit-controls');
    if (cashEl) cashEl.value = cashEl.dataset.orig;
    if (credEl) credEl.value = credEl.dataset.orig;
    if (ctrls) ctrls.style.display = 'none';
}

async function savePcts(sessionId) {
    const cashEl = document.getElementById('pct-cash-input');
    const credEl = document.getElementById('pct-credit-input');
    if (!cashEl || !credEl) return;
    const cash = parseFloat(cashEl.value);
    const credit = parseFloat(credEl.value);
    if (isNaN(cash) || isNaN(credit) || cash < 0 || cash > 100 || credit < 0 || credit > 100) {
        if (window.pfBlock) pfBlock('Bad Input', 'Enter percentages between 0 and 100.', { error: true, sound: 'error' });
        else alert('Enter percentages between 0 and 100.');
        return;
    }
    const role = _pfRole();
    let ov = _sessionOverrides[sessionId] || null;
    const need = _overrideNeeded(role, cash, credit);
    if (need !== 'ok' && !(ov && ov.token)) {
        ov = await _promptManagerPin(`A ${need} must approve these percentages.`);
        if (!ov) return;
        _sessionOverrides[sessionId] = ov;
    }
    try {
        const body = { cash_percentage: cash, credit_percentage: credit };
        if (ov && ov.token) body.override_token = ov.token;
        const r = await fetch(`/api/intake/session/${sessionId}/offer-percentage`, {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) {
            if (d.code === 'override_required') {
                // Token expired or missing — re-prompt
                delete _sessionOverrides[sessionId];
                if (window.pfBlock) pfBlock('Override Required', 'Manager approval needed for these percentages.', { error: true, sound: 'error' });
                else alert(d.error || 'Override required');
                return;
            }
            if (window.pfBlock) pfBlock('Cannot Save', d.error || 'Unknown error', { error: true, sound: 'error' });
            else alert(d.error);
            return;
        }
        if (window.pfSound) try { pfSound.success(); } catch(_) {}
        viewSession(sessionId, true);
    } catch (err) {
        if (window.pfBlock) pfBlock('Cannot Save', err.message, { error: true, sound: 'error' });
        else alert(err.message);
    }
}

// ═══════════════════════════════ ACCEPT (LEGACY PICKUP/MAIL) ═══════════════════════════════
async function acceptSession(sessionId, method) {
    let tracking = null;
    let pickupDate = null;
    if (method === 'mail') {
        tracking = await themedPrompt({
            title: '📬 Accept — Mail',
            message: 'Customer accepted. They will mail the product.',
            inputs: [{ type: 'text', label: 'Tracking number or link (optional)', placeholder: 'You can add it later' }],
            confirmText: 'Confirm Accepted',
        });
        if (tracking === null) return; // cancelled
    } else if (method === 'pickup') {
        pickupDate = await themedPrompt({
            title: '🚗 Accept — Pickup',
            message: 'Customer accepted. You will pick up the product.',
            inputs: [{ type: 'text', label: 'Pickup date', placeholder: 'e.g. 2026-03-15, or leave blank' }],
            confirmText: 'Confirm Accepted',
        });
        if (pickupDate === null) return; // cancelled
    }
    try {
        const body = { fulfillment_method: method };
        if (tracking) body.tracking_number = tracking;
        if (pickupDate) body.pickup_date = pickupDate;
        const r = await fetch(`/api/intake/session/${sessionId}/accept`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function toggleDistribution(sessionId) {
    try {
        const r = await fetch(`/api/intake/session/${sessionId}/toggle-distribution`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert(d.error || 'Failed to toggle distribution'); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function editTracking(sessionId, current) {
    const tracking = await themedPrompt({
        title: 'Edit Tracking',
        inputs: [{ type: 'text', label: 'Tracking number or URL', default: current || '' }],
        confirmText: 'Update',
    });
    if (tracking === null) return;
    try {
        const r = await fetch(`/api/intake/session/${sessionId}/tracking`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tracking_number: tracking }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function editPickupDate(sessionId, current) {
    const dateStr = await themedPrompt({
        title: 'Edit Pickup Date',
        inputs: [{ type: 'text', label: 'Pickup date (YYYY-MM-DD)', default: current || '' }],
        confirmText: 'Update',
    });
    if (dateStr === null) return;
    try {
        const r = await fetch(`/api/intake/session/${sessionId}/pickup-date`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ pickup_date: dateStr }),
        });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
    } catch(err) { alert(err.message); }
}

async function transitionSession(sessionId, action) {
    const label = transitionLabels[action] || { title: action, msg: `Transition to ${action}?`, btn: action };
    const ok = await themedConfirm(label.title, label.msg, { confirmText: label.btn });
    if (!ok) return;
    try {
        const r = await fetch(`/api/intake/session/${sessionId}/${action}`, { method: 'POST' });
        const d = await r.json();
        if (!r.ok) { alert(d.error); return; }
        viewSession(sessionId, true);
        loadSessions();
    } catch(err) { alert(err.message); }
}

async function finalizeSession(sessionId) {
    return transitionSession(sessionId, 'offer');
}

// Show the graded price badge after a card lookup returns graded_prices
function _updateGradedBadge(gradedPrices, context) {
    const _gf = _getGradeFields(context || 'raw');
    const badge = _gf.badge;
    if (!badge) return;
    const company = _gf.company.toUpperCase();
    const grade = _gf.value;
    const gd = gradedPrices?.[company]?.[grade] || {};
    const price = gd.price;
    if (price != null) {
        const conf = gd.confidence ? ` (${gd.confidence})` : '';
        badge.textContent = company + ' ' + grade + ': $' + Number(price).toFixed(2) + conf;
        badge.style.background = '';
        badge.style.display = '';
    } else {
        badge.textContent = company + ' ' + grade + ': no data';
        badge.style.display = '';
        badge.style.background = 'var(--orange, #f59e0b)';
    }
}

// ═══════════════════════════════ HEALTH CHECK ═══════════════════════════════
async function checkHealth() {
    try {
        const r = await fetch('/health');
        const d = await r.json();
        const dot = document.getElementById('health-dot');
        const txt = document.getElementById('health-text');
        if (d.status === 'healthy') {
            dot.className = 'status-dot ok';
            txt.textContent = `DB ✓ • PPT ${d.ppt === 'configured' ? '✓' : '✗'}`;
        } else {
            dot.className = 'status-dot warn';
            txt.textContent = 'Unhealthy';
        }
    } catch { /* silent */ }
}
checkHealth();
setInterval(checkHealth, 30000);

// ═══════════════════════════════════════════════════════════════
// INTAKE BREAKDOWN — SHARED MODAL WRAPPER
// ═══════════════════════════════════════════════════════════════

function openIntakeBreakdown(tcgId, productName, marketPrice) {
    openBreakdownModal({
        tcgplayerId: tcgId || null,
        productName: productName || '',
        parentMarket: parseFloat(marketPrice) || 0,
        apiBase: '/api/breakdown-cache',
        priceMode: 'best',
        onExecute: null,  // intake is recipe-only, no execution
        onSave: function(cache) {
            // Refresh breakdown badges in offer tab without full reload
            _enrichIntakeBreakdowns();
            // Patch action dropdown buttons so "Price as Breakdown" appears immediately
            if (currentSessionId && tcgId) {
                _patchBreakdownActionButtons(tcgId, cache);
            }
            // If the store tab is currently loaded, refresh it too
            const storeTab = document.getElementById('stab-store');
            if (storeTab && storeTab.style.display !== 'none' && currentSessionId) {
                storeCheck(currentSessionId);
            }
        },
    });
}

// After saving a breakdown recipe, patch the action dropdown buttons live
// so "Price as Breakdown" appears without a full viewSession reload.
function _patchBreakdownActionButtons(tcgId, cache) {
    if (!cache || !tcgId) return;
    const bdVal = parseFloat(cache.best_variant_market) || 0;
    const variantNames = cache.variant_names || 'breakdown';
    if (bdVal <= 0) return;

    // Find all rows whose item has this tcgplayer_id
    document.querySelectorAll('tr[data-item-id]').forEach(row => {
        const itemId = row.dataset.itemId;
        if (!itemId) return;

        // Check if this row belongs to the right product — find the action dropdown
        const dropdown = row.querySelector('.action-dropdown');
        if (!dropdown) return;

        // Check if a bd-price button already exists (don't double-add)
        if (dropdown.querySelector('.bd-price-btn')) return;

        // We need the sessionId — it's encoded in the existing Override Price button onclick
        const overrideBtn = [...dropdown.querySelectorAll('button')].find(b => b.textContent.includes('Override Price'));
        if (!overrideBtn) return;
        const onclickStr = overrideBtn.getAttribute('onclick') || '';
        const sidMatch = onclickStr.match(/overridePrice\('[^']+','([^']+)'/);
        if (!sidMatch) return;
        const sessionId = sidMatch[1];

        // Check this item actually has the right tcgplayer_id by looking at the TCG badge in status cell
        const statusCell = row.cells[row.cells.length - 2]; // status cell
        if (!statusCell || !statusCell.textContent.includes(`TCG#${tcgId}`)) return;

        // Find the qty — look for Change Qty button
        const qtyBtn = [...dropdown.querySelectorAll('button')].find(b => b.textContent.includes('Change Qty'));
        const qtyMatch = qtyBtn ? (qtyBtn.textContent.match(/\((\d+)\)/) || [])[1] : '1';
        const qty = parseInt(qtyMatch) || 1;

        // Inject the breakdown button before the breakdown recipe button
        const recipeBtn = [...dropdown.querySelectorAll('button')].find(b => b.textContent.includes('Breakdown Recipe'));
        const bdBtn = document.createElement('button');
        bdBtn.className = 'bd-price-btn';
        bdBtn.textContent = `💲 Price as Breakdown ($${bdVal.toFixed(2)})`;
        bdBtn.onclick = () => applyBreakdownPriceWithQty(itemId, sessionId, bdVal, variantNames, qty);
        if (recipeBtn) {
            dropdown.insertBefore(bdBtn, recipeBtn);
        } else {
            dropdown.appendChild(bdBtn);
        }
    });
}

// ═══════════════════════════════ COLLECTION META STATS ═══════════════════════════════
//
// Populates #meta-stats-panel inside the session view with category-
// level breakdowns + collection-wide totals. Categories are server-
// classified from Shopify tags (with name fallback) — see
// shared/product_categorize.py and the /meta-stats endpoint.

function _fmt$(n) {
    const v = Number(n) || 0;
    return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function _fmt$short(n) {
    const v = Number(n) || 0;
    if (v >= 10000) return '$' + Math.round(v).toLocaleString('en-US');
    return _fmt$(v);
}
function _fmtPct(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) return '—';
    return v.toFixed(1) + '%';
}
function _marginColor(pct) {
    const v = Number(pct);
    if (!Number.isFinite(v)) return 'var(--text-dim)';
    if (v >= 30) return 'var(--green)';
    if (v >= 15) return 'var(--amber)';
    return 'var(--red)';
}

async function loadMetaStats(sessionId) {
    const panel = document.getElementById('meta-stats-panel');
    if (!panel) return;
    panel.innerHTML = '<div class="loading" style="padding:8px;"><span class="spinner"></span> Calculating breakdown…</div>';
    try {
        const r = await fetch('/api/intake/session/' + encodeURIComponent(sessionId) + '/meta-stats');
        const d = await r.json();
        if (!r.ok) {
            panel.innerHTML = '<div class="alert alert-warning" style="font-size:0.8rem;">Could not compute breakdown: ' + (d.error || 'unknown') + '</div>';
            return;
        }
        renderMetaStats(panel, d);
    } catch (e) {
        panel.innerHTML = '<div class="alert alert-warning" style="font-size:0.8rem;">Breakdown unavailable: ' + e.message + '</div>';
    }
}

function renderMetaStats(panel, data) {
    const t = data.totals || {};
    const cats = data.categories || [];
    if (!cats.length) {
        panel.innerHTML = '<div class="alert alert-warning" style="font-size:0.85rem;">No items in this session yet.</div>';
        return;
    }

    // Sell value = best-of(store, breakdown, market) per item. Margin is
    // computed against that, so the headline answers 'is this collection
    // a good buy if I play it optimally?'
    const headerStats = `
        <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)); gap:12px; padding:14px; background:var(--surface-2); border:1px solid var(--border); border-radius:8px 8px 0 0; border-bottom:none;">
            <div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">SKUs</div><div style="font-size:1.1rem; font-weight:700;">${t.sku_count || 0}</div>${t.in_store_count != null ? `<div style="font-size:0.72rem; color:var(--text-dim);">${t.in_store_count} in store · ${_fmtPct(t.in_store_pct)}</div>` : ''}</div>
            <div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">Units</div><div style="font-size:1.1rem; font-weight:700;">${t.qty || 0}</div></div>
            <div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">Market</div><div style="font-size:1.1rem; font-weight:700;">${_fmt$short(t.market_value)}</div></div>
            <div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">Sell Value (best)</div><div style="font-size:1.1rem; font-weight:700;">${_fmt$short(t.sell_value)}</div>${t.store_listed_value > 0 ? `<div style="font-size:0.72rem; color:var(--text-dim);">${_fmt$short(t.store_listed_value)} listed · rest est.</div>` : ''}</div>
            ${t.breakdown_value > 0 ? `<div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">Breakdown Sell</div><div style="font-size:1.1rem; font-weight:700;">${_fmt$short(t.breakdown_value)}</div><div style="font-size:0.72rem; color:var(--text-dim);">${t.items_with_breakdown} item${t.items_with_breakdown===1?'':'s'} have BD</div></div>` : ''}
            <div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">Cash Margin</div><div style="font-size:1.1rem; font-weight:700; color:${_marginColor(t.margin_cash_pct)};">${_fmtPct(t.margin_cash_pct)}</div><div style="font-size:0.72rem; color:var(--text-dim);">paid ${_fmt$short(t.cash_offer)}</div></div>
            <div><div style="color:var(--text-dim); font-size:0.7rem; text-transform:uppercase; letter-spacing:0.04em;">Credit Margin</div><div style="font-size:1.1rem; font-weight:700; color:${_marginColor(t.margin_credit_pct)};">${_fmtPct(t.margin_credit_pct)}</div><div style="font-size:0.72rem; color:var(--text-dim);">paid ${_fmt$short(t.credit_offer)}</div></div>
        </div>`;

    // Detail header bar — sub-table of items inside an expanded
    // category. Its own headers so columns line up under labels rather
    // than under the parent table's headers (which mean different
    // things at the per-item level).
    const detailHead = `
        <thead>
            <tr style="background:var(--surface); color:var(--text-dim); font-size:0.72rem; text-transform:uppercase; letter-spacing:0.04em;">
                <th style="padding:5px 12px 5px 32px; text-align:left;">Product</th>
                <th style="padding:5px 8px; text-align:right;">Qty</th>
                <th style="padding:5px 8px; text-align:right;">Market ea</th>
                <th style="padding:5px 8px; text-align:right;">Store ea</th>
                <th style="padding:5px 8px; text-align:right;">BD ea</th>
                <th style="padding:5px 8px; text-align:right;">Best Strategy</th>
                <th style="padding:5px 12px; text-align:right;">Paid</th>
            </tr>
        </thead>`;

    const rows = cats.map((c, ci) => {
        const pct = Number(c.share_of_market_pct) || 0;
        const barWidth = Math.min(100, pct);
        const storeBadge = c.in_store_count > 0
            ? `<span style="font-size:0.7rem; color:var(--text-dim);">${c.in_store_count}/${c.sku_count} in store</span>`
            : `<span style="font-size:0.7rem; color:var(--amber);">est. (none in store)</span>`;
        const bdBadge = c.items_with_breakdown > 0
            ? `<span style="font-size:0.7rem; color:var(--accent); margin-left:6px;">${c.items_with_breakdown} BD</span>`
            : '';

        const detailRows = (c.items || []).map(it => {
            const stratColor = it.best_strategy === 'breakdown' ? 'var(--accent)'
                            : it.best_strategy === 'store'     ? 'var(--green)'
                            :                                    'var(--text-dim)';
            const stratLabel = it.best_strategy === 'breakdown' ? '✦ break down'
                            : it.best_strategy === 'store'     ? '🏪 sell sealed'
                            :                                    'market est.';
            return `<tr style="background:var(--surface-2); font-size:0.78rem;">
                <td style="padding:4px 12px 4px 32px;">${it.name || '?'}</td>
                <td style="padding:4px 8px; text-align:right; color:var(--text-dim);">${it.qty}</td>
                <td style="padding:4px 8px; text-align:right;">${_fmt$(it.market_price)}</td>
                <td style="padding:4px 8px; text-align:right;">${it.store_price != null ? _fmt$(it.store_price) : '<span style="color:var(--text-dim);">—</span>'}</td>
                <td style="padding:4px 8px; text-align:right;">${it.breakdown_price != null ? _fmt$(it.breakdown_price) : '<span style="color:var(--text-dim);">—</span>'}</td>
                <td style="padding:4px 8px; text-align:right; color:${stratColor};">${stratLabel}</td>
                <td style="padding:4px 12px; text-align:right; color:var(--text-dim);">${_fmt$(it.offer_price)}</td>
            </tr>`;
        }).join('');

        return `<tr class="meta-cat-row" data-ci="${ci}" style="cursor:pointer;">
                <td style="padding:6px 12px;"><span class="meta-twirl" style="display:inline-block; width:12px; color:var(--text-dim);">▸</span> <strong>${c.label}</strong><br>${storeBadge}${bdBadge}</td>
                <td style="padding:6px 12px; text-align:right; color:var(--text-dim);">${c.sku_count}</td>
                <td style="padding:6px 12px; text-align:right; color:var(--text-dim);">${c.qty}</td>
                <td style="padding:6px 12px; text-align:right;">${_fmt$short(c.market_value)}</td>
                <td style="padding:6px 12px; min-width:120px;">
                    <div style="display:flex; align-items:center; gap:8px;">
                        <div style="flex:1; height:6px; background:var(--surface); border-radius:3px; overflow:hidden;">
                            <div style="height:100%; width:${barWidth}%; background:var(--accent);"></div>
                        </div>
                        <span style="font-size:0.78rem; color:var(--text-dim); width:42px; text-align:right;">${_fmtPct(pct)}</span>
                    </div>
                </td>
                <td style="padding:6px 12px; text-align:right;">${_fmt$short(c.sell_value)}</td>
                <td style="padding:6px 12px; text-align:right;">${c.breakdown_value > 0 ? _fmt$short(c.breakdown_value) : '<span style="color:var(--text-dim);">—</span>'}</td>
                <td style="padding:6px 12px; text-align:right; color:${_marginColor(c.margin_cash_pct)};">${_fmtPct(c.margin_cash_pct)}</td>
                <td style="padding:6px 12px; text-align:right; color:${_marginColor(c.margin_credit_pct)};">${_fmtPct(c.margin_credit_pct)}</td>
            </tr>
            <tr class="meta-cat-detail" data-ci="${ci}" style="display:none;">
                <td colspan="9" style="padding:0;">
                    <table style="width:100%; border-collapse:collapse;">
                        ${detailHead}
                        <tbody>
                            ${detailRows || '<tr><td colspan="7" style="padding:6px 24px; font-size:0.78rem; color:var(--text-dim);">No items.</td></tr>'}
                        </tbody>
                    </table>
                </td>
            </tr>`;
    }).join('');

    panel.innerHTML = `
        <div style="background:var(--surface-2); border:1px solid var(--border); border-radius:8px;">
            ${headerStats}
            <div style="overflow-x:auto;">
                <table style="width:100%; font-size:0.85rem; border-collapse:collapse; background:var(--surface);">
                    <thead style="background:var(--surface-2);">
                        <tr style="text-align:left; color:var(--text-dim); font-size:0.72rem; text-transform:uppercase; letter-spacing:0.04em;">
                            <th style="padding:8px 12px;">Category</th>
                            <th style="padding:8px 12px; text-align:right;">SKUs</th>
                            <th style="padding:8px 12px; text-align:right;">Units</th>
                            <th style="padding:8px 12px; text-align:right;">Market</th>
                            <th style="padding:8px 12px;">Share</th>
                            <th style="padding:8px 12px; text-align:right;">Sell (best)</th>
                            <th style="padding:8px 12px; text-align:right;">Breakdown</th>
                            <th style="padding:8px 12px; text-align:right;">Cash Margin</th>
                            <th style="padding:8px 12px; text-align:right;">Credit Margin</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>`;

    panel.querySelectorAll('.meta-cat-row').forEach(row => {
        row.addEventListener('click', () => {
            const ci = row.dataset.ci;
            const detail = panel.querySelector(`.meta-cat-detail[data-ci="${ci}"]`);
            if (!detail) return;
            const open = detail.style.display !== 'none';
            detail.style.display = open ? 'none' : '';
            const twirl = row.querySelector('.meta-twirl');
            if (twirl) twirl.textContent = open ? '▸' : '▾';
        });
    });
}

// Enrich offer tab rows with live breakdown summary badges (uses shared renderBreakdownBadge)
async function _enrichIntakeBreakdowns(items, sessionId) {
    // items/sessionId optional — if not passed, use current session
    const sid = sessionId || currentSessionId;
    if (!sid) return;

    // If items not passed, fetch them
    let allItems = items;
    if (!allItems) {
        try {
            const r = await fetch(`/api/intake/session/${sid}`);
            const d = await r.json();
            allItems = d.items || [];
        } catch(e) { return; }
    }

    const tcgIds = [...new Set(allItems
        .filter(i => i.tcgplayer_id && i.is_mapped)
        .map(i => i.tcgplayer_id))];
    if (!tcgIds.length) return;

    try {
        const r = await fetch('/api/breakdown-cache/batch', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ tcgplayer_ids: tcgIds }),
        });
        const d = await r.json();
        const summaries = d.summaries || {};

        allItems.forEach(item => {
            const bd = summaries[item.tcgplayer_id];
            const cell = document.querySelector(`tr[data-item-id="${item.id}"] td.name-cell`);
            if (!cell) return;

            // Remove any old badges + pickers
            cell.querySelectorAll('.bd-badge, .bd-baked, .bd-summary-badge, .bd-live-badge, .bd-variant-picker').forEach(e => e.remove());
            if (!bd || !bd.best_variant_market) return;

            const parentInfo = { market_price: item.market_price, claimed_variant_id: item.claimed_variant_id };
            renderBreakdownBadge(cell, bd, parentInfo, { priceMode: 'best' });
            // Variant claim picker — only renders for probabilistic recipes w/ >1 variant
            if (typeof renderVariantClaimPicker === 'function') {
                renderVariantClaimPicker(cell, bd, item.claimed_variant_id || null, async (newVid) => {
                    try {
                        await fetch(`/api/intake/item/${item.id}/claim-variant`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ variant_id: newVid }),
                        });
                        // Refresh the session view so margins recalc against the new claim.
                        if (typeof viewSession === 'function' && currentSessionId) {
                            viewSession(currentSessionId);
                        } else {
                            _enrichIntakeBreakdowns();
                        }
                    } catch (e) { /* swallow — UI revert on failure handled by next refresh */ }
                });
            }
        });
    } catch(e) { /* silent */ }
}
