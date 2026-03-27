/* ───────────────────────────────────────────────────────────
   Pack Fresh — Shared UI Utilities
   toast(), themedConfirm(), esc()
   Loaded by all dark-theme staff services via /pf-static/pf_ui.js
   ─────────────────────────────────────────────────────────── */

/**
 * HTML-escape a string for safe insertion into the DOM.
 */
function esc(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

/**
 * Show a toast notification that auto-dismisses after 3.5 s.
 *
 * @param {string} msg  — text to display (HTML-escaped internally)
 * @param {'green'|'red'|'amber'|'accent'} [type='accent'] — icon/color
 */
function toast(msg, type) {
  const el = document.createElement('div');
  el.className = 'pf-toast';

  const icons = { green: '\u2713', red: '\u2715', amber: '\u26A0', accent: '\u2139' };
  const vars  = { green: 'var(--green)', red: 'var(--red)', amber: 'var(--amber)', accent: 'var(--accent)' };
  const t = type && vars[type] ? type : 'accent';

  el.innerHTML = '<span style="color:' + vars[t] + ';font-size:1rem;">' + icons[t] + '</span> ' + esc(msg);
  document.body.appendChild(el);
  setTimeout(function () { el.remove(); }, 3500);
}

/**
 * Promise-based confirm dialog styled to the dark theme.
 * Injects its own DOM on first call.
 *
 * @param {string} title
 * @param {string} message
 * @param {Object} [opts]
 * @param {string} [opts.confirmText='Confirm']
 * @param {string} [opts.cancelText='Cancel']
 * @param {boolean} [opts.dangerous=false]  — makes OK button red
 * @returns {Promise<boolean>}
 */
function themedConfirm(title, message, opts) {
  opts = opts || {};
  var confirmText = opts.confirmText || 'Confirm';
  var cancelText  = opts.cancelText  || 'Cancel';
  var dangerous   = !!opts.dangerous;

  // Ensure the overlay exists in the DOM
  var overlay = document.getElementById('pf-confirm-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'pf-confirm-overlay';
    overlay.className = 'pf-confirm-overlay';
    overlay.innerHTML =
      '<div class="pf-confirm-box">' +
        '<div class="pf-confirm-title" id="pf-confirm-title"></div>' +
        '<div class="pf-confirm-msg" id="pf-confirm-msg"></div>' +
        '<div class="pf-confirm-btns">' +
          '<button class="btn btn-ghost btn-sm" id="pf-confirm-cancel"></button>' +
          '<button class="btn btn-sm" id="pf-confirm-ok"></button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);
  }

  document.getElementById('pf-confirm-title').textContent = title;
  document.getElementById('pf-confirm-msg').textContent = message;

  var okBtn = document.getElementById('pf-confirm-ok');
  okBtn.textContent = confirmText;
  okBtn.className = dangerous ? 'btn btn-danger btn-sm' : 'btn btn-primary btn-sm';

  document.getElementById('pf-confirm-cancel').textContent = cancelText;

  overlay.classList.add('active');

  return new Promise(function (resolve) {
    function cleanup(val) {
      overlay.classList.remove('active');
      okBtn.onclick = null;
      document.getElementById('pf-confirm-cancel').onclick = null;
      resolve(val);
    }
    okBtn.onclick = function () { cleanup(true); };
    document.getElementById('pf-confirm-cancel').onclick = function () { cleanup(false); };
  });
}
