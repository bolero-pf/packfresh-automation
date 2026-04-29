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


/* ───────────────────────────────────────────────────────────
   pfSound — Web Audio API beeps. No file assets needed.
   Browsers block autoplay until first user gesture; calls before
   that resolve silently (visual feedback still fires).
   ─────────────────────────────────────────────────────────── */
var pfSound = (function () {
  var ctx = null;
  function _ctx() {
    if (ctx) return ctx;
    try {
      var Ctor = window.AudioContext || window.webkitAudioContext;
      if (!Ctor) return null;
      ctx = new Ctor();
      return ctx;
    } catch (e) { return null; }
  }

  function _tone(freq, duration, type, gain, when) {
    var c = _ctx();
    if (!c) return;
    if (c.state === 'suspended') { try { c.resume(); } catch (e) {} }
    var osc = c.createOscillator();
    var g = c.createGain();
    osc.type = type || 'sine';
    osc.frequency.value = freq;
    var t0 = (c.currentTime + (when || 0));
    g.gain.setValueAtTime(0.0001, t0);
    g.gain.exponentialRampToValueAtTime(gain || 0.18, t0 + 0.01);
    g.gain.exponentialRampToValueAtTime(0.0001, t0 + duration);
    osc.connect(g); g.connect(c.destination);
    osc.start(t0);
    osc.stop(t0 + duration + 0.02);
  }

  return {
    /* Sharp descending two-tone — "you did the wrong thing". */
    error:   function () { _tone(440, 0.12, 'square', 0.22, 0);
                           _tone(180, 0.22, 'square', 0.22, 0.10); },
    /* Rising chirp — "scan accepted / action committed". */
    success: function () { _tone(660, 0.08, 'sine', 0.18, 0);
                           _tone(990, 0.12, 'sine', 0.18, 0.07); },
    /* Single warm bell — "new hold dropped, look at me". */
    notify:  function () { _tone(880, 0.18, 'triangle', 0.22, 0);
                           _tone(660, 0.22, 'triangle', 0.18, 0.16); }
  };
})();


/* ───────────────────────────────────────────────────────────
   pfBlock(title, message, opts) — single-OK blocking modal.
   Use INSTEAD OF toast() when the user must acknowledge before
   continuing (e.g. wrong-bin scan, wrong-hold scan, wrong-card scan).
     opts.okText   — button label, default 'OK'
     opts.error    — true ⇒ red border + error sound + shake
     opts.sound    — override which pfSound to play ('error'|'notify'|'success'|null)
   Returns Promise<void> that resolves after OK is clicked.
   ─────────────────────────────────────────────────────────── */
function pfBlock(title, message, opts) {
  opts = opts || {};
  var okText  = opts.okText || 'OK';
  var isError = !!opts.error;
  var soundName = (opts.sound !== undefined)
    ? opts.sound
    : (isError ? 'error' : null);

  var overlay = document.getElementById('pf-block-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'pf-block-overlay';
    overlay.className = 'pf-block-overlay';
    overlay.innerHTML =
      '<div class="pf-block-box" id="pf-block-box">' +
        '<div class="pf-block-title" id="pf-block-title"></div>' +
        '<div class="pf-block-msg" id="pf-block-msg"></div>' +
        '<div class="pf-block-btns">' +
          '<button class="btn btn-primary" id="pf-block-ok"></button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);
  }

  var box = document.getElementById('pf-block-box');
  document.getElementById('pf-block-title').textContent = title || '';
  document.getElementById('pf-block-msg').textContent   = message || '';

  var okBtn = document.getElementById('pf-block-ok');
  okBtn.textContent = okText;
  okBtn.className = isError ? 'btn btn-danger' : 'btn btn-primary';

  box.classList.toggle('pf-block-error', isError);
  overlay.classList.add('active');
  if (isError) {
    box.classList.remove('pf-block-shake');
    void box.offsetWidth;
    box.classList.add('pf-block-shake');
  }
  if (soundName && pfSound[soundName]) { try { pfSound[soundName](); } catch (e) {} }

  setTimeout(function () { try { okBtn.focus(); } catch (e) {} }, 30);

  return new Promise(function (resolve) {
    function cleanup() {
      overlay.classList.remove('active');
      okBtn.onclick = null;
      document.removeEventListener('keydown', onKey, true);
      resolve();
    }
    function onKey(ev) {
      // Enter / Space / Escape all confirm — there's only one option anyway,
      // and a scanner sending Enter at end-of-scan should dismiss cleanly.
      if (ev.key === 'Enter' || ev.key === ' ' || ev.key === 'Escape') {
        ev.preventDefault();
        ev.stopPropagation();
        cleanup();
      }
    }
    okBtn.onclick = cleanup;
    document.addEventListener('keydown', onKey, true);
  });
}
