# Agent Spec — ingestion service (data ingestion / warehouse processing)

## Mission
Three issues in `apps/ingestion/` (ingest.pack-fresh.com), all from real
brick-and-mortar usage on 2026-04-29. These don't conflict on the same lines,
so you can work them in any order — but commit each separately.

## Repo & Service Context

- Working dir: `apps/ingestion/`  ← **ingest service** despite the name
- Domain: ingest.pack-fresh.com
- Auth: custom `require_auth` in app.py — owner / manager / associate
- Stack: Flask + single-page `templates/ingest_dashboard.html` (huge file —
  search by feature, don't try to grok it whole)
- Per-service notes: `apps/ingestion/CLAUDE.md`
- Root rules: `apps/CLAUDE.md` — read this, Sean's working style is strict.
  In particular: no summaries, no half-measures, no theoretical "you'll need
  to also do X" — code must be deployable immediately.

## Phase 1 primitives already shipped (use these, don't reinvent)

- `shared/static/pf_ui.js` exposes:
  - `pfBlock(title, msg, {error, sound, okText})` — blocking modal, must be
    acknowledged before next action. **Use this** for every wrong-scan /
    wrong-routing / invalid error path. No more error toasts for these.
  - `pfSound.error()`, `pfSound.success()`, `pfSound.notify()` — Web Audio
    beeps, no asset files. Auto-fired by `pfBlock({error:true})`.
- `window._pfUser = {name, role, email, user_id}` available everywhere via
  the shared admin bar.

## Pre-Phase-2 schema already migrated

`intake_sessions` now has these columns (sibling to existing
`offer_percentage`):

- `cash_percentage` DECIMAL(5,2)
- `credit_percentage` DECIMAL(5,2)
- `accepted_offer_type` VARCHAR(10)  — 'cash' or 'credit'

Existing rows have `cash_percentage` backfilled from `offer_percentage`;
`credit_percentage` is NULL on legacy rows. The view
`intake_session_summary` already exposes these. The ingest-service agent
owns the **writer** side of these columns — your job (#7a) is to **read
and display** them in ingestion's UI alongside whatever is currently
shown.

---

## Issues

### Issue #7a — Display cash + credit offer percentages

**Where the data lives**: `intake_sessions.cash_percentage`,
`credit_percentage`, `accepted_offer_type`. JOIN the view
`intake_session_summary` or the base table.

**What to do**: anywhere the current `offer_percentage` is shown in
ingestion's UI (session header, queue cards, breakdown summary headers,
push preview, etc.), surface BOTH percentages plus which type was
accepted. Pattern:

> Cash 65% · Credit 75% · **Accepted: Credit**

Where `accepted_offer_type` is NULL (legacy session or not yet accepted),
fall back gracefully — show whatever single percentage exists and don't
crash.

**Acceptance**:
- Every place currently rendering `offer_percentage` in templates/JS
  also renders the cash + credit pair when both columns are populated
- Legacy sessions (only `offer_percentage` filled, the others NULL)
  still render correctly without "undefined" / "NaN" / errors
- No writes — read-only consumer of the new columns

**Out of scope for this agent**: changing how the offer is calculated,
adding UI to capture both percentages on intake. Those belong to the
ingest-service agent.

---

### Issue #10 — Verify-stage thumbnails don't render on first paint

**Symptom**: First load of a session lands on the Verify tab; image
thumbnails show as broken/empty. Switching to Breakdown tab and back
forces them to load.

**Root cause to investigate**: likely one of —
1. Lazy-load IntersectionObserver wired up before the DOM nodes exist on
   first render
2. `<img loading="lazy">` with placeholder src that's never swapped on
   the initial render path
3. Initial render happens before the data fetch resolves and a stale
   no-image state is committed

Read `apps/ingestion/templates/ingest_dashboard.html`'s Verify-stage
render path and find the actual cause. **Fix the root cause**, don't
band-aid with a setTimeout.

**Acceptance**:
- Open a session, land on Verify, all card thumbs render in the same
  paint as the row text
- No tab-switching workaround required
- Doesn't regress lazy-loading on long sessions (100+ items)

---

### Issue #11 — Routing state machine rewrite

**Current behavior** (broken): when routing starts, the page pre-loads
the first card and waits. Scanning the first card "doesn't do anything"
— it gets dropped on the floor. Subsequent scans work normally.

**Target behavior** — clear beginning / middle / end:

1. **Empty start state**: routing screen shows no card. Header says
   "Scan a card to get routing." Big scan input, auto-focused.
2. **First scan**: looks up the card. Shows it with the **default
   destination preselected** based on price:
   - Routing destinations are **B** (Binder), **S** (Storage), **G**
     (Grade), **K** (bulK)
   - Defaults computed from current_price tiers (existing logic — find
     it and keep it; do not change the rules)
   - Staff can press B/S/G/K (keyboard) or click to override before
     scanning the next card
3. **Each subsequent scan**: COMMITS the currently-displayed card to
   whatever destination is selected, then loads the next card with its
   default selected. The scan event itself is the commit signal — no
   separate confirm button per card.
4. **End state**: after the last expected card, no more scans coming.
   Show a clear "Done" button (or accept Enter on an empty scan input)
   that commits the displayed card and ends the routing session.

**Error paths** (use `pfBlock({error:true})` for all of these):
- Scanned a barcode that's not in this session's routing batch:
  blocking modal "Wrong card — this barcode isn't in this session."
- Scanned a barcode that was already routed: "Already routed to {dest}."
- Database failure on commit: "Couldn't save routing — try again."

**Acceptance**:
- First scan registers (does not get dropped)
- Each scan commits the previous card with the selected destination
- Last card commits via Done button or empty-Enter
- Wrong/already-routed scans are blocking modals with the error sound,
  not silent or toasts
- Defaults still pre-fill from price tiers (don't change the rule, just
  the state machine)

**Files**: routing logic lives in `app.py` and the routing pane in
`templates/ingest_dashboard.html`. There's also a `migrate_raw_routing.py`
in the dir for context — don't change schema unless absolutely necessary.

---

## Files you will likely touch

- `apps/ingestion/app.py` — session/queue endpoints (#7a read), routing
  endpoints (#11)
- `apps/ingestion/templates/ingest_dashboard.html` — every spot rendering
  `offer_percentage` (#7a), Verify-stage render path (#10), routing pane
  (#11)
- `apps/ingestion/ingest.py` — possibly touched by #11 if routing logic
  lives there

## What NOT to do

- Don't write a separate routing service. Fix the state machine in place.
- Don't change the default-destination price tiers — those are existing
  business logic.
- No backwards-compat shims for #7a — Sean's running this in prod, just
  read the new columns and gracefully handle NULL.
- No summaries at end of work. Each commit message tells the story.

## Deploy & ship

- Push directly to `main`. Each issue is its own commit.
- Railway watches `apps/ingestion/` paths and auto-deploys.
- Sean monitors deploys live; do not ask if changes are deployed.
