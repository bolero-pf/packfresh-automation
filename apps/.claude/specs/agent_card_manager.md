# Agent Spec — card_manager Bug Fixes + Hold Queue Refactor

## Mission
Six issues in `apps/card_manager/` (cards.pack-fresh.com), all from real
brick-and-mortar usage on 2026-04-29. Tackle in **strict order** — the data
integrity bugs (#4, #5) come first because they're causing real mis-fulfillment
right now, then the polling fix that's eating staff focus, then UX/feature work.

Do not parallelize within this agent — they all touch the same templates and
will collide.

## Repo & Service Context

- Working dir: `apps/card_manager/`
- Domain: cards.pack-fresh.com
- Auth: JWT via `register_auth_hooks()`, any authenticated user (no role gating)
- Stack: Flask + single-page HTML template
- Per-service notes: `apps/card_manager/CLAUDE.md`
- Root rules: `apps/CLAUDE.md` (read this — Sean's working style is strict)

## Phase 1 primitives already shipped (use these, don't reinvent)

- `shared/static/pf_ui.js` exposes:
  - `pfBlock(title, msg, {error, sound, okText})` — blocking modal, single OK
    button, plays the configured sound, returns `Promise<void>`. **Use this**
    for every wrong-scan / wrong-bin / wrong-card error path. Toasts are no
    longer acceptable for these — staff must acknowledge before scanning the
    next card.
  - `pfSound.error()`, `pfSound.success()`, `pfSound.notify()` — Web Audio
    beeps, no asset files.
  - Existing: `toast()`, `themedConfirm()`, `esc()`.
- `window._pfUser = {name, role, email, user_id}` — set by the shared admin
  bar, available on every page after the bar's inline script runs.

## Issue Order (strict)

### 1. (Issue #4) Multi-copy barcode identity leak — DATA INTEGRITY

**Symptom**: When a hold has multiple copies of the same card+condition (e.g.
4× NM Pikachu), clicking "Return" or "Accept" on a specific row in the UI
sometimes operates on a *different* physical copy than the one the user
clicked.

**Root cause** (working theory — verify by reading the code): the picking flow
correctly accepts ANY matching barcode for a card+condition equivalence class
(the customer doesn't care which of 4 NM Pikachus they get). That loose
identity is leaking into accept/reject/return handlers — they're matching by
card identity (name/condition/etc.) instead of the exact barcode that was
scanned/clicked.

**Required fix**: every accept/reject/return action must operate on the exact
`raw_cards.barcode` that's bound to that UI row, even if multiple rows share a
card identity. The picking step is the only place loose-match is correct;
once a specific copy is scanned/picked, downstream actions must lock in that
barcode.

**Acceptance**:
- Returning card row 2 of 4 NM Pikachus removes that exact barcode from the
  hold, leaves the other 3 untouched.
- Accepting one of N copies pushes a Shopify listing for that exact barcode
  only.
- No code path resolves a card by `name + condition` for an action — only for
  picking.

### 2. (Issue #5) Duplicate Shopify listing from accept → return → accept

**Symptom**: Customer flow: accept a card, change mind → return, change mind
again → accept. On push, **two identical Shopify listings** were created
instead of one.

**Root cause** (likely): the accept handler enqueues an item for push without
checking if the same barcode is already queued; toggling state cycles back to
"accept" and re-enqueues. Or: the return doesn't actually remove the queued
item, just hides it. Read the push pipeline to confirm.

**Required fix**: state must be at most one of {accepted, returned, pending}
per barcode. Toggling reuses the same row, never creates a second one. Push
deduplicates by barcode as a defense-in-depth.

**Acceptance**:
- Accept → return → accept → push creates exactly 1 listing.
- Existing duplicate listings: don't auto-clean. Surface a warning in logs
  during push if duplicates are detected so Sean can manually clean.

### 3. (Issue #6) Polling re-renders the whole page, blowing up scan focus

**Symptom**: The 15s poll that keeps the queue counts fresh is doing a full
re-render of the entire dashboard. Staff loses scan focus, in-progress
forms reset, scroll position jumps. The earlier "scanning a wrong card lost
my place" complaint is likely the same root cause when an error toast also
triggered a refresh.

**Required fix**: the poll must only update the count badges (Pending /
Returns / etc.) and any new-arrival indicator. It must not touch:
- The scan input (preserve `document.activeElement` and selection state)
- Any open modals
- Form state in any card row
- Scroll position

The right pattern is a small endpoint that returns just the counts (and
maybe a "new since" timestamp), and a render function that updates those
specific DOM nodes by ID. If the queue *contents* changed (new hold
arrived), use **#3 sound** (`pfSound.notify()`) to ping rather than
re-render the whole list.

**Acceptance**:
- Poll runs every 15s, scan input stays focused, cursor stays where it was.
- A new hold arriving plays `pfSound.notify()` and bumps the count badge,
  doesn't redraw the page.
- Manual refresh button still does a full refresh.

### 4. (Issue #1) Hold Queue refactor — group by bin, not by card

**Current**: cards listed flat, each row shows the bin it lives in. Forces
staff to re-walk the floor for each row.

**Target**: group by `bin_label`, sorted by bin order. For each bin:
- Bin header shows total card count for that bin (sum of qty across all rows)
- Each card row in that bin shows:
  - **Larger** card image (current ones are too small to identify quickly —
    push to ~120-150px wide thumbs at minimum)
  - Card name + set + condition
  - Qty — if qty > 1, this needs a **drastic** callout. Big number, accent
    background, not a subtle "(x2)" suffix. Examples: bright accent badge
    with "QTY: 3" in 1.2rem+ font. Make it impossible to miss.
- Action buttons (return / accept / etc.) per row, scoped to that exact
  barcode (per #4)

**Acceptance**:
- Bin header at the top of each group with total count
- Cards larger / faster to identify than current
- qty > 1 is the most visually loud thing in the row
- Bins sorted in walk order (use existing `_binCompare` helper from
  ingestion's barcode page if it lives in shared, or copy the pattern:
  letter then numeric)

### 5. (Issue #13) Card editor accepts barcode scan to load

**Current**: the card editor in card_manager requires manual navigation to
find a specific card to edit.

**Target**: a barcode scan input at the top of the editor that, on Enter
(scanner end-of-scan), looks up the card by `raw_cards.barcode` and loads
its edit form. Misses fall through `pfBlock({error:true})` with "No card
with that barcode" — staff must dismiss before scanning again.

**Acceptance**:
- Scan input always focused when the editor page loads
- Valid barcode → editor populates instantly
- Invalid → red blocking modal with the error sound, then re-focus

### 6. (Issue #14) Public price-check page

**New route** under card_manager (e.g. `/price-check/`), **publicly
accessible** (skip JWT — add to `public_paths` in `register_auth_hooks`).
Read-only.

**Behavior**:
- Big scan input (touch/tablet friendly), auto-focused, scanner-Enter
  triggers lookup
- On scan, look up by barcode against everything we own:
  - `raw_cards.barcode` (singles inventory) → show card name, set,
    condition, current store price, image
  - Shopify variant barcode / SKU (sealed inventory) → show product
    title, current store price, image
  - Anything else → `pfBlock({error:true})` "Not found" with the error
    sound, then re-focus the scan input
- No edit, no auth, no PII — never show hold queue, customer data, etc.
- No external lookups (no Scrydex, no PPT, no Shopify pricing API). If
  it's not in our DB, it's "not found" — we don't sell it, we don't
  price it.

**Acceptance**:
- Reachable without login from cards.pack-fresh.com/price-check/
- Scanning any internal Pack Fresh barcode → price + image instantly
- Misses produce a blocking-modal error, not a toast
- Page never exposes anything beyond name / set / condition / price /
  image for the scanned item

## Files you will likely touch

- `apps/card_manager/app.py` — auth config, push pipeline (#5), polling
  endpoint (#6), barcode lookup (#13), price-check route (#14), action
  handlers (#4)
- `apps/card_manager/templates/*.html` — Hold Queue refactor (#1),
  poll-in-place rendering (#6), card-editor scan input (#13), price-check
  page (#14), all error toasts → `pfBlock` (every #4/#5/#13/#14 error path)

## What NOT to do

- Don't add backwards-compat shims for the bug fixes — change the code,
  don't gate it.
- Don't auto-clean existing duplicate listings (#5) — Sean handles those
  manually after seeing the warning.
- Don't refactor for refactor's sake. Six issues, six commits (or
  fewer). Each commit's diff should match the spec line.
- No summaries at end of work. Each commit message tells the story.

## Deploy & ship

- Push directly to `main`. Each issue is its own commit.
- Railway watches `apps/card_manager/` paths and auto-deploys.
- Sean is monitoring deploys live; do not ask if changes are deployed.
