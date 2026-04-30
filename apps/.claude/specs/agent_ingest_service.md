# Agent Spec — ingest-service (intake / Deals)

## Mission
Three issues in `apps/ingest-service/` (offers.pack-fresh.com), all from
real brick-and-mortar usage on 2026-04-29. You own the **writer** side of
the cash/credit offer split — the ingestion agent is consuming the new
columns in parallel. Don't break the existing single-percentage flow until
the new fields are populated.

## Repo & Service Context

- Working dir: `apps/ingest-service/`  ← **intake service** despite the name
- Domain: offers.pack-fresh.com
- Auth: JWT via shared/auth — `register_auth_hooks(app, roles=...)`
  currently allows owner / manager / associate (associate is the most
  common user here — a lot of new hires)
- Stack: Flask + `templates/intake_dashboard.html`
- Per-service notes: `apps/ingest-service/CLAUDE.md`
- Root rules: `apps/CLAUDE.md` — Sean's working style is strict. No
  summaries, no half-measures, code must be deployable immediately.

## Phase 1 primitives already shipped

- `shared/static/pf_ui.js`:
  - `pfBlock(title, msg, {error, sound, okText})` — blocking modal,
    must be acknowledged. Use for wrong-input / can't-save / lock-blocked
    scenarios.
  - `pfSound.error()` / `.success()` / `.notify()` — Web Audio beeps.
- `window._pfUser = {name, role, email, user_id}` available everywhere
  (set by the shared admin bar).

## Phase 1 manager-override primitives (this is the auth surface for #8b)

These are LIVE in the admin service:

- `POST https://admin.pack-fresh.com/api/verify-pin`
  - Body: `{ pin: "1234", action: "offer_percentage" }`
  - Success: `{ ok: true, manager: {id, name, role}, override_token,
                expires_in_seconds }`
  - Failure: 401 `{ error: "Invalid PIN" }` (generic — never reveals
    which user owns a PIN)
  - The `manager.role` tells you who approved: `"manager"` (cap 80%) or
    `"owner"` (no cap)
- `shared/auth.py::decode_override_token(token, action="offer_percentage")`
  — call from the offer-create / offer-update endpoint to validate any
  override token attached to the request. Returns `{sub, name, role,
  action, exp, ...}` or `None`. Validating with the action label
  prevents a token issued for one purpose from being replayed for
  another.

## Pre-Phase-2 schema already in place

`intake_sessions` now has these columns (LIVE, migrated):

- `cash_percentage`     DECIMAL(5,2)
- `credit_percentage`   DECIMAL(5,2)
- `accepted_offer_type` VARCHAR(10)  — 'cash' or 'credit'
- `is_walk_in`          BOOLEAN DEFAULT FALSE

Existing rows have `cash_percentage` backfilled from `offer_percentage`.
View `intake_session_summary` exposes them. Legacy `offer_percentage`
column is still present — you're going to migrate writers off it during
this work but don't drop the column yet (other services may still be
mid-migration).

---

## Issue Order (suggested, not strict — they touch different areas)

1. **#7** — schema is ready; wire writers + UI for two percentages.
   This is the biggest piece, get it solid first.
2. **#8b** — bolts onto #7's percentage UI with the lock + override.
3. **#9** — UI-only sticky buttons. Fast, ship last.

---

### Issue #7 — Cash + credit offer percentages (and walk-in flow)

**Current**: A session has one `offer_percentage`. Staff sets it once.
The status flow is `in_progress → offered → accepted → received → ...`
which assumes a remote pickup/mail step between accepting and product
arrival.

**Target**:
- A session has **both** a cash percentage and a credit percentage.
  Defaults come from #8 (75% credit / 65% cash). Staff sees both
  side-by-side in the offer UI. When the customer accepts one, the
  chosen offer is captured in `accepted_offer_type`.
- Sessions can be flagged `is_walk_in = TRUE`, meaning the customer
  is physically at the counter. On accept, walk-in sessions
  short-circuit straight to `received` status — no pickup/mail
  intermediate (because the customer is right there with the cards).

**Backend changes**:
- Every endpoint that currently writes `offer_percentage` must also
  write `cash_percentage`. Recommended pattern: write
  `cash_percentage = offer_percentage` (defense in depth), write
  `credit_percentage` separately, leave the legacy column populated.
- Capture `accepted_offer_type` when a customer commits to one offer.
  Until accepted, the column stays NULL.
- Recompute `total_offer_amount` based on `accepted_offer_type` when
  it's set; until accepted, show two totals (one per offer).
- **Walk-in short-circuit on accept**: if `is_walk_in = TRUE` and
  staff hits Accept Cash / Accept Credit, set:
    - `accepted_offer_type = 'cash' | 'credit'`
    - `status = 'received'`  (skip the `accepted` interstitial)
    - any `received_at` / `accepted_at` columns (whatever exists)
      stamped with NOW()
  For non-walk-in sessions, existing flow is preserved (offered →
  accepted → wait for pickup/mail → received).

**UI changes** (`templates/intake_dashboard.html`):
- **New Intake → Manual Entry** tab: render an `is_walk_in` checkbox,
  defaulted to **TRUE** (manual-entry sessions are almost always
  walk-ins). CSV / paste imports leave it default-FALSE.
- Anywhere a single percentage input exists, replace with a paired
  layout: "Store Credit %" + "Cash %" side-by-side.
- Show the projected total for each offer in real time as values
  change.
- Replace the single "Accept" with two clear actions: "Accept Cash"
  and "Accept Credit". Plus the existing "Reject".
- For walk-in sessions, show a small badge ("Walk-In") on the session
  header so staff knows this short-circuits to received on accept.
- Mid-flight legacy sessions (only `offer_percentage` filled) should
  display cleanly: render whatever single value exists, prompt staff
  to fill in the missing one before further actions.

**Acceptance**:
- New manual-entry sessions persist `is_walk_in = TRUE` by default
- Walk-in accept jumps the session straight to `received` and it
  appears in the ingest queue immediately — no pickup/mail step
- Non-walk-in accept still flows through pickup/mail like today
- Legacy sessions don't crash; UI shows what data exists
- Downstream ingestion service (already reading the new columns per
  #7a) keeps working without a coordinated deploy

---

### Issue #8b — Associate percentage lock + manager-PIN override

**Role policy** (these are server-side rules in the offer endpoints):

| Role | Cash % | Credit % | Notes |
|------|--------|----------|-------|
| associate | locked at default 65% | locked at default 75% | needs override to change either |
| manager | 0-80% | 0-80% | can submit directly, override needed for >80% |
| owner | 0-100% | 0-100% | no cap |

**Override semantics**:
- Override is **per-session** (Sean's spec): a manager-approved
  override token authorizes the offer they're currently editing. It
  does not unlock a rolling timer for that browser. New session →
  new override.
- The override approver's role determines the cap that's now
  acceptable. If a `manager`-role override token is attached, server
  accepts up to 80%. If `owner`-role, no cap.

**Frontend** (`templates/intake_dashboard.html`):
- Read `window._pfUser.role` to determine the user's role.
- For `associate`: render the percentage inputs **read-only** with the
  default values pre-filled. Show a "🔒 Manager Override" button next
  to them.
- For `manager`: inputs editable up to 80. If they type >80, render the
  same "🔒 Owner Override" button.
- For `owner`: inputs editable, no cap, no override UI.

**Override modal** (when the override button is pressed):
- Modal with a single PIN input (`type="password"`, `inputmode="numeric"`,
  `maxlength=8`, `pattern="[0-9]*"`). 4-8 digits.
- Submit calls `POST https://admin.pack-fresh.com/api/verify-pin` with
  `{ pin, action: "offer_percentage" }`.
- On success: stash `override_token` in the page state, unlock the
  inputs, badge the inputs with the approver's name (e.g. "Override:
  Sean (owner)") so it's clear who approved.
- On failure: `pfBlock({error:true, message: "Invalid PIN"})`. Don't
  reveal anything specific.
- The override is bound to the session being edited. Submitting the
  offer attaches `override_token` to the request body.

**Backend** (offer create / update endpoints in `app.py` and
`intake.py`):
- Read `g.user.role`. If `associate` and any percentage differs from
  default, OR if `manager` and any percentage > 80, reject unless a
  valid `override_token` is attached.
- Validate the override token via
  `decode_override_token(token, action="offer_percentage")`.
- The token's `role` claim sets the effective cap:
  - `manager` token → up to 80% allowed
  - `owner` token → no cap
- If the override is invalid / expired / wrong action: 403 with
  generic "Override required" message.
- On a successful overridden submit: log the approval pair (manager
  user_id from the token + the associate's user_id from `g.user`)
  somewhere persistent so we have an audit trail. Simplest: a
  `session_overrides` table or an `override_*` column set on
  `intake_sessions`. Sean prefers a small new table:
  `id, session_id, approved_by_user_id, approver_role, approved_for_user_id,
   action, approved_cash_pct, approved_credit_pct, created_at`.
  Add a `migrate_session_overrides.py` to create it. Idempotent.

**Acceptance**:
- Logged in as associate, percentages are locked, defaults visible,
  override button present
- Logged in as manager, can submit ≤80% directly, >80% triggers
  override flow
- Logged in as owner, no caps, no overrides ever needed
- Failed override attempts log nothing identifying about which
  manager has a PIN; successful overrides leave an audit row
- Override is per-session — opening a new session resets the lock

---

### Issue #9 — Action buttons pinned to top of session

**Current**: Approve / Reject / etc. live at the bottom of the session
view. With hundreds of items, staff has to scroll past everything to
hit them.

**Target**: Sticky-positioned action toolbar at the top of the session
view (below the page title / session header). Same buttons, same
behavior, just always visible.

**Implementation pattern**:
```css
.session-actions { position: sticky; top: 0; z-index: 5;
                   background: var(--surface); padding: 10px 0;
                   border-bottom: 1px solid var(--border); }
```
- If there's an existing pf admin bar offset (mobile shifts content
  down ~36px), respect that — set `top: 36px` on mobile or use the
  existing variable / class if one exists.
- Don't duplicate the buttons (some staff like having them at bottom
  too). One copy, sticky to top.

**Acceptance**:
- Approve / Reject / etc. always visible regardless of scroll position
- Doesn't overlap existing sticky elements (admin bar)
- Mobile-responsive

---

## Files you will likely touch

- `apps/ingest-service/app.py` — offer create / update endpoints,
  override validation, accepted_offer_type capture
- `apps/ingest-service/intake.py` — business logic for offers,
  percentage caps server-side
- `apps/ingest-service/templates/intake_dashboard.html` — paired
  percentage inputs (#7), role-aware lock + override modal (#8b),
  sticky action toolbar (#9)
- `apps/ingest-service/migrate_session_overrides.py` — new file for
  the audit table

## What NOT to do

- Don't drop `intake_sessions.offer_percentage` — leave it in place,
  just stop writing fresh values to it (or write same as
  cash_percentage during the migration window).
- Don't put the override approval logic on the frontend only. The
  server **must** be the authority — an associate hitting the API
  directly with a percentage outside their range and no token must
  fail.
- Don't reveal which user owns a PIN on the verify-pin failure path.
  Generic 401 only.
- No summaries at end of work. Each commit's message is the story.

## Deploy & ship

- Push directly to `main`. Logical commits per issue.
- Sean runs the schema migrations himself. If you create
  `migrate_session_overrides.py`, just leave it in the directory —
  Sean will run it.
- Railway watches `apps/ingest-service/` paths and auto-deploys.
- Sean monitors deploys live; do not ask if changes are deployed.
