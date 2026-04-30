# Agent Spec — price_updater (raw card row collapse)

## Mission
One issue: **#16 — price_updater treats every raw_card row as its own
edit target**, so when there are 25 copies of the same NM Pikachu and
staff wants to bump the price from $4.49 to $4.99, they have to edit 25
rows. They should be a single row.

## Repo & Service Context

- Working dir: `apps/price_updater/`
- Domain: prices.pack-fresh.com — owner-only auth
- Stack: Flask + `templates/review.html` (and friends)
- Per-service notes: `apps/price_updater/CLAUDE.md`
- Root rules: `apps/CLAUDE.md` — Sean's working style is strict.

The price_updater UI Sean is referring to is the **raw card review
dashboard** — surfaces raw_cards rows (from the singles inventory) and
lets staff approve, override, or skip price changes per row.

## Phase 1 primitives already shipped

- `shared/static/pf_ui.js`: `pfBlock`, `pfSound`, `toast`, `themedConfirm`
- `shared/price_rounding.py::charm_ceil_raw` was just updated to ceil to
  .49 / .99 increments under $10. The price_updater duplicate copy in
  `apps/price_updater/raw_card_updater.py::charm_ceil_raw` was synced —
  don't touch the rounding logic, just be aware that "suggested"
  prices may differ slightly from before.

---

## Issue #16 — Collapse identical raw cards into one row

**Symptom**: 25 copies of NM Pikachu (Base Set #58, normal variant) show
as 25 separate price-review rows. Editing a price affects only one row
even though all 25 cards point to the same SKU semantically.

**Card identity** (the grouping key — same definition we use elsewhere
in the codebase):
- `card_name`
- `set_name`
- `card_number`
- `variant`     (e.g. Holo, Reverse Holo, 1st Edition; NULL also matches NULL)
- `condition`   (NM and LP must be separate rows — they price differently)

Same identity → one row. The agent should reuse whatever card-matching
helper exists (`shared/...` or scrydex matching). If the existing
helpers compare by tcgplayer_id / scrydex_id, that's even better — use
those over string matching.

### Backend

- The query that feeds the review dashboard groups raw_cards by the
  identity columns above (or by scrydex_id + condition + variant if
  cleaner).
- Each grouped row carries:
  - `count` (how many copies)
  - `cost_basis_min` / `cost_basis_max` / `cost_basis_avg` — copies in
    the same group can have different cost bases (bought at different
    times). Show the range so staff knows the margin spread.
  - `current_price` (should be uniform across the group; if it's not,
    flag it — that's a data inconsistency)
  - `suggested_price` (single value — group's market lookup)
  - `bin_labels` — distinct bins these copies live in (helps staff
    physically locate them; could be many)
  - `barcodes` — array of all barcodes in the group, for the apply
    step
- Apply endpoint accepts a list of barcodes (or a group key) and
  writes the new price to ALL of them in one transaction. Existing
  per-row apply endpoint can stay for one-offs but the UI defaults
  to group-apply.

### UI

- Each row shows the count prominently (e.g. "× 25" badge — make it
  loud, similar to the qty>1 callout in card_manager Hold Queue).
- Single price input governs the whole group.
- Cost-basis spread shown as "Cost: $0.80 – $1.10" if the values
  differ, or single value if uniform.
- Bin column shows up to ~3 bins inline + "+5 more" hover/click for
  the rest if there are many.
- Approving/skipping/overriding applies to the entire group.

### Edge cases

- A group where the 25 copies have different `current_price` values
  (because some were edited individually before this change). Surface
  a small warning icon on the row. Staff should be able to expand the
  group and see the per-row prices, then either:
  - Accept the suggested price (uniformizes all 25)
  - Skip
  - Manual override (uniformizes to the entered value)
- Don't accidentally collapse rows that are actually different cards
  (e.g. card_number differing on reprint sets, or variant NULL vs
  "Holo" — those are different).
- Pagination: count rows after grouping. 25 NM Pikachus → 1 row, not
  25.

### Acceptance

- Bumping NM Pikachu Base Set #58 from $4.49 → $4.99 with 25 copies
  in stock results in 25 raw_cards rows updated in one click
- The dashboard shows fewer, denser rows than before — staff isn't
  scrolling through dupes
- A group with inconsistent current_price flags itself visually
- Per-row apply endpoint still works (don't remove it — defense in
  depth for any caller that uses it)
- No regressions in the suggested-price logic — `charm_ceil_raw` still
  drives suggestions

## Files you will likely touch

- `apps/price_updater/review_dashboard.py` — the review query / apply
  endpoints
- `apps/price_updater/raw_card_updater.py` — possibly, if grouping
  logic belongs in the run-time path too
- `apps/price_updater/templates/review.html` (and any partials) — UI
  collapse

## What NOT to do

- Don't change the suggestion algorithm or the `charm_ceil_raw` logic.
- Don't auto-uniformize current_prices that diverge silently — warn
  the user, let them decide.
- Don't remove the per-row apply endpoint; it's a useful primitive.
- No summaries at end of work. Each commit's message tells the story.

## Deploy & ship

- Push directly to `main`.
- Railway watches `apps/price_updater/` paths and auto-deploys.
- Sean monitors deploys live; do not ask if changes are deployed.
