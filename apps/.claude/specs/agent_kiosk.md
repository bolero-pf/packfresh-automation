# Agent Spec — kiosk (customer-facing card browser)

## Mission
One issue: **#12 — sophisticated, game-aware search filters** for the
public kiosk so customers can browse like they're filling out a deck list
("show me BUG sorceries", "Pokemon Fire types, Holo Rare and up", etc.).

## Repo & Service Context

- Working dir: `apps/kiosk/`
- Domain: kiosk.pack-fresh.com — **public**, no auth
- Stack: Flask + tablet-optimized HTML / JS frontend (look for a single-page
  template or split templates per game)
- Per-service notes: `apps/kiosk/CLAUDE.md`
- Root rules: `apps/CLAUDE.md` — Sean's working style is strict.

Recent kiosk-side context (from git log): SVG energy icons, game-aware
info fields, lightbox flip for double-faced MTG cards, tablet
breakpoints (768/1024). The kiosk already knows what game a card is —
extending it with game-specific filters is the next logical step.

## Phase 1 primitives already shipped (use these)

- `shared/static/pf_ui.js`:
  - `pfBlock(title, msg, {error})` — blocking modal for errors. Use if a
    filter combination returns 0 cards and you want to make that loud, or
    just leave an empty-state message — your call.
  - `pfSound.success()` / `.error()` — Web Audio beeps.

The kiosk is a **public** experience so use these primitives sparingly —
toasts and visual states are usually sufficient. Don't beep at customers.

---

## Issue #12 — Game-aware advanced search filters

### Per-game filter sets

**Magic: The Gathering**
- **Color**: W (white), U (blue), B (black), R (red), G (green), C (colorless).
  Two-mode toggle:
  - **Any** — card has any one of the selected colors (OR).
  - **Exactly** — card's color identity is exactly this combination (good
    for commander deck building: "BUG sorceries").
- **Card type**: Land, Creature, Artifact, Enchantment, Instant, Sorcery,
  Planeswalker, Battle. Multi-select.
- **Rarity**: Common, Uncommon, Rare, Mythic. Multi-select.

**Pokemon**
- **Type** (energy type on the card): Grass, Fire, Water, Lightning,
  Psychic, Fighting, Darkness, Metal, Fairy (legacy), Dragon, Colorless.
  Multi-select OR.
- **Rarity**: Common, Uncommon, Rare, Holo Rare, Ultra Rare, Secret Rare,
  etc. — read what's actually in our cache, don't hardcode a list that
  doesn't match Scrydex's labels.
- (Optional, only if data already exists) Card class: Pokemon / Trainer /
  Energy.

**One Piece**
- **Color**: Red, Green, Blue, Purple, Black, Yellow. Multi-select with
  the same Any / Exactly toggle as MTG (OP cards have multi-color too).
- **Card type**: Leader, Character, Event, Stage. Multi-select.
- **Rarity**: C, UC, R, SR, L, SEC. Multi-select.

### Data plumbing

All three games are fed by Scrydex (via `shared/scrydex_*` and the
`scrydex_*` cache tables). The kiosk currently queries Scrydex-cached
data for browsing. **Verify which of these filter fields are already
indexed/queryable** before building the UI:

- MTG: `colors` / `color_identity`, `type_line`, `rarity`
- Pokemon: `types` array, `rarity`
- OP: `colors`, `card_type`, `rarity`

If a field isn't queryable today (no column / no index / not parsed from
Scrydex JSON), pick the cheapest path:
1. Add a virtual column or parse-on-read in the query
2. Add a small cache extraction job (only if real performance demands)
3. Skip that filter and surface a TODO

**Don't over-engineer.** The brick-and-mortar use case is real but
volume is moderate — a clean SQL `WHERE` with a `?` per checked filter
will be fast enough for the cache sizes we have.

### UI

- Filter sidebar (collapsible on tablet, drawer on phone) that shows
  ONLY the filters relevant to the currently-selected game tab.
- Filters update results live (debounced ~150ms).
- "Clear filters" pill always visible when any filter is active.
- Selected filters render as removable chips above the results so
  customers can see what's narrowing their list.
- The Any / Exactly mode for MTG + OP color is a small segmented
  toggle right next to the color row.
- Empty state: "No cards match. Try removing a filter." Not a
  blocking modal — customers experimenting shouldn't be punished with
  a beep.

### Mobile / tablet

Sean already shipped tablet breakpoints (768 / 1024). Filters must
work cleanly at every breakpoint:
- ≥1024px: persistent sidebar
- 768-1023px: collapsible sidebar with toggle button
- <768px: bottom sheet / drawer

Touch targets ≥44px (existing `pf_theme.css` pattern).

### Acceptance

- Switching game tabs swaps the filter set (no MTG colors visible in
  Pokemon mode)
- MTG: filtering by `B + U + G` with mode "Exactly" returns only
  cards whose color identity is exactly Sultai (no mono-G, no
  4-color)
- MTG: same selection with "Any" returns mono-B, mono-U, mono-G, and
  every multicolor that includes any of those
- Pokemon: filtering by Fire returns only Fire-typed cards (and the
  rarity narrows further)
- Performance: a typical filtered query returns in <200ms on the
  cache sizes currently in prod
- Filters survive page refresh via URL query params (so a customer
  can share / bookmark a filter combo)

## Files you will likely touch

- `apps/kiosk/app.py` (or wherever the search endpoint lives) — query
  builder for game-aware filters
- `apps/kiosk/templates/*.html` — filter sidebar, segmented controls,
  chips
- `apps/kiosk/static/*.js` — debounced live filter, URL state
- Possibly `apps/shared/scrydex_*.py` if we need to surface a field
  the cache doesn't currently expose (check first before changing
  shared/)

## What NOT to do

- Don't change `shared/scrydex_*` unless absolutely needed; if you do,
  flag it in your final summary because shared changes affect every
  service.
- Don't hardcode rarity labels; read what's in the cache.
- Don't build a "deck builder" — this is search, not deck mgmt.
- Don't add auth or user accounts; kiosk stays public.
- No summaries at end of work. Each commit's message tells the story.

## Deploy & ship

- Push directly to `main`. Logical commits — schema/cache work first if
  needed, query work next, UI last.
- Railway watches `apps/kiosk/` paths and auto-deploys.
- Sean monitors deploys live; do not ask if changes are deployed.
