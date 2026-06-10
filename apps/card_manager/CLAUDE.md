# Card Manager / Card Admin (card_manager/)
> Staff panel for processing customer holds (cardadmin.pack-fresh.com)

## Key Files
- **app.py** — Flask routes: hold queue, hold detail, scan, decisions, finish, returns, missing cards, Shopify listing creation
- **templates/index.html** — Single-page app with sidebar nav (Hold Queue, Return Queue, Missing Cards)
- DB via shared/db.py (no local db.py)

## Hold Lifecycle
```
PENDING → PULLING → READY → ACCEPTED or RETURNED
```
- **PENDING**: Customer submitted hold via kiosk
- **PULLING**: Staff started pulling cards from bins (barcode scanning)
- **READY**: All items pulled (or marked MISSING), presented to customer
- Customer decides: Accept (creates Shopify draft listing) or Reject (returns to storage)
- **Finish Hold**: creates Shopify listings for ACCEPTED, returns REJECTED to Return Queue

## Card States (raw_cards.state)
- `STORED` — in a bin, available for holds
- `DISPLAY` — in a binder / display case
- `PULLED` — taken from bin for a hold
- `PENDING_SALE` — accepted, Shopify listing created (active draft)
- `SOLD` — order paid / Shopify product archived (orders/create webhook + auto-heal flip PENDING_SALE → SOLD)
- `PENDING_RETURN` — rejected or returned, needs re-shelving
- `MISSING` — couldn't be found during pulling
- `GONE` — permanently lost (flagged for audit)

## Audit (per-bin / per-binder inventory check)
- View `view-audit` (sidebar "🔎 Audit"); endpoints under `/api/audit/*`.
- Pick a bin or binder → scanner armed → each scan resolves: EXPECTED / WRONG_BIN /
  WRONG_STATE / NOT_FOUND, or auto-recovers MISSING+GONE cards to the audited location.
- WRONG_STATE on a PENDING_SALE/SOLD card shows a Restore button → `/api/audit/restore`
  clones the row into a fresh STORED/DISPLAY copy with a new barcode (barcode-swap
  recovery); the original sale record is left intact.
- "Done" flips the unscanned tail to MISSING via `/api/audit/mark-missing`.

## Barcode Label Printing
- `printBarcodeLabel(imageUrl)` opens a print window locked to 89mm×28mm landscape.
  Required — printing the raw PNG directly lets the browser scale-to-fit, which
  distorts the bars enough to be unscannable. Mirrors ingestion's `printOneBarcode`.

## Missing Cards Flow
- During PULLING, "Can't Find" button marks hold_item as MISSING + raw_card state = MISSING
- Hold can proceed to READY with MISSING items
- "Missing Cards" sidebar view lists all MISSING cards
- Scan a missing card's barcode → return to storage flow
- "Mark Gone" → permanent loss (state = GONE) via `POST /api/missing/<id>/gone`

## Shrink / GONE
- A `GONE` state is the audit exit for cards that physically can't be found.
- Two entry points: Missing Cards ("Mark Gone") and Return Queue ("Mark Gone",
  `POST /api/returns/<id>/gone`, guarded on PENDING_RETURN). A Return Queue card
  that never gets a successful return-scan is shrink (handed over unscanned, POS
  scan didn't register, theft) — Mark Gone closes it out instead of parking it.
- Shrink ledger: every state change (incl. →GONE) is auto-written to `audit_log`
  by the DB trigger `trg_log_state_transition` — query `to_state='GONE'` by
  timestamp for loss reporting. App code does NOT write audit_log; the trigger
  does, so never insert rows manually (causes duplicates).

## Decision Reversal
- After decisions, Re-accept a REJECTED card → creates Shopify listing
- Return an ACCEPTED card → deletes Shopify listing, card goes to Return Queue
- Both via `POST /api/holds/<id>/items/<id>/reverse`
- Endpoint keys off `raw_cards.state` (not `hold_items.status`), so it works on closed holds too

## Sell Tab — Active Listings & Undo
- Front-of-house person ideally lives in the Sell tab.
- `/api/sell/active` returns every PENDING_SALE card (regardless of source: hold-finalize or sell/finalize). Surfaced as a top panel in the Sell view.
- `/api/sell/pull-listing` deletes the Shopify draft, flips card to PENDING_RETURN. Used when a customer changes their mind at the register.
- `/api/sell/relist` is the inverse: PENDING_RETURN → fresh listing → PENDING_SALE. Exposed as "Sell instead" on Return Queue cards.
- `raw_cards.shopify_product_id` / `shopify_variant_id` are populated on every listing creation so undo works without consulting `hold_items` (added in shared/018).

## Hold Lock Invariant (`raw_cards.current_hold_id`)
- Set by kiosk on hold creation against the specific allocated row.
- Sibling substitution: scan_card on a different barcode of the same identity transfers the lock to the scanned copy and releases the original. Without this, the kiosk-allocated row keeps a stale lock and is hidden from kiosk browse forever.
- Every "is this card available?" scan endpoint (display set-out, sell, binder fill) calls `_resolve_hold_lock(card)` — auto-clears current_hold_id when the referenced hold is in a terminal state.
- `_heal_stale_hold_locks()` runs on boot to clean up legacy rows.

## Sidebar Badges
- `/api/badges` returns `{holds, returns, missing, active_listings}`.
- Polled every 15s globally (not just when a view is active), so pending work is visible from any tab.

## Key Patterns
- Barcode scanning via HID keyboard input (Enter key trigger, 500ms buffer timeout)
- Scanner routes to different endpoints based on active view (pull / return / missing)
- Shopify REST API for draft product creation/deletion
- `shared/storage.py` for bin assignment during returns
- Themed confirm for destructive actions
