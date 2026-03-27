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
- `PULLED` — taken from bin for a hold
- `PENDING_SALE` — accepted, Shopify listing created
- `PENDING_RETURN` — rejected or returned, needs re-shelving
- `MISSING` — couldn't be found during pulling
- `GONE` — permanently lost (flagged for audit)

## Missing Cards Flow
- During PULLING, "Can't Find" button marks hold_item as MISSING + raw_card state = MISSING
- Hold can proceed to READY with MISSING items
- "Missing Cards" sidebar view lists all MISSING cards
- Scan a missing card's barcode → return to storage flow
- "Mark Gone" → permanent loss (state = GONE)

## Decision Reversal
- After decisions, Re-accept a REJECTED card → creates Shopify listing
- Return an ACCEPTED card → deletes Shopify listing, card goes to Return Queue
- Both via `POST /api/holds/<id>/items/<id>/reverse`

## Key Patterns
- Barcode scanning via HID keyboard input (Enter key trigger, 500ms buffer timeout)
- Scanner routes to different endpoints based on active view (pull / return / missing)
- Shopify REST API for draft product creation/deletion
- `shared/storage.py` for bin assignment during returns
- Themed confirm for destructive actions
