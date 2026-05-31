-- Hard backstop for the kiosk dupe-allocation bug: prevent two hold_items
-- on the same hold from pointing at the same raw_cards row.
--
-- The bug: a request to /api/hold (or /api/checkout) that contained two
-- lines for the same card identity ran two separate SELECTs back-to-back,
-- both before any raw_cards.current_hold_id UPDATE landed. Both SELECTs
-- returned the same earliest-created row, so the hold ended up with two
-- hold_items referencing one barcode and zero copies of any other.
--
-- Application-level fix lives in kiosk/app.py (track allocated raw_card
-- ids per request, exclude them from each subsequent SELECT). This index
-- is the database-level guarantee so a regression errors loudly instead
-- of silently re-shipping the same physical card.
--
-- Partial index (raw_card_id IS NOT NULL) so sealed/slab hold_items, which
-- intentionally leave raw_card_id NULL, are not constrained.
--
-- Two historical duplicate hold_items pairs exist as of 2026-05-31. The
-- CREATE will FAIL until they are deleted; that cleanup runs as a one-off
-- before applying this migration (see the chat thread that landed this fix
-- for the exact DELETE statement).

CREATE UNIQUE INDEX IF NOT EXISTS hold_items_hold_raw_uniq
    ON hold_items(hold_id, raw_card_id)
    WHERE raw_card_id IS NOT NULL;
