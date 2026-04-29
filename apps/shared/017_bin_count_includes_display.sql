-- Bin/binder/display-case current_count was zero because the trigger only
-- counted state='STORED'. Cards in binders carry state='DISPLAY' (per
-- assign_display in shared/storage.py), so binders always reported 0/480.
-- Count every card that physically occupies a slot: STORED + DISPLAY.

CREATE OR REPLACE FUNCTION update_bin_count() RETURNS trigger AS $$
BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.bin_id IS NOT NULL THEN
        UPDATE storage_locations
        SET current_count = (
            SELECT COUNT(*) FROM raw_cards
            WHERE bin_id = NEW.bin_id AND state IN ('STORED', 'DISPLAY')
        )
        WHERE id = NEW.bin_id;
    END IF;
    IF (TG_OP = 'UPDATE' OR TG_OP = 'DELETE')
       AND OLD.bin_id IS NOT NULL
       AND OLD.bin_id IS DISTINCT FROM COALESCE(NEW.bin_id, '00000000-0000-0000-0000-000000000000'::uuid) THEN
        UPDATE storage_locations
        SET current_count = (
            SELECT COUNT(*) FROM raw_cards
            WHERE bin_id = OLD.bin_id AND state IN ('STORED', 'DISPLAY')
        )
        WHERE id = OLD.bin_id;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- One-time recount so existing binders/cases reflect reality.
UPDATE storage_locations sl
SET current_count = (
    SELECT COUNT(*) FROM raw_cards rc
    WHERE rc.bin_id = sl.id AND rc.state IN ('STORED', 'DISPLAY')
);
