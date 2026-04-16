-- Fix update_bin_count trigger to be state-aware.
-- Previously only counted bin_id changes, so marking a card MISSING
-- (while keeping bin_id) didn't decrement current_count, causing drift.
-- Now recounts current_count from actual STORED rows on every change.

CREATE OR REPLACE FUNCTION update_bin_count() RETURNS trigger AS $$
BEGIN
    -- On INSERT or UPDATE with a bin_id, recount that bin from scratch
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') AND NEW.bin_id IS NOT NULL THEN
        UPDATE storage_locations
        SET current_count = (
            SELECT COUNT(*) FROM raw_cards
            WHERE bin_id = NEW.bin_id AND state = 'STORED'
        )
        WHERE id = NEW.bin_id;
    END IF;
    -- On UPDATE (bin changed) or DELETE, recount the old bin too
    IF (TG_OP = 'UPDATE' OR TG_OP = 'DELETE')
       AND OLD.bin_id IS NOT NULL
       AND OLD.bin_id IS DISTINCT FROM COALESCE(NEW.bin_id, '00000000-0000-0000-0000-000000000000'::uuid) THEN
        UPDATE storage_locations
        SET current_count = (
            SELECT COUNT(*) FROM raw_cards
            WHERE bin_id = OLD.bin_id AND state = 'STORED'
        )
        WHERE id = OLD.bin_id;
    END IF;
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
