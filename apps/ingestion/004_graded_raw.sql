-- ============================================================
-- Migration 004: Graded slab fields + raw card storage system
-- Run once against the shared PostgreSQL DB
-- ============================================================

-- ── 1. Add graded + raw fields to intake_items ──────────────
ALTER TABLE intake_items
    ADD COLUMN IF NOT EXISTS is_graded      BOOLEAN      DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS grade_company  VARCHAR(20),   -- PSA / BGS / CGC / SGC
    ADD COLUMN IF NOT EXISTS grade_value    VARCHAR(10),   -- 10 / 9.5 / 9 / etc.
    ADD COLUMN IF NOT EXISTS cert_number    VARCHAR(50),   -- PSA cert / BGS barcode
    ADD COLUMN IF NOT EXISTS variant        VARCHAR(100),  -- Holofoil / Reverse Holofoil
    ADD COLUMN IF NOT EXISTS language       VARCHAR(20)  DEFAULT 'EN',
    ADD COLUMN IF NOT EXISTS item_status    VARCHAR(30)  DEFAULT 'good',
    ADD COLUMN IF NOT EXISTS pushed_at      TIMESTAMP;

-- ── 2. Replace boxes with proper location system ─────────────

-- storage_rows: physical rows/shelves, each with a card_type affinity
CREATE TABLE IF NOT EXISTS storage_rows (
    id          UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
    row_label   VARCHAR(20)  NOT NULL UNIQUE,  -- 'A', 'B', 'C', ...
    card_type   VARCHAR(50)  NOT NULL,         -- 'pokemon', 'magic', 'yugioh', 'other'
    description VARCHAR(255),
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- storage_locations: individual 100-card bins within a row
CREATE TABLE IF NOT EXISTS storage_locations (
    id            UUID    PRIMARY KEY DEFAULT uuid_generate_v4(),
    bin_label     VARCHAR(20)  NOT NULL UNIQUE,  -- 'A-1', 'A-2', ..., 'A-50'
    row_id        UUID         NOT NULL REFERENCES storage_rows(id),
    partition_num INTEGER      NOT NULL,          -- 1-50 within the row
    card_type     VARCHAR(50)  NOT NULL,           -- denormalized from row for fast lookup
    capacity      INTEGER      NOT NULL DEFAULT 100,
    current_count INTEGER      NOT NULL DEFAULT 0,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (row_id, partition_num)
);

CREATE INDEX IF NOT EXISTS idx_storage_locations_card_type
    ON storage_locations(card_type, current_count);

-- ── 3. Update raw_cards to use new bin system ────────────────
ALTER TABLE raw_cards
    ADD COLUMN IF NOT EXISTS bin_id       UUID REFERENCES storage_locations(id),
    ADD COLUMN IF NOT EXISTS image_url    VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS is_graded    BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS grade_company VARCHAR(20),
    ADD COLUMN IF NOT EXISTS grade_value   VARCHAR(10),
    ADD COLUMN IF NOT EXISTS variant       VARCHAR(100),
    ADD COLUMN IF NOT EXISTS language      VARCHAR(20) DEFAULT 'EN';

-- Drop old box_id if it exists (may not if schema was never fully applied)
ALTER TABLE raw_cards DROP COLUMN IF EXISTS box_id;

CREATE INDEX IF NOT EXISTS idx_raw_cards_bin ON raw_cards(bin_id);

-- ── 4. Seed default storage rows (Pokemon A–C, Magic D) ──────
INSERT INTO storage_rows (row_label, card_type, description) VALUES
    ('A', 'pokemon', 'Pokemon Row A'),
    ('B', 'pokemon', 'Pokemon Row B'),
    ('C', 'pokemon', 'Pokemon Row C'),
    ('D', 'magic',   'Magic: The Gathering Row D')
ON CONFLICT (row_label) DO NOTHING;

-- Seed bins for Row A (partitions 1-50)
INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
SELECT
    'A-' || gs.n,
    (SELECT id FROM storage_rows WHERE row_label = 'A'),
    gs.n,
    'pokemon'
FROM generate_series(1, 50) AS gs(n)
ON CONFLICT (bin_label) DO NOTHING;

-- Seed bins for Row B
INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
SELECT
    'B-' || gs.n,
    (SELECT id FROM storage_rows WHERE row_label = 'B'),
    gs.n,
    'pokemon'
FROM generate_series(1, 50) AS gs(n)
ON CONFLICT (bin_label) DO NOTHING;

-- Seed bins for Row C
INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
SELECT
    'C-' || gs.n,
    (SELECT id FROM storage_rows WHERE row_label = 'C'),
    gs.n,
    'pokemon'
FROM generate_series(1, 50) AS gs(n)
ON CONFLICT (bin_label) DO NOTHING;

-- Seed bins for Row D (Magic)
INSERT INTO storage_locations (bin_label, row_id, partition_num, card_type)
SELECT
    'D-' || gs.n,
    (SELECT id FROM storage_rows WHERE row_label = 'D'),
    gs.n,
    'magic'
FROM generate_series(1, 50) AS gs(n)
ON CONFLICT (bin_label) DO NOTHING;
