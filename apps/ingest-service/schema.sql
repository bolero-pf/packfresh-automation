-- ==========================================
-- TCG Store Database Schema
-- For Raw Card Inventory + Sealed COGS Tracking
-- ==========================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==========================================
-- PRODUCT MAPPING TABLE
-- Links Collectr product names to TCGPlayer IDs
-- ==========================================
CREATE TABLE product_mappings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    collectr_name VARCHAR(500) NOT NULL,
    tcgplayer_id BIGINT NOT NULL,
    product_type VARCHAR(50) NOT NULL, -- 'sealed' or 'raw'
    
    -- Optional fields for better matching
    set_name VARCHAR(255),
    card_number VARCHAR(50),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    use_count INTEGER DEFAULT 0,
    
    -- Ensure we don't duplicate mappings
    UNIQUE(collectr_name, product_type)
);

CREATE INDEX idx_product_mappings_collectr ON product_mappings(collectr_name);
CREATE INDEX idx_product_mappings_tcgplayer ON product_mappings(tcgplayer_id);

-- ==========================================
-- INTAKE SESSIONS
-- Tracks collection purchases
-- ==========================================
CREATE TABLE intake_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Session info
    customer_name VARCHAR(255),
    session_type VARCHAR(20) NOT NULL, -- 'sealed', 'raw', or 'mixed'
    status VARCHAR(50) NOT NULL DEFAULT 'in_progress', -- 'in_progress', 'finalized', 'cancelled'
    
    -- Pricing
    total_market_value DECIMAL(10, 2),
    offer_percentage DECIMAL(5, 2), -- e.g., 75.00 for 75%
    total_offer_amount DECIMAL(10, 2),
    
    -- Metadata
    notes TEXT,
    employee_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finalized_at TIMESTAMP,
    
    -- Source file tracking
    source_file_name VARCHAR(255),
    source_file_hash VARCHAR(64) -- To prevent duplicate imports
);

CREATE INDEX idx_intake_sessions_status ON intake_sessions(status);
CREATE INDEX idx_intake_sessions_created ON intake_sessions(created_at DESC);

-- ==========================================
-- INTAKE ITEMS (Staging Table)
-- Items in an intake session before finalization
-- ==========================================
CREATE TABLE intake_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES intake_sessions(id) ON DELETE CASCADE,
    
    -- Product identification
    product_name VARCHAR(500) NOT NULL,
    tcgplayer_id BIGINT, -- NULL until mapped
    product_type VARCHAR(50) NOT NULL, -- 'sealed' or 'raw'
    
    -- For raw cards specifically
    set_name VARCHAR(255),
    card_number VARCHAR(50),
    condition VARCHAR(20), -- NM, LP, MP, HP, DMG
    rarity VARCHAR(50),
    
    -- Pricing
    quantity INTEGER NOT NULL DEFAULT 1,
    market_price DECIMAL(10, 2) NOT NULL, -- From Collectr or TCGPlayer API
    offer_price DECIMAL(10, 2) NOT NULL, -- market_price * offer_percentage
    unit_cost_basis DECIMAL(10, 2), -- Calculated: offer_price / quantity
    
    -- Status
    is_mapped BOOLEAN DEFAULT FALSE, -- Has tcgplayer_id been linked?
    needs_review BOOLEAN DEFAULT FALSE, -- Flagged for manual review
    review_notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_intake_items_session ON intake_items(session_id);
CREATE INDEX idx_intake_items_unmapped ON intake_items(is_mapped) WHERE is_mapped = FALSE;

-- ==========================================
-- SEALED PRODUCT COGS
-- Tracks weighted average cost for Shopify products
-- ==========================================
CREATE TABLE sealed_cogs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Shopify linkage
    shopify_product_id BIGINT UNIQUE,  -- NULL until linked to Shopify product
    shopify_variant_id BIGINT,
    tcgplayer_id BIGINT NOT NULL,
    
    -- Product info (denormalized for convenience)
    product_name VARCHAR(500),
    
    -- COGS calculation
    current_quantity INTEGER NOT NULL DEFAULT 0,
    total_cost DECIMAL(10, 2) NOT NULL DEFAULT 0.00, -- Running total
    avg_cogs DECIMAL(10, 2) NOT NULL DEFAULT 0.00, -- total_cost / current_quantity
    
    -- Audit
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_intake_session_id UUID REFERENCES intake_sessions(id)
);

CREATE INDEX idx_sealed_cogs_shopify ON sealed_cogs(shopify_product_id);
CREATE INDEX idx_sealed_cogs_tcgplayer ON sealed_cogs(tcgplayer_id);

-- ==========================================
-- BOXES (Physical Storage)
-- For raw card organization
-- ==========================================
CREATE TABLE boxes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    box_number INTEGER UNIQUE NOT NULL,
    
    -- Capacity management
    capacity INTEGER NOT NULL DEFAULT 200, -- Standard card box size
    current_count INTEGER NOT NULL DEFAULT 0,
    
    -- Location
    location VARCHAR(255), -- "Back room, Shelf 3" etc.
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_boxes_number ON boxes(box_number);
CREATE INDEX idx_boxes_capacity ON boxes(current_count) WHERE current_count < capacity;

-- ==========================================
-- RAW CARDS (Active Inventory)
-- Source of truth for in-store raw card inventory
-- ==========================================
CREATE TABLE raw_cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Identification
    barcode VARCHAR(100) UNIQUE NOT NULL, -- Generated barcode for scanning
    tcgplayer_id BIGINT NOT NULL,
    
    -- Card details
    card_name VARCHAR(500) NOT NULL,
    set_name VARCHAR(255) NOT NULL,
    card_number VARCHAR(50),
    condition VARCHAR(20) NOT NULL, -- NM, LP, MP, HP, DMG
    rarity VARCHAR(50),
    
    -- State machine
    state VARCHAR(50) NOT NULL DEFAULT 'PURCHASED', 
    -- States: PURCHASED, STORED, PULLED, PENDING_SALE, REMOVED
    
    -- Pricing
    cost_basis DECIMAL(10, 2) NOT NULL, -- What we paid
    current_price DECIMAL(10, 2) NOT NULL, -- Current sell price
    last_price_update TIMESTAMP,
    
    -- Storage
    box_id UUID REFERENCES boxes(id),
    
    -- Removal tracking (when state = REMOVED)
    removal_reason VARCHAR(50), -- SOLD, CARDTRADER, GRADING, DAMAGED
    removal_date TIMESTAMP,
    sale_price DECIMAL(10, 2), -- Only if removal_reason = SOLD
    
    -- Provenance
    intake_session_id UUID REFERENCES intake_sessions(id),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stored_at TIMESTAMP, -- When first moved to STORED state
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_cards_barcode ON raw_cards(barcode);
CREATE INDEX idx_raw_cards_state ON raw_cards(state);
CREATE INDEX idx_raw_cards_box ON raw_cards(box_id);
CREATE INDEX idx_raw_cards_tcgplayer ON raw_cards(tcgplayer_id);
CREATE INDEX idx_raw_cards_stored_date ON raw_cards(stored_at) WHERE state = 'STORED';

-- ==========================================
-- AUDIT LOG
-- Immutable log of all state transitions
-- ==========================================
CREATE TABLE audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- What changed
    card_id UUID REFERENCES raw_cards(id),
    action VARCHAR(100) NOT NULL, -- 'state_transition', 'price_update', 'storage_change'
    
    -- State transitions
    from_state VARCHAR(50),
    to_state VARCHAR(50),
    
    -- Who & when
    employee_id VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Additional context (JSON for flexibility)
    metadata JSONB,
    
    -- IP/session tracking for fraud detection
    ip_address INET,
    session_id VARCHAR(100)
);

CREATE INDEX idx_audit_log_card ON audit_log(card_id);
CREATE INDEX idx_audit_log_timestamp ON audit_log(timestamp DESC);
CREATE INDEX idx_audit_log_employee ON audit_log(employee_id);
CREATE INDEX idx_audit_log_action ON audit_log(action);

-- ==========================================
-- COGS HISTORY
-- Track COGS changes over time for sealed products
-- ==========================================
CREATE TABLE cogs_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    sealed_cogs_id UUID REFERENCES sealed_cogs(id) ON DELETE CASCADE,
    
    -- Before/after snapshot
    old_quantity INTEGER,
    new_quantity INTEGER,
    old_avg_cogs DECIMAL(10, 2),
    new_avg_cogs DECIMAL(10, 2),
    
    -- What changed
    quantity_delta INTEGER, -- Positive = added inventory
    cost_added DECIMAL(10, 2),
    
    -- Context
    intake_session_id UUID REFERENCES intake_sessions(id),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cogs_history_sealed ON cogs_history(sealed_cogs_id);
CREATE INDEX idx_cogs_history_session ON cogs_history(intake_session_id);

-- ==========================================
-- HELPER VIEWS
-- ==========================================

-- View: Available raw card inventory (for kiosk)
CREATE VIEW available_raw_inventory AS
SELECT 
    r.id,
    r.barcode,
    r.card_name,
    r.set_name,
    r.card_number,
    r.condition,
    r.rarity,
    r.current_price,
    r.box_id,
    b.box_number,
    r.stored_at,
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - r.stored_at)) as days_in_inventory
FROM raw_cards r
LEFT JOIN boxes b ON r.box_id = b.id
WHERE r.state = 'STORED'
ORDER BY r.stored_at DESC;

-- View: Cards flagged for liquidation (older than 90 days)
CREATE VIEW liquidation_candidates AS
SELECT 
    r.*,
    EXTRACT(DAY FROM (CURRENT_TIMESTAMP - r.stored_at)) as days_in_inventory
FROM raw_cards r
WHERE r.state = 'STORED'
    AND r.stored_at < (CURRENT_TIMESTAMP - INTERVAL '90 days')
ORDER BY r.stored_at ASC;

-- View: Intake session summary
CREATE VIEW intake_session_summary AS
SELECT 
    s.id,
    s.customer_name,
    s.session_type,
    s.status,
    s.total_market_value,
    s.offer_percentage,
    s.total_offer_amount,
    s.created_at,
    s.finalized_at,
    COUNT(i.id) as item_count,
    SUM(i.quantity) as total_quantity,
    COUNT(*) FILTER (WHERE i.is_mapped = FALSE) as unmapped_count
FROM intake_sessions s
LEFT JOIN intake_items i ON s.id = i.session_id
GROUP BY s.id;

-- ==========================================
-- FUNCTIONS
-- ==========================================

-- Function: Update box current_count when cards are added/removed
CREATE OR REPLACE FUNCTION update_box_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.box_id IS NOT NULL THEN
        UPDATE boxes 
        SET current_count = current_count + 1,
            last_modified = CURRENT_TIMESTAMP
        WHERE id = NEW.box_id;
    ELSIF TG_OP = 'UPDATE' THEN
        -- Card moved from one box to another
        IF OLD.box_id IS DISTINCT FROM NEW.box_id THEN
            IF OLD.box_id IS NOT NULL THEN
                UPDATE boxes 
                SET current_count = current_count - 1,
                    last_modified = CURRENT_TIMESTAMP
                WHERE id = OLD.box_id;
            END IF;
            IF NEW.box_id IS NOT NULL THEN
                UPDATE boxes 
                SET current_count = current_count + 1,
                    last_modified = CURRENT_TIMESTAMP
                WHERE id = NEW.box_id;
            END IF;
        END IF;
    ELSIF TG_OP = 'DELETE' AND OLD.box_id IS NOT NULL THEN
        UPDATE boxes 
        SET current_count = current_count - 1,
            last_modified = CURRENT_TIMESTAMP
        WHERE id = OLD.box_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_box_count
AFTER INSERT OR UPDATE OR DELETE ON raw_cards
FOR EACH ROW
EXECUTE FUNCTION update_box_count();

-- Function: Auto-create audit log on state transition
CREATE OR REPLACE FUNCTION log_state_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.state IS DISTINCT FROM NEW.state THEN
        INSERT INTO audit_log (
            card_id,
            action,
            from_state,
            to_state,
            metadata
        ) VALUES (
            NEW.id,
            'state_transition',
            OLD.state,
            NEW.state,
            jsonb_build_object(
                'old_box_id', OLD.box_id,
                'new_box_id', NEW.box_id,
                'old_price', OLD.current_price,
                'new_price', NEW.current_price
            )
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_log_state_transition
AFTER UPDATE ON raw_cards
FOR EACH ROW
EXECUTE FUNCTION log_state_transition();

-- ==========================================
-- SEED DATA
-- ==========================================

-- Create first 10 storage boxes
INSERT INTO boxes (box_number, capacity) 
SELECT generate_series(1, 10), 200;

-- ==========================================
-- GRANTS (Adjust based on your Railway setup)
-- ==========================================

-- If you have a specific application user, grant permissions:
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_app_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_app_user;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO your_app_user;
