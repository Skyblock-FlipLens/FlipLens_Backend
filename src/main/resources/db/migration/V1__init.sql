-- SkyblockFlipperBackend - Unified Flip API DB Schema (PostgreSQL)
-- Version: v1

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- 1) Snapshot Layer
-- ============================================================================
CREATE TABLE IF NOT EXISTS snapshot (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    source_version VARCHAR(64) NOT NULL DEFAULT '',
    ingestion_run_id VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_timestamp, source_version)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_ts ON snapshot (snapshot_timestamp DESC);

-- ============================================================================
-- 2) Item Master Data
-- ============================================================================
CREATE TABLE IF NOT EXISTS item (
    id VARCHAR(128) PRIMARY KEY,
    display_name VARCHAR(255),
    minecraft_id VARCHAR(128),
    rarity VARCHAR(64),
    category VARCHAR(128),
    lore TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS item_info_link (
    item_id VARCHAR(128) NOT NULL REFERENCES item(id) ON DELETE CASCADE,
    position INT NOT NULL,
    info_link TEXT NOT NULL,
    PRIMARY KEY (item_id, position)
);

-- ============================================================================
-- 3) Unified Flip Core
-- ============================================================================
CREATE TABLE IF NOT EXISTS unified_flip (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id UUID NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,

    -- Auction | Bazaar | Craft | Forge | Shard | Fusion
    flip_type VARCHAR(32) NOT NULL,

    required_capital NUMERIC(20, 4) NOT NULL,
    expected_profit NUMERIC(20, 4) NOT NULL,
    roi NUMERIC(12, 6) NOT NULL,
    roi_per_hour NUMERIC(12, 6),

    duration_seconds BIGINT NOT NULL CHECK (duration_seconds >= 0),
    liquidity_score NUMERIC(7, 4),
    risk_score NUMERIC(7, 4),

    -- Optional computed fields for ranking/filtering
    risk_adjusted_roi_per_hour NUMERIC(12, 6),
    slippage_bps NUMERIC(8, 2),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (flip_type IN ('AUCTION', 'BAZAAR', 'CRAFTING', 'FORGE', 'KATGRADE', 'FUSION'))
);

CREATE INDEX IF NOT EXISTS idx_unified_flip_snapshot ON unified_flip (snapshot_id);
CREATE INDEX IF NOT EXISTS idx_unified_flip_type ON unified_flip (flip_type);
CREATE INDEX IF NOT EXISTS idx_unified_flip_profit ON unified_flip (expected_profit DESC);
CREATE INDEX IF NOT EXISTS idx_unified_flip_roi_h ON unified_flip (roi_per_hour DESC NULLS LAST);

-- Inputs/Outputs normalisiert
CREATE TABLE IF NOT EXISTS unified_flip_item (
    id BIGSERIAL PRIMARY KEY,
    flip_id UUID NOT NULL REFERENCES unified_flip(id) ON DELETE CASCADE,
    io_type VARCHAR(16) NOT NULL,
    item_id VARCHAR(128) NOT NULL REFERENCES item(id),
    amount NUMERIC(20, 6) NOT NULL CHECK (amount > 0),
    unit_price NUMERIC(20, 6),
    total_price NUMERIC(20, 6),

    -- Reihenfolge für mehrstufige Chains / Anzeige
    step_order INT,

    CHECK (io_type IN ('INPUT', 'OUTPUT'))
);

CREATE INDEX IF NOT EXISTS idx_unified_flip_item_flip ON unified_flip_item (flip_id);
CREATE INDEX IF NOT EXISTS idx_unified_flip_item_item ON unified_flip_item (item_id);

CREATE TABLE IF NOT EXISTS unified_flip_fee (
    id BIGSERIAL PRIMARY KEY,
    flip_id UUID NOT NULL REFERENCES unified_flip(id) ON DELETE CASCADE,
    fee_type VARCHAR(64) NOT NULL,
    amount NUMERIC(20, 6) NOT NULL,
    currency VARCHAR(16) NOT NULL DEFAULT 'COINS'
);

CREATE INDEX IF NOT EXISTS idx_unified_flip_fee_flip ON unified_flip_fee (flip_id);

-- ============================================================================
-- 4) Constraints, Liquidity & Risk Signals
-- ============================================================================
CREATE TABLE IF NOT EXISTS unified_flip_constraint (
    id BIGSERIAL PRIMARY KEY,
    flip_id UUID NOT NULL REFERENCES unified_flip(id) ON DELETE CASCADE,
    constraint_type VARCHAR(64) NOT NULL,
    string_value TEXT,
    int_value INT,
    long_value BIGINT
);

CREATE INDEX IF NOT EXISTS idx_unified_flip_constraint_flip ON unified_flip_constraint (flip_id);

CREATE TABLE IF NOT EXISTS unified_flip_metric (
    id BIGSERIAL PRIMARY KEY,
    flip_id UUID NOT NULL REFERENCES unified_flip(id) ON DELETE CASCADE,
    metric_key VARCHAR(128) NOT NULL,
    metric_value NUMERIC(20, 8) NOT NULL,
    metric_unit VARCHAR(32),
    UNIQUE (flip_id, metric_key)
);

-- ============================================================================
-- 5) Recipes (Craft/Forge/Fusion kompatibel)
-- ============================================================================
CREATE TABLE IF NOT EXISTS recipe (
    recipe_id VARCHAR(128) PRIMARY KEY,
    output_item_id VARCHAR(128) NOT NULL REFERENCES item(id),
    process_type VARCHAR(32) NOT NULL,
    process_duration_seconds BIGINT NOT NULL CHECK (process_duration_seconds >= 0),
    CHECK (process_type IN ('CRAFT', 'FORGE', 'FUSION', 'SHARD'))
);

CREATE TABLE IF NOT EXISTS recipe_ingredient (
    id BIGSERIAL PRIMARY KEY,
    recipe_id VARCHAR(128) NOT NULL REFERENCES recipe(recipe_id) ON DELETE CASCADE,
    item_id VARCHAR(128) NOT NULL REFERENCES item(id),
    amount INT NOT NULL CHECK (amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_recipe_output_item ON recipe (output_item_id);
CREATE INDEX IF NOT EXISTS idx_recipe_ingredient_recipe ON recipe_ingredient (recipe_id);

-- ============================================================================
-- 6) Market Snapshot Storage (Auction/Bazaar Inputs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS market_snapshot (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id UUID NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
    auction_count INT NOT NULL,
    bazaar_product_count INT NOT NULL,
    auctions_json TEXT NOT NULL,
    bazaar_products_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_snapshot ON market_snapshot (snapshot_id);

-- ============================================================================
-- 7) Source Hash Tracking
-- ============================================================================
CREATE TABLE IF NOT EXISTS data_source_hash (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_key VARCHAR(120) NOT NULL UNIQUE,
    hash VARCHAR(128) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 8) Flip Type Coverage Audit
-- ============================================================================
CREATE TABLE IF NOT EXISTS flip_type_audit (
    flip_type VARCHAR(32) PRIMARY KEY,
    ingestion_status VARCHAR(32) NOT NULL,
    calculation_status VARCHAR(32) NOT NULL,
    persistence_status VARCHAR(32) NOT NULL,
    api_exposure_status VARCHAR(32) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (flip_type IN ('AUCTION', 'BAZAAR', 'CRAFTING', 'FORGE', 'KATGRADE', 'FUSION'))
);
