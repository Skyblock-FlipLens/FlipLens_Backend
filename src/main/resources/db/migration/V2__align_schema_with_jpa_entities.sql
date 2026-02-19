-- Align schema with current JPA model used by the running application.
-- Keep V1 tables intact, add/adjust compatibility tables & columns for ddl-auto=validate.

-- ---------------------------------------------------------------------------
-- Items (NEU.model.Item)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS items (
    item_id VARCHAR(128) PRIMARY KEY,
    display_name VARCHAR(255),
    minecraft_id VARCHAR(128),
    rarity VARCHAR(64),
    category VARCHAR(128),
    lore TEXT
);

CREATE TABLE IF NOT EXISTS item_info_links (
    item_id VARCHAR(128) NOT NULL REFERENCES items(item_id) ON DELETE CASCADE,
    position INT NOT NULL,
    info_link TEXT NOT NULL,
    PRIMARY KEY (item_id, position)
);

-- ---------------------------------------------------------------------------
-- Recipes (Flipping.Recipe)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS recipes (
    recipe_id VARCHAR(128) PRIMARY KEY,
    output_item_id VARCHAR(128) NOT NULL REFERENCES items(item_id),
    process_type VARCHAR(32) NOT NULL,
    process_duration_seconds BIGINT NOT NULL CHECK (process_duration_seconds >= 0)
);

CREATE TABLE IF NOT EXISTS recipe_ingredients (
    id BIGSERIAL PRIMARY KEY,
    recipe_id VARCHAR(128) NOT NULL REFERENCES recipes(recipe_id) ON DELETE CASCADE,
    item_internal_name VARCHAR(128) NOT NULL,
    amount INT NOT NULL CHECK (amount > 0)
);

CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipe_id ON recipe_ingredients (recipe_id);

-- ---------------------------------------------------------------------------
-- Flip / Step / Constraints (Flipping domain)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS flip (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flip_type VARCHAR(32),
    result_item_id VARCHAR(128) NOT NULL,
    snapshot_timestamp_epoch_millis BIGINT
);

CREATE INDEX IF NOT EXISTS idx_flip_snapshot_ts_epoch_millis ON flip (snapshot_timestamp_epoch_millis);

CREATE TABLE IF NOT EXISTS flip_step (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    flip_id UUID NOT NULL REFERENCES flip(id) ON DELETE CASCADE,
    step_order INT,
    type VARCHAR(32) NOT NULL,
    duration_type VARCHAR(32) NOT NULL,
    base_duration_seconds BIGINT,
    duration_factor DOUBLE PRECISION,
    resource VARCHAR(32) NOT NULL,
    resource_units INT NOT NULL,
    scheduling_policy VARCHAR(32) NOT NULL,
    params_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_flip_step_flip_id ON flip_step (flip_id);

CREATE TABLE IF NOT EXISTS flip_constraints (
    flip_id UUID NOT NULL REFERENCES flip(id) ON DELETE CASCADE,
    constraint_type VARCHAR(64) NOT NULL,
    string_value TEXT,
    int_value INT,
    long_value BIGINT
);

CREATE INDEX IF NOT EXISTS idx_flip_constraints_flip_id ON flip_constraints (flip_id);

-- ---------------------------------------------------------------------------
-- Market snapshot compatibility with MarketSnapshotEntity
-- ---------------------------------------------------------------------------
ALTER TABLE market_snapshot
    ADD COLUMN IF NOT EXISTS snapshot_timestamp_epoch_millis BIGINT,
    ADD COLUMN IF NOT EXISTS created_at_epoch_millis BIGINT;

-- Hibernate validates 'text' here (columnDefinition), so ensure text type.
ALTER TABLE market_snapshot
    ALTER COLUMN auctions_json TYPE TEXT USING auctions_json::text,
    ALTER COLUMN bazaar_products_json TYPE TEXT USING bazaar_products_json::text;

-- Fill newly added epoch columns for existing rows, keeping them non-null.
UPDATE market_snapshot
SET snapshot_timestamp_epoch_millis = COALESCE(
        snapshot_timestamp_epoch_millis,
        (EXTRACT(EPOCH FROM COALESCE(created_at, NOW())) * 1000)::BIGINT
    ),
    created_at_epoch_millis = COALESCE(
        created_at_epoch_millis,
        (EXTRACT(EPOCH FROM COALESCE(created_at, NOW())) * 1000)::BIGINT
    )
WHERE snapshot_timestamp_epoch_millis IS NULL
   OR created_at_epoch_millis IS NULL;

ALTER TABLE market_snapshot
    ALTER COLUMN snapshot_timestamp_epoch_millis SET NOT NULL,
    ALTER COLUMN created_at_epoch_millis SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_market_snapshot_snapshot_ts_epoch_millis
    ON market_snapshot (snapshot_timestamp_epoch_millis);
