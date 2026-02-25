-- Low-risk index hardening for dominant flip-storage read paths.
-- Keep scope intentionally narrow to avoid over-indexing hot write tables.

-- Legacy flip reads frequently constrain by snapshot and optionally flip type.
create index if not exists idx_flip_snapshot_ts_flip_type
    on flip (snapshot_timestamp_epoch_millis, flip_type);

-- Current-storage paging commonly filters by flip type and orders by stable id.
create index if not exists idx_flip_current_flip_type_stable_flip_id
    on flip_current (flip_type, stable_flip_id);

-- Latest trend lookup resolves max(valid_to) per flip key.
create index if not exists idx_flip_trend_segment_flip_key_valid_to
    on flip_trend_segment (flip_key, valid_to_snapshot_epoch_millis);
