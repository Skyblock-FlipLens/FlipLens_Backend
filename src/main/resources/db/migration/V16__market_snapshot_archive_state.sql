create table if not exists market_snapshot_archive_state (
    source_partition varchar(255) primary key,
    parent_table varchar(255) not null,
    partition_day_utc date not null,
    raw_row_count bigint not null default 0,
    retained_snapshot_id uuid,
    retained_snapshot_timestamp_epoch_millis bigint,
    finalized boolean not null default false,
    failed boolean not null default false,
    finalized_at_epoch_millis bigint,
    updated_at_epoch_millis bigint not null
);

create index if not exists idx_market_snapshot_archive_state_parent_day
    on market_snapshot_archive_state (parent_table, partition_day_utc);
