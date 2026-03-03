create table if not exists bz_item_snapshot_daily_rollup (
    bucket_day_utc date not null,
    product_id varchar(255) not null,
    avg_buy_price double precision,
    avg_sell_price double precision,
    avg_spread double precision,
    avg_buy_volume double precision,
    avg_sell_volume double precision,
    sample_count bigint not null,
    first_snapshot_ts bigint not null,
    last_snapshot_ts bigint not null,
    created_at_epoch_millis bigint not null,
    updated_at_epoch_millis bigint not null,
    primary key (bucket_day_utc, product_id)
);

create index if not exists idx_bz_daily_rollup_product_day
    on bz_item_snapshot_daily_rollup (product_id, bucket_day_utc desc);

create table if not exists ah_item_snapshot_daily_rollup (
    bucket_day_utc date not null,
    item_key varchar(255) not null,
    avg_bin_lowest double precision,
    avg_bin_lowest5_mean double precision,
    avg_bin_p50 double precision,
    avg_bin_p95 double precision,
    avg_bid_p50 double precision,
    avg_bin_count double precision,
    avg_ending_soon_count double precision,
    sample_count bigint not null,
    first_snapshot_ts bigint not null,
    last_snapshot_ts bigint not null,
    created_at_epoch_millis bigint not null,
    updated_at_epoch_millis bigint not null,
    primary key (bucket_day_utc, item_key)
);

create index if not exists idx_ah_daily_rollup_item_day
    on ah_item_snapshot_daily_rollup (item_key, bucket_day_utc desc);
