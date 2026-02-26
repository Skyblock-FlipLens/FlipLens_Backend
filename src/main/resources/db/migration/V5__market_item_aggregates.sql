create table ah_item_snapshot (
    snapshot_ts bigint not null,
    item_key varchar(255) not null,
    bin_lowest bigint,
    bin_lowest5_mean bigint,
    bin_p50 bigint,
    bin_p95 bigint,
    bin_count integer not null default 0,
    bid_p50 bigint,
    ending_soon_count integer not null default 0,
    created_at_epoch_millis bigint not null,
    primary key (snapshot_ts, item_key)
);

create index idx_ah_item_snapshot_item_ts
    on ah_item_snapshot (item_key, snapshot_ts desc);

create index idx_ah_item_snapshot_ts
    on ah_item_snapshot (snapshot_ts);

create table bz_item_snapshot (
    snapshot_ts bigint not null,
    product_id varchar(255) not null,
    buy_price double precision,
    sell_price double precision,
    buy_volume bigint,
    sell_volume bigint,
    created_at_epoch_millis bigint not null,
    primary key (snapshot_ts, product_id)
);

create index idx_bz_item_snapshot_item_ts
    on bz_item_snapshot (product_id, snapshot_ts desc);

create index idx_bz_item_snapshot_ts
    on bz_item_snapshot (snapshot_ts);
