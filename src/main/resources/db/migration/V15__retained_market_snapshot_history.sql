create table if not exists market_snapshot_retained (
    id uuid primary key,
    snapshot_timestamp_epoch_millis bigint not null unique,
    auction_count integer not null,
    bazaar_product_count integer not null,
    auctions_json text not null,
    bazaar_products_json text not null,
    created_at_epoch_millis bigint not null,
    retained_at_epoch_millis bigint not null
);

create index if not exists idx_market_snapshot_retained_snapshot_ts_epoch_millis
    on market_snapshot_retained (snapshot_timestamp_epoch_millis desc);
