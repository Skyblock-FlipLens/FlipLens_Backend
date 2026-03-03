create table if not exists partition_watermark (
    key varchar(128) primary key,
    value bigint not null,
    updated_at_utc timestamptz not null default now()
);
