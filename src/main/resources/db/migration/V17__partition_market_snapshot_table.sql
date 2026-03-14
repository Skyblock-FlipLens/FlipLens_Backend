do $$
declare
    v_day date;
begin
    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = 'market_snapshot'
    ) and not exists (
        select 1
        from pg_partitioned_table p
        join pg_class c on c.oid = p.partrelid
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = 'market_snapshot'
    ) then
        alter table public.market_snapshot rename to market_snapshot_unpartitioned;

        create table public.market_snapshot (
            id uuid not null,
            snapshot_timestamp_epoch_millis bigint not null,
            auction_count integer not null,
            bazaar_product_count integer not null,
            auctions_json text not null,
            bazaar_products_json text not null,
            created_at_epoch_millis bigint not null
        ) partition by range (snapshot_timestamp_epoch_millis);

        for v_day in
            select distinct (to_timestamp(snapshot_timestamp_epoch_millis / 1000.0) at time zone 'UTC')::date
            from public.market_snapshot_unpartitioned
            order by 1
        loop
            perform ensure_partition_exists_day('public', 'market_snapshot', v_day);
        end loop;

        for v_day in
            select (now() at time zone 'UTC')::date + offsets.day_offset
            from generate_series(0, 14) as offsets(day_offset)
        loop
            perform ensure_partition_exists_day('public', 'market_snapshot', v_day);
        end loop;

        execute 'create table if not exists public.market_snapshot_default partition of public.market_snapshot default';

        insert into public.market_snapshot (
            id,
            snapshot_timestamp_epoch_millis,
            auction_count,
            bazaar_product_count,
            auctions_json,
            bazaar_products_json,
            created_at_epoch_millis
        )
        select
            id,
            snapshot_timestamp_epoch_millis,
            auction_count,
            bazaar_product_count,
            auctions_json,
            bazaar_products_json,
            created_at_epoch_millis
        from public.market_snapshot_unpartitioned
        order by snapshot_timestamp_epoch_millis, id;

        drop table public.market_snapshot_unpartitioned;

        create index idx_market_snapshot_snapshot_ts_epoch_millis
            on public.market_snapshot (snapshot_timestamp_epoch_millis);

        create index idx_market_snapshot_id
            on public.market_snapshot (id);
    end if;
end
$$;
