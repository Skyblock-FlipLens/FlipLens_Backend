do $$
declare
    v_day date;
begin
    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = 'ah_item_snapshot'
    ) and not exists (
        select 1
        from pg_partitioned_table p
        join pg_class c on c.oid = p.partrelid
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = 'ah_item_snapshot'
    ) then
        alter table public.ah_item_snapshot rename to ah_item_snapshot_unpartitioned;

        create table public.ah_item_snapshot (
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
        ) partition by range (snapshot_ts);

        for v_day in
            select distinct (to_timestamp(snapshot_ts / 1000.0) at time zone 'UTC')::date
            from public.ah_item_snapshot_unpartitioned
            order by 1
        loop
            perform ensure_partition_exists_day('public', 'ah_item_snapshot', v_day);
        end loop;

        for v_day in
            select (now() at time zone 'UTC')::date + offsets.day_offset
            from generate_series(0, 14) as offsets(day_offset)
        loop
            perform ensure_partition_exists_day('public', 'ah_item_snapshot', v_day);
        end loop;

        execute 'create table if not exists public.ah_item_snapshot_default partition of public.ah_item_snapshot default';

        insert into public.ah_item_snapshot (
            snapshot_ts,
            item_key,
            bin_lowest,
            bin_lowest5_mean,
            bin_p50,
            bin_p95,
            bin_count,
            bid_p50,
            ending_soon_count,
            created_at_epoch_millis
        )
        select
            snapshot_ts,
            item_key,
            bin_lowest,
            bin_lowest5_mean,
            bin_p50,
            bin_p95,
            bin_count,
            bid_p50,
            ending_soon_count,
            created_at_epoch_millis
        from public.ah_item_snapshot_unpartitioned
        order by snapshot_ts, item_key;

        drop table public.ah_item_snapshot_unpartitioned;

        create index idx_ah_item_snapshot_item_ts
            on public.ah_item_snapshot (item_key, snapshot_ts desc);

        create index idx_ah_item_snapshot_ts
            on public.ah_item_snapshot (snapshot_ts);
    end if;
end
$$;

do $$
declare
    v_day date;
begin
    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = 'bz_item_snapshot'
    ) and not exists (
        select 1
        from pg_partitioned_table p
        join pg_class c on c.oid = p.partrelid
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = 'bz_item_snapshot'
    ) then
        alter table public.bz_item_snapshot rename to bz_item_snapshot_unpartitioned;

        create table public.bz_item_snapshot (
            snapshot_ts bigint not null,
            product_id varchar(255) not null,
            buy_price double precision,
            sell_price double precision,
            buy_volume bigint,
            sell_volume bigint,
            created_at_epoch_millis bigint not null,
            primary key (snapshot_ts, product_id)
        ) partition by range (snapshot_ts);

        for v_day in
            select distinct (to_timestamp(snapshot_ts / 1000.0) at time zone 'UTC')::date
            from public.bz_item_snapshot_unpartitioned
            order by 1
        loop
            perform ensure_partition_exists_day('public', 'bz_item_snapshot', v_day);
        end loop;

        for v_day in
            select (now() at time zone 'UTC')::date + offsets.day_offset
            from generate_series(0, 14) as offsets(day_offset)
        loop
            perform ensure_partition_exists_day('public', 'bz_item_snapshot', v_day);
        end loop;

        execute 'create table if not exists public.bz_item_snapshot_default partition of public.bz_item_snapshot default';

        insert into public.bz_item_snapshot (
            snapshot_ts,
            product_id,
            buy_price,
            sell_price,
            buy_volume,
            sell_volume,
            created_at_epoch_millis
        )
        select
            snapshot_ts,
            product_id,
            buy_price,
            sell_price,
            buy_volume,
            sell_volume,
            created_at_epoch_millis
        from public.bz_item_snapshot_unpartitioned
        order by snapshot_ts, product_id;

        drop table public.bz_item_snapshot_unpartitioned;

        create index idx_bz_item_snapshot_item_ts
            on public.bz_item_snapshot (product_id, snapshot_ts desc);

        create index idx_bz_item_snapshot_ts
            on public.bz_item_snapshot (snapshot_ts);
    end if;
end
$$;
