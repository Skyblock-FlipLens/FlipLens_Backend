create or replace function ensure_partition_exists_day(
    p_schema text,
    p_parent_table text,
    p_day_utc date
)
returns text
language plpgsql
as $$
declare
    v_partition_name text;
    v_from_epoch_millis bigint;
    v_to_epoch_millis bigint;
begin
    v_partition_name := format('%s_%s', p_parent_table, to_char(p_day_utc, 'YYYY_MM_DD'));
    v_from_epoch_millis := (extract(epoch from (p_day_utc::timestamp at time zone 'UTC')) * 1000)::bigint;
    v_to_epoch_millis := (extract(epoch from ((p_day_utc + interval '1 day')::timestamp at time zone 'UTC')) * 1000)::bigint;

    execute format(
        'create table if not exists %I.%I partition of %I.%I for values from (%s) to (%s)',
        p_schema,
        v_partition_name,
        p_schema,
        p_parent_table,
        v_from_epoch_millis,
        v_to_epoch_millis
    );
    return v_partition_name;
exception
    when others then
        -- Helper is best-effort and intentionally non-fatal for environments
        -- where parent tables are not partitioned yet.
        return null;
end;
$$;
