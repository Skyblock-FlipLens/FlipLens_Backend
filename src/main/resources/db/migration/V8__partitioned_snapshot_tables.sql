create table if not exists partition_maintenance_audit (
    id bigserial primary key,
    executed_at_utc timestamptz not null default now(),
    operation varchar(64) not null,
    table_name varchar(255) not null,
    partition_name varchar(255),
    dry_run boolean not null default false,
    details text
);

create index if not exists idx_partition_maintenance_audit_executed_at
    on partition_maintenance_audit (executed_at_utc desc);
