create table compaction_control (
    id            integer primary key default 1,
    requested     boolean not null default false,
    requested_at  timestamptz,
    requested_by  text,
    last_run_at   timestamptz,
    last_run_ok   boolean,
    last_run_msg  text
);

insert into compaction_control (id)
values (1)
on conflict (id) do nothing;
