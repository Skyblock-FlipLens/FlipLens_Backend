create index if not exists idx_flip_snapshot_ts_epoch_millis
    on flip (snapshot_timestamp_epoch_millis);

alter table if exists flip_step
    drop constraint if exists fksdkp89mkhwmrhkay6wm24ociv;

alter table if exists flip_step
    drop constraint if exists fk_flip_step_flip;

alter table if exists flip_step
    add constraint fk_flip_step_flip
    foreign key (flip_id)
    references flip (id)
    on delete cascade;

alter table if exists flip_constraints
    drop constraint if exists fkmwsv7e5kia37jc5o8viqe69s2;

alter table if exists flip_constraints
    drop constraint if exists fk_flip_constraints_flip;

alter table if exists flip_constraints
    add constraint fk_flip_constraints_flip
    foreign key (flip_id)
    references flip (id)
    on delete cascade;
