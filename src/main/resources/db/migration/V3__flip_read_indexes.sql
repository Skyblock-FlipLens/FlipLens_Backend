create index if not exists idx_flip_snapshot_type
    on flip (snapshot_timestamp_epoch_millis, flip_type);

create index if not exists idx_flip_snapshot_id_desc
    on flip (snapshot_timestamp_epoch_millis desc, id desc);

create index if not exists idx_flip_current_snapshot_type
    on flip_current (snapshot_timestamp_epoch_millis, flip_type);
