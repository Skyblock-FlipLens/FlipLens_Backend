create table election_snapshot (
    fetched_at timestamp(6) with time zone not null,
    id uuid not null,
    payload_hash varchar(128) not null,
    payload_json text not null,
    primary key (id)
);

create index idx_election_snapshot_fetched_at
    on election_snapshot (fetched_at);
