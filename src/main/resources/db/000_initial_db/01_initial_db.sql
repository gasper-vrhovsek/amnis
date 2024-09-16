create table if not exists match_events
(
    id uuid PRIMARY KEY,
    match_id varchar(128) NOT NULL,
    market_id integer NOT NULL,
    outcome_id varchar(128),
    specifiers varchar(256),
    created_at timestamp
);
