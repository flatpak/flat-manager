CREATE TABLE builds (
    id serial primary key,
    is_published bool not null default false,
    created timestamp with time zone not null default now()
)
