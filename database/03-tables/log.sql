create table log(
    id serial not null primary key,
    severity smallint not null,
    component_id smallint not null,
    date timestamp not null default now(),
    message text not null,
    data json,
    acknowledged_date timestamp
);
