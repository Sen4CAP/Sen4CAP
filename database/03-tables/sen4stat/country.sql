create table country (
    country_code text not null primary key,
    name text not null,
    geom geometry not null
);
