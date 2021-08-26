create table region (
    region_code text not null primary key,
    country_code text not null,
    name text not null,
    geom geometry not null
);
