create table province (
    province_code text not null primary key,
    region_code text not null,
    name text not null,
    geom geometry not null
);
