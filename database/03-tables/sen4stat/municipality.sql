create table municipality (
    municipality_code text not null primary key,
    province_code text not null,
    name text not null,
    geom geometry not null
);
