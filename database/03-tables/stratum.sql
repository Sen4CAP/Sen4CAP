create table stratum(
    site_id smallint not null,
    year smallint not null,
    stratum_type_id smallint not null,
    stratum_id int not null,
    wkb_geometry geometry not null,
    primary key (site_id, year, stratum_type_id, stratum_id)
);
