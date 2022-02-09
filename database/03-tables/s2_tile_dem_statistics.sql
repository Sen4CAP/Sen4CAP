create table s2_tile_dem_statistics(
    tile_id text not null primary key,
    minimum smallint not null,
    maximum smallint not null,
    mean real not null,
    stddev real not null
);
