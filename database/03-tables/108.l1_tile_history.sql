create table l1_tile_history(
    satellite_id smallint not null references satellite(id),
    orbit_id int not null,
    tile_id text not null,
    downloader_history_id int not null references downloader_history(id),
    status_id int not null references l1_tile_status(id),
    status_timestamp timestamp with time zone not null default now(),
    node_id text not null,
    retry_count int not null default 0,
    failed_reason text,
    cloud_coverage int,
    snow_coverage int,
    primary key (downloader_history_id, tile_id)
);
