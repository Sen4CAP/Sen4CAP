create or replace function sp_get_site_strata(_site_id int)
    returns table (
            stratum_id int,
            geom bytea,
            epsg_code int,
            tiles char(5)[]
    )
as
$$
begin
    return query
        with site_tiles as (select tile_id
                            from sp_get_site_tiles(_site_id :: smallint, 1 :: smallint)),
             site_stratum as (select stratum.stratum_id, wkb_geometry as geometry, st_transform(wkb_geometry, 4326) :: geography as stratum_geog
                              from stratum
                              where site_id = _site_id)
        select site_stratum.stratum_id,
               ST_AsBinary(site_stratum.geometry) as geometry,
               ST_SRID(site_stratum.geometry) as epsg_code,
               array_agg(shape_tiles_s2.tile_id) as tile_id
        from site_stratum
                 inner join shape_tiles_s2 on ST_Intersects(shape_tiles_s2.geog, site_stratum.stratum_geog)
        where exists (select * from site_tiles where site_tiles.tile_id = shape_tiles_s2.tile_id)
        group by site_stratum.stratum_id, site_stratum.geometry
        order by site_stratum.stratum_id;
end;
$$
    language plpgsql
    stable;
