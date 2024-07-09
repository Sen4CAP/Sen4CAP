begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running MAJA migrations to 4.5.4';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('2.0', '3.0', '3.0.0', '3.1.0')) then

            _statement := $str$
                alter table l1_tile_history add column if not exists node_id text;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                create or replace function sp_clear_pending_l1_tiles(
                    _node_id text
                )
                returns void
                as
                $$
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    delete
                    from l1_tile_history
                    using downloader_history
                    where downloader_history.id = l1_tile_history.downloader_history_id
                      and l1_tile_history.status_id = 1 -- processing
                      and l1_tile_history.node_id = _node_id
                      and downloader_history.satellite_id in (1, 2); -- sentinel2, landsat8

                    update downloader_history
                    set status_id = 2 -- downloaded
                    where status_id = 7 -- processing
                      and not exists (
                        select *
                        from l1_tile_history
                        where status_id = 1 -- processing
                    );
                end;
                $$ language plpgsql volatile;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                create or replace function sp_mark_l1_tile_done(
                    _downloader_history_id int,
                    _tile_id text,
                    _cloud_coverage int,
                    _snow_coverage int
                )
                returns boolean
                as
                $$
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    update l1_tile_history
                    set status_id = 3, -- done
                        status_timestamp = now(),
                        failed_reason = null,
                        cloud_coverage = _cloud_coverage,
                        snow_coverage = _snow_coverage
                    where (downloader_history_id, tile_id) = (_downloader_history_id, _tile_id);

                    return sp_update_l1_tile_status(_downloader_history_id);
                end;
                $$ language plpgsql volatile;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                create or replace function sp_mark_l1_tile_failed(
                    _downloader_history_id int,
                    _tile_id text,
                    _reason text,
                    _should_retry boolean,
                    _cloud_coverage int,
                    _snow_coverage int
                )
                returns boolean
                as
                $$
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    update l1_tile_history
                    set status_id = 2, -- failed
                        status_timestamp = now(),
                        retry_count = case _should_retry
                            when true then retry_count + 1
                            else (
                                    select
                                        coalesce(
                                            (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id = (
                                                    select site_id
                                                    from downloader_history
                                                    where id = _downloader_history_id)
                                            ), (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id is null
                                            )
                                        ) :: int
                                ) + 1
                        end,
                        failed_reason = _reason,
                        cloud_coverage = _cloud_coverage,
                        snow_coverage = _snow_coverage
                    where (downloader_history_id, tile_id) = (_downloader_history_id, _tile_id);

                    return sp_update_l1_tile_status(_downloader_history_id);
                end;
                $$ language plpgsql volatile;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                create or replace function sp_start_l1_tile_processing(
                    _node_id text
                )
                returns table (
                    site_id int,
                    satellite_id smallint,
                    orbit_id int,
                    tile_id text,
                    downloader_history_id int,
                    path text,
                    prev_l2a_path text
                ) as
                $$
                declare _satellite_id smallint;
                declare _orbit_id int;
                declare _tile_id text;
                declare _downloader_history_id int;
                declare _path text;
                declare _prev_l2a_path text;
                declare _site_id int;
                declare _product_date timestamp;
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    create temporary table if not exists site_config(
                        key,
                        site_id,
                        value
                    ) as
                    select
                        keys.key,
                        site.id,
                        config.value
                    from site
                    cross join (
                        values
                            ('processor.l2a.s2.implementation'),
                            ('processor.l2a.optical.retry-interval'),
                            ('processor.l2a.optical.max-retries'),
                            ('processor.l2a.optical.num-workers'),
                            ('s2.enabled'),
                            ('l8.enabled')
                    ) as keys(key)
                    cross join lateral (
                        select
                            coalesce((
                                select value
                                from config
                                where key = keys.key
                                and config.site_id = site.id
                            ), (
                                select value
                                from config
                                where key = keys.key
                                and config.site_id is null
                            )) as value
                    ) config;

                    select l1_tile_history.satellite_id,
                           l1_tile_history.orbit_id,
                           l1_tile_history.tile_id,
                           l1_tile_history.downloader_history_id
                    into _satellite_id,
                         _orbit_id,
                         _tile_id,
                         _downloader_history_id
                    from l1_tile_history
                    inner join downloader_history on downloader_history.id = l1_tile_history.downloader_history_id
                    inner join site on site.id = downloader_history.site_id
                    cross join lateral (
                        select
                            (
                                select value :: int as max_retries
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 'processor.l2a.optical.max-retries'
                            ),
                            (
                                select value :: interval as retry_interval
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 'processor.l2a.optical.retry-interval'
                            ),
                            (
                                select value :: boolean as s2_enabled
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 's2.enabled'
                            ),
                            (
                                select value :: boolean as l8_enabled
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 'l8.enabled'
                            )
                    ) config
                    where l1_tile_history.status_id = 2 -- failed
                      and l1_tile_history.retry_count < config.max_retries
                      and l1_tile_history.status_timestamp < now() - config.retry_interval
                      and case downloader_history.satellite_id
                              when 1 then config.s2_enabled
                              when 2 then config.l8_enabled
                              else false
                      end
                      and (
                          site.enabled
                          or exists (
                              select *
                              from downloader_history
                              where downloader_history.status_id = 2 -- downloaded
                                and l1_tile_history.tile_id = any(downloader_history.tiles)
                                and l1_tile_history.orbit_id = downloader_history.orbit_id
                                and exists (
                                    select *
                                    from site
                                    where site.id = downloader_history.site_id
                                      and site.enabled
                                )
                          )
                      )
                    order by l1_tile_history.status_timestamp
                    limit 1;

                    if found then
                        select downloader_history.product_date,
                               downloader_history.full_path,
                               downloader_history.site_id
                        into _product_date,
                             _path,
                             _site_id
                        from downloader_history
                        where id = _downloader_history_id;

                        update l1_tile_history
                        set status_id = 1, -- processing
                            status_timestamp = now(),
                            node_id = _node_id
                        where (l1_tile_history.downloader_history_id, l1_tile_history.tile_id) = (_downloader_history_id, _tile_id);
                    else
                        select distinct
                            downloader_history.satellite_id,
                            downloader_history.orbit_id,
                            tile_ids.tile_id,
                            downloader_history.id,
                            downloader_history.product_date,
                            downloader_history.full_path,
                            downloader_history.site_id
                        into _satellite_id,
                            _orbit_id,
                            _tile_id,
                            _downloader_history_id,
                            _product_date,
                            _path,
                            _site_id
                        from downloader_history
                        inner join site on site.id = downloader_history.site_id
                        cross join lateral (
                                select unnest(tiles) as tile_id
                            ) tile_ids
                        cross join lateral (
                            select
                                (
                                    select value as l2a_implementation
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'processor.l2a.s2.implementation'
                                ),
                                (
                                    select value :: int as max_retries
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'processor.l2a.optical.max-retries'
                                ),
                                (
                                    select value :: boolean as s2_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 's2.enabled'
                                ),
                                (
                                    select value :: boolean as l8_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'l8.enabled'
                                )
                        ) config
                        where (
                            config.l2a_implementation = 'sen2cor'
                            and downloader_history.satellite_id = 1
                            or not exists (
                                select *
                                from l1_tile_history
                                where (l1_tile_history.satellite_id,
                                    l1_tile_history.orbit_id,
                                    l1_tile_history.tile_id) =
                                    (downloader_history.satellite_id,
                                    downloader_history.orbit_id,
                                    tile_ids.tile_id)
                                and (status_id = 1 or -- processing
                                    retry_count < config.max_retries and status_id = 2 -- failed
                                )
                            )
                        )
                        and not exists (
                            select *
                            from l1_tile_history
                            where (l1_tile_history.downloader_history_id, l1_tile_history.tile_id) = (downloader_history.id, tile_ids.tile_id)
                        )
                        and downloader_history.status_id in (2, 7) -- downloaded, processing
                        and site.enabled
                        and downloader_history.satellite_id in (1, 2) -- sentinel2, landsat8
                        and case downloader_history.satellite_id
                                when 1 then config.s2_enabled
                                when 2 then config.l8_enabled
                                else false
                        end
                        order by satellite_id,
                                orbit_id,
                                tile_id,
                                product_date
                        limit 1;

                        if found then
                            insert into l1_tile_history (
                                satellite_id,
                                orbit_id,
                                tile_id,
                                downloader_history_id,
                                status_id,
                                node_id
                            ) values (
                                _satellite_id,
                                _orbit_id,
                                _tile_id,
                                _downloader_history_id,
                                1, -- processing
                                _node_id
                            );

                            update downloader_history
                            set status_id = 7 -- processing
                            where id = _downloader_history_id;
                        end if;
                    end if;

                    if _downloader_history_id is not null then
                        select product.full_path
                        into _prev_l2a_path
                        from product
                        where product.site_id = _site_id
                          and product.product_type_id = 1 -- l2a
                          and product.satellite_id = _satellite_id
                          and product.created_timestamp < _product_date
                          and product.tiles :: text[] @> array[_tile_id]
                          and (product.satellite_id <> 1 -- sentinel2
                               or product.orbit_id = _orbit_id)
                        order by created_timestamp desc
                        limit 1;

                        return query
                            select _site_id,
                                _satellite_id,
                                _orbit_id,
                                _tile_id,
                                _downloader_history_id,
                                _path,
                                _prev_l2a_path;
                    end if;
                end;
                $$ language plpgsql volatile;

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                create or replace function sp_update_l1_tile_status(
                    _downloader_history_id int
                )
                returns boolean
                as
                $$
                begin
                    if not exists(
                        select unnest(tiles)
                        from downloader_history
                        where id = _downloader_history_id
                        except all
                        select tile_id
                        from l1_tile_history
                        where downloader_history_id = _downloader_history_id
                          and (l1_tile_history.status_id = 3 -- done
                            or l1_tile_history.status_id = 2 -- failed
                                and l1_tile_history.retry_count >= (
                                    select
                                        coalesce(
                                            (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id = (
                                                    select site_id
                                                    from downloader_history
                                                    where id = _downloader_history_id)
                                            ), (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id is null
                                            )
                                        ) :: int
                                )
                          )
                    ) then
                        if exists(
                            select *
                            from l1_tile_history
                            where downloader_history_id = _downloader_history_id
                              and status_id = 3 -- done
                        ) then
                            update downloader_history
                            set status_id = 5 -- processed
                            where id = _downloader_history_id;
                        else
                            update downloader_history
                            set status_id = 6 -- processing_failed
                            where id = _downloader_history_id;
                        end if;
                        return true;
                    else
                        return false;
                    end if;
                end;
                $$ language plpgsql volatile;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required) VALUES (1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true) ON conflict(id) DO UPDATE SET required = true;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false) ON conflict(id) DO UPDATE SET required = false;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (7, 'L2-S1 Pre-Processor', 'l2-s1', 'L2 S1 &mdash; SAR Pre-Processor', true) ON conflict(id) DO UPDATE SET required = true;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (8, 'LPIS/GSAA', 'lpis', 'LPIS / GSAA Processor', true)  ON conflict(id) DO UPDATE SET required = true;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (9, 'S4C L4A Crop Type','s4c_l4a', 'Sen4CAP L4A &mdash; Crop Type', false)  ON conflict(id) DO UPDATE SET required = false;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (10, 'S4C L4B Grassland Mowing','s4c_l4b', 'Sen4CAP L4B &mdash; Grassland Mowing', false)  ON conflict(id) DO UPDATE SET required = false;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (11, 'S4C L4C Agricultural Practices','s4c_l4c', 'Sen4CAP L4C &mdash; Agricultural Practices', false)  ON conflict(id) DO UPDATE SET required = false;
                INSERT INTO processor (id, name, short_name, label, required) VALUES (14, 'S4C Marker Database PR1','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1', false)  ON conflict(id) DO UPDATE SET required = false;
                INSERT INTO processor (id, name, short_name, label, required)  VALUES  (21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true) ON conflict(id) DO UPDATE SET required = true;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$            
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('monitor-agent.disk-path', NULL, '/mnt/archive/', '2015-07-20 10:27:29.301355+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/';
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$                        
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_fapar', 'L3B processor will produce FAPAR', 'int', TRUE, 4, TRUE, 'Produce FAPAR', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce FAPAR', label = 'Produce FAPAR', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_fcover', 'L3B processor will produce FCOVER', 'int', TRUE, 4, TRUE, 'Produce FCOVER', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce FCOVER', label = 'Produce FCOVER', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_lai', 'L3B processor will produce LAI', 'int', TRUE, 4, TRUE, 'Produce LAI', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce LAI', label = 'Produce LAI', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_ndvi', 'L3B processor will produce NDVI', 'int', TRUE, 4, TRUE, 'Produce NDVI', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce NDVI', label = 'Produce NDVI', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                
                
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_ndwi', 'L3B processor will produce NDWI', 'int', TRUE, 4, TRUE, 'Produce NDWI', NULL)  ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce NDWI', label = 'Produce NDWI', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_brightness', 'L3B processor will produce brightness', 'int', TRUE, 4, TRUE, 'Produce brightness', NULL)  ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce brightness', label = 'Produce brightness', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.cloud_optimized_geotiff_output', 'Generate L3B Cloud Optimized Geotiff outputs', 'int', TRUE, 4, TRUE, 'Generate L3B Cloud Optimized Geotiff outputs', NULL)  ON conflict(key) DO UPDATE SET friendly_name = 'Generate L3B Cloud Optimized Geotiff outputs', label = 'Generate L3B Cloud Optimized Geotiff outputs', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                
                -- UPDATE config SET value = 1 WHERE key in ('processor.l3b.filter.produce_fapar', 'processor.l3b.filter.produce_fcover', 'processor.l3b.filter.produce_lai', 'processor.l3b.filter.produce_ndvi', 'processor.l3b.filter.produce_ndwi', 'processor.l3b.filter.produce_brightness', 'processor.l3b.cloud_optimized_geotiff_output') and value = 'true';
                -- UPDATE config SET value = 0 WHERE key in ('processor.l3b.filter.produce_fapar', 'processor.l3b.filter.produce_fcover', 'processor.l3b.filter.produce_lai', 'processor.l3b.filter.produce_ndvi', 'processor.l3b.filter.produce_ndwi', 'processor.l3b.filter.produce_brightness', 'processor.l3b.cloud_optimized_geotiff_output') and value = 'false';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja.gipp-path', NULL, '/mnt/archive/gipp/maja') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/gipp/maja';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja.remove-fre', NULL, '0') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja.remove-sre', NULL, '1') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '1';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.cog-tiffs', NULL, '0')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.compress-tiffs', NULL, '0')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.max-retries', NULL, '3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '3';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.num-workers', NULL, '4') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '4';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.output-path', NULL, '/mnt/archive/maccs_def/{site}/{processor}/')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/maccs_def/{site}/{processor}/';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.retry-interval', NULL, '1 day') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '1 day';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.s2.implementation', NULL, 'maja') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'maja';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.sen2cor.gipp-path', NULL, '/mnt/archive/gipp/sen2cor') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/gipp/sen2cor';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.srtm-path', NULL, '/mnt/archive/srtm') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/srtm';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.swbd-path', NULL, '/mnt/archive/swbd') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/swbd';
--            INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.working-dir', NULL, '/mnt/archive/demmaccs_tmp/') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/demmaccs_tmp/';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.processors_image', NULL, 'sen4x/l2a-processors:0.2.3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-processors:0.2.3';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.sen2cor_image', NULL, 'sen4x/sen2cor:2.10.01-ubuntu-20.04') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/sen2cor:2.10.01-ubuntu-20.04';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja_image', NULL, 'sen4x/maja:4.5.4-centos-7') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/maja:4.5.4-centos-7';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.4.1') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'osgeo/gdal:ubuntu-full-3.4.1';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.l8_align_image', NULL, 'sen4x/l2a-l8-alignment:0.1.2') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-l8-alignment:0.1.2';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.dem_image', NULL, 'sen4x/l2a-dem:0.1.3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-dem:0.1.3';
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            
           _statement := 'update meta set version = ''3.1.0'';';
            raise notice '%', _statement;
            execute _statement;
            
        end if;
    end if;

    raise notice 'complete';
	
end;
$migration$;

commit;