begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('1.3', '2.0')) then

            -- New Tables
            _statement := $str$
                CREATE TABLE IF NOT EXISTS log(
                    id serial not null primary key,
                    severity smallint not null,
                    component_id smallint not null,
                    date timestamp not null default now(),
                    message text not null,
                    data json,
                    acknowledged_date timestamp
                );
                CREATE TABLE IF NOT EXISTS component(
                    id smallint not null primary key,
                    name text not null
                );
                
                CREATE TABLE IF NOT EXISTS severity(
                    id smallint not null primary key,
                    name text not null
                );
                
                INSERT INTO severity(id, name) VALUES (1, 'Debug'), (2, 'Info'), (3, 'Warning'), (4, 'Error'), (5, 'Fatal') ON conflict DO nothing;
                
                ALTER TABLE log DROP CONSTRAINT IF EXISTS fk_log_component;
                ALTER TABLE log ADD CONSTRAINT fk_log_component FOREIGN KEY(component_id) REFERENCES component(id);
                ALTER TABLE log DROP CONSTRAINT IF EXISTS fk_log_severity;
                ALTER TABLE log ADD CONSTRAINT fk_log_severity FOREIGN KEY(severity) REFERENCES severity(id);
                
                
            $str$;
            raise notice '%', _statement;
            execute _statement;
        
            -- Product types and processor IDs changes
            _statement := $str$    
                INSERT INTO product_type (id, name, description, is_raster) VALUES (17, 's4c_mdb1','Sen4CAP Marker Database PR1', 'false')
                        on conflict(id) DO UPDATE SET id = 17, name = 's4c_mdb1', description = 'Sen4CAP Marker Database PR1', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (18, 's4c_mdb2','Sen4CAP Marker Database PR2', 'false')
                        on conflict(id) DO UPDATE SET id = 18, name = 's4c_mdb2', description = 'Sen4CAP Marker Database PR2', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (19, 's4c_mdb3','Sen4CAP Marker Database PR3', 'false')
                        on conflict(id) DO UPDATE SET id = 19, name = 's4c_mdb3', description = 'Sen4CAP Marker Database PR3', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (20, 's4c_mdb4','Sen4CAP Marker Database PR4', 'false')
                        on conflict(id) DO UPDATE SET id = 20, name = 's4c_mdb4', description = 'Sen4CAP Marker Database PR4', is_raster = 'false';
                
                UPDATE product_type SET name = 'l3b' WHERE id = 3;
                UPDATE product_type SET description = 'L3B LAI mono-date product' WHERE id = 3;
                UPDATE product_type SET id = 12 where id = 4 and name = 's4c_l4a';
                UPDATE product_type SET id = 13 where id = 5 and name = 's4c_l4b';
                UPDATE product_type SET id = 15 where id = 6 and name = 's4c_l4c';
                UPDATE product_type SET description = 'Sen4CAP L4A Crop type product' WHERE id = 11;
                UPDATE product_type SET description = 'Sen4CAP L4B Grassland Mowing product' WHERE id = 13;
                UPDATE product_type SET description = 'Sen4CAP Sen4CAP L4C Agricultural Practices product' WHERE id = 14;
                UPDATE product_type SET description = 'Sen4CAP L3C LAI Reprocessed product' WHERE id = 16;
                
                UPDATE product SET product_type_id = 12 WHERE product_type_id = 4 AND full_path LIKE '%s4c_l4a%';
                UPDATE product SET product_type_id = 13 WHERE product_type_id = 5 AND full_path LIKE '%s4c_l4b%';
                UPDATE product SET product_type_id = 15 WHERE product_type_id = 6 AND full_path LIKE '%s4c_l4c%';
                UPDATE product SET product_type_id = 16 WHERE product_type_id = 8 AND full_path LIKE '%s4c_l3c%';
             
                UPDATE job SET processor_id = 9 WHERE processor_id = 4 and (select short_name from processor where id = 4) = 's4c_l4a';
                UPDATE job SET processor_id = 10 WHERE processor_id = 5 and (select short_name from processor where id = 5) = 's4c_l4b';
                UPDATE job SET processor_id = 11 WHERE processor_id = 6 and (select short_name from processor where id = 6) = 's4c_l4c';
             
                UPDATE scheduled_task SET processor_id = 9 WHERE processor_id = 4 and (select short_name from processor where id = 4) = 's4c_l4a';
                UPDATE scheduled_task SET processor_id = 10 WHERE processor_id = 5 and (select short_name from processor where id = 5) = 's4c_l4b';
                UPDATE scheduled_task SET processor_id = 11 WHERE processor_id = 6 and (select short_name from processor where id = 6) = 's4c_l4c';
               
                UPDATE processor SET id = 9 where id = 4 and short_name = 's4c_l4a';
                UPDATE processor SET id = 10 where id = 5 and short_name = 's4c_l4b';
                UPDATE processor SET id = 11 where id = 6 and short_name = 's4c_l4c';

                UPDATE product SET processor_id = 9 WHERE processor_id = 4 AND full_path LIKE '%s4c_l4a%';
                UPDATE product SET processor_id = 10 WHERE processor_id = 5 AND full_path LIKE '%s4c_l4b%';
                UPDATE product SET processor_id = 11 WHERE processor_id = 6 AND full_path LIKE '%s4c_l4c%';
             
                INSERT INTO processor (id, name, short_name, label) VALUES (14, 'S4C Marker Database PR1','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1') on conflict DO nothing;

            $str$;
            raise notice '%', _statement;
            execute _statement;
                
            _statement := $str$
                create or replace function sp_clear_pending_l1_tiles()
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
                create or replace function sp_start_l1_tile_processing()
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
                            status_timestamp = now()
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
                                status_id
                            ) values (
                                _satellite_id,
                                _orbit_id,
                                _tile_id,
                                _downloader_history_id,
                                1 -- processing
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
                UPDATE config_category SET allow_per_site_customization = true;
                DELETE FROM config_category WHERE id = 16;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DELETE FROM config where key = 'demmaccs.maccs-launcher';
                DELETE FROM config where key = 'demmaccs.srtm-path';
                DELETE FROM config where key = 'demmaccs.swbd-path';
                DELETE FROM config where key = 'demmaccs.working-dir';
                DELETE FROM config where key = 'demmaccs.output-path';
                DELETE FROM config where key = 'demmaccs.gips-path';
                DELETE FROM config WHERE key = 'demmaccs.compress-tiffs';
                DELETE FROM config WHERE key = 'demmaccs.cog-tiffs';
                DELETE FROM config WHERE key = 'demmaccs.remove-sre';
                DELETE FROM config WHERE key = 'demmaccs.remove-fre';
                DELETE FROM config WHERE key = 'processor.l2a.s2.retry-interval';
            
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.s2.implementation', NULL, 'maja', '2020-09-07 14:17:52.846794+03') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.max-retries', NULL, '3', '2020-09-15 16:02:27.164968+03') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.num-workers', NULL, '4', '2020-09-07 14:36:37.906825+03') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.retry-interval', NULL, '1 day', '2020-09-07 14:36:37.906825+03') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.compress-tiffs', NULL, '0', '2017-10-24 14:56:57.501918+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.cog-tiffs', NULL, '0', '2017-10-24 14:56:57.501918+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.gipp-path', NULL, '/mnt/archive/gipp/maja', '2016-02-24 18:12:16.464479+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.launcher', NULL, '/opt/maja/3.2.2/bin/maja', '2016-02-25 16:29:07.763339+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.remove-sre', NULL, '1', '2017-10-24 14:56:57.501918+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.remove-fre', NULL, '0', '2017-10-24 14:56:57.501918+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.sen2cor.gipp-path', NULL, '/mnt/archive/gipp/sen2cor', '2020-09-15 16:48:05.415193+03') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.output-path', NULL, '/mnt/archive/maccs_def/{site}/{processor}/', '2016-02-24 18:09:17.379905+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.srtm-path', NULL, '/mnt/archive/srtm', '2016-02-25 11:11:36.372405+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.swbd-path', NULL, '/mnt/archive/swbd', '2016-02-25 11:12:04.008319+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.working-dir', NULL, '/mnt/archive/demmaccs_tmp/', '2016-02-25 17:31:06.01191+02') ON conflict DO nothing;

                -- Tillage processor keys               
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.tillage_monitoring', NULL, '0', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                -- Marker database keys
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_mdb1', NULL, '/mnt/archive/orchestrator_temp/s4c_mdb1/{job_id}/{task_id}-{module}', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.extract-l4c-markers', NULL, '/usr/bin/extract_l4c_markers_wrapper.py', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb-csv-to-ipc-export', NULL, '/usr/bin/run_csv_to_ipc.sh', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.sched_prds_hist_file', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/l4c_scheduled_prds_history.txt', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_amp', NULL, 'N/A', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_cohe', NULL, 'N/A', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_ndvi', NULL, 'N/A', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.markers_add_no_data_rows', NULL, '1', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.ndvi_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.amp_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.cohe_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.lai_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_mdb1.slurm_qos', NULL, 'qoss4cmdb1', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'qoss4cmdb1';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4a.slurm_qos', NULL, 'qoss4cl4a', '2015-08-24 17:44:38.29255+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'qoss4cl4a';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4b.slurm_qos', NULL, 'qoss4cl4b', '2015-08-24 17:44:38.29255+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'qoss4cl4b';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4c.slurm_qos', NULL, 'qoss4cl4c', '2015-08-24 17:44:38.29255+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'qoss4cl4c';

                -- Executor/orchestrator/scheduler changes
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.inter-proc-com-type', NULL, 'http', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.resource-manager.name', NULL, 'slurm', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.http-server.listen-ip', NULL, '127.0.0.1', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.http-server.listen-port', NULL, '8084', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.http-server.listen-port', NULL, '8083', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.http-server.listen-ip', NULL, '127.0.0.1', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
            
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DELETE FROM config_metadata where key = 'demmaccs.maccs-launcher';
                DELETE FROM config_metadata where key = 'demmaccs.srtm-path';
                DELETE FROM config_metadata where key = 'demmaccs.swbd-path';
                DELETE FROM config_metadata where key = 'demmaccs.working-dir';
                DELETE FROM config_metadata where key = 'demmaccs.output-path';
                DELETE FROM config_metadata where key = 'demmaccs.gips-path';
                DELETE FROM config_metadata WHERE key = 'demmaccs.compress-tiffs';
                DELETE FROM config_metadata WHERE key = 'demmaccs.cog-tiffs';
                DELETE FROM config_metadata WHERE key = 'demmaccs.remove-sre';
                DELETE FROM config_metadata WHERE key = 'demmaccs.remove-fre';
                DELETE FROM config_metadata WHERE key = 'processor.l2a.s2.retry-interval';

                INSERT INTO config_metadata VALUES ('downloader.skip.existing', 'If enabled, products downloaded for another site will be duplicated, in database only, for the current site', 'bool', false, 15) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.max-retries', 'Number of retries for the L2A processor', 'int', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.num-workers', 'Parallelism degree of the L2A processor', 'int', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.retry-interval', 'Retry interval for the L2A processor', 'string', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.compress-tiffs', 'Compress the resulted L2A TIFF files', 'bool', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.cog-tiffs', 'Produce L2A tiff files as Cloud Optimized Geotiff', 'bool', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.gipp-path', 'MAJA GIPP path', 'directory', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.launcher', 'MAJA binary location', 'file', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-sre', 'Remove SRE files from resulted L2A product', 'bool', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-fre', 'Remove FRE files from resulted L2A product', 'bool', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.sen2cor.gipp-path', 'Sen2Cor GIPP path', 'directory', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.output-path', 'path for L2A products', 'directory', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.srtm-path', 'Path to the DEM dataset', 'directory', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.swbd-path', 'Path to the SWBD dataset', 'directory', false, 2) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.working-dir', 'Working directory', 'string', false, 2) ON conflict DO nothing;
                
                
                -- Marker database new config category                
                INSERT INTO config_category VALUES (26, 'S4C Marker database 1', 4, true) ON conflict DO nothing;
                
                -- Tillage processor keys               
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tillage_monitoring', 'Enable or disable tillage monitoring', 'bool', false, 20, true, 'Enable or disable tillage monitoring') ON conflict DO nothing;
                
                -- Marker database keys                
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_mdb1.slurm_qos', 'Slurm QOS for MDB1 processor', 'string', true, 8) ON conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_amp', 'The list of AMP products', 'select', FALSE, 26, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}') ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_cohe', 'The list of COHE products', 'select', FALSE, 26, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}') ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_ndvi', 'The list of NDVI products', 'select', FALSE, 26, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}') ON conflict DO nothing;

                INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_mdb1', 'Path for S4C MDB1 temporary files', 'string', false, 1) ON conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('executor.module.path.extract-l4c-markers', 'Script for extracting L4C markers', 'file', true, 8) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.mdb-csv-to-ipc-export', 'Script for extracting markers csv to IPC file', 'file', true, 8) ON conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.markers_add_no_data_rows', 'Add in markers parcel rows containg only NA/NA1/NR', 'bool', true, 20, true, 'Add in markers parcel rows containg only NA/NA1/NR') ON conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.sched_prds_hist_file', 'File where the list of the scheduled L4Cs is kept', 'string', true, 26) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndvi_enabled', 'NDVI markers extraction enabled', 'bool', true, 26) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_enabled', 'AMP markers extraction enabled', 'bool', true, 26) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.cohe_enabled', 'COHE markers extraction enabled', 'bool', true, 26) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.lai_enabled', 'LAI markers extraction enabled', 'bool', true, 26) ON conflict DO nothing;
                
                -- Executor/orchestrator/scheduler changes
                INSERT INTO config_metadata VALUES ('general.inter-proc-com-type', 'Type of the interprocess communication', 'string', false, 1) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.resource-manager.name', 'Executor resource manager name', 'string', false, 1) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.http-server.listen-ip', 'Executor HTTP listen ip', 'string', false, 1) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.http-server.listen-port', 'Executor HTTP listen port', 'string', false, 1) ON conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-ip', 'Orchestrator HTTP listen ip', 'string', false, 1) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-port', 'Orchestrator HTTP listen port', 'string', false, 1) ON conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$


            $str$;
            raise notice '%', _statement;
            execute _statement;

           _statement := 'update meta set version = ''2.0'';';
            raise notice '%', _statement;
            execute _statement;
        end if;
    end if;

    raise notice 'complete';
end;
$migration$;

commit;

