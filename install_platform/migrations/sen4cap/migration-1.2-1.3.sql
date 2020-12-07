begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('1.2', '1.3')) then
            _statement := $str$
                INSERT INTO processor (id, name, short_name, label) VALUES (3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI') on conflict(id) DO UPDATE SET name = 'L3B Vegetation Status', short_name = 'l3b', label = 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI';

                DELETE FROM config WHERE key = 'processor.l3b.lai.link_l3c_to_l3b';
                DELETE FROM config WHERE key = 'processor.l3b_lai.sub_products';

                DELETE FROM config WHERE key = 'executor.processor.l3b_lai.slurm_qos';
                DELETE FROM config WHERE key = 'general.scratch-path.l3b_lai';
                INSERT INTO config(key, site_id, value) VALUES ('executor.processor.l3b.slurm_qos', NULL, 'qoslai') on conflict DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('general.scratch-path.l3b', NULL, '/mnt/archive/orchestrator_temp/l3b/{job_id}/{task_id}-{module}') on conflict DO nothing;

                INSERT INTO config_category VALUES (2, 'L2A Atmospheric Correction', 1, false) on conflict(id) DO UPDATE SET name = 'L2A Atmospheric Correction',               allow_per_site_customization = false;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.s2.implementation', NULL, 'maja', '2020-09-07 14:17:52.846794+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.s2.retry-interval', NULL, '1 day', '2020-09-07 14:36:37.906825+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.num-workers', NULL, '4', '2020-09-07 14:36:37.906825+03') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.s2.retry-interval', 'Retry interval for the L2A processor', 'string', false, 2) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.num-workers', 'Parallelism degree of the L2A processor', 'int', false, 2) on conflict DO nothing;

                DELETE FROM config WHERE key = 'executor.module.path.bands-extractor';
                DELETE FROM config WHERE key = 'executor.module.path.data-smoothing';
                DELETE FROM config WHERE key = 'executor.module.path.feature-extraction';
                DELETE FROM config WHERE key = 'executor.module.path.lai-bv-err-image-invertion';
                DELETE FROM config WHERE key = 'executor.module.path.lai-bv-image-invertion';
                DELETE FROM config WHERE key = 'executor.module.path.lai-bv-input-variable-generation';
                DELETE FROM config WHERE key = 'executor.module.path.lai-err-time-series-builder';
                DELETE FROM config WHERE key = 'executor.module.path.lai-inverse-model-learning';
                DELETE FROM config WHERE key = 'executor.module.path.lai-local-window-reproc-splitter';
                DELETE FROM config WHERE key = 'executor.module.path.lai-local-window-reprocessing';
                DELETE FROM config WHERE key = 'executor.module.path.lai-models-extractor';
                DELETE FROM config WHERE key = 'executor.module.path.lai-mono-date-mask-flags';
                DELETE FROM config WHERE key = 'executor.module.path.lai-msk-flags-time-series-builder';
                DELETE FROM config WHERE key = 'executor.module.path.lai-ndvi-rvi-extractor';
                DELETE FROM config WHERE key = 'executor.module.path.lai-prosail-simulator';
                DELETE FROM config WHERE key = 'executor.module.path.lai-time-series-builder';
                DELETE FROM config WHERE key = 'executor.module.path.lai-training-data-generator';
                DELETE FROM config WHERE key = 'executor.module.path.product-formatter';
                DELETE FROM config WHERE key = 'executor.module.path.xml-statistics';
                DELETE FROM config WHERE key = 'executor.module.path.dummy-module';

                DELETE FROM config_metadata WHERE key = 'executor.module.path.bands-extractor';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.data-smoothing';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.feature-extraction';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-bv-err-image-invertion';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-bv-image-invertion';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-bv-input-variable-generation';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-err-time-series-builder';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-inverse-model-learning';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-local-window-reproc-splitter';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-local-window-reprocessing';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-models-extractor';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-mono-date-mask-flags';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-msk-flags-time-series-builder';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-ndvi-rvi-extractor';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-prosail-simulator';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-time-series-builder';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-training-data-generator';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.product-formatter';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.xml-statistics';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.dummy-module';


                DELETE FROM config WHERE key = 'processor.l3b.lai.localwnd.bwr';
                DELETE FROM config WHERE key = 'processor.l3b.lai.localwnd.fwr';

                DELETE FROM config WHERE key = 'processor.l3b.mono_date_lai';
                DELETE FROM config WHERE key = 'processor.l3b.reprocess';
                DELETE FROM config WHERE key = 'processor.l3b.fitted';

                DELETE FROM config WHERE key = 'processor.l3b.mono_date_ndvi_only';
                DELETE FROM config WHERE key = 'processor.l3b.ndvi.tiles_filter';


                DELETE FROM config WHERE key = 'processor.l3b.lai.produce_ndvi';
                DELETE FROM config WHERE key = 'processor.l3b.lai.produce_lai';
                DELETE FROM config WHERE key = 'processor.l3b.lai.produce_fapar';
                DELETE FROM config WHERE key = 'processor.l3b.lai.produce_fcover';

                DELETE FROM config_metadata WHERE key = 'processor.l3b.lai.link_l3c_to_l3b';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.mono_date_ndvi_only';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.ndvi.tiles_filter';

                DELETE FROM config_metadata WHERE key = 'processor.l3b.lai.produce_ndvi';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.lai.produce_lai';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.lai.produce_fapar';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.lai.produce_fcover';

                DELETE FROM config_metadata WHERE key = 'executor.processor.l3b_lai.slurm_qos';
                DELETE FROM config_metadata WHERE key = 'general.scratch-path.l3b_lai';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.fitted';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.lai.localwnd.fwr';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.mono_date_lai';
                DELETE FROM config_metadata WHERE key = 'processor.l3b.reprocess';

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_ndvi', NULL, '1', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_lai', NULL, '1', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_fapar', NULL, '1', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_fcover', NULL, '1', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_in_domain_flags', NULL, '0', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndvi', 'L3B LAI processor will produce NDVI', 'int', false, 4) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_lai', 'L3B LAI processor will produce LAI', 'int', false, 4) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fapar', 'L3B LAI processor will produce FAPAR', 'int', false, 4) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fcover', 'L3B LAI processor will produce FCOVER', 'int', false, 4) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_in_domain_flags', 'L3B processor will input domain flags', 'int', false, 4) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('executor.processor.l3b.slurm_qos', 'Slurm QOS for LAI processor', 'string', true, 8) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.scratch-path.l3b', 'Path for L3B temporary files', 'string', false, 1) on conflict DO nothing;

                DELETE FROM config WHERE key = 'executor.module.path.lai-end-of-job';
                DELETE FROM config_metadata WHERE key = 'executor.module.path.lai-end-of-job';

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.end-of-job', NULL, '/usr/bin/true', '2016-01-12 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.end-of-job', 'End of a multi root steps job', 'file', true, 8) on conflict DO nothing;

            $str$;
            raise notice '%', _statement;
            execute _statement;


            _statement := $str$
                CREATE OR REPLACE FUNCTION sp_get_dashboard_products(
                    _site_id integer[] DEFAULT NULL::integer[],
                    _product_type_id integer[] DEFAULT NULL::integer[],
                    _season_id smallint DEFAULT NULL::smallint,
                    _satellit_id integer[] DEFAULT NULL::integer[],
                    _since_timestamp timestamp with time zone DEFAULT NULL::timestamp with time zone,
                    _until_timestamp timestamp with time zone DEFAULT NULL::timestamp with time zone,
                    _tiles character varying[] DEFAULT NULL::character varying[])
                  RETURNS SETOF json AS
                $BODY$
                        DECLARE q text;
                        BEGIN
                            q := $sql$
                            WITH site_names(id, name, geog, row) AS (
                                select id, name, st_astext(geog), row_number() over (order by name)
                                from site
                                ),
                                product_type_names(id, name, description, row) AS (
                                select id, name, description, row_number() over (order by description)
                                from product_type
                                ),
                                data(id, satellite_id, product, product_type_id, product_type,product_type_description,processor,site,full_path,quicklook_image,footprint,created_timestamp, site_coord) AS (
                                SELECT
                                P.id,
                                P.satellite_id,
                                P.name,
                                PT.id,
                                PT.name,
                                PT.description,
                                PR.name,
                                S.name,
                                P.full_path,
                                P.quicklook_image,
                                P.footprint,
                                P.created_timestamp,
                                S.geog
                                FROM product P
                                JOIN product_type_names PT ON P.product_type_id = PT.id
                                JOIN processor PR ON P.processor_id = PR.id
                                JOIN site_names S ON P.site_id = S.id
                                WHERE TRUE -- COALESCE(P.is_archived, FALSE) = FALSE
                                AND EXISTS (
                                    SELECT * FROM season WHERE season.site_id =P.site_id AND P.created_timestamp BETWEEN season.start_date AND season.end_date + interval '1 day'
                            $sql$;
                            IF $3 IS NOT NULL THEN
                            q := q || $sql$
                                AND season.id=$3
                                $sql$;
                            END IF;

                            q := q || $sql$
                            )
                            $sql$;

                            IF $1 IS NOT NULL THEN
                            q := q || $sql$
                                AND P.site_id = ANY($1)
                            $sql$;
                            END IF;

                            IF $2 IS NOT NULL THEN
                            q := q || $sql$
                                AND P.product_type_id= ANY($2)

                                $sql$;
                            END IF;

                        IF $5 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp >= to_timestamp(cast($5 as TEXT),'YYYY-MM-DD HH24:MI:SS')
                            $sql$;
                        END IF;

                        IF $6 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp <= to_timestamp(cast($6 as TEXT),'YYYY-MM-DD HH24:MI:SS') + interval '1 day'
                            $sql$;
                        END IF;

                        IF $7 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.tiles <@$7 AND P.tiles!='{}'
                            $sql$;
                        END IF;


                        q := q || $sql$
                            ORDER BY S.row, PT.row, P.name
                            )
                        --         select * from data;
                            SELECT array_to_json(array_agg(row_to_json(data)), true) FROM data;
                            $sql$;

                        --     raise notice '%', q;

                            RETURN QUERY
                            EXECUTE q
                            USING _site_id, _product_type_id, _season_id, _satellit_id, _since_timestamp, _until_timestamp, _tiles;
                        END
                        $BODY$
                  LANGUAGE plpgsql STABLE;
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
                    where status_id = 1; -- processing

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
                update config_category set allow_per_site_customization = true where id = 2;
                delete from config_metadata where key in ('demmaccs.output-path', 'demmaccs.gips-path', 'demmaccs.srtm-path', 'demmaccs.swbd-path', 'demmaccs.maccs-launcher', 'demmaccs.working-dir', 'demmaccs.compress-tiffs', 'demmaccs.cog-tiffs', 'demmaccs.remove-sre', 'demmaccs.remove-fre');

                -- srtm, swbd name
                -- working dir name, type
                -- remove {sre,fre} name
                update config set key = 'processor.l2a.maja.launcher', friendly_name = 'MAJA binary location', type = 'file' where key = 'demmaccs.maccs-launcher';
                update config set key = 'processor.l2a.srtm-path', friendly_name = 'Path to SRTM dataset', type = 'directory' where key = 'demmaccs.srtm-path';
                update config set key = 'processor.l2a.swbd-path', friendly_name = 'Path to SWBD dataset', type = 'directory' where key = 'demmaccs.swbd-path';
                update config set key = 'processor.l2a.working-dir', friendly_name = 'Working directory', type = 'directory' where key = 'demmaccs.working-dir';
                update config set key = 'processor.l2a.optical.output-path', friendly_name = 'path for L2A products', type = 'file' where key = 'demmaccs.output-path';
                update config set key = 'processor.l2a.optical.compress-tiffs', friendly_name = 'Compress the resulted L2A TIFF files' where key = 'demmaccs.compress-tiffs';
                update config set key = 'processor.l2a.optical.cog-tiffs', friendly_name = 'Produce L2A tiff files as Cloud Optimized Geotiff' where key = 'demmaccs.cog-tiffs';
                update config set key = 'processor.l2a.maja.remove-sre', friendly_name = 'Remove SRE files from resulting products' where key = 'demmaccs.remove-sre';
                update config set key = 'processor.l2a.maja.remove-fre', friendly_name = 'Remove FRE files from resulting products' where key = 'demmaccs.remove-fre';

                INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2);
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.max-retries', 'Number of retries for the L2A processor', 'int', false, 2);
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.num-workers', 'Parallelism degree of the L2A processor', 'int', false, 2);
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.retry-interval', 'Retry interval for the L2A processor', 'string', false, 2);
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.gipp-path', 'MAJA GIPP path', 'directory', false, 2);
                INSERT INTO config_metadata VALUES ('processor.l2a.sen2cor.gipp-path', 'Sen2Cor GIPP path', 'directory', false, 2);

                update config set key = 'processor.l2a.srtm-path' where key = 'demmaccs.srtm-path';
                update config set key = 'processor.l2a.swbd-path' where key = 'demmaccs.swbd-path';
                update config set key = 'processor.l2a.working-dir' where key = 'demmaccs.working-dir';
                update config set key = 'processor.l2a.optical.output-path' where key = 'demmaccs.output-path';
                update config set key = 'processor.l2a.optical.compress-tiffs' where key = 'demmaccs.compress-tiffs';
                update config set key = 'processor.l2a.optical.cog-tiffs' where key = 'demmaccs.cog-tiffs';
                update config set key = 'processor.l2a.maja.remove-sre' where key = 'demmaccs.remove-sre';
                update config set key = 'processor.l2a.maja.remove-fre' where key = 'demmaccs.remove-fre';

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.s2.implementation', NULL, 'maja', '2020-09-07 14:17:52.846794+03');
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.max-retries', NULL, '3', '2020-09-15 16:02:27.164968+03');
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.num-workers', NULL, '4', '2020-09-07 14:36:37.906825+03');
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.retry-interval', NULL, '1 day', '2020-09-07 14:36:37.906825+03');
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.gipp-path', NULL, '/mnt/archive/gipp/maja', '2016-02-24 18:12:16.464479+02');
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.sen2cor.gipp-path', NULL, '/mnt/archive/gipp/sen2cor', '2020-09-15 16:48:05.415193+03');

                delete from config where key = 'demmaccs.gips-path';

                delete from config_category where id = 16;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;

           _statement := 'update meta set version = ''1.3'';';
            raise notice '%', _statement;
            execute _statement;
        end if;
    end if;

    raise notice 'complete';
end;
$migration$;

commit;

