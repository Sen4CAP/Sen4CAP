create or replace function sp_start_fmask_l1_tile_processing(
    _node_id text
)
returns table (
    site_id int,
    satellite_id smallint,
    downloader_history_id int,
    path text) as
$$
declare _satellite_id smallint;
declare _downloader_history_id int;
declare _path text;
declare _site_id int;
declare _product_date timestamp;
declare _orbit_id integer;
declare _tile_id text;
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
            ('processor.fmask.enabled'),
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

    select fmask_history.satellite_id,
           fmask_history.downloader_history_id
    into _satellite_id,
         _downloader_history_id
    from fmask_history
    where status_id = 2 -- failed
      and retry_count < 3
      and status_timestamp < now() - interval '1 day'
    order by status_timestamp
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

        update fmask_history
        set status_id = 1, -- processing
            status_timestamp = now(),
            node_id = _node_id
        where (fmask_history.downloader_history_id) = (_downloader_history_id);
    else
        select distinct
            downloader_history.satellite_id,
            downloader_history.id,
            downloader_history.product_date,
            downloader_history.full_path,
            downloader_history.site_id,
            downloader_history.orbit_id,
            downloader_history.tiles[1]
        into _satellite_id,
            _downloader_history_id,
            _product_date,
            _path,
            _site_id,
            _orbit_id,
            _tile_id
        from downloader_history
        inner join site on site.id = downloader_history.site_id
        cross join lateral (
            select
                (
                    select value :: boolean as fmask_enabled
                    from site_config
                    where site_config.site_id = downloader_history.site_id
                    and key = 'processor.fmask.enabled'
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
        where not exists (
            select *
            from fmask_history
            where fmask_history.downloader_history_id = downloader_history.id
        )
        and downloader_history.status_id in (2, 5, 7) -- downloaded, processing
        and site.enabled
        and fmask_enabled
        and case downloader_history.satellite_id
            when 1 then config.s2_enabled
            when 2 then config.l8_enabled
            else false
        end
        order by satellite_id, product_date
        limit 1;

        if found then
            insert into fmask_history (
                satellite_id,
                downloader_history_id,
                status_id,
                node_id
            ) values (
                _satellite_id,
                _downloader_history_id,
                1, -- processing
                _node_id
            );
        end if;
    end if;

    if _downloader_history_id is not null then
        return query
            select _site_id,
                _satellite_id,
                _downloader_history_id,
                _path;
    end if;
end;
$$ language plpgsql volatile;
