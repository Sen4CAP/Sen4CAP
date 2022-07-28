-- drop function sp_is_fmask_preprocessing_done(smallint, timestamp, timestamp)

CREATE OR REPLACE FUNCTION sp_is_fmask_preprocessing_done
(
    _site_id site.id%TYPE,
    _start_date timestamp DEFAULT NULL::timestamp,
    _end_date timestamp DEFAULT NULL::timestamp
  )
  RETURNS boolean AS
$func$

declare _fmask_enabled boolean;

BEGIN
    with site_config (key, site_id, value) as (
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
        ) config )
    select value :: boolean into _fmask_enabled
        from site_config
        where site_config.site_id = _site_id
        and key = 'processor.fmask.enabled';
    

    RETURN (not _fmask_enabled) or (select count(*) from downloader_history where 
            status_id in (2, 5, 7) and 
            satellite_id in (1, 2) and 
            site_id = _site_id and 
            (_start_date is null or product_date >= _start_date) and 
            (_end_date is null or product_date < _end_date + interval '1 day') and 
            id not in (select downloader_history_id from fmask_history) or
            id in (select downloader_history_id from fmask_history where status_id = 1 or 
                       (status_id = 2 and retry_count < 3))) = 0;
END
$func$ LANGUAGE plpgsql STABLE;
