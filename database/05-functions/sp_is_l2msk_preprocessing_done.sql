-- DROP function sp_is_l2msk_preprocessing_done(smallint, timestamp, timestamp);

CREATE OR REPLACE FUNCTION sp_is_l2msk_preprocessing_done
(
    _site_id site.id%TYPE,
    _start_date timestamp DEFAULT NULL::timestamp,
    _end_date timestamp DEFAULT NULL::timestamp
  )
  RETURNS boolean AS
$func$

declare _l2a_msk_enabled boolean;

BEGIN
    with site_config (key, site_id, value) as (
    select
            keys.key,
            site.id,
            config.value
        from site
        cross join (
            values
                ('processor.l2a_msk.enabled')
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
    select value :: boolean into _l2a_msk_enabled
        from site_config
        where site_config.site_id = _site_id
        and key = 'processor.l2a_msk.enabled';
    
    RETURN (
            (_l2a_msk_enabled is NULL or NOT _l2a_msk_enabled) or 
            (SELECT COUNT(id) FROM product WHERE product_type_id = 1 -- l2a products
                AND site_id = _site_id
                AND (_start_date is null or created_timestamp >= _start_date) AND 
                (_end_date is null  or created_timestamp < _end_date + interval '1 day') AND
                NOT EXISTS (
                    SELECT product_id FROM product_provenance WHERE parent_product_id = id AND
                           product_id IN (SELECT id FROM product WHERE site_id = _site_id AND product_type_id = 26))) = 0);
    
END
$func$ LANGUAGE plpgsql STABLE;