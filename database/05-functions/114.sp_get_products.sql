-- Function: sp_get_products(smallint, smallint, timestamp with time zone, timestamp with time zone)

-- DROP FUNCTION sp_get_products(smallint, smallint, timestamp with time zone, timestamp with time zone);

CREATE OR REPLACE FUNCTION sp_get_products(
    IN _site_id smallint DEFAULT NULL::smallint,
    IN _product_type_id smallint DEFAULT NULL::smallint,
    IN _start_time timestamp with time zone DEFAULT NULL::timestamp with time zone,
    IN _end_time timestamp with time zone DEFAULT NULL::timestamp with time zone)
  RETURNS TABLE(product_id integer, product_type_id smallint, site_id smallint, satellite_id integer, name character varying, 
                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone,
                                quicklook_image character varying, geog geography,  orbit_id integer, tiles character varying[], 
                                downloader_history_id integer) AS
$BODY$
DECLARE q text;
BEGIN
    q := $sql$
        SELECT id, product_type_id, site_id, satellite_id, name, 
             full_path, created_timestamp, inserted_timestamp, 
             quicklook_image, geog, orbit_id, tiles, downloader_history_id FROM product P 
        WHERE TRUE$sql$;

    IF NULLIF($1, -1) IS NOT NULL THEN
        q := q || $sql$
            AND P.site_id = $1$sql$;
    END IF;
    IF NULLIF($2, -1) IS NOT NULL THEN
        q := q || $sql$
            AND P.product_type_id = $2$sql$;
    END IF;
    IF $3 IS NOT NULL THEN
        q := q || $sql$
            AND P.created_timestamp >= $3$sql$;
    END IF;
    IF $4 IS NOT NULL THEN
        q := q || $sql$
            AND P.created_timestamp <= $4$sql$;
    END IF;
    q := q || $SQL$
        ORDER BY P.name;$SQL$;

    -- raise notice '%', q;
    
	RETURN QUERY
        EXECUTE q
        USING $1, $2, $3, $4;
END
$BODY$
  LANGUAGE plpgsql STABLE;
