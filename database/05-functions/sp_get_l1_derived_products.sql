CREATE OR REPLACE FUNCTION sp_get_l1_derived_products(
    IN site_id smallint DEFAULT NULL::smallint,
    IN product_type_id smallint DEFAULT NULL::smallint,
    IN downloader_history_ids json DEFAULT NULL::json,
    IN start_time timestamp with time zone DEFAULT NULL::timestamp with time zone,
    IN end_time timestamp with time zone DEFAULT NULL::timestamp with time zone)
  RETURNS TABLE("ProductId" integer, "ProcessorId" smallint, "ProductTypeId" smallint, "SiteId" smallint, "SatId" integer, "ProductName" character varying, 
                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, job_id integer,
                quicklook_image character varying, footprint polygon,  orbit_id integer, tiles character varying[], 
                downloader_history_id integer) AS
$BODY$
DECLARE q text;
BEGIN
    q := $sql$
    SELECT P.id AS ProductId,
        P.processor_id AS ProcessorId, 
        P.product_type_id AS ProductTypeId,
        P.site_id AS SiteId,
        P.satellite_id AS SatId,
        P.name AS ProductName,
        P.full_path,
        P.created_timestamp,
        P.inserted_timestamp,
        P.job_id,
        P.quicklook_image as quicklook,
        P.footprint,
        P.orbit_id,
        P.tiles,
        P.downloader_history_id
    FROM product P WHERE TRUE$sql$;
    
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
            AND P.downloader_history_id IN (SELECT value::integer FROM json_array_elements_text($3))
        $sql$;
    END IF;		
    IF $4 IS NOT NULL THEN
        q := q || $sql$
            AND P.created_timestamp >= $4$sql$;
    END IF;
    IF $5 IS NOT NULL THEN
        q := q || $sql$
            AND P.created_timestamp <= $5$sql$;
    END IF;
    q := q || $SQL$
        ORDER BY P.name;$SQL$;

    -- raise notice '%', q;
    
	RETURN QUERY
        EXECUTE q
        USING $1, $2, $3, $4, $5;
END
$BODY$
  LANGUAGE plpgsql STABLE;
