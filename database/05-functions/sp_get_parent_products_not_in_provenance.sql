CREATE OR REPLACE FUNCTION sp_get_parent_products_not_in_provenance(
    IN _site_id smallint,
    IN _source_products_type_id json,
    IN _derived_product_type_id smallint,
    IN _start_time timestamp with time zone DEFAULT NULL::timestamp with time zone,
    IN _end_time timestamp with time zone DEFAULT NULL::timestamp with time zone)
  RETURNS TABLE("ProductId" integer, "ProcessorId" smallint, "ProductTypeId" smallint, "SiteId" smallint, "SatId" integer, "ProductName" character varying, 
                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, job_id integer,
                quicklook_image character varying, footprint polygon,  orbit_id integer, tiles character varying[], 
                downloader_history_id integer) AS
$BODY$
DECLARE q text;
BEGIN
    q := $sql$
        SELECT id, processor_id, product_type_id, site_id, satellite_id, name, 
                 full_path, created_timestamp, inserted_timestamp, job_id,
                 quicklook_image, footprint, orbit_id, tiles, downloader_history_id
             FROM product P WHERE site_id = $1 AND product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))
             AND NOT EXISTS (
                SELECT product_id FROM product_provenance WHERE parent_product_id = id AND
                       product_id IN (SELECT id FROM product WHERE site_id = $1 AND product_type_id = $3)
        ) $sql$;
        -- SELECT id, processor_id, product_type_id, site_id, satellite_id, name, 
        --         full_path, created_timestamp, inserted_timestamp, job_id,
        --         quicklook_image, footprint, orbit_id, tiles, downloader_history_id
        --     FROM product P WHERE site_id = $1 AND product_type_id in (SELECT value::smallint FROM json_array_elements_text($2))
        --     AND NOT EXISTS (
        --         SELECT id FROM product_provenance PV JOIN product P ON P.id = PV.parent_product_id 
        --         WHERE site_id = $1 AND PV.parent_product_id = id AND P.product_type_id = $3
        --     )  $sql$;
    
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
