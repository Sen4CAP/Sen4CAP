CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance_by_id(
    IN _derived_product_id smallint,
    IN _source_products_type_id json)
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
                 quicklook_image, geog, orbit_id, tiles, downloader_history_id
             FROM product P WHERE product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))
             AND EXISTS (
                SELECT product_id FROM product_provenance WHERE parent_product_id = id AND product_id = $1
        ) $sql$;
        -- SELECT id, product_type_id, site_id, satellite_id, name, 
        --         full_path, created_timestamp, inserted_timestamp, job_id,
        --         quicklook_image, geog, orbit_id, tiles, downloader_history_id
        --     FROM product P WHERE site_id = $1 AND product_type_id in (SELECT value::smallint FROM json_array_elements_text($2))
        --     AND EXISTS (
        --         SELECT id FROM product_provenance PV JOIN product P ON P.id = PV.parent_product_id 
        --         WHERE site_id = $1 AND PV.parent_product_id = id AND P.product_type_id = $3
        --     )  $sql$;
    q := q || $SQL$
        ORDER BY P.name;$SQL$;

    -- raise notice '%', q;
    
    RETURN QUERY
        EXECUTE q
        USING $1, $2;
END
$BODY$
  LANGUAGE plpgsql STABLE;
