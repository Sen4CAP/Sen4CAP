CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance_by_ids(
    IN _derived_product_ids json,
    IN _source_products_type_id json)
  RETURNS TABLE(parent_product_id integer, parent_product_type_id smallint, parent_site_id smallint, parent_satellite_id integer, parent_name character varying, 
                parent_full_path character varying, parent_created_timestamp timestamp with time zone, parent_inserted_timestamp timestamp with time zone,
                parent_quicklook_image character varying, parent_geog geography,  parent_orbit_id integer, parent_tiles character varying[], 
                parent_downloader_history_id integer, product_id integer) AS
$BODY$
DECLARE q text;
BEGIN
    q := $sql$
        WITH derived_products AS (SELECT product_provenance.product_id as prd_id,
                                  product_provenance.parent_product_id as parent_id
                     FROM product 
                     INNER JOIN product_provenance on product.id = product_provenance.product_id
                     WHERE product_id IN (SELECT value::integer FROM json_array_elements_text($1)))
            SELECT id, product_type_id, site_id, satellite_id, name, 
                         full_path, created_timestamp, inserted_timestamp, 
                         quicklook_image, geog, orbit_id, tiles, downloader_history_id,
                         derived_products.prd_id
            FROM product P INNER JOIN derived_products on P.id = derived_products.parent_id
            WHERE product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))	
        $sql$;
    q := q || $SQL$
        ORDER BY P.name;$SQL$;

    -- raise notice '%', q;
    
    RETURN QUERY
        EXECUTE q
        USING $1, $2;
END
$BODY$
  LANGUAGE plpgsql STABLE;
