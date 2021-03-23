CREATE OR REPLACE FUNCTION sp_get_full_products_by_name(IN _names json)
  RETURNS TABLE(product_id integer, product_type_id smallint, processor_id smallint, satellite_id integer, site_id smallint, name character varying, 
                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, job_id integer, 
                quicklook_image character varying, footprint polygon, orbit_id integer, tiles character varying[],
               downloader_history_id integer) AS
$BODY$
                BEGIN
                    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.processor_id, product.satellite_id, product.site_id, product.name, 
                                        product.full_path, product.created_timestamp, product.inserted_timestamp, product.job_id,
                                        product.quicklook_image, product.footprint, product.orbit_id, product.tiles,
                                        product.downloader_history_id
                                        
                    FROM product
                    WHERE product.name in (SELECT value::character varying FROM json_array_elements_text(_names));
                END;
                $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION sp_get_full_products_by_name(json)
  OWNER TO admin;                
