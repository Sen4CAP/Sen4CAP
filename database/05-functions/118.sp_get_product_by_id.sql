CREATE OR REPLACE FUNCTION sp_get_product_by_id(IN _id integer)
  RETURNS TABLE(product_id integer, product_type_id smallint, processor_id smallint, site_id smallint, full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone,
                satellite_id integer, name character varying, quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[], downloader_history_id integer) AS
$BODY$
                BEGIN
                    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.processor_id, product.site_id, product.full_path, product.created_timestamp, product.inserted_timestamp,
                    product.satellite_id, product.name,  product.quicklook_image, product.geog, product.orbit_id, product.tiles, product.downloader_history_id
                    FROM product
                    WHERE product.id = _id;
                END;
                $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION sp_get_product_by_id(integer)
  OWNER TO admin;
