CREATE OR REPLACE FUNCTION sp_get_products_for_tile(_site_id site.id%TYPE, _tile_id CHARACTER VARYING, _product_type_id SMALLINT, _satellite_id satellite.id%TYPE, _out_satellite_id satellite.id%TYPE)
  RETURNS TABLE(product_id integer, product_type_id smallint, site_id smallint, 
                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, 
                satellite_id integer, name character varying, 
                quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[],
               downloader_history_id integer)
AS $$
DECLARE _geog GEOGRAPHY;
BEGIN
    CASE _satellite_id
        WHEN 1 THEN -- sentinel2
            _geog := (SELECT shape_tiles_s2.geog FROM shape_tiles_s2 WHERE tile_id = _tile_id);
        WHEN 2 THEN -- landsat8
            _geog := (SELECT shape_tiles_l8 FROM shape_tiles_l8 WHERE shape_tiles_l8.pr = _tile_id :: INT);
    END CASE;

    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.site_id, 
                    product.full_path, product.created_timestamp, product.inserted_timestamp, 
                    product.satellite_id, product.name, 
                    product.quicklook_image, product.geog, product.orbit_id, product.tiles,
                    product.downloader_history_id
        FROM product
        WHERE product.site_id = _site_id AND
              product.satellite_id = _out_satellite_id AND
              product.product_type_id = _product_type_id AND  
              ST_Intersects(product.geog, _geog);
END;
$$
LANGUAGE plpgsql
STABLE;
