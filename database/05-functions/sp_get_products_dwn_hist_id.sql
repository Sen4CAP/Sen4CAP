-- DROP FUNCTION sp_get_products_dwn_hist_ids(json)
CREATE OR REPLACE FUNCTION sp_get_products_dwn_hist_ids(IN ids json)
  RETURNS TABLE(product_id integer, downloader_history_id integer) AS
$BODY$
	BEGIN
		RETURN QUERY SELECT product.id, product.downloader_history_id
		FROM   product
		WHERE id IN (SELECT value::integer FROM json_array_elements_text($1) ) AND 
              product.downloader_history_id IS NOT NULL;
   END;
$BODY$  
LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION sp_get_product_by_id(integer)
  OWNER TO admin;
  
  
  
-- CREATE OR REPLACE FUNCTION sp_get_products_dwn_hist_ids(IN ids json)
--   RETURNS TABLE(id integer, downloader_history_id integer) AS
-- $func$
--    SELECT id, downloader_history_id
--    FROM   product
--    JOIN  (SELECT value::integer FROM json_array_elements_text($1)) t(id) USING (id)
-- $func$  LANGUAGE sql;  