DROP FUNCTION IF EXISTS sp_get_dashboard_products_site(integer, integer[], smallint, integer[], timestamp with time zone, timestamp with time zone, character varying[]);
CREATE OR REPLACE FUNCTION sp_get_dashboard_products_site(_site_id integer, _product_type_id integer[] DEFAULT NULL::integer[], _season_id smallint DEFAULT NULL::smallint, _satellit_id integer[] DEFAULT NULL::integer[], _since_timestamp timestamp with time zone DEFAULT NULL::timestamp with time zone, _until_timestamp timestamp with time zone DEFAULT NULL::timestamp with time zone, _tiles character varying[] DEFAULT NULL::character varying[])
 RETURNS SETOF json
AS $BODY$
BEGIN
	RETURN QUERY
	WITH product_type_names(id, name, description, row) AS (
		SELECT id, name, description, ROW_NUMBER() OVER (ORDER BY description)
			FROM product_type),
		data (id, satellite_id, product, product_type_id, product_type,product_type_description,processor,site_id,full_path,quicklook_image,footprint,created_timestamp) AS (
		SELECT	P.id,
				P.satellite_id,
				P.name,
				PT.id,
				PT.name,
				PT.description,
				PR.name,
				S.id,
				P.full_path,
				P.quicklook_image,
				P.footprint,
				P.created_timestamp
			FROM product P
				JOIN product_type_names PT ON P.product_type_id = PT.id
				JOIN processor PR ON P.processor_id = PR.id
				JOIN site S ON P.site_id = S.id
			WHERE EXISTS (SELECT * FROM season 
						  WHERE season.site_id = P.site_id AND P.created_timestamp BETWEEN season.start_date AND season.end_date
						  	AND ($3 IS NULL OR season.id = $3))
				AND P.site_id = $1
				AND ($2 IS NULL OR P.product_type_id = ANY($2))
				AND ($4 IS NULL OR P.satellite_id = ANY($4))
				AND ($5 IS NULL OR P.created_timestamp >= to_timestamp(cast($5 as TEXT),'YYYY-MM-DD HH24:MI:SS'))
				AND ($6 IS NULL OR P.created_timestamp <= to_timestamp(cast($6 as TEXT),'YYYY-MM-DD HH24:MI:SS') + interval '1 day')
				AND ($7 IS NULL OR (P.tiles <@$7 AND P.tiles!='{}'))
			ORDER BY PT.row, P.name)
			SELECT COALESCE(array_to_json(array_agg(row_to_json(data)), true), '[]'::json) FROM data;
			--SELECT * FROM data;
END
$BODY$
LANGUAGE plpgsql STABLE
  COST 100;
ALTER FUNCTION sp_get_dashboard_products_site(integer, integer[], smallint, integer[], timestamp with time zone, timestamp with time zone, character varying[])
  OWNER TO admin;
