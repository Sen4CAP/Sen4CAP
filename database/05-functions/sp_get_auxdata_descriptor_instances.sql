-- DROP FUNCTION public.sp_get_auxdata_descriptor_instances(smallint, smallint, integer);
CREATE OR REPLACE FUNCTION public.sp_get_auxdata_descriptor_instances(
    IN _site_id smallint, 
    IN _season_id smallint,
    IN _year integer)
  RETURNS TABLE(site_id smallint, auxdata_descriptor_id smallint, year integer, season_id smallint, auxdata_file_id smallint, file_name character varying, parameters json) AS
$BODY$

BEGIN 
    RETURN QUERY 
    WITH last_ops AS (
        SELECT ao.auxdata_descriptor_id, MAX(ao.operation_order) AS operation_order 
            FROM auxdata_operation ao
            GROUP BY ao.auxdata_descriptor_id)
    SELECT _site_id as site_id, d.id AS auxdata_descriptor_id, 
        CASE WHEN d.unique_by = 'year' THEN _year ELSE null::integer END AS "year", 
        CASE WHEN d.unique_by = 'season' THEN null::smallint ELSE _season_id END AS season_id, 
        f.id AS auxdata_file_id, null::character varying AS "file_name",
        (SELECT o2.parameters 
            FROM auxdata_operation o2 
                JOIN last_ops ON last_ops.auxdata_descriptor_id = o2.auxdata_descriptor_id AND last_ops.operation_order = o2.operation_order 
            WHERE o2.auxdata_descriptor_id = d.id) AS parameters
    FROM 	auxdata_descriptor d
        JOIN auxdata_operation o ON o.auxdata_descriptor_id = d.id
        JOIN auxdata_operation_file f ON f.auxdata_operation_id = o.id
    WHERE d.id NOT IN (SELECT s.auxdata_descriptor_id from site_auxdata s WHERE s.auxdata_descriptor_id = d.id AND s.site_id = _site_id AND ((d.unique_by = 'year' AND s.year = _year) OR (d.unique_by = 'season' AND s.season_id = _season_id)))
    ORDER BY d.id, f.id;
END
$BODY$
  LANGUAGE plpgsql STABLE
  COST 100
  ROWS 1000;
ALTER FUNCTION public.sp_get_auxdata_descriptor_instances(smallint, smallint, integer)
  OWNER TO admin;
