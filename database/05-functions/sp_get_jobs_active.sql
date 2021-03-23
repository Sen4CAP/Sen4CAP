CREATE OR REPLACE FUNCTION sp_get_jobs_active(_processor_id smallint DEFAULT NULL, _site_id smallint DEFAULT NULL)
  RETURNS TABLE(job_id integer, processor_id smallint, site_id smallint, status_id smallint) AS
$BODY$
    DECLARE q text;
    BEGIN
        q := $sql$
            SELECT J.id AS job_id, J.processor_id, J.site_id, J.status_id                                       
            FROM job J
            WHERE J.status_id NOT IN (6,7,8) $sql$; -- Finished, Cancelled, Error
            
            IF NULLIF($1, -1) IS NOT NULL THEN
                q := q || $sql$
                        AND J.processor_id = $1$sql$;
            END IF;            

            IF NULLIF($2, -1) IS NOT NULL THEN
                q := q || $sql$
                        AND J.site_id = $2$sql$;
            END IF;  

        -- raise notice '%', q;
        
        RETURN QUERY
            EXECUTE q
            USING $1, $2;
    END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION sp_get_jobs_active(smallint, smallint)
  OWNER TO admin;
