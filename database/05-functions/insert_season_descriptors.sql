-- DROP FUNCTION public.insert_season_descriptors() CASCADE;
CREATE OR REPLACE FUNCTION public.insert_season_descriptors()
    RETURNS trigger AS
$BODY$
BEGIN
    INSERT INTO site_auxdata (site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, status_id, parameters, output)
        SELECT site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, 3, parameters, null -- initially the status is 3=NeedsInput
            FROM sp_get_auxdata_descriptor_instances(NEW.site_id, NEW.id, DATE_PART('year', NEW.start_date)::integer);
    RETURN NEW;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.insert_season_descriptors()
  OWNER TO admin;