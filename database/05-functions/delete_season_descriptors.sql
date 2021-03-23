-- DROP FUNCTION public.delete_season_descriptors() CASCADE;

CREATE OR REPLACE FUNCTION public.delete_season_descriptors()
    RETURNS trigger AS
$BODY$
BEGIN
    DELETE FROM site_auxdata
        WHERE site_id = OLD.site_id AND season_id = OLD.id;
    RETURN OLD;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.delete_season_descriptors()
  OWNER TO admin;
