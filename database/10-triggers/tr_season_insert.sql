-- DROP TRIGGER tr_season_insert
CREATE TRIGGER tr_season_insert
    AFTER INSERT
    ON public.season
    FOR EACH ROW
    EXECUTE FUNCTION public.insert_season_descriptors();