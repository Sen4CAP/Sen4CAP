CREATE TRIGGER tr_season_delete 
    BEFORE DELETE ON public.season 
    FOR EACH ROW EXECUTE PROCEDURE public.delete_season_descriptors();