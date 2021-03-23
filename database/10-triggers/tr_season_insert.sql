CREATE TRIGGER tr_season_insert 
    AFTER INSERT ON public.season 
    FOR EACH ROW EXECUTE PROCEDURE public.insert_season_descriptors();