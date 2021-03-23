CREATE OR REPLACE FUNCTION sp_get_processors()
RETURNS TABLE (
    id processor.id%TYPE,
    "short_name" processor."short_name"%TYPE,
    "name" processor."name"%TYPE,
    "required" processor."required"%TYPE
)
AS $$
BEGIN
    RETURN QUERY
        SELECT processor.id,
               processor.short_name,
               processor.name,
               processor.required
        FROM processor
        ORDER BY processor.id;
END
$$
LANGUAGE plpgsql
STABLE;
