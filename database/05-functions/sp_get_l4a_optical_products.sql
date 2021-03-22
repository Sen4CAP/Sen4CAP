create or replace function sp_l4a_get_optical_products(
    _site_id product.site_id%type,
    _satellite_id product.satellite_id%type,
    _season_start product.created_timestamp%type,
    _season_end product.created_timestamp%type,
    _tiles product.tiles%type,
    _products text[]
)
returns table (
    site_id product.site_id%type,
    full_path product.full_path%type,
    tile text,
    created_timestamp product.created_timestamp%type
)
as
$$
declare q text;

begin
    q := $sql$
        select site_id,
               full_path,
               unnest(tiles) :: text as tile,
               created_timestamp
        from product
        where site_id = $1
          and satellite_id = $2
          and product_type_id = 1
          and created_timestamp between $3 and $4 + interval '1 day'$sql$;
    if _tiles is not null then
        q := q || $sql$
            and tiles && $5 :: character varying[]$sql$;
    end if;
    if _products is not null then
        q := q || $sql$
            and name = any($6)$sql$;
    end if;
    q := q || ';';

    -- raise notice '%', q;

    return query
        execute q
        using _site_id,
              _satellite_id,
              _season_start,
              _season_end,
              _tiles,
              _products;
end
$$
language plpgsql stable;
