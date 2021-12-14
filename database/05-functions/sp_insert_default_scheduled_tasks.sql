create or replace function sp_insert_default_scheduled_tasks(
    _season_id season.id%type,
    _processor_id processor.id%type default null
)
returns void as
$$
declare _site_id site.id%type;
declare _site_name site.short_name%type;
declare _processor_name processor.short_name%type;
declare _season_name season.name%type;
declare _start_date season.start_date%type;
declare _mid_date season.start_date%type;
begin
    select site.short_name
    into _site_name
    from season
    inner join site on site.id = season.site_id
    where season.id = _season_id;

    select processor.short_name
    into _processor_name
    from processor
    where id = _processor_id;

    if not found then
        raise exception 'Invalid season id %', _season_id;
    end if;

    select site_id,
           name,
           start_date,
           mid_date
    into _site_id,
         _season_name,
         _start_date,
         _mid_date
    from season
    where id = _season_id;

	if _processor_id is null or (_processor_id = 2 and _processor_name = 'l3a') then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_L3A' :: character varying,
                    2,
                    _site_id :: int,
                    _season_id :: int,
                    2::smallint,
                    0::smallint,
                    31::smallint,
                    (select date_trunc('month', _start_date) + interval '1 month' - interval '1 day'),
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

	if _processor_id is null or (_processor_id = 3 and (_processor_name = 'l3b_lai' or _processor_name = 'l3b'))  then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_L3B' :: character varying,
                    3,
                    _site_id :: int,
                    _season_id :: int,
                    1::smallint,
                    1::smallint,
                    0::smallint,
                    _start_date + 1,
                    60,
                    1 :: smallint,
                    '{"general_params":{"product_type":"L3B"}}' :: json);
    end if;

	if _processor_id is null or (_processor_id = 5 and _processor_name = 'l4a') then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_L4A' :: character varying,
                    5,
                    _site_id :: int,
                    _season_id :: int,
                    2::smallint,
                    0::smallint,
                    31::smallint,
                    _mid_date,
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

	if _processor_id is null or (_processor_id = 6 and _processor_name = 'l4b') then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_L4B' :: character varying,
                    6,
                    _site_id :: int,
                    _season_id :: int,
                    2::smallint,
                    0::smallint,
                    31::smallint,
                    _mid_date,
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

  	if _processor_id is null or _processor_name = 's4c_l4a' then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_S4C_L4A' :: character varying,
                    9,
                    _site_id :: int,
                    _season_id :: int,
                    2::smallint,
                    0::smallint,
                    31::smallint,
                    _mid_date,
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

  	if _processor_id is null or _processor_name = 's4c_l4b' then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_S4C_L4B' :: character varying,
                    10,
                    _site_id :: int,
                    _season_id :: int,
                    2::smallint,
                    0::smallint,
                    31::smallint,
                    _start_date + 31,
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

  	if _processor_id is null or _processor_name = 's4c_l4c' then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_S4C_L4C' :: character varying,
                    11,
                    _site_id :: int,
                    _season_id :: int,
                    1::smallint,
                    7::smallint,
                    0::smallint,
                    _start_date + 7,
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

    if _processor_id is null or (_processor_id = 14 and _processor_name = 's4c_mdb1')  then
        perform sp_insert_scheduled_task(
                    _site_name || '_' || _season_name || '_S4C_MDB1' :: character varying,
                    14,
                    _site_id :: int,
                    _season_id :: int,
                    1::smallint,
                    1::smallint,
                    0::smallint,
                    _start_date + 1,
                    60,
                    1 :: smallint,
                    '{}' :: json);
    end if;

    if _processor_id is not null and _processor_id not in (2, 3, 5, 6, 9, 10, 11, 12, 13, 14) then
        raise exception 'No default jobs defined for processor id %', _processor_id;
    end if;

end;
$$
    language plpgsql volatile;
