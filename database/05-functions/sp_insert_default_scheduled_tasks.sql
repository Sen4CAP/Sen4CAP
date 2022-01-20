create or replace function sp_insert_default_scheduled_tasks(
    _season_id season.id%type,
    _processor_id processor.id%type default null
)
    returns void as
$$
declare
    _site_id site.id%type;
    declare _site_short_name site.short_name%type;
    declare _season_name season.name%type;
    declare _season_start_date season.start_date%type;
    declare _season_mid_date season.start_date%type;
begin
    select site.id,
           site.short_name,
           season.name,
           season.start_date,
           season.mid_date
    into
        _site_id,
        _site_short_name,
        _season_name,
        _season_start_date,
        _season_mid_date
    from season
             inner join site on site.id = season.site_id
    where season.id = _season_id;

    if not found then
        raise exception 'Invalid season id %', _season_id;
    end if;

    perform sp_insert_scheduled_task(
            name,
            processor_id,
            _site_id :: int,
            _season_id :: int,
            repeat_type,
            repeat_after_days,
            repeat_on_month_day,
            first_run_time,
            retry_seconds,
            priority,
            processor_params
        )
    from sp_evaluate_default_scheduled_tasks(_site_short_name, _season_name, _season_start_date, _season_mid_date)
    where _processor_id is null
       or processor_id = _processor_id;
end;
$$
    language plpgsql volatile;
