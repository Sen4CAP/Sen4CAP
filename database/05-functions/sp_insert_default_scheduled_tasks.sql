create or replace function sp_insert_default_scheduled_tasks(
    _season_id season.id%type,
    _processor_id processor.id%type default null
)
    returns void as
$$
declare
    _site_id site.id%type;
    declare _site_name site.short_name%type;
    declare _season_name season.name%type;
    declare _start_date season.start_date%type;
    declare _mid_date season.start_date%type;
begin
    select site.id,
           site.short_name,
           season.name,
           season.start_date,
           season.mid_date
    into
        _site_id,
        _site_name,
        _season_name,
        _start_date,
        _mid_date
    from season
             inner join site on site.id = season.site_id
    where season.id = _season_id;

    if not found then
        raise exception 'Invalid season id %', _season_id;
    end if;

    perform sp_insert_scheduled_task(
                            _site_name || '_' || _season_name || '_' || suffix :: character varying,
                            processor_id,
                            _site_id :: int,
                            _season_id :: int,
                            repeat_type,
                            repeat_after_days,
                            repeat_on_month_day,
                            date_trunc(
                                    coalesce(first_run_base_trunc, 'day'),
                                    case first_run_base
                                        when 'start' then _start_date
                                        when 'mid' then _mid_date
                                        end
                                ) + coalesce(first_run_offset, interval '0'),
                            retry_seconds,
                            priority,
                            coalesce(processor_arguments, '{}')
        )
    from default_scheduled_tasks
    where _processor_id is null
       or processor_id = _processor_id;
end;
$$
    language plpgsql volatile;
