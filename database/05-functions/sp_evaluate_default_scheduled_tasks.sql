create or replace function sp_evaluate_default_scheduled_tasks(
    _site_short_name text,
    _season_name text,
    _start_date date,
    _mid_date date)
    returns table
            (
                name text,
                processor_id smallint,
                repeat_type smallint,
                repeat_after_days smallint,
                repeat_on_month_day smallint,
                first_run_time timestamptz,
                retry_seconds int,
                priority smallint,
                processor_params json
            )
as
$$
begin
    return query
        select _site_short_name || '_' || _season_name || '_' || suffix,
               default_scheduled_tasks.processor_id,
               default_scheduled_tasks.repeat_type,
               default_scheduled_tasks.repeat_after_days,
               default_scheduled_tasks.repeat_on_month_day,
               date_trunc(
                       coalesce(default_scheduled_tasks.first_run_base_trunc, 'day'),
                       case default_scheduled_tasks.first_run_base
                           when 'start' then _start_date
                           when 'mid' then _mid_date
                           end
                   ) + coalesce(default_scheduled_tasks.first_run_offset, interval '0'),
               default_scheduled_tasks.retry_seconds,
               default_scheduled_tasks.priority,
               coalesce(default_scheduled_tasks.processor_arguments, '{}')
        from default_scheduled_tasks;
end;
$$
    language plpgsql stable;
