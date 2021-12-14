﻿CREATE OR REPLACE FUNCTION sp_dashboard_update_scheduled_task(
    _schedule_id smallint,
    _repeat_type smallint,
    _repeat_after_days smallint,
    _repeat_on_month_day smallint,
    _first_run_time timestamptz,
    _processor_params json)
  RETURNS void AS
$BODY$
BEGIN

UPDATE scheduled_task
SET	processor_params = _processor_params,
	repeat_type = _repeat_type,
	repeat_after_days = _repeat_after_days,
	repeat_on_month_day = _repeat_on_month_day,
	first_run_time = _first_run_time
WHERE id = _schedule_id;

UPDATE scheduled_task_status
SET next_schedule =_first_run_time,
	last_scheduled_run = null,
	last_run_timestamp = null,
	last_retry_timestamp = null,
	estimated_next_run_time = null
WHERE task_id = _schedule_id;

END;
$BODY$
  LANGUAGE plpgsql
