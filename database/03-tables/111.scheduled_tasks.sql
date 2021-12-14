CREATE TABLE scheduled_task
(
  id smallserial NOT NULL,
  name character varying NOT NULL,
  processor_id smallint NOT NULL,
  site_id smallint NOT NULL,
  season_id smallint not null,
  processor_params character varying,

  repeat_type smallint,
  repeat_after_days smallint,
  repeat_on_month_day smallint,

  retry_seconds integer,

  priority smallint,

  first_run_time timestamptz not null,

  CONSTRAINT scheduled_task_pkey PRIMARY KEY (id)
);

CREATE TABLE scheduled_task_status
(
  id smallserial NOT NULL,
  task_id smallint NOT NULL,

  next_schedule timestamptz not null,

  last_scheduled_run timestamptz,
  last_run_timestamp timestamptz,
  last_retry_timestamp timestamptz,

  estimated_next_run_time timestamptz,

  CONSTRAINT scheduled_task_status_pkey PRIMARY KEY (id)
);
