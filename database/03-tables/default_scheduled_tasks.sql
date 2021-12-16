create table default_scheduled_tasks
(
    processor_id smallint not null,
    suffix text not null,
    repeat_type smallint not null,
    repeat_after_days smallint not null,
    repeat_on_month_day smallint not null,
    first_run_base text not null,
    first_run_base_trunc text,
    first_run_offset interval,
    retry_seconds int not null,
    priority smallint not null,
    processor_arguments json,
    constraint default_scheduled_tasks_pkey primary key (processor_id, suffix)
);
