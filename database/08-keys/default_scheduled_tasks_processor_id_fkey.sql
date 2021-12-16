alter table default_scheduled_tasks
    add constraint default_scheduled_tasks_processor_id_fkey foreign key (processor_id) references processor (id);
