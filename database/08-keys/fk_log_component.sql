alter table log
    add constraint fk_log_component foreign key(component_id) references component(id);
