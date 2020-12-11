alter table log
    add constraint fk_log_severity foreign key(severity) references severity(id);
