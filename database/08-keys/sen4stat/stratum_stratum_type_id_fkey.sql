alter table stratum add constraint stratum_stratum_type_id_fkey foreign key (stratum_type_id) references stratum_type (stratum_type_id);
