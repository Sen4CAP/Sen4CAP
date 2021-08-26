alter table stratum add constraint stratum_site_id_fkey foreign key (site_id) references site (id);
