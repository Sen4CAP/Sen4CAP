alter table region add constraint region_country_code_fkey foreign key (country_code) references country (country_code);
