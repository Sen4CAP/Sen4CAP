alter table province add constraint province_region_code_fkey foreign key (region_code) references region (region_code);
