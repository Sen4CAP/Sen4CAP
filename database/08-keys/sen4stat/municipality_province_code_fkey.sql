alter table municipality add constraint municipality_province_code_fkey foreign key (province_code) references province (province_code);
