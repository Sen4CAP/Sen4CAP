create table crop_remapping_set_detail
(
    crop_remapping_set_id int not null,
    original_code int not null references crop_list_n4 (code_n4),
    remapped_code_pre int,
    description_pre text,
    remapped_code_post int,
    description_post text,
    primary key (crop_remapping_set_id, original_code)
);
