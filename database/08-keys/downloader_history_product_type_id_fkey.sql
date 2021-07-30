alter table downloader_history add constraint downloader_history_product_type_id_fkey foreign key (product_type_id) references product_type (id);
