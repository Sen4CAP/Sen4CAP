ALTER TABLE ONLY site_auxdata
    ADD CONSTRAINT fk_site_auxdata_operation_file FOREIGN KEY (auxdata_file_id) REFERENCES auxdata_operation_file(id);
