ALTER TABLE ONLY auxdata_operation_file
    ADD CONSTRAINT fk_auxdata_file FOREIGN KEY (auxdata_operation_id) REFERENCES auxdata_operation(id);
