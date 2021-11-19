ALTER TABLE ONLY auxdata_operation
    ADD CONSTRAINT fk_auxdata_file FOREIGN KEY (auxdata_file_id)
        REFERENCES auxdata_file (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION;
