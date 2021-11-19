ALTER TABLE ONLY auxdata_file
    ADD CONSTRAINT fk_auxdata_descriptor_file FOREIGN KEY (auxdata_descriptor_id)
        REFERENCES auxdata_descriptor (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION;
