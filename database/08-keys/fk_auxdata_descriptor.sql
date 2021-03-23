ALTER TABLE ONLY auxdata_operation
    ADD CONSTRAINT fk_auxdata_descriptor FOREIGN KEY (auxdata_descriptor_id) REFERENCES auxdata_descriptor(id);
