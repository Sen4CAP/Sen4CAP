ALTER TABLE ONLY site_auxdata
    ADD CONSTRAINT fk_site_auxdata_descriptor FOREIGN KEY (auxdata_descriptor_id) REFERENCES auxdata_descriptor(id);
