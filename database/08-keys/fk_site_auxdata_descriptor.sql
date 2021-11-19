ALTER TABLE ONLY site_auxdata
    ADD CONSTRAINT fk_site_auxdata_descriptor FOREIGN KEY (auxdata_descriptor_id)
        REFERENCES auxdata_descriptor (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION;
