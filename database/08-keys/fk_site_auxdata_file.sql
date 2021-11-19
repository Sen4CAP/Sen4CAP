ALTER TABLE ONLY site_auxdata
    ADD CONSTRAINT fk_site_auxdata_file FOREIGN KEY (auxdata_file_id)
        REFERENCES auxdata_file (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION;
