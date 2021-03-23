ALTER TABLE ONLY site_auxdata
    ADD CONSTRAINT fk_site_auxdata_activity_status FOREIGN KEY (status_id) REFERENCES activity_status(id);
