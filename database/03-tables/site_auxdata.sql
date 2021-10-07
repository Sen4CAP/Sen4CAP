CREATE TABLE IF NOT EXISTS site_auxdata (
    id smallserial NOT NULL,
    site_id smallint NOT NULL,
    auxdata_descriptor_id smallint NOT NULL,
    year smallint,
    season_id smallint,
    auxdata_file_id smallint NOT NULL,
    file_name character varying,
    status_id smallint,
    parameters json,
    output character varying,
    CONSTRAINT site_auxdata_pkey PRIMARY KEY (id),
    CONSTRAINT u_site_auxdata UNIQUE (site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id),
    CONSTRAINT fk_site_auxdata_descriptor FOREIGN KEY (auxdata_descriptor_id)
        REFERENCES auxdata_descriptor (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT fk_site_auxdata_file FOREIGN KEY (auxdata_file_id)
        REFERENCES auxdata_file (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT fk_site_auxdata_activity_Status FOREIGN KEY (status_id)
        REFERENCES activity_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION    
);