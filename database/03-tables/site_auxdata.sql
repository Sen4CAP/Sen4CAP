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
    CONSTRAINT u_site_auxdata UNIQUE (site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id)
);