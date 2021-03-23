CREATE TABLE IF NOT EXISTS auxdata_descriptor (
    id smallint NOT NULL,
    name character varying NOT NULL,
    label character varying NOT NULL,
    unique_by character varying NOT NULL,
    CONSTRAINT auxdata_descriptor_pkey PRIMARY KEY (id),
    CONSTRAINT check_unique_by CHECK ((((unique_by)::text = 'season'::text) OR ((unique_by)::text = 'year'::text)))
);