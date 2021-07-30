CREATE TABLE downloader_history
(
  id serial NOT NULL,
  site_id smallint NOT NULL,
  satellite_id smallint,
  product_type_id smallint,
  product_name character varying NOT NULL,
  full_path character varying NOT NULL,
  created_timestamp timestamp with time zone NOT NULL DEFAULT now(),
  status_id smallint NOT NULL,
  no_of_retries smallint NOT NULL DEFAULT 0,
  product_date timestamp with time zone NOT NULL DEFAULT now(),
  orbit_id INTEGER NOT NULL DEFAULT -1,
  orbit_type_id smallint,
  status_reason character varying NULL,
  tiles text[] NULL,
  footprint geography NULL,
  CONSTRAINT downloader_history_pkey PRIMARY KEY (id)
)
