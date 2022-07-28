CREATE TABLE processor
(
  id smallint not null,
  name character varying NOT NULL DEFAULT '',
  description character varying,
  short_name character varying,
  label character varying,
  required boolean NOT NULL DEFAULT false,
  supported_satellite_ids smallint[],
  mandatory_satellite_ids smallint[],
  is_admin_ui_visible boolean NOT NULL DEFAULT false,
  is_service_ui_visible boolean NOT NULL DEFAULT false,
  lpis_required boolean NOT NULL DEFAULT false,
  additional_config_required boolean NOT NULL DEFAULT false,
  CONSTRAINT processor_pkey PRIMARY KEY (id)
)
