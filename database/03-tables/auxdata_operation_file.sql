CREATE TABLE IF NOT EXISTS auxdata_operation_file (
    id smallserial NOT NULL,
    auxdata_operation_id smallint NOT NULL,
    file_order smallint NOT NULL,
    name character varying,
    label character varying NOT NULL,
    extensions character varying[],
    required boolean DEFAULT false,
    CONSTRAINT auxdata_operation_file_pkey PRIMARY KEY (id),
    CONSTRAINT u_auxdata_operation_file UNIQUE (auxdata_operation_id, file_order)
);