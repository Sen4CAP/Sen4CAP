CREATE TABLE IF NOT EXISTS auxdata_operation (
    id smallserial NOT NULL,
    auxdata_descriptor_id smallint NOT NULL,
    operation_order smallint NOT NULL,
    name character varying NOT NULL,
    output_type character varying,
    handler_path character varying,
    processor_id smallint NOT NULL,
    parameters json,
    CONSTRAINT auxdata_operation_pkey PRIMARY KEY (id),
    CONSTRAINT u_auxdata_operation UNIQUE (auxdata_descriptor_id, operation_order)
);