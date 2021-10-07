CREATE TABLE IF NOT EXISTS auxdata_operation (
    id smallserial NOT NULL,
    auxdata_file_id smallint NOT NULL,
    operation_order smallint NOT NULL,
    name character varying NOT NULL,
    output_type character varying,
    handler_path character varying,
    processor_id smallint NOT NULL,
    async boolean NOT NULL DEFAULT false,
    parameters json,
    CONSTRAINT auxdata_operation_pkey PRIMARY KEY (id),
    CONSTRAINT u_auxdata_operation UNIQUE (auxdata_file_id, operation_order),
    CONSTRAINT fk_auxdata_file FOREIGN KEY (auxdata_file_id)
        REFERENCES auxdata_file (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT fk_auxdata_operation_processor FOREIGN KEY (processor_id)
        REFERENCES processor (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION    
);