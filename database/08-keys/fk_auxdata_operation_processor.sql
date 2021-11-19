ALTER TABLE ONLY auxdata_operation
    ADD CONSTRAINT fk_auxdata_operation_processor FOREIGN KEY (processor_id)
        REFERENCES processor (id) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION;
