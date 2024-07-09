begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('2.0.3', '3.0.0')) then
            raise notice 'upgrading from 2.0.3 to 3.0.0';

            raise notice 'patching 2.0.3';
        
            -- New Tables
            _statement := $str$
                CREATE TABLE IF NOT EXISTS log(
                    id serial not null primary key,
                    severity smallint not null,
                    component_id smallint not null,
                    date timestamp not null default now(),
                    message text not null,
                    data json,
                    acknowledged_date timestamp
                );
                CREATE TABLE IF NOT EXISTS component(
                    id smallint not null primary key,
                    name text not null
                );
                
                CREATE TABLE IF NOT EXISTS severity(
                    id smallint not null primary key,
                    name text not null
                );
                
                CREATE TABLE IF NOT EXISTS product_provenance(
                    product_id int not null,
                    parent_product_id int not null,
                    parent_product_date timestamp with time zone not null,
                    constraint product_provenance_pkey primary key (product_id, parent_product_id)
                );

                CREATE TABLE IF NOT EXISTS auxdata_descriptor (
                    id smallint NOT NULL,
                    name character varying NOT NULL,
                    label character varying NOT NULL,
                    unique_by character varying NOT NULL,
                    CONSTRAINT auxdata_descriptor_pkey PRIMARY KEY (id),
                    CONSTRAINT check_unique_by CHECK ((((unique_by)::text = 'season'::text) OR ((unique_by)::text = 'year'::text)))
                );
                
                CREATE TABLE IF NOT EXISTS auxdata_file (
                    id smallserial NOT NULL,
                    auxdata_descriptor_id smallint NOT NULL,
                    file_order smallint NOT NULL,
                    name character varying,
                    label character varying NOT NULL,
                    extensions character varying[],
                    required boolean DEFAULT false,
                    CONSTRAINT auxdata_file_pkey PRIMARY KEY (id),
                    CONSTRAINT u_auxdata_descriptor_file UNIQUE (auxdata_descriptor_id, file_order),
                    CONSTRAINT fk_auxdata_descriptor_file FOREIGN KEY (auxdata_descriptor_id)
                        REFERENCES auxdata_descriptor (id) MATCH SIMPLE
                        ON UPDATE NO ACTION ON DELETE NO ACTION    
                );            

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
                
                create table IF NOT EXISTS default_scheduled_tasks
                (
                    processor_id smallint not null,
                    suffix text not null,
                    repeat_type smallint not null,
                    repeat_after_days smallint not null,
                    repeat_on_month_day smallint not null,
                    first_run_base text not null,
                    first_run_base_trunc text,
                    first_run_offset interval,
                    retry_seconds int not null,
                    priority smallint not null,
                    processor_arguments json,
                    constraint default_scheduled_tasks_pkey primary key (processor_id, suffix)
                );                
                
                CREATE TABLE IF NOT EXISTS l3_veg_stats
                (
                  l2a_product_id int NOT NULL,
                  site_id int NOT NULL,
                  simplified_l2a_name character varying(512) NOT NULL,
                  tile character varying NOT NULL,
                  orbit int NOT NULL,
                  stats json NOT NULL,
                  CONSTRAINT pk_l3_veg_stats_details PRIMARY KEY (l2a_product_id), 
                  CONSTRAINT fk_product FOREIGN KEY (l2a_product_id) REFERENCES product (id) MATCH SIMPLE 
                  ON UPDATE NO ACTION ON DELETE CASCADE
                );
                
                create table IF NOT EXISTS s2_tile_dem_statistics(
                    tile_id text not null primary key,
                    minimum smallint not null,
                    maximum smallint not null,
                    mean real not null,
                    stddev real not null
                );                
                
                CREATE TABLE IF NOT EXISTS public.service (
                    id serial NOT NULL,
                    site_id smallint NOT NULL,
                    name character varying NOT NULL,
                    footprint_filename character varying,
                    additional_support text,
                    additional_data_specifications text,
                    CONSTRAINT service_pkey PRIMARY KEY (id),
                    CONSTRAINT service_site_id_name_key UNIQUE (site_id, name)
                );

                ALTER TABLE public.service OWNER TO admin;
                
                
                CREATE TABLE IF NOT EXISTS public.service_processors (
                    service_id smallint NOT NULL,
                    processor_id smallint NOT NULL,
                    CONSTRAINT service_processors_pkey PRIMARY KEY (service_id, processor_id)
                );

                ALTER TABLE public.service_processors OWNER TO admin;
                
            $str$;
            raise notice '%', _statement;
            execute _statement;
        
            _statement := $str$
                CREATE SCHEMA IF NOT EXISTS gadm;
                ALTER SCHEMA gadm OWNER TO admin;

                CREATE TABLE IF NOT EXISTS gadm.adm_l1 (
                    gid integer NOT NULL,
                    gid_0 character varying(80),
                    name_0 character varying(80),
                    gid_1 character varying(80),
                    name_1 character varying(80),
                    varname_1 character varying(129),
                    nl_name_1 character varying(87),
                    type_1 character varying(80),
                    engtype_1 character varying(80),
                    cc_1 character varying(80),
                    hasc_1 character varying(80),
                    geom public.geometry(MultiPolygon),
                    enabled boolean DEFAULT true NOT NULL
                );
                ALTER TABLE gadm.adm_l1 OWNER TO postgres;

                CREATE SEQUENCE IF NOT EXISTS gadm.adm_l1_gid_seq
                    AS integer
                    START WITH 1
                    INCREMENT BY 1
                    NO MINVALUE
                    NO MAXVALUE
                    CACHE 1;
                ALTER TABLE gadm.adm_l1_gid_seq OWNER TO postgres;
                ALTER SEQUENCE gadm.adm_l1_gid_seq OWNED BY gadm.adm_l1.gid;

                CREATE TABLE IF NOT EXISTS gadm.adm_l2 (
                    gid integer NOT NULL,
                    gid_0 character varying(80),
                    name_0 character varying(80),
                    gid_1 character varying(80),
                    name_1 character varying(80),
                    nl_name_1 character varying(87),
                    gid_2 character varying(80),
                    name_2 character varying(80),
                    varname_2 character varying(116),
                    nl_name_2 character varying(80),
                    type_2 character varying(80),
                    engtype_2 character varying(80),
                    cc_2 character varying(80),
                    hasc_2 character varying(80),
                    geom public.geometry(MultiPolygon)
                );
                ALTER TABLE gadm.adm_l2 OWNER TO postgres;

                CREATE SEQUENCE IF NOT EXISTS gadm.adm_l2_gid_seq
                    AS integer
                    START WITH 1
                    INCREMENT BY 1
                    NO MINVALUE
                    NO MAXVALUE
                    CACHE 1;

                ALTER TABLE gadm.adm_l2_gid_seq OWNER TO postgres;
                ALTER SEQUENCE gadm.adm_l2_gid_seq OWNED BY gadm.adm_l2.gid;

                CREATE TABLE IF NOT EXISTS gadm.countries (
                    gid integer NOT NULL,
                    fips character varying(2),
                    iso2 character varying(2),
                    iso3 character varying(3),
                    un smallint,
                    name character varying(50),
                    area integer,
                    pop2005 bigint,
                    region smallint,
                    subregion smallint,
                    lon double precision,
                    lat double precision,
                    geom public.geometry(MultiPolygon),
                    enabled boolean DEFAULT true NOT NULL
                );
                ALTER TABLE gadm.countries OWNER TO admin;

                CREATE TABLE IF NOT EXISTS gadm.regions (
                    name character varying(30) NOT NULL,
                    geom public.geometry NOT NULL,
                    id integer NOT NULL,
                    enabled boolean DEFAULT true NOT NULL
                );
                ALTER TABLE gadm.regions OWNER TO admin;

                CREATE TABLE IF NOT EXISTS gadm.subregions (
                    name character varying(100) NOT NULL,
                    geom public.geometry NOT NULL,
                    region_id integer,
                    id integer NOT NULL,
                    enabled boolean DEFAULT true NOT NULL
                );
                ALTER TABLE gadm.subregions OWNER TO admin;
                ALTER TABLE ONLY gadm.adm_l1 ALTER COLUMN gid SET DEFAULT nextval('gadm.adm_l1_gid_seq'::regclass);
                ALTER TABLE ONLY gadm.adm_l2 ALTER COLUMN gid SET DEFAULT nextval('gadm.adm_l2_gid_seq'::regclass);
                ALTER TABLE ONLY gadm.adm_l1 DROP CONSTRAINT IF EXISTS adm_l1_pkey CASCADE;
                ALTER TABLE ONLY gadm.adm_l1 ADD CONSTRAINT adm_l1_pkey PRIMARY KEY (gid);
                ALTER TABLE ONLY gadm.adm_l2 DROP CONSTRAINT IF EXISTS adm_l2_pkey CASCADE;
                ALTER TABLE ONLY gadm.adm_l2 ADD CONSTRAINT adm_l2_pkey PRIMARY KEY (gid);
                ALTER TABLE ONLY gadm.regions DROP CONSTRAINT IF EXISTS pk_regions CASCADE;
                ALTER TABLE ONLY gadm.regions ADD CONSTRAINT pk_regions PRIMARY KEY (id);
                ALTER TABLE ONLY gadm.subregions DROP CONSTRAINT IF EXISTS pk_subregions CASCADE;
                ALTER TABLE ONLY gadm.subregions ADD CONSTRAINT pk_subregions PRIMARY KEY (id);
                ALTER TABLE ONLY gadm.countries DROP CONSTRAINT IF EXISTS "tm_world_borders_simpl-0.3_pkey" CASCADE;
                ALTER TABLE ONLY gadm.countries ADD CONSTRAINT "tm_world_borders_simpl-0.3_pkey" PRIMARY KEY (gid);
                ALTER TABLE ONLY gadm.adm_l1 DROP CONSTRAINT IF EXISTS u_gid_1 CASCADE;
                ALTER TABLE ONLY gadm.adm_l1 ADD CONSTRAINT u_gid_1 UNIQUE (gid_1);
                ALTER TABLE ONLY gadm.countries DROP CONSTRAINT IF EXISTS u_iso_code CASCADE;
                ALTER TABLE ONLY gadm.countries ADD CONSTRAINT u_iso_code UNIQUE (iso3);
                CREATE INDEX IF NOT EXISTS adm_l1_geom_idx ON gadm.adm_l1 USING gist (geom);
                CREATE INDEX IF NOT EXISTS adm_l2_geom_idx ON gadm.adm_l2 USING gist (geom);

                CREATE INDEX IF NOT EXISTS fki_fk_adm_l1_countries ON gadm.adm_l1 USING btree (gid_0);
                CREATE INDEX IF NOT EXISTS fki_fk_adm_l2_adm_l1 ON gadm.adm_l2 USING btree (gid_1);
                CREATE INDEX IF NOT EXISTS fki_fk_adm_l2_countries ON gadm.adm_l2 USING btree (gid_0);
                CREATE INDEX IF NOT EXISTS fki_fk_countries_regions ON gadm.countries USING btree (region);
                CREATE INDEX IF NOT EXISTS fki_fk_countries_subregions ON gadm.countries USING btree (subregion);
                CREATE INDEX IF NOT EXISTS fki_p ON gadm.subregions USING btree (region_id);
                CREATE INDEX IF NOT EXISTS idx_countries_enabled ON gadm.countries USING btree (enabled);
                CREATE INDEX IF NOT EXISTS idx_regions_enabled ON gadm.regions USING btree (enabled);
                CREATE INDEX IF NOT EXISTS idx_subregions_enabled ON gadm.subregions USING btree (enabled);
                CREATE INDEX IF NOT EXISTS ix_adm_l1_country ON gadm.adm_l1 USING btree (name_0) INCLUDE (name_1);
                CREATE INDEX IF NOT EXISTS "tm_world_borders_simpl-0.3_geom_idx" ON gadm.countries USING gist (geom);

                ALTER TABLE ONLY gadm.adm_l1 ADD CONSTRAINT fk_adm_l1_countries FOREIGN KEY (gid_0) REFERENCES gadm.countries(iso3) NOT VALID;
                ALTER TABLE ONLY gadm.adm_l2 ADD CONSTRAINT fk_adm_l2_adm_l1 FOREIGN KEY (gid_1) REFERENCES gadm.adm_l1(gid_1) NOT VALID;
                ALTER TABLE ONLY gadm.adm_l2 ADD CONSTRAINT fk_adm_l2_countries FOREIGN KEY (gid_0) REFERENCES gadm.countries(iso3) NOT VALID;
                ALTER TABLE ONLY gadm.countries ADD CONSTRAINT fk_countries_regions FOREIGN KEY (region) REFERENCES gadm.regions(id) NOT VALID;
                ALTER TABLE ONLY gadm.countries ADD CONSTRAINT fk_countries_subregions FOREIGN KEY (subregion) REFERENCES gadm.subregions(id) NOT VALID;
                ALTER TABLE ONLY gadm.subregions ADD CONSTRAINT fk_subregions_regions FOREIGN KEY (region_id) REFERENCES gadm.regions(id) NOT VALID;            
            $str$;
            raise notice '%', _statement;
            execute _statement;            
        
            -- Changed tables
            _statement := $str$   
                ALTER TABLE node_resource_log ALTER COLUMN timestamp SET DATA TYPE timestamp with time zone;
                ALTER TABLE datasource ADD COLUMN IF NOT EXISTS product_type character varying(50) DEFAULT NULL;
                alter table l1_tile_history add column if not exists node_id text;
                alter table fmask_history add column if not exists node_id text;
                
                ALTER TABLE downloader_history ADD COLUMN IF NOT EXISTS product_type_id smallint;
                ALTER TABLE downloader_history ALTER COLUMN satellite_id DROP NOT NULL;

                ALTER TABLE satellite ADD COLUMN IF NOT EXISTS required BOOLEAN NOT NULL DEFAULT false;
                UPDATE satellite SET required = true WHERE id IN (1, 3); -- S2, S1


                ALTER TABLE processor ADD COLUMN IF NOT EXISTS required BOOLEAN NOT NULL DEFAULT false;
                UPDATE processor SET required = true WHERE id IN (1, 7, 8, 21); -- l2a, l2-s1, lpis. Others (MDB1)?
                ALTER TABLE processor ADD COLUMN IF NOT EXISTS supported_satellite_ids smallint[];
                ALTER TABLE processor ALTER COLUMN supported_satellite_ids DROP NOT NULL;

                ALTER TABLE processor ADD COLUMN IF NOT EXISTS mandatory_satellite_ids smallint[];
                ALTER TABLE processor ALTER COLUMN mandatory_satellite_ids DROP NOT NULL;
                
                ALTER TABLE processor ADD COLUMN IF NOT EXISTS is_service_ui_visible BOOLEAN DEFAULT true;
                ALTER TABLE processor ALTER COLUMN is_service_ui_visible DROP NOT NULL;
                ALTER TABLE processor ADD COLUMN IF NOT EXISTS is_admin_ui_visible BOOLEAN DEFAULT true;
                ALTER TABLE processor ALTER COLUMN is_admin_ui_visible DROP NOT NULL;
                ALTER TABLE processor ADD COLUMN IF NOT EXISTS lpis_required BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE processor ADD COLUMN IF NOT EXISTS additional_config_required boolean NOT NULL DEFAULT false;

                ALTER TABLE config_metadata ADD COLUMN IF NOT EXISTS is_service_ui_visible boolean NOT NULL DEFAULT false;

                ALTER TABLE config_category ADD COLUMN IF NOT EXISTS parent_category_id smallint DEFAULT NULL;                
                
            $str$;
            raise notice '%', _statement;
            execute _statement;   

            IF (select pg_typeof(last_run_timestamp)::text from scheduled_task_status limit 1) = 'character varying' THEN
                raise notice 'Executing update of scheduled_task_status last_retry_timestamp and estimated_next_run_time';
                 _statement := $str$
                    UPDATE scheduled_task_status SET last_retry_timestamp = null WHERE last_retry_timestamp='';
                    UPDATE scheduled_task_status SET estimated_next_run_time = null WHERE estimated_next_run_time='';                    
                $str$;
                raise notice '%', _statement;
                execute _statement;    
            ELSE 
                raise notice 'No updates done last_retry_timestamp and estimated_next_run_time columns from table scheduled_task_status';
            END IF;
            
            _statement := $str$
                ALTER TABLE scheduled_task ALTER COLUMN first_run_time SET DATA TYPE timestamptz USING first_run_time::timestamptz;
                ALTER TABLE scheduled_task ALTER COLUMN first_run_time SET NOT NULL;
                
                ALTER TABLE scheduled_task_status ALTER COLUMN next_schedule SET DATA TYPE timestamptz USING next_schedule::timestamptz;
                ALTER TABLE scheduled_task_status ALTER COLUMN next_schedule SET NOT NULL;
                ALTER TABLE scheduled_task_status ALTER COLUMN last_scheduled_run SET DATA TYPE timestamptz USING last_scheduled_run::timestamptz;
                ALTER TABLE scheduled_task_status ALTER COLUMN last_run_timestamp SET DATA TYPE timestamptz USING last_run_timestamp::timestamptz;
                ALTER TABLE scheduled_task_status ALTER COLUMN last_retry_timestamp SET DATA TYPE timestamptz USING last_retry_timestamp::timestamptz;
                ALTER TABLE scheduled_task_status ALTER COLUMN estimated_next_run_time SET DATA TYPE timestamptz USING estimated_next_run_time::timestamptz;                

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                CREATE OR REPLACE VIEW v_site_config as
                SELECT config_metadata.key,
                       site.id as site_id,
                       config.value
                FROM site
                         CROSS JOIN config_metadata
                         CROSS JOIN LATERAL (
                    SELECT COALESCE((
                                        SELECT value
                                        FROM config
                                        WHERE key = config_metadata.key
                                          AND config.site_id = site.id
                                    ), (
                                        SELECT value
                                        FROM config
                                        WHERE key = config_metadata.key
                                          AND config.site_id IS NULL
                                    )) AS value
                    ) config;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DROP FUNCTION IF EXISTS sp_get_processors();
                CREATE OR REPLACE FUNCTION sp_get_processors()
                RETURNS TABLE (
                    id processor.id%TYPE,
                    "short_name" processor."short_name"%TYPE,
                    "name" processor."name"%TYPE,
                    "required" processor."required"%TYPE
                )
                AS $$
                BEGIN
                    RETURN QUERY
                        SELECT processor.id,
                               processor.short_name,
                               processor.name,
                               processor.required
                        FROM processor
                        ORDER BY processor.id;
                END
                $$
                LANGUAGE plpgsql
                STABLE;  

                DROP FUNCTION IF EXISTS sp_get_scheduled_tasks();
                CREATE OR REPLACE FUNCTION sp_get_scheduled_tasks()
                RETURNS TABLE (
                    id scheduled_task.id%TYPE,
                    name scheduled_task.name%TYPE,

                    processor_id scheduled_task.processor_id%TYPE,
                    site_id scheduled_task.site_id%TYPE,
                    season_id scheduled_task.season_id%TYPE,
                    processor_params scheduled_task.processor_params%TYPE,

                    repeat_type scheduled_task.repeat_type%TYPE,
                    repeat_after_days scheduled_task.repeat_after_days%TYPE,
                    repeat_on_month_day scheduled_task.repeat_on_month_day%TYPE,
                    retry_seconds scheduled_task.retry_seconds%TYPE,

                    priority scheduled_task.priority%TYPE,

                    first_run_time timestamptz,

                    status_id scheduled_task_status.id%TYPE,
                    next_schedule timestamptz,
                    last_scheduled_run timestamptz,
                    last_run_timestamp timestamptz,
                    last_retry_timestamp timestamptz,
                    estimated_next_run_time timestamptz
                )
                AS $$
                BEGIN
                    RETURN QUERY
                        SELECT scheduled_task.id,
                               scheduled_task.name,
                               scheduled_task.processor_id,
                               scheduled_task.site_id,
                               scheduled_task.season_id,
                               scheduled_task.processor_params,
                               scheduled_task.repeat_type,
                               scheduled_task.repeat_after_days,
                               scheduled_task.repeat_on_month_day,
                               scheduled_task.retry_seconds,
                               scheduled_task.priority,
                               scheduled_task.first_run_time,
                               scheduled_task_status.id,
                               scheduled_task_status.next_schedule,
                               scheduled_task_status.last_scheduled_run,
                               scheduled_task_status.last_run_timestamp,
                               scheduled_task_status.last_retry_timestamp,
                               scheduled_task_status.estimated_next_run_time
                        FROM scheduled_task
                        INNER JOIN scheduled_task_status ON scheduled_task.id = scheduled_task_status.task_id
                        INNER JOIN site on site.id = scheduled_task.site_id
                        WHERE site.enabled;
                END
                $$
                LANGUAGE plpgsql
                STABLE;


                DROP FUNCTION sp_get_products(smallint, smallint, timestamp with time zone, timestamp with time zone);

                CREATE OR REPLACE FUNCTION sp_get_products(
                    IN _site_id smallint DEFAULT NULL::smallint,
                    IN _product_type_id smallint DEFAULT NULL::smallint,
                    IN _start_time timestamp with time zone DEFAULT NULL::timestamp with time zone,
                    IN _end_time timestamp with time zone DEFAULT NULL::timestamp with time zone)
                  RETURNS TABLE(product_id integer, product_type_id smallint, site_id smallint, satellite_id integer, name character varying, 
                                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone,
                                                quicklook_image character varying, geog geography,  orbit_id integer, tiles character varying[], 
                                                downloader_history_id integer) AS
                $BODY$
                DECLARE q text;
                BEGIN
                    q := $sql$
                        SELECT id, product_type_id, site_id, satellite_id, name, 
                             full_path, created_timestamp, inserted_timestamp, 
                             quicklook_image, geog, orbit_id, tiles, downloader_history_id FROM product P 
                        WHERE TRUE$sql$;

                    IF NULLIF($1, -1) IS NOT NULL THEN
                        q := q || $sql$
                            AND P.site_id = $1$sql$;
                    END IF;
                    IF NULLIF($2, -1) IS NOT NULL THEN
                        q := q || $sql$
                            AND P.product_type_id = $2$sql$;
                    END IF;
                    IF $3 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp >= $3$sql$;
                    END IF;
                    IF $4 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp <= $4$sql$;
                    END IF;
                    q := q || $SQL$
                        ORDER BY P.name;$SQL$;

                    -- raise notice '%', q;
                    
                    RETURN QUERY
                        EXECUTE q
                        USING $1, $2, $3, $4;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE;


                DROP FUNCTION IF EXISTS sp_insert_scheduled_task(character varying, integer, integer, integer, 
                                        smallint, smallint, smallint, character varying, integer, smallint, json);
                CREATE OR REPLACE FUNCTION sp_insert_scheduled_task(
                    _name character varying,
                    _processor_id integer,
                    _site_id integer,
                    _season_id integer,
                    _repeat_type smallint,
                    _repeat_after_days smallint,
                    _repeat_on_month_day smallint,
                    _first_run_time timestamptz,
                    _retry_seconds  integer,
                    _priority smallint,
                    _processor_params json)
                  RETURNS integer AS
                $BODY$
                DECLARE _return_id int;
                BEGIN

                    INSERT INTO scheduled_task(
                        name,
                        processor_id,
                        site_id,
                        season_id,
                        repeat_type,
                        repeat_after_days,
                        repeat_on_month_day,
                        first_run_time,
                        retry_seconds,
                        priority,
                        processor_params)
                    VALUES (
                        _name,
                        _processor_id,
                        _site_id,
                        _season_id,
                        _repeat_type,
                        _repeat_after_days,
                        _repeat_on_month_day,
                        _first_run_time,
                        _retry_seconds,
                        _priority,
                        _processor_params
                    ) RETURNING id INTO _return_id;

                    INSERT INTO scheduled_task_status(
                        task_id,
                        next_schedule,
                        last_scheduled_run,
                        last_run_timestamp,
                        last_retry_timestamp,
                        estimated_next_run_time)
                    VALUES (
                        _return_id,
                        _first_run_time,
                        null,
                        null,
                        null,
                        null
                    );

                    RETURN _return_id;

                END;
                $BODY$
                  LANGUAGE plpgsql VOLATILE;            

                DROP FUNCTION sp_get_product_by_name(IN _site_id site.id%TYPE, IN _name character varying);
                CREATE OR REPLACE FUNCTION sp_get_product_by_name(
                    _site_id site.id%TYPE,
                    _name character varying)
                  RETURNS TABLE(product_id smallint, product_type_id smallint, processor_id smallint, site_id smallint, full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone,
                                satellite_id integer, name character varying, quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[], downloader_history_id integer) AS
                $BODY$
                BEGIN

                RETURN QUERY SELECT product.product_type_id AS product_id, product.product_type_id, product.processor_id, product.site_id, product.full_path, product.created_timestamp, product.inserted_timestamp,
                                    product.satellite_id, product.name, product.quicklook_image, product.geog, product.orbit_id, product.tiles, product.downloader_history_id
                FROM product
                WHERE product.site_id = _site_id AND
                      product.name = _name;

                END;
                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_product_by_name(smallint, character varying)
                  OWNER TO admin;


                DROP FUNCTION sp_get_product_by_id(IN _id integer);
                CREATE OR REPLACE FUNCTION sp_get_product_by_id(IN _id integer)
                  RETURNS TABLE(product_id integer, product_type_id smallint, processor_id smallint, site_id smallint, full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone,
                                satellite_id integer, name character varying, quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[], downloader_history_id integer) AS
                $BODY$
                                BEGIN
                                    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.processor_id, product.site_id, product.full_path, product.created_timestamp, product.inserted_timestamp,
                                    product.satellite_id, product.name,  product.quicklook_image, product.geog, product.orbit_id, product.tiles, product.downloader_history_id
                                    FROM product
                                    WHERE product.id = _id;
                                END;
                                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_product_by_id(integer)
                  OWNER TO admin;


                DROP FUNCTION IF EXISTS sp_pad_left_json_history_array(json, timestamp, varchar);
                CREATE OR REPLACE FUNCTION sp_pad_left_json_history_array(
                IN _history json,
                IN _since timestamp with time zone,
                IN _interval varchar
                )
                RETURNS json AS $$
                DECLARE temp_array json[];
                DECLARE temp_json json;
                DECLARE previous_timestamp timestamp with time zone;
                BEGIN

                    -- Get the array of timestamp - value json pairs
                    SELECT array_agg(history_array.value::json) INTO temp_array FROM (SELECT * FROM json_array_elements(_history)) AS history_array;

                    -- If the array is not empty, get the oldes timestamp
                    IF temp_array IS NULL OR array_length(temp_array,1) = 0 THEN
                        previous_timestamp := now();
                    ELSE
                        previous_timestamp := timestamp with time zone 'epoch' + (temp_array[1]::json->>0)::bigint / 1000 * INTERVAL '1 second';
                    END IF;

                    -- Add values to the left of the array until the desired "since" timestamp is reached
                    LOOP
                        -- Compute the new previous timestamp
                        previous_timestamp := previous_timestamp - _interval::interval;

                        -- If using the new previous timestamp would take the array beyond the since, break
                        IF previous_timestamp < _since THEN
                            EXIT;
                        END IF;

                        temp_json := json_build_array(extract(epoch from previous_timestamp)::bigint * 1000, null);
                        temp_array := array_prepend(temp_json, temp_array);
                    END LOOP;

                    temp_json := array_to_json(temp_array);

                    RETURN temp_json;

                END;
                $$ LANGUAGE plpgsql;
                
                
                --
                DROP FUNCTION IF EXISTS sp_pad_right_json_history_array(json, timestamp, varchar);
                CREATE OR REPLACE FUNCTION sp_pad_right_json_history_array(
                IN _history json,
                IN _since timestamp with time zone,
                IN _interval varchar
                )
                RETURNS json AS $$
                DECLARE temp_array json[];
                DECLARE temp_json json;
                DECLARE previous_timestamp timestamp with time zone;
                DECLARE to_timestamp timestamp with time zone;
                BEGIN

                    -- Get the array of timestamp - value json pairs
                    SELECT array_agg(history_array.value::json) INTO temp_array FROM (SELECT * FROM json_array_elements(_history)) AS history_array;

                    -- The previous timestamp always starts from now
                    previous_timestamp := now();

                    -- If the array is not empty, get the newest timestamp; otherwise use _since as the oldest entry to go to
                    IF temp_array IS NULL OR array_length(temp_array,1) = 0 THEN
                        to_timestamp := _since;
                    ELSE
                        to_timestamp := timestamp with time zone 'epoch' + (temp_array[array_length(temp_array, 1)]::json->>0)::bigint / 1000 * INTERVAL '1 second';
                    END IF;

                    -- Add values to the right of the array until the desired "to" timestamp is reached
                    LOOP
                        -- Compute the new previous timestamp
                        previous_timestamp := previous_timestamp - _interval::interval;

                        -- If using the new previous timestamp would take the array beyond the to, or beyond the _since, break. This keeps the array from growing larger than needed.
                        IF previous_timestamp < to_timestamp OR previous_timestamp < _since THEN
                            EXIT;
                        END IF;

                        temp_json := json_build_array(extract(epoch from previous_timestamp)::bigint * 1000, null);
                        temp_array := array_append(temp_array, temp_json);
                    END LOOP;

                    temp_json := array_to_json(temp_array);

                    RETURN temp_json;

                END;
                $$ LANGUAGE plpgsql;

                CREATE OR REPLACE FUNCTION sp_get_dashboard_server_resource_data()
                    RETURNS json AS $$
                    DECLARE current_node RECORD;
                    DECLARE temp_json json;
                    DECLARE temp_json2 json;
                    DECLARE cpu_user_history_json json;
                    DECLARE cpu_system_history_json json;
                    DECLARE ram_history_json json;
                    DECLARE swap_history_json json;
                    DECLARE load_1min_history_json json;
                    DECLARE load_5min_history_json json;
                    DECLARE load_15min_history_json json;

                    DECLARE since timestamp with time zone;
                    BEGIN

                        CREATE TEMP TABLE current_nodes (
                            name character varying,
                            cpu_user_now smallint,
                            cpu_user_history json,
                            cpu_system_now smallint,
                            cpu_system_history json,
                            ram_now real,
                            ram_available real,
                            ram_unit character varying,
                            ram_history json,
                            swap_now real,
                            swap_available real,
                            swap_unit character varying,
                            swap_history json,
                            disk_used real,
                            disk_available real,
                            disk_unit character varying,
                            load_1min real,
                            load_5min real,
                            load_15min real,
                            load_1min_history json,
                            load_5min_history json,
                            load_15min_history json
                            ) ON COMMIT DROP;

                        -- Get the list of nodes to return the resources for
                        INSERT INTO current_nodes (name)
                        SELECT DISTINCT	node_name
                        FROM node_resource_log ORDER BY node_resource_log.node_name;

                        -- Ensure that default values are set for some of the fields
                        UPDATE current_nodes
                        SET
                            cpu_user_now = 0,
                            cpu_system_now = 0,
                            ram_now = 0,
                            ram_available = 0,
                            ram_unit = 'GB',
                            swap_now = 0,
                            swap_available = 0,
                            swap_unit = 'GB',
                            disk_used = 0,
                            disk_available = 0,
                            disk_unit = 'GB',
                            load_1min = 0,
                            load_5min = 0,
                            load_15min = 0;

                        -- Go through the nodes and compute their data
                        FOR current_node IN SELECT * FROM current_nodes ORDER BY name LOOP

                            -- First, get the NOW data
                            UPDATE current_nodes
                            SET
                                cpu_user_now = coalesce(current_node_now.cpu_user,0) / 10,
                                cpu_system_now = coalesce(current_node_now.cpu_system,0) / 10,
                                ram_now = round(coalesce(current_node_now.mem_used_kb,0)::numeric / 1048576::numeric, 2),	-- Convert to GB
                                ram_available = round(coalesce(current_node_now.mem_total_kb,0)::numeric / 1048576::numeric, 2),	-- Convert to GB
                                ram_unit = 'GB',
                                swap_now = round(coalesce(current_node_now.swap_used_kb,0)::numeric / 1048576::numeric, 2),	-- Convert to GB
                                swap_available = round(coalesce(current_node_now.swap_total_kb,0)::numeric / 1048576::numeric, 2),	-- Convert to GB
                                swap_unit = 'GB',
                                disk_used = round(coalesce(current_node_now.disk_used_bytes,0)::numeric / 1073741824::numeric, 2),	-- Convert to GB
                                disk_available = round(coalesce(current_node_now.disk_total_bytes,0)::numeric / 1073741824::numeric, 2),	-- Convert to GB
                                disk_unit = 'GB',
                                load_1min = coalesce(current_node_now.load_avg_1m,0) / 100,
                                load_5min = coalesce(current_node_now.load_avg_5m,0) / 100,
                                load_15min = coalesce(current_node_now.load_avg_15m,0) / 100
                            FROM (SELECT * FROM node_resource_log WHERE node_resource_log.node_name = current_node.name
                            AND timestamp >= now() - '1 minute'::interval
                            ORDER BY timestamp DESC LIMIT 1) AS current_node_now
                            WHERE current_nodes.name = current_node.name;

                            -- The history will be shown since:
                            since := now() - '15 minutes'::interval;

                            -- Next, get the HISTORY data
                            SELECT
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, resource_history.cpu_user / 10))),
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, resource_history.cpu_system / 10))),
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, round(resource_history.mem_used_kb::numeric / 1048576::numeric, 2)))),	-- Convert to GB
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, round(resource_history.swap_used_kb::numeric / 1048576::numeric, 2)))),	-- Convert to GB
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, resource_history.load_avg_1m / 100))),
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, resource_history.load_avg_5m / 100))),
                                array_to_json(array_agg( json_build_array(extract(epoch from resource_history.timestamp)::bigint * 1000, resource_history.load_avg_15m / 100)))
                            INTO
                                cpu_user_history_json,
                                cpu_system_history_json,
                                ram_history_json,
                                swap_history_json,
                                load_1min_history_json,
                                load_5min_history_json,
                                load_15min_history_json
                            FROM (
                                SELECT
                                timestamp,
                                cpu_user,
                                cpu_system,
                                mem_used_kb,
                                swap_used_kb,
                                load_avg_1m,
                                load_avg_5m,
                                load_avg_15m
                                FROM node_resource_log
                                WHERE node_resource_log.node_name = current_node.name
                                AND node_resource_log.timestamp >= since
                                ORDER BY timestamp DESC) resource_history;

                            -- Make sure that there are enough entries in the arrays so that the graph is shown as coming from right to left in the first 15 minutes
                            cpu_user_history_json := sp_pad_left_json_history_array(cpu_user_history_json, since, '1 minute');
                            cpu_system_history_json := sp_pad_left_json_history_array(cpu_system_history_json, since, '1 minute');
                            ram_history_json := sp_pad_left_json_history_array(ram_history_json, since, '1 minute');
                            swap_history_json := sp_pad_left_json_history_array(swap_history_json, since, '1 minute');
                            load_1min_history_json := sp_pad_left_json_history_array(load_1min_history_json, since, '1 minute');
                            load_5min_history_json := sp_pad_left_json_history_array(load_5min_history_json, since, '1 minute');
                            load_15min_history_json := sp_pad_left_json_history_array(load_15min_history_json, since, '1 minute');

                            -- Make sure that there are entries added in the arrays even if there isn't data up to now
                            cpu_user_history_json := sp_pad_right_json_history_array(cpu_user_history_json, since, '1 minute');
                            cpu_system_history_json := sp_pad_right_json_history_array(cpu_system_history_json, since, '1 minute');
                            ram_history_json := sp_pad_right_json_history_array(ram_history_json, since, '1 minute');
                            swap_history_json := sp_pad_right_json_history_array(swap_history_json, since, '1 minute');
                            load_1min_history_json := sp_pad_right_json_history_array(load_1min_history_json, since, '1 minute');
                            load_5min_history_json := sp_pad_right_json_history_array(load_5min_history_json, since, '1 minute');
                            load_15min_history_json := sp_pad_right_json_history_array(load_15min_history_json, since, '1 minute');

                            UPDATE current_nodes
                            SET
                                cpu_user_history = cpu_user_history_json,
                                cpu_system_history = cpu_system_history_json,
                                ram_history = ram_history_json,
                                swap_history = swap_history_json,
                                load_1min_history = load_1min_history_json,
                                load_5min_history = load_5min_history_json,
                                load_15min_history = load_15min_history_json
                            WHERE current_nodes.name = current_node.name;

                        END LOOP;

                        SELECT array_to_json(array_agg(row_to_json(current_nodes_details, true))) INTO temp_json
                        FROM (SELECT * FROM current_nodes) AS current_nodes_details;

                        temp_json2 := json_build_object('server_resources', temp_json);

                        RETURN temp_json2;


                    END;
                    $$ LANGUAGE plpgsql;


                -- 
                CREATE OR REPLACE FUNCTION sp_get_dashboard_processor_statistics()
                  RETURNS json AS
                $BODY$
                DECLARE current_processor RECORD;
                DECLARE temp_json json;
                DECLARE temp_json2 json;
                DECLARE temp_json3 json;
                DECLARE temp_array json[];
                DECLARE temp_array2 json[];
                DECLARE return_string text;
                BEGIN

                    CREATE TEMP TABLE processors (
                        id smallint,
                        name character varying
                        ) ON COMMIT DROP;

                    -- Get the list of processors to return the resources for
                    INSERT INTO processors (id, name)
                    SELECT id, short_name
                    FROM processor ORDER BY name;

                    return_string := '{';

                    -- Go through the processors and compute their data
                    FOR current_processor IN SELECT * FROM processors ORDER BY name LOOP

                        IF return_string != '{' THEN
                            return_string := return_string || ',';
                        END IF;

                        -- First compute the resource averages
                        WITH job_resources AS(
                        SELECT 
                        max(entry_timestamp) AS last_run,
                        sum(duration_ms) AS total_duration,
                        sum(user_cpu_ms) AS total_user_cpu,
                        sum(system_cpu_ms) AS total_system_cpu,
                        sum(max_rss_kb) AS total_max_rss,
                        sum(max_vm_size_kb) AS total_max_vm_size,
                        sum(disk_read_b) AS total_disk_read,
                        sum(disk_write_b) AS total_disk_write
                        FROM step_resource_log 
                        INNER JOIN task ON step_resource_log.task_id = task.id
                        INNER JOIN job ON task.job_id = job.id AND job.processor_id = current_processor.id
                        GROUP BY job.id)
                        SELECT '[' ||
                        '["Last Run On","' || to_char(max(last_run), 'YYYY-MM-DD HH:MI:SS') || '"],' ||
                        '["Average Duration","' || to_char(avg(total_duration) / 1000 * INTERVAL '1 second', 'HH24:MI:SS.MS') || '"],' ||
                        '["Average User CPU","' || to_char(avg(total_user_cpu) / 1000 * INTERVAL '1 second', 'HH24:MI:SS.MS') || '"],' ||
                        '["Average System CPU","' || to_char(avg(total_system_cpu) / 1000 * INTERVAL '1 second', 'HH24:MI:SS.MS') || '"],' ||
                        '["Average Max RSS","' || round(avg(total_max_rss)::numeric / 1024::numeric, 2)::varchar || ' MB' || '"],' ||
                        '["Average Max VM","' || round(avg(total_max_vm_size)::numeric / 1024::numeric, 2)::varchar || ' MB' || '"],' ||
                        '["Average Disk Read","' || round(avg(total_disk_read)::numeric / 1048576::numeric, 2)::varchar || ' MB' || '"],' ||
                        '["Average Disk Write","' || round(avg(total_disk_write)::numeric / 1048576::numeric, 2)::varchar || ' MB' || '"]' ||
                        ']' INTO temp_json
                        FROM job_resources;

                        temp_json := coalesce(temp_json, '[["Last Run On","never"],["Average Duration","00:00:00.000"],["Average User CPU","00:00:00.000"],["Average System CPU","00:00:00.000"],["Average Max RSS","0.00 MB"],["Average Max VM","0.00 MB"],["Average Disk Read","0.00 MB"],["Average Disk Write","0.00 MB"]]');

                        -- Next compute the output statistics
                        temp_array := array[]::json[];
                        SELECT json_build_array('Number of Products', count(*)) INTO temp_json2 FROM product WHERE processor_id = current_processor.id;
                        temp_array := array_append(temp_array, temp_json2);

                        WITH step_statistics AS(
                        SELECT 
                        count(*) AS no_of_tiles, 
                        sum(duration_ms)/count(*) AS average_duration_per_tile
                        FROM step_resource_log 
                        INNER JOIN task ON step_resource_log.task_id = task.id
                        INNER JOIN job ON task.job_id = job.id AND job.processor_id = current_processor.id
                        GROUP BY job.id)
                        SELECT array[json_build_array('Average Tiles per Product', coalesce(round(avg(no_of_tiles),2), 0)), json_build_array('Average Duration per Tile', coalesce(to_char(avg(average_duration_per_tile) / 1000 * INTERVAL '1 second', 'HH24:MI:SS.MS'), '00:00:00.000'))]
                        INTO temp_array2
                        FROM step_statistics;

                        temp_array := array_cat(temp_array, temp_array2);
                        temp_json2 := array_to_json(temp_array);

                        -- Last get the configuration parameters
                        WITH config_params AS (
                        SELECT json_build_array(
                        substring(key from length('processor.' || current_processor.name || '.')+1) || CASE coalesce(config.site_id,0) WHEN 0 THEN '' ELSE '(' || site.short_name || ')' END,
                        value) AS param
                        FROM config
                        LEFT OUTER JOIN site ON config.site_id = site.id
                        WHERE config.key ILIKE 'processor.' || current_processor.name || '.%')
                        SELECT array_to_json(array_agg(config_params.param)) INTO temp_json3
                        FROM config_params;

                        -- Update the return json with the computed data
                        return_string := return_string || '"' || current_processor.name || '_statistics" :' || json_build_object('resources', temp_json, 'output', coalesce(temp_json2, '[]'), 'configuration', coalesce(temp_json3, '[]'));
                        
                    END LOOP;

                    return_string := return_string || '}';
                    RETURN return_string::json;

                END;
                $BODY$
                  LANGUAGE plpgsql;


                DROP FUNCTION IF EXISTS public.delete_season_descriptors() CASCADE;
                CREATE OR REPLACE FUNCTION public.delete_season_descriptors()
                    RETURNS trigger AS
                $BODY$
                BEGIN
                    DELETE FROM site_auxdata
                        WHERE site_id = OLD.site_id AND season_id = OLD.id;
                    RETURN OLD;
                END;
                $BODY$
                LANGUAGE plpgsql VOLATILE
                  COST 100;
                ALTER FUNCTION public.delete_season_descriptors()
                  OWNER TO admin;                
                
                
                DROP FUNCTION IF EXISTS public.insert_season_descriptors() CASCADE;
                CREATE OR REPLACE FUNCTION public.insert_season_descriptors()
                    RETURNS trigger AS
                $BODY$
                BEGIN
                    INSERT INTO site_auxdata (site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, status_id, parameters, output)
                        SELECT site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, 3, parameters, null -- initially the status is 3=NeedsInput
                            FROM sp_get_auxdata_descriptor_instances(NEW.site_id, NEW.id, DATE_PART('year', NEW.start_date)::integer);
                    RETURN NEW;
                END;
                $BODY$
                LANGUAGE plpgsql VOLATILE
                  COST 100;
                ALTER FUNCTION public.insert_season_descriptors()
                  OWNER TO admin;
  
                drop function if exists sp_clear_pending_fmask_tiles;
                create or replace function sp_clear_pending_fmask_tiles(
                    _node_id text
                )
                returns void
                as
                $$
                begin
                    delete
                    from fmask_history
                    where status_id = 1 -- processing
                      and node_id = _node_id;
                end;
                $$ language plpgsql volatile;
  
  
                drop function if exists sp_clear_pending_l1_tiles;
                create or replace function sp_clear_pending_l1_tiles(
                    _node_id text
                )
                returns void
                as
                $$
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    delete
                    from l1_tile_history
                    using downloader_history
                    where downloader_history.id = l1_tile_history.downloader_history_id
                    and l1_tile_history.status_id = 1 -- processing
                    and l1_tile_history.node_id = _node_id
                    and downloader_history.satellite_id in (1, 2); -- sentinel2, landsat8

                    update downloader_history
                    set status_id = 2 -- downloaded
                    where status_id = 7 -- processing
                    and not exists (
                        select *
                        from l1_tile_history
                        where status_id = 1 -- processing
                    );
                end;
                $$ language plpgsql volatile;

                CREATE OR REPLACE FUNCTION sp_dashboard_add_site(
                    _name character varying,
                    _geog character varying,
                    _enabled boolean)
                RETURNS smallint AS
                $BODY$
                DECLARE _short_name character varying;
                DECLARE return_id smallint;
                BEGIN

                    _short_name := lower(_name);
                    _short_name := regexp_replace(_short_name, '\W+', '_', 'g');
                    _short_name := regexp_replace(_short_name, '_+', '_', 'g');
                    _short_name := regexp_replace(_short_name, '^_', '');
                    _short_name := regexp_replace(_short_name, '_$', '');

                    INSERT INTO site (name, short_name, geog, enabled)
                    VALUES (_name, _short_name, ST_Multi(ST_Force2D(ST_GeometryFromText(_geog))) :: geography, _enabled)
                    RETURNING id INTO return_id;

                    INSERT INTO site_tiles(site_id, satellite_id, tiles)
                    VALUES
                    (return_id, 1, (select array_agg(tile_id) from sp_get_site_tiles(return_id, 1 :: smallint))),
                    (return_id, 2, (select array_agg(tile_id) from sp_get_site_tiles(return_id, 2 :: smallint)));

                    RETURN return_id;
                END;
                $BODY$
                LANGUAGE plpgsql VOLATILE;

                DROP FUNCTION IF EXISTS sp_dashboard_update_scheduled_task(smallint, smallint, smallint, smallint, character varying, json);            
                CREATE OR REPLACE FUNCTION sp_dashboard_update_scheduled_task(
                    _schedule_id smallint,
                    _repeat_type smallint,
                    _repeat_after_days smallint,
                    _repeat_on_month_day smallint,
                    _first_run_time timestamptz,
                    _processor_params json)
                  RETURNS void AS
                $BODY$
                BEGIN

                UPDATE scheduled_task
                SET	processor_params = _processor_params,
                    repeat_type = _repeat_type,
                    repeat_after_days = _repeat_after_days,
                    repeat_on_month_day = _repeat_on_month_day,
                    first_run_time = _first_run_time
                WHERE id = _schedule_id;

                UPDATE scheduled_task_status
                SET next_schedule =_first_run_time,
                    last_scheduled_run = null,
                    last_run_timestamp = null,
                    last_retry_timestamp = null,
                    estimated_next_run_time = null
                WHERE task_id = _schedule_id;

                END;
                $BODY$
                  LANGUAGE plpgsql;


                create or replace function sp_evaluate_default_scheduled_tasks(
                    _prefix text,
                    _start_date date,
                    _mid_date date
                )
                    returns table
                            (
                                name text,
                                processor_id smallint,
                                repeat_type smallint,
                                repeat_after_days smallint,
                                repeat_on_month_day smallint,
                                first_run_time timestamptz,
                                retry_seconds int,
                                priority smallint,
                                processor_params json
                            )
                as
                $$
                begin
                    return query
                        select _prefix || '_' || suffix,
                               default_scheduled_tasks.processor_id,
                               default_scheduled_tasks.repeat_type,
                               default_scheduled_tasks.repeat_after_days,
                               default_scheduled_tasks.repeat_on_month_day,
                               date_trunc(
                                       coalesce(default_scheduled_tasks.first_run_base_trunc, 'day'),
                                       case default_scheduled_tasks.first_run_base
                                           when 'start' then _start_date
                                           when 'mid' then _mid_date
                                           end
                                   ) + coalesce(default_scheduled_tasks.first_run_offset, interval '0'),
                               default_scheduled_tasks.retry_seconds,
                               default_scheduled_tasks.priority,
                               coalesce(default_scheduled_tasks.processor_arguments, '{}')
                        from default_scheduled_tasks;
                end;
                $$
                    language plpgsql stable;


                DROP FUNCTION IF EXISTS sp_get_auxdata_descriptor_instances(smallint, smallint, integer);
                CREATE OR REPLACE FUNCTION public.sp_get_auxdata_descriptor_instances(
                    _site_id smallint,
                    _season_id smallint,
                    _year integer)
                    RETURNS TABLE(site_id smallint, auxdata_descriptor_id smallint, year integer, season_id smallint, auxdata_file_id smallint, file_name character varying, parameters json) 
                    LANGUAGE 'plpgsql'
                    COST 100
                    STABLE PARALLEL UNSAFE
                    ROWS 1000
                AS $BODY$

                BEGIN 
                    RETURN QUERY 

                    WITH last_ops AS (
                        SELECT f.id, COALESCE(MAX(o.operation_order), 0) AS operation_order 
                            FROM auxdata_file f LEFT JOIN auxdata_operation o on o.auxdata_file_id = f.id
                            GROUP BY f.id)
                    SELECT _site_id as site_id, d.id AS auxdata_descriptor_id, 
                        CASE WHEN d.unique_by = 'year' THEN _year ELSE null::integer END AS "year", 
                        CASE WHEN d.unique_by = 'season' THEN _season_id ELSE null::smallint END AS season_id, 
                        f.id AS auxdata_file_id, null::character varying AS "file_name",
                        o.parameters
                    FROM 	auxdata_descriptor d
                        JOIN auxdata_file f ON f.auxdata_descriptor_id = d.id
                        JOIN last_ops l ON l.id = f.id
                        LEFT JOIN auxdata_operation o ON o.auxdata_file_id = f.id AND o.operation_order = l.operation_order
                    WHERE	d.id NOT IN (SELECT s.auxdata_descriptor_id from site_auxdata s WHERE s.auxdata_descriptor_id = d.id AND s.site_id = _site_id AND ((d.unique_by = 'year' AND s.year = _year) OR (d.unique_by = 'season' AND s.season_id = _season_id)))
                    ORDER BY d.id, f.id;
                END

                $BODY$;

                ALTER FUNCTION public.sp_get_auxdata_descriptor_instances(smallint, smallint, integer)
                    OWNER TO admin;


                DROP FUNCTION IF EXISTS sp_get_dashboard_processors();
                CREATE OR REPLACE FUNCTION sp_get_dashboard_processors(processor_id smallint DEFAULT NULL::smallint)
                  RETURNS json AS
                $BODY$
                DECLARE return_string text;
                BEGIN
                    WITH data(id,name,description,short_name) AS (
                        SELECT 	PROC.id, 
                            PROC.name,
                            PROC.description, 
                            PROC.short_name,
                            PROC.required
                        FROM processor PROC
                        WHERE
                            ($1 IS NULL OR PROC.id = $1)
                        ORDER BY PROC.name
                    )

                    SELECT array_to_json(array_agg(row_to_json(data)),true) INTO return_string FROM data;
                    RETURN return_string::json;
                END
                $BODY$
                  LANGUAGE plpgsql VOLATILE;


                DROP FUNCTION IF EXISTS sp_get_dashboard_products_site(integer, integer[], smallint, integer[], timestamp with time zone, timestamp with time zone, character varying[]);
                CREATE OR REPLACE FUNCTION sp_get_dashboard_products_site(_site_id integer, _product_type_id integer[] DEFAULT NULL::integer[], _season_id smallint DEFAULT NULL::smallint, _satellit_id integer[] DEFAULT NULL::integer[], _since_timestamp timestamp with time zone DEFAULT NULL::timestamp with time zone, _until_timestamp timestamp with time zone DEFAULT NULL::timestamp with time zone, _tiles character varying[] DEFAULT NULL::character varying[])
                 RETURNS SETOF json
                AS $BODY$
                BEGIN
                    RETURN QUERY
                    WITH product_type_names(id, name, description, row) AS (
                        SELECT id, name, description, ROW_NUMBER() OVER (ORDER BY description)
                            FROM product_type),
                        data (id, satellite_id, product, product_type_id, product_type,product_type_description,processor,site_id,full_path,quicklook_image,footprint,created_timestamp) AS (
                        SELECT	P.id,
                                P.satellite_id,
                                P.name,
                                PT.id,
                                PT.name,
                                PT.description,
                                PR.name,
                                S.id,
                                P.full_path,
                                P.quicklook_image,
                                P.footprint,
                                P.created_timestamp
                            FROM product P
                                JOIN product_type_names PT ON P.product_type_id = PT.id
                                JOIN processor PR ON P.processor_id = PR.id
                                JOIN site S ON P.site_id = S.id
                            WHERE EXISTS (SELECT * FROM season 
                                          WHERE season.site_id = P.site_id AND P.created_timestamp BETWEEN season.start_date AND season.end_date
                                            AND ($3 IS NULL OR season.id = $3))
                                AND ($1 IS NULL OR P.site_id = $1)
                                AND ($2 IS NULL OR P.product_type_id = ANY($2))
                                AND ($4 IS NULL OR P.satellite_id = ANY($4))
                                AND ($5 IS NULL OR P.created_timestamp >= to_timestamp(cast($5 as TEXT),'YYYY-MM-DD HH24:MI:SS'))
                                AND ($6 IS NULL OR P.created_timestamp <= to_timestamp(cast($6 as TEXT),'YYYY-MM-DD HH24:MI:SS') + interval '1 day')
                                AND ($7 IS NULL OR (P.tiles <@$7 AND P.tiles!='{}'))
                            ORDER BY PT.row, P.name)
                            SELECT COALESCE(array_to_json(array_agg(row_to_json(data)), true), '[]'::json) FROM data;
                            --SELECT * FROM data;
                END
                $BODY$
                LANGUAGE plpgsql STABLE
                  COST 100;
                ALTER FUNCTION sp_get_dashboard_products_site(integer, integer[], smallint, integer[], timestamp with time zone, timestamp with time zone, character varying[])
                  OWNER TO admin;


                CREATE OR REPLACE FUNCTION sp_get_jobs_active(_processor_id smallint DEFAULT NULL, _site_id smallint DEFAULT NULL)
                  RETURNS TABLE(job_id integer, processor_id smallint, site_id smallint, status_id smallint) AS
                $BODY$
                    DECLARE q text;
                    BEGIN
                        q := $sql$
                            SELECT J.id AS job_id, J.processor_id, J.site_id, J.status_id                                       
                            FROM job J
                            WHERE J.status_id NOT IN (6,7,8) $sql$; -- Finished, Cancelled, Error
                            
                            IF NULLIF($1, -1) IS NOT NULL THEN
                                q := q || $sql$
                                        AND J.processor_id = $1$sql$;
                            END IF;            

                            IF NULLIF($2, -1) IS NOT NULL THEN
                                q := q || $sql$
                                        AND J.site_id = $2$sql$;
                            END IF;  

                        -- raise notice '%', q;
                        
                        RETURN QUERY
                            EXECUTE q
                            USING $1, $2;
                    END;
                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_jobs_active(smallint, smallint)
                  OWNER TO admin;


                DROP FUNCTION IF EXISTS sp_get_l1_derived_products(smallint,smallint,json,timestamp with time zone,timestamp with time zone);
                CREATE OR REPLACE FUNCTION sp_get_l1_derived_products(
                    IN site_id smallint DEFAULT NULL::smallint,
                    IN product_type_id smallint DEFAULT NULL::smallint,
                    IN downloader_history_ids json DEFAULT NULL::json,
                    IN start_time timestamp with time zone DEFAULT NULL::timestamp with time zone,
                    IN end_time timestamp with time zone DEFAULT NULL::timestamp with time zone)
                  RETURNS TABLE("ProductId" integer, "ProductTypeId" smallint, "SiteId" smallint, "SatId" integer, "ProductName" character varying, 
                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone,
                                quicklook_image character varying, geog geography,  orbit_id integer, tiles character varying[], 
                                downloader_history_id integer) AS
                $BODY$
                DECLARE q text;
                BEGIN
                    q := $sql$
                    SELECT P.id AS ProductId,
                        P.product_type_id AS ProductTypeId,
                        P.site_id AS SiteId,
                        P.satellite_id AS SatId,
                        P.name AS ProductName,
                        P.full_path,
                        P.created_timestamp,
                        P.inserted_timestamp,
                        P.quicklook_image as quicklook,
                        P.geog,
                        P.orbit_id,
                        P.tiles,
                        P.downloader_history_id
                    FROM product P WHERE TRUE$sql$;
                    
                    IF NULLIF($1, -1) IS NOT NULL THEN
                        q := q || $sql$
                            AND P.site_id = $1$sql$;
                    END IF;
                    IF NULLIF($2, -1) IS NOT NULL THEN
                        q := q || $sql$
                            AND P.product_type_id = $2$sql$;
                    END IF;
                    IF $3 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.downloader_history_id IN (SELECT value::integer FROM json_array_elements_text($3))
                        $sql$;
                    END IF;		
                    IF $4 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp >= $4$sql$;
                    END IF;
                    IF $5 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp <= $5$sql$;
                    END IF;
                    q := q || $SQL$
                        ORDER BY P.name;$SQL$;

                    -- raise notice '%', q;
                    
                    RETURN QUERY
                        EXECUTE q
                        USING $1, $2, $3, $4, $5;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE;
                
                DROP FUNCTION IF EXISTS sp_l4a_get_optical_products(product.site_id%type, product.satellite_id%type, 
                            product.created_timestamp%type, product.created_timestamp%type, product.tiles%type, text[]);
                create or replace function sp_l4a_get_optical_products(
                    _site_id product.site_id%type,
                    _satellite_id product.satellite_id%type,
                    _season_start product.created_timestamp%type,
                    _season_end product.created_timestamp%type,
                    _tiles product.tiles%type,
                    _products text[]
                )
                returns table (
                    site_id product.site_id%type,
                    full_path product.full_path%type,
                    tile text,
                    created_timestamp product.created_timestamp%type
                )
                as
                $$
                declare q text;

                begin
                    q := $sql$
                        select site_id,
                               full_path,
                               unnest(tiles) :: text as tile,
                               created_timestamp
                        from product
                        where site_id = $1
                          and satellite_id = $2
                          and product_type_id = 1
                          and created_timestamp between $3 and $4 + interval '1 day'$sql$;
                    if _tiles is not null then
                        q := q || $sql$
                            and tiles && $5 :: character varying[]$sql$;
                    end if;
                    if _products is not null then
                        q := q || $sql$
                            and name = any($6)$sql$;
                    end if;
                    q := q || ';';

                    -- raise notice '%', q;

                    return query
                        execute q
                        using _site_id,
                              _satellite_id,
                              _season_start,
                              _season_end,
                              _tiles,
                              _products;
                end
                $$
                language plpgsql stable;

                DROP FUNCTION IF EXISTS sp_get_parent_products_in_provenance_by_ids(json, json);            
                CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance_by_ids(
                    IN _derived_product_ids json,
                    IN _source_products_type_id json)
                  RETURNS TABLE(parent_product_id integer, parent_product_type_id smallint, parent_site_id smallint, parent_satellite_id integer, parent_name character varying, 
                                parent_full_path character varying, parent_created_timestamp timestamp with time zone, parent_inserted_timestamp timestamp with time zone,
                                parent_quicklook_image character varying, parent_geog geography,  parent_orbit_id integer, parent_tiles character varying[], 
                                parent_downloader_history_id integer, product_id integer) AS
                $BODY$
                DECLARE q text;
                BEGIN
                    q := $sql$
                        WITH derived_products AS (SELECT product_provenance.product_id as prd_id,
                                                  product_provenance.parent_product_id as parent_id
                                     FROM product 
                                     INNER JOIN product_provenance on product.id = product_provenance.product_id
                                     WHERE product_id IN (SELECT value::integer FROM json_array_elements_text($1)))
                            SELECT id, product_type_id, site_id, satellite_id, name, 
                                         full_path, created_timestamp, inserted_timestamp, 
                                         quicklook_image, geog, orbit_id, tiles, downloader_history_id,
                                         derived_products.prd_id
                            FROM product P INNER JOIN derived_products on P.id = derived_products.parent_id
                            WHERE product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))	
                        $sql$;
                    q := q || $SQL$
                        ORDER BY P.name;$SQL$;

                    -- raise notice '%', q;
                    
                    RETURN QUERY
                        EXECUTE q
                        USING $1, $2;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE;


                DROP FUNCTION IF EXISTS sp_get_parent_products_in_provenance_by_ids(json, json);            
                CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance_by_ids(
                    IN _derived_product_ids json,
                    IN _source_products_type_id json)
                  RETURNS TABLE(parent_product_id integer, parent_product_type_id smallint, parent_site_id smallint, parent_satellite_id integer, parent_name character varying, 
                                parent_full_path character varying, parent_created_timestamp timestamp with time zone, parent_inserted_timestamp timestamp with time zone,
                                parent_quicklook_image character varying, parent_geog geography,  parent_orbit_id integer, parent_tiles character varying[], 
                                parent_downloader_history_id integer, product_id integer) AS
                $BODY$
                DECLARE q text;
                BEGIN
                    q := $sql$
                        WITH derived_products AS (SELECT product_provenance.product_id as prd_id,
                                                  product_provenance.parent_product_id as parent_id
                                     FROM product 
                                     INNER JOIN product_provenance on product.id = product_provenance.product_id
                                     WHERE product_id IN (SELECT value::integer FROM json_array_elements_text($1)))
                            SELECT id, product_type_id, site_id, satellite_id, name, 
                                         full_path, created_timestamp, inserted_timestamp, 
                                         quicklook_image, geog, orbit_id, tiles, downloader_history_id,
                                         derived_products.prd_id
                            FROM product P INNER JOIN derived_products on P.id = derived_products.parent_id
                            WHERE product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))	
                        $sql$;
                    q := q || $SQL$
                        ORDER BY P.name;$SQL$;

                    -- raise notice '%', q;
                    
                    RETURN QUERY
                        EXECUTE q
                        USING $1, $2;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE;


                DROP FUNCTION IF EXISTS sp_get_parent_products_in_provenance_by_ids(json, json);            
                CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance_by_ids(
                    IN _derived_product_ids json,
                    IN _source_products_type_id json)
                  RETURNS TABLE(parent_product_id integer, parent_product_type_id smallint, parent_site_id smallint, parent_satellite_id integer, parent_name character varying, 
                                parent_full_path character varying, parent_created_timestamp timestamp with time zone, parent_inserted_timestamp timestamp with time zone,
                                parent_quicklook_image character varying, parent_geog geography,  parent_orbit_id integer, parent_tiles character varying[], 
                                parent_downloader_history_id integer, product_id integer) AS
                $BODY$
                DECLARE q text;
                BEGIN
                    q := $sql$
                        WITH derived_products AS (SELECT product_provenance.product_id as prd_id,
                                                  product_provenance.parent_product_id as parent_id
                                     FROM product 
                                     INNER JOIN product_provenance on product.id = product_provenance.product_id
                                     WHERE product_id IN (SELECT value::integer FROM json_array_elements_text($1)))
                            SELECT id, product_type_id, site_id, satellite_id, name, 
                                         full_path, created_timestamp, inserted_timestamp, 
                                         quicklook_image, geog, orbit_id, tiles, downloader_history_id,
                                         derived_products.prd_id
                            FROM product P INNER JOIN derived_products on P.id = derived_products.parent_id
                            WHERE product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))	
                        $sql$;
                    q := q || $SQL$
                        ORDER BY P.name;$SQL$;

                    -- raise notice '%', q;
                    
                    RETURN QUERY
                        EXECUTE q
                        USING $1, $2;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE;


                DROP FUNCTION IF EXISTS sp_get_parent_products_not_in_provenance(smallint, json, smallint, timestamp with time zone, timestamp with time zone);
                CREATE OR REPLACE FUNCTION sp_get_parent_products_not_in_provenance(
                    IN _site_id smallint,
                    IN _source_products_type_id json,
                    IN _derived_product_type_id smallint,
                    IN _start_time timestamp with time zone DEFAULT NULL::timestamp with time zone,
                    IN _end_time timestamp with time zone DEFAULT NULL::timestamp with time zone)
                  RETURNS TABLE("ProductId" integer, "ProductTypeId" smallint, "SiteId" smallint, "SatId" integer, "ProductName" character varying, 
                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, 
                                quicklook_image character varying, geog geography,  orbit_id integer, tiles character varying[], 
                                downloader_history_id integer) AS
                $BODY$
                DECLARE q text;
                BEGIN
                    q := $sql$
                        SELECT id, product_type_id, site_id, satellite_id, name, 
                                 full_path, created_timestamp, inserted_timestamp, 
                                 quicklook_image, geog, orbit_id, tiles, downloader_history_id
                             FROM product P WHERE site_id = $1 AND product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))
                             AND NOT EXISTS (
                                SELECT product_id FROM product_provenance WHERE parent_product_id = id AND
                                       product_id IN (SELECT id FROM product WHERE site_id = $1 AND product_type_id = $3)
                        ) $sql$;
                        -- SELECT id, product_type_id, site_id, satellite_id, name, 
                        --         full_path, created_timestamp, inserted_timestamp, 
                        --         quicklook_image, geog, orbit_id, tiles, downloader_history_id
                        --     FROM product P WHERE site_id = $1 AND product_type_id in (SELECT value::smallint FROM json_array_elements_text($2))
                        --     AND NOT EXISTS (
                        --         SELECT id FROM product_provenance PV JOIN product P ON P.id = PV.parent_product_id 
                        --         WHERE site_id = $1 AND PV.parent_product_id = id AND P.product_type_id = $3
                        --     )  $sql$;
                    
                    IF $4 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp >= $4$sql$;
                    END IF;
                    IF $5 IS NOT NULL THEN
                        q := q || $sql$
                            AND P.created_timestamp <= $5$sql$;
                    END IF;
                    q := q || $SQL$
                        ORDER BY P.name;$SQL$;

                    -- raise notice '%', q;
                    
                    RETURN QUERY
                        EXECUTE q
                        USING $1, $2, $3, $4, $5;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE;

                DROP FUNCTION IF EXISTS sp_get_full_products_by_id(json);
                DROP FUNCTION IF EXISTS sp_get_products_by_id(json);
                CREATE OR REPLACE FUNCTION sp_get_products_by_id(IN _ids json)
                  RETURNS TABLE(product_id integer, product_type_id smallint, site_id smallint, 
                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, 
                                satellite_id integer, name character varying, 
                                quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[],
                               downloader_history_id integer) AS
                $BODY$
                                BEGIN
                                    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.site_id, 
                                                        product.full_path, product.created_timestamp, product.inserted_timestamp,
                                                        product.satellite_id, product.name, 
                                                        product.quicklook_image, product.geog, product.orbit_id, product.tiles,
                                                        product.downloader_history_id
                                                        
                                    FROM product
                                    WHERE product.id in (SELECT value::integer FROM json_array_elements_text(_ids));
                                END;
                                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_products_by_id(json)
                  OWNER TO admin;

                  
                DROP FUNCTION IF EXISTS sp_get_full_products_by_name(json);
                DROP FUNCTION IF EXISTS sp_get_products_by_name(smallint, json);
                CREATE OR REPLACE FUNCTION sp_get_products_by_name(_site_id smallint, IN _names json)
                  RETURNS TABLE(product_id integer, product_type_id smallint, site_id smallint, 
                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, 
                                satellite_id integer, name character varying, 
                                quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[],
                               downloader_history_id integer) AS
                $BODY$
                                BEGIN
                                    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.site_id, 
                                                        product.full_path, product.created_timestamp, product.inserted_timestamp, 
                                                        product.satellite_id, product.name, 
                                                        product.quicklook_image, product.geog, product.orbit_id, product.tiles,
                                                        product.downloader_history_id
                                                        
                                    FROM product
                                    WHERE product.site_id = _site_id AND product.name in (SELECT value::character varying FROM json_array_elements_text(_names));
                                END;
                                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_products_by_name(smallint, json)
                  OWNER TO admin;                

                CREATE OR REPLACE FUNCTION sp_get_products_dwn_hist_ids(IN ids json)
                  RETURNS TABLE(product_id integer, downloader_history_id integer) AS
                $BODY$
                    BEGIN
                        RETURN QUERY SELECT product.id, product.downloader_history_id
                        FROM   product
                        WHERE id IN (SELECT value::integer FROM json_array_elements_text($1) ) AND 
                              product.downloader_history_id IS NOT NULL;
                   END;
                $BODY$  
                LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_product_by_id(integer)
                  OWNER TO admin;                


                DROP FUNCTION sp_get_products_for_tile(_site_id site.id%TYPE, _tile_id CHARACTER VARYING, _product_type_id SMALLINT, _satellite_id satellite.id%TYPE, _out_satellite_id satellite.id%TYPE);
                CREATE OR REPLACE FUNCTION sp_get_products_for_tile(_site_id site.id%TYPE, _tile_id CHARACTER VARYING, _product_type_id SMALLINT, _satellite_id satellite.id%TYPE, _out_satellite_id satellite.id%TYPE)
                  RETURNS TABLE(product_id integer, product_type_id smallint, site_id smallint, 
                                full_path character varying, created_timestamp timestamp with time zone, inserted_timestamp timestamp with time zone, 
                                satellite_id integer, name character varying, 
                                quicklook_image character varying, geog geography, orbit_id integer, tiles character varying[],
                               downloader_history_id integer)
                AS $$
                DECLARE _geog GEOGRAPHY;
                BEGIN
                    CASE _satellite_id
                        WHEN 1 THEN -- sentinel2
                            _geog := (SELECT shape_tiles_s2.geog FROM shape_tiles_s2 WHERE tile_id = _tile_id);
                        WHEN 2 THEN -- landsat8
                            _geog := (SELECT shape_tiles_l8 FROM shape_tiles_l8 WHERE shape_tiles_l8.pr = _tile_id :: INT);
                    END CASE;

                    RETURN QUERY SELECT product.id AS product_id, product.product_type_id, product.site_id, 
                                    product.full_path, product.created_timestamp, product.inserted_timestamp, 
                                    product.satellite_id, product.name, 
                                    product.quicklook_image, product.geog, product.orbit_id, product.tiles,
                                    product.downloader_history_id
                        FROM product
                        WHERE product.site_id = _site_id AND
                              product.satellite_id = _out_satellite_id AND
                              product.product_type_id = _product_type_id AND  
                              ST_Intersects(product.geog, _geog);
                END;
                $$
                LANGUAGE plpgsql
                STABLE;


                DROP FUNCTION IF EXISTS reports.sp_get_s1_statistics(smallint);

                CREATE OR REPLACE FUNCTION reports.sp_get_s1_statistics(
                    site_id smallint)
                    RETURNS TABLE(site smallint, downloader_history_id integer, orbit_id integer, acquisition_date date, acquisition character varying, acquisition_status character varying, intersection_date date, intersected_product character varying, intersected_status smallint, intersection double precision, polarisation character varying, l2_product character varying, l2_coverage double precision, status_reason character varying) 
                    LANGUAGE 'plpgsql'
                    COST 100
                    STABLE PARALLEL UNSAFE
                    ROWS 1000

                AS $BODY$
                BEGIN
                    RETURN QUERY
                    WITH d AS (select dh.*,ds.status_description from public.downloader_history dh join public.downloader_status ds on ds.id = dh.status_id)
                --procesarile
                select 	$1 as site,
                    d.id,
                    d.orbit_id as orbit, 
                    to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
                    d.product_name as acquisition,
                    d.status_description as acquisition_status,
                    to_date(substr(split_part(di.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
                    di.product_name as intersected_product,
                    cast(i.status_id as smallint) as intersected_status,
                    st_area(st_intersection(di.footprint, d.footprint)) / st_area(d.footprint) * 100 as intersection,
                    split_part(p.name, '_', 6)::character varying as polarisation,
                    p.name as l2_product,
                    st_area(st_intersection(d.footprint, p.geog))/st_area(d.footprint) * 100 as l2_coverage,
                    d.status_reason
                    from d
                    join public.l1_tile_history i
                         on d.id=i.downloader_history_id
                    join public.downloader_history di 
                         on di.product_name =i.tile_id
                    join public.product p on p.downloader_history_id = d.id
                    WHERE NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
                                         WHERE sr.downloader_history_id = d.id  
                                              AND sr.intersected_product = di.product_name 
                                              AND sr.site_id = di.site_id
                                              AND sr.l2_product = p.name)
                        and d.site_id = $1 
                        AND d.satellite_id = 3 
                        and di.id is not null
                        and p.name like concat('%', substr(split_part(di.product_name, '_', 6), 1, 15),'%')
                        
                union
                select 	$1 as site,
                    d.id,
                    d.orbit_id as orbit,
                    to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
                    d.product_name as acquisition,
                    d.status_description as acquisition_status,
                    to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
                    i.product_name as intersected_product,
                    i.status_id as intersected_status,
                    case when i.footprint is null then null else st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100 end as intersection,
                    null as polarisation,
                    null as l2_product,
                    null as l2_coverage,
                    null as status_reason
                    from  d
                        left outer join public.downloader_history i 
                            ON i.site_id = d.site_id 
                                AND i.orbit_id = d.orbit_id 
                                AND i.satellite_id = d.satellite_id 
                                and st_intersects(d.footprint, i.footprint) 
                                AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 5 AND 7 
                                AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
                    where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr WHERE sr.downloader_history_id = d.id) 
                        AND d.site_id = $1 
                        AND d.satellite_id = 3 
                        --and d.status_id != 5
                        AND d.status_id NOT IN (5,6,7,8) -- produse care nu au intrari in l1_tile_history
                --5	"processed"
                --6	"processing_failed"
                --7	"processing"
                --8	"processing_cld_failed"

                union

                    --produse cu status_id=5(processed) in tabela downloader_history, 
                    --care au intersectii in tabela l1_tile_history cu status_id=3(done), 
                    --dar care nu se regasesc in tabela product. 
                    --fals procesate
                select 	$1 as site,
                    d.id,
                    d.orbit_id as orbit,
                    to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
                    d.product_name as acquisition,
                    d.status_description as acquisition_status,
                    to_date(substr(split_part(di.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
                    di.product_name as intersected_product,
                    cast(i.status_id as smallint) as intersected_status,
                    case when di.footprint is null then null else st_area(st_intersection(di.footprint, d.footprint)) / st_area(d.footprint) * 100 end as intersection,
                    null as polarisation,
                    null as l2_product,
                    null as l2_coverage,
                    null as status_reason
                    from  d
                         join public.l1_tile_history i -- au intersectii
                            on d.id=i.downloader_history_id
                         join public.downloader_history di 
                            on di.product_name =i.tile_id
                        left outer join product p
                            on d.id=p.downloader_history_id 
                    where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
                                     WHERE sr.downloader_history_id = d.id)
                        and d.site_id =$1
                        AND d.satellite_id = 3 
                        and d.status_id = 5 --au status_id=5(processed)
                        and p.id is null-- nu se gasesc in tabela product
                        
                union

                    --produse cu status_id=5(processed) in tabela downloader_history
                    --dar care nu au intersectii in tabela l1_tile_history 
                    --fals procesate
                select 	$1 as site,
                    d.id,
                    d.orbit_id as orbit,
                    to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
                    d.product_name as acquisition,
                    d.status_description as acquisition_status,
                    null as intersection_date,
                    null as intersected_product,
                    null as intersected_status,
                    null as intersection,
                    null as polarisation,
                    null as l2_product,
                    null as l2_coverage,
                    null as status_reason
                    from  d
                         left outer join public.l1_tile_history i
                            on d.id=i.downloader_history_id
                    where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
                                     WHERE sr.downloader_history_id = d.id)
                        and d.site_id =$1
                        AND d.satellite_id = 3 
                        and d.status_id = 5 --au status_id=5(processed)
                        and i.downloader_history_id is null; --dar nu au intersectii
                END
                $BODY$;

                ALTER FUNCTION reports.sp_get_s1_statistics(smallint)
                    OWNER TO postgres;            


                DROP FUNCTION IF EXISTS sp_get_season_scheduled_processors(_season_id smallint);
                CREATE OR REPLACE FUNCTION sp_get_season_scheduled_processors(_season_id smallint)
                 RETURNS TABLE(processor_id smallint, processor_name character varying, processor_short_name character varying, required boolean)
                AS $BODY$
                begin
                    return query
                        select
                            processor.id,
                            processor.name,
                            processor.short_name,
                            processor.required
                        from processor
                        where exists(select *
                                     from scheduled_task
                                     where scheduled_task.season_id = _season_id
                                       and scheduled_task.processor_id = processor.id)
                        order by processor.short_name;
                end;
                $BODY$
                  LANGUAGE plpgsql STABLE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_season_scheduled_processors(smallint)
                  OWNER TO postgres;                



                DROP FUNCTION IF EXISTS sp_get_vegetation_statistics(integer, character varying, boolean);
                CREATE OR REPLACE FUNCTION sp_get_vegetation_statistics(_site_id integer, IN _name character varying,
                                                                       IN _exactMatch boolean DEFAULT false)
                  RETURNS TABLE(product_id integer, product_date timestamp with time zone, statistics json) AS
                $BODY$
                    BEGIN
                        if _exactMatch then 
                            RETURN QUERY 
                            with SimplifiedPrdName as (
                --				select CONCAT(substring(_name, 0, 4), '_', substring(_name, 12, 15), '_', substring(_name, 34, 11) )
                                select CONCAT((string_to_array(_name, '_'))[1], '_', (string_to_array(_name, '_'))[3], '_', (string_to_array(_name, '_'))[5], '_', (string_to_array(_name, '_'))[6])
                            )
                            SELECT product.id AS product_id, product.created_timestamp as product_date, l3_veg_stats.stats as statistics
                                                FROM l3_veg_stats inner join product on (product.id = l3_veg_stats.l2a_product_id)
                                    where l3_veg_stats.site_id = _site_id and 
                                          l3_veg_stats.simplified_l2a_name in (select * from SimplifiedPrdName);
                        else 
                            RETURN QUERY 
                            with InPrdInfos (orbit, tile, date) as (
                                select substring(_name, 35, 3)::int, 
                                        substring(_name, 40, 5),
                                        substring(_name, 12, 15)
                            )
                            SELECT product.id AS product_id, product.created_timestamp as product_date, l3_veg_stats.stats as statistics
                                                FROM l3_veg_stats inner join product on (product.id = l3_veg_stats.l2a_product_id)
                                    where l3_veg_stats.site_id = _site_id and 
                                          l3_veg_stats.tile in (select tile from InPrdInfos) and 
                                          l3_veg_stats.orbit in (select orbit from InPrdInfos) and 
                                          product.created_timestamp <= (select date::timestamptz from InPrdInfos)
                                          order by product.created_timestamp desc limit 1;
                        end if;
                    END;
                $BODY$
                LANGUAGE plpgsql VOLATILE
                COST 100
                ROWS 1000;
                ALTER FUNCTION sp_get_vegetation_statistics(integer, character varying, boolean)
                  OWNER TO admin;             


                DROP FUNCTION IF EXISTS sp_insert_default_scheduled_tasks(season.id%type, processor.id%type);
                create or replace function sp_insert_default_scheduled_tasks(
                    _season_id season.id%type,
                    _processor_id processor.id%type default null
                )
                    returns void as
                $$
                declare
                    _site_id site.id%type;
                    declare _prefix text;
                    declare _season_start_date season.start_date%type;
                    declare _season_mid_date season.start_date%type;
                begin
                    select site.id,
                           site.short_name || '_' || season.name,
                           season.start_date,
                           season.mid_date
                    into
                        _site_id,
                        _prefix,
                        _season_start_date,
                        _season_mid_date
                    from season
                             inner join site on site.id = season.site_id
                    where season.id = _season_id;

                    if not found then
                        raise exception 'Invalid season id %', _season_id;
                    end if;

                    perform sp_insert_scheduled_task(
                            name,
                            processor_id,
                            _site_id :: int,
                            _season_id :: int,
                            repeat_type,
                            repeat_after_days,
                            repeat_on_month_day,
                            first_run_time,
                            retry_seconds,
                            priority,
                            processor_params
                        )
                    from sp_evaluate_default_scheduled_tasks(_prefix, _season_start_date, _season_mid_date)
                    where _processor_id is null
                       or processor_id = _processor_id;
                end;
                $$
                    language plpgsql volatile;



                DROP FUNCTION IF EXISTS sp_insert_product(smallint, smallint, integer, smallint, integer, character varying, timestamp with time zone, character varying, character varying, geography, integer, json, smallint, integer);
                CREATE OR REPLACE FUNCTION sp_insert_product(_product_type_id smallint, _processor_id smallint, _satellite_id integer, _site_id smallint, _job_id integer,
                    _full_path character varying, _created_timestamp timestamp with time zone, _name character varying, _quicklook_image character varying, _footprint geography,
                    _orbit_id integer, _tiles json, _orbit_type_id smallint DEFAULT NULL::smallint, _downloader_history_id integer DEFAULT NULL::integer, _parent_product_ids json DEFAULT NULL::json)
                  RETURNS integer AS
                $BODY$
                DECLARE return_id product.id%TYPE;
                BEGIN
                    UPDATE product
                    SET job_id = _job_id,
                        full_path = _full_path,
                        created_timestamp = _created_timestamp,
                        quicklook_image = _quicklook_image,
                        footprint = (SELECT '(' || string_agg(REPLACE(replace(ST_AsText(geom) :: text, 'POINT', ''), ' ', ','), ',') || ')'
                                     from ST_DumpPoints(ST_Envelope(_footprint :: geometry))
                                     WHERE path[2] IN (1, 3)) :: POLYGON,
                        geog = _footprint,
                        tiles = array(select tile :: character varying from json_array_elements_text(_tiles) tile),
                        is_archived = FALSE
                    WHERE product_type_id = _product_type_id
                      AND processor_id = _processor_id
                      AND satellite_id = _satellite_id
                      AND site_id = _site_id
                      AND COALESCE(orbit_id, 0) = COALESCE(_orbit_id, 0)
                      AND "name" = _name
                    RETURNING id INTO return_id;

                    IF NOT FOUND THEN
                        INSERT INTO product(
                            product_type_id,
                            processor_id,
                            satellite_id,
                            job_id,
                            site_id,
                            full_path,
                            created_timestamp,
                            "name",
                            quicklook_image,
                            footprint,
                            geog,
                            orbit_id,
                            tiles,
                            orbit_type_id,
                            downloader_history_id
                        )
                        VALUES (
                            _product_type_id,
                            _processor_id,
                            _satellite_id,
                            _job_id,
                            _site_id,
                            _full_path,
                            COALESCE(_created_timestamp, now()),
                            _name,
                            _quicklook_image,
                            (SELECT '(' || string_agg(REPLACE(replace(ST_AsText(geom) :: text, 'POINT', ''), ' ', ','), ',') || ')'
                             from ST_DumpPoints(ST_Envelope(_footprint :: geometry))
                             WHERE path[2] IN (1, 3)) :: POLYGON,
                             _footprint,
                             _orbit_id,
                            array(select tile :: character varying from json_array_elements_text(_tiles) tile),
                            _orbit_type_id,
                            _downloader_history_id
                        )
                        RETURNING id INTO return_id;
                        
                        IF _parent_product_ids IS NOT NULL THEN
                            WITH parent_infos AS (
                                SELECT id as parent_product_id, created_timestamp as parent_product_date FROM product WHERE id IN (SELECT value::integer FROM json_array_elements_text(_parent_product_ids))
                            )
                            INSERT INTO product_provenance(product_id, parent_product_id, parent_product_date) 
                                        SELECT return_id, parent_product_id, parent_product_date from parent_infos;
                        END IF;

                        INSERT INTO event(
                            type_id,
                            data,
                            submitted_timestamp)
                            VALUES (
                            3, -- "ProductAvailable"
                            ('{"product_id":' || return_id || '}') :: json,
                            now()
                        );
                    END IF;

                    RETURN return_id;
                END;
                $BODY$
                  LANGUAGE plpgsql VOLATILE;
                ALTER FUNCTION sp_insert_product(smallint, smallint, integer, smallint, integer, character varying, timestamp with time zone, character varying, character varying, geography, integer, json, smallint, integer, _parent_product_ids json)
                  OWNER TO admin;


                DROP function IF EXISTS sp_insert_product_provenance(int, int, int);
                create or replace function sp_insert_product_provenance(
                    _product_id int,
                    _parent_product_id int,
                    _parent_product_date timestamp with time zone
                )
                returns void
                as $$
                begin
                    insert into product_provenance(product_id, parent_product_id, parent_product_date)
                    values (_product_id, _parent_product_id, _parent_product_date);
                end;
                $$
                language plpgsql volatile;


                CREATE OR REPLACE FUNCTION sp_is_fmask_preprocessing_done
                (
                    _site_id site.id%TYPE,
                    _start_date timestamp DEFAULT NULL::timestamp,
                    _end_date timestamp DEFAULT NULL::timestamp
                  )
                  RETURNS boolean AS
                $func$

                declare _fmask_enabled boolean;

                BEGIN
                    with site_config (key, site_id, value) as (
                    select
                            keys.key,
                            site.id,
                            config.value
                        from site
                        cross join (
                            values
                                ('processor.fmask.enabled'),
                                ('s2.enabled'),
                                ('l8.enabled')
                        ) as keys(key)
                        cross join lateral (
                            select
                                coalesce((
                                    select value
                                    from config
                                    where key = keys.key
                                    and config.site_id = site.id
                                ), (
                                    select value
                                    from config
                                    where key = keys.key
                                    and config.site_id is null
                                )) as value
                        ) config )
                    select value :: boolean into _fmask_enabled
                        from site_config
                        where site_config.site_id = _site_id
                        and key = 'processor.fmask.enabled';
                    

                    RETURN (not _fmask_enabled) or (select count(*) from downloader_history where 
                            status_id in (2, 5, 7) and 
                            satellite_id in (1, 2) and 
                            site_id = _site_id and 
                            (_start_date is null or product_date >= _start_date) and 
                            (_end_date is null or product_date < _end_date + interval '1 day') and 
                            id not in (select downloader_history_id from fmask_history) or
                            id in (select downloader_history_id from fmask_history where status_id = 1 or 
                                       (status_id = 2 and retry_count < 3))) = 0;
                END
                $func$ LANGUAGE plpgsql STABLE;

                CREATE OR REPLACE FUNCTION sp_is_l2_preprocessing_done
                (
                    _site_id site.id%TYPE,
                    _sat_ids json,
                    _start_date timestamp DEFAULT NULL::timestamp,
                    _end_date timestamp DEFAULT NULL::timestamp
                  )
                  RETURNS boolean AS
                $func$

                BEGIN
                   RETURN (select count(*) from downloader_history 
                            where 
                                site_id = _site_id and 
                                satellite_id IN (SELECT value::smallint FROM json_array_elements_text($2)) and 
                                (_start_date is null or product_date >= _start_date) and 
                                (_end_date is null or product_date < _end_date + interval '1 day') and
                                status_id in (1, 2, 7))  = 0    -- downloading, not processed or processing 
                            and (select count(*) from downloader_history    -- we need to have some products imported
                            where 
                                site_id = _site_id and 
                                satellite_id IN (SELECT value::smallint FROM json_array_elements_text($2))) > 0;
                END
                $func$ LANGUAGE plpgsql STABLE;            


                CREATE OR REPLACE FUNCTION sp_is_l2msk_preprocessing_done
                (
                    _site_id site.id%TYPE,
                    _start_date timestamp DEFAULT NULL::timestamp,
                    _end_date timestamp DEFAULT NULL::timestamp
                  )
                  RETURNS boolean AS
                $func$

                declare _l2a_msk_enabled boolean;

                BEGIN
                    with site_config (key, site_id, value) as (
                    select
                            keys.key,
                            site.id,
                            config.value
                        from site
                        cross join (
                            values
                                ('processor.l2a_msk.enabled')
                        ) as keys(key)
                        cross join lateral (
                            select
                                coalesce((
                                    select value
                                    from config
                                    where key = keys.key
                                    and config.site_id = site.id
                                ), (
                                    select value
                                    from config
                                    where key = keys.key
                                    and config.site_id is null
                                )) as value
                        ) config )
                    select value :: boolean into _l2a_msk_enabled
                        from site_config
                        where site_config.site_id = _site_id
                        and key = 'processor.l2a_msk.enabled';
                    
                    RETURN (
                            (_l2a_msk_enabled is NULL or NOT _l2a_msk_enabled) or 
                            (SELECT COUNT(id) FROM product WHERE product_type_id = 1 -- l2a products
                                AND site_id = _site_id
                                AND (_start_date is null or created_timestamp >= _start_date) AND 
                                (_end_date is null  or created_timestamp < _end_date + interval '1 day') AND
                                NOT EXISTS (
                                    SELECT product_id FROM product_provenance WHERE parent_product_id = id AND
                                           product_id IN (SELECT id FROM product WHERE site_id = _site_id AND product_type_id = 26))) = 0);
                    
                END
                $func$ LANGUAGE plpgsql STABLE;            


                CREATE OR REPLACE FUNCTION sp_is_l3b_preprocessing_done
                (
                    _site_id site.id%TYPE,
                    _start_date timestamp DEFAULT NULL::timestamp,
                    _end_date timestamp DEFAULT NULL::timestamp
                  )
                  RETURNS boolean AS
                $func$

                BEGIN
                    RETURN (select count(*) from sp_get_parent_products_not_in_provenance(_site_id, '[1, 26]', 3, _start_date, _end_date)) = 0;
                   
                END
                $func$ LANGUAGE plpgsql STABLE;            


                drop function if exists sp_mark_fmask_l1_tile_failed;
                create or replace function sp_mark_fmask_l1_tile_failed(
                    _downloader_history_id int,
                    _reason text,
                    _should_retry boolean
                )
                returns boolean
                as
                $$
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    update fmask_history
                    set status_id = 2, -- failed
                        status_timestamp = now(),
                        retry_count = case _should_retry
                            when true then retry_count + 1
                            else 4
                        end,
                        failed_reason = _reason
                    where (downloader_history_id) = (_downloader_history_id);

                    return true;
                end;
                $$ language plpgsql volatile;


                create or replace function sp_mark_l1_tile_failed(
                    _downloader_history_id int,
                    _tile_id text,
                    _reason text,
                    _should_retry boolean,
                    _cloud_coverage int,
                    _snow_coverage int
                )
                returns boolean
                as
                $$
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    update l1_tile_history
                    set status_id = 2, -- failed
                        status_timestamp = now(),
                        retry_count = case _should_retry
                            when true then retry_count + 1
                            else (
                                    select
                                        coalesce(
                                            (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id = (
                                                    select site_id
                                                    from downloader_history
                                                    where id = _downloader_history_id)
                                            ), (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id is null
                                            )
                                        ) :: int
                                ) + 1
                        end,
                        failed_reason = _reason,
                        cloud_coverage = _cloud_coverage,
                        snow_coverage = _snow_coverage
                    where (downloader_history_id, tile_id) = (_downloader_history_id, _tile_id);

                    return sp_update_l1_tile_status(_downloader_history_id);
                end;
                $$ language plpgsql volatile;


                DROP FUNCTION IF EXISTS reports.sp_reports_s1_statistics(smallint, integer, date, date);
                CREATE OR REPLACE FUNCTION reports.sp_reports_s1_statistics(
                    siteid smallint DEFAULT NULL::smallint,
                    orbitid integer DEFAULT NULL::integer,
                    fromdate date DEFAULT NULL::date,
                    todate date DEFAULT NULL::date)
                    RETURNS TABLE(calendar_date date, acquisitions integer, failed_to_download integer, pairs integer, processed integer, not_yet_processed integer, falsely_processed integer, no_intersections integer, errors integer, partially_processed integer) 
                    LANGUAGE 'plpgsql'
                    COST 100
                    STABLE PARALLEL UNSAFE
                    ROWS 1000

                AS $BODY$
                DECLARE startDate date;
                DECLARE endDate date;
                DECLARE temporalOffset smallint;
                DECLARE minIntersection decimal;
                                BEGIN
                                    IF $3 IS NULL THEN
                                        SELECT MIN(acquisition_date) INTO startDate FROM reports.s1_report;
                                    ELSE
                                        SELECT fromDate INTO startDate;
                                    END IF;
                                    IF $4 IS NULL THEN
                                        SELECT MAX(acquisition_date) INTO endDate FROM reports.s1_report;
                                    ELSE
                                        SELECT toDate INTO endDate;
                                    END IF;
                                    
                                    SELECT cast(value as  smallint) INTO temporalOffset FROM config where key='processor.l2s1.temporal.offset';
                                    
                                    SELECT cast(value as  decimal) INTO minIntersection FROM config where key='processor.l2s1.min.intersection';
                                
                                    RETURN QUERY
                                    WITH 	calendar AS 
                                            (SELECT date_trunc('day', dd)::date AS cdate 
                                                FROM generate_series(startDate::timestamp, endDate::timestamp, '1 day'::interval) dd),
                                       ac AS 
                                            (SELECT acquisition_date, COUNT(DISTINCT downloader_history_id) AS acquisitions 
                                                FROM reports.s1_report 
                                                WHERE ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                                GROUP BY acquisition_date 
                                                ORDER BY acquisition_date),
                                        p AS
                                            (  SELECT to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date,COUNT(*) AS pairs
                                                    FROM public.downloader_history dh
                                                        JOIN public.downloader_history i
                                                            ON dh.site_id = i.site_id 
                                                                AND dh.satellite_id = i.satellite_id 
                                                                AND dh.orbit_id = i.orbit_id
                                                                and  dh.satellite_id=3 
                                                                AND ($1 IS NULL OR dh.site_id = $1) AND ($2 IS NULL OR dh.orbit_id = $2) 
                                                    WHERE ST_INTERSECTS(dh.footprint, i.footprint)
                                                        AND DATE_PART('day', i.product_date - dh.product_date) BETWEEN (temporalOffset -1) AND (temporalOffset + 1)
                                                        AND st_area(st_intersection(dh.footprint, i.footprint)) / st_area(dh.footprint) > minIntersection
                                                        AND to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') BETWEEN startDate AND endDate
                                                GROUP BY to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD')
                                                ORDER BY to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD')
                                            ),
                                        --produse procesate: au status processed si toate procesarile pereche au status done
                                         productsWithStatusProcessed as(select downloader_history_id,count(distinct intersected_product) as nrIntersections 
                                                                            from reports.s1_report 
                                                                            WHERE status_description = 'processed' AND intersected_product IS not NULL 
                                                                                AND EXISTS ( SELECT downloader_history_id from product where product.downloader_history_id=reports.s1_report.downloader_history_id)
                                                                                AND ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                                                            group by downloader_history_id),
                                         productsWithStatusProcessed_IntersectionsWithStatusDone as
                                            (SELECT downloader_history_id FROM  productsWithStatusProcessed 
                                                    where productsWithStatusProcessed.nrIntersections = (select count(*) from l1_tile_history 
                                                                            where downloader_history_id=productsWithStatusProcessed.downloader_history_id
                                                                            and status_id=3)),
                                         proc AS 
                                             (SELECT acquisition_date, COUNT(distinct reports.s1_report.downloader_history_id) AS cnt 
                                                FROM reports.s1_report join  productsWithStatusProcessed_IntersectionsWithStatusDone
                                                    on reports.s1_report.downloader_history_id=productsWithStatusProcessed_IntersectionsWithStatusDone.downloader_history_id
                                                GROUP BY acquisition_date 
                                                ORDER BY acquisition_date   
                                             ),
                                         --produse partial procesate= produse ptr care exista procesari failed sau processing	
                                         productsWithStatusProcessed_FailledOrProcessing as
                                            (SELECT downloader_history_id FROM  productsWithStatusProcessed 
                                                    where exists (select * from l1_tile_history 
                                                                            where downloader_history_id=productsWithStatusProcessed.downloader_history_id
                                                                            and status_id in (1,2))
                                            ),
                                         partially_proc AS 
                                             (SELECT acquisition_date, COUNT(distinct reports.s1_report.downloader_history_id) AS cnt 
                                                FROM reports.s1_report join productsWithStatusProcessed_FailledOrProcessing
                                                    on reports.s1_report.downloader_history_id=productsWithStatusProcessed_FailledOrProcessing.downloader_history_id
                                                GROUP BY acquisition_date 
                                                ORDER BY acquisition_date   
                                             ),
                                             
                                        ndld AS 
                                            (SELECT acquisition_date, count(downloader_history_id) AS cnt 
                                                FROM reports.s1_report 
                                                WHERE status_description IN ('failed','aborted') AND intersected_product IS NULL AND
                                                    ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                                GROUP BY acquisition_date 
                                                ORDER BY acquisition_date),
                                             
                                        dld AS
                                            (SELECT r.acquisition_date, COUNT(r.downloader_history_id) AS cnt
                                                FROM reports.s1_report r
                                                WHERE r.status_description IN ('downloaded', 'processing') AND r.intersected_product IS NOT NULL AND
                                                    ($1 IS NULL OR r.site_id = $1) AND ($2 IS NULL OR r.orbit_id = $2) AND r.acquisition_date BETWEEN startDate AND endDate
                                                     AND NOT EXISTS (SELECT s.downloader_history_id FROM reports.s1_report s
                                                                    WHERE s.downloader_history_id = r.downloader_history_id AND r.l2_product LIKE '%COHE%')
                                                GROUP BY acquisition_date
                                                ORDER BY acquisition_date),
                                             
                                        fproc AS 
                                            (SELECT acquisition_date, COUNT(distinct downloader_history_id) AS cnt 
                                                FROM reports.s1_report 
                                                WHERE status_description = 'processed'
                                                    AND (intersected_product IS NULL OR NOT EXISTS( SELECT downloader_history_id from product where product.downloader_history_id=reports.s1_report.downloader_history_id))
                                                    AND ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                                GROUP BY acquisition_date 
                                                ORDER BY acquisition_date),
                                         downh AS
                                            (  SELECT to_date(substr(split_part(dh.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date,*
                                                    FROM public.downloader_history dh
                                                        where ($1 IS NULL OR dh.site_id = $1) AND ($2 IS NULL OR dh.orbit_id = $2)	AND dh.satellite_id=3 
                                            ),		
                                        ni AS
                                            (  SELECT acquisition_date,COUNT(*) AS cnt
                                                    FROM downh dh
                                                        LEFT OUTER JOIN public.downloader_history i
                                                            ON dh.site_id = i.site_id 
                                                                AND dh.satellite_id = i.satellite_id 
                                                                AND dh.orbit_id = i.orbit_id
                                                                AND ST_INTERSECTS(dh.footprint, i.footprint)
                                                                AND DATE_PART('day', i.product_date - dh.product_date) BETWEEN (temporalOffset - 1) AND (temporalOffset + 1)
                                                                AND st_area(st_intersection(dh.footprint, i.footprint)) / st_area(dh.footprint) > minIntersection
                                                                AND acquisition_date BETWEEN startDate AND endDate
                                                    where i.id is NULL
                                                GROUP BY acquisition_date
                                                ORDER BY acquisition_date
                                            ),	
                                             
                                        --errors: produse cu status processing_failed si cu toate procesarile pereche failed
                                         productsWithStatusFailed as(select downloader_history_id,count(distinct intersected_product) as nrIntersections 
                                                                         from reports.s1_report 
                                                                         where status_description='processing_failed'
                                                                             AND ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                                                         group by downloader_history_id),
                                        productsWithStatusFailed_IntersectionsWithStatusFailed AS (select * 
                                            from productsWithStatusFailed 
                                                    where productsWithStatusFailed.nrIntersections = (select count(*) from l1_tile_history 
                                                                            where downloader_history_id=productsWithStatusFailed.downloader_history_id
                                                                            and status_id=2)),							 
                                        e AS 
                                            (SELECT acquisition_date, COUNT(distinct reports.s1_report.downloader_history_id) AS cnt 
                                                FROM reports.s1_report join  productsWithStatusFailed_IntersectionsWithStatusFailed
                                                    on reports.s1_report.downloader_history_id=productsWithStatusFailed_IntersectionsWithStatusFailed.downloader_history_id
                                                GROUP BY acquisition_date 
                                                ORDER BY acquisition_date)
                                    SELECT 	c.cdate, 
                                        COALESCE(ac.acquisitions, 0)::integer,
                                        COALESCE(ndld.cnt, 0)::integer,
                                        COALESCE(p.pairs, 0)::integer, 
                                        COALESCE(proc.cnt, 0)::integer, 
                                        COALESCE(dld.cnt, 0)::integer,
                                        COALESCE(fproc.cnt, 0)::integer,
                                        COALESCE(ni.cnt, 0)::integer,
                                        COALESCE(e.cnt, 0)::integer,
                                        COALESCE(partially_proc.cnt, 0)::integer
                                    FROM calendar c
                                        LEFT JOIN ac ON ac.acquisition_date = c.cdate
                                        LEFT JOIN ndld ON ndld.acquisition_date = c.cdate
                                        LEFT JOIN p ON p.acquisition_date = c.cdate
                                        LEFT JOIN proc ON proc.acquisition_date = c.cdate
                                        LEFT JOIN dld ON dld.acquisition_date = c.cdate
                                        LEFT JOIN fproc ON fproc.acquisition_date = c.cdate
                                        LEFT JOIN ni ON ni.acquisition_date = c.cdate
                                        LEFT JOIN e ON e.acquisition_date = c.cdate
                                        LEFT JOIN partially_proc ON partially_proc.acquisition_date = c.cdate;
                                END
                $BODY$;

                ALTER FUNCTION reports.sp_reports_s1_statistics(smallint, integer, date, date)
                    OWNER TO admin;            
                
                --
				DROP FUNCTION IF EXISTS sp_set_user_password(character varying, character varying, text);
                CREATE OR REPLACE FUNCTION sp_set_user_password(
                    IN user_name character varying,
                    IN email character varying,
                    IN pwd text	
                )RETURNS integer AS
                    $BODY$
                    DECLARE user_id smallint;

                    BEGIN 
                        SELECT id into user_id FROM "user" WHERE "user".login = $1 AND "user".email = $2;

                        IF user_id IS NOT NULL THEN
                            IF char_length(trim(pwd))>0 THEN

                                UPDATE "user"
                                     SET password = crypt($3, gen_salt('md5'))
                                     WHERE id = user_id ;--AND password = crypt(user_pwd, password);
                                RETURN 1;
                            ELSE 
                                RETURN 0;
                            END IF;
                        ELSE RETURN 2;
                        END IF;

                    END;
                    $BODY$
                    LANGUAGE plpgsql VOLATILE;



                drop function if exists sp_start_fmask_l1_tile_processing;
                create or replace function sp_start_fmask_l1_tile_processing(
                    _node_id text
                )
                returns table (
                    site_id int,
                    satellite_id smallint,
                    downloader_history_id int,
                    path text,
                    orbit_id int,
                    tile_id text) as
                $$
                declare _satellite_id smallint;
                declare _downloader_history_id int;
                declare _path text;
                declare _site_id int;
                declare _orbit_id int;
                declare _tile_id text;
                declare _product_date timestamp;
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    create temporary table if not exists site_config(
                        key,
                        site_id,
                        value
                    ) as
                    select
                        keys.key,
                        site.id,
                        config.value
                    from site
                    cross join (
                        values
                            ('processor.fmask.enabled'),
                            ('s2.enabled'),
                            ('l8.enabled')
                    ) as keys(key)
                    cross join lateral (
                        select
                            coalesce((
                                select value
                                from config
                                where key = keys.key
                                and config.site_id = site.id
                            ), (
                                select value
                                from config
                                where key = keys.key
                                and config.site_id is null
                            )) as value
                    ) config;

                    select fmask_history.satellite_id,
                           fmask_history.downloader_history_id
                    into _satellite_id,
                         _downloader_history_id
                    from fmask_history
                    where status_id = 2 -- failed
                      and retry_count < 3
                      and status_timestamp < now() - interval '1 day'
                    order by status_timestamp
                    limit 1;

                    if found then
                        select downloader_history.product_date,
                               downloader_history.full_path,
                               downloader_history.site_id
                        into _product_date,
                             _path,
                             _site_id
                        from downloader_history
                        where id = _downloader_history_id;

                        update fmask_history
                        set status_id = 1, -- processing
                            status_timestamp = now(),
                            node_id = _node_id
                        where (fmask_history.downloader_history_id) = (_downloader_history_id);
                    else
                        select distinct
                            downloader_history.satellite_id,
                            downloader_history.id,
                            downloader_history.product_date,
                            downloader_history.full_path,
                            downloader_history.site_id,
                            downloader_history.orbit_id,
                            downloader_history.tiles[1]
                        into _satellite_id,
                            _downloader_history_id,
                            _product_date,
                            _path,
                            _site_id,
                            _orbit_id,
                            _tile_id            
                        from downloader_history
                        inner join site on site.id = downloader_history.site_id
                        cross join lateral (
                            select
                                (
                                    select value :: boolean as fmask_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'processor.fmask.enabled'
                                ),
                                (
                                    select value :: boolean as s2_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 's2.enabled'
                                ),
                                (
                                    select value :: boolean as l8_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'l8.enabled'
                                )
                        ) config
                        where not exists (
                            select *
                            from fmask_history
                            where fmask_history.downloader_history_id = downloader_history.id
                        )
                        and downloader_history.status_id in (2, 5, 7) -- downloaded, processing
                        and site.enabled
                        and fmask_enabled
                        and case downloader_history.satellite_id
                            when 1 then config.s2_enabled
                            when 2 then config.l8_enabled
                            else false
                        end
                        order by satellite_id, product_date
                        limit 1;

                        if found then
                            insert into fmask_history (
                                satellite_id,
                                downloader_history_id,
                                status_id,
                                node_id
                            ) values (
                                _satellite_id,
                                _downloader_history_id,
                                1, -- processing
                                _node_id
                            );
                        end if;
                    end if;

                    if _downloader_history_id is not null then
                        return query
                            select _site_id,
                                _satellite_id,
                                _downloader_history_id,
                                _path,
                                _orbit_id,
                                _tile_id;
                    end if;
                end;
                $$ language plpgsql volatile;
                
                --
                drop function if exists sp_start_l1_tile_processing;
                create or replace function sp_start_l1_tile_processing(
                    _node_id text
                )
                returns table (
                    site_id int,
                    satellite_id smallint,
                    orbit_id int,
                    tile_id text,
                    downloader_history_id int,
                    path text,
                    prev_l2a_path text
                ) as
                $$
                declare _satellite_id smallint;
                declare _orbit_id int;
                declare _tile_id text;
                declare _downloader_history_id int;
                declare _path text;
                declare _prev_l2a_path text;
                declare _site_id int;
                declare _product_date timestamp;
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    create temporary table if not exists site_config(
                        key,
                        site_id,
                        value
                    ) as
                    select
                        keys.key,
                        site.id,
                        config.value
                    from site
                    cross join (
                        values
                            ('processor.l2a.s2.implementation'),
                            ('processor.l2a.optical.retry-interval'),
                            ('processor.l2a.optical.max-retries'),
                            ('processor.l2a.optical.num-workers'),
                            ('s2.enabled'),
                            ('l8.enabled')
                    ) as keys(key)
                    cross join lateral (
                        select
                            coalesce((
                                select value
                                from config
                                where key = keys.key
                                and config.site_id = site.id
                            ), (
                                select value
                                from config
                                where key = keys.key
                                and config.site_id is null
                            )) as value
                    ) config;

                    select l1_tile_history.satellite_id,
                           l1_tile_history.orbit_id,
                           l1_tile_history.tile_id,
                           l1_tile_history.downloader_history_id
                    into _satellite_id,
                         _orbit_id,
                         _tile_id,
                         _downloader_history_id
                    from l1_tile_history
                    inner join downloader_history on downloader_history.id = l1_tile_history.downloader_history_id
                    inner join site on site.id = downloader_history.site_id
                    cross join lateral (
                        select
                            (
                                select value :: int as max_retries
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 'processor.l2a.optical.max-retries'
                            ),
                            (
                                select value :: interval as retry_interval
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 'processor.l2a.optical.retry-interval'
                            ),
                            (
                                select value :: boolean as s2_enabled
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 's2.enabled'
                            ),
                            (
                                select value :: boolean as l8_enabled
                                from site_config
                                where site_config.site_id = downloader_history.site_id
                                  and key = 'l8.enabled'
                            )
                    ) config
                    where l1_tile_history.status_id = 2 -- failed
                      and l1_tile_history.retry_count < config.max_retries
                      and l1_tile_history.status_timestamp < now() - config.retry_interval
                      and case downloader_history.satellite_id
                              when 1 then config.s2_enabled
                              when 2 then config.l8_enabled
                              else false
                      end
                      and (
                          site.enabled
                          or exists (
                              select *
                              from downloader_history
                              where downloader_history.status_id = 2 -- downloaded
                                and l1_tile_history.tile_id = any(downloader_history.tiles)
                                and l1_tile_history.orbit_id = downloader_history.orbit_id
                                and exists (
                                    select *
                                    from site
                                    where site.id = downloader_history.site_id
                                      and site.enabled
                                )
                          )
                      )
                    order by l1_tile_history.status_timestamp
                    limit 1;

                    if found then
                        select downloader_history.product_date,
                               downloader_history.full_path,
                               downloader_history.site_id
                        into _product_date,
                             _path,
                             _site_id
                        from downloader_history
                        where id = _downloader_history_id;

                        update l1_tile_history
                        set status_id = 1, -- processing
                            status_timestamp = now(),
                            node_id = _node_id
                        where (l1_tile_history.downloader_history_id, l1_tile_history.tile_id) = (_downloader_history_id, _tile_id);
                    else
                        select distinct
                            downloader_history.satellite_id,
                            downloader_history.orbit_id,
                            tile_ids.tile_id,
                            downloader_history.id,
                            downloader_history.product_date,
                            downloader_history.full_path,
                            downloader_history.site_id
                        into _satellite_id,
                            _orbit_id,
                            _tile_id,
                            _downloader_history_id,
                            _product_date,
                            _path,
                            _site_id
                        from downloader_history
                        inner join site on site.id = downloader_history.site_id
                        cross join lateral (
                                select unnest(tiles) as tile_id
                            ) tile_ids
                        cross join lateral (
                            select
                                (
                                    select value as l2a_implementation
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'processor.l2a.s2.implementation'
                                ),
                                (
                                    select value :: int as max_retries
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'processor.l2a.optical.max-retries'
                                ),
                                (
                                    select value :: boolean as s2_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 's2.enabled'
                                ),
                                (
                                    select value :: boolean as l8_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'l8.enabled'
                                )
                        ) config
                        where (
                            config.l2a_implementation = 'sen2cor'
                            and downloader_history.satellite_id = 1
                            or not exists (
                                select *
                                from l1_tile_history
                                where (l1_tile_history.satellite_id,
                                    l1_tile_history.orbit_id,
                                    l1_tile_history.tile_id) =
                                    (downloader_history.satellite_id,
                                    downloader_history.orbit_id,
                                    tile_ids.tile_id)
                                and (status_id = 1 or -- processing
                                    retry_count < config.max_retries and status_id = 2 -- failed
                                )
                            )
                        )
                        and not exists (
                            select *
                            from l1_tile_history
                            where (l1_tile_history.downloader_history_id, l1_tile_history.tile_id) = (downloader_history.id, tile_ids.tile_id)
                        )
                        and downloader_history.status_id in (2, 7) -- downloaded, processing
                        and site.enabled
                        and downloader_history.satellite_id in (1, 2) -- sentinel2, landsat8
                        and case downloader_history.satellite_id
                                when 1 then config.s2_enabled
                                when 2 then config.l8_enabled
                                else false
                        end
                        order by satellite_id,
                                orbit_id,
                                tile_id,
                                product_date
                        limit 1;

                        if found then
                            insert into l1_tile_history (
                                satellite_id,
                                orbit_id,
                                tile_id,
                                downloader_history_id,
                                status_id,
                                node_id
                            ) values (
                                _satellite_id,
                                _orbit_id,
                                _tile_id,
                                _downloader_history_id,
                                1, -- processing
                                _node_id
                            );

                            update downloader_history
                            set status_id = 7 -- processing
                            where id = _downloader_history_id;
                        end if;
                    end if;

                    if _downloader_history_id is not null then
                        select product.full_path
                        into _prev_l2a_path
                        from product
                        where product.site_id = _site_id
                          and product.product_type_id = 1 -- l2a
                          and product.satellite_id = _satellite_id
                          and product.created_timestamp < _product_date
                          and product.tiles :: text[] @> array[_tile_id]
                          and (product.satellite_id <> 1 -- sentinel2
                               or product.orbit_id = _orbit_id)
                        order by created_timestamp desc
                        limit 1;

                        return query
                            select _site_id,
                                _satellite_id,
                                _orbit_id,
                                _tile_id,
                                _downloader_history_id,
                                _path,
                                _prev_l2a_path;
                    end if;
                end;
                $$ language plpgsql volatile;
                
                --
                create or replace function sp_update_l1_tile_status(
                    _downloader_history_id int
                )
                returns boolean
                as
                $$
                begin
                    if not exists(
                        select unnest(tiles)
                        from downloader_history
                        where id = _downloader_history_id
                        except all
                        select tile_id
                        from l1_tile_history
                        where downloader_history_id = _downloader_history_id
                          and (l1_tile_history.status_id = 3 -- done
                            or l1_tile_history.status_id = 2 -- failed
                                and l1_tile_history.retry_count >= (
                                    select
                                        coalesce(
                                            (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id = (
                                                    select site_id
                                                    from downloader_history
                                                    where id = _downloader_history_id)
                                            ), (
                                                select value
                                                from config
                                                where key = 'processor.l2a.optical.max-retries'
                                                and site_id is null
                                            )
                                        ) :: int
                                )
                          )
                    ) then
                        if exists(
                            select *
                            from l1_tile_history
                            where downloader_history_id = _downloader_history_id
                              and status_id = 3 -- done
                        ) then
                            update downloader_history
                            set status_id = 5 -- processed
                            where id = _downloader_history_id;
                        else
                            update downloader_history
                            set status_id = 6 -- processing_failed
                            where id = _downloader_history_id;
                        end if;
                        return true;
                    else
                        return false;
                    end if;
                end;
                $$ language plpgsql volatile;
            $str$;
            raise notice '%', _statement;
            execute _statement;
                    
            _statement := $str$
                ALTER TABLE default_scheduled_tasks DROP CONSTRAINT IF EXISTS default_scheduled_tasks_processor_id_fkey;
                ALTER TABLE default_scheduled_tasks
                    ADD CONSTRAINT default_scheduled_tasks_processor_id_fkey FOREIGN KEY (processor_id) REFERENCES processor (id);

                ALTER TABLE downloader_history DROP CONSTRAINT IF EXISTS downloader_history_product_type_id_fkey;    
                alter table downloader_history 
                    add constraint downloader_history_product_type_id_fkey foreign key (product_type_id) references product_type (id);
                    
                ALTER TABLE log DROP CONSTRAINT IF EXISTS fk_log_component;        
                alter table log
                    add constraint fk_log_component foreign key(component_id) references component(id);
    
                ALTER TABLE log DROP CONSTRAINT IF EXISTS fk_log_severity;            
                alter table log
                    add constraint fk_log_severity foreign key(severity) references severity(id);  

                ALTER TABLE service_processors DROP CONSTRAINT IF EXISTS fk_service_id;
                ALTER TABLE service_processors DROP CONSTRAINT IF EXISTS fk_service_processor_id;
                ALTER TABLE public.service DROP CONSTRAINT IF EXISTS fk_service_site;
                
                ALTER TABLE service_processors ADD CONSTRAINT fk_service_id FOREIGN KEY (service_id) REFERENCES public.service(id);
                ALTER TABLE service_processors ADD CONSTRAINT fk_service_processor_id FOREIGN KEY (processor_id) REFERENCES public.processor(id);
                ALTER TABLE ONLY public.service ADD CONSTRAINT fk_service_site FOREIGN KEY (site_id) REFERENCES public.site(id);

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
            
                CREATE TRIGGER tr_season_delete
                    BEFORE DELETE ON season
                    FOR EACH ROW
                    EXECUTE PROCEDURE delete_season_descriptors();    

                CREATE TRIGGER tr_season_insert
                    AFTER INSERT
                    ON public.season
                    FOR EACH ROW
                    EXECUTE FUNCTION public.insert_season_descriptors();
            $str$;
            raise notice '%', _statement;
            execute _statement;

            -- FMask Upgrades
            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES (24, 'FMask','fmask', 'FMask', true, '{1,2}', null, false, false, false, false) on conflict DO nothing;
                
                INSERT INTO product_type (id, name, description, is_raster) VALUES (25, 'fmask','Fmask mask product', true) on conflict DO nothing;

                INSERT INTO config_category VALUES (31, 'FMask', 18, true) on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.extractor_image', NULL, 'sen4x/fmask_extractor:0.1.2', '2021-03-18 14:43:00.720811+00') 
                            on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/fmask_extractor:0.1.2';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.3.1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.image', NULL, 'sen4x/fmask:4.4-ubuntu-20.04', '2021-03-18 14:43:00.720811+00') 
                            on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/fmask:4.4-ubuntu-20.04';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.cog-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.compress-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud-shadow', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.snow', NULL, '0', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.enabled', NULL, 'false', '2021-02-10 15:58:31.878939+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.max-retries', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.num-workers', NULL, '2', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.output-path', NULL, '/mnt/archive/fmask_def/{site}/fmask/', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.retry-interval', NULL, '1 minute', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold', NULL, '20', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.l8', NULL, '17.5', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.s2', NULL, '20', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.working-dir', NULL, '/mnt/archive/fmask_tmp/', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.fmask.enabled', 'Controls whether to run Fmask on optical products', 'bool', false, 31, FALSE, 'Controls whether to run Fmask on optical products', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.extractor_image', 'FMask extractor docker image name', 'string', false, 31, FALSE, 'FMask extractor docker image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.gdal_image', 'gdal docker image for FMask', 'string', false, 31, FALSE, 'gdal docker image for FMask', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.image', 'FMask docker image', 'string', false, 31, FALSE, 'FMask docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.cog-tiffs', 'Output rasters as COG', 'bool', false, 31, FALSE, 'Output rasters as COG', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.compress-tiffs', 'Compress output rasters', 'bool', false, 31, FALSE, 'Compress output rasters', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.cloud-shadow', 'Cloud shaddow dilation percent', 'int', false, 31, FALSE, 'Cloud shaddow dilation percent', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.cloud', 'Cloud dilation percent', 'int', false, 31, FALSE, 'Cloud dilation percent', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.snow', 'Snow dilation percent', 'int', false, 31, FALSE, 'Snow dilation percent', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.max-retries', 'Maximum number of retries for a product', 'int', false, 31, FALSE, 'Maximum number of retries for a product', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.num-workers', 'Number of workers', 'int', false, 31, FALSE, 'Number of workers', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.output-path', 'Output path', 'string', false, 31, FALSE, 'Output path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.retry-interval', 'Retry interval', 'int', false, 31, FALSE, 'Retry interval', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold.l8', 'Threshold for L8', 'int', false, 31, FALSE, 'Threshold for L8', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold.s2', 'Threshold for S2', 'int', false, 31, FALSE, 'Threshold for S2', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold', 'Global threshold', 'int', false, 31, FALSE, 'Global threshold', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.fmask.working-dir', 'Working directory', 'string', false, 31, FALSE, 'Working directory', NULL) ON conflict(key) DO UPDATE SET type = 'string';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                       (15, 'Validity flags','l2a_msk', 'Validity flags', true, '{1,2}', null, false, false, false, false) on conflict DO nothing;
                INSERT INTO product_type (id, name, description, is_raster) VALUES (26, 'l2a_msk','L2A product with validity mask', true) on conflict DO nothing;
                INSERT INTO config_category VALUES (27, 'Validity Flags', 17, true) on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l2a_msk', NULL, '/mnt/archive/orchestrator_temp/l2a_msk/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l2a_msk.slurm_qos', NULL, 'qosvaliditymsk', '2015-08-24 17:44:38.29255+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.enabled', NULL, 'false', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.compress', NULL, 'true', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.cog', NULL, 'false', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('general.scratch-path.l2a_msk', 'Path for Masked L2A temporary files', 'string', false, 27, FALSE, 'Path for Masked L2A temporary files', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.enabled', 'Enable or disable the validity flags', 'bool', false, 27, FALSE, 'Enable or disable the validity flags', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.l2a_msk.slurm_qos', 'Slurm QOS for validity masks', 'string', true, 8) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.compress', 'Compress output flags', 'bool', false, 27, FALSE, 'Compress output flags', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.cog', 'Produce output flags as COG', 'bool', false, 27, FALSE, 'Produce output flags as COG', NULL) on conflict DO nothing;

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config_category VALUES (8, 'Executor', 8, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (12, 'Dashboard', 9, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (13, 'Monitoring Agent', 10, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (14, 'Resources', 11, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (17, 'Site', 14, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config_category VALUES (32, 'T-Rex Updater', 32, false) ON conflict DO NOTHING;
                
                INSERT INTO processor (id, name, short_name, label) VALUES (21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater') ON conflict DO NOTHING;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.trex-updater', NULL, 't-rex-genconfig.py', '2021-10-11 22:39:08.407059+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.trex.slurm_qos', NULL, 'qostrex', '2021-10-11 17:44:38.29255+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.trex-updater.use_docker', NULL, '1', '2021-02-19 14:43:00.720811+00') on conflict DO nothing; 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.trex-updater.docker_image', NULL, 'sen4cap/data-preparation:0.1', '2021-02-19 14:43:00.720811+00') on conflict DO nothing; 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.trex-updater.docker_add_mounts', NULL, '/var/run/docker.sock:/var/run/docker.sock,/var/lib/t-rex:/var/lib/t-rex', '2021-02-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.trex.t-rex-container', NULL, 'docker_t-rex_1', '2021-10-11 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.trex.t-rex-output-file', NULL, '/var/lib/t-rex/t-rex.toml', '2021-10-11 11:09:43.978921+02') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('executor.module.path.trex-updater', 'T-Rex script', 'string', false, 32, FALSE, 'T-Rex script', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.trex.slurm_qos', 'Slurm QOS for TRex', 'string', true, 8, FALSE, 'Slurm QOS for TRex', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.trex-updater.use_docker', 'T-Rex use docker', 'int', false, 32, FALSE, 'T-Rex use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.trex-updater.docker_image', 'T-Rex docker image', 'string', false, 32, FALSE, 'T-Rex docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.trex-updater.docker_add_mounts', 'T-Rex container additional mounts', 'string', false, 32, FALSE, 'T-Rex container additional mounts', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.trex.t-rex-container', 'T-Rex container name', 'string', false, 32, FALSE, 'T-Rex container name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.trex.t-rex-output-file', 'T-Rex output file', 'string', false, 32, FALSE, 'T-Rex output file', NULL) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$                        
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_fapar', 'L3B processor will produce FAPAR', 'int', TRUE, 4, TRUE, 'Produce FAPAR', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce FAPAR', label = 'Produce FAPAR', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_fcover', 'L3B processor will produce FCOVER', 'int', TRUE, 4, TRUE, 'Produce FCOVER', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce FCOVER', label = 'Produce FCOVER', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_lai', 'L3B processor will produce LAI', 'int', TRUE, 4, TRUE, 'Produce LAI', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce LAI', label = 'Produce LAI', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_ndvi', 'L3B processor will produce NDVI', 'int', TRUE, 4, TRUE, 'Produce NDVI', NULL) ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce NDVI', label = 'Produce NDVI', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                
                
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_ndwi', 'L3B processor will produce NDWI', 'int', TRUE, 4, TRUE, 'Produce NDWI', NULL)  ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce NDWI', label = 'Produce NDWI', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.filter.produce_brightness', 'L3B processor will produce brightness', 'int', TRUE, 4, TRUE, 'Produce brightness', NULL)  ON conflict(key) DO UPDATE SET friendly_name = 'L3B processor will produce brightness', label = 'Produce brightness', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                INSERT INTO config_metadata (key, friendly_name, type, is_advanced, config_category_id, is_site_visible, label, values) VALUES ('processor.l3b.cloud_optimized_geotiff_output', 'Generate L3B Cloud Optimized Geotiff outputs', 'int', TRUE, 4, TRUE, 'Generate L3B Cloud Optimized Geotiff outputs', NULL)  ON conflict(key) DO UPDATE SET friendly_name = 'Generate L3B Cloud Optimized Geotiff outputs', label = 'Generate L3B Cloud Optimized Geotiff outputs', is_site_visible = TRUE, type = 'int', is_advanced = TRUE;
                
                -- UPDATE config SET value = 1 WHERE key in ('processor.l3b.filter.produce_fapar', 'processor.l3b.filter.produce_fcover', 'processor.l3b.filter.produce_lai', 'processor.l3b.filter.produce_ndvi', 'processor.l3b.filter.produce_ndwi', 'processor.l3b.filter.produce_brightness', 'processor.l3b.cloud_optimized_geotiff_output') and value = 'true';
                -- UPDATE config SET value = 0 WHERE key in ('processor.l3b.filter.produce_fapar', 'processor.l3b.filter.produce_fcover', 'processor.l3b.filter.produce_lai', 'processor.l3b.filter.produce_ndvi', 'processor.l3b.filter.produce_ndwi', 'processor.l3b.filter.produce_brightness', 'processor.l3b.cloud_optimized_geotiff_output') and value = 'false';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value) VALUES ('downloader.l8.query.days.back', NULL, '5') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value) VALUES ('downloader.s1.query.days.back', NULL, '5') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value) VALUES ('downloader.s2.query.days.back', NULL, '5') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value) VALUES ('downloader.query.timeout', NULL, '90') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '90';
                
                INSERT INTO config(key, site_id, value) VALUES ('executor.http-server.listen-ip', NULL, '127.0.0.1') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('executor.http-server.listen-ip', NULL, '8084') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                
                INSERT INTO config(key, site_id, value) VALUES ('executor.module.path.export-product-launcher', NULL, '/usr/bin/export-product-launcher.py') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                
                INSERT INTO config(key, site_id, value) VALUES ('executor.resource-manager.name', NULL, 'slurm') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                
                INSERT INTO config(key, site_id, value) VALUES ('general.inter-proc-com-type', NULL, 'http') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('general.orchestrator.docker_add_mounts', NULL, '') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.2.0') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('general.orchestrator.use_docker', NULL, '1') on conflict (key, COALESCE(site_id, -1)) DO nothing;

                INSERT INTO config(key, site_id, value) VALUES ('processor.l3a.synthesis_date', NULL, '') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l3a.synthesis_date', 'Synthesis date [YYYYMMDD]', 'string', false, 3, true, 'Synthesis date', NULL, true) on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value) VALUES ('processor.l3b.filter.chain_inputs_steps', NULL, false) on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('processor.l3b.filter.produce_brightness', NULL, false) on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('processor.l3b.filter.produce_ndwi', NULL, false) on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('processor.l3b.produce_mosaic', NULL, false) on conflict (key, COALESCE(site_id, -1)) DO nothing;
                
                INSERT INTO config(key, site_id, value) VALUES ('processor.l4a.reference_data_source', NULL, 'insitu') on conflict (key, COALESCE(site_id, -1)) DO nothing;
                INSERT INTO config(key, site_id, value) VALUES ('processor.l4b.reference_data_source', NULL, 'insitu') on conflict (key, COALESCE(site_id, -1)) DO nothing;

                INSERT INTO config_metadata VALUES ('processor.l3b.filter.chain_inputs_steps', 'Chain L3B products', 'bool', TRUE, 4, TRUE, 'Chain L3B products', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l4a.reference_data_source', 'Reference data source', 'string', true, 5, true, 'Reference data source', '{ "allowed_values": [{ "value": "insitu", "display": "Insitu data" }, { "value": "reference_map", "display": "Reference Map (non supervised)" }, { "value": "earthsignature", "display": "EarthSignature" }] }', true) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l4b.reference_data_source', 'Reference data source', 'string', true, 6, true, 'Reference data source', '{ "allowed_values": [{ "value": "insitu", "display": "Insitu data" }, { "value": "earthsignature", "display": "EarthSignature" }] }', true) on conflict DO nothing;
                
                DELETE FROM config WHERE key IN ('s1.preprocessing.enabled', 's1.preprocessing.path');

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
                INSERT INTO product_type (id, name, description, is_raster) VALUES (2, 's2a_l3a','Sen2Agri L3A Composite product', true) ON CONFLICT (id) DO UPDATE SET name = 's2a_l3a', description = 'Sen2Agri L3A Composite product';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (3, 'l3b','L3B product', true) ON CONFLICT (id) DO UPDATE SET name = 'l3b', description = 'L3B product';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (4, 's2a_l3e','Sen2Agri L3E Pheno NDVI product', true) ON CONFLICT (id) DO UPDATE SET name = 's2a_l3e', description = 'Sen2Agri L3E Pheno NDVI product';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (5, 's2a_l4a','Sen2Agri L4A Crop mask product', true) ON CONFLICT (id) DO UPDATE SET name = 's2a_l4a', description = 'Sen2Agri L4A Crop mask product';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (6, 's2a_l4b','Sen2Agri L4B Crop type product', true) ON CONFLICT (id) DO UPDATE SET name = 's2a_l4b', description = 'Sen2Agri L4B Crop type product';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (8, 's2a_l3c','Sen2Agri L3C LAI Reprocessed product', true) ON CONFLICT (id) DO UPDATE SET name = 's2a_l3c', description = 'Sen2Agri L3C LAI Reprocessed product';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (9, 's2a_l3d','Sen2Agri L3D LAI End of Season product', true) ON CONFLICT (id) DO UPDATE SET name = 's2a_l3d', description = 'Sen2Agri L3D LAI End of Season product';
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
                DELETE FROM config WHERE key in ('demmaccs.cog-tiffs', 'demmaccs.compress-tiffs', 'demmaccs.gips-path', 'demmaccs.maccs-launcher', 'demmaccs.output-path', 'demmaccs.remove-fre', 'demmaccs.remove-sre', 'demmaccs.srtm-path', 'demmaccs.swbd-path', 'demmaccs.working-dir');
                DELETE FROM config_metadata WHERE key in ('demmaccs.cog-tiffs', 'demmaccs.compress-tiffs', 'demmaccs.gips-path', 'demmaccs.maccs-launcher', 'demmaccs.output-path', 'demmaccs.remove-fre', 'demmaccs.remove-sre', 'demmaccs.srtm-path', 'demmaccs.swbd-path', 'demmaccs.working-dir');
                
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja.gipp-path', NULL, '/mnt/archive/gipp/maja') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/gipp/maja';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja.remove-fre', NULL, '0') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja.remove-sre', NULL, '1') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '1';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.cog-tiffs', NULL, '0')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.compress-tiffs', NULL, '0')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.max-retries', NULL, '3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '3';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.num-workers', NULL, '4') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '4';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.output-path', NULL, '/mnt/archive/maccs_def/{site}/{processor}/')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/maccs_def/{site}/{processor}/';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.optical.retry-interval', NULL, '1 day') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '1 day';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.s2.implementation', NULL, 'maja') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'maja';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.sen2cor.gipp-path', NULL, '/mnt/archive/gipp/sen2cor') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/gipp/sen2cor';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.srtm-path', NULL, '/mnt/archive/srtm') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/srtm';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.swbd-path', NULL, '/mnt/archive/swbd') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/swbd';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.working-dir', NULL, '/mnt/archive/demmaccs_tmp/') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/demmaccs_tmp/';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.processors_image', NULL, 'sen4x/l2a-processors:0.2.3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-processors:0.2.3';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.sen2cor_image', NULL, 'sen4x/sen2cor:2.10.01-ubuntu-20.04') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/sen2cor:2.10.01-ubuntu-20.04';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja_image', NULL, 'sen4x/maja:4.5.4-centos-7') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/maja:4.5.4-centos-7';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.4.1') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'osgeo/gdal:ubuntu-full-3.4.1';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.l8_align_image', NULL, 'sen4x/l2a-l8-alignment:0.1.2') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-l8-alignment:0.1.2';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.dem_image', NULL, 'sen4x/l2a-dem:0.1.3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-dem:0.1.3';
                
                INSERT INTO config_metadata VALUES ('executor.processor.l2a.name', 'L2A Processor Name', 'string', true, 8, FALSE, 'L2A Processor Name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.l2a.path', 'L2A Processor Path', 'file', false, 8, FALSE, 'L2A Processor Path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.gipp-path', 'MAJA GIPP path', 'directory', false, 2, FALSE, 'MAJA GIPP path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-fre', 'Remove FRE files from resulted L2A product', 'bool', false, 2, FALSE, 'Remove FRE files from resulted L2A product', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-sre', 'Remove SRE files from resulted L2A product', 'bool', false, 2, FALSE, 'Remove SRE files from resulted L2A product', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.cog-tiffs', 'Produce L2A tiff files as Cloud Optimized Geotiff', 'bool', false, 2, FALSE, 'Produce L2A tiff files as Cloud Optimized Geotiff', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.compress-tiffs', 'Compress the resulted L2A TIFF files', 'bool', false, 2, FALSE, 'Compress the resulted L2A TIFF files', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.max-retries', 'Number of retries for the L2A processor', 'int', false, 2, FALSE, 'Number of retries for the L2A processor', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.num-workers', 'Parallelism degree of the L2A processor', 'int', false, 2, FALSE, 'Parallelism degree of the L2A processor', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.output-path', 'path for L2A products', 'directory', false, 2, FALSE, 'path for L2A products', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.optical.retry-interval', 'Retry interval for the L2A processor', 'string', false, 2, FALSE, 'Retry interval for the L2A processor', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2, false, 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)' , '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }') on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.sen2cor.gipp-path', 'Sen2Cor GIPP path', 'directory', false, 2, FALSE, 'Sen2Cor GIPP path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.srtm-path', 'Path to the DEM dataset', 'directory', false, 2, FALSE, 'Path to the DEM dataset', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.swbd-path', 'Path to the SWBD dataset', 'directory', false, 2, FALSE, 'Path to the SWBD dataset', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2a.working-dir', 'Working directory', 'string', false, 2, FALSE, 'Working directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.processors_image','L2a processors image name','string',false,2, FALSE, 'L2a processors image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.sen2cor_image','Sen2Cor image name','string',false,2, FALSE, 'Sen2Cor image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.maja_image','MAJA image name','string',false,2, FALSE, 'MAJA image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.gdal_image','GDAL image name','string',false,2, FALSE, 'GDAL image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.l8_align_image','L8 align image name','string',false,2, FALSE, 'L8 align image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.dem_image','DEM image name','string',false,2, FALSE, 'DEM image name', NULL) on conflict DO nothing;
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true, '{1,2}', null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'L2A Atmospheric Corrections', short_name = 'l2a', label = 'L2A &mdash; Atmospheric Corrections', required = true, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (2, 'Sen2Agri L3A Composite','l3a', 'Sen2Agri L3A &mdash; Cloud-free Composite', false, '{1,2}', '{1}', true, false, false, false, 'The Cloud-free Reflectance Composite product provides a cloud-free temporal synthesis of surface reflectance values in the 10 Sentinel-2 bands designed for land observation. It is delivered with several masks that will help appraising its quality.')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L3A Composite', short_name = 'l3a', label = 'Sen2Agri L3A &mdash; Cloud-free Composite', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false, description = 'The Cloud-free Reflectance Composite product provides a cloud-free temporal synthesis of surface reflectance values in the 10 Sentinel-2 bands designed for land observation. It is delivered with several masks that will help appraising its quality.';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false, '{1,2}', null, true, true, false, false,
                                      'Vegetation Status Indicators: informs about the evolution of the green vegetation corresponding to the crop vegetative development')
                        ON CONFLICT (id) DO UPDATE SET name = 'L3B Vegetation Status', short_name = 'l3b', label = 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false, 
                                                       description = 'Vegetation Status Indicators: informs about the evolution of the green vegetation corresponding to the crop vegetative development';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (4, 'Sen2Agri L3E Pheno NDVI metrics','l3e', 'Sen2Agri L3E &mdash; Phenology Indices', false, '{1,2}', '{1}', true, false, false, false, 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L3E Pheno NDVI metrics', short_name = 'l3e', label = 'Sen2Agri L3E &mdash; Phenology Indices', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false, description = 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (5, 'Sen2Agri L4A Crop Mask','l4a', 'Sen2Agri L4A &mdash; Cropland Mask', false, '{1,2}', '{1}', true, false, false, false, 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L4A Crop Mask', short_name = 'l4a', label = 'Sen2Agri L4A &mdash; Cropland Mask', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}',
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = true, description = 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                       (6, 'Sen2Agri L4B Crop Type','l4b', 'Sen2Agri L4B &mdash; Crop Type Map', false, '{1,2}', '{1}', true, false, false, false, 'Crop Type Map: map of the main crop types in a given region, with a minimum mapping unit of 0.01 ha and provided along with several quality flags. The crop types are classified over the cropland area identified in the cropland mask. The map is generated twice over the season, at the middle and at the end of the season')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L4B Crop Type', short_name = 'l4b', label = 'Sen2Agri L4B &mdash; Crop Type Map', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = true, description = 'Crop Type Map: map of the main crop types in a given region, with a minimum mapping unit of 0.01 ha and provided along with several quality flags. The crop types are classified over the cropland area identified in the cropland mask. The map is generated twice over the season, at the middle and at the end of the season';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (12, 'S2A L3C LAI N-Days Reprocessing','s2a_l3c', 'S2A L3C &mdash; LAI N-Days Reprocessing', false, '{1,2}', null, true, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'S2A L3C LAI N-Days Reprocessing', short_name = 's2a_l3c', label = 'S2A L3C &mdash; LAI N-Days Reprocessing', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null,
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (13, 'S2A L3D LAI Fitted Reprocessing','s2a_l3d', 'S2A L3d &mdash; LAI Fitted Reprocessing', false, '{1,2}', null, true, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'S2A L3D LAI Fitted Reprocessing', short_name = 's2a_l3d', label = 'S2A L3d &mdash; LAI Fitted Reprocessing', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null,
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;


                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (15, 'Validity flags','l2a_msk', 'Validity flags', true, '{1,2}', null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'Validity flags', short_name = 'l2a_msk', label = 'Validity flags', required = true, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES 
                                      (21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true, null, null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'T-Rex Updater',  short_name = 't_rex_updater', label = 'T-Rex Updater', required = true, supported_satellite_ids = null, mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$

            $str$;
            raise notice '%', _statement;
            execute _statement;


            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
        
            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;                

            _statement := $str$                        
            $str$;
            raise notice '%', _statement;
            execute _statement;                

            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$                                        
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$  
            $str$;
            raise notice '%', _statement;
            execute _statement;                
                
            _statement := 'update meta set version = ''3.0.0'';';
            raise notice '%', _statement;
            execute _statement;
        end if;
    end if;
    raise notice 'complete';
end;
$migration$;

commit;


