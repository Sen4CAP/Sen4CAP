begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('3.1.0', '3.2.0')) then
            _statement := $str$
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

                ALTER TABLE datasource ADD COLUMN IF NOT EXISTS product_type character varying(50) DEFAULT NULL;
                
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
                INSERT INTO satellite(id, satellite_name) VALUES (4, 'landsat9') on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                CREATE TABLE IF NOT EXISTS default_scheduled_tasks
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
                
                ALTER TABLE default_scheduled_tasks DROP CONSTRAINT IF EXISTS default_scheduled_tasks_processor_id_fkey;
                ALTER TABLE default_scheduled_tasks
                    ADD CONSTRAINT default_scheduled_tasks_processor_id_fkey FOREIGN KEY (processor_id) REFERENCES processor (id);
                
                $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$  
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$  
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
                CREATE TABLE IF NOT EXISTS s2_tile_dem_statistics(
                    tile_id text not null primary key,
                    minimum smallint not null,
                    maximum smallint not null,
                    mean real not null,
                    stddev real not null
                );
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true, '{1,2}', null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'L2A Atmospheric Corrections', short_name = 'l2a', label = 'L2A &mdash; Atmospheric Corrections', required = true, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
--                                      (2, 'Sen2Agri L3A Composite','l3a', 'Sen2Agri L3A &mdash; Cloud-free Composite', false, '{1,2}', '{1}', true, false, false, false, 'The Cloud-free Reflectance Composite product provides a cloud-free temporal synthesis of surface reflectance values in the 10 Sentinel-2 bands designed for land observation. It is delivered with several masks that will help appraising its quality.')
--                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L3A Composite', short_name = 'l3a', label = 'Sen2Agri L3A &mdash; Cloud-free Composite', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false, description = 'The Cloud-free Reflectance Composite product provides a cloud-free temporal synthesis of surface reflectance values in the 10 Sentinel-2 bands designed for land observation. It is delivered with several masks that will help appraising its quality.';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false, '{1,2}', null, true, true, false, false,
                                      'Vegetation Status Indicators: informs about the evolution of the green vegetation corresponding to the crop vegetative development')
                        ON CONFLICT (id) DO UPDATE SET name = 'L3B Vegetation Status', short_name = 'l3b', label = 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false, 
                                                       description = 'Vegetation Status Indicators: informs about the evolution of the green vegetation corresponding to the crop vegetative development';

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
--                                      (4, 'Sen2Agri L3E Pheno NDVI metrics','l3e', 'Sen2Agri L3E &mdash; Phenology Indices', false, '{1,2}', '{1}', true, false, false, false, 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension')
--                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L3E Pheno NDVI metrics', short_name = 'l3e', label = 'Sen2Agri L3E &mdash; Phenology Indices', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false, description = 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension';

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
--                                      (5, 'Sen2Agri L4A Crop Mask','l4a', 'Sen2Agri L4A &mdash; Cropland Mask', false, '{1,2}', '{1}', true, false, false, false, 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension')
--                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L4A Crop Mask', short_name = 'l4a', label = 'Sen2Agri L4A &mdash; Cropland Mask', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}',
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = true, description = 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension';

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
--                                       (6, 'Sen2Agri L4B Crop Type','l4b', 'Sen2Agri L4B &mdash; Crop Type Map', false, '{1,2}', '{1}', true, false, false, false, 'Crop Type Map: map of the main crop types in a given region, with a minimum mapping unit of 0.01 ha and provided along with several quality flags. The crop types are classified over the cropland area identified in the cropland mask. The map is generated twice over the season, at the middle and at the end of the season')
--                        ON CONFLICT (id) DO UPDATE SET name = 'Sen2Agri L4B Crop Type', short_name = 'l4b', label = 'Sen2Agri L4B &mdash; Crop Type Map', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = true, description = 'Crop Type Map: map of the main crop types in a given region, with a minimum mapping unit of 0.01 ha and provided along with several quality flags. The crop types are classified over the cropland area identified in the cropland mask. The map is generated twice over the season, at the middle and at the end of the season';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (7, 'L2-S1 Pre-Processor', 'l2s1', 'L2 S1 &mdash; SAR Pre-Processor', true, '{3}', '{3}', false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'L2-S1 Pre-Processor',  short_name = 'l2s1', label = 'L2 S1 &mdash; SAR Pre-Processor', required = true, supported_satellite_ids = '{3}', mandatory_satellite_ids = '{3}',
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (8, 'LPIS/GSAA', 'lpis', 'LPIS / GSAA Processor', true, null, null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'LPIS/GSAA', short_name = 'lpis', label = 'LPIS / GSAA Processor', required = true, supported_satellite_ids = null, mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (9, 'Sen4CAP L4A Crop Type','s4c_l4a', 'Sen4CAP L4A &mdash; Crop Type', false, '{1,2,3}', null, true, true, true, false,
                                      'Parcel Level Crop Type: a subset of the parcels from the declaration dataset is used to train the Random Forest model which is then applied to the whole declaration dataset')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen4CAP L4A Crop Type', short_name = 's4c_l4a', label = 'Sen4CAP L4A &mdash; Crop Type', required = false, supported_satellite_ids = '{1,2,3}',mandatory_satellite_ids =  null, 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = false,
                                                       description = 'Parcel Level Crop Type: a subset of the parcels from the declaration dataset is used to train the Random Forest model which is then applied to the whole declaration dataset';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (10, 'Sen4CAP L4B Grassland Mowing','s4c_l4b', 'Sen4CAP L4B &mdash; Grassland Mowing', false, '{1,2,3}', null, true, false, true, true, 'Grassland mowing: detects the mowing events with data ranges at parcel-level')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen4CAP L4B Grassland Mowing', short_name = 's4c_l4b', label = 'Sen4CAP L4B &mdash; Grassland Mowing', required = false, supported_satellite_ids = '{1,2,3}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = true, description = 'Grassland mowing: detects the mowing events with data ranges at parcel-level';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (11, 'Sen4CAP L4C Agricultural Practices','s4c_l4c', 'Sen4CAP L4C &mdash; Agricultural Practices', false, '{1,2,3}', '{1,3}', true, false, true, true,
                                      'Agricultural practices: developed methodology relies on the analysis of dense temporal profiles. The generation of temporal profiles is based on optical (S2 and L8) and Synthetic Aperture Radar (SAR - S1) imagery. NDVI is used as the optical-based signal, at a spatial resolution of 10 m. The SAR-based signals include backscatter temporal profiles (ascending and descending orbits for dual VV and VH polarization) and coherence temporal profiles (for VV polarization) at 20 m spatial resolution')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen4CAP L4C Agricultural Practices', short_name = 's4c_l4c', label = 'Sen4CAP L4C &mdash; Agricultural Practices', required = false, supported_satellite_ids = '{1,2,3}', mandatory_satellite_ids = '{1,3}' , is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = true,
                                                       description = 'Agricultural practices: developed methodology relies on the analysis of dense temporal profiles. The generation of temporal profiles is based on optical (S2 and L8) and Synthetic Aperture Radar (SAR - S1) imagery. NDVI is used as the optical-based signal, at a spatial resolution of 10 m. The SAR-based signals include backscatter temporal profiles (ascending and descending orbits for dual VV and VH polarization) and coherence temporal profiles (for VV polarization) at 20 m spatial resolution';

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
--                                      (12, 'S2A L3C LAI N-Days Reprocessing','s2a_l3c', 'S2A L3C &mdash; LAI N-Days Reprocessing', false, '{1,2}', null, true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'S2A L3C LAI N-Days Reprocessing', short_name = 's2a_l3c', label = 'S2A L3C &mdash; LAI N-Days Reprocessing', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null,
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
--                                      (13, 'S2A L3D LAI Fitted Reprocessing','s2a_l3d', 'S2A L3d &mdash; LAI Fitted Reprocessing', false, '{1,2}', null, true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'S2A L3D LAI Fitted Reprocessing', short_name = 's2a_l3d', label = 'S2A L3d &mdash; LAI Fitted Reprocessing', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null,
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description) VALUES
                                      (14, 'Sen4CAP Marker Database PR1','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1', false, '{1,2,3}', null, true, false, true, false, 
                                      'Markers database: a set of basic markers extracted at parcel level (mean and standard deviation for coherence, amplitude and biophysical indicators) used for deriving new user products')
                        ON CONFLICT (id) DO UPDATE SET name = 'Sen4CAP Marker Database PR1', short_name = 's4c_mdb1', label = 'MD_PR1 &mdash; Marker Database PR1', required = false, supported_satellite_ids = '{1,2,3}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = false,
                                                       description = 'Markers database: a set of basic markers extracted at parcel level (mean and standard deviation for coherence, amplitude and biophysical indicators) used for deriving new user products';

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                                      (15, 'Validity flags','l2a_msk', 'Validity flags', true, '{1,2}', null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'Validity flags', short_name = 'l2a_msk', label = 'Validity flags', required = true, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
--                                      (16, 'S4S Permanent Crop','s4s_perm_crop', 'S4S Permanent crop', false, '{1,2}', null, true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'S4S Permanent Crop', short_name = 's4s_perm_crop', label = 'S4S Permanent crop', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
--                                      (17, 'S4S Yield Features','s4s_yield_feat', 'S4S Yield Features', false, '{1,2}', null, true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'S4S Yield Features', short_name = 's4s_yield_feat', label = 'S4S Yield Features', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = null, 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
--                                      (18, 'Era5 weather','era5_weather', 'Era5 weather', true, null, null, false, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'Era5 weather', short_name = 'era5_weather', label = 'Era5 weather', required = true, supported_satellite_ids = null, mandatory_satellite_ids = null, 
--                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES 
--                                      (20, 'S4S Crop Mapping', 's4s_crop_mapping', 'S4S Crop Mapping', false, '{1,2,3}', null, true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'S4S Crop Mapping',  short_name = 's4s_crop_mapping', label = 'S4S Crop Mapping', required = false, supported_satellite_ids = '{1,2,3}', mandatory_satellite_ids = null, 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = true, additional_config_required = false;

                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES 
                                      (21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true, null, null, false, false, false, false)
                        ON CONFLICT (id) DO UPDATE SET name = 'T-Rex Updater',  short_name = 't_rex_updater', label = 'T-Rex Updater', required = true, supported_satellite_ids = null, mandatory_satellite_ids = null, 
                                                       is_admin_ui_visible = false, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES 
--                                      (22, 'L3 S1 Composite', 'l3_s1_comp', 'L3 S1 Composite', false, '{3}', '{3}', true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'L3 S1 Composite',  short_name = 'l3_s1_comp', label = 'L3 S1 Composite', required = false, supported_satellite_ids = '{3}', mandatory_satellite_ids = '{3}', 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

--                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES 
--                                      (23, 'L3 Indicators Composite', 'l3_ind_comp', 'L3 Indicators Composite', false, '{1,2}', '{1}', true, false, false, false)
--                        ON CONFLICT (id) DO UPDATE SET name = 'L3 Indicators Composite',  short_name = 'l3_ind_comp', label = 'L3 Indicators Composite', required = false, supported_satellite_ids = '{1,2}', mandatory_satellite_ids = '{1}', 
--                                                       is_admin_ui_visible = true, is_service_ui_visible = false, lpis_required = false, additional_config_required = false;

             $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            -- Data initialization
            _statement := $str$
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
                  LANGUAGE plpgsql            
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.query.timeout', NULL, '90','2020-07-22 19:52:22.244592+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02')on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('monitor-agent.disk-path', NULL, '/mnt/archive/', '2015-07-20 10:27:29.301355+03') 
                            on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.2.0', '2021-01-14 12:11:21.800537+00')            on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/processors:3.2.0';
                
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.processors_image', NULL, 'sen4x/l2a-processors:0.2.3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-processors:0.2.3';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.sen2cor_image', NULL, 'sen4x/sen2cor:2.10.01-ubuntu-20.04') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/sen2cor:2.10.01-ubuntu-20.04';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.maja_image', NULL, 'sen4x/maja:4.5.4-centos-7') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/maja:4.5.4-centos-7';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.4.1') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'osgeo/gdal:ubuntu-full-3.4.1';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.l8_align_image', NULL, 'sen4x/l2a-l8-alignment:0.1.2') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-l8-alignment:0.1.2';
                INSERT INTO config(key, site_id, value) VALUES ('processor.l2a.dem_image', NULL, 'sen4x/l2a-dem:0.1.3') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-dem:0.1.3';

                
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.stdev_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.minmax_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.median_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.p25_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.p75_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_l2a', NULL, 'N/A', '2024-03-16 17:31:06.01191+02') on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                -- L3B Updates
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.chain_inputs_steps', NULL, 'false', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.produce_mosaic', NULL, 'true', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.l3b.filter.chain_inputs_steps', 'Chain L3B products', 'bool', TRUE, 4, TRUE, 'Chain L3B products', NULL) ON conflict do nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.produce_mosaic', 'Generate L3B mosaic', 'bool', TRUE, 4, TRUE, 'Generate L3B mosaic', NULL) ON conflict do nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.cloud_optimized_geotiff_output', 'Generate L3B Cloud Optimized Geotiff outputs', 'bool', TRUE, 4, TRUE, 'Generate L3B Cloud Optimized Geotiff outputs', NULL)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fapar', 'L3B processor will produce FAPAR', 'bool', TRUE, 4, TRUE, 'Produce FAPAR', NULL, TRUE)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce FAPAR', is_service_ui_visible = TRUE;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fcover', 'L3B processor will produce FCOVER', 'bool', TRUE, 4, TRUE, 'Produce FCOVER', NULL, true)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce FCOVER', is_service_ui_visible = TRUE;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_in_domain_flags', 'L3B processor will produce input domain flags', 'bool', TRUE, 4, TRUE, 'Produce input domain flags', NULL, TRUE)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce input domain flags', is_service_ui_visible = TRUE;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_lai', 'L3B processor will produce LAI', 'bool', TRUE, 4, TRUE, 'Produce LAI', NULL, TRUE)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce LAI', is_service_ui_visible = TRUE;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndvi', 'L3B processor will produce NDVI', 'bool', TRUE, 4, TRUE, 'Produce NDVI', NULL, TRUE)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce NDVI', is_service_ui_visible = TRUE;

                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndwi', 'L3B processor will produce NDWI', 'bool', TRUE, 4, TRUE, 'Produce NDWI', NULL, TRUE)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce NDWI', is_service_ui_visible = TRUE;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_brightness', 'L3B processor will produce brightness', 'bool', TRUE, 4, FALSE, 'Produce brightness', NULL, FALSE)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_advanced = TRUE, is_site_visible = TRUE, label = 'Produce brightness', is_service_ui_visible = FALSE;

                -- L4C Updates
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tillage_monitoring', 'Enable tillage monitoring', 'bool', false, 20, true, 'Enable tillage monitoring', NULL)
                    ON conflict(key) DO UPDATE SET type = 'bool', is_site_visible = TRUE;
                    
                -- MDB 1
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_l2a', 'The list of L2A products', 'select', FALSE, 26, TRUE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_enabled', 'AMP markers extraction enabled', 'bool', true, 26, true, 'Extract Amplitude markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract Amplitude markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.cohe_enabled', 'COHE markers extraction enabled', 'bool', true, 26, true, 'Extract Coherence markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract Coherence markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.data_extr_dir', 'Location for the MDB1 data extration files', 'string', true, 26, FALSE, 'Location for the MDB1 data extration files', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Location for the MDB1 data extration files';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fapar_enabled', 'FAPAR markers extraction enabled', 'bool', true, 26, true, 'Extract FAPAR markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract FAPAR markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fcover_enabled', 'FCOVER markers extraction enabled', 'bool', true, 26, true, 'Extract FCOVER markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract FCOVER markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.lai_enabled', 'LAI markers extraction enabled', 'bool', true, 26, true, 'Extract LAI markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract LAI markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndvi_enabled', 'NDVI markers extraction enabled', 'bool', true, 26, true, 'Extract NDVI markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract NDVI markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', 'AMP VV/VH markers extraction enabled', 'bool', true, 26, true, 'Extract Amplitude VV/VH markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract Amplitude VV/VH markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.valid_pixels_enabled', 'Number of valid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Extract number of valid pixels per parcel', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract number of valid pixels per parcel';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.minmax_enabled', 'Min/Max per parcel extraction enabled', 'bool', true, 26, true, 'Extract Min/Max values', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract Min/Max values';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.median_enabled', 'Median per parcels extraction enabled', 'bool', true, 26, true, 'Extract Median', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract Median';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.p25_enabled', 'P25 per parcels extraction enabled', 'bool', true, 26, true, 'Extract P25', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract P25';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.p75_enabled', 'P75 per parcels extraction enabled', 'bool', true, 26, true, 'Extract P75', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract P75';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab02_enabled', 'Reflectance band B02 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B02 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B02 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab03_enabled', 'Reflectance band B03 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B03 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B03 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab04_enabled', 'Reflectance band B04 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B04 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B04 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab05_enabled', 'Reflectance band B05 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B05 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B05 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab06_enabled', 'Reflectance band B06 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B06 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B06 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab07_enabled', 'Reflectance band B07 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B07 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B07 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab08_enabled', 'Reflectance band B08 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B08 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B08 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab8a_enabled', 'Reflectance band B8A markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B8A markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B8A markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab11_enabled', 'Reflectance band B11 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B11 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B11 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab12_enabled', 'Reflectance band B12 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B12 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract reflectance band B12 markers';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_enabled', 'MDB3 markers extraction enabled', 'bool', true, 26, true, 'Extract MDB3 markers', NULL)
                    ON conflict(key) DO UPDATE SET is_site_visible = TRUE, label = 'Extract MDB3 markers';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config_metadata VALUES ('processor.l2s1.enabled', 'S1 pre-processing enabled', 'bool', false, 23, FALSE, 'S1 pre-processing enabled', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'S1 pre-processing enabled', label = 'S1 pre-processing enabled';
                INSERT INTO config_metadata VALUES ('processor.l2s1.parallelism', 'Number of jobs to run in parallel', 'int', false, 23, FALSE, 'Number of jobs to run in parallel', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Number of jobs to run in parallel', label = 'Number of jobs to run in parallel';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.path', 'Final S1 L2 products path', 'string', false, 23, FALSE, 'Final S1 L2 products path', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Final S1 L2 products path', label = 'Final S1 L2 products path';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.offset', 'Coherence interval', 'int', false, 23, FALSE, 'Coherence interval', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Coherence interval', label = 'Coherence interval';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.work.dir', 'Temporary S1 L2 files path', 'string', false, 23, FALSE, 'Temporary S1 L2 files path', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Temporary S1 L2 files path', label = 'Temporary S1 L2 files path';                

                INSERT INTO config_metadata VALUES ('processor.l2s1.compute.amplitude', 'Compute amplitude', 'bool', false, 23, FALSE, 'Compute amplitude', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Compute amplitude', label = 'Compute amplitude';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.compute.coherence', 'Compute coherence', 'bool', false, 23, FALSE, 'Compute coherence', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Compute coherence', label = 'Compute coherence';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.parallelism', 'GPT parallelism', 'int', false, 23, FALSE, 'GPT parallelism', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'GPT parallelism', label = 'GPT parallelism';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.tile.cache.size', 'GPT tile cache size', 'int', false, 23, FALSE, 'GPT tile cache size', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'GPT tile cache size', label = 'GPT tile cache size';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.join.amplitude.steps', 'Join amplitude steps', 'bool', false, 23, FALSE, 'Join amplitude steps', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Join amplitude steps', label = 'Join amplitude steps';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.join.coherence.steps', 'Join coherence steps', 'bool', false, 23, FALSE, 'Join coherence steps', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Join coherence steps', label = 'Join coherence steps';                

                INSERT INTO config_metadata VALUES ('dem.name', 'DEM to use', 'string', false, 23, FALSE, 'DEM to use', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'DEM to use', label = 'DEM to use';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.acquisition.delay', 'Acquisition delay', 'int', false, 23, FALSE, 'Acquisition delay', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Acquisition delay', label = 'Acquisition delay';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.copy.locally', 'Copy input products locally', 'bool', false, 23, FALSE, 'Copy input products locally', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Copy input products locally', label = 'Copy input products locally';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.crop.nodata', 'Crop NODATA', 'bool', false, 23, FALSE, 'Crop NODATA', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Crop NODATA', label = 'Crop NODATA';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.crop.output', 'Crop output', 'bool', false, 23, FALSE, 'Crop output', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Crop output', label = 'Crop output';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.extract.histogram', 'Extract histogram', 'bool', false, 23, FALSE, 'Extract histogram', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Extract histogram', label = 'Extract histogram';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.interval', 'Preprocessing job interval', 'int', false, 23, FALSE, 'Preprocessing job interval', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Preprocessing job interval', label = 'Preprocessing job interval';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.keep.intermediate', 'Keep intermediate files', 'bool', false, 23, FALSE, 'Keep intermediate files', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Keep intermediate files', label = 'Keep intermediate files';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.master', 'Primary acquisition for colocation', 'string', false, 23, FALSE, 'Primary acquisition for colocation', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Primary acquisition for colocation', label = 'Primary acquisition for colocation';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.intersection', 'Minimum % of SLC overlaps for coherence', 'float', false, 23, FALSE, 'Minimum % of SLC overlaps for coherence', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Minimum % of SLC overlaps for coherence', label = 'Minimum % of SLC overlaps for coherence';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.output.extension', 'Output extension', 'string', false, 23, FALSE, 'Output extension', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Output extension', label = 'Output extension';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.output.format', 'Output format', 'string', false, 23, FALSE, 'Output format', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Output format', label = 'Output format';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.overwrite.existing', 'Overwrite existing products', 'bool', false, 23, FALSE, 'Overwrite existing products', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Overwrite existing products', label = 'Overwrite existing products';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.parallel.steps.enabled', 'Run steps in parallel', 'bool', false, 23, FALSE, 'Run steps in parallel', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Run steps in parallel', label = 'Run steps in parallel';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.pixel.spacing', 'Output spatial resolution', 'float', false, 23, FALSE, 'Output spatial resolution', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Output spatial resolution', label = 'Output spatial resolution';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.polarisations', 'Polarisations', 'string', false, 23, FALSE, 'Polarisations', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Polarisations', label = 'Polarisations';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.process.newest', 'Process newest scenes first', 'bool', false, 23, FALSE, 'Process newest scenes first', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Process newest scenes first', label = 'Process newest scenes first';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.projection', 'Output projection', 'string', false, 23, FALSE, 'Output projection', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Output projection', label = 'Output projection';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.resolve.links', 'Resolve links', 'bool', false, 23, FALSE, 'Resolve links', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Resolve links', label = 'Resolve links';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.step.timeout', 'Step timeout', 'int', false, 23, FALSE, 'Step timeout', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Step timeout', label = 'Step timeout';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.filter.interval', 'Temporal filter window', 'int', false, 23, FALSE, 'Temporal filter window', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Temporal filter window', label = 'Temporal filter window';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.version', 'Processor version', 'string', false, 23, FALSE, 'Processor version', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Processor version', label = 'Processor version';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.memory', 'Minimum free memory for a step', 'string', false, 23, FALSE, 'Minimum free memory for a step', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Minimum free memory for a step', label = 'Minimum free memory for a step';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.disk', 'Minimum disk storage for a step', 'string', false, 23, FALSE, 'Minimum disk storage for a step', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Minimum disk storage for a step', label = 'Minimum disk storage for a step';                

                INSERT INTO config_metadata VALUES ('processor.l2s1.use.other.site.products', 'Reuse S1 L2 products created for other sites', 'string', false, 23, FALSE, 'Reuse S1 L2 products created for other sites', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Reuse S1 L2 products created for other sites', label = 'Reuse S1 L2 products created for other sites';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.s2.intersection', 'Minimum % of intersection for S2 tile clipping', 'string', false, 23, FALSE, 'Minimum % of intersection for S2 tile clipping', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Minimum % of intersection for S2 tile clipping', label = 'Minimum % of intersection for S2 tile clipping';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.ignore.previous.orbit.failure', 'Continue processing in case of a failure from the same orbit', 'string', false, 23, FALSE, 'Continue processing in case of a failure from the same orbit', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Continue processing in case of a failure from the same orbit', label = 'Continue processing in case of a failure from the same orbit';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.convert.int', 'Make S1 L2 product pixel type UInt16', 'string', false, 23, FALSE, 'Make S1 L2 product pixel type UInt16', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Make S1 L2 product pixel type UInt16', label = 'Make S1 L2 product pixel type UInt16';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.otb.min.memory', 'Memory to allocate to OTB steps', 'string', false, 23, FALSE, 'Memory to allocate to OTB steps', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Memory to allocate to OTB steps', label = 'Memory to allocate to OTB steps';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.otb.enable.compression', 'Compress OTB steps output', 'string', false, 23, FALSE, 'Compress OTB steps output', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Compress OTB steps output', label = 'Compress OTB steps output';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.terrain.flattening.autodetect', 'Decide usage of gamma naught correction based on detected elevation', 'string', false, 23, FALSE, 'Decide usage of gamma naught correction based on detected elevation', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Decide usage of gamma naught correction based on detected elevation', label = 'Decide usage of gamma naught correction based on detected elevation';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.terrain.flattening.enabled', 'Enable usage of gamma naught correction', 'string', false, 23, FALSE, 'Enable usage of gamma naught correction', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Enable usage of gamma naught correction', label = 'Enable usage of gamma naught correction';                
                INSERT INTO config_metadata VALUES ('processor.l2s1.subset.before.tc.enabled', 'Subset SLC scene before terrain correction', 'string', false, 23, FALSE, 'Subset SLC scene before terrain correction', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Subset SLC scene before terrain correction', label = 'Subset SLC scene before terrain correction';                
            
                INSERT INTO config_metadata VALUES ('processor.l2s1.bck.scale', 'Backscatter scaling factor', 'int', false, 23, FALSE, 'Backscatter scaling factor', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Backscatter scaling factor', label = 'Backscatter scaling factor'; 
                INSERT INTO config_metadata VALUES ('processor.l2s1.cohe.scale', 'Coherence scaling factor', 'int', false, 23, FALSE, 'Coherence scaling factor', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Coherence scaling factor', label = 'Coherence scaling factor'; 
                INSERT INTO config_metadata VALUES ('processor.l2s1.compress.enabled', 'Compress outputs (V1)', 'string', false, 23, FALSE, 'Compress outputs (V1)', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Compress outputs (V1)', label = 'Compress outputs (V1)'; 
                INSERT INTO config_metadata VALUES ('processor.l2s1.crop.enabled', 'Enable cropping by S2 (V1)', 'string', false, 23, FALSE, 'Enable cropping by S2 (V1)', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Enable cropping by S2 (V1)', label = 'Enable cropping by S2 (V1)'; 
                INSERT INTO config_metadata VALUES ('processor.l2s1.tiled.tiff', 'Create tiled output tiff', 'string', false, 23, FALSE, 'Create tiled output tiff', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Create tiled output tiff', label = 'Create tiled output tiff'; 
                INSERT INTO config_metadata VALUES ('processor.l2s1.zarr.conversion', 'Convert products to zarr', 'string', false, 23, FALSE, 'Convert products to zarr', NULL)
                    ON conflict(key) DO UPDATE SET friendly_name = 'Convert products to zarr', label = 'Convert products to zarr'; 

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('dem.name', NULL, 'SRTM 1Sec HGT', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.parallelism', NULL, '1', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.path', NULL, '/mnt/archive/{site}/l2a-s1', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.temporal.offset', NULL, '6','2020-07-22 19:52:22.42305+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.work.dir', NULL, '/mnt/archive/s1_preprocessing_work_dir', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.compute.amplitude', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.compute.coherence', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.gpt.parallelism', NULL, '8', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.gpt.tile.cache.size', NULL, '256', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.join.amplitude.steps', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.join.coherence.steps', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.acquisition.delay', NULL, '2', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.copy.locally', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.crop.nodata', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.crop.output', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.extract.histogram', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.interval', NULL, '60', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.keep.intermediate', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.master', NULL, 'S1B', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.intersection', NULL, '0.05', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.output.extension', NULL, '.tif', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.output.format', NULL, 'GDAL-GTiff-WRITER', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.overwrite.existing', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.parallel.steps.enabled', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.pixel.spacing', NULL, '20', '2022-09-30 10:31:00.501+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '20';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.polarisations', NULL, 'VV;VH', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.process.newest', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.projection', NULL, 'PROJCS["ETRS89 / LAEA Europe", GEOGCS["ETRS89", DATUM["European Terrestrial Reference System 1989", SPHEROID["GRS 1980", 6378137.0, 298.257222101, AUTHORITY["EPSG","7019"]], TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], AUTHORITY["EPSG","6258"]], PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]], UNIT["degree", 0.017453292519943295], AXIS["Geodetic longitude", EAST], AXIS["Geodetic latitude", NORTH], AUTHORITY["EPSG","4258"]], PROJECTION["Lambert_Azimuthal_Equal_Area", AUTHORITY["EPSG","9820"]], PARAMETER["latitude_of_center", 52.0], PARAMETER["longitude_of_center", 10.0], PARAMETER["false_easting", 4321000.0], PARAMETER["false_northing", 3210000.0], UNIT["m", 1.0], AXIS["Easting", EAST], AXIS["Northing", NORTH], AUTHORITY["EPSG","3035"]]', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.resolve.links', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.step.timeout', NULL, '60', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.temporal.filter.interval', NULL, '0', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.version', NULL, '1', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.memory', NULL, '8192', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.disk', NULL, '16384', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.use.other.site.products', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.s2.intersection', NULL, '0.05', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.ignore.previous.orbit.failure', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.otb.min.memory', NULL, '2048', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.otb.enable.compression', NULL, true, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.terrain.flattening.autodetect', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.terrain.flattening.enabled', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.subset.before.tc.enabled', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.bck.scale', NULL, '10000', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.cohe.scale', NULL, '10000', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                -- In V2, the following 4 lines should be set to true
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.compress.enabled', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.convert.int', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.crop.enabled', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.tiled.tiff', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.zarr.conversion', NULL, false, '2022-09-30 10:31:00.501+02') on conflict DO nothing;
            
            
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

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config_category VALUES (35, 'Zarr Converter', 35, true) on conflict DO nothing;
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES (19, 'Zarr converter', 'zarr', 'Zarr converter', false, null, null, false, false, false, false) on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.zarr-converter', NULL, 's2x_prd_to_zarr.py', '2022-04-18 14:25:14.193131+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.zarr', NULL, '/mnt/archive/orchestrator_temp/zarr/{job_id}/{task_id}-{module}', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.zarr.keep_job_folders', NULL, '0', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.zarr.slurm_qos', NULL, 'qoszarr', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.zarr.enabled', NULL, 'false', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.zarr.input_l3b', NULL, 'N/A', '2019-02-18 15:27:41.861613+02') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('executor.processor.zarr.keep_job_folders', 'Keep ZARR intermediate folders', 'int', false, 8, FALSE, 'Keep ZARR intermediate folders', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.zarr-converter', 'Zarr converter Path', 'file', true, 8, FALSE, 'Zarr converter Path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.scratch-path.zarr', 'Path for Zarr temporary files', 'string', false, 1, FALSE, 'Path for Zarr temporary files', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.zarr.slurm_qos', 'Slurm QOS for Zarr processor', 'string', true, 8, FALSE, 'Slurm QOS for Zarr processor', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.zarr.enabled', 'Zarr conversion enabled', 'bool', false, 35, FALSE, 'Zarr conversion enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.zarr.input_l3b', 'The list of L3B products', 'select', FALSE, 35, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}') on conflict DO nothing;
                

            $str$;
            raise notice '%', _statement;
            execute _statement;
                    
            _statement := $str$
                INSERT INTO public.default_scheduled_tasks VALUES (3, 'L3B', 1, 1, 0, 'start', NULL, '1 day', 60, 1, NULL) on conflict DO nothing;
                INSERT INTO public.default_scheduled_tasks VALUES (9, 'S4C_L4A', 2, 0, 31, 'mid', NULL, NULL, 60, 1, NULL) on conflict DO nothing;
                INSERT INTO public.default_scheduled_tasks VALUES (10, 'S4C_L4B', 2, 0, 31, 'start', NULL, '31 days', 60, 1, NULL) on conflict DO nothing;
                INSERT INTO public.default_scheduled_tasks VALUES (11, 'S4C_L4C', 1, 7, 0, 'start', NULL, '7 days', 60, 1, NULL) on conflict DO nothing;
                INSERT INTO public.default_scheduled_tasks VALUES (14, 'S4C_MDB1', 1, 1, 0, 'start', NULL, '1 day', 60, 1, NULL) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcel_id_col_name', NULL, 'NewID', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_csv_file_name_pattern', NULL, 'decl_.*_\d{4}.csv', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_optical_file_name_pattern', NULL, '.*_buf_5m.shp', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_sar_file_name_pattern', NULL, '.*_buf_10m.shp', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_csv_file_name_pattern', 'Parcels product csv file name pattern', 'string', false, 1, FALSE, 'Parcels product csv file name pattern', NULL)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_optical_file_name_pattern', 'Parcels product optical file name pattern', 'string', false, 1, FALSE, 'Parcels product optical file name pattern', NULL)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_sar_file_name_pattern', 'Parcels product SAR file name pattern', 'string', false, 1, FALSE, 'Parcels product SAR file name pattern', NULL)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcel_id_col_name', 'Parcels parcels id columns name', 'string', false, 1, FALSE, 'Parcels parcels id columns name', NULL)  on conflict DO nothing;

                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.export-product-launcher', NULL, '/usr/bin/export-product-launcher.py', '2019-04-12 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/bin/export-product-launcher.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4b_cfg_import', NULL, 's4c_l4b_import_config.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_l4b_import_config.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_cfg_import', NULL, 's4c_l4c_import_config.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_l4c_import_config.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_practices_export', NULL, '/usr/bin/s4c_l4c_export_all_practices.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/bin/s4c_l4c_export_all_practices.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_practices_import', NULL, 's4c_l4c_import_practice.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_l4c_import_practice.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lpis_import', NULL, 'data-preparation.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'data-preparation.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb-csv-to-ipc-export', NULL, 'csv_to_ipc.py', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'csv_to_ipc.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.ogr2ogr',  NULL, '/usr/local/bin/ogr2ogr', '2019-10-18 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/local/bin/ogr2ogr';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-crop-type', NULL, 'crop-type-wrapper.py', '2019-02-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'crop-type-wrapper.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-grassland-gen-input-shp',  NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/generate_grassland_mowing_input_shp.py', '2019-10-18 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/generate_grassland_mowing_input_shp.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-grassland-extract-products', NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/s4c-l4b-extract-products.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/s4c-l4b-extract-products.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-l4a-extract-parcels', NULL, 'extract-parcels.py', '2021-01-15 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract-parcels.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb3-input-tables-extract', NULL, 's4c_mdb3_input_tables.py', '2021-01-15 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_mdb3_input_tables.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb3-extract-markers', NULL, 'extract_mdb3_markers.py', '2021-01-15 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_mdb3_markers.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lpis_list_columns', NULL, 'read_shp_cols.py', '2022-02-12 17:09:18.767175+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'read_shp_cols.py';
                                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.lpis.lut_upload_path', NULL, '/mnt/archive/upload/LUT/{site}', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.lpis.path', NULL, '/mnt/archive/lpis/{site}/{year}', '2019-06-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.lpis.upload_path', NULL, '/mnt/archive/upload/lpis/{site}', '2019-10-11 16:15:00.0+02') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('executor.module.path.extract-l4c-markers', 'Script for extracting L4C markers', 'file', true, 8, FALSE, 'Script for extracting L4C markers', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.mdb-csv-to-ipc-export', 'Script for extracting markers csv to IPC file', 'file', true, 8, FALSE, 'Script for extracting markers csv to IPC file', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-crop-type', 'L4A Crop Type main execution script path', 'file', true, 8, FALSE, 'L4A Crop Type main execution script path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s1', 'L4B S1 main execution script path', 'file', true, 8, FALSE, 'L4B S1 main execution script path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s2', 'L4B S2 main execution script path', 'file', true, 8, FALSE, 'L4B S2 main execution script path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.export-product-launcher', 'Script for exporting L4A/L4C products to shapefiles', 'file', true, 8, FALSE, 'Script for exporting L4A/L4C products to shapefiles', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.l4b_cfg_import', 'Script for importing S4C L4B config file', 'file', true, 8, FALSE, 'Script for importing S4C L4B config file', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.l4c_cfg_import', 'Script for importing S4C L4C config file', 'file', true, 8, FALSE, 'Script for importing S4C L4C config file', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.l4c_practices_export', 'Script for exported S4C L4C files', 'file', true, 8, FALSE, 'Script for exported S4C L4C files', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.l4c_practices_import', 'Script for importing S4C L4C practices file', 'file', true, 8, FALSE, 'Script for importing S4C L4C practices file', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.lpis_import', 'Script for importing S4C LPIS/GSAA file(s)', 'file', true, 8, FALSE, 'Script for importing S4C LPIS/GSAA file(s)', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.mdb3-extract-markers', 'Script for importing MDB3 markers from a TSA result', 'file', true, 8, FALSE, 'Script for importing MDB3 markers from a TSA result', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.mdb3-input-tables-extract', 'Script for preparing MDB3 input tables', 'file', true, 8, FALSE, 'Script for preparing MDB3 input tables', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.ogr2ogr', 'ogr2ogr file path', 'file', true, 8, FALSE, 'ogr2ogr file path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-extract-products', 'Script for extracting S4C L4B input products', 'file', true, 8, FALSE, 'Script for extracting S4C L4B input products', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-gen-input-shp', 'Script for generating S4C L4B input shapefile', 'file', true, 8, FALSE, 'Script for generating S4C L4B input shapefile', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-l4a-extract-parcels', 'Script for extracting S4C L4A input parcels', 'file', true, 8, FALSE, 'Script for extracting S4C L4A input parcels', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.lpis_list_columns', 'Script for extracting the column names from a shapefile', 'string', true, 8, FALSE, 'Script for extracting the column names from a shapefile', NULL) on conflict DO nothing;

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            
            _statement := $str$
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
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.input_l2a', 'The list of L2A products', 'select', FALSE, 22, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 22, TRUE, 'Start date', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 22, TRUE, 'End date', NULL, FALSE) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.check_ancestors.disabled', NULL, 'true', '2023-03-17 14:43:00.720811+00') on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('orchestrator.check_ancestors.disabled', 'Disable processor wait for inputs', 'bool', false, 1, FALSE, 'Disable processor wait for inputs', NULL) on conflict DO nothing; 
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.ndwi_enabled', NULL, 'false', '2023-03-02 17:31:06.01191+02') on conflict DO nothing; 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.brightness_enabled', NULL, 'false', '2023-03-02 17:31:06.01191+02') on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndwi_enabled', 'NDWI markers extraction enabled', 'bool', true, 26, true, 'Extract NDWI markers', NULL, true) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.brightness_enabled', 'Brightness markers extraction enabled', 'bool', true, 26, true, 'Extract Brightness markers', NULL, true)  on conflict DO nothing; 
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                -- TODO : Add gadm_tables_data.sql and s2_tile_dem_statistics.sql
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
           _statement := 'update meta set version = ''3.2.0'';';
            raise notice '%', _statement;
            execute _statement;
        end if;
    end if;

    raise notice 'complete';
end;
$migration$;

commit;


