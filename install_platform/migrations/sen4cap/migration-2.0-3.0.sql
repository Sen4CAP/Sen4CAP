begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('2.0', '3.0')) then

-- ---------------------------- TODO --------------------------------------
    -- For existing sites from 2.0 we should run something like the following after filling  the auxdata tables:
    
                -- INSERT INTO site_auxdata (site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, status_id, parameters, output)
                --         SELECT site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, 3, parameters, null -- initially the status is 3=NeedsInput
                --             FROM sp_get_auxdata_descriptor_instances(1::smallint, 1::smallint, 2021::integer);
-- ---------------------------- END TODO --------------------------------------
            
            _statement := $str$
            
                UPDATE config SET value = '/mnt/upload/{site}' WHERE key = 'site.upload-path' AND value = '/mnt/upload/{user}';
            
                -- Modified existing tables
                ALTER TABLE processor ADD COLUMN IF NOT EXISTS required BOOLEAN NOT NULL DEFAULT false;
                UPDATE processor SET required = true WHERE id IN (1, 7, 8); -- l2a, l2-s1, lpis. Others (MDB1)?

                ALTER TABLE satellite ADD COLUMN IF NOT EXISTS required BOOLEAN NOT NULL DEFAULT false;
                UPDATE satellite SET required = true WHERE id IN (1, 3); -- S2, S1

                -- New Tables
                CREATE TABLE IF NOT EXISTS auxdata_descriptor (
                    id smallint NOT NULL,
                    name character varying NOT NULL,
                    label character varying NOT NULL,
                    unique_by character varying NOT NULL,
                    CONSTRAINT auxdata_descriptor_pkey PRIMARY KEY (id),
                    CONSTRAINT check_unique_by CHECK ((((unique_by)::text = 'season'::text) OR ((unique_by)::text = 'year'::text)))
                );
            
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
                    CONSTRAINT u_auxdata_operation UNIQUE (auxdata_descriptor_id, operation_order),
                    CONSTRAINT fk_auxdata_descriptor FOREIGN KEY (auxdata_descriptor_id) REFERENCES auxdata_descriptor(id),
                    CONSTRAINT fk_auxdata_operation_processor FOREIGN KEY (processor_id) REFERENCES processor(id)
                );
            
                CREATE TABLE IF NOT EXISTS auxdata_operation_file (
                    id smallserial NOT NULL,
                    auxdata_operation_id smallint NOT NULL,
                    file_order smallint NOT NULL,
                    name character varying,
                    label character varying NOT NULL,
                    extensions character varying[],
                    required boolean DEFAULT false,
                    CONSTRAINT auxdata_operation_file_pkey PRIMARY KEY (id),
                    CONSTRAINT u_auxdata_operation_file UNIQUE (auxdata_operation_id, file_order),
                    CONSTRAINT fk_auxdata_file FOREIGN KEY (auxdata_operation_id) REFERENCES auxdata_operation(id)
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
                    CONSTRAINT fk_site_auxdata_activity_status FOREIGN KEY (status_id) REFERENCES activity_status(id),
                    CONSTRAINT fk_site_auxdata_descriptor FOREIGN KEY (auxdata_descriptor_id) REFERENCES auxdata_descriptor(id),
                    CONSTRAINT fk_site_auxdata_operation_file FOREIGN KEY (auxdata_file_id) REFERENCES auxdata_operation_file(id)
                );

                -- Functions
                DROP FUNCTION IF EXISTS insert_season_descriptors() CASCADE;
                CREATE OR REPLACE FUNCTION insert_season_descriptors()
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
                ALTER FUNCTION insert_season_descriptors()
                  OWNER TO admin;

                CREATE TRIGGER tr_season_insert
                    AFTER INSERT ON season
                    FOR EACH ROW
                    EXECUTE PROCEDURE insert_season_descriptors();

                -- Trigger before delete

                DROP FUNCTION IF EXISTS delete_season_descriptors() CASCADE;

                CREATE OR REPLACE FUNCTION delete_season_descriptors()
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
                ALTER FUNCTION delete_season_descriptors()
                  OWNER TO admin;

                CREATE TRIGGER tr_season_delete
                    BEFORE DELETE ON season
                    FOR EACH ROW
                    EXECUTE PROCEDURE delete_season_descriptors();    


                DROP FUNCTION IF EXISTS sp_get_auxdata_descriptor_instances(smallint, smallint, integer);
                CREATE OR REPLACE FUNCTION sp_get_auxdata_descriptor_instances(
                    IN _site_id smallint, 
                    IN _season_id smallint,
                    IN _year integer)
                  RETURNS TABLE(site_id smallint, auxdata_descriptor_id smallint, year integer, season_id smallint, auxdata_file_id smallint, file_name character varying, parameters json) AS
                $BODY$

                BEGIN 
                    RETURN QUERY 
                    WITH last_ops AS (
                        SELECT ao.auxdata_descriptor_id, MAX(ao.operation_order) AS operation_order 
                            FROM auxdata_operation ao
                            GROUP BY ao.auxdata_descriptor_id)
                    SELECT _site_id as site_id, d.id AS auxdata_descriptor_id, 
                        CASE WHEN d.unique_by = 'year' THEN _year ELSE null::integer END AS "year", 
                        CASE WHEN d.unique_by = 'season' THEN null::smallint ELSE _season_id END AS season_id, 
                        f.id AS auxdata_file_id, null::character varying AS "file_name",
                        (SELECT o2.parameters 
                            FROM auxdata_operation o2 
                                JOIN last_ops ON last_ops.auxdata_descriptor_id = o2.auxdata_descriptor_id AND last_ops.operation_order = o2.operation_order 
                            WHERE o2.auxdata_descriptor_id = d.id) AS parameters
                    FROM 	auxdata_descriptor d
                        JOIN auxdata_operation o ON o.auxdata_descriptor_id = d.id
                        JOIN auxdata_operation_file f ON f.auxdata_operation_id = o.id
                    WHERE d.id NOT IN (SELECT s.auxdata_descriptor_id from site_auxdata s WHERE s.auxdata_descriptor_id = d.id AND s.site_id = _site_id AND ((d.unique_by = 'year' AND s.year = _year) OR (d.unique_by = 'season' AND s.season_id = _season_id)))
                    ORDER BY d.id, f.id;
                END
                $BODY$
                  LANGUAGE plpgsql STABLE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_auxdata_descriptor_instances(smallint, smallint, integer)
                  OWNER TO admin;

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
                                AND P.site_id = $1
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

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (1, 'declaration', 'Declarations', 'year') ON conflict(id) DO UPDATE SET name = 'declaration', label = 'Declarations', unique_by = 'year';
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (2, 'l4b_config', 'L4B Configuration', 'year') ON conflict(id) DO UPDATE SET name = 'l4b_config', label = 'L4B Configuration', unique_by = 'year';
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (3, 'l4c_config', 'L4C Configuration', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_config', label = 'L4C Configuration', unique_by = 'year';
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (4, 'l4c_cc_info', 'L4C CC practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_cc_info', label = 'L4C CC practices infos', unique_by = 'year';
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (5, 'l4c_fl_info', 'L4C FL practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_fl_info', label = 'L4C FL practices infos', unique_by = 'year';
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (6, 'l4c_nfc_info', 'L4C NFC practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_nfc_info', label = 'L4C NFC practices infos', unique_by = 'year';
                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (7, 'l4c_na_info', 'L4C NA practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_na_info', label = 'L4C NA practices infos', unique_by = 'year';
                

                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (1,1,'Upload', null, null,8, null) ON conflict DO nothing ;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (1,2,'Extract','string[]','{executor.module.path.lpis_list_columns}', 8, null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (1,3,'Import', null,'{executor.module.path.lpis_import}', 8,
                            '{"parameters": [
                                { "name": "parcelColumns","label": "Parcel Columns","type": "[Ljava.lang.String;","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": 2,"value": null},
                                {"name": "holdingColumns","label": "Holding Columns","type": "[Ljava.lang.String;","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": 2,"value": null},
                                {"name": "cropCodeColumn","label": "Crop Code Column","type": "java.lang.String","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": 2,"value": null},
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;
                
                -- L4B config parameters
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (2,1,'Upload', null, null,10 , null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (2,2,'Import', null,'{executor.module.path.l4b_cfg_import}',10,
                            '{"parameters": [
                                {"name": "mowingStartDate","label": "Mowing Start Date","type": "java.util.Date","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null},
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;

                -- L4C config parameters
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (3,1,'Upload', null, null,11 , null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (3,2,'Import', null,'{executor.module.path.l4c_cfg_import}',11,
                            '{"parameters": [
                                {"name": "practices","label": "Practices","type": "[Ljava.lang.String;","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null},
                                {"name": "country","label": "Country","type": "[Ljava.lang.String;","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}, 
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;
                             
                -- L4C practices parameters
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (4,1,'Upload', null, null,11 , null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (4,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
                            '{"parameters": [
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (5,1,'Upload', null, null,11 , null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (5,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
                            '{"parameters": [
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (6,1,'Upload', null, null,11 , null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (6,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
                            '{"parameters": [
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (7,1,'Upload', null, null,11 , null) ON conflict DO nothing;
                INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (7,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
                            '{"parameters": [
                                {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
                             }') ON conflict DO nothing;
                
                
                -- Files descriptors
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'LPIS','{zip}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 1 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 2, null, 'LUT','{csv}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 1 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;
                
                -- L4B 
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'L4B Cfg','{cfg}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 2 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;

                -- L4C config 
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'L4C Cfg','{cfg}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 3 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;

                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'Practice file','{csv}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 4 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'Practice file','{csv}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 5 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'Practice file','{csv}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 6 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;
                INSERT INTO auxdata_operation_file (auxdata_operation_id,file_order,name,label,extensions,required) 		
                            (SELECT id, 1, null, 'Practice file', '{csv}', true FROM auxdata_operation WHERE auxdata_descriptor_id = 7 ORDER BY id ASC LIMIT 1) ON conflict DO nothing;

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
            
            -- Data initialization
            _statement := $str$
                
            $str$;
            raise notice '%', _statement;
            execute _statement;


            _statement := $str$
                ALTER TABLE product_details_l4c ADD COLUMN IF NOT EXISTS tl_week text, ADD COLUMN IF NOT EXISTS tl_w_start text, ADD COLUMN IF NOT EXISTS tl_w_end text;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DELETE FROM config WHERE key='executor.module.path.s4c-grassland-mowing';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-grassland-extract-products', NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/s4c-l4b-extract-products.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/s4c-l4b-extract-products.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-extract-products.use_docker', NULL, '0', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-mowing.docker_image', NULL, 'sen4cap/grassland_mowing', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/grassland_mowing';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-mowing.use_docker', NULL, '1', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '1';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-grassland-gen-input-shp',  NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/generate_grassland_mowing_input_shp.py', '2019-10-18 22:39:08.407059+02')on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/generate_grassland_mowing_input_shp.py';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;


            _statement := $str$
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
                  
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance_by_id(
                    IN _derived_product_id smallint,
                    IN _source_products_type_id json)
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
                                 quicklook_image, geog, orbit_id, tiles, downloader_history_id
                             FROM product P WHERE product_type_id IN (SELECT value::smallint FROM json_array_elements_text($2))
                             AND EXISTS (
                                SELECT product_id FROM product_provenance WHERE parent_product_id = id AND product_id = $1
                        ) $sql$;
                        -- SELECT id, product_type_id, site_id, satellite_id, name, 
                        --         full_path, created_timestamp, inserted_timestamp, job_id,
                        --         quicklook_image, geog, orbit_id, tiles, downloader_history_id
                        --     FROM product P WHERE site_id = $1 AND product_type_id in (SELECT value::smallint FROM json_array_elements_text($2))
                        --     AND EXISTS (
                        --         SELECT id FROM product_provenance PV JOIN product P ON P.id = PV.parent_product_id 
                        --         WHERE site_id = $1 AND PV.parent_product_id = id AND P.product_type_id = $3
                        --     )  $sql$;
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
                DROP FUNCTION IF EXISTS sp_get_parent_products_in_provenance(smallint, json, smallint, timestamp with time zone, timestamp with time zone);
                CREATE OR REPLACE FUNCTION sp_get_parent_products_in_provenance(
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
                             AND EXISTS (
                                SELECT product_id FROM product_provenance WHERE parent_product_id = id AND
                                       product_id IN (SELECT id FROM product WHERE site_id = $1 AND product_type_id = $3)
                        ) $sql$;
                        -- SELECT id, product_type_id, site_id, satellite_id, name, 
                        --         full_path, created_timestamp, inserted_timestamp, 
                        --         quicklook_image, geog, orbit_id, tiles, downloader_history_id
                        --     FROM product P WHERE site_id = $1 AND product_type_id in (SELECT value::smallint FROM json_array_elements_text($2))
                        --     AND EXISTS (
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
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
            
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$            
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                delete from config where key = 'processor.l2a.maja.launcher';
                delete from config_metadata where key = 'processor.l2a.maja.launcher';
                
                insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.processors_image', NULL, 'sen4x/l2a-processors:0.1', '2021-04-19 16:30:00.0') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-processors:0.1';
                insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.sen2cor_image', NULL, 'sen4x/sen2cor:2.9.0-ubuntu-20.04', '2021-04-19 16:30:00.0') on conflict DO nothing;
                insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja_image', NULL, 'sen4x/maja:3.2.2-centos-7', '2021-04-19 16:30:00.0') on conflict DO nothing;
                insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.2.0', '2021-04-19 16:30:00.0') on conflict DO nothing;
                insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.l8_align_image', NULL, 'sen4x/l2a-l8-alignment:0.1', '2021-04-19 16:30:00.0') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-l8-alignment:0.1';
                insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.dem_image', NULL, 'sen4x/l2a-dem:0.1', '2021-04-19 16:30:00.0') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/l2a-dem:0.1';

                INSERT INTO config_metadata VALUES('processor.l2a.processors_image','L2a processors image name','string',false,2, FALSE, 'L2a processors image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.sen2cor_image','Sen2Cor image name','string',false,2, FALSE, 'Sen2Cor image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.maja4_image','MAJA 4 image name','string',false,2, FALSE, 'MAJA 4 image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.maja3_image','MAJA 3 image name','string',false,2, FALSE, 'MAJA 3 image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.gdal_image','GDAL image name','string',false,2, FALSE, 'GDAL image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.l8_align_image','L8 align image name','string',false,2, FALSE, 'L8 align image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.dem_image','DEM image name','string',false,2, FALSE, 'DEM image name', NULL) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            -- FMask Upgrades
            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.extractor_image', NULL, 'sen4x/fmask_extractor:0.1', '2021-03-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/fmask_extractor:0.1';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.2.0', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.image', NULL, 'sen4x/fmask:4.2', '2021-03-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/fmask:4.2';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.cog-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.compress-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud-shadow', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.snow', NULL, '0', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.enabled', NULL, '0', '2021-02-10 15:58:31.878939+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.max-retries', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.num-workers', NULL, '2', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.output-path', NULL, '/mnt/archive/fmask_def/{site}/fmask/', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.retry-interval', NULL, '1 minute', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold', NULL, '20', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.l8', NULL, '17.5', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.s2', NULL, '20', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.working-dir', NULL, '/mnt/archive/fmask_tmp/', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.fmask.enabled', 'Controls whether to run Fmask on optical products', 'bool', false, 2) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                drop function if exists sp_start_l1_tile_processing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                drop function if exists sp_clear_pending_l1_tiles;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                drop function if exists sp_start_fmask_l1_tile_processing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                drop function if exists sp_clear_pending_fmask_tiles;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                alter table l1_tile_history add column if not exists node_id text;
                update l1_tile_history set node_id = '';
                alter table l1_tile_history alter column node_id set not null;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                alter table fmask_history add column IF NOT EXISTS node_id text;
                update fmask_history set node_id = '';
                alter table fmask_history alter column node_id set not null;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                drop function if exists sp_mark_fmask_l1_tile_failed;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
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
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DELETE FROM config WHERE key = 'processor.s4c_l4c.ndvi_data_extr_dir';
                DELETE FROM config WHERE key = 'processor.s4c_l4c.amp_data_extr_dir';
                DELETE FROM config WHERE key = 'processor.s4c_l4c.cohe_data_extr_dir';
                DELETE FROM config WHERE key = 'processor.s4c_l4c.filter_ids_path';
                DELETE FROM config WHERE key = 'processor.s4c_l4b.gen_input_shp_path';
            
            
                INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2, false, null, '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }') ON conflict(key) DO UPDATE SET values = '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }';
                
                -- L4A config_metadata updates for 3.0
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.best-s2-pix', 'Minimum number of S2 pixels for parcels to use in training', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels for parcels to use in training', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-node-size', 'Minimum node size', 'int', TRUE, 5, TRUE, 'Minimum node size', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s1-pix', 'Minimum number of S1 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S1 pixels', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s2-pix', 'Minimum number of S2 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.num-trees', 'Number of RF trees', 'int', TRUE, 5, TRUE, 'Number of RF trees', '{ "bounds": { "min": 0, "max": 1000 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 1000 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-min', 'Minimum parcels to assess a crop type', 'int', TRUE, 5, TRUE, 'Minimum parcels to assess a crop type', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-h', 'Upper threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Upper threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 5000 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-l', 'Lower threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Lower threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 5000 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-k', 'Number of SMOTE neighbours', 'int', TRUE, 5, TRUE, 'Number of SMOTE neighbours', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-target', 'Target sample count for SMOTE', 'int', TRUE, 5, TRUE, 'Target sample count for SMOTE', '{ "bounds": { "min": 0, "max": 5000 } }')  ON conflict(key) DO UPDATE SET values = '{ "bounds": { "min": 0, "max": 5000 } }';
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.mode', 'Mode', 'string', FALSE, 5, TRUE, 'Mode (both, s1-only, s2-only)', '{ "allowed_values": [{ "value": "s1-only", "display": "S1 Only" }, { "value": "s2-only", "display": "S2 only" }, { "value": "both", "display": "Both" }] }') ON conflict(key) DO UPDATE SET values = '{ "allowed_values": [{ "value": "s1-only", "display": "S1 Only" }, { "value": "s2-only", "display": "S2 only" }, { "value": "both", "display": "Both" }] }';

                -- cleanup for existing invalid values
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.lc', 'LC classes to assess', 'string', TRUE, 5, TRUE, 'LC classes to assess', null) ON conflict(key) DO UPDATE SET values = null;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-h', 'Training ratio for common crop types', 'float', TRUE, 5, TRUE, 'Training ratio for common crop types', null)  ON conflict(key) DO UPDATE SET values = null;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-l', 'Training ratio for uncommon crop types', 'float', TRUE, 5, TRUE, 'Training ratio for uncommon crop types', null)  ON conflict(key) DO UPDATE SET values = null;

                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.valid_pixels_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab02_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab03_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab04_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab05_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab06_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab07_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab08_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab8a_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab11_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab12_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', 'AMP VV/VH markers extraction enabled', 'bool', true, 26, FALSE, 'AMP VV/VH markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.valid_pixels_enabled', 'Number of valid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Number of valid pixels per parcels extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab02_enabled', 'Reflectance band B02 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B02 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab03_enabled', 'Reflectance band B03 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B03 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab04_enabled', 'Reflectance band B04 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B04 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab05_enabled', 'Reflectance band B05 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B05 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab06_enabled', 'Reflectance band B06 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B06 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab07_enabled', 'Reflectance band B07 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B07 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab08_enabled', 'Reflectance band B08 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B08 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab8a_enabled', 'Reflectance band B8A markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B8A markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab11_enabled', 'Reflectance band B11 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B11 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab12_enabled', 'Reflectance band B12 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B12 markers extraction enabled', NULL) on conflict DO nothing;
                
            
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l2a_msk', NULL, '/mnt/archive/orchestrator_temp/l2a_msk/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;


           _statement := 'update meta set version = ''3.0'';';
            raise notice '%', _statement;
            execute _statement;
        end if;
    end if;

    raise notice 'complete';
end;
$migration$;

commit;


