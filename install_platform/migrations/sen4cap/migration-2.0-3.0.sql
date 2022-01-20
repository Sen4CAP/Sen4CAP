begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('2.0', '3.0', '3.0.0')) then

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
                UPDATE processor SET required = true WHERE id IN (1, 7, 8, 21); -- l2a, l2-s1, lpis. Others (MDB1)?

                ALTER TABLE satellite ADD COLUMN IF NOT EXISTS required BOOLEAN NOT NULL DEFAULT false;
                UPDATE satellite SET required = true WHERE id IN (1, 3); -- S2, S1

                ALTER TABLE downloader_history ADD COLUMN IF NOT EXISTS product_type_id smallint;
                ALTER TABLE downloader_history ALTER COLUMN satellite_id DROP NOT NULL;

                ALTER TABLE node_resource_log ALTER COLUMN timestamp SET DATA TYPE timestamp with time zone;
                
                -- New Tables
                CREATE TABLE IF NOT EXISTS product_provenance(
                    product_id int NOT NULL,
                    parent_product_id int NOT NULL,
                    parent_product_date timestamp with time zone NOT NULL,
                    CONSTRAINT product_provenance_pkey PRIMARY KEY (product_id, parent_product_id)
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

                -- Functions
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

                -- 
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
                
                
                --
                DROP FUNCTION sp_get_job_output(integer);
                create or replace function sp_get_job_output(
                    _job_id job.id%type
                ) returns table (
                    step_name step.name%type,
                    command text,
                    stdout_text step_resource_log.stdout_text%type,
                    stderr_text step_resource_log.stderr_text%type,
                    exit_code step.exit_code%type,
                    execution_status step.status_id%type
                ) as
                $$
                begin
                    return query
                        select step.name,
                               array_to_string(array_prepend(config.value :: text, array(select json_array_elements_text(json_extract_path(step.parameters, 'arguments')))), ' ') as command,
                               step_resource_log.stdout_text,
                               step_resource_log.stderr_text,
                               step.exit_code,
                               step.status_id
                        from task
                        inner join step on step.task_id = task.id
                        left outer join step_resource_log on step_resource_log.step_name = step.name and step_resource_log.task_id = task.id
                        left outer join config on config.site_id is null and config.key = 'executor.module.path.' || task.module_short_name
                        where task.job_id = _job_id
                        order by step.submit_timestamp;
                end;
                $$
                    language plpgsql stable;

                DROP FUNCTION IF EXISTS insert_season_descriptors() CASCADE;
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

                CREATE TRIGGER tr_season_insert
                    AFTER INSERT
                    ON public.season
                    FOR EACH ROW
                    EXECUTE FUNCTION public.insert_season_descriptors();

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
                    WHERE d.id NOT IN (SELECT s.auxdata_descriptor_id from site_auxdata s WHERE s.auxdata_descriptor_id = d.id AND s.site_id = _site_id AND ((d.unique_by = 'year' AND s.year = _year) OR (d.unique_by = 'season' AND s.season_id = _season_id)))
                    ORDER BY d.id, f.id;
                END

                $BODY$;

                ALTER FUNCTION public.sp_get_auxdata_descriptor_instances(smallint, smallint, integer)
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
                
                -- Files descriptors

                -- Declarations
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (1, 1, 1, 'LPIS', '{zip}', true) on conflict DO NOTHING;
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (2, 1, 2, 'LUT', '{csv}', false) on conflict DO NOTHING;

                -- L4B config 
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (3, 2, 1, 'L4B Cfg', '{cfg}', true) on conflict(id) DO UPDATE SET extensions = '{cfg}';

                -- L4C config 
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (4, 3, 1, 'L4C Cfg','{cfg}', false) on conflict DO NOTHING;
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (5, 4, 1, 'CC Practice file','{csv}', false) on conflict(id) DO UPDATE SET label = 'CC Practice file';
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (6, 5, 1, 'FL Practice file','{csv}', false) on conflict(id) DO UPDATE SET label = 'FL Practice file';
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (7, 6, 1, 'NFC Practice file','{csv}', false) on conflict(id) DO UPDATE SET label = 'NFC Practice file';
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (8, 7, 1, 'NA Practice file', '{csv}', false) on conflict(id) DO UPDATE SET label = 'NA Practice file';
                

            -- Declarations operations
            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async)
                        VALUES (1, 1, 1, 'Upload', '{executor.module.path.lpis_list_columns}', 8, '{"parameters": [{"name": "file", "command": "-p", "type": "java.io.File", "required": true, "refFileId": 1}]}', '{"columns":"string[]"}', false) on conflict DO NOTHING;
            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) VALUES (2, 1, 2, 'Import', '{executor.module.path.lpis_import}', 8, '{"parameters": [{"name": "lpisFile", "command": "--lpis", "type": "java.io.File", "required": true, "refFileId": 1},{"name": "lutFile", "command": "--lut", "type": "java.io.File", "required": false, "refFileId": 2}, {"name": "parcelColumns","command": "--parcel-id-cols","label": "Parcel Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true}, {"name": "holdingColumns","command": "--holding-id-cols","label": "Holding Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true}, {"name": "cropCodeColumn","command": "--crop-code-col","label": "Crop Code Column","type": "java.lang.String","valueSet":["$columns"],"required": true}, {"name": "mode","command":"--mode","label": "Import Mode","type": "java.lang.String","valueSet":["update","replace","incremental"],"defaultValue":"update","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name": "lpisRootPath","label": null,"type": "java.lang.String","value":"{processor.lpis.path}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "lpisFile", "command": "--lpis", "type": "java.io.File", "required": true, "refFileId": 1},{"name": "lutFile", "command": "--lut", "type": "java.io.File", "required": false, "refFileId": 2}, {"name": "parcelColumns","command": "--parcel-id-cols","label": "Parcel Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true}, {"name": "holdingColumns","command": "--holding-id-cols","label": "Holding Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true}, {"name": "cropCodeColumn","command": "--crop-code-col","label": "Crop Code Column","type": "java.lang.String","valueSet":["$columns"],"required": true}, {"name": "mode","command":"--mode","label": "Import Mode","type": "java.lang.String","valueSet":["update","replace","incremental"],"defaultValue":"update","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name": "lpisRootPath","label": null,"type": "java.lang.String","value":"{processor.lpis.path}", "required": true}]}';

            -- L4B Config operations
            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async)
            VALUES (3, 3, 1, 'Import', '{executor.module.path.l4b_cfg_import}', 10, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 3},{"name": "mowingStartDate","label": "Mowing Start Date","type": "java.util.Date", "command":"--mowing-start-date","required": false},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "l4bCfgRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4b.cfg_dir}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 3},{"name": "mowingStartDate","label": "Mowing Start Date","type": "java.util.Date", "command":"--mowing-start-date","required": false},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "l4bCfgRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4b.cfg_dir}", "required": true}]}', async = true, name ='Import';

            -- L4C config operations
            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) 
            VALUES (4, 4, 1, 'Import', '{executor.module.path.l4c_cfg_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 4},{"name": "practices","label": "Practices","type": "java.lang.String", "command":"--practices","required": true},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "country","label": "Country","type": "java.lang.String", "command":"--country","required": true}, {"name": "l4cCfgRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 4},{"name": "practices","label": "Practices","type": "java.lang.String", "command":"--practices","required": true},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "country","label": "Country","type": "java.lang.String", "command":"--country","required": true}, {"name": "l4cCfgRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', async = true, name ='Import';

            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) 
            VALUES (5, 5, 1, 'Import', '{executor.module.path.l4c_practices_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 5},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"CC"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 5},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"CC"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', async = true, name ='Import';

            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) 
            VALUES (6, 6, 1, 'Import', '{executor.module.path.l4c_practices_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 6},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"FL"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 6},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"FL"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', async = true, name ='Import';
                
            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) 
            VALUES (7, 7, 1, 'Import', '{executor.module.path.l4c_practices_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 7},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"NFC"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 7},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"NFC"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', async = true, name ='Import';

            INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) 
            VALUES (8, 8, 1, 'Import', '{executor.module.path.l4c_practices_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 8},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"NA"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 8},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name":"practice","command":"--practice","label":null,"type":"java.lang.String","required":"true", "defaultValue":"NA"}, {"name": "l4cPracticesRootPath","label": null,"type": "java.lang.String","value":"{processor.s4c_l4c.cfg_dir}", "required": true}]}', async = true, name ='Import';

            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO product_type (id, name, description, is_raster) VALUES (1, 'l2a','L2A Atmospheric correction', true) on conflict(id) DO UPDATE SET name = 'l2a', description = 'L2A Atmospheric correction', is_raster = 'true';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (3, 'l3b','L3B product', true) on conflict(id) DO UPDATE SET name = 'l3b', description = 'L3B product', is_raster = 'true';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (7, 'l1c','L1C product', true) on conflict(id) DO UPDATE SET name = 'l1c', description = 'L1C product', is_raster = 'true';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (10, 's1_l2a_amp','Sentinel 1 L2 Amplitude product', true) on conflict(id) DO UPDATE SET name = 's1_l2a_amp', description = 'Sentinel 1 L2 Amplitude product', is_raster = 'true';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (11, 's1_l2a_cohe','Sentinel 1 L2 Coherence product', true) on conflict(id) DO UPDATE SET name = 's1_l2a_cohe', description = 'Sentinel 1 L2 Coherence product', is_raster = 'true';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (12, 's4c_l4a','Sen4CAP L4A Crop type product', false) on conflict(id) DO UPDATE SET name = 's4c_l4a', description = 'Sen4CAP L4A Crop type product', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (13, 's4c_l4b','Sen4CAP L4B Grassland Mowing product', false) on conflict(id) DO UPDATE SET name = 's4c_l4b', description = 'Sen4CAP L4B Grassland Mowing product', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (14, 'lpis', 'LPIS product', false) on conflict(id) DO UPDATE SET name = 'lpis', description = 'LPIS product', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (15, 's4c_l4c','Sen4CAP L4C Agricultural Practices product', false) on conflict(id) DO UPDATE SET name = 's4c_l4c', description = 'Sen4CAP L4C Agricultural Practices product', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (17, 's4c_mdb1','Sen4CAP Marker Database Basic StdDev/Mean', false) on conflict(id) DO UPDATE SET name = 's4c_mdb1', description = 'Sen4CAP Marker Database Basic StdDev/Mean', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (18, 's4c_mdb2','Sen4CAP Marker Database AMP VV/VH Ratio', false) on conflict(id) DO UPDATE SET name = 's4c_mdb2', description = 'Sen4CAP Marker Database AMP VV/VH Ratio', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (19, 's4c_mdb3','Sen4CAP Marker Database L4C M1-M5', false) on conflict(id) DO UPDATE SET name = 's4c_mdb3', description = 'Sen4CAP Marker Database L4C M1-M5', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (20, 's4c_mdb_l4a_opt_main','Sen4CAP L4A Optical Main Features', false) on conflict(id) DO UPDATE SET name = 's4c_mdb_l4a_opt_main', description = 'Sen4CAP L4A Optical Main Features', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (21, 's4c_mdb_l4a_opt_re','Sen4CAP L4A Optical Red-Edge Features', false) on conflict(id) DO UPDATE SET name = 's4c_mdb_l4a_opt_re', description = 'Sen4CAP L4A Optical Red-Edge Features', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (22, 's4c_mdb_l4a_sar_main','Sen4CAP L4A SAR Main Features', false) on conflict(id) DO UPDATE SET name = 's4c_mdb_l4a_sar_main', description = 'Sen4CAP L4A SAR Main Features', is_raster = 'false';
                INSERT INTO product_type (id, name, description, is_raster) VALUES (23, 's4c_mdb_l4a_sar_temp','Sen4CAP L4A SAR Temporal Features', false) on conflict(id) DO UPDATE SET name = 's4c_mdb_l4a_sar_temp', description = 'Sen4CAP L4A SAR Temporal Features', is_raster = 'false';
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
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-mowing.docker_image', NULL, 'sen4cap/grassland_mowing:3.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/grassland_mowing:3.0.0';
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
                INSERT INTO config_metadata VALUES('processor.l2a.maja_image','MAJA image name','string',false,2, FALSE, 'MAJA image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.sen2cor_image','Sen2Cor image name','string',false,2, FALSE, 'Sen2Cor image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.gdal_image','GDAL image name','string',false,2, FALSE, 'GDAL image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.l8_align_image','L8 align image name','string',false,2, FALSE, 'L8 align image name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES('processor.l2a.dem_image','DEM image name','string',false,2, FALSE, 'DEM image name', NULL) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;
                INSERT INTO config_category VALUES (8, 'Executor', 8, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (12, 'Dashboard', 9, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (13, 'Monitoring Agent', 10, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (14, 'Resources', 11, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (17, 'Site', 14, false) ON conflict(id) DO UPDATE SET allow_per_site_customization = false;
                INSERT INTO config_category VALUES (19, 'S4C L4B Grassland Mowing', 15, true) ON conflict(id) DO UPDATE SET name = 'S4C L4B Grassland Mowing';
                INSERT INTO config_category VALUES (20, 'S4C L4C Agricultural Practices', 16, true) ON conflict(id) DO UPDATE SET name = 'S4C L4C Agricultural Practices';
                INSERT INTO config_category VALUES (22, 'S4C L4A Crop Type', 22, true) ON conflict(id) DO UPDATE SET name = 'S4C L4A Crop Type';
                INSERT INTO config_category VALUES (23, 'S1 L2 Pre-processor', 23, true) on conflict DO nothing;
                INSERT INTO config_category VALUES (26, 'S4C Markers Database 1', 26, true)  ON conflict(id) DO UPDATE SET name = 'S4C Markers Database 1';
            _statement := $str$
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            -- FMask Upgrades
            _statement := $str$
            
                -- TODO - Not installed in version 3.0. To be inserted in a future version

                DELETE FROM config_category WHERE id = 31;
                DELETE FROM product_type WHERE id = 25;
                DELETE FROM processor WHERE id = 15;
                
                DELETE FROM config WHERE key IN ('processor.fmask.extractor_image','processor.fmask.gdal_image','processor.fmask.image', 'processor.fmask.optical.cog-tiffs', 'processor.fmask.optical.compress-tiffs', 'processor.fmask.optical.dilation.cloud', 'processor.fmask.optical.dilation.cloud-shadow', 'processor.fmask.optical.dilation.snow', 'processor.fmask.enabled', 'processor.fmask.optical.max-retries', 'processor.fmask.optical.num-workers', 'processor.fmask.optical.output-path', 'processor.fmask.optical.retry-interval', 'processor.fmask.optical.threshold', 'processor.fmask.optical.threshold.l8', 'processor.fmask.optical.threshold.s2', 'processor.fmask.working-dir');    
            
                DELETE FROM config_metadata WHERE key IN ('processor.fmask.extractor_image','processor.fmask.gdal_image','processor.fmask.image', 'processor.fmask.optical.cog-tiffs', 'processor.fmask.optical.compress-tiffs', 'processor.fmask.optical.dilation.cloud', 'processor.fmask.optical.dilation.cloud-shadow', 'processor.fmask.optical.dilation.snow', 'processor.fmask.enabled', 'processor.fmask.optical.max-retries', 'processor.fmask.optical.num-workers', 'processor.fmask.optical.output-path', 'processor.fmask.optical.retry-interval', 'processor.fmask.optical.threshold', 'processor.fmask.optical.threshold.l8', 'processor.fmask.optical.threshold.s2', 'processor.fmask.working-dir');

                -- INSERT INTO product_type (id, name, description, is_raster) VALUES (25, 'fmask','Fmask mask product', true) on conflict DO nothing;

                -- INSERT INTO config_category VALUES (31, 'FMask', 18, true) on conflict DO nothing;
                
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.extractor_image', NULL, 'sen4x/fmask_extractor:0.1', '2021-03-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/fmask_extractor:0.1';
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.2.0', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.image', NULL, 'sen4x/fmask:4.2', '2021-03-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/fmask:4.2';
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.cog-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.compress-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud-shadow', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.snow', NULL, '0', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.enabled', NULL, '0', '2021-02-10 15:58:31.878939+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.max-retries', NULL, '3', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.num-workers', NULL, '2', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.output-path', NULL, '/mnt/archive/fmask_def/{site}/fmask/', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.retry-interval', NULL, '1 minute', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold', NULL, '20', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.l8', NULL, '17.5', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.s2', NULL, '20', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.working-dir', NULL, '/mnt/archive/fmask_tmp/', '2021-03-18 14:43:00.720811+00') on conflict DO nothing;
                -- 
                -- INSERT INTO config_metadata VALUES ('processor.fmask.enabled', 'Controls whether to run Fmask on optical products', 'bool', false, 31, FALSE, 'Controls whether to run Fmask on optical products', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.extractor_image', 'FMask extractor docker image name', 'string', false, 31, FALSE, 'FMask extractor docker image name', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.gdal_image', 'gdal docker image for FMask', 'string', false, 31, FALSE, 'gdal docker image for FMask', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.image', 'FMask docker image', 'string', false, 31, FALSE, 'FMask docker image', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.cog-tiffs', 'Output rasters as COG', 'bool', false, 31, FALSE, 'Output rasters as COG', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.compress-tiffs', 'Compress output rasters', 'bool', false, 31, FALSE, 'Compress output rasters', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.cloud-shadow', 'Cloud shaddow dilation percent', 'int', false, 31, FALSE, 'Cloud shaddow dilation percent', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.cloud', 'Cloud dilation percent', 'int', false, 31, FALSE, 'Cloud dilation percent', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.snow', 'Snow dilation percent', 'int', false, 31, FALSE, 'Snow dilation percent', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.max-retries', 'Maximum number of retries for a product', 'int', false, 31, FALSE, 'Maximum number of retries for a product', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.num-workers', 'Number of workers', 'int', false, 31, FALSE, 'Number of workers', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.output-path', 'Output path', 'string', false, 31, FALSE, 'Output path', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.retry-interval', 'Retry interval', 'int', false, 31, FALSE, 'Retry interval', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold.l8', 'Threshold for L8', 'int', false, 31, FALSE, 'Threshold for L8', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold.s2', 'Threshold for S2', 'int', false, 31, FALSE, 'Threshold for S2', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold', 'Global threshold', 'int', false, 31, FALSE, 'Global threshold', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.fmask.working-dir', 'Working directory', 'string', false, 31, FALSE, 'Working directory', NULL) ON conflict(key) DO UPDATE SET type = 'string';
                
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
                -- FUNCTION: reports.sp_get_s1_statistics(smallint)
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$            
                -- FUNCTION: reports.sp_reports_s1_statistics(smallint, integer, date, date)
                
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
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
                DELETE FROM config WHERE key = 'processor.s4c_l4c.ndvi_data_extr_dir';
                DELETE FROM config WHERE key = 'processor.s4c_l4c.amp_data_extr_dir';
                DELETE FROM config WHERE key = 'processor.s4c_l4c.cohe_data_extr_dir';
                DELETE FROM config WHERE key = 'processor.s4c_l4b.gen_input_shp_path';
            
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.filter_ids_path', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/ts_input_tables/FilterIds/Sen4CAP_L4C_FilterIds.csv', '2019-10-18 15:27:41.861613+02') on conflict do nothing;
            
                INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2, false, 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }') ON conflict(key) DO UPDATE SET label = 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', values = '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }';
                
                -- L4A config_metadata updates for 3.0
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.best-s2-pix', 'Minimum number of S2 pixels for parcels to use in training', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels for parcels to use in training', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-node-size', 'Minimum node size', 'int', TRUE, 5, TRUE, 'Minimum node size', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s1-pix', 'Minimum number of S1 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S1 pixels', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s2-pix', 'Minimum number of S2 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.num-trees', 'Number of RF trees', 'int', TRUE, 5, TRUE, 'Number of RF trees', '{ "bounds": { "min": 0, "max": 1000 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 1000 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-min', 'Minimum parcels to assess a crop type', 'int', TRUE, 5, TRUE, 'Minimum parcels to assess a crop type', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-h', 'Upper threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Upper threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 5000 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-l', 'Lower threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Lower threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 5000 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-k', 'Number of SMOTE neighbours', 'int', TRUE, 5, TRUE, 'Number of SMOTE neighbours', '{ "bounds": { "min": 0, "max": 100 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 100 } }';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-target', 'Target sample count for SMOTE', 'int', TRUE, 5, TRUE, 'Target sample count for SMOTE', '{ "bounds": { "min": 0, "max": 5000 } }')  ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "bounds": { "min": 0, "max": 5000 } }';
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.mode', 'Mode', 'string', FALSE, 5, TRUE, 'Mode (both, s1-only, s2-only)', '{ "allowed_values": [{ "value": "s1-only", "display": "S1 Only" }, { "value": "s2-only", "display": "S2 only" }, { "value": "both", "display": "Both" }] }') ON conflict(key) DO UPDATE SET config_category_id = 22, values = '{ "allowed_values": [{ "value": "s1-only", "display": "S1 Only" }, { "value": "s2-only", "display": "S2 only" }, { "value": "both", "display": "Both" }] }';

                -- cleanup for existing invalid values
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.lc', 'LC classes to assess', 'string', TRUE, 5, TRUE, 'LC classes to assess', null) ON conflict(key) DO UPDATE SET config_category_id = 22, values = null;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-h', 'Training ratio for common crop types', 'float', TRUE, 5, TRUE, 'Training ratio for common crop types', null)  ON conflict(key) DO UPDATE SET config_category_id = 22, values = null;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-l', 'Training ratio for uncommon crop types', 'float', TRUE, 5, TRUE, 'Training ratio for uncommon crop types', null)  ON conflict(key) DO UPDATE SET config_category_id = 22, values = null;

                
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
                UPDATE product_type SET is_raster = false WHERE name = 'lpis';
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DELETE FROM product_type WHERE name = 's4c_l3c';
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                DELETE FROM config WHERE key = 'general.scratch-path.l2a_msk';
                DELETE FROM config WHERE key = 'processor.l2a_msk.enabled';
                DELETE FROM config_metadata WHERE key = 'general.scratch-path.l2a_msk';
                DELETE FROM config_metadata WHERE key = 'processor.l2a_msk.enabled';
                DELETE FROM config_category WHERE id = 27;
                DELETE FROM processor WHERE short_name = 'l2a_msk';
                DELETE FROM product_type WHERE name = 'l2a_msk';
                
                -- TODO - Not installed in version 3.0. To be inserted in a future version
                -- INSERT INTO processor (id, name, short_name, label) VALUES (15, 'Validity flags','l2a_msk', 'Validity flags') on conflict DO nothing;
                -- INSERT INTO product_type (id, name, description, is_raster) VALUES (26, 'l2a_msk','L2A product with validity mask', true) on conflict DO nothing;
                -- INSERT INTO config_category VALUES (27, 'Validity Flags', 17, true) on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l2a_msk', NULL, '/mnt/archive/orchestrator_temp/l2a_msk/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.enabled', NULL, 'true', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('general.scratch-path.l2a_msk', 'Path for Masked L2A temporary files', 'string', false, 27, FALSE, 'Path for Masked L2A temporary files', NULL) on conflict DO nothing;
                -- INSERT INTO config_metadata VALUES ('processor.l2a_msk.enabled', 'Enable or disable the validity flags', 'bool', false, 27, FALSE, 'Enable or disable the validity flags', NULL) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.0.0', '2021-01-14 12:11:21.800537+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/processors:3.0.0';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcel_id_col_name', NULL, 'NewID', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_csv_file_name_pattern', NULL, 'decl_.*_\d{4}.csv', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_optical_file_name_pattern', NULL, '.*_buf_5m.shp', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_sar_file_name_pattern', NULL, '.*_buf_10m.shp', '2019-10-11 16:15:00.0+02') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_ndwi', NULL, '0', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_brightness', NULL, '0', '2017-10-24 14:56:57.501918+02') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.mdb3-input-tables-extract.docker_image', NULL, 'sen4cap/data-preparation:0.1', '2021-02-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.mdb3-input-tables-extract.use_docker', NULL, '1', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb3-input-tables-extract', NULL, 's4c_mdb3_input_tables.py', '2021-01-15 22:39:08.407059+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb3-extract-markers', NULL, 'extract_mdb3_markers.py', '2021-01-15 22:39:08.407059+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.mdb3_enabled', NULL, 'false', '2021-10-01 17:31:06.01191+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.mdb3_input_tables', NULL, '/mnt/archive/marker_database_files/mdb1/{site}/{year}/input_tables.csv', '2021-05-16 17:31:06.01191+02') on conflict DO nothing;

                -- delete keys needed
                DELETE from config WHERE key = 'extract.histogram';
                
                DELETE from config WHERE key = 'executor.module.path.extract-l4c-markers';
                DELETE from config WHERE key = 'general.orchestrator.mdb-csv-to-ipc-export.use_docker';
                DELETE from config WHERE key = 'executor.module.path.extract-l4c-markers';
                DELETE from config WHERE key = 'general.orchestrator.extract-l4c-markers.use_docker';
                
                DELETE from config where key = 'processor.s4c_mdb1.input_ndvi';
                DELETE from config where key = 'processor.s4c_l4b.input_ndvi';
                DELETE from config where key = 'processor.s4c_l4c.input_ndvi';
                DELETE from config where key = 'processor.s4c_l4b.gen_shp_py_script';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_l3b', NULL, 'N/A', '2019-02-19 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.input_l3b', NULL, 'N/A', '2019-02-19 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.input_l3b', NULL, 'N/A', '2019-02-19 11:09:43.978921+02') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.nrt_data_extr_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb-csv-to-ipc-export', NULL, 'csv_to_ipc.py', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'csv_to_ipc.py';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config_metadata VALUES ('mail.message.batch.limit', 'Batch limit of mail message', 'int', false, 1, FALSE, 'Batch limit of mail message', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4b.keep_job_folders', 'Keep S4C L4B temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4B temporary product files for the orchestrator jobs', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4c.keep_job_folders', 'Keep S4C L4C temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4C temporary product files for the orchestrator jobs', NULL) on conflict DO nothing;
            
                INSERT INTO config_metadata VALUES ('downloader.l8.query.days.back', 'Number of back days for L8 download', 'int', false, 15, FALSE, 'Number of back days for L8 download', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.s1.query.days.back', 'Number of back days for S1 download', 'int', false, 15, FALSE, 'Number of back days for S1 download', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.s2.query.days.back', 'Number of back days for S2 download', 'int', false, 15, FALSE, 'Number of back days for S2 download', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.L8.query.days.back', 'Number of back days for L8 download', 'int', false, 15, FALSE, 'Number of back days for L8 download', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.S1.query.days.back', 'Number of back days for S1 download', 'int', false, 15, FALSE, 'Number of back days for S1 download', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.S2.query.days.back', 'Number of back days for S2 download', 'int', false, 15, FALSE, 'Number of back days for S2 download', NULL) on conflict DO nothing;
                
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

                INSERT INTO config_metadata VALUES ('scheduled.reports.enabled', 'Reports scheduler enabled', 'bool', false, 15, FALSE, 'Reports scheduler enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('scheduled.reports.interval', 'Interval for reports update scheduling', 'int', false, 15, FALSE, 'Interval for reports update scheduling', NULL) on conflict DO nothing;
                
                
                INSERT INTO config_metadata VALUES ('processor.l2s1.compute.amplitude', 'Compute amplitude', 'bool', false, 23, FALSE, 'Compute amplitude', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.compute.coherence', 'Compute coherence', 'bool', false, 23, FALSE, 'Compute coherence', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.parallelism', 'GPT parallelism', 'int', false, 23, FALSE, 'GPT parallelism', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.tile.cache.size', 'GPT tile cache size', 'int', false, 23, FALSE, 'GPT tile cache size', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.join.amplitude.steps', 'Join amplitude steps', 'bool', false, 23, FALSE, 'Join amplitude steps', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.join.coherence.steps', 'Join coherence steps', 'bool', false, 23, FALSE, 'Join coherence steps', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndwi', 'L3B processor will produce NDWI', 'int', false, 4, FALSE, 'Produce NDVI', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_brightness', 'L3B processor will produce brightness', 'int', false, 4, FALSE, 'Produce brightness', NULL) on conflict DO nothing;
                
                DELETE from config_metadata where key = 'processor.s4c_mdb1.input_ndvi';
                DELETE from config_metadata where key = 'processor.s4c_l4b.input_ndvi';
                DELETE from config_metadata where key = 'processor.s4c_l4c.input_ndvi';
               
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_l3b', 'The list of L3B products', 'select', FALSE, 19, FALSE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_l3b', 'The list of L3B products', 'select', FALSE, 20, FALSE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_l3b', 'The list of L3B products', 'select', FALSE, 26, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}';
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_amp', 'The list of AMP products', 'select', FALSE, 19, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}'; 
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_amp', 'The list of AMP products', 'select', FALSE, 20, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_amp', 'The list of AMP products', 'select', FALSE, 26, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}';
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_cohe', 'The list of COHE products', 'select', FALSE, 19, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}';
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_cohe', 'The list of COHE products', 'select', FALSE, 20, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}';
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_cohe', 'The list of COHE products', 'select', FALSE, 26, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}') ON conflict(key) DO UPDATE SET values = '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}';

                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.cfg_dir', 'Config files directory', 'string', FALSE, 19, FALSE, 'Config files directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.cfg_upload_dir', 'Site upload files directory', 'string', FALSE, 19, FALSE, 'Site upload files directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_product_types', 'Input product types', 'string', FALSE, 19, FALSE, 'Input product types', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s1_py_script', 'Script for S1 detection', 'string', FALSE, 19, FALSE, 'Script for S1 detection', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s2_py_script', 'Script for S2 detection', 'string', FALSE, 19, FALSE, 'Script for S1 detection', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.default_config_path', 'The default configuration files for all L4B processors', 'file', FALSE, 19, FALSE, 'The default configuration files for all L4B processors', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.year', 'Current L4B processing year for site', 'int', FALSE, 19, FALSE, 'Current L4B processing year for site', NULL) on conflict DO nothing;

                -- TODO: See if these 2 are used, if not, they should be removed
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.sub_steps', 'Substeps', 'string', FALSE, 19, FALSE, 'Substeps', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4b.working_dir', 'Working directory', 'string', FALSE, 19, FALSE, 'Working directory', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.default_config_path', 'The default configuration files for all L4C processors', 'file', FALSE, 20, FALSE, 'The default configuration files for all L4C processors', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.cfg_dir', 'Config files directory', 'string', FALSE, 20, FALSE, 'Config files directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.cfg_upload_dir', 'Site upload files directory', 'string', FALSE, 20, FALSE, 'Site upload files directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.country', 'Site country', 'string', FALSE, 20, FALSE, 'Site country', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.data_extr_dir', 'Data extraction directory', 'string', FALSE, 20, FALSE, 'Data extraction directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.execution_operation', 'Execution operation', 'string', FALSE, 20, FALSE, 'Execution operation', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.filter_ids_path', 'Filtering parcels ids file', 'string', FALSE, 20, FALSE, 'Filtering parcels ids file', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.nrt_data_extr_enabled', 'NRT data extration enabled', 'int', FALSE, 20, FALSE, 'NRT data extration enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.practices', 'Configured practices list', 'string', FALSE, 20, FALSE, 'Configured practices list', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.sub_steps', 'Substeps', 'string', FALSE, 20, FALSE, 'Substeps', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tillage_monitoring', 'Enable tillage monitoring', 'int', FALSE, 20, TRUE, 'Enable tillage monitoring', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tsa_min_acqs_no', 'TSA min number of acquisitions', 'int', FALSE, 20, FALSE, 'TSA min number of acquisitions', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.ts_input_tables_dir', 'TSA input tables directory', 'string', FALSE, 20, FALSE, 'TSA input tables directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.ts_input_tables_upload_root_dir', 'Input tables upload root directory', 'string', FALSE, 20, FALSE, 'Input tables upload root directory', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.use_prev_prd', 'Use previous TSA', 'int', FALSE, 20, FALSE, 'Use previous TSA', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4c.year', 'Current L4C processing year for site', 'int', FALSE, 20, FALSE, 'Current L4C processing year for site', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_mdb1.keep_job_folders', 'Keep MDB1 temporary product files for the orchestrator jobs', 'string', true, 8, FALSE, 'Keep MDB1 temporary product files for the orchestrator jobs', NULL)  on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('general.orchestrator.use_docker', 'Orchestrator use docker when invoking processors', 'int', false, 1, FALSE, 'Use docker when invoking processors', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.docker_image', 'Processors docker image', 'string', false, 1, FALSE, 'Processors docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.docker_add_mounts', 'Additional docker mounts for processors', 'string', false, 1, FALSE, 'Additional docker mounts for processors', NULL) on conflict DO nothing;
                -- TODO: The next ones should be moved in the sections corresponding to processors
                INSERT INTO config_metadata VALUES ('general.orchestrator.export-product-launcher.use_docker', 'Export product use docker', 'int', false, 1, FALSE, 'Export product use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.mdb3-input-tables-extract.docker_image', 'MDB3 input tables extraction docker image', 'string', false, 1, FALSE, 'MDB3 input tables extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.mdb3-input-tables-extract.use_docker', 'MDB3 input tables extraction use docker', 'int', false, 1, FALSE, 'MDB3 input tables extraction use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-extract-products.use_docker', 'S4C L4B products extraction use docker', 'int', false, 1, FALSE, 'S4C L4B products extraction use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-gen-input-shp.use_docker', 'S4C L4B inputs shp extraction use docker', 'int', false, 1, FALSE, 'S4C L4B inputs shp extraction use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-mowing.docker_image', 'S4C L4B docker image', 'string', false, 1, FALSE, 'S4C L4B docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-mowing.use_docker', 'S4C L4B use docker', 'int', false, 1, FALSE, 'S4C L4B use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-l4a-extract-parcels.use_docker', 'S4C L4A extract parcels use docker', 'int', false, 1, FALSE, 'S4C L4A extract parcels use docker', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('site.path', 'Site path', 'file', false, 17, FALSE, 'Site path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('site.url', 'Site url', 'string', false, 17, FALSE, 'Site url', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('gdal.apps.path', 'Gdal applications path', 'string', false, 1, FALSE, 'Gdal applications path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('gdal.installer', 'Gdal installer', 'string', false, 1, FALSE, 'Gdal installer', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_csv_file_name_pattern', 'Parcels product csv file name pattern', 'string', false, 1, FALSE, 'Parcels product csv file name pattern', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_optical_file_name_pattern', 'Parcels product optical file name pattern', 'string', false, 1, FALSE, 'Parcels product optical file name pattern', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_sar_file_name_pattern', 'Parcels product SAR file name pattern', 'string', false, 1, FALSE, 'Parcels product SAR file name pattern', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.parcels_product.parcel_id_col_name', 'Parcels parcels id columns name', 'string', false, 1, FALSE, 'Parcels parcels id columns name', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_enabled', 'MDB3 markers extraction enabled', 'bool', true, 26, FALSE, 'MDB3 markers extraction enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_input_tables', 'MDB3 input tables location', 'string', true, 26, FALSE, 'MDB3 input tables location', NULL) ON conflict(key) DO UPDATE SET type = 'string', values = null;

                INSERT INTO config_metadata VALUES ('processor.lpis.path', 'The path to the pre-processed LPIS products', 'string', false, 21, FALSE, 'The path to the pre-processed LPIS products', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.lpis.lut_upload_path', 'Site LUT upload path', 'string', false, 21, FALSE, 'Site LUT upload path', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.lpis.upload_path', 'Site LPIS upload path', 'string', false, 21, FALSE, 'Site LPIS upload path', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('disk.monitor.interval', 'Disk Monitor interval', 'int', false, 13, FALSE, 'Disk Monitor interval', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('downloader.query.timeout', 'Download query timeout', 'int', false, 15, FALSE, 'Download query timeout', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.l2s1.enabled', 'S1 pre-processing enabled', 'bool', false, 23, FALSE, 'S1 pre-processing enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.parallelism', 'Tiles to classify in parallel', 'int', false, 23, FALSE, 'Tiles to classify in parallel', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.path', 'The path where the S1 L2 products will be created', 'string', false, 23, FALSE, 'The path where the S1 L2 products will be created', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.offset', 'S1 pre-processor offset', 'int', false, 23, FALSE, 'S1 pre-processor offset', NULL)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.work.dir', 'The path where to create the temporary S1 L2A files', 'string', false, 23, FALSE, 'The path where to create the temporary S1 L2A files', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.l2s1.compute.amplitude', 'Compute amplitude', 'bool', false, 23, FALSE, 'Compute amplitude', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.compute.coherence', 'Compute coherence', 'bool', false, 23, FALSE, 'Compute coherence', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.parallelism', 'GPT parallelism', 'int', false, 23, FALSE, 'GPT parallelism', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.tile.cache.size', 'GPT tile cache size', 'int', false, 23, FALSE, 'GPT tile cache size', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.join.amplitude.steps', 'Join amplitude steps', 'bool', false, 23, FALSE, 'Join amplitude steps', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.join.coherence.steps', 'Join coherence steps', 'bool', false, 23, FALSE, 'Join coherence steps', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('dem.name', 'DEM name', 'string', false, 23, FALSE, 'DEM name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('primary.sensor', 'Primary sensor', 'string', false, 23, FALSE, 'Primary sensor', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.acquisition.delay', 'Acquisition delay', 'int', false, 23, FALSE, 'Acquisition delay', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.copy.locally', 'Copy input products locally', 'bool', false, 23, FALSE, 'Copy input products locally', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.crop.nodata', 'Crop NODATA', 'bool', false, 23, FALSE, 'Crop NODATA', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.crop.output', 'Crop output', 'bool', false, 23, FALSE, 'Crop output', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.extract.histogram', 'Extract histogram', 'bool', false, 23, FALSE, 'Extract histogram', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.interval', 'Interval', 'int', false, 23, FALSE, 'Interval', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.keep.intermediate', 'Keep intermediate files', 'bool', false, 23, FALSE, 'Keep intermediate files', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.master', 'S1 master name', 'string', false, 23, FALSE, 'S1 master name', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.intersection', 'Minimum intersection', 'float', false, 23, FALSE, 'Minimum intersection', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.output.extension', 'Output extension', 'string', false, 23, FALSE, 'Output extension', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.output.format', 'Output format', 'string', false, 23, FALSE, 'Output format', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.overwrite.existing', 'Overwrite existing products', 'bool', false, 23, FALSE, 'Overwrite existing products', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.parallel.steps.enabled', 'Parallel steps enabled', 'bool', false, 23, FALSE, 'Parallel steps enabled', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.pixel.spacing', 'Pixel spacing', 'float', false, 23, FALSE, 'Pixel spacing', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.polarisations', 'Polarisations', 'string', false, 23, FALSE, 'Polarisations', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.process.newest', 'Process newest first', 'bool', false, 23, FALSE, 'Process newest first', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.projection', 'Projection', 'string', false, 23, FALSE, 'Projection', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.resolve.links', 'Resolve links', 'bool', false, 23, FALSE, 'Resolve links', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.step.timeout', 'Step timeout', 'int', false, 23, FALSE, 'Step timeout', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.filter.interval', 'Temporal filter interval', 'int', false, 23, FALSE, 'Temporal filter interval', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.version', 'Processor version', 'string', false, 23, FALSE, 'Processor version', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.memory', 'Minimum memory to run step', 'string', false, 23, FALSE, 'Minimum memory to run step', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.l2s1.min.disk', 'Minimum disk to run step', 'string', false, 23, FALSE, 'Minimum disk to run step', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('downloader.timeout', 'Timeout between download retries ', 'int', false, 15, FALSE, 'Timeout between download retries ', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.skip.existing', 'If enabled, products downloaded for another site will be duplicated, in database only, for the current site', 'bool', false, 15, FALSE, 'Use products already downloaded on another site', NULL) on conflict DO nothing;
                        
                -- Force using it from docker otherwith it will use the script existing in /usr/bin 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lpis_import', NULL, 'data-preparation.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'data-preparation.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lpis_list_columns', NULL, 'read_shp_cols.py', '2021-01-15 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'read_shp_cols.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4b_cfg_import', NULL, 's4c_l4b_import_config.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_l4b_import_config.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_cfg_import', NULL, 's4c_l4c_import_config.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_l4c_import_config.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_practices_export', NULL, '/usr/bin/s4c_l4c_export_all_practices.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/bin/s4c_l4c_export_all_practices.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_practices_import', NULL, 's4c_l4c_import_practice.py', '2019-10-22 22:39:08.407059+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4c_l4c_import_practice.py';
                
                INSERT INTO config_metadata VALUES ('executor.module.path.lpis_list_columns', 'Script for extracting the column names from a shapefile', 'string', true, 8, FALSE, 'Script for extracting the column names from a shapefile', NULL) on conflict DO nothing;
                        
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

            -- Update site_auxdata for existing sites
            _statement := $str$
                WITH seasons AS (
                        SELECT 
                            season.id AS season_id,
                            season.site_id AS site_id,
                            DATE_PART('year', season.start_date)::integer as season_year
                        FROM season)
                INSERT INTO site_auxdata (site_id, auxdata_descriptor_id, year, season_id, auxdata_file_id, file_name, status_id, parameters, output)
                    SELECT f.site_id, f.auxdata_descriptor_id, f.year, f.season_id, f.auxdata_file_id, f.file_name, 3, f.parameters, null -- initially the status is 3=NeedsInput
                        FROM seasons s, lateral sp_get_auxdata_descriptor_instances(s.site_id, s.season_id::smallint, s.season_year::integer) f ;   
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


