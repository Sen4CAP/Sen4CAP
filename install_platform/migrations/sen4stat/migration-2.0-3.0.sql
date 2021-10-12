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
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s_perm_crop.use_docker', NULL, '1', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '1';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s_perm_crop.docker_image', NULL, 'sen4x/otb:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/otb:1.0.0';
                -- for the gdal_sieve we override the docker image with the osgeo/gdal image
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-sieve.docker_image', NULL, 'osgeo/gdal:ubuntu-full-3.2.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'osgeo/gdal:ubuntu-full-3.2.0';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-extract-inputs.use_docker', NULL, '0', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-build-refl-stack-tif.use_docker', NULL, '0', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-samples-rasterization.use_docker', NULL, '0', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-run-broceliande.use_docker', NULL, '0', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-extract-inputs', NULL, '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-extract-inputs.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-extract-inputs.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-build-refl-stack-tif', NULL, '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-build-refl-stack.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-build-refl-stack.py';                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-samples-rasterization', NULL, '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-rasterization.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-rasterization.py';                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-sieve', NULL, '/usr/bin/gdal_sieve.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/bin/gdal_sieve.py';                

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-run-broceliande', NULL, '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-run-broceliande.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/usr/share/sen2agri/S4S_Permanent_Crops/Bin/s4s-perm-crops-run-broceliande.py';                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_perm_crop.broceliande-docker-image', NULL, 'registry.gitlab.inria.fr/obelix/broceliande/develop:2.4.20200614', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'registry.gitlab.inria.fr/obelix/broceliande/develop:2.4.20200614';
                
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


