begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('3.2.0', '3.3.0', '4.0.0')) then

            _statement := $str$
                INSERT INTO config_category VALUES (36, 'Parcel heterogeneity', 36, true) on conflict DO nothing;
                INSERT INTO config_category VALUES (37, 'Bare soil detection', 37, true) on conflict DO nothing;
                INSERT INTO config_category VALUES (40, 'S4C L4 Change Detection', 40, true) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            
            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES 
                (25, 'Parcel Heterogeneity','s4c_heterog', 'Parcel Heterogeneity', true, '{1,2,3}', null, true, false, true, false) on conflict DO nothing;
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                (26, 'Bare soil detection','s4c_bare_soil', 'Bare soil detection', true, '{1,2,3}', '{1}', true, false, true, false) on conflict DO nothing;
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES
                (28, 'L4 Change Detection','s4c_change_detection', 'L4 Change Detection', false, '{1,2,3}', null, true, false, true, false) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('l9.enabled', NULL, 'false', '2024-02-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_sar_file_name_pattern', NULL, '.*_(\d{4,5})_buf_10m.shp', '2019-10-11 16:15:00.0+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '.*_(\d{4,5})_buf_10m.shp';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.docker_script_unit_image', NULL, 'sen4cap/data-preparation:0.3', '2023-11-16 20:05:00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.sacct-max-retries', NULL, '1', '2023-10-26 17:03:39.541136+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.3.0', '2021-01-14 12:11:21.800537+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/processors:3.3.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-crop-type-mapping.docker_image', NULL, 'sen4x/crop-map-s4s:0.2.0', '2022-08-22 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/crop-map-s4s:0.2.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.temporal.filter.interval', NULL, '24', '2022-09-30 10:31:00.501+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '24';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.container', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.domain', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.password', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.projectId', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.url', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.user', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('primary.sensor', NULL, 'S2', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.enabled', NULL, 'false', '2021-05-18 17:54:17.288095+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.tiled', NULL, 'true', '2023-07-28 17:54:17.288095+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.water_is_valid', NULL, 'false', '2023-07-28 17:54:17.288095+03') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.snow_is_valid', NULL, 'false', '2023-07-28 17:54:17.288095+03') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.reference_polygons', NULL, '', '2016-03-03 14:46:26.267227+02') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.crop_mask', NULL, '', '2022-03-08 10:58:03.38654+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.reference_polygons', NULL, '', '2016-03-03 14:46:26.267227+02') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.invalid_pixels_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') on conflict DO nothing;

                
                INSERT INTO config_metadata VALUES ('general.docker_script_unit_image', 'Sen4CAP services scripts docker image', 'string', false, 1, FALSE, 'Sen4CAP services scripts docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.sacct-max-retries', 'Slurm SACCT max retries', 'int', true, 1, FALSE, 'Slurm SACCT max retries', NULL) on conflict DO nothing; 
                
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.tiled', 'Produce output flags as Tiled', 'bool', false, 27, FALSE, 'Produce output flags as Tiled', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('downloader.l9.write-dir', 'Write directory for Landsat9', 'string', false, 15, FALSE, 'Write directory for Landsat9', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('downloader.l9.enabled', 'L9 downloader is enabled', 'bool', false, 15, FALSE, 'L9 downloader is enabled', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('l9.enabled', 'L9 is enabled', 'bool', false, 15, FALSE, 'L9 is enabled', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('object.storage.container', 'Object storage container', 'string', false, 23, FALSE, 'Object storage container', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('object.storage.domain', 'Object storage domain', 'string', false, 23, FALSE, 'Object storage domain', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('object.storage.password', 'Object storage password', 'string', false, 23, FALSE, 'Object storage password', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('object.storage.projectId', 'Object storage project ID', 'string', false, 23, FALSE, 'Object storage project ID', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('object.storage.url', 'Object storage URL', 'string', false, 23, FALSE, 'Object storage URL', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('object.storage.user', 'Object storage user', 'string', false, 23, FALSE, 'Object storage user', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('primary.sensor', 'Primary sensor', 'string', false, 23, FALSE, 'Primary sensor', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.l2a_msk.tiled', 'Produce output flags as Tiled', 'bool', false, 27, FALSE, 'Produce output flags as Tiled', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.water_is_valid', 'Consider water pixels as valid', 'bool', false, 27, FALSE, 'Consider water pixels as valid', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.snow_is_valid', 'Consider snow pixels as valid', 'bool', false, 27, FALSE, 'Consider snow pixels as valid', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.l4a.reference_polygons', 'Insitu reference polygons location (if Insity data was selected). The shapefile should be copied first in this location on server', 'string', true, 5, true, 'Insitu reference polygons location', null, false) on conflict DO nothing; 


                INSERT INTO config_metadata VALUES ('archiver.max_age.l4b', 'L4B Product Max Age (days)', 'int', false, 7, FALSE, 'L4B Product Max Age (days)', NULL) ON conflict(key) DO UPDATE SET label = 'L4B Product Max Age (days)';
                INSERT INTO config_metadata VALUES ('general.scratch-path.l4b', 'Path for L4B temporary files', 'string', false, 1, FALSE, 'Path for L4B temporary files', NULL) ON conflict(key) DO UPDATE SET label = 'Path for L4B temporary files';
                INSERT INTO config_metadata VALUES ('processor.l4b.classifier', 'Random forest clasifier / SVM classifier choices=[rf, svm]', 'string', false, 6, FALSE, 'Random forest clasifier / SVM classifier choices=[rf, svm]', NULL) ON conflict(key) DO UPDATE SET label = 'Random forest clasifier / SVM classifier choices=[rf, svm]';
                INSERT INTO config_metadata VALUES ('processor.l4b.classifier.field', 'Training samples feature name', 'string', false, 6, FALSE, 'Training samples feature name', NULL) ON conflict(key) DO UPDATE SET label = 'Training samples feature name';

                INSERT INTO config_metadata VALUES ('processor.l4b.crop_mask', 'Crop Mask Product name or full product path', 'string', true, 6, true, 'Crop Mask Product') on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.l4b.reference_polygons', 'Insitu reference polygons location (if Insity data was selected). The shapefile should be copied first in this location on server', 'string', true, 6, false, 'Insitu reference polygons location', null, false) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.invalid_pixels_enabled', 'Number of invalid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Extract number of invalid pixels per parcel', NULL, true) on conflict DO nothing; 

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$  
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_heterog', NULL, '/mnt/archive/orchestrator_temp/s4c_heterog/{job_id}/{task_id}-{module}', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_heterog.keep_job_folders', NULL, '0', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_heterog.slurm_qos', NULL, 'qoss4cheterog', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-preparation-s1.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-preparation-s2.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-analysis-s2.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-analysis-s1.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-tiles-analysis-merge.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-heterog-period-analysis.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-heterog-extract-s1-list.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-temporal-resampling.docker_image',  NULL, 'sen4x/processors-new:0.1.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-heterog-crop-type',  NULL, 'heterog_crop_type_wrapper.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-preparation-s1',  NULL, 'cluster_preparation_s1.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-preparation-s2',  NULL, 'cluster_preparation_s2.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-analysis-s1',  NULL, 'cluster_analysis_s1.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-analysis-s2',  NULL, 'cluster_analysis_s2.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-heterog-extract-s1-list', NULL, 'heterog_s1_rasters_extractor.py', '2022-04-18 14:25:14.193131+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-tiles-analysis-merge', NULL, 'heterog_tiles_merge.py', '2022-04-18 14:25:14.193131+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-heterog-period-analysis', NULL, 'heterog_period_analysis.py', '2022-04-18 14:25:14.193131+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.start_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.end_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.clustering_period', NULL, 30, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.temporal_resampling_max_dist', NULL, 30, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.temporal_resampling_windows_radius', NULL, 15, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.mask_value', NULL, 0, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.nan_value', NULL, -10000, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s1_temporal_resampling_interval', NULL, 7, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s2_temporal_resampling_interval', NULL, 10, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s1_clusters_number', NULL, 4, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s2_clusters_number', NULL, 4, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.isolated_pixels_thr', NULL, 6, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.isolated_pixels_smoothing_radius', NULL, 1, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.search_radius_s1', NULL, 2, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.search_radius_s2', NULL, 1, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.full_connectivity', NULL, false, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s1_min_cluster_pixels', NULL, 20, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s2_min_cluster_pixels', NULL, 20, '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.ndvi_clust_dist_thr', NULL, '0.17', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.percentage_hererogeneity', NULL, '0.9', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_bare_soil', NULL, '/mnt/archive/orchestrator_temp/s4c_bare_soil/{job_id}/{task_id}-{module}', '2023-10-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_bare_soil.keep_job_folders', NULL, '0', '2023-10-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_bare_soil.slurm_qos', NULL, 'qoss4cbaresoil', '2023-10-09 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s2-calibration.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s1-calibration.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s2-model.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s1-model.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-markers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s2-calibration',  NULL, 's4c_bs_calibration_s2.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s1-calibration',  NULL, 's4c_bs_calibration_s1.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s2-model',  NULL, 's4c_bs_model_s2.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s1-model',  NULL, 's4c_bs_model_s1.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-markers', NULL, 's4c_bs_markers.py', '2023-09-18 14:25:14.193131+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.start_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.end_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_bs_ndvi_thr', NULL, 0.15, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_ndvi_thr', NULL, 0.45, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_bs_ndwi_thr', NULL, 0., '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_ndwi_thr', NULL, 0.3, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_bs_ndti_thr', NULL, 0.1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_ndti_thr', NULL, 0.25, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_fcover_thr', NULL, 0.01, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.model_estimator_no', NULL, 30, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_long_period', NULL, 60, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_short_period', NULL, 30, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_s2_periods_no', NULL, 3, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_s1_periods_no', NULL, 4, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_bs_s2_threshold', NULL, 0.75, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_nbs_s2_threshold', NULL, 0.8, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_bs_s1_threshold', NULL, 0.65, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_nbs_s1_threshold', NULL, 0.7, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_change_detection', NULL, '/mnt/archive/orchestrator_temp/s4c_change_detection/{job_id}/{task_id}-{module}', '2023-10-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_change_detection.keep_job_folders', NULL, '0', '2023-10-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_change_detection.slurm_qos', NULL, 'qoss4cchangedet', '2023-10-09 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-extract-common-parcels.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-filter-lpis-cols.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-lai-outliers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-veg-growth-markers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-bs-markers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-computation.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-consolidation.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-extract-common-parcels', NULL, 'match_sites_parcels.py', '2023-09-18 14:25:14.193131+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-filter-lpis-cols',  NULL, 'filter_csv_by_cols.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-lai-outliers',  NULL, 'lai_outliers_computation.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-veg-growth-markers',  NULL, 'veg_growth_markers_extraction.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-bs-markers',  NULL, 'bs_markers_extraction.py', '2023-09-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-computation', NULL, 's4c_change_detection.py', '2023-09-18 14:25:14.193131+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-consolidation', NULL, 's4c_change_detection_consolidation.py', '2023-09-18 14:25:14.193131+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.start_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.end_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_site_id', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_start_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_end_date', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_mdb1_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_bs_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.mdb1_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.bs_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.sites_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02') on conflict DO nothing;

                -- Reference period Grassland changes 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_incr', NULL, 2, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_thr', NULL, 50, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_incr', NULL, 1.5, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                -- Reference period Permanent crops changes 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_incr', NULL, 3, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_thr', NULL, 50, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_thr', NULL, 20, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                -- Reference period Arable land changes
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;

                -- Current year Grassland changes 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ttdayss2_incr', NULL, 2, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_thr', NULL, 25, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_incr', NULL, 1.5, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_consecstab_thr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_consecstab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                -- Current year Permanent crops changes 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_incr', NULL, 3, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_areaveg_thr', NULL, 25, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_areaveg_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ratiostab_thr', NULL, 20, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ratiostab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                -- Current year Arable land changes
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.arableland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.arableland_ttdayss2_incr', NULL, 1, '2023-10-03 11:09:43.978921+02') on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                -- Heterogeneity processor
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-preparation.docker_image', 'Heterogeneity cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity cluster preparation docker image', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s2.docker_image', 'Heterogeneity S2 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 cluster analysis docker image', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s1.docker_image', 'Heterogeneity S1 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 cluster analysis docker image', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-extract-s1-list.docker_image', 'Heterogeneity S1 list extractor docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 list extractor docker image', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-temporal-resampling.docker_image', 'Heterogeneity S2 temporal resampling docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 temporal resampling docker image', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-tiles-analysis-merge.docker_image', 'Heterogeneity tiles analysis merge docker image', 'string', false, 1, FALSE, 'Heterogeneity tiles analysis merge docker image', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-period-analysis.docker_image', 'Heterogeneity S2 period analysis docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 period analysis docker image', NULL) on conflict DO nothing; 

                -- Bare soil processor
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-calibration.docker_image', 'Bare Soil S2 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Calibration docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-calibration.docker_image', 'Bare Soil S1 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Calibration docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-model.docker_image', 'Bare Soil S2 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Model docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-model.docker_image', 'Bare Soil S1 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Model docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-markers.docker_image', 'Bare Soil Markers extraction docker image', 'string', false, 1, FALSE, 'Bare Soil Markers extraction docker image', NULL)  on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('executor.processor.s4c_heterog.slurm_qos', 'Slurm QOS for Parcels Heterogeneity processor', 'string', true, 8, FALSE, 'Slurm QOS for Parcels Heterogeneity processor', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_heterog', 'Path for Parcels Heterogeneity temporary files', 'string', false, 1, FALSE, 'Path for Parcels Heterogeneity temporary files', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_heterog.keep_job_folders', 'Keep Parcels Heterogeneity intermediate folders', 'int', false, 8, FALSE, 'Keep Parcels Heterogeneity intermediate folders', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.input_l2a', 'The list of L2A products', 'select', FALSE, 36, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-preparation-s1.docker_image', 'Heterogeneity S1 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 cluster preparation docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-preparation-s2.docker_image', 'Heterogeneity S2 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 cluster preparation docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s2.docker_image', 'Heterogeneity S2 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 cluster analysis docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s1.docker_image', 'Heterogeneity S1 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 cluster analysis docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-extract-s1-list.docker_image', 'Heterogeneity S1 list extractor docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 list extractor docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-temporal-resampling.docker_image', 'Heterogeneity S2 temporal resampling docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 temporal resampling docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-tiles-analysis-merge.docker_image', 'Heterogeneity tiles analysis merge docker image', 'string', false, 1, FALSE, 'Heterogeneity tiles analysis merge docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-period-analysis.docker_image', 'Heterogeneity S2 period analysis docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 period analysis docker image', NULL)  on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-heterog-crop-type', 'Heterogeneity crop type wrapper path', 'file', true, 8, FALSE, 'Heterogeneity crop type wrapper', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-preparation-s1', 'Heterogeneity S1 cluster preparation path', 'file', true, 8, FALSE, 'Heterogeneity S1 cluster preparation path', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-preparation-s2', 'Heterogeneity S2 cluster preparation path', 'file', true, 8, FALSE, 'Heterogeneity S2 cluster preparation path', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-analysis-s1', 'Heterogeneity S1 cluster analysis path', 'file', true, 8, FALSE, 'Heterogeneity S1 cluster analysis path', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-analysis-s2', 'Heterogeneity S2 cluster analysis path', 'file', true, 8, FALSE, 'Heterogeneity S2 cluster analysis path', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-heterog-extract-s1-list', 'Heterogeneity S1 list extractor path', 'file', true, 8, FALSE, 'Heterogeneity S1 list extractor path', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-tiles-analysis-merge', 'Heterogeneity tiles analysis merge path', 'file', true, 8, FALSE, 'Heterogeneity tiles analysis merge path', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-heterog-period-analysis', 'Heterogeneity period analysis path', 'file', true, 8, FALSE, 'Heterogeneity period analysis path', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.start_date', 'Start date', 'string', false, 36, true, 'Start date', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.end_date', 'End date', 'string', false, 36, true, 'End date', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.clustering_period', 'Clustering period', 'int', true, 36, true, 'Clustering period', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.temporal_resampling_max_dist', 'Temporal resampling maximum distance', 'int', true, 36, true, 'Temporal resampling maximum distance', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.temporal_resampling_windows_radius', 'Temporal resampling window radius', 'int', true, 36, true, 'Temporal resampling window radius', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.mask_value', 'Mask value', 'int', true, 36, true, 'Mask value', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.nan_value', 'NaN value', 'int', true, 36, true, 'NaN value', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s1_temporal_resampling_interval', 'S1 temporal resampling interval', 'int', true, 36, true, 'S1 temporal resampling interval', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s2_temporal_resampling_interval', 'S2 temporal resampling interval', 'int', true, 36, true, 'S2 temporal resampling interval', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s1_clusters_number', 'S1 Number of Clusters', 'int', true, 36, true, 'S1 Number of Clusters', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s2_clusters_number', 'S2 Number of Clusters', 'int', true, 36, true, 'S2 Number of Clusters', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.isolated_pixels_thr', 'Isolated pixels threshold', 'int', true, 36, true, 'Isolated pixels threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.isolated_pixels_smoothing_radius', 'Spatial Smoothing radius', 'int', true, 36, true, 'Spatial Smoothing radius', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.search_radius_s1', 'Spatial connectivity S1 search radius', 'int', true, 36, true, 'Spatial connectivity S1 search radius', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.search_radius_s2', 'Spatial connectivity S1 search radius', 'int', true, 36, true, 'Spatial connectivity S2 search radius', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.full_connectivity', 'Use full connectivity', 'bool', true, 36, true, 'Use full connectivity', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s1_min_cluster_pixels', 'S1 minimum cluster pixels', 'int', true, 36, true, 'S1 minimum cluster pixels', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s2_min_cluster_pixels', 'S2 minimum cluster pixels', 'int', true, 36, true, 'S2 minimum cluster pixels', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.ndvi_clust_dist_thr', 'Threshold of the NDVI distance calculated between clusters', 'float', true, 36, true, 'Threshold of the NDVI distance', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_heterog.percentage_hererogeneity', 'Pixels percentage corresponding to the biggest cluster in the parcel', 'float', true, 36, true, 'Heterogeneity percentage', null) on conflict DO nothing; 


                -- -----------------------------------------------------------
                -- S4C Bare Soil Specific Keys
                -- -----------------------------------------------------------
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_bare_soil.slurm_qos', 'Slurm QOS for Bare Soil processor', 'string', true, 8, FALSE, 'Slurm QOS for Bare Soil processor', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_bare_soil', 'Path for Bare Soil temporary files', 'string', false, 1, FALSE, 'Path for Bare Soil temporary files', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_bare_soil.keep_job_folders', 'Keep Bare Soil intermediate folders', 'int', false, 8, FALSE, 'Keep Bare Soil intermediate folders', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.input_l2a', 'The list of L2A products', 'select', FALSE, 37, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-calibration.docker_image', 'Bare Soil S2 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Calibration docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-calibration.docker_image', 'Bare Soil S1 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Calibration docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-model.docker_image', 'Bare Soil S2 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Model docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-model.docker_image', 'Bare Soil S1 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Model docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-markers.docker_image', 'Bare Soil Markers extraction docker image', 'string', false, 1, FALSE, 'Bare Soil Markers extraction docker image', NULL)  on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s2-calibration', 'Bare Soil S2 Calibration script', 'string', true, 8, FALSE, 'Bare Soil S2 Calibration script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s1-calibration', 'Bare Soil S1 Calibration script', 'string', true, 8, FALSE, 'Bare Soil S1 Calibration script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s2-model', 'Bare Soil S2 Model script', 'string', true, 8, FALSE, 'Bare Soil S2 Model script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s1-model', 'Bare Soil S1 Model script', 'string', true, 8, FALSE, 'Bare Soil S1 Model script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-markers', 'Bare Soil Markers extraction script', 'string', true, 8, FALSE, 'Bare Soil Markers extraction script', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.start_date', 'Start date', 'string', false, 37, true, 'Start date', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.end_date', 'End date', 'string', false, 37, true, 'End date', null) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_bs_ndvi_thr', 'Calibration BS NDVI Threshold', 'float', true, 37, true, 'Calibration BS NDVI Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_ndvi_thr', 'Calibration NBS NDVI Threshold', 'float', true, 37, true, 'Calibration NBS NDVI Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_bs_ndwi_thr', 'Calibration BS NDWI Threshold', 'float', true, 37, true, 'Calibration BS NDWI Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_ndwi_thr', 'Calibration NBS NDWI Threshold', 'float', true, 37, true, 'Calibration NBS NDWI Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_bs_ndti_thr', 'Calibration BS NDTI Threshold', 'float', true, 37, true, 'Calibration BS NDTI Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_ndti_thr', 'Calibration NBS NDTI Threshold', 'float', true, 37, true, 'Calibration NBS NDTI Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_fcover_thr', 'Calibration NBS fCover Threshold', 'float', true, 37, true, 'Calibration NBS fCover Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.model_estimator_no', 'Number of estimators for model', 'int', true, 37, true, 'Number of estimators for model', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_long_period', 'Markers Long Period', 'int', true, 37, true, 'Markers Long Period', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_short_period', 'Markers Short Period', 'int', true, 37, true, 'Markers Short Period', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_s2_periods_no', 'Markers Number of S2 periods', 'int', true, 37, true, 'Markers Number of S2 periods', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_s1_periods_no', 'Markers Number of S1 periods', 'int', true, 37, true, 'Markers Number of S1 periods', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_bs_s2_threshold', 'Markers S2 BS Threshold', 'float', true, 37, true, 'Markers S2 BS Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_nbs_s2_threshold', 'Markers S2 NBS Threshold', 'float', true, 37, true, 'Markers S2 NBS Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_bs_s1_threshold', 'Markers S1 BS Threshold', 'float', true, 37, true, 'Markers S1 BS Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_nbs_s1_threshold', 'Markers S1 NBS Threshold', 'float', true, 37, true, 'Markers S1 NBS Threshold', null) on conflict DO nothing; 

                -- -----------------------------------------------------------
                -- S4C Change Detection Specific Keys
                -- -----------------------------------------------------------

                INSERT INTO config_metadata VALUES ('executor.processor.s4c_change_detection.slurm_qos', 'Slurm QOS for Change Detection processor', 'string', true, 8, FALSE, 'Slurm QOS for Change Detection processor', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_change_detection', 'Path for Change Detection temporary files', 'string', false, 1, FALSE, 'Path for Change Detection temporary files', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.processor.s4c_change_detection.keep_job_folders', 'Keep Change Detection intermediate folders', 'int', false, 8, FALSE, 'Keep Change Detection intermediate folders', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.input_l2a', 'The list of L2A products', 'select', FALSE, 40, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-extract-common-parcels.docker_image', 'Common parcels extraction docker image', 'string', false, 1, FALSE, 'Common parcels extraction docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-filter-lpis-cols.docker_image', 'LPIS columns filtering docker image', 'string', false, 1, FALSE, 'LPIS columns filtering docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-lai-outliers.docker_image', 'LAI Outliers docker image', 'string', false, 1, FALSE, 'LAI Outliers docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-veg-growth-markers.docker_image', 'Vegetation Growth markers docker image', 'string', false, 1, FALSE, 'Vegetation Growth markers docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-bs-markers.docker_image', 'Bare soil markers filtering docker image', 'string', false, 1, FALSE, 'Bare soil markers filtering docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-computation.docker_image', 'Change Detection computation docker image', 'string', false, 1, FALSE, 'Change Detection computation docker image', NULL)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-consolidation.docker_image', 'Change Detection consolidation docker image', 'string', false, 1, FALSE, 'Change Detection consolidation docker image', NULL)  on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-extract-common-parcels', 'Common parcels extraction script', 'string', true, 8, FALSE, 'Common parcels extraction script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-filter-lpis-cols', 'LPIS columns filtering script', 'string', true, 8, FALSE, 'LPIS columns filtering script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-lai-outliers', 'LAI Outliers script', 'string', true, 8, FALSE, 'LAI Outliers script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-veg-growth-markers', 'Vegetation Growth markers script', 'string', true, 8, FALSE, 'Vegetation Growth markers script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-bs-markers', 'Bare soil markers filtering script', 'string', true, 8, FALSE, 'Bare soil markers filtering script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-computation', 'Change Detection computation script', 'string', true, 8, FALSE, 'Change Detection computation script', NULL) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-consolidation', 'Change Detection consolidation script', 'string', true, 8, FALSE, 'Change Detection consolidation script', NULL) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.start_date', 'Start date', 'string', false, 40, true, 'Start date', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.end_date', 'End date', 'string', false, 40, true, 'End date', null) on conflict DO nothing; 

                insert into config_metadata values ('processor.s4c_change_detection.ref_site_id', 'Reference site', 'string', false, 40, true, 'Reference site', '{ "allowed_values_source": { "database_object": "site", "value_column": "id", "label_column": "name" } }', true) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_start_date', 'Reference Start date', 'string', false, 40, true, 'Reference Start date', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_end_date', 'Reference End date', 'string', false, 40, true, 'Reference End date', null) on conflict DO nothing; 

                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_mdb1_ids_mapping', 'Reference MDB1 NewID mapping file', 'string', true, 40, true, 'Reference MDB1 NewID mapping file', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_bs_ids_mapping', 'Reference BS NewID mapping file', 'string', true, 40, true, 'Reference BS NewID mapping file', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.mdb1_ids_mapping', 'Current MDB1 NewID mapping file', 'string', true, 40, true, 'Current MDB1 NewID mapping file', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.bs_ids_mapping', 'Current BS NewID mapping file', 'string', true, 40, true, 'Current BS NewID mapping file', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.sites_ids_mapping', 'Sites NewIDs mapping file', 'string', true, 40, true, 'Sites NewIDs mapping file', null) on conflict DO nothing; 

                -- Reference period Grassland changes 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_thr', 'Reference Grassland TTdaysS2 Threshold', 'string', true, 40, true, 'Reference Grassland TTdaysS2 Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_incr', 'Reference Grassland TTdaysS2 Increment', 'string', true, 40, true, 'Reference Grassland TTdaysS2 Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_thr', 'Reference Grassland Ratio Stability Threshold Min', 'string', true, 40, true, 'Reference Grassland Ratio Stability Threshold Min', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_thr', 'Reference Grassland Ratio Stability Threshold Max', 'string', true, 40, true, 'Reference Grassland Ratio Stability Threshold Max', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_incr', 'Reference Grassland Ratio Stability Min Increment', 'string', true, 40, true, 'Reference Grassland Ratio Stability Min Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_incr', 'Reference Grassland Ratio Stability Max Increment', 'string', true, 40, true, 'Reference Grassland Ratio Stability Max Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_thr', 'Reference Grassland ConsecC Stability Threshold', 'string', true, 40, true, 'Reference Grassland ConsecC Stability Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_incr', 'Reference Grassland ConsecC Stability Increment', 'string', true, 40, true, 'Reference Grassland ConsecC Stability Increment', null) on conflict DO nothing; 
                -- Reference period Permanent crops changes 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_thr', 'Reference Permanent Crops TTdaysS2 Threshold', 'string', true, 40, true, 'Reference Permanent Crops TTdaysS2 Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_incr', 'Reference Permanent Crops TTdaysS2 Increment', 'string', true, 40, true, 'Reference Permanent Crops TTdaysS2 Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_thr', 'Reference Permanent Crops AreaVeg Threshold', 'string', true, 40, true, 'Reference Permanent Crops AreaVeg Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_incr', 'Reference Permanent Crops AreaVeg Increment', 'string', true, 40, true, 'Reference Permanent Crops AreaVeg Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_thr', 'Reference Grassland Ratio Stability Threshold', 'string', true, 40, true, 'Reference Grassland Ratio Stability Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_incr', 'Reference Grassland Ratio Stability Increment', 'string', true, 40, true, 'Reference Grassland Ratio Stability Increment', null) on conflict DO nothing; 
                -- Reference period Arable land changes
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_thr', 'Reference Arable land TTdaysS2 Threshold', 'string', true, 40, true, 'Reference Arable land TTdaysS2 Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_incr', 'Reference Arable land TTdaysS2 Increment', 'string', true, 40, true, 'Reference Arable land TTdaysS2 Increment', null) on conflict DO nothing; 

                -- Current year Grassland changes 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ttdayss2_thr', 'Grassland TTdaysS2 Threshold', 'string', true, 40, true, 'Grassland TTdaysS2 Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ttdayss2_incr', 'Grassland TTdaysS2 Increment', 'string', true, 40, true, 'Grassland TTdaysS2 Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_thr', 'Grassland Ratio Stability Threshold Min', 'string', true, 40, true, 'Grassland Ratio Stability Threshold Min', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_thr', 'Grassland Ratio Stability Threshold Max', 'string', true, 40, true, 'Grassland Ratio Stability Threshold Max', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_incr', 'Grassland Ratio Stability Min Increment', 'string', true, 40, true, 'Grassland Ratio Stability Min Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_incr', 'Grassland Ratio Stability Max Increment', 'string', true, 40, true, 'Grassland Ratio Stability Max Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_consecstab_thr', 'Grassland ConsecC Stability Threshold', 'string', true, 40, true, 'Grassland ConsecC Stability Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_consecstab_incr', 'Grassland ConsecC Stability Increment', 'string', true, 40, true, 'Grassland ConsecC Stability Increment', null) on conflict DO nothing; 
                -- Current year Permanent crops changes 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_thr', 'Permanent Crops TTdaysS2 Threshold', 'string', true, 40, true, 'Permanent Crops TTdaysS2 Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_incr', 'Permanent Crops TTdaysS2 Increment', 'string', true, 40, true, 'Permanent Crops TTdaysS2 Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_areaveg_thr', 'Permanent Crops AreaVeg Threshold', 'string', true, 40, true, 'Permanent Crops AreaVeg Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_areaveg_incr', 'Permanent Crops AreaVeg Increment', 'string', true, 40, true, 'Permanent Crops AreaVeg Increment', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ratiostab_thr', 'Grassland Ratio Stability Threshold', 'string', true, 40, true, 'Grassland Ratio Stability Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ratiostab_incr', 'Grassland Ratio Stability Increment', 'string', true, 40, true, 'Grassland Ratio Stability Increment', null) on conflict DO nothing; 
                -- Current year Arable land changes
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.arableland_ttdayss2_thr', 'Arable land TTdaysS2 Threshold', 'string', true, 40, true, 'Arable land TTdaysS2 Threshold', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.arableland_ttdayss2_incr', 'Arable land TTdaysS2 Increment', 'string', true, 40, true, 'Arable land TTdaysS2 Increment', null) on conflict DO nothing; 

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$  
                INSERT INTO product_type (id, name, description, is_raster) VALUES (24, 's4c_heterogeneity','Parcel Heterogeneity', false) on conflict DO nothing;
                INSERT INTO product_type (id, name, description, is_raster) VALUES (33, 's4c_bare_soil','Bare soil', false) on conflict DO nothing;
                INSERT INTO product_type (id, name, description, is_raster) VALUES (35, 's4c_change_detection','L4 Change Detection', false) on conflict DO nothing;
            $str$;
            raise notice '%', _statement;
            execute _statement;

            _statement := $str$
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_input_tables', 'MDB3 input tables location', 'string', true, 26, false, 'MDB3 input tables location', NULL) on conflict (key) DO UPDATE SET is_site_visible = false;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$  
                DELETE FROM config WHERE key = 'processor.l4a.earth_signature_insitu';
                DELETE FROM config WHERE key = 'processor.l4b.earth_signature_insitu';
                DELETE FROM config WHERE key = 'processor.l2a_msk.cog';
                DELETE FROM config WHERE key = 'processor.l4b.crop-mask';

                DELETE FROM config_metadata WHERE key = 'processor.l4a.earth_signature_insitu';
                DELETE FROM config_metadata WHERE key = 'processor.l4b.earth_signature_insitu';
                DELETE FROM config_metadata WHERE key = 'processor.l2a_msk.cog';
                DELETE FROM config_metadata WHERE key = 'processor.l4b.crop-mask';
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
                declare _product_date timestamp;
                declare _orbit_id integer;
                declare _tile_id text;
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
                        where fmask_history.downloader_history_id = _downloader_history_id;
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
                        and downloader_history.product_name not like '%_MSIL2A_%'
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

                CREATE OR REPLACE FUNCTION sp_get_job_definition(IN _job_id integer)
                  RETURNS TABLE(processor_id smallint, site_id smallint, parameters json) AS
                $BODY$
                BEGIN

                RETURN QUERY SELECT job.processor_id, job.site_id, job.parameters FROM job WHERE job.id = _job_id;

                CREATE OR REPLACE FUNCTION sp_get_job_steps(
                IN _job_id int
                ) RETURNS TABLE (
                task_id int,
                module_short_name character varying,
                step_name character varying,
                preceding_task_ids integer[],
                parameters json
                ) AS $$
                BEGIN

                RETURN QUERY
                WITH job_tasks AS (
                        SELECT task.id AS task_id, task.module_short_name as module_short_name, task.preceding_task_ids as preceding_task_ids
                        FROM task
                        WHERE task.job_id = _job_id
                    )
                SELECT job_tasks.task_id, job_tasks.module_short_name, step.name AS step_name, job_tasks.preceding_task_ids , step.parameters
                FROM job_tasks INNER JOIN step ON job_tasks.task_id = step.task_id;

                END;
                $$ LANGUAGE plpgsql;

                END;
                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_job_definition(integer)
                  OWNER TO admin;            
                create or replace function sp_get_site_strata(_site_id int)
                    returns table (
                            stratum_id int,
                            geom bytea,
                            epsg_code int,
                            tiles char(5)[]
                    )
                as
                $$
                begin
                    return query
                        with site_tiles as (select tile_id
                                            from sp_get_site_tiles(_site_id :: smallint, 1 :: smallint)),
                             site_stratum as (select stratum.stratum_id, wkb_geometry as geometry, st_transform(wkb_geometry, 4326) :: geography as stratum_geog
                                              from stratum
                                              where site_id = _site_id)
                        select site_stratum.stratum_id,
                               ST_AsBinary(site_stratum.geometry) as geometry,
                               ST_SRID(site_stratum.geometry) as epsg_code,
                               array_agg(shape_tiles_s2.tile_id) as tile_id
                        from site_stratum
                                 inner join shape_tiles_s2 on ST_Intersects(shape_tiles_s2.geog, site_stratum.stratum_geog)
                        where exists (select * from site_tiles where site_tiles.tile_id = shape_tiles_s2.tile_id)
                        group by site_stratum.stratum_id, site_stratum.geometry
                        order by site_stratum.stratum_id;
                end;
                $$
                    language plpgsql
                    stable;
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
           _statement := 'update meta set version = ''4.0.0'';';
            raise notice '%', _statement;
            execute _statement;
        end if;
    end if;

    raise notice 'complete';
end;
$migration$;

commit;


