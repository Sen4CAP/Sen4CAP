-- -----------------------------------------------------------
-- General Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.archive_path', 'Archive Path', 'string', false, 7, FALSE, 'Archive Path', NULL);
INSERT INTO config_metadata VALUES ('archiver.max_age.l2a', 'L2A Product Max Age (days)', 'int', false, 7, FALSE, 'L2A Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path', 'Default path for temporary files', 'string', false, 1, FALSE, 'Default path for temporary files', NULL);
INSERT INTO config_metadata VALUES ('http-listener.listen-port', 'Dashboard Listen Port', 'int', true, 12, FALSE, 'Dashboard Listen Port', NULL);
INSERT INTO config_metadata VALUES ('http-listener.root-path', 'Document Root Path', 'directory', true, 12, FALSE, 'Document Root Path', NULL);
INSERT INTO config_metadata VALUES ('mail.message.batch.limit', 'Batch limit of mail message', 'int', false, 1, FALSE, 'Batch limit of mail message', NULL);
INSERT INTO config_metadata VALUES ('monitor-agent.disk-path', 'Disk Path To Monitor For Space', 'directory', false, 13, FALSE, 'Disk Path To Monitor For Space', NULL);
INSERT INTO config_metadata VALUES ('monitor-agent.scan-interval', 'Measurement Interval (s)', 'int', true, 13, FALSE, 'Measurement Interval (s)', NULL);
INSERT INTO config_metadata VALUES ('resources.working-mem', 'OTB applications working memory (MB)', 'int', true, 14, FALSE, 'OTB applications working memory (MB)', NULL);
INSERT INTO config_metadata VALUES ('site.path', 'Site path', 'file', false, 17, FALSE, 'Site path', NULL);
INSERT INTO config_metadata VALUES ('site.upload-path', 'Upload path', 'string', false, 17, FALSE, 'Upload path', NULL);
INSERT INTO config_metadata VALUES ('site.url', 'Site url', 'string', false, 17, FALSE, 'Site url', NULL) ;

-- -----------------------------------------------------------
-- Executor/orchestrator/scheduler Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.http-server.listen-ip', 'Executor HTTP listen ip', 'string', false, 1, FALSE, 'Executor HTTP listen ip', NULL);
INSERT INTO config_metadata VALUES ('executor.http-server.listen-port', 'Executor HTTP listen port', 'string', false, 1, FALSE, 'Executor HTTP listen port', NULL);
INSERT INTO config_metadata VALUES ('executor.listen-ip', 'Executor IP Address', 'string', true, 8, FALSE, 'Executor IP Address', NULL);
INSERT INTO config_metadata VALUES ('executor.listen-port', 'Executor Port', 'int', true, 8, FALSE, 'Executor Port', NULL);
INSERT INTO config_metadata VALUES ('executor.resource-manager.name', 'Executor resource manager name', 'string', false, 1, FALSE, 'Executor resource manager name', NULL);
INSERT INTO config_metadata VALUES ('executor.wrapper-path', 'Processor Wrapper Path', 'file', true, 8, FALSE, 'Processor Wrapper Path', NULL);
INSERT INTO config_metadata VALUES ('executor.wrp-executes-local', 'Execution of wrappers are only local', 'int', true, 8, FALSE, 'Execution of wrappers are only local', NULL);
INSERT INTO config_metadata VALUES ('executor.wrp-send-retries-no', 'Number of wrapper retries to connect to executor when TCP error', 'int', true, 8, FALSE, 'Number of wrapper retries to connect to executor when TCP error', NULL);
INSERT INTO config_metadata VALUES ('executor.wrp-timeout-between-retries', 'Timeout between wrapper retries to executor when TCP error', 'int', true, 8, FALSE, 'Timeout between wrapper retries to executor when TCP error', NULL);
INSERT INTO config_metadata VALUES ('general.inter-proc-com-type', 'Type of the interprocess communication', 'string', false, 1, FALSE, 'Type of the interprocess communication', NULL);
INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-ip', 'Orchestrator HTTP listen ip', 'string', false, 1, FALSE, 'Orchestrator HTTP listen ip', NULL);
INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-port', 'Orchestrator HTTP listen port', 'string', false, 1, FALSE, 'Orchestrator HTTP listen port', NULL);

-- -----------------------------------------------------------
-- Executor module paths
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.module.path.color-mapping', 'Color Mapping Path', 'file', true, 8, FALSE, 'Color Mapping Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.compression', 'Compression Path', 'file', true, 8, FALSE, 'Compression Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.compute-confusion-matrix', 'Compute Confusion Matrix Path', 'file', true, 8, FALSE, 'Compute Confusion Matrix Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.compute-image-statistics', 'Compute image statistics', 'file', true, 8, FALSE, 'Compute image statistics', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.compute-images-statistics', 'Compute Images Statistics Path', 'file', true, 8, FALSE, 'Compute Images Statistics Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.concatenate-images', 'Concatenate Images Path', 'file', true, 8, FALSE, 'Concatenate Images Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.crop-mask-fused', 'Crop mask script with stratification', 'file', true, 8, FALSE, 'Crop mask script with stratification', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.crop-type-fused', 'Crop type script with stratification', 'file', true, 8, FALSE, 'Crop type script with stratification', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.dimensionality-reduction', 'Dimensionality reduction', 'file', true, 8, FALSE, 'Dimensionality reduction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.end-of-job', 'End of a multi root steps job', 'file', true, 8, FALSE, 'End of a multi root steps job', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.extract-l4c-markers', 'Script for extracting L4C markers', 'file', true, 8, FALSE, 'Script for extracting L4C markers', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.files-remover', 'Removes the given files (ex. cleanup of intermediate files)', 'file', false, 8, FALSE, 'Removes the given files (ex. cleanup of intermediate files)', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.gdalbuildvrt', 'Path for gdalbuildvrt', 'file', true, 8, FALSE, 'Path for gdalbuildvrt', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.gdal_translate', 'Path for gdal_translate', 'file', true, 8, FALSE, 'Path for gdal_translate', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.image-classifier', 'Image Classifier Path', 'file', true, 8, FALSE, 'Image Classifier Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.image-compression', 'Image compression', 'file', true, 8, FALSE, 'Image compression', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.mdb-csv-to-ipc-export', 'Script for extracting markers csv to IPC file', 'file', true, 8, FALSE, 'Script for extracting markers csv to IPC file', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-crop-type', 'L4A Crop Type main execution script path', 'file', true, 8, FALSE, 'L4A Crop Type main execution script path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s1', 'L4B S1 main execution script path', 'file', true, 8, FALSE, 'L4B S1 main execution script path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s2', 'L4B S2 main execution script path', 'file', true, 8, FALSE, 'L4B S2 main execution script path', NULL);

-- -----------------------------------------------------------
-- Downloader Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('downloader.enabled', 'Downloader is enabled', 'bool', false, 15, FALSE, 'Downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.enabled', 'L8 downloader is enabled', 'bool', false, 15, FALSE, 'L8 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.forcestart', 'Forces the L8 download to start again from the beginning of the season', 'bool', false, 15, FALSE, 'Forces the L8 download to start again from the beginning of the season', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.max-retries', 'Maximum retries for downloading a product', 'int', false, 15, FALSE, 'Maximum retries for downloading a product', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.write-dir', 'Write directory for Landsat8', 'string', false, 15, FALSE, 'Write directory for Landsat8', NULL);
INSERT INTO config_metadata VALUES ('downloader.max-cloud-coverage', 'Maximum Cloud Coverage (%)', 'int', false, 15, FALSE, 'Maximum Cloud Coverage (%)', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.enabled', 'S1 downloader is enabled', 'bool', false, 15, FALSE, 'S1 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.forcestart', 'Forces the S1 download to start again from the beginning of the season', 'bool', false, 15, FALSE, 'Forces the S1 download to start again from the beginning of the season', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.write-dir', 'Write directory for Sentinel1', 'string', false, 15, FALSE, 'Write directory for Sentinel1', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.enabled', 'S2 downloader is enabled', 'bool', false, 15, FALSE, 'S2 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.forcestart', 'Forces the S2 download to start again from the beginning of the season', 'bool', false, 15, FALSE, 'Forces the S2 download to start again from the beginning of the season', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.max-retries', 'Maximum retries for downloading a product', 'int', false, 15, FALSE, 'Maximum retries for downloading a product', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.write-dir', 'Write directory for Sentinel2', 'string', false, 15, FALSE, 'Write directory for Sentinel2', NULL);
INSERT INTO config_metadata VALUES ('downloader.skip.existing', 'If enabled, products downloaded for another site will be duplicated, in database only, for the current site', 'bool', false, 15, FALSE, 'TO_BE_REPLACED', NULL);
INSERT INTO config_metadata VALUES ('downloader.start.offset', 'Season start offset in months', 'int', false, 15, FALSE, 'Season start offset in months', NULL);
INSERT INTO config_metadata VALUES ('downloader.timeout', 'Timeout between download retries ', 'int', false, 15, FALSE, 'Timeout between download retries ', NULL);
INSERT INTO config_metadata VALUES ('downloader.use.esa.l2a', 'Enable S2 L2A ESA products download', 'bool', false, 15, FALSE, 'Enable S2 L2A ESA products download', NULL);
INSERT INTO config_metadata VALUES ('l8.enabled', 'L8 is enabled', 'bool', false, 15, FALSE, 'L8 is enabled', NULL);
INSERT INTO config_metadata VALUES ('s1.enabled', 'S1 is enabled', 'bool', false, 15, FALSE, 'S1 is enabled', NULL);
INSERT INTO config_metadata VALUES ('s2.enabled', 'S2 is enabled', 'bool', false, 15, FALSE, 'S2 is enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.lookup.enabled', 'Scheduled lookup is enabled', 'bool', false, 15, FALSE, 'Scheduled lookup is enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.deleteAfter', 'Delete the products after they were uploaded to object storage', 'bool', false, 15, FALSE, 'Delete the products after they were uploaded to object storage', NULL);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.enabled', 'Scheduled object storage move enabled', 'bool', false, 15, FALSE, 'Scheduled object storage move enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.product.types', 'Product types to move to object storage (separated by ;)', 'string', false, 15, FALSE, 'Product types to move to object storage (separated by ;)', NULL);
INSERT INTO config_metadata VALUES ('scheduled.retry.enabled', 'Scheduled retry is enabled', 'bool', false, 15, FALSE, 'Scheduled retry is enabled', NULL);

-- -----------------------------------------------------------
-- L2A processor Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.l2a.name', 'L2A Processor Name', 'string', true, 8, FALSE, 'L2A Processor Name', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l2a.path', 'L2A Processor Path', 'file', false, 8, FALSE, 'L2A Processor Path', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.gipp-path', 'MAJA GIPP path', 'directory', false, 2, FALSE, 'MAJA GIPP path', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-fre', 'Remove FRE files from resulted L2A product', 'bool', false, 2, FALSE, 'Remove FRE files from resulted L2A product', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-sre', 'Remove SRE files from resulted L2A product', 'bool', false, 2, FALSE, 'Remove SRE files from resulted L2A product', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.cog-tiffs', 'Produce L2A tiff files as Cloud Optimized Geotiff', 'bool', false, 2, FALSE, 'Produce L2A tiff files as Cloud Optimized Geotiff', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.compress-tiffs', 'Compress the resulted L2A TIFF files', 'bool', false, 2, FALSE, 'Compress the resulted L2A TIFF files', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.max-retries', 'Number of retries for the L2A processor', 'int', false, 2, FALSE, 'Number of retries for the L2A processor', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.num-workers', 'Parallelism degree of the L2A processor', 'int', false, 2, FALSE, 'Parallelism degree of the L2A processor', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.output-path', 'path for L2A products', 'directory', false, 2, FALSE, 'path for L2A products', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.retry-interval', 'Retry interval for the L2A processor', 'string', false, 2, FALSE, 'Retry interval for the L2A processor', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2, false, null, '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }');
INSERT INTO config_metadata VALUES ('processor.l2a.sen2cor.gipp-path', 'Sen2Cor GIPP path', 'directory', false, 2, FALSE, 'Sen2Cor GIPP path', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.srtm-path', 'Path to the DEM dataset', 'directory', false, 2, FALSE, 'Path to the DEM dataset', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a.swbd-path', 'Path to the SWBD dataset', 'directory', false, 2, FALSE, 'Path to the SWBD dataset', NULL);

INSERT INTO config_metadata VALUES ('processor.l2a.working-dir', 'Working directory', 'string', false, 2, FALSE, 'Working directory', NULL);
INSERT INTO config_metadata VALUES('processor.l2a.processors_image','L2a processors image name','string',false,2, FALSE, 'L2a processors image name', NULL);
INSERT INTO config_metadata VALUES('processor.l2a.sen2cor_image','Sen2Cor image name','string',false,2, FALSE, 'Sen2Cor image name', NULL);
INSERT INTO config_metadata VALUES('processor.l2a.maja_image','MAJA image name','string',false,2, FALSE, 'MAJA image name', NULL);
INSERT INTO config_metadata VALUES('processor.l2a.gdal_image','GDAL image name','string',false,2, FALSE, 'GDAL image name', NULL);
INSERT INTO config_metadata VALUES('processor.l2a.l8_align_image','L8 align image name','string',false,2, FALSE, 'L8 align image name', NULL);
INSERT INTO config_metadata VALUES('processor.l2a.dem_image','DEM image name','string',false,2, FALSE, 'DEM image name', NULL);

-- -----------------------------------------------------------
-- L2S1 processor Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.l2s1.enabled', 'S1 pre-processing enabled', 'bool', false, 15, FALSE, 'S1 pre-processing enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.parallelism', 'Tiles to classify in parallel', 'int', false, 15, FALSE, 'Tiles to classify in parallel', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.path', 'The path where the S1 L2 products will be created', 'string', false, 15, FALSE, 'The path where the S1 L2 products will be created', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.offset', 'S1 pre-processor offset', 'int', false, 15, FALSE, 'S1 pre-processor offset', NULL) ;
INSERT INTO config_metadata VALUES ('processor.l2s1.work.dir', 'The path where to create the temporary S1 L2A files', 'string', false, 15, FALSE, 'The path where to create the temporary S1 L2A files', NULL);

-- -----------------------------------------------------------
-- LPIS configuration Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.lpis.path', 'The path to the pre-processed LPIS products', 'string', false, 21, FALSE, 'The path to the pre-processed LPIS products', NULL);

-- -----------------------------------------------------------
-- L3A Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- L3B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l3b', 'L3B Product Max Age (days)', 'int', false, 7, FALSE, 'L3B Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3b.keep_job_folders', 'Keep L3B temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep L3B temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3b.slurm_qos', 'Slurm QOS for LAI processor', 'string', true, 8, FALSE, 'Slurm QOS for LAI processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3b', 'Path for L3B temporary files', 'string', false, 1, FALSE, 'Path for L3B temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.cloud_optimized_geotiff_output', 'Generate L3B Cloud Optimized Geotiff outputs', 'bool', false, 4, FALSE, 'Generate L3B Cloud Optimized Geotiff outputs', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fapar', 'L3B LAI processor will produce FAPAR', 'int', false, 4, FALSE, 'L3B LAI processor will produce FAPAR', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fcover', 'L3B LAI processor will produce FCOVER', 'int', false, 4, FALSE, 'L3B LAI processor will produce FCOVER', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_in_domain_flags', 'L3B processor will input domain flags', 'int', false, 4, FALSE, 'L3B processor will input domain flags', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_lai', 'L3B LAI processor will produce LAI', 'int', false, 4, FALSE, 'L3B LAI processor will produce LAI', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndvi', 'L3B LAI processor will produce NDVI', 'int', false, 4, FALSE, 'L3B LAI processor will produce NDVI', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.generate_models', 'Specifies if models should be generated or not for LAI', 'int', false, 4, FALSE, 'Specifies if models should be generated or not for LAI', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.l1c_availability_days', 'Number of days before current scheduled date within we must have L1C processed (default 20)', 'int', false, 4, FALSE, 'Number of days before current scheduled date within we must have L1C processed (default 20)', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.global_bv_samples_file', 'Common LAI BV sample distribution file', 'file', false, 4, FALSE, 'Common LAI BV sample distribution file', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.laibandscfgfile', 'Configuration of the bands to be used for LAI', 'file', false, 4, FALSE, 'Configuration of the bands to be used for LAI', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.lut_path', 'L3B LUT file path', 'file', false, 4, FALSE, 'L3B LUT file path', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.modelsfolder', 'Folder where the models are located', 'directory', false, 4, FALSE, 'Folder where the models are located', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.rsrcfgfile', 'L3B RSR file configuration for ProsailSimulator', 'file', false, 4, FALSE, 'L3B RSR file configuration for ProsailSimulator', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.tiles_filter', 'L3B tiles filter', 'string', false, 4, FALSE, 'L3B tiles filter', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.use_inra_version', 'L3B LAI processor will use INRA algorithm implementation', 'int', false, 4, FALSE, 'L3B LAI processor will use INRA algorithm implementation', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.use_lai_bands_cfg', 'Use LAI bands configuration file', 'int', false, 4, FALSE, 'Use LAI bands configuration file', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.production_interval', 'The backward processing interval from the scheduled date for L3B products', 'int', false, 4, FALSE, 'The backward processing interval from the scheduled date for L3B products', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.reproc_production_interval', 'The backward processing interval from the scheduled date for L3C products', 'int', false, 4, FALSE, 'The backward processing interval from the scheduled date for L3C products', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.sched_wait_proc_inputs', 'L3B/L3C/L3D LAI scheduled jobs wait for products to become available', 'int', false, 4, FALSE, 'L3B/L3C/L3D LAI scheduled jobs wait for products to become available', NULL);

-- -----------------------------------------------------------
-- S2A_L3C Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- S2A_L3D Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- L3E Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- L4A Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- L4B Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- S4C_L4A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4a', 'S4C L4A Product Max Age (days)', 'int', false, 7, FALSE, 'S4C L4A Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4a.keep_job_folders', 'Keep S4C L4A temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4A temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4a.slurm_qos', 'Slurm QOS for S4C L4A processor', 'string', true, 8, FALSE, 'Slurm QOS for S4C L4A processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4a', 'Path for  S4C L4A temporary files', 'string', false, 1, FALSE, 'Path for  S4C L4A temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.best-s2-pix', 'Minimum number of S2 pixels for parcels to use in training', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels for parcels to use in training', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.lc', 'LC classes to assess', 'string', TRUE, 5, TRUE, 'LC classes to assess', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-node-size', 'Minimum node size', 'int', TRUE, 5, TRUE, 'Minimum node size', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s1-pix', 'Minimum number of S1 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S1 pixels', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s2-pix', 'Minimum number of S2 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.mode', 'Mode', 'string', FALSE, 5, TRUE, 'Mode (both, s1-only, s2-only)', '{ "allowed_values": [{ "value": "s1-only", "display": "S1 Only" }, { "value": "s2-only", "display": "S2 only" }, { "value": "both", "display": "Both" }] }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.num-trees', 'Number of RF trees', 'int', TRUE, 5, TRUE, 'Number of RF trees', '{ "bounds": { "min": 0, "max": 1000 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-min', 'Minimum parcels to assess a crop type', 'int', TRUE, 5, TRUE, 'Minimum parcels to assess a crop type', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-h', 'Upper threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Upper threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-l', 'Lower threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Lower threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-h', 'Training ratio for common crop types', 'float', TRUE, 5, TRUE, 'Training ratio for common crop types', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-l', 'Training ratio for uncommon crop types', 'float', TRUE, 5, TRUE, 'Training ratio for uncommon crop types', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-k', 'Number of SMOTE neighbours', 'int', TRUE, 5, TRUE, 'Number of SMOTE neighbours', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-target', 'Target sample count for SMOTE', 'int', TRUE, 5, TRUE, 'Target sample count for SMOTE', '{ "bounds": { "min": 0, "max": 5000 } }');

-- -----------------------------------------------------------
-- S4C_L4B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4b', 'S4C L4B Product Max Age (days)', 'int', false, 7, FALSE, 'S4C L4B Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4b.keep_job_folders', 'Keep S4C L4B temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4B temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4b.slurm_qos', 'Slurm QOS for S4C L4B processor', 'string', true, 8, FALSE, 'Slurm QOS for S4C L4B processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4b', 'Path for S4C L4B temporary files', 'string', false, 1, FALSE, 'Path for S4C L4B temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.default_config_path', 'The default configuration files for all L4B processors', 'file', FALSE, 19, FALSE, 'The default configuration files for all L4B processors', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.end_date', 'End date for the mowing detection', 'string', FALSE, 19, TRUE, 'End date for the mowing detection', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_amp', 'The list of AMP products', 'select', FALSE, 19, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_cohe', 'The list of COHE products', 'select', FALSE, 19, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_ndvi', 'The list of NDVI products', 'select', FALSE, 19, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s1_s2_startdate_diff', 'Offset in days between S1 and S2 start dates', 'string', TRUE, 19, TRUE, 'Offset in days between S1 and S2 start dates', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.start_date', 'Start date for the mowing detection', 'string', FALSE, 19, TRUE, 'Start date for the mowing detection', NULL);

-- -----------------------------------------------------------
-- S4C_L4C Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4c', 'S4C L4C Product Max Age (days)', 'int', false, 7, FALSE, 'S4C L4C Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4c.keep_job_folders', 'Keep S4C L4C temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4C temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4c.slurm_qos', 'Slurm QOS for S4C L4C processor', 'string', true, 8, FALSE, 'Slurm QOS for S4C L4C processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4c', 'Path for S4C L4C temporary files', 'string', false, 1, FALSE, 'Path for S4C L4C temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.default_config_path', 'The default configuration files for all L4C processors', 'file', FALSE, 20, FALSE, 'The default configuration files for all L4C processors', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_amp', 'The list of AMP products', 'select', FALSE, 20, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_cohe', 'The list of COHE products', 'select', FALSE, 20, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_ndvi', 'The list of NDVI products', 'select', FALSE, 20, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.markers_add_no_data_rows', 'Add in markers parcel rows containg only NA/NA1/NR', 'bool', true, 20, true, 'Add in markers parcel rows containg only NA/NA1/NR', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.prds_per_group', 'Data extraction number of products per group', 'int', FALSE, 20, FALSE, 'Data extraction number of products per group', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.sched_prds_hist_file', 'File where the list of the scheduled L4Cs is kept', 'string', true, 20, FALSE, 'File where the list of the scheduled L4Cs is kept', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tillage_monitoring', 'Enable or disable tillage monitoring', 'int', false, 20, true, 'Enable or disable tillage monitoring', FALSE, 'Enable or disable tillage monitoring', NULL);

-- -----------------------------------------------------------
-- S4C_MDB1 Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s4c_mdb1.slurm_qos', 'Slurm QOS for MDB1 processor', 'string', true, 8, FALSE, 'Slurm QOS for MDB1 processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_mdb1', 'Path for S4C MDB1 temporary files', 'string', false, 1, FALSE, 'Path for S4C MDB1 temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_enabled', 'AMP markers extraction enabled', 'bool', true, 26, FALSE, 'AMP markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.cohe_enabled', 'COHE markers extraction enabled', 'bool', true, 26, FALSE, 'COHE markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.data_extr_dir', 'Location for the MDB1 data extration files', 'string', true, 26, FALSE, 'Location for the MDB1 data extration files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fapar_enabled', 'FAPAR markers extraction enabled', 'bool', true, 26, FALSE, 'FAPAR markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fcover_enabled', 'FCOVER markers extraction enabled', 'bool', true, 26, FALSE, 'FCOVER markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_amp', 'The list of AMP products', 'select', FALSE, 26, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_cohe', 'The list of COHE products', 'select', FALSE, 26, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_ndvi', 'The list of NDVI products', 'select', FALSE, 26, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.lai_enabled', 'LAI markers extraction enabled', 'bool', true, 26, FALSE, 'LAI markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndvi_enabled', 'NDVI markers extraction enabled', 'bool', true, 26, FALSE, 'NDVI markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', 'AMP VV/VH markers extraction enabled', 'bool', true, 26, FALSE, 'AMP VV/VH markers extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.valid_pixels_enabled', 'Number of valid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Number of valid pixels per parcels extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab02_enabled', 'Reflectance band B02 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B02 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab03_enabled', 'Reflectance band B03 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B03 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab04_enabled', 'Reflectance band B04 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B04 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab05_enabled', 'Reflectance band B05 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B05 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab06_enabled', 'Reflectance band B06 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B06 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab07_enabled', 'Reflectance band B07 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B07 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab08_enabled', 'Reflectance band B08 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B08 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab8a_enabled', 'Reflectance band B8A markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B8A markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab11_enabled', 'Reflectance band B11 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B11 markers extraction enabled', NULL); 
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab12_enabled', 'Reflectance band B12 markers extraction enabled', 'bool', true, 26, FALSE, 'Reflectance band B12 markers extraction enabled', NULL); 

-- -----------------------------------------------------------
-- Fmask Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.fmask.enabled', 'Controls whether to run Fmask on optical products', 'bool', false, 2, FALSE, 'Controls whether to run Fmask on optical products', NULL););
