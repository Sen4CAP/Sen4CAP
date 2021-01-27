INSERT INTO config_metadata VALUES ('archiver.archive_path', 'Archive Path', 'string', false, 7);

INSERT INTO config_metadata VALUES ('archiver.max_age.l2a', 'L2A Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('archiver.max_age.l3b', 'L3B Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4a', 'L4A Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4b', 'L4A Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4c', 'L4A Product Max Age (days)', 'int', false, 7);

INSERT INTO config_metadata VALUES ('downloader.enabled', 'Downloader is enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('downloader.l8.enabled', 'L8 downloader is enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('downloader.l8.max-retries', 'Maximum retries for downloading a product', 'int', false, 15);
INSERT INTO config_metadata VALUES ('downloader.l8.write-dir', 'Write directory for Landsat8', 'string', false, 15);
INSERT INTO config_metadata VALUES ('downloader.max-cloud-coverage', 'Maximum Cloud Coverage (%)', 'int', false, 15);
INSERT INTO config_metadata VALUES ('downloader.s1.enabled', 'S1 downloader is enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('downloader.s2.enabled', 'S2 downloader is enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('downloader.s2.max-retries', 'Maximum retries for downloading a product', 'int', false, 15);
INSERT INTO config_metadata VALUES ('downloader.s2.write-dir', 'Write directory for Sentinel2', 'string', false, 15);
INSERT INTO config_metadata VALUES ('downloader.start.offset', 'Season start offset in months', 'int', false, 15);
INSERT INTO config_metadata VALUES ('downloader.s1.write-dir', 'Write directory for Sentinel1', 'string', false, 15);
INSERT INTO config_metadata VALUES ('downloader.use.esa.l2a', 'Enable S2 L2A ESA products download', 'bool', false, 15);

INSERT INTO config_metadata VALUES ('executor.listen-ip', 'Executor IP Address', 'string', true, 8);
INSERT INTO config_metadata VALUES ('executor.listen-port', 'Executor Port', 'int', true, 8);

INSERT INTO config_metadata VALUES ('executor.module.path.color-mapping', 'Color Mapping Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.compression', 'Compression Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.compute-confusion-matrix', 'Compute Confusion Matrix Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.compute-image-statistics', 'Compute image statistics', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.compute-images-statistics', 'Compute Images Statistics Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.concatenate-images', 'Concatenate Images Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.crop-mask-fused', 'Crop mask script with stratification', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.crop-type-fused', 'Crop type script with stratification', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.dimensionality-reduction', 'Dimensionality reduction', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.files-remover', 'Removes the given files (ex. cleanup of intermediate files)', 'file', false, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.gdalbuildvrt', 'Path for gdalbuildvrt', 'bool', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.gdal_translate', 'Path for gdal_translate', 'bool', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.image-classifier', 'Image Classifier Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.image-compression', 'Image compression', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.end-of-job', 'End of a multi root steps job', 'file', true, 8);

INSERT INTO config_metadata VALUES ('executor.module.path.s4c-crop-type', 'L4A Crop Type main execution script path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s1', 'L4B S1 main execution script path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s2', 'L4B S2 main execution script path', 'file', true, 8);

INSERT INTO config_metadata VALUES ('executor.processor.l2a.name', 'L2A Processor Name', 'string', true, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l2a.path', 'L2A Processor Path', 'file', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l3b.keep_job_folders', 'Keep L3B/C/D temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l3b.name', 'L3B Processor Name', 'string', true, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l3b.path', 'L3B Processor Path', 'file', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l3b.slurm_qos', 'Slurm QOS for LAI processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4a.slurm_qos', 'Slurm QOS for L4A processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4b.slurm_qos', 'Slurm QOS for L4B processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4c.slurm_qos', 'Slurm QOS for L4C processor', 'string', true, 8);

INSERT INTO config_metadata VALUES ('executor.shapes_dir', 'Tiles shapes directory', 'directory', true, 8);
INSERT INTO config_metadata VALUES ('executor.wrapper-path', 'Processor Wrapper Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.wrp-executes-local', 'Execution of wrappers are only local', 'int', true, 8);
INSERT INTO config_metadata VALUES ('executor.wrp-send-retries-no', 'Number of wrapper retries to connect to executor when TCP error', 'int', true, 8);
INSERT INTO config_metadata VALUES ('executor.wrp-timeout-between-retries', 'Timeout between wrapper retries to executor when TCP error', 'int', true, 8);

INSERT INTO config_metadata VALUES ('general.scratch-path', 'Default path for temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3b', 'Path for L3B/L3C/L3D temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4a', 'Path for L4A temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4b', 'Path for L4B temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4c', 'Path for L4C temporary files', 'string', false, 1);

INSERT INTO config_metadata VALUES ('http-listener.listen-port', 'Dashboard Listen Port', 'int', true, 12);
INSERT INTO config_metadata VALUES ('http-listener.root-path', 'Document Root Path', 'directory', true, 12);

INSERT INTO config_metadata VALUES ('l8.enabled', 'L8 is enabled', 'bool', false, 15);

INSERT INTO config_metadata VALUES ('monitor-agent.disk-path', 'Disk Path To Monitor For Space', 'directory', false, 13);
INSERT INTO config_metadata VALUES ('monitor-agent.scan-interval', 'Measurement Interval (s)', 'int', true, 13);

INSERT INTO config_metadata VALUES ('processor.l3b.cloud_optimized_geotiff_output', 'Generate L3B Cloud Optimized Geotiff outputs', 'bool', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.generate_models', 'Specifies if models should be generated or not for LAI', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.global_bv_samples_file', 'Common LAI BV sample distribution file', 'file', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.laibandscfgfile', 'Configuration of the bands to be used for LAI', 'file', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.lut_path', 'L3B LUT file path', 'file', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.modelsfolder', 'Folder where the models are located', 'directory', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fapar', 'L3B LAI processor will produce FAPAR', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fcover', 'L3B LAI processor will produce FCOVER', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_lai', 'L3B LAI processor will produce LAI', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndvi', 'L3B LAI processor will produce NDVI', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_in_domain_flags', 'L3B processor will input domain flags', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.rsrcfgfile', 'L3B RSR file configuration for ProsailSimulator', 'file', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.tiles_filter', 'processor.l3b.lai.tiles_filter', 'string', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.use_inra_version', 'L3B LAI processor will use INRA algorithm implementation', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.use_lai_bands_cfg', 'Use LAI bands configuration file', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.production_interval', 'The backward processing interval from the scheduled date for L3B products', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.reproc_production_interval', 'The backward processing interval from the scheduled date for L3C products', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.sched_wait_proc_inputs', 'L3B/L3C/L3D LAI scheduled jobs wait for products to become available', 'int', false, 4);
INSERT INTO config_metadata VALUES ('processor.l3b.l1c_availability_days', 'Number of days before current scheduled date within we must have L1C processed (default 20)', 'int', false, 4);

INSERT INTO config_metadata VALUES ('processor.l4a.reference_data_dir', 'Crop Tye folder where insitu data are checked', 'string', false, 5);

INSERT INTO config_metadata VALUES ('processor.s4c_l4a.lc', 'LC classes to assess', 'string', TRUE, 5, TRUE, 'LC classes to assess', '{"min":"","step":"","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.mode', 'Mode', 'string', FALSE, 5, TRUE, 'Mode (both, s1-only, s2-only)', '{"min":"","step":"","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s2-pix', 'Minimum number of S2 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s1-pix', 'Minimum number of S1 pixels', 'int', TRUE, 5, TRUE, 'Minimum number of S1 pixels', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.best-s2-pix', 'Minimum number of S2 pixels for parcels to use in training', 'int', TRUE, 5, TRUE, 'Minimum number of S2 pixels for parcels to use in training', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-min', 'Minimum parcels to assess a crop type', 'int', TRUE, 5, TRUE, 'Minimum parcels to assess a crop type', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-h', 'Upper threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Upper threshold for parcel counts by crop type', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-l', 'Lower threshold for parcel counts by crop type', 'int', TRUE, 5, TRUE, 'Lower threshold for parcel counts by crop type', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-h', 'Training ratio for common crop types', 'float', TRUE, 5, TRUE, 'Training ratio for common crop types', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-l', 'Training ratio for uncommon crop types', 'float', TRUE, 5, TRUE, 'Training ratio for uncommon crop types', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-target', 'Target sample count for SMOTE', 'int', TRUE, 5, TRUE, 'Target sample count for SMOTE', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-k', 'Number of SMOTE neighbours', 'int', TRUE, 5, TRUE, 'Number of SMOTE neighbours', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.num-trees', 'Number of RF trees', 'int', TRUE, 5, TRUE, 'Number of RF trees', '{"min":"0","step":"1","max":""}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-node-size', 'Minimum node size', 'int', TRUE, 5, TRUE, 'Minimum node size', '{"min":"0","step":"1","max":""}');

INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_amp', 'The list of AMP products', 'select', FALSE, 19, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_cohe', 'The list of COHE products', 'select', FALSE, 19, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_ndvi', 'The list of NDVI products', 'select', FALSE, 19, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}');

INSERT INTO config_metadata VALUES ('processor.s4c_l4b.start_date', 'Start date for the mowing detection', 'string', FALSE, 19, TRUE, 'Start date for the mowing detection');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.end_date', 'End date for the mowing detection', 'string', FALSE, 19, TRUE, 'End date for the mowing detection');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s1_s2_startdate_diff', 'Offset in days between S1 and S2 start dates', 'string', TRUE, 19, TRUE, 'Offset in days between S1 and S2 start dates');

INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_amp', 'The list of AMP products', 'select', FALSE, 20, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_cohe', 'The list of COHE products', 'select', FALSE, 20, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_ndvi', 'The list of NDVI products', 'select', FALSE, 20, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}');

INSERT INTO config_metadata VALUES ('processor.s4c_l4b.config_path', 'The default configuration files for all L4B processors', 'file', FALSE, 19);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.config_path', 'The default configuration files for all L4C processors', 'file', FALSE, 20);

INSERT INTO config_metadata VALUES ('processor.lpis.path', 'The path to the pre-processed LPIS products', 'string', false, 21);

INSERT INTO config_metadata VALUES ('resources.working-mem', 'OTB applications working memory (MB)', 'int', true, 14);

INSERT INTO config_metadata VALUES ('s1.enabled', 'S1 is enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('processor.l2s1.path', 'The path where the S1 L2 products will be created', 'string', false, 15);
INSERT INTO config_metadata VALUES ('processor.l2s1.work.dir', 'The path where to create the temporary S1 L2A files', 'string', false, 15);
INSERT INTO config_metadata VALUES ('processor.l2s1.enabled', 'S1 pre-processing enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('s2.enabled', 'S2 is enabled', 'bool', false, 15);

INSERT INTO config_metadata VALUES ('scheduled.lookup.enabled', 'Scheduled lookup is enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.deleteAfter', 'Delete the products after they were uploaded to object storage', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.enabled', 'Scheduled object storage move enabled', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.product.types', 'Product types to move to object storage (separated by ;)', 'string', false, 15);
INSERT INTO config_metadata VALUES ('scheduled.retry.enabled', 'Scheduled retry is enabled', 'bool', false, 15);

INSERT INTO config_metadata VALUES ('site.upload-path', 'Upload path', 'string', false, 17);

INSERT INTO config_metadata VALUES ('downloader.s2.forcestart', 'Forces the S2 download to start again from the beginning of the season', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('downloader.l8.forcestart', 'Forces the L8 download to start again from the beginning of the season', 'bool', false, 15);
INSERT INTO config_metadata VALUES ('downloader.s1.forcestart', 'Forces the S1 download to start again from the beginning of the season', 'bool', false, 15);

INSERT INTO config_metadata VALUES ('downloader.skip.existing', 'If enabled, products downloaded for another site will be duplicated, in database only, for the current site', 'bool', false, 15);

INSERT INTO config_metadata VALUES ('processor.s4c_l4c.prds_per_group', 'Data extraction number of products per group', 'int', FALSE, 20);

INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.max-retries', 'Number of retries for the L2A processor', 'int', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.num-workers', 'Parallelism degree of the L2A processor', 'int', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.retry-interval', 'Retry interval for the L2A processor', 'string', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.compress-tiffs', 'Compress the resulted L2A TIFF files', 'bool', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.cog-tiffs', 'Produce L2A tiff files as Cloud Optimized Geotiff', 'bool', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.gipp-path', 'MAJA GIPP path', 'directory', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.launcher', 'MAJA binary location', 'file', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-sre', 'Remove SRE files from resulted L2A product', 'bool', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.maja.remove-fre', 'Remove FRE files from resulted L2A product', 'bool', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.sen2cor.gipp-path', 'Sen2Cor GIPP path', 'directory', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.optical.output-path', 'path for L2A products', 'directory', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.srtm-path', 'path where the SRTM files are to be found', 'directory', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.swbd-path', 'path where the SWBD files are to be found', 'directory', false, 2);
INSERT INTO config_metadata VALUES ('processor.l2a.working-dir', 'working directory', 'string', false, 2);

-- Tillage processor keys               
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tillage_monitoring', 'Enable or disable tillage monitoring', 'int', false, 20, true, 'Enable or disable tillage monitoring');

-- Marker database keys                
INSERT INTO config_metadata VALUES ('executor.processor.s4c_mdb1.slurm_qos', 'Slurm QOS for MDB1 processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_amp', 'The list of AMP products', 'select', FALSE, 26, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_cohe', 'The list of COHE products', 'select', FALSE, 26, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_ndvi', 'The list of NDVI products', 'select', FALSE, 26, TRUE, 'Available NDVI input files', '{"name":"inputFiles_NDVI[]","product_type_id":3}');
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_mdb1', 'Path for S4C MDB1 temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('executor.module.path.extract-l4c-markers', 'Script for extracting L4C markers', 'file', true, 8);

INSERT INTO config_metadata VALUES ('processor.s4c_l4c.markers_add_no_data_rows', 'Add in markers parcel rows containg only NA/NA1/NR', 'bool', true, 20, true, 'Add in markers parcel rows containg only NA/NA1/NR');

INSERT INTO config_metadata VALUES ('executor.module.path.mdb-csv-to-ipc-export', 'Script for extracting markers csv to IPC file', 'file', true, 8);

INSERT INTO config_metadata VALUES ('processor.s4c_l4c.sched_prds_hist_file', 'File where the list of the scheduled L4Cs is kept', 'string', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndvi_enabled', 'NDVI markers extraction enabled', 'bool', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_enabled', 'AMP markers extraction enabled', 'bool', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.cohe_enabled', 'COHE markers extraction enabled', 'bool', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.lai_enabled', 'LAI markers extraction enabled', 'bool', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fapar_enabled', 'FAPAR markers extraction enabled', 'bool', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fcover_enabled', 'FCOVER markers extraction enabled', 'bool', true, 26);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.data_extr_dir', 'Location for the MDB1 data extration files', 'string', true, 26);

-- Executor/orchestrator/scheduler changes
INSERT INTO config_metadata VALUES ('general.inter-proc-com-type', 'Type of the interprocess communication', 'string', false, 1);
INSERT INTO config_metadata VALUES ('executor.resource-manager.name', 'Executor resource manager name', 'string', false, 1);
INSERT INTO config_metadata VALUES ('executor.http-server.listen-ip', 'Executor HTTP listen ip', 'string', false, 1);
INSERT INTO config_metadata VALUES ('executor.http-server.listen-port', 'Executor HTTP listen port', 'string', false, 1);
INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-ip', 'Orchestrator HTTP listen ip', 'string', false, 1);
INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-port', 'Orchestrator HTTP listen port', 'string', false, 1);

