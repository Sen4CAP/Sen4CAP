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
INSERT INTO config_metadata VALUES ('executor.module.path.files-remover', 'Removes the given files (ex. cleanup of intermediate files)', 'file', false, 8, FALSE, 'Removes the given files (ex. cleanup of intermediate files)', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.gdalbuildvrt', 'Path for gdalbuildvrt', 'file', true, 8, FALSE, 'Path for gdalbuildvrt', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.gdal_translate', 'Path for gdal_translate', 'file', true, 8, FALSE, 'Path for gdal_translate', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.image-classifier', 'Image Classifier Path', 'file', true, 8, FALSE, 'Image Classifier Path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.image-compression', 'Image compression', 'file', true, 8, FALSE, 'Image compression', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.lsms-segmentation', 'LSMS segmentation', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.lsms-small-regions-merging', 'LSMS small regions merging', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.mean-shift-smoothing', 'Mean shift smoothing', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.no-operation-step', 'A job no operation step executable', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.train-images-classifier', 'Train Images Classifier Path', 'file', true, 8);

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

-- -----------------------------------------------------------
-- LPIS configuration Specific Keys
-- -----------------------------------------------------------


-- -----------------------------------------------------------
-- L3A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l3a', 'L3A Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('executor.processor.l3a.keep_job_folders', 'Keep L3A temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l3a.slurm_qos', 'Slurm QOS for composite processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3a', 'Path for L3A temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.l3a.bandsmapping', 'Bands mapping file for S2', 'file', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.cloud_optimized_geotiff_output', 'Generate L3A Cloud Optimized Geotiff outputs', 'bool', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.generate_20m_s2_resolution', 'Specifies if composite for S2 20M resolution should be generated', 'int', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.half_synthesis', 'Half synthesis interval in days', 'int', false, 3, true, 'Half synthesis');
INSERT INTO config_metadata VALUES ('processor.l3a.lut_path', 'L3A LUT file path', 'file', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.preproc.scatcoeffs_10m', 'Scattering coefficients file for S2 10 m', 'file', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.preproc.scatcoeffs_20m', 'Scattering coefficients file for S2 20 m', 'file', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.sched_wait_proc_inputs', 'L3A Composite scheduled jobs wait for products to become available', 'int', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.synth_date_sched_offset', 'Difference in days between the scheduled and the synthesis date', 'int', false, 3);
INSERT INTO config_metadata VALUES ('processor.l3a.weight.aot.maxaot', 'Maximum value of the linear range for weights w.r.t. AOT', 'float', true, 3, true, 'Maximum value of the linear range for weights w.r.t. AOT');
INSERT INTO config_metadata VALUES ('processor.l3a.weight.aot.maxweight', 'Maximum weight depending on AOT', 'float', true, 3, true, 'Maximum weight depending on AOT');
INSERT INTO config_metadata VALUES ('processor.l3a.weight.aot.minweight', 'Minimum weight depending on AOT', 'float', true, 3, true, 'Minimum weight depending on AOT');
INSERT INTO config_metadata VALUES ('processor.l3a.weight.cloud.coarseresolution', 'Coarse resolution for quicker convolution', 'int', true, 3, true, 'Coarse resolution for quicker convolution');
INSERT INTO config_metadata VALUES ('processor.l3a.weight.cloud.sigmalarge', 'Standard deviation of gaussian filter for distance to large clouds', 'float', true, 3, true, 'Standard deviation of gaussian filter for distance to small clouds');
INSERT INTO config_metadata VALUES ('processor.l3a.weight.cloud.sigmasmall', 'Standard deviation of gaussian filter for distance to small clouds', 'float', true, 3, true, 'Standard deviation of gaussian filter for distance to large clouds');
INSERT INTO config_metadata VALUES ('processor.l3a.weight.total.weightdatemin', 'Minimum weight at edge of the synthesis time window', 'float', true, 3, true, 'Minimum weight at edge of the synthesis time window');

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
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3c.keep_job_folders', 'Keep L3C temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3c.slurm_qos', 'Slurm QOS for LAI processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.s2a_l3c', 'Path for L3C temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.s2a_l3c.localwnd.bwr', 'Backward radius of the window for N-day reprocessing', 'int', false, 24, true, 'Backward window');
INSERT INTO config_metadata VALUES ('processor.s2a_l3c.localwnd.fwr', 'Forward radius of the window for N-day reprocessing', 'int', true, 24, true, 'Forward window');
INSERT INTO config_metadata VALUES ('processor.s2a_l3c.lut_path', 'L3C LUT file path', 'file', false, 24);

-- -----------------------------------------------------------
-- S2A_L3D Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3d.keep_job_folders', 'Keep L3D temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3d.slurm_qos', 'Slurm QOS for LAI processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.s2a_l3d', 'Path for L3D temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.s2a_l3d.lut_path', 'L3D LUT file path', 'file', false, 25);

-- -----------------------------------------------------------
-- L3E Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.l3e.keep_job_folders', 'Keep L3E temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l3e.slurm_qos', 'Slurm QOS for Pheno NDVI processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3e', 'Path for L3E temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.l3e.cloud_optimized_geotiff_output', 'Generate L3E Cloud Optimized Geotiff outputs', 'bool', false, 18);
INSERT INTO config_metadata VALUES ('processor.l3e.sched_wait_proc_inputs', 'L3E PhenoNDVI scheduled jobs wait for products to become available', 'int', false, 18);

-- -----------------------------------------------------------
-- L4A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l4a', 'L4A Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('executor.processor.l4a.keep_job_folders', 'Keep L4A temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l4a.slurm_qos', 'Slurm QOS for CropMask processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.l4a', 'Path for L4A temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier', 'Random forest clasifier / SVM classifier choices=[rf, svm]', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.field', 'Classifier field', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.rf.max', 'maximum depth of the trees used for Random Forest classifier', 'int', true, 5, true, 'Max depth', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.rf.min', 'minimum number of samples in each node used by the classifier', 'int', true, 5, true, 'Minimum number of samples', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.rf.nbtrees', 'The number of trees used for training', 'int', true, 5, true, 'Training trees', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.svm.k', 'Classifier SVM K', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.svm.opt', 'Classifier SVM Opt', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.cloud_optimized_geotiff_output', 'Generate L4A Cloud Optimized Geotiff outputs', 'bool', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.erode-radius', 'The radius used for erosion', 'int', true, 5, true, 'Erosion radius');
INSERT INTO config_metadata VALUES ('processor.l4a.lut_path', 'L4A LUT file path', 'file', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.mahalanobis-alpha', 'The parameter alpha used by the mahalanobis function', 'float', true, 5, true, 'Mahalanobis alpha');
INSERT INTO config_metadata VALUES ('processor.l4a.max-parallelism', 'Tiles to classify in parallel', 'int', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.min-area', 'The min nb of pixel used in crop/nocrop decision when equal samples', 'int', true, 5, true, 'The minium number of pixels', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4a.mission', 'The main mission for the time series', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.nbcomp', 'The number of components used by dimensionality reduction', 'int', true, 5, true, 'Number of components');
INSERT INTO config_metadata VALUES ('processor.l4a.random_seed', 'The random seed used for training', 'float', true, 5, true, 'Random seed');
INSERT INTO config_metadata VALUES ('processor.l4a.range-radius', 'The range radius defining the radius (expressed in radiometry unit) in the multispectral space', 'float', true, 5, true, 'Range radius');
INSERT INTO config_metadata VALUES ('processor.l4a.reference-map', 'Reference map for crop mask with no in-situ data', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.reference_data_dir', 'CropMask folder where insitu data are checked', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.sample-ratio', 'The ratio between the validation and training polygons', 'float', true, 5, true, 'Ratio');
INSERT INTO config_metadata VALUES ('processor.l4a.sched_wait_proc_inputs', 'L4A Crop Mask scheduled jobs wait for products to become available', 'int', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.segmentation-minsize', 'Minimum size of a region (in pixel unit) in segmentation.', 'int', true, 5, true, 'Minim size of a region');
INSERT INTO config_metadata VALUES ('processor.l4a.segmentation-spatial-radius', 'The spatial radius of the neighborhood used for segmentation', 'int', true, 5, true, 'Spatial radius');
INSERT INTO config_metadata VALUES ('processor.l4a.skip-segmentation', 'Skip L4A segmentation', 'bool', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.smoothing-lambda', 'The lambda parameter used in data smoothing', 'float', true, 5, true, 'Lambda');
INSERT INTO config_metadata VALUES ('processor.l4a.temporal_resampling_mode', 'The temporal resampling mode choices=[resample, gapfill]', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.tile-threads-hint', 'Threads to use for classification of a tile', 'int', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.training-samples-number', 'The number of samples included in the training set', 'int', true, 5, true, 'Training set sample');
INSERT INTO config_metadata VALUES ('processor.l4a.window', 'The window expressed in number of records used for the temporal features extraction', 'int', true, 5, true, 'Window records');

-- -----------------------------------------------------------
-- L4B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l4b', 'L4B Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('executor.processor.l4b.keep_job_folders', 'Keep L4B temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l4b.slurm_qos', 'Slurm QOS for CropType processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.l4b', 'Path for L4B temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier', 'Random forest clasifier / SVM classifier choices=[rf, svm]', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.field', 'Training samples feature name', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.rf.max', 'maximum depth of the trees used for Random Forest classifier', 'int', true, 6, true, 'Random Forest classifier max depth', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.rf.min', 'minimum number of samples in each node used by the classifier', 'int', true, 6, true, 'Minimum number of samples', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.rf.nbtrees', 'The number of trees used for training', 'int', true, 6, true, 'Training trees', '{"min":"0","step":"1","max":"","type":"classifier"}');
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.svm.k', 'Type of kernel', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.svm.opt', 'Automatic optimisation of the parameters', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.cloud_optimized_geotiff_output', 'Generate L4B Cloud Optimized Geotiff outputs', 'bool', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.crop-mask', 'Crop mask file path or product folder to be used', 'file', false, 6, true, 'Crop masks', '{"name":"cropMask"}');
INSERT INTO config_metadata VALUES ('processor.l4b.lut_path', 'L4B LUT file path', 'file', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.max-parallelism', 'Tiles to classify in parallel', 'int', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.mission', 'The main mission for the time series', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.random_seed', 'The random seed used for training', 'float', true, 6, true, 'Random seed');
INSERT INTO config_metadata VALUES ('processor.l4b.sample-ratio', 'The ratio between the validation and training polygons', 'float', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.sched_wait_proc_inputs', 'L4B Crop Type scheduled jobs wait for products to become available', 'int', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.temporal_resampling_mode', 'The temporal resampling mode choices=[resample, gapfill]', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.tile-threads-hint', 'Threads to use for classification of a tile', 'int', false, 6);

-- -----------------------------------------------------------
-- S4C_L4A Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- S4C_L4B Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- S4C_L4C Specific Keys
-- -----------------------------------------------------------

-- -----------------------------------------------------------
-- S4C_MDB1 Specific Keys
-- -----------------------------------------------------------

