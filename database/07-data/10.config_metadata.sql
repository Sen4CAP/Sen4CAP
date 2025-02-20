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

INSERT INTO config_metadata VALUES ('gdal.apps.path', 'Gdal applications path', 'string', false, 1, FALSE, 'Gdal applications path', NULL) ;
INSERT INTO config_metadata VALUES ('gdal.installer', 'Gdal installer', 'string', false, 1, FALSE, 'Gdal installer', NULL) ;
INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_csv_file_name_pattern', 'Parcels product csv file name pattern', 'string', false, 1, FALSE, 'Parcels product csv file name pattern', NULL) ;
INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_optical_file_name_pattern', 'Parcels product optical file name pattern', 'string', false, 1, FALSE, 'Parcels product optical file name pattern', NULL) ;
INSERT INTO config_metadata VALUES ('general.parcels_product.parcels_sar_file_name_pattern', 'Parcels product SAR file name pattern', 'string', false, 1, FALSE, 'Parcels product SAR file name pattern', NULL) ;
INSERT INTO config_metadata VALUES ('general.parcels_product.parcel_id_col_name', 'Parcels parcels id columns name', 'string', false, 1, FALSE, 'Parcels parcels id columns name', NULL) ;

INSERT INTO config_metadata VALUES ('disk.monitor.interval', 'Disk Monitor interval', 'int', false, 13, FALSE, 'Disk Monitor interval', NULL);

INSERT INTO config_metadata VALUES ('general.docker_script_unit_image', 'Sen4CAP services scripts docker image', 'string', false, 1, FALSE, 'Sen4CAP services scripts docker image', NULL) ;

-- -----------------------------------------------------------
-- Executor/orchestrator/scheduler Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.http-server.listen-ip', 'Executor HTTP listen ip', 'string', false, 1, FALSE, 'Executor HTTP listen ip', NULL);
INSERT INTO config_metadata VALUES ('executor.http-server.listen-port', 'Executor HTTP listen port', 'string', false, 1, FALSE, 'Executor HTTP listen port', NULL);
INSERT INTO config_metadata VALUES ('executor.listen-ip', 'Executor IP Address', 'string', true, 8, FALSE, 'Executor IP Address', NULL);
INSERT INTO config_metadata VALUES ('executor.listen-port', 'Executor Port', 'int', true, 8, FALSE, 'Executor Port', NULL);
INSERT INTO config_metadata VALUES ('executor.resource-manager.name', 'Executor resource manager name', 'string', false, 1, FALSE, 'Executor resource manager name', NULL);
INSERT INTO config_metadata VALUES ('executor.sacct-max-retries', 'Slurm SACCT max retries', 'int', true, 1, FALSE, 'Slurm SACCT max retries', NULL);
INSERT INTO config_metadata VALUES ('executor.wrapper-path', 'Processor Wrapper Path', 'file', true, 8, FALSE, 'Processor Wrapper Path', NULL);
INSERT INTO config_metadata VALUES ('executor.wrp-executes-local', 'Execution of wrappers are only local', 'int', true, 8, FALSE, 'Execution of wrappers are only local', NULL);
INSERT INTO config_metadata VALUES ('executor.wrp-send-retries-no', 'Number of wrapper retries to connect to executor when TCP error', 'int', true, 8, FALSE, 'Number of wrapper retries to connect to executor when TCP error', NULL);
INSERT INTO config_metadata VALUES ('executor.wrp-timeout-between-retries', 'Timeout between wrapper retries to executor when TCP error', 'int', true, 8, FALSE, 'Timeout between wrapper retries to executor when TCP error', NULL);
INSERT INTO config_metadata VALUES ('general.inter-proc-com-type', 'Type of the interprocess communication', 'string', false, 1, FALSE, 'Type of the interprocess communication', NULL);
INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-ip', 'Orchestrator HTTP listen ip', 'string', false, 1, FALSE, 'Orchestrator HTTP listen ip', NULL);
INSERT INTO config_metadata VALUES ('orchestrator.http-server.listen-port', 'Orchestrator HTTP listen port', 'string', false, 1, FALSE, 'Orchestrator HTTP listen port', NULL);

INSERT INTO config_metadata VALUES ('general.orchestrator.use_docker', 'Orchestrator use docker when invoking processors', 'int', false, 1, FALSE, 'Use docker when invoking processors', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.docker_image', 'Processors docker image', 'string', false, 1, FALSE, 'Processors docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.earth-signature.docker_image', 'EarthSignature docker image', 'string', false, 1, FALSE, 'EarthSignature  docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.docker_add_mounts', 'Additional docker mounts for processors', 'string', false, 1, FALSE, 'Additional docker mounts for processors', NULL) ;
-- TODO: The next ones should be moved in the sections corresponding to processors
INSERT INTO config_metadata VALUES ('general.orchestrator.export-product-launcher.use_docker', 'Export product use docker', 'int', false, 1, FALSE, 'Export product use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.mdb3-input-tables-extract.docker_image', 'MDB3 input tables extraction docker image', 'string', false, 1, FALSE, 'MDB3 input tables extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.mdb3-input-tables-extract.use_docker', 'MDB3 input tables extraction use docker', 'int', false, 1, FALSE, 'MDB3 input tables extraction use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-extract-products.use_docker', 'S4C L4B products extraction use docker', 'int', false, 1, FALSE, 'S4C L4B products extraction use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-gen-input-shp.use_docker', 'S4C L4B inputs shp extraction use docker', 'int', false, 1, FALSE, 'S4C L4B inputs shp extraction use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-mowing.docker_image', 'S4C L4B docker image', 'string', false, 1, FALSE, 'S4C L4B docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-grassland-mowing.use_docker', 'S4C L4B use docker', 'int', false, 1, FALSE, 'S4C L4B use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-l4a-extract-parcels.use_docker', 'S4C L4A extract parcels use docker', 'int', false, 1, FALSE, 'S4C L4A extract parcels use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-crop-type-mapping.docker_image', 'S4S Crop Type Mapping docker image', 'string', false, 1, FALSE, 'S4S Crop Type Mapping docker image', NULL) ;

INSERT INTO config_metadata VALUES ('orchestrator.check_ancestors.disabled', 'Disable processor wait for inputs', 'bool', false, 1, FALSE, 'Disable processor wait for inputs', NULL) ;

-- Heterogeneity processor
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-preparation.docker_image', 'Heterogeneity cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity cluster preparation docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s2.docker_image', 'Heterogeneity S2 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 cluster analysis docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s1.docker_image', 'Heterogeneity S1 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 cluster analysis docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-extract-s1-list.docker_image', 'Heterogeneity S1 list extractor docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 list extractor docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-temporal-resampling.docker_image', 'Heterogeneity S2 temporal resampling docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 temporal resampling docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-tiles-analysis-merge.docker_image', 'Heterogeneity tiles analysis merge docker image', 'string', false, 1, FALSE, 'Heterogeneity tiles analysis merge docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-period-analysis.docker_image', 'Heterogeneity S2 period analysis docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 period analysis docker image', NULL);

-- Bare soil processor
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-calibration.docker_image', 'Bare Soil S2 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Calibration docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-calibration.docker_image', 'Bare Soil S1 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Calibration docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-model.docker_image', 'Bare Soil S2 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Model docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-model.docker_image', 'Bare Soil S1 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Model docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-markers.docker_image', 'Bare Soil Markers extraction docker image', 'string', false, 1, FALSE, 'Bare Soil Markers extraction docker image', NULL) ;

-- Docker images
INSERT INTO config_metadata VALUES ('general.orchestrator.ndvi-veg-stats.docker_image', 'NDVI vegetation statistics docker image', 'string', false, 1, FALSE, 'NDVI vegetation statistics docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-extract-weather-features.docker_image', 'Yield Weather Features docker image', 'string', false, 1, FALSE, 'Yield Weather Features docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crop-post-processing.docker_image', 'Permanent Crops Post-Processing docker image', 'string', false, 1, FALSE, 'Permanent Crops Post-Processing docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-build-refl-stack-tif.docker_image', 'Permanent Crops build reflectance stack docker image', 'string', false, 1, FALSE, 'Permanent Crops build reflectance stack docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-extract-inputs.docker_image', 'Permanent Crops Extract inputs docker image', 'string', false, 1, FALSE, 'Permanent Crops Extract inputs docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-extract-parcels.docker_image', 'Permanent Crops Extract parcels docker image', 'string', false, 1, FALSE, 'Permanent Crops Extract parcels docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-samples-rasterization.docker_image', 'Permanent Crops samples rasterization docker image', 'string', false, 1, FALSE, 'Permanent Crops samples rasterization docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-sieve.docker_image', 'Permanent Crops crop sieve docker image', 'string', false, 1, FALSE, 'Permanent Crops crop sieve docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-safy-lut.docker_image', 'Yield SAFY LUT docker image', 'string', false, 1, FALSE, 'Yield SAFY LUT docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-safy-optim.docker_image', 'Yield SAFY Optimization docker image', 'string', false, 1, FALSE, 'Yield SAFY Optimization docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-savitzky-golay-wrp.docker_image', 'Yield SU Savitzky Golay docker image', 'string', false, 1, FALSE, 'Yield SU Savitzky Golay docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-savitzky-golay.docker_image', 'Yield Savitzky Golay docker image', 'string', false, 1, FALSE, 'Yield Savitzky Golay docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-crop-types-extraction.docker_image', 'Yield crop types extraction docker image', 'string', false, 1, FALSE, 'Yield crop types extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-esu-aggregate.docker_image', 'Yield SU ESU aggregation docker image', 'string', false, 1, FALSE, 'Yield SU ESU aggregation docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-esu-extraction.docker_image', 'Yield SU ESU extraction docker image', 'string', false, 1, FALSE, 'Yield SU ESU extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-features-extraction-wrp.docker_image', 'Yield SU Features extraction docker image', 'string', false, 1, FALSE, 'Yield SU Features extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-features-extraction.docker_image', 'Yield Features extraction docker image', 'string', false, 1, FALSE, 'Yield Features extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-model.docker_image', 'Yield Model docker image', 'string', false, 1, FALSE, 'Yield Model docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-parcels-extraction.docker_image', 'Yield Parcels extraction docker image', 'string', false, 1, FALSE, 'Yield Parcels extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-reference-extraction.docker_image', 'Reference Yield extraction docker image', 'string', false, 1, FALSE, 'Reference Yield extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-su-merge-yearly-features.docker_image', 'Yield SU yearly features merge docker image', 'string', false, 1, FALSE, 'Yield SU yearly features merge docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-su-model-wrp.docker_image', 'Yield SU Model docker image', 'string', false, 1, FALSE, 'Yield SU Model docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-trend-features-extraction.docker_image', 'Yield SU Trend features extraction docker image', 'string', false, 1, FALSE, 'Yield SU Trend features extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s_perm_crop.docker_image', 'Permanent Crops default docker image', 'string', false, 1, FALSE, 'Permanent Crops default docker image', NULL) ;

INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-run-broceliande.use_docker', 'Broceliande execution use docker', 'int', false, 1, FALSE, 'Broceliande execution use docker', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4s_perm_crop.use_docker', 'Permanent crops use docker default value', 'int', false, 1, FALSE, 'Permanent crops use docker default value', NULL) ;

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
INSERT INTO config_metadata VALUES ('executor.module.path.lsms-segmentation', 'LSMS segmentation', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.lsms-small-regions-merging', 'LSMS small regions merging', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.mean-shift-smoothing', 'Mean shift smoothing', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-crop-type', 'L4A Crop Type main execution script path', 'file', true, 8, FALSE, 'L4A Crop Type main execution script path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s1', 'L4B S1 main execution script path', 'file', true, 8, FALSE, 'L4B S1 main execution script path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-mowing-s2', 'L4B S2 main execution script path', 'file', true, 8, FALSE, 'L4B S2 main execution script path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.train-images-classifier', 'Train Images Classifier Path', 'file', true, 8);
INSERT INTO config_metadata VALUES ('executor.module.path.export-product-launcher', 'Script for exporting L4A/L4C products to shapefiles', 'file', true, 8, FALSE, 'Script for exporting L4A/L4C products to shapefiles', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.l4b_cfg_import', 'Script for importing S4C L4B config file', 'file', true, 8, FALSE, 'Script for importing S4C L4B config file', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.l4c_cfg_import', 'Script for importing S4C L4C config file', 'file', true, 8, FALSE, 'Script for importing S4C L4C config file', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.l4c_practices_export', 'Script for exported S4C L4C files', 'file', true, 8, FALSE, 'Script for exported S4C L4C files', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.l4c_practices_import', 'Script for importing S4C L4C practices file', 'file', true, 8, FALSE, 'Script for importing S4C L4C practices file', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.lpis_import', 'Script for importing S4C LPIS/GSAA file(s)', 'file', true, 8, FALSE, 'Script for importing S4C LPIS/GSAA file(s)', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.mdb3-extract-markers', 'Script for importing MDB3 markers from a TSA result', 'file', true, 8, FALSE, 'Script for importing MDB3 markers from a TSA result', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.mdb3-input-tables-extract', 'Script for preparing MDB3 input tables', 'file', true, 8, FALSE, 'Script for preparing MDB3 input tables', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.ogr2ogr', 'ogr2ogr file path', 'file', true, 8, FALSE, 'ogr2ogr file path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-extract-products', 'Script for extracting S4C L4B input products', 'file', true, 8, FALSE, 'Script for extracting S4C L4B input products', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-grassland-gen-input-shp', 'Script for generating S4C L4B input shapefile', 'file', true, 8, FALSE, 'Script for generating S4C L4B input shapefile', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-l4a-extract-parcels', 'Script for extracting S4C L4A input parcels', 'file', true, 8, FALSE, 'Script for extracting S4C L4A input parcels', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.lpis_list_columns', 'Script for extracting the column names from a shapefile', 'string', true, 8, FALSE, 'Script for extracting the column names from a shapefile', NULL);

INSERT INTO config_metadata VALUES ('executor.module.path.earth-signature', 'Script for earth signature', 'file', true, 8, FALSE, 'Script for earth signature', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.ndvi-veg-stats', 'Script for extracting NDVI vegetation stats', 'file', true, 8, FALSE, 'Script for extracting NDVI vegetation stats', NULL);

INSERT INTO config_metadata VALUES ('executor.module.path.s4s-extract-weather-features', 'Script for extracting S4S Weather Features', 'file', true, 8, FALSE, 'Script for extracting S4S Weather Features', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-merge-all-features-wrp', 'Script for merging all Yield SU features', 'file', true, 8, FALSE, 'Script for merging all Yield SU features', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crop-post-processing', 'Script for Permanent Crops Post Processing', 'file', true, 8, FALSE, 'Script for Permanent Crops Post Processing', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-build-refl-stack-tif', 'Script for Permanent Crops reflectance stack building', 'file', true, 8, FALSE, 'Script for Permanent Crops reflectance stack building', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-extract-inputs', 'Script for Permanent Crops inputs extraction', 'file', true, 8, FALSE, 'Script for Permanent Crops inputs extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-extract-parcels', 'Script for Permanent Crops parcels', 'file', true, 8, FALSE, 'Script for Permanent Crops parcels', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-run-broceliande', 'Script for Permanent Crops Broceliande execution', 'file', true, 8, FALSE, 'Script for Permanent Crops Broceliande execution', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-samples-rasterization', 'Script for Permanent Crops samples rasterization', 'file', true, 8, FALSE, 'Script for Permanent Crops samples rasterization', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-sieve', 'Script for Permanent Crops crop sieve', 'file', true, 8, FALSE, 'Script for Permanent Crops crop sieve', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-safy-lut', 'Script for Yield Safy LUT', 'file', true, 8, FALSE, 'Script for Yield Safy LUT', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-safy-optim', 'Script for Yield Safy Optimization', 'file', true, 8, FALSE, 'Script for Yield Safy Optimization', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-savitzky-golay', 'Script for Yield Savitzky Golay', 'file', true, 8, FALSE, 'Script for Yield Savitzky Golay', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-savitzky-golay-wrp', 'Script for Yield SU Savitzky Golay', 'file', true, 8, FALSE, 'Script for Yield SU Savitzky Golay', NULL);

INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-crop-types-extraction', 'Script for Yield Crop Types extraction', 'file', true, 8, FALSE, 'Script for Yield Crop Types extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-esu-aggregate', 'Script for Yield SU ESU aggregate', 'file', true, 8, FALSE, 'Script for Yield SU ESU aggregate', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-esu-extraction', 'Script for Yield SU ESU extraction', 'file', true, 8, FALSE, 'Script for Yield SU ESU extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-features-extraction', 'Script for Yield features extraction', 'file', true, 8, FALSE, 'Script for Yield features extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-features-extraction-wrp', 'Script for Yield SU features extraction', 'file', true, 8, FALSE, 'Script for Yield SU features extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-model', 'Script for Yield Model execution', 'file', true, 8, FALSE, 'Script for Yield Model execution', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-parcels-extraction', 'Script for Yield parcels extraction', 'file', true, 8, FALSE, 'Script for Yield parcels extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-reference-extraction', 'Script for reference Yield extraction', 'file', true, 8, FALSE, 'Script for reference Yield extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-su-merge-yearly-features', 'Script for Yield SU merging yearly features', 'file', true, 8, FALSE, 'Script for Yield SU merging yearly features', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-su-model-wrp', 'Script for Yield SU Model execution', 'file', true, 8, FALSE, 'Script for Yield SU Model execution', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-trend-features-extraction', 'Script for Yield SU Trend features extraction', 'file', true, 8, FALSE, 'Script for Yield SU Trend features extraction', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s_admin_units_import', 'Script for importing S4S adminstrative units', 'file', true, 8, FALSE, 'Script for importing S4S adminstrative units', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s_parcels_import', 'Script for importing S4S parcels', 'file', true, 8, FALSE, 'Script for importing S4S parcels', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s_yield_safy_import', 'Script for importing SAFY config file', 'file', true, 8, FALSE, 'Script for importing SAFY config file', NULL);

-- -----------------------------------------------------------
-- Downloader Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('downloader.enabled', 'Downloader is enabled', 'bool', false, 15, FALSE, 'Downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.enabled', 'L8 downloader is enabled', 'bool', false, 15, FALSE, 'L8 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.forcestart', 'Forces the L8 download to start again from the beginning of the season', 'bool', false, 15, FALSE, 'Forces the L8 download to start again from the beginning of the season', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.max-retries', 'Maximum retries for downloading a product', 'int', false, 15, FALSE, 'Maximum retries for downloading a product', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.write-dir', 'Write directory for Landsat8', 'string', false, 15, FALSE, 'Write directory for Landsat8', NULL);
INSERT INTO config_metadata VALUES ('downloader.l9.write-dir', 'Write directory for Landsat9', 'string', false, 15, FALSE, 'Write directory for Landsat9', NULL);
INSERT INTO config_metadata VALUES ('downloader.l9.enabled', 'L9 downloader is enabled', 'bool', false, 15, FALSE, 'L9 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.max-cloud-coverage', 'Maximum Cloud Coverage (%)', 'int', false, 15, FALSE, 'Maximum Cloud Coverage (%)', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.enabled', 'S1 downloader is enabled', 'bool', false, 15, FALSE, 'S1 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.forcestart', 'Forces the S1 download to start again from the beginning of the season', 'bool', false, 15, FALSE, 'Forces the S1 download to start again from the beginning of the season', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.write-dir', 'Write directory for Sentinel1', 'string', false, 15, FALSE, 'Write directory for Sentinel1', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.enabled', 'S2 downloader is enabled', 'bool', false, 15, FALSE, 'S2 downloader is enabled', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.forcestart', 'Forces the S2 download to start again from the beginning of the season', 'bool', false, 15, FALSE, 'Forces the S2 download to start again from the beginning of the season', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.max-retries', 'Maximum retries for downloading a product', 'int', false, 15, FALSE, 'Maximum retries for downloading a product', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.write-dir', 'Write directory for Sentinel2', 'string', false, 15, FALSE, 'Write directory for Sentinel2', NULL);
INSERT INTO config_metadata VALUES ('downloader.l8.query.days.back', 'Number of back days for L8 download', 'int', false, 15, FALSE, 'Number of back days for L8 download', NULL);
INSERT INTO config_metadata VALUES ('downloader.s1.query.days.back', 'Number of back days for S1 download', 'int', false, 15, FALSE, 'Number of back days for S1 download', NULL);
INSERT INTO config_metadata VALUES ('downloader.s2.query.days.back', 'Number of back days for S2 download', 'int', false, 15, FALSE, 'Number of back days for S2 download', NULL);
INSERT INTO config_metadata VALUES ('downloader.L8.query.days.back', 'Number of back days for L8 download', 'int', false, 15, FALSE, 'Number of back days for L8 download', NULL);
INSERT INTO config_metadata VALUES ('downloader.S1.query.days.back', 'Number of back days for S1 download', 'int', false, 15, FALSE, 'Number of back days for S1 download', NULL);
INSERT INTO config_metadata VALUES ('downloader.S2.query.days.back', 'Number of back days for S2 download', 'int', false, 15, FALSE, 'Number of back days for S2 download', NULL);
INSERT INTO config_metadata VALUES ('downloader.skip.existing', 'If enabled, products downloaded for another site will be duplicated, in database only, for the current site', 'bool', false, 15, FALSE, 'Use products already downloaded on another site', NULL);
INSERT INTO config_metadata VALUES ('downloader.start.offset', 'Season start offset in months', 'int', false, 15, FALSE, 'Season start offset in months', NULL);
INSERT INTO config_metadata VALUES ('downloader.timeout', 'Timeout between download retries ', 'int', false, 15, FALSE, 'Timeout between download retries ', NULL);
INSERT INTO config_metadata VALUES ('downloader.use.esa.l2a', 'Enable S2 L2A ESA products download', 'bool', false, 15, FALSE, 'Enable S2 L2A ESA products download', NULL);
INSERT INTO config_metadata VALUES ('l8.enabled', 'L8 is enabled', 'bool', false, 15, FALSE, 'L8 is enabled', NULL);
INSERT INTO config_metadata VALUES ('l9.enabled', 'L9 is enabled', 'bool', false, 15, FALSE, 'L9 is enabled', NULL);
INSERT INTO config_metadata VALUES ('s1.enabled', 'S1 is enabled', 'bool', false, 15, FALSE, 'S1 is enabled', NULL);
INSERT INTO config_metadata VALUES ('s2.enabled', 'S2 is enabled', 'bool', false, 15, FALSE, 'S2 is enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.lookup.enabled', 'Scheduled lookup is enabled', 'bool', false, 15, FALSE, 'Scheduled lookup is enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.deleteAfter', 'Delete the products after they were uploaded to object storage', 'bool', false, 15, FALSE, 'Delete the products after they were uploaded to object storage', NULL);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.enabled', 'Scheduled object storage move enabled', 'bool', false, 15, FALSE, 'Scheduled object storage move enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.object.storage.move.product.types', 'Product types to move to object storage (separated by ;)', 'string', false, 15, FALSE, 'Product types to move to object storage (separated by ;)', NULL);
INSERT INTO config_metadata VALUES ('scheduled.retry.enabled', 'Scheduled retry is enabled', 'bool', false, 15, FALSE, 'Scheduled retry is enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.reports.enabled', 'Reports scheduler enabled', 'bool', false, 15, FALSE, 'Reports scheduler enabled', NULL);
INSERT INTO config_metadata VALUES ('scheduled.reports.interval', 'Interval for reports update scheduling', 'int', false, 15, FALSE, 'Interval for reports update scheduling', NULL);
INSERT INTO config_metadata VALUES ('downloader.query.timeout', 'Download query timeout', 'int', false, 15, FALSE, 'Download query timeout', NULL);

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
INSERT INTO config_metadata VALUES ('processor.l2a.s2.implementation', 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)', 'string', false, 2, false, 'L2A processor to use for Sentinel-2 products (`maja` or `sen2cor`)' , '{ "allowed_values": [{ "value": "maja", "display": "MAJA" }, { "value": "sen2cor", "display": "Sen2Cor" }] }');
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
INSERT INTO config_metadata VALUES ('processor.l2s1.enabled', 'S1 pre-processing enabled', 'bool', false, 23, FALSE, 'S1 pre-processing enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.parallelism', 'Number of jobs to run in parallel', 'int', false, 23, FALSE, 'Number of jobs to run in parallel', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.path', 'Final S1 L2 products path', 'string', false, 23, FALSE, 'Final S1 L2 products path', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.offset', 'Coherence interval', 'int', false, 23, FALSE, 'Coherence interval', NULL) ;
INSERT INTO config_metadata VALUES ('processor.l2s1.work.dir', 'Temporary S1 L2 files path', 'string', false, 23, FALSE, 'Temporary S1 L2 files path', NULL);

INSERT INTO config_metadata VALUES ('processor.l2s1.compute.amplitude', 'Compute amplitude', 'bool', false, 23, FALSE, 'Compute amplitude', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.compute.coherence', 'Compute coherence', 'bool', false, 23, FALSE, 'Compute coherence', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.parallelism', 'GPT parallelism', 'int', false, 23, FALSE, 'GPT parallelism', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.gpt.tile.cache.size', 'GPT tile cache size', 'int', false, 23, FALSE, 'GPT tile cache size', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.join.amplitude.steps', 'Join amplitude steps', 'bool', false, 23, FALSE, 'Join amplitude steps', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.join.coherence.steps', 'Join coherence steps', 'bool', false, 23, FALSE, 'Join coherence steps', NULL);

INSERT INTO config_metadata VALUES ('dem.name', 'DEM to use', 'string', false, 23, FALSE, 'DEM to use', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.acquisition.delay', 'Acquisition delay', 'int', false, 23, FALSE, 'Acquisition delay', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.copy.locally', 'Copy input products locally', 'bool', false, 23, FALSE, 'Copy input products locally', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.crop.nodata', 'Crop NODATA', 'bool', false, 23, FALSE, 'Crop NODATA', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.crop.output', 'Crop output', 'bool', false, 23, FALSE, 'Crop output', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.extract.histogram', 'Extract histogram', 'bool', false, 23, FALSE, 'Extract histogram', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.interval', 'Preprocessing job interval', 'int', false, 23, FALSE, 'Preprocessing job interval', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.keep.intermediate', 'Keep intermediate files', 'bool', false, 23, FALSE, 'Keep intermediate files', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.master', 'Primary acquisition for colocation', 'string', false, 23, FALSE, 'Primary acquisition for colocation', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.min.intersection', 'Minimum % of SLC overlaps for coherence', 'float', false, 23, FALSE, 'Minimum % of SLC overlaps for coherence', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.output.extension', 'Output extension', 'string', false, 23, FALSE, 'Output extension', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.output.format', 'Output format', 'string', false, 23, FALSE, 'Output format', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.overwrite.existing', 'Overwrite existing products', 'bool', false, 23, FALSE, 'Overwrite existing products', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.parallel.steps.enabled', 'Run steps in parallel', 'bool', false, 23, FALSE, 'Run steps in parallel', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.pixel.spacing', 'Output spatial resolution', 'float', false, 23, FALSE, 'Output spatial resolution', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.polarisations', 'Polarisations', 'string', false, 23, FALSE, 'Polarisations', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.process.newest', 'Process newest scenes first', 'bool', false, 23, FALSE, 'Process newest scenes first', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.projection', 'Output projection', 'string', false, 23, FALSE, 'Output projection', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.resolve.links', 'Resolve links', 'bool', false, 23, FALSE, 'Resolve links', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.step.timeout', 'Step timeout', 'int', false, 23, FALSE, 'Step timeout', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.temporal.filter.interval', 'Temporal filter window', 'int', false, 23, FALSE, 'Temporal filter window', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.version', 'Processor version', 'string', false, 23, FALSE, 'Processor version', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.min.memory', 'Minimum free memory for a step', 'string', false, 23, FALSE, 'Minimum free memory for a step', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.min.disk', 'Minimum disk storage for a step', 'string', false, 23, FALSE, 'Minimum disk storage for a step', NULL);

INSERT INTO config_metadata VALUES ('processor.l2s1.use.other.site.products', 'Reuse S1 L2 products created for other sites', 'string', false, 23, FALSE, 'Reuse S1 L2 products created for other sites', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.min.s2.intersection', 'Minimum % of intersection for S2 tile clipping', 'string', false, 23, FALSE, 'Minimum % of intersection for S2 tile clipping', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.ignore.previous.orbit.failure', 'Continue processing in case of a failure from the same orbit', 'string', false, 23, FALSE, 'Continue processing in case of a failure from the same orbit', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.convert.int', 'Make S1 L2 product pixel type UInt16', 'string', false, 23, FALSE, 'Make S1 L2 product pixel type UInt16', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.otb.min.memory', 'Memory to allocate to OTB steps', 'string', false, 23, FALSE, 'Memory to allocate to OTB steps', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.otb.enable.compression', 'Compress OTB steps output', 'string', false, 23, FALSE, 'Compress OTB steps output', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.terrain.flattening.autodetect', 'Decide usage of gamma naught correction based on detected elevation', 'string', false, 23, FALSE, 'Decide usage of gamma naught correction based on detected elevation', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.terrain.flattening.enabled', 'Enable usage of gamma naught correction', 'string', false, 23, FALSE, 'Enable usage of gamma naught correction', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.subset.before.tc.enabled', 'Subset SLC scene before terrain correction', 'string', false, 23, FALSE, 'Subset SLC scene before terrain correction', NULL);

INSERT INTO config_metadata VALUES ('processor.l2s1.bck.scale', 'Backscatter scaling factor', 'int', false, 23, FALSE, 'Backscatter scaling factor', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.cohe.scale', 'Coherence scaling factor', 'int', false, 23, FALSE, 'Coherence scaling factor', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.compress.enabled', 'Compress outputs (V1)', 'string', false, 23, FALSE, 'Compress outputs (V1)', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.crop.enabled', 'Enable cropping by S2 (V1)', 'string', false, 23, FALSE, 'Enable cropping by S2 (V1)', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.tiled.tiff', 'Create tiled output tiff', 'string', false, 23, FALSE, 'Create tiled output tiff', NULL);
INSERT INTO config_metadata VALUES ('processor.l2s1.zarr.conversion', 'Convert products to zarr', 'string', false, 23, FALSE, 'Convert products to zarr', NULL);

INSERT INTO config_metadata VALUES ('object.storage.container', 'Object storage container', 'string', false, 23, FALSE, 'Object storage container', NULL);
INSERT INTO config_metadata VALUES ('object.storage.domain', 'Object storage domain', 'string', false, 23, FALSE, 'Object storage domain', NULL);
INSERT INTO config_metadata VALUES ('object.storage.password', 'Object storage password', 'string', false, 23, FALSE, 'Object storage password', NULL);
INSERT INTO config_metadata VALUES ('object.storage.projectId', 'Object storage project ID', 'string', false, 23, FALSE, 'Object storage project ID', NULL);
INSERT INTO config_metadata VALUES ('object.storage.url', 'Object storage URL', 'string', false, 23, FALSE, 'Object storage URL', NULL);
INSERT INTO config_metadata VALUES ('object.storage.user', 'Object storage user', 'string', false, 23, FALSE, 'Object storage user', NULL);
INSERT INTO config_metadata VALUES ('primary.sensor', 'Primary sensor', 'string', false, 23, FALSE, 'Primary sensor', NULL);

-- -----------------------------------------------------------
-- LPIS configuration Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.lpis.path', 'The path to the pre-processed LPIS products', 'string', false, 21, FALSE, 'The path to the pre-processed LPIS products', NULL);
INSERT INTO config_metadata VALUES ('processor.lpis.lut_upload_path', 'Site LUT upload path', 'string', false, 21, FALSE, 'Site LUT upload path', NULL);
INSERT INTO config_metadata VALUES ('processor.lpis.upload_path', 'Site LPIS upload path', 'string', false, 21, FALSE, 'Site LPIS upload path', NULL);

-- -----------------------------------------------------------
-- L3A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l3a', 'L3A Product Max Age (days)', 'int', false, 7, FALSE, 'L3A Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3a.keep_job_folders', 'Keep L3A temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep L3A temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3a.slurm_qos', 'Slurm QOS for composite processor', 'string', true, 8, FALSE, 'Slurm QOS for composite processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3a', 'Path for L3A temporary files', 'string', false, 1, FALSE, 'Path for L3A temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.bandsmapping', 'Bands mapping file for S2', 'file', false, 3, FALSE, 'Bands mapping file for S2', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.cloud_optimized_geotiff_output', 'Generate L3A Cloud Optimized Geotiff outputs', 'bool', TRUE, 3, TRUE, 'Generate L3A Cloud Optimized Geotiff outputs', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.generate_20m_s2_resolution', 'Specifies if composite for S2 20M resolution should be generated', 'bool', false, 3, true, 'Specifies if composite for S2 20M resolution should be generated', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3a.half_synthesis', 'Half synthesis interval in days', 'int', false, 3, true, 'Half synthesis', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3a.synthesis_date', 'Synthesis date [YYYYMMDD]', 'string', false, 3, true, 'Synthesis date', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3a.lut_path', 'L3A LUT file path', 'file', false, 3, FALSE, 'L3A LUT file path', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.preproc.scatcoeffs_10m', 'Scattering coefficients file for S2 10 m', 'file', false, 3, FALSE, 'Scattering coefficients file for S2 10 m', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.preproc.scatcoeffs_20m', 'Scattering coefficients file for S2 20 m', 'file', false, 3, FALSE, 'Scattering coefficients file for S2 20 m', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.sched_wait_proc_inputs', 'L3A Composite scheduled jobs wait for products to become available', 'int', false, 3, FALSE, 'L3A Composite scheduled jobs wait for products to become available', NULL);
INSERT INTO config_metadata VALUES ('processor.l3a.synth_date_sched_offset', 'Difference in days between the scheduled and the synthesis date', 'int', false, 3, FALSE, 'Difference in days between the scheduled and the synthesis date', NULL);
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
INSERT INTO config_metadata VALUES ('executor.processor.l3b.slurm_qos', 'Slurm QOS for L3B processor', 'string', true, 8, FALSE, 'Slurm QOS for LAI processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3b', 'Path for L3B temporary files', 'string', false, 1, FALSE, 'Path for L3B temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.cloud_optimized_geotiff_output', 'Generate L3B Cloud Optimized Geotiff outputs', 'bool', TRUE, 4, TRUE, 'Generate L3B Cloud Optimized Geotiff outputs', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fapar', 'L3B processor will produce FAPAR', 'bool', TRUE, 4, TRUE, 'Produce FAPAR', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_fcover', 'L3B processor will produce FCOVER', 'bool', TRUE, 4, TRUE, 'Produce FCOVER', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_in_domain_flags', 'L3B processor will produce input domain flags', 'bool', TRUE, 4, TRUE, 'Produce input domain flags', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_lai', 'L3B processor will produce LAI', 'bool', TRUE, 4, TRUE, 'Produce LAI', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndvi', 'L3B processor will produce NDVI', 'bool', TRUE, 4, TRUE, 'Produce NDVI', NULL, true);

INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_ndwi', 'L3B processor will produce NDWI', 'bool', TRUE, 4, TRUE, 'Produce NDWI', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.produce_brightness', 'L3B processor will produce brightness', 'bool', TRUE, 4, FALSE, 'Produce brightness', NULL, FALSE);

INSERT INTO config_metadata VALUES ('processor.l3b.produce_mosaic', 'L3B processor will produce mosaic and product preview', 'bool', TRUE, 4, TRUE, 'Produce mosaic', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.l3b.filter.chain_inputs_steps', 'Chain L3B products', 'bool', TRUE, 4, TRUE, 'Chain L3B products', NULL, FALSE);

INSERT INTO config_metadata VALUES ('processor.l3b.generate_models', 'Specifies if models should be generated or not for LAI', 'int', false, 4, FALSE, 'Specifies if models should be generated or not for LAI', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.l1c_availability_days', 'Number of days before current scheduled date within we must have L1C processed (default 20)', 'int', FALSE, 4, FALSE, 'Number of days before current scheduled date within we must have L1C processed (default 20)', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.global_bv_samples_file', 'Common LAI BV sample distribution file', 'file', FALSE, 4, FALSE, 'Common LAI BV sample distribution file', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.laibandscfgfile', 'Configuration of the bands to be used for LAI', 'file', FALSE, 4, FALSE, 'Configuration of the bands to be used for LAI', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.lut_path', 'L3B LUT file path', 'file', FALSE, 4, FALSE, 'L3B LUT file path', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.modelsfolder', 'Folder where the models are located', 'directory', FALSE, 4, FALSE, 'Folder where the models are located', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.rsrcfgfile', 'L3B RSR file configuration for ProsailSimulator', 'file', FALSE, 4, FALSE, 'L3B RSR file configuration for ProsailSimulator', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.tiles_filter', 'L3B tiles filter', 'string', FALSE, 4, FALSE, 'L3B tiles filter', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.use_inra_version', 'L3B LAI processor will use INRA algorithm implementation', 'int', FALSE, 4, FALSE, 'L3B LAI processor will use INRA algorithm implementation', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.lai.use_lai_bands_cfg', 'Use LAI bands configuration file', 'int', FALSE, 4, FALSE, 'Use LAI bands configuration file', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.production_interval', 'The backward processing interval from the scheduled date for L3B products', 'int', FALSE, 4, FALSE, 'The backward processing interval from the scheduled date for L3B products', NULL);
-- TODO: This should be removed or moved to S2A_L3C ?
INSERT INTO config_metadata VALUES ('processor.l3b.reproc_production_interval', 'The backward processing interval from the scheduled date for L3C products', 'int', FALSE, 4, FALSE, 'The backward processing interval from the scheduled date for L3C products', NULL);
INSERT INTO config_metadata VALUES ('processor.l3b.sched_wait_proc_inputs', 'Scheduled jobs wait for products to become available', 'int', FALSE, 4, FALSE, 'Scheduled jobs wait for products to become available', NULL);

-- -----------------------------------------------------------
-- L2A Masked Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('general.scratch-path.l2a_msk', 'Path for Masked L2A temporary files', 'string', false, 27, FALSE, 'Path for Masked L2A temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a_msk.enabled', 'Enable or disable the validity flags', 'bool', false, 27, FALSE, 'Enable or disable the validity flags', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a_msk.compress', 'Compress output flags', 'bool', false, 27, FALSE, 'Compress output flags', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a_msk.tiled', 'Produce output flags as Tiled', 'bool', false, 27, FALSE, 'Produce output flags as Tiled', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a_msk.water_is_valid', 'Consider water pixels as valid', 'bool', false, 27, FALSE, 'Consider water pixels as valid', NULL);
INSERT INTO config_metadata VALUES ('processor.l2a_msk.snow_is_valid', 'Consider snow pixels as valid', 'bool', false, 27, FALSE, 'Consider snow pixels as valid', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l2a_msk.slurm_qos', 'Slurm QOS for validity masks', 'string', true, 8);

-- -----------------------------------------------------------
-- S2A_L3C Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3c.keep_job_folders', 'Keep L3C temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep L3C temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3c.slurm_qos', 'Slurm QOS for L3C processor', 'string', true, 8, FALSE, 'Slurm QOS for L3C processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s2a_l3c', 'Path for L3C temporary files', 'string', false, 1, FALSE, 'Path for Masked L3C temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s2a_l3c.localwnd.bwr', 'Backward radius of the window for N-day reprocessing', 'int', false, 24, true, 'Backward window');
INSERT INTO config_metadata VALUES ('processor.s2a_l3c.localwnd.fwr', 'Forward radius of the window for N-day reprocessing', 'int', true, 24, true, 'Forward window');
INSERT INTO config_metadata VALUES ('processor.s2a_l3c.lut_path', 'L3C LUT file path', 'file', false, 24, FALSE, 'L3C LUT file path', NULL);

-- -----------------------------------------------------------
-- S2A_L3D Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3d.keep_job_folders', 'Keep L3D temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep L3D temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s2a_l3d.slurm_qos', 'Slurm QOS for LAI processor', 'string', true, 8, FALSE, 'Slurm QOS for L3D processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s2a_l3d', 'Path for L3D temporary files', 'string', false, 1, FALSE, 'Path for Masked L3D temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s2a_l3d.lut_path', 'L3D LUT file path', 'file', false, 25, FALSE, 'L3D LUT file path', NULL);

-- -----------------------------------------------------------
-- L3E Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.l3e.keep_job_folders', 'Keep L3E temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep L3E temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3e.slurm_qos', 'Slurm QOS for Pheno NDVI processor', 'string', true, 8, FALSE, 'Slurm QOS for Pheno NDVI processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3e', 'Path for L3E temporary files', 'string', false, 1, FALSE, 'Path for Masked L3E temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.l3e.cloud_optimized_geotiff_output', 'Generate L3E COG outputs', 'bool', false, 18, FALSE, 'Generate L3E COG outputs', NULL);
INSERT INTO config_metadata VALUES ('processor.l3e.sched_wait_proc_inputs', 'L3E PhenoNDVI scheduled jobs wait for products to become available', 'int', false, 18, FALSE, 'L3E PhenoNDVI scheduled jobs wait for products to become available', NULL);

-- -----------------------------------------------------------
-- L4A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l4a', 'L4A Product Max Age (days)', 'int', false, 7);
INSERT INTO config_metadata VALUES ('executor.processor.l4a.keep_job_folders', 'Keep L4A temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l4a.slurm_qos', 'Slurm QOS for CropMask processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.l4a', 'Path for L4A temporary files', 'string', false, 1);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier', 'Random forest clasifier / SVM classifier choices=[rf, svm]', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.field', 'Classifier field', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.rf.max', 'Maximum depth of the trees used for Random Forest classifier', 'int', true, 5, true, 'Max depth', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.rf.min', 'Minimum number of samples in each node used by the classifier', 'int', true, 5, true, 'Minimum number of samples', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.rf.nbtrees', 'The number of trees used for training', 'int', true, 5, true, 'Training trees', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.svm.k', 'Classifier SVM K', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.classifier.svm.opt', 'Classifier SVM Opt', 'string', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.cloud_optimized_geotiff_output', 'Generate L4A Cloud Optimized Geotiff outputs', 'bool', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.erode-radius', 'The radius used for erosion', 'int', true, 5, true, 'Erosion radius');
INSERT INTO config_metadata VALUES ('processor.l4a.lut_path', 'L4A LUT file path', 'file', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.mahalanobis-alpha', 'The parameter alpha used by the mahalanobis function', 'float', true, 5, true, 'Mahalanobis alpha');
INSERT INTO config_metadata VALUES ('processor.l4a.max-parallelism', 'Tiles to classify in parallel', 'int', false, 5);
INSERT INTO config_metadata VALUES ('processor.l4a.min-area', 'The min nb of pixel used in crop/nocrop decision when equal samples', 'int', true, 5, true, 'The minium number of pixels', '{ "bounds": { "min": 0, "max": 5000 } }');
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

INSERT INTO config_metadata VALUES ('processor.l4a.reference_data_source', 'Reference data source', 'string', true, 5, true, 'Reference data source', '{ "allowed_values": [{ "value": "insitu", "display": "Insitu data" }, { "value": "reference_map", "display": "Reference Map (non supervised)" }] }', true);
INSERT INTO config_metadata VALUES ('processor.l4a.reference_polygons', 'Insitu reference polygons location (if Insity data was selected). The shapefile should be copied first in this location on server', 'string', true, 5, true, 'Insitu reference polygons location', null, false);

-- INSERT INTO config_metadata VALUES ('processor.l4a.reference_data_source.insitu', 'Insitu data', 'file', true, 5, true, 'Insitu file', null, true);
-- INSERT INTO config_metadata VALUES ('processor.l4a.earth_signature_insitu', 'EarthSignature insitu data location', 'file', true, 5, true, 'EarthSignature insitu data location', null, false);


-- -----------------------------------------------------------
-- L4B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.l4b', 'L4B Product Max Age (days)', 'int', false, 7, FALSE, 'L4B Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l4b.keep_job_folders', 'Keep L4B temporary product files for the orchestrator jobs', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.l4b.slurm_qos', 'Slurm QOS for CropType processor', 'string', true, 8);
INSERT INTO config_metadata VALUES ('general.scratch-path.l4b', 'Path for L4B temporary files', 'string', false, 1, FALSE, 'Path for L4B temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier', 'Random forest clasifier / SVM classifier choices=[rf, svm]', 'string', false, 6, FALSE, 'Random forest clasifier / SVM classifier choices=[rf, svm]', NULL);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.field', 'Training samples feature name', 'string', false, 6, FALSE, 'Training samples feature name', NULL);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.rf.max', 'maximum depth of the trees used for Random Forest classifier', 'int', true, 6, true, 'Random Forest classifier max depth', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.rf.min', 'minimum number of samples in each node used by the classifier', 'int', true, 6, true, 'Minimum number of samples', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.rf.nbtrees', 'The number of trees used for training', 'int', true, 6, true, 'Training trees', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.svm.k', 'Type of kernel', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.classifier.svm.opt', 'Automatic optimisation of the parameters', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.cloud_optimized_geotiff_output', 'Generate L4B Cloud Optimized Geotiff outputs', 'bool', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.lut_path', 'L4B LUT file path', 'file', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.max-parallelism', 'Tiles to classify in parallel', 'int', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.mission', 'The main mission for the time series', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.random_seed', 'The random seed used for training', 'float', true, 6, true, 'Random seed');
INSERT INTO config_metadata VALUES ('processor.l4b.sample-ratio', 'The ratio between the validation and training polygons', 'float', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.sched_wait_proc_inputs', 'L4B Crop Type scheduled jobs wait for products to become available', 'int', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.temporal_resampling_mode', 'The temporal resampling mode choices=[resample, gapfill]', 'string', false, 6);
INSERT INTO config_metadata VALUES ('processor.l4b.tile-threads-hint', 'Threads to use for classification of a tile', 'int', false, 6);


INSERT INTO config_metadata VALUES ('processor.l4b.crop_mask', 'Crop Mask Product name or full product path', 'string', true, 6, true, 'Crop Mask Product');
INSERT INTO config_metadata VALUES ('processor.l4b.reference_data_source', 'Reference data source', 'string', true, 6, true, 'Reference data source', '{ "allowed_values": [{ "value": "insitu", "display": "Insitu data" }] }', true);
INSERT INTO config_metadata VALUES ('processor.l4b.reference_polygons', 'Insitu reference polygons location (if Insity data was selected). The shapefile should be copied first in this location on server', 'string', true, 6, false, 'Insitu reference polygons location', null, false);

-- INSERT INTO config_metadata VALUES ('processor.l4b.earth_signature_insitu', 'EarthSignature insitu data location', 'file', true, 6, false, 'EarthSignature insitu data location', null, false);

-- -----------------------------------------------------------
-- S4C_L4A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4a', 'S4C L4A Product Max Age (days)', 'int', false, 7, FALSE, 'S4C L4A Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4a.keep_job_folders', 'Keep S4C L4A temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4A temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4a.slurm_qos', 'Slurm QOS for S4C L4A processor', 'string', true, 8, FALSE, 'Slurm QOS for S4C L4A processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4a', 'Path for  S4C L4A temporary files', 'string', false, 1, FALSE, 'Path for  S4C L4A temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.input_l2a', 'The list of L2A products', 'select', FALSE, 22, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 22, TRUE, 'Start date', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 22, TRUE, 'End date', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.best-s2-pix', 'Minimum number of S2 pixels for parcels to use in training', 'int', TRUE, 22, TRUE, 'Minimum number of S2 pixels for parcels to use in training', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.lc', 'LC classes to assess', 'string', TRUE, 22, TRUE, 'LC classes to assess', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-node-size', 'Minimum node size', 'int', TRUE, 22, TRUE, 'Minimum node size', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s1-pix', 'Minimum number of S1 pixels', 'int', TRUE, 22, TRUE, 'Minimum number of S1 pixels', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.min-s2-pix', 'Minimum number of S2 pixels', 'int', TRUE, 22, TRUE, 'Minimum number of S2 pixels', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.mode', 'Mode', 'string', FALSE, 22, TRUE, 'Mode (both, s1-only, s2-only)', '{ "allowed_values": [{ "value": "s1-only", "display": "S1 Only" }, { "value": "s2-only", "display": "S2 only" }, { "value": "both", "display": "Both" }] }', true);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.num-trees', 'Number of RF trees', 'int', TRUE, 22, TRUE, 'Number of RF trees', '{ "bounds": { "min": 0, "max": 1000 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-min', 'Minimum parcels to assess a crop type', 'int', TRUE, 22, TRUE, 'Minimum parcels to assess a crop type', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-h', 'Upper threshold for parcel counts by crop type', 'int', TRUE, 22, TRUE, 'Upper threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.pa-train-l', 'Lower threshold for parcel counts by crop type', 'int', TRUE, 22, TRUE, 'Lower threshold for parcel counts by crop type', '{ "bounds": { "min": 0, "max": 5000 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-h', 'Training ratio for common crop types', 'float', TRUE, 22, TRUE, 'Training ratio for common crop types', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.sample-ratio-l', 'Training ratio for uncommon crop types', 'float', TRUE, 22, TRUE, 'Training ratio for uncommon crop types', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-k', 'Number of SMOTE neighbours', 'int', TRUE, 22, TRUE, 'Number of SMOTE neighbours', '{ "bounds": { "min": 0, "max": 100 } }');
INSERT INTO config_metadata VALUES ('processor.s4c_l4a.smote-target', 'Target sample count for SMOTE', 'int', TRUE, 22, TRUE, 'Target sample count for SMOTE', '{ "bounds": { "min": 0, "max": 5000 } }');

-- -----------------------------------------------------------
-- S4C_L4B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4b', 'S4C L4B Product Max Age (days)', 'int', false, 7, FALSE, 'S4C L4B Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4b.keep_job_folders', 'Keep S4C L4B temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4B temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4b.slurm_qos', 'Slurm QOS for S4C L4B processor', 'string', true, 8, FALSE, 'Slurm QOS for S4C L4B processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4b', 'Path for S4C L4B temporary files', 'string', false, 1, FALSE, 'Path for S4C L4B temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.default_config_path', 'The default configuration files for all L4B processors', 'file', FALSE, 19, FALSE, 'The default configuration files for all L4B processors', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.end_date', 'End date for the mowing detection', 'string', FALSE, 19, TRUE, 'End date for the mowing detection', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_amp', 'The list of AMP products', 'select', FALSE, 19, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_cohe', 'The list of COHE products', 'select', FALSE, 19, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_l3b', 'The list of L3B products', 'select', FALSE, 19, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s1_s2_startdate_diff', 'Offset in days between S1 and S2 start dates', 'string', TRUE, 19, TRUE, 'Offset in days between S1 and S2 start dates', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.start_date', 'Start date for the mowing detection', 'string', FALSE, 19, TRUE, 'Start date for the mowing detection', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_l4b.cfg_dir', 'Config files directory', 'string', FALSE, 19, FALSE, 'Config files directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.cfg_upload_dir', 'Site upload files directory', 'string', FALSE, 19, FALSE, 'Site upload files directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.input_product_types', 'Input product types', 'string', FALSE, 19, FALSE, 'Input product types', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s1_py_script', 'Script for S1 detection', 'string', FALSE, 19, FALSE, 'Script for S1 detection', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.s2_py_script', 'Script for S2 detection', 'string', FALSE, 19, FALSE, 'Script for S1 detection', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.year', 'Current L4B processing year for site', 'int', FALSE, 19, FALSE, 'Current L4B processing year for site', NULL);

-- TODO: See if these 2 are used, if not, they should be removed
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.sub_steps', 'Substeps', 'string', FALSE, 19, FALSE, 'Substeps', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4b.working_dir', 'Working directory', 'string', FALSE, 19, FALSE, 'Working directory', NULL);

-- -----------------------------------------------------------
-- S4C_L4C Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('archiver.max_age.s4c_l4c', 'S4C L4C Product Max Age (days)', 'int', false, 7, FALSE, 'S4C L4C Product Max Age (days)', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4c.keep_job_folders', 'Keep S4C L4C temporary product files for the orchestrator jobs', 'int', false, 8, FALSE, 'Keep S4C L4C temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_l4c.slurm_qos', 'Slurm QOS for S4C L4C processor', 'string', true, 8, FALSE, 'Slurm QOS for S4C L4C processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_l4c', 'Path for S4C L4C temporary files', 'string', false, 1, FALSE, 'Path for S4C L4C temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.default_config_path', 'The default configuration files for all L4C processors', 'file', FALSE, 20, FALSE, 'The default configuration files for all L4C processors', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_amp', 'The list of AMP products', 'select', FALSE, 20, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_cohe', 'The list of COHE products', 'select', FALSE, 20, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.input_l3b', 'The list of L3B products', 'select', FALSE, 20, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}');
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.markers_add_no_data_rows', 'Add in markers parcel rows containg only NA/NA1/NR', 'bool', true, 20, true, 'Add in markers parcel rows containg only NA/NA1/NR', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.prds_per_group', 'Data extraction number of products per group', 'int', FALSE, 20, FALSE, 'Data extraction number of products per group', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.sched_prds_hist_file', 'File where the list of the scheduled L4Cs is kept', 'string', true, 20, FALSE, 'File where the list of the scheduled L4Cs is kept', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tillage_monitoring', 'Enable tillage monitoring', 'bool', false, 20, true, 'Enable tillage monitoring', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.cfg_dir', 'Config files directory', 'string', FALSE, 20, FALSE, 'Config files directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.cfg_upload_dir', 'Site upload files directory', 'string', FALSE, 20, FALSE, 'Site upload files directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.country', 'Site country', 'string', FALSE, 20, FALSE, 'Site country', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.data_extr_dir', 'Data extraction directory', 'string', FALSE, 20, FALSE, 'Data extraction directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.execution_operation', 'Execution operation', 'string', FALSE, 20, FALSE, 'Execution operation', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.filter_ids_path', 'Filtering parcels ids file', 'string', FALSE, 20, FALSE, 'Filtering parcels ids file', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.nrt_data_extr_enabled', 'NRT data extration enabled', 'int', FALSE, 20, FALSE, 'NRT data extration enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.practices', 'Configured practices list', 'string', FALSE, 20, FALSE, 'Configured practices list', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.sub_steps', 'Substeps', 'string', FALSE, 20, FALSE, 'Substeps', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.tsa_min_acqs_no', 'TSA min number of acquisitions', 'int', FALSE, 20, FALSE, 'TSA min number of acquisitions', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.ts_input_tables_dir', 'TSA input tables directory', 'string', FALSE, 20, FALSE, 'TSA input tables directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.ts_input_tables_upload_root_dir', 'Input tables upload root directory', 'string', FALSE, 20, FALSE, 'Input tables upload root directory', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.use_prev_prd', 'Use previous TSA', 'int', FALSE, 20, FALSE, 'Use previous TSA', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_l4c.year', 'Current L4C processing year for site', 'int', FALSE, 20, FALSE, 'Current L4C processing year for site', NULL);

-- -----------------------------------------------------------
-- S4C_MDB1 Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s4c_mdb1.slurm_qos', 'Slurm QOS for MDB1 processor', 'string', true, 8, FALSE, 'Slurm QOS for MDB1 processor', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_mdb1.keep_job_folders', 'Keep MDB1 temporary product files for the orchestrator jobs', 'string', true, 8, FALSE, 'Keep MDB1 temporary product files for the orchestrator jobs', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_mdb1', 'Path for S4C MDB1 temporary files', 'string', false, 1, FALSE, 'Path for S4C MDB1 temporary files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_enabled', 'AMP markers extraction enabled', 'bool', true, 26, true, 'Extract Amplitude markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.cohe_enabled', 'COHE markers extraction enabled', 'bool', true, 26, true, 'Extract Coherence markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.data_extr_dir', 'Location for the MDB1 data extration files', 'string', true, 26, FALSE, 'Location for the MDB1 data extration files', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fapar_enabled', 'FAPAR markers extraction enabled', 'bool', true, 26, true, 'Extract FAPAR markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.fcover_enabled', 'FCOVER markers extraction enabled', 'bool', true, 26, true, 'Extract FCOVER markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndwi_enabled', 'NDWI markers extraction enabled', 'bool', true, 26, true, 'Extract NDWI markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.brightness_enabled', 'Brightness markers extraction enabled', 'bool', true, 26, true, 'Extract Brightness markers', NULL, true);

INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_l2a', 'The list of L2A products', 'select', FALSE, 26, TRUE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_amp', 'The list of AMP products', 'select', FALSE, 26, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_cohe', 'The list of COHE products', 'select', FALSE, 26, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.input_l3b', 'The list of L3B products', 'select', FALSE, 26, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}');
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.lai_enabled', 'LAI markers extraction enabled', 'bool', true, 26, true, 'Extract LAI markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndvi_enabled', 'NDVI markers extraction enabled', 'bool', true, 26, true, 'Extract NDVI markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', 'AMP VV/VH markers extraction enabled', 'bool', true, 26, true, 'Extract Amplitude VV/VH markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.valid_pixels_enabled', 'Number of valid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Extract number of valid pixels per parcel', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.invalid_pixels_enabled', 'Number of invalid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Extract number of invalid pixels per parcel', NULL, true);
-- INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.stdev_enabled', 'Stdev per parcel extraction enabled', 'bool', true, 26, FALSE, 'Stdev per parcel extraction enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.minmax_enabled', 'Min/Max per parcel extraction enabled', 'bool', true, 26, true, 'Min/Max per parcel extraction enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.median_enabled', 'Median per parcels extraction enabled', 'bool', true, 26, true, 'Median per parcels extraction enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.p25_enabled', 'P25 per parcels extraction enabled', 'bool', true, 26, true, 'P25 per parcels extraction enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.p75_enabled', 'P75 per parcels extraction enabled', 'bool', true, 26, true, 'P75 per parcels extraction enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab02_enabled', 'Reflectance band B02 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B02 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab03_enabled', 'Reflectance band B03 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B03 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab04_enabled', 'Reflectance band B04 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B04 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab05_enabled', 'Reflectance band B05 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B05 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab06_enabled', 'Reflectance band B06 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B06 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab07_enabled', 'Reflectance band B07 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B07 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab08_enabled', 'Reflectance band B08 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B08 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab8a_enabled', 'Reflectance band B8A markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B8A markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab11_enabled', 'Reflectance band B11 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B11 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.l2ab12_enabled', 'Reflectance band B12 markers extraction enabled', 'bool', true, 26, true, 'Extract reflectance band B12 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_enabled', 'MDB3 markers extraction enabled', 'bool', true, 26, true, 'Extract MDB3 markers', NULL, true);
INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_input_tables', 'MDB3 input tables location', 'string', true, 26, false, 'MDB3 input tables location', NULL);

-- -----------------------------------------------------------
-- Fmask Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.fmask.enabled', 'Controls whether to run Fmask on optical products', 'bool', false, 31, FALSE, 'Enable FMask', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.extractor_image', 'FMask extractor docker image name', 'string', false, 31, FALSE, 'FMask extractor docker image name', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.gdal_image', 'gdal docker image for FMask', 'string', false, 31, FALSE, 'gdal docker image for FMask', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.image', 'FMask docker image', 'string', false, 31, FALSE, 'FMask docker image', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.cog-tiffs', 'Output rasters as COG', 'bool', false, 31, FALSE, 'Output rasters as COG', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.compress-tiffs', 'Compress output rasters', 'bool', false, 31, FALSE, 'Compress output rasters', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.cloud-shadow', 'Cloud shaddow dilation percent', 'int', false, 31, FALSE, 'Cloud shaddow dilation percent', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.cloud', 'Cloud dilation percent', 'int', false, 31, FALSE, 'Cloud dilation percent', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.dilation.snow', 'Snow dilation percent', 'int', false, 31, FALSE, 'Snow dilation percent', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.max-retries', 'Maximum number of retries for a product', 'int', false, 31, FALSE, 'Maximum number of retries for a product', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.num-workers', 'Number of workers', 'int', false, 31, FALSE, 'Number of workers', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.output-path', 'Output path', 'string', false, 31, FALSE, 'Output path', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.retry-interval', 'Retry interval', 'string', false, 31, FALSE, 'Retry interval', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold.l8', 'Threshold for L8', 'int', false, 31, FALSE, 'Threshold for L8', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold.s2', 'Threshold for S2', 'int', false, 31, FALSE, 'Threshold for S2', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.optical.threshold', 'Global threshold', 'int', false, 31, FALSE, 'Global threshold', NULL);
INSERT INTO config_metadata VALUES ('processor.fmask.working-dir', 'Working directory', 'string', false, 31, FALSE, 'Working directory', NULL);

-- -----------------------------------------------------------
-- T-Rex Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.module.path.trex-updater', 'T-Rex script', 'string', false, 32, FALSE, 'T-Rex script', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.trex.slurm_qos', 'Slurm QOS for TRex', 'string', true, 8, FALSE, 'Slurm QOS for TRex', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.trex-updater.use_docker', 'T-Rex use docker', 'int', false, 32, FALSE, 'T-Rex use docker', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.trex-updater.docker_image', 'T-Rex docker image', 'string', false, 32, FALSE, 'T-Rex docker image', NULL);
INSERT INTO config_metadata VALUES ('general.orchestrator.trex-updater.docker_add_mounts', 'T-Rex container additional mounts', 'string', false, 32, FALSE, 'T-Rex container additional mounts', NULL);
INSERT INTO config_metadata VALUES ('processor.trex.t-rex-container', 'T-Rex container name', 'string', false, 32, FALSE, 'T-Rex container name', NULL);
INSERT INTO config_metadata VALUES ('processor.trex.t-rex-output-file', 'T-Rex output file', 'string', false, 32, FALSE, 'T-Rex output file', NULL);

-- -----------------------------------------------------------
-- L3 S1 Composite Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.l3_s1_comp.slurm_qos', 'Slurm QOS for S1 Composite', 'string', true, 8, FALSE, 'Slurm QOS for S1 Composite', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3_s1_comp', 'Path for L3 S1 composite temporary files', 'string', false, 1, FALSE, 'Path for L3 S1 composites temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3_s1_comp.keep_job_folders', 'Keep L3 S1 Composites temporary files', 'int', false, 8);

INSERT INTO config_metadata VALUES ('processor.l3_s1_comp.polarisations', 'Polarisation(s)', 'string', false, 33, true, 'Polarisation(s)', '{ "allowed_values": [{ "value": "VV", "display": "VV" }, { "value": "VH", "display": "VH" }, { "value": "VVVH", "display": "VV+VH" }] }', true);
INSERT INTO config_metadata VALUES ('processor.l3_s1_comp.period', 'Composite period', 'int', false, 33, true, 'Composite period', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3_s1_comp.method', 'Compositing method', 'string', false, 33, true, 'Compositing method', '{ "allowed_values": [{ "value": "mean", "display": "Mean" }, { "value": "min", "display": "Minimum" }, { "value": "max", "display": "Maximum" }, { "value": "waverage", "display": "Weighted Average" }, { "value": "median", "display": "Median" }, { "value": "last", "display": "Last" }] }', true);
INSERT INTO config_metadata VALUES ('processor.l3_s1_comp.extract_valid_pixels_cnt', 'Extract validity flags', 'bool', false, 33, true, 'Extract validity flags', NULL, true);

INSERT INTO config_metadata VALUES ('processor.l3_s1_comp.input_amp', 'The list of AMP products', 'select', FALSE, 33, TRUE, 'Available AMP input files', '{"name":"inputFiles_AMP[]","product_type_id":10,"satellite_ids":[3]}');
-- INSERT INTO config_metadata VALUES ('processor.l3_s1_comp.input_cohe', 'The list of COHE products', 'select', FALSE, 33, TRUE, 'Available COHE input files', '{"name":"inputFiles_COHE[]","product_type_id":11,"satellite_ids":[3]}');

-- -----------------------------------------------------------
-- L3 Biophysical Indicators Composite Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.l3_ind_comp.slurm_qos', 'Slurm QOS for Indicators Composite', 'string', true, 8, FALSE, 'Slurm QOS for Indicators Composite', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.l3_ind_comp', 'Path for L3 Indicators composite temporary files', 'string', false, 1, FALSE, 'Path for L3 Optical composites temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.l3_ind_comp.keep_job_folders', 'Keep L3 Indicators Composites temporary files', 'int', false, 8);

INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.ndvi_enabled', 'NDVI Composite enabled', 'bool', false, 34, true, 'NDVI Composite enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.lai_enabled', 'LAI Composite enabled', 'bool', false, 34, true, 'LAI Composite enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.fapar_enabled', 'FAPAR Composite enabled', 'bool', false, 34, true, 'FAPAR Composite enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.fcover_enabled', 'FCOVER Composite enabled', 'bool', false, 34, true, 'FCOVER Composite enabled', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.period', 'Composite period', 'int', false, 34, true, 'Composite period', NULL, true);
INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.extract_valid_pixels_cnt', 'Extract validity flags', 'bool', false, 34, true, 'Extract validity flags', NULL, true);

INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.method', 'Compositing method', 'string', false, 34, true, 'Compositing method', '{ "allowed_values": [{ "value": "mean", "display": "Mean" }, { "value": "min", "display": "Minimum" }, { "value": "max", "display": "Maximum" }, { "value": "waverage", "display": "Weighted Average" }, { "value": "median", "display": "Median" }, { "value": "last", "display": "Last" }] }', true);

INSERT INTO config_metadata VALUES ('processor.l3_ind_comp.input_l3b', 'The list of L3B products', 'select', FALSE, 34, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}');

-- -----------------------------------------------------------
-- Sen4Stat CropMapping processor
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('general.scratch-path.s4s_crop_mapping', 'Path for Crop Type Mapping temporary files', 'string', false, 1, FALSE, 'Path for S4S Crop Mapping temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_crop_mapping.keep_job_folders', 'Keep S4S Crop Mapping temporary files', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_crop_mapping.slurm_qos', 'Slurm QOS for S4S Crop Mapping', 'string', true, 8, FALSE, 'Slurm QOS for S4S Crop Mapping', NULL);

insert into config_metadata values ('processor.insitu.path', 'The path to the pre-processed in-situ data products', 'string', false, 21, false, 'The path to the pre-processed in-situ data products', null);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s-crop-type-mapping', 'Script for Crop Type Mapping', 'string', true, 8, FALSE, 'Script for Crop Type Mapping', NULL);

INSERT INTO config_metadata VALUES ('processor.s4s_crop_mapping.input_l2a', 'The list of L2A products', 'select', FALSE, 30, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_crop_mapping.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 30, TRUE, 'Start date', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_crop_mapping.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 30, TRUE, 'End date', NULL, FALSE);

insert into config_metadata values ('processor.s4s_crop_mapping.pix-min', 'Minimum number of pixels of polygons', 'int', true, 30, true, 'Minimum number of pixels for polygons', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-best', 'Minimum number of pixels of polygons used for training', 'int', true, 30, true, 'Minimum number of pixels of polygons used for training', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-ratio-min', 'Minimum crop to total pixel ratio', 'float', true, 30, true, 'Minimum crop to total pixel ratio', null);
insert into config_metadata values ('processor.s4s_crop_mapping.poly-min', 'Minimum number of polygons for crops', 'int', true, 30, true, 'Minimum number of polygons for crops', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-ratio-hi', 'Minimum crop to total pixel ratio for strategy 1', 'float', true, 30, true, 'Minimum crop to total pixel ratio for strategy 1', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-ratio-lo', 'Minimum crop to total pixel ratio for strategy 2', 'float', true, 30, true, 'Minimum crop to total pixel ratio for strategy 2', null);
insert into config_metadata values ('processor.s4s_crop_mapping.monitored-land-covers', 'Land cover class filter', 'string', true, 30, true, 'Land cover class filter', null);
insert into config_metadata values ('processor.s4s_crop_mapping.monitored-crops', 'Crop class filter', 'string', true, 30, true, 'Crop class filter', null);
insert into config_metadata values ('processor.s4s_crop_mapping.monitored-crops-remapped-pre', 'Pre-remapped monitored crop class filter', 'string', true, 30, true, 'Pre-remapped monitored crop class filter', null);
insert into config_metadata values ('processor.s4s_crop_mapping.excluded-crops-remapped-pre', 'Pre-remapped excluded crop class filter', 'string', true, 30, true, 'Pre-remapped excluded crop class filter', null);
insert into config_metadata values ('processor.s4s_crop_mapping.smote-ratio', 'Synthetic sample ratio', 'float', true, 30, true, 'Synthetic sample ratio', null);
insert into config_metadata values ('processor.s4s_crop_mapping.sample-ratio-hi', 'Training pixel ratio for strategy 1', 'float', true, 30, true, 'Training pixel ratio for strategy 1', null);
insert into config_metadata values ('processor.s4s_crop_mapping.sample-ratio-lo', 'Training pixel ratio for strategies 2 and 3', 'float', true, 30, true, 'Training pixel ratio for strategies 2 and 3', null);
insert into config_metadata values ('processor.s4s_crop_mapping.rf.max-depth', 'Maximum depth of RF trees', 'int', true, 30, true, 'Maximum depth of RF trees', null);
insert into config_metadata values ('processor.s4s_crop_mapping.rf.min-samples', 'Minimum samples in RF tree nodes', 'int', true, 30, true, 'Minimum samples in RF tree nodes', null);
insert into config_metadata values ('processor.s4s_crop_mapping.rf.num-trees', 'Number of RF trees', 'int', true, 30, true, 'Number of RF trees', null);

insert into config_metadata values ('processor.s4s_crop_mapping.crop_remapping_set_id', 'Crop Remapping Set', 'string', true, 30, true, 'Crop Remapping Set', '{ "allowed_values_source": { "database_object": "crop_remapping_set", "value_column": "crop_remapping_set_id", "label_column": "name" } }', true);

insert into config_metadata values ('processor.s4s_crop_mapping.features-filter', 'Features filter. If provided, the features are given as comma separated values. Possible values are sr10 (S2 Reflectance 10m), sr20 (S2 reflectance 20m), vi (Vegetation indices), vis (Vegetation indices Statistics), sar (S1 features), re (Red edge features)', 'string', true, 30, true, 'Features filter', null);

-- -----------------------------------------------------------
-- ERA5
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.era5_weather.enabled', 'ERA5 processor enabled', 'bool', false, 39, FALSE, 'ERA5 processor enabled', NULL);


-- -----------------------------------------------------------
-- S4S Parcels Import Processor
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.municipalities_upload_dir', 'Municipalities upload dir', 'string', TRUE, 21, FALSE, 'Municipalities upload dir', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.parcel_stats_upload_dir', 'Parcels stats upload dir', 'string', TRUE, 21, FALSE, 'Parcels stats  upload dir', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.parcels_upload_dir', 'Parcels geometries upload dir', 'string', TRUE, 21, FALSE, 'Parcels geometries upload dir', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.provinces_upload_dir', 'Provinces upload dir', 'string', TRUE, 21, FALSE, 'Provinces upload dir', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.regions_upload_dir', 'Regions upload dir', 'string', TRUE, 21, FALSE, 'Regions upload dir', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.segments_upload_dir', 'Segments upload dir', 'string', TRUE, 21, FALSE, 'Segments upload dir', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_parcels.working_dir', 'Parcels import working dir', 'string', TRUE, 21, FALSE, 'Parcels import working dir', NULL, FALSE);


-- -----------------------------------------------------------
-- S4S Permanent Crops Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s4s_perm_crop.keep_job_folders', 'Keep S4S Permanent Crops temporary files', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_perm_crop.slurm_qos', 'Slurm QOS for Permanent Crops', 'string', true, 8, FALSE, 'Slurm QOS for Permanent Crops', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4s_perm_crop', 'Path for Permanent Crops temporary files', 'string', false, 1, FALSE, 'Path for Permanent Crops temporary files', NULL);

INSERT INTO config_metadata VALUES ('processor.s4s_perm_crop.broceliande-docker-image', 'Broceliande docker image', 'string', TRUE, 28, FALSE, 'Broceliande docker image', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_perm_crop.vec_field', 'Permanent crops field name', 'string', TRUE, 28, FALSE, 'Permanent crops field name', NULL, FALSE);

-- -----------------------------------------------------------
-- S4S Yield Features Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('general.scratch-path.s4s_yield_feat', 'Path for Yield temporary files', 'string', false, 1, FALSE, 'Path for Yield temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_feat.keep_job_folders', 'Keep yield temporary files', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_feat.slurm_qos', 'Slurm QOS for Yield', 'string', true, 8, FALSE, 'Slurm QOS for Yield', NULL);

INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.algorithm', 'Yield estimation algorithm', 'string', true, 29, true, 'Yield estimation algorithm', '{ "allowed_values": [{ "value": "rf", "display": "Random Forest" }, { "value": "lmr", "display": "Linear regression"}, { "value": "svm", "display": "Support Vector Machine"}] }', true);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.selection-type',  'Yield estimation selection type', 'string', true, 29, true, 'Yield estimation selection type', '{ "allowed_values": [{ "value": "automatic", "display": "Automatic" }, { "value": "manual", "display": "Manual"}, { "value": "none", "display": "None" }] }', true);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.max-automatic-features-no', 'Maximum number of automatic features', 'int', true, 29, true, 'Maximum number of automatic features', null);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.manual-selection-features', 'Manual selection features', 'string', true, 29, true, 'Manual selection features', null);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.input_l2a', 'The list of L2A products', 'select', FALSE, 29, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 29, TRUE, 'Start date', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 29, TRUE, 'End date', NULL, FALSE);

INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.parcel_id_col_name', 'Yield Features ID column name', 'string', TRUE, 29, FALSE, 'Yield Features ID column name', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.safy_params_path', 'SAFY Params path', 'string', TRUE, 29, FALSE, 'SAFY Params path', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.safy_params_upload_dir', 'SAFY params upload dir', 'string', TRUE, 29, FALSE, 'SAFY params upload dir', NULL, FALSE);

-- -----------------------------------------------------------
-- S4S Yield SU Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('general.scratch-path.s4s_yield_su', 'Path for Yield SU temporary files', 'string', false, 1, FALSE, 'Path for Yield SU temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_su.keep_job_folders', 'Keep yield temporary files', 'int', false, 8);
INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_su.slurm_qos', 'Slurm QOS for Yield SU', 'string', true, 8, FALSE, 'Slurm QOS for Yield SU', NULL);

INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.algorithm', 'Yield estimation algorithm', 'string', true, 38, true, 'Yield estimation algorithm', '{ "allowed_values": [{ "value": "rf", "display": "Random Forest" }, { "value": "lmr", "display": "Linear regression"}, { "value": "svm", "display": "Support Vector Machine"}] }', true);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.selection-type',  'Yield estimation selection type', 'string', true, 38, true, 'Yield estimation selection type', '{ "allowed_values": [{ "value": "automatic", "display": "Automatic" }, { "value": "manual", "display": "Manual"}, { "value": "none", "display": "None" }] }', true);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.max-automatic-features-no', 'Maximum number of automatic features', 'int', true, 38, true, 'Maximum number of automatic features', null);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.manual-selection-features', 'Manual selection features', 'string', true, 38, true, 'Manual selection features', null);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.input_l2a', 'The list of L2A products', 'select', FALSE, 38, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 38, TRUE, 'Start date', NULL, FALSE);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 38, TRUE, 'End date', NULL, FALSE);

INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.su_path', 'Yield SU path', 'string', TRUE, 38, FALSE, 'Yield SU path', null);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.data_extr_dir', 'Yield SU data extraction dir', 'string', TRUE, 38, FALSE, 'Yield SU data extraction dir', null);

INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.historical_data_upload_dir', 'Yield SU historical data upload dir', 'string', TRUE, 38, FALSE, 'Yield SU historical data upload dir', null);
INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.historical_data_path', 'Yield SU historical data import location', 'string', TRUE, 38, FALSE, 'Yield SU historical data import location', null);
INSERT INTO config_metadata VALUES ('executor.module.path.s4s_yield_su_historical_data_import', 'Yield SU historical data import script', 'string', TRUE, 38, FALSE, 'Yield SU historical data import script', null);

-- -----------------------------------------------------------
-- Zarr converter Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.zarr.slurm_qos', 'Slurm QOS for Zarr processor', 'string', true, 8, FALSE, 'Slurm QOS for Zarr processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.zarr', 'Path for Zarr temporary files', 'string', false, 1, FALSE, 'Path for Zarr temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.zarr.keep_job_folders', 'Keep ZARR intermediate folders', 'int', false, 8, FALSE, 'Keep ZARR intermediate folders', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.zarr-converter', 'Zarr converter Path', 'file', true, 8, FALSE, 'Zarr converter Path', NULL);

INSERT INTO config_metadata VALUES ('processor.zarr.enabled', 'Zarr conversion enabled', 'bool', false, 35, FALSE, 'Zarr conversion enabled', NULL);
INSERT INTO config_metadata VALUES ('processor.zarr.input_l3b', 'The list of L3B products', 'select', FALSE, 35, TRUE, 'Available L3B input files', '{"name":"inputFiles_L3B[]","product_type_id":3,"satellite_ids":[1,2]}');

-- -----------------------------------------------------------
-- Parcels Heterogeneity Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s4c_heterog.slurm_qos', 'Slurm QOS for Parcels Heterogeneity processor', 'string', true, 8, FALSE, 'Slurm QOS for Parcels Heterogeneity processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_heterog', 'Path for Parcels Heterogeneity temporary files', 'string', false, 1, FALSE, 'Path for Parcels Heterogeneity temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_heterog.keep_job_folders', 'Keep Parcels Heterogeneity intermediate folders', 'int', false, 8, FALSE, 'Keep Parcels Heterogeneity intermediate folders', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_heterog.input_l2a', 'The list of L2A products', 'select', FALSE, 36, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);

INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-preparation-s1.docker_image', 'Heterogeneity S1 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 cluster preparation docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-preparation-s2.docker_image', 'Heterogeneity S2 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 cluster preparation docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s2.docker_image', 'Heterogeneity S2 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 cluster analysis docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-analysis-s1.docker_image', 'Heterogeneity S1 cluster preparation docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 cluster analysis docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-extract-s1-list.docker_image', 'Heterogeneity S1 list extractor docker image', 'string', false, 1, FALSE, 'Heterogeneity S1 list extractor docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-temporal-resampling.docker_image', 'Heterogeneity S2 temporal resampling docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 temporal resampling docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-cluster-tiles-analysis-merge.docker_image', 'Heterogeneity tiles analysis merge docker image', 'string', false, 1, FALSE, 'Heterogeneity tiles analysis merge docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-heterog-period-analysis.docker_image', 'Heterogeneity S2 period analysis docker image', 'string', false, 1, FALSE, 'Heterogeneity S2 period analysis docker image', NULL) ;

INSERT INTO config_metadata VALUES ('executor.module.path.s4c-heterog-crop-type', 'Heterogeneity crop type wrapper path', 'file', true, 8, FALSE, 'Heterogeneity crop type wrapper', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-preparation-s1', 'Heterogeneity S1 cluster preparation path', 'file', true, 8, FALSE, 'Heterogeneity S1 cluster preparation path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-preparation-s2', 'Heterogeneity S2 cluster preparation path', 'file', true, 8, FALSE, 'Heterogeneity S2 cluster preparation path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-analysis-s1', 'Heterogeneity S1 cluster analysis path', 'file', true, 8, FALSE, 'Heterogeneity S1 cluster analysis path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-analysis-s2', 'Heterogeneity S2 cluster analysis path', 'file', true, 8, FALSE, 'Heterogeneity S2 cluster analysis path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-heterog-extract-s1-list', 'Heterogeneity S1 list extractor path', 'file', true, 8, FALSE, 'Heterogeneity S1 list extractor path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-cluster-tiles-analysis-merge', 'Heterogeneity tiles analysis merge path', 'file', true, 8, FALSE, 'Heterogeneity tiles analysis merge path', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-heterog-period-analysis', 'Heterogeneity period analysis path', 'file', true, 8, FALSE, 'Heterogeneity period analysis path', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_heterog.start_date', 'Start date', 'string', false, 36, true, 'Start date', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.end_date', 'End date', 'string', false, 36, true, 'End date', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.clustering_period', 'Clustering period', 'int', true, 36, true, 'Clustering period', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.temporal_resampling_max_dist', 'Temporal resampling maximum distance', 'int', true, 36, true, 'Temporal resampling maximum distance', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.temporal_resampling_windows_radius', 'Temporal resampling window radius', 'int', true, 36, true, 'Temporal resampling window radius', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.mask_value', 'Mask value', 'int', true, 36, true, 'Mask value', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.nan_value', 'NaN value', 'int', true, 36, true, 'NaN value', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s1_temporal_resampling_interval', 'S1 temporal resampling interval', 'int', true, 36, true, 'S1 temporal resampling interval', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s2_temporal_resampling_interval', 'S2 temporal resampling interval', 'int', true, 36, true, 'S2 temporal resampling interval', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s1_clusters_number', 'S1 Number of Clusters', 'int', true, 36, true, 'S1 Number of Clusters', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s2_clusters_number', 'S2 Number of Clusters', 'int', true, 36, true, 'S2 Number of Clusters', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.isolated_pixels_thr', 'Isolated pixels threshold', 'int', true, 36, true, 'Isolated pixels threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.isolated_pixels_smoothing_radius', 'Spatial Smoothing radius', 'int', true, 36, true, 'Spatial Smoothing radius', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.search_radius_s1', 'Spatial connectivity S1 search radius', 'int', true, 36, true, 'Spatial connectivity S1 search radius', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.search_radius_s2', 'Spatial connectivity S1 search radius', 'int', true, 36, true, 'Spatial connectivity S2 search radius', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.full_connectivity', 'Use full connectivity', 'bool', true, 36, true, 'Use full connectivity', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s1_min_cluster_pixels', 'S1 minimum cluster pixels', 'int', true, 36, true, 'S1 minimum cluster pixels', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.s2_min_cluster_pixels', 'S2 minimum cluster pixels', 'int', true, 36, true, 'S2 minimum cluster pixels', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.ndvi_clust_dist_thr', 'Threshold of the NDVI distance calculated between clusters', 'float', true, 36, true, 'Threshold of the NDVI distance', null);
INSERT INTO config_metadata VALUES ('processor.s4c_heterog.percentage_hererogeneity', 'Pixels percentage corresponding to the biggest cluster in the parcel', 'float', true, 36, true, 'Heterogeneity percentage', null);


-- -----------------------------------------------------------
-- S4C Bare Soil Specific Keys
-- -----------------------------------------------------------
INSERT INTO config_metadata VALUES ('executor.processor.s4c_bare_soil.slurm_qos', 'Slurm QOS for Bare Soil processor', 'string', true, 8, FALSE, 'Slurm QOS for Bare Soil processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_bare_soil', 'Path for Bare Soil temporary files', 'string', false, 1, FALSE, 'Path for Bare Soil temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_bare_soil.keep_job_folders', 'Keep Bare Soil intermediate folders', 'int', false, 8, FALSE, 'Keep Bare Soil intermediate folders', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.input_l2a', 'The list of L2A products', 'select', FALSE, 37, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);

INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-calibration.docker_image', 'Bare Soil S2 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Calibration docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-calibration.docker_image', 'Bare Soil S1 Calibration docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Calibration docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s2-model.docker_image', 'Bare Soil S2 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S2 Model docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-s1-model.docker_image', 'Bare Soil S1 Model docker image', 'string', false, 1, FALSE, 'Bare Soil S1 Model docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-bare-soil-markers.docker_image', 'Bare Soil Markers extraction docker image', 'string', false, 1, FALSE, 'Bare Soil Markers extraction docker image', NULL) ;

INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s2-calibration', 'Bare Soil S2 Calibration script', 'string', true, 8, FALSE, 'Bare Soil S2 Calibration script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s1-calibration', 'Bare Soil S1 Calibration script', 'string', true, 8, FALSE, 'Bare Soil S1 Calibration script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s2-model', 'Bare Soil S2 Model script', 'string', true, 8, FALSE, 'Bare Soil S2 Model script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-s1-model', 'Bare Soil S1 Model script', 'string', true, 8, FALSE, 'Bare Soil S1 Model script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-bare-soil-markers', 'Bare Soil Markers extraction script', 'string', true, 8, FALSE, 'Bare Soil Markers extraction script', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.start_date', 'Start date', 'string', false, 37, true, 'Start date', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.end_date', 'End date', 'string', false, 37, true, 'End date', null);

INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_bs_ndvi_thr', 'Calibration BS NDVI Threshold', 'float', true, 37, true, 'Calibration BS NDVI Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_ndvi_thr', 'Calibration NBS NDVI Threshold', 'float', true, 37, true, 'Calibration NBS NDVI Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_bs_ndwi_thr', 'Calibration BS NDWI Threshold', 'float', true, 37, true, 'Calibration BS NDWI Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_ndwi_thr', 'Calibration NBS NDWI Threshold', 'float', true, 37, true, 'Calibration NBS NDWI Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_bs_ndti_thr', 'Calibration BS NDTI Threshold', 'float', true, 37, true, 'Calibration BS NDTI Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_ndti_thr', 'Calibration NBS NDTI Threshold', 'float', true, 37, true, 'Calibration NBS NDTI Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.calib_nbs_fcover_thr', 'Calibration NBS fCover Threshold', 'float', true, 37, true, 'Calibration NBS fCover Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.model_estimator_no', 'Number of estimators for model', 'int', true, 37, true, 'Number of estimators for model', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_long_period', 'Markers Long Period', 'int', true, 37, true, 'Markers Long Period', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_short_period', 'Markers Short Period', 'int', true, 37, true, 'Markers Short Period', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_s2_periods_no', 'Markers Number of S2 periods', 'int', true, 37, true, 'Markers Number of S2 periods', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_s1_periods_no', 'Markers Number of S1 periods', 'int', true, 37, true, 'Markers Number of S1 periods', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_bs_s2_threshold', 'Markers S2 BS Threshold', 'float', true, 37, true, 'Markers S2 BS Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_nbs_s2_threshold', 'Markers S2 NBS Threshold', 'float', true, 37, true, 'Markers S2 NBS Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_bs_s1_threshold', 'Markers S1 BS Threshold', 'float', true, 37, true, 'Markers S1 BS Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_bare_soil.markers_nbs_s1_threshold', 'Markers S1 NBS Threshold', 'float', true, 37, true, 'Markers S1 NBS Threshold', null);

-- -----------------------------------------------------------
-- S4C Change Detection Specific Keys
-- -----------------------------------------------------------

INSERT INTO config_metadata VALUES ('executor.processor.s4c_change_detection.slurm_qos', 'Slurm QOS for Change Detection processor', 'string', true, 8, FALSE, 'Slurm QOS for Change Detection processor', NULL);
INSERT INTO config_metadata VALUES ('general.scratch-path.s4c_change_detection', 'Path for Change Detection temporary files', 'string', false, 1, FALSE, 'Path for Change Detection temporary files', NULL);
INSERT INTO config_metadata VALUES ('executor.processor.s4c_change_detection.keep_job_folders', 'Keep Change Detection intermediate folders', 'int', false, 8, FALSE, 'Keep Change Detection intermediate folders', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.input_l2a', 'The list of L2A products', 'select', FALSE, 40, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE);

INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-extract-common-parcels.docker_image', 'Common parcels extraction docker image', 'string', false, 1, FALSE, 'Common parcels extraction docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-filter-lpis-cols.docker_image', 'LPIS columns filtering docker image', 'string', false, 1, FALSE, 'LPIS columns filtering docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-lai-outliers.docker_image', 'LAI Outliers docker image', 'string', false, 1, FALSE, 'LAI Outliers docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-veg-growth-markers.docker_image', 'Vegetation Growth markers docker image', 'string', false, 1, FALSE, 'Vegetation Growth markers docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-bs-markers.docker_image', 'Bare soil markers filtering docker image', 'string', false, 1, FALSE, 'Bare soil markers filtering docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-computation.docker_image', 'Change Detection computation docker image', 'string', false, 1, FALSE, 'Change Detection computation docker image', NULL) ;
INSERT INTO config_metadata VALUES ('general.orchestrator.s4c-change-detection-consolidation.docker_image', 'Change Detection consolidation docker image', 'string', false, 1, FALSE, 'Change Detection consolidation docker image', NULL) ;

INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-extract-common-parcels', 'Common parcels extraction script', 'string', true, 8, FALSE, 'Common parcels extraction script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-filter-lpis-cols', 'LPIS columns filtering script', 'string', true, 8, FALSE, 'LPIS columns filtering script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-lai-outliers', 'LAI Outliers script', 'string', true, 8, FALSE, 'LAI Outliers script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-veg-growth-markers', 'Vegetation Growth markers script', 'string', true, 8, FALSE, 'Vegetation Growth markers script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-bs-markers', 'Bare soil markers filtering script', 'string', true, 8, FALSE, 'Bare soil markers filtering script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-computation', 'Change Detection computation script', 'string', true, 8, FALSE, 'Change Detection computation script', NULL);
INSERT INTO config_metadata VALUES ('executor.module.path.s4c-change-detection-consolidation', 'Change Detection consolidation script', 'string', true, 8, FALSE, 'Change Detection consolidation script', NULL);

INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.start_date', 'Start date', 'string', false, 40, true, 'Start date', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.end_date', 'End date', 'string', false, 40, true, 'End date', null);

insert into config_metadata values ('processor.s4c_change_detection.ref_site_id', 'Reference site', 'string', false, 40, true, 'Reference site', '{ "allowed_values_source": { "database_object": "site", "value_column": "id", "label_column": "name" } }', true);

INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_start_date', 'Reference Start date', 'string', false, 40, true, 'Reference Start date', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_end_date', 'Reference End date', 'string', false, 40, true, 'Reference End date', null);

INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_mdb1_ids_mapping', 'Reference MDB1 NewID mapping file', 'string', true, 40, true, 'Reference MDB1 NewID mapping file', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_bs_ids_mapping', 'Reference BS NewID mapping file', 'string', true, 40, true, 'Reference BS NewID mapping file', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.mdb1_ids_mapping', 'Current MDB1 NewID mapping file', 'string', true, 40, true, 'Current MDB1 NewID mapping file', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.bs_ids_mapping', 'Current BS NewID mapping file', 'string', true, 40, true, 'Current BS NewID mapping file', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.sites_ids_mapping', 'Sites NewIDs mapping file', 'string', true, 40, true, 'Sites NewIDs mapping file', null);

-- Reference period Grassland changes
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_thr', 'Reference Grassland TTdaysS2 Threshold', 'string', true, 40, true, 'Reference Grassland TTdaysS2 Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_incr', 'Reference Grassland TTdaysS2 Increment', 'string', true, 40, true, 'Reference Grassland TTdaysS2 Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_thr', 'Reference Grassland Ratio Stability Threshold Min', 'string', true, 40, true, 'Reference Grassland Ratio Stability Threshold Min', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_thr', 'Reference Grassland Ratio Stability Threshold Max', 'string', true, 40, true, 'Reference Grassland Ratio Stability Threshold Max', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_incr', 'Reference Grassland Ratio Stability Min Increment', 'string', true, 40, true, 'Reference Grassland Ratio Stability Min Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_incr', 'Reference Grassland Ratio Stability Max Increment', 'string', true, 40, true, 'Reference Grassland Ratio Stability Max Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_thr', 'Reference Grassland ConsecC Stability Threshold', 'string', true, 40, true, 'Reference Grassland ConsecC Stability Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_incr', 'Reference Grassland ConsecC Stability Increment', 'string', true, 40, true, 'Reference Grassland ConsecC Stability Increment', null);
-- Reference period Permanent crops changes
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_thr', 'Reference Permanent Crops TTdaysS2 Threshold', 'string', true, 40, true, 'Reference Permanent Crops TTdaysS2 Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_incr', 'Reference Permanent Crops TTdaysS2 Increment', 'string', true, 40, true, 'Reference Permanent Crops TTdaysS2 Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_thr', 'Reference Permanent Crops AreaVeg Threshold', 'string', true, 40, true, 'Reference Permanent Crops AreaVeg Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_incr', 'Reference Permanent Crops AreaVeg Increment', 'string', true, 40, true, 'Reference Permanent Crops AreaVeg Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_thr', 'Reference Grassland Ratio Stability Threshold', 'string', true, 40, true, 'Reference Grassland Ratio Stability Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_incr', 'Reference Grassland Ratio Stability Increment', 'string', true, 40, true, 'Reference Grassland Ratio Stability Increment', null);
-- Reference period Arable land changes
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_thr', 'Reference Arable land TTdaysS2 Threshold', 'string', true, 40, true, 'Reference Arable land TTdaysS2 Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_incr', 'Reference Arable land TTdaysS2 Increment', 'string', true, 40, true, 'Reference Arable land TTdaysS2 Increment', null);

-- Current year Grassland changes
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ttdayss2_thr', 'Grassland TTdaysS2 Threshold', 'string', true, 40, true, 'Grassland TTdaysS2 Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ttdayss2_incr', 'Grassland TTdaysS2 Increment', 'string', true, 40, true, 'Grassland TTdaysS2 Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_thr', 'Grassland Ratio Stability Threshold Min', 'string', true, 40, true, 'Grassland Ratio Stability Threshold Min', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_thr', 'Grassland Ratio Stability Threshold Max', 'string', true, 40, true, 'Grassland Ratio Stability Threshold Max', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_incr', 'Grassland Ratio Stability Min Increment', 'string', true, 40, true, 'Grassland Ratio Stability Min Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_incr', 'Grassland Ratio Stability Max Increment', 'string', true, 40, true, 'Grassland Ratio Stability Max Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_consecstab_thr', 'Grassland ConsecC Stability Threshold', 'string', true, 40, true, 'Grassland ConsecC Stability Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.grassland_consecstab_incr', 'Grassland ConsecC Stability Increment', 'string', true, 40, true, 'Grassland ConsecC Stability Increment', null);
-- Current year Permanent crops changes
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_thr', 'Permanent Crops TTdaysS2 Threshold', 'string', true, 40, true, 'Permanent Crops TTdaysS2 Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_incr', 'Permanent Crops TTdaysS2 Increment', 'string', true, 40, true, 'Permanent Crops TTdaysS2 Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_areaveg_thr', 'Permanent Crops AreaVeg Threshold', 'string', true, 40, true, 'Permanent Crops AreaVeg Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_areaveg_incr', 'Permanent Crops AreaVeg Increment', 'string', true, 40, true, 'Permanent Crops AreaVeg Increment', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ratiostab_thr', 'Grassland Ratio Stability Threshold', 'string', true, 40, true, 'Grassland Ratio Stability Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.permcrops_ratiostab_incr', 'Grassland Ratio Stability Increment', 'string', true, 40, true, 'Grassland Ratio Stability Increment', null);
-- Current year Arable land changes
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.arableland_ttdayss2_thr', 'Arable land TTdaysS2 Threshold', 'string', true, 40, true, 'Arable land TTdaysS2 Threshold', null);
INSERT INTO config_metadata VALUES ('processor.s4c_change_detection.arableland_ttdayss2_incr', 'Arable land TTdaysS2 Increment', 'string', true, 40, true, 'Arable land TTdaysS2 Increment', null);
