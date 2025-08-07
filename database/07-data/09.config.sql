-- -----------------------------------------------------------
-- General keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.archive_path', NULL, '/mnt/archive/{site}/{processor}/', '2016-02-18 17:29:41.20487+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.l2a', NULL, '5', '2015-07-20 16:31:33.655476+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path', NULL, '/mnt/archive/orchestrator_temp/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('http-listener.listen-port', NULL, '8082', '2015-07-03 13:59:21.338392+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('mail.message.batch.limit', NULL, '0','2020-07-22 19:52:23.560587+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('monitor-agent.disk-path', NULL, '/mnt/archive/', '2015-07-20 10:27:29.301355+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('monitor-agent.scan-interval', NULL, '60', '2015-07-20 10:28:08.27395+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('resources.working-mem', NULL, '1024', '2015-09-08 11:03:21.87284+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.lookup.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.object.storage.move.deleteAfter', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.object.storage.move.enabled', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.object.storage.move.product.types', NULL, '', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.reports.enabled', NULL, 'true', '2020-05-04 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.reports.interval', NULL, '24', '2020-05-04 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('scheduled.retry.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('site.path', NULL, '/usr/share/sen2agri/sen2agri-services/static','2020-07-22 19:52:22.841374+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('site.upload-path', NULL, '/mnt/upload/{site}', '2016-03-01 15:02:31.980394+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('site.url', NULL, ' file:///usr/share/sen2agri/sen2agri-services/static/ ','2020-07-22 19:52:22.42305+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('l8.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('l9.enabled', NULL, 'false', '2024-02-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('s1.enabled', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('s2.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcel_id_col_name', NULL, 'NewID', '2019-10-11 16:15:00.0+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_csv_file_name_pattern', NULL, 'decl_.*_\d{4}.csv', '2019-10-11 16:15:00.0+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_optical_file_name_pattern', NULL, '.*_buf_5m.shp', '2019-10-11 16:15:00.0+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_sar_file_name_pattern', NULL, '.*_(\d{4,5})_buf_10m.shp', '2019-10-11 16:15:00.0+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.docker_script_unit_image', NULL, 'sen4cap/data-preparation:0.3', '2023-11-16 20:05:00');

-- -----------------------------------------------------------
-- Executor/orchestrator/scheduler specific keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.http-server.listen-ip', NULL, '127.0.0.1', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.http-server.listen-port', NULL, '8084', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.listen-ip', NULL, '127.0.0.1', '2015-06-03 17:03:39.541136+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.listen-port', NULL, '7777', '2015-07-07 12:17:06.182674+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.resource-manager.name', NULL, 'slurm', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.wrapper-path', NULL, '/usr/bin/sen2agri-processor-wrapper', '2015-07-23 16:54:54.092462+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.wrp-executes-local', NULL, '1', '2015-06-03 17:03:39.541136+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.wrp-send-retries-no', NULL, '3600', '2015-06-03 17:03:39.541136+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.wrp-timeout-between-retries', NULL, '1000', '2015-06-03 17:03:39.541136+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.sacct-max-retries', NULL, '1', '2023-10-26 17:03:39.541136+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.inter-proc-com-type', NULL, 'http', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_add_mounts', NULL, '', '2021-01-21 10:23:12.993537+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.3.0', '2021-01-14 12:11:21.800537+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.earth-signature.docker_image',  NULL, 'snapearth/earthagriculture:0.1', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.ndvi-veg-stats.docker_image',  NULL, 'snapearth/earthagriculture:0.1', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.export-product-launcher.use_docker', NULL, '0', '2021-01-20 11:44:25.330355+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-extract-products.use_docker', NULL, '0', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-gen-input-shp.use_docker', NULL, '0', '2021-01-18 14:41:25.651377+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-mowing.docker_image', NULL, 'sen4cap/grassland_mowing:3.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-grassland-mowing.use_docker', NULL, '1', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-l4a-extract-parcels.use_docker', NULL, '0', '2021-01-20 18:50:52.244303+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.use_docker', NULL, '1', '2021-01-14 12:11:21.800537+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.http-server.listen-ip', NULL, '127.0.0.1', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.http-server.listen-port', NULL, '8083', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.mdb3-input-tables-extract.docker_image', NULL, 'sen4cap/data-preparation:0.1', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.mdb3-input-tables-extract.use_docker', NULL, '1', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-crop-type-mapping.docker_image', NULL, 'sen4x/crop-map-s4s:0.4.0', '2022-08-22 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.check_ancestors.disabled', NULL, 'true', '2023-03-17 14:43:00.720811+00');


-- -----------------------------------------------------------
-- Executor module paths
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.color-mapping', NULL, '/usr/bin/otbcli_ColorMapping', '2015-11-17 17:06:25.784583+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.compression', NULL, '/usr/bin/otbcli_Convert', '2015-11-17 17:06:34.7028+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.compute-confusion-matrix', NULL, '/usr/bin/otbcli_ComputeConfusionMatrix', '2015-08-12 17:09:22.060276+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.compute-image-statistics', NULL, '/usr/bin/otbcli_ComputeImagesStatistics', '2016-02-23 12:29:52.586902+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.compute-images-statistics', NULL, '/usr/bin/otbcli_ComputeImagesStatistics', '2015-08-12 17:09:17.216345+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.concatenate-images', NULL, '/usr/bin/otbcli_ConcatenateImages', '2015-09-07 10:20:52.117401+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.crop-mask-fused', NULL, 'CropMaskFused.py', '2015-12-17 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.crop-type-fused', NULL, 'CropTypeFused.py', '2015-12-17 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.earth-signature', NULL, 'earthsignature.py', '2022-04-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.ndvi-veg-stats', NULL, 'extract_l3_veg_stats.py', '2022-04-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.zarr-converter', NULL, 's2x_prd_to_zarr.py', '2022-04-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.dimensionality-reduction', NULL, 'otbcli_DimensionalityReduction', '2016-02-22 22:39:08.262715+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.end-of-job', NULL, '/usr/bin/true', '2016-01-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.export-product-launcher', NULL, '/usr/bin/export-product-launcher.py', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.files-remover', NULL, '/usr/bin/rm', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.gdalbuildvrt', NULL, '/usr/bin/gdalbuildvrt', '2018-08-30 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.gdal_translate', NULL, '/usr/bin/gdal_translate', '2018-08-30 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.image-classifier', NULL, '/usr/bin/otbcli_ImageClassifier', '2015-08-12 17:09:20.418973+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.image-compression', NULL, '/usr/bin/otbcli_Convert', '2016-02-22 22:39:08.386406+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4b_cfg_import', NULL, 's4c_l4b_import_config.py', '2019-10-22 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_cfg_import', NULL, 's4c_l4c_import_config.py', '2019-10-22 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_practices_export', NULL, '/usr/bin/s4c_l4c_export_all_practices.py', '2019-10-22 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.l4c_practices_import', NULL, 's4c_l4c_import_practice.py', '2019-10-22 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lpis_import', NULL, 'data-preparation.py', '2019-10-22 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb-csv-to-ipc-export', NULL, 'csv_to_ipc.py', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lsms-segmentation', NULL, '/usr/bin/otbcli_LSMSSegmentation', '2016-02-22 22:39:08.324364+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lsms-small-regions-merging', NULL, '/usr/bin/otbcli_LSMSSmallRegionsMerging', '2016-02-22 22:39:08.344953+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mean-shift-smoothing', NULL, '/usr/bin/otbcli_MeanShiftSmoothing', '2016-02-22 22:39:08.303643+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.ogr2ogr',  NULL, '/usr/bin/ogr2ogr', '2019-10-18 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-crop-type', NULL, 'crop-type-wrapper.py', '2019-02-22 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-grassland-gen-input-shp',  NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/generate_grassland_mowing_input_shp.py', '2019-10-18 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-grassland-extract-products', NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/s4c-l4b-extract-products.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-l4a-extract-parcels', NULL, 'extract-parcels.py', '2021-01-15 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.train-images-classifier', NULL, '/usr/bin/otbcli_TrainImagesClassifier', '2015-08-12 17:09:18.767175+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb3-input-tables-extract', NULL, 's4c_mdb3_input_tables.py', '2021-01-15 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.mdb3-extract-markers', NULL, 'extract_mdb3_markers.py', '2021-01-15 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.lpis_list_columns', NULL, 'read_shp_cols.py', '2022-02-12 17:09:18.767175+03');


-- -----------------------------------------------------------
-- Downloader specific keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.forcestart', NULL, 'false', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.max-retries', NULL, '3', '2016-03-15 15:44:22.03691+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.write-dir', NULL, '/mnt/archive/dwn_def/l8/default', '2016-02-26 19:30:06.821627+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.max-cloud-coverage', NULL, '100', '2016-02-03 18:05:38.425734+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.enabled', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.forcestart', NULL, 'false', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.write-dir', NULL, '/mnt/archive/dwn_def/s1/default/', '2016-07-20 20:05:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.forcestart', NULL, 'false', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.max-retries', NULL, '3', '2016-03-15 15:44:14.118906+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.write-dir', NULL, '/mnt/archive/dwn_def/s2/default', '2016-02-26 19:26:49.986675+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.skip.existing', NULL, 'false', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.start.offset', NULL, '2', '2016-07-20 20:05:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.timeout', NULL, '9000','2020-07-22 19:52:22.244592+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.use.esa.l2a', NULL, 'false', '2019-12-16 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.query.timeout', NULL, '90','2020-07-22 19:52:22.244592+00');

-- -----------------------------------------------------------
-- L2A processor Specific keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l2a.name', NULL, 'L2A', '2015-06-03 17:02:50.028002+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l2a.path', NULL, '/bin/false', '2015-07-20 16:31:23.208369+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.gipp-path', NULL, '/mnt/archive/gipp/maja', '2016-02-24 18:12:16.464479+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.remove-fre', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja.remove-sre', NULL, '1', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.cog-tiffs', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.compress-tiffs', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.max-retries', NULL, '3', '2020-09-15 16:02:27.164968+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.num-workers', NULL, '4', '2020-09-07 14:36:37.906825+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.output-path', NULL, '/mnt/archive/maccs_def/{site}/{processor}/', '2016-02-24 18:09:17.379905+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.optical.retry-interval', NULL, '1 day', '2020-09-07 14:36:37.906825+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.s2.implementation', NULL, 'maja', '2020-09-07 14:17:52.846794+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.sen2cor.gipp-path', NULL, '/mnt/archive/gipp/sen2cor', '2020-09-15 16:48:05.415193+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.srtm-path', NULL, '/mnt/archive/srtm', '2016-02-25 11:11:36.372405+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.swbd-path', NULL, '/mnt/archive/swbd', '2016-02-25 11:12:04.008319+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a.working-dir', NULL, '/mnt/archive/demmaccs_tmp/', '2016-02-25 17:31:06.01191+02');
insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.processors_image', NULL, 'sen4x/l2a-processors:0.2.5', '2021-04-19 16:30:00.0');
insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.sen2cor_image', NULL, 'sen4x/sen2cor:2.10.01-ubuntu-20.04', '2021-04-19 16:30:00.0');
insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.maja_image', NULL, 'sen4x/maja:4.5.4-centos-7', '2021-04-19 16:30:00.0');
insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.4.1', '2021-04-19 16:30:00.0');
insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.l8_align_image', NULL, 'sen4x/l2a-l8-alignment:0.1.2', '2021-04-19 16:30:00.0');
insert into config(key, site_id, value, last_updated) VALUES ('processor.l2a.dem_image', NULL, 'sen4x/l2a-dem:0.1.3', '2021-04-19 16:30:00.0');

-- -----------------------------------------------------------
-- L2S1 processor Specific keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('dem.name', NULL, 'SRTM 1Sec HGT', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.enabled', NULL, 'false', '2020-05-18 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.parallelism', NULL, '1','2020-07-22 19:52:22.395661+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.path', NULL, '/mnt/archive/{site}/l2a-s1', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.temporal.offset', NULL, '6','2020-07-22 19:52:22.42305+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.work.dir', NULL, '/mnt/archive/s1_preprocessing_work_dir', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.compute.amplitude', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.compute.coherence', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.gpt.parallelism', NULL, '8', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.gpt.tile.cache.size', NULL, '256', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.join.amplitude.steps', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.join.coherence.steps', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.acquisition.delay', NULL, '2', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.copy.locally', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.crop.nodata', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.crop.output', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.extract.histogram', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.interval', NULL, '60', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.keep.intermediate', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.master', NULL, 'S1B', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.intersection', NULL, '0.05', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.output.extension', NULL, '.tif', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.output.format', NULL, 'GDAL-GTiff-WRITER', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.overwrite.existing', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.parallel.steps.enabled', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.pixel.spacing', NULL, '10', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.polarisations', NULL, 'VV;VH', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.process.newest', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.projection', NULL, 'PROJCS["ETRS89 / LAEA Europe", GEOGCS["ETRS89", DATUM["European Terrestrial Reference System 1989", SPHEROID["GRS 1980", 6378137.0, 298.257222101, AUTHORITY["EPSG","7019"]], TOWGS84[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], AUTHORITY["EPSG","6258"]], PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]], UNIT["degree", 0.017453292519943295], AXIS["Geodetic longitude", EAST], AXIS["Geodetic latitude", NORTH], AUTHORITY["EPSG","4258"]], PROJECTION["Lambert_Azimuthal_Equal_Area", AUTHORITY["EPSG","9820"]], PARAMETER["latitude_of_center", 52.0], PARAMETER["longitude_of_center", 10.0], PARAMETER["false_easting", 4321000.0], PARAMETER["false_northing", 3210000.0], UNIT["m", 1.0], AXIS["Easting", EAST], AXIS["Northing", NORTH], AUTHORITY["EPSG","3035"]]', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.resolve.links', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.step.timeout', NULL, '60', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.temporal.filter.interval', NULL, '24', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.version', NULL, '1', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.memory', NULL, '8192', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.disk', NULL, '16384', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.use.other.site.products', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.min.s2.intersection', NULL, '0.05', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.ignore.previous.orbit.failure', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.otb.min.memory', NULL, '2048', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.otb.enable.compression', NULL, true, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.terrain.flattening.autodetect', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.terrain.flattening.enabled', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.subset.before.tc.enabled', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.bck.scale', NULL, '10000', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.cohe.scale', NULL, '10000', '2022-09-30 10:31:00.501+02');
-- In V2, the following 4 lines should be set to true
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.compress.enabled', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.convert.int', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.crop.enabled', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.tiled.tiff', NULL, false, '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.zarr.conversion', NULL, false, '2022-09-30 10:31:00.501+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.container', NULL, '', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.domain', NULL, '', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.password', NULL, '', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.projectId', NULL, '', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.url', NULL, '', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.user', NULL, '', '2022-09-30 10:31:00.501+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('primary.sensor', NULL, 'S2', '2022-09-30 10:31:00.501+02');


-- -----------------------------------------------------------
-- LPIS configuration specific keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.lpis.lut_upload_path', NULL, '/mnt/archive/upload/LUT/{site}', '2019-10-11 16:15:00.0+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.lpis.path', NULL, '/mnt/archive/lpis/{site}/{year}', '2019-06-11 16:15:00.0+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.lpis.upload_path', NULL, '/mnt/archive/upload/lpis/{site}', '2019-10-11 16:15:00.0+02');

-- -----------------------------------------------------------
-- L3A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.l3a', NULL, '1', '2015-06-02 11:39:40.357184+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3a.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3a.slurm_qos', NULL, 'qoscomposite', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l3a', NULL, '/mnt/archive/orchestrator_temp/l3a/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.bandsmapping', NULL, '/usr/share/sen2agri/bands_mapping_s2.txt', '2016-02-29 14:08:01.731357+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.cloud_optimized_geotiff_output', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.generate_20m_s2_resolution', NULL, 'true', '2016-02-26 19:30:06.821627+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.half_synthesis', NULL, '25', '2016-02-25 09:00:57.21868+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.synthesis_date', NULL, '', '2016-02-25 09:00:57.21868+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.lut_path', NULL, '/usr/share/sen2agri/composite.map', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.preproc.scatcoeffs_10m', NULL, '/usr/share/sen2agri/scattering_coeffs_10m.txt', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.preproc.scatcoeffs_20m', NULL, '/usr/share/sen2agri/scattering_coeffs_20m.txt', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.sched_wait_proc_inputs', NULL, '1', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.synth_date_sched_offset', NULL, '30', '2016-02-25 09:00:47.212845+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.aot.maxaot', NULL, '0.8', '2015-12-15 16:03:45.738858+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.aot.maxweight', NULL, '1', '2015-12-15 16:39:33.011849+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.aot.minweight', NULL, '0.33', '2015-12-15 16:38:59.421733+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.cloud.coarseresolution', NULL, '240', '2015-12-15 16:39:42.524199+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.cloud.sigmalarge', NULL, '10', '2015-12-15 16:40:31.123914+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.cloud.sigmasmall', NULL, '2', '2015-12-15 16:39:54.875658+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.weight.total.weightdatemin', NULL, '0.5', '2015-12-15 16:40:39.851801+02');

-- -----------------------------------------------------------
-- L3B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.l3b', NULL, '1', '2015-06-02 11:39:45.99546+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3b.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3b.slurm_qos', NULL, 'qoslai', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l3b', NULL, '/mnt/archive/orchestrator_temp/l3b/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.cloud_optimized_geotiff_output', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_fapar', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_fcover', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_in_domain_flags', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_lai', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_ndvi', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_ndwi', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.produce_brightness', NULL, 'false', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.filter.chain_inputs_steps', NULL, 'false', '2017-10-24 14:56:57.501918+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.produce_mosaic', NULL, 'true', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.generate_models', NULL, '1', '2016-02-29 12:03:08.445828+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.l1c_availability_days', NULL, '20', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.global_bv_samples_file', NULL, '/usr/share/sen2agri/LaiCommonBVDistributionSamples.txt', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.laibandscfgfile', NULL, '/usr/share/sen2agri/Lai_Bands_Cfgs_Belcam.cfg', '2016-02-16 11:54:47.223904+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.lut_path', NULL, '/usr/share/sen2agri/lai.map', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.modelsfolder', NULL, '/mnt/archive/L3B_GeneratedModels/', '2016-02-16 11:54:47.123972+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.rsrcfgfile', NULL, '/usr/share/sen2agri/rsr_cfg.txt', '2016-02-16 11:54:47.223904+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.tiles_filter', NULL, '', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.use_inra_version', NULL, '1', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.lai.use_lai_bands_cfg', NULL, '1', '2016-02-16 11:54:47.223904+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.production_interval', NULL, '10', '2016-02-29 12:03:31.197823+02');
-- TODO: This should be removed or moved to S2A_L3C ?
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.reproc_production_interval', NULL, '30', '2016-02-29 12:03:31.197823+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3b.sched_wait_proc_inputs', NULL, '0', '2015-07-10 17:54:17.288095+03');

-- -----------------------------------------------------------
-- L2A Masked Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l2a_msk', NULL, '/mnt/archive/orchestrator_temp/l2a_msk/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.enabled', NULL, 'true', '2021-05-18 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.compress', NULL, 'true', '2021-05-18 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.tiled', NULL, 'true', '2023-07-28 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.water_is_valid', NULL, 'false', '2023-07-28 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.snow_is_valid', NULL, 'false', '2023-07-28 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l2a_msk.slurm_qos', NULL, 'qosvaliditymsk', '2015-08-24 17:44:38.29255+03');

-- -----------------------------------------------------------
-- S2A_L3C Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s2a_l3c.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s2a_l3c.slurm_qos', NULL, 'qoslai', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s2a_l3c', NULL, '/mnt/archive/orchestrator_temp/s2a_l3c/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s2a_l3c.localwnd.bwr', NULL, '2', '2020-09-03 14:54:40.30341+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s2a_l3c.localwnd.fwr', NULL, '0', '2020-09-03 14:54:40.387588+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s2a_l3c.lut_path', NULL, '/usr/share/sen2agri/lai.map', '2020-09-03 14:08:07.963143+02');

-- -----------------------------------------------------------
-- S2A_L3D Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s2a_l3d.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s2a_l3d.slurm_qos', NULL, 'qoslai', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s2a_l3d', NULL, '/mnt/archive/orchestrator_temp/s2a_l3d/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s2a_l3d.lut_path', NULL, '/usr/share/sen2agri/lai.map', '2020-09-03 14:08:07.963143+02');

-- -----------------------------------------------------------
-- L3E Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3e.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3e.slurm_qos', NULL, 'qospheno', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l3e', NULL, '/mnt/archive/orchestrator_temp/l3e/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3e.cloud_optimized_geotiff_output', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3e.sched_wait_proc_inputs', NULL, '1', '2015-07-10 17:54:17.288095+03');

-- -----------------------------------------------------------
-- L4A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.l4a', NULL, '1', '2015-06-02 11:39:50.928228+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l4a.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l4a.slurm_qos', NULL, 'qoscropmask', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l4a', NULL, '/mnt/archive/orchestrator_temp/l4a/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier', NULL, 'rf', '2016-03-10 18:27:19.634909+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier.field', NULL, 'CROP', '2016-03-10 11:32:30.859069+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier.rf.max', NULL, '25', '2016-03-10 11:33:33.450239+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier.rf.min', NULL, '25', '2016-03-10 11:34:22.252075+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier.rf.nbtrees', NULL, '100', '2016-03-10 11:33:20.290289+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier.svm.k', NULL, 'rbf', '2016-03-10 11:50:39.844696+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.classifier.svm.opt', NULL, '1', '2016-03-10 11:50:42.660864+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.cloud_optimized_geotiff_output', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.erode-radius', NULL, '1', '2016-03-09 16:42:46.48022+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.lut_path', NULL, '/usr/share/sen2agri/crop-mask.lut', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.mahalanobis-alpha', NULL, '0.01', '2016-03-09 16:42:55.696346+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.max-parallelism', NULL, '0', '2018-01-26 17:17:52+02:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.min-area', NULL, '20', '2016-03-09 16:43:02.69676+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.mission', NULL, 'SENTINEL', '2016-03-09 16:28:59.820251+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.nbcomp', NULL, '6', '2016-03-09 16:41:59.169527+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.random_seed', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.range-radius', NULL, '0.65', '2016-03-09 16:42:16.59406+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.reference-map', NULL, '/mnt/archive/reference_data/ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif', '2016-07-13 14:41:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.reference_data_dir', NULL, '/mnt/archive/insitu/{site}/', '2016-03-03 14:46:26.267227+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.sample-ratio', NULL, '0.75', '2016-03-10 11:34:53.889769+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.sched_wait_proc_inputs', NULL, '1', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.segmentation-minsize', NULL, '10', '2016-03-09 16:42:25.393274+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.segmentation-spatial-radius', NULL, '10', '2016-03-09 16:42:09.033172+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.skip-segmentation', NULL, 'false', '2016-10-31 17:32:00+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.smoothing-lambda', NULL, '2', '2016-03-09 16:41:38.409554+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.temporal_resampling_mode', NULL, 'resample', '2016-03-09 16:40:46.810185+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.tile-threads-hint', NULL, '4', '2018-01-26 17:17:52+02:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.training-samples-number', NULL, '40000', '2016-03-10 11:40:44.732473+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.window', NULL, '6', '2016-03-09 16:41:29.945249+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.reference_data_source', NULL, 'insitu', '2022-03-08 10:58:03.38654+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.reference_polygons', NULL, '', '2016-03-03 14:46:26.267227+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.reference_data_source.insitu', NULL, '', '2022-03-08 10:58:03.38654+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4a.earth_signature_insitu', NULL, '/mnt/archive/earth_signature/{site}/l4a/earth_signature_insitu.shp', '2016-03-03 14:46:26.267227+02');

-- -----------------------------------------------------------
-- L4B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.l4b', NULL, '1', '2015-06-02 11:39:56.99407+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l4b.keep_job_folders', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l4b.slurm_qos', NULL, 'qoscroptype', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l4b', NULL, '/mnt/archive/orchestrator_temp/l4b/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier', NULL, 'rf', '2016-03-10 18:29:07.926989+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier.field', NULL, 'CODE', '2016-03-10 18:29:16.079704+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier.rf.max', NULL, '25', '2016-03-10 18:29:20.56483+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier.rf.min', NULL, '25', '2016-03-10 18:29:28.531434+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier.rf.nbtrees', NULL, '100', '2016-03-10 18:29:30.835912+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier.svm.k', NULL, 'rbf', '2016-03-10 18:29:41.383911+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.classifier.svm.opt', NULL, '1', '2016-03-10 18:29:43.250971+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.cloud_optimized_geotiff_output', NULL, '0', '2017-10-24 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.lut_path', NULL, '/usr/share/sen2agri/crop-type.lut', '2016-02-29 14:08:07.963143+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.max-parallelism', NULL, '0', '2018-01-26 17:17:52+02:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.mission', NULL, 'SENTINEL', '2016-03-09 16:28:59.820251+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.random_seed', NULL, '0', '2016-03-09 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.sample-ratio', NULL, '0.75', '2016-03-10 18:29:44.979786+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.sched_wait_proc_inputs', NULL, '1', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.temporal_resampling_mode', NULL, 'resample', '2016-03-10 18:40:21.140908+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.tile-threads-hint', NULL, '4', '2018-01-26 17:17:52+02:00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.reference_data_source', NULL, 'insitu', '2022-03-08 10:58:03.38654+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.crop_mask', NULL, '', '2022-03-08 10:58:03.38654+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.reference_polygons', NULL, '', '2016-03-03 14:46:26.267227+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.reference_data_source.insitu', NULL, '', '2022-03-08 10:58:03.38654+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l4b.earth_signature_insitu', NULL, '/mnt/archive/earth_signature/{site}/l4b/earth_signature_insitu.shp', '2016-03-03 14:46:26.267227+02');

-- -----------------------------------------------------------
-- S4C_L4A Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.s4c_l4a', NULL, '1', '2015-06-02 11:39:50.928228+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4a.keep_job_folders', NULL, '0', '2016-10-18 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4a.slurm_qos', NULL, 'qoss4cl4a', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_l4a', NULL, '/mnt/archive/orchestrator_temp/s4c_l4a/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.best-s2-pix', NULL, '10', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.lc', NULL, '1234', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.min-node-size', NULL, '10', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.min-s1-pix', NULL, '1', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.min-s2-pix', NULL, '3', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.mode', NULL, 'both', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.num-trees', NULL, '300', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.pa-min', NULL, '30', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.pa-train-h', NULL, '4000', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.pa-train-l', NULL, '1333', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.sample-ratio-h', NULL, '0.25', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.sample-ratio-l', NULL, '0.75', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.smote-k', NULL, '5', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.smote-target', NULL, '1000', '2019-02-19 11:09:58.820032+02');

-- -----------------------------------------------------------
-- S4C_L4B Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.s4c_l4b', NULL, '1', '2015-06-02 11:39:50.928228+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4b.keep_job_folders', NULL, '0', '2016-10-18 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4b.slurm_qos', NULL, 'qoss4cl4b', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_l4b', NULL, '/mnt/archive/orchestrator_temp/s4c_l4b/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.cfg_dir',  NULL, '/mnt/archive/grassland_mowing_files/{site}/{year}/config/', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.cfg_upload_dir',  NULL, '/mnt/archive/upload/grassland_mowing_cfg/{site}', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.default_config_path', NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/src_ini/S4C_L4B_Default_Config.cfg', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.end_date',  NULL, '', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.input_amp', NULL, 'N/A', '2019-02-19 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.input_cohe', NULL, 'N/A', '2019-02-19 11:10:25.068169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.input_l3b', NULL, 'N/A', '2019-02-19 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.input_product_types',  NULL, 'S1_S2', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.s1_py_script',  NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/src_s1/S1_main.py', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.s1_s2_startdate_diff',  NULL, '0', '2020-10-02 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.s2_py_script',  NULL, '/usr/share/sen2agri/S4C_L4B_GrasslandMowing/Bin/src_s2/S2_main.py', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.start_date',  NULL, '', '2019-10-18 15:27:41.861613+02');
-- TODO: See if these 2 are used, if not, they should be removed
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.sub_steps',  NULL, 'S1_S2, S1, S2', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4b.working_dir',  NULL, '/mnt/archive/grassland_mowing_files/{site}/{year}/working_dir/', '2019-10-18 15:27:41.861613+02');

-- -----------------------------------------------------------
-- S4C_L4C Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('archiver.max_age.s4c_l4c', NULL, '1', '2015-06-02 11:39:50.928228+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4c.keep_job_folders', NULL, '0', '2016-10-18 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_l4c.slurm_qos', NULL, 'qoss4cl4c', '2015-08-24 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_l4c', NULL, '/mnt/archive/orchestrator_temp/s4c_l4c/{job_id}/{task_id}-{module}', '2015-07-10 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.cfg_dir', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/config/', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.cfg_upload_dir',  NULL, '/mnt/archive/upload/agric_practices_files/{site}/config', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.country', NULL, 'CNTRY', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.data_extr_dir', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/data_extraction/{product_type}', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.default_config_path', NULL, '/usr/share/sen2agri/S4C_L4C_Configurations/S4C_L4C_Default_Config.cfg', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.execution_operation', NULL, 'ALL', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.filter_ids_path', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/ts_input_tables/FilterIds/Sen4CAP_L4C_FilterIds.csv', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.input_amp', NULL, 'N/A', '2019-02-18 15:28:22.404745+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.input_cohe', NULL, 'N/A', '2019-02-18 15:28:41.060339+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.input_l3b', NULL, 'N/A', '2019-02-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.markers_add_no_data_rows', NULL, '1', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.nrt_data_extr_enabled', NULL, 'false', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.practices', NULL, 'NA', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.prds_per_group', NULL, '1', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.sched_prds_hist_file', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/l4c_scheduled_prds_history.txt', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.sub_steps', NULL, 'ALL,DataExtraction,CatchCrop,Fallow,NFC,HarvestOnly,AllTimeSeriesAnalysis', '2019-04-12 14:56:57.501918+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.tillage_monitoring', NULL, '0', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.tsa_min_acqs_no', NULL, '15', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.ts_input_tables_dir', NULL, '/mnt/archive/agric_practices_files/{site}/{year}/ts_input_tables/{practice}', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.ts_input_tables_upload_root_dir',  NULL, '/mnt/archive/upload/agric_practices_files/{site}/ts_input_tables', '2019-10-18 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4c.use_prev_prd', NULL, '1', '2019-10-18 15:27:41.861613+02');

-- -----------------------------------------------------------
-- S4C_MDB1 Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_mdb1.keep_job_folders', NULL, '0', '2016-10-18 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_mdb1.slurm_qos', NULL, 'qoss4cmdb1', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_mdb1', NULL, '/mnt/archive/orchestrator_temp/s4c_mdb1/{job_id}/{task_id}-{module}', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.amp_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.cohe_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.data_extr_dir', NULL, '/mnt/archive/marker_database_files/mdb1/{site}/{year}/data_extraction/', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.fapar_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.fcover_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.ndwi_enabled', NULL, 'false', '2023-03-02 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.brightness_enabled', NULL, 'false', '2023-03-02 17:31:06.01191+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_l2a', NULL, 'N/A', '2024-03-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_amp', NULL, 'N/A', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_cohe', NULL, 'N/A', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.input_l3b', NULL, 'N/A', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.lai_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.ndvi_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.valid_pixels_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.invalid_pixels_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.stdev_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.minmax_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.median_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.p25_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.p75_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab02_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab03_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab04_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab05_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab06_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab07_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab08_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab8a_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab11_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.l2ab12_enabled', NULL, 'false', '2021-05-16 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.mdb3_enabled', NULL, 'false', '2021-10-01 17:31:06.01191+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.mdb3_input_tables', NULL, '/mnt/archive/marker_database_files/mdb1/{site}/{year}/input_tables.csv', '2021-05-16 17:31:06.01191+02');

-- -----------------------------------------------------------
-- FMask Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.enabled', NULL, 'false', '2021-02-10 15:58:31.878939+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.extractor_image', NULL, 'sen4x/fmask_extractor:0.1.8', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.gdal_image', NULL, 'osgeo/gdal:ubuntu-full-3.3.1', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.image', NULL, 'sen4x/fmask:4.4-ubuntu-20.04', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.cog-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.compress-tiffs', NULL, '1', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud', NULL, '3', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.cloud-shadow', NULL, '3', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.dilation.snow', NULL, '0', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.max-retries', NULL, '3', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.num-workers', NULL, '2', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.output-path', NULL, '/mnt/archive/fmask_def/{site}/fmask/', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.retry-interval', NULL, '1 minute', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold', NULL, '20', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.l8', NULL, '17.5', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.optical.threshold.s2', NULL, '20', '2021-03-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.working-dir', NULL, '/mnt/archive/fmask_tmp/', '2021-03-18 14:43:00.720811+00');

-- -----------------------------------------------------------
-- S4S Permanent Crops Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4s_perm_crop', NULL, '/mnt/archive/orchestrator_temp/s4s_perm_crop/{job_id}/{task_id}-{module}', '2021-08-23 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_perm_crop.keep_job_folders', NULL, '1', '2021-08-23 16:41:20.194169+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_perm_crop.slurm_qos', NULL, 'qoss4spermcrops', '2021-12-09 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s_perm_crop.use_docker',  NULL, '1',	'2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s_perm_crop.docker_image',  NULL, 'sen4x/otb:7.2.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-samples-rasterization.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-samples-rasterization',  NULL, 's4s-perm-crops-rasterization.py', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-run-broceliande.use_docker',  NULL, '0', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-run-broceliande',  NULL, 's4s-perm-crops-run-broceliande.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_perm_crop.broceliande-docker-image',  NULL, 'registry.gitlab.inria.fr/obelix/broceliande/develop:2.7.20210608', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-extract-inputs',  NULL, 's4s-perm-crops-extract-inputs.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-extract-inputs.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-build-refl-stack-tif',  NULL, 's4s-perm-crops-build-refl-stack.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-build-refl-stack-tif.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-sieve.docker_image',  NULL, 'osgeo/gdal:ubuntu-full-3.2.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-sieve',  NULL, '/usr/bin/gdal_sieve.py', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_perm_crop.vec_field',  NULL, 'code_n1', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-extract-parcels',  NULL, 'extract_yield_parcels.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-extract-parcels.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crop-post-processing',  NULL, 's4s-perm-crops-post-processing.py', '2024-01-11 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crop-post-processing.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2024-01-11 14:43:00.720811+00');


-- -----------------------------------------------------------
-- S4S Yield Features Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4s_yield_feat', NULL, '/mnt/archive/orchestrator_temp/s4s_yield_feat/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_yield_feat.keep_job_folders', NULL, '1', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_yield_feat.slurm_qos', NULL, 'qoss4syield', '2021-12-09 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-savitzky-golay.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-extract-weather-features.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-safy-lut.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-safy-optim.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-features-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-parcels-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-reference-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-model.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-crop-types-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-savitzky-golay',  NULL, 'run_savitzky_golay.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-extract-weather-features',  NULL, 'extract_weather_features.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-safy-lut',  NULL, 'run_safy_lut.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-safy-optim',  NULL, 'run_safy_optim.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-features-extraction',  NULL, 'extract_yield_features.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-parcels-extraction',  NULL, 'extract_yield_parcels.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-reference-extraction',  NULL, 'extract_yield_reference.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-model',  NULL, 'S4S_Yield_Model.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-crop-types-extraction',  NULL, 'extract_crop_codes.py', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.algorithm',  NULL, 'rf', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.selection-type',  NULL, 'automatic', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.max-automatic-features-no',  NULL, '44', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.manual-selection-features',  NULL, '', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.safy_params_upload_dir',  NULL, '/mnt/archive/s4s_yield_upload/safy_params', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.safy_params_path',  NULL, '/mnt/archive/s4s_yield/{site}/{year}/SAFY_Config/safy_params.json', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_yield_safy_import',  NULL, 's4s-yield-safy-params-import.py', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02');

-- -----------------------------------------------------------
-- S4S Yield SU Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4s_yield_su', NULL, '/mnt/archive/orchestrator_temp/s4s_yield_su/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_yield_su.keep_job_folders', NULL, '1', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_yield_su.slurm_qos', NULL, 'qoss4syield', '2021-12-09 11:09:43.978921+02');

-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s_yield_su.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-esu-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-esu-aggregate.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-savitzky-golay-wrp.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-features-extraction-wrp.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-trend-features-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-su-merge-yearly-features.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-su-model-wrp.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-esu-extraction',  NULL, 's4s_yieldsu_esu_extraction.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-esu-aggregate',  NULL, 's4s_yieldsu_esu_ts_aggregation.py', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-savitzky-golay-wrp',  NULL, 'run_savitzky_golay_su_wrp.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-merge-all-features-wrp',  NULL, 's4s_yieldsu_all_markers_merge_wrp.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-features-extraction-wrp',  NULL, 's4s_yieldsu_extract_yield_features.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-trend-features-extraction',  NULL, 's4s_yieldsu_trend_computation.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-su-merge-yearly-features',  NULL, 's4s_yieldsu_merge_features.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-su-model-wrp',  NULL, 's4s_yieldsu_model.py', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.algorithm',  NULL, 'rf', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.selection-type',  NULL, 'automatic', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.max-automatic-features-no',  NULL, '44', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.manual-selection-features',  NULL, '', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.su_path',  NULL, '/mnt/archive/s4s_yield/{site}/yield_su/', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.data_extr_dir',  NULL, '/mnt/archive/marker_database_files/yield_su/mdb1/{site}/{year}/data_extraction/', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.historical_data_upload_dir',  NULL, '/mnt/archive/s4s_yield_upload/su_historical_data', '2023-09-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.historical_data_path',  NULL, '/mnt/archive/s4s_yield/{site}/yield_su/HistoricalData/SU_yield_historical_data.csv', '2023-09-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_yield_su_historical_data_import',  NULL, 's4s-yield-su-historical-data-import.py', '2023-09-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02');

-- -----------------------------------------------------------
-- ERA5 Weather Download Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.era5_weather.enabled',  NULL, 'false', '2021-02-19 14:43:00.720811+00');

-- -----------------------------------------------------------
-- S4S Parcels Import Processor
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_parcels_import',  NULL, 'data-preparation-s4s.py', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_admin_units_import',  NULL, '/usr/bin/s4s-admin-units-import.py', '2021-02-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.regions_upload_dir',  NULL, '/mnt/archive/s4s_parcels_upload/regions', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.provinces_upload_dir',  NULL, '/mnt/archive/s4s_parcels_upload/provinces', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.municipalities_upload_dir',  NULL, '/mnt/archive/s4s_parcels_upload/municipalities', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.segments_upload_dir',  NULL, '/mnt/archive/s4s_parcels_upload/segments', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.parcels_upload_dir',  NULL, '/mnt/archive/s4s_parcels_upload/parcels', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.parcel_stats_upload_dir',  NULL, '/mnt/archive/s4s_parcels_upload/parcel_statistics', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_parcels.working_dir',  NULL, '/mnt/archive/s4s_parcels_import_tmp', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.parcel_id_col_name', NULL, 'parcel_id', '2021-08-23 18:43:00.720811+00');

-- -----------------------------------------------------------
-- T-Rex Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.trex-updater', NULL, 't-rex-genconfig.py', '2021-10-11 22:39:08.407059+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.trex.slurm_qos', NULL, 'qostrex', '2021-10-11 17:44:38.29255+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.trex-updater.use_docker', NULL, '1', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.trex-updater.docker_image', NULL, 'sen4cap/data-preparation:0.1', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.trex-updater.docker_add_mounts', NULL, '/var/run/docker.sock:/var/run/docker.sock,/var/lib/t-rex:/var/lib/t-rex', '2021-02-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.trex.t-rex-container', NULL, 'docker_t-rex_1', '2021-10-11 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.trex.t-rex-output-file', NULL, '/var/lib/t-rex/t-rex.toml', '2021-10-11 11:09:43.978921+02');

-- -----------------------------------------------------------
-- L3 S1 Composite Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l3_s1_comp', NULL, '/mnt/archive/orchestrator_temp/l3_s1_comp/{job_id}/{task_id}-{module}', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3_s1_comp.keep_job_folders', NULL, '0', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3_s1_comp.slurm_qos', NULL, 'qosl3gencomp', '2021-12-09 11:09:43.978921+02');

-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.amp_enabled', NULL, 'true', '2021-12-09 11:09:43.978921+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.cohe_enabled', NULL, 'true', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.polarisations', NULL, 'VVVH', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.period', NULL, '30', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.method', NULL, 'mean', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.extract_valid_pixels_cnt', NULL, 'false', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.input_amp', NULL, 'N/A', '2019-02-18 15:28:22.404745+02');
-- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_s1_comp.input_cohe', NULL, 'N/A', '2019-02-18 15:28:41.060339+02');

-- -----------------------------------------------------------
-- L3 Biophysical Indicators Composite Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.l3_ind_comp', NULL, '/mnt/archive/orchestrator_temp/l3_ind_comp/{job_id}/{task_id}-{module}', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3_ind_comp.keep_job_folders', NULL, '0', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.l3_ind_comp.slurm_qos', NULL, 'qosl3gencomp', '2021-12-09 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.ndvi_enabled', NULL, 'true', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.lai_enabled', NULL, 'true', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.fapar_enabled', NULL, 'true', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.fcover_enabled', NULL, 'true', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.period', NULL, '30', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.method', NULL, 'mean', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.extract_valid_pixels_cnt', NULL, 'false', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3_ind_comp.input_l3b', NULL, 'N/A', '2019-02-18 15:27:41.861613+02');

-- -----------------------------------------------------------
-- Sen4Stat CropMapping processor
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4s_crop_mapping', NULL, '/mnt/archive/orchestrator_temp/s4s_crop_mapping/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_crop_mapping.keep_job_folders', NULL, '1', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_crop_mapping.slurm_qos', NULL, 'qoss4scropmap', '2021-12-09 11:09:43.978921+02');

insert into config(key, site_id, value, last_updated) values ('processor.insitu.path', null, '/mnt/archive/insitu/{site}/{year}', '2021-08-26 20:29:59.7325+03');
insert into config(key, site_id, value, last_updated) values ('executor.module.path.s4s-crop-type-mapping',  NULL, 'crop-map-s4s.py', '2021-01-18 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02');

insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.pix-min', null, '1', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.pix-best', null, '1', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.pix-ratio-min', null, '0.0002', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.poly-min', null, '1', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.pix-ratio-hi', null, '0.05', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.pix-ratio-lo', null, '0.01', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.monitored-land-covers', null, '{1,2,3,4,5,6,7,8,9}', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.monitored-crops', null, '', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.monitored-crops-remapped-pre', null, '', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.excluded-crops-remapped-pre', null, '', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.smote-ratio', null, '0.0075', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.sample-ratio-hi', null, '0.25', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.sample-ratio-lo', null, '0.75', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.rf.max-depth', null, '25', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.rf.min-samples', null, '25', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.rf.num-trees', null, '100', '2021-08-30 13:56:07.537023+03');
insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.crop_remapping_set_id', null, '-1', '2021-08-30 13:56:07.537023+03');

insert into config(key, site_id, value, last_updated) values ('processor.s4s_crop_mapping.features-filter', null, '', '2021-08-30 13:56:07.537023+03');

-- -----------------------------------------------------------
-- Zarr converter Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.zarr', NULL, '/mnt/archive/orchestrator_temp/zarr/{job_id}/{task_id}-{module}', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.zarr.keep_job_folders', NULL, '0', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.zarr.slurm_qos', NULL, 'qoszarr', '2021-12-09 11:09:43.978921+02');


INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.zarr.enabled', NULL, 'false', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.zarr.input_l3b', NULL, 'N/A', '2019-02-18 15:27:41.861613+02');

-- -----------------------------------------------------------
-- Parcels Heterogeneity Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_heterog', NULL, '/mnt/archive/orchestrator_temp/s4c_heterog/{job_id}/{task_id}-{module}', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_heterog.keep_job_folders', NULL, '0', '2021-12-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_heterog.slurm_qos', NULL, 'qoss4cheterog', '2021-12-09 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-preparation-s1.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-preparation-s2.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-analysis-s2.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-analysis-s1.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-cluster-tiles-analysis-merge.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-heterog-period-analysis.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-heterog-extract-s1-list.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-temporal-resampling.docker_image',  NULL, 'sen4x/processors-new:0.1.0', '2023-08-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-heterog-crop-type',  NULL, 'heterog_crop_type_wrapper.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-preparation-s1',  NULL, 'cluster_preparation_s1.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-preparation-s2',  NULL, 'cluster_preparation_s2.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-analysis-s1',  NULL, 'cluster_analysis_s1.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-analysis-s2',  NULL, 'cluster_analysis_s2.py', '2021-01-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-heterog-extract-s1-list', NULL, 'heterog_s1_rasters_extractor.py', '2022-04-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-cluster-tiles-analysis-merge', NULL, 'heterog_tiles_merge.py', '2022-04-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-heterog-period-analysis', NULL, 'heterog_period_analysis.py', '2022-04-18 14:25:14.193131+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.start_date', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.end_date', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.clustering_period', NULL, 30, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.temporal_resampling_max_dist', NULL, 30, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.temporal_resampling_windows_radius', NULL, 15, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.mask_value', NULL, 0, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.nan_value', NULL, -10000, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s1_temporal_resampling_interval', NULL, 7, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s2_temporal_resampling_interval', NULL, 10, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s1_clusters_number', NULL, 4, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s2_clusters_number', NULL, 4, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.isolated_pixels_thr', NULL, 6, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.isolated_pixels_smoothing_radius', NULL, 1, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.search_radius_s1', NULL, 2, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.search_radius_s2', NULL, 1, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.full_connectivity', NULL, false, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s1_min_cluster_pixels', NULL, 20, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.s2_min_cluster_pixels', NULL, 20, '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.ndvi_clust_dist_thr', NULL, '0.17', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_heterog.percentage_hererogeneity', NULL, '0.9', '2023-03-31 11:09:43.978921+02');


-- -----------------------------------------------------------
-- S4C Bare Soil Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_bare_soil', NULL, '/mnt/archive/orchestrator_temp/s4c_bare_soil/{job_id}/{task_id}-{module}', '2023-10-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_bare_soil.keep_job_folders', NULL, '0', '2023-10-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_bare_soil.slurm_qos', NULL, 'qoss4cbaresoil', '2023-10-09 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s2-calibration.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s1-calibration.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s2-model.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-s1-model.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-bare-soil-markers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s2-calibration',  NULL, 's4c_bs_calibration_s2.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s1-calibration',  NULL, 's4c_bs_calibration_s1.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s2-model',  NULL, 's4c_bs_model_s2.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-s1-model',  NULL, 's4c_bs_model_s1.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-bare-soil-markers', NULL, 's4c_bs_markers.py', '2023-09-18 14:25:14.193131+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.start_date', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.end_date', NULL, '', '2023-03-31 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_bs_ndvi_thr', NULL, 0.15, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_ndvi_thr', NULL, 0.45, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_bs_ndwi_thr', NULL, 0., '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_ndwi_thr', NULL, 0.3, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_bs_ndti_thr', NULL, 0.1, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_ndti_thr', NULL, 0.25, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.calib_nbs_fcover_thr', NULL, 0.01, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.model_estimator_no', NULL, 30, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_long_period', NULL, 60, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_short_period', NULL, 30, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_s2_periods_no', NULL, 3, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_s1_periods_no', NULL, 4, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_bs_s2_threshold', NULL, 0.75, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_nbs_s2_threshold', NULL, 0.8, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_bs_s1_threshold', NULL, 0.65, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_bare_soil.markers_nbs_s1_threshold', NULL, 0.7, '2023-10-03 11:09:43.978921+02');


-- -----------------------------------------------------------
-- S4C Change Detection Specific Keys
-- -----------------------------------------------------------
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4c_change_detection', NULL, '/mnt/archive/orchestrator_temp/s4c_change_detection/{job_id}/{task_id}-{module}', '2023-10-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_change_detection.keep_job_folders', NULL, '0', '2023-10-09 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4c_change_detection.slurm_qos', NULL, 'qoss4cchangedet', '2023-10-09 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-extract-common-parcels.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-filter-lpis-cols.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-lai-outliers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-veg-growth-markers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-bs-markers.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-computation.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4c-change-detection-consolidation.docker_image',  NULL, 'sen4cap/processors-scripts:3.3.0', '2023-08-19 14:43:00.720811+00');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-extract-common-parcels', NULL, 'match_sites_parcels.py', '2023-09-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-filter-lpis-cols',  NULL, 'filter_csv_by_cols.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-lai-outliers',  NULL, 'lai_outliers_computation.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-veg-growth-markers',  NULL, 'veg_growth_markers_extraction.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-bs-markers',  NULL, 'bs_markers_extraction.py', '2023-09-18 14:43:00.720811+00');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-computation', NULL, 's4c_change_detection.py', '2023-09-18 14:25:14.193131+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4c-change-detection-consolidation', NULL, 's4c_change_detection_consolidation.py', '2023-09-18 14:25:14.193131+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.start_date', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.end_date', NULL, '', '2023-03-31 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_site_id', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_start_date', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_end_date', NULL, '', '2023-03-31 11:09:43.978921+02');

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_mdb1_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_bs_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.mdb1_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.bs_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.sites_ids_mapping', NULL, '', '2023-03-31 11:09:43.978921+02');

-- Reference period Grassland changes
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ttdayss2_incr', NULL, 2, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_thr', NULL, 50, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_min_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_ratiostab_max_incr', NULL, 1.5, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_grassland_consecstab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
-- Reference period Permanent crops changes
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ttdayss2_incr', NULL, 3, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_thr', NULL, 50, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_areaveg_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_thr', NULL, 20, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_permcrops_ratiostab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
-- Reference period Arable land changes
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.ref_arableland_ttdayss2_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');

-- Current year Grassland changes
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ttdayss2_incr', NULL, 2, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_thr', NULL, 25, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_min_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_ratiostab_max_incr', NULL, 1.5, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_consecstab_thr', NULL, 1, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.grassland_consecstab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
-- Current year Permanent crops changes
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ttdayss2_incr', NULL, 3, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_areaveg_thr', NULL, 25, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_areaveg_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ratiostab_thr', NULL, 20, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.permcrops_ratiostab_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
-- Current year Arable land changes
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.arableland_ttdayss2_thr', NULL, 0, '2023-10-03 11:09:43.978921+02');
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_change_detection.arableland_ttdayss2_incr', NULL, 1, '2023-10-03 11:09:43.978921+02');
