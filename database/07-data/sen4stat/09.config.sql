INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.use.esa.l2a', NULL, 'true', '2020-05-18 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.fmask.enabled', NULL, 'true', '2020-05-18 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.enabled', NULL, 'true', '2020-05-18 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('s1.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.tiled.tiff', NULL, true, '2022-09-30 10:31:00.501+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcel_id_col_name', NULL, 'parcel_id', '2019-10-11 16:15:00.0+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'parcel_id';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_optical_file_name_pattern', NULL, 'in_?situ_.*_buf_10m.shp', '2019-10-11 16:15:00.0+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'in_?situ_.*_buf_10m.shp';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_sar_file_name_pattern', NULL, 'in_?situ_.*_(\d{4,5})_buf_10m.shp', '2019-10-11 16:15:00.0+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'in_?situ_.*_(\d{4,5})_buf_10m.shp';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.amp_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.cohe_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.ndvi_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.fapar_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.fcover_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.amp_vvvh_enabled', NULL, 'false', '2020-12-16 17:31:06.01191+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.water_is_valid', NULL, 'true', '2023-07-28 17:54:17.288095+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.snow_is_valid', NULL, 'true', '2023-07-28 17:54:17.288095+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.era5_weather.enabled',  NULL, 'true', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.start.offset', NULL, '0', '2016-07-20 20:05:00')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.docker_script_unit_image', NULL, 'sen4cap/data-preparation:0.3', '2023-11-16 20:05:00')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/data-preparation:0.3';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.version', NULL, '2', '2023-11-16 20:05:00')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '2';

