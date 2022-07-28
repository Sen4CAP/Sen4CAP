INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true, '{1,2}', null, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(2, 'S2A L3A Composite','l3a', 'Sen2Agri L3A &mdash; Cloud-free Composite', false, '{1,2}', '{1}', true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false, '{1,2}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(4, 'S2A L3E Pheno NDVI metrics','l3e', 'Sen2Agri L3E &mdash; Phenology Indices', false, '{1,2}', '{1}', true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(5, 'S2A L4A Crop Mask','l4a', 'Sen2Agri L4A &mdash; Cropland Mask', false, '{1,2}', '{1}', true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(6, 'S2A L4B Crop Type','l4b', 'Sen2Agri L4B &mdash; Crop Type Map', false, '{1,2}', '{1}', true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(7, 'L2-S1 Pre-Processor', 'l2-s1', 'L2 S1 &mdash; SAR Pre-Processor', true, '{3}', '{3}', true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(8, 'LPIS/GSAA', 'lpis', 'LPIS / GSAA Processor', true, null, null, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(9, 'S4C L4A Crop Type','s4c_l4a', 'Sen4CAP L4A &mdash; Crop Type', false, '{1,2,3}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(10, 'S4C L4B Grassland Mowing','s4c_l4b', 'Sen4CAP L4B &mdash; Grassland Mowing', false, '{1,2,3}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(11, 'S4C L4C Agricultural Practices','s4c_l4c', 'Sen4CAP L4C &mdash; Agricultural Practices', false, '{1,2,3}', '{1,2}', true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(12, 'S2A L3C LAI N-Days Reprocessing','s2a_l3c', 'S2A L3C &mdash; LAI N-Days Reprocessing', false, '{1,2}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(13, 'S2A L3D LAI Fitted Reprocessing','s2a_l3d', 'S2A L3d &mdash; LAI Fitted Reprocessing', false, '{1,2}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(14, 'S4C Marker Database PR1','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1', false, '{1,2,3}', null, true, false);

INSERT INTO processor 
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES 
(15, 'Validity flags','l2a_msk', 'Validity flags', true, '{1,2}', null, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(16, 'S4S Permanent Crop','s4s_perm_crop', 'S4S Permanent crop', false, '{1,2}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(17, 'S4S Yield Features','s4s_yield_feat', 'S4S Yield Features', false, '{1,2}', null, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES
(18, 'Era5 weather','era5_weather', 'Era5 weather', true, null, null, false, false);

INSERT INTO processor 
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible) 
VALUES 
(20, 'S4S Crop Mapping', 's4s_crop_mapping', 'S4S Crop Mapping', false, '{1,2,3}', null, true, false);

INSERT INTO processor 
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible)
VALUES 
(21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true, null, null, false, false);

INSERT INTO processor 
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible) 
VALUES 
(22, 'L3 S1 Composite', 'l3_s1_comp', 'L3 S1 Composite', false, '{3}', '{3}', true, false);

INSERT INTO processor 
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible) 
VALUES 
(23, 'L3 Indicators Composite', 'l3_ind_comp', 'L3 Indicators Composite', false, '{1,2}', '{1}', true, false);
