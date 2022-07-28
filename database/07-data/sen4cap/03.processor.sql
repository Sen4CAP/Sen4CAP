INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true, '{1,2}', null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false, '{1,2}', null, true, true, false, false, 'Vegetation Status Indicators: informs about the evolution of the green vegetation corresponding to the crop vegetative development');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(7, 'L2-S1 Pre-Processor', 'l2s1', 'L2 S1 &mdash; SAR Pre-Processor', true, '{3}', '{3}', false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(8, 'LPIS/GSAA', 'lpis', 'LPIS / GSAA Processor', true, null, null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(9, 'S4C L4A Crop Type','s4c_l4a', 'Sen4CAP L4A &mdash; Crop Type', false, '{1,2,3}', null, true, true, true, false, 'Parcel Level Crop Type: a subset of the parcels from the declaration dataset is used to train the Random Forest model which is then applied to the whole declaration dataset');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(10, 'S4C L4B Grassland Mowing','s4c_l4b', 'Sen4CAP L4B &mdash; Grassland Mowing', false, '{1,2,3}', null, true, false, true, true);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(11, 'S4C L4C Agricultural Practices','s4c_l4c', 'Sen4CAP L4C &mdash; Agricultural Practices', false, '{1,2,3}', '{1,3}', true, false, true, true, 'Agricultural practices: developed methodology relies on the analysis of dense temporal profiles. The generation of temporal profiles is based on optical (S2 and L8) and Synthetic Aperture Radar (SAR - S1) imagery. NDVI is used as the optical-based signal, at a spatial resolution of 10 m. The SAR-based signals include backscatter temporal profiles (ascending and descending orbits for dual VV and VH polarization) and coherence temporal profiles (for VV polarization) at 20 m spatial resolution');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(14, 'S4C Marker Database PR1','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1', false, '{1,2,3}', null, true, false, true, false, 'Markers database: a set of basic markers extracted at parcel level (mean and standard deviation for coherence, amplitude and biophysical indicators) used for deriving new user products');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(15, 'Validity flags','l2a_msk', 'Validity flags', true, '{1,2}', null, false, false, false, false);

INSERT INTO processor 
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) 
VALUES 
(21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true, null, null, false, false, false, false);

