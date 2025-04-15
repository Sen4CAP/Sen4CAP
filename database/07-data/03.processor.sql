INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true, '{1,2}', null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(2, 'Cloud free Composite','l3a', 'Sen2Agri L3A &mdash; Cloud-free Composite', false, '{1,2}', '{1}', true, false, false, false, 'The Cloud-free Reflectance Composite product provides a cloud-free temporal synthesis of surface reflectance values in the 10 Sentinel-2 bands designed for land observation. It is delivered with several masks that will help appraising its quality.');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(3, 'Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false, '{1,2}', null, true, true, false, false, 'Vegetation Status Indicators: informs about the evolution of the green vegetation corresponding to the crop vegetative development');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(4, 'Pheno NDVI metrics','l3e', 'Sen2Agri L3E &mdash; Phenology Indices', false, '{1,2}', '{1}', true, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(5, 'Crop Mask','l4a', 'Sen2Agri L4A &mdash; Cropland Mask', false, '{1,2}', '{1}', true, false, false, true, 'Dynamic Crop Mask: binary map separating annual cropland areas and other areas, thus corresponding to a mask over annually cultivated area. This binary map is produced along the agricultural season on a monthly basis, to serve for instance as a mask for monitoring crop growing conditions, as basis for sampling stratification and for agricultural extension');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(6, 'Crop Type','l4b', 'Sen2Agri L4B &mdash; Crop Type Map', false, '{1,2}', '{1}', true, true, false, true, 'Crop Type Map: map of the main crop types in a given region, with a minimum mapping unit of 0.01 ha and provided along with several quality flags. The crop types are classified over the cropland area identified in the cropland mask. The map is generated twice over the season, at the middle and at the end of the season');

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
(9, 'Parcel level Crop Type','s4c_l4a', 'Sen4CAP L4A &mdash; Crop Type', false, '{1,2,3}', null, true, true, true, false, 'Parcel Level Crop Type: a subset of the parcels from the declaration dataset is used to train the Random Forest model which is then applied to the whole declaration dataset');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(10, 'Grassland Mowing','s4c_l4b', 'Sen4CAP L4B &mdash; Grassland Mowing', false, '{1,2,3}', null, true, false, true, true, 'Grassland mowing: detects the mowing events with data ranges at parcel-level');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(11, 'Agricultural Practices','s4c_l4c', 'Sen4CAP L4C &mdash; Agricultural Practices', false, '{1,2,3}', '{1,3}', true, false, true, true, 'Agricultural practices: developed methodology relies on the analysis of dense temporal profiles. The generation of temporal profiles is based on optical (S2 and L8) and Synthetic Aperture Radar (SAR - S1) imagery. NDVI is used as the optical-based signal, at a spatial resolution of 10 m. The SAR-based signals include backscatter temporal profiles (ascending and descending orbits for dual VV and VH polarization) and coherence temporal profiles (for VV polarization) at 20 m spatial resolution');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(12, 'S2A L3C LAI N-Days Reprocessing','s2a_l3c', 'S2A L3C &mdash; LAI N-Days Reprocessing', false, '{1,2}', null, true, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(13, 'S2A L3D LAI Fitted Reprocessing','s2a_l3d', 'S2A L3D &mdash; LAI Fitted Reprocessing', false, '{1,2}', null, true, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required, description)
VALUES
(14, 'Markers Database','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1', false, '{1,2,3}', null, true, false, true, false, 'Markers database: a set of basic markers extracted at parcel level (mean and standard deviation for coherence, amplitude and biophysical indicators) used for deriving new user products');

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(15, 'Validity flags','l2a_msk', 'Validity flags', true, '{1,2}', null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(16, 'L4 Permanent Crop','s4s_perm_crop', 'L4 Permanent crop', false, '{1,2}', null, true, false, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(17, 'L3 Yield','s4s_yield_feat', 'L3 Yield', false, '{1,2}', null, true, false, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(18, 'Era5 weather','era5_weather', 'Era5 weather', true, null, null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(19, 'Zarr converter', 'zarr', 'Zarr converter', false, null, null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(20, 'L4 Crop Mapping', 's4s_crop_mapping', 'L4 Crop Mapping', false, '{1,2,3}', null, true, false, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true, null, null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(22, 'L3 S1 Composite', 'l3_s1_comp', 'L3 S1 Composite', false, '{3}', '{3}', true, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(23, 'L3 Indicators Composite', 'l3_ind_comp', 'L3 Indicators Composite', false, '{1,2}', '{1}', true, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(24, 'FMask','fmask', 'FMask', true, '{1,2}', null, false, false, false, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(25, 'Parcel Heterogeneity','s4c_heterog', 'Parcel Heterogeneity', true, '{1,2,3}', null, true, false, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(26, 'Bare soil detection','s4c_bare_soil', 'Bare soil detection', true, '{1,2,3}', '{1}', true, false, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(27, 'L3 Yield SU','s4s_yield_su', 'L3 Yield SU', false, '{1,2}', null, true, false, true, false);

INSERT INTO processor
(id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required)
VALUES
(28, 'L4 Change Detection','s4c_change_detection', 'L4 Change Detection', false, '{1,2,3}', null, true, false, true, false);
