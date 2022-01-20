INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(2, 'S2A L3A Composite','l3a', 'Sen2Agri L3A &mdash; Cloud-free Composite', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(4, 'S2A L3E Pheno NDVI metrics','l3e', 'Sen2Agri L3E &mdash; Phenology Indices', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(5, 'S2A L4A Crop Mask','l4a', 'Sen2Agri L4A &mdash; Cropland Mask', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(6, 'S2A L4B Crop Type','l4b', 'Sen2Agri L4B &mdash; Crop Type Map', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(12, 'S2A L3C LAI N-Days Reprocessing','s2a_l3c', 'S2A L3C &mdash; LAI N-Days Reprocessing', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(13, 'S2A L3D LAI Fitted Reprocessing','s2a_l3d', 'S2A L3d &mdash; LAI Fitted Reprocessing', false);

INSERT INTO processor 
(id, name, short_name, label, required)
VALUES 
(15, 'Validity flags','l2a_msk', 'Validity flags', false);

INSERT INTO processor 
(id, name, short_name, label, required) 
VALUES 
(21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true);

