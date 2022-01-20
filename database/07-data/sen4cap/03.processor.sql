INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(1, 'L2A Atmospheric Corrections','l2a', 'L2A &mdash; Atmospheric Corrections', true);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(3, 'L3B Vegetation Status','l3b', 'L3B &mdash; LAI/FAPAR/FCOVER/NDVI', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(7, 'L2-S1 Pre-Processor', 'l2-s1', 'L2 S1 &mdash; SAR Pre-Processor', true);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(8, 'LPIS/GSAA', 'lpis', 'LPIS / GSAA Processor', true);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(9, 'S4C L4A Crop Type','s4c_l4a', 'Sen4CAP L4A &mdash; Crop Type', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(10, 'S4C L4B Grassland Mowing','s4c_l4b', 'Sen4CAP L4B &mdash; Grassland Mowing', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(11, 'S4C L4C Agricultural Practices','s4c_l4c', 'Sen4CAP L4C &mdash; Agricultural Practices', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(14, 'S4C Marker Database PR1','s4c_mdb1', 'MD_PR1 &mdash; Marker Database PR1', false);

INSERT INTO processor
(id, name, short_name, label, required)
VALUES
(15, 'Validity flags','l2a_msk', 'Validity flags', true);

INSERT INTO processor 
(id, name, short_name, label, required) 
VALUES 
(21, 'T-Rex Updater', 't_rex_updater', 'T-Rex Updater', true);
