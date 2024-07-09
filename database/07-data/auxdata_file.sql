-- Files descriptors

-- Sen4CAP
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (1, 1, 1, 'LPIS', '{zip}', true);
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (2, 1, 2, 'LUT', '{csv}', false);

-- L4B config 
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (3, 2, 1, 'L4B Cfg', '{cfg}', true);

-- L4C config 
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (4, 3, 1, 'L4C Cfg','{cfg}', false);
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (5, 4, 1, 'CC Practice file','{csv}', false);
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (6, 5, 1, 'FL Practice file','{csv}', false);
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (7, 6, 1, 'NFC Practice file','{csv}', false);
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (8, 7, 1, 'NA Practice file', '{csv}', false);


-- Sen4Stat
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (9, 8, 1, 'Parcel geometries', '{zip}', true);
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (10, 8, 2, 'Parcel statistics', '{csv}', false);

-- Admin Units
-- INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (11, 9, 1, 'Regions', '{zip}', false);
-- INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (12, 9, 2, 'Provinces', '{zip}', false);
-- INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (13, 9, 3, 'Municipalities', '{zip}', false);
-- INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (14, 9, 4, 'Segments', '{zip}', false);

-- SAFY PARAMS
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (15, 10, 1, 'SAFY Params', '{json}', false);


--Sen2Agri
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (16, 11, 1, 'Insitu data', '{zip}', true);

-- SU Yield Historical Data
INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (17, 12, 1, 'SU Yield historical data', '{csv}', false);
