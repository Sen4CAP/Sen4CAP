-- Files descriptors

-- Declarations
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
