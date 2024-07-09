-- Sen4CAP descriptors
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (1, 'declaration', 'Declarations', 'year') ON conflict(id) DO UPDATE SET name = 'declaration', label = 'Declarations', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (2, 'l4b_config', 'L4B Configuration', 'year') ON conflict(id) DO UPDATE SET name = 'l4b_config', label = 'L4B Configuration', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (3, 'l4c_config', 'L4C Configuration', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_config', label = 'L4C Configuration', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (4, 'l4c_cc_info', 'L4C CC practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_cc_info', label = 'L4C CC practices infos', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (5, 'l4c_fl_info', 'L4C FL practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_fl_info', label = 'L4C FL practices infos', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (6, 'l4c_nfc_info', 'L4C NFC practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_nfc_info', label = 'L4C NFC practices infos', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (7, 'l4c_na_info', 'L4C NA practices infos', 'year') ON conflict(id) DO UPDATE SET name = 'l4c_na_info', label = 'L4C NA practices infos', unique_by = 'year';

-- Sen4Stat descriptors
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (8, 'parcels', 'Parcels', 'year') ON conflict(id) DO UPDATE SET name = 'parcels', label = 'Parcels', unique_by = 'year';
-- INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (9, 'admin_units', 'Admin Units', 'year') ON conflict(id) DO UPDATE SET name = 'admin_units', label = 'Admin Units', unique_by = 'year';
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (10, 'safy_params', 'SAFY params', 'year') ON conflict(id) DO UPDATE SET name = 'safy_params', label = 'SAFY params', unique_by = 'year';

-- Sen2Agri descriptors
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (11, 'insitu', 'Insitu data', 'season') ON conflict(id) DO UPDATE SET name = 'insitu', label = 'Insitu data', unique_by = 'season';

-- Sen4Stat Yield SU descriptor
INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (12, 'su_yield_historical_data', 'SU Yield historical data', 'year') ON conflict(id) DO UPDATE SET name = 'su_yield_historical_data', label = 'SU Yield historical data', unique_by = 'year';