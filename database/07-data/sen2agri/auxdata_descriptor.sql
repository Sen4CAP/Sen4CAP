INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (11, 'insitu', 'Insitu data', 'season') ON conflict(id) DO UPDATE SET name = 'insitu', label = 'Insitu data', unique_by = 'season';
