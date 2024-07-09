INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.enabled', NULL, 'true', '2020-05-18 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.pixel.spacing', NULL, '20', '2022-09-30 10:31:00.501+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '20';

INSERT INTO config(key, site_id, value, last_updated) VALUES ('s1.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.enabled', NULL, 'true', '2017-10-24 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';

-- disable validity masks for now in Sen4CAP
INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.enabled', NULL, 'false', '2017-10-24 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'false';