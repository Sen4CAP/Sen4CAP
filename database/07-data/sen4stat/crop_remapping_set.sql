COPY public.crop_remapping_set (crop_remapping_set_id, name) FROM stdin;
1	Senegal pre-classification grouping
\.

SELECT pg_catalog.setval('public.crop_remapping_set_crop_remapping_set_id_seq', 1, true);
