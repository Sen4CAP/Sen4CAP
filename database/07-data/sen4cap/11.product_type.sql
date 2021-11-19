INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(1, 'l2a','L2A Atmospheric correction', true);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(3, 'l3b','L3B product', true);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(7, 'l1c','L1C product', true);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(10, 's1_l2a_amp','Sentinel 1 L2 Amplitude product', true);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(11, 's1_l2a_cohe','Sentinel 1 L2 Coherence product', true);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(12, 's4c_l4a','Sen4CAP L4A Crop type product', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(13, 's4c_l4b','Sen4CAP L4B Grassland Mowing product', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(14, 'lpis', 'LPIS product', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(15, 's4c_l4c','Sen4CAP L4C Agricultural Practices product', false);

-- TODO: Not yet implemented
-- INSERT INTO product_type
-- (id, name, description)
-- VALUES
-- (16, 's4c_l3c','Sen4CAP L3C LAI Reprocessed product');

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(17, 's4c_mdb1','Sen4CAP Marker Database Basic StdDev/Mean', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(18, 's4c_mdb2','Sen4CAP Marker Database AMP VV/VH Ratio', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(19, 's4c_mdb3','Sen4CAP Marker Database L4C M1-M5', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(20, 's4c_mdb_l4a_opt_main','Sen4CAP L4A Optical Main Features', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(21, 's4c_mdb_l4a_opt_re','Sen4CAP L4A Optical Red-Edge Features', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(22, 's4c_mdb_l4a_sar_main','Sen4CAP L4A SAR Main Features', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(23, 's4c_mdb_l4a_sar_temp','Sen4CAP L4A SAR Temporal Features', false);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(25, 'fmask','Fmask mask product', true);

INSERT INTO product_type
(id, name, description, is_raster)
VALUES
(26, 'l2a_msk','L2A product with validity mask', true);


