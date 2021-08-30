insert into config_metadata values ('processor.insitu.path', 'The path to the pre-processed in-situ data products', 'string', false, 21, false, 'The path to the pre-processed in-situ data products', null);

insert into config_metadata values ('processor.s4s_crop_mapping.pix-min', 'Minimum number of pixels of polygons', 'int', false, 30, true, 'Minimum number of pixels for polygons', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-best', 'Minimum number of pixels of polygons used for training', 'int', false, 30, true, 'Minimum number of pixels of polygons used for training', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-ratio-min', 'Minimum crop to total pixel ratio', 'float', false, 30, true, 'Minimum crop to total pixel ratio', null);
insert into config_metadata values ('processor.s4s_crop_mapping.poly-min', 'Minimum number of polygons for crops', 'int', false, 30, true, 'Minimum number of polygons for crops', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-ratio-hi', 'Minimum crop to total pixel ratio for strategy 1', 'float', false, 30, true, 'Minimum crop to total pixel ratio for strategy 1', null);
insert into config_metadata values ('processor.s4s_crop_mapping.pix-ratio-lo', 'Minimum crop to total pixel ratio for strategy 2', 'float', false, 30, true, 'Minimum crop to total pixel ratio for strategy 2', null);
insert into config_metadata values ('processor.s4s_crop_mapping.monitored-land-covers', 'Land cover class filter', 'string', false, 30, true, 'Land cover class filter', null);
insert into config_metadata values ('processor.s4s_crop_mapping.monitored-crops', 'Crop class filter', 'string', false, 30, true, 'Crop class filter', null);
insert into config_metadata values ('processor.s4s_crop_mapping.smote-ratio', 'Synthetic sample ratio', 'float', false, 30, true, 'Synthetic sample ratio', null);
insert into config_metadata values ('processor.s4s_crop_mapping.sample-ratio-hi', 'Training pixel ratio for strategy 1', 'float', false, 30, true, 'Training pixel ratio for strategy 1', null);
insert into config_metadata values ('processor.s4s_crop_mapping.sample-ratio-lo', 'Training pixel ratio for strategies 2 and 3', 'float', false, 30, true, 'Training pixel ratio for strategies 2 and 3', null);
insert into config_metadata values ('processor.s4s_crop_mapping.rf.max-depth', 'Maximum depth of RF trees', 'int', false, 30, true, 'Maximum depth of RF trees', null);
insert into config_metadata values ('processor.s4s_crop_mapping.rf.min-samples', 'Minimum samples in RF tree nodes', 'int', false, 30, true, 'Minimum samples in RF tree nodes', null);
insert into config_metadata values ('processor.s4s_crop_mapping.rf.num-trees', 'Number of RF trees', 'int', false, 30, true, 'Number of RF trees', null);
