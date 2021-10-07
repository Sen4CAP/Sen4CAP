
-- Declarations operations
INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async)
            VALUES 	(1, 1, 1, 'Upload', '{executor.module.path.lpis_list_columns}', 8, '{"parameters": [{"name": "file", "command": "-p", "type": "java.io.File", "required": true, "refFileId": 1}]}', '{"columns":"string[]"}', false);
INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async)
            VALUES 	(2, 1, 2, 'Import', '{executor.module.path.lpis_import}', 8, '{"parameters": [{"name": "lpisFile", "command": "--lpis", "type": "java.io.File", "required": true, "refFileId": 1},{"name": "lutFile", "command": "--lut", "type": "java.io.File", "required": false, "refFileId": 2},{"name": "parcelColumns","command": "--parcel-id-cols","label": "Parcel Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true},{"name": "holdingColumns","command": "--holding-id-cols","label": "Holding Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true},{"name": "cropCodeColumn","command": "--crop-code-col","label": "Crop Code Column","type": "java.lang.String","valueSet":["$columns"],"required": true},{"name": "mode","command":"--mode","label": "Import Mode","type": "java.lang.String","valueSet":["update","replace","incremental"],"defaultValue":"update","required": true},{"name":"siteId","command":"--site_id","label":null,"type":"java.lang.Integer","required":"true"},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}]}', null, true);


-- L4B Config operations
INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type)
            VALUES 	(3, 3, 1, 'Upload', '{executor.module.path.l4b_cfg_import}', 10, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 3},{"name": "mowingStartDate","label": "Mowing Start Date","type": "java.util.Date","required": false},{"name": "year","label": "Year","type": "java.lang.Integer","required": false}]}', null);

-- L4C config operations
INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type)
            VALUES 	(4, 4, 1, 'Upload', '{executor.module.path.l4c_cfg_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 4},{"name": "practices","label": "Practices","type": "java.lang.String","required": true},{"name": "year","label": "Year","type": "java.lang.Integer","required": false}, {"name": "country","label": "Country","type": "java.lang.String","required": true}]}', null);

INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type)
            VALUES 	(5, 5, 1, 'Upload', '{executor.module.path.l4c_practices_import}', 11, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 4},{"name": "practices","label": "Practices","type": "java.lang.String","required": true},{"name": "year","label": "Year","type": "java.lang.Integer","required": false}, {"name": "country","label": "Country","type": "java.lang.String","required": true}]}', null);


-- COSMIN's PREV
-- INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async)
-- 	VALUES 	(1, 1, 1, 'Upload', '{executor.module.path.lpis_list_columns}', 8, '{"parameters": [{"name": "file", "command": "-p", "type": "java.io.File", "required": true, "refFileId": 1}]}', '{"columns":"string[]"}', false),
-- 			(2, 1, 2, 'Import', '{executor.module.path.lpis_import}', 8, '{"parameters": [{"name": "lpisFile", "command": "--lpis", "type": "java.io.File", "required": true, "refFileId": 1},{"name": "lutFile", "command": "--lut", "type": "java.io.File", "required": false, "refFileId": 2},{"name": "parcelColumns","command": "--parcel-id-cols","label": "Parcel Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true},{"name": "holdingColumns","command": "--holding-id-cols","label": "Holding Columns","type": "[Ljava.lang.String;","valueSet":["$columns"],"required": true},{"name": "cropCodeColumn","command": "--crop-code-col","label": "Crop Code Column","type": "java.lang.String","valueSet":["$columns"],"required": true},{"name": "mode","command":"--mode","label": "Import Mode","type": "java.lang.String","valueSet":["update","replace","incremental"],"defaultValue":"update","required": true},{"name":"siteId","command":"--site_id","label":null,"type":"java.lang.Integer","required":"true"},{"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}]}', null, true);
--             
-- INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type)
-- 	VALUES 	(3, 3, 1, 'Upload', '{executor.module.path.l4b_cfg_import}', 5, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 3},{"name": "mowingStartDate","label": "Mowing Start Date","type": "java.util.Date","required": false},{"name": "year","label": "Year","type": "java.lang.Integer","required": false}]}', null);
-- 
            
            
-- -- L4B config parameters
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (2,1,'Upload', null, null,10 , null) ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (2,2,'Import', null,'{executor.module.path.l4b_cfg_import}',10,
--             '{"parameters": [
--                 {"name": "mowingStartDate","label": "Mowing Start Date","type": "java.util.Date","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null},
--                 {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
--              }') ON conflict DO nothing;
-- 
-- -- L4C config parameters
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (3,1,'Upload', null, null,11 , null) ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (3,2,'Import', null,'{executor.module.path.l4c_cfg_import}',11,
--             '{"parameters": [
--                 {"name": "practices","label": "Practices","type": "[Ljava.lang.String;","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null},
--                 {"name": "country","label": "Country","type": "[Ljava.lang.String;","required": true,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}, 
--                 {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
--              }') ON conflict DO nothing;
--              
-- -- L4C practices parameters
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (4,1,'Upload', null, null,11 , null) ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (4,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
--             '{"parameters": [
--                 {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
--              }') ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (5,1,'Upload', null, null,11 , null) ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (5,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
--             '{"parameters": [
--                 {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
--              }') ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (6,1,'Upload', null, null,11 , null) ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (6,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
--             '{"parameters": [
--                 {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
--              }') ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (7,1,'Upload', null, null,11 , null) ON conflict DO nothing;
-- INSERT INTO auxdata_operation (auxdata_descriptor_id,operation_order,name,output_type,handler_path,processor_id,parameters) VALUES (7,2,'Import', null,'{executor.module.path.l4c_practices_import}',11,
--             '{"parameters": [
--                 {"name": "year","label": "Year","type": "java.lang.Integer","required": false,"defaultValue": null,"valueSet": null,"valueSetRef": null,"value": null}]
--              }') ON conflict DO nothing;
-- 