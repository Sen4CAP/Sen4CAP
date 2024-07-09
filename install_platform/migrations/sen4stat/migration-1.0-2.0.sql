begin transaction;

do $migration$
declare _statement text;
begin
    raise notice 'running migrations';

    if exists (select * from information_schema.tables where table_schema = 'public' and table_name = 'meta') then
        if exists (select * from meta where version in ('3.0.0', '3.1.0', '1.1', '2.0')) then
        
            _statement := $str$
                CREATE OR REPLACE FUNCTION sp_is_l2_preprocessing_done
                (
                    _site_id site.id%TYPE,
                    _sat_ids json,
                    _start_date timestamp DEFAULT NULL::timestamp,
                    _end_date timestamp DEFAULT NULL::timestamp
                  )
                  RETURNS boolean AS
                $func$

                declare _has_s1 boolean;
                declare _all_prds_cnt INT:= 0;
                declare _s1_not_processed_cnt INT:= 0;

                BEGIN

                    -- S1 is a little bit special as it keeps status 2 for products with no previous intersection
                    with res(value) as (
                        select 3 = ANY(SELECT value::smallint FROM json_array_elements_text($2))
                    )
                    select value :: boolean into _has_s1 from res;
                    
                    if _has_s1 then
                        with res0(val0) as (
                            -- we need to have some products imported
                            select count(*) from downloader_history where 
                                    (_start_date is null or product_date >= _start_date) and 
                                    (_end_date is null or product_date < _end_date + interval '1 day') and
                                    site_id = _site_id and satellite_id = 3        
                        )
                        select val0 :: int into _all_prds_cnt from res0;
                        if _all_prds_cnt = 0 then
                            raise notice 'No S1 products downloaded so far!';
                            return FALSE;
                        end if;
                        
                        with res1(val1) as (
                            select count(*) from downloader_history where 
                                site_id = _site_id and 
                                satellite_id = 3 and
                                (_start_date is null or product_date >= _start_date) and 
                                (_end_date is null or product_date < _end_date + interval '1 day') and
                                ((status_id = 2 and status_reason not like '%No previous product%') OR
                                    (status_id in (1,7)))  
                        )
                        select val1 :: int into _s1_not_processed_cnt from res1;
                        raise notice 'A number of % S1 products not processed or with errors (except the ones with No previous product)' , _s1_not_processed_cnt;
                        return (_s1_not_processed_cnt = 0);
                    else
                        -- Optical - S2, L8, L9 and others. If at least one satellite is processed, we can proceed.
                        -- Normally, we should check if each satellite is enabled, the downloader is enabled for it 
                        with res0(val0) as (
                            -- we need to have some products imported
                            select count(*) from downloader_history where 
                                    site_id = _site_id and 
                                    (_start_date is null or product_date >= _start_date) and 
                                    (_end_date is null or product_date < _end_date + interval '1 day') and
                                    satellite_id IN (SELECT value::smallint FROM json_array_elements_text($2))
                        )
                        select val0 :: int into _all_prds_cnt from res0;
                        if _all_prds_cnt = 0 then
                            raise notice 'No L1 products downloaded so far!';
                            return FALSE;
                        end if;
                        
                        RETURN 
                        (
                            select count(*) from downloader_history 
                                where 
                                    site_id = _site_id and 
                                    satellite_id IN (SELECT value::smallint FROM json_array_elements_text($2)) and 
                                    (_start_date is null or product_date >= _start_date) and 
                                    (_end_date is null or product_date < _end_date + interval '1 day') and
                                    status_id in (1, 2, 7)              -- downloading, not processed or processing 
                        )  = 0;
                    end if;
                END
                $func$ LANGUAGE plpgsql STABLE;            
            
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$

                create table IF NOT EXISTS crop_remapping_set
                (
                    crop_remapping_set_id int not null primary key generated by default as identity,
                    name text not null
                );
                CREATE TABLE IF NOT EXISTS crop_remapping_set_detail
                (
                    crop_remapping_set_id int not null,
                    original_code int not null references crop_list_n4 (code_n4),
                    remapped_code_pre int,
                    description_pre text,
                    remapped_code_post int,
                    description_post text,
                    primary key (crop_remapping_set_id, original_code)
                );

                INSERT INTO public.crop_remapping_set (crop_remapping_set_id, name) values (1, 'Default grouping') on conflict do nothing;
                SELECT pg_catalog.setval('public.crop_remapping_set_crop_remapping_set_id_seq', 1, true);

                insert into public.crop_list_n1 (code_n1, name) values (0, 'Unknown') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (1, 'Annual cropland') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (2, 'Perennial crops') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (3, 'Grasslands and meadows') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (4, 'Fallows') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (5, 'Shrub land') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (6, 'Forest') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (7, 'Bare soil') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (8, 'Built-up surfaces') ON conflict DO nothing;
                insert into public.crop_list_n1 (code_n1, name) values (9, 'Water bodies') ON conflict DO nothing;

                insert into public.crop_list_n2 (code_n2, name, code_n1) values (0, 'Unknown', 0) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (11, 'Cereals', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (12, 'Vegetables and melons', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (14, 'Oilseed crops', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (15, 'Root/tuber crops with high starch or insulin content', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (16, 'Beverage and spice crops', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (17, 'Leguminous crops', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (18, 'Sugar crops', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (19, 'Other annual crops', 1) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (21, 'Fruits trees', 2) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (22, 'Vineyards', 2) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (23, 'Olive groves', 2) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (24, 'Trees', 2) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (29, 'Other perennial crops', 2) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (31, 'Grassland and meadows', 3) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (41, 'Fallows', 4) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (51, 'Shrub land', 5) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (61, 'Conifers', 6) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (62, 'Deciduous', 6) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (63, 'Conifers and deciduous', 6) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (69, 'Forest', 6) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (71, 'Sparsely vegetated', 7) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (72, 'Bare soils', 7) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (81, 'Urban', 8) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (82, 'Industrial and commercial', 8) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (83, 'Transport', 8) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (84, 'Greenhouses', 8) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (85, 'Other build-up surface', 8) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (91, 'Seas, lagoons and estuaries', 9) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (92, 'Inland waters', 9) ON conflict DO nothing;
                insert into public.crop_list_n2 (code_n2, name, code_n1) values (93, 'Other waters bodies', 9) ON conflict DO nothing;

                insert into public.crop_list_n3 (code_n3, name, code_n2) values (0, 'Unknown', 0) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (111, 'Wheat', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (112, 'Maize', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (113, 'Rice', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (114, 'Sorghum', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (115, 'Barley', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (116, 'Rye', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (117, 'Oats', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (118, 'Millets', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (119, 'Other cereals', 11) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (121, 'Leafy or stem vegetables', 12) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (122, 'Fruit-bearing vegetables', 12) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (123, 'Root, bulb or tuberous vegetables', 12) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (124, 'Mushrooms and truffles', 12) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (129, 'Other vegetables', 12) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (141, 'Soya beans', 14) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (142, 'Groundnuts', 14) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (143, 'Other oilseed crops', 14) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (151, 'Potatoes', 15) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (152, 'Sweet potatoes', 15) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (153, 'Cassava', 15) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (154, 'Yams', 15) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (159, 'Other root/tuber crops', 15) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (161, 'Spice crops', 16) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (162, 'Beverage crops', 16) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (171, 'Beans', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (172, 'Broad beans', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (173, 'Chickpeas', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (174, 'Cow peas', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (175, 'Lentils', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (176, 'Lupins', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (177, 'Peas', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (178, 'Pigeon peas', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (179, 'Other leguminous crops', 17) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (181, 'Sugar beet', 18) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (182, 'Sugar cane', 18) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (183, 'Sweet sorghum', 18) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (189, 'Other sugar crops', 18) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (191, 'Grasses and other fodder crops', 19) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (192, 'Fibre crops', 19) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (193, 'Medicinal, aromatic, pesticidal or similar crops', 19) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (194, 'Flower crops', 19) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (199, 'Other annual crops', 19) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (211, 'No citrus fruits trees', 21) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (212, 'Citrus fruits trees', 21) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (213, 'Other fruits trees', 21) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (221, 'Vineyards', 22) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (231, 'Olive groves', 23) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (241, 'Trees', 24) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (291, 'Succulent plant', 29) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (299, 'Other perennial crops', 29) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (311, 'Grassland', 31) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (319, 'Grassland and meadows', 31) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (411, 'Fallows', 41) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (511, 'Shrub land', 51) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (611, 'Conifers', 61) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (621, 'Deciduous slow growth', 62) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (622, 'Deciduous rapid growth', 62) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (631, 'Conifers and deciduous', 63) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (699, 'Forest', 69) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (711, 'Sparsely vegetated', 71) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (721, 'Bare soils', 72) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (811, 'Urban', 81) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (821, 'Industrial and commercial', 82) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (831, 'Transport', 83) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (841, 'Greenhouses', 84) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (851, 'Other build-up surface', 85) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (911, 'Seas', 91) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (912, 'Lagoons and estuaries', 91) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (921, 'Permanent inland waters', 92) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (922, 'Temporary inland waters', 92) ON conflict DO nothing;
                insert into public.crop_list_n3 (code_n3, name, code_n2) values (931, 'Other waters bodies', 93) ON conflict DO nothing;

                insert into public.crop_list_n4 (code_n4, name, code_n3) values (0, 'Unknown', 0) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1111, 'Winter wheat', 111) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1112, 'Spring wheat', 111) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1113, 'Hard wheat', 111) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1114, 'Soft wheat', 111) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1115, 'Triticale', 111) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1121, 'Maize', 112) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1131, 'Rice', 113) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1141, 'Sorghum', 114) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1151, 'Barley two-row', 115) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1152, 'Barley six-row', 115) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1161, 'Rye', 116) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1171, 'Oats', 117) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1181, 'Millets', 118) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1191, 'Mixed cereals', 119) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1192, 'Other cereals', 119) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1193, 'Quinoa', 119) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1211, 'Artichokes', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1212, 'Asparagus', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1213, 'Cabbages', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1214, 'Cauliflowers & brocoli', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1215, 'Lettuce', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1216, 'Spinach', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1217, 'Chicory', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1218, 'Celery', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1219, 'Other leafy or stem vegetables', 121) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1221, 'Cucumbers', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1222, 'Eggplants (aubergines)', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1223, 'Tomatoes', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1224, 'Watermelons', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1225, 'Cantaloupes and other melons', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1226, 'Pumpkin, squash and gourds', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1227, 'Strawberries', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1229, 'Other fruit-bearing vegetables', 122) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1231, 'Carrots', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1232, 'Turnips', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1233, 'Garlic', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1234, 'Onions (incl. shallots)', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1235, 'Leeks & other alliaceous vegetables', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1236, 'Beetroots', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1239, 'Other root, bulb or tuberous vegetables', 123) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1241, 'Mushrooms', 124) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1291, 'Other vegetables', 129) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1411, 'Soya beans', 141) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1421, 'Groundnuts', 142) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1431, 'Castor bean', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1432, 'Linseed', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1433, 'Mustard', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1434, 'Niger seed', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1435, 'Rapeseed', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1436, 'Safflower', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1437, 'Sesame', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1438, 'Sunflower', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1439, 'Other oilseed crops', 143) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1511, 'Potatoes', 151) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1521, 'Sweet potatoes', 152) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1531, 'Cassava', 153) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1541, 'Yams', 154) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1591, 'Other root/tuber crops', 159) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1611, 'Chilies and pepers', 161) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1612, 'Anise, badian and fennel', 161) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1613, 'Other spice crops', 161) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1621, 'Hops', 162) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1711, 'Beans', 171) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1721, 'Broad beans', 172) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1731, 'Chickpeas', 173) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1741, 'Cow peas', 174) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1751, 'Lentils', 175) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1761, 'Lupins', 176) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1771, 'Peas', 177) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1781, 'Pigeon peas', 178) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1791, 'Other leguminous crops', 179) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1811, 'Sugar beet', 181) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1821, 'Sugar cane', 182) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1831, 'Sweet sorghum', 183) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1891, 'Other sugar crops', 189) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1911, 'Alfalfa', 191) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1912, 'Vetches', 191) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1919, 'Grasses and other fodder crops', 191) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1921, 'Cotton', 192) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1922, 'Jute, kenaf and other similar crops', 192) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1923, 'Flax, hemp and other similar crops', 192) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1929, 'Other fibre crops', 192) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1931, 'Medicinal, aromatic, pesticidal or similar crops', 193) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1941, 'Flowers crops', 194) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1991, 'Tobacco', 199) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1992, 'Crop combinations', 199) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (1999, 'Other annual crops', 199) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2111, 'No citrus fruits trees', 211) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2112, 'No citrus fruits shrub', 211) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2121, 'Orange', 212) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2122, 'Mandarin', 212) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2123, 'Lemon', 212) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2124, 'Bitter orange', 212) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2125, 'Grapefruit', 212) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2129, 'Other citrus fruits trees', 212) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2131, 'Pineapple', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2132, 'Avocado', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2133, 'Banana', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2134, 'Cacao', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2136, 'Coffee', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2137, 'Tea', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2139, 'Other exotic fruits tree', 213) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2211, 'Vineyards', 221) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2311, 'Olives groves', 231) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2411, 'Poplar', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2412, 'Pawlonia', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2413, 'Holm oaks', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2414, 'Tree nurseries', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2415, 'Carob trees', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2416, 'Other woody crops', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2419, 'Other woody crops (caper, wicker, mulberry, etc.)', 241) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2911, 'Aloe vera', 291) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2912, 'Agave', 291) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (2999, 'Other perennial crops', 299) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (3111, 'Natural meadows (mown once a year)', 311) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (3112, 'High mountain pasture', 311) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (3113, 'Grassland', 311) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (3199, 'Grassland and meadows', 319) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (4111, 'Fallows 1 year', 411) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (4112, 'Fallows 2 years', 411) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (4113, 'Fallows 3 years', 411) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (4114, 'Fallows 4 years', 411) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (4115, 'Fallows >5 years', 411) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (5111, 'Shrub land', 511) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (6111, 'Conifers', 611) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (6211, 'Deciduous slow growth', 621) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (6221, 'Deciduous rapid growth', 622) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (6311, 'Conifers and deciduous', 631) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (6999, 'Forest', 699) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (7111, 'Sparsely vegetated', 711) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (7211, 'Bare soils', 721) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (8111, 'Urban', 811) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (8211, 'Industrial and commercial', 821) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (8311, 'Transport', 831) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (8411, 'Greenhouses', 841) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (8511, 'Other build-up surface', 851) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9111, 'Seas', 911) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9121, 'Coastal lagoons', 912) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9122, 'Estuaries', 912) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9211, 'Permanent non-current inland waters (reservoirs, swamps, lakes, ...)', 921) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9212, 'Permanent rivers and streams', 921) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9213, 'Other permanent inland waters', 921) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9221, 'Temporary non-current inland waters (reservoirs, swamps, lakes, ...)', 922) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9222, 'Temporary rivers and streams', 922) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9223, 'Irrigation pools, ponds, fountains, wells', 922) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9224, 'Other temporary inland waters', 922) ON conflict DO nothing;
                insert into public.crop_list_n4 (code_n4, name, code_n3) values (9311, 'Open hydraulic infrastructures (ditches, canals...)', 931) ON conflict DO nothing;

                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1111, 1111, 'Winter wheat', 111, 'Wheat') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1112, 1112, 'Spring wheat', 111, 'Wheat') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1113, 1113, 'Hard wheat', 111, 'Wheat') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1114, 1114, 'Soft wheat', 111, 'Wheat') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1115, 1115, 'Triticale', 111, 'Wheat') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1121, 1121, 'Maize', 112, 'Maize') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1131, 1131, 'Rice', 113, 'Rice') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1141, 1141, 'Sorghum', 114, 'Sorghum') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1151, 1151, 'Barley two-row', 115, 'Barley') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1152, 1152, 'Barley six-row', 115, 'Barley') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1161, 1161, 'Rye', 116, 'Rye') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1171, 1171, 'Oats', 117, 'Oats') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1181, 1181, 'Millets', 118, 'Millets') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1192, 1192, 'Other cereals', 119, 'Other cereals') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1193, 1193, 'Quinoa', 119, 'Other cereals') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1211, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1212, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1213, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1214, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1215, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1216, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1217, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1218, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1219, 121, 'Leafy or stem vegetables', 121, 'Leafy or stem vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1221, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1222, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1223, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1224, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1225, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1226, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1227, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1229, 122, 'Fruit-bearing vegetables', 122, 'Fruit-bearing vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1231, 1231, 'Carrots', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1232, 1232, 'Turnips', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1233, 1233, 'Garlic', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1234, 1234, 'Onions (incl. shallots)', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1235, 1235, 'Leeks & other alliaceous vegetables', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1236, 1236, 'Beetroots', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1239, 1239, 'Other root, buld or tuberous vegetables', 123, 'Root, bulb or tuberous vegetables') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1241, 1241, 'Mushrooms', 124, 'Mushrooms and truffles') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1411, 1411, 'Soya beans', 141, 'Soya beans') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1421, 1421, 'Groundnuts', 142, 'Groundnuts') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1431, 1431, 'Castor bean', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1432, 1432, 'Linseed', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1433, 1433, 'Mustard', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1434, 1434, 'Niger seed', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1435, 1435, 'Rapeseed', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1436, 1436, 'Safflower', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1437, 1437, 'Sesame', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1438, 1438, 'Sunflower', 143, 'Other oilseed crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1511, 1511, 'Potatoes', 151, 'Potatoes') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1521, 1521, 'Sweet potatoes', 152, 'Sweet potatoes') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1531, 1531, 'Cassava', 153, 'Cassava') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1541, 1541, 'Yams', 154, 'Yams') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1611, 161, 'Spice crops', 161, 'Spice crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1612, 161, 'Spice crops', 161, 'Spice crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1613, 161, 'Spice crops', 161, 'Spice crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1621, 1621, 'Hops', 162, 'Hops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1711, 1711, 'Beans', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1721, 1721, 'Broad beans', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1731, 1731, 'Chickpeas', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1741, 1741, 'Cow peas', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1751, 1751, 'Lentils', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1761, 1761, 'Lupins', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1771, 1771, 'Peas', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1781, 1781, 'Pigeon peas', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1811, 1811, 'Sugar beet', 181, 'Sugar beet') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1821, 1821, 'Sugar cane', 182, 'Sugar cane') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1831, 1141, 'Sorghum', 114, 'Sorghum') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1911, 1911, 'Alfalfa', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1912, 1912, 'Vetches', 17, 'Leguminous crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1919, 3, 'Grassland and meadows', 3, 'Grassland and meadows') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1921, 1921, 'Cotton', 192, 'Fibre crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1922, 1922, 'Jute, kenaf and other similar crops', 192, 'Fibre crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1923, 1923, 'Flax, hemp and other similar crops', 192, 'Fibre crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1931, 1931, 'Medicinal, aromatic, pesticidal or similar crops', 1931, 'Medicinal, aromatic, pesticidal or similar crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1941, 1941, 'Flowers crops', 1941, 'Flowers crops') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 1991, 1991, 'Tobacco', 1991, 'Tobacco') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2111, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2112, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2121, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2122, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2123, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2124, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2125, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2129, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2131, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2132, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2133, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2134, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2136, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2137, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2139, 21, 'Fruits trees', 21, 'Fruits trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2211, 22, 'Vineyards', 22, 'Vineyards') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2311, 23, 'Olive groves', 23, 'Olive groves') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2411, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2412, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2413, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2414, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2415, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2416, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2419, 24, 'Trees', 24, 'Trees') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2911, 291, 'Succulent plant', 291, 'Succulent plant') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 2912, 291, 'Succulent plant', 291, 'Succulent plant') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 3111, 3, 'Grassland and meadows', 3, 'Grassland and meadows') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 3112, 3, 'Grassland and meadows', 3, 'Grassland and meadows') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 3113, 3, 'Grassland and meadows', 3, 'Grassland and meadows') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 3199, 3, 'Grassland and meadows', 3, 'Grassland and meadows') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 5111, 5, 'Shrub land', 5, 'Shrub land') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 6111, 61, 'Conifers', 6, 'Forest') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 6211, 62, 'Deciduous', 6, 'Forest') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 6221, 62, 'Deciduous', 6, 'Forest') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 6311, 63, 'Conifers and deciduous', 6, 'Forest') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 6999, 69, 'Forest', 6, 'Forest') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 7111, 71, 'Sparsely vegetated', 7, 'Bare soil') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 7211, 72, 'Bare soils', 7, 'Bare soil') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 8111, 81, 'Urban', 8, 'Build-up surface') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 8211, 82, 'Industrial and commercial', 8, 'Build-up surface') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 8311, 83, 'Transport', 8, 'Build-up surface') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 8411, 84, 'Greenhouses', 8, 'Build-up surface') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 8511, 85, 'Other build-up surface', 8, 'Build-up surface') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9111, 911, 'Seas', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9121, 912, 'Lagoons and estuaries', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9122, 912, 'Lagoons and estuaries', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9211, 9211, 'Permanent non-current inland waters (reservoirs, swamps, lakes,...)', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9212, 9212, 'Permanent rivers and streams', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9213, 9213, 'Other permanent inland waters', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9221, 9221, 'Temporary non-current inland waters (reservoirs, swamps, lakes,...)', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9222, 9222, 'Temporary rivers and streams', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9223, 9223, 'Irrigation pools, ponds, fountains, wells', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9224, 9224, 'Other temporary inland waters', 9, 'Water bodies') ON conflict DO nothing;
                insert into public.crop_remapping_set_detail (crop_remapping_set_id, original_code, remapped_code_pre, description_pre, remapped_code_post, description_post) values (1, 9311, 9311, 'Open hydraulic infrastructures (ditches, canals...)', 9, 'Water bodies') ON conflict DO nothing;


                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES (24, 'FMask','fmask', 'FMask', true, '{1,2}', null, false, false, false, false) ON conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.3.0', '2023-03-17 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/processors:3.3.0';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('orchestrator.check_ancestors.disabled', NULL, 'false', '2023-03-17 14:43:00.720811+00') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.l8.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s1.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.s2.query.days.back', NULL, '5', '2020-07-02 14:56:57.501918+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '5';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.temporal.filter.interval', NULL, '24', '2022-09-30 10:31:00.501+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '24';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2s1.tiled.tiff', NULL, true, '2022-09-30 10:31:00.501+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l3a.generate_20m_s2_resolution', NULL, 'true', '2016-02-26 19:30:06.821627+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_l4a.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.invalid_pixels_enabled', NULL, 'true', '2020-12-16 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.ndwi_enabled', NULL, 'false', '2023-03-02 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4c_mdb1.brightness_enabled', NULL, 'false', '2023-03-02 17:31:06.01191+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_perm_crop.slurm_qos', NULL, 'qoss4spermcrops', '2021-12-09 11:09:43.978921+02') ON conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s_perm_crop.docker_image',  NULL, 'sen4x/otb:7.2.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/otb:7.2.0';
                
                -- Permanent crops processor updates
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-samples-rasterization.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/otb:7.2.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-samples-rasterization',  NULL, 's4s-perm-crops-rasterization.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4s-perm-crops-rasterization.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-run-broceliande',  NULL, 's4s-perm-crops-run-broceliande.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4s-perm-crops-run-broceliande.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_perm_crop.broceliande-docker-image',  NULL, 'registry.gitlab.inria.fr/obelix/broceliande/develop:2.7.20210608', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'registry.gitlab.inria.fr/obelix/broceliande/develop:2.7.20210608';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-extract-inputs',  NULL, 's4s-perm-crops-extract-inputs.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4s-perm-crops-extract-inputs.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-extract-inputs.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-build-refl-stack-tif',  NULL, 's4s-perm-crops-build-refl-stack.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4s-perm-crops-build-refl-stack.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-build-refl-stack-tif.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-sieve.docker_image',  NULL, 'osgeo/gdal:ubuntu-full-3.2.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'osgeo/gdal:ubuntu-full-3.2.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_perm_crop.vec_field',  NULL, 'code_n1', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'code_n1';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crops-extract-parcels',  NULL, 'extract_yield_parcels.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_yield_parcels.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crops-extract-parcels.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';

                -- Yield processor updates
                DELETE FROM config WHERE key = 'general.orchestrator.s4s_yield_feat.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s_yield_feat.docker_image';
                
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-savitzky-golay.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-extract-weather-features.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-safy-lut.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-safy-optim.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-yield-features-extraction.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-yield-parcels-extraction.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-yield-reference-extraction.use_docker';
                DELETE FROM config WHERE key = 'general.orchestrator.s4s-yield-model.use_docker';
                DELETE FROM config WHERE key = 'processor.s4s_yield_feat.input_l3b';

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.scratch-path.s4s_yield_feat', NULL, '/mnt/archive/orchestrator_temp/s4s_yield_feat/{job_id}/{task_id}-{module}', '2021-05-18 17:54:17.288095+03') on conflict DO nothing; 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_yield_feat.keep_job_folders', NULL, '1', '2021-12-09 11:09:43.978921+02') on conflict DO nothing; 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_yield_feat.slurm_qos', NULL, 'qoss4syield', '2021-12-09 11:09:43.978921+02') on conflict DO nothing; 

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-savitzky-golay.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-extract-weather-features.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-safy-lut.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-safy-optim.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-features-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-parcels-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-reference-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-model.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-crop-types-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4stat/processors:1.0.0';

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-savitzky-golay',  NULL, 'run_savitzky_golay.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'run_savitzky_golay.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-extract-weather-features',  NULL, 'extract_weather_features.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_weather_features.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-safy-lut',  NULL, 'run_safy_lut.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'run_safy_lut.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-safy-optim',  NULL, 'run_safy_optim.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'run_safy_optim.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-features-extraction',  NULL, 'extract_yield_features.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_yield_features.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-parcels-extraction',  NULL, 'extract_yield_parcels.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_yield_parcels.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-reference-extraction',  NULL, 'extract_yield_reference.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_yield_reference.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-model',  NULL, 'S4S_Yield_Model.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'S4S_Yield_Model.py';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-crop-types-extraction',  NULL, 'extract_crop_codes.py', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'extract_crop_codes.py';

                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.enable_yield_model',  NULL, 'true', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.algorithm',  NULL, 'rf', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.selection-type',  NULL, 'automatic', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.max-automatic-features-no',  NULL, '44', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.manual-selection-features',  NULL, '', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.yield_features_product',  NULL, '', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.safy_params_upload_dir',  NULL, '/mnt/archive/s4s_yield_upload/safy_params', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/s4s_yield_upload/safy_params';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.safy_params_path',  NULL, '/mnt/archive/s4s_yield/{site}/{year}/SAFY_Config/safy_params.json', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '/mnt/archive/s4s_yield/{site}/{year}/SAFY_Config/safy_params.json';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_yield_safy_import',  NULL, 's4s-yield-safy-params-import.py', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 's4s-yield-safy-params-import.py';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_feat.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02') on conflict DO nothing;

                -- Crop Type Mapping processor    
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_crop_mapping.keep_job_folders', NULL, '1', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.processor.s4s_crop_mapping.slurm_qos', NULL, 'qoss4scropmap', '2021-12-09 11:09:43.978921+02') on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.input_l2a', NULL, 'N/A', '2023-03-04 11:09:58.820032+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.start_date',  NULL, '', '2023-10-04 15:27:41.861613+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.end_date',  NULL, '', '2023-03-04 15:27:41.861613+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.crop_remapping_set_id', null, '-1', '2021-08-30 13:56:07.537023+03') on conflict DO nothing; 
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_crop_mapping.features-filter', null, '', '2021-08-30 13:56:07.537023+03') on conflict DO nothing; 
                
                INSERT INTO config_metadata VALUES ('orchestrator.check_ancestors.disabled', 'Disable processor wait for inputs', 'bool', false, 1, FALSE, 'Disable processor wait for inputs', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.input_l2a', 'The list of L2A products', 'select', FALSE, 22, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 22, TRUE, 'Start date', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_l4a.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 22, TRUE, 'End date', NULL, FALSE)  on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.invalid_pixels_enabled', 'Number of invalid pixels per parcels extraction enabled', 'bool', true, 26, FALSE, 'Extract number of invalid pixels per parcel', NULL, true) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.ndwi_enabled', 'NDWI markers extraction enabled', 'bool', true, 26, true, 'Extract NDWI markers', NULL, true) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.brightness_enabled', 'Brightness markers extraction enabled', 'bool', true, 26, true, 'Extract Brightness markers', NULL, true) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4c_mdb1.mdb3_input_tables', 'MDB3 input tables location', 'string', true, 26, false, 'MDB3 input tables location', NULL) on conflict (key) DO UPDATE SET is_site_visible = false;
                
                INSERT INTO config_metadata VALUES ('processor.s4s_crop_mapping.input_l2a', 'The list of L2A products', 'select', FALSE, 30, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_crop_mapping.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 30, TRUE, 'Start date', NULL, FALSE)  on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_crop_mapping.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 30, TRUE, 'End date', NULL, FALSE)  on conflict DO nothing;
                insert into config_metadata values ('processor.s4s_crop_mapping.crop_remapping_set_id', 'Crop Remapping Set', 'string', true, 30, true, 'Crop Remapping Set', '{ "allowed_values_source": { "database_object": "crop_remapping_set", "value_column": "crop_remapping_set_id", "label_column": "name" } }', true)   on conflict DO nothing;
                insert into config_metadata values ('processor.s4s_crop_mapping.features-filter', 'Features filter. If provided, the features are given as comma separated values. Possible values are sr10 (S2 Reflectance 10m), sr20 (S2 reflectance 20m), vi (Vegetation indices), vis (Vegetation indices Statistics), sar (S1 features), re (Red edge features)', 'string', true, 30, true, 'Features filter', null)  on conflict DO nothing;
                
                -- Yield
                INSERT INTO config_metadata VALUES ('general.scratch-path.s4s_yield_feat', 'Path for Yield temporary files', 'string', false, 1) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_feat.keep_job_folders', 'Keep yield temporary files', 'int', false, 8) on conflict DO nothing; 
                
                -- INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.enable_yield_model', 'Enable yield estimation', 'bool', false, 29, true, 'Enable yield estimation', NULL, true) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.algorithm', 'Yield estimation algorithm', 'string', true, 29, true, 'Yield estimation algorithm', '{ "allowed_values": [{ "value": "rf", "display": "Random Forest" }, { "value": "lmr", "display": "Linear regression"}, { "value": "svm", "display": "Support Vector Machine"}] }', true) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.selection-type',  'Yield estimation selection type', 'string', true, 29, true, 'Yield estimation selection type', '{ "allowed_values": [{ "value": "automatic", "display": "Automatic" }, { "value": "manual", "display": "Manual"}, { "value": "none", "display": "None" }] }', true) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.max-automatic-features-no', 'Maximum number of automatic features', 'int', true, 29, true, 'Maximum number of automatic features', null) on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.manual-selection-features', 'Manual selection features', 'string', true, 29, true, 'Manual selection features', null) on conflict DO nothing; 
                -- INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.yield_features_product', 'Yield features product', 'string', true, 29, true, 'Yield features product', null) on conflict DO nothing; 
                
                
                DELETE FROM config_metadata WHERE key = 'processor.s4s_yield_feat.input_l3b';
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.input_l2a', 'The list of L2A products', 'select', FALSE, 29, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE)  on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 29, TRUE, 'Start date', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 29, TRUE, 'End date', NULL, FALSE) on conflict DO nothing;
                
                
                -- Old keys, if exist
                DELETE FROM config WHERE key = 'processor.s4s_yield_feat.yield_features_product';
                DELETE FROM config WHERE key = 'processor.s4s_yield_feat.enable_yield_model';

                DELETE FROM config_metadata WHERE key = 'processor.s4s_yield_feat.yield_features_product';
                DELETE FROM config_metadata WHERE key = 'processor.s4s_yield_feat.enable_yield_model';

                
            $str$;
            raise notice '%', _statement;
            execute _statement;
                
            _statement := $str$
                INSERT INTO default_scheduled_tasks values (15, 'L2A_MSK', 1, 1, 0, 'start', NULL, '1 day', 60, 1, NULL) on conflict DO nothing; 

                DELETE FROM config WHERE key = 'processor.l2a_msk.cog';
                DELETE FROM config_metadata WHERE key = 'processor.l2a_msk.cog';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.tiled', NULL, 'true', '2023-07-28 17:54:17.288095+03') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.tiled', 'Produce output flags as Tiled', 'bool', false, 27, FALSE, 'Produce output flags as Tiled', NULL) on conflict DO nothing; 
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.water_is_valid', NULL, 'true', '2023-07-28 17:54:17.288095+03') on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.water_is_valid', 'Consider water pixels as valid', 'bool', false, 27, FALSE, 'Consider water pixels as valid', NULL) on conflict DO nothing; 
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.l2a_msk.snow_is_valid', NULL, 'true', '2023-07-28 17:54:17.288095+03') on conflict DO nothing; 
                INSERT INTO config_metadata VALUES ('processor.l2a_msk.snow_is_valid', 'Consider snow pixels as valid', 'bool', false, 27, FALSE, 'Consider snow pixels as valid', NULL) on conflict DO nothing; 
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.parcels_product.parcels_sar_file_name_pattern', NULL, 'in_?situ_.*_(\d{4,5})_buf_10m.shp', '2019-10-11 16:15:00.0+02') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'in_?situ_.*_(\d{4,5})_buf_10m.shp';

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            
            -- Updates after 1.1 corrections
            _statement := $str$
                INSERT INTO processor (id, name, short_name, label, required, supported_satellite_ids, mandatory_satellite_ids, is_admin_ui_visible, is_service_ui_visible, lpis_required, additional_config_required) VALUES (27, 'L3 Yield SU','s4s_yield_su', 'L3 Yield SU', false, '{1,2}', null, true, false, true, false)  ON conflict DO nothing;
                INSERT INTO product_type (id, name, description, is_raster) VALUES (34, 's4s_yield_su','L3 Yield SU', false)  ON conflict DO nothing;
                INSERT INTO config_category VALUES (38, 'L3 Yield SU', 38, true) ON conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.docker_image', NULL, 'sen4cap/processors:3.3.0', '2021-01-14 12:11:21.800537+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4cap/processors:3.3.0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-trend-features-extraction.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-trend-features-extraction',  NULL, 's4s_yieldsu_trend_computation.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.historical_data_upload_dir',  NULL, '/mnt/archive/s4s_yield_upload/su_historical_data', '2023-09-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.historical_data_path',  NULL, '/mnt/archive/s4s_yield/{site}/yield_su/HistoricalData/SU_yield_historical_data.csv', '2023-09-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_yield_su_historical_data_import',  NULL, 's4s-yield-su-historical-data-import.py', '2023-09-19 14:43:00.720811+00') on conflict DO nothing;
                -- INSERT INTO config(key, site_id, value, last_updated) VALUES ('processor.s4s_yield_su.enable_yield_model',  NULL, 'true', '2021-01-18 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'true';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-su-merge-yearly-features.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-su-merge-yearly-features',  NULL, 's4s_yieldsu_merge_features.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-su-model-wrp.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-su-model-wrp',  NULL, 's4s_yieldsu_model.py', '2021-01-18 14:43:00.720811+00') on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.input_l2a', 'The list of L2A products', 'select', FALSE, 29, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE) on conflict (key) do update set config_category_id = 29;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 29, TRUE, 'Start date', NULL, FALSE) on conflict (key) do update set config_category_id = 29;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 29, TRUE, 'End date', NULL, FALSE) on conflict (key) do update set config_category_id = 29;
                INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_su.slurm_qos', 'Slurm QOS for Yield SU', 'string', true, 8, FALSE, 'Slurm QOS for Yield SU', NULL) on conflict DO nothing; 

                -- INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.enable_yield_model', 'Enable yield estimation', 'bool', false, 38, true, 'Enable yield estimation', NULL, true) on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.algorithm', 'Yield estimation algorithm', 'string', true, 38, true, 'Yield estimation algorithm', '{ "allowed_values": [{ "value": "rf", "display": "Random Forest" }, { "value": "lmr", "display": "Linear regression"}, { "value": "svm", "display": "Support Vector Machine"}] }', true)  on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.selection-type',  'Yield estimation selection type', 'string', true, 38, true, 'Yield estimation selection type', '{ "allowed_values": [{ "value": "automatic", "display": "Automatic" }, { "value": "manual", "display": "Manual"}, { "value": "none", "display": "None" }] }', true)  on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.max-automatic-features-no', 'Maximum number of automatic features', 'int', true, 38, true, 'Maximum number of automatic features', null)  on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.manual-selection-features', 'Manual selection features', 'string', true, 38, true, 'Manual selection features', null)  on conflict (key) do update set config_category_id = 38;
                -- INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.yield_features_product', 'Yield features product', 'string', true, 38, true, 'Yield features product', null)  on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.input_l2a', 'The list of L2A products', 'select', FALSE, 38, FALSE, 'Available L2A input files', '{"name":"inputFiles_L2A[]","product_type_id":1,"satellite_ids":[1,2]}', FALSE)  on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.start_date', 'Start date (YYYY-MM-DD)', 'string', FALSE, 38, TRUE, 'Start date', NULL, FALSE)  on conflict (key) do update set config_category_id = 38;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.end_date', 'End date (YYYY-MM-DD)', 'string', FALSE, 38, TRUE, 'End date', NULL, FALSE)  on conflict (key) do update set config_category_id = 38;

                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.su_path', 'Yield SU path', 'string', TRUE, 29, FALSE, 'Yield SU path', null) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.data_extr_dir', 'Yield SU data extraction dir', 'string', TRUE, 29, FALSE, 'Yield SU data extraction dir', null) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.historical_data_upload_dir', 'Yield SU historical data upload dir', 'string', TRUE, 29, FALSE, 'Yield SU historical data upload dir', null) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_su.historical_data_path', 'Yield SU historical data import location', 'string', TRUE, 29, FALSE, 'Yield SU historical data import location', null) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s_yield_su_historical_data_import', 'Yield SU historical data import script', 'string', TRUE, 29, FALSE, 'Yield SU historical data import script', null) on conflict DO nothing;

                INSERT INTO auxdata_descriptor (id, name, label, unique_by) VALUES (12, 'su_yield_historical_data', 'SU Yield historical data', 'year') ON conflict(id) DO UPDATE SET name = 'su_yield_historical_data', label = 'SU Yield historical data', unique_by = 'year';
                INSERT INTO auxdata_file (id, auxdata_descriptor_id, file_order, label, extensions, required) VALUES (17, 12, 1, 'SU Yield historical data', '{csv}', false) on conflict DO nothing; 
                INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) 
            VALUES (18, 17, 1, 'Import', '{executor.module.path.s4s_yield_su_historical_data_import}', 27, '{"parameters": [{"name": "file", "command": "--input-file", "type": "java.io.File", "required": true, "refFileId": 17}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "suYieldHistDataImportRootPath","label": null,"type": "java.lang.String","value":"{processor.s4s_yield_su.historical_data_upload_dir}", "required": true},{"name": "suYieldHistDataTarget", "label": null,"type": "java.lang.String","value":"{processor.s4s_yield_su.historical_data_path}", "required": true}]}', null, true) on conflict DO nothing;            

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s_parcels_import',  NULL, 'data-preparation-s4s.py', '2021-02-19 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'data-preparation-s4s.py';
                
                
                INSERT INTO auxdata_operation (id, auxdata_file_id, operation_order, name, handler_path, processor_id, parameters, output_type, async) VALUES (10, 9, 2, 'Import', '{executor.module.path.s4s_parcels_import}', 8, '{"parameters": [{"name": "lpisFile", "command": "--parcels-geom", "type": "java.io.File", "required": true, "refFileId": 9},{"name": "lutFile", "command": "--statistical-data", "type": "java.io.File", "required": false, "refFileId": 10}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name": "lpisRootPath","label": null,"type": "java.lang.String","value":"{processor.insitu.path}", "required": true}]}', null, true) on conflict(id) DO UPDATE SET parameters = '{"parameters": [{"name": "lpisFile", "command": "--parcels-geom", "type": "java.io.File", "required": true, "refFileId": 9},{"name": "lutFile", "command": "--statistical-data", "type": "java.io.File", "required": false, "refFileId": 10}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name": "lpisRootPath","label": null,"type": "java.lang.String","value":"{processor.insitu.path}", "required": true}]}';
                
                
                UPDATE site_auxdata set parameters = '{"parameters": [{"name": "lpisFile", "command": "--parcels-geom", "type": "java.io.File", "required": true, "refFileId": 9},{"name": "lutFile", "command": "--statistical-data", "type": "java.io.File", "required": false, "refFileId": 10}, {"name":"siteId","command":"--site-id","label":null,"type":"java.lang.Integer","required":"true"}, {"name": "year","command":"--year","label": "Year","type": "java.lang.Integer","required": true}, {"name": "lpisRootPath","label": null,"type": "java.lang.String","value":"{processor.insitu.path}", "required": true}]}' where auxdata_file_id = 9 and auxdata_descriptor_id = 8;
                
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-crop-type-mapping.docker_image', NULL, 'sen4x/crop-map-s4s:0.2.0', '2022-08-22 14:43:00.720811+00') on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = 'sen4x/crop-map-s4s:0.2.0';
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('downloader.start.offset', NULL, '0', '2016-07-20 20:05:00')  on conflict (key, COALESCE(site_id, -1)) DO UPDATE SET value = '0';
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.sacct-max-retries', NULL, '1', '2023-10-26 17:03:39.541136+03') on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.sacct-max-retries', 'Slurm SACCT max retries', 'int', true, 1, FALSE, 'Slurm SACCT max retries', NULL) on conflict DO nothing;
                
                
                -- Permanent crops
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-perm-crop-post-processing',  NULL, 's4s-perm-crops-post-processing.py', '2024-01-11 14:43:00.720811+00') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-perm-crop-post-processing.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2024-01-11 14:43:00.720811+00') on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-su-merge-yearly-features.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00')  on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('general.orchestrator.s4s-yield-su-model-wrp.docker_image',  NULL, 'sen4stat/processors:1.0.0', '2021-02-19 14:43:00.720811+00')  on conflict DO nothing;

                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-su-merge-yearly-features',  NULL, 's4s_yieldsu_merge_features.py', '2021-01-18 14:43:00.720811+00')  on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('executor.module.path.s4s-yield-su-model-wrp',  NULL, 's4s_yieldsu_model.py', '2021-01-18 14:43:00.720811+00')  on conflict DO nothing;
                
                
                -- Old keys, if exist
                DELETE FROM config WHERE key = 'processor.s4s_yield_su.yield_features_product';
                DELETE FROM config WHERE key = 'processor.s4s_yield_su.enable_yield_model';

                DELETE FROM config_metadata WHERE key = 'processor.s4s_yield_su.yield_features_product';
                DELETE FROM config_metadata WHERE key = 'processor.s4s_yield_su.enable_yield_model';
                

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$            
                create or replace function sp_start_fmask_l1_tile_processing(
                    _node_id text
                )
                returns table (
                    site_id int,
                    satellite_id smallint,
                    downloader_history_id int,
                    path text,
                    orbit_id int,
                    tile_id text) as
                $$
                declare _satellite_id smallint;
                declare _downloader_history_id int;
                declare _path text;
                declare _site_id int;
                declare _product_date timestamp;
                declare _orbit_id integer;
                declare _tile_id text;
                begin
                    if (select current_setting('transaction_isolation') not ilike 'serializable') then
                        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
                    end if;

                    create temporary table if not exists site_config(
                        key,
                        site_id,
                        value
                    ) as
                    select
                        keys.key,
                        site.id,
                        config.value
                    from site
                    cross join (
                        values
                            ('processor.fmask.enabled'),
                            ('s2.enabled'),
                            ('l8.enabled')
                    ) as keys(key)
                    cross join lateral (
                        select
                            coalesce((
                                select value
                                from config
                                where key = keys.key
                                and config.site_id = site.id
                            ), (
                                select value
                                from config
                                where key = keys.key
                                and config.site_id is null
                            )) as value
                    ) config;

                    select fmask_history.satellite_id,
                           fmask_history.downloader_history_id
                    into _satellite_id,
                         _downloader_history_id
                    from fmask_history
                    where status_id = 2 -- failed
                      and retry_count < 3
                      and status_timestamp < now() - interval '1 day'
                    order by status_timestamp
                    limit 1;

                    if found then
                        select downloader_history.product_date,
                               downloader_history.full_path,
                               downloader_history.site_id
                        into _product_date,
                             _path,
                             _site_id
                        from downloader_history
                        where id = _downloader_history_id;

                        update fmask_history
                        set status_id = 1, -- processing
                            status_timestamp = now(),
                            node_id = _node_id
                        where fmask_history.downloader_history_id = _downloader_history_id;
                    else
                        select distinct
                            downloader_history.satellite_id,
                            downloader_history.id,
                            downloader_history.product_date,
                            downloader_history.full_path,
                            downloader_history.site_id,
                            downloader_history.orbit_id,
                            downloader_history.tiles[1]
                        into _satellite_id,
                            _downloader_history_id,
                            _product_date,
                            _path,
                            _site_id,
                            _orbit_id,
                            _tile_id            
                        from downloader_history
                        inner join site on site.id = downloader_history.site_id
                        cross join lateral (
                            select
                                (
                                    select value :: boolean as fmask_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'processor.fmask.enabled'
                                ),
                                (
                                    select value :: boolean as s2_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 's2.enabled'
                                ),
                                (
                                    select value :: boolean as l8_enabled
                                    from site_config
                                    where site_config.site_id = downloader_history.site_id
                                    and key = 'l8.enabled'
                                )
                        ) config
                        where not exists (
                            select *
                            from fmask_history
                            where fmask_history.downloader_history_id = downloader_history.id
                        )
                        and downloader_history.status_id in (2, 5, 7) -- downloaded, processing
                        and downloader_history.product_name not like '%_MSIL2A_%'
                        and site.enabled
                        and fmask_enabled
                        and case downloader_history.satellite_id
                            when 1 then config.s2_enabled
                            when 2 then config.l8_enabled
                            else false
                        end
                        order by satellite_id, product_date
                        limit 1;

                        if found then
                            insert into fmask_history (
                                satellite_id,
                                downloader_history_id,
                                status_id,
                                node_id
                            ) values (
                                _satellite_id,
                                _downloader_history_id,
                                1, -- processing
                                _node_id
                            );
                        end if;
                    end if;

                    if _downloader_history_id is not null then
                        return query
                            select _site_id,
                                _satellite_id,
                                _downloader_history_id,
                                _path,
                                _orbit_id,
                                _tile_id;
                    end if;
                end;
                $$ language plpgsql volatile;
            
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$            
                create or replace function sp_get_site_strata(_site_id int)
                    returns table (
                            stratum_id int,
                            geom bytea,
                            epsg_code int,
                            tiles char(5)[]
                    )
                as
                $$
                begin
                    return query
                        with site_tiles as (select tile_id
                                            from sp_get_site_tiles(_site_id :: smallint, 1 :: smallint)),
                             site_stratum as (select stratum.stratum_id, wkb_geometry as geometry, st_transform(wkb_geometry, 4326) :: geography as stratum_geog
                                              from stratum
                                              where site_id = _site_id)
                        select site_stratum.stratum_id,
                               ST_AsBinary(site_stratum.geometry) as geometry,
                               ST_SRID(site_stratum.geometry) as epsg_code,
                               array_agg(shape_tiles_s2.tile_id) as tile_id
                        from site_stratum
                                 inner join shape_tiles_s2 on ST_Intersects(shape_tiles_s2.geog, site_stratum.stratum_geog)
                        where exists (select * from site_tiles where site_tiles.tile_id = shape_tiles_s2.tile_id)
                        group by site_stratum.stratum_id, site_stratum.geometry
                        order by site_stratum.stratum_id;
                end;
                $$
                    language plpgsql
                    stable;            

                CREATE OR REPLACE FUNCTION sp_get_job_definition(IN _job_id integer)
                  RETURNS TABLE(processor_id smallint, site_id smallint, parameters json) AS
                $BODY$
                BEGIN

                RETURN QUERY SELECT job.processor_id, job.site_id, job.parameters FROM job WHERE job.id = _job_id;

                END;
                $BODY$
                  LANGUAGE plpgsql VOLATILE
                  COST 100
                  ROWS 1000;
                ALTER FUNCTION sp_get_job_definition(integer)
                  OWNER TO admin;            
                
                CREATE OR REPLACE FUNCTION sp_get_job_steps(
                IN _job_id int
                ) RETURNS TABLE (
                task_id int,
                module_short_name character varying,
                step_name character varying,
                preceding_task_ids integer[],
                parameters json
                ) AS $$
                BEGIN

                RETURN QUERY
                WITH job_tasks AS (
                        SELECT task.id AS task_id, task.module_short_name as module_short_name, task.preceding_task_ids as preceding_task_ids
                        FROM task
                        WHERE task.job_id = _job_id
                    )
                SELECT job_tasks.task_id, job_tasks.module_short_name, step.name AS step_name, job_tasks.preceding_task_ids , step.parameters
                FROM job_tasks INNER JOIN step ON job_tasks.task_id = step.task_id;

                END;
                $$ LANGUAGE plpgsql;

            $str$;
            raise notice '%', _statement;
            execute _statement;
            
            _statement := $str$            
                -- Adding missing config_metadata keys to have them correctly in the Configuration tab
                INSERT INTO config_metadata VALUES ('executor.module.path.earth-signature', 'Script for earth signature', 'file', true, 8, FALSE, 'Script for earth signature', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.ndvi-veg-stats', 'Script for extracting NDVI vegetation stats', 'file', true, 8, FALSE, 'Script for extracting NDVI vegetation stats', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-extract-weather-features', 'Script for extracting S4S Weather Features', 'file', true, 8, FALSE, 'Script for extracting S4S Weather Features', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-merge-all-features-wrp', 'Script for merging all Yield SU features', 'file', true, 8, FALSE, 'Script for merging all Yield SU features', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crop-post-processing', 'Script for Permanent Crops Post Processing', 'file', true, 8, FALSE, 'Script for Permanent Crops Post Processing', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-build-refl-stack-tif', 'Script for Permanent Crops reflectance stack building', 'file', true, 8, FALSE, 'Script for Permanent Crops reflectance stack building', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-extract-inputs', 'Script for Permanent Crops inputs extraction', 'file', true, 8, FALSE, 'Script for Permanent Crops inputs extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-extract-parcels', 'Script for Permanent Crops parcels', 'file', true, 8, FALSE, 'Script for Permanent Crops parcels', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-run-broceliande', 'Script for Permanent Crops Broceliande execution', 'file', true, 8, FALSE, 'Script for Permanent Crops Broceliande execution', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-samples-rasterization', 'Script for Permanent Crops samples rasterization', 'file', true, 8, FALSE, 'Script for Permanent Crops samples rasterization', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-perm-crops-sieve', 'Script for Permanent Crops crop sieve', 'file', true, 8, FALSE, 'Script for Permanent Crops crop sieve', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-safy-lut', 'Script for Yield Safy LUT', 'file', true, 8, FALSE, 'Script for Yield Safy LUT', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-safy-optim', 'Script for Yield Safy Optimization', 'file', true, 8, FALSE, 'Script for Yield Safy Optimization', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-savitzky-golay', 'Script for Yield Savitzky Golay', 'file', true, 8, FALSE, 'Script for Yield Savitzky Golay', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-savitzky-golay-wrp', 'Script for Yield SU Savitzky Golay', 'file', true, 8, FALSE, 'Script for Yield SU Savitzky Golay', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-crop-types-extraction', 'Script for Yield Crop Types extraction', 'file', true, 8, FALSE, 'Script for Yield Crop Types extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-esu-aggregate', 'Script for Yield SU ESU aggregate', 'file', true, 8, FALSE, 'Script for Yield SU ESU aggregate', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-esu-extraction', 'Script for Yield SU ESU extraction', 'file', true, 8, FALSE, 'Script for Yield SU ESU extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-features-extraction', 'Script for Yield features extraction', 'file', true, 8, FALSE, 'Script for Yield features extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-features-extraction-wrp', 'Script for Yield SU features extraction', 'file', true, 8, FALSE, 'Script for Yield SU features extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-model', 'Script for Yield Model execution', 'file', true, 8, FALSE, 'Script for Yield Model execution', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-parcels-extraction', 'Script for Yield parcels extraction', 'file', true, 8, FALSE, 'Script for Yield parcels extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-reference-extraction', 'Script for reference Yield extraction', 'file', true, 8, FALSE, 'Script for reference Yield extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-su-merge-yearly-features', 'Script for Yield SU merging yearly features', 'file', true, 8, FALSE, 'Script for Yield SU merging yearly features', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-su-model-wrp', 'Script for Yield SU Model execution', 'file', true, 8, FALSE, 'Script for Yield SU Model execution', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s-yield-trend-features-extraction', 'Script for Yield SU Trend features extraction', 'file', true, 8, FALSE, 'Script for Yield SU Trend features extraction', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s_admin_units_import', 'Script for importing S4S adminstrative units', 'file', true, 8, FALSE, 'Script for importing S4S adminstrative units', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s_parcels_import', 'Script for importing S4S parcels', 'file', true, 8, FALSE, 'Script for importing S4S parcels', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.module.path.s4s_yield_safy_import', 'Script for importing SAFY config file', 'file', true, 8, FALSE, 'Script for importing SAFY config file', NULL) on conflict DO nothing;
                
                
                INSERT INTO config_metadata VALUES ('executor.processor.s4s_crop_mapping.keep_job_folders', 'Keep S4S Crop Mapping temporary files', 'int', false, 8) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.s4s_perm_crop.keep_job_folders', 'Keep S4S Permanent Crops temporary files', 'int', false, 8) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('executor.processor.s4s_crop_mapping.slurm_qos', 'Slurm QOS for S4S Crop Mapping', 'string', true, 8, FALSE, 'Slurm QOS for S4S Crop Mapping', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.s4s_perm_crop.slurm_qos', 'Slurm QOS for Permanent Crops', 'string', true, 8, FALSE, 'Slurm QOS for Permanent Crops', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('executor.processor.s4s_yield_feat.slurm_qos', 'Slurm QOS for Yield', 'string', true, 8, FALSE, 'Slurm QOS for Yield', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('general.scratch-path.s4s_perm_crop', 'Path for Permanent Crops temporary files', 'string', false, 1) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('general.orchestrator.ndvi-veg-stats.docker_image', 'NDVI vegetation statistics docker image', 'string', false, 1, FALSE, 'NDVI vegetation statistics docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-extract-weather-features.docker_image', 'Yield Weather Features docker image', 'string', false, 1, FALSE, 'Yield Weather Features docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crop-post-processing.docker_image', 'Permanent Crops Post-Processing docker image', 'string', false, 1, FALSE, 'Permanent Crops Post-Processing docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-build-refl-stack-tif.docker_image', 'Permanent Crops build reflectance stack docker image', 'string', false, 1, FALSE, 'Permanent Crops build reflectance stack docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-extract-inputs.docker_image', 'Permanent Crops Extract inputs docker image', 'string', false, 1, FALSE, 'Permanent Crops Extract inputs docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-extract-parcels.docker_image', 'Permanent Crops Extract parcels docker image', 'string', false, 1, FALSE, 'Permanent Crops Extract parcels docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-samples-rasterization.docker_image', 'Permanent Crops samples rasterization docker image', 'string', false, 1, FALSE, 'Permanent Crops samples rasterization docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-sieve.docker_image', 'Permanent Crops crop sieve docker image', 'string', false, 1, FALSE, 'Permanent Crops crop sieve docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-safy-lut.docker_image', 'Yield SAFY LUT docker image', 'string', false, 1, FALSE, 'Yield SAFY LUT docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-safy-optim.docker_image', 'Yield SAFY Optimization docker image', 'string', false, 1, FALSE, 'Yield SAFY Optimization docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-savitzky-golay-wrp.docker_image', 'Yield SU Savitzky Golay docker image', 'string', false, 1, FALSE, 'Yield SU Savitzky Golay docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-savitzky-golay.docker_image', 'Yield Savitzky Golay docker image', 'string', false, 1, FALSE, 'Yield Savitzky Golay docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-crop-types-extraction.docker_image', 'Yield crop types extraction docker image', 'string', false, 1, FALSE, 'Yield crop types extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-esu-aggregate.docker_image', 'Yield SU ESU aggregation docker image', 'string', false, 1, FALSE, 'Yield SU ESU aggregation docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-esu-extraction.docker_image', 'Yield SU ESU extraction docker image', 'string', false, 1, FALSE, 'Yield SU ESU extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-features-extraction-wrp.docker_image', 'Yield SU Features extraction docker image', 'string', false, 1, FALSE, 'Yield SU Features extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-features-extraction.docker_image', 'Yield Features extraction docker image', 'string', false, 1, FALSE, 'Yield Features extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-model.docker_image', 'Yield Model docker image', 'string', false, 1, FALSE, 'Yield Model docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-parcels-extraction.docker_image', 'Yield Parcels extraction docker image', 'string', false, 1, FALSE, 'Yield Parcels extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-reference-extraction.docker_image', 'Reference Yield extraction docker image', 'string', false, 1, FALSE, 'Reference Yield extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-su-merge-yearly-features.docker_image', 'Yield SU yearly features merge docker image', 'string', false, 1, FALSE, 'Yield SU yearly features merge docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-su-model-wrp.docker_image', 'Yield SU Model docker image', 'string', false, 1, FALSE, 'Yield SU Model docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-yield-trend-features-extraction.docker_image', 'Yield SU Trend features extraction docker image', 'string', false, 1, FALSE, 'Yield SU Trend features extraction docker image', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s_perm_crop.docker_image', 'Permanent Crops default docker image', 'string', false, 1, FALSE, 'Permanent Crops default docker image', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s-perm-crops-run-broceliande.use_docker', 'Broceliande execution use docker', 'int', false, 1, FALSE, 'Broceliande execution use docker', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('general.orchestrator.s4s_perm_crop.use_docker', 'Permanent crops use docker default value', 'int', false, 1, FALSE, 'Permanent crops use docker default value', NULL) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('downloader.l9.write-dir', 'Write directory for Landsat9', 'string', false, 15, FALSE, 'Write directory for Landsat9', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('downloader.l9.enabled', 'L9 downloader is enabled', 'bool', false, 15, FALSE, 'L9 downloader is enabled', NULL) on conflict DO nothing;
                
                
                INSERT INTO config_category VALUES (39, 'ERA5', 39, true) ON conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.era5_weather.enabled', 'ERA5 processor enabled', 'bool', false, 38, FALSE, 'ERA5 processor enabled', NULL) on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.parcel_id_col_name', 'Yield Features ID column name', 'string', TRUE, 29, FALSE, 'Yield Features ID column name', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.safy_params_path', 'SAFY Params path', 'string', TRUE, 29, FALSE, 'SAFY Params path', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_yield_feat.safy_params_upload_dir', 'SAFY params upload dir', 'string', TRUE, 29, FALSE, 'SAFY params upload dir', NULL, FALSE) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4s_perm_crop.broceliande-docker-image', 'Broceliande docker image', 'string', TRUE, 28, FALSE, 'Broceliande docker image', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_perm_crop.vec_field', 'Permanent crops field name', 'string', TRUE, 28, FALSE, 'Permanent crops field name', NULL, FALSE) on conflict DO nothing;

                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.municipalities_upload_dir', 'Municipalities upload dir', 'string', TRUE, 21, FALSE, 'Municipalities upload dir', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.parcel_stats_upload_dir', 'Parcels stats upload dir', 'string', TRUE, 21, FALSE, 'Parcels stats  upload dir', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.parcels_upload_dir', 'Parcels geometries upload dir', 'string', TRUE, 21, FALSE, 'Parcels geometries upload dir', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.provinces_upload_dir', 'Provinces upload dir', 'string', TRUE, 21, FALSE, 'Provinces upload dir', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.regions_upload_dir', 'Regions upload dir', 'string', TRUE, 21, FALSE, 'Regions upload dir', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.segments_upload_dir', 'Segments upload dir', 'string', TRUE, 21, FALSE, 'Segments upload dir', NULL, FALSE) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('processor.s4s_parcels.working_dir', 'Parcels import working dir', 'string', TRUE, 21, FALSE, 'Parcels import working dir', NULL, FALSE) on conflict DO nothing;
                
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.container', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.domain', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.password', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.projectId', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.url', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('object.storage.user', NULL, '', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('primary.sensor', NULL, 'S2', '2022-09-30 10:31:00.501+02') on conflict DO nothing;
                
                INSERT INTO config_metadata VALUES ('object.storage.container', 'Object storage container', 'string', false, 23, FALSE, 'Object storage container', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('object.storage.domain', 'Object storage domain', 'string', false, 23, FALSE, 'Object storage domain', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('object.storage.password', 'Object storage password', 'string', false, 23, FALSE, 'Object storage password', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('object.storage.projectId', 'Object storage project ID', 'string', false, 23, FALSE, 'Object storage project ID', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('object.storage.url', 'Object storage URL', 'string', false, 23, FALSE, 'Object storage URL', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('object.storage.user', 'Object storage user', 'string', false, 23, FALSE, 'Object storage user', NULL) on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('primary.sensor', 'Primary sensor', 'string', false, 23, FALSE, 'Primary sensor', NULL) on conflict DO nothing;
                
                INSERT INTO config(key, site_id, value, last_updated) VALUES ('l9.enabled', NULL, 'false', '2024-02-24 14:56:57.501918+02') on conflict DO nothing;
                INSERT INTO config_metadata VALUES ('l9.enabled', 'L9 is enabled', 'bool', false, 15, FALSE, 'L9 is enabled', NULL) on conflict DO nothing;
                
                
                UPDATE config_metadata SET config_category_id = 38 WHERE KEY = 'processor.s4s_yield_su.su_path';
                UPDATE config_metadata SET config_category_id = 38 WHERE KEY = 'processor.s4s_yield_su.data_extr_dir';

                UPDATE config_metadata SET config_category_id = 38 WHERE KEY = 'processor.s4s_yield_su.historical_data_upload_dir';
                UPDATE config_metadata SET config_category_id = 38 WHERE KEY = 'processor.s4s_yield_su.historical_data_path';
                UPDATE config_metadata SET config_category_id = 38 WHERE KEY = 'executor.module.path.s4s_yield_su_historical_data_import';
                
            $str$;
            raise notice '%', _statement;
            execute _statement;
            
           _statement := 'update meta set version = ''2.0'';';
            raise notice '%', _statement;
            execute _statement;
            
        end if;
    end if;
    raise notice 'complete';
end;
$migration$;

commit;


                            