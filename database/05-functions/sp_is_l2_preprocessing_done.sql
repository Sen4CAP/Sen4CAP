-- DROP function sp_is_l2_preprocessing_done(smallint, json, timestamp,timestamp)
CREATE OR REPLACE FUNCTION sp_is_l2_preprocessing_done
(
    _site_id site.id%TYPE,
    _sat_ids json,
    _start_date timestamp DEFAULT NULL::timestamp,
    _end_date timestamp DEFAULT NULL::timestamp
  )
  RETURNS boolean AS
$func$

BEGIN
   RETURN (select count(*) from downloader_history 
            where 
                site_id = _site_id and 
                satellite_id IN (SELECT value::smallint FROM json_array_elements_text($2)) and 
                (_start_date is null or product_date >= _start_date) and 
                (_end_date is null or product_date < _end_date + interval '1 day') and
                status_id in (1, 2, 7))  = 0    -- downloading, not processed or processing 
            and (select count(*) from downloader_history    -- we need to have some products imported
            where 
                site_id = _site_id and 
                satellite_id IN (SELECT value::smallint FROM json_array_elements_text($2))) > 0;
END
$func$ LANGUAGE plpgsql STABLE;