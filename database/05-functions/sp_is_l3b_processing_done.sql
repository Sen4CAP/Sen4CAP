-- DROP function sp_is_l3b_preprocessing_done(smallint, timestamp, timestamp);
CREATE OR REPLACE FUNCTION sp_is_l3b_preprocessing_done
(
    _site_id site.id%TYPE,
    _start_date timestamp DEFAULT NULL::timestamp,
    _end_date timestamp DEFAULT NULL::timestamp
  )
  RETURNS boolean AS
$func$

BEGIN
    RETURN (select count(*) from sp_get_parent_products_not_in_provenance(_site_id, '[1, 26]', 3, _start_date, _end_date)) = 0;
   
END
$func$ LANGUAGE plpgsql STABLE;