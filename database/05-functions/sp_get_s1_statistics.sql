-- FUNCTION: reports.sp_get_s1_statistics(smallint)
-- DROP FUNCTION IF EXISTS reports.sp_get_s1_statistics(smallint);

CREATE OR REPLACE FUNCTION reports.sp_get_s1_statistics(
	site_id smallint)
    RETURNS TABLE(site smallint, downloader_history_id integer, orbit_id integer, acquisition_date date, acquisition character varying, acquisition_status character varying, intersection_date date, intersected_product character varying, intersected_status smallint, intersection double precision, polarisation character varying, l2_product character varying, l2_coverage double precision, status_reason character varying) 
    LANGUAGE 'plpgsql'
    COST 100
    STABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	RETURN QUERY
    WITH d AS (select dh.*,ds.status_description from public.downloader_history dh join public.downloader_status ds on ds.id = dh.status_id)
--procesarile
select 	$1 as site,
	d.id,
	d.orbit_id as orbit, 
 	to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
	d.product_name as acquisition,
 	d.status_description as acquisition_status,
 	--to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
	to_date(substr(split_part(di.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
 	--i.product_name as intersected_product,
	di.product_name as intersected_product,
 	--i.status_id as intersected_status,
	cast(i.status_id as smallint) as intersected_status,
 	--st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100 as intersection,
	st_area(st_intersection(di.footprint, d.footprint)) / st_area(d.footprint) * 100 as intersection,
 	split_part(p.name, '_', 6)::character varying as polarisation,
 	p.name as l2_product,
 	st_area(st_intersection(d.footprint, p.geog))/st_area(d.footprint) * 100 as l2_coverage,
 	d.status_reason
	from d
	--join public.downloader_history i on i.site_id = d.site_id AND i.orbit_id = d.orbit_id AND i.satellite_id = d.satellite_id and st_intersects(d.footprint, i.footprint) AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 5 AND 7 AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
    join public.l1_tile_history i
		 on d.id=i.downloader_history_id
	join public.downloader_history di 
	 	 on di.product_name =i.tile_id
	join public.product p on p.downloader_history_id = d.id
	WHERE NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
					 	 WHERE sr.downloader_history_id = d.id  
							--AND sr.intersected_product = i.product_name
					          AND sr.intersected_product = di.product_name 
							--AND sr.site_id = i.site_id
					 		  AND sr.site_id = di.site_id
							  AND sr.l2_product = p.name)
		and d.site_id = $1 
		AND d.satellite_id = 3 
		--and i.id is not null
		and di.id is not null
		--and p.name like concat('%', substr(split_part(i.product_name, '_', 6), 1, 15),'%')
		and p.name like concat('%', substr(split_part(di.product_name, '_', 6), 1, 15),'%')
		
union

/*select 	$1 as site,
	d.id,
	d.orbit_id as orbit, 
 	to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
	d.product_name as acquisition,
 	d.status_description as acquisition_status,
 	--to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
	to_date(substr(split_part(di.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
 	--i.product_name as intersected_product,
	di.product_name as intersected_product,
 	--i.status_id as intersected_status,
	cast(i.status_id as smallint) as intersected_status,
 	--st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100 as intersection,
	st_area(st_intersection(di.footprint, d.footprint)) / st_area(d.footprint) * 100 as intersection,
 	split_part(p.name, '_', 6)::character varying as polarisation,
 	p.name as l2_product,
 	st_area(st_intersection(d.footprint, p.geog))/st_area(d.footprint) * 100 as l2_coverage,
 	d.status_reason
	from d
	--join public.downloader_history i on i.site_id = d.site_id AND i.orbit_id = d.orbit_id AND i.satellite_id = d.satellite_id and st_intersects(d.footprint, i.footprint) AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 11 AND 13 AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
	join public.l1_tile_history i
		 on d.id=i.downloader_history_id
	join public.downloader_history di 
	 	 on di.product_name =i.tile_id
	join public.product p on p.downloader_history_id = d.id
	left outer join public.product_stats ps on ps.product_id = p.id
	WHERE NOT EXISTS(SELECT sr.* FROM reports.s1_report sr
					 WHERE sr.downloader_history_id = d.id 
					 --AND sr.intersected_product = i.product_name
					 AND sr.intersected_product = di.product_name 
					 --AND sr.site_id = i.site_id 
					 AND sr.site_id = di.site_id
					 AND sr.l2_product = p.name)
		and d.site_id = $1 
		AND d.satellite_id = 3 
		--and i.id is not null
		and di.id is not null 
		--and left(d.product_name, 3) = left(i.product_name, 3)
	    and left(d.product_name, 3) = left(di.product_name, 3)
		--and p.name like concat('%', substr(split_part(i.product_name, '_', 6), 1, 15),'%')
		and p.name like concat('%', substr(split_part(di.product_name, '_', 6), 1, 15),'%')
union
*/
/*select 	$1 as site,
	d.id,
	d.orbit_id as orbit,
	to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
	d.product_name as acquisition,
 	--ds.status_description as acquisition_status,
	d.status_description as acquisition_status,
 	--to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
	to_date(substr(split_part(di.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
 	--i.product_name as intersected_product,
	di.product_name as intersected_product,
	--i.status_id as intersected_status,
 	cast(i.status_id as smallint) as intersected_status,
 	--case when i.footprint is null then null else st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100 end as intersection,
 	case when di.footprint is null then null else st_area(st_intersection(di.footprint, d.footprint)) / st_area(d.footprint) * 100 end as intersection,
	null as polarisation,
 	null as l2_product,
 	null as l2_coverage,
 	null as status_reason
	--from public.downloader_history d
	from  d
		--join public.downloader_status ds on ds.id = d.status_id
		--left outer join public.downloader_history i on i.site_id = d.site_id AND i.orbit_id = d.orbit_id AND i.satellite_id = d.satellite_id and st_intersects(d.footprint, i.footprint) AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 5 AND 7 AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
		left outer join public.l1_tile_history i
			on d.id=i.downloader_history_id
		left outer join public.downloader_history di 
			on di.product_name =i.tile_id
	where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
					 WHERE sr.downloader_history_id = d.id)
		and d.site_id = $1 
		AND d.satellite_id = 3 
		and d.status_id != 5
*/

select 	$1 as site,
	d.id,
	d.orbit_id as orbit,
	to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
	d.product_name as acquisition,
 	d.status_description as acquisition_status,
 	to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
 	i.product_name as intersected_product,
 	i.status_id as intersected_status,
 	case when i.footprint is null then null else st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) * 100 end as intersection,
 	null as polarisation,
 	null as l2_product,
 	null as l2_coverage,
 	null as status_reason
	from  d
		left outer join public.downloader_history i 
			ON i.site_id = d.site_id 
				AND i.orbit_id = d.orbit_id 
				AND i.satellite_id = d.satellite_id 
				and st_intersects(d.footprint, i.footprint) 
				AND DATE_PART('day', d.product_date - i.product_date) BETWEEN 5 AND 7 
				AND st_area(st_intersection(i.footprint, d.footprint)) / st_area(d.footprint) > 0.05
	where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr WHERE sr.downloader_history_id = d.id) 
		AND d.site_id = $1 
		AND d.satellite_id = 3 
		--and d.status_id != 5
		AND d.status_id NOT IN (5,6,7,8) -- produse care nu au intrari in l1_tile_history
--5	"processed"
--6	"processing_failed"
--7	"processing"
--8	"processing_cld_failed"

union

	--produse cu status_id=5(processed) in tabela downloader_history, 
	--care au intersectii in tabela l1_tile_history cu status_id=3(done), 
	--dar care nu se regasesc in tabela product. 
	--fals procesate
select 	$1 as site,
	d.id,
	d.orbit_id as orbit,
	to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
	d.product_name as acquisition,
	d.status_description as acquisition_status,
	to_date(substr(split_part(di.product_name, '_', 6), 1, 8),'YYYYMMDD') as intersection_date,
	di.product_name as intersected_product,
 	cast(i.status_id as smallint) as intersected_status,
    case when di.footprint is null then null else st_area(st_intersection(di.footprint, d.footprint)) / st_area(d.footprint) * 100 end as intersection,
	null as polarisation,
 	null as l2_product,
 	null as l2_coverage,
 	null as status_reason
	from  d
		 join public.l1_tile_history i -- au intersectii
			on d.id=i.downloader_history_id
		 join public.downloader_history di 
			on di.product_name =i.tile_id
		left outer join product p
			on d.id=p.downloader_history_id 
	where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
					 WHERE sr.downloader_history_id = d.id)
		and d.site_id =$1
		AND d.satellite_id = 3 
		and d.status_id = 5 --au status_id=5(processed)
		and p.id is null-- nu se gasesc in tabela product
		
union

	--produse cu status_id=5(processed) in tabela downloader_history
	--dar care nu au intersectii in tabela l1_tile_history 
	--fals procesate
select 	$1 as site,
	d.id,
	d.orbit_id as orbit,
	to_date(substr(split_part(d.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date, 
	d.product_name as acquisition,
	d.status_description as acquisition_status,
	null as intersection_date,
	null as intersected_product,
 	null as intersected_status,
    null as intersection,
	null as polarisation,
 	null as l2_product,
 	null as l2_coverage,
 	null as status_reason
	from  d
		 left outer join public.l1_tile_history i
			on d.id=i.downloader_history_id
	where NOT EXISTS(SELECT sr.* FROM reports.s1_report sr 
					 WHERE sr.downloader_history_id = d.id)
		and d.site_id =$1
		AND d.satellite_id = 3 
		and d.status_id = 5 --au status_id=5(processed)
		and i.downloader_history_id is null; --dar nu au intersectii
END
$BODY$;

ALTER FUNCTION reports.sp_get_s1_statistics(smallint)
    OWNER TO postgres;
