-- FUNCTION: reports.sp_reports_s1_statistics(smallint, integer, date, date)

-- DROP FUNCTION IF EXISTS reports.sp_reports_s1_statistics(smallint, integer, date, date);

CREATE OR REPLACE FUNCTION reports.sp_reports_s1_statistics(
	siteid smallint DEFAULT NULL::smallint,
	orbitid integer DEFAULT NULL::integer,
	fromdate date DEFAULT NULL::date,
	todate date DEFAULT NULL::date)
    RETURNS TABLE(calendar_date date, acquisitions integer, failed_to_download integer, pairs integer, processed integer, not_yet_processed integer, falsely_processed integer, no_intersections integer, errors integer, partially_processed integer) 
    LANGUAGE 'plpgsql'
    COST 100
    STABLE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE startDate date;
DECLARE endDate date;
DECLARE temporalOffset smallint;
DECLARE minIntersection decimal;
                BEGIN
                    IF $3 IS NULL THEN
                        SELECT MIN(acquisition_date) INTO startDate FROM reports.s1_report;
                    ELSE
                        SELECT fromDate INTO startDate;
                    END IF;
                    IF $4 IS NULL THEN
                        SELECT MAX(acquisition_date) INTO endDate FROM reports.s1_report;
                    ELSE
                        SELECT toDate INTO endDate;
                    END IF;
	                
					SELECT cast(value as  smallint) INTO temporalOffset FROM config where key='processor.l2s1.temporal.offset';
				    
					SELECT cast(value as  decimal) INTO minIntersection FROM config where key='processor.l2s1.min.intersection';
				
                    RETURN QUERY
                    WITH 	calendar AS 
                            (SELECT date_trunc('day', dd)::date AS cdate 
                                FROM generate_series(startDate::timestamp, endDate::timestamp, '1 day'::interval) dd),
                       ac AS 
                            (SELECT acquisition_date, COUNT(DISTINCT downloader_history_id) AS acquisitions 
                                FROM reports.s1_report 
                                WHERE ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),
								
                       /* p AS 
                            (SELECT acquisition_date, COUNT(intersected_product) AS pairs 
                                FROM reports.s1_report 
                                WHERE ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),*/
						p AS
							(  SELECT to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date,COUNT(*) AS pairs
									FROM public.downloader_history dh
										JOIN public.downloader_history i
											ON dh.site_id = i.site_id 
												AND dh.satellite_id = i.satellite_id 
												AND dh.orbit_id = i.orbit_id
												and  dh.satellite_id=3 
												AND ($1 IS NULL OR dh.site_id = $1) AND ($2 IS NULL OR dh.orbit_id = $2) 
									WHERE ST_INTERSECTS(dh.footprint, i.footprint)
										AND DATE_PART('day', i.product_date - dh.product_date) BETWEEN (temporalOffset -1) AND (temporalOffset + 1)
										AND st_area(st_intersection(dh.footprint, i.footprint)) / st_area(dh.footprint) > minIntersection
										AND to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD') BETWEEN startDate AND endDate
							 	GROUP BY to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD')
                                ORDER BY to_date(substr(split_part(i.product_name, '_', 6), 1, 8),'YYYYMMDD')
							),
							  
                        /*proc AS 
                            (SELECT acquisition_date, COUNT(intersected_product) AS cnt 
                                FROM reports.s1_report 
                                WHERE status_description = 'processed' AND
                                    ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),*/
						--produse procesate: au status processed si toate procesarile pereche au status done
						 productsWithStatusProcessed as(select downloader_history_id,count(distinct intersected_product) as nrIntersections 
															from reports.s1_report 
														    WHERE status_description = 'processed' AND intersected_product IS not NULL 
																AND EXISTS ( SELECT downloader_history_id from product where product.downloader_history_id=reports.s1_report.downloader_history_id)
																AND ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
															group by downloader_history_id),
						 productsWithStatusProcessed_IntersectionsWithStatusDone as
							(SELECT downloader_history_id FROM  productsWithStatusProcessed 
									where productsWithStatusProcessed.nrIntersections = (select count(*) from l1_tile_history 
															where downloader_history_id=productsWithStatusProcessed.downloader_history_id
															and status_id=3)),
						 proc AS 
							 (SELECT acquisition_date, COUNT(distinct reports.s1_report.downloader_history_id) AS cnt 
                                FROM reports.s1_report join  productsWithStatusProcessed_IntersectionsWithStatusDone
							 		on reports.s1_report.downloader_history_id=productsWithStatusProcessed_IntersectionsWithStatusDone.downloader_history_id
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date   
							 ),
						 --produse partial procesate= produse ptr care exista procesari failed sau processing	
						 productsWithStatusProcessed_FailledOrProcessing as
							(SELECT downloader_history_id FROM  productsWithStatusProcessed 
									where exists (select * from l1_tile_history 
															where downloader_history_id=productsWithStatusProcessed.downloader_history_id
															and status_id in (1,2))
							),
						 partially_proc AS 
							 (SELECT acquisition_date, COUNT(distinct reports.s1_report.downloader_history_id) AS cnt 
                                FROM reports.s1_report join productsWithStatusProcessed_FailledOrProcessing
							 		on reports.s1_report.downloader_history_id=productsWithStatusProcessed_FailledOrProcessing.downloader_history_id
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date   
							 ),
							 
                        ndld AS 
                            (SELECT acquisition_date, count(downloader_history_id) AS cnt 
                                FROM reports.s1_report 
                                WHERE status_description IN ('failed','aborted') AND intersected_product IS NULL AND
                                    ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),
							 
                        dld AS
                            (SELECT r.acquisition_date, COUNT(r.downloader_history_id) AS cnt
                                FROM reports.s1_report r
                                WHERE r.status_description IN ('downloaded', 'processing') AND r.intersected_product IS NOT NULL AND
                                    ($1 IS NULL OR r.site_id = $1) AND ($2 IS NULL OR r.orbit_id = $2) AND r.acquisition_date BETWEEN startDate AND endDate
                                     AND NOT EXISTS (SELECT s.downloader_history_id FROM reports.s1_report s
                                                    WHERE s.downloader_history_id = r.downloader_history_id AND r.l2_product LIKE '%COHE%')
                                GROUP BY acquisition_date
                                ORDER BY acquisition_date),
							 
                       /*fproc AS 
                            (SELECT acquisition_date, COUNT(DISTINCT downloader_history_id) AS cnt 
                                FROM reports.s1_report 
                                WHERE status_description = 'processed' AND intersected_product IS NULL AND
                                    ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),*/
						fproc AS 
                            (SELECT acquisition_date, COUNT(distinct downloader_history_id) AS cnt 
                                FROM reports.s1_report 
                                WHERE status_description = 'processed'
							 		AND (intersected_product IS NULL OR NOT EXISTS( SELECT downloader_history_id from product where product.downloader_history_id=reports.s1_report.downloader_history_id))
                                    AND ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),
                       /* ni AS 
                            (SELECT acquisition_date, count(downloader_history_id) AS cnt 
                                FROM reports.s1_report 
                                WHERE status_description = 'downloaded' AND intersected_product IS NULL AND
                                    ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date),*/
						 downh AS
							(  SELECT to_date(substr(split_part(dh.product_name, '_', 6), 1, 8),'YYYYMMDD') as acquisition_date,*
									FROM public.downloader_history dh
										where ($1 IS NULL OR dh.site_id = $1) AND ($2 IS NULL OR dh.orbit_id = $2)	AND dh.satellite_id=3 
							),		
						ni AS
							(  SELECT acquisition_date,COUNT(*) AS cnt
									FROM downh dh
										LEFT OUTER JOIN public.downloader_history i
											ON dh.site_id = i.site_id 
												AND dh.satellite_id = i.satellite_id 
												AND dh.orbit_id = i.orbit_id
												AND ST_INTERSECTS(dh.footprint, i.footprint)
												AND DATE_PART('day', i.product_date - dh.product_date) BETWEEN (temporalOffset - 1) AND (temporalOffset + 1)
												AND st_area(st_intersection(dh.footprint, i.footprint)) / st_area(dh.footprint) > minIntersection
												AND acquisition_date BETWEEN startDate AND endDate
									where i.id is NULL
							 	GROUP BY acquisition_date
                                ORDER BY acquisition_date
							),	
							 
                       /* e AS 
                            (SELECT acquisition_date, COUNT(intersected_product) AS cnt 
                                FROM reports.s1_report 
                                WHERE status_description LIKE 'processing_%failed' AND
                                    ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date)*/
					    --errors: produse cu status processing_failed si cu toate procesarile pereche failed
						 productsWithStatusFailed as(select downloader_history_id,count(distinct intersected_product) as nrIntersections 
														 from reports.s1_report 
														 where status_description='processing_failed'
															 AND ($1 IS NULL OR site_id = $1) AND ($2 IS NULL OR orbit_id = $2) AND acquisition_date BETWEEN startDate AND endDate
														 group by downloader_history_id),
						productsWithStatusFailed_IntersectionsWithStatusFailed AS (select * 
							from productsWithStatusFailed 
									where productsWithStatusFailed.nrIntersections = (select count(*) from l1_tile_history 
															where downloader_history_id=productsWithStatusFailed.downloader_history_id
															and status_id=2)),							 
						e AS 
                            (SELECT acquisition_date, COUNT(distinct reports.s1_report.downloader_history_id) AS cnt 
                                FROM reports.s1_report join  productsWithStatusFailed_IntersectionsWithStatusFailed
							 		on reports.s1_report.downloader_history_id=productsWithStatusFailed_IntersectionsWithStatusFailed.downloader_history_id
                                GROUP BY acquisition_date 
                                ORDER BY acquisition_date)
                    SELECT 	c.cdate, 
                        COALESCE(ac.acquisitions, 0)::integer,
                        COALESCE(ndld.cnt, 0)::integer,
                        COALESCE(p.pairs, 0)::integer, 
                        COALESCE(proc.cnt, 0)::integer, 
                        COALESCE(dld.cnt, 0)::integer,
                        COALESCE(fproc.cnt, 0)::integer,
                        COALESCE(ni.cnt, 0)::integer,
                        COALESCE(e.cnt, 0)::integer,
						COALESCE(partially_proc.cnt, 0)::integer
                    FROM calendar c
                        LEFT JOIN ac ON ac.acquisition_date = c.cdate
                        LEFT JOIN ndld ON ndld.acquisition_date = c.cdate
                        LEFT JOIN p ON p.acquisition_date = c.cdate
                        LEFT JOIN proc ON proc.acquisition_date = c.cdate
                        LEFT JOIN dld ON dld.acquisition_date = c.cdate
                        LEFT JOIN fproc ON fproc.acquisition_date = c.cdate
                        LEFT JOIN ni ON ni.acquisition_date = c.cdate
                        LEFT JOIN e ON e.acquisition_date = c.cdate
						LEFT JOIN partially_proc ON partially_proc.acquisition_date = c.cdate;
                END
$BODY$;

ALTER FUNCTION reports.sp_reports_s1_statistics(smallint, integer, date, date)
    OWNER TO admin;
