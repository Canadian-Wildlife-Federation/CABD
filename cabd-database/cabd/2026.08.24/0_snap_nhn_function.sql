-- IN CHYF DATABASE:
-- ALTER FOREIGN TABLE chyf_flowpath ALTER COLUMN geometry TYPE public.geometry(LineStringZM, 4617);


CREATE OR REPLACE FUNCTION cabd.snap_point_to_nhn_network(raw_geom geometry, max_distance_m double precision)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$

DECLARE
  fp_geom GEOMETRY;
BEGIN

	SELECT ST_LineMerge(fp.geometry_ls)
	INTO fp_geom
	FROM nhn_raw.flowpaths fp
	WHERE st_expand(raw_geom::geometry, 0.01) && fp.geometry_ls and st_distance(raw_geom::geography, fp.geometry_ls::geography) < max_distance_m
	ORDER BY ST_Distance(fp.geometry_ls::geography, raw_geom::geography)
	LIMIT 1;	

	if (fp_geom is not null) then
		RETURN ST_LineInterpolatePoint(fp_geom, ST_LineLocatePoint(fp_geom, raw_geom));
		--RAISE NOTICE '%s', fp_rec.distance;	
	END IF;
    RETURN NULL;
END;
$function$
;
