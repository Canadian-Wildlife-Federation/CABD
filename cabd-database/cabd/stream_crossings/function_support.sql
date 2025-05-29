--
-- given a point geometry looks up the appropriate province territory code
-- from the cabd.province_territory_codes table
--
CREATE OR REPLACE FUNCTION cabd.find_province_territory_code(raw_geom geometry)
 RETURNS varchar
 LANGUAGE plpgsql
AS $function$
DECLARE
  code VARCHAR;
BEGIN
	SELECT a.code
	INTO code
	FROM cabd.province_territory_codes  a
	WHERE ST_INTERSECTS(raw_geom, geometry)
	LIMIT 1;	

    return code;
    
END;
$function$
;

--
-- This function returns the closest point on the CHYF stream network to the provided
-- point. If no flowpath exist within max_distance then null is returned;
-- Max distance is provided in meters.
--
-- DROP FUNCTION cabd.snap_point_to_chyf_network(geometry, float8);
CREATE OR REPLACE FUNCTION cabd.snap_point_to_chyf_network(raw_geom geometry, max_distance_m double precision)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE
  fp_geom GEOMETRY;
BEGIN

	
	SELECT fp.geometry 
	INTO fp_geom
	FROM chyf_flowpath fp
	WHERE ST_DWithin(fp.geometry::geography, raw_geom::geography, max_distance_m)
	ORDER BY ST_Distance(fp.geometry::geography, raw_geom::geography)
	LIMIT 1;	

	if (fp_geom is not null) then
		RETURN ST_LineInterpolatePoint(fp_geom, ST_LineLocatePoint(fp_geom, raw_geom));
		--RAISE NOTICE '%s', fp_rec.distance;	
	END IF;
    RETURN NULL;
END;
$function$
;

--
-- This function returns the closest point on the NHN stream network to the provided
-- point. If no flowpath exist within max_distance then null is returned;
-- Max distance is provided in meters.
-- DROP FUNCTION cabd.snap_point_to_nhn_network(geometry, float8);
CREATE OR REPLACE FUNCTION cabd.snap_point_to_nhn_network(raw_geom geometry, max_distance_m double precision)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$

DECLARE
  fp_geom GEOMETRY;
BEGIN

	SELECT fp.geometry 
	INTO fp_geom
	FROM nhn_raw.flowpaths fp
	WHERE st_expand(raw_geom::geometry, 0.01) && fp.geometry_ls and st_distance(raw_geom::geography, fp.geometry_ls::geography) < max_distance_m
	ORDER BY ST_Distance(fp.geometry::geography, raw_geom::geography)
	LIMIT 1;	

	if (fp_geom is not null) then
		RETURN ST_LineInterpolatePoint(fp_geom, ST_LineLocatePoint(fp_geom, raw_geom));
		--RAISE NOTICE '%s', fp_rec.distance;	
	END IF;
    RETURN NULL;
END;
$function$
;
