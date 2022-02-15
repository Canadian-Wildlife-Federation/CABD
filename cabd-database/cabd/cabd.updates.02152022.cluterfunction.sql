-- function for clusting points
-- potential way to improve is not to pick the closest 
-- point to the center each time for the start point; for the first
-- one it makes sense but for the next ones it might make more sense
--to find something a bit further away so the cluster radius aren't overlapping

CREATE OR REPLACE FUNCTION cabd.cluster_features(
	  viewschema character varying,  viewname character varying, 
	  distance double precision, xmin double precision,
	  ymin double precision, xmax double precision,
	  ymax double precision, srid integer)
returns table (result_geometry geometry(point, 4617), result_cnt integer)
LANGUAGE plpgsql
AS $function$
declare
	tcenter geometry(point, 4617);
	currentpoint geometry(point, 4617);
	currentcnt integer;
	_deletecnt integer;
begin
	EXECUTE format('CREATE TEMPORARY TABLE cluster_temp AS WITH bounds AS (SELECT st_Transform(st_makeenvelope($1,$2,$3,$4,$5), 4617) as geom) SELECT geometry as geom FROM %I.%I, bounds WHERE st_intersects(geometry, bounds.geom)', viewschema,viewname) using xmin, ymin, xmax, ymax, srid;
	tcenter := st_centroid(st_transform(st_makeenvelope(xmin,ymin,xmax,ymax,srid), 4617));
	currentcnt := 0;
	LOOP 
		select count(*) from cluster_temp into currentcnt;
		if currentcnt = 0 then
			exit;
		end if;
		--find nearest point to  center
		select geom from cluster_temp order by geom <-> tcenter limit 1 into currentpoint;
		delete from cluster_temp where st_intersects(geom, st_buffer(currentpoint, distance));
		GET DIAGNOSTICS _deletecnt = ROW_COUNT;
		result_geometry := currentpoint;
		result_cnt := _deletecnt;
		return next;
	END LOOP;
	EXECUTE format('DROP TABLE cluster_temp');
END;
$function$;