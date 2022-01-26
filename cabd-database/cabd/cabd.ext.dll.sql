create extension postgis;
create extension "uuid-ossp";
create extension postgres_fdw;

drop server chyf_server cascade;
CREATE SERVER chyf_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'cabd-postgres-dev.postgres.database.azure.com', 
        port '5432', dbname 'chyf', sslmode 'require', extensions 'postgis');
        
CREATE USER MAPPING FOR public
        SERVER chyf_server
        OPTIONS (user 'chyf@cabd-postgres-dev', password 'XXXXXXXXXXX');            
        
CREATE FOREIGN TABLE chyf_flowpath (
 region_id character varying(32),     
 type character varying,    
 rank character varying,        
 length double precision ,        
 name character varying,         
 nameid character varying ,        
 geometry geometry(LineString,4617) 
)
SERVER chyf_server
OPTIONS (schema_name 'chyf2', table_name 'eflowpath');  

grant select on chyf_flowpath to public;

CREATE OR REPLACE FUNCTION cabd.snap_to_network(src_schema varchar, src_table varchar, raw_geom varchar, snapped_geom varchar, max_distance_m double precision) RETURNS VOID AS $$
DECLARE
  pnt_rec RECORD;
  fp_rec RECORD;
BEGIN

	FOR pnt_rec IN EXECUTE format('SELECT cabd_id, %I as rawg FROM %I.%I WHERE %I is not null', raw_geom, src_schema, src_table,raw_geom) 
	LOOP 
		--RAISE NOTICE '%s: %s', pnt_rec.cabd_id, pnt_rec.rawg;
		FOR fp_rec IN EXECUTE format ('SELECT fp.geometry as geometry, st_distance(%L::geometry::geography, fp.geometry::geography) AS distance FROM chyf_flowpath fp WHERE st_expand(%L::geometry, 0.01) && fp.geometry and st_distance(%L::geometry::geography, fp.geometry::geography) < 50 ORDER BY distance ', pnt_rec.rawg, pnt_rec.rawg, pnt_rec.rawg)
		LOOP
			EXECUTE format('UPDATE %I.%I SET %I = ST_LineInterpolatePoint(%L::geometry, ST_LineLocatePoint(%L::geometry, %L::geometry) ) WHERE cabd_id = %L', src_schema, src_table, snapped_geom,fp_rec.geometry, fp_rec.geometry, pnt_rec.rawg, pnt_rec.cabd_id);
			--RAISE NOTICE '%s', fp_rec.distance;	
			EXIT;
		
		END LOOP;
	END LOOP;
END;
$$ LANGUAGE plpgsql;