create extension postgis;
create extension "uuid-ossp";
create extension postgres_fdw;

CREATE SERVER chyf_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'cabd-postgres-dev.postgres.database.azure.com', 
        port '5432', dbname 'chyf', sslmode 'require');
        
CREATE USER MAPPING FOR public
        SERVER chyf_server
        OPTIONS (user 'chyf@cabd-postgres-dev', password 'GJyF9F2B^]C8yFqG');            
        
CREATE FOREIGN TABLE chyf_flowpath (
 region_id character varying(32),     
 type character varying,    
 rank character varying,        
 length double precision ,        
 name character varying,         
 nameid character varying ,        
 geometry geometry(LineString,4326) 
)
SERVER chyf_server
OPTIONS (schema_name 'chyf', table_name 'flowpath');  

grant select on chyf_flowpath to public;

CREATE OR REPLACE FUNCTION cabd.snap_to_network(src_schema varchar, src_table varchar, raw_geom varchar, snapped_geom varchar, max_distance_m double precision) RETURNS VOID AS $$
BEGIN
 EXECUTE 'UPDATE ' || src_schema || '.' || src_table || ' SET ' || snapped_geom || ' =  foo.snapped
 FROM
(
  SELECT DISTINCT ON (cabd_id)
      cabd_id,
      ST_LineInterpolatePoint(fp_geometry, ST_LineLocatePoint(fp_geometry, pnt_geometry) ) as snapped
  FROM
(
      SELECT
          pnts.cabd_id as cabd_id,
          fp.geometry as fp_geometry,
          pnts.' || raw_geom || ' as pnt_geometry,
          ST_Distance(fp.geometry, pnts.' || raw_geom || ') AS distance
      FROM
        ' || src_schema || '.' || src_table || ' pnts,
          chyf_flowpath fp
      WHERE
		pnts.original_point is not null and 
		st_expand(pnts.original_point, 0.01) && fp.geometry
      ORDER BY cabd_id, distance
  ) AS subquery
 ) as foo where foo.cabd_id = ' || src_schema || '.' || src_table || '.cabd_id
  AND st_distance(foo.snapped::geography, ' || src_schema || '.' || src_table || '.' || raw_geom || '::geography) < ' || max_distance_m;
END;
$$ LANGUAGE plpgsql; 

