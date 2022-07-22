import psycopg2 as pg2
import sys
import subprocess
import os
from nhn_data_qa import run_qa

#alternate ogr option
ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";
#ogr = "C:\\Program Files\\QGIS 3.22.3\\bin\\ogr2ogr.exe"


if len(sys.argv) != 8:
    print("Invalid Usage: nhn_data_load.py <host> <port> <dbname> <dbuser> <dbpassword> <nhnworkunit> <filename>")
    
dbHost = sys.argv[1]
dbPort = sys.argv[2]
dbName = sys.argv[3]
dbUser = sys.argv[4]
dbPassword = sys.argv[5]

nhnworkunit = sys.argv[6].upper()
nhnzipfile = sys.argv[7]

snaptogrid = 0.0000001


def log(message):
    if (1):
        print(message);

#temporary schema for loading and manipulating data 
workingSchema = "nhn" + nhnworkunit.lower();
log("loading data into: " + workingSchema)

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort);
                   #sslmode='require')


with conn.cursor() as cursor:
    cursor.execute("DROP SCHEMA IF EXISTS " + workingSchema + " CASCADE");
    cursor.execute("CREATE SCHEMA " + workingSchema);
conn.commit();

#load files into db
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"


my_env = os.environ.copy()
#my_env["PGCLIENTENCODING"] = "LATIN1"

#flowpaths
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.eflowpath" -lco GEOMETRY_NAME=geometry -nlt LINESTRING "' + nhnzipfile + '" "NHN_HN_NLFLOW_1"' 
log(pycmd)
subprocess.run(pycmd, env=my_env)

#waterbodies
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.ecatchment" -lco GEOMETRY_NAME=geometry -nlt POLYGON "' + nhnzipfile + '" "NHN_HD_WATERBODY_2"'
log(pycmd)
subprocess.run(pycmd, env=my_env)

#workunit limit
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.aoi" -lco GEOMETRY_NAME=geometry -nlt POLYGON "' + nhnzipfile + '" "NHN_WORKUNIT_LIMIT_2"'
log(pycmd)
subprocess.run(pycmd, env=my_env)

#COASTLINE
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.shoreline" -lco GEOMETRY_NAME=geometry -nlt LINESTRING "' + nhnzipfile + '" "NHN_HN_LITTORAL_1"'
log(pycmd)
subprocess.run(pycmd, env=my_env)
      
#DELIMITORS
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingSchema + '.delimiter" -lco GEOMETRY_NAME=geometry -nlt LINESTRING "' + nhnzipfile + '" "NHN_HN_DELIMITER_1"'
log(pycmd)
subprocess.run(pycmd, env=my_env)

#determine SRID of datasets
srid = None
query = f"""
select srid from public.geometry_columns where f_table_schema = '{workingSchema}' and f_table_name = 'eflowpath';
"""
log(query)
with conn.cursor() as cursor:
    cursor.execute(query)
    srid = cursor.fetchone()[0]

if (srid is None):
    print ("ERROR: unable to determine SRID for eflowpath table")
    sys.exit();


#transform data
query = f"""
--create aoi and update referenced tables
ALTER TABLE {workingSchema}.aoi ADD column id uuid default uuid_generate_v4() not null;
ALTER TABLE {workingSchema}.aoi RENAME column datasetname to name;
ALTER TABLE {workingSchema}.aoi ADD column status varchar default 'READY';

ALTER TABLE {workingSchema}.eflowpath add column aoi_id uuid;
UPDATE {workingSchema}.eflowpath set aoi_id = (SELECT id from {workingSchema}.aoi);

ALTER TABLE {workingSchema}.ecatchment add column aoi_id uuid;
UPDATE {workingSchema}.ecatchment set aoi_id = (SELECT id from {workingSchema}.aoi);

CREATE TABLE IF NOT EXISTS {workingSchema}.shoreline(internal_id uuid primary key, geometry geometry(LineString, {srid}));
CREATE TABLE IF NOT EXISTS {workingSchema}.delimiter(geometry geometry(LineString, {srid}));

-- add delimiters to shoreline merging as required
WITH merged AS (
SELECT st_linemerge(st_collect(geometry)) as geom 
FROM {workingSchema}."delimiter" WHERE delimitertype = 3
)
INSERT INTO {workingSchema}.shoreline(geometry) 
SELECT st_geometryn(geom, generate_series(1, st_numgeometries(geom))) FROM merged;

ALTER TABLE {workingSchema}.shoreline add column aoi_id uuid;
UPDATE {workingSchema}.shoreline set aoi_id = (SELECT id from {workingSchema}.aoi);

--add id column
ALTER TABLE {workingSchema}.shoreline add column id uuid default uuid_generate_v4();
ALTER TABLE {workingSchema}.ecatchment add column id uuid default uuid_generate_v4();
ALTER TABLE {workingSchema}.eflowpath add column id uuid default uuid_generate_v4();

--eflowpath
--nhnflowpath type  -1 Unknown , 0 - None, 1 - Observed, 2 - Inferred, 3 - Constructed 
--eftype 1 - REACH, 2-BANK, 3-SKELETON, 4-INFRASTRUCTURE
--efsubtype  #10 - Observed, 20 - Inferred, 99 - Unspecified

ALTER TABLE {workingSchema}.eflowpath add column ef_type smallint CHECK (ef_type IN (1,2,3,4));
ALTER TABLE {workingSchema}.eflowpath add column ef_subtype smallint CHECK (ef_subtype IN (10, 20, 99));
ALTER TABLE {workingSchema}.eflowpath add column direction_known smallint CHECK (direction_known IN (1, -1));
UPDATE {workingSchema}.eflowpath set ef_type = CASE WHEN networkFlowType = 2 THEN 3 ELSE 1 END;
UPDATE {workingSchema}.eflowpath set ef_subtype = CASE
  WHEN networkFlowType = 3 THEN 20
  WHEN networkFlowType = 2 THEN null
  WHEN networkFlowType IN (-1, 0) THEN 99
  ELSE 10 END;

--fix empty strings in name fields
UPDATE {workingSchema}.eflowpath SET geographicalnamedb = TRIM(geographicalnamedb);
UPDATE {workingSchema}.eflowpath SET geographicalnamedb = NULL WHERE geographicalnamedb = '';
UPDATE {workingSchema}.eflowpath SET nameid1 = NULL WHERE TRIM(nameid1) = '';
UPDATE {workingSchema}.eflowpath SET nameid2 = NULL WHERE TRIM(nameid2) = '';
UPDATE {workingSchema}.eflowpath SET name1 = NULL WHERE TRIM(name1) = '';
UPDATE {workingSchema}.eflowpath SET name2 = NULL WHERE TRIM(name2) = '';

--digitized in opposite direction
UPDATE {workingSchema}.eflowpath set geometry = reverse(geometry) where flowdirection = 2;
UPDATE {workingSchema}.eflowpath set direction_known = case when flowdirection = 1 or flowdirection = 2 then 1 else -1 end;

--ecatchment
ALTER TABLE {workingSchema}.ecatchment add column ec_type smallint CHECK (ec_type IN (1,2,3,4,5));
ALTER TABLE {workingSchema}.ecatchment add column ec_subtype smallint CHECK (ec_subtype IN (10, 11, 12, 20, 30, 40, 41, 50, 90, 99));
ALTER TABLE {workingSchema}.ecatchment add column is_reservoir boolean default false;

UPDATE {workingSchema}.ecatchment set ec_type = 4;

--nhn 0 - not available, 1 - canal, 2-conduit, 3-ditch,4-lake,5-reservoir,6-watercourse,7-tidal river, 8-liquid waste
--also found 10=sidechannel in nhn data but not documented
--chyf 99 - unknown, 41 - canal, 50-conduit, 41-ditch,10-lake,40-watercourse,30-tidal river, 12-liquid waste
UPDATE {workingSchema}.ecatchment set is_reservoir = true WHERE waterdefinition = 5;
UPDATE {workingSchema}.ecatchment set ec_subtype = CASE 
WHEN waterdefinition = 0 then 99
WHEN waterdefinition = 1 then 41
WHEN waterdefinition = 2 then 50
WHEN waterdefinition = 3 then 41
WHEN waterdefinition = 4 then 10
WHEN waterdefinition = 5 then 10
WHEN waterdefinition = 6 then 40
WHEN waterdefinition = 7 then 20
WHEN waterdefinition = 8 then 12
WHEN waterdefinition = 10 then 40
ELSE 99 END;

--fix empty strings in name fields
UPDATE {workingSchema}.ecatchment SET geographicalnamedb = TRIM(geographicalnamedb);
UPDATE {workingSchema}.ecatchment SET geographicalnamedb = NULL WHERE geographicalnamedb = '';
UPDATE {workingSchema}.ecatchment SET lakeid1 = NULL WHERE TRIM(lakeid1) = '';
UPDATE {workingSchema}.ecatchment SET lakeid2 = NULL WHERE TRIM(lakeid2) = '';
UPDATE {workingSchema}.ecatchment SET riverid1 = NULL WHERE TRIM(riverid1) = '';
UPDATE {workingSchema}.ecatchment SET riverid2 = NULL WHERE TRIM(riverid2) = '';
UPDATE {workingSchema}.ecatchment SET lakename1 = NULL WHERE TRIM(lakename1) = '';
UPDATE {workingSchema}.ecatchment SET lakename2 = NULL WHERE TRIM(lakename2) = '';
UPDATE {workingSchema}.ecatchment SET rivername1 = NULL WHERE TRIM(rivername1) = '';
UPDATE {workingSchema}.ecatchment SET rivername2 = NULL WHERE TRIM(rivername2) = '';
 
--snap to grid to deal with noding problems
UPDATE {workingSchema}.eflowpath set geometry = ST_RemoveRepeatedPoints(st_snaptogrid(geometry, {snaptogrid}));
UPDATE {workingSchema}.ecatchment set geometry = ST_RemoveRepeatedPoints(st_snaptogrid(geometry, {snaptogrid}));
UPDATE {workingSchema}.aoi set geometry = ST_RemoveRepeatedPoints(st_snaptogrid(geometry, {snaptogrid}));
UPDATE {workingSchema}.shoreline set geometry = ST_RemoveRepeatedPoints(st_snaptogrid(geometry, {snaptogrid}));
 
--populate terminal points table the best we can
CREATE TABLE {workingSchema}.terminal_node(id uuid primary key, aoi_id uuid, flow_direction int, geometry geometry(POINT, {srid}));
 
--eflowpath intersections with aoi
with points as (
select distinct st_pointn(geometry, generate_series(1, st_numpoints(geometry)  )) as geometry
from (
 SELECT st_exteriorring(geometry) as geometry FROM {workingSchema}.aoi 
  UNION
  SELECT st_interiorringn(geometry, generate_series(1, st_numinteriorrings(geometry)))  as geometry FROM {workingSchema}.aoi
) foo),
startend as (
select st_startpoint(geometry) as geometry from {workingSchema}.eflowpath where ef_type in (1,4)
union 
select st_endpoint(geometry) from {workingSchema}.eflowpath where ef_type in (1,4)
)
INSERT INTO {workingSchema}.terminal_node(id, geometry)
select uuid_generate_v4(), points.geometry 
from points where geometry in (select geometry from startend);

--ecatchment intersections with aoi

--point intersections
INSERT INTO {workingSchema}.terminal_node(id, geometry) 
SELECT uuid_generate_v4(), geometry
FROM
(
  SELECT st_geometryn(geometry, generate_series(1, st_numgeometries(geometry)))  as geometry
  FROM 
  (
    SELECT st_intersection(a.geometry, b.geometry) as geometry
    FROM {workingSchema}.ecatchment a,  
    (
      select st_exteriorring(geometry) as geometry from {workingSchema}.aoi 
      union
      select st_interiorringn(geometry, generate_series(1, st_numinteriorrings(geometry)))  as geometry from {workingSchema}.aoi
    ) b
    WHERE a.ec_type in (4) and a.geometry && b.geometry and st_intersects(a.geometry, b.geometry)
  ) foo
) bar 
WHERE upper(st_geometrytype(geometry)) = 'ST_POINT';


    
-- linear intersections
INSERT INTO {workingSchema}.terminal_node(id, geometry) 
with eintersect as (
  select st_Geometryn(geometry, generate_series(1, st_numgeometries(geometry))) as geometry, uuid_generate_v4() as uuid
  from(
    select st_linemerge(st_intersection(a.geometry, b.geometry)) as geometry
    from {workingSchema}.ecatchment a,  
    (
      select st_exteriorring(geometry) as geometry from {workingSchema}.aoi 
      union
      select st_interiorringn(geometry, generate_series(1, st_numinteriorrings(geometry)))  as geometry from {workingSchema}.aoi
    ) b
    where a.ec_type in (4) and a.geometry && b.geometry and st_intersects(a.geometry, b.geometry)
  ) as foo
)
select uuid_generate_v4(), st_closestpoint(st_union(geom), st_centroid(rawg))
from (
select st_pointn(geometry, generate_series(1, st_numpoints(geometry))) as geom, uuid, geometry as rawg from eintersect) a
group by uuid, rawg;


UPDATE {workingSchema}.terminal_node SET flow_direction = z.fd
FROM (
select b.id, e.geometry, case when e.flowdirection != 1 then null when st_equals(st_startpoint(e.geometry), b.geometry) then 1 else 2 end as fd
from {workingSchema}.eflowpath e , {workingSchema}.terminal_node b 
where e.geometry && b.geometry and 
(st_equals(st_startpoint(e.geometry), b.geometry) or st_equals(st_endpoint(e.geometry), b.geometry))
) as z where z.id = {workingSchema}.terminal_node.id;

UPDATE {workingSchema}.terminal_node set aoi_id = (SELECT id from {workingSchema}.aoi);
UPDATE {workingSchema}.terminal_node set geometry = st_snaptogrid(geometry, {snaptogrid});

ALTER SCHEMA {workingSchema} OWNER TO chyf;
ALTER TABLE {workingSchema}.aoi OWNER TO chyf;
ALTER TABLE {workingSchema}.delimiter OWNER TO chyf;
ALTER TABLE {workingSchema}.ecatchment OWNER TO chyf;
ALTER TABLE {workingSchema}.eflowpath OWNER TO chyf;
ALTER TABLE {workingSchema}.shoreline OWNER TO chyf;
ALTER TABLE {workingSchema}.terminal_node OWNER TO chyf;

"""
log(query)

with conn.cursor() as cursor:
    cursor.execute(query);
conn.commit();

run_qa(conn, nhnworkunit)

print ("LOAD DONE")
