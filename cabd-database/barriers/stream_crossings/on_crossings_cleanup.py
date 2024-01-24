import sys
import argparse
import configparser
import ast
import getpass
import psycopg2 as pg2

#-- PARSE COMMAND LINE ARGUMENTS --
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
args = parser.parse_args()
configfile = args.c

#-- READ PARAMETERS FOR CONFIG FILE --
config = configparser.ConfigParser()
config.read(configfile)

#database settings
dbHost = config['DATABASE']['host']
dbPort = config['DATABASE']['port']
dbName = config['DATABASE']['name']
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

#output data schema
schema = config['DATABASE']['data_schema']
cabdSRID = config['DATABASE']['cabdSRID']

mSRID  = config['SETTINGS']['mSRID']
mGeometry = config['SETTINGS']['mGeometry']

#data tables
#set to an empty dict if doesn't exist for data
rail = ast.literal_eval(config['DATASETS']['railTable'])
roads = ast.literal_eval(config['DATASETS']['roadsTable'])
resourceRoads = ast.literal_eval(config['DATASETS']['resourceRoadsTable'])
trail = ast.literal_eval(config['DATASETS']['trailTable'])

all_datasets = rail | roads | resourceRoads | trail

railTable = [k for k in rail]
roadsTable = [k for k in roads]
resourceRoadsTable = [k for k in resourceRoads]
trailTable = [k for k in trail]

railTable = None if not railTable else railTable
roadsTable = None if not roadsTable else roadsTable
resourceRoadsTable = None if not resourceRoadsTable else resourceRoadsTable
trailTable = None if not trailTable else trailTable

#geometry and unique id fields from the above tables
#id MUST be an integer
geometry = config['DATASETS']['geometryField'].strip()
id = config['DATASETS']['idField'].strip()

#all source transport layers to be used for computing crossings
layers = [k for k in all_datasets]

#non-rail layers - these are included in the clustering
#these should be in order of priority for assigning ids to point
nonRailLayers = ast.literal_eval(config['DATASETS']['nonRailLayers'])

#rail layers - these are clustered separately from other features
railLayers = ast.literal_eval(config['DATASETS']['railLayers'])

print ("-- Processing Parameters --")
print (f"Database: {dbHost}:{dbPort}/{dbName}")
print (f"Data Schema: {schema}")
print (f"CABD SRID: {cabdSRID}")
print (f"Meters Projection: {mSRID} ")

print (f"Data Tables: {railTable} {roadsTable} {resourceRoadsTable} {trailTable} ")
print (f"Id/Geometry Fields: {id} {geometry}")

print ("----")

# Ontario-specific additional datasets
railStructureLine = 'nrwn_on_structure_ln'
railStructurePt = 'nrwn_on_structure_pt'

#--
#-- function to execute a query
#--
def executeQuery(connection, sql):
    #print (sql)
    with connection.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()

#--
#-- checks if the first column of the first row
#-- of the query results is 0 otherwise
# -- ends the program
def checkEmpty(connection, sql, error):
    with connection.cursor() as cursor:
        cursor.execute(sql)
        count = cursor.fetchone()
        if count[0] != 0:
            print ("ERROR: " + error)
            sys.exit(-1)


# -- MAIN SCRIPT --

print("Connecting to database...")

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

print("Cleaning up points on rail lines converted to trails...")

sql = f"SELECT count(*) FROM {schema}.modelled_crossings WHERE {mGeometry} is null;"
checkEmpty(conn, sql, f"modelled_crossings table should not have any rows with null values in {mGeometry}")


sql = f"""
--find points within 5 m of each other

DROP TABLE IF EXISTS {schema}.close_points;

CREATE TABLE {schema}.close_points AS (
	with clusters as (
	SELECT id, transport_feature_source, transport_feature_id, trail_name, trail_association, {mGeometry},
      ST_ClusterDBSCAN({mGeometry}, eps := 5, minpoints := 2) OVER() AS cid
	FROM {schema}.modelled_crossings
    where transport_feature_source in ('{railTable[0]}', '{trailTable[0]}'))
select * from clusters
where cid is not null
order by cid asc);

--add the trail attributes to rail crossings so we don't lose name and owner
UPDATE {schema}.close_points a
set trail_name = b.trail_name,
	trail_association = b.trail_association
from {schema}.close_points b
where b.cid = a.cid
and a.transport_feature_source = '{railTable[0]}'
and b.transport_feature_source = '{trailTable[0]}';

--get points to keep and remove the others
alter table {schema}.modelled_crossings add column if not exists "comments" varchar(256);

with keep AS (
	select distinct on (i.cid) i.*
	FROM {schema}.close_points i
	where transport_feature_source = '{railTable[0]}'
	order by i.cid),
clusters AS (
	select distinct cid from {schema}.close_points
	where transport_feature_source = '{trailTable[0]}'
	)
update {schema}.modelled_crossings a
set
    "comments" = 'rail line converted to a trail',
    trail_name = b.trail_name,
    trail_association = b.trail_association
from {schema}.close_points b
where a.id in (select id from keep where cid in (select cid from clusters))
and a.id = b.id
and a.transport_feature_source = '{railTable[0]}';

with keep AS (
	select distinct on (i.cid) i.*
	FROM {schema}.close_points i
	where transport_feature_source = '{railTable[0]}'
	order by i.cid)
delete from {schema}.modelled_crossings
where id in (select id from {schema}.close_points)
and transport_feature_source = '{trailTable[0]}';

DROP TABLE IF EXISTS {schema}.close_points;
"""
executeQuery(conn, sql)

sql = f"""
	with clusters as (
	SELECT id, transport_feature_source, transport_feature_id, {mGeometry},
      ST_ClusterDBSCAN({mGeometry}, eps := 5, minpoints := 2) OVER() AS cid
	FROM {schema}.modelled_crossings
    where transport_feature_source in ('{railTable[0]}', '{trailTable[0]}'))

    select count(*) from clusters
    where cid is not null;
"""
checkEmpty(conn, sql, "There are still rail and trail crossings within 5 m of each other that need to be removed")

print("Removing crossings on winter roads and other invalid road types...")

sql = f"""
DELETE FROM {schema}.modelled_crossings WHERE transport_feature_source = '{resourceRoadsTable[0]}' AND water_crossing_removed_ind = 'Yes';
DELETE FROM {schema}.modelled_crossings WHERE transport_feature_source = '{resourceRoadsTable[0]}' AND national_road_class = 'Winter';

DELETE FROM {schema}.modelled_crossings WHERE transport_feature_source = '{roadsTable[0]}' AND road_class = 'Winter';
DELETE FROM {schema}.modelled_crossings WHERE transport_feature_source = '{roadsTable[0]}' AND road_element_type IN ('FERRY CONNECTION', 'VIRTUAL ROAD');
"""
executeQuery(conn, sql)

print("Mapping column names to modelled crossings data structure...")

sql = f"""
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_type varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_name varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_type varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_surface varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_owner varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS railway_operator varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS num_railway_tracks varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_condition varchar;

UPDATE {schema}.modelled_crossings SET transport_feature_type = 
    CASE
    WHEN transport_feature_source = '{railTable[0]}' THEN 'rail'
    WHEN transport_feature_source = '{roadsTable[0]}' THEN 'road'
    WHEN transport_feature_source = '{resourceRoadsTable[0]}' THEN 'resource road'
    WHEN transport_feature_source = '{trailTable[0]}' THEN 'trail'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_name = 
    CASE
    WHEN transport_feature_source = '{roadsTable[0]}' THEN official_street_name
    WHEN transport_feature_source = '{resourceRoadsTable[0]}' THEN road_name
    WHEN (transport_feature_source = '{trailTable[0]}' OR trail_name IS NOT NULL) THEN trail_name
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = 
    CASE
    WHEN transport_feature_source = '{roadsTable[0]}' THEN road_class
    WHEN transport_feature_source = '{resourceRoadsTable[0]}' THEN national_road_class
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_surface = surface_type;

UPDATE {schema}.modelled_crossings SET transport_feature_owner = 
    CASE
    WHEN (transport_feature_source = '{railTable[0]}' AND  trail_association IS NULL) THEN ownerena
    WHEN transport_feature_source = '{resourceRoadsTable[0]}' THEN responsibility_class
    WHEN (transport_feature_source = '{trailTable[0]}' OR trail_association IS NOT NULL) THEN trail_association
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET railway_operator = operatoena;
UPDATE {schema}.modelled_crossings SET num_railway_tracks = numtracks;
UPDATE {schema}.modelled_crossings SET transport_feature_condition = status;

"""
executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ALTER COLUMN transport_feature_id TYPE varchar;
UPDATE {schema}.modelled_crossings SET transport_feature_id = 
    CASE
    WHEN transport_feature_source = '{railTable[0]}' THEN nrwn_nid::varchar
    WHEN transport_feature_source = '{roadsTable[0]}' THEN orn_ogf_id::varchar
    WHEN transport_feature_source = '{resourceRoadsTable[0]}' THEN mnrf_ogf_id::varchar
    WHEN transport_feature_source = '{trailTable[0]}' THEN otn_ogf_id::varchar
    ELSE NULL END;
"""
executeQuery(conn, sql)

for k in all_datasets:
    print(k)
    if k:
        attributeValues = all_datasets[k]
    else:
        continue

    print("Attribute Values:", attributeValues)

    for val in attributeValues:
        sql = f"ALTER TABLE {schema}.modelled_crossings DROP COLUMN IF EXISTS {val};"
        executeQuery(conn, sql)

print(f"""Getting additional structure information from {railTable[0]}""")

sql = f"""
--find structure points within 25 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_structure_points;

CREATE TABLE {schema}.temp_structure_points AS (
    SELECT DISTINCT ON (s.nid) s.nid AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{railStructurePt} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 25)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

--find structure lines within 1 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_structure_lines;

CREATE TABLE {schema}.temp_structure_lines AS (
    SELECT DISTINCT ON (s.nid) s.nid AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{railStructureLine} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 1)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;
UPDATE {schema}.modelled_crossings SET crossing_type = 'culvert' FROM {schema}.{railStructurePt} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_points) AND s.structype ILIKE '%culvert%';
UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge' FROM {schema}.{railStructureLine} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_lines) AND s.structype ILIKE '%bridge%';

DROP TABLE {schema}.temp_structure_points;
DROP TABLE {schema}.temp_structure_lines;

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge' WHERE strahler_order >= 6 AND crossing_type IS NULL;

ALTER TABLE {schema}.modelled_crossings ADD CONSTRAINT {schema}_modelled_crossings PRIMARY KEY (id);
"""
executeQuery(conn, sql)

sql = f"""
drop table if exists {schema}.parallel_crossings;

create table {schema}.parallel_crossings as (
	select * from (
		select id, chyf_stream_id, transport_feature_id, geometry
			, row_number() over (partition by chyf_stream_id, transport_feature_id order by id desc) as rn
			, count(*) over (partition by chyf_stream_id, transport_feature_id) cn 
		from {schema}.modelled_crossings
	) t where cn > 1
	order by cn desc
);

grant select on {schema}.parallel_crossings to cwf_user;
"""
executeQuery(conn, sql)

print("** CLEANUP COMPLETE **")
