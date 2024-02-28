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

mSRID = config['SETTINGS']['mSRID']
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

# Alberta-specific additional datasets
railPt = 'afr_rail_pt'
roadPt = 'afr_road_pt'

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

sql = f"SELECT count(*) FROM {schema}.modelled_crossings WHERE {mGeometry} is null;"
checkEmpty(conn, sql, "modelled_crossings table should not have any rows with null values in {mGeometry}")

print("Removing crossings on winter roads and other invalid road types...")

sql = f"""
--remove winter roads and ferry crossings
UPDATE {schema}.modelled_crossings SET reviewer_status = 'removed', reviewer_comments = 'Automatically removed crossing on ferry route' WHERE afr_road_feature_type = 1;
UPDATE {schema}.modelled_crossings SET reviewer_status = 'removed', reviewer_comments = 'Automatically removed crossing on winter road' WHERE afr_road_feature_type = 12;
"""
executeQuery(conn, sql)

print("Mapping column names to modelled crossings data structure...")

sql = f"""
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type_source varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS num_railway_tracks varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS passability_status varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS railway_operator varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_paved_status varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_surface varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_type varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_condition varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_name varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_owner varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_type varchar;
"""
executeQuery(conn, sql)

if railTable:
    for table in railTable:
        sql = f"""UPDATE {schema}.modelled_crossings SET transport_feature_type = 'rail' WHERE transport_feature_source = '{table}';"""
        executeQuery(conn, sql)

if roadsTable:
    for table in roadsTable:
        sql = f"""UPDATE {schema}.modelled_crossings SET transport_feature_type = 'road' WHERE transport_feature_source = '{table}';"""
        executeQuery(conn, sql)

if resourceRoadsTable:
    for table in resourceRoadsTable:
        sql = f"""UPDATE {schema}.modelled_crossings SET transport_feature_type = 'resource_road' WHERE transport_feature_source = '{table}';"""
        executeQuery(conn, sql)

if trailTable:
    for table in trailTable:
        sql = f"""UPDATE {schema}.modelled_crossings SET transport_feature_type = 'trail' WHERE transport_feature_source = '{table}';"""
        executeQuery(conn, sql)

sql = f"""
UPDATE {schema}.modelled_crossings SET "name" = substring("name", '\S(?:.*\S)*');
UPDATE {schema}.modelled_crossings SET transport_feature_name = "name" WHERE "name" IS NOT NULL;
UPDATE {schema}.modelled_crossings SET transport_feature_condition = 'Abandoned' WHERE afr_rail_feature_type = 1;

UPDATE {schema}.modelled_crossings SET roadway_type = 
    CASE
    WHEN afr_road_feature_type = 2 THEN 'Ford/Winter Crossing'
    WHEN afr_road_feature_type = 3 THEN 'Interchange Ramp'
    WHEN afr_road_feature_type = 4 THEN 'One Lane Gravel Road'
    WHEN afr_road_feature_type = 5 THEN 'Two Lane Gravel Road'
    WHEN afr_road_feature_type = 6 THEN 'Divided Paved Road'
    WHEN afr_road_feature_type = 7 THEN 'One Lane Undivided Paved Road'
    WHEN afr_road_feature_type = 8 THEN 'Two Lane Undivided Paved Road'
    WHEN afr_road_feature_type = 9 THEN 'Four Lane Undivided Paved Road'
    WHEN afr_road_feature_type = 10 THEN 'Driveway'
    WHEN afr_road_feature_type = 14 THEN 'Dry-Weather Road'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_surface = 
    CASE
    WHEN roadway_type ILIKE '%Paved%' THEN 'Paved'
    WHEN roadway_type ILIKE '%Gravel%' THEN 'Gravel'
    ELSE NULL END;

"""
executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ALTER COLUMN transport_feature_id TYPE varchar;
UPDATE {schema}.modelled_crossings SET transport_feature_id = 
    CASE
    WHEN transport_feature_source = '{railTable[0]}' THEN afr_rail_objectid::varchar
    WHEN transport_feature_source = '{roadsTable[0]}' THEN afr_road_objectid::varchar
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

print("Getting additional structure information...")

sql = f"""
--find rail points within 1 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_rail_points;

CREATE TABLE {schema}.temp_rail_points AS (
    SELECT DISTINCT ON (s.objectid) s.objectid AS structure_id, s.feature_type AS feature_type, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{railPt} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 1)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

--find road points within 1 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_road_points;

CREATE TABLE {schema}.temp_road_points AS (
    SELECT DISTINCT ON (s.objectid) s.objectid AS structure_id, s.feature_type AS feature_type, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{roadPt} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 1)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge', crossing_type_source = 'crossing type set based on match from {railPt}'
    FROM {schema}.{railPt} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_rail_points)
    AND s.feature_type IN (2,3);

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge', crossing_type_source = 'crossing type set based on match from {roadPt}'
    FROM {schema}.{roadPt} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_road_points)
    AND s.feature_type = 4;

DROP TABLE {schema}.temp_rail_points;
DROP TABLE {schema}.temp_road_points;

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge', crossing_type_source = 'crossing type set based on strahler order' WHERE strahler_order >= 6 AND crossing_type IS NULL;

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
