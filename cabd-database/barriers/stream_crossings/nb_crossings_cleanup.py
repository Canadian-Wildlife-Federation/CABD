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

print("Mapping column names to modelled crossings data structure...")

sql = f"""
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_subtype varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_subtype_source varchar;
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
UPDATE {schema}.modelled_crossings SET reviewer_status = 'removed', reviewer_comments = 'Automatically removed crossing on dam' WHERE struc_type = 'Dam';
UPDATE {schema}.modelled_crossings SET crossing_subtype = 'bridge', crossing_subtype_source = 'crossing subtype set based on structure type from {roadsTable[0]}' WHERE struc_type ILIKE '%bridge%';

UPDATE {schema}.modelled_crossings SET transport_feature_name =
    CASE
    WHEN track_name_en IS NOT NULL THEN track_name_en
    WHEN formatted_street_name IS NOT NULL THEN formatted_street_name
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_owner = track_owner_name_en;
UPDATE {schema}.modelled_crossings SET railway_operator = operatr_name_en;
UPDATE {schema}.modelled_crossings SET num_railway_tracks = number_of_tracks;
UPDATE {schema}.modelled_crossings SET transport_feature_condition =
    CASE
    WHEN track_status = 467 THEN 'Discontinued'
    WHEN track_status = 468 THEn 'Operational'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = func_road_class;

UPDATE {schema}.modelled_crossings SET roadway_surface =
    CASE
    WHEN paved_status = 'Paved' THEN paved_status
    WHEN unpaved_road_surf_type IN ('Dirt', 'Gravel') THEN unpaved_road_surf_type
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET crossing_subtype = 'bridge', crossing_subtype_source = 'crossing subtype set based on strahler order' WHERE strahler_order >= 6 AND crossing_subtype IS NULL;
"""
executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ALTER COLUMN transport_feature_id TYPE varchar;
UPDATE {schema}.modelled_crossings SET transport_feature_id = 
    CASE
    WHEN transport_feature_source = '{roadsTable[0]}' THEN nid::varchar
    WHEN transport_feature_source = '{railTable[0]}' THEN feature_id::varchar
    WHEN transport_feature_source = '{resourceRoadsTable[0]}' THEN objectid::varchar
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

executeQuery(conn, sql)

sql = f"""

ALTER TABLE {schema}.modelled_crossings ADD CONSTRAINT {schema}_modelled_crossings PRIMARY KEY (id);

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
