import psycopg2 as pg2
import sys
import argparse
import configparser
import ast


#-- PARSE COMMAND LINE ARGUMENTS --  
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=True)
parser.add_argument('-user', type=str, help='the username to access the database')
parser.add_argument('-password', type=str, help='the password to access the database')
args = parser.parse_args()
configfile = args.c

#-- READ PARAMETERS FOR CONFIG FILE -- 
config = configparser.ConfigParser()
config.read(configfile)

#database settings 
dbHost = config['DATABASE']['host']
dbPort = config['DATABASE']['port']
dbName = config['DATABASE']['name']
dbUser = args.user
dbPassword = args.password

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
# Yukon Territory specific additional datasets
railStructureLine = 'nrwn_yt_structure_ln'
railStructurePt = 'nrwn_yt_structure_pt'
strucCulvert = 'yt_struc_culvert'
drainCulvert = 'yt_drain_culvert'

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
        if (count[0] != 0):
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

print("Removing crossings on ice roads...")

sql = f"""
DELETE FROM {schema}.modelled_crossings WHERE "closing" = 'Summer' OR "roadclass" = 'Winter';
"""
executeQuery(conn,sql)

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
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;
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
UPDATE {schema}.modelled_crossings SET transport_feature_name =
    CASE
    WHEN r_stname_c IS NOT NULL AND r_stname_c != 'None'
        THEN r_stname_c
    WHEN (r_stname_c IS NULL OR r_stname_c = 'None')
        AND rtename1en IS NOT NULL AND rtename1en != 'None'
        THEN rtename1en
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = roadclass;

UPDATE {schema}.modelled_crossings SET roadway_surface =
    CASE
    WHEN pavstatus = 'Paved' THEN pavstatus
    WHEN unpavsurf IN ('Dirt', 'Gravel') THEN unpavsurf
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_owner = ownerena;

UPDATE {schema}.modelled_crossings SET railway_operator = operatoena;
UPDATE {schema}.modelled_crossings SET num_railway_tracks = numtracks;
UPDATE {schema}.modelled_crossings SET transport_feature_condition = status;

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge' WHERE structtype = 'Bridge';

"""
executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ALTER COLUMN transport_feature_id TYPE varchar;
UPDATE {schema}.modelled_crossings SET transport_feature_id = 
    CASE
    WHEN nrwn_nid IS NOT NULL THEN nrwn_nid::varchar
    WHEN nrn_nid IS NOT NULL THEN nrn_nid::varchar
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

print(f"""Getting additional structure information from {railTable} and culvert datasets""")

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

--find structural culverts within 25 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_struc_culverts;

CREATE TABLE {schema}.temp_struc_culverts AS (
    SELECT DISTINCT ON (s.struc_culvert_id) s.struc_culvert_id AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{strucCulvert} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 25)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

--find drainage culverts within 25 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_drain_culverts;

CREATE TABLE {schema}.temp_drain_culverts AS (
    SELECT DISTINCT ON (s.drain_culvert_id) s.drain_culvert_id AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{drainCulvert} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 25)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

UPDATE {schema}.modelled_crossings SET crossing_type = lower(s.structype) FROM {schema}.{railStructurePt} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_points);
UPDATE {schema}.modelled_crossings SET crossing_type = lower(s.structype) FROM {schema}.{railStructureLine} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_lines);
UPDATE {schema}.modelled_crossings SET crossing_type = 'culvert' WHERE id IN (SELECT modelled_id FROM {schema}.temp_struc_culverts);
UPDATE {schema}.modelled_crossings SET crossing_type = 'culvert' WHERE id IN (SELECT modelled_id FROM {schema}.temp_drain_culverts);

DROP TABLE {schema}.temp_structure_points;
DROP TABLE {schema}.temp_structure_lines;
DROP TABLE {schema}.temp_struc_culverts;
DROP TABLE {schema}.temp_drain_culverts;

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

grant select on {schema}.parallel_crossings to gistech;
"""
executeQuery(conn, sql)

print("** CLEANUP COMPLETE **")