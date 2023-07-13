import psycopg2 as pg2
import sys
import argparse
import configparser


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

mSRID = config['SETTINGS']['mSRID']
mGeometry = config['SETTINGS']['mGeometry']
#distance in meters (mSRID projection units) for clustering points
clusterDistance = config['SETTINGS']['clusterDistance']

#data tables
#set to None if doesn't exist for data 
railTable = config['DATASETS']['railTable'].strip()
roadsTable = config['DATASETS']['roadsTable'].strip()
resourceRoadsTable = config['DATASETS']['resourceRoadsTable'].strip()
trailTable = config['DATASETS']['trailTable'].strip()

railTable = None if railTable == "None" else railTable
roadsTable = None if roadsTable == "None" else roadsTable
resourceRoadsTable = None if resourceRoadsTable == "None" else resourceRoadsTable
trailTable = None if trailTable == "None" else trailTable

railAttributes = config['DATASETS']['railAttributes'].strip()
trailAttributes = config['DATASETS']['trailAttributes'].strip()
roadAttributes = config['DATASETS']['roadAttributes'].strip()
resourceRoadsAttributes = config['DATASETS']['resourceRoadsAttributes'].strip()

#geometry and unique id fields from the above tables
#id MUST be an integer 
geometry = config['DATASETS']['geometryField'].strip()
id = config['DATASETS']['idField'].strip()
   

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

print("Removing crossings on winter roads and other invalid road types...")

sql = f"""
--remove winter roads and ferry crossings
DELETE FROM {schema}.modelled_crossings WHERE transport_feature_source = '{roadsTable}' AND afr_road_feature_type IN (1, 12);
"""
executeQuery(conn, sql)

print("Mapping column names to modelled crossings data structure...")

sql = f"""
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_type varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_name varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS transport_feature_condition varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_type varchar;
ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS roadway_surface varchar;

UPDATE {schema}.modelled_crossings SET transport_feature_type = 
    CASE
    WHEN transport_feature_source = '{railTable}' THEN 'rail'
    WHEN transport_feature_source = '{roadsTable}' THEN 'road'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET "name" = substring("name", '\S(?:.*\S)*');
UPDATE {schema}.modelled_crossings SET transport_feature_name = "name" WHERE "name" IS NOT NULL;
UPDATE {schema}.modelled_crossings SET transport_feature_condition = 'Abandoned' WHERE transport_feature_source = '{railTable}' AND afr_rail_feature_type = 1;

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
    WHEN transport_feature_source = '{railTable}' THEN afr_rail_objectid::varchar
    WHEN transport_feature_source = '{roadsTable}' THEN afr_road_objectid::varchar
    ELSE NULL END;
"""
executeQuery(conn, sql)

attributeValues = [railAttributes, roadAttributes]
attributeTables = [railTable, roadsTable]

for i in range(0, len(attributeTables), 1):
    print(attributeTables[i])
    if (attributeTables[i] is None):
        continue

    if (attributeValues[i] is None or attributeValues[i] == ""):
        continue
    
    fields = attributeValues[i].split(",")
    for field in fields:
        sql = f"ALTER TABLE {schema}.modelled_crossings DROP COLUMN IF EXISTS {field};"
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

ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;
UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge'
    FROM {schema}.{railPt} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_rail_points)
    AND s.feature_type IN (2,3);

ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;
UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge'
    FROM {schema}.{roadPt} s WHERE id IN (SELECT modelled_id FROM {schema}.temp_road_points)
    AND s.feature_type = 4;

DROP TABLE {schema}.temp_rail_points;
DROP TABLE {schema}.temp_road_points;

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge' WHERE strahler_order >= 6 AND crossing_type IS NULL;

"""
executeQuery(conn, sql)

print("** CLEANUP COMPLETE **")