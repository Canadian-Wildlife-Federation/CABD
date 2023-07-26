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

# Yukon Territory specific additional datasets
railStructureLine = 'nrwn_nt_structure_ln'
railStructurePt = 'nrwn_nt_structure_pt'
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

UPDATE {schema}.modelled_crossings SET transport_feature_type = 
    CASE
    WHEN transport_feature_source = '{railTable}' THEN 'rail'
    WHEN transport_feature_source = '{roadsTable}' THEN 'road'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_name =
    CASE
    WHEN transport_feature_source = '{roadsTable}'
        AND r_stname_c IS NOT NULL AND r_stname_c != 'None'
        THEN r_stname_c
    WHEN transport_feature_source = '{roadsTable}'
        AND (r_stname_c IS NULL OR r_stname_c = 'None')
        AND rtename1en IS NOT NULL AND rtename1en != 'None'
        THEN rtename1en
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = 
    CASE
    WHEN transport_feature_source = '{roadsTable}' THEN roadclass
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_surface =
    CASE
    WHEN pavstatus = 'Paved' THEN pavstatus
    WHEN unpavsurf IN ('Dirt', 'Gravel') THEN unpavsurf
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_owner = 
    CASE
    WHEN transport_feature_source = '{railTable}' THEN ownerena
    ELSE NULL END;

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
    WHEN transport_feature_source = '{railTable}' THEN nrwn_nid::varchar
    WHEN transport_feature_source = '{roadsTable}' THEN nrn_nid::varchar
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

print(f"""Getting additional structure information from {railTable}...""")

sql = f"""
--find structure points within 25 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_structure_points;

CREATE TABLE {schema}.temp_structure_points AS (
    SELECT DISTINCT ON (s.nid) s.nid AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.nrwn_nt_structure_pt s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 25)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

--find structure lines within 1 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_structure_lines;

CREATE TABLE {schema}.temp_structure_lines AS (
    SELECT DISTINCT ON (s.nid) s.nid AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.nrwn_nt_structure_ln s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 1)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

UPDATE {schema}.modelled_crossings SET crossing_type = lower(s.structype) FROM {schema}.nrwn_nt_structure_pt s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_points);
UPDATE {schema}.modelled_crossings SET crossing_type = lower(s.structype) FROM {schema}.nrwn_nt_structure_ln s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_lines);

DROP TABLE {schema}.temp_structure_points;
DROP TABLE {schema}.temp_structure_lines;

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge' WHERE strahler_order >= 6 AND crossing_type IS NULL;

ALTER TABLE {schema}.modelled_crossings ADD CONSTRAINT {schema}_modelled_crossings PRIMARY KEY (id);

"""
executeQuery(conn, sql)

print("** CLEANUP COMPLETE **")