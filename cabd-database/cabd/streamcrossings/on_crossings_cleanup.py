import psycopg2 as pg2
import sys
import argparse
import configparser


#-- PARSE COMMAND LINE ARGUMENTS --  
parser = argparse.ArgumentParser(description='Processing stream crossings.')
parser.add_argument('-c', type=str, help='the configuration file', required=False)
parser.add_argument('-user', type=str, help='the username to access the database')
parser.add_argument('-password', type=str, help='the password to access the database')
args = parser.parse_args()
configfile = "config.ini"
if (args.c):
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

print("Cleaning up points on rail lines converted to trails...")

sql = f"SELECT count(*) FROM {schema}.modelled_crossings WHERE {mGeometry} is null;"
checkEmpty(conn, sql, "modelled_crossings table should not have any rows with null values in {mGeometry}")


sql = f"""
--find points within 1 m of each other

DROP TABLE IF EXISTS {schema}.close_points;

CREATE TABLE {schema}.close_points AS (
	with clusters as (
	SELECT id, transport_feature_source, transport_feature_id, {mGeometry},
      ST_ClusterDBSCAN({mGeometry}, eps := 1, minpoints := 2) OVER() AS cid
	FROM {schema}.modelled_crossings)
select * from clusters
where cid is not null
order by cid asc);

--get points to keep and remove the others
alter table {schema}.modelled_crossings add column if not exists "comments" varchar(256);

with keep AS (
	select distinct on (i.cid) i.*
	FROM {schema}.close_points i
	where transport_feature_source = '{railTable}'
	order by i.cid),
clusters AS (
	select distinct cid from {schema}.close_points
	where transport_feature_source = '{trailTable}'
	)
update {schema}.modelled_crossings
set "comments" = 'rail line converted to a trail'
where id in (select id from keep where cid in (select cid from clusters));

with keep AS (
	select distinct on (i.cid) i.*
	FROM {schema}.close_points i
	where transport_feature_source = '{railTable}'
	order by i.cid)
delete from {schema}.modelled_crossings
where id not in (select id from keep)
and id in (select id from {schema}.close_points);

DROP TABLE IF EXISTS {schema}.close_points;
"""

executeQuery(conn, sql)

sql = f"""
	with clusters as (
	SELECT id, transport_feature_source, transport_feature_id, {mGeometry},
      ST_ClusterDBSCAN({mGeometry}, eps := 1, minpoints := 2) OVER() AS cid
	FROM {schema}.modelled_crossings)

    select count(*) from clusters
    where cid is not null;
"""
checkEmpty(conn, sql, "There are still modelled crossings within 1 m of each other that need to be removed")

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
    WHEN transport_feature_source = '{railTable}' THEN 'rail'
    WHEN transport_feature_source = '{roadsTable}' THEN 'road'
    WHEN transport_feature_source = '{resourceRoadsTable}' THEN 'resource road'
    WHEN transport_feature_source = '{trailTable}' THEN 'trail'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_name = 
    CASE
    WHEN transport_feature_source = '{roadsTable}' THEN official_street_name
    WHEN transport_feature_source = '{resourceRoadsTable}' THEN road_name
    WHEN transport_feature_source = '{trailTable}' THEN trail_name
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = 
    CASE
    WHEN transport_feature_source = '{roadsTable}' THEN road_class
    WHEN transport_feature_source = '{resourceRoadsTable}' THEN national_road_class
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_surface = surface_type;

UPDATE {schema}.modelled_crossings SET transport_feature_owner = 
    CASE
    WHEN transport_feature_source = '{railTable}' THEN ownerena
    WHEN transport_feature_source = '{resourceRoadsTable}' THEN responsibility_class
    WHEN transport_feature_source = '{trailTable}' THEN trail_association
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET railway_operator = operatoena;
UPDATE {schema}.modelled_crossings SET num_railway_tracks = numtracks;
UPDATE {schema}.modelled_crossings SET transport_feature_condition = status;

"""

executeQuery(conn, sql)

print("Getting additional information from",railTable,"...")

sql = f"""
--find structure points within 25 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_structure_points;

CREATE TABLE {schema}.temp_structure_points AS (
    SELECT DISTINCT ON (s.nid) s.nid AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.nrwn_on_structure_pt s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 25)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

--find structure lines within 1 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_structure_lines;

CREATE TABLE {schema}.temp_structure_lines AS (
    SELECT DISTINCT ON (s.nid) s.nid AS structure_id, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.nrwn_on_structure_ln s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 1)
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;
UPDATE {schema}.modelled_crossings SET crossing_type = lower(s.structype) FROM {schema}.nrwn_on_structure_pt s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_points);
UPDATE {schema}.modelled_crossings SET crossing_type = lower(s.structype) FROM {schema}.nrwn_on_structure_ln s WHERE id IN (SELECT modelled_id FROM {schema}.temp_structure_lines);

DROP TABLE {schema}.temp_structure_points;
DROP TABLE {schema}.temp_structure_lines;

"""
executeQuery(conn, sql)

print("** CLEANUP COMPLETE **")