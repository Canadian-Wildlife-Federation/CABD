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

# TO DO: add this logic

print("** CLEANUP COMPLETE **")