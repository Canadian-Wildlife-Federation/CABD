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

# PEI-specific additional datasets
strucPt = 'nssdb_pt'
topoPt = 'nstdb_pt'
topoLn = 'nstdb_ln'
topoPoly = 'nstdb_poly'

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
UPDATE {schema}.modelled_crossings SET transport_feature_name = street WHERE street IS NOT NULL AND street != '-';

UPDATE {schema}.modelled_crossings SET transport_feature_type =
    CASE
    WHEN feat_desc ILIKE '%trail%' THEN 'trail'
    WHEN feat_desc ILIKE '%railroad%' THEN 'rail'
    ELSE transport_feature_type END;

UPDATE {schema}.modelled_crossings SET crossing_type =
    CASE
    WHEN feat_desc ILIKE '%bridge%' THEN 'bridge'
    WHEN nstdb_ln_feat_desc ILIKE '%bridge%' THEN 'bridge'
    WHEN nstdb_ln_feat_desc ILIKE '%culvert%' THEN 'culvert'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = roadc_desc WHERE roadc_desc IS NOT NULL;
UPDATE {schema}.modelled_crossings SET transport_feature_owner = owner_desc WHERE owner_desc IS NOT NULL;
UPDATE {schema}.modelled_crossings SET transport_feature_condition = 'abandoned' WHERE feat_desc ILIKE '%abandoned%';

UPDATE {schema}.modelled_crossings SET roadway_surface =
    CASE
    WHEN feat_desc ILIKE '%- paved%' THEN 'Paved'
    WHEN feat_desc ILIKE '%- unpaved%' THEN 'Unpaved'
    ELSE NULL END;

"""
executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ALTER COLUMN transport_feature_id TYPE varchar;
UPDATE {schema}.modelled_crossings SET transport_feature_id = 
    CASE
    WHEN transport_feature_source = '{roadsTable[0]}' THEN roadsegid::varchar
    WHEN transport_feature_source = '{roadsTable[1]}' THEN nstdb_ln_shape_fid::varchar
    ELSE NULL END;
"""
executeQuery(conn, sql)

for k in all_datasets:
    if k:
        attributeValues = all_datasets[k]
    else:
        continue

    for val in attributeValues:
        sql = f"ALTER TABLE {schema}.modelled_crossings DROP COLUMN IF EXISTS {val};"
        executeQuery(conn, sql)

print("Getting additional structure information...")

sql = f"""
ALTER TABLE {schema}.{topoLn} ADD COLUMN IF NOT EXISTS geometry_m geometry(LineString, {mSRID});
UPDATE {schema}.{topoLn} SET geometry_m = ST_Transform(ST_LineMerge(geometry), {mSRID});

ALTER TABLE {schema}.modelled_crossings ADD COLUMN IF NOT EXISTS crossing_type varchar;

--find NS Structure Database points within 30 m of modelled crossings
DROP TABLE IF EXISTS {schema}.temp_struc_pt;

CREATE TABLE {schema}.temp_struc_pt AS (
    SELECT DISTINCT ON (s.fid) s.fid AS structure_id, s.structurename AS structurename, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{strucPt} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 30)
    AND (s.structurename ILIKE '%bridge%' OR s.structurename ILIKE '%culvert%')
    AND m.crossing_type IS NULL
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

UPDATE {schema}.modelled_crossings SET crossing_type = 'bridge' FROM {schema}.temp_struc_pt s
    WHERE s.modelled_id = id AND s.structurename ILIKE '%bridge%' AND s.structurename NOT ILIKE '%culvert bridge%';

UPDATE {schema}.modelled_crossings SET crossing_type = 'culvert' FROM {schema}.temp_struc_pt s
    WHERE s.modelled_id = id AND s.structurename ILIKE '%culvert%' OR s.structurename ILIKE '%culvert bridge%';

--find NS Topo points within 25 m of modelled crossing
DROP TABLE IF EXISTS {schema}.temp_topo_pt;

CREATE TABLE {schema}.temp_topo_pt AS (
    SELECT DISTINCT ON (s.fid) s.fid AS structure_id, s.feat_desc AS feat_desc, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(s.geometry, m.geometry_m) AS dist, s.geometry
    FROM {schema}.{topoPt} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(s.geometry, m.geometry_m, 25)
    AND m.crossing_type IS NULL
    ORDER BY structure_id, modelled_id, ST_Distance(s.geometry, m.geometry_m)
);

UPDATE {schema}.modelled_crossings SET crossing_type = 'culvert' FROM {schema}.temp_topo_pt s
    WHERE s.modelled_id = id;

--find NS Topo polygons within 20 m of modelled crossing
DROP TABLE IF EXISTS {schema}.temp_topo_poly;

CREATE TABLE {schema}.temp_topo_poly AS (
    SELECT DISTINCT ON (s.fid) s.fid AS structure_id, s.feat_desc AS feat_desc, m.id AS modelled_id, m.transport_feature_source AS transport_feature_source, ST_Distance(ST_Transform(s.geometry, {mSRID}), m.geometry_m) AS dist, ST_Transform(s.geometry, {mSRID})
    FROM {schema}.{topoPoly} s, {schema}.modelled_crossings m
    WHERE ST_DWithin(ST_Transform(s.geometry, {mSRID}), m.geometry_m, 20)
    AND m.crossing_type IS NULL
    ORDER BY structure_id, modelled_id, ST_Distance(ST_Transform(s.geometry, {mSRID}), m.geometry_m)
);

UPDATE {schema}.modelled_crossings SET crossing_type = 'culvert' FROM {schema}.temp_topo_poly s
    WHERE s.modelled_id = id AND s.feat_desc ILIKE '%bridge%';

DROP TABLE {schema}.temp_struc_pt;
DROP TABLE {schema}.temp_topo_pt;
DROP TABLE {schema}.temp_topo_poly;

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
