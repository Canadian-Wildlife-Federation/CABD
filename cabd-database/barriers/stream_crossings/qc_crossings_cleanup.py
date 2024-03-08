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
UPDATE {schema}.modelled_crossings SET reviewer_status = 'removed', reviewer_comments = 'Automatically removed crossing on ferry route' WHERE classvoie = 'Transbordeur' OR clsrte = 'Liaison maritime';
UPDATE {schema}.modelled_crossings SET reviewer_status = 'removed', reviewer_comments = 'Automatically removed crossing on winter road' WHERE caractrte = 'Saisonnier';
"""
executeQuery(conn, sql)

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
UPDATE {schema}.modelled_crossings SET transport_feature_type = 
    CASE
    WHEN caractrte = 'Pont ferroviaire' THEN 'rail'
    WHEN caractrte = 'Pont ferroviaire (abandonné) / piéton' THEN 'rail'
    WHEN clsrte = 'Accès aux ressources' THEN 'resource_road'
    WHEN clsrte = 'Accès aux ressources et aux localités isolées' THEN 'resource_road'
    ELSE transport_feature_type END;

UPDATE {schema}.modelled_crossings SET crossing_subtype = 
    CASE
    WHEN caractrte = 'Passerelle piétonnière' THEN 'bridge'
    WHEN caractrte = 'Passerelle piétonnière et cycliste' THEN 'bridge'
    WHEN caractrte = 'Pont couvert' THEN 'bridge'
    WHEN caractrte = 'Pont ferroviaire' THEN 'bridge'
    WHEN caractrte = 'Pont ferroviaire (abandonné) / piéton' THEN 'bridge'
    WHEN caractrte = 'Ponceau' THEN 'culvert'
    WHEN caractrte = 'Pont' THEN 'bridge'
    WHEN caractrte = 'Pont à étagement' THEN 'bridge'
    WHEN typestruct = 'Pont' THEN 'bridge'
    WHEN typestruct = 'Pont mobile' THEN 'bridge'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET crossing_subtype_source = 'crossing subtype set based on structure type from {roadsTable[0]}'
    WHERE caractrte IN ('Passerelle piétonnière', 'Passerelle piétonnière et cycliste', 'Pont couvert', 'Pont ferroviaire', 'Pont ferroviaire (abandonné) / piéton', 'Ponceau', 'Pont', 'Pont à étagement');

UPDATE {schema}.modelled_crossings SET crossing_subtype_source = 'crossing subtype set based on structure type from {railTable[0]}'
    WHERE typestruc IN ('Pont', 'Pont mobile');

UPDATE {schema}.modelled_crossings SET transport_feature_name =
    CASE
    WHEN nomrte IS NOT NULL AND nomrte != '' THEN nomrte
    WHEN nomvoie IS NOT NULL AND nomvoie != '' THEN nomvoie
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET transport_feature_owner =
    CASE
    WHEN gestion IS NOT NULL AND gestion != '' AND gestion NOT ILIKE 'Inconnu' THEN gestion
    WHEN nmfrpropr IS NOT NULL AND nmfrpropr != '' AND nmfrpropr NOT ILIKE 'Inconnu' THEN nmfrpropr
    WHEN nom_club IS NOT NULL AND nom_club != '' THEN nom_club
    WHEN nomgestion IS NOT NULL AND nomgestion != '' THEN nomgestion
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET railway_operator = nmfrexploi WHERE nmfrexploi IS NOT NULL AND nmfrexploi != '' AND nmfrexploi != 'Aucun';

UPDATE {schema}.modelled_crossings SET transport_feature_condition =
    CASE 
    WHEN caractrte = 'Pont ferroviaire (abandonné) / piéton' THEN 'abandoned'
    WHEN etat = 'Abandonné' THEN 'abandoned'
    WHEN etat = 'Opérationnel' THEN 'operational'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_type = 
    CASE
    WHEN clsrte = 'Autoroute' THEN 'Highway'
    WHEN clsrte = 'Nationale' THEN 'National'
    WHEN clsrte = 'Régionale' THEN 'Regional'
    WHEN clsrte = 'Collectrice municipale' THEN 'Collector'
    WHEN clsrte = 'Collectrice de transit' THEN 'Collector'
    WHEN clsrte = 'Accès aux ressources' THEN 'Resource'
    WHEN clsrte = 'Accès aux ressources et aux localités isolées' THEN 'Resource'
    WHEN clsrte = 'Locale' THEN 'Local'
    WHEN clsrte = 'Artère' THEN 'Arterial'
    WHEN clsrte = 'Rue piétonne' THEN 'Pedestrian'
    ELSE NULL END;

UPDATE {schema}.modelled_crossings SET roadway_paved_status = 
    CASE
    WHEN etatrev = 'Revêtue' THEN 'Paved'
    WHEN etatrev = 'Non revêtue' THEN 'Unpaved'
    ELSE NULL END;

"""
executeQuery(conn, sql)

sql = f"""
ALTER TABLE {schema}.modelled_crossings ALTER COLUMN transport_feature_id TYPE varchar;
UPDATE {schema}.modelled_crossings SET transport_feature_id = 
    CASE
    WHEN transport_feature_source = '{railTable[0]}' THEN reseau_ferroviaire_uuid::varchar
    WHEN transport_feature_source = '{roadsTable[0]}' THEN reseau_routier_uuid::varchar
    WHEN transport_feature_source = '{trailTable[0]}' THEN sentiers_quad_fqcq_uuid::varchar
    WHEN transport_feature_source = '{trailTable[1]}' THEN route_verte_uuid::varchar
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
UPDATE {schema}.modelled_crossings SET crossing_subtype = 'bridge', crossing_subtype_source = 'crossing subtype set based on strahler order' WHERE strahler_order >= 6 AND crossing_subtype IS NULL;
ALTER TABLE {schema}.modelled_crossings DROP CONSTRAINT IF EXISTS {schema}_modelled_crossings_pkey;
ALTER TABLE {schema}.modelled_crossings ADD CONSTRAINT {schema}_modelled_crossings_pkey PRIMARY KEY (id);
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
