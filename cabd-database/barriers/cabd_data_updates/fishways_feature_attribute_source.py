import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dbUser = sys.argv[1]
dbPassword = sys.argv[2]

if dataFile == '':
    print('Data file required. Usage: py fishways_feature_attribute_source.py <dbUser> <dbPassword>')
    sys.exit()

workingSchema = "featurecopy"
workingTableRaw = "fishways"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "fishways_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw
featureTableRaw = "fishways_feature_source"
featureTable = workingSchema + "." + featureTableRaw

print("Connecting to database...")

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

print("Adding data source ids...")
loadQuery = f"""

UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source_text = 'cwf_canfish' THEN (SELECT id FROM cabd.data_source WHERE name = 'cwf_canfish')
    ELSE NULL END;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

print("Adding rows to attribute_source and feature_source tables...")

loadQuery = f"""

TRUNCATE TABLE {featureTable};

--insert any missing rows into attribute_source table (i.e., new features added between import and now)

INSERT INTO {attributeTable} (cabd_id)
	(SELECT cabd_id FROM {workingTable} WHERE cabd_id NOT IN
	(SELECT cabd_id FROM {attributeTable}));

--insert rows into feature_source table from data source columns

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), data_source_id
FROM {workingTable} WHERE data_source_text = 'cwf_canfish'
ON CONFLICT DO NOTHING;

--insert rows into feature_source table from named columns

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), cwf_canfish
FROM {workingTable} WHERE cwf_canfish IS NOT NULL
ON CONFLICT DO NOTHING;

"""

with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()
conn.close()

print("\n" + "Script complete! All attribute source and feature source rows created for " + workingTable)