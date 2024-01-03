# This script loads a CSV into the database containing updated data sources for dams

import subprocess
import sys
import getpass
import psycopg2 as pg2

ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

damSchema = "waterfalls"
damTable = damSchema + ".waterfalls"
updateTable = damSchema + '.waterfalls_feature_source'

if len(sys.argv) != 3:
    print("Invalid usage: py waterfall_feature_source_updates.py <dataFile> <tableName>")
    sys.exit()

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

#clear any data from previous tries
query = f"""
DROP TABLE IF EXISTS {sourceTable};
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

#load data using ogr
orgDb = "dbname='" + dbName + "' host='"+ dbHost +"' port='"+ dbPort + "' user='" + dbUser + "' password='" + dbPassword + "'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" "' + dataFile + '"' + ' -nln "' + sourceTable + '" -oo AUTODETECT_TYPE=YES -oo EMPTY_STRING_AS_NULL=YES'
print(pycmd)
subprocess.run(pycmd)
print("Data loaded to table: " + sourceTable)

query = f"""
--remove any updates for features that will be deleted
DELETE FROM {sourceTable} WHERE cabd_id::uuid IN (
    SELECT cabd_id FROM cabd.waterfall_updates
    WHERE entry_classification = 'delete feature'
);

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    DISTINCT cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'bceccs_fiss'),
    fiss_id
FROM {sourceTable}
WHERE fiss_id IS NOT NULL AND cabd_id::uuid IN (
	SELECT cabd_id FROM {updateTable} WHERE datasource_id = (
		SELECT id FROM cabd.data_source WHERE name = 'bceccs_fiss' AND datasource_feature_id IS NULL
	))
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;
"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Done!")
