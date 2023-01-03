# This script loads a CSV into the database containing new data sources
# This must be completed BEFORE running map_dam_updates.py
# That script will look in the cabd.data_source table for the unique ids for each data source
# 
# IMPORTANT: You must review your CSV for encoding issues before import. The expected encoding
# for your CSV is UTF-8 and the only non-unicode characters allowed are French characters
# This can be easily done by opening up the CSV in Visual Studio Code or another code editor.
# You should also check for % signs in your CSV. SharePoint's CSV exporter changes # signs
# to % signs, so you will need to find and replace these instances.
# Avoid replacing legitimate % signs in your CSV.

import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbName = "cabd"
dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbUser = sys.argv[3]
dbPassword = sys.argv[4]

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw
# print(sourceTable)

updateSchema = "cabd"
updateTable = updateSchema + '.data_source'

if len(sys.argv) != 5:
    print("Invalid usage: py load_data_sources.py <dataFile> <tableName> <dbUser> <dbPassword>")
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

updateQuery = f"""

--add source type and generate uuids
ALTER TABLE {sourceTable} ADD COLUMN source_type varchar;
ALTER TABLE {sourceTable} ADD COLUMN id uuid;

UPDATE {sourceTable} SET last_updated = NULL WHERE last_updated = 'n.d.';
ALTER TABLE {sourceTable} ALTER COLUMN last_updated TYPE date USING last_updated::date;

UPDATE {sourceTable} SET source_type = 'non-spatial';
UPDATE {sourceTable} SET id = gen_random_uuid();

--add records to data source table
ALTER TABLE {updateTable} ADD CONSTRAINT unique_name UNIQUE (name);

INSERT INTO {updateTable}
    (id,
    name,
    version_date,
    source,
    source_type)
SELECT
    id,
    data_source_short_name,
    last_updated,
    reference,
    source_type
FROM {sourceTable}
ON CONFLICT DO NOTHING;

ALTER TABLE cabd.data_source DROP CONSTRAINT unique_name;
"""

print("Cleaning CSV and adding records to " + updateTable)
# print(updateQuery)
with conn.cursor() as cursor:
    cursor.execute(updateQuery)
conn.commit()
conn.close()

print("Script complete")