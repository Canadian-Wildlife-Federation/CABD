# This script loads a CSV into the database containing new data sources
# This must be run BEFORE running map_dam_updates.py and AFTER updates have been loaded
#
# This script assumes all data sources imported are NON-SPATIAL
#
# IMPORTANT: You must review your CSV for encoding issues before import. The expected encoding
# for your CSV is UTF-8 and the only non-unicode characters allowed are French characters
# This can be easily done by opening up the CSV in Visual Studio Code or another code editor.
# You should also check for % signs in your CSV. SharePoint's CSV exporter changes # signs
# to % signs, so you will need to find and replace these instances.
# Avoid replacing legitimate % signs in your CSV.

import subprocess
import sys
import getpass
import psycopg2 as pg2

ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"

dbName = "cabd_dev_2023"
dbHost = "localhost"
dbPort = "5433"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

updateSchema = "cabd"
updateTable = updateSchema + '.data_source'
stagingTable = updateSchema + '.data_source_updates'
damUpdateTable = updateSchema + '.dam_updates'
fishwayUpdateTable = updateSchema + '.fishway_updates'
waterfallUpdateTable = updateSchema + '.waterfall_updates'

if len(sys.argv) != 3:
    print("Invalid usage: py load_data_sources.py <dataFile> <tableName>")
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
--remove existing data sources
DELETE FROM {sourceTable} WHERE data_source_short_name IN (SELECT name FROM {updateTable});

ALTER TABLE {sourceTable} ALTER COLUMN submitted_on TYPE timestamptz USING submitted_on::timestamptz;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS "status" varchar;

--TO DO: DEAL WITH ANY CASES WHERE THE FIRST INSTANCE IS MISSING INFO
--remove duplicate data sources, keeping first submitted
--first instance of a data source should always have the full info
WITH cte AS (
  SELECT ogc_fid, data_source_short_name, email,
     row_number() OVER(PARTITION BY data_source_short_name ORDER BY submitted_on ASC) AS rn
  FROM {sourceTable}
)
DELETE FROM {sourceTable}
    WHERE ogc_fid IN (SELECT ogc_fid FROM cte WHERE rn > 1);

--remove any sources that aren't in the update tables
WITH temp AS (
    SELECT DISTINCT data_source_short_name FROM {damUpdateTable}
    UNION
    SELECT DISTINCT data_source_short_name FROM {fishwayUpdateTable}
    UNION
    SELECT DISTINCT data_source_short_name FROM {waterfallUpdateTable}
)
DELETE FROM {sourceTable} WHERE data_source_short_name NOT IN (SELECT DISTINCT data_source_short_name FROM temp);

DELETE FROM {sourceTable} WHERE "status" IN ('complete', 'do not process', 'on hold', 'on hold until next fishway update');

--add source type and generate uuids
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS source_type varchar;
ALTER TABLE {sourceTable} ADD COLUMN id uuid;

UPDATE {sourceTable} SET last_updated = TRIM(last_updated);
UPDATE {sourceTable} SET last_updated = NULL WHERE last_updated = 'n.d.';
ALTER TABLE {sourceTable} ALTER COLUMN last_updated TYPE date USING last_updated::date;

UPDATE {sourceTable} SET source_type = 'non-spatial'; --NOTE THIS ASSUMES ALL DATA SOURCES ARE NON-SPATIAL
UPDATE {sourceTable} SET id = gen_random_uuid();

ALTER TABLE {sourceTable} DROP COLUMN IF EXISTS "item type";
ALTER TABLE {sourceTable} DROP COLUMN IF EXISTS "path";
"""

stageQuery = f"""
CREATE TABLE IF NOT EXISTS {stagingTable} (LIKE {updateTable});

INSERT INTO {stagingTable}
    (id,
    name,
    version_date,
    source,
    source_type,
    full_name,
    organization_name,
    data_source_category)
SELECT
    id,
    data_source_short_name,
    last_updated,
    reference,
    source_type,
    full_name,
    organization_name,
    data_source_category
FROM {sourceTable}
ON CONFLICT DO NOTHING;
"""

print("Cleaning CSV")
with conn.cursor() as cursor:
    cursor.execute(updateQuery)
    print("Adding to staging table")
    cursor.execute(stageQuery)
conn.commit()
conn.close()

print("Script complete")
