# This script inserts any data sources in the staging table into the live data sources table
# This must be run BEFORE running map_dam_updates.py
# That script will look in the cabd.data_source table for the unique ids for each data source
# 
# This script assumes all data sources imported are NON-SPATIAL
# 
# IMPORTANT: You must review your CSV for encoding issues before import. The expected encoding
# for your CSV is UTF-8 and the only non-unicode characters allowed are French characters
# This can be easily done by opening up the CSV in Visual Studio Code or another code editor.
# You should also check for % signs in your CSV. SharePoint's CSV exporter changes # signs
# to % signs, so you will need to find and replace these instances.
# Avoid replacing legitimate % signs in your CSV.

import getpass
import psycopg2 as pg2

# ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"
ogr = "C:\\Program Files\\QGIS 3.22.1\\bin\\ogr2ogr.exe"

# dbHost = "localhost"
# dbPort = "5432"
# dbName = "cabd_dev_2024"

dbHost = "cabd-postgres-prod.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

updateSchema = "cabd"
updateTable = updateSchema + '.data_source'
stagingTable = updateSchema + '.data_source_updates'

conn = pg2.connect(database=dbName,
                   user=dbUser,
                   host=dbHost,
                   password=dbPassword,
                   port=dbPort)

insertQuery = f"""
--add records to data source table
ALTER TABLE {updateTable} ADD CONSTRAINT ds_unique_name UNIQUE (name);

INSERT INTO {updateTable}
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
    name,
    version_date,
    source,
    source_type,
    full_name,
    organization_name,
    data_source_category
FROM {stagingTable}
ON CONFLICT DO NOTHING;

ALTER TABLE cabd.data_source DROP CONSTRAINT ds_unique_name;
"""

print("Adding records to " + updateTable)
with conn.cursor() as cursor:
    cursor.execute(insertQuery)
conn.commit()
conn.close()

print("Script complete")
