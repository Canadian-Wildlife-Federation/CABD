import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbName = "cabd"
dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbUser = sys.argv[3]
dbPassword = sys.argv[4]

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

updateSchema = "featurecopy"
damUpdateTable = updateSchema + '.dam_updates'

if len(sys.argv) != 5:
    print("Invalid usage: py load_dam_updates.py <dataFile> <tableName> <dbUser> <dbPassword>")
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

# loadQuery = f"""

# --TO DO: add code to clean CSV - deal with coded value fields, trim fields, etc.

# --TO DO: create damUpdateTable with proper format if not exists
# --should include all attributes from dams.dams plus lat/long,
# --entry_classification, and data_source

# --TO DO: add remaining fields once this script has been tested
# INSERT INTO {script.damUpdateTable} (
#     cabd_id,
#     entry_classification,
#     data_source,
#     latitude,
#     longitude,
#     use_analysis,
#     dam_name_en
# )
# SELECT
#     cabd_id,
#     entry_classification,
#     data_source,
#     latitude,
#     longitude,
#     use_analysis,
#     dam_name_en
# FROM {script.sourceTable};

# """

# with conn.cursor() as cursor:
#     cursor.execute(loadQuery)
# conn.commit()
conn.close()