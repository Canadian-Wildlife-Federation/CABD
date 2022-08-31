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

#load data using ogr - will overwrite sourceTable if you tried loading it already
orgDb = "dbname='" + dbName + "' host='"+ dbHost +"' port='"+ dbPort + "' user='" + dbUser + "' password='" + dbPassword + "'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" "' + dataFile + '"' + ' -lco OVERWRITE=YES -nln "' + sourceTable + '" -oo AUTODETECT_TYPE=YES'
print(pycmd)
subprocess.run(pycmd)

# loadQuery = f"""

# --TO DO: add code to clean CSV - deal with coded value fields, trim fields, etc.

# --add new data sources and match new uuids for each data source to each record
# ALTER TABLE cabd.data_source ADD CONSTRAINT unique_name (name);
# INSERT INTO cabd.data_source (name, id, source_type)
#     SELECT DISTINCT data_source, gen_random_uuid(), 'non-spatial' FROM {script.sourceTable}
#     ON CONFLICT DO NOTHING;
# ALTER TABLE cabd.data_source DROP CONSTRAINT unique_name;

# --add data source ids to the table
# ALTER TABLE {script.sourceTable} RENAME COLUMN data_source to data_source_text;
# ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
# UPDATE {script.sourceTable} AS s SET data_source = d.id FROM cabd.data_source AS d WHERE d.name = s.data_source_text;

# --TO DO: create damUpdateTable with proper format if not exists
# --should include all attributes from dams.dams plus lat/long,
# --entry_classification, data_source, and data_source_text

# --TO DO: add remaining fields once this script has been tested
# INSERT INTO {script.damUpdateTable} (
#     cabd_id,
#     entry_classification,
#     data_source,
#     data_source_text,
#     latitude,
#     longitude,
#     use_analysis,
#     dam_name_en
# )
# SELECT
#     cabd_id,
#     entry_classification,
#     data_source,
#     data_source_text,
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