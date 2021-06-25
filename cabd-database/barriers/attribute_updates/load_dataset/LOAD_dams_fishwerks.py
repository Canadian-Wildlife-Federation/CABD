import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "localhost"
dbPort = "5432"
dbName = "cabd_dev"
dbUser = "xxxx@cabd_dev"
dbPassword = "xxxx"

#create temporary table and table to be inserted into CABD dataset
tempSchema = "source_data"
tempTableRaw = "dams_fishwerks_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_fishwerks"
workingTable = workingSchema + "." + workingTableRaw

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_fishwerks.py <datafile>")
    sys.exit()


print("Loading data from file " +  dataFile)


conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

query = f"""
CREATE SCHEMA IF NOT EXISTS {tempSchema};
CREATE SCHEMA IF NOT EXISTS {workingSchema};
DROP TABLE IF EXISTS {tempTable};
DROP TABLE IF EXISTS {workingTable};
"""

with conn.cursor() as cursor:
    cursor.execute(query);
conn.commit();

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + tempTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + '"' + dataFile + '"'
print(pycmd)
subprocess.run(pycmd)

#run scripts to convert the data
print("Running mapping queries...")
query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET waterbody_name_en = 'Lake ' || lake;
UPDATE {tempTable} SET data_source = 'fishwerks_' || barrier_id;

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {workingTable}(
    cabd_id uuid,
    waterbody_name_en varchar(512),
    data_source text PRIMARY KEY,
);
INSERT INTO {workingTable}(
    waterbody_name_en,
    data_source
)
SELECT
    waterbody_name_en,
    data_source
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN waterbody_name_en;
"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_fishwerks AS fishwerks
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	fishwerks.data_source = duplicates.data_source
	OR fishwerks.data_source = duplicates.dups_fishwerks;
"""

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)