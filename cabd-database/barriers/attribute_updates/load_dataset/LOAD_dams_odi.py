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
tempTableRaw = "dams_odi_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_odi"
workingTable = workingSchema + "." + workingTableRaw

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_odi.py <datafile>")
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
ALTER TABLE {tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {tempTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {tempTable} ADD COLUMN data_source text;
ALTER TABLE {tempTable} ADD COLUMN "comments" text;

UPDATE {tempTable} SET dam_name_en = DAM_NAME;
UPDATE {tempTable} SET owner = DAM_OWNERSHIP;
UPDATE {tempTable} SET ownership_type_code = 
    CASE
    WHEN DAM_OWNERSHIP = 'Conservation Authority' THEN 1
    WHEN DAM_OWNERSHIP = 'Federal' THEN 2
    WHEN DAM_OWNERSHIP = 'Municipal' THEN 3
    WHEN DAM_OWNERSHIP = 'Private' THEN 4
    WHEN DAM_OWNERSHIP = 'Provincial' THEN 5
    WHEN DAM_OWNERSHIP = 'Ontario Power Generation' THEN 5
    ELSE NULL END;
UPDATE {tempTable} SET data_source = 'ODI_' || ogf_id;
UPDATE {tempTable} SET "comments" = GENERAL_COMMENTS;

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
CREATE TABLE {workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    ownership_type_code int2,
    data_source text PRIMARY KEY,
    comments text
);
INSERT INTO {workingTable}(
    dam_name_en,
    "owner",
    ownership_type_code,
    data_source,
    comments
)
SELECT
    dam_name_en,
    "owner",
    ownership_type_code,
    data_source,
    comments
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "owner",
    DROP COLUMN ownership_type_code,
    DROP COLUMN comments;

    """

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_odi AS odi
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	odi.data_source = duplicates.data_source
	OR odi.data_source = duplicates.dups_odi;
    """

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script Complete")
print("Data loaded into table: " + workingTable)