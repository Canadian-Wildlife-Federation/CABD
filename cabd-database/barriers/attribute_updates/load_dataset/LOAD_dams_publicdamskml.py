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
tempTableRaw = "dams_publicdamskml_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_publicdamskml"
workingTable = workingSchema + "." + workingTableRaw

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_publicdamskml.py <datafile>")
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
ALTER TABLE {tempTable} RENAME COLUMN height_m TO height_m_orig;
ALTER TABLE {tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {tempTable} ADD COLUMN height_m float4;
ALTER TABLE {tempTable} ADD COLUMN length_m float4;
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET dam_name_en = dam_name;
UPDATE {tempTable} SET construction_year = commissioned_year::numeric;
UPDATE {tempTable} SET "owner" = dam_owner;
UPDATE {tempTable} SET operating_status_code =
    CASE 
    WHEN dam_operation_code = 'Abandoned' THEN 1
    WHEN dam_operation_code = 'Active' THEN 2
    WHEN dam_operation_code IN ('Decommissioned', 'Removed') THEN 3
    WHEN dam_operation_code = 'Deactivated' THEN 4
    WHEN dam_operation_code IN ('Application', 'Not Constructed', 'Breached (Failed)') THEN 5
    ELSE NULL END;
UPDATE {tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'Concrete Arch' THEN 1
    WHEN dam_type = 'Buttress' THEN 2
    WHEN dam_type = 'Earthfill' THEN 3
    WHEN dam_type = 'Concrete Gravity' THEN 4
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Steel' THEN 7
    WHEN dam_type = 'Log Crib' THEN 8
    WHEN dam_type IN ('Concrete', 'Dugout/Pond', 'Other') THEN 10
    ELSE NULL END;
UPDATE {tempTable} SET height_m = height_m_orig::float4;
UPDATE {tempTable} SET length_m = crest_length_m::float4;
UPDATE {tempTable} SET data_source = 'Public_Dams_KML_' || objectid;

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    construction_year numeric,
    "owner" varchar(512),
    operating_status_code int2,
    construction_type_code int2,
    height_m float4,
    length_m float4,
    data_source text PRIMARY KEY
);
INSERT INTO {workingTable}(
    dam_name_en,
    construction_year,
    "owner",
    operating_status_code,
    construction_type_code,
    height_m,
    length_m,
    data_source
)
SELECT
    dam_name_en,
    construction_year,
    "owner",
    operating_status_code,
    construction_type_code,
    height_m,
    length_m,
    data_source
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN "owner",
    DROP COLUMN operating_status_code,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m,
    DROP COLUMN length_m;

"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_publicdamskml AS publicdamskml
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	publicdamskml.data_source = duplicates.data_source
	OR publicdamskml.data_source = duplicates.dups_publicdamskml;
"""

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)