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
tempTableRaw = "dams_[datasetid]_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_[datasetid]"
workingTable = workingSchema + "." + workingTableRaw

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_[datasetid].py <datafile>")
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
ALTER TABLE {tempTable} ADD COLUMN [newcolumn] [datatype];
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET [newcolumn] = [sourcecolumn];
UPDATE {tempTable} SET data_source = '[datasetid]_' || [idfield];

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid; --only if it has fid

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {workingTable}(
    cabd_id uuid,
    [newcolumn] [datatype],
    data_source text PRIMARY KEY
);
INSERT INTO {workingTable}(
    [newcolumn],
    data_source
)
SELECT
    [newcolumn],
    data_source
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN [newcolumn];

"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_[datasetid] AS [datasetid]
SET
	cabd_id = duplicates.cabd_id
FROM
	load.duplicates AS duplicates
WHERE
	([datasetid].data_source_id = duplicates.data_source_id AND duplicates.data_source = [datasetid])
	OR [datasetid].data_source_id = duplicates.dups_[datasetid];
"""

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)