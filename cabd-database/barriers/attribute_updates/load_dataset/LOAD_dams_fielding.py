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
tempTableRaw = "dams_fielding_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_fielding"
workingTable = workingSchema + "." + workingTableRaw

dataFile = ""

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_fielding.py <datafile>")
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
    cursor.execute(query)
conn.commit()

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
ALTER TABLE {tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {tempTable} ADD COLUMN use_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {tempTable} ADD COLUMN function_code int2;
ALTER TABLE {tempTable} ADD COLUMN "comments" text;
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET dam_name_en = name_of_structure;
UPDATE {tempTable} SET construction_year =
    CASE
    WHEN regexp_match(year_constructed, '^[0-9]{{4}}$') IS NOT NULL THEN year_constructed::numeric
    WHEN regexp_match(year_constructed, '^~[0-9]{{4}}$') IS NOT NULL THEN (regexp_replace(year_constructed, '[^0-9]', '', 'g'))::numeric
    WHEN regexp_match(year_constructed, '^[0-9]{{4}}-[0-9]{{1,4}}$') IS NOT NULL THEN SPLIT_PART(year_constructed, '-', 1)::numeric
    WHEN regexp_match(year_constructed, '^(circa)\s[0-9]{{4}}$') IS NOT NULL THEN (regexp_replace(year_constructed, '[^0-9]', '', 'g'))::numeric
    WHEN year_constructed = 'April, 1985' THEN 1985
    WHEN year_constructed = 'Aug. 1st/1989' THEN 1989
    ELSE NULL END;
UPDATE {tempTable} SET use_code =
    CASE
    WHEN main_purpose_of_dam = 'water supply - hydroelectric' THEN 2
    WHEN main_purpose_of_dam = 'Water supply - municipal' THEN 3
    WHEN primary_function_of_dam = 'Aboiteau or other flood reduction structure' THEN 4
    WHEN primary_function_of_dam = 'Navigation aid' THEN 6
    WHEN primary_function_of_dam = 'Mine tailings management' THEN 8
    ELSE NULL END;
UPDATE {tempTable} SET use_electricity_code =
    CASE
    WHEN main_purpose_of_dam = 'water supply - hydroelectric' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET use_supply_code = 
    CASE
    WHEN main_purpose_of_dam ILIKE 'Water supply%' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET use_floodcontrol_code = 
    CASE
    WHEN primary_function_of_dam = 'Aboiteau or other flood reduction structure' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET use_recreation_code = 
    CASE
    WHEN main_purpose_of_dam = 'Non consumptive - aquatic recreation enhancement' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET use_navigation_code = 
    CASE
    WHEN primary_function_of_dam = 'Navigation aid' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET use_pollution_code = 
    CASE
    WHEN primary_function_of_dam = 'Mine tailings management' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET function_code = 
    CASE
    WHEN primary_function_of_dam = 'Water impoundment/storage' THEN 1
    ELSE NULL END;
UPDATE {tempTable} SET "comments" = main_purpose_of_dam;
UPDATE {tempTable} SET data_source = 'fielding_' || dam_id_number;

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
    use_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_recreation_code int2,
    use_navigation_code int2,
    use_pollution_code int2,
    function_code int2,
    "comments" text,
    data_source text PRIMARY KEY
);
INSERT INTO {workingTable}(
    dam_name_en,
    construction_year,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_pollution_code,
    function_code,
    "comments",
    data_source
)
SELECT
    dam_name_en,
    construction_year,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_pollution_code,
    function_code,
    "comments",
    data_source
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN use_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN use_pollution_code,
    DROP COLUMN function_code,
    DROP COLUMN "comments";

"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_fielding AS fielding
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	fielding.data_source = duplicates.data_source
	OR fielding.data_source = duplicates.dups_fielding;
"""

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)