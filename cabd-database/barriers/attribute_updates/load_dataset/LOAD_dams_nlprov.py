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
tempTableRaw = "dams_nlprov_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_nlprov"
workingTable = workingSchema + "." + workingTableRaw

dataFile = ""

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_nlprov.py <datafile>")
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
ALTER TABLE {tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {tempTable} ADD COLUMN height_m float4;
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET dam_name_en = dam_name;
UPDATE {tempTable} SET "owner" = owner_name;
UPDATE {tempTable} SET construction_year = year_built::numeric;
UPDATE {tempTable} SET operating_status_code =
    CASE
    WHEN dam_status = 'Abandoned' THEN 1
    WHEN dam_status = 'Active' THEN 2
    WHEN dam_status =  'Decommissioned' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_code =
    CASE 
    WHEN purpose_drinking IN ('Primary', 'Secondary') THEN 3
    WHEN purpose_industrial IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_hydro IN ('Primary', 'Secondary') THEN 2
    WHEN purpose_flood IN ('Primary', 'Secondary') THEN 4
    WHEN purpose_ice IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_forestry IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_unknown IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_other IN ('Primary', 'Secondary') THEN 10
    ELSE NULL END;
UPDATE {tempTable} SET use_electricity_code =
    CASE 
    WHEN purpose_hydro = 'Primary' THEN 1
    WHEN purpose_hydro = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_supply_code =
    CASE
    WHEN purpose_drinking = 'Primary' THEN 1
    WHEN purpose_drinking = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_floodcontrol_code =
    CASE
    WHEN purpose_flood = 'Primary' THEN 1
    WHEN purpose_flood = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_other_code =
    CASE
    WHEN purpose_other = 'Primary' THEN 1
    WHEN purpose_other = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'CBD = Concrete Buttress Dam' THEN 2
    WHEN dam_type = 'RCCG = roller compacted concrete, gravity dam' THEN 4
    WHEN dam_type = 'CAGD = concrete arch gravity dam' THEN 4
    WHEN dam_type = 'RFTC = rock filled timber crib' THEN 8
    WHEN dam_type = 'XX = other type' THEN 10
    WHEN dam_type ILIKE 'CAD = Concrete Arch' THEN 1
    WHEN dam_type ILIKE 'EF = Earthfill' THEN 3
    WHEN dam_type ILIKE 'CGD = Concrete Gravity' THEN 4
    WHEN dam_type ILIKE 'RFCC = Rockfill, Central Core' THEN 6
    WHEN dam_type ILIKE 'CFRD = Concrete Faced Rockfill Dam' THEN 6
    WHEN dam_type IS NULL THEN 9
    ELSE NULL END;
UPDATE {tempTable} SET height_m = dim_max_height;
UPDATE {tempTable} SET data_source = 'nlprov_' || objectid;

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid; --only if it has fid

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    construction_year numeric,
    operating_status_code int2,
    use_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_other_code int2,
    construction_type_code int2,
    height_m float4,
    data_source text PRIMARY KEY
);
INSERT INTO {workingTable}(
    dam_name_en,
    "owner",
    construction_year,
    operating_status_code,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_other_code,
    construction_type_code,
    height_m,
    data_source
)
SELECT
    dam_name_en,
    "owner",
    construction_year,
    operating_status_code,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_other_code,
    construction_type_code,
    height_m,
    data_source
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "owner",
    DROP COLUMN construction_year,
    DROP COLUMN operating_status_code,
    DROP COLUMN use_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_other_code,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m;

"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_nlprov AS nlprov
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	nlprov.data_source = duplicates.data_source
	OR nlprov.data_source = duplicates.dups_nlprov;
"""

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)