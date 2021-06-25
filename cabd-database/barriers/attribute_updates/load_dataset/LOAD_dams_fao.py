import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "localhost"
dbPort = "5432"
dbName = "cabd_dev"
dbUser = "kohearn@cabd_dev"
dbPassword = "denim-snail"

#create temporary table and table to be inserted into CABD dataset
tempSchema = "source_data"
tempTableRaw = "dams_fao_original"
tempTable = tempSchema + "." + tempTableRaw
workingSchema = "load"
workingTableRaw = "dams_fao"
workingTable = workingSchema + "." + workingTableRaw

dataFile = ""

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: LOAD_dams_fao.py <datafile>")
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
ALTER TABLE {tempTable} ADD COLUMN nearest_municipality varchar(512);
ALTER TABLE {tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {tempTable} ADD COLUMN height_m float4;
ALTER TABLE {tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {tempTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {tempTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {tempTable} ADD COLUMN comments text;
ALTER TABLE {tempTable} ADD COLUMN data_source text;

UPDATE {tempTable} SET dam_name_en = name_of_dam;
UPDATE {tempTable} SET nearest_municipality = nearest_city;
UPDATE {tempTable} SET waterbody_name_en =
    CASE
    WHEN regexp_match(river, '.*River.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Creek.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Falls.*') IS NOT NULL THEN river
    WHEN river IS NULL THEN NULL
    ELSE (river || ' River') END;
UPDATE {tempTable} SET construction_year = 
    CASE
    WHEN completed_operational_since = 'Incomplete?' THEN NULL
    ELSE completed_operational_since::numeric END;
UPDATE {tempTable} SET height_m = dam_height_m;
UPDATE {tempTable} SET storage_capacity_mcm = reservoir_capacity_million_m3;
UPDATE {tempTable} SET reservoir_area_skm = reservoir_area_km2;
UPDATE {tempTable} SET use_irrigation_code = 
    CASE
    WHEN irrigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_supply_code = 
    CASE
    WHEN water_supply = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_floodcontrol_code = 
    CASE
    WHEN flood_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_electricity_code = 
    CASE
    WHEN hydroelectricity_mw = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_navigation_code = 
    CASE
    WHEN navigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_recreation_code = 
    CASE
    WHEN recreation = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_pollution_code = 
    CASE
    WHEN pollution_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET use_other_code =
    CASE
    WHEN other = 'x' THEN 3
    ELSE NULL END;
UPDATE {tempTable} SET "comments" = comments_orig;
UPDATE {tempTable} SET data_source = 'fao_' || id_fao;

ALTER TABLE {tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {tempTable} DROP CONSTRAINT {tempTableRaw}_pkey;
ALTER TABLE {tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    nearest_municipality varchar(512),
    waterbody_name_en varchar(512),
    construction_year numeric,
    height_m float4,
    storage_capacity_mcm float8,
    reservoir_area_skm float4,
    use_irrigation_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_electricity_code int2,
    use_navigation_code int2,
    use_recreation_code int2,
    use_pollution_code int2,
    use_other_code int2,
    comments text,
    data_source text PRIMARY KEY
);
INSERT INTO {workingTable}(
    dam_name_en,
    nearest_municipality,
    waterbody_name_en,
    construction_year,
    height_m,
    storage_capacity_mcm,
    reservoir_area_skm,
    use_irrigation_code,
    use_supply_code,
    use_floodcontrol_code,
    use_electricity_code,
    use_navigation_code,
    use_recreation_code,
    use_pollution_code,
    use_other_code,
    "comments",
    data_source
)
SELECT
    dam_name_en,
    nearest_municipality,
    waterbody_name_en,
    construction_year,
    height_m,
    storage_capacity_mcm,
    reservoir_area_skm,
    use_irrigation_code,
    use_supply_code,
    use_floodcontrol_code,
    use_electricity_code,
    use_navigation_code,
    use_recreation_code,
    use_pollution_code,
    use_other_code,
    "comments",
    data_source
FROM {tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN nearest_municipality,
    DROP COLUMN waterbody_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN height_m,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN reservoir_area_skm,
    DROP COLUMN use_irrigation_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_pollution_code,
    DROP COLUMN use_other_code,
    DROP COLUMN "comments";

"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Finding CABD IDs...")
query = f"""
UPDATE
	load.dams_fao AS fao
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	load.duplicates AS duplicates
WHERE
	fao.data_source = duplicates.data_source
	OR fao.data_source = duplicates.dups_fao;
"""

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)