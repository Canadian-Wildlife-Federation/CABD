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

damSchema = "dams"
damTable = damSchema + ".dams"

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
print("Data loaded to table")

loadQuery = f"""

--create damUpdateTable with proper format
CREATE TABLE IF NOT EXISTS {damUpdateTable} (LIKE {damTable});
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS latitude decimal(8,6);
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS longitude decimal(9,6);
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS entry_classification varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS data_source_short_name varchar;

--make sure records are not duplicated
ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS record_unique;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT record_unique UNIQUE (cabd_id, data_source_short_name);

--clean CSV input
--check that entry_classification meets constraints
ALTER TABLE {sourceTable} ADD CONSTRAINT entry_classification_check CHECK (entry_classification IN ('add feature', 'modify feature', 'delete feature'));

--trim fields that are getting a type conversion
UPDATE {sourceTable} SET cabd_id = TRIM(cabd_id);
UPDATE {sourceTable} SET maintenance_last = TRIM(maintenance_last);
UPDATE {sourceTable} SET maintenance_next = TRIM(maintenance_next);
UPDATE {sourceTable} SET spillway_capacity = TRIM(spillway_capacity);
UPDATE {sourceTable} SET avg_rate_of_discharge_ls = TRIM(avg_rate_of_discharge_ls);

--change field types
ALTER TABLE {sourceTable} ALTER COLUMN cabd_id TYPE uuid USING cabd_id::uuid;
ALTER TABLE {sourceTable} ALTER COLUMN maintenance_last TYPE date USING maintenance_last::date;
ALTER TABLE {sourceTable} ALTER COLUMN maintenance_next TYPE date USING maintenance_next::date;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_capacity TYPE double USING spillway_capacity::double;
ALTER TABLE {sourceTable} ALTER COLUMN avg_rate_of_discharge_ls TYPE double USING avg_rate_of_discharge_ls::double;

--trim varchars and categorical fields that are not coded values
UPDATE {sourceTable} SET email = TRIM(email);
UPDATE {sourceTable} SET data_source_short_name = TRIM(data_source_short_name);
UPDATE {sourceTable} SET use_analysis = TRIM(use_analysis);
UPDATE {sourceTable} SET dam_name_en = TRIM(dam_name_en);
UPDATE {sourceTable} SET dam_name_fr = TRIM(dam_name_fr);
UPDATE {sourceTable} SET facility_name_en = TRIM(facility_name_en);
UPDATE {sourceTable} SET facility_name_fr = TRIM(facility_name_fr);
UPDATE {sourceTable} SET waterbody_name_en = TRIM(waterbody_name_en);
UPDATE {sourceTable} SET waterbody_name_fr = TRIM(waterbody_name_fr);
UPDATE {sourceTable} SET reservoir_present = TRIM(reservoir_present);
UPDATE {sourceTable} SET reservoir_name_en = TRIM(reservoir_name_en);
UPDATE {sourceTable} SET reservoir_name_fr = TRIM(reservoir_name_fr);
UPDATE {sourceTable} SET reservoir_area_skm = TRIM(reservoir_area_skm);
UPDATE {sourceTable} SET reservoir_depth_m = TRIM(reservoir_depth_m);
UPDATE {sourceTable} SET "owner" = TRIM("owner");
UPDATE {sourceTable} SET operating_notes = TRIM(operating_notes);
UPDATE {sourceTable} SET hydro_peaking_system = TRIM(hydro_peaking_system);
UPDATE {sourceTable} SET passability_status_note = TRIM(passability_status_note);
UPDATE {sourceTable} SET assess_schedule = TRIM(assess_schedule);
UPDATE {sourceTable} SET federal_compliance_status = TRIM(federal_compliance_status);
UPDATE {sourceTable} SET provincial_compliance_status = TRIM(provincial_compliance_status);
UPDATE {sourceTable} SET degree_of_regulation_pc = TRIM(degree_of_regulation_pc);
UPDATE {sourceTable} SET "comments" = TRIM("comments");

--deal with coded value fields
UPDATE {sourceTable} SET ownership_type_code =
    CASE
    WHEN ownership_type_code = 'charity/non-profit' THEN 1
    WHEN ownership_type_code = 'federal' THEN 2
    WHEN ownership_type_code = 'municipal' THEN 3
    WHEN ownership_type_code = 'private' THEN 4
    WHEN ownership_type_code = 'provincial/territorial' THEN 5
    WHEN ownership_type_code = 'other' THEN 6
    WHEN ownership_type_code = 'unknown' THEN 7
    WHEN ownership_type_code = 'indigenous' THEN 8
    WHEN ownership_type_code IS NULL THEN NULL
    ELSE ownership_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN ownership_type_code TYPE int2 USING ownership_type_code::int2;

UPDATE {sourceTable} SET operating_status_code =
    CASE
    WHEN operating_status_code = 'abandoned/orphaned' THEN 1
    WHEN operating_status_code = 'active' THEN 2
    WHEN operating_status_code = 'decommissioned/removed' THEN 3
    WHEN operating_status_code = 'retired/closed' THEN 4
    WHEN operating_status_code = 'unknown' THEN 5
    WHEN operating_status_code = 'remediated' THEN 6
    WHEN operating_status_code IS NULL THEN NULL
    ELSE operating_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN operating_status_code TYPE int2 USING operating_status_code::int2;

--TO DO: update this with final codes
UPDATE {sourceTable} SET structure_type_code =
    CASE
    WHEN structure_type_code = 'dam - arch' THEN
    WHEN structure_type_code = 'dam - buttress' THEN
    WHEN structure_type_code = 'dam - embankment' THEN
    WHEN structure_type_code = 'dam - gravity' THEN
    WHEN structure_type_code = 'dam - multiple arch' THEN
    WHEN structure_type_code = 'weir' THEN
    WHEN structure_type_code = 'powerhouse' THEN
    WHEN structure_type_code = 'spillway' THEN
    WHEN structure_type_code = 'dike/canal/embankment' THEN
    WHEN structure_type_code = 'lock' THEN
    WHEN structure_type_code = 'aboiteau/tide gate' THEN
    WHEN structure_type_code = 'unknown' THEN
    WHEN structure_type_code = 'other' THEN
    WHEN structure_type_code IS NULL THEN NULL
    ELSE structure_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN structure_type_code TYPE int2 USING structure_type_code::int2;

--TO DO: update this with final codes
UPDATE {sourceTable} SET construction_material_code =
    CASE
    WHEN construction_material_code = 'concrete' THEN
    WHEN construction_material_code = 'masonry' THEN
    WHEN construction_material_code = 'earth' THEN
    WHEN construction_material_code = 'rock' THEN
    WHEN construction_material_code = 'timber' THEN
    WHEN construction_material_code = 'steel' THEN
    WHEN construction_material_code = 'other' THEN
    WHEN construction_material_code = 'unknown' THEN
    WHEN construction_material_code IS NULL THEN NULL
    ELSE construction_material_code END;
ALTER TABLE {sourceTable} ALTER COLUMN construction_material_code TYPE int2 USING construction_material_code::int2;

--TO DO: update this with final codes
UPDATE {sourceTable} SET function_code =
    CASE
    WHEN function_code = 'storage' THEN
    WHEN function_code = 'diversion' THEN
    WHEN function_code = 'detention' THEN
    WHEN function_code = 'saddle' THEN
    WHEN function_code = 'hydro - closed-cycle pumped storage' THEN
    WHEN function_code = 'hydro - conventional storage' THEN
    WHEN function_code = 'hydro - open-cycle pumped storage' THEN
    WHEN function_code = 'hydro - run-of-river' THEN
    WHEN function_code = 'hydro - tidal' THEN
    WHEN function_code = 'other' THEN
    WHEN function_code = 'unknown' THEN
    WHEN function_code IS NULL THEN NULL
    ELSE function_code END;
ALTER TABLE {sourceTable} ALTER COLUMN function_code TYPE int2 USING function_code::int2;

UPDATE {sourceTable} SET use_code =
    CASE
    WHEN use_code = 'irrigation' THEN 1
    WHEN use_code = 'hydroelectricity' THEN 2
    WHEN use_code = 'water supply' THEN 3
    WHEN use_code = 'flood control' THEN 4
    WHEN use_code = 'recreation' THEN 5
    WHEN use_code = 'navigation' THEN 6
    WHEN use_code = 'fisheries' THEN 7
    WHEN use_code = 'pollution control' THEN 8
    WHEN use_code = 'invasive species control' THEN 9
    WHEN use_code = 'other' THEN 10
    WHEN use_code = 'unknown' THEN 11
    WHEN use_code IS NULL THEN NULL
    ELSE use_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_code TYPE int2 USING use_code::int2;

UPDATE {sourceTable} SET use_irrigation_code =
    CASE
    WHEN use_irrigation_code = 'main' THEN 1
    WHEN use_irrigation_code = 'major' THEN 2
    WHEN use_irrigation_code = 'secondary' THEN 3
    WHEN use_irrigation_code IS NULL THEN NULL
    ELSE use_irrigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_irrigation_code TYPE int2 USING use_irrigation_code::int2;

UPDATE {sourceTable} SET use_electricity_code =
    CASE
    WHEN use_electricity_code = 'main' THEN 1
    WHEN use_electricity_code = 'major' THEN 2
    WHEN use_electricity_code = 'secondary' THEN 3
    WHEN use_electricity_code IS NULL THEN NULL
    ELSE use_electricity_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_electricity_code TYPE int2 USING use_electricity_code::int2;

UPDATE {sourceTable} SET use_supply_code =
    CASE
    WHEN use_supply_code = 'main' THEN 1
    WHEN use_supply_code = 'major' THEN 2
    WHEN use_supply_code = 'secondary' THEN 3
    WHEN use_supply_code IS NULL THEN NULL
    ELSE use_supply_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_supply_code TYPE int2 USING use_supply_code::int2;

UPDATE {sourceTable} SET use_floodcontrol_code =
    CASE
    WHEN use_floodcontrol_code = 'main' THEN 1
    WHEN use_floodcontrol_code = 'major' THEN 2
    WHEN use_floodcontrol_code = 'secondary' THEN 3
    WHEN use_floodcontrol_code IS NULL THEN NULL
    ELSE use_floodcontrol_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_floodcontrol_code TYPE int2 USING use_floodcontrol_code::int2;

UPDATE {sourceTable} SET use_recreation_code =
    CASE
    WHEN use_recreation_code = 'main' THEN 1
    WHEN use_recreation_code = 'major' THEN 2
    WHEN use_recreation_code = 'secondary' THEN 3
    WHEN use_recreation_code IS NULL THEN NULL
    ELSE use_recreation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_recreation_code TYPE int2 USING use_recreation_code::int2;

UPDATE {sourceTable} SET use_navigation_code =
    CASE
    WHEN use_navigation_code = 'main' THEN 1
    WHEN use_navigation_code = 'major' THEN 2
    WHEN use_navigation_code = 'secondary' THEN 3
    WHEN use_navigation_code IS NULL THEN NULL
    ELSE use_navigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_navigation_code TYPE int2 USING use_navigation_code::int2;

UPDATE {sourceTable} SET use_fisheries_code =
    CASE
    WHEN use_fisheries_code = 'main' THEN 1
    WHEN use_fisheries_code = 'major' THEN 2
    WHEN use_fisheries_code = 'secondary' THEN 3
    WHEN use_fisheries_code IS NULL THEN NULL
    ELSE use_fisheries_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_fisheries_code TYPE int2 USING use_fisheries_code::int2;

UPDATE {sourceTable} SET use_pollution_code =
    CASE
    WHEN use_pollution_code = 'main' THEN 1
    WHEN use_pollution_code = 'major' THEN 2
    WHEN use_pollution_code = 'secondary' THEN 3
    WHEN use_pollution_code IS NULL THEN NULL
    ELSE use_pollution_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_pollution_code TYPE int2 USING use_pollution_code::int2;

UPDATE {sourceTable} SET use_invasivespecies_code =
    CASE
    WHEN use_invasivespecies_code = 'main' THEN 1
    WHEN use_invasivespecies_code = 'major' THEN 2
    WHEN use_invasivespecies_code = 'secondary' THEN 3
    WHEN use_invasivespecies_code IS NULL THEN NULL
    ELSE use_invasivespecies_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_invasivespecies_code TYPE int2 USING use_invasivespecies_code::int2;

UPDATE {sourceTable} SET use_other_code =
    CASE
    WHEN use_other_code = 'main' THEN 1
    WHEN use_other_code = 'major' THEN 2
    WHEN use_other_code = 'secondary' THEN 3
    WHEN use_other_code IS NULL THEN NULL
    ELSE use_other_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_other_code TYPE int2 USING use_other_code::int2;

UPDATE {sourceTable} SET turbine_type_code =
    CASE
    WHEN turbine_type_code = 'cross-flow' THEN 1
    WHEN turbine_type_code = 'francis' THEN 2
    WHEN turbine_type_code = 'kaplan' THEN 3
    WHEN turbine_type_code = 'pelton' THEN 4
    WHEN turbine_type_code = 'unknown' THEN 5
    WHEN turbine_type_code = 'other' THEN 6
    WHEN turbine_type_code IS NULL THEN NULL
    ELSE turbine_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN turbine_type_code TYPE int2 USING turbine_type_code::int2;

UPDATE {sourceTable} SET lake_control_code =
    CASE
    WHEN lake_control_code = 'yes' THEN 1
    WHEN lake_control_code = 'enlarged' THEN 2
    WHEN lake_control_code = 'maybe' THEN 3
    WHEN lake_control_code IS NULL THEN NULL
    ELSE lake_control_code END;
ALTER TABLE {sourceTable} ALTER COLUMN lake_control_code TYPE int2 USING lake_control_code::int2;

UPDATE {sourceTable} SET spillway_type_code =
    CASE
    WHEN spillway_type_code = 'combined' THEN 1
    WHEN spillway_type_code = 'free' THEN 2
    WHEN spillway_type_code = 'gated' THEN 3
    WHEN spillway_type_code = 'other' THEN 4
    WHEN spillway_type_code = 'none' THEN 5
    WHEN spillway_type_code = 'unknown' THEN 6
    WHEN spillway_type_code IS NULL THEN NULL
    ELSE spillway_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_type_code TYPE int2 USING spillway_type_code::int2;

UPDATE {sourceTable} SET up_passage_type_code =
    CASE
    WHEN up_passage_type_code = 'denil' THEN 1
    WHEN up_passage_type_code = 'nature-like fishway' THEN 2
    WHEN up_passage_type_code = 'pool and weir' THEN 3
    WHEN up_passage_type_code = 'pool and weir with hole' THEN 4
    WHEN up_passage_type_code = 'trap and truck' THEN 5
    WHEN up_passage_type_code = 'vertical slot' THEN 6
    WHEN up_passage_type_code = 'other' THEN 7
    WHEN up_passage_type_code = 'no structure' THEN 8
    WHEN up_passage_type_code = 'unknown' THEN 9
    WHEN up_passage_type_code IS NULL THEN NULL
    ELSE up_passage_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN up_passage_type_code TYPE int2 USING up_passage_type_code::int2;

UPDATE {sourceTable} SET passability_status_code =
    CASE
    WHEN passability_status_code = 'barrier' THEN 1
    WHEN passability_status_code = 'partial barrier' THEN 2
    WHEN passability_status_code = 'passable' THEN 3
    WHEN passability_status_code = 'unknown' THEN 4
    WHEN passability_status_code IS NULL THEN NULL
    ELSE passability_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN passability_status_code TYPE int2 USING passability_status_code::int2;

UPDATE {sourceTable} SET down_passage_route_code =
    CASE
    WHEN down_passage_route_code = 'bypass' THEN 1
    WHEN down_passage_route_code = 'river channel' THEN 2
    WHEN down_passage_route_code = 'spillway' THEN 3
    WHEN down_passage_route_code = 'turbine' THEN 4
    WHEN down_passage_route_code IS NULL THEN NULL
    ELSE down_passage_route_code END;
ALTER TABLE {sourceTable} ALTER COLUMN down_passage_route_code TYPE int2 USING down_passage_route_code::int2;

UPDATE {sourceTable} SET condition_code =
    CASE
    WHEN condition_code = 'good' THEN 1
    WHEN condition_code = 'fair' THEN 2
    WHEN condition_code = 'poor' THEN 3
    WHEN condition_code = 'unreliable' THEN 4
    WHEN condition_code IS NULL THEN NULL
    ELSE condition_code END;
ALTER TABLE {sourceTable} ALTER COLUMN condition_code TYPE int2 USING condition_code::int2;

--TO DO: add remaining fields once this script has been tested
--INSERT INTO {damUpdateTable} (
--     cabd_id,
--     entry_classification,
--     data_source_short_name,
--     latitude,
--     longitude,
--     use_analysis,
--     dam_name_en
-- )
-- SELECT
--     cabd_id,
--     entry_classification,
--     data_source_short_name,
--     latitude,
--     longitude,
--     use_analysis,
--     dam_name_en
-- FROM {sourceTable};

"""
print("Cleaning CSV and adding records to " + damUpdateTable)
print(loadQuery)
# with conn.cursor() as cursor:
#     cursor.execute(loadQuery)
# conn.commit()
conn.close()

print("Script complete")