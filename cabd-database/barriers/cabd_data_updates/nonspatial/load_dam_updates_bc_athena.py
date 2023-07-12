# This script loads a CSV into the database containing updated attribute information for dams
# Your CSV requires 4 additional fields beyond CABD attributes fields for tracking purposes
# These fields are: latitude, longitude, entry_classification, and data_source_short_name
# 
# IMPORTANT: You must review your CSV for encoding issues before import. The expected encoding
# for your CSV is ANSI (for records containing French characters) or UTF-8 (for records 
# without French characters) and the only unicode characters allowed are French characters.
# You should also check for % signs in your CSV. SharePoint's CSV exporter changes # signs
# to % signs, so you will need to find and replace these instances.
# Avoid replacing legitimate % signs in your CSV.

import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"

dbName = "cabd_dev"
dbHost = "localhost"
dbPort = "5432"
dbUser = sys.argv[3]
dbPassword = sys.argv[4]

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

updateSchema = "cabd"
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
print("Data loaded to table: " + sourceTable)

loadQuery = f"""

--create damUpdateTable with proper format if not exists
CREATE TABLE IF NOT EXISTS {damUpdateTable} (LIKE {damTable});
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS id serial PRIMARY KEY;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS latitude decimal(8,6);
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS longitude decimal(9,6);
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS entry_classification varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS data_source_short_name varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS reviewer_comments varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS update_type varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS submitted_on varchar;

ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'wait', 'done'));
ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS update_type_check;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS record_unique;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT record_unique UNIQUE (cabd_id, data_source_short_name);

--ALTER TABLE IF EXISTS {damUpdateTable} OWNER to cabd;
--ALTER TABLE IF EXISTS {sourceTable} OWNER to cabd;

--clean CSV input

ALTER TABLE {sourceTable} ADD CONSTRAINT entry_classification_check CHECK (entry_classification IN ('new feature', 'modify feature', 'delete feature'));
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS "status" varchar;
UPDATE {sourceTable} SET "status" = 'ready' WHERE reviewer_comments IS NULL;
UPDATE {sourceTable} SET "status" = 'needs review' WHERE reviewer_comments IS NOT NULL;
ALTER TABLE {sourceTable} ADD COLUMN update_type varchar default 'cwf';
UPDATE {sourceTable} SET cabd_id = gen_random_uuid() WHERE entry_classification = 'new feature' AND cabd_id IS NULL;


--trim fields that are getting a type conversion
UPDATE {sourceTable} SET cabd_id = TRIM(cabd_id);
--UPDATE {sourceTable} SET removed_year = TRIM(removed_year);
--UPDATE {sourceTable} SET maintenance_last = TRIM(maintenance_last);
--UPDATE {sourceTable} SET maintenance_next = TRIM(maintenance_next);
--UPDATE {sourceTable} SET spillway_capacity = TRIM(spillway_capacity);
--UPDATE {sourceTable} SET hydro_peaking_system = TRIM(hydro_peaking_system);
--UPDATE {sourceTable} SET federal_flow_req = TRIM(federal_flow_req);
--UPDATE {sourceTable} SET provincial_flow_req = TRIM(provincial_flow_req);
--UPDATE {sourceTable} SET degree_of_regulation_pc = TRIM(degree_of_regulation_pc);

--change field types
ALTER TABLE {sourceTable} ALTER COLUMN cabd_id TYPE uuid USING cabd_id::uuid;
--ALTER TABLE {sourceTable} ALTER COLUMN removed_year TYPE numeric USING removed_year::numeric;
--ALTER TABLE {sourceTable} ALTER COLUMN maintenance_last TYPE date USING maintenance_last::date;
--ALTER TABLE {sourceTable} ALTER COLUMN maintenance_next TYPE date USING maintenance_next::date;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_capacity TYPE double precision USING spillway_capacity::double precision;
ALTER TABLE {sourceTable} ALTER COLUMN avg_rate_of_discharge_ls TYPE double precision USING avg_rate_of_discharge_ls::double precision;
--ALTER TABLE {sourceTable} ALTER COLUMN hydro_peaking_system TYPE boolean USING hydro_peaking_system::boolean;
--ALTER TABLE {sourceTable} ALTER COLUMN expected_end_of_life TYPE smallint USING expected_end_of_life::smallint;
--ALTER TABLE {sourceTable} ALTER COLUMN federal_flow_req TYPE double precision USING federal_flow_req::double precision;
--ALTER TABLE {sourceTable} ALTER COLUMN provincial_flow_req TYPE double precision USING provincial_flow_req::double precision;
--ALTER TABLE {sourceTable} ALTER COLUMN degree_of_regulation_pc TYPE real USING degree_of_regulation_pc::real;
ALTER TABLE {sourceTable} ALTER COLUMN reservoir_depth_m TYPE real USING reservoir_depth_m::real;
ALTER TABLE {sourceTable} ALTER COLUMN turbine_number TYPE smallint USING turbine_number::smallint;

--trim varchars and categorical fields that are not coded values
UPDATE {sourceTable} SET email = TRIM(email);
UPDATE {sourceTable} SET data_source_short_name = TRIM(data_source_short_name);
UPDATE {sourceTable} SET dam_name_en = TRIM(dam_name_en);
--UPDATE {sourceTable} SET dam_name_fr = TRIM(dam_name_fr);
UPDATE {sourceTable} SET facility_name_en = TRIM(facility_name_en);
--UPDATE {sourceTable} SET facility_name_fr = TRIM(facility_name_fr);
--UPDATE {sourceTable} SET waterbody_name_en = TRIM(waterbody_name_en);
--UPDATE {sourceTable} SET waterbody_name_fr = TRIM(waterbody_name_fr);
UPDATE {sourceTable} SET reservoir_name_en = TRIM(reservoir_name_en);
--UPDATE {sourceTable} SET reservoir_name_fr = TRIM(reservoir_name_fr);
UPDATE {sourceTable} SET "owner" = TRIM("owner");
--UPDATE {sourceTable} SET operating_notes = TRIM(operating_notes);
--UPDATE {sourceTable} SET passability_status_note = TRIM(passability_status_note);
--UPDATE {sourceTable} SET assess_schedule = TRIM(assess_schedule);
--UPDATE {sourceTable} SET federal_compliance_status = TRIM(federal_compliance_status);
--UPDATE {sourceTable} SET provincial_compliance_status = TRIM(provincial_compliance_status);
--UPDATE {sourceTable} SET "comments" = TRIM("comments");

--deal with coded value fields
UPDATE {sourceTable} SET province_territory_code = LOWER(province_territory_code);

UPDATE {sourceTable} SET ownership_type_code =
    CASE
    WHEN ownership_type_code ILIKE 'charity/non-profit' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Charity/ Non-profit')
    WHEN ownership_type_code ILIKE 'federal' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Federal')
    WHEN ownership_type_code ILIKE 'municipal' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Municipal')
    WHEN ownership_type_code ILIKE 'private' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Private')
    WHEN ownership_type_code ILIKE 'provincial/territorial' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Provincial/ Territorial')
    WHEN ownership_type_code ILIKE 'other' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Other')
    WHEN ownership_type_code ILIKE 'unknown' THEN  (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown')
    WHEN ownership_type_code ILIKE 'indigenous' THEN  (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Indigenous')
    WHEN ownership_type_code IS NULL THEN NULL
    ELSE ownership_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN ownership_type_code TYPE int2 USING ownership_type_code::int2;

UPDATE {sourceTable} SET operating_status_code =
    CASE
    WHEN operating_status_code ILIKE 'abandoned/orphaned' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Abandoned/ Orphaned')
    WHEN operating_status_code ILIKE 'active' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Active')
    WHEN operating_status_code ILIKE 'decommissioned/removed' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Decommissioned/ Removed')
    WHEN operating_status_code ILIKE 'retired/closed' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Retired/ Closed')
    WHEN operating_status_code ILIKE 'remediated' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Remediated')
    WHEN operating_status_code ILIKE 'unknown' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Unknown')
    WHEN operating_status_code IS NULL THEN NULL
    ELSE operating_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN operating_status_code TYPE int2 USING operating_status_code::int2;

UPDATE {sourceTable} SET structure_type_code =
    CASE
    WHEN structure_type_code ILIKE 'dam - arch' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Arch')
    WHEN structure_type_code ILIKE 'dam - buttress' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN structure_type_code ILIKE 'dam - embankment' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN structure_type_code ILIKE 'dam - gravity' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Gravity')
    WHEN structure_type_code ILIKE 'dam - multiple arch' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Multiple Arch')
    WHEN structure_type_code ILIKE 'dam - other' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN structure_type_code ILIKE 'weir' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Weir')
    WHEN structure_type_code ILIKE 'spillway' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Spillway')
    WHEN structure_type_code ILIKE 'powerhouse' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Powerhouse')
    WHEN structure_type_code ILIKE 'lateral barrier' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Lateral Barrier')
    WHEN structure_type_code ILIKE 'lock' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Lock')
    WHEN structure_type_code ILIKE 'aboiteau/tide gate' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Aboiteau/ Tide Gate')
    WHEN structure_type_code ILIKE 'other' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Other')
    WHEN structure_type_code ILIKE 'unknown' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Unknown')
    WHEN structure_type_code IS NULL THEN NULL
    ELSE structure_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN structure_type_code TYPE int2 USING structure_type_code::int2;

-- UPDATE {sourceTable} SET construction_material_code =
--     CASE
--     WHEN construction_material_code ILIKE 'concrete' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Concrete')
--     WHEN construction_material_code ILIKE 'masonry' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Masonry')
--     WHEN construction_material_code ILIKE 'earth' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Earth')
--     WHEN construction_material_code ILIKE 'rock' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Rock')
--     WHEN construction_material_code ILIKE 'timber' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Timber')
--     WHEN construction_material_code ILIKE 'steel' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Steel')
--     WHEN construction_material_code ILIKE 'other' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Other')
--     WHEN construction_material_code ILIKE 'unknown' THEN (select code::varchar FROM dams.construction_material_codes WHERE name_en = 'Unknown')
--     WHEN construction_material_code IS NULL THEN NULL
--     ELSE construction_material_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN construction_material_code TYPE int2 USING construction_material_code::int2;

UPDATE {sourceTable} SET function_code =
    CASE
    WHEN function_code ILIKE 'storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Storage')
    WHEN function_code ILIKE 'diversion' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Diversion')
    WHEN function_code ILIKE 'detention' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Detention')
    WHEN function_code ILIKE 'saddle' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Saddle')
    WHEN function_code ILIKE 'hydro - closed-cycle pumped storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Closed-cycle pumped storage')
    WHEN function_code ILIKE 'hydro - conventional storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Conventional storage')
    WHEN function_code ILIKE 'hydro - open-cycle pumped storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Open-cycle pumped storage')
    WHEN function_code ILIKE 'hydro - run-of-river' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Run-of-river')
    WHEN function_code ILIKE 'hydro - tidal' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Tidal')
    WHEN function_code ILIKE 'hydro - other' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Other')
    WHEN function_code ILIKE 'other' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Other')
    WHEN function_code ILIKE 'unknown' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Unknown')
    WHEN function_code IS NULL THEN NULL
    ELSE function_code END;
ALTER TABLE {sourceTable} ALTER COLUMN function_code TYPE int2 USING function_code::int2;

UPDATE {sourceTable} SET use_code =
    CASE
    WHEN use_code ILIKE 'irrigation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Irrigation')
    WHEN use_code ILIKE 'hydroelectricity' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Hydroelectricity')
    WHEN use_code ILIKE 'water supply' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Water supply')
    WHEN use_code ILIKE 'flood control' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Flood control')
    WHEN use_code ILIKE 'recreation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Recreation')
    WHEN use_code ILIKE 'navigation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Navigation')
    WHEN use_code ILIKE 'fisheries' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Fisheries')
    WHEN use_code ILIKE 'pollution control' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Pollution control')
    WHEN use_code ILIKE 'invasive species control' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Invasive species control')
    WHEN use_code ILIKE 'wildlife conservation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Wildlife Conservation')
    WHEN use_code ILIKE 'other' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Other')
    WHEN use_code ILIKE 'unknown' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Unknown')
    WHEN use_code IS NULL THEN NULL
    ELSE use_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_code TYPE int2 USING use_code::int2;

UPDATE {sourceTable} SET use_irrigation_code =
    CASE
    WHEN use_irrigation_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_irrigation_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_irrigation_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_irrigation_code IS NULL THEN NULL
    ELSE use_irrigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_irrigation_code TYPE int2 USING use_irrigation_code::int2;

UPDATE {sourceTable} SET use_electricity_code =
    CASE
    WHEN use_electricity_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_electricity_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_electricity_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_electricity_code IS NULL THEN NULL
    ELSE use_electricity_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_electricity_code TYPE int2 USING use_electricity_code::int2;

UPDATE {sourceTable} SET use_supply_code =
    CASE
    WHEN use_supply_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_supply_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_supply_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_supply_code IS NULL THEN NULL
    ELSE use_supply_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_supply_code TYPE int2 USING use_supply_code::int2;

UPDATE {sourceTable} SET use_floodcontrol_code =
    CASE
    WHEN use_floodcontrol_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_floodcontrol_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_floodcontrol_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_floodcontrol_code IS NULL THEN NULL
    ELSE use_floodcontrol_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_floodcontrol_code TYPE int2 USING use_floodcontrol_code::int2;

UPDATE {sourceTable} SET use_recreation_code =
    CASE
    WHEN use_recreation_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_recreation_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_recreation_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_recreation_code IS NULL THEN NULL
    ELSE use_recreation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_recreation_code TYPE int2 USING use_recreation_code::int2;

UPDATE {sourceTable} SET use_navigation_code =
    CASE
    WHEN use_navigation_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_navigation_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_navigation_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_navigation_code IS NULL THEN NULL
    ELSE use_navigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_navigation_code TYPE int2 USING use_navigation_code::int2;

UPDATE {sourceTable} SET use_fish_code =
    CASE
    WHEN use_fish_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_fish_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_fish_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_fish_code IS NULL THEN NULL
    ELSE use_fish_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_fish_code TYPE int2 USING use_fish_code::int2;

UPDATE {sourceTable} SET use_pollution_code =
    CASE
    WHEN use_pollution_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_pollution_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_pollution_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_pollution_code IS NULL THEN NULL
    ELSE use_pollution_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_pollution_code TYPE int2 USING use_pollution_code::int2;

UPDATE {sourceTable} SET use_invasivespecies_code =
    CASE
    WHEN use_invasivespecies_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_invasivespecies_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_invasivespecies_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_invasivespecies_code IS NULL THEN NULL
    ELSE use_invasivespecies_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_invasivespecies_code TYPE int2 USING use_invasivespecies_code::int2;

-- UPDATE {sourceTable} SET use_conservation_code =
--     CASE
--     WHEN use_conservation_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
--     WHEN use_conservation_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
--     WHEN use_conservation_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
--     WHEN use_conservation_code IS NULL THEN NULL
--     ELSE use_conservation_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN use_conservation_code TYPE int2 USING use_conservation_code::int2;

UPDATE {sourceTable} SET use_other_code =
    CASE
    WHEN use_other_code ILIKE 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_other_code ILIKE 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_other_code ILIKE 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_other_code IS NULL THEN NULL
    ELSE use_other_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_other_code TYPE int2 USING use_other_code::int2;

UPDATE {sourceTable} SET turbine_type_code =
    CASE
    WHEN turbine_type_code ILIKE 'cross-flow' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Cross-flow')
    WHEN turbine_type_code ILIKE 'francis' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Francis')
    WHEN turbine_type_code ILIKE 'kaplan' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Kaplan')
    WHEN turbine_type_code ILIKE 'pelton' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Pelton')
    WHEN turbine_type_code ILIKE 'other' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Other')
    WHEN turbine_type_code ILIKE 'unknown' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Unknown')
    WHEN turbine_type_code IS NULL THEN NULL
    ELSE turbine_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN turbine_type_code TYPE int2 USING turbine_type_code::int2;

-- UPDATE {sourceTable} SET lake_control_code =
--     CASE
--     WHEN lake_control_code ILIKE 'yes' THEN (select code::varchar FROM dams.lake_control_codes WHERE name_en = 'Yes')
--     WHEN lake_control_code ILIKE 'enlarged' THEN (select code::varchar FROM dams.lake_control_codes WHERE name_en = 'Enlarged')
--     WHEN lake_control_code ILIKE 'maybe' THEN (select code::varchar FROM dams.lake_control_codes WHERE name_en = 'Maybe')
--     WHEN lake_control_code IS NULL THEN NULL
--     ELSE lake_control_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN lake_control_code TYPE int2 USING lake_control_code::int2;

UPDATE {sourceTable} SET spillway_type_code =
    CASE
    WHEN spillway_type_code ILIKE 'combined' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Combined')
    WHEN spillway_type_code ILIKE 'free' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Free')
    WHEN spillway_type_code ILIKE 'gated' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Gated')
    WHEN spillway_type_code ILIKE 'other' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Other')
    WHEN spillway_type_code ILIKE 'none' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'None')
    WHEN spillway_type_code ILIKE 'unknown' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Unknown')
    WHEN spillway_type_code IS NULL THEN NULL
    ELSE spillway_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_type_code TYPE int2 USING spillway_type_code::int2;

UPDATE {sourceTable} SET up_passage_type_code =
    CASE
    WHEN up_passage_type_code ILIKE 'denil' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Denil')
    WHEN up_passage_type_code ILIKE 'nature-like fishway' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Nature-like fishway')
    WHEN up_passage_type_code ILIKE 'pool and weir' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Pool and weir')
    WHEN up_passage_type_code ILIKE 'pool and weir with hole' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Pool and weir with hole')
    WHEN up_passage_type_code ILIKE 'trap and truck' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Trap and truck')
    WHEN up_passage_type_code ILIKE 'vertical slot' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Vertical slot')
    WHEN up_passage_type_code ILIKE 'other' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Other')
    WHEN up_passage_type_code ILIKE 'no structure' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'No structure')
    WHEN up_passage_type_code ILIKE 'unknown' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')
    WHEN up_passage_type_code IS NULL THEN NULL
    ELSE up_passage_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN up_passage_type_code TYPE int2 USING up_passage_type_code::int2;

-- UPDATE {sourceTable} SET passability_status_code =
--     CASE
--     WHEN passability_status_code ILIKE 'barrier' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Barrier')
--     WHEN passability_status_code ILIKE 'partial barrier' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Partial Barrier')
--     WHEN passability_status_code ILIKE 'passable' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Passable')
--     WHEN passability_status_code ILIKE 'unknown' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
--     WHEN passability_status_code IS NULL THEN NULL
--     ELSE passability_status_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN passability_status_code TYPE int2 USING passability_status_code::int2;

-- UPDATE {sourceTable} SET down_passage_route_code =
--     CASE
--     WHEN down_passage_route_code ILIKE 'bypass' THEN (select code::varchar FROM dams.downstream_passage_route_codes WHERE name_en = 'Bypass')
--     WHEN down_passage_route_code ILIKE 'river channel' THEN (select code::varchar FROM dams.downstream_passage_route_codes WHERE name_en = 'River channel')
--     WHEN down_passage_route_code ILIKE 'spillway' THEN (select code::varchar FROM dams.downstream_passage_route_codes WHERE name_en = 'Spillway')
--     WHEN down_passage_route_code ILIKE 'turbine' THEN (select code::varchar FROM dams.downstream_passage_route_codes WHERE name_en = 'Turbine')
--     WHEN down_passage_route_code IS NULL THEN NULL
--     ELSE down_passage_route_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN down_passage_route_code TYPE int2 USING down_passage_route_code::int2;

-- UPDATE {sourceTable} SET condition_code =
--     CASE
--     WHEN condition_code ILIKE 'good' THEN (select code::varchar FROM dams.condition_codes WHERE name_en = 'Good')
--     WHEN condition_code ILIKE 'fair' THEN (select code::varchar FROM dams.condition_codes WHERE name_en = 'Fair')
--     WHEN condition_code ILIKE 'poor' THEN (select code::varchar FROM dams.condition_codes WHERE name_en = 'Poor')
--     WHEN condition_code ILIKE 'unreliable' THEN (select code::varchar FROM dams.condition_codes WHERE name_en = 'Unreliable')
--     WHEN condition_code IS NULL THEN NULL
--     ELSE condition_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN condition_code TYPE int2 USING condition_code::int2;
"""

moveQuery = f"""
--move updates into staging table
INSERT INTO {damUpdateTable} (
    cabd_id,
    latitude,
    longitude,
    entry_classification,
    data_source_short_name,
    update_status,
    reviewer_comments,
    update_type,
    submitted_on,
    dam_name_en,
    --dam_name_fr,
    --waterbody_name_en,
    --waterbody_name_fr,
    reservoir_name_en,
    --reservoir_name_fr,
    province_territory_code,
    "owner",
    ownership_type_code,
    --provincial_compliance_status,
    --federal_compliance_status,
    --operating_notes,
    operating_status_code,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_fish_code,
    use_pollution_code,
    use_invasivespecies_code,
    --use_conservation_code,
    use_other_code,
    --lake_control_code,
    construction_year,
    --removed_year,
    --assess_schedule,
    --expected_end_of_life,
    --maintenance_last,
    --maintenance_next,
    function_code,
    --condition_code,
    structure_type_code,
    --construction_material_code,
    height_m,
    length_m,
    spillway_capacity,
    spillway_type_code,
    reservoir_present,
    reservoir_area_skm,
    reservoir_depth_m,
    storage_capacity_mcm,
    avg_rate_of_discharge_ls,
    --degree_of_regulation_pc,
    --provincial_flow_req,
    --federal_flow_req,
    --hydro_peaking_system,
    generating_capacity_mwh,
    turbine_number,
    turbine_type_code,
    up_passage_type_code,
    --down_passage_route_code,
    --"comments",
    --passability_status_code,
    --passability_status_note,
    use_analysis,
    facility_name_en
    --facility_name_fr
)
SELECT
    cabd_id,
    latitude,
    longitude,
    entry_classification,
    data_source_short_name,
    "status",
    reviewer_comments,
    update_type,
    submitted_on,
    dam_name_en,
    --dam_name_fr,
    --waterbody_name_en,
    --waterbody_name_fr,
    reservoir_name_en,
    --reservoir_name_fr,
    province_territory_code,
    "owner",
    ownership_type_code,
    --provincial_compliance_status,
    --federal_compliance_status,
    --operating_notes,
    operating_status_code,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_fish_code,
    use_pollution_code,
    use_invasivespecies_code,
    --use_conservation_code,
    use_other_code,
    --lake_control_code,
    construction_year,
    --removed_year,
    --assess_schedule,
    --expected_end_of_life,
    --maintenance_last,
    --maintenance_next,
    function_code,
    --condition_code,
    structure_type_code,
    --construction_material_code,
    height_m,
    length_m,
    spillway_capacity,
    spillway_type_code,
    reservoir_present,
    reservoir_area_skm,
    reservoir_depth_m,
    storage_capacity_mcm,
    avg_rate_of_discharge_ls,
    --degree_of_regulation_pc,
    --provincial_flow_req,
    --federal_flow_req,
    --hydro_peaking_system,
    generating_capacity_mwh,
    turbine_number,
    turbine_type_code,
    up_passage_type_code,
    --down_passage_route_code,
    --"comments",
    --passability_status_code,
    --passability_status_note,
    use_analysis,
    facility_name_en
    --facility_name_fr
FROM {sourceTable};
"""

print("Cleaning CSV...")
# print(loadQuery)
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
print("Adding records to " + damUpdateTable)
print(moveQuery)
# with conn.cursor() as cursor:
#     cursor.execute(moveQuery)
conn.commit()
conn.close()

print("Script complete")