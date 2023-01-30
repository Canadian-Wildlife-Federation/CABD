# This script loads a CSV into the database containing updated attribute information for dams
# Your CSV requires 4 additional fields beyond CABD attributes fields for tracking purposes
# These fields are: latitude, longitude, entry_classification, and data_source_short_name
# 
# IMPORTANT: You must review your CSV for encoding issues before import. The expected encoding
# for your CSV is ANSI (for records containing French characters) or UTF-8 (for records 
# without French characters) and the only non-unicode characters allowed are French characters.
# You should also check for % signs in your CSV. SharePoint's CSV exporter changes # signs
# to % signs, so you will need to find and replace these instances.
# Avoid replacing legitimate % signs in your CSV.

import psycopg2 as pg2
import subprocess
import sys

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbName = "cabd_dev" # change back to production details when ready
dbHost = "localhost" # change back to production details when ready
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
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS latitude decimal(8,6);
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS longitude decimal(9,6);
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS entry_classification varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS data_source_short_name varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS reviewer_comments varchar;
ALTER TABLE {damUpdateTable} ADD COLUMN IF NOT EXISTS update_type varchar;

ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'done'));
ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS update_type_check;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE {damUpdateTable} DROP CONSTRAINT IF EXISTS record_unique;
ALTER TABLE {damUpdateTable} ADD CONSTRAINT record_unique UNIQUE (cabd_id, data_source_short_name);

ALTER TABLE IF EXISTS {damUpdateTable} OWNER to cabd;
ALTER TABLE IF EXISTS {sourceTable} OWNER to cabd;

--clean CSV input

ALTER TABLE {sourceTable} ADD CONSTRAINT entry_classification_check CHECK (entry_classification IN ('new feature', 'modify feature', 'delete feature'));
ALTER TABLE {sourceTable} ADD COLUMN "status" varchar;
UPDATE {sourceTable} SET "status" = 'ready' WHERE reviewer_comments IS NULL;
UPDATE {sourceTable} SET "status" = 'needs review' WHERE reviewer_comments IS NOT NULL;
ALTER TABLE {sourceTable} ADD COLUMN update_type varchar default 'cwf';
UPDATE {sourceTable} SET cabd_id = gen_random_uuid() WHERE entry_classification = 'new feature';


--trim fields that are getting a type conversion
UPDATE {sourceTable} SET cabd_id = TRIM(cabd_id);
UPDATE {sourceTable} SET removed_year = TRIM(removed_year);
UPDATE {sourceTable} SET maintenance_last = TRIM(maintenance_last);
UPDATE {sourceTable} SET maintenance_next = TRIM(maintenance_next);
UPDATE {sourceTable} SET spillway_capacity = TRIM(spillway_capacity);
UPDATE {sourceTable} SET hydro_peaking_system = TRIM(hydro_peaking_system);
UPDATE {sourceTable} SET federal_flow_req = TRIM(federal_flow_req);
UPDATE {sourceTable} SET provincial_flow_req = TRIM(provincial_flow_req);
UPDATE {sourceTable} SET degree_of_regulation_pc = TRIM(degree_of_regulation_pc);

--change field types
ALTER TABLE {sourceTable} ALTER COLUMN cabd_id TYPE uuid USING cabd_id::uuid;
ALTER TABLE {sourceTable} ALTER COLUMN removed_year TYPE numeric USING removed_year::numeric;
ALTER TABLE {sourceTable} ALTER COLUMN maintenance_last TYPE date USING maintenance_last::date;
ALTER TABLE {sourceTable} ALTER COLUMN maintenance_next TYPE date USING maintenance_next::date;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_capacity TYPE double precision USING spillway_capacity::double precision;
ALTER TABLE {sourceTable} ALTER COLUMN avg_rate_of_discharge_ls TYPE double precision USING avg_rate_of_discharge_ls::double precision;
ALTER TABLE {sourceTable} ALTER COLUMN hydro_peaking_system TYPE boolean USING hydro_peaking_system::boolean;
ALTER TABLE {sourceTable} ALTER COLUMN expected_end_of_life TYPE smallint USING expected_end_of_life::smallint;
ALTER TABLE {sourceTable} ALTER COLUMN federal_flow_req TYPE double precision USING federal_flow_req::double precision;
ALTER TABLE {sourceTable} ALTER COLUMN provincial_flow_req TYPE double precision USING provincial_flow_req::double precision;
ALTER TABLE {sourceTable} ALTER COLUMN degree_of_regulation_pc TYPE real USING degree_of_regulation_pc::real;
ALTER TABLE {sourceTable} ALTER COLUMN reservoir_depth_m TYPE real USING reservoir_depth_m::real;
ALTER TABLE {sourceTable} ALTER COLUMN turbine_number TYPE smallint USING turbine_number::smallint;

--trim varchars and categorical fields that are not coded values
UPDATE {sourceTable} SET email = TRIM(email);
UPDATE {sourceTable} SET data_source_short_name = TRIM(data_source_short_name);
UPDATE {sourceTable} SET dam_name_en = TRIM(dam_name_en);
UPDATE {sourceTable} SET dam_name_fr = TRIM(dam_name_fr);
UPDATE {sourceTable} SET facility_name_en = TRIM(facility_name_en);
UPDATE {sourceTable} SET facility_name_fr = TRIM(facility_name_fr);
UPDATE {sourceTable} SET waterbody_name_en = TRIM(waterbody_name_en);
UPDATE {sourceTable} SET waterbody_name_fr = TRIM(waterbody_name_fr);
UPDATE {sourceTable} SET reservoir_name_en = TRIM(reservoir_name_en);
UPDATE {sourceTable} SET reservoir_name_fr = TRIM(reservoir_name_fr);
UPDATE {sourceTable} SET "owner" = TRIM("owner");
UPDATE {sourceTable} SET operating_notes = TRIM(operating_notes);
UPDATE {sourceTable} SET passability_status_note = TRIM(passability_status_note);
UPDATE {sourceTable} SET assess_schedule = TRIM(assess_schedule);
UPDATE {sourceTable} SET federal_compliance_status = TRIM(federal_compliance_status);
UPDATE {sourceTable} SET provincial_compliance_status = TRIM(provincial_compliance_status);
UPDATE {sourceTable} SET "comments" = TRIM("comments");

--deal with coded value fields
UPDATE {sourceTable} SET province_territory_code = LOWER(province_territory_code);

UPDATE {sourceTable} SET ownership_type_code =
    CASE
    WHEN ownership_type_code = 'charity/non-profit' THEN (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Charity/ Non-profit')
    WHEN ownership_type_code = 'federal' THEN (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Federal')
    WHEN ownership_type_code = 'municipal' THEN (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Municipal')
    WHEN ownership_type_code = 'private' THEN (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Private')
    WHEN ownership_type_code = 'provincial/territorial' THEN (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Provincial/ Territorial')
    WHEN ownership_type_code = 'other' THEN (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Other')
    WHEN ownership_type_code = 'unknown' THEN  (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown')
    WHEN ownership_type_code = 'indigenous' THEN  (SELECT code FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Indigenous')
    WHEN ownership_type_code IS NULL THEN NULL
    ELSE ownership_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN ownership_type_code TYPE int2 USING ownership_type_code::int2;

UPDATE {sourceTable} SET operating_status_code =
    CASE
    WHEN operating_status_code = 'abandoned/orphaned' THEN (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Abandoned/ Orphaned')
    WHEN operating_status_code = 'active' THEN (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Active')
    WHEN operating_status_code = 'decommissioned/removed' THEN (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Decommissioned/ Removed')
    WHEN operating_status_code = 'retired/closed' THEN (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Retired/ Closed')
    WHEN operating_status_code = 'remediated' THEN (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Remediated')
    WHEN operating_status_code = 'unknown' THEN (SELECT code FROM dams.operating_status_codes WHERE name_en = 'Unknown')
    WHEN operating_status_code IS NULL THEN NULL
    ELSE operating_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN operating_status_code TYPE int2 USING operating_status_code::int2;

UPDATE {sourceTable} SET structure_type_code =
    CASE
    WHEN structure_type_code = 'dam - arch' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Arch')
    WHEN structure_type_code = 'dam - buttress' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN structure_type_code = 'dam - embankment' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN structure_type_code = 'dam - gravity' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Gravity')
    WHEN structure_type_code = 'dam - multiple arch' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Multiple Arch')
    WHEN structure_type_code = 'dam - other' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN structure_type_code = 'weir' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Weir')
    WHEN structure_type_code = 'spillway' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Spillway')
    WHEN structure_type_code = 'powerhouse' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Powerhouse')
    WHEN structure_type_code = 'lateral barrier' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Lateral Barrier')
    WHEN structure_type_code = 'lock' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Lock')
    WHEN structure_type_code = 'aboiteau/tide gate' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Aboiteau/ Tide Gate')
    WHEN structure_type_code = 'other' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Other')
    WHEN structure_type_code = 'unknown' THEN (SELECT code FROM dams.structure_type_codes WHERE name_en = 'Unknown')
    WHEN structure_type_code IS NULL THEN NULL
    ELSE structure_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN structure_type_code TYPE int2 USING structure_type_code::int2;

UPDATE {sourceTable} SET construction_material_code =
    CASE
    WHEN construction_material_code = 'concrete' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Concrete')
    WHEN construction_material_code = 'masonry' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Masonry')
    WHEN construction_material_code = 'earth' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Earth')
    WHEN construction_material_code = 'rock' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Rock')
    WHEN construction_material_code = 'timber' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Timber')
    WHEN construction_material_code = 'steel' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Steel')
    WHEN construction_material_code = 'other' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Other')
    WHEN construction_material_code = 'unknown' THEN (SELECT code FROM dams.construction_materail_codes WHERE name_en = 'Unknown')
    WHEN construction_material_code IS NULL THEN NULL
    ELSE construction_material_code END;
ALTER TABLE {sourceTable} ALTER COLUMN construction_material_code TYPE int2 USING construction_material_code::int2;

UPDATE {sourceTable} SET function_code =
    CASE
    WHEN function_code = 'storage' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Storage')
    WHEN function_code = 'diversion' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Diversion')
    WHEN function_code = 'detention' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Detention')
    WHEN function_code = 'saddle' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Saddle')
    WHEN function_code = 'hydro - closed-cycle pumped storage' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Closed-cycle pumped storage')
    WHEN function_code = 'hydro - conventional storage' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Conventional storage')
    WHEN function_code = 'hydro - open-cycle pumped storage' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Open-cycle pumped storage')
    WHEN function_code = 'hydro - run-of-river' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Run-of-river')
    WHEN function_code = 'hydro - tidal' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Tidal')
    WHEN function_code = 'hydro - other' THEN THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Hydro - Other')
    WHEN function_code = 'other' THEN THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Other')
    WHEN function_code = 'unknown' THEN (SELECT code FROM dams.function_codes WHERE name_en = 'Unknown')
    WHEN function_code IS NULL THEN NULL
    ELSE function_code END;
ALTER TABLE {sourceTable} ALTER COLUMN function_code TYPE int2 USING function_code::int2;

UPDATE {sourceTable} SET use_code =
    CASE
    WHEN use_code = 'irrigation' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Irrigation')
    WHEN use_code = 'hydroelectricity' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Hydroelectricity')
    WHEN use_code = 'water supply' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Water supply')
    WHEN use_code = 'flood control' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Flood control')
    WHEN use_code = 'recreation' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Recreation')
    WHEN use_code = 'navigation' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Navigation')
    WHEN use_code = 'fisheries' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Fisheries')
    WHEN use_code = 'pollution control' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Pollution control')
    WHEN use_code = 'invasive species control' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Invasive species control')
    WHEN use_code = 'wildlife conservation' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Wildlife Conservation')
    WHEN use_code = 'other' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Other')
    WHEN use_code = 'unknown' THEN (SELECT code FROM dams.dam_use_codes WHERE name_en = 'Unknown')
    WHEN use_code IS NULL THEN NULL
    ELSE use_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_code TYPE int2 USING use_code::int2;

UPDATE {sourceTable} SET use_irrigation_code =
    CASE
    WHEN use_irrigation_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_irrigation_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_irrigation_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_irrigation_code IS NULL THEN NULL
    ELSE use_irrigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_irrigation_code TYPE int2 USING use_irrigation_code::int2;

UPDATE {sourceTable} SET use_electricity_code =
    CASE
    WHEN use_electricity_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_electricity_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_electricity_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_electricity_code IS NULL THEN NULL
    ELSE use_electricity_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_electricity_code TYPE int2 USING use_electricity_code::int2;

UPDATE {sourceTable} SET use_supply_code =
    CASE
    WHEN use_supply_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_supply_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_supply_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_supply_code IS NULL THEN NULL
    ELSE use_supply_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_supply_code TYPE int2 USING use_supply_code::int2;

UPDATE {sourceTable} SET use_floodcontrol_code =
    CASE
    WHEN use_floodcontrol_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_floodcontrol_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_floodcontrol_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_floodcontrol_code IS NULL THEN NULL
    ELSE use_floodcontrol_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_floodcontrol_code TYPE int2 USING use_floodcontrol_code::int2;

UPDATE {sourceTable} SET use_recreation_code =
    CASE
    WHEN use_recreation_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_recreation_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_recreation_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_recreation_code IS NULL THEN NULL
    ELSE use_recreation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_recreation_code TYPE int2 USING use_recreation_code::int2;

UPDATE {sourceTable} SET use_navigation_code =
    CASE
    WHEN use_navigation_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_navigation_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_navigation_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_navigation_code IS NULL THEN NULL
    ELSE use_navigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_navigation_code TYPE int2 USING use_navigation_code::int2;

UPDATE {sourceTable} SET use_fish_code =
    CASE
    WHEN use_fish_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_fish_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_fish_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_fish_code IS NULL THEN NULL
    ELSE use_fish_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_fish_code TYPE int2 USING use_fish_code::int2;

UPDATE {sourceTable} SET use_pollution_code =
    CASE
    WHEN use_pollution_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_pollution_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_pollution_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_pollution_code IS NULL THEN NULL
    ELSE use_pollution_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_pollution_code TYPE int2 USING use_pollution_code::int2;

UPDATE {sourceTable} SET use_invasivespecies_code =
    CASE
    WHEN use_invasivespecies_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_invasivespecies_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_invasivespecies_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_invasivespecies_code IS NULL THEN NULL
    ELSE use_invasivespecies_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_invasivespecies_code TYPE int2 USING use_invasivespecies_code::int2;

-- UPDATE {sourceTable} SET use_conservation_code =
--     CASE
--     WHEN use_conservation_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
--     WHEN use_conservation_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
--     WHEN use_conservation_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
--     WHEN use_conservation_code IS NULL THEN NULL
--     ELSE use_conservation_code END;
-- ALTER TABLE {sourceTable} ALTER COLUMN use_conservation_code TYPE int2 USING use_conservation_code::int2;

UPDATE {sourceTable} SET use_other_code =
    CASE
    WHEN use_other_code = 'main' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Main')
    WHEN use_other_code = 'major' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Major')
    WHEN use_other_code = 'secondary' THEN (SELECT code FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_other_code IS NULL THEN NULL
    ELSE use_other_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_other_code TYPE int2 USING use_other_code::int2;

UPDATE {sourceTable} SET turbine_type_code =
    CASE
    WHEN turbine_type_code = 'cross-flow' THEN (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Cross-flow')
    WHEN turbine_type_code = 'francis' THEN (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Francis')
    WHEN turbine_type_code = 'kaplan' THEN (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Kaplan')
    WHEN turbine_type_code = 'pelton' THEN (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Pelton')
    WHEN turbine_type_code = 'other' THEN (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Other')
    WHEN turbine_type_code = 'unknown' THEN (SELECT code FROM dams.turbine_type_codes WHERE name_en = 'Unknown')
    WHEN turbine_type_code IS NULL THEN NULL
    ELSE turbine_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN turbine_type_code TYPE int2 USING turbine_type_code::int2;

UPDATE {sourceTable} SET lake_control_code =
    CASE
    WHEN lake_control_code = 'yes' THEN (SELECT code FROM dams.lake_control_codes WHERE name_en = 'Yes')
    WHEN lake_control_code = 'enlarged' THEN (SELECT code FROM dams.lake_control_codes WHERE name_en = 'Enlarged')
    WHEN lake_control_code = 'maybe' THEN (SELECT code FROM dams.lake_control_codes WHERE name_en = 'Maybe')
    WHEN lake_control_code IS NULL THEN NULL
    ELSE lake_control_code END;
ALTER TABLE {sourceTable} ALTER COLUMN lake_control_code TYPE int2 USING lake_control_code::int2;

UPDATE {sourceTable} SET spillway_type_code =
    CASE
    WHEN spillway_type_code = 'combined' THEN (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'Combined')
    WHEN spillway_type_code = 'free' THEN (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'Free')
    WHEN spillway_type_code = 'gated' THEN (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'Gated')
    WHEN spillway_type_code = 'other' THEN (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'Other')
    WHEN spillway_type_code = 'none' THEN (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'None')
    WHEN spillway_type_code = 'unknown' THEN (SELECT code FROM dams.spillway_type_codes WHERE name_en = 'Unknown')
    WHEN spillway_type_code IS NULL THEN NULL
    ELSE spillway_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_type_code TYPE int2 USING spillway_type_code::int2;

UPDATE {sourceTable} SET up_passage_type_code =
    CASE
    WHEN up_passage_type_code = 'denil' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Denil')
    WHEN up_passage_type_code = 'nature-like fishway' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Nature-like fishway')
    WHEN up_passage_type_code = 'pool and weir' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Pool and weir')
    WHEN up_passage_type_code = 'pool and weir with hole' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Pool and weir with hole')
    WHEN up_passage_type_code = 'trap and truck' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Trap and truck')
    WHEN up_passage_type_code = 'vertical slot' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Vertical slot')
    WHEN up_passage_type_code = 'other' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Other')
    WHEN up_passage_type_code = 'no structure' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'No structure')
    WHEN up_passage_type_code = 'unknown' THEN (SELECT code FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')
    WHEN up_passage_type_code IS NULL THEN NULL
    ELSE up_passage_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN up_passage_type_code TYPE int2 USING up_passage_type_code::int2;

UPDATE {sourceTable} SET passability_status_code =
    CASE
    WHEN passability_status_code = 'barrier' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Barrier')
    WHEN passability_status_code = 'partial barrier' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Partial Barrier')
    WHEN passability_status_code = 'passable' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Passable')
    WHEN passability_status_code = 'unknown' THEN (SELECT code FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
    WHEN passability_status_code IS NULL THEN NULL
    ELSE passability_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN passability_status_code TYPE int2 USING passability_status_code::int2;

UPDATE {sourceTable} SET down_passage_route_code =
    CASE
    WHEN down_passage_route_code = 'bypass' THEN (SELECT code FROM dams.downstream_passage_route_codes WHERE name_en = 'Bypass')
    WHEN down_passage_route_code = 'river channel' THEN (SELECT code FROM dams.downstream_passage_route_codes WHERE name_en = 'River channel')
    WHEN down_passage_route_code = 'spillway' THEN (SELECT code FROM dams.downstream_passage_route_codes WHERE name_en = 'Spillway')
    WHEN down_passage_route_code = 'turbine' THEN (SELECT code FROM dams.downstream_passage_route_codes WHERE name_en = 'Turbine')
    WHEN down_passage_route_code IS NULL THEN NULL
    ELSE down_passage_route_code END;
ALTER TABLE {sourceTable} ALTER COLUMN down_passage_route_code TYPE int2 USING down_passage_route_code::int2;

UPDATE {sourceTable} SET condition_code =
    CASE
    WHEN condition_code = 'good' THEN (SELECT code FROM dams.condition_codes WHERE name_en = 'Good')
    WHEN condition_code = 'fair' THEN (SELECT code FROM dams.condition_codes WHERE name_en = 'Fair')
    WHEN condition_code = 'poor' THEN (SELECT code FROM dams.condition_codes WHERE name_en = 'Poor')
    WHEN condition_code = 'unreliable' THEN (SELECT code FROM dams.condition_codes WHERE name_en = 'Unreliable')
    WHEN condition_code IS NULL THEN NULL
    ELSE condition_code END;
ALTER TABLE {sourceTable} ALTER COLUMN condition_code TYPE int2 USING condition_code::int2;

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
    dam_name_en,
    dam_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    reservoir_name_en,
    reservoir_name_fr,
    province_territory_code,
    "owner",
    ownership_type_code,
    provincial_compliance_status,
    federal_compliance_status,
    operating_notes,
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
    -- use_conservation_code,
    use_other_code,
    lake_control_code,
    construction_year,
    removed_year,
    assess_schedule,
    expected_end_of_life,
    maintenance_last,
    maintenance_next,
    function_code,
    condition_code,
    structure_type_code,
    construction_material_code,
    height_m,
    length_m,
    spillway_capacity,
    spillway_type_code,
    reservoir_present,
    reservoir_area_skm,
    reservoir_depth_m,
    storage_capacity_mcm,
    avg_rate_of_discharge_ls,
    degree_of_regulation_pc,
    provincial_flow_req,
    federal_flow_req,
    hydro_peaking_system,
    generating_capacity_mwh,
    turbine_number,
    turbine_type_code,
    up_passage_type_code,
    down_passage_route_code,
    "comments",
    passability_status_code,
    passability_status_note,
    use_analysis,
    facility_name_en,
    facility_name_fr
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
    dam_name_en,
    dam_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    reservoir_name_en,
    reservoir_name_fr,
    province_territory_code,
    "owner",
    ownership_type_code,
    provincial_compliance_status,
    federal_compliance_status,
    operating_notes,
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
    -- use_conservation_code,
    use_other_code,
    lake_control_code,
    construction_year,
    removed_year,
    assess_schedule,
    expected_end_of_life,
    maintenance_last,
    maintenance_next,
    function_code,
    condition_code,
    structure_type_code,
    construction_material_code,
    height_m,
    length_m,
    spillway_capacity,
    spillway_type_code,
    reservoir_present,
    reservoir_area_skm,
    reservoir_depth_m,
    storage_capacity_mcm,
    avg_rate_of_discharge_ls,
    degree_of_regulation_pc,
    provincial_flow_req,
    federal_flow_req,
    hydro_peaking_system,
    generating_capacity_mwh,
    turbine_number,
    turbine_type_code,
    up_passage_type_code,
    down_passage_route_code,
    "comments",
    passability_status_code,
    passability_status_note,
    use_analysis,
    facility_name_en,
    facility_name_fr
FROM {sourceTable};
"""

print("Cleaning CSV and adding records to " + damUpdateTable)
print(loadQuery)
# with conn.cursor() as cursor:
#     cursor.execute(loadQuery)
# conn.commit()
# conn.close()

# print("Script complete")