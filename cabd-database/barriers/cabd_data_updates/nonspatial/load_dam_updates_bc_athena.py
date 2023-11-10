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

import subprocess
import sys
import getpass
import psycopg2 as pg2

ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"

dbName = "cabd_dev_2023"
dbHost = "localhost"
dbPort = "5433"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

updateSchema = "cabd"
damUpdateTable = updateSchema + '.dam_updates'

damSchema = "dams"
damTable = damSchema + ".dams"

if len(sys.argv) != 3:
    print("Invalid usage: py load_dam_updates.py <dataFile> <tableName>")
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

--clean CSV input
ALTER TABLE {sourceTable} ADD CONSTRAINT entry_classification_check CHECK (entry_classification IN ('new feature', 'modify feature', 'delete feature'));
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS "status" varchar;
UPDATE {sourceTable} SET "status" = 'ready' WHERE reviewer_comments IS NULL;
UPDATE {sourceTable} SET "status" = 'needs review' WHERE reviewer_comments IS NOT NULL;
ALTER TABLE {sourceTable} ADD COLUMN update_type varchar default 'cwf';
UPDATE {sourceTable} SET cabd_id = gen_random_uuid() WHERE entry_classification = 'new feature' AND cabd_id IS NULL;

ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS assess_schedule character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS comments character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS condition_code smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS construction_material_code smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS dam_name_fr character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS degree_of_regulation_pc real;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS down_passage_route_code smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS expected_end_of_life smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS facility_name_fr character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS federal_compliance_status character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS federal_flow_req double precision;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS hydro_peaking_system boolean;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS lake_control_code smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS maintenance_last date;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS maintenance_next date;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS name character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS operating_notes character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS organization character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS passability_status_code smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS passability_status_note character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS provincial_compliance_status character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS provincial_flow_req double precision;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS release_version double precision;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS removed_year numeric;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS reservoir_name_fr character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS status character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS update_type character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS use_conservation_code smallint;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS waterbody_name_en character varying;
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS waterbody_name_fr character varying;

--trim fields that are getting a type conversion
UPDATE {sourceTable} SET cabd_id = TRIM(cabd_id);

--change field types
ALTER TABLE {sourceTable} ALTER COLUMN cabd_id TYPE uuid USING cabd_id::uuid;
ALTER TABLE {sourceTable} ALTER COLUMN province_territory_code TYPE varchar USING province_territory_code::varchar;
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
ALTER TABLE {sourceTable} ALTER COLUMN height_m TYPE real USING height_m::real;
ALTER TABLE {sourceTable} ALTER COLUMN length_m TYPE real USING length_m::real;
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
    WHEN LOWER(ownership_type_code) = 'charity/non-profit' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Charity/ Non-profit')
    WHEN LOWER(ownership_type_code) = 'federal' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Federal')
    WHEN LOWER(ownership_type_code) = 'municipal' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Municipal')
    WHEN LOWER(ownership_type_code) = 'private' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Private')
    WHEN LOWER(ownership_type_code) = 'provincial/territorial' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Provincial/ Territorial')
    WHEN LOWER(ownership_type_code) = 'other' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Other')
    WHEN LOWER(ownership_type_code) = 'unknown' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Unknown')
    WHEN LOWER(ownership_type_code) = 'indigenous' THEN (select code::varchar FROM cabd.barrier_ownership_type_codes WHERE name_en = 'Indigenous')
    WHEN ownership_type_code IS NULL THEN NULL
    ELSE ownership_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN ownership_type_code TYPE int2 USING ownership_type_code::int2;


UPDATE {sourceTable} SET operating_status_code =
    CASE
    WHEN LOWER(operating_status_code) = 'abandoned/orphaned' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Abandoned/ Orphaned')
    WHEN LOWER(operating_status_code) = 'active' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Active')
    WHEN LOWER(operating_status_code) = 'decommissioned/removed' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Decommissioned/ Removed')
    WHEN LOWER(operating_status_code) = 'retired/closed' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Retired/ Closed')
    WHEN LOWER(operating_status_code) = 'remediated' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Remediated')
    WHEN LOWER(operating_status_code) = 'unknown' THEN (select code::varchar FROM dams.operating_status_codes WHERE name_en = 'Unknown')
    WHEN operating_status_code IS NULL THEN NULL
    ELSE operating_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN operating_status_code TYPE int2 USING operating_status_code::int2;

UPDATE {sourceTable} SET structure_type_code =
    CASE
    WHEN LOWER(structure_type_code) = 'dam - arch' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Arch')
    WHEN LOWER(structure_type_code) = 'dam - buttress' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Buttress')
    WHEN LOWER(structure_type_code) = 'dam - embankment' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Embankment')
    WHEN LOWER(structure_type_code) = 'dam - gravity' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Gravity')
    WHEN LOWER(structure_type_code) = 'dam - multiple arch' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Multiple Arch')
    WHEN LOWER(structure_type_code) = 'dam - other' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Dam - Other')
    WHEN LOWER(structure_type_code) = 'weir' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Weir')
    WHEN LOWER(structure_type_code) = 'spillway' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Spillway')
    WHEN LOWER(structure_type_code) = 'powerhouse' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Powerhouse')
    WHEN LOWER(structure_type_code) = 'lateral barrier' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Lateral Barrier')
    WHEN LOWER(structure_type_code) = 'lock' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Lock')
    WHEN LOWER(structure_type_code) = 'aboiteau/tide gate' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Aboiteau/ Tide Gate')
    WHEN LOWER(structure_type_code) = 'other' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Other')
    WHEN LOWER(structure_type_code) = 'unknown' THEN (select code::varchar FROM dams.structure_type_codes WHERE name_en = 'Unknown')
    WHEN structure_type_code IS NULL THEN NULL
    ELSE structure_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN structure_type_code TYPE int2 USING structure_type_code::int2;

UPDATE {sourceTable} SET function_code =
    CASE
    WHEN LOWER(function_code) = 'storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Storage')
    WHEN LOWER(function_code) = 'diversion' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Diversion')
    WHEN LOWER(function_code) = 'detention' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Detention')
    WHEN LOWER(function_code) = 'saddle' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Saddle')
    WHEN LOWER(function_code) = 'hydro - closed-cycle pumped storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Closed-cycle pumped storage')
    WHEN LOWER(function_code) = 'hydro - conventional storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Conventional storage')
    WHEN LOWER(function_code) = 'hydro - open-cycle pumped storage' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Open-cycle pumped storage')
    WHEN LOWER(function_code) = 'hydro - run-of-river' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Run-of-river')
    WHEN LOWER(function_code) = 'hydro - tidal' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Tidal')
    WHEN LOWER(function_code) = 'hydro - other' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Hydro - Other')
    WHEN LOWER(function_code) = 'other' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Other')
    WHEN LOWER(function_code) = 'unknown' THEN (select code::varchar FROM dams.function_codes WHERE name_en = 'Unknown')
    WHEN function_code IS NULL THEN NULL
    ELSE function_code END;
ALTER TABLE {sourceTable} ALTER COLUMN function_code TYPE int2 USING function_code::int2;

UPDATE {sourceTable} SET use_code =
    CASE
    WHEN LOWER(use_code) = 'irrigation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Irrigation')
    WHEN LOWER(use_code) = 'hydroelectricity' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Hydroelectricity')
    WHEN LOWER(use_code) = 'water supply' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Water supply')
    WHEN LOWER(use_code) = 'flood control' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Flood control')
    WHEN LOWER(use_code) = 'recreation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Recreation')
    WHEN LOWER(use_code) = 'navigation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Navigation')
    WHEN LOWER(use_code) = 'fisheries' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Fisheries')
    WHEN LOWER(use_code) = 'pollution control' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Pollution control')
    WHEN LOWER(use_code) = 'invasive species control' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Invasive species control')
    WHEN LOWER(use_code) = 'wildlife conservation' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Wildlife Conservation')
    WHEN LOWER(use_code) = 'other' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Other')
    WHEN LOWER(use_code) = 'unknown' THEN (select code::varchar FROM dams.dam_use_codes WHERE name_en = 'Unknown')
    WHEN use_code IS NULL THEN NULL
    ELSE use_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_code TYPE int2 USING use_code::int2;

UPDATE {sourceTable} SET use_irrigation_code =
    CASE
    WHEN LOWER(use_irrigation_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_irrigation_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_irrigation_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_irrigation_code IS NULL THEN NULL
    ELSE use_irrigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_irrigation_code TYPE int2 USING use_irrigation_code::int2;

UPDATE {sourceTable} SET use_electricity_code =
    CASE
    WHEN LOWER(use_electricity_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_electricity_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_electricity_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_electricity_code IS NULL THEN NULL
    ELSE use_electricity_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_electricity_code TYPE int2 USING use_electricity_code::int2;

UPDATE {sourceTable} SET use_supply_code =
    CASE
    WHEN LOWER(use_supply_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_supply_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_supply_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_supply_code IS NULL THEN NULL
    ELSE use_supply_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_supply_code TYPE int2 USING use_supply_code::int2;

UPDATE {sourceTable} SET use_floodcontrol_code =
    CASE
    WHEN LOWER(use_floodcontrol_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_floodcontrol_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_floodcontrol_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_floodcontrol_code IS NULL THEN NULL
    ELSE use_floodcontrol_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_floodcontrol_code TYPE int2 USING use_floodcontrol_code::int2;

UPDATE {sourceTable} SET use_recreation_code =
    CASE
    WHEN LOWER(use_recreation_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_recreation_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_recreation_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_recreation_code IS NULL THEN NULL
    ELSE use_recreation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_recreation_code TYPE int2 USING use_recreation_code::int2;

UPDATE {sourceTable} SET use_navigation_code =
    CASE
    WHEN LOWER(use_navigation_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_navigation_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_navigation_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_navigation_code IS NULL THEN NULL
    ELSE use_navigation_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_navigation_code TYPE int2 USING use_navigation_code::int2;

UPDATE {sourceTable} SET use_fish_code =
    CASE
    WHEN LOWER(use_fish_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_fish_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_fish_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_fish_code IS NULL THEN NULL
    ELSE use_fish_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_fish_code TYPE int2 USING use_fish_code::int2;

UPDATE {sourceTable} SET use_pollution_code =
    CASE
    WHEN LOWER(use_pollution_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_pollution_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_pollution_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_pollution_code IS NULL THEN NULL
    ELSE use_pollution_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_pollution_code TYPE int2 USING use_pollution_code::int2;

UPDATE {sourceTable} SET use_invasivespecies_code =
    CASE
    WHEN LOWER(use_invasivespecies_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_invasivespecies_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_invasivespecies_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_invasivespecies_code IS NULL THEN NULL
    ELSE use_invasivespecies_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_invasivespecies_code TYPE int2 USING use_invasivespecies_code::int2;

UPDATE {sourceTable} SET use_other_code =
    CASE
    WHEN LOWER(use_other_code) = 'main' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Main')
    WHEN LOWER(use_other_code) = 'major' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Major')
    WHEN LOWER(use_other_code) = 'secondary' THEN (select code::varchar FROM dams.use_codes WHERE name_en = 'Secondary')
    WHEN use_other_code IS NULL THEN NULL
    ELSE use_other_code END;
ALTER TABLE {sourceTable} ALTER COLUMN use_other_code TYPE int2 USING use_other_code::int2;

UPDATE {sourceTable} SET turbine_type_code =
    CASE
    WHEN turbine_type_code = 'cross-flow' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Cross-flow')
    WHEN turbine_type_code = 'francis' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Francis')
    WHEN turbine_type_code = 'kaplan' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Kaplan')
    WHEN turbine_type_code = 'pelton' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Pelton')
    WHEN turbine_type_code = 'other' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Other')
    WHEN turbine_type_code = 'unknown' THEN (select code::varchar FROM dams.turbine_type_codes WHERE name_en = 'Unknown')
    WHEN turbine_type_code IS NULL THEN NULL
    ELSE turbine_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN turbine_type_code TYPE int2 USING turbine_type_code::int2;

UPDATE {sourceTable} SET spillway_type_code =
    CASE
    WHEN LOWER(spillway_type_code) = 'combined' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Combined')
    WHEN LOWER(spillway_type_code) = 'free' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Free')
    WHEN LOWER(spillway_type_code) = 'gated' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Gated')
    WHEN LOWER(spillway_type_code) = 'other' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Other')
    WHEN LOWER(spillway_type_code) = 'none' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'None')
    WHEN LOWER(spillway_type_code) = 'unknown' THEN (select code::varchar FROM dams.spillway_type_codes WHERE name_en = 'Unknown')
    WHEN spillway_type_code IS NULL THEN NULL
    ELSE spillway_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN spillway_type_code TYPE int2 USING spillway_type_code::int2;

UPDATE {sourceTable} SET up_passage_type_code =
    CASE
    WHEN up_passage_type_code = 'denil' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Denil')
    WHEN up_passage_type_code = 'nature-like fishway' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Nature-like fishway')
    WHEN up_passage_type_code = 'pool and weir' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Pool and weir')
    WHEN up_passage_type_code = 'pool and weir with hole' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Pool and weir with hole')
    WHEN up_passage_type_code = 'trap and truck' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Trap and truck')
    WHEN up_passage_type_code = 'vertical slot' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Vertical slot')
    WHEN up_passage_type_code = 'other' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Other')
    WHEN up_passage_type_code = 'no structure' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'No structure')
    WHEN up_passage_type_code = 'unknown' THEN (select code::varchar FROM cabd.upstream_passage_type_codes WHERE name_en = 'Unknown')
    WHEN up_passage_type_code IS NULL THEN NULL
    ELSE up_passage_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN up_passage_type_code TYPE int2 USING up_passage_type_code::int2;
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
    use_conservation_code,
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
    submitted_on,
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
    use_conservation_code,
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

print("Cleaning CSV")
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
    print("Adding records to " + damUpdateTable)
    cursor.execute(moveQuery)
conn.commit()
conn.close()

print("Script complete")
