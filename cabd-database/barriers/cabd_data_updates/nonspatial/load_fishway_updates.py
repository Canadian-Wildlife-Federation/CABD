# This script loads a CSV into the database containing updated attribute information for fishways
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

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

updateSchema = "cabd"
fishUpdateTable = updateSchema + '.fishway_updates'

fishSchema = "fishways"
fishTable = fishSchema + ".fishways"

if len(sys.argv) != 3:
    print("Invalid usage: py load_fishway_updates.py <dataFile> <tableName>")
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

--create fishUpdateTable with proper format if not exists
CREATE TABLE IF NOT EXISTS {fishUpdateTable} (LIKE {fishTable});
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS id serial PRIMARY KEY;
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS latitude decimal(8,6);
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS longitude decimal(9,6);
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS entry_classification varchar;
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS data_source_short_name varchar;
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS reviewer_comments varchar;
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS update_type varchar;
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS submitted_on varchar;
ALTER TABLE {fishUpdateTable} ALTER COLUMN fishpass_type_code DROP NOT NULL;

ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS known_notuse varchar;
ALTER TABLE {fishUpdateTable} ADD COLUMN IF NOT EXISTS known_use varchar;

ALTER TABLE {fishUpdateTable} DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE {fishUpdateTable} ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'wait', 'done'));
ALTER TABLE {fishUpdateTable} DROP CONSTRAINT IF EXISTS update_type_check;
ALTER TABLE {fishUpdateTable} ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE {fishUpdateTable} DROP CONSTRAINT IF EXISTS fish_record_unique;
ALTER TABLE {fishUpdateTable} ADD CONSTRAINT fish_record_unique UNIQUE (cabd_id, data_source_short_name);

--ALTER TABLE IF EXISTS {fishUpdateTable} OWNER to cabd;
--ALTER TABLE IF EXISTS {sourceTable} OWNER to cabd;

--clean CSV input
DELETE FROM {sourceTable} WHERE "status" IN ('complete', 'do not process', 'on hold');
ALTER TABLE {sourceTable} DROP COLUMN "status";
ALTER TABLE {sourceTable} DROP COLUMN "item type";
ALTER TABLE {sourceTable} DROP COLUMN "path";
ALTER TABLE {sourceTable} ADD CONSTRAINT entry_classification_check CHECK (entry_classification IN ('new feature', 'modify feature', 'delete feature'));
ALTER TABLE {sourceTable} ADD COLUMN IF NOT EXISTS "status" varchar;
UPDATE {sourceTable} SET reviewer_comments = TRIM(reviewer_comments);
UPDATE {sourceTable} SET reviewer_comments = NULL WHERE reviewer_comments = '';
UPDATE {sourceTable} SET "status" = 'ready' WHERE reviewer_comments IS NULL;
UPDATE {sourceTable} SET "status" = 'needs review' WHERE reviewer_comments IS NOT NULL;
ALTER TABLE {sourceTable} ADD COLUMN update_type varchar default 'cwf';
UPDATE {sourceTable} SET cabd_id = gen_random_uuid() WHERE entry_classification = 'new feature' AND cabd_id IS NULL;


--trim fields that are getting a type conversion
UPDATE {sourceTable} SET cabd_id = TRIM(cabd_id);

--change field types
ALTER TABLE {sourceTable} ALTER COLUMN cabd_id TYPE uuid USING cabd_id::uuid;
ALTER TABLE {sourceTable} ALTER COLUMN dam_id TYPE uuid USING dam_id::uuid;
ALTER TABLE {sourceTable} ALTER COLUMN province_territory_code TYPE varchar USING province_territory_code::varchar;
ALTER TABLE {sourceTable} ALTER COLUMN depth_m TYPE real USING depth_m::real;
ALTER TABLE {sourceTable} ALTER COLUMN designed_on_biology TYPE boolean USING designed_on_biology::boolean;
ALTER TABLE {sourceTable} ALTER COLUMN estimate_of_attraction_pct TYPE real USING estimate_of_attraction_pct::real;
ALTER TABLE {sourceTable} ALTER COLUMN estimate_of_passage_success_pct TYPE real USING estimate_of_passage_success_pct::real;
ALTER TABLE {sourceTable} ALTER COLUMN has_evaluating_studies TYPE boolean USING has_evaluating_studies::boolean;
ALTER TABLE {sourceTable} ALTER COLUMN max_fishway_velocity_ms TYPE real USING max_fishway_velocity_ms::real;
ALTER TABLE {sourceTable} ALTER COLUMN modified TYPE boolean USING modified::boolean;

--trim varchars and categorical fields that are not coded values
UPDATE {sourceTable} SET email = TRIM(email);
UPDATE {sourceTable} SET data_source_short_name = TRIM(data_source_short_name);

--deal with coded value fields
UPDATE {sourceTable} SET province_territory_code = LOWER(province_territory_code);

UPDATE {sourceTable} SET entrance_location_code = b.code
    FROM fishways.entrance_location_codes b
    WHERE b.name_en = initcap(entrance_location_code);
ALTER TABLE {sourceTable} ALTER COLUMN entrance_location_code TYPE int2 USING entrance_location_code::int2;

UPDATE {sourceTable} SET entrance_position_code = b.code
    FROM fishways.entrance_position_codes b
    WHERE b.name_en = initcap(entrance_position_code);
ALTER TABLE {sourceTable} ALTER COLUMN entrance_position_code TYPE int2 USING entrance_position_code::int2;

UPDATE {sourceTable} SET fishpass_type_code = b.code
    FROM cabd.upstream_passage_type_codes b
    WHERE b.name_en ilike fishpass_type_code;
ALTER TABLE {sourceTable} ALTER COLUMN fishpass_type_code TYPE int2 USING fishpass_type_code::int2;

"""

moveQuery = f"""
--move updates into staging table
INSERT INTO {fishUpdateTable} (
    cabd_id,
    latitude,
    longitude,
    entry_classification,
    data_source_short_name,
    update_status,
    reviewer_comments,
    update_type,
    submitted_on,
    architect,
    constructed_by,
    contracted_by,
    dam_id,
    depth_m,
    designed_on_biology,
    elevation_m,
    engineering_notes,
    entrance_location_code,
    entrance_position_code,
    estimate_of_attraction_pct,
    estimate_of_passage_success_pct,
    fishpass_type_code,
    fishway_reference_id,
    gradient,
    has_evaluating_studies,
    known_notuse,
    known_use,
    length_m,
    max_fishway_velocity_ms,
    mean_fishway_velocity_ms,
    modification_purpose,
    modification_year,
    modified,
    monitoring_equipment,
    nature_of_evaluation_studies,
    operated_by,
    operating_notes,
    operation_period,
    plans_held_by,
    province_territory_code,
    purpose,
    river_name_en,
    river_name_fr,
    structure_name_en,
    structure_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    year_constructed
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
    architect,
    constructed_by,
    contracted_by,
    dam_id,
    depth_m,
    designed_on_biology,
    elevation_m,
    engineering_notes,
    entrance_location_code,
    entrance_position_code,
    estimate_of_attraction_pct,
    estimate_of_passage_success_pct,
    fishpass_type_code,
    fishway_reference_id,
    gradient,
    has_evaluating_studies,
    known_notuse,
    known_use,
    length_m,
    max_fishway_velocity_ms,
    mean_fishway_velocity_ms,
    modification_purpose,
    modification_year,
    modified,
    monitoring_equipment,
    nature_of_evaluation_studies,
    operated_by,
    operating_notes,
    operation_period,
    plans_held_by,
    province_territory_code,
    purpose,
    river_name_en,
    river_name_fr,
    structure_name_en,
    structure_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    year_constructed
FROM {sourceTable};
"""

print("Cleaning CSV")
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
    print("Adding records to " + fishUpdateTable)
    cursor.execute(moveQuery)
conn.commit()
conn.close()

print("Script complete")
