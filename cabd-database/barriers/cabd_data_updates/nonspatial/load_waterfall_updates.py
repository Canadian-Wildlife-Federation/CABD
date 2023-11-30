# This script loads a CSV into the database containing updated attribute information for waterfalls
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
fallUpdateTable = updateSchema + '.waterfall_updates'

fallSchema = "waterfalls"
fallTable = fallSchema + ".waterfalls"

if len(sys.argv) != 3:
    print("Invalid usage: py load_waterfall_updates.py <dataFile> <tableName>")
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

--create fallUpdateTable with proper format if not exists
CREATE TABLE IF NOT EXISTS {fallUpdateTable} (LIKE {fallTable});
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS id serial PRIMARY KEY;
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS latitude decimal(8,6);
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS longitude decimal(9,6);
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS entry_classification varchar;
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS data_source_short_name varchar;
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS reviewer_comments varchar;
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS update_type varchar;
ALTER TABLE {fallUpdateTable} ADD COLUMN IF NOT EXISTS submitted_on varchar;

ALTER TABLE {fallUpdateTable} DROP CONSTRAINT IF EXISTS status_check;
ALTER TABLE {fallUpdateTable} ADD CONSTRAINT status_check CHECK (update_status IN ('needs review', 'ready', 'wait', 'done'));
ALTER TABLE {fallUpdateTable} DROP CONSTRAINT IF EXISTS update_type_check;
ALTER TABLE {fallUpdateTable} ADD CONSTRAINT update_type_check CHECK (update_type IN ('cwf', 'user'));

--make sure records are not duplicated
ALTER TABLE {fallUpdateTable} DROP CONSTRAINT IF EXISTS fall_record_unique;
ALTER TABLE {fallUpdateTable} ADD CONSTRAINT fall_record_unique UNIQUE (cabd_id, data_source_short_name);

--ALTER TABLE IF EXISTS {fallUpdateTable} OWNER to cabd;
--ALTER TABLE IF EXISTS {sourceTable} OWNER to cabd;

--clean CSV input
DELETE FROM {sourceTable} WHERE "status" IN ('complete', 'do not process', 'on hold');
ALTER TABLE {sourceTable} DROP COLUMN "status";
ALTER TABLE {sourceTable} DROP COLUMN IF EXISTS "item type";
ALTER TABLE {sourceTable} DROP COLUMN IF EXISTS "path";
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
ALTER TABLE {sourceTable} ALTER COLUMN province_territory_code TYPE varchar USING province_territory_code::varchar;
ALTER TABLE {sourceTable} ALTER COLUMN use_analysis TYPE boolean USING use_analysis::boolean;

--trim varchars and categorical fields that are not coded values
UPDATE {sourceTable} SET email = TRIM(email);
UPDATE {sourceTable} SET data_source_short_name = TRIM(data_source_short_name);

--deal with coded value fields
UPDATE {sourceTable} SET province_territory_code = LOWER(province_territory_code);

UPDATE {sourceTable} SET passability_status_code =
    CASE
    WHEN passability_status_code = 'barrier' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Barrier')
    WHEN passability_status_code = 'partial barrier' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Partial Barrier')
    WHEN passability_status_code = 'passable' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Passable')
    WHEN passability_status_code = 'unknown' THEN (select code::varchar FROM cabd.passability_status_codes WHERE name_en = 'Unknown')
    WHEN passability_status_code IS NULL THEN NULL
    ELSE passability_status_code END;
ALTER TABLE {sourceTable} ALTER COLUMN passability_status_code TYPE int2 USING passability_status_code::int2;

UPDATE {sourceTable} SET waterfall_type_code =
    CASE
    WHEN waterfall_type_code = 'cascade' THEN (select code::varchar FROM waterfalls.waterfall_type_codes WHERE name_en = 'Cascade')
    WHEN waterfall_type_code = 'cascade or chute' THEN (select code::varchar FROM waterfalls.waterfall_type_codes WHERE name_en = 'Cascade or Chute')
    WHEN waterfall_type_code = 'cascade/chute' THEN (select code::varchar FROM waterfalls.waterfall_type_codes WHERE name_en = 'Cascade/Chute')
    WHEN waterfall_type_code = 'unknown' THEN (select code::varchar FROM waterfalls.waterfall_type_codes WHERE name_en = 'Unknown')
    WHEN waterfall_type_code IS NULL THEN NULL
    ELSE waterfall_type_code END;
ALTER TABLE {sourceTable} ALTER COLUMN waterfall_type_code TYPE int2 USING waterfall_type_code::int2;

"""

moveQuery = f"""
--move updates into staging table
INSERT INTO {fallUpdateTable} (
    cabd_id,
    latitude,
    longitude,
    entry_classification,
    data_source_short_name,
    update_status,
    reviewer_comments,
    update_type,
    submitted_on,
    fall_name_en,
    fall_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    province_territory_code,
    fall_height_m,
    comments,
    passability_status_code,
    waterfall_type_code,
    use_analysis
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
    fall_name_en,
    fall_name_fr,
    waterbody_name_en,
    waterbody_name_fr,
    province_territory_code,
    fall_height_m,
    comments,
    passability_status_code,
    waterfall_type_code,
    use_analysis
FROM {sourceTable};
"""

print("Cleaning CSV")
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
    print("Adding records to " + fallUpdateTable)
    cursor.execute(moveQuery)
conn.commit()
conn.close()

print("Script complete")
