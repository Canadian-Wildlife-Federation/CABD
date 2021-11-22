import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dataFile = ""
dataFile = sys.argv[1]
#provinceCode should be 'ab', 'bc', etc.
provinceCode = sys.argv[2]
dbUser = sys.argv[3]
dbPassword = sys.argv[4]

if dataFile == '':
    print("Data file required. Usage: LOAD_waterfall_review.py <datafile> <provinceCode> <dbUser> <dbPassword>")
    sys.exit()

#this is the temporary table the data is loaded into
workingSchema = "featurecopy"
workingTableRaw = "waterfalls"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "waterfalls_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw

#maximum distance for snapping barriers to stream network in meters
snappingDistance = 150

print("Loading data from file " +  dataFile)

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

query = f"""
CREATE SCHEMA IF NOT EXISTS {workingSchema};
ALTER TABLE {attributeTable} DROP CONSTRAINT IF EXISTS {workingTableRaw}_attribute_source_cabd_id_fkey;
DROP TABLE IF EXISTS {workingTable} CASCADE;
TRUNCATE TABLE {attributeTable};
"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' '"' + dataFile + '" ' + provinceCode + "_waterfall_review"
print(pycmd)
subprocess.run(pycmd)

#add columns from cabd
print("Adding feature columns...")
loadQuery = f"""

ALTER TABLE {workingTable} ADD COLUMN cabd_id uuid;
UPDATE {workingTable} SET cabd_id = uuid_generate_v4();
ALTER TABLE {workingTable} DROP CONSTRAINT {workingTableRaw}_pkey;
ALTER TABLE {workingTable} ADD PRIMARY KEY (cabd_id);

ALTER TABLE {workingTable} ADD COLUMN latitude double precision;
ALTER TABLE {workingTable} ADD COLUMN longitude double precision;
ALTER TABLE {workingTable} ADD COLUMN fall_name_en character varying(512);
ALTER TABLE {workingTable} ADD COLUMN fall_name_fr character varying(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en character varying(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr character varying(512);
ALTER TABLE {workingTable} DROP COLUMN IF EXISTS province;
ALTER TABLE {workingTable} ADD COLUMN province_territory_code character varying(2) NOT NULL;
ALTER TABLE {workingTable} ALTER COLUMN nhn_workunit_id TYPE varchar(7);
ALTER TABLE {workingTable} ADD COLUMN municipality character varying(512);
ALTER TABLE {workingTable} ADD COLUMN fall_height_m real;
ALTER TABLE {workingTable} ADD COLUMN last_modified date;
ALTER TABLE {workingTable} ADD COLUMN comments text;
ALTER TABLE {workingTable} ADD COLUMN complete_level_code smallint;
ALTER TABLE {workingTable} ADD COLUMN snapped_point geometry(Point, 4617);
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(Point, 4617);
ALTER TABLE {workingTable} ADD COLUMN passability_status_code smallint;

ALTER TABLE {workingTable} ALTER COLUMN data_source TYPE varchar;
UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source = '1' THEN 'ncc'
    WHEN data_source = '2' THEN 'nswf'
    WHEN data_source = '3' THEN 'odi'
    WHEN data_source = '4' THEN 'wrispublicdams'
    WHEN data_source = '5' THEN 'fwa'
    WHEN data_source = '6' THEN 'ab_basefeatures'
    WHEN data_source = '7' THEN 'cehq'
    WHEN data_source = '8' THEN 'ohn'
    WHEN data_source = '9' THEN 'nbhn'
    WHEN data_source = '10' THEN 'gfielding'
    WHEN data_source = '11' THEN 'fishwerks'
    WHEN data_source = '12' THEN 'fiss'
    WHEN data_source = '13' THEN 'publicdamskml'
    WHEN data_source = '14' THEN 'nlprov'
    WHEN data_source = '15' THEN 'npdp'
    WHEN data_source = '16' THEN 'canvec'
    WHEN data_source = '17' THEN 'nhn'
    WHEN data_source = '18' THEN 'goodd'
    WHEN data_source = '19' THEN 'grand'
    WHEN data_source = '20' THEN 'fao'
    WHEN data_source = '21' THEN 'cwf'
    WHEN data_source = '22' THEN 'canfishpass'
    WHEN data_source = '23' THEN 'mbprov'
    WHEN data_source = '24' THEN 'cgndb'
    WHEN data_source = '25' THEN 'wsa_sk'
    WHEN data_source = '26' THEN 'sk_hydro'
    WHEN data_source = '27' THEN 'bc_hydro_wiki'
    WHEN data_source = '28' THEN 'lsds'
    ELSE NULL END;
"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

print("Fetching constraints...")
loadQuery = f"""

INSERT INTO {attributeTable} (cabd_id) SELECT cabd_id FROM {workingTable};

ALTER TABLE {attributeTable} ADD CONSTRAINT {workingTableRaw}_cabd_id_fkey FOREIGN KEY (cabd_id) REFERENCES {workingTable} (cabd_id);

ALTER TABLE {workingTable}
    ADD CONSTRAINT waterfalls_fk_1 FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes (code),
    ADD CONSTRAINT waterfalls_fk_2 FOREIGN KEY (complete_level_code) REFERENCES waterfalls.waterfall_complete_level_codes (code).
    ADD CONSTRAINT waterfalls_fk_4 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes (code)
;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)

#snap points
print("Snapping to CHyF network...")
snapQuery = f"""
UPDATE {workingTable} SET original_point = ST_GeometryN(geometry, 1);
SELECT featurecopy.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
UPDATE {workingTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

CREATE INDEX {workingTableRaw}_idx ON {workingTable} USING gist (snapped_point);
"""

with conn.cursor() as cursor:
    cursor.execute(snapQuery)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)