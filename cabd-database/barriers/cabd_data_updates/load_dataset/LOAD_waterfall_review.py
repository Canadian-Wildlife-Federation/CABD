import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dataFile = ""
dataFile = sys.argv[1]
#provinceCode should be 'ab', 'bc', etc. but this is just to grab the correct layer from gpkg
#e.g., provinceCode of 'atlantic' is fine if the layer you want is named 'atlantic_waterfall_review'
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
ALTER TABLE {workingTable} ADD COLUMN fall_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN fall_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {workingTable} DROP COLUMN IF EXISTS province;
ALTER TABLE {workingTable} ADD COLUMN province_territory_code varchar(2);
ALTER TABLE {workingTable} ALTER COLUMN nhn_workunit_id TYPE varchar(7);
ALTER TABLE {workingTable} ADD COLUMN municipality varchar(512);
ALTER TABLE {workingTable} ADD COLUMN fall_height_m real;
ALTER TABLE {workingTable} ADD COLUMN last_modified date;
ALTER TABLE {workingTable} ADD COLUMN comments text;
ALTER TABLE {workingTable} ADD COLUMN complete_level_code smallint;
ALTER TABLE {workingTable} ADD COLUMN snapped_point geometry(Point, 4617);
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(Point, 4617);
ALTER TABLE {workingTable} ADD COLUMN passability_status_code smallint;

ALTER TABLE {workingTable} ALTER COLUMN data_source TYPE varchar;
ALTER TABLE {workingTable} RENAME COLUMN data_source TO data_source_text;
UPDATE {workingTable} SET data_source_text = 
    CASE
    WHEN data_source_text = '1' THEN 'ncc'
    WHEN data_source_text = '2' THEN 'nswf'
    WHEN data_source_text = '5' THEN 'fwa'
    WHEN data_source_text = '6' THEN 'ab_basefeatures'
    WHEN data_source_text = '8' THEN 'ohn'
    WHEN data_source_text = '11' THEN 'fishwerks'
    WHEN data_source_text = '12' THEN 'fiss'
    WHEN data_source_text = '17' THEN 'nhn'
    WHEN data_source_text = '21' THEN 'cwf'
    WHEN data_source_text = '24' THEN 'cgndb'
    WHEN data_source_text = '26' THEN 'sk_hydro'
    WHEN data_source_text = '29' THEN 'canvec_hy_obstacles'
    WHEN data_source_text = '30' THEN 'wikipedia'
    WHEN data_source_text = '31' THEN 'nbhn_hy_obstacles'
    WHEN data_source_text = '32' THEN 'nbwf'
    WHEN data_source_text = '33' THEN 'mrmaps_nswf'
    ELSE NULL END;

ALTER TABLE {workingTable} ADD COLUMN data_source uuid;
UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source_text = 'ab_basefeatures' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy')
    WHEN data_source_text = 'canvec_hy_obstacles' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_hyf')
    WHEN data_source_text = 'cgndb' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb')
    WHEN data_source_text = 'cwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'cwf')
    WHEN data_source_text = 'fishwerks' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks')
    WHEN data_source_text = 'fiss' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'bceccs_fiss')
    WHEN data_source_text = 'fwa' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa')
    WHEN data_source_text = 'mrmaps_nswf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nswf')
    WHEN data_source_text = 'nbhn_hy_obstacles' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_ho')
    WHEN data_source_text = 'nbwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf')
    WHEN data_source_text = 'ncc' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab')
    WHEN data_source_text = 'nhn' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn')
    WHEN data_source_text = 'nswf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf')
    WHEN data_source_text = 'ohn' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn')
    WHEN data_source_text = 'sk_hydro' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography')
    WHEN data_source_text = 'wikipedia' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs')
    END;

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
    ADD CONSTRAINT waterfalls_fk_2 FOREIGN KEY (complete_level_code) REFERENCES waterfalls.waterfall_complete_level_codes (code),
    ADD CONSTRAINT waterfalls_fk_4 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes (code)
;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)

#snap points
# print("Snapping to CHyF network...")
# snapQuery = f"""
# UPDATE {workingTable} SET original_point = ST_GeometryN(geometry, 1);
# SELECT featurecopy.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
# UPDATE {workingTable} SET snapped_point = original_point WHERE snapped_point IS NULL;

# CREATE INDEX {workingTableRaw}_idx ON {workingTable} USING gist(snapped_point);
# """

# with conn.cursor() as cursor:
#     cursor.execute(snapQuery)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)