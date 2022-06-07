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
    print("Data file required. Usage: py LOAD_waterfall_review.py <datafile> <provinceCode> <dbUser> <dbPassword>")
    sys.exit()

#this is the temporary table the data is loaded into
workingSchema = "featurecopy"
workingTableRaw = "waterfalls"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "waterfalls_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw
featureTableRaw = "waterfalls_feature_source"
featureTable = workingSchema + "." + featureTableRaw

#maximum distance for snapping barriers to stream network in meters
snappingDistance = 150

print("Loading data from file " +  dataFile)

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

#note that the attribute table has been created ahead of time with all constraints from production attribute table
query = f"""
CREATE SCHEMA IF NOT EXISTS {workingSchema};
ALTER TABLE {attributeTable} DROP CONSTRAINT IF EXISTS {workingTableRaw}_cabd_id_fkey;
DROP TABLE IF EXISTS {workingTable} CASCADE;
TRUNCATE TABLE {attributeTable};
TRUNCATE TABLE {featureTable};
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
ALTER TABLE {workingTable} DROP COLUMN fid;

ALTER TABLE {workingTable} ADD COLUMN fall_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN fall_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {workingTable} DROP COLUMN IF EXISTS province;
ALTER TABLE {workingTable} ADD COLUMN province_territory_code varchar(2);
ALTER TABLE {workingTable} ALTER COLUMN nhn_watershed_id TYPE varchar(7);
ALTER TABLE {workingTable} ADD COLUMN municipality varchar(512);
ALTER TABLE {workingTable} ADD COLUMN fall_height_m real;
ALTER TABLE {workingTable} ADD COLUMN last_modified date;
ALTER TABLE {workingTable} ADD COLUMN comments text;
ALTER TABLE {workingTable} ADD COLUMN complete_level_code smallint;
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(Point, 4617);
ALTER TABLE {workingTable} ADD COLUMN snapped_point geometry(Point, 4617);
ALTER TABLE {workingTable} ADD COLUMN passability_status_code smallint;

ALTER TABLE {workingTable} ADD COLUMN data_source uuid;
UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source_text = 'aep_bf_hy' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy')
    WHEN data_source_text = 'nrcan_canvec_hyf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_hyf')
    WHEN data_source_text = 'nrcan_cgndb' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb')
    WHEN data_source_text = 'cwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'cwf')
    WHEN data_source_text = 'wid_fishwerks' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks')
    WHEN data_source_text = 'bceccs_fiss' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'bceccs_fiss')
    WHEN data_source_text = 'bcflnrord_fwa' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa')
    WHEN data_source_text = 'mrmaps_nswf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nswf')
    WHEN data_source_text = 'nberd_nbhn_ho' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_ho')
    WHEN data_source_text = 'mrmaps_nbwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf')
    WHEN data_source_text = 'ncc_chu_ab' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab')
    WHEN data_source_text = 'nrcan_nhn' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn')
    WHEN data_source_text = 'nse_td_wf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf')
    WHEN data_source_text = 'mndmnrf_ohn' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn')
    WHEN data_source_text = 'skmoe_hydrography' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography')
    WHEN data_source_text = 'wiki_cdn_wfs' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs')
    END;

UPDATE {workingTable} SET original_point = ST_GeometryN(geometry, 1);
UPDATE {workingTable} SET snapped_point = original_point WHERE snapped_point IS NULL;
CREATE INDEX {workingTableRaw}_idx ON {workingTable} USING gist(snapped_point);

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

print("Adding constraints...")
loadQuery = f"""

INSERT INTO {attributeTable} (cabd_id) SELECT cabd_id FROM {workingTable};

ALTER TABLE {attributeTable} ADD CONSTRAINT {workingTableRaw}_cabd_id_fkey FOREIGN KEY (cabd_id)
    REFERENCES {workingTable} (cabd_id)
    ON UPDATE NO ACTION
    ON DELETE CASCADE;

ALTER TABLE {workingTable}
    ADD CONSTRAINT waterfalls_fk_1 FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes (code),
    ADD CONSTRAINT waterfalls_fk_2 FOREIGN KEY (complete_level_code) REFERENCES waterfalls.waterfall_complete_level_codes (code),
    ADD CONSTRAINT waterfalls_fk_4 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes (code);

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)

#set up feature source table
print("Creating feature_source table...")
loadQuery = f"""

DROP TABLE IF EXISTS {featureTable};
CREATE TABLE {featureTable} (
    cabd_id uuid NOT NULL,
    datasource_id uuid NOT NULL,
    datasource_feature_id character varying NOT NULL,
    CONSTRAINT waterfalls_feature_source_pkey PRIMARY KEY (cabd_id, datasource_id),
    CONSTRAINT waterfalls_datasource_id_fk FOREIGN KEY (datasource_id)
        REFERENCES cabd.data_source (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE RESTRICT,
    CONSTRAINT waterfalls_feature_source_cabd_id_fk FOREIGN KEY (cabd_id)
        REFERENCES {workingTable} (cabd_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);
ALTER TABLE {featureTable} OWNER to cabd;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

loadQuery = f"""

--insert rows into feature_source table from data source columns

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy'), data_source_id
FROM {workingTable} WHERE data_source_text = 'aep_bf_hy'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_hyf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_canvec_hyf'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_cgndb'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wid_fishwerks'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bceccs_fiss'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bceccs_fiss'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bcflnrord_fwa'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nswf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mrmaps_nswf'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_ho'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nberd_nbhn_ho'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mrmaps_nbwf'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), data_source_id
FROM {workingTable} WHERE data_source_text = 'ncc_chu_ab'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_nhn'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nse_td_wf'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mndmnrf_ohn'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), data_source_id
FROM {workingTable} WHERE data_source_text = 'skmoe_hydrography'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wiki_cdn_wfs'
ON CONFLICT DO NOTHING;

--insert rows into feature_source table from named columns

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy'), aep_bf_hy
FROM {workingTable} WHERE aep_bf_hy IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_hyf'), nrcan_canvec_hyf
FROM {workingTable} WHERE nrcan_canvec_hyf IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb'), nrcan_cgndb
FROM {workingTable} WHERE nrcan_cgndb IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks'), wid_fishwerks
FROM {workingTable} WHERE wid_fishwerks IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bceccs_fiss'), bceccs_fiss
FROM {workingTable} WHERE bceccs_fiss IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa'), bcflnrord_fwa
FROM {workingTable} WHERE bcflnrord_fwa IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nswf'), mrmaps_nswf
FROM {workingTable} WHERE mrmaps_nswf IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_ho'), nberd_nbhn_ho
FROM {workingTable} WHERE nberd_nbhn_ho IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf'), mrmaps_nbwf
FROM {workingTable} WHERE mrmaps_nbwf IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), ncc_chu_ab
FROM {workingTable} WHERE ncc_chu_ab IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn'), nrcan_nhn
FROM {workingTable} WHERE nrcan_nhn IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf'), nse_td_wf
FROM {workingTable} WHERE nse_td_wf IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn'), mndmnrf_ohn
FROM {workingTable} WHERE mndmnrf_ohn IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), skmoe_hydrography
FROM {workingTable} WHERE skmoe_hydrography IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs'), wiki_cdn_wfs
FROM {workingTable} WHERE wiki_cdn_wfs IS NOT NULL
ON CONFLICT DO NOTHING;

"""

#snap points - commented out until we have chyf networks ready
# print("Snapping to CHyF network...")
# snapQuery = f"""
# UPDATE {workingTable} SET original_point = ST_GeometryN(geometry, 1);
# SELECT featurecopy.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
# UPDATE {workingTable} SET snapped_point = original_point WHERE snapped_point IS NULL;
# CREATE INDEX {workingTableRaw}_idx ON {workingTable} USING gist(snapped_point);
# """

# with conn.cursor() as cursor:
#     cursor.execute(snapQuery)

# conn.commit()
conn.close()

print("\n" + "Script complete! Data loaded into table: " + workingTable)

print(loadQuery)
print("Run the query above to insert rows into the feature_source table")