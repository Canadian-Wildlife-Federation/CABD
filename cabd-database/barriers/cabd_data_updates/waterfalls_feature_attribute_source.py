import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dbUser = sys.argv[1]
dbPassword = sys.argv[2]

workingSchema = "featurecopy"
workingTableRaw = "waterfalls"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "waterfalls_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw
featureTableRaw = "waterfalls_feature_source"
featureTable = workingSchema + "." + featureTableRaw

print("Connecting to database...")

conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

print("Adding data source ids...")
loadQuery = f"""

UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source_text = 'aep_bf_hy' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy')
    WHEN data_source_text = 'bceccs_fiss' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'bceccs_fiss')
    WHEN data_source_text = 'bcflnrord_fwa' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa')
    WHEN data_source_text = 'cwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'cwf')
    WHEN data_source_text = 'mndmnrf_ohn' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn')
    WHEN data_source_text = 'mrmaps_nbwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf')
    WHEN data_source_text = 'mrmaps_nswf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nswf')
    WHEN data_source_text = 'nberd_nbhn_ho' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_ho')
    WHEN data_source_text = 'ncc_chu_ab' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab')
    WHEN data_source_text = 'nrcan_canvec_hyf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_hyf')
    WHEN data_source_text = 'nrcan_cgndb' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb')
    WHEN data_source_text = 'nrcan_nhn' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn')
    WHEN data_source_text = 'nse_td_wf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf')
    WHEN data_source_text = 'skmoe_hydrography' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography')
    WHEN data_source_text = 'wid_fishwerks' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks')
    WHEN data_source_text = 'wiki_cdn_wfs' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs')
    ELSE NULL END;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

print("Adding rows to attribute_source and feature_source tables...")

loadQuery = f"""

TRUNCATE TABLE {featureTable};

--insert any missing rows into attribute_source table (i.e., new features added between import and now)

INSERT INTO {attributeTable} (cabd_id)
	(SELECT cabd_id FROM {workingTable} WHERE cabd_id NOT IN
	(SELECT cabd_id FROM {attributeTable}));

--insert rows into feature_source table from data source columns

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy'), data_source_id
FROM {workingTable} WHERE data_source_text = 'aep_bf_hy'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mndmnrf_ohn'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mrmaps_nbwf'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), data_source_id
FROM {workingTable} WHERE data_source_text = 'ncc_chu_ab'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_nhn'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nse_td_wf'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), data_source_id
FROM {workingTable} WHERE data_source_text = 'skmoe_hydrography'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wid_fishwerks'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wiki_cdn_wfs'
ON CONFLICT DO NOTHING;


--insert rows into feature_source table from named columns
--IMPORTANT: comment out any lines below where column does not exist in your workingTable

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy'), aep_bf_hy
FROM {workingTable} WHERE aep_bf_hy IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn'), mndmnrf_ohn
FROM {workingTable} WHERE mndmnrf_ohn IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mrmaps_nbwf'), mrmaps_nbwf
FROM {workingTable} WHERE mrmaps_nbwf IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), ncc_chu_ab
FROM {workingTable} WHERE ncc_chu_ab IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn'), nrcan_nhn
FROM {workingTable} WHERE nrcan_nhn IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf'), nse_td_wf
FROM {workingTable} WHERE nse_td_wf IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), skmoe_hydrography
FROM {workingTable} WHERE skmoe_hydrography IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks'), wid_fishwerks
FROM {workingTable} WHERE wid_fishwerks IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_cdn_wfs'), wiki_cdn_wfs
FROM {workingTable} WHERE wiki_cdn_wfs IS NOT NULL
ON CONFLICT DO NOTHING;

"""

with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()
conn.close()

print("\n" + "Script complete! All attribute source and feature source rows created for " + workingTable)