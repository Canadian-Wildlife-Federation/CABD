import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dbUser = sys.argv[1]
dbPassword = sys.argv[2]

workingSchema = "featurecopy"
workingTableRaw = "dams"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "dams_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw
featureTableRaw = "dams_feature_source"
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
    WHEN data_source_text = 'aep_bf_hy' THEN (SELECT id FROM cabd.data_source WHERE name = 'aep_bf_hy')
    WHEN data_source_text = 'bceccs_fiss' THEN (SELECT id FROM cabd.data_source WHERE name = 'bceccs_fiss')
    WHEN data_source_text = 'bcflnrord_fwa' THEN (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_fwa')
    WHEN data_source_text = 'bcflnrord_kml_pubdams' THEN (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_kml_pubdams')
    WHEN data_source_text = 'bcflnrord_wris_pubdams' THEN (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_wris_pubdams')
    WHEN data_source_text = 'cwf' THEN (SELECT id FROM cabd.data_source WHERE "name" = 'cwf')
    WHEN data_source_text = 'cwf_canfish' THEN (SELECT id FROM cabd.data_source WHERE name = 'cwf_canfish')
    WHEN data_source_text = 'fao_aquastat' THEN (SELECT id FROM cabd.data_source WHERE name = 'fao_aquastat')
    WHEN data_source_text = 'gdw_goodd' THEN (SELECT id FROM cabd.data_source WHERE name = 'gdw_goodd')
    WHEN data_source_text = 'gdw_grand' THEN (SELECT id FROM cabd.data_source WHERE name = 'gdw_grand')
    WHEN data_source_text = 'megis_impounds' THEN (SELECT id FROM cabd.data_source WHERE name = 'megis_impounds')
    WHEN data_source_text = 'mi_prov_ww' THEN (SELECT id FROM cabd.data_source WHERE name = 'mi_prov_ww')
    WHEN data_source_text = 'mndmnrf_odi' THEN (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_odi')
    WHEN data_source_text = 'mndmnrf_ohn' THEN (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_ohn')
    WHEN data_source_text = 'nberd_nbhn_mmh' THEN (SELECT id FROM cabd.data_source WHERE name = 'nberd_nbhn_mmh')
    WHEN data_source_text = 'ncc_chu_ab' THEN (SELECT id FROM cabd.data_source WHERE name = 'ncc_chu_ab')
    WHEN data_source_text = 'nleccm_nlpdi' THEN (SELECT id FROM cabd.data_source WHERE name = 'nleccm_nlpdi')
    WHEN data_source_text = 'nrcan_canvec_mm' THEN (SELECT id FROM cabd.data_source WHERE name = 'nrcan_canvec_mm')
    WHEN data_source_text = 'nrcan_cgndb' THEN (SELECT id FROM cabd.data_source WHERE name = 'nrcan_cgndb')
    WHEN data_source_text = 'nrcan_nhn' THEN (SELECT id FROM cabd.data_source WHERE name = 'nrcan_nhn')
    WHEN data_source_text = 'nse_td_wf' THEN (SELECT id FROM cabd.data_source WHERE name = 'nse_td_wf')
    WHEN data_source_text = 'nse_wcsd_gfielding' THEN (SELECT id FROM cabd.data_source WHERE name = 'nse_wcsd_gfielding')
    WHEN data_source_text = 'qmelcc_repbarrages' THEN (SELECT id FROM cabd.data_source WHERE name = 'qmelcc_repbarrages')
    WHEN data_source_text = 'skmoe_hydrography' THEN (SELECT id FROM cabd.data_source WHERE name = 'skmoe_hydrography')
    WHEN data_source_text = 'su_npdp' THEN (SELECT id FROM cabd.data_source WHERE name = 'su_npdp')
    WHEN data_source_text = 'swp_lsdi' THEN (SELECT id FROM cabd.data_source WHERE name = 'swp_lsdi')
    WHEN data_source_text = 'usace_nid' THEN (SELECT id FROM cabd.data_source WHERE name = 'usace_nid')
    WHEN data_source_text = 'wid_fishwerks' THEN (SELECT id FROM cabd.data_source WHERE name = 'wid_fishwerks')
    WHEN data_source_text = 'wiki_gs_bc' THEN (SELECT id FROM cabd.data_source WHERE name = 'wiki_gs_bc')
    WHEN data_source_text = 'wsa_sk_owned_dams' THEN (SELECT id FROM cabd.data_source WHERE name = 'wsa_sk_owned_dams')
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_kml_pubdams'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bcflnrord_kml_pubdams'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_wris_pubdams'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bcflnrord_wris_pubdams'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), data_source_id
FROM {workingTable} WHERE data_source_text = 'cwf_canfish'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'fao_aquastat'), data_source_id
FROM {workingTable} WHERE data_source_text = 'fao_aquastat'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'gdw_goodd'), data_source_id
FROM {workingTable} WHERE data_source_text = 'gdw_goodd'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'gdw_grand'), data_source_id
FROM {workingTable} WHERE data_source_text = 'gdw_grand'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'megis_impounds'), data_source_id
FROM {workingTable} WHERE data_source_text = 'megis_impounds'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mi_prov_ww'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mi_prov_ww'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_odi'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mndmnrf_odi'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mndmnrf_ohn'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_mmh'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nberd_nbhn_mmh'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), data_source_id
FROM {workingTable} WHERE data_source_text = 'ncc_chu_ab'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nleccm_nlpdi'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nleccm_nlpdi'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_mm'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_canvec_mm'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_wcsd_gfielding'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nse_wcsd_gfielding'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'qmelcc_repbarrages'), data_source_id
FROM {workingTable} WHERE data_source_text = 'qmelcc_repbarrages'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), data_source_id
FROM {workingTable} WHERE data_source_text = 'skmoe_hydrography'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'su_npdp'), data_source_id
FROM {workingTable} WHERE data_source_text = 'su_npdp'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'swp_lsdi'), data_source_id
FROM {workingTable} WHERE data_source_text = 'swp_lsdi'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'usace_nid'), data_source_id
FROM {workingTable} WHERE data_source_text = 'usace_nid'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wid_fishwerks'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_gs_bc'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wiki_gs_bc'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wsa_sk_owned_dams'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wsa_sk_owned_dams'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_kml_pubdams'), bcflnrord_kml_pubdams
FROM {workingTable} WHERE bcflnrord_kml_pubdams IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_wris_pubdams'), bcflnrord_wris_pubdams
FROM {workingTable} WHERE bcflnrord_wris_pubdams IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), cwf_canfish
FROM {workingTable} WHERE cwf_canfish IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'fao_aquastat'), fao_aquastat
FROM {workingTable} WHERE fao_aquastat IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'gdw_goodd'), gdw_goodd
FROM {workingTable} WHERE gdw_goodd IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'gdw_grand'), gdw_grand
FROM {workingTable} WHERE gdw_grand IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'megis_impounds'), megis_impounds
FROM {workingTable} WHERE megis_impounds IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mi_prov_ww'), mi_prov_ww
FROM {workingTable} WHERE mi_prov_ww IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_odi'), mndmnrf_odi
FROM {workingTable} WHERE mndmnrf_odi IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_ohn'), mndmnrf_ohn
FROM {workingTable} WHERE mndmnrf_ohn IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nberd_nbhn_mmh'), nberd_nbhn_mmh
FROM {workingTable} WHERE nberd_nbhn_mmh IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), ncc_chu_ab
FROM {workingTable} WHERE ncc_chu_ab IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nleccm_nlpdi'), nleccm_nlpdi
FROM {workingTable} WHERE nleccm_nlpdi IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_mm'), nrcan_canvec_mm
FROM {workingTable} WHERE nrcan_canvec_mm IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_wcsd_gfielding'), nse_wcsd_gfielding
FROM {workingTable} WHERE nse_wcsd_gfielding IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'qmelcc_repbarrages'), qmelcc_repbarrages
FROM {workingTable} WHERE qmelcc_repbarrages IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), skmoe_hydrography
FROM {workingTable} WHERE skmoe_hydrography IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'su_npdp'), su_npdp
FROM {workingTable} WHERE su_npdp IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'swp_lsdi'), swp_lsdi
FROM {workingTable} WHERE swp_lsdi IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'usace_nid'), usace_nid
FROM {workingTable} WHERE usace_nid IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wid_fishwerks'), wid_fishwerks
FROM {workingTable} WHERE wid_fishwerks IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_gs_bc'), wiki_gs_bc
FROM {workingTable} WHERE wiki_gs_bc IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wsa_sk_owned_dams'), wsa_sk_owned_dams
FROM {workingTable} WHERE wsa_sk_owned_dams IS NOT NULL
ON CONFLICT DO NOTHING;

"""

with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()
conn.close()

print("\n" + "Script complete! All attribute source and feature source rows created for " + workingTable)