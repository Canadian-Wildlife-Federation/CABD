# This script loads a CSV into the database containing updated data sources for dams

import subprocess
import sys
import getpass
import psycopg2 as pg2

ogr = "C:\\Program Files\\GDAL\\ogr2ogr.exe"

dbHost = "cabd-postgres.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = input(f"""Enter username to access {dbName}:\n""")
dbPassword = getpass.getpass(f"""Enter password to access {dbName}:\n""")

dataFile = ""
dataFile = sys.argv[1]

sourceSchema = "source_data"
sourceTableRaw = sys.argv[2]
sourceTable = sourceSchema + "." + sourceTableRaw

damSchema = "dams"
damTable = damSchema + ".dams"
updateTable = damSchema + '.dams_feature_source'

if len(sys.argv) != 3:
    print("Invalid usage: py dam_feature_source_updates.py <dataFile> <tableName>")
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

query = f"""
--remove any updates for features that will be deleted
DELETE FROM {sourceTable} WHERE cabd_id::uuid IN (
    SELECT cabd_id FROM cabd.dam_updates
    WHERE entry_classification = 'delete feature'
);

ALTER TABLE cabd.data_source ADD CONSTRAINT ds_unique_name UNIQUE (name);

INSERT INTO cabd.data_source (
	id,
	name,
	version_date,
	source,
	source_type,
	full_name,
	organization_name,
	data_source_category,
	source_id_field,
	licence
)
VALUES (
	gen_random_uuid(),
	'grca_dams',
	'2021-12-13',
	'Grand River Conservation Authority, 2021. Dams. Accessed November 14, 2023, from https://data.grandriver.ca/downloads-geospatial.html',
	'spatial',
	'Grand River Conservation Authority Dams',
	'Grand River Conservation Authority',
	'ngo/non-profit',
	'dam_id',
	'https://data.grandriver.ca/docs/GRCA%20Open%20Data%20Licence%20v2.pdf'
)
ON CONFLICT DO NOTHING;

ALTER TABLE cabd.data_source DROP CONSTRAINT ds_unique_name;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'aep_bf_hy'),
    aep_bf_hybf_id
FROM {sourceTable}
WHERE aep_bf_hybf_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'bceccs_fiss'),
    bceccs_fiss_fish_obstacle_point_id
FROM {sourceTable}
WHERE bceccs_fiss_fish_obstacle_point_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_fwa'),
    bcflnrord_fwa_obstruction_id
FROM {sourceTable}
WHERE bcflnrord_fwa_obstruction_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_kml_pubdams'),
    bcflnrord_kml_pubdams_OBJECTID
FROM {sourceTable}
WHERE bcflnrord_kml_pubdams_OBJECTID IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_wris_pubdams'),
    bcflnrord_wris_pubdams_objectid
FROM {sourceTable}
WHERE bcflnrord_wris_pubdams_objectid IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'fao_aquastat'),
    fao_aquastat_id_fao
FROM {sourceTable}
WHERE fao_aquastat_id_fao IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'gdw_goodd'),
    gdw_goodd_DAM_ID
FROM {sourceTable}
WHERE gdw_goodd_DAM_ID IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'gdw_grand'),
    gdw_grand_grand_id
FROM {sourceTable}
WHERE gdw_grand_grand_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'grca_dams'),
    grca_dams_dam_id
FROM {sourceTable}
WHERE grca_dams_dam_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_odi'),
    mndmnrf_odi_ogf_id
FROM {sourceTable}
WHERE mndmnrf_odi_ogf_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

--fix a couple dams that had the wrong id field
UPDATE {updateTable} SET datasource_feature_id = a.ogf_id
FROM source_data.mndmnrf_odi a
WHERE datasource_id = (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_odi')
AND datasource_feature_id IN (SELECT dam_id::varchar FROM source_data.mndmnrf_odi);

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_ohn'),
    mndmnrf_ohn_ogf_id
FROM {sourceTable}
WHERE mndmnrf_ohn_ogf_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nberd_nbhn_mmh'),
    nberd_nbhn_mmh_nid
FROM {sourceTable}
WHERE nberd_nbhn_mmh_nid IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'ncc_chu_ab'),
    ncc_chu_ab_unique_id
FROM {sourceTable}
WHERE ncc_chu_ab_unique_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nleccm_nlpdi'),
    nleccm_nlpdi_dam_index_num
FROM {sourceTable}
WHERE nleccm_nlpdi_dam_index_num IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nrcan_canvec_mm'),
    nrcan_canvec_mm_feature_id
FROM {sourceTable}
WHERE nrcan_canvec_mm_feature_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nrcan_canvec_mm'),
    nrcan_canvec_mm_line_feature_id
FROM {sourceTable}
WHERE nrcan_canvec_mm_line_feature_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nrcan_canvec_mm'),
    nrcan_canvec_mm_poly_feature_id
FROM {sourceTable}
WHERE nrcan_canvec_mm_poly_feature_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nrcan_cgndb'),
    nrcan_cgndb_CGNDB_ID
FROM {sourceTable}
WHERE nrcan_cgndb_CGNDB_ID IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nrcan_nhn'),
    nrcan_nhn_nid
FROM {sourceTable}
WHERE nrcan_nhn_nid IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nse_td_wf'),
    nse_td_wf_shape_fid
FROM {sourceTable}
WHERE nse_td_wf_shape_fid IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'nse_wcsd_gfielding'),
    nse_wcsd_gfielding_Dam_ID_Number
FROM {sourceTable}
WHERE nse_wcsd_gfielding_Dam_ID_Number IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'qmelcc_repbarrages'),
    qmelcc_repbarrages_numéro_barrage
FROM {sourceTable}
WHERE qmelcc_repbarrages_numéro_barrage IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'su_npdp'),
    su_npdp_npdp_id
FROM {sourceTable}
WHERE su_npdp_npdp_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

INSERT INTO {updateTable} (
    cabd_id,
    datasource_id,
    datasource_feature_id
    )
SELECT
    cabd_id::uuid,
    (SELECT id FROM cabd.data_source WHERE name = 'wid_fishwerks'),
    wid_fishwerks_barrier_id
FROM {sourceTable}
WHERE wid_fishwerks_barrier_id IS NOT NULL
ON CONFLICT (cabd_id, datasource_id) DO UPDATE
SET datasource_feature_id = EXCLUDED.datasource_feature_id;

"""
with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

print("Done!")
