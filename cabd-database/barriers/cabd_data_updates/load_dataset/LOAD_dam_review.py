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
#e.g., provinceCode of 'atlantic' is fine if the layer you want is named 'atlantic_dam_review'
provinceCode = sys.argv[2]
dbUser = sys.argv[3]
dbPassword = sys.argv[4]

if dataFile == '':
    print('Data file required. Usage: py LOAD_dam_review.py "<datafile>" <provinceCode> <dbUser> <dbPassword>')
    sys.exit()

#this is the temporary table the data is loaded into
workingSchema = "featurecopy"
workingTableRaw = "dams"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "dams_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw
featureTableRaw = "dams_feature_source"
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
ALTER TABLE {attributeTable} DROP CONSTRAINT IF EXISTS dams_cabd_id_fkey;
DROP TABLE IF EXISTS {workingTable} CASCADE;
TRUNCATE TABLE {attributeTable};
TRUNCATE TABLE {featureTable};
"""

with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' '"' + dataFile + '" ' + provinceCode + "_dam_review"
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

ALTER TABLE {workingTable} ADD COLUMN latitude double precision;
ALTER TABLE {workingTable} ADD COLUMN longitude double precision;
ALTER TABLE {workingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN facility_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN facility_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN reservoir_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN reservoir_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN watershed_group_code varchar(32);
ALTER TABLE {workingTable} ALTER COLUMN nhn_watershed_id TYPE varchar(7);
ALTER TABLE {workingTable} ADD COLUMN province_territory_code varchar(2);
ALTER TABLE {workingTable} ADD COLUMN municipality varchar(512);
ALTER TABLE {workingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {workingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {workingTable} ADD COLUMN provincial_compliance_status varchar(64);
ALTER TABLE {workingTable} ADD COLUMN federal_compliance_status varchar(64);
ALTER TABLE {workingTable} ADD COLUMN operating_notes text;
ALTER TABLE {workingTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_fish_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_invasivespecies_code int2;
ALTER TABLE {workingTable} ADD COLUMN use_other_code int2;
ALTER TABLE {workingTable} ADD COLUMN lake_control_code int2;
ALTER TABLE {workingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {workingTable} ADD COLUMN removed_year numeric;
ALTER TABLE {workingTable} ADD COLUMN assess_schedule varchar(100);
ALTER TABLE {workingTable} ADD COLUMN expected_life int2;
ALTER TABLE {workingTable} ADD COLUMN maintenance_last date;
ALTER TABLE {workingTable} ADD COLUMN maintenance_next date;
ALTER TABLE {workingTable} ADD COLUMN condition_code int2;
ALTER TABLE {workingTable} ADD COLUMN function_code int2;
ALTER TABLE {workingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {workingTable} ADD COLUMN height_m float4;
ALTER TABLE {workingTable} ADD COLUMN length_m float4;
ALTER TABLE {workingTable} ADD COLUMN size_class_code int2;
ALTER TABLE {workingTable} ADD COLUMN spillway_capacity float8;
ALTER TABLE {workingTable} ADD COLUMN spillway_type_code int2;
ALTER TABLE {workingTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {workingTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {workingTable} ADD COLUMN reservoir_depth_m float4;
ALTER TABLE {workingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {workingTable} ADD COLUMN avg_rate_of_discharge_ls float8;
ALTER TABLE {workingTable} ADD COLUMN degree_of_regulation_pc float4;
ALTER TABLE {workingTable} ADD COLUMN catchment_area_skm float8;
ALTER TABLE {workingTable} ADD COLUMN provincial_flow_req float8;
ALTER TABLE {workingTable} ADD COLUMN federal_flow_req float8;
ALTER TABLE {workingTable} ADD COLUMN hydro_peaking_system bool;
ALTER TABLE {workingTable} ADD COLUMN generating_capacity_mwh float8;
ALTER TABLE {workingTable} ADD COLUMN turbine_number int2;
ALTER TABLE {workingTable} ADD COLUMN turbine_type_code int2;
ALTER TABLE {workingTable} ADD COLUMN up_passage_type_code int2;
ALTER TABLE {workingTable} ADD COLUMN down_passage_route_code int2;
ALTER TABLE {workingTable} ADD COLUMN last_modified date;
ALTER TABLE {workingTable} ADD COLUMN complete_level_code int2;
ALTER TABLE {workingTable} ADD COLUMN "comments" text;
ALTER TABLE {workingTable} ADD COLUMN upstream_linear_km float8;
ALTER TABLE {workingTable} ADD COLUMN passability_status_code int2;
ALTER TABLE {workingTable} ADD COLUMN passability_status_note text;
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(POINT, 4617);
ALTER TABLE {workingTable} ADD COLUMN snapped_point geometry(POINT, 4617);

ALTER TABLE {workingTable} ALTER COLUMN data_source TYPE varchar;
ALTER TABLE {workingTable} RENAME COLUMN data_source TO data_source_text;

ALTER TABLE {workingTable} ADD COLUMN data_source uuid;
UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source_text = 'ncc_chu_ab' THEN (SELECT id FROM cabd.data_source WHERE name = 'ncc_chu_ab')
    WHEN data_source_text = 'nse_td_wf' THEN (SELECT id FROM cabd.data_source WHERE name = 'nse_td_wf')
    WHEN data_source_text = 'mndmnrf_odi' THEN (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_odi')
    WHEN data_source_text = 'bcflnrord_wris_pubdams' THEN (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_wris_pubdams')
    WHEN data_source_text = 'bcflnrord_fwa' THEN (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_fwa')
    WHEN data_source_text = 'aep_bf_hy' THEN (SELECT id FROM cabd.data_source WHERE name = 'aep_bf_hy')
    WHEN data_source_text = 'qmelcc_repbarrages' THEN (SELECT id FROM cabd.data_source WHERE name = 'qmelcc_repbarrages')
    WHEN data_source_text = 'mndmnrf_ohn' THEN (SELECT id FROM cabd.data_source WHERE name = 'mndmnrf_ohn')
    WHEN data_source_text = 'nberd_nbhn_mmh' THEN (SELECT id FROM cabd.data_source WHERE name = 'nberd_nbhn_mmh')
    WHEN data_source_text = 'nse_wcsd_gfielding' THEN (SELECT id FROM cabd.data_source WHERE name = 'nse_wcsd_gfielding')
    WHEN data_source_text = 'wid_fishwerks' THEN (SELECT id FROM cabd.data_source WHERE name = 'wid_fishwerks')
    WHEN data_source_text = 'bceccs_fiss' THEN (SELECT id FROM cabd.data_source WHERE name = 'bceccs_fiss')
    WHEN data_source_text = 'bcflnrord_kml_pubdams' THEN (SELECT id FROM cabd.data_source WHERE name = 'bcflnrord_kml_pubdams')
    WHEN data_source_text = 'nleccm_nlpdi' THEN (SELECT id FROM cabd.data_source WHERE name = 'nleccm_nlpdi')
    WHEN data_source_text = 'su_npdp' THEN (SELECT id FROM cabd.data_source WHERE name = 'su_npdp')
    WHEN data_source_text = 'nrcan_canvec_mm' THEN (SELECT id FROM cabd.data_source WHERE name = 'nrcan_canvec_mm')
    WHEN data_source_text = 'nrcan_nhn' THEN (SELECT id FROM cabd.data_source WHERE name = 'nrcan_nhn')
    WHEN data_source_text = 'gdw_goodd' THEN (SELECT id FROM cabd.data_source WHERE name = 'gdw_goodd')
    WHEN data_source_text = 'gdw_grand' THEN (SELECT id FROM cabd.data_source WHERE name = 'gdw_grand')
    WHEN data_source_text = 'fao_aquastat' THEN (SELECT id FROM cabd.data_source WHERE name = 'fao_aquastat')
    WHEN data_source_text = 'cwf_canfish' THEN (SELECT id FROM cabd.data_source WHERE name = 'cwf_canfish')
    WHEN data_source_text = 'mi_prov_ww' THEN (SELECT id FROM cabd.data_source WHERE name = 'mi_prov_ww')
    WHEN data_source_text = 'nrcan_cgndb' THEN (SELECT id FROM cabd.data_source WHERE name = 'nrcan_cgndb')
    WHEN data_source_text = 'wsa_sk_owned_dams' THEN (SELECT id FROM cabd.data_source WHERE name = 'wsa_sk_owned_dams')
    WHEN data_source_text = 'skmoe_hydrography' THEN (SELECT id FROM cabd.data_source WHERE name = 'skmoe_hydrography')
    WHEN data_source_text = 'wiki_gs_bc' THEN (SELECT id FROM cabd.data_source WHERE name = 'wiki_gs_bc')
    WHEN data_source_text = 'swp_lsdi' THEN (SELECT id FROM cabd.data_source WHERE name = 'swp_lsdi')
    WHEN data_source_text = 'usace_nid' THEN (SELECT id FROM cabd.data_source WHERE name = 'usace_nid')
    WHEN data_source_text = 'megis_impounds' THEN (SELECT id FROM cabd.data_source WHERE name = 'megis_impounds')
    ELSE NULL END;

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
    ADD CONSTRAINT dams_fk FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes (code),
    ADD CONSTRAINT dams_fk_2 FOREIGN KEY (ownership_type_code) REFERENCES cabd.barrier_ownership_type_codes (code),
    ADD CONSTRAINT dams_fk_3 FOREIGN KEY (operating_status_code) REFERENCES dams.operating_status_codes (code),
    ADD CONSTRAINT dams_fk_4 FOREIGN KEY (use_code) REFERENCES dams.dam_use_codes (code),
    ADD CONSTRAINT dams_fk_5 FOREIGN KEY (use_irrigation_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_6 FOREIGN KEY (use_electricity_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_7 FOREIGN KEY (use_supply_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_8 FOREIGN KEY (use_floodcontrol_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_9 FOREIGN KEY (use_recreation_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_10 FOREIGN KEY (use_navigation_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_11 FOREIGN KEY (use_fish_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_12 FOREIGN KEY (use_pollution_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_13 FOREIGN KEY (use_invasivespecies_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_14 FOREIGN KEY (use_other_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_fk_15 FOREIGN KEY (condition_code) REFERENCES dams.condition_codes (code),
    ADD CONSTRAINT dams_fk_16 FOREIGN KEY (function_code) REFERENCES dams.function_codes (code),
    ADD CONSTRAINT dams_fk_17 FOREIGN KEY (construction_type_code) REFERENCES dams.construction_type_codes (code),
    ADD CONSTRAINT dams_fk_18 FOREIGN KEY (size_class_code) REFERENCES dams.size_codes (code),
    ADD CONSTRAINT dams_fk_19 FOREIGN KEY (spillway_type_code) REFERENCES dams.spillway_type_codes (code),
    ADD CONSTRAINT dams_fk_20 FOREIGN KEY (up_passage_type_code) REFERENCES cabd.upstream_passage_type_codes (code),
    ADD CONSTRAINT dams_fk_21 FOREIGN KEY (down_passage_route_code) REFERENCES dams.downstream_passage_route_codes (code), 
    ADD CONSTRAINT dams_fk_22 FOREIGN KEY (turbine_type_code) REFERENCES dams.turbine_type_codes (code),
    ADD CONSTRAINT dams_fk_23 FOREIGN KEY (complete_level_code) REFERENCES dams.dam_complete_level_codes (code),
    ADD CONSTRAINT dams_fk_24 FOREIGN KEY (lake_control_code) REFERENCES dams.lake_control_codes (code),
    ADD CONSTRAINT dams_fk_26 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes (code)
;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

#set up feature source table
print("Creating feature_source table...")
loadQuery = f"""

DROP TABLE IF EXISTS {featureTable};
CREATE TABLE {featureTable} (
    cabd_id uuid NOT NULL,
    datasource_id uuid NOT NULL,
    datasource_feature_id character varying NOT NULL,
    CONSTRAINT dams_feature_source_pkey PRIMARY KEY (cabd_id, datasource_id),
    CONSTRAINT dams_datasource_id_fk FOREIGN KEY (datasource_id)
        REFERENCES cabd.data_source (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE RESTRICT,
    CONSTRAINT dams_feature_source_cabd_id_fk FOREIGN KEY (cabd_id)
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), data_source_id
FROM {workingTable} WHERE data_source_text = 'ncc_chu_ab'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nse_td_wf'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_odi'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mndmnrf_odi'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_wris_pubdams'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bcflnrord_wris_pubdams'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bcflnrord_fwa'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy'), data_source_id
FROM {workingTable} WHERE data_source_text = 'aep_bf_hy'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'qmelcc_repbarrages'), data_source_id
FROM {workingTable} WHERE data_source_text = 'qmelcc_repbarrages'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_wcsd_gfielding'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nse_wcsd_gfielding'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_kml_pubdams'), data_source_id
FROM {workingTable} WHERE data_source_text = 'bcflnrord_kml_pubdams'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nleccm_nlpdi'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nleccm_nlpdi'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'su_npdp'), data_source_id
FROM {workingTable} WHERE data_source_text = 'su_npdp'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_mm'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_canvec_mm'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_nhn'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'fao_aquastat'), data_source_id
FROM {workingTable} WHERE data_source_text = 'fao_aquastat'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), data_source_id
FROM {workingTable} WHERE data_source_text = 'cwf_canfish'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mi_prov_ww'), data_source_id
FROM {workingTable} WHERE data_source_text = 'mi_prov_ww'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb'), data_source_id
FROM {workingTable} WHERE data_source_text = 'nrcan_cgndb'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wsa_sk_owned_dams'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wsa_sk_owned_dams'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), data_source_id
FROM {workingTable} WHERE data_source_text = 'skmoe_hydrography'
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_gs_bc'), data_source_id
FROM {workingTable} WHERE data_source_text = 'wiki_gs_bc'
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'megis_impounds'), data_source_id
FROM {workingTable} WHERE data_source_text = 'megis_impounds'
ON CONFLICT DO NOTHING;

--insert rows into feature_source table from named columns

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'ncc_chu_ab'), ncc_chu_ab
FROM {workingTable} WHERE ncc_chu_ab IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_td_wf'), nse_td_wf
FROM {workingTable} WHERE nse_td_wf IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mndmnrf_odi'), mndmnrf_odi
FROM {workingTable} WHERE mndmnrf_odi IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_wris_pubdams'), bcflnrord_wris_pubdams
FROM {workingTable} WHERE bcflnrord_wris_pubdams IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_fwa'), bcflnrord_fwa
FROM {workingTable} WHERE bcflnrord_fwa IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'aep_bf_hy'), aep_bf_hy
FROM {workingTable} WHERE aep_bf_hy IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'qmelcc_repbarrages'), qmelcc_repbarrages
FROM {workingTable} WHERE qmelcc_repbarrages IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nse_wcsd_gfielding'), nse_wcsd_gfielding
FROM {workingTable} WHERE nse_wcsd_gfielding IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'bcflnrord_kml_pubdams'), bcflnrord_kml_pubdams
FROM {workingTable} WHERE bcflnrord_kml_pubdams IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nleccm_nlpdi'), nleccm_nlpdi
FROM {workingTable} WHERE nleccm_nlpdi IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'su_npdp'), su_npdp
FROM {workingTable} WHERE su_npdp IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_canvec_mm'), nrcan_canvec_mm
FROM {workingTable} WHERE nrcan_canvec_mm IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_nhn'), nrcan_nhn
FROM {workingTable} WHERE nrcan_nhn IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'fao_aquastat'), fao_aquastat
FROM {workingTable} WHERE fao_aquastat IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'cwf_canfish'), cwf_canfish
FROM {workingTable} WHERE cwf_canfish IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'mi_prov_ww'), mi_prov_ww
FROM {workingTable} WHERE mi_prov_ww IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'nrcan_cgndb'), nrcan_cgndb
FROM {workingTable} WHERE nrcan_cgndb IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wsa_sk_owned_dams'), wsa_sk_owned_dams
FROM {workingTable} WHERE wsa_sk_owned_dams IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'skmoe_hydrography'), skmoe_hydrography
FROM {workingTable} WHERE skmoe_hydrography IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {featureTable} (cabd_id, datasource_id, datasource_feature_id)
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'wiki_gs_bc'), wiki_gs_bc
FROM {workingTable} WHERE wiki_gs_bc IS NOT NULL
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
SELECT cabd_id, (SELECT id FROM cabd.data_source WHERE "name" = 'megis_impounds'), megis_impounds
FROM {workingTable} WHERE megis_impounds IS NOT NULL
ON CONFLICT DO NOTHING;

"""

#snap points - commented out until we have chyf networks ready
# print("Snapping to CHyF network...")
# snapQuery = f"""
# UPDATE {workingTable} SET original_point = ST_GeometryN(geometry, 1);
# SELECT featurecopy.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
# UPDATE {workingTable} SET snapped_point = original_point WHERE snapped_point IS NULL;
# CREATE INDEX {workingTableRaw}_idx ON {workingTable} USING gist (snapped_point);
# """

# with conn.cursor() as cursor:
#     cursor.execute(snapQuery)

# conn.commit()
conn.close()

print("\n" + "Script complete! Data loaded into table: " + workingTable)

print(loadQuery)
print("Run the query above to insert rows into the feature_source table")