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
    print("Data file required. Usage: py LOAD_dam_review.py <datafile> <provinceCode> <dbUser> <dbPassword>")
    sys.exit()

#this is the temporary table the data is loaded into
workingSchema = "featurecopy"
workingTableRaw = "dams"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "dams_attribute_source"
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
ALTER TABLE {attributeTable} DROP CONSTRAINT IF EXISTS dams_medium_large_attribute_source_cabd_id_fkey;
DROP TABLE IF EXISTS {workingTable} CASCADE;
TRUNCATE TABLE {attributeTable};
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

ALTER TABLE {workingTable} ADD COLUMN latitude double precision;
ALTER TABLE {workingTable} ADD COLUMN longitude double precision;
ALTER TABLE {workingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN reservoir_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN reservoir_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN watershed_group_code varchar(32);
ALTER TABLE {workingTable} ALTER COLUMN nhn_workunit_id TYPE varchar(7);
ALTER TABLE {workingTable} DROP COLUMN IF EXISTS province;
ALTER TABLE {workingTable} ADD COLUMN province_territory_code varchar(2);
ALTER TABLE {workingTable} ADD COLUMN nearest_municipality varchar(512);
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
ALTER TABLE {workingTable} ADD COLUMN capture_date date;
ALTER TABLE {workingTable} ADD COLUMN last_update date;
ALTER TABLE {workingTable} ADD COLUMN complete_level_code int2;
ALTER TABLE {workingTable} ADD COLUMN "comments" text;
ALTER TABLE {workingTable} ADD COLUMN upstream_linear_km float8;
ALTER TABLE {workingTable} ADD COLUMN passability_status_code int2;
ALTER TABLE {workingTable} ADD COLUMN passability_status_note text;
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(POINT, 4617);
ALTER TABLE {workingTable} ADD COLUMN snapped_point geometry(POINT, 4617);

ALTER TABLE {workingTable} ALTER COLUMN data_source TYPE varchar;
ALTER TABLE {workingTable} RENAME COLUMN data_source TO data_source_text;
UPDATE {workingTable} SET data_source_text = 
    CASE
    WHEN data_source_text = '1' THEN 'ncc'
    WHEN data_source_text = '2' THEN 'nswf'
    WHEN data_source_text = '3' THEN 'odi'
    WHEN data_source_text = '4' THEN 'wrispublicdams'
    WHEN data_source_text = '5' THEN 'fwa'
    WHEN data_source_text = '6' THEN 'ab_basefeatures'
    WHEN data_source_text = '7' THEN 'cehq'
    WHEN data_source_text = '8' THEN 'ohn'
    WHEN data_source_text = '9' THEN 'nbhn'
    WHEN data_source_text = '10' THEN 'gfielding'
    WHEN data_source_text = '11' THEN 'fishwerks'
    WHEN data_source_text = '12' THEN 'fiss'
    WHEN data_source_text = '13' THEN 'publicdamskml'
    WHEN data_source_text = '14' THEN 'nlprov'
    WHEN data_source_text = '15' THEN 'npdp'
    WHEN data_source_text = '16' THEN 'canvec'
    WHEN data_source_text = '17' THEN 'nhn'
    WHEN data_source_text = '18' THEN 'goodd'
    WHEN data_source_text = '19' THEN 'grand'
    WHEN data_source_text = '20' THEN 'fao'
    WHEN data_source_text = '21' THEN 'cwf'
    WHEN data_source_text = '22' THEN 'canfishpass'
    WHEN data_source_text = '23' THEN 'mbprov'
    WHEN data_source_text = '24' THEN 'cgndb'
    WHEN data_source_text = '25' THEN 'wsa_sk'
    WHEN data_source_text = '26' THEN 'sk_hydro'
    WHEN data_source_text = '27' THEN 'bc_hydro_wiki'
    WHEN data_source_text = '28' THEN 'lsds'
    ELSE NULL END;

ALTER TABLE {workingTable} ADD COLUMN data_source uuid;
UPDATE {workingTable} SET data_source = 
    CASE
    WHEN data_source_text = 'ab_basefeatures' THEN '85e725a2-bb6d-45d5-a6c5-1bf7ceed28db'::uuid
    WHEN data_source_text = 'bc_hydro_wiki' THEN 'ed6b7f22-10ad-4dcb-bdd3-163ce895805e'::uuid
    WHEN data_source_text = 'canfishpass' THEN '7fe9e701-d804-40e6-8113-6b2c3656d1bd'::uuid
    WHEN data_source_text = 'canvec' THEN '4bb309bf-be07-47bf-b134-9a43834001c2'::uuid
    WHEN data_source_text = 'cehq' THEN '217bf7db-be4d-4f86-9e53-a1a6499da46a'::uuid
    WHEN data_source_text = 'cgndb' THEN 'bc77aaa4-7a4e-43a1-84f1-9c5f6ea24912'::uuid
    WHEN data_source_text = 'cwf' THEN 'd9918f2c-2b1d-47ac-918d-8aa026c4849f'::uuid
    WHEN data_source_text = 'fao' THEN '53645b80-17df-4d0c-9e83-93ab7bbb4420'::uuid
    WHEN data_source_text = 'fishwerks' THEN '2f486903-b777-464d-891c-6581400b2788'::uuid
    WHEN data_source_text = 'fiss' THEN '67ecfa8f-e156-45ef-81b5-fb93bd5b23c4'::uuid
    WHEN data_source_text = 'fwa' THEN 'd794807d-a816-49dd-a76f-3490c0abd317'::uuid
    WHEN data_source_text = 'gfielding' THEN '41b947a0-867d-4dd1-aa08-3609bf5679de'::uuid
    WHEN data_source_text = 'goodd' THEN '8c7b28eb-164a-4dc6-a121-0f8cb8005215'::uuid
    WHEN data_source_text = 'grand' THEN 'f5b5b26b-05b1-45e6-829f-ba9c4199f6be'::uuid
    WHEN data_source_text = 'lsds' THEN '35155b7e-d08e-4dd8-8588-780b5d1f7f2b'::uuid
    WHEN data_source_text = 'mbprov' THEN '187f9524-8a06-4553-a7a7-316755101110'::uuid
    WHEN data_source_text = 'nbhn' THEN '41fef339-840f-40c8-b048-5dfc5ae395d0'::uuid
    WHEN data_source_text = 'ncc' THEN 'ce45dfdb-26d1-47ae-9f9c-2b353f3676d1'::uuid
    WHEN data_source_text = 'nhn' THEN '9417da74-5cc8-4efa-8f43-0524fa47996d'::uuid
    WHEN data_source_text = 'nlprov' THEN '2bab6e19-ef39-4973-b9e2-f4c47617ff2c'::uuid
    WHEN data_source_text = 'npdp' THEN '6a9ca7af-1ae6-4b98-b79a-c207eeaf2bd9'::uuid
    WHEN data_source_text = 'nswf' THEN '3f5c9d6e-4d4f-48af-b57d-cb2a1e2671a0'::uuid
    WHEN data_source_text = 'odi' THEN '3f1e088a-03e3-4a6f-a4c0-38196d34efe8'::uuid
    WHEN data_source_text = 'ohn' THEN 'eb1e7314-7de8-46b8-94a6-1be8bfef17d1'::uuid
    WHEN data_source_text = 'publicdamskml' THEN 'eb825594-cd3f-4323-8e3d-ea92af0bf0f9'::uuid
    WHEN data_source_text = 'sk_hydro' THEN 'a855a3c9-3fed-4c0e-b123-48e0b0a93914'::uuid
    WHEN data_source_text = 'wrispublicdams' THEN 'eb1d2553-7535-4b8c-99c3-06487214ccae'::uuid
    WHEN data_source_text = 'wsa_sk' THEN '1d5038f4-40ef-4ace-9aaf-213dd3a1e616'::uuid
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
    ADD CONSTRAINT dams_medium_large_fk FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_10 FOREIGN KEY (use_navigation_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_11 FOREIGN KEY (use_fish_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_12 FOREIGN KEY (use_pollution_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_13 FOREIGN KEY (use_invasivespecies_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_14 FOREIGN KEY (use_other_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_15 FOREIGN KEY (condition_code) REFERENCES dams.condition_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_16 FOREIGN KEY (function_code) REFERENCES dams.function_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_17 FOREIGN KEY (construction_type_code) REFERENCES dams.construction_type_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_18 FOREIGN KEY (size_class_code) REFERENCES dams.size_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_19 FOREIGN KEY (spillway_type_code) REFERENCES dams.spillway_type_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_2 FOREIGN KEY (ownership_type_code) REFERENCES cabd.barrier_ownership_type_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_20 FOREIGN KEY (up_passage_type_code) REFERENCES cabd.upstream_passage_type_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_21 FOREIGN KEY (down_passage_route_code) REFERENCES dams.downstream_passage_route_codes (code), 
    ADD CONSTRAINT dams_medium_large_fk_22 FOREIGN KEY (turbine_type_code) REFERENCES dams.turbine_type_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_23 FOREIGN KEY (complete_level_code) REFERENCES dams.dam_complete_level_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_24 FOREIGN KEY (lake_control_code) REFERENCES dams.lake_control_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_26 FOREIGN KEY (passability_status_code) REFERENCES cabd.passability_status_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_3 FOREIGN KEY (operating_status_code) REFERENCES dams.operating_status_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_4 FOREIGN KEY (use_code) REFERENCES dams.dam_use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_5 FOREIGN KEY (use_irrigation_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_6 FOREIGN KEY (use_electricity_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_7 FOREIGN KEY (use_supply_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_8 FOREIGN KEY (use_floodcontrol_code) REFERENCES dams.use_codes (code),
    ADD CONSTRAINT dams_medium_large_fk_9 FOREIGN KEY (use_recreation_code) REFERENCES dams.use_codes (code)
;

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

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