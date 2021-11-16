import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe"

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

dataFile = ""
dataFile = sys.argv[1]
print(dataFile)
dbUser = sys.argv[2]
dbPassword = sys.argv[3]

if dataFile == '':
    print("Data file required. Usage: LOAD_dam_review.py <datafile> <dbUser> <dbPassword>")
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
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' '"' + dataFile + '" ' + "ab_dam_review"
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
ALTER TABLE {workingTable} DROP COLUMN province;
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

loadQuery = f"""

INSERT INTO {attributeTable} (cabd_id) SELECT cabd_id FROM {workingTable};

ALTER TABLE {attributeTable}
    ADD CONSTRAINT dams_cabd_id_fkey FOREIGN KEY (cabd_id) 
    REFERENCES {workingTable} (cabd_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION;

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

--CREATE TABLE featurecopy.dams_snapped_check AS
--(SELECT cabd.cabd_id, cabd.dam_name_en, cabd.dam_name_fr, cabd.snapped_point
    --FROM {workingTable} AS cabd, fpoutput.eflowpath AS fp
    --WHERE
        --(ST_Intersects(cabd.snapped_point, fp.geometry) AND cabd.use_analysis IS FALSE)
        --OR (ST_Disjoint(cabd.snapped_point, fp.geometry) AND cabd.use_analysis IS TRUE)
--);
"""

with conn.cursor() as cursor:
    cursor.execute(snapQuery)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)
##print("Check featurecopy.dams_snapped_check for improperly-snapped points")