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
    print("Data file required. Usage: LOAD_fishway_review.py <datafile> <provinceCode> <dbUser> <dbPassword>")
    sys.exit()

#this is the temporary table the data is loaded into
workingSchema = "featurecopy"
workingTableRaw = "fishways"
workingTable = workingSchema + "." + workingTableRaw
attributeTableRaw = "fishways_attribute_source"
attributeTable = workingSchema + "." + attributeTableRaw

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
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' '"' + dataFile + '" ' + provinceCode + "_fishway_review"
print(pycmd)
subprocess.run(pycmd)

#add columns from cabd
print("Adding feature columns...")
loadQuery = f"""

ALTER TABLE {workingTable} ADD COLUMN cabd_id uuid;
UPDATE {workingTable} SET cabd_id = uuid_generate_v4();
ALTER TABLE {workingTable} DROP CONSTRAINT {workingTableRaw}_pkey;
ALTER TABLE {workingTable} ADD PRIMARY KEY (cabd_id);

ALTER TABLE {workingTable} ADD COLUMN dam_id uuid;
ALTER TABLE {workingTable} ADD COLUMN dam_name_en character varying(512);
ALTER TABLE {workingTable} ADD COLUMN dam_name_fr character varying(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en character varying(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr character varying(512);
ALTER TABLE {workingTable} ADD COLUMN river_name_en character varying(512);
ALTER TABLE {workingTable} ADD COLUMN river_name_fr character varying(512);
ALTER TABLE {workingTable} ALTER COLUMN nhn_workunit_id TYPE varchar(7);
ALTER TABLE {workingTable} DROP COLUMN IF EXISTS province;
ALTER TABLE {workingTable} ADD COLUMN province_territory_code character varying(2);
ALTER TABLE {workingTable} ADD COLUMN municipality character varying(512);
ALTER TABLE {workingTable} ADD COLUMN fishpass_type_code smallint;
ALTER TABLE {workingTable} ADD COLUMN monitoring_equipment text;
ALTER TABLE {workingTable} ADD COLUMN architect text;
ALTER TABLE {workingTable} ADD COLUMN contracted_by text;
ALTER TABLE {workingTable} ADD COLUMN constructed_by text;
ALTER TABLE {workingTable} ADD COLUMN plans_held_by text;
ALTER TABLE {workingTable} ADD COLUMN purpose text;
ALTER TABLE {workingTable} ADD COLUMN designed_on_biology boolean;
ALTER TABLE {workingTable} ADD COLUMN length_m real;
ALTER TABLE {workingTable} ADD COLUMN elevation_m real;
ALTER TABLE {workingTable} ADD COLUMN gradient real;
ALTER TABLE {workingTable} ADD COLUMN depth_m real;
ALTER TABLE {workingTable} ADD COLUMN entrance_location_code smallint;
ALTER TABLE {workingTable} ADD COLUMN entrance_position_code smallint;
ALTER TABLE {workingTable} ADD COLUMN modified boolean;
ALTER TABLE {workingTable} ADD COLUMN modification_year smallint;
ALTER TABLE {workingTable} ADD COLUMN modification_purpose text;
ALTER TABLE {workingTable} ADD COLUMN year_constructed smallint;
ALTER TABLE {workingTable} ADD COLUMN operated_by text;
ALTER TABLE {workingTable} ADD COLUMN operation_period text;
ALTER TABLE {workingTable} ADD COLUMN has_evaluating_studies boolean;
ALTER TABLE {workingTable} ADD COLUMN nature_of_evaluation_studies text;
ALTER TABLE {workingTable} ADD COLUMN engineering_notes text;
ALTER TABLE {workingTable} ADD COLUMN operating_notes text;
ALTER TABLE {workingTable} ADD COLUMN mean_fishway_velocity_ms real;
ALTER TABLE {workingTable} ADD COLUMN max_fishway_velocity_ms real;
ALTER TABLE {workingTable} ADD COLUMN estimate_of_attraction_pct real;
ALTER TABLE {workingTable} ADD COLUMN estimate_of_passage_success_pct real;
ALTER TABLE {workingTable} ADD COLUMN species_known_to_use_fishway varchar;
ALTER TABLE {workingTable} ADD COLUMN species_known_not_to_use_fishway varchar;
ALTER TABLE {workingTable} ADD COLUMN fishway_reference_id character varying(256);
ALTER TABLE {workingTable} ADD COLUMN complete_level_code smallint;
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(Point, 4617);

ALTER TABLE {workingTable} ALTER COLUMN data_source TYPE varchar;
ALTER TABLE {workingTable} RENAME COLUMN data_source TO data_source_text;
UPDATE {workingTable} SET data_source_text = 'canfishpass' WHERE data_source_text = '22';

ALTER TABLE {workingTable} ADD COLUMN data_source uuid;
UPDATE {workingTable} SET data_source = '7fe9e701-d804-40e6-8113-6b2c3656d1bd'::uuid WHERE data_source_text = 'canfishpass';

UPDATE {workingTable} SET original_point = ST_GeometryN(geometry, 1);
CREATE INDEX {workingTableRaw}_idx ON {workingTable} USING gist (original_point);

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)
conn.commit()

print("Fetching constraints...")
loadQuery = f"""

INSERT INTO {attributeTable} (cabd_id) SELECT cabd_id FROM {workingTable};

ALTER TABLE {attributeTable} ADD CONSTRAINT {workingTableRaw}_cabd_id_fkey FOREIGN KEY (cabd_id) REFERENCES {workingTable} (cabd_id);

ALTER TABLE {workingTable}
    ADD CONSTRAINT fishpass_fk FOREIGN KEY (fishpass_type_code) REFERENCES cabd.upstream_passage_type_codes (code),
    ADD CONSTRAINT fishpass_fk_1 FOREIGN KEY (province_territory_code) REFERENCES cabd.province_territory_codes (code),
    ADD CONSTRAINT fishpass_fk_3 FOREIGN KEY (entrance_location_code) REFERENCES fishways.entrance_location_codes (code),
    ADD CONSTRAINT fishpass_fk_4 FOREIGN KEY (entrance_position_code) REFERENCES fishways.entrance_position_codes (code),
    ADD CONSTRAINT fishways_fk FOREIGN KEY (dam_id) REFERENCES {workingSchema}.dams (cabd_id),
    ADD CONSTRAINT fishways_fk_8 FOREIGN KEY (complete_level_code) REFERENCES fishways.fishway_complete_level_codes (code);

"""
with conn.cursor() as cursor:
    cursor.execute(loadQuery)

conn.commit()
conn.close()

print("Script complete")
print("Data loaded into table: " + workingTable)