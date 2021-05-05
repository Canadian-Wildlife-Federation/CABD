import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = "XXXX@cabd-postgres-dev"
dbPassword = "XXXX"

#this is the temporary table the data is loaded into
workingSchema = "load"
workingTableRaw = "waterfalls_wwd"
workingTable = workingSchema + "." + workingTableRaw

#maximum distance for snapping barriers to stream network in meters
snappingDistance = 50 

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: cabd_waterfalls_wwd.py <datafile>")
    sys.exit()


print("Loading data from file " +  dataFile)


conn = pg2.connect(database=dbName, 
                   user=dbUser, 
                   host=dbHost, 
                   password=dbPassword, 
                   port=dbPort)

query = f"""
CREATE SCHEMA if not exists {workingSchema};
DROP TABLE if exists {workingTable};
"""

with conn.cursor() as cursor:
    cursor.execute(query);
conn.commit();

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + dataFile
print(pycmd)
subprocess.run(pycmd)

#run scripts to convert the data
query = f"""
alter table {workingTable} add column cabd_id uuid;
update {workingTable} set cabd_id = uuid_generate_v4();
alter table {workingTable} add column latitude double precision;
alter table {workingTable} add column longitude double precision;
alter table {workingTable} add column fall_name_en varchar(512);
alter table {workingTable} add column fall_name_fr varchar(512);
alter table {workingTable} add column waterbody_name_en varchar(512);
alter table {workingTable} add column waterbody_name_fr varchar(512);
alter table {workingTable} add column watershed_group_code varchar(32);
alter table {workingTable} add column nhn_workunit_id varchar(7);
alter table {workingTable} add column province_territory_code varchar(2);
alter table {workingTable} add column nearest_municipality varchar(512);
alter table {workingTable} add column fall_height_m float4 ;
alter table {workingTable} add column capture_date date;
alter table {workingTable} add column last_update date;
alter table {workingTable} add column "comments" text;
alter table {workingTable} add column data_source_id varchar(256);
alter table {workingTable} add column data_source varchar(256);
alter table {workingTable} add column complete_level_code int2;
alter table {workingTable} add column passability_status_code int2;
alter table {workingTable} add column passability_status_note text;
alter table {workingTable} add column original_point geometry(POINT, 4326);
alter table {workingTable} add column snapped_point geometry(POINT, 4326);

update {workingTable} set fall_name_en = Name;
--update {workingTable} set fall_name_fr;
--update {workingTable} set waterbody_name_en;
--update {workingTable} set waterbody_name_fr;
--update {workingTable} set watershed_group_code;
--update {workingTable} set nhn_workunit_id;
--update {workingTable} set province_territory_code;
--update {workingTable} set nearest_municipality;
--update {workingTable} set fall_height_m;
--update {workingTable} set capture_date;
--update {workingTable} set last_update;
--update {workingTable} set "comments";
update {workingTable} set data_source_id = OID_;
update {workingTable} set data_source = 'wwd';
--update {workingTable} set complete_level_code;
update {workingTable} set passability_status_code = 
    CASE
    WHEN "height_m" < 5 THEN 2
    WHEN "height_m" >= 5 THEN 1
    WHEN "height_m" IS NULL THEN 4
    ELSE NULL END;
--update{workingTable} set passability_status_note;

update {workingTable} set original_point = st_transform(st_geometryN(geometry, 1), 4326);
select cabd.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
update {workingTable} set snapped_point = original_point where snapped_point is null;
update {workingTable} set province_territory_code = a.code from cabd.province_territory_codes a where st_contains(a.geometry, original_point) and province_territory_code is null; 
update {workingTable} set nhn_workunit_id = a.id from cabd.nhn_workunit a where st_contains(a.polygon, original_point) and nhn_workunit_id is null;
    """

with conn.cursor() as cursor:
    cursor.execute(query)

conn.commit()
conn.close()
        
updatequery = f"""
INSERT INTO waterfalls.waterfalls(cabd_id,fall_name_en,fall_name_fr,waterbody_name_en,waterbody_name_fr,watershed_group_code,province_territory_code,nearest_municipality,fall_height_m,capture_date,last_update,"comments",data_source_id,data_source,complete_level_code,original_point,snapped_point,passability_status_code,passability_status_note)
SELECT cabd_id,fall_name_en,fall_name_fr,waterbody_name_en,waterbody_name_fr,watershed_group_code,province_territory_code,nearest_municipality,fall_height_m,capture_date,last_update,"comments",data_source_id,data_source,complete_level_code,original_point,snapped_point,passability_status_code,passability_status_note
FROM {workingTable};
"""

print("Script Complete")
print("Data loaded into table: " + workingTable)
print("To add data to barrier web app run the following query:")
print(updatequery)