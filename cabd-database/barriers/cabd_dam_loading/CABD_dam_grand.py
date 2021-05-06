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
workingTableRaw = "dams_grand"
workingTable = workingSchema + "." + workingTableRaw

#maximum distance for snapping barriers to stream network in meters
snappingDistance = 50 

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: cabd_dams_grand.py <datafile>")
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
print("running mapping queries...")
query = f"""
alter table {workingTable} add column cabd_id uuid;
update {workingTable} set cabd_id = uuid_generate_v4();
alter table {workingTable} add column latitude double precision;
alter table {workingTable} add column longitude double precision;
alter table {workingTable} add column dam_name_en varchar(512);
alter table {workingTable} add column dam_name_fr varchar(512);
alter table {workingTable} add column waterbody_name_en varchar(512);
alter table {workingTable} add column waterbody_name_fr varchar(512);
alter table {workingTable} add column reservoir_name_en varchar(512);
alter table {workingTable} add column reservoir_name_fr varchar(512);
alter table {workingTable} add column watershed_group_code varchar(32);
alter table {workingTable} add column nhn_workunit_id varchar(7);
alter table {workingTable} add column province_territory_code varchar(2);
alter table {workingTable} add column nearest_municipality varchar(512);
alter table {workingTable} add column "owner" varchar(512);
alter table {workingTable} add column ownership_type_code int2;
alter table {workingTable} add column provincial_compliance_status varchar(64);
alter table {workingTable} add column federal_compliance_status varchar(64);
alter table {workingTable} add column operating_notes text;
alter table {workingTable} add column operating_status_code int2;
alter table {workingTable} add column use_code int2;
alter table {workingTable} add column use_irrigation_code int2;
alter table {workingTable} add column use_electricity_code int2;
alter table {workingTable} add column use_supply_code int2;
alter table {workingTable} add column use_floodcontrol_code int2;
alter table {workingTable} add column use_recreation_code int2;
alter table {workingTable} add column use_navigation_code int2;
alter table {workingTable} add column use_fish_code int2;
alter table {workingTable} add column use_pollution_code int2;
alter table {workingTable} add column use_invasivespecies_code int2;
alter table {workingTable} add column use_other_code int2;
alter table {workingTable} add column lake_control_code int2;
alter table {workingTable} add column construction_year numeric;
alter table {workingTable} add column assess_schedule varchar(100);
alter table {workingTable} add column expected_life int2;
alter table {workingTable} add column maintenance_last date;
alter table {workingTable} add column maintenance_next date;
alter table {workingTable} add column condition_code int2;
alter table {workingTable} add column function_code int2;
alter table {workingTable} add column construction_type_code int2;
alter table {workingTable} add column height_m float4;
alter table {workingTable} add column length_m float4;
alter table {workingTable} add column size_class_code int2;
alter table {workingTable} add column spillway_capacity float8;
alter table {workingTable} add column spillway_type_code int2;
alter table {workingTable} add column reservoir_present bool;
alter table {workingTable} add column reservoir_area_skm float4;
alter table {workingTable} add column reservoir_depth_m float4;
alter table {workingTable} add column storage_capacity_mcm float8;
alter table {workingTable} add column avg_rate_of_discharge_ls float8;
alter table {workingTable} add column degree_of_regulation_pc float4;
alter table {workingTable} add column catchment_area_skm float8;
alter table {workingTable} add column provincial_flow_req float8;
alter table {workingTable} add column federal_flow_req float8;
alter table {workingTable} add column hydro_peaking_system bool;
alter table {workingTable} add column generating_capacity_mwh float8;
alter table {workingTable} add column turbine_number int2;
alter table {workingTable} add column turbine_type_code int2;
alter table {workingTable} add column up_passage_type_code int2;
alter table {workingTable} add column down_passage_route_code int2;
alter table {workingTable} add column capture_date date;
alter table {workingTable} add column last_update date;
alter table {workingTable} add column data_source_id varchar(256);
alter table {workingTable} add column data_source varchar(256);
alter table {workingTable} add column complete_level_code int2;
alter table {workingTable} rename "comments" to "comments_orig";
alter table {workingTable} add column "comments" text;
alter table {workingTable} add column upstream_linear_km float8;
alter table {workingTable} add column passability_status_code int2;
alter table {workingTable} add column passability_status_note text;
alter table {workingTable} add column original_point geometry(POINT, 4326);
alter table {workingTable} add column snapped_point geometry(POINT, 4326);

update {workingTable} set dam_name_en = DAM_NAME;
--update {workingTable} set dam_name_fr;
update {workingTable} set waterbody_name_en = 
    CASE
    WHEN RIVER IS NOT NULL AND RIVER NOT LIKE '%Creek%' AND RIVER NOT LIKE '%River%' THEN RIVER || ' River'
    WHEN RIVER LIKE '%Creek%' OR RIVER LIKE '%River%' THEN RIVER
    ELSE NULL END;
--update {workingTable} set waterbody_name_fr;
update {workingTable} set reservoir_name_en = RES_NAME;
--update {workingTable} set reservoir_name_fr;
--update {workingTable} set watershed_group_code;
--update {workingTable} set nhn_workunit_id;
update {workingTable} set province_territory_code =
    CASE
    WHEN ADMIN_UNIT = 'Alberta' THEN 'ab'
    WHEN ADMIN_UNIT = 'British Columbia' THEN 'bc'
    WHEN ADMIN_UNIT= 'Manitoba' THEN 'mb'
    WHEN ADMIN_UNIT = 'New Brunswick' THEN 'nb'
    WHEN ADMIN_UNIT = 'Newfoundland and Labrador' THEN 'nl'
    WHEN ADMIN_UNIT = 'Nova Scotia' THEN 'ns'
    WHEN ADMIN_UNIT = 'Northwest Territories' THEN 'nt'
    WHEN ADMIN_UNIT = 'Nunavut' THEN 'nu'
    WHEN ADMIN_UNIT = 'Ontario' THEN 'on'
    WHEN ADMIN_UNIT = 'Prince Edward Island' THEN 'pe'
    WHEN ADMIN_UNIT = 'Saskatchewan' THEN 'sk'
    WHEN admin_unit = 'Yukon' THEN 'yt'
    WHEN admin_unit = 'Quebec' THEN 'qc'
    ELSE NULL end;
update {workingTable} set nearest_municipality = NEAR_CITY;
--update {workingTable} set owner;
--update {workingTable} set ownership_type_code;
--update {workingTable} set provincial_compliance_status;
--update {workingTable} set federal_compliance_status;
--update {workingTable} set operating_notes;
--update {workingTable} set operating_status_code;
update {workingTable} set use_code =
    CASE 
    WHEN MAIN_USE = 'Hydroelectricity' THEN 2
    WHEN MAIN_USE = 'Irrigation' THEN 1
    WHEN MAIN_USE = 'Other' THEN 10
    WHEN MAIN_USE = 'Recreation' THEN 5
    WHEN MAIN_USE = 'Water supply' THEN 3
    WHEN MAIN_USE = 'Flood control' THEN 4
    ELSE NULL end;
update {workingTable} set use_irrigation_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_electricity_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_supply_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_floodcontrol_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_recreation_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_navigation_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_fish_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_pollution_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_invasivespecies_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set use_other_code =
    CASE 
    WHEN USE_IRRI = 'Sec' THEN 3
    WHEN USE_IRRI = 'Main' THEN 1
    ELSE NULL end;
update {workingTable} set lake_control_code = 
    CASE 
    WHEN LAKE_CTRL = 'Yes' THEN 1
    WHEN LAKE_CTRL = 'Enlarged' THEN 2
    WHEN LAKE_CTRL = 'Maybe' THEN 3
    ELSE NULL end;
update {workingTable} set construction_year = YEAR;
--update {workingTable} set assess_schedule;
--update {workingTable} set expected_life;
--update {workingTable} set maintenance_last;
--update {workingTable} set maintenance_next;
--update {workingTable} set condition_code;
--update {workingTable} set function_code;
--update {workingTable} set construction_type_code;
update {workingTable} set height_m = CASE WHEN DAM_HGT_M = -99 THEN NULL ELSE DAM_HGT_M END;
update {workingTable} set length_m = CASE WHEN DAM_LEN_M = -99 THEN NULL ELSE DAM_LEN_M END;
update {workingTable} set size_class_code = 
    CASE
    WHEN "height_m" < 5 THEN 1
    WHEN "height_m" >= 5 AND  "height_m" < 15 THEN 2
    WHEN "height_m" >= 15 THEN 3
    ELSE NULL END;
--update {workingTable} set spillway_capacity;
--update {workingTable} set spillway_type_code;
update {workingTable} set reservoir_present = CASE WHEN AREA_SKM = -99 THEN FALSE ELSE TRUE END;
update {workingTable} set reservoir_area_skm = CASE WHEN AREA_SKM = -99 THEN NULL ELSE AREA_SKM END;
update {workingTable} set reservoir_depth_m = CASE WHEN DEPTH_M = -99 THEN NULL ELSE DEPTH_M END;
update {workingTable} set avg_rate_of_discharge_ls = DIS_AVG_LS;
update {workingTable} set storage_capacity_mcm = CASE WHEN CAP_MCM = -99 THEN NULL ELSE CAP_MCM END;;
update {workingTable} set degree_of_regulation_pc = DOR_PC;
update {workingTable} set catchment_area_skm = CATCH_SKM;
--update {workingTable} set provincial_flow_req;
--update {workingTable} set federal_flow_req;
--update {workingTable} set hydro_peaking_system;
--update {workingTable} set generating_capacity_mwh;
--update {workingTable} set turbine_number;
--update {workingTable} set turbine_type_code;
--update {workingTable} set upstream_linear_km;
--update {workingTable} set passability_status_code;
--update {workingTable} set passability_status_note;
--update {workingTable} set up_passage_type_code;
--update {workingTable} set down_passage_route_code;
--update {workingTable} set capture_date;
--update {workingTable} set last_update;
update {workingTable} set data_source_id = GRAND_ID;
update {workingTable} set data_source = 'GRanD_Database_v1.3';
update {workingTable} set "comments" = "comments_orig";
update {workingTable} set complete_level_code = 
    CASE 
    WHEN QUALITY = '1: Verified' THEN 4
    WHEN QUALITY = '2: Good' THEN 3
    WHEN QUALITY = '3: Fair' THEN 3
    WHEN QUALITY = '4: Poor' THEN 2
    WHEN QUALITY = '5: Unreliable' THEN 1
    ELSE NULL end;

update {workingTable} set original_point = st_setsrid(st_makepoint(longitude, latitude), 4326);
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
INSERT INTO dams.dams_medium_large(
cabd_id, dam_name_en,
dam_name_fr,waterbody_name_en,waterbody_name_fr,
reservoir_name_en,reservoir_name_fr,watershed_group_code,
province_territory_code,nearest_municipality,"owner",
ownership_type_code,
nhn_workunit_id,provincial_compliance_status,federal_compliance_status,operating_notes,
operating_status_code,use_code,use_irrigation_code,
use_electricity_code,use_supply_code,use_floodcontrol_code,
use_recreation_code,use_navigation_code,use_fish_code,
use_pollution_code,use_invasivespecies_code,use_other_code,
lake_control_code,construction_year,
assess_schedule,expected_life,maintenance_last,
maintenance_next,function_code,condition_code,construction_type_code,
height_m,length_m,size_class_code,spillway_capacity,spillway_type_code,
reservoir_present,reservoir_area_skm,reservoir_depth_m,storage_capacity_mcm,
avg_rate_of_discharge_ls,degree_of_regulation_pc,provincial_flow_req,
federal_flow_req,catchment_area_skm,hydro_peaking_system,
generating_capacity_mwh,turbine_number,turbine_type_code,
up_passage_type_code,down_passage_route_code,capture_date,last_update,
data_source_id,data_source,"comments",complete_level_code,
upstream_linear_km,passability_status_code,passability_status_note,original_point,snapped_point)
SELECT
cabd_id, dam_name_en,
dam_name_fr,waterbody_name_en,waterbody_name_fr,
reservoir_name_en,reservoir_name_fr,watershed_group_code,
nhn_workunit_id,province_territory_code,nearest_municipality,"owner",
ownership_type_code,
provincial_compliance_status,federal_compliance_status,operating_notes,
operating_status_code,use_code,use_irrigation_code,
use_electricity_code,use_supply_code,use_floodcontrol_code,
use_recreation_code,use_navigation_code,use_fish_code,
use_pollution_code,use_invasivespecies_code,use_other_code,
lake_control_code,construction_year,
assess_schedule,expected_life,maintenance_last,
maintenance_next,function_code,condition_code,construction_type_code,
height_m,length_m,size_class_code,spillway_capacity,spillway_type_code,
reservoir_present,reservoir_area_skm,reservoir_depth_m,storage_capacity_mcm,
avg_rate_of_discharge_ls,degree_of_regulation_pc,provincial_flow_req,
federal_flow_req,catchment_area_skm,hydro_peaking_system,
generating_capacity_mwh,turbine_number,turbine_type_code,
up_passage_type_code,down_passage_route_code,capture_date,last_update,
data_source_id,data_source,"comments",complete_level_code, 
upstream_linear_km,passability_status_code,passability_status_note, original_point,snapped_point
FROM {workingTable};
"""

print("Script Complete")
print("Data loaded into table: " + workingTable)
print("To add data to barrier web app run the following query:")
print(updatequery)