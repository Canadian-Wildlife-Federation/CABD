import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\Program Files\\QGIS 3.12\\bin\\ogr2ogr.exe";

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = "XXXX@cabd-postgres-dev"
dbPassword = "XXXX"

#this is the temporary table the data is loaded into
workingSchema = "load"
workingTableRaw = "gdams"
workingTable = workingSchema + "." + workingTableRaw

#maximum distance for snapping barriers to stream network in meters
snappingDistance = 50 

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: cabd_dam_GRAND.py <datafile>")
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
alter table {workingTable} add column province_compliance_status varchar(64);
alter table {workingTable} add column federal_compliance_status varchar(64);
alter table {workingTable} add column operating_note text;
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

alter table {workingTable} add column upstream_linear_km float8;
alter table {workingTable} add column passabiltiy_status_code int2;
alter table {workingTable} add column passabiltiy_status_note text;

alter table {workingTable} add column original_point geometry(POINT, 4326);
alter table {workingTable} add column snapped_point geometry(POINT, 4326);

update {workingTable} set latitude = LAT_DD, longitude = LONG_DD;

update {workingTable} set 
    dam_name_en = DAM_NAME, 
    waterbody_name_en = RIVER, 
    reservoir_name_en = RES_NAME,
    nearest_municipality = NEAR_CITY;

update {workingTable} set province_territory_code = 
    case when ADMIN_UNIT = 'Alberta' THEN 'ab'
    when ADMIN_UNIT = 'British Columbia' THEN 'bc'
    when ADMIN_UNIT= 'Manitoba' THEN 'mb'
    when ADMIN_UNIT = 'New Brunswick' THEN 'nb'
    when ADMIN_UNIT = 'Newfoundland and Labrador' THEN 'nl'
    when ADMIN_UNIT = 'Nova Scotia' THEN 'ns'
    when ADMIN_UNIT = 'Northwest Territories' THEN 'nt'
    when ADMIN_UNIT = 'Nunavut' THEN 'nu'
    when ADMIN_UNIT = 'Ontario' THEN 'on'
    when ADMIN_UNIT = 'Prince Edward Island' THEN 'pe'
    when ADMIN_UNIT = 'Saskatchewan' THEN 'sk'
    when admin_unit = 'Yukon' THEN 'yt'
    when admin_unit = 'Quebec' THEN 'qc'
    ELSE null END;

update {workingTable} set use_code = 
    case when MAIN_USE = 'Hydroelectricity' THEN 2
    when MAIN_USE = 'Irrigation' THEN 1
    when MAIN_USE = 'Other' THEN 10
    when MAIN_USE = 'Recreation' THEN 5
    when MAIN_USE = 'Water supply' THEN 3
    when MAIN_USE = 'Flood control' THEN 4
    else NULL end;


update {workingTable} set use_irrigation_code = 
    case when USE_IRRI = 'Sec' THEN 3
    when USE_IRRI = 'Main' THEN 1
    else NULL end;

update {workingTable} set use_electricity_code = 
    case when USE_ELEC = 'Sec' THEN 3
    when USE_ELEC = 'Main' THEN 1
    else NULL end;

update {workingTable} set use_supply_code = 
    case when USE_SUPP = 'Sec' THEN 3
    when USE_SUPP = 'Main' THEN 1
    else NULL end;

update {workingTable} set use_floodcontrol_code = 
    case when USE_FCON = 'Sec' THEN 3
    when USE_FCON = 'Main' THEN 1
    else NULL end;

update {workingTable} set use_recreation_code = 
    case when USE_RECR = 'Sec' THEN 3
    when USE_FCON = 'Main' THEN 1
    else NULL end;


update {workingTable} set use_navigation_code = 
    case when USE_NAVI = 'Sec' THEN 3
    when USE_NAVI = 'Main' THEN 1
    else NULL end;


update {workingTable} set use_fish_code = 
    case when USE_FISH = 'Sec' THEN 3
    when USE_FISH = 'Main' THEN 1
    else NULL end;

update {workingTable} set use_pollution_code = 
    case when USE_PCON = 'Sec' THEN 3
    when USE_PCON = 'Main' THEN 1
    else NULL end;

update {workingTable} set use_other_code = 
    case when USE_OTHR = 'Sec' THEN 3
    when USE_OTHR = 'Main' THEN 1
    else NULL end;

update {workingTable} set lake_control_code = 
    case when LAKE_CTRL = 'Yes' THEN 1
    when LAKE_CTRL = 'Enlarged' THEN 2
    when LAKE_CTRL = 'Maybe' THEN 3
    else NULL end;


update {workingTable} set size_class_code = 
    case when DAM_HGT_M < 0 THEN NULL
    when DAM_HGT_M >= 0 AND DAM_HGT_M < 15 THEN 2
    when DAM_HGT_M >= 15 THEN 3
    else NULL end;

update {workingTable} set reservoir_present = case when AREA_SKM = -99 THEN FALSE ELSE TRUE END;


update {workingTable} set 
    reservoir_area_skm = CASE WHEN AREA_SKM = -99 THEN NULL ELSE AREA_SKM END,  
    reservoir_depth_m = CASE WHEN DEPTH_M = -99 THEN NULL ELSE DEPTH_M END,
    storage_capacity_mcm = CASE WHEN CAP_MCM = -99 THEN NULL ELSE CAP_MCM END,
    avg_rate_of_discharge_ls = DIS_AVG_LS,
    degree_of_regulation_pc = DOR_PC,
    catchment_area_skm = CATCH_SKM;

update {workingTable} set "data_source_id" = GRAND_ID;
update {workingTable} set "data_source" = 'GRanD_Database_v1.3';

update {workingTable} set complete_level_code = 
    case when QUALITY = '1: Verified' THEN 4
    when QUALITY = '2: Good' THEN 3
    when QUALITY = '3: Fair' THEN 3
    when QUALITY = '4: Poor' THEN 2
    when QUALITY = '5: Unreliable' THEN 1
    else NULL end;

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
province_compliance_status,federal_compliance_status,operating_note,
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
province_territory_code,nearest_municipality,"owner",
ownership_type_code,
province_compliance_status,federal_compliance_status,operating_note,
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
upstream_linear_km,passability_status_code,passability_status_note,original_point,snapped_point
FROM {workingTable};
"""

print("Script Complete")
print("Data loaded into table: " + workingTable)
print("To add data to barrier web app run the following query:")
print(updatequery)