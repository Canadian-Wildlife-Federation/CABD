import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\Program Files\\QGIS 3.12\\bin\\ogr2ogr.exe";

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"
dbUser = "XXXX@cabd-postgres-dev"
dbPassword = "XXXX"

dbHost = "localhost"
dbPort = "8448"
dbName = "cabd"
dbUser = "cabd@cabd-postgres-dev"
dbPassword = "s#ZAf*mUU8?wHb8e"

#this is the temporary table the data is loaded into
workingSchema = "load"
workingTableRaw = "npdpdams"
workingTable = workingSchema + "." + workingTableRaw

#maximum distance for snapping barriers to stream network in meters
snappingDistance = 50 

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: cabd_dam_NPDP.py <datafile>")
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

alter table {workingTable} add column dam_name_en varchar(512);
alter table {workingTable} add column dam_name_fr varchar(512);
alter table {workingTable} add column waterbody_name_en varchar(512);
alter table {workingTable} add column waterbody_name_fr varchar(512);
alter table {workingTable} add column reservoir_name_en varchar(512);
alter table {workingTable} add column reservoir_name_fr varchar(512);
alter table {workingTable} add column watershed_group_code varchar(32);
alter table {workingTable} add column  province_territory_code varchar(2);
alter table {workingTable} add column nearest_municipality varchar(512);
alter table {workingTable} add column "owner" varchar(512);
alter table {workingTable} add column ownership_type_code int2;
alter table {workingTable} add column province_reg_body varchar(512);
alter table {workingTable} add column federal_reg_body varchar(512);
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
alter table {workingTable} add column removed_year numeric;
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
alter table {workingTable} add column "comments" text;

alter table {workingTable} add column original_point geometry(POINT, 4326);
alter table {workingTable} add column snapped_point geometry(POINT, 4326);


update {workingTable} set 
    dam_name_en = "dam name", 
    waterbody_name_en = river, 
    nearest_municipality = "nearest downstream city";

update {workingTable} set province_territory_code = 
CASE WHEN "state/ province" = 'ON/QC' THEN 'on'
    WHEN "state/ province" = 'QC/ON' THEN 'qc'
    WHEN "state/ province" = 'NB/Maine (US)' THEN 'nb'
    WHEN "state/ province" = 'Ouareau' THEN 'qc'
    WHEN "state/ province" = 'NF' THEN 'nl' 
    WHEN "state/ province" is not null THEN lower("state/ province")
    ELSE NULL
END;
  

update {workingTable} set use_code = 
CASE WHEN "main purpose" =  'Irrigation' THEN 1
    WHEN "main purpose" = 'Hydroelectricity' THEN 2
    WHEN "main purpose" = 'Water Supply' THEN 3
    WHEN "main purpose" =  'Flood Control' THEN 4
    WHEN "main purpose" =  'Recreation' THEN 5
    WHEN "main purpose" = 'Navigation' THEN 6
    WHEN "main purpose" = 'Fishery' THEN 7
    WHEN "main purpose" = 'Pollution control' THEN 8
    WHEN "main purpose" = 'Invasive species control' THEN 9
    WHEN  "main purpose" = 'Other' THEN 10
    ELSE NULL
    END;

update {workingTable} set use_irrigation_code = 
CASE
    WHEN "main purpose" = 'Irrigation' THEN 1
    WHEN "other purposes" = 'Irrigation' THEN 2
    WHEN "other purposes" = 'Irrigation; Recreation' THEN 2
    WHEN "other purposes" = 'Water Supply; Irrigation; Recreation' THEN 3
    ELSE NULL
END;

update {workingTable} set use_electricity_code = 
CASE
    WHEN "main purpose" = 'Hydroelectricity' THEN 1
    WHEN "other purposes" = 'Hydroelectricity' THEN 2
    WHEN "other purposes" = 'Hydroelectricity; Recreation' THEN 2 
    WHEN "other purposes" = 'Hydroelectricity; Water Supply' THEN 2
    WHEN "other purposes" =  'Hydropower; Recreation' THEN 2
    WHEN  "other purposes" = 'Water Supply; Hydroelectricity' THEN 3
    ELSE NULL
END;

update {workingTable} set use_supply_code = 
CASE
    WHEN "main purpose" = 'Water Supply' THEN 1
    WHEN "other purposes" = 'Water Supply'  THEN 2
    WHEN "other purposes" = 'Water Supply; Flood Control; Recreation' THEN 2
    WHEN "other purposes" = 'Water Supply; Irrigation; Recreation' THEN 2 
    WHEN "other purposes" = 'Water Supply; Hydroelectricity' THEN 2
    WHEN "other purposes" = 'Water Supply; Recreation' THEN 2
    WHEN "other purposes" = 'Flood Control; Recreation; Water Supply' THEN 3 
    WHEN "other purposes" =  'Hydroelectricity; Water Supply' THEN 3
    ELSE NULL
END;

update {workingTable} set use_floodcontrol_code = 
CASE
    WHEN "main purpose" = 'Flood Control' THEN 1
    WHEN "other purposes" = 'Flood Control' THEN 2
    WHEN "other purposes" = 'Flood Control, Navigation' THEN 2
    WHEN "other purposes" = 'Flood Control; Navigation' THEN 2
    WHEN "other purposes" = 'Flood Control; Recreation' THEN 2
    WHEN "other purposes" = 'Flood Control; Recreation; Water Supply' THEN 2
    WHEN "other purposes" = 'Water Supply; Flood Control; Recreation' THEN 3
    ELSE NULL
END;

update {workingTable} set use_recreation_code = 
CASE
    WHEN "main purpose" = 'Recreation' THEN 1
    WHEN "other purposes" = 'Recreation' THEN 2
    WHEN "other purposes" = 'Flood Control; Recreation' THEN 3
    WHEN "other purposes" = 'Flood Control; Recreation; Water Supply' THEN 3 
    WHEN "other purposes" = 'Hydroelectricity; Recreation' THEN 3
    WHEN "other purposes" = 'Hydropower; Recreation' THEN 3
    WHEN "other purposes" = 'Irrigation; Recreation' THEN 3
    WHEN "other purposes" = 'Navigation; Recreation' THEN 3
    WHEN "other purposes" = 'Water Supply; Flood Control; Recreation' THEN 3
    WHEN "other purposes" = 'Water Supply; Irrigation; Recreation' THEN 3 
    WHEN "other purposes" = 'Water Supply; Recreation' THEN 3
    ELSE NULL
END;


update {workingTable} set use_navigation_code = 
CASE
    WHEN "other purposes" = 'Navigation' THEN 2
    WHEN "other purposes" = 'Navigation; Recreation' THEN 2
    WHEN "other purposes" = 'Flood Control, Navigation' THEN 3
    WHEN "other purposes" = 'Flood Control; Navigation' THEN 3
    ELSE NULL
END;

update {workingTable} set 
    construction_year = "year completed", 
    maintenance_last = cast('01-01-' || "year modified" as date),
    height_m = "dam height (m)", 
    length_m = "dam lengh (m)";


update {workingTable} set construction_type_code = 
CASE
    WHEN "dam type" = 'Arch' THEN 1
    WHEN "dam type" = 'Arch; Rockfill' THEN 1
    WHEN "dam type" = 'Arch; Rockfill; Gravity' THEN 1
    WHEN "dam type" = 'Buttress' THEN 2
    WHEN "dam type" = 'Buttress; Earth' THEN 2
    WHEN "dam type" = 'Earth'THEN 3
    WHEN "dam type" = 'Earth; Gravity' THEN 3
    WHEN "dam type" = 'Earth; Gravity; Rockfill' THEN 3
    WHEN "dam type" = 'Earth; Rockfill' THEN 3
    WHEN "dam type" = 'Earth; Rockfill; Arch' THEN 3
    WHEN "dam type" = 'Earth; Rockfill; Gravity' THEN 3
    WHEN "dam type" = 'Earth;Gravity' THEN 3
    WHEN "dam type" = 'Earth;Rockfill' THEN 3
    WHEN "dam type" = 'Gavity'THEN 4
    WHEN "dam type" = 'Gravity' THEN 4
    WHEN "dam type" = 'Gravity; Earth'THEN 4
    WHEN "dam type" = 'Gravity; Rockfill' THEN 4
    WHEN "dam type" = 'Gravity; Rockfill; Earth' THEN 4
    WHEN "dam type" = 'Multi-Arch' THEN 5
    WHEN "dam type" = 'Multiple Arch' THEN 5
    WHEN "dam type" = 'Rockfill'  THEN 6
    WHEN "dam type" = 'Rockfill; Earth' THEN 6
    WHEN "dam type" = 'Rockfill; Earth; Gravity' THEN 6
    WHEN "dam type" = 'Rockfill; Gravity' THEN 6
    WHEN "dam type" = 'Unknown'  THEN 9
    WHEN "dam type" IS NULL THEN 9
END;

update {workingTable} set 
    storage_capacity_mcm = cast(replace("normal reservoir storage (m3)", ',', '') as double precision),
    generating_capacity_mwh = "electric capacity";


update {workingTable} set reservoir_present = case when storage_capacity_mcm is not null and  storage_capacity_mcm > 0 then true else false end;

update {workingTable} set "data_source_id" = npdp_id;
update {workingTable} set "data_source" = 'NPDP';

update {workingTable} set original_point = st_setsrid(st_makepoint(longitude, latitude), 4326);
select cabd.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
update {workingTable} set snapped_point = original_point where snapped_point is null;

update {workingTable} set province_territory_code = a.code from cabd.province_territory_codes a where st_contains(a.geometry, original_point) and province_territory_code is null;
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
ownership_type_code,province_reg_body,federal_reg_body,
province_compliance_status,federal_compliance_status,operating_note,
operating_status_code,use_code,use_irrigation_code,
use_electricity_code,use_supply_code,use_floodcontrol_code,
use_recreation_code,use_navigation_code,use_fish_code,
use_pollution_code,use_invasivespecies_code,use_other_code,
lake_control_code,construction_year,removed_year,
assess_schedule,expected_life,maintenance_last,
maintenance_next,function_code,condition_code,construction_type_code,
height_m,length_m,size_class_code,spillway_capacity,spillway_type_code,
reservoir_present,reservoir_area_skm,reservoir_depth_m,storage_capacity_mcm,
avg_rate_of_discharge_ls,degree_of_regulation_pc,provincial_flow_req,
federal_flow_req,catchment_area_skm,hydro_peaking_system,
generating_capacity_mwh,turbine_number,turbine_type_code,
up_passage_type_code,down_passage_route_code,capture_date,last_update,
data_source_id,data_source,"comments",complete_level_code,original_point,snapped_point)
SELECT
cabd_id, dam_name_en,
dam_name_fr,waterbody_name_en,waterbody_name_fr,
reservoir_name_en,reservoir_name_fr,watershed_group_code,
province_territory_code,nearest_municipality,"owner",
ownership_type_code,province_reg_body,federal_reg_body,
province_compliance_status,federal_compliance_status,operating_note,
operating_status_code,use_code,use_irrigation_code,
use_electricity_code,use_supply_code,use_floodcontrol_code,
use_recreation_code,use_navigation_code,use_fish_code,
use_pollution_code,use_invasivespecies_code,use_other_code,
lake_control_code,construction_year,removed_year,
assess_schedule,expected_life,maintenance_last,
maintenance_next,function_code,condition_code,construction_type_code,
height_m,length_m,size_class_code,spillway_capacity,spillway_type_code,
reservoir_present,reservoir_area_skm,reservoir_depth_m,storage_capacity_mcm,
avg_rate_of_discharge_ls,degree_of_regulation_pc,provincial_flow_req,
federal_flow_req,catchment_area_skm,hydro_peaking_system,
generating_capacity_mwh,turbine_number,turbine_type_code,
up_passage_type_code,down_passage_route_code,capture_date,last_update,
data_source_id,data_source,"comments",complete_level_code, original_point,snapped_point
FROM {workingTable};
"""

print("Script Complete")
print("Data loaded into table: " + workingTable)
print("To add data to barrier web app run the following query:")
print(updatequery)