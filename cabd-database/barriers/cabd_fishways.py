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

workingSchema = "load"
workingTableRaw = "fishways"
speciesMapping = "fishmapping"
workingTable = workingSchema + "." + workingTableRaw
speciesMappingTable = workingSchema + "." + speciesMapping

snappingDistance = 50 #maximum distance for snapping in meters

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]
    
if dataFile == '':
    print("Data file required.  Usage: cabd_fishways.py <datafile>")
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
DROP TABLE if exists {speciesMappingTable};
"""

with conn.cursor() as cursor:
    cursor.execute(query);
conn.commit();

#load data using ogr
orgDb="dbname='" + dbName + "' host='"+ dbHost+"' port='"+dbPort+"' user='"+dbUser+"' password='"+ dbPassword+"'"
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -sql "SELECT * FROM Fishway_Table" -nln "' + workingTable + '" --config OGR_XLSX_HEADERS FORCE ' + dataFile
print(pycmd)
subprocess.run(pycmd)

#run scripts to convert the data
print("running mapping queries...")
query = f"""

delete from {workingTable} where "fishway type" is null;

alter table {workingTable} add column cabd_id uuid;
update {workingTable} set cabd_id = uuid_generate_v4();

alter table {workingTable} add column dam_id uuid;
alter table {workingTable} add column latitude double precision;
alter table {workingTable} add column longitude double precision;
alter table {workingTable} add column dam_name_en varchar(512);
alter table {workingTable} add column dam_name_fr varchar(512);
alter table {workingTable} add column waterbody_name_en varchar(512);
alter table {workingTable} add column waterbody_name_fr varchar(512);
alter table {workingTable} add column river_name_en varchar(512);
alter table {workingTable} add column river_name_fr varchar(512);
alter table {workingTable} add column watershed_group_code varchar(32);
alter table {workingTable} add column province_territory_code varchar(2);
alter table {workingTable} add column nhn_workunit_id varchar(7);
alter table {workingTable} add column nearest_municipality varchar(512);

alter table {workingTable} add column fishpass_type_code int2 ;
alter table {workingTable} add column monitoring_equipment text;
alter table {workingTable} add column contracted_by text;
alter table {workingTable} add column constructed_by text;
alter table {workingTable} add column plans_held_by text;
alter table {workingTable} add column purpose text;

alter table {workingTable} add column designed_on_biology bool;
alter table {workingTable} add column length_m float4;
alter table {workingTable} add column elevation_m float4;
alter table {workingTable} add column inclination_pct float4;
alter table {workingTable} add column depth_m float4;
alter table {workingTable} add column entrance_location_code int2;
alter table {workingTable} add column entrance_position_code int2;
alter table {workingTable} add column modified boolean;
alter table {workingTable} add column modification_year int2;
alter table {workingTable} add column modification_purpose text;
alter table {workingTable} add column year_constructed int2;
alter table {workingTable} add column operated_by text;
alter table {workingTable} add column operation_period text;
alter table {workingTable} add column has_evaluating_studies boolean;
alter table {workingTable} add column nature_of_evaluation_studies text;
alter table {workingTable} add column engineering_notes text;
alter table {workingTable} add column operating_notes text;
alter table {workingTable} add column mean_fishway_velocity_ms float4;
alter table {workingTable} add column max_fishway_velocity_ms float4;
alter table {workingTable} add column estimate_of_attraction_pct float4;
alter table {workingTable} add column estimate_of_passage_success_pct float4;
alter table {workingTable} add column fishway_reference_id VARCHAR(256);
alter table {workingTable} add column data_source_id varchar(256);
alter table {workingTable} add column data_source varchar(256);
alter table {workingTable} add column complete_level_code int2;

alter table {workingTable} add column original_point geometry(POINT, 4326);
alter table {workingTable} add column snapped_point geometry(POINT, 4326);


update {workingTable} set latitude = cast("gps decimal latitude" as double precision);
update {workingTable} set longitude = cast("gps decimal longitude" as double precision);
update {workingTable} set dam_name_en =  "name of dam/barrier";

update {workingTable} set province_territory_code =
 case when "province/ territory" = 'Alberta' THEN 'ab'
 when "province/ territory" = 'British Columbia' THEN 'bc'
 when "province/ territory" = 'Yukon' THEN 'yt'
 when "province/ territory" = 'Northwest Territories' THEN 'nt'
 when "province/ territory" = 'Prince Edward Island' THEN 'pe'
 when "province/ territory" = 'Ontario' THEN 'on'
 when "province/ territory" = 'Nova Scotia' THEN 'ns'
 when "province/ territory" = 'Newfoundland' THEN 'nl'
 when "province/ territory" = 'Saskatchewan' THEN 'sk'
 when "province/ territory" = 'New Brunswick' THEN 'nb'
 when "province/ territory" = 'Quebec' THEN 'qc'
 when "province/ territory" = 'Manitoba' THEN 'mb'
 when "province/ territory" = 'Nunavut' THEN 'nu'
 else null end;

update {workingTable} set nearest_municipality = "municipality";

update {workingTable} set fishpass_type_code = 
case WHEN "fishway type" = 'Vertical slot' THEN 6
WHEN "fishway type" = 'Pool&Weir' THEN 3
WHEN "fishway type" = 'Runaround' THEN 2
WHEN "fishway type" = 'Stop-Log Pool & Weir' THEN 3
WHEN "fishway type" = 'Denil' THEN 1
WHEN "fishway type" = 'Pool and riffle' THEN 2
WHEN "fishway type" = 'Pool and weir' THEN 3
WHEN "fishway type" = 'Other' THEN 7
WHEN "fishway type" = 'Pool and orifice' THEN 4
WHEN "fishway type" = 'Eel ladder' THEN 7
WHEN "fishway type" = 'Unknown' THEN 9
WHEN "fishway type" = 'Alaska steep pass' THEN 1
WHEN "fishway type" = 'Notch' THEN 4
WHEN "fishway type" = 'Vertical Slot' THEN 6
WHEN "fishway type" = 'Steep pass' THEN 1
WHEN "fishway type" = 'denil' THEN 1
WHEN "fishway type" = 'Bypass channel' THEN 2
WHEN "fishway type" = 'Pool and Weir' THEN 3
WHEN "fishway type" = 'Rock ramp' THEN 2
ELSE NULL
END;

update {workingTable} set fishpass_type_code = 11 WHERE fishpass_type_code is null;

update {workingTable} set
     monitoring_equipment = "monitoring equipment",
     contracted_by = "contracted by",
     constructed_by = "constructed by",
     plans_held_by = "plans held by",
     purpose = "purpose of fishway";

update {workingTable} set designed_on_biology = case 
    when "designed based on biology?" = 'Yes' THEN true
    when "designed based on biology?" = 'yes' THEN true
    when "designed based on biology?" = 'No' THEN false
    else null
end;


update {workingTable} set 
    length_m = cast("length of fishway (m)" as double precision),
    elevation_m = cast("elevation (m)" as double precision),
    inclination_pct = cast("inclination (%)" as double precision),
    depth_m = cast("mean channel depth (m)" as double precision);
    
update {workingTable} set entrance_location_code = case     
    when "bank or midstream entrance" = 'Midstream' THEN 1
    when "bank or midstream entrance" = 'Bank' THEN 2
    else null
end;


update {workingTable} set entrance_position_code = case     
    when "entrance position in water column" = 'Bottom' THEN 1
    when "entrance position in water column" = 'Surface' THEN 2
    when "entrance position in water column" = 'Bottom and Surface' THEN 3
    when "entrance position in water column" = 'Bottom and surface' THEN 3
    when "entrance position in water column" = 'Mid-column' THEN 4
    else null
end;

update {workingTable} set modified = case 
    when "post_construction modifications?" = 'Yes' THEN true
    when "post_construction modifications?" = 'No' THEN false
    else null
end;

update {workingTable} set modification_year = cast("date of modification" as integer);
update {workingTable} set modification_purpose = "reason for modification";
update {workingTable} set year_constructed = cast("date constructed" as integer);


update {workingTable} set operated_by = "operated by";
update {workingTable} set operation_period = "period of operation";

update {workingTable} set has_evaluating_studies = case 
    when "evaluating studies" = 'Yes' THEN true
    when "evaluating studies" = 'No' THEN false
    else null
end;

update {workingTable} set 
    nature_of_evaluation_studies = "nature of evaluation studies",
    engineering_notes = "engineering notes";

update {workingTable} set 
    data_source = 'CANFISHPASS',
    data_source_id = null; 


create table {speciesMappingTable} (name varchar, fishid uuid);

insert into {speciesMappingTable}
  select distinct rtrim(ltrim(id)) from (
   select distinct split_part(id, ',', generate_series(1, 50)) as id from 
   (
     select split_part("species known to use fishway", E'\n', generate_series(1,50))  as id from {workingTable}
     union
     select split_part("species known not to use fishway", E'\n', generate_series(1,50)) as id from {workingTable}
   ) foo
   where foo.id is not null and foo.id != '')
   bar
 where id is not null and id != '';
 
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where lower({speciesMapping}.name) = lower(a.species_name);
update {speciesMappingTable} set fishid = a.id from cabd.fish_species  a where lower({speciesMapping}.name) = lower(a.common_name) and fishid is null;
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Ameiurus nebolosus' and a.name = 'Brown bullhead (Ameiurus nebulosus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Amploplites rupestris' and a.name = 'Rock bass (Ambloplites rupestris)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Carpoides cyprinus' and a.name = 'Quillback (Carpiodes cyprinus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Castomus commersoni' and a.name = 'White sucker (Catostomus commersonii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Castostomous commersoni' and a.name = 'White sucker (Catostomus commersonii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Castostomus commersoni' and a.name = 'White sucker (Catostomus commersonii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Catostomus commersoni' and a.name = 'White sucker (Catostomus commersonii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Castostomus castosmus' and a.name = 'Longnose sucker (Catostomus catostomus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Catostmous catostomus' and a.name = 'Longnose sucker (Catostomus catostomus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Chinook' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'coho' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Hioden tergisus' and a.name = 'Mooneye (Hiodon tergisus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Ictalurus punctactus' and a.name = 'Channel catfish (Ictalurus punctatus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Ictiobus cpyrinellus' and a.name = 'Bigmouth buffalo (Ictiobus cyprinellus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Morone chryops' and a.name = 'White bass (Morone chrysops)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Moxostoma nacrolepidotum' and a.name = 'Shorthead redhorse (Moxostoma macrolepidotum)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Nocomis bigutattus' and a.name = 'Hornyhead chub (Nocomis biguttatus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Ocorhynchus clarkii'  and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus clarkii' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhynchus clarki' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhynchus clarkii' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus tshwaytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus gorbuscha' and a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchornychus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhyncus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus tschawyscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus tschawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus tshwaytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus tsawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus tshawytsha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchornychus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhyncus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhychus mykiss'  and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchornychus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchrorhyncus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhyncus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus mykis'  and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhynchus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncoryhnchus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus gorbuscha' and a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhyncus gorbuscha' and a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus keta' and a.name = 'Chum salmon (Oncorhynchus keta)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorynchus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhynchus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Oncorhyncus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Onchorhyncus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Petromyzon marinus' and a.name = 'Sea lamprey (Petromyzon marinus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Petromyzon marinus' and a.name = 'Sea lamprey (Petromyzon marinus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Poxomis nigromaculatus' and a.name = 'Black crappie (Pomoxis nigromaculatus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'rainbow trout' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'sockeye' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'bull trout' and a.name = 'Bull trout (Salvelinus confluentus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Salmo clarki' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Salmo salar' and a.name = 'Atlantic salmon (Salmo salar)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Rainbow Smelt' and a.name = 'Rainbow smelt (Osmerus mordax)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Osmerus mordax' and a.name = 'Rainbow smelt (Osmerus mordax)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Prosopium williamsoni' and a.name = 'Mountain whitefish (Prosopium williamsoni)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Salvelinus confluentus' and a.name = 'Bull trout (Salvelinus confluentus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Salvelinus malma malma' and a.name = 'Dolly Varden trout (Salvelinus malma)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Alewives' and a.name = 'Alewife (Alosa pseudoharengus)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Sander Lucius' and a.name = 'Zander (Sander lucioperca)';
update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'Stizostedion canadense' and a.name = 'Sauger (Sander canadensis)';

update {speciesMappingTable} set fishid = a.id from cabd.fish_species a where {speciesMapping}.name = 'members of the Catostomus genus' and a.name = 'Longnose sucker (Catostomus catostomus)';
insert into {speciesMappingTable} (fishid, name) select a.id, 'members of the Catostomus genus' from cabd.fish_species a where a.name in ('Bridgelip sucker (Catostomus columbianus)','White sucker (Catostomus commersonii)', 'Mountain sucker (Catostomus platyrhynchus)', 'Largescale sucker (Catostumus macrocheilus)');

update {workingTable} set original_point = st_setsrid(st_makepoint(longitude, latitude), 4326);
update {workingTable} set snapped_point = original_point where snapped_point is null;
select cabd.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});

update {workingTable} set province_territory_code = a.code from cabd.province_territory_codes a where st_contains(a.geometry, original_point) and province_territory_code is null;

--compute dam_id based on 100m buffer
update {workingTable} set dam_id = foo.dam_id
FROM
(SELECT DISTINCT ON (cabd_id) cabd_id, dam_id
  FROM (
      SELECT
          fish.cabd_id,
          dam.cabd_id as dam_id
      FROM
        {workingTable} fish,
        dams.dams_medium_large dam,
        ST_Distance(fish.original_point, dam.original_point) as distance
      WHERE
          ST_Distance(fish.original_point, dam.original_point) < 0.01 and
          ST_Distance(fish.original_point::geography, dam.original_point::geography) < 100
      ORDER BY cabd_id, distance
  ) bar
  ) foo
  where foo.cabd_id = {workingTable}.cabd_id;

    """




#alter table {workingTable} add column fishway_reference_id VARCHAR(256);    


with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()
conn.close()
        
updatequery = f"""
insert into fishways.fishways 
(cabd_id , dam_id, dam_name_en, dam_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
watershed_group_code, nhn_workunit_id, province_territory_code, nearest_municipality,fishpass_type_code,
monitoring_equipment, architect, contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m,
inclination_pct, depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, year_constructed,
operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes, operating_notes, mean_fishway_velocity_ms,
max_fishway_velocity_ms, estimate_of_attraction_pct, estimate_of_passage_success_pct, fishway_reference_id, data_source_id,
data_source, complete_level_code, original_point, snapped_point )
select
cabd_id , dam_id, dam_name_en, dam_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
watershed_group_code, nhn_workunit_id, province_territory_code, nearest_municipality,fishpass_type_code,
monitoring_equipment, architect, contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m,
inclination_pct, depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, year_constructed,
operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes, operating_notes, mean_fishway_velocity_ms,
max_fishway_velocity_ms, estimate_of_attraction_pct, estimate_of_passage_success_pct, fishway_reference_id, data_source_id,
data_source, complete_level_code, original_point, snapped_point
FROM {workingTable};

insert into fishways.species_mapping (fishway_id, species_id, known_to_use)
select distinct a.cabd_id, b.fishid, true
from {workingTable} a, {speciesMappingTable} b
where a."species known to use fishway" like '%' || b.name || '%' and b.fishid is not null;

insert into fishways.species_mapping (fishway_id, species_id, known_to_use)
select cabd_id, fishid, false from 
(select distinct a.cabd_id, b.fishid
from {workingTable} a, {speciesMappingTable} b
where a."species known not to use fishway" like '%' || b.name || '%' and b.fishid is not null
except 
select fishway_id, species_id from fishways.species_mapping sm ) foo;

"""

print("Script Complete")
print("Data loaded into table: " + workingTable)
print("To add data to barrier web app run the following queries:")
print(updatequery)