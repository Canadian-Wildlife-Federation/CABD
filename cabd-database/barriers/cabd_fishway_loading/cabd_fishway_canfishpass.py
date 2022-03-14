import psycopg2 as pg2
import sys
import subprocess

ogr = "C:\\OSGeo4W64\\bin\\ogr2ogr.exe";

dbHost = "cabd-postgres-dev.postgres.database.azure.com"
dbPort = "5432"
dbName = "cabd"

workingSchema = "load"
workingTableRaw = "fishways"
speciesMapping = "fishmapping"
workingTable = workingSchema + "." + workingTableRaw
speciesMappingTable = workingSchema + "." + speciesMapping

snappingDistance = 50 #maximum distance for snapping in meters

dataFile = "";

if len(sys.argv) == 2:
    dataFile = sys.argv[1]

dbUser = sys.argv[2]
dbPassword = sys.argv[3]
    
if dataFile == '':
    print("Data file required.  Usage: cabd_fishways.py <datafile> <dbUser> <dbPassword>")
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
pycmd = '"' + ogr + '" -f "PostgreSQL" PG:"' + orgDb + '" -nln "' + workingTable + '" -lco GEOMETRY_NAME=geometry -nlt PROMOTE_TO_MULTI ' + dataFile
print(pycmd)
subprocess.run(pycmd)

#run scripts to convert the data
print("running mapping queries...")
query = f"""
DELETE FROM {workingTable} WHERE "fishway type" IS NULL;
ALTER TABLE {workingTable} ADD COLUMN cabd_id uuid;
UPDATE {workingTable} SET cabd_id = uuid_generate_v4();
ALTER TABLE {workingTable} ADD COLUMN dam_id uuid;
ALTER TABLE {workingTable} ADD COLUMN latitude double precision;
ALTER TABLE {workingTable} ADD COLUMN longitude double precision;
ALTER TABLE {workingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN waterbody_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN river_name_en varchar(512);
ALTER TABLE {workingTable} ADD COLUMN river_name_fr varchar(512);
ALTER TABLE {workingTable} ADD COLUMN watershed_group_code varchar(32);
ALTER TABLE {workingTable} ADD COLUMN province_territory_code varchar(2);
ALTER TABLE {workingTable} ADD COLUMN nhn_workunit_id varchar(7);
ALTER TABLE {workingTable} ADD COLUMN nearest_municipality varchar(512);
ALTER TABLE {workingTable} ADD COLUMN fishpass_type_code int2 ;
ALTER TABLE {workingTable} ADD COLUMN monitoring_equipment text;
ALTER TABLE {workingTable} ADD COLUMN contracted_by text;
ALTER TABLE {workingTable} ADD COLUMN constructed_by text;
ALTER TABLE {workingTable} ADD COLUMN plans_held_by text;
ALTER TABLE {workingTable} ADD COLUMN purpose text;
ALTER TABLE {workingTable} ADD COLUMN designed_on_biology bool;
ALTER TABLE {workingTable} ADD COLUMN length_m float4;
ALTER TABLE {workingTable} ADD COLUMN elevation_m float4;
ALTER TABLE {workingTable} ADD COLUMN inclination_pct float4;
ALTER TABLE {workingTable} ADD COLUMN depth_m float4;
ALTER TABLE {workingTable} ADD COLUMN entrance_location_code int2;
ALTER TABLE {workingTable} ADD COLUMN entrance_position_code int2;
ALTER TABLE {workingTable} ADD COLUMN modified boolean;
ALTER TABLE {workingTable} ADD COLUMN modification_year int2;
ALTER TABLE {workingTable} ADD COLUMN modification_purpose text;
ALTER TABLE {workingTable} ADD COLUMN year_constructed int2;
ALTER TABLE {workingTable} ADD COLUMN operated_by text;
ALTER TABLE {workingTable} ADD COLUMN operation_period text;
ALTER TABLE {workingTable} ADD COLUMN has_evaluating_studies boolean;
ALTER TABLE {workingTable} ADD COLUMN nature_of_evaluation_studies text;
ALTER TABLE {workingTable} ADD COLUMN engineering_notes text;
ALTER TABLE {workingTable} ADD COLUMN operating_notes text;
ALTER TABLE {workingTable} ADD COLUMN mean_fishway_velocity_ms float4;
ALTER TABLE {workingTable} ADD COLUMN max_fishway_velocity_ms float4;
ALTER TABLE {workingTable} ADD COLUMN estimate_of_attraction_pct float4;
ALTER TABLE {workingTable} ADD COLUMN estimate_of_passage_success_pct float4;
ALTER TABLE {workingTable} ADD COLUMN fishway_reference_id VARCHAR(256);
ALTER TABLE {workingTable} ADD COLUMN data_source_id varchar(256);
ALTER TABLE {workingTable} ADD COLUMN data_source varchar(256);
ALTER TABLE {workingTable} ADD COLUMN complete_level_code int2;
ALTER TABLE {workingTable} ADD COLUMN original_point geometry(POINT, 4326);
ALTER TABLE {workingTable} ADD COLUMN snapped_point geometry(POINT, 4326);
UPDATE {workingTable} SET latitude = cast("gps decimal latitude" as double precision);
UPDATE {workingTable} SET longitude = cast("gps decimal longitude" as double precision);
UPDATE {workingTable} SET longitude = NULL, latitude = NULL WHERE longitude = 0 and latitude = 0;
UPDATE {workingTable} SET dam_name_en =  "name of dam/barrier";
UPDATE {workingTable} SET province_territory_code =
    CASE WHEN "province/ territory" = 'Alberta' THEN 'ab'
    WHEN "province/ territory" = 'British Columbia' THEN 'bc'
    WHEN "province/ territory" = 'Yukon' THEN 'yt'
    WHEN "province/ territory" = 'Northwest Territories' THEN 'nt'
    WHEN "province/ territory" = 'Prince Edward Island' THEN 'pe'
    WHEN "province/ territory" = 'Ontario' THEN 'on'
    WHEN "province/ territory" = 'Nova Scotia' THEN 'ns'
    WHEN "province/ territory" = 'Newfoundland' THEN 'nl'
    WHEN "province/ territory" = 'Saskatchewan' THEN 'sk'
    WHEN "province/ territory" = 'New Brunswick' THEN 'nb'
    WHEN "province/ territory" = 'Quebec' THEN 'qc'
    WHEN "province/ territory" = 'Manitoba' THEN 'mb'
    WHEN "province/ territory" = 'Nunavut' THEN 'nu'
    ELSE NULL END;
UPDATE {workingTable} SET nearest_municipality = "municipality";
UPDATE {workingTable} SET fishpass_type_code = 
    CASE WHEN "fishway type" = 'Vertical slot' THEN 6
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
UPDATE {workingTable} SET fishpass_type_code = 9 WHERE fishpass_type_code IS NULL;
UPDATE {workingTable} SET
     monitoring_equipment = "monitoring equipment",
     contracted_by = "contracted by",
     constructed_by = "constructed by",
     plans_held_by = "plans held by",
     purpose = "purpose of fishway";
UPDATE {workingTable} SET designed_on_biology = 
    CASE WHEN "designed based on biology?" = 'Yes' THEN true
    WHEN "designed based on biology?" = 'yes' THEN true
    WHEN "designed based on biology?" = 'No' THEN false
    ELSE NULL
END;
UPDATE {workingTable} SET 
    length_m = cast("length of fishway (m)" as double precision),
    elevation_m = cast("elevation (m)" as double precision),
    inclination_pct = cast("inclination (%)" as double precision),
    depth_m = cast("mean channel depth (m)" as double precision);
    
UPDATE {workingTable} SET entrance_location_code =    
    CASE WHEN "bank or midstream entrance" = 'Midstream' THEN 1
    WHEN "bank or midstream entrance" = 'Bank' THEN 2
    ELSE NULL
END;
UPDATE {workingTable} SET entrance_position_code = 
    CASE WHEN "entrance position in water column" = 'Bottom' THEN 1
    WHEN "entrance position in water column" = 'Surface' THEN 2
    WHEN "entrance position in water column" = 'Bottom and Surface' THEN 3
    WHEN "entrance position in water column" = 'Bottom and surface' THEN 3
    WHEN "entrance position in water column" = 'Mid-column' THEN 4
    ELSE NULL
END;
UPDATE {workingTable} SET modified = case 
    WHEN "post_construction modifications?" = 'Yes' THEN true
    WHEN "post_construction modifications?" = 'No' THEN false
    ELSE NULL
END;
UPDATE {workingTable} SET modification_year = cast("date of modification" as integer);
UPDATE {workingTable} SET modification_purpose = "reason for modification";
UPDATE {workingTable} SET year_constructed = cast("date constructed" as integer);
UPDATE {workingTable} SET operated_by = "operated by";
UPDATE {workingTable} SET operation_period = "period of operation";
UPDATE {workingTable} SET has_evaluating_studies =
    CASE WHEN "evaluating studies" = 'Yes' THEN true
    WHEN "evaluating studies" = 'No' THEN false
    ELSE NULL
END;
UPDATE {workingTable} SET 
    nature_of_evaluation_studies = "nature of evaluation studies",
    engineering_notes = "engineering notes";
UPDATE {workingTable} SET 
    data_source = 'CANFISHPASS',
    data_source_id = NULL; 
create table {speciesMappingTable} (name varchar, fishid uuid);
insert into {speciesMappingTable}
  select distinct rtrim(ltrim(id)) from (
   select distinct split_part(id, ',', generate_series(1, 50)) as id from 
   (
     select split_part("species known to use fishway", E'\n', generate_series(1,50))  as id from {workingTable}
     union
     select split_part("species known not to use fishway", E'\n', generate_series(1,50)) as id from {workingTable}
   ) foo
   WHERE foo.id IS NOT NULL and foo.id != '')
   bar
 WHERE id IS NOT NULL and id != '';
 
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE lower({speciesMapping}.name) = lower(a.species_name);
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species  a WHERE lower({speciesMapping}.name) = lower(a.common_name) and fishid IS NULL;
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Ameiurus nebolosus' and a.name = 'Brown bullhead (Ameiurus nebulosus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Amploplites rupestris' and a.name = 'Rock bass (Ambloplites rupestris)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Carpoides cyprinus' and a.name = 'Quillback (Carpiodes cyprinus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Castomus commersoni' and a.name = 'White sucker (Catostomus commersonii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Castostomous commersoni' and a.name = 'White sucker (Catostomus commersonii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Castostomus commersoni' and a.name = 'White sucker (Catostomus commersonii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Catostomus commersoni' and a.name = 'White sucker (Catostomus commersonii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Castostomus castosmus' and a.name = 'Longnose sucker (Catostomus catostomus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Catostmous catostomus' and a.name = 'Longnose sucker (Catostomus catostomus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Chinook' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'coho' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Hioden tergisus' and a.name = 'Mooneye (Hiodon tergisus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Ictalurus punctactus' and a.name = 'Channel catfish (Ictalurus punctatus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Ictiobus cpyrinellus' and a.name = 'Bigmouth buffalo (Ictiobus cyprinellus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Morone chryops' and a.name = 'White bass (Morone chrysops)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Moxostoma nacrolepidotum' and a.name = 'Shorthead redhorse (Moxostoma macrolepidotum)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Nocomis bigutattus' and a.name = 'Hornyhead chub (Nocomis biguttatus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Ocorhynchus clarkii'  and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus clarkii' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhynchus clarki' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhynchus clarkii' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus tshwaytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus gorbuscha' and a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchornychus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhyncus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus kisutch' and a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus tschawyscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus tschawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus tshwaytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus tsawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus tshawytsha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchornychus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhyncus tshawytscha' and a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhychus mykiss'  and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchornychus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchrorhyncus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhyncus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus mykis'  and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhynchus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncoryhnchus mykiss' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus gorbuscha' and a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhyncus gorbuscha' and a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus keta' and a.name = 'Chum salmon (Oncorhynchus keta)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorynchus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhynchus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Oncorhyncus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Onchorhyncus nerka' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Petromyzon marinus' and a.name = 'Sea lamprey (Petromyzon marinus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Petromyzon marinus' and a.name = 'Sea lamprey (Petromyzon marinus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Poxomis nigromaculatus' and a.name = 'Black crappie (Pomoxis nigromaculatus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'rainbow trout' and a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'sockeye' and a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'bull trout' and a.name = 'Bull trout (Salvelinus confluentus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Salmo clarki' and a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Salmo salar' and a.name = 'Atlantic salmon (Salmo salar)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Rainbow Smelt' and a.name = 'Rainbow smelt (Osmerus mordax)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Osmerus mordax' and a.name = 'Rainbow smelt (Osmerus mordax)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Prosopium williamsoni' and a.name = 'Mountain whitefish (Prosopium williamsoni)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Salvelinus confluentus' and a.name = 'Bull trout (Salvelinus confluentus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Salvelinus malma malma' and a.name = 'Dolly Varden trout (Salvelinus malma)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Alewives' and a.name = 'Alewife (Alosa pseudoharengus)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Sander Lucius' and a.name = 'Zander (Sander lucioperca)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'Stizostedion canadense' and a.name = 'Sauger (Sander canadensis)';
UPDATE {speciesMappingTable} SET fishid = a.id from cabd.fish_species a WHERE {speciesMapping}.name = 'members of the Catostomus genus' and a.name = 'Longnose sucker (Catostomus catostomus)';
insert into {speciesMappingTable} (fishid, name) select a.id, 'members of the Catostomus genus' from cabd.fish_species a WHERE a.name in ('Bridgelip sucker (Catostomus columbianus)','White sucker (Catostomus commersonii)', 'Mountain sucker (Catostomus platyrhynchus)', 'Largescale sucker (Catostumus macrocheilus)');
UPDATE {workingTable} SET original_point = st_transform(st_geometryN(geometry, 1), 4326);
UPDATE {workingTable} SET snapped_point = original_point WHERE snapped_point IS NULL;
select cabd.snap_to_network('{workingSchema}', '{workingTableRaw}', 'original_point', 'snapped_point', {snappingDistance});
UPDATE {workingTable} SET province_territory_code = a.code from cabd.province_territory_codes a WHERE st_contains(a.geometry, original_point) and province_territory_code IS NULL;
UPDATE {workingTable} SET nhn_workunit_id = a.id from cabd.nhn_workunit a WHERE st_contains(a.polygon, original_point) and nhn_workunit_id IS NULL;
--compute dam_id based on 100m buffer
UPDATE {workingTable} SET dam_id = foo.dam_id
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
  WHERE foo.cabd_id = {workingTable}.cabd_id;
    """

#ALTER TABLE {workingTable} ADD COLUMN fishway_reference_id VARCHAR(256);    


with conn.cursor() as cursor:
    cursor.execute(query)
conn.commit()
conn.close()
        
updatequery = f"""
INSERT INTO fishways.fishways 
(cabd_id , dam_id, dam_name_en, dam_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
watershed_group_code, nhn_workunit_id, province_territory_code, nearest_municipality,fishpass_type_code,
monitoring_equipment, architect, contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m,
inclination_pct, depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, year_constructed,
operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes, operating_notes, mean_fishway_velocity_ms,
max_fishway_velocity_ms, estimate_of_attraction_pct, estimate_of_passage_success_pct, fishway_reference_id, data_source_id,
data_source, complete_level_code, original_point, snapped_point )
SELECT
cabd_id , dam_id, dam_name_en, dam_name_fr, waterbody_name_en, waterbody_name_fr, river_name_en, river_name_fr,
watershed_group_code, nhn_workunit_id, province_territory_code, nearest_municipality,fishpass_type_code,
monitoring_equipment, architect, contracted_by, constructed_by, plans_held_by, purpose, designed_on_biology, length_m, elevation_m,
inclination_pct, depth_m, entrance_location_code, entrance_position_code, modified, modification_year, modification_purpose, year_constructed,
operated_by, operation_period, has_evaluating_studies, nature_of_evaluation_studies, engineering_notes, operating_notes, mean_fishway_velocity_ms,
max_fishway_velocity_ms, estimate_of_attraction_pct, estimate_of_passage_success_pct, fishway_reference_id, data_source_id,
data_source, complete_level_code, original_point, snapped_point
FROM {workingTable};
INSERT INTO fishways.species_mapping (fishway_id, species_id, known_to_use)
SELECT DISTINCT a.cabd_id, b.fishid, true
FROM {workingTable} a, {speciesMappingTable} b
WHERE a."species known to use fishway" like '%' || b.name || '%' and b.fishid IS NOT NULL;
INSERT INTO fishways.species_mapping (fishway_id, species_id, known_to_use)
SELECT cabd_id, fishid, false from 
(SELECT DISTINCT a.cabd_id, b.fishid
FROM {workingTable} a, {speciesMappingTable} b
WHERE a."species known not to use fishway" like '%' || b.name || '%' and b.fishid IS NOT NULL
EXCEPT
SELECT fishway_id, species_id from fishways.species_mapping sm ) foo;
"""

print("Script Complete")
print("Data loaded into table: " + workingTable)
print("To add data to barrier web app run the following queries:")
print(updatequery)