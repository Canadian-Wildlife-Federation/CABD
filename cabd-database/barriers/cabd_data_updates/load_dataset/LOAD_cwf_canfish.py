import LOAD_main as main

script = main.LoadingScript("cwf_canfish")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = fid;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.fishWorkingTable};
CREATE TABLE {script.fishWorkingTable} AS
    SELECT 
        "name of dam/barrier",
        "stream/river",
        "fishway type",
        "monitoring equipment",
        "contracted by",
        "constructed by",
        "plans held by",
        "purpose of fishway",
        "designed based on biology?",
        "length of fishway (m)",
        "elevation (m)",
        "inclination (%)",
        "mean channel depth (m)",
        "bank or midstream entrance",
        "entrance position in water column",
        "post_construction modifications?",
        "date of modification",
        "reason for modification",
        "date constructed",
        "operated by",
        "period of operation",
        "evaluating studies",
        "nature of evaluation studies",
        "species known to use fishway",
        "species known not to use fishway",
        "engineering notes",
        "operations notes",
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.fishWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fishWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fishWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fishWorkingTable} ADD COLUMN dam_id uuid;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN river_name_en varchar(512);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN fishpass_type_code int2;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN monitoring_equipment text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN contracted_by text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN constructed_by text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN plans_held_by text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN purpose text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN designed_on_biology bool;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN elevation_m float4;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN gradient float4;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN depth_m float4;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN entrance_location_code int2;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN entrance_position_code int2;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN modified boolean;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN modification_year int2;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN modification_purpose text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN year_constructed int2;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN operated_by text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN operation_period text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN has_evaluating_studies boolean;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN nature_of_evaluation_studies text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN engineering_notes text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN operating_notes text;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN species_known_to_use_fishway varchar;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN species_known_not_to_use_fishway varchar;

UPDATE {script.fishWorkingTable} SET dam_name_en = "name of dam/barrier";
UPDATE {script.fishWorkingTable} SET river_name_en = "stream/river";
UPDATE {script.fishWorkingTable} SET fishpass_type_code = 
    CASE 
    WHEN "fishway type" IN ('Denil', 'denil', 'Steep pass', 'Alaska steep pass') THEN 1
    WHEN "fishway type" IN ('Runaround', 'Pool and riffle', 'Bypass channel', 'Rock ramp') THEN 2
    WHEN "fishway type" ILIKE '%weir%' THEN 3
    WHEN "fishway type" IN ('Pool and orifice', 'Notch') THEN 4
    WHEN "fishway type" ILIKE 'Vertical slot' THEN 6
    WHEN "fishway type" IN ('Eel ladder', 'Other') THEN 7
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET
     monitoring_equipment = "monitoring equipment",
     contracted_by = "contracted by",
     constructed_by = "constructed by",
     plans_held_by = "plans held by",
     purpose = "purpose of fishway";
UPDATE {script.fishWorkingTable} SET designed_on_biology = 
    CASE 
    WHEN "designed based on biology?" ILIKE 'Yes' THEN true
    WHEN "designed based on biology?" = 'No' THEN false
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET 
    length_m = cast("length of fishway (m)" as double precision),
    elevation_m = cast("elevation (m)" as double precision),
    gradient = cast("inclination (%)" as double precision),
    depth_m = cast("mean channel depth (m)" as double precision);
UPDATE {script.fishWorkingTable} SET entrance_location_code =    
    CASE 
    WHEN "bank or midstream entrance" = 'Midstream' THEN 1
    WHEN "bank or midstream entrance" = 'Bank' THEN 2
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET entrance_position_code = 
    CASE 
    WHEN "entrance position in water column" = 'Bottom' THEN 1
    WHEN "entrance position in water column" = 'Surface' THEN 2
    WHEN "entrance position in water column" ILIKE 'Bottom and Surface' THEN 3
    WHEN "entrance position in water column" = 'Mid-column' THEN 4
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET modified = 
    CASE 
    WHEN "post_construction modifications?" ILIKE 'Yes' THEN true
    WHEN "post_construction modifications?" = 'No' THEN false
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET modification_year = cast("date of modification" as integer);
UPDATE {script.fishWorkingTable} SET modification_purpose = "reason for modification";
UPDATE {script.fishWorkingTable} SET year_constructed = cast("date constructed" as integer);
UPDATE {script.fishWorkingTable} SET operated_by = "operated by";
UPDATE {script.fishWorkingTable} SET operation_period = "period of operation";
UPDATE {script.fishWorkingTable} SET has_evaluating_studies =
    CASE WHEN "evaluating studies" = 'Yes' THEN true
    WHEN "evaluating studies" = 'No' THEN false
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET 
    nature_of_evaluation_studies = "nature of evaluation studies",
    engineering_notes = "engineering notes",
    operating_notes = "operations notes",
    species_known_to_use_fishway = "species known to use fishway",
    species_known_not_to_use_fishway = "species known not to use fishway";

--delete extra fields so only mapped fields remain
 ALTER TABLE {script.fishWorkingTable}
    DROP COLUMN "name of dam/barrier",
    DROP COLUMN "stream/river",
    DROP COLUMN "fishway type",
    DROP COLUMN "monitoring equipment",
    DROP COLUMN "contracted by",
    DROP COLUMN "constructed by",
    DROP COLUMN "plans held by",
    DROP COLUMN "purpose of fishway",
    DROP COLUMN "designed based on biology?",
    DROP COLUMN "length of fishway (m)",
    DROP COLUMN "elevation (m)",
    DROP COLUMN "inclination (%)",
    DROP COLUMN "mean channel depth (m)",
    DROP COLUMN "bank or midstream entrance",
    DROP COLUMN "entrance position in water column",
    DROP COLUMN "post_construction modifications?",
    DROP COLUMN "date of modification",
    DROP COLUMN "reason for modification",
    DROP COLUMN "date constructed",
    DROP COLUMN "operated by",
    DROP COLUMN "period of operation",
    DROP COLUMN "evaluating studies",
    DROP COLUMN "nature of evaluation studies",
    DROP COLUMN "engineering notes",
    DROP COLUMN "operations notes",
    DROP COLUMN "species known to use fishway",
    DROP COLUMN "species known not to use fishway";


--create species mapping table
DROP TABLE IF EXISTS {script.speciesMappingTable};
CREATE TABLE {script.speciesMappingTable} (name varchar, fishid uuid);
INSERT INTO {script.speciesMappingTable}
  SELECT DISTINCT rtrim(ltrim(id)) FROM (
   SELECT DISTINCT split_part(id, ',', generate_series(1, 50)) AS id FROM 
   (
    SELECT split_part(species_known_to_use_fishway, E'\n', generate_series(1,50))  AS id FROM {script.fishWorkingTable}
    UNION
    SELECT split_part(species_known_not_to_use_fishway, E'\n', generate_series(1,50)) AS id FROM {script.fishWorkingTable}
   ) foo
   WHERE foo.id IS NOT NULL and foo.id != '')
   bar
 WHERE id IS NOT NULL and id != '';
 
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE LOWER({script.speciesMapping}.name) = LOWER(a.species_name);
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE LOWER({script.speciesMapping}.name) = LOWER(a.common_name) AND fishid IS NULL;
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Ameiurus nebolosus' AND a.name = 'Brown bullhead (Ameiurus nebulosus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Amploplites rupestris' AND a.name = 'Rock bass (Ambloplites rupestris)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Carpoides cyprinus' AND a.name = 'Quillback (Carpiodes cyprinus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Castomus commersoni' AND a.name = 'White sucker (Catostomus commersonii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Castostomous commersoni' AND a.name = 'White sucker (Catostomus commersonii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Castostomus commersoni' AND a.name = 'White sucker (Catostomus commersonii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Catostomus commersoni' AND a.name = 'White sucker (Catostomus commersonii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Castostomus castosmus' AND a.name = 'Longnose sucker (Catostomus catostomus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Catostmous catostomus' AND a.name = 'Longnose sucker (Catostomus catostomus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Chinook' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'coho' AND a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Hioden tergisus' AND a.name = 'Mooneye (Hiodon tergisus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Ictalurus punctactus' AND a.name = 'Channel catfish (Ictalurus punctatus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Ictiobus cpyrinellus' AND a.name = 'Bigmouth buffalo (Ictiobus cyprinellus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Morone chryops' AND a.name = 'White bass (Morone chrysops)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Moxostoma nacrolepidotum' AND a.name = 'Shorthead redhorse (Moxostoma macrolepidotum)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Nocomis bigutattus' AND a.name = 'Hornyhead chub (Nocomis biguttatus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Ocorhynchus clarkii'  AND a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus clarkii' AND a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhynchus clarki' AND a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhynchus clarkii' AND a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus nerka' AND a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus tshawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus tshwaytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus gorbuscha' AND a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus kisutch' AND a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchornychus kisutch' AND a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus kisutch' AND a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhyncus kisutch' AND a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus kisutch' AND a.name = 'Coho salmon (Oncorhynchus kisutch)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus tschawyscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus tschawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus tshawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus tshwaytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus tsawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus tshawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus tshawytsha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchornychus tshawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhyncus tshawytscha' AND a.name = 'Chinook salmon (Oncorhynchus tshawytscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhychus mykiss'  AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchornychus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchrorhyncus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhyncus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus mykis'  AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhynchus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncoryhnchus mykiss' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus gorbuscha' AND a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhyncus gorbuscha' AND a.name = 'Pink salmon (Oncorhynchus gorbuscha)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus keta' AND a.name = 'Chum salmon (Oncorhynchus keta)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorynchus nerka' AND a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhynchus nerka' AND a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Oncorhyncus nerka' AND a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Onchorhyncus nerka' AND a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Petromyzon marinus' AND a.name = 'Sea lamprey (Petromyzon marinus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Petromyzon marinus' AND a.name = 'Sea lamprey (Petromyzon marinus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Poxomis nigromaculatus' AND a.name = 'Black crappie (Pomoxis nigromaculatus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'rainbow trout' AND a.name = 'Rainbow/steelhead trout (Oncorhynchus mykiss)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'sockeye' AND a.name = 'Sockeye/Kokanee salmon (Oncoryhnchus nerka)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'bull trout' AND a.name = 'Bull trout (Salvelinus confluentus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Salmo clarki' AND a.name = 'Cutthroat trout (Oncorhyncus clarkii)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Salmo salar' AND a.name = 'Atlantic salmon (Salmo salar)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Rainbow Smelt' AND a.name = 'Rainbow smelt (Osmerus mordax)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Osmerus mordax' AND a.name = 'Rainbow smelt (Osmerus mordax)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Prosopium williamsoni' AND a.name = 'Mountain whitefish (Prosopium williamsoni)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Salvelinus confluentus' AND a.name = 'Bull trout (Salvelinus confluentus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Salvelinus malma malma' AND a.name = 'Dolly Varden trout (Salvelinus malma)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Alewives' AND a.name = 'Alewife (Alosa pseudoharengus)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Sander Lucius' AND a.name = 'Zander (Sander lucioperca)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'Stizostedion canadense' AND a.name = 'Sauger (Sander canadensis)';
UPDATE {script.speciesMappingTable} SET fishid = a.id FROM cabd.fish_species a WHERE {script.speciesMapping}.name = 'members of the Catostomus genus' AND a.name = 'Longnose sucker (Catostomus catostomus)';
INSERT INTO {script.speciesMappingTable} (fishid, name) SELECT a.id, 'members of the Catostomus genus' FROM cabd.fish_species a WHERE a.name IN ('Bridgelip sucker (Catostomus columbianus)','White sucker (Catostomus commersonii)', 'Mountain sucker (Catostomus platyrhynchus)', 'Largescale sucker (Catostumus macrocheilus)');

"""

script.do_work(query)
