import LOAD_main as main

script = main.LoadingScript("cwf_canfish")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = canfishpass_id;
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
        name_of_dam_barrier,
        province_territory,
        stream_river,
        fishway_type,
        monitoring_equipment,
        contracted_by,
        constructed_by,
        plans_held_by,
        purpose_of_fishway,
        designed_on_biology,
        length_fishway_m,
        elevation_fishway_m,
        inclination_pct,
        mean_channel_depth_m,
        bank_midstream_entrance,
        entrance_position_water_column,
        post_construction_modification,
        date_of_modification,
        modification_reason,
        date_constructed,
        operated_by,
        operation_period,
        has_evaluating_studies,
        nature_of_evaluation_studies,
        species_known_to_use_fishway,
        species_known_not_to_use_fishway,
        engineering_notes,
        operating_notes,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.fishWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fishWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fishWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fishWorkingTable} ADD COLUMN dam_id uuid;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN structure_name_en varchar(512);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN structure_name_fr varchar(512);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN river_name_en varchar(512);
ALTER TABLE {script.fishWorkingTable} ADD COLUMN fishpass_type_code int2;
ALTER TABLE {script.fishWorkingTable} ADD COLUMN purpose text;
ALTER TABLE {script.fishWorkingTable} ALTER COLUMN designed_on_biology TYPE bool USING designed_on_biology::bool;
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
ALTER TABLE {script.fishWorkingTable} ALTER COLUMN has_evaluating_studies TYPE bool USING has_evaluating_studies::bool;
ALTER TABLE {script.fishWorkingTable} ALTER COLUMN species_known_to_use_fishway TYPE varchar;
ALTER TABLE {script.fishWorkingTable} ALTER COLUMN species_known_not_to_use_fishway TYPE varchar;

UPDATE {script.fishWorkingTable} SET structure_name_en = name_of_dam_barrier;
UPDATE {script.fishWorkingTable} SET structure_name_fr = name_of_dam_barrier WHERE province_territory = 'Quebec';
UPDATE {script.fishWorkingTable} SET river_name_en = stream_river;
UPDATE {script.fishWorkingTable} SET fishpass_type_code = 
    CASE 
    WHEN fishway_type IN ('Denil', 'denil', 'Steep pass', 'Alaska steep pass') THEN 1
    WHEN fishway_type IN ('Runaround', 'Pool and riffle', 'Bypass channel', 'Rock ramp') THEN 2
    WHEN fishway_type ILIKE '%weir%' THEN 3
    WHEN fishway_type IN ('Pool and orifice', 'Notch') THEN 4
    WHEN fishway_type ILIKE 'Vertical slot' THEN 6
    WHEN fishway_type IN ('Eel ladder', 'Other') THEN 7
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET purpose = purpose_of_fishway;
UPDATE {script.fishWorkingTable} SET 
    length_m = cast(length_fishway_m as double precision),
    elevation_m = cast(elevation_fishway_m as double precision),
    gradient = cast(inclination_pct as double precision),
    depth_m = cast(mean_channel_depth_m as double precision);
UPDATE {script.fishWorkingTable} SET entrance_location_code =    
    CASE 
    WHEN bank_midstream_entrance = 'Midstream' THEN 1
    WHEN bank_midstream_entrance = 'Bank' THEN 2
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET entrance_position_code = 
    CASE 
    WHEN entrance_position_water_column = 'Bottom' THEN 1
    WHEN entrance_position_water_column = 'Surface' THEN 2
    WHEN entrance_position_water_column ILIKE 'Bottom and Surface' THEN 3
    WHEN entrance_position_water_column = 'Mid-column' THEN 4
    ELSE NULL END;
UPDATE {script.fishWorkingTable} SET modified = post_construction_modification::bool USING post_construction_modification::bool;
UPDATE {script.fishWorkingTable} SET modification_year = cast(date_of_modification as integer);
UPDATE {script.fishWorkingTable} SET modification_purpose = modification_reason;
UPDATE {script.fishWorkingTable} SET year_constructed = cast(date_constructed as integer);

--delete extra fields so only mapped fields remain
 ALTER TABLE {script.fishWorkingTable}
    DROP COLUMN name_of_dam_barrier,
    DROP COLUMN province_territory,
    DROP COLUMN stream_river,
    DROP COLUMN fishway_type,
    DROP COLUMN purpose_of_fishway,
    DROP COLUMN length_fishway_m,
    DROP COLUMN elevation_fishway_m,
    DROP COLUMN inclination_pct,
    DROP COLUMN mean_channel_depth_m,
    DROP COLUMN bank_midstream_entrance,
    DROP COLUMN entrance_position_water_column,
    DROP COLUMN post_construction_modification,
    DROP COLUMN date_of_modification,
    DROP COLUMN modification_reason,
    DROP COLUMN date_constructed;


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
