import LOAD_dams_main as main

script = main.DamLoadingScript("gfielding")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = dam_id_number;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN function_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN "comments" text;

UPDATE {script.tempTable} SET dam_name_en = name_of_structure;
UPDATE {script.tempTable} SET construction_year =
    CASE
    WHEN regexp_match(year_constructed, '^[0-9]{{4}}$') IS NOT NULL THEN year_constructed::numeric
    WHEN regexp_match(year_constructed, '^~[0-9]{{4}}$') IS NOT NULL THEN (regexp_replace(year_constructed, '[^0-9]', '', 'g'))::numeric
    WHEN regexp_match(year_constructed, '^[0-9]{{4}}-[0-9]{{1,4}}$') IS NOT NULL THEN SPLIT_PART(year_constructed, '-', 1)::numeric
    WHEN regexp_match(year_constructed, '^(circa)\s[0-9]{{4}}$') IS NOT NULL THEN (regexp_replace(year_constructed, '[^0-9]', '', 'g'))::numeric
    WHEN year_constructed = 'April, 1985' THEN 1985
    WHEN year_constructed = 'Aug. 1st/1989' THEN 1989
    ELSE NULL END;
UPDATE {script.tempTable} SET use_code =
    CASE
    WHEN main_purpose_of_dam = 'water supply - hydroelectric' THEN 2
    WHEN main_purpose_of_dam = 'Water supply - municipal' THEN 3
    WHEN primary_function_of_dam = 'Aboiteau or other flood reduction structure' THEN 4
    WHEN primary_function_of_dam = 'Navigation aid' THEN 6
    WHEN primary_function_of_dam = 'Mine tailings management' THEN 8
    WHEN primary_function_of_dam IS NULL THEN 11 --new unknown use_code value
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code =
    CASE
    WHEN main_purpose_of_dam = 'water supply - hydroelectric' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code = 
    CASE
    WHEN main_purpose_of_dam ILIKE 'Water supply%' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code = 
    CASE
    WHEN primary_function_of_dam = 'Aboiteau or other flood reduction structure' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_recreation_code = 
    CASE
    WHEN main_purpose_of_dam = 'Non consumptive - aquatic recreation enhancement' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_navigation_code = 
    CASE
    WHEN primary_function_of_dam = 'Navigation aid' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_pollution_code = 
    CASE
    WHEN primary_function_of_dam = 'Mine tailings management' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET function_code = 
    CASE
    WHEN primary_function_of_dam = 'Water impoundment/storage' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET "comments" = main_purpose_of_dam;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    construction_year numeric,
    use_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_recreation_code int2,
    use_navigation_code int2,
    use_pollution_code int2,
    function_code int2,
    "comments" text,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    construction_year,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_pollution_code,
    function_code,
    "comments",
    data_source,
    data_source_id
)
SELECT
    dam_name_en,
    construction_year,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_pollution_code,
    function_code,
    "comments",
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN use_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN use_pollution_code,
    DROP COLUMN function_code,
    DROP COLUMN "comments";

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS gfielding
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (gfielding.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'gfielding') 
    OR gfielding.data_source_id = duplicates.dups_gfielding;       
"""



#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (uuid, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'gfielding', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabd.construction_year END,
    use_code = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.use_code ELSE cabd.use_code END,         
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.use_recreation_code ELSE cabd.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.use_navigation_code ELSE cabd.use_navigation_code END,
    use_pollution_code = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.use_pollution_code ELSE cabd.use_pollution_code END,
    function_code = CASE WHEN (cabd.function_code IS NULL AND origin.function_code IS NOT NULL) THEN origin.function_code ELSE cabd.function_code END,
    "comments" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin."comments" ELSE cabd."comments" END   
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

UPDATE 
    {script.damAttributeTable} as cabd
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.dam_name_en_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabd.construction_year_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_code_ds END,         
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_navigation_code_ds END,
    use_pollution_code_ds = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_pollution_code_ds END,
    function_code_ds = CASE WHEN (cabd.function_code IS NULL AND origin.function_code IS NOT NULL) THEN origin.data_source ELSE cabd.function_code_ds END,
    "comments_ds" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin.data_source ELSE cabd.comments_ds END,
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_en_dsfid END    
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabd.construction_year_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_code_dsfid END,         
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_floodcontrol_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_recreation_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_navigation_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_pollution_code_dsfid END,
    function_code_dsfid = CASE WHEN (cabd.function_code IS NULL AND origin.function_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.function_code_dsfid END,
    "comments_dsfid" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin.data_source_id ELSE cabd.comments_dsfid END
FROM
    {script.workingTable} AS origin    
WHERE
    origin.cabd_id = cabd.cabd_id;


--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)