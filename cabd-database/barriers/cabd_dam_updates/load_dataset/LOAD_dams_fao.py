import LOAD_dams_main as main

script = main.DamLoadingScript("fao")
    
query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = id_fao;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN municipality varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.tempTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN comments text;

UPDATE {script.tempTable} SET dam_name_en = name_of_dam;
UPDATE {script.tempTable} SET municipality = nearest_city;
UPDATE {script.tempTable} SET waterbody_name_en =
    CASE
    WHEN regexp_match(river, '.*River.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Creek.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Falls.*') IS NOT NULL THEN river
    WHEN river IS NULL THEN NULL
    ELSE (river || ' River') END;
UPDATE {script.tempTable} SET construction_year = 
    CASE
    WHEN completed_operational_since = 'Incomplete?' THEN NULL
    ELSE completed_operational_since::numeric END;
UPDATE {script.tempTable} SET height_m = dam_height_m;
UPDATE {script.tempTable} SET storage_capacity_mcm = reservoir_capacity_million_m3;
UPDATE {script.tempTable} SET reservoir_area_skm = reservoir_area_km2;
UPDATE {script.tempTable} SET use_irrigation_code = 
    CASE
    WHEN irrigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code = 
    CASE
    WHEN water_supply = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code = 
    CASE
    WHEN flood_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code = 
    CASE
    WHEN hydroelectricity_mw = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_navigation_code = 
    CASE
    WHEN navigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_recreation_code = 
    CASE
    WHEN recreation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_pollution_code = 
    CASE
    WHEN pollution_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_other_code =
    CASE
    WHEN other = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET "comments" = comments_orig;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    municipality varchar(512),
    waterbody_name_en varchar(512),
    construction_year numeric,
    height_m float4,
    storage_capacity_mcm float8,
    reservoir_area_skm float4,
    use_irrigation_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_electricity_code int2,
    use_navigation_code int2,
    use_recreation_code int2,
    use_pollution_code int2,
    use_other_code int2,
    comments text,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    municipality,
    waterbody_name_en,
    construction_year,
    height_m,
    storage_capacity_mcm,
    reservoir_area_skm,
    use_irrigation_code,
    use_supply_code,
    use_floodcontrol_code,
    use_electricity_code,
    use_navigation_code,
    use_recreation_code,
    use_pollution_code,
    use_other_code,
    "comments",
    data_source,
    data_source_id
)
SELECT
    dam_name_en,
    municipality,
    waterbody_name_en,
    construction_year,
    height_m,
    storage_capacity_mcm,
    reservoir_area_skm,
    use_irrigation_code,
    use_supply_code,
    use_floodcontrol_code,
    use_electricity_code,
    use_navigation_code,
    use_recreation_code,
    use_pollution_code,
    use_other_code,
    "comments",
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN municipality,
    DROP COLUMN waterbody_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN height_m,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN reservoir_area_skm,
    DROP COLUMN use_irrigation_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_pollution_code,
    DROP COLUMN use_other_code,
    DROP COLUMN "comments";

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS fao
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (fao.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'fao') 
    OR fao.data_source_id = duplicates.dups_fao;       
"""


#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'fao', now(), null, null, 'Data update - ' || now());

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.dam_name_en_ds END,
    municipality_ds = CASE WHEN (cabd.municipality IS NULL AND origin.municipality IS NOT NULL) THEN origin.data_source ELSE cabdsource.municipality_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.waterbody_name_en_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabdsource.construction_year_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabdsource.height_m_ds END,         
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source ELSE cabdsource.storage_capacity_mcm_ds END,
    reservoir_area_skm_ds = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.data_source ELSE cabdsource.reservoir_area_skm_ds END,
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code IS NULL AND origin.use_irrigation_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_irrigation_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_navigation_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_recreation_code_ds END,
    use_pollution_code_ds = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_pollution_code_ds END,
    use_other_code_ds = CASE WHEN (cabd.use_other_code IS NULL AND origin.use_other_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_other_code_ds END,
    "comments_ds" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin.data_source ELSE cabdsource.comments_ds END,
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    municipality_dsfid = CASE WHEN (cabd.municipality IS NULL AND origin.municipality IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.municipality_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.construction_year_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.height_m_dsfid END,         
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.storage_capacity_mcm_dsfid END,
    reservoir_area_skm_dsfid = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.reservoir_area_skm_dsfid END,
    use_irrigation_code_dsfid = CASE WHEN (cabd.use_irrigation_code IS NULL AND origin.use_irrigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_irrigation_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_floodcontrol_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_navigation_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_recreation_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_pollution_code_dsfid END,
    use_other_code_dsfid = CASE WHEN (cabd.use_other_code IS NULL AND origin.use_other_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_other_code_dsfid END,
    "comments_dsfid" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.comments_dsfid END   
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS origin
WHERE
    cabdsource.cabd_id = origin.cabd_id and cabd.cabd_id = cabdsource.cabd_id;


UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    municipality = CASE WHEN (cabd.municipality IS NULL AND origin.municipality IS NOT NULL) THEN origin.municipality ELSE cabd.municipality END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.waterbody_name_en ELSE cabd.waterbody_name_en END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabd.construction_year END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabd.height_m END,         
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,  
    reservoir_area_skm = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code IS NULL AND origin.use_irrigation_code IS NOT NULL) THEN origin.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.use_electricity_code ELSE cabd.use_electricity_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.use_navigation_code ELSE cabd.use_navigation_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.use_recreation_code ELSE cabd.use_recreation_code END,
    use_pollution_code = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.use_pollution_code ELSE cabd.use_pollution_code END,
    use_other_code = CASE WHEN (cabd.use_other_code IS NULL AND origin.use_other_code IS NOT NULL) THEN origin.use_other_code ELSE cabd.use_other_code END,
    "comments" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin."comments" ELSE cabd."comments" END   
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)