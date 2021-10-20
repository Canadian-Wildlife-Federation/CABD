import LOAD_dams_main as main

script = main.DamLoadingScript("nlprov")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = dam_index_num;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET "owner" = owner_name;

--TO DO: add case statements to handle ownership_type_code assignments
UPDATE {script.tempTable} SET ownership_type_code =
    CASE
    WHEN
    WHEN
    WHEN
    WHEN
    ELSE NULL END;

UPDATE {script.tempTable} SET construction_year = year_built::numeric;
UPDATE {script.tempTable} SET operating_status_code =
    CASE
    WHEN dam_status = 'Abandoned' THEN 1
    WHEN dam_status = 'Active' THEN 2
    WHEN dam_status =  'Decommissioned' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_code =
    CASE 
    WHEN purpose_drinking = 'Primary' THEN 3
    WHEN purpose_industrial = 'Primary' THEN 10
    WHEN purpose_hydro = 'Primary' THEN 2
    WHEN purpose_flood = 'Primary' THEN 4
    WHEN purpose_ice = 'Primary' THEN 10
    WHEN purpose_forestry = 'Primary' THEN 10
    WHEN purpose_other = 'Primary' THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code =
    CASE 
    WHEN purpose_hydro = 'Primary' THEN 1
    WHEN purpose_hydro = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code =
    CASE
    WHEN purpose_drinking = 'Primary' THEN 1
    WHEN purpose_drinking = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code =
    CASE
    WHEN purpose_flood = 'Primary' THEN 1
    WHEN purpose_flood = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_other_code =
    CASE
    WHEN purpose_other = 'Primary' THEN 1
    WHEN purpose_other = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'CBD = Concrete Buttress Dam' THEN 2
    WHEN dam_type = 'RCCG = roller compacted concrete, gravity dam' THEN 4
    WHEN dam_type = 'CAGD = concrete arch gravity dam' THEN 4
    WHEN dam_type = 'RFTC = rock filled timber crib' THEN 8
    WHEN dam_type = 'XX = other type' THEN 10
    WHEN dam_type ILIKE 'CAD = Concrete Arch' THEN 1
    WHEN dam_type ILIKE 'EF = Earthfill' THEN 3
    WHEN dam_type ILIKE 'CGD = Concrete Gravity' THEN 4
    WHEN dam_type ILIKE 'RFCC = Rockfill, Central Core' THEN 6
    WHEN dam_type ILIKE 'CFRD = Concrete Faced Rockfill Dam' THEN 6
    WHEN dam_type IS NULL THEN 9
    ELSE NULL END;
UPDATE {script.tempTable} SET height_m = dim_max_height;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid; --only if it has fid

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    ownership_type_code int2,
    construction_year numeric,
    operating_status_code int2,
    use_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_other_code int2,
    construction_type_code int2,
    height_m float4,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    "owner",
    ownership_type_code,
    construction_year,
    operating_status_code,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_other_code,
    construction_type_code,
    height_m,
    data_source,
    data_source_id
)
SELECT
    dam_name_en,
    "owner",
    ownership_type_code,
    construction_year,
    operating_status_code,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_other_code,
    construction_type_code,
    height_m,
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "owner",
    DROP COLUMN ownership_type_code,
    DROP COLUMN construction_year,
    DROP COLUMN operating_status_code,
    DROP COLUMN use_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_other_code,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS nlprov
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (nlprov.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'nlprov') 
    OR nlprov.data_source_id = duplicates.dups_nlprov;       
"""
#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'nlprov', now(), null, null, 'Data update - ' || now());

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.dam_name_en_ds END,
    owner_ds = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source ELSE cabdsource.owner_ds END,
    ownership_type_code_ds = CASE WHEN (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.ownership_type_code_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year is null and origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabdsource.construction_year_ds END,
    operating_status_code_ds = CASE WHEN (cabd.operating_status_code is null and origin.operating_status_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.operating_status_code_ds END,
    use_code_ds = CASE WHEN (cabd.use_code is null and origin.use_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code is null and origin.use_electricity_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code is null and origin.use_supply_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code is null and origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_floodcontrol_code_ds END,
    use_other_code_ds = CASE WHEN (cabd.use_other_code is null and origin.use_other_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.use_other_code_ds END,
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.construction_type_code_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabdsource.height_m_ds END,

    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    owner_dsfid = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.owner_dsfid END,
    ownership_type_code_dsfid = CASE WHEN (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.ownership_type_code_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year is null and origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.construction_year_dsfid END,
    operating_status_code_dsfid = CASE WHEN (cabd.operating_status_code is null and origin.operating_status_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.operating_status_code_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code is null and origin.use_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code is null and origin.use_electricity_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code is null and origin.use_supply_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code is null and origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_floodcontrol_code_dsfid END,
    use_other_code_dsfid = CASE WHEN (cabd.use_other_code is null and origin.use_other_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.use_other_code_dsfid END,
    construction_type_code_dsfid = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.construction_type_code_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.height_m_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS origin
WHERE
    cabdsource.cabd_id = origin.cabd_id and cabd.cabd_id = cabdsource.cabd_id;
    
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,   
    "owner" = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.owner ELSE cabd.owner END,
    ownership_type_code = CASE WHEN (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) THEN origin.ownership_type_code ELSE cabd.ownership_type_code END,
    construction_year = CASE WHEN (cabd.construction_year is null and origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabd.construction_year END,
    operating_status_code = CASE WHEN (cabd.operating_status_code is null and origin.operating_status_code IS NOT NULL) THEN origin.operating_status_code ELSE cabd.operating_status_code END,
    use_code = CASE WHEN (cabd.use_code is null and origin.use_code IS NOT NULL) THEN origin.use_code ELSE cabd.use_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code is null and origin.use_electricity_code IS NOT NULL) THEN origin.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code is null and origin.use_supply_code IS NOT NULL) THEN origin.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code is null and origin.use_floodcontrol_code IS NOT NULL) THEN origin.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_other_code = CASE WHEN (cabd.use_other_code is null and origin.use_other_code IS NOT NULL) THEN origin.use_other_code ELSE cabd.use_other_code END,    
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabd.height_m END        
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)