import LOAD_dams_main as main

script = main.DamLoadingScript("publicdamskml")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = objectid;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} RENAME COLUMN height_m TO height_m_orig;
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET construction_year = commissioned_year::numeric;
UPDATE {script.tempTable} SET "owner" = dam_owner;
UPDATE {script.tempTable} SET operating_status_code =
    CASE 
    WHEN dam_operation_code = 'Abandoned' THEN 1
    WHEN dam_operation_code = 'Active' THEN 2
    WHEN dam_operation_code IN ('Decommissioned', 'Removed') THEN 3
    WHEN dam_operation_code = 'Deactivated' THEN 4
    WHEN dam_operation_code IN ('Application', 'Not Constructed', 'Breached (Failed)') THEN 5
    ELSE NULL END;
UPDATE {script.tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'Concrete Arch' THEN 1
    WHEN dam_type = 'Buttress' THEN 2
    WHEN dam_type = 'Earthfill' THEN 3
    WHEN dam_type = 'Concrete Gravity' THEN 4
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Steel' THEN 7
    WHEN dam_type = 'Log Crib' THEN 8
    WHEN dam_type IN ('Concrete', 'Dugout/Pond', 'Other') THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET height_m = height_m_orig::float4;
UPDATE {script.tempTable} SET length_m = crest_length_m::float4;

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
    "owner" varchar(512),
    operating_status_code int2,
    construction_type_code int2,
    height_m float4,
    length_m float4,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    construction_year,
    "owner",
    operating_status_code,
    construction_type_code,
    height_m,
    length_m,
    data_source,
    data_source_id
)
SELECT
    dam_name_en,
    construction_year,
    "owner",
    operating_status_code,
    construction_type_code,
    height_m,
    length_m,
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN "owner",
    DROP COLUMN operating_status_code,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m,
    DROP COLUMN length_m;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS publicdamskml
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (publicdamskml.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'publicdamskml') 
    OR publicdamskml.data_source_id = duplicates.dups_publicdamskml;       
"""

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (uuid, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'publicdamskml', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    "owner" = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.owner ELSE cabd.owner END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabd.construction_year END,
    operating_status_code = CASE WHEN (cabd.operating_status_code IS NULL AND origin.operating_status_code IS NOT NULL) THEN origin.operating_status_code ELSE cabd.operating_status_code END,
    construction_type_code = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.construction_type_code ELSE cabd.construction_type_code END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.length_m ELSE cabd.length_m END        
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

UPDATE 
    {script.damAttributeTable} as cabd
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.dam_name_en_ds END,
    "owner_ds" = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source ELSE cabd.owner_ds END,
    construction_type_code_ds = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source ELSE cabd.construction_type_code_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabd.construction_year_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabd.height_m_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source ELSE cabd.length_m_ds END,
    operating_status_code_ds = CASE WHEN (cabd.operating_status_code IS NULL AND origin.operating_status_code IS NOT NULL) THEN origin.data_source ELSE cabd.operating_status_code_ds END,    
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_en_dsfid END,
    "owner_dsfid" = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source_id ELSE cabd.owner_dsfid END,
    construction_type_code_dsfid = CASE WHEN (cabd.construction_type_code IS NULL AND origin.construction_type_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.construction_type_code_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabd.construction_year_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.height_m_dsfid END,
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.length_m_dsfid END,
    operating_status_code_dsfid = CASE WHEN (cabd.operating_status_code IS NULL AND origin.operating_status_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.operating_status_code_dsfid END    
FROM
    {script.workingTable} AS origin    
WHERE
    origin.cabd_id = cabd.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)