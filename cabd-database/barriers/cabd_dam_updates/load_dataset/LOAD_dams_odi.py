import LOAD_dams_main as main

script = main.DamLoadingScript("odi")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = dam_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN "comments" text;

UPDATE {script.tempTable} SET dam_name_en = DAM_NAME;
UPDATE {script.tempTable} SET owner = DAM_OWNERSHIP;
UPDATE {script.tempTable} SET ownership_type_code = 
    CASE
    WHEN DAM_OWNERSHIP = 'Conservation Authority' THEN 1
    WHEN DAM_OWNERSHIP = 'Federal' THEN 2
    WHEN DAM_OWNERSHIP = 'Municipal' THEN 3
    WHEN DAM_OWNERSHIP = 'Private' THEN 4
    WHEN DAM_OWNERSHIP = 'Provincial' THEN 5
    WHEN DAM_OWNERSHIP = 'Ontario Power Generation' THEN 5
    ELSE NULL END;

UPDATE {script.tempTable} SET "comments" = GENERAL_COMMENTS;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} DROP COLUMN fid;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);

--create workingTable and insert mapped attributes
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    ownership_type_code int2,
    comments text,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    "owner",
    ownership_type_code,
    comments,
    data_source,
    data_source_id    
)
SELECT
    dam_name_en,
    "owner",
    ownership_type_code,
    comments,
    data_source,
    data_source_id    
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "owner",
    DROP COLUMN ownership_type_code,
    DROP COLUMN comments;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS odi
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (odi.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'odi') 
    OR odi.data_source_id = duplicates.dups_odi;       
    """

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'odi', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET 
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.dam_name_en_ds END,
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    owner_ds = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source ELSE cabdsource.owner_ds END,
    owner_dsfid = CASE WHEN (cabd.owner IS NULL AND origin.owner IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.owner_dsfid END,
    ownership_type_code_ds = CASE WHEN (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.ownership_type_code_ds END,
    ownership_type_code_dsfid = CASE WHEN (cabd.ownership_type_code IS NULL AND origin.ownership_type_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.ownership_type_code_dsfid END,
    comments_ds = CASE WHEN (cabd.comments IS NULL AND origin.comments IS NOT NULL) THEN origin.data_source ELSE cabdsource.comments_ds END,
    comments_dsfid = CASE WHEN (cabd.comments IS NULL AND origin.comments IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.comments_dsfid END
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
    comments = CASE WHEN (cabd.comments IS NULL AND origin.comments IS NOT NULL) THEN origin.comments ELSE cabd.comments END
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)