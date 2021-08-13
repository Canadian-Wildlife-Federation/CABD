import LOAD_dams_main as main

script = main.DamLoadingScript("nhn")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = nid;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;

UPDATE {script.tempTable} SET dam_name_en = name1;
UPDATE {script.tempTable} SET operating_status_code = 
    CASE
    WHEN manmadestatus = -1 then 5
    WHEN manmadestatus = 1 THEN 2
    ELSE NULL END;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    operating_status_code int2,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    operating_status_code,
    data_source,
    data_source_id
)
SELECT
    dam_name_en,
    operating_status_code,
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN operating_status_code;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS nhn
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (nhn.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'nhn') 
    OR nhn.data_source_id = duplicates.dups_nhn;       
"""

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'nhn', now(), null, null, 'Data update - ' || now());

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.dam_name_en_ds END,
    operating_status_code_ds = CASE WHEN (cabd.operating_status_code IS NULL AND origin.operating_status_code IS NOT NULL) THEN origin.data_source ELSE cabdsource.operating_status_code_ds END,    
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.dam_name_en_dsfid END,
    operating_status_code_dsfid = CASE WHEN (cabd.operating_status_code IS NULL AND origin.operating_status_code IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.operating_status_code_dsfid END
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS origin
WHERE
    cabdsource.cabd_id = origin.cabd_id and cabd.cabd_id = cabdsource.cabd_id;


UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    operating_status_code = CASE WHEN (cabd.operating_status_code IS NULL AND origin.operating_status_code IS NOT NULL) THEN origin.operating_status_code ELSE cabd.operating_status_code END
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)