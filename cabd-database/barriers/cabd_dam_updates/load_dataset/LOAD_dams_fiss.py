import LOAD_dams_main as main

script = main.DamLoadingScript("fiss")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = fish_obstacle_point_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);

UPDATE {script.tempTable} SET height_m = 
    CASE
    WHEN height > 0 AND height <> 9999 THEN height
    WHEN height = 9999 THEN NULL
    ELSE NULL END;
UPDATE {script.tempTable} SET length_m = 
    CASE
    WHEN length > 0 AND length <> 9999 THEN length
    WHEN length = 9999 THEN NULL
    ELSE NULL END;
UPDATE {script.tempTable} SET waterbody_name_en = gazetted_name;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    height_m float4,
    length_m float4,
    waterbody_name_en varchar(512),
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    height_m,
    length_m,
    waterbody_name_en,
    data_source,
    data_source_id
)
SELECT
    height_m,
    length_m,
    waterbody_name_en,
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN height_m,
    DROP COLUMN length_m,
    DROP COLUMN waterbody_name_en;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS fiss
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
    (fiss.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'fiss') 
    OR fiss.data_source_id = duplicates.dups_fiss;       
"""

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'fiss', now(), null, null, 'Data update - ' || now());

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabdsource.height_m_ds END,
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source ELSE cabdsource.length_m_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source ELSE cabdsource.waterbody_name_en_ds END,
 
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.height_m_dsfid END,
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.length_m_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.waterbody_name_en_dsfid END    
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS origin
WHERE
    cabdsource.cabd_id = origin.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabd.height_m END,
    length_m = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.length_m ELSE cabd.length_m END,        
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.waterbody_name_en ELSE cabd.waterbody_name_en END        
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)