import LOAD_dams_main as main

script = main.DamLoadingScript("fishwerks")

query = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = barrier_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);

UPDATE {script.tempTable} SET waterbody_name_en = 'Lake ' || lake;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    waterbody_name_en varchar(512),
    duplicate_id varchar,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    waterbody_name_en,
    duplicate_id, 
    data_source, 
    data_source_id
)
SELECT
    waterbody_name_en,
    'fishwerks_' || data_source_id,
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN waterbody_name_en;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS fishwerks
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	fishwerks.duplicate_id = duplicates.data_source
	OR fishwerks.duplicate_id = duplicates.dups_fishwerks;
"""

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (uuid, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'fishwerks', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE
    {script.damTable} AS cabd
SET
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.waterbody_name_en ELSE cabd.waterbody_name_en END
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

UPDATE 
    {script.damAttributeTable} as cabd
SET    
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.waterbody_name_en_ds END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.waterbody_name_en_dsfid END
FROM
    {script.workingTable} AS origin    
WHERE
    origin.cabd_id = cabd.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)