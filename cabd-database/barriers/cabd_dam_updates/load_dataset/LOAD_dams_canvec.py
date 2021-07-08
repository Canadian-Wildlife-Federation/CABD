import LOAD_dams_main as main

script = main.DamLoadingScript("canvec")

mappingquery = f"""

--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = feature_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_fr varchar(512);

UPDATE {script.tempTable} SET dam_name_en = name_1_en;
UPDATE {script.tempTable} SET dam_name_fr = name_1_fr;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    dam_name_fr varchar(512),
    duplicate_id varchar,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    dam_name_fr,
    duplicate_id, 
    data_source, 
    data_source_id
)
SELECT
    dam_name_en,
    dam_name_fr,
    'canvec_' || data_source_id, 
    data_source, 
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN dam_name_fr;
    
    """


#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (uuid, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'canvec', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    dam_name_fr = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.dam_name_fr ELSE cabd.dam_name_fr END
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

UPDATE 
    {script.damAttributeTable} as cabd
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.dam_name_en_ds END,   
    dam_name_fr_ds = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.data_source ELSE cabd.dam_name_fr_ds END,   
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_en_dsfid END,
    dam_name_fr_dsfid = CASE WHEN (cabd.dam_name_fr IS NULL AND origin.dam_name_fr IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_fr_dsfid END    
FROM
    {script.workingTable} AS origin    
WHERE
    origin.cabd_id = cabd.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(mappingquery, prodquery)