import LOAD_dams_main as main

script = main.DamLoadingScript("skhydro")

query = f"""

--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = feature_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN {col1} varchar(512);

UPDATE {script.tempTable} SET {col1} = name_1_en;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    {col1} varchar(512),
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    cabd_id,
    {col1},
    data_source,
    data_source_id
)
SELECT
    cabd_id,
    {col1},
    data_source,
    data_source_id
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN {col1};

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS skhydro
SET
	cabd_id = duplicates.cabd_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	(skhydro.data_source_id = duplicates.data_source_id AND duplicates.data_source = 'skhydro') 
	OR skhydro.data_source_id = duplicates.dups_skhydro;    
"""


#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (id, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'skhydro', now(), null, null, 'Data update - ' || now());

--update existing features
UPDATE 
    {script.damAttributeTable} AS cabdsource
SET    
    {col1}_ds = CASE WHEN (cabd.{col1} IS NULL AND origin.{col1} IS NOT NULL) THEN origin.data_source ELSE cabdsource.{col1}_ds END,  
    
    {col1}_fr_dsfid = CASE WHEN (cabd.{col1} IS NULL AND origin.{col1} IS NOT NULL) THEN origin.data_source_id ELSE cabdsource.{col1}dsfid END    
FROM
    {script.damTable} AS cabd,
    {script.workingTable} AS origin
WHERE
    cabdsource.cabd_id = origin.cabd_id and cabd.cabd_id = cabdsource.cabd_id;

UPDATE
    {script.damTable} AS cabd
SET
    {col1} = CASE WHEN (cabd.{col1} IS NULL AND origin.{col1} IS NOT NULL) THEN origin.{col1} ELSE cabd.{col1} END
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(query, prodquery)