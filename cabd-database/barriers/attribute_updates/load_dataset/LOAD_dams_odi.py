import LOAD_dams_main as main

script = main.DamLoadingScript("odi")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;
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
UPDATE {script.tempTable} SET data_source = 'ODI_' || ogf_id;
UPDATE {script.tempTable} SET "comments" = GENERAL_COMMENTS;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    ownership_type_code int2,
    data_source text PRIMARY KEY,
    comments text
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    "owner",
    ownership_type_code,
    data_source,
    comments
)
SELECT
    dam_name_en,
    "owner",
    ownership_type_code,
    data_source,
    comments
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
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	odi.data_source = duplicates.data_source
	OR odi.data_source = duplicates.dups_odi;
    """

script.do_work(query)