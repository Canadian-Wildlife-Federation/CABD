import LOAD_dams_main as main

script = main.DamLoadingScript("ohn")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN comments text;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = 
    CASE
    WHEN regexp_match(general_comments, '(?: - MNR)') IS NOT NULL THEN general_comments
    ELSE NULL END;
UPDATE {script.tempTable} SET comments = general_comments;
UPDATE {script.tempTable} SET data_source = 'ohn_' || ogf_id;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    comments text,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    "comments",
    data_source
)
SELECT
    dam_name_en,
    "comments",
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "comments";

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS ohn
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	ohn.data_source = duplicates.data_source
	OR ohn.data_source = duplicates.dups_ohn;
"""

script.do_work(query)