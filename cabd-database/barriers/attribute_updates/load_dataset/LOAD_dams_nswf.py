import LOAD_dams_main as main

script = main.DamLoadingScript("nswf")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET waterbody_name_en = rivname_1;
UPDATE {script.tempTable} SET data_source = 'nswf_' || shape_fid;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    waterbody_name_en varchar(512),
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    waterbody_name_en,
    data_source
)
SELECT
    waterbody_name_en,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN waterbody_name_en;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS nswf
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	nswf.data_source = duplicates.data_source
	OR nswf.data_source = duplicates.dups_nswf;
"""

script.do_work(query)