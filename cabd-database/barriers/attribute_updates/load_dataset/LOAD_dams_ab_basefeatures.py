import LOAD_dams_main as main

script = main.DamLoadingScript("abbasefeatures")
    
mappingquery = f"""

--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN capture_date_orig date;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = name;
UPDATE {script.tempTable} SET capture_date = capture_date_orig;
UPDATE {script.tempTable} SET data_source = 'ab_basefeatures_' || bf_id;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    capture_date date,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    capture_date,
    data_source
)
SELECT
    dam_name_en,
    capture_date,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN capture_date;

--Finding CABD IDs...
UPDATE
    {script.workingTable} AS ab_basefeatures
SET
    cabd_id = duplicates.cabd_dam_id
FROM
    {script.duplicatestable} AS duplicates
WHERE
    ab_basefeatures.data_source = duplicates.data_source
    OR ab_basefeatures.data_source = duplicates.dups_ab_basefeatures;       
"""

script.do_work(mappingquery)
