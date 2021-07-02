import LOAD_dams_main as main

script = main.DamLoadingScript("mbhb")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN capture_date date;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = 
    CASE 
    WHEN name_1 IS NOT NULL then name_1
    WHEN name_1 IS NULL then local_name
    ELSE NULL END;
UPDATE {script.tempTable} SET operating_status_code = 
    CASE
    WHEN manmade_status = -1 then 5
    WHEN manmade_status = 1 then 2
    WHEN manmade_status = 2 then 1
    ELSE NULL END;
UPDATE {script.tempTable} SET capture_date =
    CASE
    WHEN length(validity_date) = 8 THEN TO_DATE(validity_date, 'YYYYMMDD')
    ELSE NULL END;
UPDATE {script.tempTable} SET data_source = 'nbhn_' || nid;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    operating_status_code int2,
    capture_date date,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    operating_status_code,
    capture_date,
    data_source
)
SELECT
    dam_name_en,
    operating_status_code,
    capture_date,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN operating_status_code,
    DROP COLUMN capture_date;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS nbhn
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	nbhn.data_source = duplicates.data_source
	OR nbhn.data_source = duplicates.dups_nbhn;
"""

script.do_work(query)