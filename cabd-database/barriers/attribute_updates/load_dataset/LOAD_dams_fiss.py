import LOAD_dams_main as main

script = main.DamLoadingScript("fiss")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN capture_date date;
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

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
UPDATE {script.tempTable} SET capture_date = survey_date;
UPDATE {script.tempTable} SET waterbody_name_en = gazetted_name;
UPDATE {script.tempTable} SET data_source = 'FISS_Obstacles_' || fish_obstacle_point_id;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    height_m float4,
    length_m float4,
    capture_date date,
    waterbody_name_en varchar(512),
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    height_m,
    length_m,
    capture_date,
    waterbody_name_en,
    data_source
)
SELECT
    height_m,
    length_m,
    capture_date,
    waterbody_name_en,
    data_source
FROM {script.tempTable};

delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN height_m,
    DROP COLUMN length_m,
    DROP COLUMN capture_date,
    DROP COLUMN waterbody_name_en;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS fiss
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	fiss.data_source = duplicates.data_source
	OR fiss.data_source = duplicates.dups_fiss;
"""

script.do_work(query)