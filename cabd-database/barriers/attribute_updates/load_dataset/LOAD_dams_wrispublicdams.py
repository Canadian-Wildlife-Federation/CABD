import LOAD_dams_main as main

script = main.DamLoadingScript("sridpublicdams")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN function_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = 
    CASE
    WHEN dam_name IS NOT NULL THEN dam_name
    WHEN dam_name IS NULL AND alternate_dam_name IS NOT NULL THEN alternate_dam_name
    ELSE NULL END;
UPDATE {script.tempTable} SET "owner" = dam_owner; 
UPDATE {script.tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'Concrete–arch' THEN 1
    WHEN dam_type = 'Concrete–slab/buttress' THEN 2
    WHEN dam_type IN ('Earthfill', 'Embankment–homogenous', 'Embankment–unknown', 'Embankment–zoned') THEN 3
    WHEN dam_type = 'Concrete–gravity' THEN 4
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Steel' THEN 7
    WHEN dam_type = 'Log crib' THEN 8
    WHEN dam_type = 'Unknown' THEN 9
    WHEN dam_type IN ('Combination', 'Concrete', 'Other') THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET function_code = 
    CASE
    WHEN dam_function = 'SADDLE' THEN 6
    ELSE NULL END; 
UPDATE {script.tempTable} SET construction_year = commissioned_year::numeric; 
UPDATE {script.tempTable} SET height_m = dam_height_m; 
UPDATE {script.tempTable} SET length_m = crest_length_m; 
UPDATE {script.tempTable} SET operating_status_code =
    CASE 
    WHEN dam_operation_code = 'Abandoned' THEN 1
    WHEN dam_operation_code = 'Active' THEN 2
    WHEN dam_operation_code IN ('Decommissioned', 'Removed') THEN 3
    WHEN dam_operation_code = 'Deactivated' THEN 4
    WHEN dam_operation_code IN ('Application', 'Not Constructed', 'Breached (Failed)') THEN 5
    ELSE NULL END; 
UPDATE {script.tempTable} SET data_source = 'WRIS_Public_Dams_' || objectid;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    construction_type_code int2,
    function_code int2,
    construction_year numeric,
    height_m float4,
    length_m float4,
    operating_status_code int2,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    "owner",
    construction_type_code,
    function_code,
    construction_year,
    height_m,
    length_m,
    operating_status_code,
    data_source
)
SELECT
    dam_name_en,
    "owner",
    construction_type_code,
    function_code,
    construction_year,
    height_m,
    length_m,
    operating_status_code,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "owner",
    DROP COLUMN construction_type_code,
    DROP COLUMN function_code,
    DROP COLUMN construction_year,
    DROP COLUMN height_m,
    DROP COLUMN length_m,
    DROP COLUMN operating_status_code;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS wrispublicdams
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	wrispublicdams.data_source = duplicates.data_source
	OR wrispublicdams.data_source = duplicates.dups_wrispublicdams;
"""

script.do_work(query)