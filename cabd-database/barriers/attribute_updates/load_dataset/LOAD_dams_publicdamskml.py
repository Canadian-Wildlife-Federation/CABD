import LOAD_dams_main as main

script = main.DamLoadingScript("publicdamskml")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} RENAME COLUMN height_m TO height_m_orig;
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET construction_year = commissioned_year::numeric;
UPDATE {script.tempTable} SET "owner" = dam_owner;
UPDATE {script.tempTable} SET operating_status_code =
    CASE 
    WHEN dam_operation_code = 'Abandoned' THEN 1
    WHEN dam_operation_code = 'Active' THEN 2
    WHEN dam_operation_code IN ('Decommissioned', 'Removed') THEN 3
    WHEN dam_operation_code = 'Deactivated' THEN 4
    WHEN dam_operation_code IN ('Application', 'Not Constructed', 'Breached (Failed)') THEN 5
    ELSE NULL END;
UPDATE {script.tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'Concrete Arch' THEN 1
    WHEN dam_type = 'Buttress' THEN 2
    WHEN dam_type = 'Earthfill' THEN 3
    WHEN dam_type = 'Concrete Gravity' THEN 4
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Steel' THEN 7
    WHEN dam_type = 'Log Crib' THEN 8
    WHEN dam_type IN ('Concrete', 'Dugout/Pond', 'Other') THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET height_m = height_m_orig::float4;
UPDATE {script.tempTable} SET length_m = crest_length_m::float4;
UPDATE {script.tempTable} SET data_source = 'Public_Dams_KML_' || objectid;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    construction_year numeric,
    "owner" varchar(512),
    operating_status_code int2,
    construction_type_code int2,
    height_m float4,
    length_m float4,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    construction_year,
    "owner",
    operating_status_code,
    construction_type_code,
    height_m,
    length_m,
    data_source
)
SELECT
    dam_name_en,
    construction_year,
    "owner",
    operating_status_code,
    construction_type_code,
    height_m,
    length_m,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN "owner",
    DROP COLUMN operating_status_code,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m,
    DROP COLUMN length_m;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS publicdamskml
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	publicdamskml.data_source = duplicates.data_source
	OR publicdamskml.data_source = duplicates.dups_publicdamskml;
"""

script.do_work(query)