import LOAD_dams_main as main

script = main.DamLoadingScript("nlprov")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET "owner" = owner_name;
UPDATE {script.tempTable} SET construction_year = year_built::numeric;
UPDATE {script.tempTable} SET operating_status_code =
    CASE
    WHEN dam_status = 'Abandoned' THEN 1
    WHEN dam_status = 'Active' THEN 2
    WHEN dam_status =  'Decommissioned' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_code =
    CASE 
    WHEN purpose_drinking IN ('Primary', 'Secondary') THEN 3
    WHEN purpose_industrial IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_hydro IN ('Primary', 'Secondary') THEN 2
    WHEN purpose_flood IN ('Primary', 'Secondary') THEN 4
    WHEN purpose_ice IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_forestry IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_unknown IN ('Primary', 'Secondary') THEN 10
    WHEN purpose_other IN ('Primary', 'Secondary') THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code =
    CASE 
    WHEN purpose_hydro = 'Primary' THEN 1
    WHEN purpose_hydro = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code =
    CASE
    WHEN purpose_drinking = 'Primary' THEN 1
    WHEN purpose_drinking = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code =
    CASE
    WHEN purpose_flood = 'Primary' THEN 1
    WHEN purpose_flood = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_other_code =
    CASE
    WHEN purpose_other = 'Primary' THEN 1
    WHEN purpose_other = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET construction_type_code =
    CASE 
    WHEN dam_type = 'CBD = Concrete Buttress Dam' THEN 2
    WHEN dam_type = 'RCCG = roller compacted concrete, gravity dam' THEN 4
    WHEN dam_type = 'CAGD = concrete arch gravity dam' THEN 4
    WHEN dam_type = 'RFTC = rock filled timber crib' THEN 8
    WHEN dam_type = 'XX = other type' THEN 10
    WHEN dam_type ILIKE 'CAD = Concrete Arch' THEN 1
    WHEN dam_type ILIKE 'EF = Earthfill' THEN 3
    WHEN dam_type ILIKE 'CGD = Concrete Gravity' THEN 4
    WHEN dam_type ILIKE 'RFCC = Rockfill, Central Core' THEN 6
    WHEN dam_type ILIKE 'CFRD = Concrete Faced Rockfill Dam' THEN 6
    WHEN dam_type IS NULL THEN 9
    ELSE NULL END;
UPDATE {script.tempTable} SET height_m = dim_max_height;
UPDATE {script.tempTable} SET data_source = 'nlprov_' || objectid;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid; --only if it has fid

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    "owner" varchar(512),
    construction_year numeric,
    operating_status_code int2,
    use_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_other_code int2,
    construction_type_code int2,
    height_m float4,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    "owner",
    construction_year,
    operating_status_code,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_other_code,
    construction_type_code,
    height_m,
    data_source
)
SELECT
    dam_name_en,
    "owner",
    construction_year,
    operating_status_code,
    use_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_other_code,
    construction_type_code,
    height_m,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN "owner",
    DROP COLUMN construction_year,
    DROP COLUMN operating_status_code,
    DROP COLUMN use_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_other_code,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS nlprov
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	nlprov.data_source = duplicates.data_source
	OR nlprov.data_source = duplicates.dups_nlprov;
"""

script.do_work(query)