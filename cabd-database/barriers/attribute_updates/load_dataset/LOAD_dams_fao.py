import LOAD_dams_main as main

script = main.DamLoadingScript("fao")
    
query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN nearest_municipality varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.tempTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN comments text;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = name_of_dam;
UPDATE {script.tempTable} SET nearest_municipality = nearest_city;
UPDATE {script.tempTable} SET waterbody_name_en =
    CASE
    WHEN regexp_match(river, '.*River.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Creek.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Falls.*') IS NOT NULL THEN river
    WHEN river IS NULL THEN NULL
    ELSE (river || ' River') END;
UPDATE {script.tempTable} SET construction_year = 
    CASE
    WHEN completed_operational_since = 'Incomplete?' THEN NULL
    ELSE completed_operational_since::numeric END;
UPDATE {script.tempTable} SET height_m = dam_height_m;
UPDATE {script.tempTable} SET storage_capacity_mcm = reservoir_capacity_million_m3;
UPDATE {script.tempTable} SET reservoir_area_skm = reservoir_area_km2;
UPDATE {script.tempTable} SET use_irrigation_code = 
    CASE
    WHEN irrigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code = 
    CASE
    WHEN water_supply = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code = 
    CASE
    WHEN flood_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code = 
    CASE
    WHEN hydroelectricity_mw = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_navigation_code = 
    CASE
    WHEN navigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_recreation_code = 
    CASE
    WHEN recreation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_pollution_code = 
    CASE
    WHEN pollution_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_other_code =
    CASE
    WHEN other = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET "comments" = comments_orig;
UPDATE {script.tempTable} SET data_source = 'fao_' || id_fao;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    nearest_municipality varchar(512),
    waterbody_name_en varchar(512),
    construction_year numeric,
    height_m float4,
    storage_capacity_mcm float8,
    reservoir_area_skm float4,
    use_irrigation_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_electricity_code int2,
    use_navigation_code int2,
    use_recreation_code int2,
    use_pollution_code int2,
    use_other_code int2,
    comments text,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    nearest_municipality,
    waterbody_name_en,
    construction_year,
    height_m,
    storage_capacity_mcm,
    reservoir_area_skm,
    use_irrigation_code,
    use_supply_code,
    use_floodcontrol_code,
    use_electricity_code,
    use_navigation_code,
    use_recreation_code,
    use_pollution_code,
    use_other_code,
    "comments",
    data_source
)
SELECT
    dam_name_en,
    nearest_municipality,
    waterbody_name_en,
    construction_year,
    height_m,
    storage_capacity_mcm,
    reservoir_area_skm,
    use_irrigation_code,
    use_supply_code,
    use_floodcontrol_code,
    use_electricity_code,
    use_navigation_code,
    use_recreation_code,
    use_pollution_code,
    use_other_code,
    "comments",
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN nearest_municipality,
    DROP COLUMN waterbody_name_en,
    DROP COLUMN construction_year,
    DROP COLUMN height_m,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN reservoir_area_skm,
    DROP COLUMN use_irrigation_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_pollution_code,
    DROP COLUMN use_other_code,
    DROP COLUMN "comments";

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS fao
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatetable} AS duplicates
WHERE
	fao.data_source = duplicates.data_source
	OR fao.data_source = duplicates.dups_fao;
"""

script.do_work(query)