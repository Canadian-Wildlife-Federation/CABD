import LOAD_dams_main as main

script = main.DamLoadingScript("grand")
    
mappingquery = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} RENAME COLUMN "comments" TO comments_orig;
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN nearest_municipality varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_depth_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.tempTable} ADD COLUMN avg_rate_of_discharge_ls float8;
ALTER TABLE {script.tempTable} ADD COLUMN degree_of_regulation_pc float4;
ALTER TABLE {script.tempTable} ADD COLUMN catchment_area_skm float8;
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_fish_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN lake_control_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN "comments" text;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET reservoir_name_en = res_name;
UPDATE {script.tempTable} SET waterbody_name_en = 
    CASE
    WHEN regexp_match(river, '.*River.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Creek.*') IS NOT NULL THEN river
    WHEN river IS NULL THEN NULL
    ELSE (river || ' River') END;
UPDATE {script.tempTable} SET nearest_municipality = near_city;
UPDATE {script.tempTable} SET construction_year = year;
UPDATE {script.tempTable} SET height_m = 
    CASE
    WHEN dam_heigt_m = -99 THEN NULL
    ELSE dam_heigt_m END;
UPDATE {script.tempTable} SET length_m = 
    CASE
    WHEN dam_len_m = -99 THEN NULL
    ELSE dam_len_m END;
UPDATE {script.tempTable} SET reservoir_area_skm = 
    CASE
    WHEN area_skm = -99 THEN NULL
    ELSE area_skm END;
UPDATE {script.tempTable} SET reservoir_depth_m = 
    CASE
    WHEN depth_m = -99 THEN NULL
    ELSE depth_m END;
UPDATE {script.tempTable} SET reservoir_present = 
    CASE
    WHEN area_skm = -99 THEN FALSE
    ELSE TRUE END;
UPDATE {script.tempTable} SET storage_capacity_mcm = cap_mcm;
UPDATE {script.tempTable} SET avg_rate_of_discharge_ls = dis_avg_ls;
UPDATE {script.tempTable} SET degree_of_regulation_pc = dor_pc;
UPDATE {script.tempTable} SET catchment_area_skm =
    CASE
    WHEN catch_skm = 0 THEN NULL
    ELSE catch_skm END;
UPDATE {script.tempTable} SET use_code =
    CASE 
    WHEN main_use = 'Irrigation' THEN 1
    WHEN main_use = 'Hydroelectricity' THEN 2
    WHEN main_use = 'Water supply' THEN 3
    WHEN main_use = 'Flood control' THEN 4
    WHEN main_use = 'Recreation' THEN 5
    WHEN main_use = 'Other' THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET use_irrigation_code =
    CASE 
    WHEN use_irri = 'Sec' THEN 3
    WHEN use_irri = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code =
    CASE 
    WHEN use_elec = 'Sec' THEN 3
    WHEN use_elec = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code =
    CASE 
    WHEN use_supp = 'Sec' THEN 3
    WHEN use_supp = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code =
    CASE 
    WHEN use_fcon = 'Sec' THEN 3
    WHEN use_fcon = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_recreation_code =
    CASE 
    WHEN use_recr = 'Sec' THEN 3
    WHEN use_recr = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_navigation_code =
    CASE 
    WHEN use_navi = 'Sec' THEN 3
    WHEN use_navi = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_fish_code =
    CASE 
    WHEN use_fish = 'Sec' THEN 3
    WHEN use_fish = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_pollution_code =
    CASE 
    WHEN use_pcon = 'Sec' THEN 3
    WHEN use_pcon = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET use_other_code =
    CASE 
    WHEN use_other = 'Sec' THEN 3
    WHEN use_other = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.tempTable} SET lake_control_code =
    CASE 
    WHEN lake_ctrl = 'Yes' THEN 1
    WHEN lake_ctrl = 'Maybe' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET "comments" = comments_orig;
UPDATE {script.tempTable} SET data_source = 'GRanD_Database_v1.3_' || grand_id;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    reservoir_name_en varchar(512),
    waterbody_name_en varchar(512),
    nearest_municipality varchar(512),
    construction_year numeric,
    height_m float4,
    length_m float4,
    reservoir_area_skm float4,
    reservoir_depth_m float4,
    reservoir_present bool,
    storage_capacity_mcm float8,
    avg_rate_of_discharge_ls float8,
    degree_of_regulation_pc float4,
    catchment_area_skm float8,
    use_code int2,
    use_irrigation_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_recreation_code int2,
    use_navigation_code int2,
    use_fish_code int2,
    use_pollution_code int2,
    use_other_code int2,
    lake_control_code int2,
    "comments" text,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    reservoir_name_en,
    waterbody_name_en,
    nearest_municipality,
    construction_year,
    height_m,
    length_m,
    reservoir_area_skm,
    reservoir_depth_m,
    reservoir_present,
    storage_capacity_mcm,
    avg_rate_of_discharge_ls,
    degree_of_regulation_pc,
    catchment_area_skm,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_fish_code,
    use_pollution_code,
    use_other_code,
    lake_control_code,
    "comments",
    data_source
)
SELECT
    dam_name_en,
    reservoir_name_en,
    waterbody_name_en,
    nearest_municipality,
    construction_year,
    height_m,
    length_m,
    reservoir_area_skm,
    reservoir_depth_m,
    reservoir_present,
    storage_capacity_mcm,
    avg_rate_of_discharge_ls,
    degree_of_regulation_pc,
    catchment_area_skm,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    use_fish_code,
    use_pollution_code,
    use_other_code,
    lake_control_code,
    "comments",
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN reservoir_name_en,
    DROP COLUMN waterbody_name_en,
    DROP COLUMN nearest_municipality,
    DROP COLUMN construction_year,
    DROP COLUMN height_m,
    DROP COLUMN length_m,
    DROP COLUMN reservoir_area_skm,
    DROP COLUMN reservoir_depth_m,
    DROP COLUMN reservoir_present,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN avg_rate_of_discharge_ls,
    DROP COLUMN degree_of_regulation_pc,
    DROP COLUMN catchment_area_skm,
    DROP COLUMN use_code,
    DROP COLUMN use_irrigation_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN use_fish_code,
    DROP COLUMN use_pollution_code,
    DROP COLUMN use_other_code,
    DROP COLUMN lake_control_code,
    DROP COLUMN "comments";

UPDATE
    {script.workingTable} AS grand
SET
    cabd_id = duplicates.cabd_dam_id
FROM
    {script.duplicatestable} AS duplicates
WHERE
    grand.data_source = duplicates.data_source
    OR grand.data_source = duplicates.dups_grand;
"""

script.do_work(mappingquery)


