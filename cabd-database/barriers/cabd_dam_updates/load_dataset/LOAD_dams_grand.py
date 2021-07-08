import LOAD_dams_main as main

script = main.DamLoadingScript("grand")
    
mappingquery = f"""
--data source fields
ALTER TABLE {script.tempTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.tempTable} ADD COLUMN data_source_id varchar;
UPDATE {script.tempTable} SET data_source_id = grand_id;
UPDATE {script.tempTable} SET data_source = '{script.dsUuid}';

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

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source_id);
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
    duplicate_id varchar,
    data_source uuid not null,
    data_source_id varchar PRIMARY KEY
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
    duplicate_id,
    data_source,
    data_source_id
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
    'GRanD_Database_v1.3_' || data_source_id,
    data_source,
    data_source_id
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
    grand.duplicate_id = duplicates.data_source
    OR grand.duplicate_id = duplicates.dups_grand;
"""

#this query updates the production data tables
#with the data from the working tables
prodquery = f"""

--create new data source record
INSERT INTO cabd.data_source (uuid, name, version_date, version_number, source, comments)
VALUES('{script.dsUuid}', 'GRanD Database v1.3', now(), null, null, 'Data update - ' || now());

--update existing features 
UPDATE
    {script.damTable} AS cabd
SET
    dam_name_en = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.dam_name_en ELSE cabd.dam_name_en END,
    reservoir_name_en = CASE WHEN (cabd.reservoir_name_en IS NULL AND origin.reservoir_name_en IS NOT NULL) THEN origin.reservoir_name_en ELSE cabd.reservoir_name_en END,
    waterbody_name_en = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.waterbody_name_en ELSE cabd.waterbody_name_en END,
    nearest_municipality = CASE WHEN (cabd.nearest_municipality IS NULL AND origin.nearest_municipality IS NOT NULL) THEN origin.nearest_municipality ELSE cabd.nearest_municipality END,
    construction_year = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.construction_year ELSE cabd.construction_year END,
    height_m = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.height_m ELSE cabd.height_m END,         
    length_m = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.length_m ELSE cabd.length_m END,         
    reservoir_area_skm = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.reservoir_area_skm ELSE cabd.reservoir_area_skm END,
    reservoir_depth_m = CASE WHEN (cabd.reservoir_depth_m IS NULL AND origin.reservoir_depth_m IS NOT NULL) THEN origin.reservoir_depth_m ELSE cabd.reservoir_depth_m END,
    reservoir_present = CASE WHEN (cabd.reservoir_present IS NULL AND origin.reservoir_present IS NOT NULL) THEN origin.reservoir_present ELSE cabd.reservoir_present END,
    storage_capacity_mcm = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.storage_capacity_mcm ELSE cabd.storage_capacity_mcm END,
    avg_rate_of_discharge_ls = CASE WHEN (cabd.avg_rate_of_discharge_ls IS NULL AND origin.avg_rate_of_discharge_ls IS NOT NULL) THEN origin.avg_rate_of_discharge_ls ELSE cabd.avg_rate_of_discharge_ls END,
    degree_of_regulation_pc = CASE WHEN (cabd.degree_of_regulation_pc IS NULL AND origin.degree_of_regulation_pc IS NOT NULL) THEN origin.degree_of_regulation_pc ELSE cabd.degree_of_regulation_pc END,
    catchment_area_skm = CASE WHEN (cabd.catchment_area_skm IS NULL AND origin.catchment_area_skm IS NOT NULL) THEN origin.catchment_area_skm ELSE cabd.catchment_area_skm END,
    use_code = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.use_code ELSE cabd.use_code END,         
    use_irrigation_code = CASE WHEN (cabd.use_irrigation_code IS NULL AND origin.use_irrigation_code IS NOT NULL) THEN origin.use_irrigation_code ELSE cabd.use_irrigation_code END,
    use_electricity_code = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.use_electricity_code ELSE cabd.use_electricity_code END,
    use_supply_code = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.use_supply_code ELSE cabd.use_supply_code END,
    use_floodcontrol_code = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.use_floodcontrol_code ELSE cabd.use_floodcontrol_code END,
    use_recreation_code = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.use_recreation_code ELSE cabd.use_recreation_code END,
    use_navigation_code = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.use_navigation_code ELSE cabd.use_navigation_code END,
    use_fish_code = CASE WHEN (cabd.use_fish_code IS NULL AND origin.use_fish_code IS NOT NULL) THEN origin.use_fish_code ELSE cabd.use_fish_code END,
    use_pollution_code = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.use_pollution_code ELSE cabd.use_pollution_code END,
    use_other_code = CASE WHEN (cabd.use_other_code IS NULL AND origin.use_other_code IS NOT NULL) THEN origin.use_other_code ELSE cabd.use_other_code END,
    lake_control_code = CASE WHEN (cabd.lake_control_code IS NULL AND origin.lake_control_code IS NOT NULL) THEN origin.lake_control_code ELSE cabd.lake_control_code END,
    "comments" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin."comments" ELSE cabd."comments" END   
FROM
    {script.workingTable} AS origin
WHERE
    cabd.cabd_id = origin.cabd_id;

UPDATE 
    {script.damAttributeTable} as cabd
SET    
    dam_name_en_ds = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.dam_name_en_ds END,
    reservoir_name_en_ds = CASE WHEN (cabd.reservoir_name_en IS NULL AND origin.reservoir_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_name_en_ds END,
    waterbody_name_en_ds = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source ELSE cabd.waterbody_name_en_ds END,
    nearest_municipality_ds = CASE WHEN (cabd.nearest_municipality IS NULL AND origin.nearest_municipality IS NOT NULL) THEN origin.data_source ELSE cabd.nearest_municipality_ds END,
    construction_year_ds = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source ELSE cabd.construction_year_ds END,
    height_m_ds = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source ELSE cabd.height_m_ds END,         
    length_m_ds = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source ELSE cabd.length_m_ds END,         
    reservoir_area_skm_ds = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_area_skm_ds END,
    reservoir_depth_m_ds = CASE WHEN (cabd.reservoir_depth_m IS NULL AND origin.reservoir_depth_m IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_depth_m_ds END,
    reservoir_present_ds = CASE WHEN (cabd.reservoir_present IS NULL AND origin.reservoir_present IS NOT NULL) THEN origin.data_source ELSE cabd.reservoir_present_ds END,
    storage_capacity_mcm_ds = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source ELSE cabd.storage_capacity_mcm_ds END,
    avg_rate_of_discharge_ls_ds = CASE WHEN (cabd.avg_rate_of_discharge_ls IS NULL AND origin.avg_rate_of_discharge_ls IS NOT NULL) THEN origin.data_source ELSE cabd.avg_rate_of_discharge_ls_ds END,
    degree_of_regulation_pc_ds = CASE WHEN (cabd.degree_of_regulation_pc IS NULL AND origin.degree_of_regulation_pc IS NOT NULL) THEN origin.data_source ELSE cabd.degree_of_regulation_pc_ds END,
    catchment_area_skm_ds = CASE WHEN (cabd.catchment_area_skm IS NULL AND origin.catchment_area_skm IS NOT NULL) THEN origin.data_source ELSE cabd.catchment_area_skm_ds END,
    use_code_ds = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_code_ds END,         
    use_irrigation_code_ds = CASE WHEN (cabd.use_irrigation_code IS NULL AND origin.use_irrigation_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_irrigation_code_ds END,
    use_electricity_code_ds = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_electricity_code_ds END,
    use_supply_code_ds = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_supply_code_ds END,
    use_floodcontrol_code_ds = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_floodcontrol_code_ds END,
    use_recreation_code_ds = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_recreation_code_ds END,
    use_navigation_code_ds = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_navigation_code_ds END,
    use_fish_code_ds = CASE WHEN (cabd.use_fish_code IS NULL AND origin.use_fish_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_fish_code_ds END,
    use_pollution_code_ds = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_pollution_code_ds END,
    use_other_code_ds = CASE WHEN (cabd.use_other_code IS NULL AND origin.use_other_code IS NOT NULL) THEN origin.data_source ELSE cabd.use_other_code_ds END,
    lake_control_code_ds = CASE WHEN (cabd.lake_control_code IS NULL AND origin.lake_control_code IS NOT NULL) THEN origin.data_source ELSE cabd.lake_control_code_ds END,
    "comments_ds" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin.data_source ELSE cabd.comments_ds END,
    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_en_dsfid END    
    dam_name_en_dsfid = CASE WHEN (cabd.dam_name_en IS NULL AND origin.dam_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.dam_name_en_dsfid END,
    reservoir_name_en_dsfid = CASE WHEN (cabd.reservoir_name_en IS NULL AND origin.reservoir_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_name_en_dsfid END,
    waterbody_name_en_dsfid = CASE WHEN (cabd.waterbody_name_en IS NULL AND origin.waterbody_name_en IS NOT NULL) THEN origin.data_source_id ELSE cabd.waterbody_name_en_dsfid END,
    nearest_municipality_dsfid = CASE WHEN (cabd.nearest_municipality IS NULL AND origin.nearest_municipality IS NOT NULL) THEN origin.data_source_id ELSE cabd.nearest_municipality_dsfid END,
    construction_year_dsfid = CASE WHEN (cabd.construction_year IS NULL AND origin.construction_year IS NOT NULL) THEN origin.data_source_id ELSE cabd.construction_year_dsfid END,
    height_m_dsfid = CASE WHEN (cabd.height_m IS NULL AND origin.height_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.height_m_dsfid END,         
    length_m_dsfid = CASE WHEN (cabd.length_m IS NULL AND origin.length_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.length_m_dsfid END,         
    reservoir_area_skm_dsfid = CASE WHEN (cabd.reservoir_area_skm IS NULL AND origin.reservoir_area_skm IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_area_skm_dsfid END,
    reservoir_depth_m_dsfid = CASE WHEN (cabd.reservoir_depth_m IS NULL AND origin.reservoir_depth_m IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_depth_m_dsfid END,
    reservoir_present_dsfid = CASE WHEN (cabd.reservoir_present IS NULL AND origin.reservoir_present IS NOT NULL) THEN origin.data_source_id ELSE cabd.reservoir_present_dsfid END,
    storage_capacity_mcm_dsfid = CASE WHEN (cabd.storage_capacity_mcm IS NULL AND origin.storage_capacity_mcm IS NOT NULL) THEN origin.data_source_id ELSE cabd.storage_capacity_mcm_dsfid END,
    avg_rate_of_discharge_ls_dsfid = CASE WHEN (cabd.avg_rate_of_discharge_ls IS NULL AND origin.avg_rate_of_discharge_ls IS NOT NULL) THEN origin.data_source_id ELSE cabd.avg_rate_of_discharge_ls_dsfid END,
    degree_of_regulation_pc_dsfid = CASE WHEN (cabd.degree_of_regulation_pc IS NULL AND origin.degree_of_regulation_pc IS NOT NULL) THEN origin.data_source_id ELSE cabd.degree_of_regulation_pc_dsfid END,
    catchment_area_skm_dsfid = CASE WHEN (cabd.catchment_area_skm IS NULL AND origin.catchment_area_skm IS NOT NULL) THEN origin.data_source_id ELSE cabd.catchment_area_skm_dsfid END,
    use_code_dsfid = CASE WHEN (cabd.use_code IS NULL AND origin.use_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_code_dsfid END,         
    use_irrigation_code_dsfid = CASE WHEN (cabd.use_irrigation_code IS NULL AND origin.use_irrigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_irrigation_code_dsfid END,
    use_electricity_code_dsfid = CASE WHEN (cabd.use_electricity_code IS NULL AND origin.use_electricity_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_electricity_code_dsfid END,
    use_supply_code_dsfid = CASE WHEN (cabd.use_supply_code IS NULL AND origin.use_supply_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_supply_code_dsfid END,
    use_floodcontrol_code_dsfid = CASE WHEN (cabd.use_floodcontrol_code IS NULL AND origin.use_floodcontrol_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_floodcontrol_code_dsfid END,
    use_recreation_code_dsfid = CASE WHEN (cabd.use_recreation_code IS NULL AND origin.use_recreation_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_recreation_code_dsfid END,
    use_navigation_code_dsfid = CASE WHEN (cabd.use_navigation_code IS NULL AND origin.use_navigation_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_navigation_code_dsfid END,
    use_fish_code_dsfid = CASE WHEN (cabd.use_fish_code IS NULL AND origin.use_fish_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_fish_code_dsfid END,
    use_pollution_code_dsfid = CASE WHEN (cabd.use_pollution_code IS NULL AND origin.use_pollution_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_pollution_code_dsfid END,
    use_other_code_dsfid = CASE WHEN (cabd.use_other_code IS NULL AND origin.use_other_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.use_other_code_dsfid END,
    lake_control_code_dsfid = CASE WHEN (cabd.lake_control_code IS NULL AND origin.lake_control_code IS NOT NULL) THEN origin.data_source_id ELSE cabd.lake_control_code_dsfid END,
    "comments_dsfid" = CASE WHEN (cabd."comments" IS NULL AND origin."comments" IS NOT NULL) THEN origin.data_source_id ELSE cabd.comments_dsfid END,    
FROM
    {script.workingTable} AS origin    
WHERE
    origin.cabd_id = cabd.cabd_id;

--TODO: manage new features & duplicates table with new features
    
"""

script.do_work(mappingquery, prodquery)


