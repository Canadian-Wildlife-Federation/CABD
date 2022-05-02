import LOAD_main as main

script = main.LoadingScript("gdw_grand")

query = f"""
--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = grand_id;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        dam_name,
        res_name,
        river,
        year,
        dam_height_m,
        dam_len_m,
        area_skm,
        depth_m,
        cap_mcm,
        dis_avg_ls,
        dor_pc,
        catch_skm,
        main_use,
        use_irri,
        use_elec,
        use_supp,
        use_fcon,
        use_recr,
        use_navi,
        use_fish,
        use_pcon,
        use_other,
        lake_ctrl,
        comments,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_depth_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN avg_rate_of_discharge_ls float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN degree_of_regulation_pc float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN catchment_area_skm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_fish_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN lake_control_code int2;
ALTER TABLE {script.damWorkingTable} RENAME COLUMN "comments" TO comments_orig;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.damWorkingTable} SET dam_name_en = dam_name;
UPDATE {script.damWorkingTable} SET reservoir_name_en = res_name;
UPDATE {script.damWorkingTable} SET waterbody_name_en = 
    CASE
    WHEN regexp_match(river, '.*River.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Creek.*') IS NOT NULL THEN river
    WHEN river IS NULL THEN NULL
    ELSE (river || ' River') END;
UPDATE {script.damWorkingTable} SET construction_year = 
    CASE
    WHEN year = -99 THEN NULL
    ELSE year END;
UPDATE {script.damWorkingTable} SET height_m = 
    CASE
    WHEN dam_height_m = -99 THEN NULL
    ELSE dam_height_m END;
UPDATE {script.damWorkingTable} SET length_m = 
    CASE
    WHEN dam_len_m = -99 THEN NULL
    ELSE dam_len_m END;
UPDATE {script.damWorkingTable} SET reservoir_area_skm = 
    CASE
    WHEN area_skm = -99 THEN NULL
    ELSE area_skm END;
UPDATE {script.damWorkingTable} SET reservoir_depth_m = 
    CASE
    WHEN depth_m = -99 THEN NULL
    ELSE depth_m END;
UPDATE {script.damWorkingTable} SET reservoir_present = 
    CASE
    WHEN area_skm = -99 THEN FALSE
    WHEN area_skm > 0 THEN TRUE
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = cap_mcm;
UPDATE {script.damWorkingTable} SET avg_rate_of_discharge_ls = dis_avg_ls;
UPDATE {script.damWorkingTable} SET degree_of_regulation_pc = dor_pc;
UPDATE {script.damWorkingTable} SET catchment_area_skm =
    CASE
    WHEN catch_skm = 0 THEN NULL
    ELSE catch_skm END;
UPDATE {script.damWorkingTable} SET use_code =
    CASE 
    WHEN main_use = 'Irrigation' THEN 1
    WHEN main_use = 'Hydroelectricity' THEN 2
    WHEN main_use = 'Water supply' THEN 3
    WHEN main_use = 'Flood control' THEN 4
    WHEN main_use = 'Recreation' THEN 5
    WHEN main_use = 'Other' THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_irrigation_code =
    CASE 
    WHEN use_irri = 'Sec' THEN 3
    WHEN use_irri = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code =
    CASE 
    WHEN use_elec = 'Sec' THEN 3
    WHEN use_elec = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code =
    CASE 
    WHEN use_supp = 'Sec' THEN 3
    WHEN use_supp = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_floodcontrol_code =
    CASE 
    WHEN use_fcon = 'Sec' THEN 3
    WHEN use_fcon = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_recreation_code =
    CASE 
    WHEN use_recr = 'Sec' THEN 3
    WHEN use_recr = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_navigation_code =
    CASE 
    WHEN use_navi = 'Sec' THEN 3
    WHEN use_navi = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_fish_code =
    CASE 
    WHEN use_fish = 'Sec' THEN 3
    WHEN use_fish = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_pollution_code =
    CASE 
    WHEN use_pcon = 'Sec' THEN 3
    WHEN use_pcon = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_other_code =
    CASE 
    WHEN use_other = 'Sec' THEN 3
    WHEN use_other = 'Main' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET lake_control_code =
    CASE 
    WHEN lake_ctrl = 'Yes' THEN 1
    WHEN lake_ctrl = 'Maybe' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET "comments" = 
    CASE
    WHEN comments_orig ILIKE '%replaces%' OR comments_orig IN('Not sure', 'Polygon too large?') THEN NULL
    ELSE comments_orig END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN res_name,
    DROP COLUMN river,
    DROP COLUMN year,
    DROP COLUMN dam_height_m,
    DROP COLUMN dam_len_m,
    DROP COLUMN area_skm,
    DROP COLUMN depth_m,
    DROP COLUMN cap_mcm,
    DROP COLUMN dis_avg_ls,
    DROP COLUMN dor_pc,
    DROP COLUMN catch_skm,
    DROP COLUMN main_use,
    DROP COLUMN use_irri,
    DROP COLUMN use_elec,
    DROP COLUMN use_supp,
    DROP COLUMN use_fcon,
    DROP COLUMN use_recr,
    DROP COLUMN use_navi,
    DROP COLUMN use_fish,
    DROP COLUMN use_pcon,
    DROP COLUMN use_other,
    DROP COLUMN lake_ctrl,
    DROP COLUMN comments_orig;
    
"""

script.do_work(query)
