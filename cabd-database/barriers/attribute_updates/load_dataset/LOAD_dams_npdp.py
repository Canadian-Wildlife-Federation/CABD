import LOAD_dams_main as main

script = main.DamLoadingScript("npdp")

query = f"""
--add new columns and populate tempTable with mapped attributes
ALTER TABLE {script.tempTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.tempTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN height_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN length_m float4;
ALTER TABLE {script.tempTable} ADD COLUMN use_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.tempTable} ADD COLUMN maintenance_last date;
ALTER TABLE {script.tempTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.tempTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.tempTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.tempTable} ADD COLUMN generating_capacity_mwh float8;
ALTER TABLE {script.tempTable} ADD COLUMN data_source text;

UPDATE {script.tempTable} SET dam_name_en = dam_name;
UPDATE {script.tempTable} SET waterbody_name_en = river;
UPDATE {script.tempTable} SET construction_type_code =
    CASE
    WHEN dam_type = 'Arch' THEN 1
    WHEN dam_type = 'Arch; Rockfill' THEN 1
    WHEN dam_type = 'Arch; Rockfill; Gravity' THEN 1
    WHEN dam_type = 'Buttress' THEN 2
    WHEN dam_type = 'Buttress; Earth' THEN 2
    WHEN dam_type = 'Earth' THEN 3
    WHEN dam_type = 'Earth; Gravity' THEN 3
    WHEN dam_type = 'Earth; Gravity; Rockfill' THEN 3
    WHEN dam_type = 'Earth; Rockfill' THEN 3
    WHEN dam_type = 'Earth; Rockfill; Arch' THEN 3
    WHEN dam_type = 'Earth; Rockfill; Gravity' THEN 3
    WHEN dam_type = 'Earth;Gravity' THEN 3
    WHEN dam_type = 'Earth;Rockfill' THEN 3
    WHEN dam_type = 'Gavity' THEN 4
    WHEN dam_type = 'Gravity' THEN 4
    WHEN dam_type = 'Gravity; Earth' THEN 4
    WHEN dam_type = 'Gravity; Rockfill' THEN 4
    WHEN dam_type = 'Gravity; Rockfill; Earth' THEN 4
    WHEN dam_type = 'Multi-Arch' THEN 5
    WHEN dam_type = 'Multiple Arch' THEN 5
    WHEN dam_type = 'Rockfill' THEN 6
    WHEN dam_type = 'Rockfill; Earth' THEN 6
    WHEN dam_type = 'Rockfill; Earth; Gravity' THEN 6
    WHEN dam_type = 'Rockfill; Gravity' THEN 6
    WHEN dam_type = 'Unknown' THEN 9
    WHEN dam_type IS NULL THEN 9 END;
UPDATE {script.tempTable} SET height_m = dam_height_m;
UPDATE {script.tempTable} SET length_m = dam_length_m;
UPDATE {script.tempTable} SET use_code =
    CASE 
    WHEN main_purpose = 'Irrigation' THEN 1
    WHEN main_purpose = 'Hydroelectricity' THEN 2
    WHEN main_purpose = 'Water Supply' THEN 3
    WHEN main_purpose = 'Flood Control' THEN 4
    WHEN main_purpose = 'Recreation' THEN 5
    WHEN main_purpose = 'Navigation' THEN 6
    WHEN main_purpose = 'Fishery' THEN 7
    WHEN main_purpose = 'Pollution control' THEN 8
    WHEN main_purpose = 'Invasive species control' THEN 9
    WHEN main_purpose = 'Other' THEN 10
    ELSE NULL END;
UPDATE {script.tempTable} SET use_irrigation_code =
    CASE
    WHEN main_purpose = 'Irrigation' THEN 1
    WHEN other_purposes = 'Irrigation' THEN 2
    WHEN other_purposes = 'Irrigation; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_electricity_code =
    CASE
    WHEN main_purpose = 'Hydroelectricity' THEN 1
    WHEN other_purposes = 'Hydroelectricity' THEN 2
    WHEN other_purposes = 'Hydroelectricity; Recreation' THEN 2 
    WHEN other_purposes = 'Hydroelectricity; Water Supply' THEN 2
    WHEN other_purposes = 'Hydropower; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Hydroelectricity' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_supply_code =
    CASE
    WHEN main_purpose = 'Water Supply' THEN 1
    WHEN other_purposes = 'Water Supply' THEN 2
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 2 
    WHEN other_purposes = 'Water Supply; Hydroelectricity' THEN 2
    WHEN other_purposes = 'Water Supply; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 3 
    WHEN other_purposes = 'Hydroelectricity; Water Supply' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_floodcontrol_code =
    CASE
    WHEN main_purpose = 'Flood Control' THEN 1
    WHEN other_purposes = 'Flood Control' THEN 2
    WHEN other_purposes = 'Flood Control, Navigation' THEN 2
    WHEN other_purposes = 'Flood Control; Navigation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 2
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_recreation_code =
    CASE
    WHEN main_purpose = 'Recreation' THEN 1
    WHEN other_purposes = 'Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation' THEN 3
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 3 
    WHEN other_purposes = 'Hydroelectricity; Recreation' THEN 3
    WHEN other_purposes = 'Hydropower; Recreation' THEN 3
    WHEN other_purposes = 'Irrigation; Recreation' THEN 3
    WHEN other_purposes = 'Navigation; Recreation' THEN 3
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 3
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 3 
    WHEN other_purposes = 'Water Supply; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET use_navigation_code =
    CASE
    WHEN other_purposes = 'Navigation' THEN 2
    WHEN other_purposes = 'Navigation; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control, Navigation' THEN 3
    WHEN other_purposes = 'Flood Control; Navigation' THEN 3
    ELSE NULL END;
UPDATE {script.tempTable} SET maintenance_last = ('01-01-' || year_modified)::date;
UPDATE {script.tempTable} SET construction_year = year_completed;
UPDATE {script.tempTable} SET storage_capacity_mcm = (regexp_replace(normal_reservoir_storage_m3, '[^0-9.]+', '', 'g'))::float8;
UPDATE {script.tempTable} SET storage_capacity_mcm = storage_capacity_mcm / 1000000;
UPDATE {script.tempTable} SET reservoir_present =
    CASE
    WHEN storage_capacity_mcm IS NOT NULL AND storage_capacity_mcm > 0 THEN TRUE
    ELSE FALSE END;
UPDATE {script.tempTable} SET generating_capacity_mwh = electric_capacity;
UPDATE {script.tempTable} SET data_source = 'NPDP_' || npdp_id;

ALTER TABLE {script.tempTable} ALTER COLUMN data_source SET NOT NULL;
ALTER TABLE {script.tempTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.tempTable} ADD PRIMARY KEY (data_source);
ALTER TABLE {script.tempTable} DROP COLUMN fid;

--create workingTable and insert mapped attributes
--ensure all the columns match the new columns you added
CREATE TABLE {script.workingTable}(
    cabd_id uuid,
    dam_name_en varchar(512),
    waterbody_name_en varchar(512),
    construction_type_code int2,
    height_m float4,
    length_m float4,
    use_code int2,
    use_irrigation_code int2,
    use_electricity_code int2,
    use_supply_code int2,
    use_floodcontrol_code int2,
    use_recreation_code int2,
    use_navigation_code int2,
    maintenance_last date,
    construction_year numeric,
    storage_capacity_mcm float8,
    reservoir_present bool,
    generating_capacity_mwh float8,
    data_source text PRIMARY KEY
);
INSERT INTO {script.workingTable}(
    dam_name_en,
    waterbody_name_en,
    construction_type_code,
    height_m,
    length_m,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    maintenance_last,
    construction_year,
    storage_capacity_mcm,
    reservoir_present,
    generating_capacity_mwh,
    data_source
)
SELECT
    dam_name_en,
    waterbody_name_en,
    construction_type_code,
    height_m,
    length_m,
    use_code,
    use_irrigation_code,
    use_electricity_code,
    use_supply_code,
    use_floodcontrol_code,
    use_recreation_code,
    use_navigation_code,
    maintenance_last,
    construction_year,
    storage_capacity_mcm,
    reservoir_present,
    generating_capacity_mwh,
    data_source
FROM {script.tempTable};

--delete extra fields from tempTable except data_source
ALTER TABLE {script.tempTable}
    DROP COLUMN dam_name_en,
    DROP COLUMN waterbody_name_en,
    DROP COLUMN construction_type_code,
    DROP COLUMN height_m,
    DROP COLUMN length_m,
    DROP COLUMN use_code,
    DROP COLUMN use_irrigation_code,
    DROP COLUMN use_electricity_code,
    DROP COLUMN use_supply_code,
    DROP COLUMN use_floodcontrol_code,
    DROP COLUMN use_recreation_code,
    DROP COLUMN use_navigation_code,
    DROP COLUMN maintenance_last,
    DROP COLUMN construction_year,
    DROP COLUMN storage_capacity_mcm,
    DROP COLUMN reservoir_present,
    DROP COLUMN generating_capacity_mwh;

-- Finding CABD IDs...
UPDATE
	{script.workingTable} AS npdp
SET
	cabd_id = duplicates.cabd_dam_id
FROM
	{script.duplicatestable} AS duplicates
WHERE
	npdp.data_source = duplicates.data_source
	OR npdp.data_source = duplicates.dups_npdp;
"""

script.do_work(query)