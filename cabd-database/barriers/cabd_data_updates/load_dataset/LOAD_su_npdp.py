import LOAD_main as main

script = main.LoadingScript("npdp")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = npdp_id;
UPDATE {script.sourceTable} SET data_source = '6a9ca7af-1ae6-4b98-b79a-c207eeaf2bd9';
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
        river,
        dam_type,
        dam_height_m,
        dam_length_m,
        main_purpose,
        other_purposes,
        year_modified,
        year_completed,
        normal_reservoir_storage_m3,
        electric_capacity,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN maintenance_last date;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_present bool;
ALTER TABLE {script.damWorkingTable} ADD COLUMN generating_capacity_mwh float8;

UPDATE {script.damWorkingTable} SET dam_name_en = dam_name;
UPDATE {script.damWorkingTable} SET waterbody_name_en = river;
UPDATE {script.damWorkingTable} SET construction_type_code =
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
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET height_m = dam_height_m;
UPDATE {script.damWorkingTable} SET length_m = dam_length_m;
UPDATE {script.damWorkingTable} SET use_code =
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
UPDATE {script.damWorkingTable} SET use_irrigation_code =
    CASE
    WHEN main_purpose = 'Irrigation' THEN 1
    WHEN other_purposes = 'Irrigation' THEN 2
    WHEN other_purposes = 'Irrigation; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Irrigation; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code =
    CASE
    WHEN main_purpose = 'Hydroelectricity' THEN 1
    WHEN other_purposes = 'Hydroelectricity' THEN 2
    WHEN other_purposes = 'Hydroelectricity; Recreation' THEN 2 
    WHEN other_purposes = 'Hydroelectricity; Water Supply' THEN 2
    WHEN other_purposes = 'Hydropower; Recreation' THEN 2
    WHEN other_purposes = 'Water Supply; Hydroelectricity' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code =
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
UPDATE {script.damWorkingTable} SET use_floodcontrol_code =
    CASE
    WHEN main_purpose = 'Flood Control' THEN 1
    WHEN other_purposes = 'Flood Control' THEN 2
    WHEN other_purposes = 'Flood Control, Navigation' THEN 2
    WHEN other_purposes = 'Flood Control; Navigation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control; Recreation; Water Supply' THEN 2
    WHEN other_purposes = 'Water Supply; Flood Control; Recreation' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_recreation_code =
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
UPDATE {script.damWorkingTable} SET use_navigation_code =
    CASE
    WHEN other_purposes = 'Navigation' THEN 2
    WHEN other_purposes = 'Navigation; Recreation' THEN 2
    WHEN other_purposes = 'Flood Control, Navigation' THEN 3
    WHEN other_purposes = 'Flood Control; Navigation' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET maintenance_last = ('01-01-' || year_modified)::date;
UPDATE {script.damWorkingTable} SET construction_year = year_completed;
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = (regexp_replace(normal_reservoir_storage_m3, '[^0-9.]+', '', 'g'))::float8;
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = storage_capacity_mcm / 1000000;
UPDATE {script.damWorkingTable} SET reservoir_present =
    CASE
    WHEN storage_capacity_mcm IS NOT NULL AND storage_capacity_mcm > 0 THEN TRUE
    ELSE FALSE END;
UPDATE {script.damWorkingTable} SET generating_capacity_mwh = electric_capacity;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN river,
    DROP COLUMN dam_type,
    DROP COLUMN dam_height_m,
    DROP COLUMN dam_length_m,
    DROP COLUMN main_purpose,
    DROP COLUMN other_purposes,
    DROP COLUMN year_modified,
    DROP COLUMN year_completed,
    DROP COLUMN normal_reservoir_storage_m3,
    DROP COLUMN electric_capacity;
    
"""

script.do_work(query)
