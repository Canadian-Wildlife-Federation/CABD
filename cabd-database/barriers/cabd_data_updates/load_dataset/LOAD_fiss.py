import LOAD_main as main

script = main.LoadingScript("fiss")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = fish_obstacle_point_id;
UPDATE {script.sourceTable} SET data_source = '67ecfa8f-e156-45ef-81b5-fb93bd5b23c4';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--split into dams, add new columns, and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT
        height,
        length,
        gazetted_name,
        obstacle_name,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE (obstacle_name ILIKE '%Dam%' AND obstacle_name NOT ILIKE '%Beaver%');

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN function_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_fish_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_other_code int2;

UPDATE {script.damWorkingTable} SET height_m = 
    CASE
    WHEN height = 9999 THEN NULL
    WHEN height > 0 THEN height
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET length_m = 
    CASE
    WHEN length = 9999 THEN NULL
    WHEN length > 0 THEN length
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET waterbody_name_en = gazetted_name;

UPDATE {script.damWorkingTable} SET use_code = 
    CASE
    WHEN obstacle_name = 'Irrigation District Dam' THEN 1
    WHEN obstacle_name ILIKE 'Hydro Dam' THEN 2
    WHEN obstacle_name = 'Fisheries Management Dam' THEN 7
    WHEN obstacle_name = 'Water Management Storage Dam' THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET function_code = 
    CASE
    WHEN obstacle_name = 'Water Management Storage Dam' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_irrigation_code = 
    CASE
    WHEN obstacle_name = 'Irrigation District Dam' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code =
    CASE
    WHEN obstacle_name ILIKE 'Hydro Dam' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_fish_code =
    CASE
    WHEN obstacle_name = 'Fisheries Management Dam' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_other_code =
    CASE
    WHEN obstacle_name = 'Water Management Storage Dam' THEN 1
    ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN height,
    DROP COLUMN length,
    DROP COLUMN gazetted_name,
    DROP COLUMN obstacle_name;


--split into waterfalls, add new columns, and map attributes
DROP TABLE IF EXISTS {script.fallWorkingTable};
CREATE TABLE {script.fallWorkingTable} AS
    SELECT
        gazetted_name,
        height,
        data_source,
        data_source_id
    FROM {script.sourceTable} WHERE obstacle_name IN ('Cascade', 'Cascade or Chute', 'Cascade/Chute', 'Falls');

ALTER TABLE {script.fallWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.fallWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.fallWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.fallWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.fallWorkingTable} ADD COLUMN fall_height_m float4;

UPDATE {script.fallWorkingTable} SET waterbody_name_en = gazetted_name;
UPDATE {script.fallWorkingTable} SET fall_height_m =
    CASE
    WHEN height = 9999 OR height = -1000 THEN NULL
    WHEN height > 0 THEN height
    ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.fallWorkingTable}
    DROP COLUMN gazetted_name,
    DROP COLUMN height;

"""

script.do_work(query)