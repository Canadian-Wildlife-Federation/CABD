import LOAD_main as main

script = main.LoadingScript("fao_aquastat")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = id_fao;
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
        name_of_dam,
        river,
        completed_operational_since,
        dam_height_m,
        reservoir_capacity_million_m3,
        reservoir_area_km2,
        irrigation,
        water_supply,
        flood_control,
        hydroelectricity_mw,
        navigation,
        recreation,
        pollution_control,
        other,
        comments_orig,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN reservoir_area_skm float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_irrigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.damWorkingTable} SET dam_name_en = name_of_dam;
UPDATE {script.damWorkingTable} SET waterbody_name_en =
    CASE
    WHEN regexp_match(river, '.*River.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Creek.*') IS NOT NULL THEN river
    WHEN regexp_match(river, '.*Falls.*') IS NOT NULL THEN river
    WHEN river IS NULL THEN NULL
    ELSE (river || ' River') END;
UPDATE {script.damWorkingTable} SET construction_year = 
    CASE
    WHEN completed_operational_since = 'Incomplete?' THEN NULL
    ELSE completed_operational_since::numeric END;
UPDATE {script.damWorkingTable} SET height_m = dam_height_m;
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = reservoir_capacity_million_m3;
UPDATE {script.damWorkingTable} SET reservoir_area_skm = reservoir_area_km2;
UPDATE {script.damWorkingTable} SET use_irrigation_code = 
    CASE
    WHEN irrigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code = 
    CASE
    WHEN water_supply = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_floodcontrol_code = 
    CASE
    WHEN flood_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code = 
    CASE
    WHEN hydroelectricity_mw = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_navigation_code = 
    CASE
    WHEN navigation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_recreation_code = 
    CASE
    WHEN recreation = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_pollution_code = 
    CASE
    WHEN pollution_control = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_other_code =
    CASE
    WHEN other = 'x' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET "comments" = comments_orig;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN name_of_dam,
    DROP COLUMN river,
    DROP COLUMN completed_operational_since,
    DROP COLUMN dam_height_m,
    DROP COLUMN reservoir_capacity_million_m3,
    DROP COLUMN reservoir_area_km2,
    DROP COLUMN irrigation,
    DROP COLUMN water_supply,
    DROP COLUMN flood_control,
    DROP COLUMN hydroelectricity_mw,
    DROP COLUMN navigation,
    DROP COLUMN recreation,
    DROP COLUMN pollution_control,
    DROP COLUMN other,
    DROP COLUMN comments_orig;

"""

script.do_work(query)
