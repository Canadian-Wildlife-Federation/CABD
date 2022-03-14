import LOAD_main as main

script = main.LoadingScript("nlprov")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source uuid;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = dam_index_num;
UPDATE {script.sourceTable} SET data_source = '2bab6e19-ef39-4973-b9e2-f4c47617ff2c';
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
        owner_name,
        year_built,
        dam_status,
        purpose_drinking,
        purpose_industrial,
        purpose_hydro,
        purpose_flood,
        purpose_ice,
        purpose_forestry,
        purpose_other,
        dam_type,
        dim_max_height,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_other_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;

UPDATE {script.damWorkingTable} SET dam_name_en = dam_name;
UPDATE {script.damWorkingTable} SET "owner" = owner_name;

UPDATE {script.damWorkingTable} SET ownership_type_code =
    CASE
    WHEN "owner" = 'Ducks Unlimited Canada' THEN 1
    WHEN "owner" LIKE 'Parks%' THEN 2
    WHEN 
        (regexp_match("owner", '(?i)(Town)|(City)|(Local)|(Works)|(Division)|(Community Gov)|(First Nation)|(Grenfell)') IS NOT NULL)
        THEN 3
    WHEN
        (regexp_match("owner", '(Natural)|(Government of)|(Nalcor)') IS NOT NULL)
        THEN 5
    WHEN "owner" IN(
        'Memorial University of Newfoundland',
        'North Shore Development Association',
        'Ocean Pond Projects Association')
        THEN 6
    WHEN "owner" = 'Unknown Owner' THEN 7
    ELSE 4 END;

UPDATE {script.damWorkingTable} SET construction_year = year_built::numeric;
UPDATE {script.damWorkingTable} SET operating_status_code =
    CASE
    WHEN dam_status = 'Abandoned' THEN 1
    WHEN dam_status = 'Active' THEN 2
    WHEN dam_status =  'Decommissioned' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_code =
    CASE 
    WHEN purpose_drinking = 'Primary' THEN 3
    WHEN purpose_industrial = 'Primary' THEN 10
    WHEN purpose_hydro = 'Primary' THEN 2
    WHEN purpose_flood = 'Primary' THEN 4
    WHEN purpose_ice = 'Primary' THEN 10
    WHEN purpose_forestry = 'Primary' THEN 10
    WHEN purpose_other = 'Primary' THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code =
    CASE 
    WHEN purpose_hydro = 'Primary' THEN 1
    WHEN purpose_hydro = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code =
    CASE
    WHEN purpose_drinking = 'Primary' THEN 1
    WHEN purpose_drinking = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_floodcontrol_code =
    CASE
    WHEN purpose_flood = 'Primary' THEN 1
    WHEN purpose_flood = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_other_code =
    CASE
    WHEN purpose_other = 'Primary' THEN 1
    WHEN purpose_other = 'Secondary' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET construction_type_code =
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
UPDATE {script.damWorkingTable} SET height_m = dim_max_height;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN owner_name,
    DROP COLUMN year_built,
    DROP COLUMN dam_status,
    DROP COLUMN purpose_drinking,
    DROP COLUMN purpose_industrial,
    DROP COLUMN purpose_hydro,
    DROP COLUMN purpose_flood,
    DROP COLUMN purpose_ice,
    DROP COLUMN purpose_forestry,
    DROP COLUMN purpose_other,
    DROP COLUMN dam_type,
    DROP COLUMN dim_max_height;

"""

script.do_work(query)