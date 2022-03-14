import LOAD_main as main

script = main.LoadingScript("megis_impounds")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = unique_id;
UPDATE {script.sourceTable} SET data_source = '22603047-8dd3-41e1-9891-d5cff42ec3f6';
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (fid);
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        fid,
        "name",
        river,
        owner,
        owner_code,
        maj_type,
        maj_purpose,
        year_completed,
        struct_hgt,
        length,
        norm_cap,
        state_reg,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (fid);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} RENAME COLUMN "owner" TO owner_orig;
ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN waterbody_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN "owner" varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN ownership_type_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_type_code int2;
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
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN length_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN provincial_compliance_status varchar(64);

UPDATE {script.damWorkingTable} SET dam_name_en = "name";
UPDATE {script.damWorkingTable} SET waterbody_name_en = initcap(river);
UPDATE {script.damWorkingTable} SET "owner" = initcap(owner_orig);
UPDATE {script.damWorkingTable} SET ownership_type_code =
    CASE
    WHEN owner_code = 'F' THEN 2
    WHEN (regexp_match("owner", '(?i)(Water Dist)|(City)|(Town)') IS NOT NULL) THEN 3
    WHEN (regexp_match("owner", '(?i)(Assn)') IS NOT NULL) THEN 6
    WHEN "owner" ILIKE '%Co' THEN 4
    WHEN (regexp_match("owner", '(?i)(Corp)|(Inc)') IS NOT NULL) THEN 4
    WHEN (regexp_match("owner", '(?i)(Game)|(Church)|(East Pond)|(F+G)|(Desert)|(National Wildlife)') IS NOT NULL) THEN 1
    WHEN (regexp_match("owner", '(?i)(Inland Fish)|(State Of Me)') IS NOT NULL) THEN 5
    WHEN "owner" IN (
        'Brindis Leather Company',
        'Burnham Hydro.',
        'Diamond International',
        'Ellsworth Falls Lumber C',
        'Guilford Industries',
        'Hudson Pulp & Paper',
        'Madison Electric Works',
        'Maine Yankee Atomic Pwr',
        'Mc Cain Foods',
        'Miller Industries',
        'Miller Realty',
        'Rogers',
        'Seabright Development')
        THEN 4
    WHEN "owner" = 'Unknown' THEN 7
    ELSE 6 END;
UPDATE {script.damWorkingTable} SET construction_type_code =
    CASE
    WHEN maj_type LIKE 'VA%' THEN 1
    WHEN maj_type LIKE 'CB%' THEN 2
    WHEN maj_type LIKE 'RE%' THEN 3
    WHEN maj_type LIKE 'PG%' THEN 4
    WHEN maj_type LIKE 'ER%' THEN 6
    WHEN maj_type LIKE 'OT%' THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_code =
    CASE
    WHEN maj_purpose LIKE 'I%' THEN 1
    WHEN maj_purpose LIKE 'H%' THEN 2
    WHEN maj_purpose LIKE 'S%' THEN 3
    WHEN maj_purpose LIKE 'C%' THEN 4
    WHEN maj_purpose LIKE 'R%' THEN 5
    WHEN maj_purpose LIKE 'N%' THEN 6
    WHEN maj_purpose LIKE 'D%' THEN 8
    WHEN maj_purpose LIKE 'O%' THEN 10
    WHEN maj_purpose LIKE 'P%' THEN 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_irrigation_code = 
    CASE
    WHEN use_code = 1 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[I]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[I]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code = 
    CASE
    WHEN use_code = 2 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[H]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[H]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code =
    CASE
    WHEN use_code = 3 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[S]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[S]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_floodcontrol_code =
    CASE
    WHEN use_code = 4 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[C]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[C]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_recreation_code =
    CASE
    WHEN use_code = 5 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[R]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[R]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_navigation_code =
    CASE
    WHEN use_code = 6 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[N]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[N]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_fish_code =
    CASE
    WHEN (regexp_match(maj_purpose, '^.{1}[F]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[F]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_pollution_code =
    CASE
    WHEN use_code = 8 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[DT]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[DT]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_other_code =
    CASE
    WHEN use_code = 10 THEN 1
    WHEN (regexp_match(maj_purpose, '^.{1}[OP]') IS NOT NULL) THEN 2
    WHEN (regexp_match(maj_purpose, '^.{2}[OP]') IS NOT NULL) THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET construction_year = year_completed::numeric;
UPDATE {script.damWorkingTable} SET height_m = struct_hgt/3.281;
UPDATE {script.damWorkingTable} SET length_m = length/3.281;
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = (norm_cap*1233)/1000000;
UPDATE {script.damWorkingTable} SET provincial_compliance_status = 
    CASE
    WHEN state_reg = 'DOE FERC' THEN 'US Department of Energy - FERC'
    WHEN state_reg = 'INT JOINT COMM' THEN 'International Joint Commission'
    ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN name,
    DROP COLUMN river,
    DROP COLUMN owner_orig,
    DROP COLUMN owner_code,
    DROP COLUMN maj_type,
    DROP COLUMN maj_purpose,
    DROP COLUMN year_completed,
    DROP COLUMN length,
    DROP COLUMN struct_hgt,
    DROP COLUMN norm_cap,
    DROP COLUMN state_reg;

"""

script.do_work(query)