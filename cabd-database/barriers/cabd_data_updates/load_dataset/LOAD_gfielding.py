import LOAD_main as main

script = main.LoadingScript("gfielding")

query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = dam_id_number;
UPDATE {script.sourceTable} SET data_source = '41b947a0-867d-4dd1-aa08-3609bf5679de';
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
        name_of_structure,
        year_constructed,
        main_purpose_of_dam,
        primary_function_of_dam,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_electricity_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_supply_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_floodcontrol_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_recreation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_navigation_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN use_pollution_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN function_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN operating_status_code int2;
ALTER TABLE {script.damWorkingTable} ADD COLUMN "comments" text;

UPDATE {script.damWorkingTable} SET dam_name_en = name_of_structure;
UPDATE {script.damWorkingTable} SET construction_year =
    CASE
    WHEN regexp_match(year_constructed, '^[0-9]{{4}}$') IS NOT NULL THEN year_constructed::numeric
    WHEN regexp_match(year_constructed, '^~[0-9]{{4}}$') IS NOT NULL THEN (regexp_replace(year_constructed, '[^0-9]', '', 'g'))::numeric
    WHEN regexp_match(year_constructed, '^[0-9]{{4}}-[0-9]{{1,4}}$') IS NOT NULL THEN SPLIT_PART(year_constructed, '-', 1)::numeric
    WHEN regexp_match(year_constructed, '^(circa)\s[0-9]{{4}}$') IS NOT NULL THEN (regexp_replace(year_constructed, '[^0-9]', '', 'g'))::numeric
    WHEN year_constructed = 'April, 1985' THEN 1985
    WHEN year_constructed = 'Aug. 1st/1989' THEN 1989
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_code =
    CASE
    WHEN main_purpose_of_dam = 'Water supply - agricultural' THEN 1
    WHEN main_purpose_of_dam = 'water supply - hydroelectric' THEN 2
    WHEN main_purpose_of_dam in('Water supply - municipal', 'Water supply - domestic') THEN 3
    WHEN primary_function_of_dam = 'Aboiteau or other flood reduction structure' THEN 4
    WHEN main_purpose_of_dam in('Water supply - recreation facilities', 'Non consumptive - aquatic recreation enhancement') THEN 5
    WHEN primary_function_of_dam = 'Navigation aid' THEN 6
    WHEN primary_function_of_dam = 'Mine tailings management' THEN 8
    WHEN main_purpose_of_dam is not null AND main_purpose_of_dam <> 'N/A' then 10
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_electricity_code =
    CASE
    WHEN main_purpose_of_dam = 'water supply - hydroelectric' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_supply_code = 
    CASE
    WHEN main_purpose_of_dam in('Water supply - municipal', 'Water supply - domestic') THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_floodcontrol_code = 
    CASE
    WHEN primary_function_of_dam = 'Aboiteau or other flood reduction structure' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_recreation_code = 
    CASE
    WHEN main_purpose_of_dam in('Water supply - recreation facilities', 'Non consumptive - aquatic recreation enhancement') THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_navigation_code = 
    CASE
    WHEN primary_function_of_dam = 'Navigation aid' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET use_pollution_code = 
    CASE
    WHEN primary_function_of_dam = 'Mine tailings management' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET function_code = 
    CASE
    WHEN primary_function_of_dam = 'Water impoundment/storage' THEN 1
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET operating_status_code =
    CASE
    WHEN primary_function_of_dam = 'Decommissioned' THEN 3
    ELSE NULL END;
UPDATE {script.damWorkingTable} SET "comments" = main_purpose_of_dam;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN name_of_structure,
    DROP COLUMN year_constructed,
    DROP COLUMN main_purpose_of_dam,
    DROP COLUMN primary_function_of_dam;

"""

script.do_work(query)