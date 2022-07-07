import LOAD_main as main

script = main.LoadingScript("wsa_sk_owned_dams")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = OBJECTID;
UPDATE {script.sourceTable} SET data_source = (SELECT id FROM cabd.data_source WHERE name = '{script.datasetname}');
ALTER TABLE {script.sourceTable} ALTER COLUMN data_source TYPE uuid USING data_source::uuid;
ALTER TABLE {script.sourceTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);
ALTER TABLE {script.sourceTable} DROP CONSTRAINT {script.datasetname}_pkey;
ALTER TABLE {script.sourceTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.sourceTable} DROP COLUMN IF EXISTS fid;
ALTER TABLE {script.sourceTable} DROP COLUMN geometry;


--add new columns and map attributes
DROP TABLE IF EXISTS {script.damWorkingTable};
CREATE TABLE {script.damWorkingTable} AS
    SELECT 
        dam_name,
        construct,
        dam_height_in_metres,
        reservoir_capacity_in_cubic_decametres,
        year_of_upgrade_or_expansion,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN construction_year numeric;
ALTER TABLE {script.damWorkingTable} ADD COLUMN height_m float4;
ALTER TABLE {script.damWorkingTable} ADD COLUMN storage_capacity_mcm float8;
ALTER TABLE {script.damWorkingTable} ADD COLUMN maintenance_last date;
UPDATE {script.damWorkingTable} SET reservoir_capacity_in_cubic_decametres = replace(reservoir_capacity_in_cubic_decametres, ',', '');
ALTER TABLE {script.damWorkingTable} ALTER COLUMN reservoir_capacity_in_cubic_decametres TYPE float8 USING reservoir_capacity_in_cubic_decametres::float8;

UPDATE {script.damWorkingTable} SET dam_name_en = dam_name;
UPDATE {script.damWorkingTable} SET construction_year = construct;
UPDATE {script.damWorkingTable} SET height_m = dam_height_in_metres;
UPDATE {script.damWorkingTable} SET storage_capacity_mcm = (reservoir_capacity_in_cubic_decametres/1000);
UPDATE {script.damWorkingTable} SET maintenance_last = ('01-01-' || year_of_upgrade_or_expansion)::date;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN dam_name,
    DROP COLUMN construct,
    DROP COLUMN dam_height_in_metres,
    DROP COLUMN reservoir_capacity_in_cubic_decametres,
    DROP COLUMN year_of_upgrade_or_expansion;

"""

script.do_work(query)
