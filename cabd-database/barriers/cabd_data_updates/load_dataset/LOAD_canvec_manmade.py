import LOAD_main as main

script = main.LoadingScript("canvec_manmade")
    
query = f"""

--data source fields
ALTER TABLE {script.sourceTable} ADD COLUMN data_source varchar;
ALTER TABLE {script.sourceTable} ADD COLUMN data_source_id varchar;
UPDATE {script.sourceTable} SET data_source_id = feature_id;
UPDATE {script.sourceTable} SET data_source = '4bb309bf-be07-47bf-b134-9a43834001c2';
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
        name_1_en,
        name_1_fr,
        is_in_operation_en,
        data_source,
        data_source_id
    FROM {script.sourceTable};

ALTER TABLE {script.damWorkingTable} ALTER COLUMN data_source_id SET NOT NULL;
ALTER TABLE {script.damWorkingTable} ADD PRIMARY KEY (data_source_id);
ALTER TABLE {script.damWorkingTable} ADD COLUMN cabd_id uuid;
ALTER TABLE {script.damWorkingTable} ADD CONSTRAINT data_source_fkey FOREIGN KEY (data_source) REFERENCES cabd.data_source (id);

ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_en varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN dam_name_fr varchar(512);
ALTER TABLE {script.damWorkingTable} ADD COLUMN operating_status_code int2;

UPDATE {script.damWorkingTable} SET dam_name_en = name_1_en;
UPDATE {script.damWorkingTable} SET dam_name_fr = name_1_fr;
UPDATE {script.damWorkingTable} SET operating_status_code = CASE WHEN is_in_operation_en = 'True' THEN 2 ELSE NULL END;

--delete extra fields so only mapped fields remain
ALTER TABLE {script.damWorkingTable}
    DROP COLUMN name_1_en,
    DROP COLUMN name_1_fr,
    DROP COLUMN is_in_operation_en;

"""

script.do_work(query)